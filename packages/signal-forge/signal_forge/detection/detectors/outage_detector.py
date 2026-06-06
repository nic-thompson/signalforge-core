"""
signal_forge.detection.detectors.outage_detector

Store outage detector. Implements the EmissionDetector protocol.

Observes window emissions from a DistinctCountAggregation registered
under the name "distinct_devices". For each emission, computes the
offline ratio for the store (1 - reporting / registered) and emits a
DetectionEvent when the ratio crosses threshold_ratio. State machine
mirrors OfflineDetector's: transition into outage emits, transition
back to not-outage resets silently.

Replay-deterministic: same input emission sequence produces the same
output detection sequence, including detection_id and source_event_id.
Those identity fields are derived via signal_forge.identity.derive
(UUIDv5 over stable coordinates — store_id and window bounds), not
minted as uuid4, so two runs over the same emissions produce
byte-identical identities. (They were uuid4-based through Phases 2-3
and made deterministic when the Phase 4 dataset layer's replay
byte-identity test required it.)

Design notes captured in docs/working-notes.md under D-7 (state
machine semantics) and D-8 (DetectionEvent schema).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import ClassVar, Literal
from uuid import uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.detection.types import DETECTION_TYPE_STORE_OUTAGE
from signal_forge.identity import derive
from signal_forge.streaming.window_aggregator import WindowEmission

# State machine: not_outage (implicit, store not in dict) -> outage.
# Transition into outage emits one detection. Transition back to
# not_outage resets state silently; recovery is not a detection event
# in Phase 2.
_StoreState = Literal["outage"]


@dataclass
class OutageDetector:
    """
    Emission detector emitting DetectionEvents when more than
    ``threshold_ratio`` of a store's devices are not reporting in a
    window.

    Parameters
    ----------
    threshold_ratio:
        Offline ratio above which the store transitions to outage.
        Strict greater-than: 0.5 means "more than 50%", not
        "50% or more". Typically sourced from
        ``PlatformSettings.outage_threshold_ratio``.
    registered_count_lookup:
        Callable mapping a store_id to its count of registered
        devices, or ``None`` if the store is not registered. Stores
        with no registered devices (count == 0) or unregistered
        stores (lookup returns None) are skipped silently — there's
        no meaningful ratio to compute.
    """

    name: ClassVar[str] = "OutageDetector"
    aggregation_name: ClassVar[str] = "distinct_devices"

    threshold_ratio: float
    registered_count_lookup: Callable[[str], int | None]

    _state: dict[str, _StoreState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0.0 < self.threshold_ratio < 1.0:
            raise ValueError(
                "threshold_ratio must be in the open interval (0.0, 1.0); "
                f"got {self.threshold_ratio}"
            )

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]:
        store_id = emission.partition_key
        reporting_count = emission.value

        registered = self.registered_count_lookup(store_id)
        if not registered:
            # Unregistered store or no registered devices. Skip: there
            # is no meaningful ratio to compute. Defensive against
            # misconfiguration (partition extractor returning something
            # the registry doesn't know about).
            return []

        # Defensive clamp: in correct operation reporting_count is
        # bounded by registered, but registry/aggregation can drift
        # transiently when new devices register mid-window. A negative
        # offline count is nonsense; treat as zero.
        offline_count = max(0, registered - reporting_count)
        offline_ratio = offline_count / registered

        currently_in_outage = self._state.get(store_id) == "outage"

        if offline_ratio > self.threshold_ratio:
            if currently_in_outage:
                # Already in outage, don't re-emit. Once per transition.
                return []
            self._state[store_id] = "outage"
            return [
                self._build_detection(
                    store_id=store_id,
                    offline_count=offline_count,
                    registered=registered,
                    offline_ratio=offline_ratio,
                    emission=emission,
                )
            ]
        else:
            # Below threshold. If we were in outage, reset state
            # silently. Recovery is not a detection event in Phase 2.
            if currently_in_outage:
                del self._state[store_id]
            return []

    def _build_detection(
        self,
        *,
        store_id: str,
        offline_count: int,
        registered: int,
        offline_ratio: float,
        emission: WindowEmission,
    ) -> DetectionEvent:
        offline_pct = round(offline_ratio * 100)
        threshold_pct = round(self.threshold_ratio * 100)
        detection_id = derive(
            "detection.store_outage",
            store_id,
            emission.window_start,
            emission.window_end,
        )
        payload = DetectionEventPayload(
            detection_id=detection_id,
            detection_type=DETECTION_TYPE_STORE_OUTAGE,
            severity=DetectionSeverity.CRITICAL,
            detected_at=emission.window_end,
            store_id=store_id,
            device_id=None,
            source_event_id=derive(
                "source.store_outage",
                store_id,
                emission.window_start,
                emission.window_end,
            ),
            threshold_breached=(
                f"{offline_count} of {registered} devices not reporting "
                f"({offline_pct}% offline, threshold {threshold_pct}%)"
            ),
            details={
                "offline_count": offline_count,
                "registered_count": registered,
                "offline_ratio": offline_ratio,
                "threshold_ratio": self.threshold_ratio,
                "window_start": emission.window_start.isoformat(),
                "window_end": emission.window_end.isoformat(),
            },
        )
        trace_id = (
            emission.last_contributing_trace_id
            if emission.last_contributing_trace_id is not None
            else str(uuid4())
        )
        return DetectionEvent(
            event_id=derive("event.detection", detection_id),
            event_timestamp=emission.window_end,
            trace=TraceContext(trace_id=trace_id),
            payload=payload,
        )
