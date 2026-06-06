"""
signal_forge.detection.detectors.offline_detector

Offline detector. Implements the EventDetector protocol.

Tracks last-seen timestamps per device. When an event arrives whose
timestamp exceeds threshold_seconds beyond a known device's last-seen,
that device transitions to "offline" and a DetectionEvent is emitted.

Replay-deterministic by construction: state is initialised empty, only
event-derived timestamps drive state changes, no wall-clock reads.

Design notes captured in docs/working-notes.md under D-7.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import ClassVar, Literal
from uuid import UUID

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.detection.types import DETECTION_TYPE_DEVICE_OFFLINE
from signal_forge.identity import derive
from signal_forge.streaming.event_protocol import TelemetryEvent

# State machine: unseen (implicit, not in the dict) -> seen -> offline.
# "seen -> offline" emits one detection. "offline -> seen" resets state
# silently; recovery is not a detection event in Phase 2.
_DeviceState = Literal["seen", "offline"]


@dataclass
class OfflineDetector:
    """
    Event detector emitting DetectionEvents when a previously-seen
    device falls silent beyond ``threshold_seconds``.

    Parameters
    ----------
    threshold_seconds:
        Silent gap (in event-time seconds) beyond which a seen device
        transitions to offline. Typically sourced from
        ``PlatformSettings.offline_threshold_seconds``.
    device_id_extractor:
        Callable mapping a ``TelemetryEvent`` to its ``device_id``, or
        ``None`` if the event does not identify a device. Events for
        which the extractor returns ``None`` do not affect detector
        state and do not trigger a scan.
    store_lookup:
        Callable mapping a ``device_id`` to its registered store, or
        ``None`` if the device is not registered. When a device crosses
        the offline threshold but ``store_lookup`` returns ``None``,
        no detection is emitted (the ``DetectionEvent`` schema requires
        a non-empty ``store_id``).
    """

    name: ClassVar[str] = "OfflineDetector"

    threshold_seconds: int
    device_id_extractor: Callable[[TelemetryEvent], UUID | None]
    store_lookup: Callable[[UUID], str | None]

    _last_seen: dict[UUID, datetime] = field(default_factory=dict)
    _state: dict[UUID, _DeviceState] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.threshold_seconds <= 0:
            raise ValueError("threshold_seconds must be > 0")

    def observe_event(self, event: TelemetryEvent) -> list[DetectionEvent]:
        device_id = self.device_id_extractor(event)
        if device_id is None:
            # Event doesn't identify a device. Skip entirely — including
            # the silence scan, because the event's timestamp is not
            # device-attributable.
            return []

        # Update state for the current device first, then scan others.
        # Order matters: the current device's _last_seen must be fresh
        # before the scan, otherwise it could appear in the scan as
        # "silent" against its own arrival.
        previous_state = self._state.get(device_id)
        self._last_seen[device_id] = event.event_timestamp
        self._state[device_id] = "seen"

        # Recovery transition: was offline, now back. No emission;
        # tracked silently. Logging the transition belongs to Phase 5,
        # not to a detector whose contract is "fire on offline".
        _ = previous_state  # explicit no-op; documents the read

        # Scan all known devices for silence breaches. Skip devices
        # already in the offline state (they emitted on entry).
        threshold = timedelta(seconds=self.threshold_seconds)
        detections: list[DetectionEvent] = []
        for d, last in self._last_seen.items():
            if self._state[d] != "seen":
                continue
            if event.event_timestamp - last <= threshold:
                continue
            # Crossed threshold. Look up store; if unregistered, skip.
            store_id = self.store_lookup(d)
            if not store_id:
                continue
            # Transition and emit.
            self._state[d] = "offline"
            detections.append(
                self._build_detection(
                    device_id=d,
                    store_id=store_id,
                    silent_for=event.event_timestamp - last,
                    source_event=event,
                )
            )
        return detections

    def _build_detection(
        self,
        *,
        device_id: UUID,
        store_id: str,
        silent_for: timedelta,
        source_event: TelemetryEvent,
    ) -> DetectionEvent:
        silent_seconds = int(silent_for.total_seconds())
        detection_id = derive(
            "detection.device_offline", store_id, device_id, source_event.event_id
        )
        payload = DetectionEventPayload(
            detection_id=detection_id,
            detection_type=DETECTION_TYPE_DEVICE_OFFLINE,
            severity=DetectionSeverity.WARNING,
            detected_at=source_event.event_timestamp,
            store_id=store_id,
            device_id=device_id,
            source_event_id=source_event.event_id,
            threshold_breached=(
                f"no events for {silent_seconds}s "
                f"(threshold {self.threshold_seconds}s)"
            ),
            details={
                "silent_seconds": silent_seconds,
                "threshold_seconds": self.threshold_seconds,
            },
        )
        return DetectionEvent(
            event_id=derive("event.detection", detection_id),
            event_timestamp=source_event.event_timestamp,
            trace=TraceContext(trace_id=source_event.trace.trace_id),
            payload=payload,
        )
