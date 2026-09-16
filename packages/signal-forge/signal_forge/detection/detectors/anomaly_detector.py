"""
signal_forge.detection.detectors.anomaly_detector

Signal anomaly detector. Implements the EmissionDetector protocol.

Observes window emissions and emits a DetectionEvent when the
emission value crosses a configured threshold in the configured
direction. State machine matches OfflineDetector and OutageDetector:
transition into anomaly emits, transition out resets silently.

The default aggregation_name is 'signal_value'. Operators tracking
multiple signals (latency, error rate, etc.) should subclass and
override aggregation_name per signal. Each subclass is routed
independently by the pipeline's emission-detector dispatch.

Design notes captured in docs/working-notes.md under D-7 (state
machine semantics) and D-8 (DetectionEvent schema).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Literal
from uuid import uuid4

from event_schema_contracts.base.identity import derive
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.detection.types import DETECTION_TYPE_SIGNAL_ANOMALY
from signal_forge.streaming.window_aggregator import WindowEmission

# State machine: not_anomalous (implicit, partition_key not in dict)
# -> anomalous. Transition into anomalous emits one detection;
# transition back resets state silently. Recovery is a Phase 5
# concern (alert acknowledgement and reminder cadence).
_AnomalyState = Literal["anomalous"]

_Comparison = Literal["above", "below"]


@dataclass
class AnomalyDetector:
    """
    Emission detector emitting DetectionEvents when an emission value
    crosses a configured threshold in the configured direction.

    Parameters
    ----------
    threshold:
        The value the emission's ``value`` is compared against.
    comparison:
        ``"above"`` (default) emits when ``emission.value > threshold``;
        ``"below"`` emits when ``emission.value < threshold``.
        Strict comparison — value exactly equal to threshold does NOT
        trigger.
    signal_name:
        Free-form label included in detection metadata. Use this to
        distinguish detectors watching different signals when reading
        alerts (e.g. "latency_ms" vs "errors_per_minute").
    """

    # Named for this component rather than left to BaseEvent's own
    # auto-injection, which defaults to "unknown" — indistinguishable
    # downstream from a producer that never named itself at all.
    _SOURCE = "signal-forge.anomaly_detector"

    name: ClassVar[str] = "AnomalyDetector"
    aggregation_name: ClassVar[str] = "signal_value"

    threshold: float
    comparison: _Comparison = "above"
    signal_name: str = "signal"

    _state: dict[str, _AnomalyState] = field(default_factory=dict)

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]:
        partition_key = emission.partition_key
        value = float(emission.value)

        is_anomalous = self._compare(value)
        currently_anomalous = self._state.get(partition_key) == "anomalous"

        if is_anomalous:
            if currently_anomalous:
                # Already in anomaly; don't re-emit. Once per transition.
                return []
            self._state[partition_key] = "anomalous"
            return [
                self._build_detection(
                    partition_key=partition_key,
                    value=value,
                    emission=emission,
                )
            ]
        else:
            if currently_anomalous:
                # Recovery: clear state silently.
                del self._state[partition_key]
            return []

    def _compare(self, value: float) -> bool:
        if self.comparison == "above":
            return value > self.threshold
        return value < self.threshold

    def _build_detection(
        self,
        *,
        partition_key: str,
        value: float,
        emission: WindowEmission,
    ) -> DetectionEvent:
        direction_word = "above" if self.comparison == "above" else "below"
        detection_id = derive(
            "detection.signal_anomaly",
            partition_key,
            self.signal_name,
            emission.window_start,
            emission.window_end,
        )
        payload = DetectionEventPayload(
            detection_id=detection_id,
            detection_type=DETECTION_TYPE_SIGNAL_ANOMALY,
            severity=DetectionSeverity.WARNING,
            detected_at=emission.window_end,
            store_id=partition_key,
            device_id=None,
            source_event_id=derive(
                "source.signal_anomaly",
                partition_key,
                self.signal_name,
                emission.window_start,
                emission.window_end,
            ),
            threshold_breached=(
                f"{self.signal_name} {value} {direction_word} "
                f"threshold {self.threshold}"
            ),
            details={
                "signal_name": self.signal_name,
                "value": value,
                "threshold": self.threshold,
                "comparison": self.comparison,
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
            metadata=EventMetadata(
                event_type=DetectionEvent.__event_type__,
                schema_version=DetectionEvent.__schema_version__,
                source=self._SOURCE,
            ),
            payload=payload,
        )
