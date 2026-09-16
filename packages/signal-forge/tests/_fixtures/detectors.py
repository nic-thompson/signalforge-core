"""
Reusable detector test doubles.

These fakes implement the EventDetector and EmissionDetector protocols
in a controllable way for pipeline integration tests:

- They record every input they observe, so tests can assert on the
  dispatch sequence ("detector D saw event E in this order").

- They emit zero, one, or many DetectionEvents on demand, so tests can
  exercise the pipeline's collection logic for empty/single/multiple
  detection cases.

- A separate ``RaisingEventDetector`` / ``RaisingEmissionDetector``
  pair lets tests exercise per-detector failure isolation.

The fakes use real ``TraceContext`` instances rather than duck-typed
test doubles because ``DetectionEvent`` enforces strict pydantic
validation on its trace field.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import ClassVar
from uuid import uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_SIGNAL_ANOMALY,
)
from signal_forge.streaming.event_protocol import TelemetryEvent
from signal_forge.streaming.window_aggregator import WindowEmission


def _build_detection(
    *,
    detection_type: str,
    detected_at: datetime,
    store_id: str,
    source_event_id,
    trace_id: str | None,
) -> DetectionEvent:
    """
    Construct a minimal valid DetectionEvent.

    ``trace_id`` is optional because emission detectors fall back to
    a fresh trace_id when ``WindowEmission.last_contributing_trace_id``
    is None (no prior contributor).
    """
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type=detection_type,
        severity=DetectionSeverity.INFO,
        detected_at=detected_at,
        store_id=store_id,
        source_event_id=source_event_id,
        threshold_breached="fake threshold",
    )
    return DetectionEvent(
        event_timestamp=detected_at,
        trace=TraceContext(trace_id=uuid4() if trace_id is None else trace_id),
        payload=payload,
    )


@dataclasses.dataclass
class FakeEventDetector:
    """
    EventDetector that records observations and emits a configurable
    number of detections per event.

    By default emits exactly one detection per observation. Set
    ``detections_per_observation=0`` to emit nothing, or a higher
    number for fan-out tests.
    """

    name: ClassVar[str] = "FakeEventDetector"

    detections_per_observation: int = 1
    observations: list[TelemetryEvent] = dataclasses.field(default_factory=list)

    def observe_event(self, event: TelemetryEvent) -> list[DetectionEvent]:
        self.observations.append(event)
        return [
            _build_detection(
                detection_type=DETECTION_TYPE_DEVICE_OFFLINE,
                detected_at=event.event_timestamp,
                store_id="fake-store",
                source_event_id=event.event_id,
                trace_id=str(event.trace.trace_id),
            )
            for _ in range(self.detections_per_observation)
        ]


@dataclasses.dataclass
class FakeEmissionDetector:
    """
    EmissionDetector that records observations and emits one detection
    per emission, propagating the emission's last_contributing_trace_id.

    The class-level ``aggregation_name`` defaults to "count" (matching
    the aggregator registered by ``make_pipeline``); override via
    subclass for tests that need a different routing key.
    """

    name: ClassVar[str] = "FakeEmissionDetector"
    aggregation_name: ClassVar[str] = "count"

    observations: list[WindowEmission] = dataclasses.field(default_factory=list)

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]:
        self.observations.append(emission)
        return [
            _build_detection(
                detection_type=DETECTION_TYPE_SIGNAL_ANOMALY,
                detected_at=emission.window_end,
                store_id=emission.partition_key,
                source_event_id=uuid4(),
                trace_id=emission.last_contributing_trace_id,
            )
        ]


@dataclasses.dataclass
class RaisingEventDetector:
    """
    EventDetector that always raises. For testing per-detector
    failure isolation.
    """

    name: ClassVar[str] = "RaisingEventDetector"

    def observe_event(self, event: TelemetryEvent) -> list[DetectionEvent]:
        raise RuntimeError("RaisingEventDetector deliberately raised")


@dataclasses.dataclass
class RaisingEmissionDetector:
    """
    EmissionDetector that always raises. For testing per-detector
    failure isolation.
    """

    name: ClassVar[str] = "RaisingEmissionDetector"
    aggregation_name: ClassVar[str] = "count"

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]:
        raise RuntimeError("RaisingEmissionDetector deliberately raised")
