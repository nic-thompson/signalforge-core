"""
Structural conformance tests for the detector protocols.

These tests do not exercise behaviour — there is no behaviour to
exercise on a Protocol. They confirm that:

1. Concrete classes that implement every protocol member can be
   constructed and used where the protocol type is expected.

2. The protocols carry the attributes we expect at the class level
   (``name`` for both, plus ``aggregation_name`` for ``EmissionDetector``).

3. The protocols' methods accept and return the intended types,
   confirmed by constructing real ``DetectionEvent`` instances from
   a fake detector.

The static type-checking at the pipeline's ``register_*`` call sites
is the real safety net for protocol conformance; these tests catch
the most common runtime accident — forgetting to declare a class
variable — before it surfaces as a registration-time error.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from typing import ClassVar
from uuid import uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from stream_pipeline.detection.protocols import EmissionDetector, EventDetector
from stream_pipeline.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_SIGNAL_ANOMALY,
)
from stream_pipeline.streaming.event_protocol import TelemetryEvent
from stream_pipeline.streaming.window_aggregator import WindowEmission
from tests._fixtures.events import FakeEvent

# ---------------------------------------------------------------------------
# Fake detectors used purely to exercise protocol conformance
# ---------------------------------------------------------------------------


class _FakeEventDetector:
    """A minimal EventDetector implementation for protocol conformance tests."""

    name: ClassVar[str] = "FakeEventDetector"

    def observe_event(self, event: TelemetryEvent) -> list[DetectionEvent]:
        # Always emit one detection per observation, just to confirm the
        # return-type contract is respected.
        payload = DetectionEventPayload(
            detection_id=uuid4(),
            detection_type=DETECTION_TYPE_DEVICE_OFFLINE,
            severity=DetectionSeverity.INFO,
            detected_at=event.event_timestamp,
            store_id="fake-store",
            source_event_id=event.event_id,
            threshold_breached="fake threshold",
        )
        return [
            DetectionEvent(
                event_timestamp=event.event_timestamp,
                trace=TraceContext(trace_id=uuid4()),
                payload=payload,
            )
        ]


class _FakeEmissionDetector:
    """A minimal EmissionDetector implementation for protocol conformance tests."""

    name: ClassVar[str] = "FakeEmissionDetector"
    aggregation_name: ClassVar[str] = "fake_aggregation"

    def observe_emission(self, emission: WindowEmission) -> list[DetectionEvent]:
        payload = DetectionEventPayload(
            detection_id=uuid4(),
            detection_type=DETECTION_TYPE_SIGNAL_ANOMALY,
            severity=DetectionSeverity.WARNING,
            detected_at=emission.window_end,
            store_id=emission.partition_key,
            source_event_id=uuid4(),
            threshold_breached="fake emission threshold",
        )
        return [
            DetectionEvent(
                event_timestamp=emission.window_end,
                trace=TraceContext(trace_id=uuid4()),
                payload=payload,
            )
        ]


# ---------------------------------------------------------------------------
# EventDetector conformance
# ---------------------------------------------------------------------------


class EventDetectorProtocolTest(unittest.TestCase):
    def test_fake_detector_has_required_class_attributes(self) -> None:
        # name must exist on the class itself (ClassVar), not just the instance.
        self.assertEqual(_FakeEventDetector.name, "FakeEventDetector")

    def test_fake_detector_returns_list_of_detection_events(self) -> None:
        detector = _FakeEventDetector()
        event = FakeEvent(
            event_type="device.heartbeat",
            schema_version="v1",
            event_timestamp=datetime(2026, 5, 3, 12, 0, 0, tzinfo=UTC),
        )

        detections = detector.observe_event(event)

        self.assertIsInstance(detections, list)
        self.assertEqual(len(detections), 1)
        self.assertIsInstance(detections[0], DetectionEvent)

    def test_protocol_can_be_used_as_type_hint(self) -> None:
        # If this function annotation worked at module load (above), the
        # protocol is usable as a type hint. Re-asserting at runtime catches
        # accidental name shadowing.
        def takes_event_detector(d: EventDetector) -> str:
            return d.name

        self.assertEqual(takes_event_detector(_FakeEventDetector()), "FakeEventDetector")


# ---------------------------------------------------------------------------
# EmissionDetector conformance
# ---------------------------------------------------------------------------


class EmissionDetectorProtocolTest(unittest.TestCase):
    def test_fake_detector_has_required_class_attributes(self) -> None:
        self.assertEqual(_FakeEmissionDetector.name, "FakeEmissionDetector")
        self.assertEqual(_FakeEmissionDetector.aggregation_name, "fake_aggregation")

    def test_fake_detector_returns_list_of_detection_events(self) -> None:
        detector = _FakeEmissionDetector()
        emission = WindowEmission(
            aggregation_name="fake_aggregation",
            partition_key="store-001",
            window_start=datetime(2026, 5, 3, 12, 0, 0, tzinfo=UTC),
            window_end=datetime(2026, 5, 3, 12, 0, 5, tzinfo=UTC),
            value=42,
            event_count=3,
            is_repair=False,
        )

        detections = detector.observe_emission(emission)

        self.assertIsInstance(detections, list)
        self.assertEqual(len(detections), 1)
        self.assertIsInstance(detections[0], DetectionEvent)

    def test_protocol_can_be_used_as_type_hint(self) -> None:
        def takes_emission_detector(d: EmissionDetector) -> tuple[str, str]:
            return d.name, d.aggregation_name

        result = takes_emission_detector(_FakeEmissionDetector())
        self.assertEqual(result, ("FakeEmissionDetector", "fake_aggregation"))


if __name__ == "__main__":
    unittest.main()
