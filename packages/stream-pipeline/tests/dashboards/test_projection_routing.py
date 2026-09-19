"""
Integration tests for stream_pipeline.dashboards.routing.route_detections.

Two anchors prove the projections consume real pipeline output:

- a genuinely end-to-end test stands up a real RealtimePipeline with a real
  OfflineDetector, feeds events that drive a real device.offline then
  device.online, routes the results into a real OfflineCountProjection, and
  asserts the gauge tracks the detector's output;
- a fan-out test routes one ProcessingResult carrying one detection of each
  type into all three projections at once and asserts each picked out its
  own type.

The remaining tests pin the bulkheading contract: a raising projection is
isolated and logged, strict mode re-raises, an empty result is a no-op, and
a generator of projections is not exhausted after the first detection.
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from stream_pipeline.dashboards.active_outage_projection import ActiveOutageProjection
from stream_pipeline.dashboards.anomaly_rate_projection import AnomalyRateProjection
from stream_pipeline.dashboards.offline_count_projection import OfflineCountProjection
from stream_pipeline.dashboards.projection_store import InMemoryProjectionStore
from stream_pipeline.dashboards.routing import route_detections
from stream_pipeline.detection.detectors import OfflineDetector
from stream_pipeline.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_SIGNAL_ANOMALY,
    DETECTION_TYPE_STORE_OUTAGE,
)
from stream_pipeline.streaming.event_router import EventRouter
from stream_pipeline.streaming.realtime_pipeline import (
    ProcessingResult,
    RealtimePipeline,
    by_event_source,
)
from stream_pipeline.streaming.watermark_manager import (
    EventClassification,
    WatermarkManager,
)
from tests._fixtures.events import FakeEvent, RecordingLogger

_AT = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds_since_epoch)


@dataclass(frozen=True)
class DevicePayload:
    """Payload carrying a device_id for the OfflineDetector's extractor."""

    device_id: UUID


@dataclass
class _RaisingProjection:
    """A projection that always raises, for the bulkhead tests."""

    def observe(self, detection: DetectionEvent) -> None:
        raise RuntimeError("projection deliberately raised")


def _make_pipeline() -> tuple[RealtimePipeline, EventRouter]:
    router = EventRouter()
    watermarks = WatermarkManager(lateness_tolerance_seconds=60)
    pipeline = RealtimePipeline(
        router=router,
        watermark_manager=watermarks,
        partition_extractor=by_event_source,
    )
    return pipeline, router


def _detection(
    detection_type: str,
    store_id: str,
    *,
    device_id: UUID | None = None,
    details: dict | None = None,
    detected_at: datetime = _AT,
    severity: DetectionSeverity = DetectionSeverity.WARNING,
) -> DetectionEvent:
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type=detection_type,
        severity=severity,
        detected_at=detected_at,
        store_id=store_id,
        device_id=device_id,
        source_event_id=uuid4(),
        threshold_breached="test",
        details=details or {},
    )
    return DetectionEvent(
        event_id=uuid4(),
        event_timestamp=detected_at,
        trace=TraceContext(trace_id=uuid4()),
        payload=payload,
    )


def _result(
    detections: list[DetectionEvent], *, extraction_failed: bool = False
) -> ProcessingResult:
    return ProcessingResult(
        event_id=str(uuid4()),
        partition_key=None if extraction_failed else "store-1",
        classification=None if extraction_failed else EventClassification.ON_TIME,
        handlers_invoked=0,
        handler_failures=0,
        emissions=[],
        detections=list(detections),
        alerts=[],
        extraction_failed=extraction_failed,
        features=[],
    )


class EndToEndRoutingTest(unittest.TestCase):
    def test_real_pipeline_offline_detector_drives_offline_count_projection(self):
        # A real OfflineDetector wired into a real pipeline. Device A goes
        # silent past the 300s threshold (discovered when B's later event
        # arrives), then reports again and recovers. Routing the real
        # device.offline / device.online detections must move the gauge
        # 0 -> 1 -> 0.
        dev_a, dev_b = uuid4(), uuid4()
        stores = {dev_a: "store-1", dev_b: "store-1"}
        detector = OfflineDetector(
            threshold_seconds=300,
            device_id_extractor=lambda e: e.payload.device_id,
            store_lookup=stores.get,
        )
        pipeline, _router = _make_pipeline()
        pipeline.register_event_detector(detector)

        projection = OfflineCountProjection(InMemoryProjectionStore())

        events = [
            FakeEvent(
                event_type="telemetry.heartbeat",
                schema_version="v1",
                event_timestamp=epoch_aligned(0),
                payload=DevicePayload(dev_a),
            ),
            FakeEvent(
                event_type="telemetry.heartbeat",
                schema_version="v1",
                event_timestamp=epoch_aligned(400),  # discovers A silent 400s > 300
                payload=DevicePayload(dev_b),
            ),
            FakeEvent(
                event_type="telemetry.heartbeat",
                schema_version="v1",
                event_timestamp=epoch_aligned(500),  # A reports again -> recovery
                payload=DevicePayload(dev_a),
            ),
        ]

        counts = []
        for event in events:
            result = pipeline.process(event)
            route_detections(result, [projection])
            counts.append(projection.offline_count("store-1"))

        self.assertEqual(counts, [0, 1, 0])

    def test_one_result_fans_out_to_all_three_projections(self):
        # A single result carrying one detection of each type. Each
        # projection picks out its own type and ignores the others. The
        # three projections share one store and namespace by view.
        store = InMemoryProjectionStore()
        offline = OfflineCountProjection(store)
        outage = ActiveOutageProjection(store)
        rate = AnomalyRateProjection(store)

        detections = [
            _detection(
                DETECTION_TYPE_DEVICE_OFFLINE, "store-1", device_id=uuid4()
            ),
            _detection(
                DETECTION_TYPE_STORE_OUTAGE, "store-2",
                severity=DetectionSeverity.CRITICAL,
            ),
            _detection(
                DETECTION_TYPE_SIGNAL_ANOMALY, "store-3",
                details={"signal_name": "latency"},
            ),
        ]
        route_detections(_result(detections), [offline, outage, rate])

        # Each projection reacted only to its own type.
        self.assertEqual(offline.offline_count("store-1"), 1)
        self.assertEqual(offline.stores_with_offline(), ["store-1"])
        self.assertEqual(outage.active_outage_count(), 1)
        self.assertTrue(outage.is_in_outage("store-2"))
        self.assertEqual(
            rate.count_in_window(
                "latency", as_of=_AT + timedelta(seconds=60)
            ),
            1,
        )


class BulkheadTest(unittest.TestCase):
    def test_raising_projection_is_isolated_and_logged(self):
        store = InMemoryProjectionStore()
        good = OfflineCountProjection(store)
        bad = _RaisingProjection()
        rec = RecordingLogger()

        detection = _detection(
            DETECTION_TYPE_DEVICE_OFFLINE, "store-1", device_id=uuid4()
        )
        # bad is offered the detection first; it raises and is skipped,
        # good still updates.
        route_detections(_result([detection]), [bad, good], logger=rec)

        self.assertEqual(good.offline_count("store-1"), 1)
        errors = rec.by_event_type("dashboards.projection_error")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].metadata["projection"], "_RaisingProjection")
        self.assertEqual(errors[0].metadata["detection_type"], "device.offline")

    def test_strict_mode_propagates_projection_failure(self):
        detection = _detection(
            DETECTION_TYPE_DEVICE_OFFLINE, "store-1", device_id=uuid4()
        )
        with self.assertRaises(RuntimeError):
            route_detections(
                _result([detection]), [_RaisingProjection()], strict=True
            )

    def test_empty_result_is_a_noop(self):
        store = InMemoryProjectionStore()
        offline = OfflineCountProjection(store)
        rec = RecordingLogger()

        # Mirrors the extraction-failure result, which carries no detections.
        route_detections(
            _result([], extraction_failed=True), [offline], logger=rec
        )
        self.assertEqual(offline.stores_with_offline(), [])
        self.assertEqual(rec.by_event_type("dashboards.projection_error"), [])

    def test_generator_of_projections_sees_every_detection(self):
        # If route_detections did not materialise the projections iterable,
        # the generator would be exhausted after the first detection and the
        # second would reach no projection. Two offline detections for
        # distinct devices must both land -> count 2.
        store = InMemoryProjectionStore()
        offline = OfflineCountProjection(store)
        detections = [
            _detection(DETECTION_TYPE_DEVICE_OFFLINE, "store-1", device_id=uuid4()),
            _detection(DETECTION_TYPE_DEVICE_OFFLINE, "store-1", device_id=uuid4()),
        ]
        route_detections(_result(detections), (p for p in [offline]))
        self.assertEqual(offline.offline_count("store-1"), 2)


if __name__ == "__main__":
    unittest.main()
