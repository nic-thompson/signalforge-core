"""
Tests for alert-router integration in signal_forge.streaming.realtime_pipeline.

The pipeline wires an optional, single AlertRouter that consumes the
detections a process() call produces and emits alerts onto
ProcessingResult.alerts. These tests verify the integration contract:

- register_alert_router accepts one router; a second raises
- a pipeline with no router registered processes normally (alerts == [])
- the router turns each produced detection into an alert on the result
- multiple / zero detections map to the same alert count
- the extraction-failure path produces no alerts
- a raising router is logged with lineage and isolated (default mode)
- strict mode propagates a router exception

Detections are fed by a FakeEventDetector so the tests control how many
flow into the router, isolating "does the wiring carry detections
through to alerts" from "do detectors fire correctly" (tested
elsewhere). Acknowledgement-annotation logic is covered by the router's
own unit tests in tests/alerts/test_alert_routing.py; these tests cover
only the pipeline wiring.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from event_schema_contracts.base.identity import derive

from signal_forge.alerts.acknowledgement_registry import AcknowledgementRegistry
from signal_forge.alerts.alert_router import AlertRouter
from signal_forge.streaming.event_router import EventRouter
from signal_forge.streaming.realtime_pipeline import (
    RealtimePipeline,
    by_event_source,
)
from signal_forge.streaming.watermark_manager import WatermarkManager
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.detectors import FakeEventDetector
from tests._fixtures.events import FakeEvent, RecordingLogger

_LOG_ALERT_ROUTER_ERROR = "alerts.router_error"


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds_since_epoch)


def make_pipeline(
    *,
    strict: bool = False,
    lateness: int = 60,
) -> tuple[RealtimePipeline, RecordingLogger]:
    rec = RecordingLogger()
    pipeline = RealtimePipeline(
        router=EventRouter(logger=rec),
        watermark_manager=WatermarkManager(lateness_tolerance_seconds=lateness),
        partition_extractor=by_event_source,
        strict=strict,
        logger=rec,
    )
    pipeline.register_aggregator(
        "count",
        WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=lateness,
        ),
    )
    return pipeline, rec


def make_event(seconds: int = 102) -> FakeEvent:
    return FakeEvent(
        event_type="device.registration",
        schema_version="v1",
        event_timestamp=epoch_aligned(seconds),
    )


class _RaisingAlertRouter:
    """Structural stand-in that always raises on route()."""

    def __init__(self) -> None:
        self.calls = 0

    def route(self, detections):  # type: ignore[no-untyped-def]
        self.calls += 1
        raise RuntimeError("router boom")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class RegistrationTest(unittest.TestCase):
    def test_second_registration_raises(self) -> None:
        pipeline, _rec = make_pipeline()
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))
        with self.assertRaises(ValueError):
            pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

    def test_no_router_yields_empty_alerts(self) -> None:
        pipeline, _rec = make_pipeline()
        result = pipeline.process(make_event())
        self.assertEqual(result.alerts, [])


# ---------------------------------------------------------------------------
# Success-path dispatch
# ---------------------------------------------------------------------------


class DispatchTest(unittest.TestCase):
    def test_each_detection_becomes_an_alert(self) -> None:
        pipeline, _rec = make_pipeline()
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=1)
        )
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        result = pipeline.process(make_event())

        self.assertEqual(len(result.detections), 1)
        self.assertEqual(len(result.alerts), 1)
        alert = result.alerts[0]
        detection = result.detections[0]
        self.assertEqual(
            alert.payload.alert_id,
            derive("alert", str(detection.payload.detection_id)),
        )
        self.assertEqual(alert.payload.detection_id, detection.payload.detection_id)

    def test_multiple_detections_become_multiple_alerts(self) -> None:
        pipeline, _rec = make_pipeline()
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=3)
        )
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        result = pipeline.process(make_event())

        self.assertEqual(len(result.detections), 3)
        self.assertEqual(len(result.alerts), 3)

    def test_no_detections_yields_no_alerts(self) -> None:
        pipeline, _rec = make_pipeline()
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=0)
        )
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        result = pipeline.process(make_event())

        self.assertEqual(result.detections, [])
        self.assertEqual(result.alerts, [])

    def test_alerts_unacknowledged_by_default(self) -> None:
        # With an empty registry, every routed alert is annotated
        # acknowledged=False. (Annotation logic itself is unit-tested;
        # this confirms the wired router carries the annotation through.)
        pipeline, _rec = make_pipeline()
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=1)
        )
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        result = pipeline.process(make_event())

        self.assertFalse(result.alerts[0].payload.details["acknowledged"])


# ---------------------------------------------------------------------------
# Extraction-failure path
# ---------------------------------------------------------------------------


class ExtractionFailureTest(unittest.TestCase):
    def test_extraction_failure_yields_no_alerts(self) -> None:
        # An empty source makes by_event_source produce an empty
        # partition key, which the pipeline treats as extraction
        # failure. The failure path must still carry alerts == [].
        pipeline, _rec = make_pipeline()
        pipeline.register_alert_router(AlertRouter(AcknowledgementRegistry()))

        bad = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
            source="",
        )
        result = pipeline.process(bad)

        self.assertTrue(result.extraction_failed)
        self.assertEqual(result.alerts, [])


# ---------------------------------------------------------------------------
# Failure isolation
# ---------------------------------------------------------------------------


class FailureIsolationTest(unittest.TestCase):
    def test_raising_router_is_isolated_in_default_mode(self) -> None:
        pipeline, rec = make_pipeline(strict=False)
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=1)
        )
        pipeline.register_alert_router(_RaisingAlertRouter())

        # The router raises, but process() completes; the result carries
        # the detections and empty alerts, and the failure is logged.
        result = pipeline.process(make_event())

        self.assertEqual(len(result.detections), 1)
        self.assertEqual(result.alerts, [])
        self.assertEqual(
            len(rec.by_event_type(_LOG_ALERT_ROUTER_ERROR)), 1
        )

    def test_strict_mode_propagates_router_error(self) -> None:
        pipeline, _rec = make_pipeline(strict=True)
        pipeline.register_event_detector(
            FakeEventDetector(detections_per_observation=1)
        )
        pipeline.register_alert_router(_RaisingAlertRouter())

        with self.assertRaises(RuntimeError):
            pipeline.process(make_event())


if __name__ == "__main__":
    unittest.main()
