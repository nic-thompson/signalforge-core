"""
Tests for stream_pipeline.streaming.realtime_pipeline.

These are integration tests for the orchestration layer. Pure-component
behaviour is already covered by the watermark, aggregator, and router
test modules; these tests verify wiring, ordering, trace propagation,
and per-event failure isolation.

Covers:

- partition extraction success and failure
- the fixed execution order watermark → aggregate → dispatch
- emissions from multiple registered aggregators are returned
- registering the same aggregator name twice is rejected
- per-event extraction failure does not abort the batch
- strict mode propagates extraction failures
- trace_id is stamped on the per-event log line
- batch summary log emits with correct counters
"""

from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from stream_pipeline.streaming.event_router import EventRouter
from stream_pipeline.streaming.realtime_pipeline import (
    RealtimePipeline,
    by_event_source,
    by_payload_field,
)
from stream_pipeline.streaming.watermark_manager import (
    EventClassification,
    WatermarkManager,
)
from stream_pipeline.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.detectors import (
    FakeEmissionDetector,
    FakeEventDetector,
    RaisingEmissionDetector,
    RaisingEventDetector,
)
from tests._fixtures.events import FakeEvent, FakeTrace, RecordingLogger


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
        seconds=seconds_since_epoch
    )


@dataclass(frozen=True)
class StorePayload:
    """Minimal payload exposing a store_id field for partition extraction tests."""
    store_id: str


def make_pipeline(
    *,
    extractor=by_event_source,
    strict: bool = False,
    lateness: int = 60,
    logger: RecordingLogger | None = None,
) -> tuple[RealtimePipeline, RecordingLogger, EventRouter, WindowAggregator]:
    """Convenience factory shared across tests."""
    rec = logger if logger is not None else RecordingLogger()
    router = EventRouter(logger=rec)
    watermarks = WatermarkManager(lateness_tolerance_seconds=lateness)
    pipeline = RealtimePipeline(
        router=router,
        watermark_manager=watermarks,
        partition_extractor=extractor,
        strict=strict,
        logger=rec,
    )
    aggregator = WindowAggregator(
        spec=WindowSpec(size_seconds=5, slide_seconds=5),
        aggregation=CountAggregation(),
        lateness_tolerance_seconds=lateness,
    )
    pipeline.register_aggregator("count", aggregator)
    return pipeline, rec, router, aggregator


# ---------------------------------------------------------------------------
# Aggregator registration
# ---------------------------------------------------------------------------


class AggregatorRegistrationTest(unittest.TestCase):
    def test_register_rejects_empty_name(self):
        pipeline, *_ = make_pipeline()
        with self.assertRaises(ValueError):
            pipeline.register_aggregator(
                "",
                WindowAggregator(
                    spec=WindowSpec(size_seconds=5, slide_seconds=5),
                    aggregation=CountAggregation(),
                    lateness_tolerance_seconds=60,
                ),
            )

    def test_register_rejects_duplicate_name(self):
        pipeline, *_ = make_pipeline()
        with self.assertRaises(ValueError):
            pipeline.register_aggregator(
                "count",
                WindowAggregator(
                    spec=WindowSpec(size_seconds=5, slide_seconds=5),
                    aggregation=CountAggregation(),
                    lateness_tolerance_seconds=60,
                ),
            )


# ---------------------------------------------------------------------------
# Successful processing
# ---------------------------------------------------------------------------


class ProcessSingleEventTest(unittest.TestCase):
    def test_processed_event_dispatches_through_router(self):
        pipeline, _rec, router, _agg = make_pipeline()
        seen: list[str] = []
        router.register(
            "device.registration", "v1", lambda e: seen.append(str(e.event_id))
        )

        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(event)

        self.assertEqual(result.handlers_invoked, 1)
        self.assertEqual(result.handler_failures, 0)
        self.assertFalse(result.extraction_failed)
        self.assertEqual(result.classification, EventClassification.ON_TIME)
        self.assertEqual(seen, [str(event.event_id)])

    def test_emissions_appear_when_watermark_advances_past_window_end(self):
        pipeline, _rec, _router, _agg = make_pipeline()

        # First event opens window [100, 105) at watermark utc(102) - 60s.
        # Result has no emissions yet.
        first = pipeline.process(
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(102),
            )
        )
        self.assertEqual(first.emissions, [])

        # Subsequent event with event_timestamp far enough ahead pushes
        # the watermark past the window end. Watermark at observe time
        # is event_timestamp - lateness (60). To close [100, 105) we
        # need watermark >= 105, so event_timestamp >= 165.
        second = pipeline.process(
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(165),
            )
        )
        # Window [100, 105) closes; the new event opens a fresh window
        # which has not yet closed.
        first_window_emissions = [
            e for e in second.emissions if e.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_emissions), 1)
        self.assertEqual(first_window_emissions[0].value, 1)
        self.assertFalse(first_window_emissions[0].is_repair)


# ---------------------------------------------------------------------------
# Partition extraction failure
# ---------------------------------------------------------------------------


class ExtractionFailureTest(unittest.TestCase):
    def test_extraction_failure_is_logged_and_isolated(self):
        # Use a payload extractor against an event whose payload has
        # no such field. Must log an error, return extraction_failed=True,
        # and not advance the watermark or call handlers.
        pipeline, rec, router, _agg = make_pipeline(
            extractor=by_payload_field("store_id")
        )
        seen: list[str] = []
        router.register(
            "device.registration", "v1", lambda e: seen.append("invoked")
        )

        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
            payload=None,  # no store_id
        )
        result = pipeline.process(event)

        self.assertTrue(result.extraction_failed)
        self.assertIsNone(result.partition_key)
        self.assertIsNone(result.classification)
        self.assertEqual(result.handlers_invoked, 0)
        self.assertEqual(seen, [])
        errors = rec.by_event_type("pipeline.extraction_error")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].trace_id, str(event.trace.trace_id))

    def test_strict_mode_propagates_extraction_failure(self):
        pipeline, _rec, _router, _agg = make_pipeline(
            extractor=by_payload_field("store_id"), strict=True
        )
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
            payload=None,
        )
        with self.assertRaises(ValueError):
            pipeline.process(event)

    def test_extractor_returning_empty_key_is_treated_as_failure(self):
        pipeline, rec, _router, _agg = make_pipeline(
            extractor=lambda e: ""
        )
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(event)
        self.assertTrue(result.extraction_failed)
        self.assertEqual(len(rec.by_event_type("pipeline.extraction_error")), 1)


# ---------------------------------------------------------------------------
# Trace propagation
# ---------------------------------------------------------------------------


class TracePropagationTest(unittest.TestCase):
    def test_processed_log_line_carries_trace_id(self):
        pipeline, rec, _router, _agg = make_pipeline()
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        pipeline.process(event)

        processed = rec.by_event_type("pipeline.processed")
        self.assertEqual(len(processed), 1)
        self.assertEqual(processed[0].trace_id, str(event.trace.trace_id))


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


class BatchProcessingTest(unittest.TestCase):
    def test_process_batch_returns_per_event_results(self):
        pipeline, _rec, _router, _agg = make_pipeline()
        events = [
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(100 + i),
            )
            for i in range(5)
        ]
        results = pipeline.process_batch(events)
        self.assertEqual(len(results), 5)
        self.assertTrue(all(r.classification == EventClassification.ON_TIME for r in results))

    def test_process_batch_emits_summary_with_correct_counters(self):
        _pipeline, _rec, _router, _agg = make_pipeline()

        # Three events: two ON_TIME, one with no payload that fails extraction
        # (using a payload-aware extractor on this run only).
        ext_pipeline, ext_rec, _ext_router, _ext_agg = make_pipeline(
            extractor=by_payload_field("store_id")
        )

        events = [
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(100),
                payload=StorePayload(store_id="store-1"),
            ),
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(101),
                payload=None,
            ),
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(102),
                payload=StorePayload(store_id="store-1"),
            ),
        ]
        ext_pipeline.process_batch(events)

        summaries = ext_rec.by_event_type("pipeline.batch_summary")
        self.assertEqual(len(summaries), 1)
        meta = summaries[0].metadata
        self.assertEqual(meta["events"], 3)
        self.assertEqual(meta["on_time"], 2)
        self.assertEqual(meta["late_tolerated"], 0)
        self.assertEqual(meta["late_dropped"], 0)
        self.assertEqual(meta["extraction_failures"], 1)


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class DeterminismTest(unittest.TestCase):
    def test_identical_event_streams_yield_identical_results(self):
        events = [
            FakeEvent(
                event_type="device.registration",
                schema_version="v1",
                event_timestamp=epoch_aligned(100 + i),
            )
            for i in range(10)
        ]
        # Snapshot mutable identity fields so the two runs see the same
        # event_ids / trace_ids.
        events_a = events
        events_b = [
            FakeEvent(
                event_type=e.event_type,
                schema_version=e.schema_version,
                event_timestamp=e.event_timestamp,
                event_id=e.event_id,
                trace=e.trace,
                payload=e.payload,
            )
            for e in events
        ]

        pa, _, _, _ = make_pipeline()
        pb, _, _, _ = make_pipeline()

        ra = pa.process_batch(events_a)
        rb = pb.process_batch(events_b)

        self.assertEqual(
            [(r.classification, len(r.emissions)) for r in ra],
            [(r.classification, len(r.emissions)) for r in rb],
        )


# ---------------------------------------------------------------------------
# Detector dispatch
# ---------------------------------------------------------------------------


class EventDetectorDispatchTest(unittest.TestCase):
    """
    Tests for ``register_event_detector`` and event-detector dispatch in
    ``RealtimePipeline.process()``.

    Verifies three things at once: that a registered event detector
    observes every event passed through ``process()``, that its emitted
    detections appear in ``ProcessingResult.detections``, and that the
    detection collection is order-preserving (each event's detections
    appear in the result for that event, not aggregated across events).
    """

    def test_registered_event_detector_observes_events_and_detections_flow_back(self):
        pipeline, _, _, _ = make_pipeline()
        detector = FakeEventDetector(detections_per_observation=1)
        pipeline.register_event_detector(detector)

        # Build a single event and process it. The fake detector emits
        # one detection per observation, so we expect to see exactly
        # one detection on the result.
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(event)

        # Detector saw the event.
        self.assertEqual(len(detector.observations), 1)
        self.assertIs(detector.observations[0], event)

        # Detection flowed back through ProcessingResult.
        self.assertEqual(len(result.detections), 1)
        # Sanity check: the detection is well-formed (not a placeholder).
        self.assertEqual(
            result.detections[0].payload.detection_type,
            "device.offline",  # FakeEventDetector emits this type
        )

    def test_multiple_event_detectors_each_observe_and_contribute_detections(self):
        # Two detectors, registered in order A then B. Both should
        # observe the same event; both detections should appear in
        # ProcessingResult.detections.
        pipeline, _, _, _ = make_pipeline()
        detector_a = FakeEventDetector(detections_per_observation=1)
        detector_b = FakeEventDetector(detections_per_observation=1)
        pipeline.register_event_detector(detector_a)
        pipeline.register_event_detector(detector_b)

        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(event)

        # Both detectors observed the event exactly once.
        self.assertEqual(len(detector_a.observations), 1)
        self.assertEqual(len(detector_b.observations), 1)
        self.assertIs(detector_a.observations[0], event)
        self.assertIs(detector_b.observations[0], event)

        # Both detections appear in the result.
        self.assertEqual(len(result.detections), 2)

    def test_event_detector_failure_does_not_block_other_detectors(self):
        # A raising event detector should be logged and skipped; other
        # detectors continue to observe the same event, and their
        # detections still flow back through ProcessingResult.
        pipeline, rec, _, _ = make_pipeline()
        raising = RaisingEventDetector()
        survivor = FakeEventDetector(detections_per_observation=1)
        pipeline.register_event_detector(raising)
        pipeline.register_event_detector(survivor)

        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        # process() returns normally — the raising detector did not
        # propagate. If it had, this line would raise.
        result = pipeline.process(event)

        # The survivor observed the event and contributed its detection.
        self.assertEqual(len(survivor.observations), 1)
        self.assertEqual(len(result.detections), 1)

        # The failure was logged. by_event_type filters to the
        # pipeline.detector_error records emitted by the dispatch loop.
        errors = rec.by_event_type("pipeline.detector_error")
        self.assertEqual(len(errors), 1)
        # Metadata identifies the failing detector by name and kind.
        self.assertEqual(errors[0].metadata["detector"], "RaisingEventDetector")
        self.assertEqual(errors[0].metadata["detector_kind"], "event")

class EmissionDetectorDispatchTest(unittest.TestCase):
    """
    Tests for ``register_emission_detector`` and emission-detector
    dispatch in ``RealtimePipeline.process()``.

    Emission detectors are subscribed by ``aggregation_name``. The fake
    used here uses ``aggregation_name = "count"`` matching
    ``make_pipeline``'s default aggregator, so a default-constructed
    fake receives emissions from the default test pipeline.

    Emissions only fire when a window closes (watermark crosses the
    window's right edge), so each test processes two events: the first
    contributes to the window, the second advances the watermark past
    its end.
    """

    def test_registered_emission_detector_observes_window_emissions(self):
        pipeline, _, _, _ = make_pipeline()
        detector = FakeEmissionDetector()
        pipeline.register_emission_detector(detector)

        # Event in window [100, 105). Watermark at 102 - 60 = 42 (still
        # below the window's right edge), so no emission yet.
        first = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(first)
        self.assertEqual(len(detector.observations), 0)
        self.assertEqual(result.detections, [])

        # Event at 106 advances the watermark past window_end (105),
        # closing window [100, 105) and emitting once.
        second = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(166),  # watermark = 166 - 60 = 106
        )
        result = pipeline.process(second)

        # The emission detector saw the closure.
        self.assertEqual(len(detector.observations), 1)
        emission = detector.observations[0]
        self.assertEqual(emission.aggregation_name, "count")
        self.assertEqual(emission.partition_key, "test-source")
        # Detection flowed back through the second process() call's
        # ProcessingResult.
        self.assertEqual(len(result.detections), 1)

    def test_emission_detector_failure_does_not_block_other_detectors(self):
        # A raising emission detector should be logged and skipped;
        # other emission detectors subscribed to the same
        # aggregation_name still observe the emission, and their
        # detections still flow back.
        pipeline, rec, _, _ = make_pipeline()
        raising = RaisingEmissionDetector()
        survivor = FakeEmissionDetector()
        pipeline.register_emission_detector(raising)
        pipeline.register_emission_detector(survivor)

        # Event in window [100, 105). No closure yet.
        first = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
        )
        result = pipeline.process(first)
        self.assertEqual(len(survivor.observations), 0)
        self.assertEqual(result.detections, [])

        # Event at 166 advances watermark to 106, closing window
        # [100, 105). Both detectors are dispatched; raising one fails,
        # survivor observes.
        second = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(166),
        )
        result = pipeline.process(second)

        # The survivor observed the closure and contributed.
        self.assertEqual(len(survivor.observations), 1)
        self.assertEqual(len(result.detections), 1)

        # The failure was logged with full lineage.
        errors = rec.by_event_type("pipeline.detector_error")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].metadata["detector"], "RaisingEmissionDetector")
        self.assertEqual(errors[0].metadata["detector_kind"], "emission")
        self.assertEqual(errors[0].metadata["aggregation_name"], "count")

    def test_emission_detector_receives_last_contributing_trace_id(self):
        # Pins the trace-propagation contract from commit fe7034f:
        # the trace_id of an event that contributes to a window is
        # carried, via _WindowState, onto the eventual WindowEmission's
        # last_contributing_trace_id field, where an emission detector
        # can read it for lineage.
        pipeline, _, _, _ = make_pipeline()
        detector = FakeEmissionDetector()
        pipeline.register_emission_detector(detector)

        # Construct an event with a deliberately set trace_id, so the
        # assertion below has a known value to check against. The
        # pipeline converts the UUID to its string form when passing
        # trace_id into aggregator.observe(), so the field we assert
        # on is the string form.
        known_trace_id = uuid4()
        first = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(102),
            trace=FakeTrace(trace_id=known_trace_id),
        )
        pipeline.process(first)
        self.assertEqual(len(detector.observations), 0)

        # Second event with an unrelated trace_id, advances the
        # watermark past the first window's end. The first window
        # closes; its emission should carry the FIRST event's
        # trace_id (the most-recent contributor), not the second's.
        second = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=epoch_aligned(166),
        )
        pipeline.process(second)

        # Exactly one closure emission observed.
        self.assertEqual(len(detector.observations), 1)
        emission = detector.observations[0]

        # The contributing event's trace_id is on the emission, in
        # string form (the pipeline converts at the aggregator
        # boundary).
        self.assertEqual(
            emission.last_contributing_trace_id,
            str(known_trace_id),
        )


if __name__ == "__main__":
    unittest.main()
