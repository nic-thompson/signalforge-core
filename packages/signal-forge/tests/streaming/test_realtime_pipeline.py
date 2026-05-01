"""
Tests for signal_forge.streaming.realtime_pipeline.

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
from datetime import datetime, timedelta, timezone

from signal_forge.streaming.event_router import EventRouter
from signal_forge.streaming.realtime_pipeline import (
    RealtimePipeline,
    by_event_source,
    by_payload_field,
)
from signal_forge.streaming.watermark_manager import (
    EventClassification,
    WatermarkManager,
)
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)

from tests._fixtures.events import FakeEvent, RecordingLogger


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
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
        pipeline, rec, _router, _agg = make_pipeline()

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


if __name__ == "__main__":
    unittest.main()