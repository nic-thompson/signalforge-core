"""
Tests for dataset-writer integration in signal_forge.streaming.realtime_pipeline.

Commit 7 wires an optional, single dataset writer into the pipeline.
These tests verify the integration contract:

- register_dataset_writer accepts one writer; a second raises
- the writer receives every successful ProcessingResult
- the writer is handed the exact result object process() returns
- the extraction-failure path does not call the writer
- a raising writer is logged with lineage and isolated (default mode)
- strict mode propagates a writer exception
- a pipeline with no writer registered processes normally

The writer doubles are structural DatasetWriters — a write(result)
method is all the Protocol requires.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from signal_forge.streaming.event_router import EventRouter
from signal_forge.streaming.realtime_pipeline import (
    ProcessingResult,
    RealtimePipeline,
    by_event_source,
    by_payload_field,
)
from signal_forge.streaming.watermark_manager import WatermarkManager
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, RecordingLogger

_LOG_WRITER_ERROR = "pipeline.writer_error"


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds_since_epoch)


def make_pipeline(
    *,
    extractor=by_event_source,
    strict: bool = False,
    lateness: int = 60,
) -> tuple[RealtimePipeline, RecordingLogger]:
    rec = RecordingLogger()
    pipeline = RealtimePipeline(
        router=EventRouter(logger=rec),
        watermark_manager=WatermarkManager(lateness_tolerance_seconds=lateness),
        partition_extractor=extractor,
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


class RecordingDatasetWriter:
    """Structural DatasetWriter that records the results it is handed."""

    def __init__(self) -> None:
        self.received: list[ProcessingResult] = []

    def write(self, result: ProcessingResult) -> None:
        self.received.append(result)


class RaisingDatasetWriter:
    """Structural DatasetWriter that always raises."""

    def __init__(self) -> None:
        self.calls = 0

    def write(self, result: ProcessingResult) -> None:
        self.calls += 1
        raise RuntimeError("writer boom")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


class RegistrationTest(unittest.TestCase):
    def test_second_registration_raises(self) -> None:
        pipeline, _rec = make_pipeline()
        pipeline.register_dataset_writer(RecordingDatasetWriter())
        with self.assertRaises(ValueError):
            pipeline.register_dataset_writer(RecordingDatasetWriter())

    def test_no_writer_processes_normally(self) -> None:
        pipeline, _rec = make_pipeline()
        result = pipeline.process(make_event())
        self.assertFalse(result.extraction_failed)


# ---------------------------------------------------------------------------
# Dispatch on the success path
# ---------------------------------------------------------------------------


class SuccessPathDispatchTest(unittest.TestCase):
    def test_writer_receives_result_on_success(self) -> None:
        pipeline, _rec = make_pipeline()
        writer = RecordingDatasetWriter()
        pipeline.register_dataset_writer(writer)

        result = pipeline.process(make_event())

        self.assertEqual(len(writer.received), 1)
        # The writer is handed the exact object process() returns, not a
        # copy — confirms it is the success-path result.
        self.assertIs(writer.received[0], result)

    def test_writer_receives_every_successful_result(self) -> None:
        pipeline, _rec = make_pipeline()
        writer = RecordingDatasetWriter()
        pipeline.register_dataset_writer(writer)

        # Three events, the last far enough ahead to close the first
        # window. Every successful process() hands its result over,
        # regardless of whether that result carried emissions.
        pipeline.process(make_event(102))
        pipeline.process(make_event(103))
        closing = pipeline.process(make_event(165))

        self.assertEqual(len(writer.received), 3)
        self.assertIs(writer.received[-1], closing)
        # The closing result carried the [100, 105) emission.
        self.assertTrue(
            any(e.window_start == epoch_aligned(100) for e in closing.emissions)
        )

    def test_extraction_failure_does_not_call_writer(self) -> None:
        # by_payload_field against a payload with no such field fails
        # extraction; the writer must not see that result.
        pipeline, _rec = make_pipeline(extractor=by_payload_field("store_id"))
        writer = RecordingDatasetWriter()
        pipeline.register_dataset_writer(writer)

        result = pipeline.process(make_event())

        self.assertTrue(result.extraction_failed)
        self.assertEqual(writer.received, [])


# ---------------------------------------------------------------------------
# Failure isolation
# ---------------------------------------------------------------------------


class FailureIsolationTest(unittest.TestCase):
    def test_raising_writer_is_logged_and_isolated(self) -> None:
        pipeline, rec = make_pipeline()
        writer = RaisingDatasetWriter()
        pipeline.register_dataset_writer(writer)

        event = make_event()
        # The pipeline does not propagate the writer's exception in
        # default mode, and still returns a result.
        result = pipeline.process(event)

        self.assertEqual(writer.calls, 1)
        self.assertFalse(result.extraction_failed)

        errors = rec.by_event_type(_LOG_WRITER_ERROR)
        self.assertEqual(len(errors), 1)
        log = errors[0]
        self.assertEqual(log.trace_id, str(event.trace.trace_id))
        self.assertEqual(log.metadata["event_id"], str(event.event_id))
        self.assertEqual(log.metadata["partition_key"], event.source)
        self.assertEqual(log.metadata["exception_type"], "RuntimeError")

    def test_raising_writer_does_not_abort_subsequent_events(self) -> None:
        pipeline, rec = make_pipeline()
        pipeline.register_dataset_writer(RaisingDatasetWriter())

        pipeline.process(make_event(102))
        pipeline.process(make_event(103))

        self.assertEqual(len(rec.by_event_type(_LOG_WRITER_ERROR)), 2)

    def test_strict_mode_propagates_writer_error(self) -> None:
        pipeline, _rec = make_pipeline(strict=True)
        pipeline.register_dataset_writer(RaisingDatasetWriter())
        with self.assertRaises(RuntimeError):
            pipeline.process(make_event())


if __name__ == "__main__":
    unittest.main()
