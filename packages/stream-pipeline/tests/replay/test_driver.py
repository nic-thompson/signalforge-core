"""
Tests for the Phase 7 replay driver.

The driver is small — build from ``for_replay()``, run the source, log, return
— so these tests pin the four properties that carry its contract:

1. the builder receives *replay-swapped* settings, never the live settings the
   caller passed (the isolation guarantee — D-20);
2. the injected event source is fed through ``process_batch`` faithfully, in
   order, and the results are returned unchanged;
3. one ``replay.completed`` lineage line is emitted, carrying the replay
   environment tag and the counts;
4. a single-use generator source is consumed correctly (the driver
   materialises it once, so neither the count nor the run sees an exhausted
   iterator).

The pipeline is faked: the driver's job is orchestration, not processing, so
these tests assert the driver's wiring, not the pipeline's behaviour. The
whole-control-plane determinism proof — that a real live run and a real replay
run produce byte-identical output — is the separate integration test in
``test_replay_determinism.py`` (commit 4).
"""

from __future__ import annotations

import unittest
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from stream_pipeline.replay.driver import run_replay

# --------------------------------------------------------------------------
# Test doubles
# --------------------------------------------------------------------------
# Minimal stand-ins. The driver only ever calls ``settings.for_replay()``,
# ``build_pipeline(settings)``, ``pipeline.process_batch(events)``, and
# ``logger.info(...)`` — so the doubles implement exactly that surface and
# nothing more. Using real ``PlatformSettings`` / ``RealtimePipeline`` would
# drag in the upstream contracts and AWS deps for no added coverage of the
# driver's own logic.


@dataclass
class FakeSettings:
    """Stands in for PlatformSettings; records whether for_replay ran."""

    environment: str = "live"

    def for_replay(self, replay_environment: str = "replay") -> FakeSettings:
        return replace(self, environment=replay_environment)


@dataclass
class FakePipeline:
    """Records the events it was asked to process; returns one result each."""

    built_with_environment: str
    seen: list[Any] = field(default_factory=list)

    def process_batch(self, events: Iterable[Any]) -> list[Any]:
        materialised = list(events)
        self.seen = materialised
        # One result per event, each exposing the .detections / .emissions
        # lists the driver sums for its lineage line.
        return [_FakeResult() for _ in materialised]


@dataclass
class _FakeResult:
    detections: list[Any] = field(default_factory=list)
    emissions: list[Any] = field(default_factory=list)


@dataclass
class _LogCall:
    message: str
    event_type: str
    metadata: Mapping[str, Any] | None
    trace_id: str | None


class RecordingLogger:
    """Captures info() calls for assertion."""

    def __init__(self) -> None:
        self.calls: list[_LogCall] = []

    def info(
        self,
        message: str,
        event_type: str = "",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self.calls.append(_LogCall(message, event_type, metadata, trace_id))

    # The driver only calls info(); warning/error exist to satisfy the
    # structural protocol if ever checked, and are unused here.
    def warning(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        pass

    def error(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover
        pass


def _build_pipeline_capturing(captured: list[FakePipeline]):
    """Builder that records the pipeline it built and the settings it saw."""

    def build(settings: FakeSettings) -> FakePipeline:
        pipeline = FakePipeline(built_with_environment=settings.environment)
        captured.append(pipeline)
        return pipeline

    return build


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


class RunReplayTest(unittest.TestCase):
    def test_builder_receives_replay_swapped_settings(self) -> None:
        # The caller passes LIVE settings; the driver must apply for_replay()
        # before building, so the builder never sees the live environment.
        captured: list[FakePipeline] = []
        live = FakeSettings(environment="live")

        run_replay(
            live,
            event_source=[object(), object()],
            build_pipeline=_build_pipeline_capturing(captured),
            logger=RecordingLogger(),
        )

        (pipeline,) = captured
        self.assertEqual(pipeline.built_with_environment, "replay")

    def test_events_are_processed_in_order_and_results_returned(self) -> None:
        captured: list[FakePipeline] = []
        events = [object(), object(), object()]

        results = run_replay(
            FakeSettings(),
            event_source=events,
            build_pipeline=_build_pipeline_capturing(captured),
            logger=RecordingLogger(),
        )

        (pipeline,) = captured
        self.assertEqual(pipeline.seen, events)          # same objects, same order
        self.assertEqual(len(results), len(events))      # one result per event

    def test_emits_one_replay_completed_lineage_line(self) -> None:
        logger = RecordingLogger()

        run_replay(
            FakeSettings(),
            event_source=[object(), object()],
            build_pipeline=_build_pipeline_capturing([]),
            logger=logger,
        )

        (call,) = logger.calls
        self.assertEqual(call.event_type, "replay.completed")
        assert call.metadata is not None
        self.assertEqual(call.metadata["environment"], "replay")
        self.assertEqual(call.metadata["events"], 2)
        self.assertEqual(call.metadata["results"], 2)

    def test_single_use_generator_source_is_consumed_correctly(self) -> None:
        # A generator can be iterated once. The driver materialises the source
        # before processing, so the event count and the run both see all events
        # rather than the count exhausting the iterator.
        def one_shot() -> Iterator[Any]:
            yield object()
            yield object()
            yield object()

        captured: list[FakePipeline] = []
        logger = RecordingLogger()

        results = run_replay(
            FakeSettings(),
            event_source=one_shot(),
            build_pipeline=_build_pipeline_capturing(captured),
            logger=logger,
        )

        (pipeline,) = captured
        self.assertEqual(len(pipeline.seen), 3)
        self.assertEqual(len(results), 3)
        (call,) = logger.calls
        assert call.metadata is not None
        self.assertEqual(call.metadata["events"], 3)


if __name__ == "__main__":
    unittest.main()
