"""
Tests for feature-event bundling in RealtimePipeline.

Verifies that ``RealtimePipeline.process()`` bundles window emissions
into ``WindowedFeatureVectorEvent``s and exposes them on
``ProcessingResult.features``. Bundling groups by ``(partition_key,
window_start)``; multiple aggregations on the same window collapse
into one event with all their values; separate partitions or windows
produce separate events; repair emissions produce additional events.

Trace propagation follows the best-effort pattern from D-9: the
feature event inherits the first non-None ``last_contributing_trace_id``
from emissions in the group, falling back to a fresh ``TraceContext``
when none is set.

Replay determinism at the sequence level is preserved (same inputs
-> same number of events in the same order with the same payloads).
Per-event ``event_id`` is non-deterministic (``uuid4()`` default on
``BaseEvent``), matching the trade-off accepted for ``DetectionEvent``.

Watermark math: the default ``make_pipeline`` factory uses
``lateness_tolerance_seconds=60`` and a 5-second tumbling window.
Watermark = event_timestamp - 60. To close window [100, 105), the
closing event needs event_timestamp >= 165 (so watermark >= 105).
Mirrors the pattern in test_realtime_pipeline.py.
"""

from __future__ import annotations

import unittest
from uuid import UUID, uuid4

from signal_forge.features import FEATURE_SCHEMA_VERSION
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, FakeTrace
from tests.streaming.test_realtime_pipeline import epoch_aligned, make_pipeline


def _event(
    *,
    source: str,
    seconds: int,
    schema_version: str = "v1",
    trace_id: UUID | None = None,
) -> FakeEvent:
    """
    Build a FakeEvent with a deterministic timestamp and explicit source.

    The pipeline's default ``by_event_source`` extractor reads
    ``event.metadata.source`` as the partition_key, so ``source`` here
    drives partitioning. Optional ``trace_id`` overrides the default
    fresh UUID for tests asserting trace propagation.
    """
    trace = FakeTrace(trace_id=trace_id) if trace_id is not None else FakeTrace()
    return FakeEvent(
        event_type="device.registration",
        schema_version=schema_version,
        event_timestamp=epoch_aligned(seconds),
        source=source,
        trace=trace,
    )


class FeatureBundlingTest(unittest.TestCase):
    def test_zero_emissions_produces_empty_features(self):
        # An event that doesn't trigger a window closure: watermark
        # at 102-60=42 hasn't crossed window [100, 105)'s right edge.
        # ProcessingResult.features should be [].
        pipeline, _, _, _ = make_pipeline()
        result = pipeline.process(_event(source="store-1", seconds=102))
        self.assertEqual(result.emissions, [])
        self.assertEqual(result.features, [])

    def test_single_emission_produces_single_feature_event(self):
        # Event at t=102 opens window [100, 105). Event at t=165
        # advances watermark to 105, closing the window.
        pipeline, _, _, _ = make_pipeline()
        pipeline.process(_event(source="store-1", seconds=102))
        result = pipeline.process(_event(source="store-1", seconds=165))

        # One emission for the closed window, one bundled feature event.
        first_window_emissions = [
            e for e in result.emissions if e.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_emissions), 1)

        first_window_features = [
            f for f in result.features if f.payload.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_features), 1)

        event = first_window_features[0]
        self.assertEqual(event.payload.partition_key, "store-1")
        self.assertEqual(event.payload.window_start, epoch_aligned(100))
        self.assertEqual(event.payload.window_end, epoch_aligned(105))
        self.assertEqual(event.payload.feature_values, {"count": 1})
        self.assertEqual(event.payload.feature_version, FEATURE_SCHEMA_VERSION)

    def test_multiple_aggregations_bundle_into_single_feature_event(self):
        # Two aggregations on the same window geometry. Each
        # aggregation's `name` field becomes the key in feature_values,
        # so the two CountAggregations must have distinct names. Both
        # emissions land for the same (partition_key, window_start)
        # and bundle into a single feature event with both values.
        pipeline, _, _, _ = make_pipeline()
        second_aggregator = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(name="count_b"),
            lateness_tolerance_seconds=60,
        )
        pipeline.register_aggregator("count_b", second_aggregator)

        pipeline.process(_event(source="store-1", seconds=102))
        result = pipeline.process(_event(source="store-1", seconds=165))

        first_window_emissions = [
            e for e in result.emissions if e.window_start == epoch_aligned(100)
        ]
        # Two emissions (one per aggregator).
        self.assertEqual(len(first_window_emissions), 2)

        first_window_features = [
            f for f in result.features if f.payload.window_start == epoch_aligned(100)
        ]
        # One bundled feature event with both values.
        self.assertEqual(len(first_window_features), 1)

        event = first_window_features[0]
        self.assertEqual(event.payload.partition_key, "store-1")
        self.assertEqual(event.payload.feature_values, {"count": 1, "count_b": 1})

    def test_separate_partitions_produce_separate_feature_events(self):
        # Two stores each see one event in their window, then each
        # is advanced past the right edge by a second event for the
        # same partition. The watermark is per-key, so each store's
        # window closes only when an event for that store crosses.
        pipeline, _, _, _ = make_pipeline()
        pipeline.process(_event(source="store-1", seconds=102))
        pipeline.process(_event(source="store-2", seconds=103))
        result_a = pipeline.process(_event(source="store-1", seconds=165))
        result_b = pipeline.process(_event(source="store-2", seconds=165))

        first_window_features_a = [
            f for f in result_a.features if f.payload.window_start == epoch_aligned(100)
        ]
        first_window_features_b = [
            f for f in result_b.features if f.payload.window_start == epoch_aligned(100)
        ]

        self.assertEqual(len(first_window_features_a), 1)
        self.assertEqual(len(first_window_features_b), 1)

        self.assertEqual(first_window_features_a[0].payload.partition_key, "store-1")
        self.assertEqual(first_window_features_b[0].payload.partition_key, "store-2")

    def test_repair_emission_produces_new_feature_event(self):
        # An event with timestamp inside an already-closed window
        # triggers a late-event repair when the event_timestamp is
        # within the lateness tolerance. After closing window
        # [100, 105) with an event at t=165 (watermark=105), a late
        # event at t=103 falls within [watermark - lateness=45, 105)
        # and triggers repair. The repair emission carries
        # is_repair=True and updated state; bundling produces a new
        # feature event for the same (partition_key, window_start).
        pipeline, _, _, _ = make_pipeline()
        # Open window [100, 105) with one event.
        pipeline.process(_event(source="store-1", seconds=102))
        # Close it with an event at t=165.
        result_closure = pipeline.process(_event(source="store-1", seconds=165))
        first_window_features = [
            f
            for f in result_closure.features
            if f.payload.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_features), 1)
        self.assertEqual(first_window_features[0].payload.feature_values, {"count": 1})

        # A late event at t=103 (inside the closed window's interval
        # [100, 105), and within lateness tolerance) triggers repair.
        result_repair = pipeline.process(_event(source="store-1", seconds=103))

        repair_emissions = [
            e
            for e in result_repair.emissions
            if e.window_start == epoch_aligned(100) and e.is_repair
        ]
        self.assertEqual(len(repair_emissions), 1)

        repair_features = [
            f
            for f in result_repair.features
            if f.payload.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(repair_features), 1)

        repair_event = repair_features[0]
        self.assertEqual(repair_event.payload.partition_key, "store-1")
        self.assertEqual(repair_event.payload.window_start, epoch_aligned(100))
        # Count is now 2 (original + late arrival).
        self.assertEqual(repair_event.payload.feature_values, {"count": 2})

    def test_trace_propagation_from_emission(self):
        # When an emission carries last_contributing_trace_id, the
        # bundled feature event's trace inherits that UUID. The
        # contributing event's trace is captured in the aggregator's
        # window state; the closing event's trace doesn't override
        # it because the closing event lands in a different window
        # (the closing event at t=165 opens window [165, 170), not
        # [100, 105)).
        pipeline, _, _, _ = make_pipeline()
        contributing_trace_id = uuid4()
        pipeline.process(
            _event(
                source="store-1",
                seconds=102,
                trace_id=contributing_trace_id,
            )
        )
        result = pipeline.process(_event(source="store-1", seconds=165))

        first_window_features = [
            f for f in result.features if f.payload.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_features), 1)
        self.assertEqual(
            first_window_features[0].trace.trace_id, contributing_trace_id
        )

    def test_feature_event_trace_id_is_valid_uuid(self):
        # FakeEvent always carries a FakeTrace with a fresh UUID,
        # so emissions always have a non-None
        # last_contributing_trace_id in tests. The fallback to a
        # fresh TraceContext when no trace exists is not reachable
        # through the public pipeline API. This test verifies the
        # typical path: feature events carry a valid UUID trace_id.
        pipeline, _, _, _ = make_pipeline()
        pipeline.process(_event(source="store-1", seconds=102))
        result = pipeline.process(_event(source="store-1", seconds=165))

        first_window_features = [
            f for f in result.features if f.payload.window_start == epoch_aligned(100)
        ]
        self.assertEqual(len(first_window_features), 1)
        trace_id = first_window_features[0].trace.trace_id
        self.assertIsInstance(trace_id, UUID)
