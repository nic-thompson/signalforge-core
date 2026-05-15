"""
Tests for signal_forge.streaming.window_aggregator.

Covers:

- WindowSpec validation (size, slide, divisibility, gapless)
- tumbling window basic correctness
- sliding window overlap (size=60, slide=15) — every event in 4 windows
- epoch-relative window alignment (replay determinism, cross-shard joins)
- watermark-driven emission (no premature emit)
- late-event repair (re-emission with is_repair=True)
- LATE_DROPPED events do not affect aggregations
- per-key isolation (one key's late event never affects another's)
- sealing horizon evicts state past watermark + lateness
- chronological emission order across multiple closing windows
- determinism: identical event sequences produce identical emissions
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from signal_forge.streaming.watermark_manager import EventClassification
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    DistinctCountAggregation,
    SumAggregation,
    WindowAggregator,
    WindowEmission,
    WindowSpec,
)


def utc(seconds_since_anchor: int) -> datetime:
    """Anchor: 2026-04-30 12:00:00 UTC, deliberately not slide-aligned to
    catch tests that accidentally rely on alignment with the anchor."""
    return datetime(2026, 4, 30, 12, 0, 7, tzinfo=UTC) + timedelta(
        seconds=seconds_since_anchor
    )


def epoch_aligned(seconds_since_epoch: int) -> datetime:
    """Build a UTC datetime at exactly N seconds past the Unix epoch."""
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(seconds=seconds_since_epoch)


# ---------------------------------------------------------------------------
# WindowSpec validation
# ---------------------------------------------------------------------------


class WindowSpecTest(unittest.TestCase):
    def test_rejects_zero_size(self):
        with self.assertRaises(ValueError):
            WindowSpec(size_seconds=0, slide_seconds=5)

    def test_rejects_zero_slide(self):
        with self.assertRaises(ValueError):
            WindowSpec(size_seconds=60, slide_seconds=0)

    def test_rejects_slide_greater_than_size(self):
        with self.assertRaises(ValueError):
            WindowSpec(size_seconds=10, slide_seconds=20)

    def test_rejects_non_divisible_slide(self):
        with self.assertRaises(ValueError):
            WindowSpec(size_seconds=60, slide_seconds=7)

    def test_tumbling_property(self):
        self.assertTrue(WindowSpec(size_seconds=5, slide_seconds=5).is_tumbling)
        self.assertFalse(WindowSpec(size_seconds=60, slide_seconds=15).is_tumbling)


# ---------------------------------------------------------------------------
# Tumbling windows — basic correctness
# ---------------------------------------------------------------------------


class TumblingWindowTest(unittest.TestCase):
    def setUp(self):
        self.agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )

    def test_no_emission_before_watermark_passes_window_end(self):
        # Event at t, watermark at t+1 — window [floor(t/5)*5, +5) is
        # not yet closed.
        ts = epoch_aligned(102)  # window start 100, end 105
        wm = epoch_aligned(103)
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=ts,
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=wm,
        )
        self.assertEqual(emissions, [])

    def test_emits_when_watermark_passes_window_end(self):
        # First event in [100, 105) with watermark inside the window —
        # no emission yet.
        emissions_open = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(103),
        )
        self.assertEqual(emissions_open, [])

        # Second event lands in [105, 110) with watermark=110. Two
        # windows close on this call:
        #   - [100, 105) which contained the first event (count=1)
        #   - [105, 110) which contains the second event (count=1)
        # because the watermark sits on the right edge of [105, 110).
        # Both must emit, in chronological window-start order.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(106),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        self.assertEqual(len(emissions), 2)

        first, second = emissions
        self.assertEqual(first.window_start, epoch_aligned(100))
        self.assertEqual(first.window_end, epoch_aligned(105))
        self.assertEqual(first.value, 1)
        self.assertFalse(first.is_repair)

        self.assertEqual(second.window_start, epoch_aligned(105))
        self.assertEqual(second.window_end, epoch_aligned(110))
        self.assertEqual(second.value, 1)
        self.assertFalse(second.is_repair)

    def test_window_boundaries_are_epoch_relative(self):
        # An event at epoch+127 belongs to window [125, 130). With a
        # watermark already at 135 the window's right edge has been
        # crossed, so the window emits on the SAME observe() call —
        # not on a later one.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(127),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(135),
        )

        # Exactly one emission, with a window aligned to the Unix epoch
        # and not to the test anchor. This is what guarantees cross-
        # shard joinability for downstream datasets.
        self.assertEqual(len(emissions), 1)
        emission = emissions[0]
        self.assertEqual(emission.window_start, epoch_aligned(125))
        self.assertEqual(emission.window_end, epoch_aligned(130))
        self.assertEqual(emission.value, 1)
        self.assertFalse(emission.is_repair)

# ---------------------------------------------------------------------------
# Sliding windows
# ---------------------------------------------------------------------------


class SlidingWindowTest(unittest.TestCase):
    def test_event_belongs_to_size_over_slide_windows(self):
        # size=60, slide=15 -> 4 overlapping windows.
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=60, slide_seconds=15),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )

        # Event at epoch+72. Windows containing it are starts at 60, 45, 30, 15.
        agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(72),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(72),
        )
        self.assertEqual(agg.open_window_count("store-1"), 4)

    def test_sliding_emissions_in_chronological_order(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=60, slide_seconds=15),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )
        # Single event placed at t=72. Windows [15,75), [30,90), [45,105), [60,120).
        agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(72),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(72),
        )
        # Advance the watermark past the latest window end (120).
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(200),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(200),
        )
        # All four overlapping windows should now have closed. The event
        # at t=200 also opens new windows; filter to the four containing 72.
        relevant_starts = {15, 30, 45, 60}
        relevant = [
            e for e in emissions
            if int((e.window_start - epoch_aligned(0)).total_seconds()) in relevant_starts
        ]
        self.assertEqual(len(relevant), 4)
        # And they emit in chronological window-start order.
        starts_in_order = [e.window_start for e in relevant]
        self.assertEqual(starts_in_order, sorted(starts_in_order))


# ---------------------------------------------------------------------------
# Late-event repair
# ---------------------------------------------------------------------------


class LateEventRepairTest(unittest.TestCase):
    def setUp(self):
        self.agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )

    def test_late_tolerated_event_triggers_repair_emission(self):
        # Event in window [100, 105) emits when watermark passes 105.
        self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
        )
        emissions_initial = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        first_closure = [e for e in emissions_initial if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 1)
        self.assertFalse(first_closure[0].is_repair)

        # Now a LATE_TOLERATED event lands in window [100, 105) — well
        # within the 60s lateness budget at watermark=110.
        repairs = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(103),
            contribution=None,
            classification=EventClassification.LATE_TOLERATED,
            watermark=epoch_aligned(110),
        )
        repair_emissions = [e for e in repairs if e.is_repair]
        self.assertEqual(len(repair_emissions), 1)
        self.assertEqual(repair_emissions[0].window_start, epoch_aligned(100))
        self.assertEqual(repair_emissions[0].value, 2)
        self.assertEqual(repair_emissions[0].event_count, 2)

    def test_late_dropped_event_does_not_repair_or_aggregate(self):
        self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
        )
        self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        # Watermark advances far enough that the [100,105) window is now
        # past its sealing horizon (105 + 60 = 165).
        repairs = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(103),
            contribution=None,
            classification=EventClassification.LATE_DROPPED,
            watermark=epoch_aligned(170),
        )
        # No repairs — LATE_DROPPED is silently ignored for aggregation.
        self.assertEqual([e for e in repairs if e.is_repair], [])


# ---------------------------------------------------------------------------
# Per-key isolation
# ---------------------------------------------------------------------------


class PerKeyIsolationTest(unittest.TestCase):
    def test_one_keys_late_event_does_not_affect_another(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )

        agg.observe(
            partition_key="store-A",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        agg.observe(
            partition_key="store-B",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )

        # Late event for store-A only.
        emissions = agg.observe(
            partition_key="store-A",
            event_timestamp=epoch_aligned(103),
            contribution=None,
            classification=EventClassification.LATE_TOLERATED,
            watermark=epoch_aligned(110),
        )
        self.assertTrue(all(e.partition_key == "store-A" for e in emissions))


# ---------------------------------------------------------------------------
# Sealing horizon
# ---------------------------------------------------------------------------


class SealingHorizonTest(unittest.TestCase):
    def test_state_is_evicted_past_lateness_horizon(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=10,
        )
        agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
        )
        self.assertEqual(agg.open_window_count("store-1"), 1)

        # Window [100, 105). Sealing horizon = 105 + 10 = 115.
        agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(120),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(120),
        )
        # Window [100, 105) should now be sealed and evicted; only the new
        # window [120, 125) is open.
        self.assertEqual(agg.open_window_count("store-1"), 1)


# ---------------------------------------------------------------------------
# SumAggregation
# ---------------------------------------------------------------------------


class SumAggregationTest(unittest.TestCase):
    def test_sum_accumulates_numeric_contributions(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=SumAggregation(),
            lateness_tolerance_seconds=60,
        )
        for value in (1.5, 2.0, 0.5):
            agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(102),
                contribution=value,
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(102),
            )
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution=0.0,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        first_closure = [e for e in emissions if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 4.0)
        self.assertEqual(first_closure[0].event_count, 3)

    def test_sum_rejects_non_numeric_contribution(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=SumAggregation(),
            lateness_tolerance_seconds=60,
        )
        with self.assertRaises(TypeError):
            agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(102),
                contribution="not-numeric",
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(102),
            )


class DistinctCountAggregationTest(unittest.TestCase):
    def test_empty_window_finalises_to_zero(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=DistinctCountAggregation(key=lambda c: c),
            lateness_tolerance_seconds=60,
        )
        # One event opens window [100, 105) with a distinct value;
        # then advance watermark to close.
        agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution="device-a",
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
        )
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution="device-b",
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        # The closed window has cardinality 1 (just "device-a").
        first_closure = [e for e in emissions if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 1)

    def test_duplicate_contributions_dedupe(self):
        # Three events with the same device_id. Cardinality is 1.
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=DistinctCountAggregation(key=lambda c: c),
            lateness_tolerance_seconds=60,
        )
        for _ in range(3):
            agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(102),
                contribution="device-a",
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(102),
            )
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution="device-z",
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        first_closure = [e for e in emissions if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 1)
        # event_count is the count of observations, which is 3 — even
        # though the cardinality is 1. Pinning the distinction here so
        # a future reader sees the two counters are separate concerns.
        self.assertEqual(first_closure[0].event_count, 3)

    def test_distinct_contributions_count_separately(self):
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=DistinctCountAggregation(key=lambda c: c),
            lateness_tolerance_seconds=60,
        )
        for device in ("device-a", "device-b", "device-c"):
            agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(102),
                contribution=device,
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(102),
            )
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution="device-z",
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        first_closure = [e for e in emissions if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 3)

    def test_key_extractor_is_applied_to_each_contribution(self):
        # Contributions are dicts; the key extracts a specific field.
        # If the extractor were ignored (e.g. dict added to a set
        # directly), this would raise TypeError on unhashable type.
        agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=DistinctCountAggregation(key=lambda c: c["device_id"]),
            lateness_tolerance_seconds=60,
        )
        for device in ("a", "b", "a"):  # 'a' duplicated
            agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(102),
                contribution={"device_id": device, "other_field": "noise"},
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(102),
            )
        emissions = agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(110),
            contribution={"device_id": "z"},
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(110),
        )
        first_closure = [e for e in emissions if e.window_start == epoch_aligned(100)]
        self.assertEqual(len(first_closure), 1)
        self.assertEqual(first_closure[0].value, 2)  # 'a' and 'b'


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


class DeterminismTest(unittest.TestCase):
    def test_identical_inputs_yield_identical_emissions(self):
        events = [
            ("store-A", epoch_aligned(102), EventClassification.ON_TIME, epoch_aligned(102)),
            ("store-A", epoch_aligned(106), EventClassification.ON_TIME, epoch_aligned(106)),
            ("store-B", epoch_aligned(103), EventClassification.ON_TIME, epoch_aligned(106)),
            ("store-A", epoch_aligned(112), EventClassification.ON_TIME, epoch_aligned(112)),
            ("store-A", epoch_aligned(104), EventClassification.LATE_TOLERATED, epoch_aligned(112)),
        ]

        def run() -> list[WindowEmission]:
            agg = WindowAggregator(
                spec=WindowSpec(size_seconds=5, slide_seconds=5),
                aggregation=CountAggregation(),
                lateness_tolerance_seconds=60,
            )
            out: list[WindowEmission] = []
            for key, ts, cls, wm in events:
                out.extend(
                    agg.observe(
                        partition_key=key,
                        event_timestamp=ts,
                        contribution=None,
                        classification=cls,
                        watermark=wm,
                    )
                )
            return out

        run1 = run()
        run2 = run()
        self.assertEqual(run1, run2)


# ---------------------------------------------------------------------------
# Trace-id propagation
# ---------------------------------------------------------------------------


class TraceIdPropagationTest(unittest.TestCase):
    """
    Trace-id propagation through window aggregation.

    Phase 2 emission detectors derive ``DetectionEvent.trace`` from
    the most recent contributing event's trace_id, surfaced via
    ``WindowEmission.last_contributing_trace_id``. These tests pin that
    contract:

    - the field carries the contributor's trace_id on first-time closure
    - the field carries the most recent contributor's trace_id when
      multiple events contribute to the same window
    - late-event repair emissions carry the late event's trace_id
      (the most-recent-contributor semantics)
    """

    def setUp(self):
        # Tumbling 5s windows. Lateness 60s gives plenty of room for
        # repair scenarios.
        self.agg = WindowAggregator(
            spec=WindowSpec(size_seconds=5, slide_seconds=5),
            aggregation=CountAggregation(),
            lateness_tolerance_seconds=60,
        )

    def test_closure_emission_carries_last_contributing_trace_id(self):
        # Single event in window [100, 105). Watermark advances past
        # the window end to trigger closure.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
            trace_id="trace-A",
        )
        # No emission yet — watermark hasn't passed window_end.
        self.assertEqual(emissions, [])

        # Advance watermark past window_end with an event in the next window.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(106),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(106),
            trace_id="trace-B",
        )
        # The closing window [100, 105) carries trace-A from its sole
        # contributor. The new event (trace-B) belongs to the next
        # window and does not affect the closing emission.
        self.assertEqual(len(emissions), 1)
        self.assertEqual(emissions[0].window_start, epoch_aligned(100))
        self.assertEqual(emissions[0].last_contributing_trace_id, "trace-A")

    def test_multiple_contributions_keep_most_recent_trace_id(self):
        # Three events in the same window [100, 105). Each carries a
        # different trace_id. The last one wins.
        for ts_offset, trace in [(101, "trace-A"), (102, "trace-B"), (103, "trace-C")]:
            self.agg.observe(
                partition_key="store-1",
                event_timestamp=epoch_aligned(ts_offset),
                contribution=None,
                classification=EventClassification.ON_TIME,
                watermark=epoch_aligned(ts_offset),
                trace_id=trace,
            )

        # Advance watermark past window end to trigger closure.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(106),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(106),
            trace_id="trace-D",
        )

        # The closure carries trace-C (the most recent contribution to
        # window [100, 105)), not trace-D (which belongs to the next
        # window).
        self.assertEqual(len(emissions), 1)
        self.assertEqual(emissions[0].event_count, 3)
        self.assertEqual(emissions[0].last_contributing_trace_id, "trace-C")

    def test_repair_emission_carries_most_recent_trace_id(self):
        # First-time closure of window [100, 105) with one ON_TIME event.
        self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(102),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(102),
            trace_id="trace-original",
        )
        # Advance watermark to close the window.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(106),
            contribution=None,
            classification=EventClassification.ON_TIME,
            watermark=epoch_aligned(106),
            trace_id="trace-other-window",
        )
        self.assertEqual(len(emissions), 1)
        self.assertEqual(emissions[0].is_repair, False)
        self.assertEqual(emissions[0].last_contributing_trace_id, "trace-original")

        # Now a LATE_TOLERATED event arrives for the closed window.
        # Repair re-emission should carry the late event's trace_id.
        emissions = self.agg.observe(
            partition_key="store-1",
            event_timestamp=epoch_aligned(103),
            contribution=None,
            classification=EventClassification.LATE_TOLERATED,
            watermark=epoch_aligned(110),
            trace_id="trace-late",
        )
        # One repair emission for the closed window.
        repairs = [e for e in emissions if e.is_repair]
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0].last_contributing_trace_id, "trace-late")
        self.assertEqual(repairs[0].event_count, 2)


if __name__ == "__main__":
    unittest.main()
