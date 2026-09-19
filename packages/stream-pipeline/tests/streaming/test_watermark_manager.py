"""
Tests for stream_pipeline.streaming.watermark_manager.

Covers:

- monotonicity: watermark never retracts under any event ordering
- per-key independence: stores progress in isolation
- ON_TIME / LATE_TOLERATED / LATE_DROPPED classification boundaries
- lateness=0 disables late-event correction
- input validation: timezone-naive timestamps rejected
- global watermark equals min across per-key watermarks
- determinism: same inputs in same order yield byte-identical state
"""

from __future__ import annotations

import itertools
import unittest
from datetime import UTC, datetime, timedelta

from stream_pipeline.streaming.watermark_manager import (
    EventClassification,
    WatermarkManager,
)


def utc(seconds_since_epoch: int) -> datetime:
    """Helper: build a UTC datetime at a fixed offset from a known anchor."""
    return datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC) + timedelta(
        seconds=seconds_since_epoch
    )


class WatermarkManagerConstructionTest(unittest.TestCase):
    def test_rejects_negative_lateness(self):
        with self.assertRaises(ValueError):
            WatermarkManager(lateness_tolerance_seconds=-1)

    def test_zero_lateness_is_permitted(self):
        # Zero lateness means strict-ordering mode — valid configuration.
        WatermarkManager(lateness_tolerance_seconds=0)


class WatermarkManagerInputValidationTest(unittest.TestCase):
    def test_observe_rejects_empty_key(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        with self.assertRaises(ValueError):
            manager.observe("", utc(0))

    def test_observe_rejects_naive_timestamp(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        with self.assertRaises(ValueError):
            manager.observe("store-1", datetime(2026, 4, 30, 12, 0, 0))


class WatermarkClassificationTest(unittest.TestCase):
    def test_first_event_is_on_time(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        result = manager.observe("store-1", utc(0))
        self.assertEqual(result.classification, EventClassification.ON_TIME)

    def test_event_after_high_advances_watermark_and_is_on_time(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-1", utc(0))
        result = manager.observe("store-1", utc(30))
        self.assertEqual(result.classification, EventClassification.ON_TIME)
        # Watermark = high - lateness = utc(30) - 60s = utc(-30)
        self.assertEqual(result.watermark, utc(-30))
        self.assertEqual(result.high_event_timestamp, utc(30))

    def test_event_within_lateness_budget_is_tolerated(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-1", utc(120))
        # Watermark is now utc(60). An event at utc(30) is 30s late, well
        # within the 60s budget — must be LATE_TOLERATED.
        result = manager.observe("store-1", utc(30))
        self.assertEqual(result.classification, EventClassification.LATE_TOLERATED)

    def test_event_exactly_at_watermark_is_on_time(self):
        # Boundary condition: an event whose timestamp equals the current
        # watermark is ON_TIME. This is what lets two events with the
        # same timestamp be treated consistently regardless of arrival
        # order — important for replay determinism.
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-1", utc(120))
        # Watermark is utc(60). An event exactly at utc(60):
        result = manager.observe("store-1", utc(60))
        self.assertEqual(result.classification, EventClassification.ON_TIME)

    def test_event_outside_lateness_budget_is_dropped(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-1", utc(300))
        # Watermark is now utc(240). An event at utc(60) is 180s late —
        # well past the 60s budget. Must be LATE_DROPPED.
        result = manager.observe("store-1", utc(60))
        self.assertEqual(result.classification, EventClassification.LATE_DROPPED)

    def test_event_just_inside_budget_is_tolerated_just_outside_is_dropped(self):
        # Pin the boundary precisely. Lateness budget is exactly 60s.
        # Event at watermark - 60s -> tolerated. Event at watermark - 61s ->
        # dropped.
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-1", utc(120))  # watermark -> utc(60)

        just_inside = manager.observe("store-1", utc(0))   # 60s late
        self.assertEqual(just_inside.classification, EventClassification.LATE_TOLERATED)

        # Reset and re-test with the harder boundary.
        manager2 = WatermarkManager(lateness_tolerance_seconds=60)
        manager2.observe("store-1", utc(120))  # watermark -> utc(60)
        just_outside = manager2.observe("store-1", utc(-1))   # 61s late
        self.assertEqual(
            just_outside.classification, EventClassification.LATE_DROPPED
        )


class WatermarkLatenessZeroTest(unittest.TestCase):
    def test_zero_lateness_drops_every_late_event(self):
        # In strict-ordering mode, any event predating the high-watermark
        # is dropped; no LATE_TOLERATED zone exists.
        manager = WatermarkManager(lateness_tolerance_seconds=0)
        manager.observe("store-1", utc(60))
        result = manager.observe("store-1", utc(30))
        self.assertEqual(result.classification, EventClassification.LATE_DROPPED)

    def test_zero_lateness_treats_equal_timestamps_as_on_time(self):
        manager = WatermarkManager(lateness_tolerance_seconds=0)
        manager.observe("store-1", utc(60))
        # Same timestamp as the high — watermark is also at utc(60), so
        # this event is exactly at the watermark and ON_TIME.
        result = manager.observe("store-1", utc(60))
        self.assertEqual(result.classification, EventClassification.ON_TIME)


class WatermarkMonotonicityTest(unittest.TestCase):
    def test_watermark_never_retracts(self):
        # Feed an out-of-order sequence: 100, 30, 50, 200, 10.
        # The watermark must only ever advance.
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        observations = [
            manager.observe("store-1", utc(t)) for t in [100, 30, 50, 200, 10]
        ]
        watermarks = [o.watermark for o in observations]

        # Each watermark must be >= the previous one.
        for prev, cur in itertools.pairwise(watermarks):
            self.assertGreaterEqual(cur, prev)

    def test_high_event_timestamp_only_advances(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        first = manager.observe("store-1", utc(100))
        second = manager.observe("store-1", utc(30))
        self.assertEqual(first.high_event_timestamp, utc(100))
        self.assertEqual(second.high_event_timestamp, utc(100))


class WatermarkPerKeyIsolationTest(unittest.TestCase):
    def test_keys_progress_independently(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-A", utc(300))
        # Store A watermark is utc(240). Store B has never been observed,
        # so its watermark is the epoch baseline. An event for store B at
        # utc(0) must be ON_TIME — it is the first event for that key.
        result_b = manager.observe("store-B", utc(0))
        self.assertEqual(result_b.classification, EventClassification.ON_TIME)

    def test_watermark_for_unknown_key_is_epoch_baseline(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        # No observations yet for store-Z — watermark is the epoch baseline.
        wm = manager.watermark_for("store-Z")
        self.assertEqual(wm.year, 1)  # datetime.min has year=1


class WatermarkGlobalTest(unittest.TestCase):
    def test_global_watermark_is_min_of_per_key_watermarks(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        manager.observe("store-A", utc(300))   # watermark utc(240)
        manager.observe("store-B", utc(120))   # watermark utc(60)
        manager.observe("store-C", utc(600))   # watermark utc(540)

        self.assertEqual(manager.global_watermark(), utc(60))

    def test_global_watermark_is_epoch_baseline_when_no_keys(self):
        manager = WatermarkManager(lateness_tolerance_seconds=60)
        self.assertEqual(manager.global_watermark().year, 1)


class WatermarkDeterminismTest(unittest.TestCase):
    def test_identical_inputs_yield_identical_state(self):
        # Replay determinism: feeding the same ordered events into a
        # fresh manager must produce the same per-key state byte-for-byte.
        events = [
            ("store-A", utc(0)),
            ("store-A", utc(60)),
            ("store-B", utc(30)),
            ("store-A", utc(45)),  # late by 15s, tolerated
            ("store-B", utc(120)),
        ]

        manager1 = WatermarkManager(lateness_tolerance_seconds=60)
        manager2 = WatermarkManager(lateness_tolerance_seconds=60)

        results1 = [manager1.observe(k, t) for k, t in events]
        results2 = [manager2.observe(k, t) for k, t in events]

        # Classification sequences identical
        self.assertEqual(
            [r.classification for r in results1],
            [r.classification for r in results2],
        )
        # Final watermarks identical
        self.assertEqual(
            manager1.watermark_for("store-A"),
            manager2.watermark_for("store-A"),
        )
        self.assertEqual(
            manager1.watermark_for("store-B"),
            manager2.watermark_for("store-B"),
        )
        self.assertEqual(manager1.keys(), manager2.keys())


if __name__ == "__main__":
    unittest.main()
