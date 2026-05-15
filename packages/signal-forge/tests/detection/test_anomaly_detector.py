"""
Tests for signal_forge.detection.detectors.AnomalyDetector.

Verifies the EmissionDetector contract for signal-anomaly detection:

- Strict threshold comparison in both directions: value exactly at
  threshold does not trigger; one unit over (or under) does.
- ``comparison="above"`` and ``comparison="below"`` produce the
  expected detections and skip the opposite cases.
- 'Once per transition into anomaly' semantics: a signal that stays
  anomalous across multiple emissions emits exactly one detection.
  Recovery and re-entry into anomaly emit again with distinct
  detection_ids.
- ``signal_name`` plumbs through to the threshold_breached string
  and the details payload — operators reading an alert see which
  signal triggered.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

from signal_forge.detection.detectors import AnomalyDetector
from signal_forge.streaming.window_aggregator import WindowEmission


def _emission(
    *,
    partition_key: str,
    value: float,
    window_seconds_offset: int = 0,
) -> WindowEmission:
    """
    Construct a WindowEmission as it would be produced by any
    aggregation that emits a single scalar value per window.
    """
    window_start = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC) + timedelta(
        seconds=window_seconds_offset
    )
    window_end = window_start + timedelta(seconds=300)
    return WindowEmission(
        partition_key=partition_key,
        aggregation_name="signal_value",
        window_start=window_start,
        window_end=window_end,
        value=value,
        event_count=1,
        is_repair=False,
        last_contributing_trace_id=None,
    )


class AnomalyDetectorAboveTest(unittest.TestCase):
    def setUp(self) -> None:
        self.detector = AnomalyDetector(
            threshold=300.0,
            comparison="above",
            signal_name="latency_ms",
        )

    def test_below_threshold_does_not_emit(self):
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=250.0)
        )
        self.assertEqual(detections, [])

    def test_at_exact_threshold_does_not_emit(self):
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=300.0)
        )
        self.assertEqual(detections, [])

    def test_above_threshold_emits_once(self):
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=350.0)
        )
        self.assertEqual(len(detections), 1)
        d = detections[0]
        self.assertEqual(d.payload.detection_type, "signal.anomaly")
        self.assertEqual(d.payload.store_id, "store-1")
        self.assertIsNone(d.payload.device_id)
        self.assertEqual(d.payload.details["signal_name"], "latency_ms")
        self.assertEqual(d.payload.details["value"], 350.0)
        self.assertEqual(d.payload.details["threshold"], 300.0)
        self.assertEqual(d.payload.details["comparison"], "above")
        # The threshold_breached string surfaces the signal name for
        # operators reading alerts.
        self.assertIn("latency_ms", d.payload.threshold_breached)
        self.assertIn("350.0", d.payload.threshold_breached)
        self.assertIn("300.0", d.payload.threshold_breached)

    def test_sustained_anomaly_emits_only_on_transition(self):
        first = self.detector.observe_emission(
            _emission(partition_key="store-1", value=350.0)
        )
        second = self.detector.observe_emission(
            _emission(
                partition_key="store-1",
                value=400.0,
                window_seconds_offset=300,
            )
        )
        third = self.detector.observe_emission(
            _emission(
                partition_key="store-1",
                value=380.0,
                window_seconds_offset=600,
            )
        )
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(third, [])

    def test_recovery_then_re_anomaly_emits_twice(self):
        # Flapping signal — emits on each fresh entry into anomaly.
        first = self.detector.observe_emission(
            _emission(partition_key="store-1", value=350.0)
        )
        recovery = self.detector.observe_emission(
            _emission(
                partition_key="store-1",
                value=200.0,
                window_seconds_offset=300,
            )
        )
        second = self.detector.observe_emission(
            _emission(
                partition_key="store-1",
                value=400.0,
                window_seconds_offset=600,
            )
        )
        self.assertEqual(len(first), 1)
        self.assertEqual(recovery, [])
        self.assertEqual(len(second), 1)
        self.assertNotEqual(
            first[0].payload.detection_id,
            second[0].payload.detection_id,
        )


class AnomalyDetectorBelowTest(unittest.TestCase):
    def setUp(self) -> None:
        # Signal where LOW values are anomalous — e.g. heartbeats per
        # device, where the threshold is a floor not a ceiling.
        self.detector = AnomalyDetector(
            threshold=10.0,
            comparison="below",
            signal_name="heartbeats_per_minute",
        )

    def test_above_threshold_does_not_emit(self):
        # 'below' detector should ignore high values.
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=50.0)
        )
        self.assertEqual(detections, [])

    def test_below_threshold_emits(self):
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=5.0)
        )
        self.assertEqual(len(detections), 1)
        self.assertEqual(detections[0].payload.details["comparison"], "below")
        self.assertIn("below", detections[0].payload.threshold_breached)

    def test_at_exact_threshold_does_not_emit(self):
        # Strict below: 10.0 == 10.0 does not trigger.
        detections = self.detector.observe_emission(
            _emission(partition_key="store-1", value=10.0)
        )
        self.assertEqual(detections, [])
