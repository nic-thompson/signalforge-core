"""
Tests for signal_forge.dashboards.anomaly_rate_projection.AnomalyRateProjection.

Covers the event-time bucketing fold, the as_of-parameterised read path,
idempotency under duplicate delivery (D-17), window roll-off, signal
separation, and fleet-wide aggregation across stores. All reads pass a
fixed as_of instant, so the tests carry no wall-clock dependency.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.dashboards.anomaly_rate_projection import AnomalyRateProjection
from signal_forge.dashboards.projection_store import InMemoryProjectionStore
from signal_forge.detection.types import (
    DETECTION_TYPE_SIGNAL_ANOMALY,
    DETECTION_TYPE_STORE_OUTAGE,
)

_BASE = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)


def _anomaly(
    signal_name: str,
    *,
    at: datetime,
    detection_id: object = None,
    store_id: str = "store-1",
) -> DetectionEvent:
    """Build a signal.anomaly DetectionEvent landing at event-time ``at``."""
    payload = DetectionEventPayload(
        detection_id=detection_id if detection_id is not None else uuid4(),
        detection_type=DETECTION_TYPE_SIGNAL_ANOMALY,
        severity=DetectionSeverity.WARNING,
        detected_at=at,
        store_id=store_id,
        device_id=None,
        source_event_id=uuid4(),
        threshold_breached="test",
        details={"signal_name": signal_name},
    )
    return DetectionEvent(
        event_id=uuid4(),
        event_timestamp=at,
        trace=TraceContext(trace_id=uuid4()),
        payload=payload,
    )


class AnomalyRateProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryProjectionStore()
        self.projection = AnomalyRateProjection(self.store)

    def test_cold_start_is_zero(self) -> None:
        self.assertEqual(
            self.projection.count_in_window("latency", as_of=_BASE), 0
        )
        self.assertEqual(
            self.projection.rate_per_minute("latency", as_of=_BASE), 0.0
        )

    def test_three_anomalies_in_window_count_three(self) -> None:
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=10)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=70)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=130)))
        as_of = _BASE + timedelta(seconds=300)
        self.assertEqual(
            self.projection.count_in_window("latency", as_of=as_of), 3
        )

    def test_rate_per_minute_normalises_the_count(self) -> None:
        # 3 anomalies over a 5-minute window = 0.6 per minute.
        for offset in (10, 70, 130):
            self.projection.observe(
                _anomaly("latency", at=_BASE + timedelta(seconds=offset))
            )
        as_of = _BASE + timedelta(seconds=300)
        self.assertAlmostEqual(
            self.projection.rate_per_minute(
                "latency", as_of=as_of, window_seconds=300
            ),
            0.6,
        )

    def test_duplicate_detection_id_is_idempotent(self) -> None:
        # Redelivery / replay re-run: the same deterministic detection_id
        # must not inflate the count.
        fixed = uuid4()
        at = _BASE + timedelta(seconds=10)
        self.projection.observe(_anomaly("latency", at=at, detection_id=fixed))
        self.projection.observe(_anomaly("latency", at=at, detection_id=fixed))
        self.assertEqual(
            self.projection.count_in_window(
                "latency", as_of=_BASE + timedelta(seconds=300)
            ),
            1,
        )

    def test_old_anomaly_falls_out_of_a_later_window(self) -> None:
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=10)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=610)))
        as_of = _BASE + timedelta(seconds=660)
        # Window 300 -> [360, 660): only the second anomaly.
        self.assertEqual(
            self.projection.count_in_window(
                "latency", as_of=as_of, window_seconds=300
            ),
            1,
        )

    def test_wider_window_catches_both(self) -> None:
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=10)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=610)))
        as_of = _BASE + timedelta(seconds=660)
        self.assertEqual(
            self.projection.count_in_window(
                "latency", as_of=as_of, window_seconds=700
            ),
            2,
        )

    def test_signals_are_separated(self) -> None:
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=10)))
        self.projection.observe(
            _anomaly("error_rate", at=_BASE + timedelta(seconds=10))
        )
        as_of = _BASE + timedelta(seconds=300)
        self.assertEqual(self.projection.count_in_window("latency", as_of=as_of), 1)
        self.assertEqual(
            self.projection.count_in_window("error_rate", as_of=as_of), 1
        )

    def test_same_signal_aggregates_across_stores(self) -> None:
        # The rate is fleet-wide by signal type: anomalies for the same
        # signal from different stores (distinct detection_ids) both count.
        self.projection.observe(
            _anomaly("latency", at=_BASE + timedelta(seconds=10), store_id="store-1")
        )
        self.projection.observe(
            _anomaly("latency", at=_BASE + timedelta(seconds=20), store_id="store-2")
        )
        as_of = _BASE + timedelta(seconds=300)
        self.assertEqual(self.projection.count_in_window("latency", as_of=as_of), 2)

    def test_unrelated_detection_type_is_ignored(self) -> None:
        payload = DetectionEventPayload(
            detection_id=uuid4(),
            detection_type=DETECTION_TYPE_STORE_OUTAGE,
            severity=DetectionSeverity.CRITICAL,
            detected_at=_BASE + timedelta(seconds=10),
            store_id="store-1",
            device_id=None,
            source_event_id=uuid4(),
            threshold_breached="test",
            details={},
        )
        detection = DetectionEvent(
            event_id=uuid4(),
            event_timestamp=_BASE,
            trace=TraceContext(trace_id=uuid4()),
            payload=payload,
        )
        self.projection.observe(detection)
        self.assertEqual(
            self.projection.count_in_window(
                "latency", as_of=_BASE + timedelta(seconds=300)
            ),
            0,
        )

    def test_anomalies_in_same_bucket_share_one_key(self) -> None:
        # Two anomalies 50s apart fall in the same 60s epoch-aligned bucket.
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=5)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=55)))
        self.assertEqual(len(self.store.keys(AnomalyRateProjection.VIEW)), 1)
        self.assertEqual(
            self.projection.count_in_window(
                "latency", as_of=_BASE + timedelta(seconds=300)
            ),
            2,
        )

    def test_anomalies_in_adjacent_buckets_use_distinct_keys(self) -> None:
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=5)))
        self.projection.observe(_anomaly("latency", at=_BASE + timedelta(seconds=65)))
        self.assertEqual(len(self.store.keys(AnomalyRateProjection.VIEW)), 2)


if __name__ == "__main__":
    unittest.main()
