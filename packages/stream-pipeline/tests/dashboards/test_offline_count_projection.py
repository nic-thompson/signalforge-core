"""
Tests for stream_pipeline.dashboards.offline_count_projection.OfflineCountProjection.

Covers the gauge fold and, centrally, its idempotency under duplicate
delivery — the property that distinguishes the set-of-device-ids design
from a naive counter (D-17).
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from uuid import UUID, uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from stream_pipeline.dashboards.offline_count_projection import OfflineCountProjection
from stream_pipeline.dashboards.projection_store import InMemoryProjectionStore
from stream_pipeline.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_DEVICE_ONLINE,
    DETECTION_TYPE_STORE_OUTAGE,  # adjust if your constant is named differently
)


def _detection(
    detection_type: str,
    store_id: str,
    device_id: UUID | None,
    *,
    severity: DetectionSeverity = DetectionSeverity.WARNING,
) -> DetectionEvent:
    """Build a minimal valid DetectionEvent for the projection to fold."""
    detection_id = uuid4()
    payload = DetectionEventPayload(
        detection_id=detection_id,
        detection_type=detection_type,
        severity=severity,
        detected_at=datetime(2026, 1, 1, tzinfo=UTC),
        store_id=store_id,
        device_id=device_id,
        source_event_id=uuid4(),
        threshold_breached="test",
        details={},
    )
    return DetectionEvent(
        event_id=uuid4(),
        event_timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        trace=TraceContext(trace_id=uuid4()),
        payload=payload,
    )


def _offline(store_id: str, device_id: UUID) -> DetectionEvent:
    return _detection(
        DETECTION_TYPE_DEVICE_OFFLINE, store_id, device_id,
        severity=DetectionSeverity.WARNING,
    )


def _online(store_id: str, device_id: UUID) -> DetectionEvent:
    return _detection(
        DETECTION_TYPE_DEVICE_ONLINE, store_id, device_id,
        severity=DetectionSeverity.INFO,
    )


class OfflineCountProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryProjectionStore()
        self.projection = OfflineCountProjection(self.store)

    def test_cold_start_unknown_store_is_zero(self) -> None:
        self.assertEqual(self.projection.offline_count("store-1"), 0)
        self.assertEqual(self.projection.stores_with_offline(), [])

    def test_single_offline_counts_one(self) -> None:
        self.projection.observe(_offline("store-1", uuid4()))
        self.assertEqual(self.projection.offline_count("store-1"), 1)
        self.assertEqual(self.projection.stores_with_offline(), ["store-1"])

    def test_two_devices_same_store_counts_two(self) -> None:
        self.projection.observe(_offline("store-1", uuid4()))
        self.projection.observe(_offline("store-1", uuid4()))
        self.assertEqual(self.projection.offline_count("store-1"), 2)

    def test_offline_then_online_returns_to_zero_and_cleans_key(self) -> None:
        device = uuid4()
        self.projection.observe(_offline("store-1", device))
        self.projection.observe(_online("store-1", device))
        self.assertEqual(self.projection.offline_count("store-1"), 0)
        # Key dropped once the set empties — view stays bounded.
        self.assertEqual(self.projection.stores_with_offline(), [])

    def test_duplicate_offline_is_idempotent(self) -> None:
        # The at-least-once point: a redelivered device.offline must not
        # inflate the count. This is the property the set design buys.
        device = uuid4()
        self.projection.observe(_offline("store-1", device))
        self.projection.observe(_offline("store-1", device))
        self.assertEqual(self.projection.offline_count("store-1"), 1)

    def test_online_for_never_offline_device_is_idempotent(self) -> None:
        # A redelivered or out-of-order recovery for a device not currently
        # offline must not crash or drive the count negative.
        self.projection.observe(_online("store-1", uuid4()))
        self.assertEqual(self.projection.offline_count("store-1"), 0)
        self.assertEqual(self.projection.stores_with_offline(), [])

    def test_partitions_are_independent(self) -> None:
        self.projection.observe(_offline("store-1", uuid4()))
        self.projection.observe(_offline("store-2", uuid4()))
        self.projection.observe(_offline("store-2", uuid4()))
        self.assertEqual(self.projection.offline_count("store-1"), 1)
        self.assertEqual(self.projection.offline_count("store-2"), 2)
        self.assertEqual(
            self.projection.stores_with_offline(), ["store-1", "store-2"]
        )

    def test_flapping_collapses_to_current_state(self) -> None:
        device = uuid4()
        self.projection.observe(_offline("store-1", device))
        self.projection.observe(_online("store-1", device))
        self.projection.observe(_offline("store-1", device))
        self.assertEqual(self.projection.offline_count("store-1"), 1)

    def test_unrelated_detection_type_is_ignored(self) -> None:
        self.projection.observe(
            _detection(
                DETECTION_TYPE_STORE_OUTAGE, "store-1", uuid4(),
                severity=DetectionSeverity.CRITICAL,
            )
        )
        self.assertEqual(self.projection.offline_count("store-1"), 0)
        self.assertEqual(self.projection.stores_with_offline(), [])

    def test_detection_without_device_id_is_ignored(self) -> None:
        self.projection.observe(
            _detection(DETECTION_TYPE_DEVICE_OFFLINE, "store-1", None)
        )
        self.assertEqual(self.projection.offline_count("store-1"), 0)

    def test_offline_device_ids_returns_the_set(self) -> None:
        d1, d2 = uuid4(), uuid4()
        self.projection.observe(_offline("store-1", d1))
        self.projection.observe(_offline("store-1", d2))
        self.assertEqual(
            self.projection.offline_device_ids("store-1"),
            {str(d1), str(d2)},
        )

    def test_serialised_bytes_are_order_independent(self) -> None:
        d_a, d_c = uuid4(), uuid4()

        store_x = InMemoryProjectionStore()
        proj_x = OfflineCountProjection(store_x)
        proj_x.observe(_offline("store-1", d_c))
        proj_x.observe(_offline("store-1", d_a))

        store_y = InMemoryProjectionStore()
        proj_y = OfflineCountProjection(store_y)
        proj_y.observe(_offline("store-1", d_a))
        proj_y.observe(_offline("store-1", d_c))

        self.assertEqual(
            store_x.get(OfflineCountProjection.VIEW, "store-1"),
            store_y.get(OfflineCountProjection.VIEW, "store-1"),
        )


if __name__ == "__main__":
    unittest.main()
