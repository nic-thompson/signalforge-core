"""
Tests for signal_forge.dashboards.active_outage_projection.ActiveOutageProjection.

Covers the global current-set fold and, centrally, its idempotency under
duplicate delivery — the property the key-per-store-presence layout
preserves (D-17), and partial recovery within a multi-store outage.
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

from signal_forge.dashboards.active_outage_projection import ActiveOutageProjection
from signal_forge.dashboards.projection_store import InMemoryProjectionStore
from signal_forge.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_STORE_OUTAGE,
    DETECTION_TYPE_STORE_RECOVERED,
)

_WINDOW_END = datetime(2026, 4, 30, 12, 5, 0, tzinfo=UTC)


def _detection(
    detection_type: str,
    store_id: str,
    *,
    detected_at: datetime = _WINDOW_END,
    severity: DetectionSeverity = DetectionSeverity.CRITICAL,
) -> DetectionEvent:
    """Build a minimal valid store-level DetectionEvent (device_id=None)."""
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type=detection_type,
        severity=severity,
        detected_at=detected_at,
        store_id=store_id,
        device_id=None,
        source_event_id=uuid4(),
        threshold_breached="test",
        details={},
    )
    return DetectionEvent(
        event_id=uuid4(),
        event_timestamp=detected_at,
        trace=TraceContext(trace_id=uuid4()),
        payload=payload,
    )


def _outage(store_id: str, *, detected_at: datetime = _WINDOW_END) -> DetectionEvent:
    return _detection(
        DETECTION_TYPE_STORE_OUTAGE, store_id,
        detected_at=detected_at, severity=DetectionSeverity.CRITICAL,
    )


def _recovered(store_id: str, *, detected_at: datetime = _WINDOW_END) -> DetectionEvent:
    return _detection(
        DETECTION_TYPE_STORE_RECOVERED, store_id,
        detected_at=detected_at, severity=DetectionSeverity.INFO,
    )


class ActiveOutageProjectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryProjectionStore()
        self.projection = ActiveOutageProjection(self.store)

    def test_cold_start_is_empty(self) -> None:
        self.assertEqual(self.projection.active_outage_count(), 0)
        self.assertEqual(self.projection.stores_in_outage(), [])
        self.assertFalse(self.projection.is_in_outage("store-1"))

    def test_single_outage_counts_one(self) -> None:
        self.projection.observe(_outage("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 1)
        self.assertEqual(self.projection.stores_in_outage(), ["store-1"])
        self.assertTrue(self.projection.is_in_outage("store-1"))

    def test_two_stores_count_two_and_sort(self) -> None:
        self.projection.observe(_outage("store-2"))
        self.projection.observe(_outage("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 2)
        self.assertEqual(
            self.projection.stores_in_outage(), ["store-1", "store-2"]
        )

    def test_outage_then_recovery_returns_to_empty(self) -> None:
        self.projection.observe(_outage("store-1"))
        self.projection.observe(_recovered("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 0)
        self.assertEqual(self.projection.stores_in_outage(), [])
        self.assertFalse(self.projection.is_in_outage("store-1"))

    def test_duplicate_outage_is_idempotent(self) -> None:
        # Redelivered store.outage carries the same detected_at; the
        # point write is last-write-wins with an identical value, so the
        # count does not inflate.
        self.projection.observe(_outage("store-1"))
        self.projection.observe(_outage("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 1)

    def test_recovery_for_never_outaged_store_is_idempotent(self) -> None:
        # A redelivered or out-of-order recovery for a store not currently
        # in outage must not crash or drive the count negative.
        self.projection.observe(_recovered("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 0)
        self.assertEqual(self.projection.stores_in_outage(), [])

    def test_flapping_collapses_to_current_state(self) -> None:
        self.projection.observe(_outage("store-1"))
        self.projection.observe(_recovered("store-1"))
        self.projection.observe(_outage("store-1"))
        self.assertEqual(self.projection.active_outage_count(), 1)
        self.assertTrue(self.projection.is_in_outage("store-1"))

    def test_partial_recovery_in_multi_store_outage(self) -> None:
        self.projection.observe(_outage("store-1"))
        self.projection.observe(_outage("store-2"))
        self.projection.observe(_outage("store-3"))
        self.projection.observe(_recovered("store-2"))
        self.assertEqual(self.projection.active_outage_count(), 2)
        self.assertEqual(
            self.projection.stores_in_outage(), ["store-1", "store-3"]
        )
        self.assertFalse(self.projection.is_in_outage("store-2"))

    def test_unrelated_detection_type_is_ignored(self) -> None:
        self.projection.observe(
            _detection(
                DETECTION_TYPE_DEVICE_OFFLINE, "store-1",
                severity=DetectionSeverity.WARNING,
            )
        )
        self.assertEqual(self.projection.active_outage_count(), 0)
        self.assertEqual(self.projection.stores_in_outage(), [])

    def test_marker_carries_detected_at_provenance(self) -> None:
        entered = datetime(2026, 4, 30, 12, 5, 0, tzinfo=UTC)
        self.projection.observe(_outage("store-1", detected_at=entered))
        self.assertEqual(
            self.store.get(ActiveOutageProjection.VIEW, "store-1"),
            entered.isoformat(),
        )

    def test_re_outage_after_recovery_refreshes_marker(self) -> None:
        first = datetime(2026, 4, 30, 12, 5, 0, tzinfo=UTC)
        later = first + timedelta(seconds=600)
        self.projection.observe(_outage("store-1", detected_at=first))
        self.projection.observe(_recovered("store-1"))
        self.projection.observe(_outage("store-1", detected_at=later))
        self.assertEqual(self.projection.active_outage_count(), 1)
        self.assertEqual(
            self.store.get(ActiveOutageProjection.VIEW, "store-1"),
            later.isoformat(),
        )


if __name__ == "__main__":
    unittest.main()
