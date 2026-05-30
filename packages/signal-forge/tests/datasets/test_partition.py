"""
Tests for signal_forge.datasets.partition.

Covers:

- PartitionKey field shape, hashability, equality
- _truncate_to_hour: strips sub-hour components, preserves UTC
- partition_key_from_emission: uses window_start
- partition_key_from_detection: uses event_timestamp, payload.store_id
- partition_key_from_feature: uses payload.window_start, payload.partition_key
- hour-boundary collapse: windows in the same hour produce identical keys
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from uuid import uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
    WindowedFeatureVectorPayload,
)

from signal_forge.datasets import (
    PartitionKey,
    partition_key_from_detection,
    partition_key_from_emission,
    partition_key_from_feature,
)
from signal_forge.datasets.partition import _truncate_to_hour
from signal_forge.streaming.window_aggregator import WindowEmission

# Reference timestamps inside the 14:00-15:00 UTC hour.
_T_14_15_30 = datetime(2026, 5, 29, 14, 15, 30, tzinfo=UTC)
_T_14_00_00 = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
_T_14_59_59 = datetime(2026, 5, 29, 14, 59, 59, tzinfo=UTC)


class PartitionKeyTest(unittest.TestCase):
    def test_partition_key_is_hashable(self) -> None:
        key = PartitionKey(store_id="store-1", hour=_T_14_00_00)
        # Insertion into a set requires hashability.
        self.assertEqual({key}, {key})

    def test_partition_key_equality(self) -> None:
        a = PartitionKey(store_id="store-1", hour=_T_14_00_00)
        b = PartitionKey(store_id="store-1", hour=_T_14_00_00)
        self.assertEqual(a, b)

    def test_partition_key_distinguishes_stores(self) -> None:
        a = PartitionKey(store_id="store-1", hour=_T_14_00_00)
        b = PartitionKey(store_id="store-2", hour=_T_14_00_00)
        self.assertNotEqual(a, b)


class TruncateToHourTest(unittest.TestCase):
    def test_strips_minutes_seconds_microseconds(self) -> None:
        truncated = _truncate_to_hour(
            datetime(2026, 5, 29, 14, 15, 30, 123456, tzinfo=UTC)
        )
        self.assertEqual(truncated, datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC))

    def test_preserves_utc_timezone(self) -> None:
        truncated = _truncate_to_hour(_T_14_15_30)
        self.assertEqual(truncated.tzinfo, UTC)


class PartitionKeyFromEmissionTest(unittest.TestCase):
    def test_uses_window_start(self) -> None:
        emission = WindowEmission(
            partition_key="store-1",
            aggregation_name="count",
            window_start=_T_14_15_30,
            window_end=datetime(2026, 5, 29, 14, 15, 35, tzinfo=UTC),
            value=42,
            event_count=42,
            is_repair=False,
        )
        key = partition_key_from_emission(emission)
        self.assertEqual(
            key,
            PartitionKey(store_id="store-1", hour=_T_14_00_00),
        )


class PartitionKeyFromDetectionTest(unittest.TestCase):
    def test_uses_event_timestamp_and_payload_store_id(self) -> None:
        payload = DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            detected_at=_T_14_15_30,
            store_id="store-7",
            source_event_id=uuid4(),
            threshold_breached="silent_seconds > 300",
            details={},
        )
        detection = DetectionEvent(
            event_timestamp=_T_14_15_30,
            trace=TraceContext(),
            payload=payload,
        )
        key = partition_key_from_detection(detection)
        self.assertEqual(
            key,
            PartitionKey(store_id="store-7", hour=_T_14_00_00),
        )


class PartitionKeyFromFeatureTest(unittest.TestCase):
    def test_uses_payload_window_start_and_payload_partition_key(self) -> None:
        payload = WindowedFeatureVectorPayload(
            partition_key="store-3",
            window_start=_T_14_15_30,
            window_end=datetime(2026, 5, 29, 14, 15, 35, tzinfo=UTC),
            feature_values={"count": 10},
            feature_version="v1",
        )
        feature = WindowedFeatureVectorEvent(
            event_timestamp=_T_14_15_30,
            trace=TraceContext(),
            payload=payload,
        )
        key = partition_key_from_feature(feature)
        self.assertEqual(
            key,
            PartitionKey(store_id="store-3", hour=_T_14_00_00),
        )


class HourBoundaryCollapseTest(unittest.TestCase):
    """
    Two emissions starting at different sub-hour positions within the
    same hour produce identical PartitionKeys. This is the property the
    dataset layer relies on to batch records per hourly partition.
    """

    def test_emissions_in_same_hour_collapse(self) -> None:
        early = WindowEmission(
            partition_key="store-1",
            aggregation_name="count",
            window_start=_T_14_00_00,
            window_end=datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC),
            value=1,
            event_count=1,
            is_repair=False,
        )
        late = WindowEmission(
            partition_key="store-1",
            aggregation_name="count",
            window_start=_T_14_59_59,
            window_end=datetime(2026, 5, 29, 15, 0, 4, tzinfo=UTC),
            value=2,
            event_count=2,
            is_repair=False,
        )
        self.assertEqual(
            partition_key_from_emission(early),
            partition_key_from_emission(late),
        )


if __name__ == "__main__":
    unittest.main()
