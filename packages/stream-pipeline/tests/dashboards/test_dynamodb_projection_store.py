"""
Tests for stream_pipeline.dashboards.dynamodb_projection_store.

moto-backed (the @mock_aws decorator + boto3 DynamoDB client, matching the
shape of tests/datasets/test_s3_writer.py and the EventBridge sink tests).
Covers:

- no-op when no table is configured (no client built, ops do nothing)
- put/get round-trip of an opaque string value, and cold-start None
- delete removes an item
- keys(view) returns the view's keys, sorted, isolated from other views
- anomaly_rate items carry a ttl attribute = bucket_time + retention
- current-state views (offline_count, active_outage) carry no ttl attribute
- a projection folds correctly when backed by the DynamoDB store, proving
  InMemory and DynamoDB are interchangeable behind the protocol
"""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)
from moto import mock_aws

from stream_pipeline.config.platform_settings import PlatformSettings
from stream_pipeline.dashboards.anomaly_rate_projection import AnomalyRateProjection
from stream_pipeline.dashboards.dynamodb_projection_store import (
    _DEFAULT_ANOMALY_RATE_TTL_SECONDS,
    DynamoDbProjectionStore,
)
from stream_pipeline.dashboards.offline_count_projection import OfflineCountProjection
from stream_pipeline.detection.types import DETECTION_TYPE_DEVICE_OFFLINE

_TABLE = "sf-projections"
_REGION = "us-east-1"


def _create_table(client) -> None:
    client.create_table(
        TableName=_TABLE,
        KeySchema=[
            {"AttributeName": "view", "KeyType": "HASH"},
            {"AttributeName": "key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "view", "AttributeType": "S"},
            {"AttributeName": "key", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    client.update_time_to_live(
        TableName=_TABLE,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
    )


def _offline_detection(store_id: str, device_id) -> DetectionEvent:
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type=DETECTION_TYPE_DEVICE_OFFLINE,
        severity=DetectionSeverity.WARNING,
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


class NoTableNoOpTest(unittest.TestCase):
    def test_no_table_builds_no_client_and_ops_no_op(self) -> None:
        factory_calls = 0

        def factory():
            nonlocal factory_calls
            factory_calls += 1
            return object()

        store = DynamoDbProjectionStore(
            settings=PlatformSettings(projection_table=None),
            client_factory=factory,
        )
        # No client built.
        self.assertEqual(factory_calls, 0)
        # Reads are cold-start; writes do not raise.
        self.assertIsNone(store.get("offline_count", "store-1"))
        self.assertEqual(store.keys("offline_count"), [])
        store.put("offline_count", "store-1", "[]")
        store.delete("offline_count", "store-1")


@mock_aws
class DynamoDbStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = boto3.client("dynamodb", region_name=_REGION)
        _create_table(self.client)
        self.store = DynamoDbProjectionStore(
            settings=PlatformSettings(projection_table=_TABLE),
            client_factory=lambda: self.client,
        )

    def test_put_get_round_trip(self) -> None:
        value = json.dumps(["dev-a", "dev-b"])
        self.store.put("offline_count", "store-1", value)
        self.assertEqual(self.store.get("offline_count", "store-1"), value)

    def test_get_missing_returns_none(self) -> None:
        self.assertIsNone(self.store.get("offline_count", "absent"))

    def test_delete_removes_item(self) -> None:
        self.store.put("offline_count", "store-1", "[]")
        self.store.delete("offline_count", "store-1")
        self.assertIsNone(self.store.get("offline_count", "store-1"))

    def test_keys_returns_sorted_view_isolated_keys(self) -> None:
        self.store.put("active_outage", "store-3", "t")
        self.store.put("active_outage", "store-1", "t")
        self.store.put("active_outage", "store-2", "t")
        self.store.put("offline_count", "store-9", "[]")
        self.assertEqual(
            self.store.keys("active_outage"), ["store-1", "store-2", "store-3"]
        )
        self.assertEqual(self.store.keys("offline_count"), ["store-9"])
        self.assertEqual(self.store.keys("anomaly_rate"), [])

    def test_anomaly_rate_item_carries_ttl_from_bucket_time(self) -> None:
        bucket_iso = "2026-04-30T12:00:00+00:00"
        key = f"latency|{bucket_iso}"
        self.store.put(AnomalyRateProjection.VIEW, key, json.dumps(["id1"]))

        raw = self.client.get_item(
            TableName=_TABLE,
            Key={"view": {"S": AnomalyRateProjection.VIEW}, "key": {"S": key}},
        )["Item"]
        expected = (
            int(datetime.fromisoformat(bucket_iso).timestamp())
            + _DEFAULT_ANOMALY_RATE_TTL_SECONDS
        )
        self.assertIn("ttl", raw)
        self.assertEqual(raw["ttl"]["N"], str(expected))

    def test_current_state_views_carry_no_ttl(self) -> None:
        self.store.put("offline_count", "store-1", "[]")
        self.store.put("active_outage", "store-2", "t")
        for view, key in (("offline_count", "store-1"), ("active_outage", "store-2")):
            raw = self.client.get_item(
                TableName=_TABLE,
                Key={"view": {"S": view}, "key": {"S": key}},
            )["Item"]
            self.assertNotIn("ttl", raw)

    def test_projection_folds_correctly_over_dynamo_store(self) -> None:
        # The point of the protocol: a projection backed by DynamoDB behaves
        # identically to one backed by InMemory. Fold a device.offline and
        # read the gauge back through the store's get + keys.
        projection = OfflineCountProjection(self.store)
        device = uuid4()
        projection.observe(_offline_detection("store-1", device))
        self.assertEqual(projection.offline_count("store-1"), 1)
        self.assertEqual(projection.stores_with_offline(), ["store-1"])
        self.assertEqual(projection.offline_device_ids("store-1"), {str(device)})


if __name__ == "__main__":
    unittest.main()
