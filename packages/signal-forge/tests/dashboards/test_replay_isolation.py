"""
Phase 6 replay-isolation test for dashboard projections.

The sibling of tests/datasets/test_replay_isolation.py, adapted to the
projection path. A replay of an archived event sequence must update the
materialised views in a *replay-isolated* DynamoDB table, never touching the
live table — the Chapter 11 reprocessing discipline: re-derive history
without corrupting the present.

The structural difference from the datasets sibling is the whole point. There
the S3DatasetWriter is registered on the pipeline, so for_replay() had to swap
a bucket inside a writer the pipeline owns. Projections are deliberately not on
the pipeline (D-5): they are fed by route_detections beside it. So replay
isolation here is just "point the replay run's projections at a store built
from for_replay() settings" — the caller routes, the store reads
projection_table, and the swap (projection_table <- replay_projection_table)
does the rest. The store is replay-oblivious.

The detector trick mirrors the datasets test: a signal_value aggregation plus
AnomalyDetector(threshold=0.5), and a two-event sequence that closes window
[100, 105) with count >= 1, firing one signal.anomaly detection. Its
detection_id is UUIDv5-derived (deterministic over partition_key / signal_name
/ window), so the persisted anomaly-rate value is byte-identical across the
live and replay runs — the projection-layer echo of the datasets byte-identity
assertion.
"""

from __future__ import annotations

import unittest
from uuid import UUID

import boto3
from moto import mock_aws

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.dashboards.anomaly_rate_projection import AnomalyRateProjection
from signal_forge.dashboards.dynamodb_projection_store import DynamoDbProjectionStore
from signal_forge.dashboards.routing import route_detections
from signal_forge.detection.detectors import AnomalyDetector
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, FakeTrace
from tests.streaming.test_realtime_pipeline import epoch_aligned, make_pipeline

_LIVE = "sf-live-projections"
_REPLAY = "sf-replay-projections"
# Fixed trace, mirroring the datasets sibling: an archived event's trace is
# stable across a replay. (Irrelevant to the persisted value here, since the
# anomaly-rate item holds only the derived detection_id, but kept for
# faithfulness to the template.)
_TRACE = UUID("11111111-1111-4111-8111-111111111111")


def _event(seconds: int) -> FakeEvent:
    return FakeEvent(
        event_type="device.registration",
        schema_version="v1",
        event_timestamp=epoch_aligned(seconds),
        source="store-1",
        trace=FakeTrace(trace_id=_TRACE),
    )


def _create_table(client, name: str) -> None:
    client.create_table(
        TableName=name,
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
        TableName=name,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
    )


@mock_aws
class ProjectionReplayIsolationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(self.client, _LIVE)
        _create_table(self.client, _REPLAY)
        # One fixed sequence: t=102 opens window [100, 105); t=165 advances the
        # watermark to 105 and closes it, firing one anomaly. Fed to both runs
        # — the archive replays identical events.
        self.events = [_event(102), _event(165)]

    def _items(self, table: str) -> list[tuple[str, str, str]]:
        """Every (view, key, value) in a table, sorted — the isolation probe."""
        response = self.client.scan(TableName=table)
        return sorted(
            (item["view"]["S"], item["key"]["S"], item["value"]["S"])
            for item in response.get("Items", [])
        )

    def _run(self, settings: PlatformSettings) -> None:
        # The replay-oblivious path: a store from these settings, a projection
        # backed by it, and route_detections feeding the pipeline's detections
        # in — the production wiring, not a shortcut.
        store = DynamoDbProjectionStore(
            settings=settings, client_factory=lambda: self.client
        )
        projection = AnomalyRateProjection(store)

        pipeline, *_ = make_pipeline()
        pipeline.register_aggregator(
            "signal_value",
            WindowAggregator(
                spec=WindowSpec(size_seconds=5, slide_seconds=5),
                aggregation=CountAggregation(name="signal_value"),
                lateness_tolerance_seconds=60,
            ),
        )
        # count >= 1 in the closed window clears the 0.5 threshold -> fires.
        pipeline.register_emission_detector(AnomalyDetector(threshold=0.5))

        for result in pipeline.process_batch(self.events):
            route_detections(result, [projection])

    def test_live_and_replay_write_isolated_tables_with_identical_content(self) -> None:
        settings = PlatformSettings(
            projection_table=_LIVE, replay_projection_table=_REPLAY
        )

        self._run(settings)
        # The live run touched only the live table.
        self.assertNotEqual(self._items(_LIVE), [])
        self.assertEqual(self._items(_REPLAY), [])

        self._run(settings.for_replay())
        # Both populated; isolated; identical content (deterministic detection_id).
        live_items = self._items(_LIVE)
        replay_items = self._items(_REPLAY)
        self.assertNotEqual(live_items, [])
        self.assertEqual(live_items, replay_items)
        # Non-vacuous: an anomaly_rate item is actually present.
        self.assertTrue(any(view == "anomaly_rate" for view, _, _ in live_items))

    def test_replay_run_does_not_write_to_live_table(self) -> None:
        settings = PlatformSettings(
            projection_table=_LIVE, replay_projection_table=_REPLAY
        ).for_replay()

        self._run(settings)
        self.assertEqual(self._items(_LIVE), [])
        self.assertNotEqual(self._items(_REPLAY), [])

    def test_replay_with_no_replay_table_writes_nothing(self) -> None:
        # The safety case: a replay run whose replay table was never configured
        # writes nothing, anywhere — never falling through to the live table.
        settings = PlatformSettings(
            projection_table=_LIVE, replay_projection_table=None
        ).for_replay()
        # for_replay() swapped projection_table <- replay_projection_table (None).
        self.assertIsNone(settings.projection_table)

        self._run(settings)
        self.assertEqual(self._items(_LIVE), [])
        self.assertEqual(self._items(_REPLAY), [])


if __name__ == "__main__":
    unittest.main()
