"""
Phase 7 determinism integration test — the headline.

The property the whole project was built to make true: a replay of an archived
event sequence reproduces the original run's output across the *entire control
plane*, byte-for-byte, in replay-isolated sinks that never touch live data.
This is the test the definition of done calls out (DDIA Chapter 11/12
reprocessing).

It composes the two single-sink isolation tests into one whole-control-plane
proof:

- tests/datasets/test_replay_isolation.py proved the S3DatasetWriter, which is
  registered *on* the pipeline, reproduces byte-identical Parquet across live
  and replay buckets;
- tests/dashboards/test_replay_isolation.py proved the projection store, fed
  *beside* the pipeline by route_detections, reproduces identical rows across
  live and replay tables.

Phase 7 wires *both* into one run driven through ``run_replay`` and asserts
both reproduce at once. The two sinks reach their replay isolation by the two
different mechanisms D-5 created, and that asymmetry is the point:

- The dataset writer is registered on the pipeline by ``build_pipeline``.
  ``run_replay`` calls ``build_pipeline(settings.for_replay())`` internally, so
  the writer reads the already-swapped ``dataset_bucket`` — replay isolation
  happens *inside* the driver, via the builder.
- The projections are routed by the caller, after ``run_replay`` returns its
  results. The test builds the replay projections from ``for_replay()``
  settings itself, pointing them at the replay table — replay isolation happens
  *outside* the driver, in the routing step.

Both end up isolated; proving both reproduce in a single driver-run is what
elevates this from "a sink is isolated" to "the control plane reproduces".

The fixed event sequence, detector trick, and watermark math mirror the two
sibling tests exactly: a ``signal_value`` count aggregation watched by
``AnomalyDetector(threshold=0.5)``, and a two-event sequence (t=102 opens
window [100, 105); t=165 closes it) firing one ``signal.anomaly`` whose
``detection_id`` is UUIDv5-derived and therefore identical across runs. That
determinism is what makes both the Parquet bytes and the projection row
reproducible.
"""

from __future__ import annotations

import unittest
from collections.abc import Iterable
from uuid import UUID

import boto3
from moto import mock_aws

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.dashboards.anomaly_rate_projection import AnomalyRateProjection
from signal_forge.dashboards.dynamodb_projection_store import DynamoDbProjectionStore
from signal_forge.dashboards.routing import route_detections
from signal_forge.datasets.s3_writer import S3DatasetWriter
from signal_forge.detection.detectors import AnomalyDetector
from signal_forge.replay.driver import run_replay
from signal_forge.streaming.realtime_pipeline import ProcessingResult, RealtimePipeline
from signal_forge.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, FakeTrace
from tests.streaming.test_realtime_pipeline import epoch_aligned, make_pipeline

_LIVE_BUCKET = "sf-live-dataset"
_REPLAY_BUCKET = "sf-replay-dataset"
_LIVE_TABLE = "sf-live-projections"
_REPLAY_TABLE = "sf-replay-projections"
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
class ReplayControlPlaneDeterminismTest(unittest.TestCase):
    def setUp(self) -> None:
        self.s3 = boto3.client("s3", region_name="us-east-1")
        self.s3.create_bucket(Bucket=_LIVE_BUCKET)
        self.s3.create_bucket(Bucket=_REPLAY_BUCKET)
        self.ddb = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(self.ddb, _LIVE_TABLE)
        _create_table(self.ddb, _REPLAY_TABLE)
        # One fixed sequence, fed to both runs — the archive replays identical
        # events. t=102 opens window [100, 105); t=165 closes it, firing one
        # anomaly whose UUIDv5 detection_id is identical across runs.
        self.events = [_event(102), _event(165)]

    # -- the production-shaped builder: the dataset writer, on the pipeline ---
    def _build_pipeline(self, settings: PlatformSettings) -> RealtimePipeline:
        pipeline, *_ = make_pipeline()
        pipeline.register_aggregator(
            "signal_value",
            WindowAggregator(
                spec=WindowSpec(size_seconds=5, slide_seconds=5),
                aggregation=CountAggregation(name="signal_value"),
                lateness_tolerance_seconds=60,
            ),
        )
        pipeline.register_emission_detector(AnomalyDetector(threshold=0.5))
        pipeline.register_dataset_writer(
            S3DatasetWriter(settings=settings, client_factory=lambda: self.s3)
        )
        return pipeline

    # -- the projections, routed beside the pipeline by the caller -----------
    def _route_to_projections(
        self, results: Iterable[ProcessingResult], settings: PlatformSettings
    ) -> None:
        store = DynamoDbProjectionStore(
            settings=settings, client_factory=lambda: self.ddb
        )
        projection = AnomalyRateProjection(store)
        for result in results:
            route_detections(result, [projection])

    def _run_live(self, settings: PlatformSettings) -> None:
        # Live: build with the live settings directly, drive process_batch,
        # route the results to live-table projections. This is what a
        # production live run does — no driver, no for_replay.
        pipeline = self._build_pipeline(settings)
        results = pipeline.process_batch(self.events)
        self._route_to_projections(results, settings)

    def _run_replay(self, settings: PlatformSettings) -> None:
        # Replay: the driver builds with settings.for_replay() internally (so
        # the dataset writer is replay-isolated inside the driver), and the
        # caller routes the returned results to projections built from
        # for_replay() settings (so the projection store is replay-isolated
        # outside the driver). Both mechanisms, one run.
        results = run_replay(
            settings,
            event_source=self.events,
            build_pipeline=self._build_pipeline,
        )
        self._route_to_projections(results, settings.for_replay())

    # -- probes --------------------------------------------------------------
    def _s3_keys(self, bucket: str) -> list[str]:
        response = self.s3.list_objects_v2(Bucket=bucket)
        return sorted(obj["Key"] for obj in response.get("Contents", []))

    def _s3_body(self, bucket: str, key: str) -> bytes:
        return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def _ddb_items(self, table: str) -> list[tuple[str, str, str]]:
        response = self.ddb.scan(TableName=table)
        return sorted(
            (item["view"]["S"], item["key"]["S"], item["value"]["S"])
            for item in response.get("Items", [])
        )

    # -- tests ---------------------------------------------------------------
    def test_replay_reproduces_dataset_parquet_byte_for_byte(self) -> None:
        settings = PlatformSettings(
            dataset_bucket=_LIVE_BUCKET,
            replay_dataset_bucket=_REPLAY_BUCKET,
            projection_table=_LIVE_TABLE,
            replay_projection_table=_REPLAY_TABLE,
        )
        self._run_live(settings)
        self._run_replay(settings)

        live_keys = self._s3_keys(_LIVE_BUCKET)
        replay_keys = self._s3_keys(_REPLAY_BUCKET)

        # Non-vacuous: the run produced all three dataset record types.
        roots = {k.split("/store_id=")[0] for k in live_keys}
        self.assertIn("detections", roots)
        self.assertIn("features", roots)
        self.assertTrue(any(r.startswith("emissions/") for r in roots))

        # Same keys, identical bytes — the dataset half of the control plane
        # reproduces through the driver.
        self.assertEqual(live_keys, replay_keys)
        self.assertNotEqual(live_keys, [])
        for key in live_keys:
            self.assertEqual(
                self._s3_body(_LIVE_BUCKET, key),
                self._s3_body(_REPLAY_BUCKET, key),
                f"dataset byte mismatch for {key}",
            )

    def test_replay_reproduces_projection_rows(self) -> None:
        settings = PlatformSettings(
            dataset_bucket=_LIVE_BUCKET,
            replay_dataset_bucket=_REPLAY_BUCKET,
            projection_table=_LIVE_TABLE,
            replay_projection_table=_REPLAY_TABLE,
        )
        self._run_live(settings)
        self._run_replay(settings)

        live_items = self._ddb_items(_LIVE_TABLE)
        replay_items = self._ddb_items(_REPLAY_TABLE)

        # Non-vacuous: the anomaly fired and wrote a row.
        self.assertNotEqual(live_items, [])
        # The projection half reproduces: identical (view, key, value) rows.
        self.assertEqual(live_items, replay_items)

    def test_replay_leaves_live_sinks_untouched(self) -> None:
        # A replay-only run writes to neither the live bucket nor the live
        # table — the isolation guarantee, across both sinks at once.
        settings = PlatformSettings(
            dataset_bucket=_LIVE_BUCKET,
            replay_dataset_bucket=_REPLAY_BUCKET,
            projection_table=_LIVE_TABLE,
            replay_projection_table=_REPLAY_TABLE,
        )
        self._run_replay(settings)

        self.assertEqual(self._s3_keys(_LIVE_BUCKET), [])
        self.assertEqual(self._ddb_items(_LIVE_TABLE), [])
        self.assertNotEqual(self._s3_keys(_REPLAY_BUCKET), [])
        self.assertNotEqual(self._ddb_items(_REPLAY_TABLE), [])


if __name__ == "__main__":
    unittest.main()
