"""
Tests for signal_forge.datasets.s3_writer.

moto-backed (the @mock_aws decorator + boto3 S3 client, matching the
commit-2 smoke test). Covers:

- no-op when no bucket is configured (no client built, write() does
  nothing)
- a window-close flush uploads one Parquet object per serialised table,
  round-tripping through pyarrow
- object keys are Hive-partitioned with record-type / aggregation roots
- repair emissions write a second object under the same prefix with the
  next sequence number
- detections, emissions and features land under their own table roots
- separate stores write under separate store_id partitions
- object keys are independent of the per-event uuid4 identity fields
"""

from __future__ import annotations

import io
import unittest
from datetime import UTC, datetime

import boto3
import pyarrow.parquet as pq
from moto import mock_aws

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.datasets.s3_writer import S3DatasetWriter
from tests._fixtures.dataset import detection, emission, feature, result

_BUCKET = "sf-live-dataset"
_W_START = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
_W_END = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)


def _keys(client, bucket: str) -> list[str]:
    response = client.list_objects_v2(Bucket=bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def _read_parquet(client, bucket: str, key: str):
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


class NoBucketNoOpTest(unittest.TestCase):
    def test_no_bucket_builds_no_client_and_writes_nothing(self) -> None:
        factory_calls = 0

        def factory():
            nonlocal factory_calls
            factory_calls += 1
            return object()

        writer = S3DatasetWriter(
            settings=PlatformSettings(dataset_bucket=None),
            client_factory=factory,
        )
        # A window-closing result must not raise and must not touch a client.
        writer.write(result(emissions=[emission(value=3)]))
        self.assertEqual(factory_calls, 0)


@mock_aws
class S3UploadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = boto3.client("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket=_BUCKET)
        self.writer = S3DatasetWriter(
            settings=PlatformSettings(dataset_bucket=_BUCKET),
            client_factory=lambda: self.client,
        )

    def test_emission_flush_uploads_roundtrippable_parquet(self) -> None:
        self.writer.write(result(emissions=[emission(aggregation_name="count", value=47)]))

        keys = _keys(self.client, _BUCKET)
        self.assertEqual(len(keys), 1)
        table = _read_parquet(self.client, _BUCKET, keys[0])
        self.assertEqual(table.to_pylist()[0]["value"], 47)

    def test_object_key_is_hive_partitioned_with_table_root(self) -> None:
        self.writer.write(result(emissions=[emission(aggregation_name="count")]))
        (key,) = _keys(self.client, _BUCKET)
        self.assertEqual(
            key,
            "emissions/count/store_id=store-1/"
            "year=2026/month=05/day=29/hour=14/00000000.parquet",
        )

    def test_repair_emission_writes_second_object_with_next_seq(self) -> None:
        # Original close, then a repair for the same window in a later result.
        self.writer.write(result(emissions=[emission(aggregation_name="count", value=1)]))
        self.writer.write(
            result(emissions=[emission(aggregation_name="count", value=2, is_repair=True)])
        )
        keys = _keys(self.client, _BUCKET)
        prefix = "emissions/count/store_id=store-1/year=2026/month=05/day=29/hour=14"
        self.assertEqual(
            keys,
            [f"{prefix}/00000000.parquet", f"{prefix}/00000001.parquet"],
        )

    def test_record_types_land_under_separate_roots(self) -> None:
        # One result carrying an emission (triggers the flush) plus a
        # detection and a feature buffered for the same partition.
        self.writer.write(
            result(
                emissions=[emission(aggregation_name="count")],
                detections=[detection(store_id="store-1")],
                features=[feature(partition_key="store-1")],
            )
        )
        roots = {key.split("/store_id=")[0] for key in _keys(self.client, _BUCKET)}
        self.assertEqual(roots, {"emissions/count", "detections", "features"})

    def test_separate_stores_write_under_separate_partitions(self) -> None:
        self.writer.write(
            result(
                emissions=[
                    emission(partition_key="store-1"),
                    emission(partition_key="store-2"),
                ]
            )
        )
        stores = {
            key.split("store_id=")[1].split("/")[0]
            for key in _keys(self.client, _BUCKET)
        }
        self.assertEqual(stores, {"store-1", "store-2"})

    def test_object_keys_independent_of_uuid_identity(self) -> None:
        # Two detections differing only in their uuid4 identity fields
        # produce the same object key — keys derive from partition + table
        # identity + deterministic sequence, never the random ids.
        self.writer.write(result(emissions=[emission()], detections=[detection()]))
        first = [k for k in _keys(self.client, _BUCKET) if k.startswith("detections/")]

        # Fresh bucket + writer, a different detection instance.
        self.client.create_bucket(Bucket="sf-live-two")
        writer2 = S3DatasetWriter(
            settings=PlatformSettings(dataset_bucket="sf-live-two"),
            client_factory=lambda: self.client,
        )
        writer2.write(result(emissions=[emission()], detections=[detection()]))
        second = [
            k for k in _keys(self.client, "sf-live-two") if k.startswith("detections/")
        ]
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
