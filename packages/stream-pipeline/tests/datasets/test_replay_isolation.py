"""
Phase 4 replay-isolation / byte-identity test.

The project's headline property: a replay of an archived event sequence
reproduces the original run's dataset output *byte-for-byte*, written to a
replay-isolated bucket so it never touches live data. This is the test the
definition of done calls out, and the reason the identity fields were made
deterministic (commit ``4d3e34c``): with ``detection_id``,
``source_event_id`` and the envelope ``event_id`` derived via UUIDv5 rather
than minted with ``uuid4``, the serialised Parquet no longer carries
per-run randomness, so live and replay outputs are identical bytes.

The test drives one fixed event sequence through two pipelines:

- a *live* pipeline whose ``S3DatasetWriter`` reads ``dataset_bucket`` and
  writes to the live bucket;
- a *replay* pipeline built from ``settings.for_replay()``, which swaps the
  active bucket to ``replay_dataset_bucket`` — the writer is
  replay-oblivious, it just reads ``dataset_bucket``.

Feeding the *same* event objects to both runs models replay faithfully: the
EventBridge archive replays the original events unchanged, so their
``event_id`` / ``trace_id`` are preserved across runs exactly as reusing the
objects here preserves them.

A real detector is wired (not the ``uuid4``-minting test fixtures in
``tests/_fixtures/dataset.py``, which cannot exercise byte-identity) so the
derived identity fields are what land in the detections and features tables.

Watermark math mirrors the other integration tests: 5-second tumbling
window, 60-second lateness, so an event at t=165 advances the watermark to
105 and closes window [100, 105) opened by the t=102 event.
"""

from __future__ import annotations

import io
import unittest
from uuid import UUID

import boto3
import pyarrow.parquet as pq
from moto import mock_aws

from stream_pipeline.config.platform_settings import PlatformSettings
from stream_pipeline.datasets.s3_writer import S3DatasetWriter
from stream_pipeline.detection.detectors import AnomalyDetector
from stream_pipeline.streaming.window_aggregator import (
    CountAggregation,
    WindowAggregator,
    WindowSpec,
)
from tests._fixtures.events import FakeEvent, FakeTrace
from tests.streaming.test_realtime_pipeline import epoch_aligned, make_pipeline

_LIVE = "sf-live-dataset"
_REPLAY = "sf-replay-dataset"
# Fixed trace so the serialised trace_id columns are stable across runs,
# exactly as an archived event's trace is stable across a replay.
_TRACE = UUID("11111111-1111-4111-8111-111111111111")


def _event(*, source: str, seconds: int) -> FakeEvent:
    return FakeEvent(
        event_type="device.registration",
        schema_version="v1",
        event_timestamp=epoch_aligned(seconds),
        source=source,
        trace=FakeTrace(trace_id=_TRACE),
    )


def _keys(client, bucket: str) -> list[str]:
    response = client.list_objects_v2(Bucket=bucket)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def _body(client, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


@mock_aws
class ReplayByteIdentityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = boto3.client("s3", region_name="us-east-1")
        self.client.create_bucket(Bucket=_LIVE)
        self.client.create_bucket(Bucket=_REPLAY)
        # One fixed sequence: t=102 opens window [100, 105); t=165 advances
        # the watermark to 105 and closes it. Built once and fed to both
        # runs — the archive replays identical events.
        self.events = [
            _event(source="store-1", seconds=102),
            _event(source="store-1", seconds=165),
        ]

    def _run(self, settings: PlatformSettings) -> None:
        # make_pipeline registers a "count" aggregator; add a "signal_value"
        # aggregation an AnomalyDetector can watch so a detection is produced
        # alongside the emissions and bundled feature event. All three record
        # types therefore exercise the byte-identity comparison.
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
        pipeline.register_dataset_writer(
            S3DatasetWriter(settings=settings, client_factory=lambda: self.client)
        )
        pipeline.process_batch(self.events)

    def test_live_and_replay_produce_byte_identical_parquet(self) -> None:
        settings = PlatformSettings(
            dataset_bucket=_LIVE, replay_dataset_bucket=_REPLAY
        )

        self._run(settings)               # live -> _LIVE
        self._run(settings.for_replay())  # replay -> _REPLAY (bucket swapped)

        live_keys = _keys(self.client, _LIVE)
        replay_keys = _keys(self.client, _REPLAY)

        # The run must actually have produced all three record types, or the
        # byte comparison below would be vacuously true on an empty bucket.
        roots = {k.split("/store_id=")[0] for k in live_keys}
        self.assertIn("detections", roots)
        self.assertIn("features", roots)
        self.assertTrue(any(r.startswith("emissions/") for r in roots))

        # Replay isolation: nothing leaked into the live bucket on the replay
        # run and vice versa — each run wrote only to its own bucket, and the
        # two key sets match.
        self.assertEqual(live_keys, replay_keys)
        self.assertNotEqual(live_keys, [])

        # The headline assertion: identical object bytes per key.
        for key in live_keys:
            self.assertEqual(
                _body(self.client, _LIVE, key),
                _body(self.client, _REPLAY, key),
                f"byte mismatch for {key}",
            )

    def test_replay_run_does_not_write_to_live_bucket(self) -> None:
        # A replay run with the bucket swapped must leave the live bucket
        # untouched when only the replay run executes.
        settings = PlatformSettings(
            dataset_bucket=_LIVE, replay_dataset_bucket=_REPLAY
        ).for_replay()
        self._run(settings)
        self.assertEqual(_keys(self.client, _LIVE), [])
        self.assertNotEqual(_keys(self.client, _REPLAY), [])

    def test_derived_ids_are_present_and_v5(self) -> None:
        # Guards the premise: the detection row carries UUIDv5 identity
        # fields (not uuid4), which is what makes the bytes reproducible.
        self._run(PlatformSettings(dataset_bucket=_LIVE))
        (det_key,) = [
            k for k in _keys(self.client, _LIVE) if k.startswith("detections/")
        ]
        table = pq.read_table(io.BytesIO(_body(self.client, _LIVE, det_key)))
        row = table.to_pylist()[0]
        self.assertEqual(UUID(row["detection_id"]).version, 5)
        self.assertEqual(UUID(row["source_event_id"]).version, 5)
        self.assertEqual(UUID(row["event_id"]).version, 5)


if __name__ == "__main__":
    unittest.main()
