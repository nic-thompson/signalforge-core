"""
Tests for signal_forge.datasets.serialisation.

Covers:

- round-trip per record type (emissions, detections, features)
- schema-per-file: emissions split one table per aggregation_name
- value column type follows the producing aggregation (int vs float)
- empty record types produce no table
- byte-determinism: same input -> identical bytes, repeated and across
  two independently-built equal FlushedPartitions
- row order is independent of input order and of open-map key order
- the returned table list is deterministically ordered
"""

from __future__ import annotations

import io
import unittest
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from signal_forge.datasets.partition import PartitionKey
from signal_forge.datasets.serialisation import (
    SerialisedTable,
    serialise_partition,
)
from signal_forge.datasets.writer import FlushedPartition
from tests._fixtures.dataset import detection, emission, feature

_HOUR_14 = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
_KEY = PartitionKey(store_id="store-1", hour=_HOUR_14)


def _partition(
    *,
    emissions: tuple = (),
    detections: tuple = (),
    features: tuple = (),
) -> FlushedPartition:
    return FlushedPartition(
        partition_key=_KEY,
        emissions=emissions,
        detections=detections,
        features=features,
    )


def _read(table: SerialisedTable) -> pa.Table:
    return pq.read_table(io.BytesIO(table.data))


class RoundTripTest(unittest.TestCase):
    def test_emissions_round_trip(self) -> None:
        e = emission(aggregation_name="count", value=47, event_count=47)
        tables = serialise_partition(_partition(emissions=(e,)))
        self.assertEqual(len(tables), 1)
        t = tables[0]
        self.assertEqual(t.record_type, "emissions")
        self.assertEqual(t.aggregation_name, "count")
        self.assertEqual(t.row_count, 1)
        back = _read(t).to_pylist()[0]
        self.assertEqual(back["partition_key"], "store-1")
        self.assertEqual(back["value"], 47)
        self.assertEqual(back["event_count"], 47)
        self.assertFalse(back["is_repair"])

    def test_detections_round_trip(self) -> None:
        d = detection(store_id="store-1", detection_type="device.offline")
        tables = serialise_partition(_partition(detections=(d,)))
        self.assertEqual(len(tables), 1)
        t = tables[0]
        self.assertEqual(t.record_type, "detections")
        self.assertIsNone(t.aggregation_name)
        back = _read(t).to_pylist()[0]
        self.assertEqual(back["store_id"], "store-1")
        self.assertEqual(back["detection_type"], "device.offline")
        self.assertEqual(back["severity"], "WARNING")
        self.assertEqual(back["source_event_id"], str(d.payload.source_event_id))

    def test_features_round_trip(self) -> None:
        f = feature(feature_values={"count": 3, "mean_latency": 12.5})
        tables = serialise_partition(_partition(features=(f,)))
        self.assertEqual(len(tables), 1)
        t = tables[0]
        self.assertEqual(t.record_type, "features")
        back = _read(t).to_pylist()[0]
        self.assertEqual(back["partition_key"], "store-1")
        self.assertEqual(back["feature_version"], "v1")
        # feature_values is JSON with sorted keys.
        self.assertEqual(back["feature_values"], '{"count":3,"mean_latency":12.5}')


class SchemaPerFileTest(unittest.TestCase):
    def test_emissions_split_by_aggregation_name(self) -> None:
        emissions = (
            emission(aggregation_name="count", value=5),
            emission(aggregation_name="sum", value=10.0),
            emission(aggregation_name="count", value=6),
        )
        tables = serialise_partition(_partition(emissions=emissions))
        self.assertEqual(len(tables), 2)
        names = {t.aggregation_name for t in tables}
        self.assertEqual(names, {"count", "sum"})
        counts = {t.aggregation_name: t.row_count for t in tables}
        self.assertEqual(counts, {"count": 2, "sum": 1})

    def test_count_value_column_is_integer(self) -> None:
        e = emission(aggregation_name="count", value=47)
        (t,) = serialise_partition(_partition(emissions=(e,)))
        schema = _read(t).schema
        self.assertEqual(str(schema.field("value").type), "int64")

    def test_sum_value_column_is_float(self) -> None:
        e = emission(aggregation_name="sum", value=312.5)
        (t,) = serialise_partition(_partition(emissions=(e,)))
        schema = _read(t).schema
        self.assertEqual(str(schema.field("value").type), "double")


class EmptyRecordTypeTest(unittest.TestCase):
    def test_no_detections_produces_no_detections_table(self) -> None:
        tables = serialise_partition(
            _partition(emissions=(emission(),), detections=(), features=())
        )
        self.assertEqual([t.record_type for t in tables], ["emissions"])

    def test_emissions_detections_features_all_present(self) -> None:
        tables = serialise_partition(
            _partition(
                emissions=(emission(),),
                detections=(detection(),),
                features=(feature(),),
            )
        )
        self.assertEqual(
            [t.record_type for t in tables],
            ["detections", "emissions", "features"],
        )


class DeterminismTest(unittest.TestCase):
    def test_same_input_same_bytes(self) -> None:
        e = emission(aggregation_name="count", value=5)
        p = _partition(emissions=(e,))
        first = serialise_partition(p)
        second = serialise_partition(p)
        self.assertEqual(
            [t.data for t in first],
            [t.data for t in second],
        )

    def test_two_equal_partitions_same_bytes(self) -> None:
        # Independently constructed but equal emissions -> identical bytes.
        # No uuid4-bearing record types here, so this is a true cross-run
        # byte-identity check for the emissions table.
        ts0 = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
        ts1 = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)
        e1 = emission(
            aggregation_name="count",
            value=5,
            window_start=ts0,
            window_end=ts1,
            last_contributing_trace_id="trace-abc",
        )
        e2 = emission(
            aggregation_name="count",
            value=5,
            window_start=ts0,
            window_end=ts1,
            last_contributing_trace_id="trace-abc",
        )
        a = serialise_partition(_partition(emissions=(e1,)))
        b = serialise_partition(_partition(emissions=(e2,)))
        self.assertEqual([t.data for t in a], [t.data for t in b])

    def test_row_order_independent_of_input_order(self) -> None:
        ts0 = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
        ts1 = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)
        early = emission(
            aggregation_name="count", value=1, window_start=ts0, window_end=ts1
        )
        late = emission(
            aggregation_name="count", value=2, window_start=ts1, window_end=ts1
        )
        forward = serialise_partition(_partition(emissions=(early, late)))
        reverse = serialise_partition(_partition(emissions=(late, early)))
        self.assertEqual(
            [t.data for t in forward],
            [t.data for t in reverse],
        )

    def test_open_map_encoded_with_sorted_keys(self) -> None:
        # feature_values is JSON-encoded with sorted keys, so the encoded
        # cell is independent of dict insertion order. Asserted on the
        # decoded value rather than on raw bytes: a WindowedFeatureVectorEvent
        # carries a uuid4 event_id, so two independently-constructed feature
        # events can never be byte-identical (the commit-9 identity caveat).
        unsorted_input = feature(feature_values={"mean_latency": 12.5, "count": 3})
        (t,) = serialise_partition(_partition(features=(unsorted_input,)))
        cell = _read(t).to_pylist()[0]["feature_values"]
        self.assertEqual(cell, '{"count":3,"mean_latency":12.5}')

    def test_detections_row_order_uses_stable_columns_only(self) -> None:
        # Two detections that differ only in their (uuid4) identity fields
        # but share all replay-deterministic columns sort to a fixed order,
        # so the row sequence does not depend on the random ids.
        d_early = detection(
            store_id="store-1",
            event_timestamp=datetime(2026, 5, 29, 14, 0, 1, tzinfo=UTC),
        )
        d_late = detection(
            store_id="store-1",
            event_timestamp=datetime(2026, 5, 29, 14, 0, 9, tzinfo=UTC),
        )
        forward = serialise_partition(_partition(detections=(d_early, d_late)))
        reverse = serialise_partition(_partition(detections=(d_late, d_early)))
        order_forward = [
            r["detected_at"] for r in _read(forward[0]).to_pylist()
        ]
        order_reverse = [
            r["detected_at"] for r in _read(reverse[0]).to_pylist()
        ]
        self.assertEqual(order_forward, order_reverse)
        self.assertEqual(order_forward, sorted(order_forward))


if __name__ == "__main__":
    unittest.main()
