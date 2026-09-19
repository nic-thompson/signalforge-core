"""
Schema-per-file forward compatibility across contract versions.

The Phase 4 plan settled on schema-per-file rather than union schemas
(commit 6 design): when an upstream contract evolves v1 -> v1.1, files
written before the bump keep the v1 schema and files written after carry
v1.1. No union schema is computed at write time, and no already-written
file is rewritten. Downstream consumers tolerant of schema variation read
both generations cleanly, unifying at *read* time.

This module proves that property for the detections table:

- The v1 file is real ``serialise_partition`` output.
- The v1.1 file stands in for a future serialiser generation that adds one
  column. Upstream has no v1.1 payload yet (every contract is
  ``SCHEMA_VERSION_V1``), so it is built by appending a column to the real
  v1 table and writing it with the serialiser's pinned Parquet knobs. Built
  *relative to* the real v1 schema, so the test tracks the serialiser's
  actual columns rather than a hand-fixed list that could silently drift.

Asserts:

- the two files carry DIFFERENT schemas; the v1 file is the unmodified
  serialiser output, never rewritten to a union;
- each file reads back alone with its own schema — the v1 reader never
  errors on the absent v1.1 column;
- a schema-tolerant consumer unifies the two at read time
  (``pa.concat_tables(promote_options="default")``), null-filling the new
  column for v1 rows, with neither source file mutated.
"""

from __future__ import annotations

import io
import unittest
from datetime import UTC, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from stream_pipeline.datasets.partition import PartitionKey
from stream_pipeline.datasets.serialisation import serialise_partition
from stream_pipeline.datasets.writer import FlushedPartition
from tests._fixtures.dataset import detection

_KEY = PartitionKey(store_id="store-1", hour=datetime(2026, 5, 29, 14, tzinfo=UTC))

# The serialiser's pinned Parquet knobs (serialisation.py). Replicated here
# so the simulated v1.1 file is written the same way a future serialiser
# generation would write it.
_PARQUET_VERSION = "2.6"
_PARQUET_COMPRESSION = "snappy"
_NEW_COLUMN = "confidence"


def _write(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        version=_PARQUET_VERSION,
        compression=_PARQUET_COMPRESSION,
        use_dictionary=False,
        row_group_size=max(table.num_rows, 1),
    )
    return sink.getvalue().to_pybytes()


def _v1_detection_bytes() -> bytes:
    """Real serialiser output: the v1 detections table for one detection."""
    tables = serialise_partition(
        FlushedPartition(
            partition_key=_KEY,
            emissions=(),
            detections=(detection(store_id="store-1"),),
            features=(),
        )
    )
    (det,) = [t for t in tables if t.record_type == "detections"]
    return det.data


def _v11_from_v1(v1_bytes: bytes) -> bytes:
    """
    A future-generation file: the real v1 table plus one column.

    Defined relative to the real v1 table so the simulated evolution
    tracks whatever columns the serialiser actually writes today.
    """
    v1 = pq.read_table(io.BytesIO(v1_bytes))
    v11 = v1.append_column(
        _NEW_COLUMN, pa.array([0.97] * v1.num_rows, type=pa.float64())
    )
    return _write(v11)


class SchemaPerFileTest(unittest.TestCase):
    def test_files_carry_distinct_schemas_and_v1_is_not_rewritten(self) -> None:
        v1_bytes = _v1_detection_bytes()
        v11_bytes = _v11_from_v1(v1_bytes)

        v1_schema = pq.read_table(io.BytesIO(v1_bytes)).schema
        v11_schema = pq.read_table(io.BytesIO(v11_bytes)).schema

        # Schema-per-file: the new column exists only in the v1.1 file.
        self.assertNotIn(_NEW_COLUMN, v1_schema.names)
        self.assertIn(_NEW_COLUMN, v11_schema.names)

        # The v1.1 generation is a separate file, not the v1 file rewritten
        # to a union schema — distinct bytes, and the v1 file's schema is
        # untouched by building the later generation.
        self.assertNotEqual(v1_bytes, v11_bytes)
        self.assertNotIn(
            _NEW_COLUMN, pq.read_table(io.BytesIO(v1_bytes)).schema.names
        )

    def test_each_file_reads_alone_with_its_own_schema(self) -> None:
        v1_bytes = _v1_detection_bytes()
        v11_bytes = _v11_from_v1(v1_bytes)

        # The v1 reader never errors on the absent v1.1 column.
        v1 = pq.read_table(io.BytesIO(v1_bytes))
        self.assertEqual(v1.num_rows, 1)
        self.assertNotIn(_NEW_COLUMN, v1.schema.names)

        v11 = pq.read_table(io.BytesIO(v11_bytes))
        self.assertEqual(v11.num_rows, 1)
        self.assertAlmostEqual(v11.column(_NEW_COLUMN)[0].as_py(), 0.97)

    def test_tolerant_reader_unifies_partition_at_read_time(self) -> None:
        v1_bytes = _v1_detection_bytes()
        v11_bytes = _v11_from_v1(v1_bytes)

        v1 = pq.read_table(io.BytesIO(v1_bytes))
        v11 = pq.read_table(io.BytesIO(v11_bytes))

        # A downstream consumer unifies the two generations at read time.
        # promote_options="default" null-fills columns absent from a file.
        combined = pa.concat_tables([v1, v11], promote_options="default")

        self.assertEqual(combined.num_rows, 2)
        self.assertIn(_NEW_COLUMN, combined.schema.names)

        confidence = combined.column(_NEW_COLUMN).to_pylist()
        # v1 row first (null), then v1.1 row (present).
        self.assertIsNone(confidence[0])
        self.assertAlmostEqual(confidence[1], 0.97)

    def test_read_time_union_does_not_mutate_source_files(self) -> None:
        v1_bytes = _v1_detection_bytes()
        v11_bytes = _v11_from_v1(v1_bytes)

        before = pq.read_table(io.BytesIO(v1_bytes)).schema.names
        pa.concat_tables(
            [pq.read_table(io.BytesIO(v1_bytes)), pq.read_table(io.BytesIO(v11_bytes))],
            promote_options="default",
        )
        after = pq.read_table(io.BytesIO(v1_bytes)).schema.names

        # The union is a read-time view; the source bytes are untouched.
        self.assertEqual(before, after)
        self.assertNotIn(_NEW_COLUMN, after)


if __name__ == "__main__":
    unittest.main()
