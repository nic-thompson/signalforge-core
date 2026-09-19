"""
Tests for stream_pipeline.datasets.writer.

Covers:

- empty/no-emission results buffer without flushing
- emissions flush their partitions
- detections and features buffered until a contemporaneous emission flushes them
- per-partition isolation (records for store-1 don't flush with store-2's emission)
- repair emissions trigger separate flushes
- flush clears the buffer
- multiple emissions for same partition merge into one flush
- deterministic flush order across multiple partitions in one result
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from stream_pipeline.datasets import (
    InMemoryDatasetWriter,
    PartitionKey,
)
from tests._fixtures.dataset import detection, emission, feature, result

_T_14 = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
_T_15 = datetime(2026, 5, 29, 15, 0, 0, tzinfo=UTC)
_STORE_1_HOUR_14 = PartitionKey(store_id="store-1", hour=_T_14)
_STORE_2_HOUR_14 = PartitionKey(store_id="store-2", hour=_T_14)
_STORE_1_HOUR_15 = PartitionKey(store_id="store-1", hour=_T_15)


class InMemoryDatasetWriterEmptyTest(unittest.TestCase):
    def test_empty_result_buffers_nothing(self) -> None:
        writer = InMemoryDatasetWriter()
        writer.write(result())
        self.assertEqual(writer.flushed_records(), [])
        self.assertEqual(writer.buffered_partitions(), set())


class InMemoryDatasetWriterBufferingTest(unittest.TestCase):
    def test_detection_alone_buffers_no_flush(self) -> None:
        writer = InMemoryDatasetWriter()
        writer.write(result(detections=[detection(store_id="store-1")]))
        self.assertEqual(writer.flushed_records(), [])
        self.assertEqual(writer.buffered_partitions(), {_STORE_1_HOUR_14})

    def test_feature_alone_buffers_no_flush(self) -> None:
        writer = InMemoryDatasetWriter()
        writer.write(result(features=[feature(partition_key="store-1")]))
        self.assertEqual(writer.flushed_records(), [])
        self.assertEqual(writer.buffered_partitions(), {_STORE_1_HOUR_14})


class InMemoryDatasetWriterFlushTest(unittest.TestCase):
    def test_emission_alone_flushes(self) -> None:
        writer = InMemoryDatasetWriter()
        emi = emission(partition_key="store-1")
        writer.write(result(emissions=[emi]))

        self.assertEqual(len(writer.flushed_records()), 1)
        flushed = writer.flushed_records()[0]
        self.assertEqual(flushed.partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushed.emissions, (emi,))
        self.assertEqual(flushed.detections, ())
        self.assertEqual(flushed.features, ())

    def test_emission_flushes_buffered_detections(self) -> None:
        writer = InMemoryDatasetWriter()
        det = detection(store_id="store-1")
        emi = emission(partition_key="store-1")
        writer.write(result(detections=[det]))
        writer.write(result(emissions=[emi]))

        self.assertEqual(len(writer.flushed_records()), 1)
        flushed = writer.flushed_records()[0]
        self.assertEqual(flushed.partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushed.detections, (det,))
        self.assertEqual(flushed.emissions, (emi,))

    def test_emission_flushes_buffered_features(self) -> None:
        writer = InMemoryDatasetWriter()
        feat = feature(partition_key="store-1")
        emi = emission(partition_key="store-1")
        writer.write(result(features=[feat]))
        writer.write(result(emissions=[emi]))

        self.assertEqual(len(writer.flushed_records()), 1)
        flushed = writer.flushed_records()[0]
        self.assertEqual(flushed.features, (feat,))
        self.assertEqual(flushed.emissions, (emi,))


class InMemoryDatasetWriterMultiPartitionTest(unittest.TestCase):
    def test_emissions_for_different_partitions_flush_separately(self) -> None:
        writer = InMemoryDatasetWriter()
        e1 = emission(partition_key="store-1")
        e2 = emission(partition_key="store-2")
        writer.write(result(emissions=[e1, e2]))

        flushes = writer.flushed_records()
        self.assertEqual(len(flushes), 2)
        self.assertEqual(flushes[0].partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushes[0].emissions, (e1,))
        self.assertEqual(flushes[1].partition_key, _STORE_2_HOUR_14)
        self.assertEqual(flushes[1].emissions, (e2,))

    def test_buffered_records_for_different_partitions_dont_mix(self) -> None:
        writer = InMemoryDatasetWriter()
        det_for_store_1 = detection(store_id="store-1")
        emi_for_store_2 = emission(partition_key="store-2")
        writer.write(
            result(detections=[det_for_store_1], emissions=[emi_for_store_2])
        )

        flushes = writer.flushed_records()
        self.assertEqual(len(flushes), 1)
        self.assertEqual(flushes[0].partition_key, _STORE_2_HOUR_14)
        self.assertEqual(flushes[0].detections, ())
        self.assertEqual(writer.buffered_partitions(), {_STORE_1_HOUR_14})


class InMemoryDatasetWriterRepairTest(unittest.TestCase):
    def test_repair_emission_triggers_separate_flush(self) -> None:
        writer = InMemoryDatasetWriter()
        first = emission(partition_key="store-1", is_repair=False)
        repair = emission(partition_key="store-1", is_repair=True, value=99)
        writer.write(result(emissions=[first]))
        writer.write(result(emissions=[repair]))

        flushes = writer.flushed_records()
        self.assertEqual(len(flushes), 2)
        self.assertEqual(flushes[0].partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushes[0].emissions, (first,))
        self.assertEqual(flushes[1].partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushes[1].emissions, (repair,))


class InMemoryDatasetWriterBufferLifecycleTest(unittest.TestCase):
    def test_flush_clears_buffer_for_partition(self) -> None:
        writer = InMemoryDatasetWriter()
        writer.write(result(emissions=[emission(partition_key="store-1")]))
        # After flushing, the partition has no buffered records.
        self.assertEqual(writer.buffered_partitions(), set())

    def test_multiple_emissions_same_window_flush_once_with_all(self) -> None:
        writer = InMemoryDatasetWriter()
        e1 = emission(partition_key="store-1", aggregation_name="count_a")
        e2 = emission(partition_key="store-1", aggregation_name="count_b")
        writer.write(result(emissions=[e1, e2]))

        flushes = writer.flushed_records()
        self.assertEqual(len(flushes), 1)
        self.assertEqual(flushes[0].partition_key, _STORE_1_HOUR_14)
        self.assertEqual(flushes[0].emissions, (e1, e2))


class InMemoryDatasetWriterOrderingTest(unittest.TestCase):
    def test_deterministic_order_across_partitions(self) -> None:
        writer = InMemoryDatasetWriter()
        e_first = emission(partition_key="store-2")
        e_second = emission(partition_key="store-1")
        writer.write(result(emissions=[e_first, e_second]))

        flushes = writer.flushed_records()
        # Flush order follows result.emissions iteration order.
        self.assertEqual(len(flushes), 2)
        self.assertEqual(flushes[0].partition_key, _STORE_2_HOUR_14)
        self.assertEqual(flushes[1].partition_key, _STORE_1_HOUR_14)


if __name__ == "__main__":
    unittest.main()
