"""
signal_forge.datasets

Phase 4 dataset layer. Partitions ``ProcessingResult`` outputs by
``(store_id, hour)`` and writes them to S3 in Parquet.

This commit adds the in-memory buffering writer alongside the
partition helpers shipped previously. Subsequent commits add Parquet
serialisation (commit 6), pipeline integration (commit 7), and the
S3 writer (commit 8).
"""

from __future__ import annotations

from signal_forge.datasets.partition import (
    PartitionKey,
    partition_key_from_detection,
    partition_key_from_emission,
    partition_key_from_feature,
)
from signal_forge.datasets.s3_writer import S3DatasetWriter
from signal_forge.datasets.writer import (
    DatasetWriter,
    FlushedPartition,
    InMemoryDatasetWriter,
)

__all__ = [
    "DatasetWriter",
    "FlushedPartition",
    "InMemoryDatasetWriter",
    "PartitionKey",
    "S3DatasetWriter",
    "partition_key_from_detection",
    "partition_key_from_emission",
    "partition_key_from_feature",
]
