"""
signal_forge.datasets

Phase 4 dataset layer. Partitions ``ProcessingResult`` outputs by
``(store_id, hour)`` and writes them to S3 in Parquet.

This commit ships the partitioning primitives only. The buffering
writer (Phase 4 commit 5), Parquet serialisation (commit 6), pipeline
integration (commit 7), and S3 writer (commit 8) follow in subsequent
commits.
"""

from __future__ import annotations

from signal_forge.datasets.partition import (
    PartitionKey,
    partition_key_from_detection,
    partition_key_from_emission,
    partition_key_from_feature,
)

__all__ = [
    "PartitionKey",
    "partition_key_from_detection",
    "partition_key_from_emission",
    "partition_key_from_feature",
]
