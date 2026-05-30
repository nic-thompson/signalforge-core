"""
signal_forge.datasets.writer

Phase 4 dataset writer — buffering state machine plus an in-memory
implementation.

The writer is the consumer side of the pipeline's ProcessingResult.
Each call to `write()` is one ProcessingResult; the writer buffers
records by PartitionKey and flushes when a window-close emission
appears for a partition.

A FlushedPartition is the unit of downstream output: all records
(emissions, detections, features) that buffered for a single
(store_id, hour) are emitted together. The in-memory writer in this
module surfaces flushes for unit-testing the buffering logic; commit 6
adds Parquet serialisation, commit 7 wires this into the pipeline,
commit 8 ships the S3 variant.

Replay determinism: the writer holds no clock state and no cross-call
state beyond its buffer dict. Given the same ProcessingResult sequence,
the flush sequence is byte-identical across runs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from event_schema_contracts.detection import DetectionEvent
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
)

from signal_forge.datasets.partition import (
    PartitionKey,
    partition_key_from_detection,
    partition_key_from_emission,
    partition_key_from_feature,
)
from signal_forge.streaming.realtime_pipeline import ProcessingResult
from signal_forge.streaming.window_aggregator import WindowEmission

# ---------------------------------------------------------------------------
# Writer Protocol
# ---------------------------------------------------------------------------


class DatasetWriter(Protocol):
    """
    Consumes ProcessingResult outputs and persists them to a sink.

    Implementations:
    - InMemoryDatasetWriter (this module): collects flushed partitions
      in memory for tests.
    - S3DatasetWriter (Phase 4 commit 8): writes Parquet files to S3.

    The Protocol shape is deliberately minimal — one method, no return.
    Side effects are the implementation's concern; the pipeline simply
    hands off each result and continues.
    """

    def write(self, result: ProcessingResult) -> None: ...


# ---------------------------------------------------------------------------
# Flushed-partition value type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FlushedPartition:
    """
    A partition's buffered records at flush time.

    Frozen and tuple-bearing so once flushed, the record set is
    immutable. The S3 writer (commit 8) writes one Parquet file per
    FlushedPartition; tests assert against the writer's flushed-list
    accumulator.

    Empty tuples are valid: a partition can flush with no detections
    or no features when no such records buffered for it. The emissions
    tuple is non-empty by construction — flushes are triggered by
    emissions.
    """

    partition_key: PartitionKey
    emissions: tuple[WindowEmission, ...]
    detections: tuple[DetectionEvent, ...]
    features: tuple[WindowedFeatureVectorEvent, ...]


# ---------------------------------------------------------------------------
# In-memory writer
# ---------------------------------------------------------------------------


@dataclass
class _PartitionBuffer:
    """Internal per-partition buffer. Mutable; cleared on flush."""

    emissions: list[WindowEmission] = field(default_factory=list)
    detections: list[DetectionEvent] = field(default_factory=list)
    features: list[WindowedFeatureVectorEvent] = field(default_factory=list)


class InMemoryDatasetWriter:
    """
    Buffers records per PartitionKey, flushes on window-close emissions.

    Each ProcessingResult is processed in three phases:

    1. Append detections and features to their partition buffers.
    2. Append emissions to their partition buffers.
    3. For each PartitionKey with any new emissions, emit a
       FlushedPartition containing the entire buffered contents, then
       clear the partition's buffer.

    Flushed partitions accumulate in an internal list, accessible via
    `flushed_records()` for test inspection. Repair emissions
    (`is_repair=True`) trigger separate flushes for their partition,
    producing multiple FlushedPartition entries with the same
    PartitionKey.

    Flush order within a single write() call is the iteration order of
    `result.emissions` — whichever partition's emission appears first
    is flushed first. Subsequent emissions for the same partition in
    the same result merge into one flush.
    """

    def __init__(self) -> None:
        self._buffers: dict[PartitionKey, _PartitionBuffer] = {}
        self._flushed: list[FlushedPartition] = []

    def write(self, result: ProcessingResult) -> None:
        # Phase 1: buffer detections and features by their derived
        # PartitionKey. These don't trigger flushes themselves.
        for det in result.detections:
            key = partition_key_from_detection(det)
            self._buffer_for(key).detections.append(det)

        for feat in result.features:
            key = partition_key_from_feature(feat)
            self._buffer_for(key).features.append(feat)

        # Phase 2 + 3: for each emission, buffer it and flag its
        # partition for flush. Use an insertion-ordered dict to track
        # flush order — `dict` in Python 3.7+ preserves insertion order.
        partitions_to_flush: dict[PartitionKey, None] = {}
        for emi in result.emissions:
            key = partition_key_from_emission(emi)
            self._buffer_for(key).emissions.append(emi)
            partitions_to_flush[key] = None

        # Flush each flagged partition, in insertion order.
        for key in partitions_to_flush:
            self._flush_partition(key)

    def flushed_records(self) -> list[FlushedPartition]:
        """Return all FlushedPartitions, in flush order."""
        return list(self._flushed)

    def buffered_partitions(self) -> set[PartitionKey]:
        """Return PartitionKeys currently holding any buffered records."""
        return {k for k, buf in self._buffers.items() if self._is_nonempty(buf)}

    def _buffer_for(self, key: PartitionKey) -> _PartitionBuffer:
        if key not in self._buffers:
            self._buffers[key] = _PartitionBuffer()
        return self._buffers[key]

    def _flush_partition(self, key: PartitionKey) -> None:
        buf = self._buffers[key]
        self._flushed.append(
            FlushedPartition(
                partition_key=key,
                emissions=tuple(buf.emissions),
                detections=tuple(buf.detections),
                features=tuple(buf.features),
            )
        )
        # Clear the partition's buffer. Keep the buffer object so
        # subsequent appends don't need to re-create it.
        buf.emissions.clear()
        buf.detections.clear()
        buf.features.clear()

    @staticmethod
    def _is_nonempty(buf: _PartitionBuffer) -> bool:
        return bool(buf.emissions or buf.detections or buf.features)

