"""
signal_forge.datasets.partition

Canonical partitioning unit for the Phase 4 dataset layer.

``PartitionKey`` collapses the three list-shapes on ``ProcessingResult``
(window emissions, detection events, windowed feature vector events) into
a single ``(store_id, hour)`` tuple suitable for routing to S3
partitions.

The three sources expose their store identifier differently:

- ``WindowEmission.partition_key`` — string, set by the pipeline's
  partition extractor.
- ``DetectionEvent.payload.store_id`` — string, set by each detector.
- ``WindowedFeatureVectorEvent.payload.partition_key`` — string,
  inherited from the contributing window emissions.

By convention, every one of these strings *is* a store_id. The pipeline's
partition extractor is responsible for ensuring this. Other partitioning
strategies (per-store-per-device, per-region) would require a different
dataset-layer partitioning scheme; this is not a Phase 4 concern.

The hour timestamp is derived differently depending on the source — see
the docstring on each helper for the reasoning.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from event_schema_contracts.detection import DetectionEvent
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
)

from signal_forge.streaming.window_aggregator import WindowEmission


@dataclass(frozen=True)
class PartitionKey:
    """
    Canonical partitioning unit for the Phase 4 dataset layer.

    ``store_id`` is the per-store identifier inherited from one of the
    three source types. ``hour`` is a UTC ``datetime`` truncated to the
    hour — all windows starting between 14:00:00 and 14:59:59 belong to
    the same hour partition, regardless of sub-hour position.

    Frozen so instances are hashable (used as dict keys in the dataset
    writer's buffer) and comparable.
    """

    store_id: str
    hour: datetime


def _truncate_to_hour(t: datetime) -> datetime:
    """
    Truncate a timestamp to the hour boundary, in UTC.

    Strips minutes, seconds, and microseconds. The result is always
    timezone-aware (UTC) even if the input had a different timezone.
    """
    return datetime(t.year, t.month, t.day, t.hour, tzinfo=UTC)


def partition_key_from_emission(emission: WindowEmission) -> PartitionKey:
    """
    Extract a PartitionKey from a window emission.

    Uses ``window_start`` (not ``window_end``) so windows are partitioned
    by the time they describe, not the time they were emitted. By
    convention, ``WindowEmission.partition_key`` is the store_id.
    """
    return PartitionKey(
        store_id=emission.partition_key,
        hour=_truncate_to_hour(emission.window_start),
    )


def partition_key_from_detection(detection: DetectionEvent) -> PartitionKey:
    """
    Extract a PartitionKey from a detection event.

    Uses ``event_timestamp`` (typically set from a contributing window's
    ``window_end``). When a window crosses an hour boundary, the
    detection and its contributing emissions may partition to different
    hours — downstream join queries account for this with a small
    timestamp window rather than relying on partition equality.
    """
    return PartitionKey(
        store_id=detection.payload.store_id,
        hour=_truncate_to_hour(detection.event_timestamp),
    )


def partition_key_from_feature(feature: WindowedFeatureVectorEvent) -> PartitionKey:
    """
    Extract a PartitionKey from a windowed feature vector event.

    Uses ``payload.window_start`` (the time the windowed data is *about*),
    not the event's ``event_timestamp`` (the time the event was
    constructed). The payload's ``partition_key`` is the store_id.
    """
    return PartitionKey(
        store_id=feature.payload.partition_key,
        hour=_truncate_to_hour(feature.payload.window_start),
    )
