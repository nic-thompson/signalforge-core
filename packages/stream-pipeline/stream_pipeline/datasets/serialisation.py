"""
stream_pipeline.datasets.serialisation

Phase 4 commit 6 — Parquet serialisation with schema-per-file.

Takes a ``FlushedPartition`` (the buffering unit produced by the dataset
writer) and produces Parquet bytes, one logical table per record type,
with emissions further split by ``aggregation_name``. No I/O, no S3, no
pipeline coupling: the function is ``FlushedPartition -> list[bytes-bearing
value objects]``. Commit 8's ``S3DatasetWriter`` turns each table into an
object key and uploads it; the object-key encoding (Hive-style vs flat)
is deliberately *not* decided here.

Why schema-per-file (and one file per aggregation)
---------------------------------------------------
The three record shapes (emission, detection, windowed feature vector)
are genuinely different schemas; combining them would force either a
union schema (rejected in the Phase 4 plan) or one sparse wide table.
So each record type serialises to its own table.

``WindowEmission.value`` is typed ``Any`` and its concrete type is fixed
by the producing aggregation: ``count`` and ``distinct_count`` yield
``int``, ``sum`` and ``mean`` yield ``float``. A single emissions table
would therefore carry a mixed-type ``value`` column. Splitting emissions
by ``aggregation_name`` gives each table a homogeneous, correctly-typed
``value`` column and keeps Parquet's predicate-pushdown useful — the
reason for choosing Parquet in the first place.

Determinism
-----------
The serialiser is byte-deterministic *given identical input records*:
same ``FlushedPartition`` in, same bytes out, every time and on every
machine running the same pyarrow build. This is enforced by:

- explicit schemas with a fixed field order (never inferred from dict
  iteration),
- an explicit total-order row sort that uses **only replay-deterministic
  columns** — never the ``uuid4`` identity fields (see below),
- pinned Parquet format version and compression codec,
- a single row group, so row-group boundaries don't depend on pyarrow's
  default chunking heuristics,
- JSON encoding of open maps (``details``, ``feature_values``) with
  sorted keys and no insignificant whitespace.

The one input-independent byte that ties output to the environment is
the Parquet footer's ``created_by`` string, which embeds the pyarrow
version. ``pyproject.toml`` pins ``pyarrow>=15``; within a single CI run
or replay comparison the resolved version is identical, so this does not
threaten the determinism the Phase 4 acceptance criteria assert.

Known tension for commit 9 (replay byte-identity)
--------------------------------------------------
``DetectionEvent`` carries ``detection_id`` and ``source_event_id``
(``uuid4``) and an envelope ``event_id`` (``uuid4``);
``WindowedFeatureVectorEvent`` carries an envelope ``event_id``
(``uuid4``). The project's determinism contract (see
``docs/detection-models.md`` and ``docs/feature-pipelines.md``) is
explicit that these are **sequence-deterministic, not byte-deterministic**
across independent runs. Commit 9's "byte-identical contents across live
and replay runs" assertion therefore cannot hold for the detections and
features tables while those columns are random per run.

This module does not resolve that — it is a commit-9 prerequisite — but
it does not make it worse: rows are ordered on replay-deterministic
columns only, so once the identity question is settled (deriving the IDs
via UUIDv5 from stable inputs, the "contained refinement" the detection
docs already float, or excluding the identity columns from the dataset),
byte-identity follows without touching the sort. The fields are written
faithfully for now because ``source_event_id`` is the audit-lineage hook
the upstream contract documents.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from event_schema_contracts.detection import DetectionEvent
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
)

from stream_pipeline.datasets.writer import FlushedPartition
from stream_pipeline.streaming.window_aggregator import WindowEmission

# ---------------------------------------------------------------------------
# Output value type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SerialisedTable:
    """
    One Parquet table produced from a FlushedPartition.

    ``record_type`` is one of ``"detections"``, ``"emissions"``,
    ``"features"``. ``aggregation_name`` is the emission's aggregation
    name for emission tables and ``None`` for detection and feature
    tables — emissions are split one table per aggregation so the
    ``value`` column stays homogeneously typed.

    ``data`` is the Parquet file bytes. ``row_count`` is the number of
    records in the table, surfaced so callers can assert without parsing
    the bytes back.

    Commit 8's S3 writer maps ``(record_type, aggregation_name)`` to an
    object key; this type intentionally carries no path information so it
    does not prejudge that encoding.
    """

    record_type: str
    aggregation_name: str | None
    data: bytes
    row_count: int


# ---------------------------------------------------------------------------
# Parquet write knobs (pinned for byte-determinism)
# ---------------------------------------------------------------------------

# Pin the format version and codec so output bytes depend only on the
# input records and the pyarrow build, not on pyarrow's evolving defaults.
# Dictionary encoding is deterministic given a fixed row order, but
# disabling it removes one more variable and costs nothing at the file
# sizes the window-close buffering unit produces.
_PARQUET_VERSION = "2.6"
_PARQUET_COMPRESSION = "snappy"
_USE_DICTIONARY = False

_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


def _json_scalar(value: Any) -> str:
    """
    Deterministically JSON-encode an open-map dict.

    Sorted keys and compact separators remove ordering and whitespace
    variance. ``default=str`` makes any non-JSON-native value (a UUID or
    datetime that found its way into ``details``) encode reproducibly
    rather than raising.
    """
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _write_table(table: pa.Table) -> bytes:
    """Serialise an arrow Table to Parquet bytes with pinned settings."""
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        version=_PARQUET_VERSION,
        compression=_PARQUET_COMPRESSION,
        use_dictionary=_USE_DICTIONARY,
        # One row group: all rows are a single chunk, so boundaries are
        # fixed rather than chosen by the default row_group_size heuristic.
        row_group_size=max(table.num_rows, 1),
    )
    data: bytes = sink.getvalue().to_pybytes()
    return data


# ---------------------------------------------------------------------------
# Value-column typing for emissions
# ---------------------------------------------------------------------------


def _value_array(values: list[Any]) -> pa.Array:
    """
    Build the ``value`` column for an emissions table.

    Within one ``aggregation_name`` the values are homogeneous by the
    ``Aggregation`` contract (``finalise`` has a stable return type), so
    inference picks one column type for the whole table. ``bool`` is
    checked before ``int`` because ``bool`` is an ``int`` subclass in
    Python; anything outside the numeric/bool families falls back to a
    JSON string column so an exotic custom aggregation still serialises
    deterministically instead of raising.
    """
    if values and all(isinstance(v, bool) for v in values):
        return pa.array(values, type=pa.bool_())
    if values and all(isinstance(v, int) and not isinstance(v, bool) for v in values):
        return pa.array(values, type=pa.int64())
    if values and all(
        isinstance(v, (int, float)) and not isinstance(v, bool) for v in values
    ):
        return pa.array([float(v) for v in values], type=pa.float64())
    return pa.array([_json_scalar(v) for v in values], type=pa.string())


# ---------------------------------------------------------------------------
# Per-record-type serialisers
# ---------------------------------------------------------------------------


def _emission_sort_key(e: WindowEmission) -> tuple[Any, ...]:
    # Replay-deterministic columns only. Within one aggregation table the
    # partition_key and aggregation_name are constant; window bounds plus
    # the repair flag give the meaningful order, with the trace id as a
    # final structural tiebreak (stable in a live-vs-replay comparison,
    # which reads the same archived events).
    return (
        e.window_start,
        e.window_end,
        e.is_repair,
        e.event_count,
        e.last_contributing_trace_id or "",
    )


def _serialise_emissions(
    emissions: tuple[WindowEmission, ...],
) -> list[SerialisedTable]:
    # Group by aggregation_name so each table's value column is one type.
    groups: dict[str, list[WindowEmission]] = {}
    for emi in emissions:
        groups.setdefault(emi.aggregation_name, []).append(emi)

    tables: list[SerialisedTable] = []
    for aggregation_name in sorted(groups):
        rows = sorted(groups[aggregation_name], key=_emission_sort_key)
        table = pa.table(
            {
                "partition_key": pa.array(
                    [r.partition_key for r in rows], type=pa.string()
                ),
                "aggregation_name": pa.array(
                    [r.aggregation_name for r in rows], type=pa.string()
                ),
                "window_start": pa.array(
                    [r.window_start for r in rows], type=_TIMESTAMP_TYPE
                ),
                "window_end": pa.array(
                    [r.window_end for r in rows], type=_TIMESTAMP_TYPE
                ),
                "value": _value_array([r.value for r in rows]),
                "event_count": pa.array(
                    [r.event_count for r in rows], type=pa.int64()
                ),
                "is_repair": pa.array([r.is_repair for r in rows], type=pa.bool_()),
                "last_contributing_trace_id": pa.array(
                    [r.last_contributing_trace_id for r in rows], type=pa.string()
                ),
            }
        )
        tables.append(
            SerialisedTable(
                record_type="emissions",
                aggregation_name=aggregation_name,
                data=_write_table(table),
                row_count=len(rows),
            )
        )
    return tables


def _detection_sort_key(d: DetectionEvent) -> tuple[Any, ...]:
    # Replay-deterministic columns only — never detection_id/event_id,
    # which are uuid4 and would scramble row order across runs.
    p = d.payload
    return (
        p.detected_at,
        d.event_timestamp,
        p.store_id,
        p.detection_type,
        str(p.device_id) if p.device_id is not None else "",
        p.threshold_breached,
    )


def _serialise_detections(
    detections: tuple[DetectionEvent, ...],
) -> SerialisedTable | None:
    if not detections:
        return None
    rows = sorted(detections, key=_detection_sort_key)
    table = pa.table(
        {
            "event_id": pa.array([str(d.event_id) for d in rows], type=pa.string()),
            "detection_id": pa.array(
                [str(d.payload.detection_id) for d in rows], type=pa.string()
            ),
            "detection_type": pa.array(
                [d.payload.detection_type for d in rows], type=pa.string()
            ),
            "severity": pa.array(
                [d.payload.severity.value for d in rows], type=pa.string()
            ),
            "detected_at": pa.array(
                [d.payload.detected_at for d in rows], type=_TIMESTAMP_TYPE
            ),
            "event_timestamp": pa.array(
                [d.event_timestamp for d in rows], type=_TIMESTAMP_TYPE
            ),
            "store_id": pa.array(
                [d.payload.store_id for d in rows], type=pa.string()
            ),
            "device_id": pa.array(
                [
                    str(d.payload.device_id) if d.payload.device_id is not None else None
                    for d in rows
                ],
                type=pa.string(),
            ),
            "source_event_id": pa.array(
                [str(d.payload.source_event_id) for d in rows], type=pa.string()
            ),
            "threshold_breached": pa.array(
                [d.payload.threshold_breached for d in rows], type=pa.string()
            ),
            "details": pa.array(
                [_json_scalar(d.payload.details) for d in rows], type=pa.string()
            ),
            "trace_id": pa.array(
                [str(d.trace.trace_id) for d in rows], type=pa.string()
            ),
        }
    )
    return SerialisedTable(
        record_type="detections",
        aggregation_name=None,
        data=_write_table(table),
        row_count=len(rows),
    )


def _feature_sort_key(f: WindowedFeatureVectorEvent) -> tuple[Any, ...]:
    # Replay-deterministic columns only — never event_id (uuid4).
    p = f.payload
    return (
        p.window_start,
        p.window_end,
        p.partition_key,
        p.feature_version,
        _json_scalar(p.feature_values),
    )


def _serialise_features(
    features: tuple[WindowedFeatureVectorEvent, ...],
) -> SerialisedTable | None:
    if not features:
        return None
    rows = sorted(features, key=_feature_sort_key)
    table = pa.table(
        {
            "event_id": pa.array([str(f.event_id) for f in rows], type=pa.string()),
            "partition_key": pa.array(
                [f.payload.partition_key for f in rows], type=pa.string()
            ),
            "window_start": pa.array(
                [f.payload.window_start for f in rows], type=_TIMESTAMP_TYPE
            ),
            "window_end": pa.array(
                [f.payload.window_end for f in rows], type=_TIMESTAMP_TYPE
            ),
            "feature_values": pa.array(
                [_json_scalar(f.payload.feature_values) for f in rows],
                type=pa.string(),
            ),
            "feature_version": pa.array(
                [f.payload.feature_version for f in rows], type=pa.string()
            ),
            "event_timestamp": pa.array(
                [f.event_timestamp for f in rows], type=_TIMESTAMP_TYPE
            ),
            "trace_id": pa.array(
                [str(f.trace.trace_id) for f in rows], type=pa.string()
            ),
        }
    )
    return SerialisedTable(
        record_type="features",
        aggregation_name=None,
        data=_write_table(table),
        row_count=len(rows),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def serialise_partition(partition: FlushedPartition) -> list[SerialisedTable]:
    """
    Serialise a FlushedPartition to a list of Parquet tables.

    Returns one table per non-empty record type, with emissions split one
    table per ``aggregation_name``. Empty record types produce no table —
    a partition with no detections simply has no detections table, which
    downstream "read all files for a partition" consumers handle as
    absence. The returned list is ordered deterministically by
    ``(record_type, aggregation_name)`` so the sequence of tables is
    itself reproducible.

    Each table's bytes are byte-deterministic given identical input
    records (see the module docstring for the determinism guarantees and
    the commit-9 identity-field caveat).
    """
    tables: list[SerialisedTable] = []

    detections_table = _serialise_detections(partition.detections)
    if detections_table is not None:
        tables.append(detections_table)

    tables.extend(_serialise_emissions(partition.emissions))

    features_table = _serialise_features(partition.features)
    if features_table is not None:
        tables.append(features_table)

    # Deterministic table ordering: detections, then emissions by
    # aggregation name, then features. Sorting on the same keys the
    # construction order already follows keeps this stable regardless of
    # future construction-order changes.
    tables.sort(key=lambda t: (t.record_type, t.aggregation_name or ""))
    return tables
