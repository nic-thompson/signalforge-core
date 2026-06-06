"""
signal_forge.datasets.s3_writer

Phase 4 commit 8 — S3DatasetWriter.

Composes the partition buffering (``_PartitionBufferSet``) with Parquet
serialisation (``serialise_partition``) and a boto3 S3 client. Each
window-close flush produces one or more ``SerialisedTable``s; the writer
uploads each as a Parquet object under a Hive-partitioned key.

boto3 lives here and only here — ``InMemoryDatasetWriter`` and the
buffering state machine stay dependency-free so they import in any
context (Lambda, tests, replay drivers without AWS deps).

Configuration and replay isolation
-----------------------------------
The writer reads its target bucket from ``PlatformSettings.dataset_bucket``
and is replay-oblivious: ``PlatformSettings.for_replay()`` swaps the
active bucket to the replay-isolated one *before* the writer is
constructed, so the writer never needs to know which mode it is in. When
``dataset_bucket`` is ``None`` — including a replay run whose replay
bucket was never configured — the writer is a no-op: it constructs no
client and ``write()`` does nothing. That honours ``for_replay()``'s
documented "no-op rather than silently writing to the live bucket"
safety property.

Object keys
-----------
Hive-partitioned, with record type (and aggregation name, for emissions)
as table roots *above* the partition columns::

    detections/store_id=<s>/year=<Y>/month=<M>/day=<D>/hour=<H>/<seq>.parquet
    emissions/<aggregation>/store_id=<s>/.../hour=<H>/<seq>.parquet
    features/store_id=<s>/.../hour=<H>/<seq>.parquet

Record type and aggregation are table *roots*, not partition columns,
because each carries a distinct Parquet schema (the emissions ``value``
column is ``int`` for count/distinct, ``float`` for sum/mean). A query
engine pointed at a table root then sees one uniform schema varying only
by the store/time partition columns. ``store_id``/``year``/``month``/
``day``/``hour`` are Hive partition columns, auto-detected by Athena,
Spark and DuckDB for partition pruning. The upstream partition-key
grammar forbids ``/`` and ``=``, so store ids are Hive-safe without
escaping.

Filename uniqueness
-------------------
A monotonic per-prefix sequence number. Repair emissions, and multiple
windows closing into the same hour, write additional objects under the
same prefix; each takes the next sequence number. This is
replay-deterministic because flush order is deterministic, and it is
independent of the per-event ``uuid4`` identity fields — so live and
replay runs produce byte-identical object *keys*.

Byte-identical object *contents* for the detection and feature tables
additionally require the ``uuid4`` identity fields (``detection_id``,
``source_event_id``, envelope ``event_id``) to be made deterministic.
That is a separate change preceding the Phase 4 replay-isolation test
and is deliberately out of scope here; the key scheme is what makes the
eventual content comparison possible.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.datasets.partition import PartitionKey
from signal_forge.datasets.serialisation import SerialisedTable, serialise_partition
from signal_forge.datasets.writer import FlushedPartition, _PartitionBufferSet
from signal_forge.streaming.realtime_pipeline import ProcessingResult

# The boto3 S3 client is untyped under our mypy config; the alias documents
# intent at the seam where a client is injected (tests supply a moto-backed
# client; production uses the default factory below).
S3ClientFactory = Callable[[], Any]


def _default_s3_client() -> Any:
    # Imported lazily so this module — and InMemoryDatasetWriter, which
    # shares the package — imports without boto3 present. Only the
    # production default factory pulls boto3 in.
    import boto3

    return boto3.client("s3")


def _table_root(table: SerialisedTable) -> str:
    if table.aggregation_name is not None:
        return f"{table.record_type}/{table.aggregation_name}"
    return table.record_type


def _partition_path(key: PartitionKey) -> str:
    h = key.hour
    return (
        f"store_id={key.store_id}/"
        f"year={h.year:04d}/month={h.month:02d}/day={h.day:02d}/hour={h.hour:02d}"
    )


class S3DatasetWriter:
    """
    Dataset writer that serialises flushed partitions to Parquet and
    uploads them to S3. Satisfies the ``DatasetWriter`` Protocol.

    Construct with platform settings and, optionally, a client factory.
    The factory seam lets tests inject a moto-backed client; production
    omits it and gets a real boto3 client built from the bucket name.
    The writer assumes the bucket already exists (provisioned upstream in
    ``aws-event-pipeline-infra``); it neither creates buckets nor sets
    lifecycle policies — retention is configured upstream.
    """

    def __init__(
        self,
        *,
        settings: PlatformSettings,
        client_factory: S3ClientFactory | None = None,
    ) -> None:
        self._bucket = settings.dataset_bucket
        self._buffers = _PartitionBufferSet(on_flush=self._upload_flush)
        # Per-prefix flush counter for filename uniqueness within a
        # partition. Keyed by the object-key prefix (table root + partition
        # path); deterministic because flush order is deterministic.
        self._seq: dict[str, int] = {}

        self._client: Any
        if self._bucket is None:
            # No dataset export configured: no client, write() no-ops.
            self._client = None
        else:
            factory = client_factory or _default_s3_client
            self._client = factory()

    def write(self, result: ProcessingResult) -> None:
        if self._bucket is None:
            return
        self._buffers.absorb(result)

    def _upload_flush(self, flushed: FlushedPartition) -> None:
        for table in serialise_partition(flushed):
            key = self._object_key(flushed.partition_key, table)
            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=table.data,
            )

    def _object_key(self, partition_key: PartitionKey, table: SerialisedTable) -> str:
        prefix = f"{_table_root(table)}/{_partition_path(partition_key)}"
        seq = self._seq.get(prefix, 0)
        self._seq[prefix] = seq + 1
        return f"{prefix}/{seq:08d}.parquet"
