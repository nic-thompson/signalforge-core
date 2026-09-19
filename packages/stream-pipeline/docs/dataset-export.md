# Dataset export

The dataset layer turns each `ProcessingResult` into partitioned Parquet
written to S3, so the analytics outputs (window emissions, detections, and
windowed feature vectors) become a durable, queryable dataset rather than a
transient stream. It is the production consumer of the function-shaped
pipeline's returns: where Phase 3 made features flow back as values on
`ProcessingResult`, Phase 4 routes those values — alongside emissions and
detections — to an S3-partitioned Parquet sink.

**Package:** `stream_pipeline.datasets` (`PartitionKey` and the three partition
helpers, `DatasetWriter`, `FlushedPartition`, `InMemoryDatasetWriter`,
`S3DatasetWriter`, and the `serialise_partition` serialiser).
**Output:** one Parquet object per serialised table per window-close flush,
under a Hive-partitioned object key.
**Replay determinism:** byte-identical object contents across a live run and
its replay, contingent on the derived-identity work described below.
**Configuration home:** `PlatformSettings.dataset_bucket` /
`replay_dataset_bucket`, swapped by `for_replay()`.

## What the dataset layer produces

Each `process()` result carries three parallel lists — `emissions`,
`detections`, `features`. The writer buffers them by partition and, on a
window close, flushes a partition's buffered records as one or more Parquet
tables. The three record shapes are genuinely different schemas, so each
serialises to its own table rather than a single wide or union table:
a detections table, a features table, and one emissions table *per
aggregation name*. Empty record types produce no table — a partition with
no detections simply has no detections file, which a "read all files for a
partition" consumer handles as absence.

## Partitioning

`PartitionKey` collapses the three source shapes into a single
`(store_id, hour)` tuple. The sources expose their store identifier
differently — `WindowEmission.partition_key`,
`DetectionEvent.payload.store_id`, and
`WindowedFeatureVectorEvent.payload.partition_key` — and by convention each
of those strings *is* a store_id, a convention the pipeline's partition
extractor is responsible for upholding. The `hour` component is a UTC
`datetime` truncated to the hour.

The hour is derived from the time the data is *about*, not the time a record
was constructed: emissions and features partition on `window_start`,
detections on `event_timestamp` (typically a contributing window's
`window_end`). One consequence is honest rather than hidden: when a window
straddles an hour boundary a detection and its contributing emissions can
land in adjacent hour partitions, so downstream joins use a small timestamp
window rather than relying on partition equality. `PartitionKey` is a frozen
dataclass so it is hashable (it keys the writer's buffer) and comparable.

## Unit of writing — window-close buffering

The writer does not write per `process()` call, and it does not flush on a
timer. It buffers records by `PartitionKey` and flushes a partition when a
window-close emission appears for it. Per-call writes would produce a swarm
of one- and two-row Parquet files — the canonical small-file anti-pattern,
forcing an immediate compaction job downstream. Timer-based flushing would
break the function-shaped pipeline (there is no event loop to tick) and
break replay determinism (a replay running faster than live would flush at
different boundaries). Window closes are already replay-deterministic —
they are driven by watermark advance, itself event-time-driven — so they are
the natural batching unit. Detections and features buffer without triggering
a flush; each emission buffers and flags its partition; flagged partitions
flush in first-seen order, emit their entire buffered contents, and clear.
A repair emission arriving in a later result flushes again as a fresh
`FlushedPartition` for the same key.

The flush state machine lives once, in `_PartitionBufferSet`, so every
writer shares one correct copy: `InMemoryDatasetWriter` accumulates flushes
in a list for tests; `S3DatasetWriter` serialises and uploads each.

## Serialisation — schema-per-file

`serialise_partition(FlushedPartition) -> list[SerialisedTable]` is pure: no
I/O, no S3, no pipeline coupling. It emits one table per record type, with
emissions split one table per `aggregation_name`.

The emissions split exists because `WindowEmission.value` is typed `Any` and
its concrete type is fixed by the producing aggregation — `count` and
`distinct_count` yield `int`, `sum` and `mean` yield `float`. A single
emissions table would carry a mixed-type `value` column; splitting by
aggregation gives each table a homogeneous, correctly typed column and keeps
Parquet predicate-pushdown useful.

Each file carries its own schema; no union schema is computed and no file is
rewritten when contracts evolve. Union-schema writers would have to know
every field a future version might add, making the writer fragile to
upstream evolution; schema-per-file lets contracts evolve without the writer
being redeployed in lockstep, and leaves schema reconciliation to read-time
consumers (see "Schema evolution" below).

### Byte-determinism

The serialiser is byte-deterministic given identical input records: same
`FlushedPartition` in, same bytes out, on every machine running the same
pyarrow build. This is enforced by explicit fixed-order schemas (never
inferred from dict iteration), an explicit total-order row sort that uses
**only replay-deterministic columns** — never the identity fields — pinned
Parquet format version (`2.6`) and codec (`snappy`) with dictionary encoding
disabled, a single row group so boundaries don't depend on pyarrow's
chunking heuristics, and JSON encoding of open maps (`details`,
`feature_values`) with sorted keys and compact separators. The one
input-independent byte is the Parquet footer's `created_by` string, which
embeds the pyarrow version — identical within a single CI run or replay
comparison, so it does not threaten the determinism the acceptance criteria
assert.

## Object keys

`S3DatasetWriter` composes the buffering, the serialiser, and a boto3 client,
and maps each `SerialisedTable` to a Hive-partitioned key with the record
type — and aggregation name, for emissions — as a table *root* above the
partition columns:

```
detections/store_id=<s>/year=<Y>/month=<M>/day=<D>/hour=<H>/<seq>.parquet
emissions/<aggregation>/store_id=<s>/.../hour=<H>/<seq>.parquet
features/store_id=<s>/.../hour=<H>/<seq>.parquet
```

Record type and aggregation are roots rather than partition columns because
each carries a distinct schema; a query engine pointed at a root then sees
one uniform schema varying only by the store/time partition columns.
`store_id`/`year`/`month`/`day`/`hour` are Hive partition columns,
auto-detected by Athena, Spark, and DuckDB for partition pruning; the
upstream partition-key grammar forbids `/` and `=`, so store ids are
Hive-safe without escaping.

Filename uniqueness is a monotonic per-prefix sequence number. Repair
emissions and multiple windows closing into the same hour write additional
objects under the same prefix, each taking the next sequence number. This is
replay-deterministic because flush order is deterministic, and it is
independent of the per-event identity fields — so live and replay runs
produce byte-identical object *keys*.

## Configuration and replay isolation

### `PlatformSettings` as the switch home

The live-versus-replay distinction lives in one place. `dataset_bucket` and
`replay_dataset_bucket` are `str | None` fields on `PlatformSettings`;
`for_replay()` returns a copy with `dataset_bucket` set to the replay bucket
— unconditionally, even when the replay bucket is `None`. The writer reads
`settings.dataset_bucket` and is replay-oblivious: it never learns which
mode it is in. A missing replay bucket therefore surfaces as a no-op writer
(no client constructed, `write()` does nothing) rather than a silent write
to the live bucket — the safer failure mode. This follows decision D-12:
`PlatformSettings` is the canonical home for live-versus-replay switches, and
it carries the bucket *name*, never a boto3 client or credentials — the
stdlib-only discipline survives the addition, and the client is constructed
by the writer at the one seam where AWS dependencies are allowed to live.

### Replay byte-identity

The headline property is that replaying an archived event sequence
reproduces the original run's dataset output byte-for-byte, written to a
replay-isolated bucket. Object *keys* were already identity-independent
(sequence numbers, not random ids). Byte-identical *contents* additionally
required the previously-`uuid4` identity fields — `detection_id`,
`source_event_id`, and the envelope `event_id` on detections and features —
to be made deterministic. These are now derived via UUIDv5 from stable
coordinates (`event_schema_contracts.base.identity.derive`), so the serialised bytes carry
no per-run randomness. The row sort already used replay-deterministic
columns only, so byte-identity followed from the identity change without
touching serialisation. The integration test runs one event sequence through
a live pipeline and a `for_replay()` pipeline and asserts identical keys and
identical object bytes across the two buckets.

## Schema evolution

When an upstream contract evolves `v1` -> `v1.1`, files written before the
bump keep the v1 schema and files written after carry v1.1; no union schema
is computed at write time and no already-written file is rewritten.
Downstream consumers tolerant of schema variation read both generations
cleanly, unifying at *read* time — for example pyarrow's
`concat_tables(promote_options="default")`, which null-fills columns absent
from older files. This is the classic data-lake forward-compatibility
problem, answered with the simpler option: write-side stays trivial, and the
reconciliation that does happen happens once, lazily, on read.

## What this layer deliberately does not do

The dataset layer is small on purpose. Behaviours that look like its concern
but live elsewhere:

- **No union-schema writing.** Each file is schema-per-file; reconciliation
  across versions is a read-time consumer concern, not a write-time one.

- **No compaction.** Repair emissions and multiple same-hour windows write
  additional objects rather than rewriting prior ones. Compaction — if it is
  ever needed — is a downstream batch concern; the "read all files for a
  partition" model tolerates many small files for now.

- **No bucket provisioning or lifecycle policy.** The writer assumes the
  bucket exists, provisioned upstream in `aws-event-pipeline-infra`. The
  brief's two-year retention is configured there as an S3 lifecycle policy,
  not here; this layer only declares the requirement.

- **No replay-mode awareness in the writer.** The writer reads one bucket
  name. Live-versus-replay is resolved entirely by `for_replay()` swapping
  that name in `PlatformSettings`, upstream of the writer.

- **No filtering or projection.** Every flushed partition's records are
  written. Narrowing by store, aggregation, or value is a downstream query
  concern.

- **No identity minting.** The dataset layer serialises the identity fields
  the events already carry; it does not generate them. Their determinism is
  owned by the detectors and the feature-bundling path via
  `event_schema_contracts.base.identity`.

## Cross-references

- `docs/working-notes.md` — D-4 (function-shaped pipeline), D-5
  (returns-not-side-effects, the precedent this layer consumes), D-11
  (upstream contract evolution), D-12 (`PlatformSettings` as the
  live-versus-replay switch home).
- `docs/architecture.md` — where the dataset layer sits in the stack.
- `docs/feature-pipelines.md` — the Phase 3 reference document whose shape
  this mirrors; the producer of the features this layer persists.
- `docs/roadmap.md` — Phase 4 scope, the "underestimating Phase 4" risk note,
  and DDIA Chapter 11 (Batch Processing) on file-size considerations for
  downstream consumers.
- `stream_pipeline/datasets/partition.py` — `PartitionKey` and the three
  partition helpers.
- `stream_pipeline/datasets/writer.py` — `DatasetWriter` protocol,
  `FlushedPartition`, the `_PartitionBufferSet` flush state machine, and
  `InMemoryDatasetWriter`.
- `stream_pipeline/datasets/serialisation.py` — `serialise_partition` and the
  byte-determinism knobs.
- `stream_pipeline/datasets/s3_writer.py` — `S3DatasetWriter`, object-key
  construction, and the no-op-when-unconfigured behaviour.
- `stream_pipeline/config/platform_settings.py` — the `dataset_bucket` /
  `replay_dataset_bucket` fields and `for_replay()`.
- `event_schema_contracts.base.identity` — the UUIDv5 derivation that makes replay
  byte-identity possible.
- `tests/datasets/test_replay_isolation.py` and
  `tests/datasets/test_schema_evolution.py` — the replay-byte-identity and
  schema-per-file guards.
