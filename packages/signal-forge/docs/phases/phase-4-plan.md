# Phase 4 — Dataset partitioning and S3 export

> **Status.** Branch `feat/phase-4-dataset-export` exists locally; no commits yet. `main` sits at `eb42242` (Phase 3 merge), tagged `phase-3-complete`. See the most recent file in `docs/status/` for current state.

## What Phase 4 delivers

A dataset layer that partitions `ProcessingResult` outputs by `(store_id, hour)` and writes them to S3 in Parquet. Replay-aware: separate buckets for live and replay outputs. Honours the brief's 2-year retention requirement via S3 lifecycle policies (declared in this layer's reference document; configured upstream in `aws-event-pipeline-infra`).

The deliverables are:

- **`PartitionKey` and partition helpers** — pure functions that extract `(store_id, hour)` from the three list-shapes on `ProcessingResult` (window emissions, detection events, windowed feature vector events). The three sources expose `store_id` differently; the helpers collapse them to a single canonical type.
- **`InMemoryDatasetWriter`** — the buffering state machine in isolation, no S3, no boto3. Buffers records keyed by partition key; flushes when a window-close emission appears in a `ProcessingResult`.
- **Parquet serialisation** — pyarrow-only layer that takes a buffer's records and produces Parquet bytes with deterministic schema-per-file (no union schemas).
- **`S3DatasetWriter`** — composes buffering and serialisation with boto3, mocked via `moto` in tests.
- **Pipeline integration** — `RealtimePipeline` accepts an optional dataset writer; on each `process()` result, hands the result to the writer. Replay-isolation integration test asserts byte-identical contents across live and replay runs writing to different buckets.
- **`docs/dataset-export.md`** — reference document matching the shape of `docs/feature-pipelines.md` and `docs/detection-models.md`.

Two design questions are already settled (below). Three remain open and will be settled in focused design conversations before the relevant commit.

## Context and prerequisites

Phase 4 starts from a position similar to Phase 3's: the upstream contracts are mature enough for first-deep-integration, but at least one upstream PR is plausible (per D-11). The most likely candidate is a `dataset_record` envelope or a record-shape addition to `event-schema-contracts.datasets`, only if our serialisation needs surface a contract gap that's better fixed upstream than worked around locally.

The pipeline's `ProcessingResult` already carries the three lists the writer needs (`emissions`, `detections`, `features`). D-5 ("detections flow back as a return value, not as side effects") was specifically designed to enable a thin Phase 4 writer; this phase exercises that intent.

`pyarrow` and `moto` are not currently in `pyproject.toml`'s dev-dependencies. The first non-doc commit adds them. The roadmap's risk note for Phase 4 — "S3 mock infrastructure is the time sink" — drives the discipline of putting test-infrastructure scaffolding into its own commit before any dataset logic gets written.

## PlatformSettings as the home for dataset-layer switches — settled design

The design conversation on 2026-05-28 settled this question. Decision recorded here for reference; the working-notes entry (D-12) carries the cross-phase principle.

**1. `dataset_bucket` and `replay_dataset_bucket` are first-class `PlatformSettings` fields.**

Two `str | None` fields, defaulting to `None` (meaning "no dataset export configured"). `for_replay()` swaps the active bucket by setting `dataset_bucket=self.replay_dataset_bucket` on the returned copy. The writer reads `settings.dataset_bucket` and does not care whether it's running live or replay.

Three alternatives considered:

- Per-writer config passed at construction — rejected because it would create a second live-vs-replay switching mechanism in parallel to `for_replay()`. D-5's "easy to forget flag that causes incidents" reasoning applies symmetrically here.
- A separate `DatasetSettings` object alongside `PlatformSettings` — rejected because the existing `data_retention_days` field already crosses the dataset-layer boundary (its docstring explicitly says "consumed in Phase 4"). Splitting would scatter dataset config across two homes for no clear benefit.
- The chosen pattern — keeps the live-vs-replay distinction in one place, matches the precedent `for_replay()` already established, and makes replay-determinism assertions simpler (one settings snapshot to compare rather than two).

**2. `PlatformSettings` stays stdlib-only.**

The settings object carries the bucket *name* (a `str`), not a boto3 client or credentials. The discipline established in the existing module docstring — "stdlib-only — importable from any context without pydantic/AWS deps" — survives this addition. The boto3 client gets constructed by the writer from the bucket name; that's where AWS dependencies are allowed to live.

**3. Validation: bucket names follow AWS naming rules.**

`__post_init__` validates that, when non-`None`, the bucket name matches AWS S3 bucket naming rules (3–63 chars, lowercase alphanumeric and hyphens, not starting or ending with hyphen). Misconfiguration surfaces at startup, not three hours into a replay. Same fail-fast discipline as the existing range validations on the numeric fields.

## Unit of writing — settled design

The design conversation on 2026-05-28 settled this question after considering three options:

- **A.** Per-`ProcessingResult` — every `process()` call writes its three lists immediately.
- **B.** Buffer to window-close boundaries — accumulate records, flush when a window-close emission appears in a `ProcessingResult`.
- **C.** Time-based flush — accumulate records, flush every N seconds (wall-clock or watermark-driven).

**Decision: option B — buffer to window-close boundaries.**

**1. Per-`ProcessingResult` writes (A) produce many small files.**

Each `process()` call may produce zero or one detection and zero or one window emission. Hourly partitions consisting of thousands of one-row or two-row Parquet files is the canonical Parquet anti-pattern: S3 list-and-read costs make it operationally painful, and downstream batch consumers would need an immediate compaction job to undo what we'd done. DDIA Chapter 11's discussion of file-size considerations for batch consumers applies directly.

**2. Time-based flushes (C) break the function-shaped pipeline.**

The pipeline has no event loop to tick a flush timer against (D-4). Wall-clock-driven flushing would also break replay determinism — a replay running faster than live would flush at different boundaries.

**3. Window-close-driven flushes (B) integrate cleanly with what's already there.**

Window closes are already replay-deterministic — they're driven by watermark advance, which is itself event-time-driven. The natural batching unit (a window's worth of emissions and detections) is what's already being computed. The trigger to flush a partition's buffer is the appearance of window emissions in `ProcessingResult.emissions` — which the writer can observe purely from the data it already receives, with no parallel side-channel needed.

**4. Detection-flushing follows the same buffer as emissions.**

Detections from event-detectors (which don't have a natural window) accumulate in the writer's buffer keyed by `(store_id, current_window)` and flush with that window's emissions. The alternative (per-`ProcessingResult` flush for detections, window-close for emissions) is more honest about the two streams having different cadences but produces small detection files. Downstream consumers want detections grouped with the emissions from the same window for join purposes; that wins.

**5. Repair emissions write new files for the same partition.**

When a repair emission (`is_repair=True`) arrives for a window that has already flushed, it triggers a new file write for the same `(store_id, hour)` partition. Original files are kept for audit; downstream consumers read all files for a partition and rely on Parquet schema metadata to distinguish original from repair. This is the standard "compaction on read" model — write-side stays simple, complexity moves to consumers, the trade-off matches the roadmap's Phase 4 design note.

## Schema-per-file — settled design

A short third settled decision, because it follows naturally from the unit-of-writing choice and the roadmap's pre-stated lean.

**1. Each Parquet file carries its own schema.**

When upstream contracts evolve (v1 → v1.1), files written before the bump retain the v1 schema; files written after carry v1.1. No union schema is computed; no schema migration runs.

**2. Why not union schemas.**

Union-schema writers would need to know about every possible field a future version might add, which makes the writer fragile to upstream evolution. The schema-per-file approach lets contracts evolve upstream without the writer having to be re-deployed in lockstep. Downstream readers tolerant of schema variation (Parquet's standard predicate-pushdown handles this) read either generation cleanly.

This is the "classic data-lake forward-compatibility problem" from the roadmap; the answer is the simpler one.

## Object key construction — questions to settle

Two design questions need explicit answers before the `S3DatasetWriter` commit:

1. **Partition key encoding in the object key.** Hive-style (`store_id=X/year=Y/month=M/day=D/hour=H/`) is the convention many query engines (Athena, Spark, DuckDB) auto-detect, which makes downstream consumption easier. Flat keys (`X/Y-M-D-H.parquet`) are simpler to construct and inspect. The Hive convention is probably right but worth a deliberate choice.

2. **Filename uniqueness within a partition.** Repair emissions write additional files to the same `(store_id, hour)` partition. The filename needs a uniqueness component — candidates are a monotonic sequence number (replay-deterministic but requires writer state), a UUIDv5 derived from the partition key plus contents (replay-deterministic, no state, but more opaque), or a write-time content hash (replay-deterministic, no state, slightly self-documenting). UUIDv5 or content-hash both work; pick one deliberately.

To be settled in a focused design conversation before commit 8. Estimated 15 minutes of design.

## Pipeline integration shape — question to settle

One design question for commit 7:

**How does `RealtimePipeline` know which writer to call?** Two options:

- **Constructor injection** — `RealtimePipeline(writers=[dataset_writer])`. Symmetrical with how detectors are registered. Multiple writers possible.
- **A single optional writer parameter** — `RealtimePipeline(dataset_writer=...)`. Simpler signature; matches the "one dataset layer per pipeline" reality.

Constructor injection of a list is probably more uniform with the existing detector pattern, but a single optional parameter is more honest about there only ever being one dataset writer per pipeline. Worth thinking through carefully — touches the same uniformity-vs-honesty trade-off as the Phase 3 `FeatureSink`-vs-`ProcessingResult.features` choice (which went to the more-uniform answer; precedent suggests this should too).

To be settled in a focused design conversation before commit 7. Estimated 15 minutes of design.

## Plan

Phase 4 is structured as a sequence of small, individually-bisectable commits. The implementation order is:

| # | Commit | Status |
|---|---|---|
| 1 | `docs(project): add Phase 4 plan and D-12 working note` | This commit |
| 2 | `chore(deps): add pyarrow and moto to dev-dependencies` | Pending |
| 3 | `feat(config): add dataset_bucket and replay_dataset_bucket to PlatformSettings` | Pending |
| 4 | `feat(dataset): add PartitionKey and partition helpers` | Pending |
| 5 | `feat(dataset): InMemoryDatasetWriter with window-close buffering` | Pending |
| 6 | `feat(dataset): Parquet serialisation with schema-per-file` | Pending |
| 7 | `feat(dataset): integrate dataset writer into RealtimePipeline` | Pending (design TBD) |
| 8 | `feat(dataset): S3DatasetWriter via moto` | Pending (design TBD) |
| 9 | `feat(dataset): replay isolation via PlatformSettings` | Pending |
| 10 | `feat(dataset): schema evolution across upstream contract versions` | Pending |
| 11 | `docs(dataset): add dataset-export reference document` | Pending |
| 12 | `chore(project): close Phase 4 housekeeping` | Pending |
| 13 | `docs(status): add YYYY-MM-DD phase-4-complete snapshot` | Pending |

Commit 7 integrates the in-memory writer into `RealtimePipeline` before the S3 writer arrives in commit 8. The reasoning matches Phase 3's "build the thing in isolation, then wire it in, then add the AWS edge" pattern — the integration shape is more important than the S3 plumbing, and the in-memory writer lets us shake out the integration before adding moto's complexity.

Commit 9 separates replay-isolation testing from the S3 writer's introduction in commit 8. Both use moto; keeping the replay-isolation integration test in its own commit makes the determinism assertion bisectable and reviewable independently.

The 13-commit count is above the roadmap's 10–14 estimate's midpoint but within range. Probable splits during execution: commit 5 (in-memory writer) may need a separate "buffer state machine" precursor commit before the writer's flush logic; commit 8 may need a separate "boto3 client construction" precursor before the S3-specific writer logic. Both would push the final count toward 14–15, still in range.

## Acceptance criteria

Phase 4 is done when:

- A `signal_forge/dataset/` package exists containing `PartitionKey`, partition helpers, `InMemoryDatasetWriter`, `S3DatasetWriter`, and a Parquet serialisation module.
- `PlatformSettings` exposes `dataset_bucket` and `replay_dataset_bucket` fields; `for_replay()` swaps the active bucket.
- `RealtimePipeline` accepts a dataset writer and hands `ProcessingResult` to it on every `process()` call.
- A replay-isolation integration test runs the same event sequence through live and replay-tagged pipelines, asserting byte-identical Parquet contents written to different buckets.
- A schema-evolution test confirms v1 and v1.1 records in separate files round-trip cleanly without a union schema.
- `docs/dataset-export.md` exists describing design decisions, deferred questions, and the S3 lifecycle policy stub.
- Test count grew from 166 to roughly 215–220 (estimate per the roadmap; refine as we go).
- CI green: ruff, `mypy --strict`, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian security scan.
- PR merged to main; tag `phase-4-complete` pushed.

## Estimated test count delta

Per the roadmap: 166 → ~215–220. Refined breakdown:

- `PartitionKey` and partition helpers: ~8 tests (extraction from each of three source types; degenerate cases; edge cases on `store_id` absence)
- `InMemoryDatasetWriter` buffering: ~12 tests (single-window, multi-window, repair handling, empty `ProcessingResult` handling, ordering invariants)
- Parquet serialisation: ~8 tests (round-trip per record type, schema-per-file confirmation, deterministic output bytes)
- `S3DatasetWriter` via moto: ~10 tests (object key construction, bucket configuration, error paths, multi-partition writes)
- Pipeline integration: ~6 tests (writer called on every result, replay-isolation byte-identity, configuration-absence no-op)
- Schema evolution: ~4 tests (v1 file + v1.1 file in same partition read back correctly, no union schema attempted)
- `PlatformSettings` extension: ~6 tests (default `None`, `for_replay()` swap, validation, bucket-name rules)

Total estimated growth: 166 → ~220. Comfortably within the roadmap's range.

## Cross-references

- `docs/working-notes.md` — D-4 (function-shaped pipeline), D-5 (detections-as-return-value), D-11 (upstream contract evolution as a Phase-N constant). D-12 will be added in commit 1 to capture the `PlatformSettings`-as-switch-home principle.
- `docs/roadmap.md` — Phase 4 design questions, the "Underestimating Phase 4" risk note, and the cross-references to DDIA Chapter 11 (Batch Processing).
- `docs/feature-pipelines.md` — Phase 3's reference document; Phase 4's `docs/dataset-export.md` matches its shape.
- `signal_forge/streaming/realtime_pipeline.py` — `ProcessingResult` shape; what the writer consumes.
- `signal_forge/config/platform_settings.py` — the existing `for_replay()` pattern Phase 4 extends.

## What Phase 4 delivered

> Closing update appended after Phase 4 merged. The sections above record the plan as written at phase start; this section records what was actually built. Discrepancies between the two are honest signals about how Phase 4 unfolded vs how it was scoped.

(To be filled in after merge.)
