# Status snapshot — 2026-06-04 — Phase 4 complete

> Snapshot captured before the merge of the PR for `feat/phase-4-dataset-export`. The branch is fifteen commits past `phase-3-complete` and ready to merge. Once merged and tagged `phase-4-complete`, this snapshot describes the on-`main` state. The slight time-travel in the framing matches the Phase 2 and Phase 3 snapshots.

## Where we are

`main` will sit at the merge commit for Phase 4's PR. Fifteen commits past `phase-3-complete`:

```
chore(project): close Phase 4 housekeeping
docs(datasets): add dataset-export reference document
test(datasets): schema-per-file round-trips across contract versions
test(datasets): replay byte-identity across live and replay buckets
feat(detection): derive replay-deterministic identity via UUIDv5
chore(deps): bump event-schema-contracts to 5f11c0f
feat(datasets): S3DatasetWriter via moto
refactor(datasets): extract partition buffering into reusable _PartitionBufferSet
feat(datasets): integrate dataset writer into RealtimePipeline
feat(datasets): Parquet serialisation with schema-per-file
feat(datasets): InMemoryDatasetWriter with window-close buffering
feat(datasets): add PartitionKey and partition helpers
feat(config): add dataset_bucket and replay_dataset_bucket to PlatformSettings
docs(project): add Phase 4 plan and D-12 working note
chore(deps): add pyarrow and moto to dev-dependencies
```

247 tests passing. ruff and `mypy --strict` clean across 30 source files. CI is expected green at merge (ruff check, `mypy --strict` on `signal_forge`, pytest matrix on Python 3.11 and 3.12, test-count regression guard now at 247).

## What landed in Phase 4

**Dataset layer** (`signal_forge/datasets/`):

A layer that partitions `ProcessingResult` outputs by `(store_id, hour)` and writes them to S3 in Parquet. The pieces, in dependency order:

- **`PartitionKey` and partition helpers** (`partition.py`) — collapse the three source shapes (window emissions, detections, windowed feature vectors) into a single `(store_id, hour)` tuple. Emissions and features partition on `window_start`, detections on `event_timestamp`; a frozen dataclass so it keys the writer's buffer.
- **`InMemoryDatasetWriter` + `_PartitionBufferSet`** (`writer.py`) — the window-close buffering state machine. Detections and features buffer without flushing; each emission flags its partition; flagged partitions flush in first-seen order and clear. The state machine was extracted into `_PartitionBufferSet` (the predicted commit-8 split) so the in-memory and S3 writers share one correct copy.
- **Parquet serialisation** (`serialisation.py`) — `serialise_partition` produces one table per record type, emissions split one table per `aggregation_name` so the `value` column stays homogeneously typed. Schema-per-file, byte-deterministic given identical input (fixed-order schemas, row sort on replay-deterministic columns only, pinned Parquet version/codec, single row group, JSON-with-sorted-keys for open maps).
- **`S3DatasetWriter`** (`s3_writer.py`) — composes buffering + serialisation + boto3, Hive-partitioned object keys with record-type/aggregation table roots above the partition columns, monotonic per-prefix sequence-number filenames. No-op when no bucket is configured. boto3 lives here and only here.
- **Pipeline integration** (`realtime_pipeline.py`) — `register_dataset_writer` (single writer; second registration raises), success-path dispatch with per-writer failure isolation matching the detector/aggregator pattern.

**`PlatformSettings` extension** (`config/platform_settings.py`):

`dataset_bucket` and `replay_dataset_bucket` (`str | None`), bucket-name validated at construction. `for_replay()` swaps the active bucket unconditionally, so a missing replay bucket no-ops rather than writing to live. Settings carry the bucket *name*, never a client — the stdlib-only discipline survives (decision D-12).

**Replay-deterministic identity** (`signal_forge/identity.py`):

`NAMESPACE` plus a `derive(role, *parts)` UUIDv5 helper. The three detectors and the pipeline's feature-bundling path derive `detection_id`, `source_event_id`, and the envelope `event_id` from stable coordinates instead of minting `uuid4`. This was the unplanned-but-required precursor to the replay byte-identity test: without it, the serialised Parquet carried per-run randomness. Decision D-13 captures the lesson.

**Tests:**

- `tests/test_identity.py` (11) — derive determinism and the three detectors' identity stability.
- `tests/datasets/test_replay_isolation.py` (3) — one event sequence through a live and a `for_replay()` pipeline produces byte-identical Parquet in separate buckets; replay does not touch the live bucket.
- `tests/datasets/test_schema_evolution.py` (4) — a v1 and a later-generation file coexist in one partition; each keeps its own schema, no write-time union, tolerant read-time unification.
- Plus the serialisation, writer, partition, settings, and S3-writer unit tests from commits 3–8b.

**Upstream contribution** (`event-schema-contracts` v0.5.0, PR #4, `5f11c0f`):

Added a `__uuid_v4_or_v5_fields__` policy to `UUIDv4Model` and moved `detection_id` / `source_event_id` into it, so derived UUIDv5 ids validate while the strict-v4 fields (session, device, network, feature ids) stay strict. Backward-compatible (v4 still valid), so a minor bump; `detection.event` schema version unchanged at `v1`. Pinned into this repo at `5f11c0f`.

**Reference documentation** (`docs/dataset-export.md`):

The brief-mandated dataset reference. Per-section design (partitioning, window-close buffering, schema-per-file serialisation and byte-determinism, object keys, configuration/replay isolation, schema evolution), a "what this layer deliberately does not do" section, and cross-references. Mirrors the shape of `detection-models.md` and `feature-pipelines.md`.

## What's deliberately deferred

- **Alert routing (Phase 5).** `ProcessingResult.detections` accumulates; nothing routes it to EventBridge/SNS yet. Phase 5 is the consumer, and will face the D-12 question again for its own bus/topic/replay-sink config.
- **Compaction.** Repair emissions and multiple same-hour windows write additional objects; no compaction job. The "read all files for a partition" model tolerates this for now.
- **Bucket provisioning and the 2-year lifecycle policy.** Declared in `dataset-export.md`, configured upstream in `aws-event-pipeline-infra`; not this repo's job.
- **Byte-identical identity via UUIDv5 was added; richer derivations are not.** The derivation covers the fields the dataset serialises. Any future identity field gets the same treatment.
- **Phase 7 replay driver.** The streaming and dataset layers now provide every determinism contract Phase 7 needs (byte-identical outputs, replay-isolated buckets via `for_replay()`); the Step Functions driver itself is still Phase 7.

## Open issues

- **`structured-logging-python` stdlib-logging warnings during `error()` calls** still surface in tests. Pre-existing since Phase 1; deterministic; not addressed in Phase 4.
- **`event-schema-contracts` ruff backlog** is unchanged by Phase 4's PR #4 (the new code is clean; the backlog is in existing files). `signal-forge`'s CI does not run ruff on the upstream. Note also: the upstream repo's CI runs `mypy` and `pytest` but no ruff job, so that backlog is not gating there.

## Estimate vs actual

The roadmap estimated Phase 4 at 14–18 hours, flagging the S3 mock infrastructure as the likely time sink and "underestimating Phase 4" as a named risk. The S3 mock turned out smooth (moto's `@mock_aws` + a client-factory seam was low-friction). The real overrun came from elsewhere: the replay byte-identity requirement forced the UUIDv5 identity work and an upstream contract relaxation (PR #4) that the plan did not scope — the D-11 "budget for at least one upstream PR per deep-integration phase" pattern recurring exactly as predicted. Test count grew 166 → 247 against an estimated ~220, the surplus mostly the unplanned `tests/test_identity.py`.

## Process observations worth carrying forward

- **Validators, not field types, decide whether a contract change is local (D-13).** "It's just a UUID" skipped the `UUIDv4Model` validator that forbade v5; the change needed an upstream PR, not a local edit. Construct the value you intend to produce against the real contract before building on the assumption.
- **Diff a file against its current version before overwriting from a snapshot.** `realtime_pipeline.py` was overwritten from a pre-commit-7 snapshot and silently lost `register_dataset_writer`; the dataset-writer tests caught it. Captured as a working agreement.
- **The `feat`-vs-`test` scope-tag distinction held.** Commits 9 and 10 are `test(datasets)`, not `feat`, because the feature code shipped in 3–8b and those commits add verification. The plan's `feat` wording was reconciled in the delivered addendum rather than rewritten in the table.

## How to resume

Next session, the standard sequence:

```bash
cd ~/Code/signal-forge
source .venv/bin/activate
git status
git --no-pager log --oneline | head -3
python3 -m unittest discover -s tests 2>&1 | tail -3
git tag --list
```

Expected: branch `main`, working tree clean, top commit is the Phase 4 merge, 247/247 tests, tags include `phase-1-complete` through `phase-4-complete`.

Then start Phase 5 (alert routing): create `docs/phases/phase-5-plan.md` and work through the design questions from `docs/roadmap.md`'s Phase 5 section before any implementation. The roadmap flags the push-vs-pull consumer model, acknowledgement-state storage, and idempotency keys derived from `detection_id` as the major pieces — and per D-12, alert-sink configuration (EventBridge bus, SNS topic, replay-isolated sink) should default to name fields on `PlatformSettings` swapped by `for_replay()`. Budget for at least one upstream PR per D-11.
