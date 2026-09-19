# Status snapshot — 2026-06-16 — Phase 6 complete

> Snapshot captured before the merge of PR for `feat/phase-6-dashboard-projections`. The branch is fifteen commits past `phase-5-complete` and ready to merge. Once merged and tagged `phase-6-complete`, this snapshot describes the on-`main` state.

## Where we are

`main` will sit at the merge commit for Phase 6's PR. Fifteen commits past `phase-5-complete`:

```
chore(ci): bump test-count baseline to 371 for Phase 6 close
docs(dashboards): add dashboard-projections reference document
test(dashboards): replay isolation writes projections to a sealed table, never live
build: add dashboards extra (boto3) for the DynamoDB projection store
feat(dashboards): DynamoDbProjectionStore backs the projection-store protocol with per-view TTL
feat(dashboards): route_detections folds pipeline detections into projections, bulkheaded
feat(dashboards): AnomalyRateProjection counts anomaly onsets per signal in event-time buckets
feat(dashboards): ActiveOutageProjection tracks current outage set via key-per-store-presence (D-19)
feat(dashboards): OutageDetector emits store.recovered on clear (D-18)
chore(ci): bump test-count baseline 295 -> 330
feat(dashboards): OfflineCountProjection folds offline/online into per-store gauge (D-17)
feat(dashboards): OfflineDetector emits device.online recovery (D-16)
feat(dashboards): add ProjectionStore protocol and InMemoryProjectionStore
feat(config): add projection_table and replay_projection_table to PlatformSettings
docs(project): add Phase 6 plan and D-15 working note
```

371 tests passing. ruff and `mypy --strict` clean across 40 source files. CI is expected green at merge (ruff, mypy, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian security scan).

## What landed in Phase 6

**Three projections** (`stream_pipeline/dashboards/`), each a different materialised-view shape over the detection stream:

- `OfflineCountProjection` (`offline_count_projection.py`) — a partitioned gauge. Holds, per store, the set of currently-offline device ids; `device.offline` adds, `device.online` removes, the gauge is the cardinality. The set makes the fold idempotent under at-least-once delivery (D-17): a duplicated `device.offline` is a no-op add, where a `+1` counter would drift permanently. The key is deleted when a store's set empties, bounding the view to currently-affected stores.

- `ActiveOutageProjection` (`active_outage_projection.py`) — a global current-set. Key-per-store-presence (D-19): each outaged store is its own key, the active set is `keys(view)`, the count is its length. `store.outage` writes the key, `store.recovered` deletes it. Rejected the single-hot-key alternative because a fleet-wide event would make it a contention point and a DynamoDB hot partition; key-per-presence makes each transition an independent point write.

- `AnomalyRateProjection` (`anomaly_rate_projection.py`) — a rolling rate by signal type. Event-time bucketing in `observe()` (60s epoch-aligned buckets), each `(signal_name, bucket)` holding the set of detection ids; the read path takes the caller's `now` as an `as_of` argument and sums the buckets overlapping `[as_of - window, as_of)`. The wall clock is fenced to the read, never in the fold (D-2/D-3/D-14 discipline). No eviction in the fold (eviction-(i)); the storage bound is owned by DynamoDB TTL.

**Two detector recovery enablers**, added ahead of the projections that need a clear signal to fold:

- `OfflineDetector` emits `device.online` on the offline → seen transition (D-16).
- `OutageDetector` emits `store.recovered` on the outage → not-outage transition (D-18).

Both were zero-cost upstream changes under the discriminator-pattern schema (D-8) — a new `detection_type` constant and the emission, no contract bump. They revisit D-7 ("recovery clears silently"): a silent clear becomes an emitted recovery the moment a current-state consumer needs the transition as a signal.

**`ProjectionStore` protocol and `InMemoryProjectionStore`** (`projection_store.py`):

A minimal key-value contract keyed by `(view, key)` holding opaque serialised strings; `get` returns `None` for cold start, `keys(view)` returns sorted keys. The fold logic never touches AWS; `InMemoryProjectionStore` and the DynamoDB store are interchangeable behind the protocol.

**`route_detections`** (`routing.py`):

A bulkheaded routing helper that folds a `ProcessingResult`'s detections into a set of projections, fanning each detection out to every projection (each self-filters by `detection_type`). It lives *beside* the pipeline, not registered on it — projections produce nothing the result carries and write to their own store, so unlike the alert router and dataset writer they have no reason to run inside `process()` (D-5). Keeping them out is what makes replay isolation trivial. Each `observe()` is bulkheaded (`dashboards.projection_error`, skip-and-continue), `strict=True` re-raises.

**`DynamoDbProjectionStore`** (`dynamodb_projection_store.py`):

The production backend. `view` is the partition key, `key` the sort key, so `keys(view)` is a `Query` on one partition rather than a `Scan` — the read pattern the active-outage count and anomaly-rate reads depend on. TTL for the anomaly-rate buckets is anchored on the event-time embedded in the key (`expiry = bucket_time + retention`), not a wall-clock read, so bucket expiry stays replay-deterministic; the store holds a per-view TTL resolver and writes the `ttl` attribute only for the anomaly-rate view, never the current-state views. No-op when `projection_table` is `None` (no client built, all ops no-op).

**Replay isolation** (`tests/dashboards/test_replay_isolation.py`):

`PlatformSettings.for_replay()` already swapped `projection_table` ← `replay_projection_table` (D-12); Phase 6 proves it. A live run and a `for_replay()` run over the same event sequence write to separate DynamoDB tables with byte-identical content (the anomaly `detection_id` is UUIDv5-derived, hence deterministic), and a replay whose replay table was never configured writes nothing anywhere — failing safe rather than falling through to the live table.

**Reference documentation** (`docs/dashboard-projections.md`):

The brief-mandated reference document, matching the shape of `detection-models.md` and `feature-pipelines.md`. Covers the three view shapes, the idempotent-set-fold discipline, the event-time/`as_of` split, the routing-beside-the-pipeline contrast with the alert router and dataset writer, the DynamoDB schema, and the event-time-anchored TTL.

**Packaging** (`pyproject.toml`):

A `dashboards` optional-dependency extra carrying `boto3`, parallel to `datasets`, consistent with the per-phase-extra philosophy.

## Design decisions

Recorded as working notes D-15 through D-19:

- **D-15** — projections update per-emission (always-fresh) rather than batched-per-window, the right trade-off for the sub-5-second dashboard requirement.
- **D-16 / D-18** — recovery emissions (`device.online`, `store.recovered`): a detector's silent clear becomes an emitted event when a current-state consumer needs the transition.
- **D-17** — projections fold via idempotent set membership, not accumulating counters, because detections arrive over an at-least-once channel.
- **D-19** — global current-set projections use key-per-presence, not a single hot key, to avoid write contention and DynamoDB hot partitions.

Three further calls are documented in the relevant module docstrings rather than as separate D-notes, being applications of existing decisions: the event-time/`as_of` read fencing (D-2/D-3/D-14 applied), eviction-(i) with DynamoDB-TTL ownership of the bound, and the TTL-anchored-on-event-time scheme.

## What's deliberately deferred

- **The dashboard UI itself.** Only the projections that feed it are in scope; the front end is out of the brief.

- **`QuantileAggregation` and statistical anomaly models.** Phase 6 ships threshold-driven anomaly counting. Quantiles and statistical models (z-scores, EWMA, isolation forest) remain follow-up work per the roadmap.

- **Watermark-observer-driven offline detection.** Phase 2 uses scan-on-event; Phase 2.5 may revisit. Unchanged in Phase 6.

- **`DeviceRegistry` deregistration.** The brief doesn't describe device deletion; registrations persist so historical detections still resolve.

## Open issues

- **`AnomalyRateProjection` in-memory store grows without bound.** Eviction-(i): the fold does not prune. The DynamoDB store bounds bucket storage via TTL; the `InMemoryProjectionStore` does not evict, so a long-running non-DynamoDB process accumulates one key per signal per time bucket indefinitely. Acceptable for tests and the DynamoDB-backed production path; tracked in `docs/working-notes.md`.

- **`structured-logging-python` stdlib-logging warnings during `error()`/`info()` calls** continue to surface in tests, now reproduced from a dashboards integration test too (the end-to-end routing test exercises the pipeline's logging path). Pre-existing since Phase 1; deterministic; cosmetic output pollution, not a failure. Still queued for a focused upstream cleanup.

- **CI runner Node 20 deprecation.** `actions/checkout@v4` and `actions/setup-python@v5` run on Node 20, forced to Node 24 from 2026-06-16 and removed from runners 2026-09-16. CI is green and unaffected for now, but the removal deadline is closer; the fix (bump the action versions) is a small CI PR that should land before September.

- **`event-schema-contracts` ruff backlog.** Pre-existing on the upstream main branch; not in our code. Cleanup PR still queued. Phase 6 added no upstream PRs (the recovery emissions were zero-cost under the discriminator pattern), so the backlog is unchanged.

## Estimate vs actual

Phase 6 was estimated at 10-14 hours in the roadmap. Actual was within range — closer to the lower end than Phases 3-5, because Phase 6 added no upstream contract evolution: the discriminator-pattern schema absorbed both recovery detection types at zero cost, and the `ProjectionStore` protocol (designed in commit 3) meant the DynamoDB backend slotted in behind a stable seam rather than forcing a redesign. The moto/DynamoDB work, flagged by the roadmap as the likely time sink, was contained because the Phase 4 S3 patterns (client-factory seam, no-op-when-unconfigured, `@mock_aws` harness) transferred directly.

## Process observations worth carrying forward

- **`ruff check --fix` on a new file before handoff, not eyeball import-grouping.** Import ordering is the fixer's call; eyeballing isort grouping passed inspection twice while ruff still objected. The cheap, reliable move is to run the fixer on every new file before relying on it being clean.

- **`git add` on an unmodified file silently stages nothing.** The `dashboards` extra slipped out of the DynamoDB store commit because the `pyproject.toml` edit hadn't been applied when `git add pyproject.toml` ran; the commit landed two files instead of three and nobody noticed until the count was read back. The guard is the one already in the working agreements: read the `git status --short` line count against the intended file count *before* committing.

- **The test-count baseline rides the grown-NOTE until phase close, by design.** The baseline sat behind the live count through most of Phase 6 (the `>=` guard passes growth with a NOTE), bumped once at close (330 → 371). This is the agreed pattern, but it means the guard is loose mid-phase; the close bump is a required line-item, not optional housekeeping.

- **moto is pip-installable, so the DynamoDB store was verified against real moto in the sandbox**, not drafted-to-contract. Fourteen logic checks ran against an actual mock table before handoff — round-trip, TTL attribute presence/absence per view, reserved-word query aliasing. Where a dependency is installable, sandbox verification beats draft-to-contract; where it isn't (`event-schema-contracts`), the local run remains the gate.

## How to resume

Next session, the standard sequence:

```bash
cd ~/Code/stream-pipeline
source .venv/bin/activate
git status
git --no-pager log --oneline | head -3
python3 -m unittest discover -s tests 2>&1 | tail -3
git tag --list
```

Expected: branch `main`, working tree clean, top commit is the Phase 6 merge, 371/371 tests, tags include `phase-1-complete` through `phase-6-complete`.

Then start Phase 7 — the last phase to v1.0.0. Create `docs/phases/phase-7-plan.md` and work through the design questions from `docs/roadmap.md`'s Phase 7 section before any implementation. The scope is replay and backfill orchestration: a CLI/Step Functions driver that takes a `(start_time, end_time, event_pattern)` window, iterates archived events from the EventBridge archive in arrival order, feeds them through a `RealtimePipeline` configured with `PlatformSettings.for_replay()`, and routes outputs to sealed replay sinks — the dataset writer's replay bucket (Phase 4), the alert sink's replay gating (Phase 5), and now the projection store's replay table (Phase 6). The headline deliverable is the determinism integration test the definition of done calls out: the same event sequence through the live pipeline and the replay driver, asserting byte-identical outputs.

The replay-isolation groundwork is already laid in every consumer phase — Phase 4's byte-identity test, Phase 5's environment gating, Phase 6's table swap — so Phase 7 is largely orchestration over seams that already exist, rather than new isolation machinery.
