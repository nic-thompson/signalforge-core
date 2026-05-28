# Status snapshot — 2026-05-27 — Phase 3 complete

> Snapshot captured before the merge of PR for `feat/phase-3-feature-pipelines`. The branch is ten commits past `phase-2-complete` and ready to merge. Once merged and tagged `phase-3-complete`, this snapshot describes the on-`main` state.

## Where we are

`main` will sit at the merge commit for Phase 3's PR. Ten commits past `phase-2-complete`:

```
docs(status): add 2026-05-27 phase-3-complete snapshot
chore(project): close Phase 3 housekeeping
docs(features): add feature-pipelines reference document
feat(features): bundle aggregation emissions into WindowedFeatureVectorEvents
chore(deps): bump event-schema-contracts to 456269b
feat(streaming): add MeanAggregation
refactor(detection): wire OfflineDetector and OutageDetector tests to DeviceRegistry
feat(detection): add DeviceRegistry component
docs(project): add Phase 3 plan and D-10 working note
chore(deps): bump event-schema-contracts to 5fc7980
```

166 tests passing. ruff and `mypy --strict` clean across 25 source files. CI is expected green at merge (ruff, mypy, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian security scan).

## What landed in Phase 3

**`DeviceRegistry`** (`signal_forge/detection/device_registry.py`):

Router-subscribed projection of `device.registration` events into a live `device_id → store_id` mapping. Subscribed to `("device.registration", "v1")` via the `EventRouter`; each observed registration updates a dual internal mapping (`_device_to_store` and `_store_to_devices`) that backs two O(1) queries. Idempotent on duplicate registrations; handles re-registration to a different store; defensive cleanup of stores that lose all their devices.

The query API matches the constructor-callable shapes Phase 2 detectors use: `store_for(device_id) -> str | None` and `device_count(store_id) -> int`. Phase 2's `OfflineDetector` and `OutageDetector` tests now wire to a real registry via the `tests/_fixtures/registration.py` helper, replacing the dict-and-lambda pattern.

**`MeanAggregation`** (`signal_forge/streaming/window_aggregator.py`):

Fourth aggregation alongside `CountAggregation`, `SumAggregation`, and `DistinctCountAggregation`. State is a `(total, count)` tuple; `finalise()` returns `sum / count` for non-empty windows and `0.0` for empty windows. The empty-window discriminator is `WindowEmission.event_count`, not a sentinel value — downstream consumers that need to distinguish "the mean was zero" from "no contributions" check `event_count > 0` before trusting the value. Non-numeric contributions raise `TypeError`, matching `SumAggregation`'s precedent.

**Feature emissions to `ProcessingResult.features`** (`signal_forge/streaming/realtime_pipeline.py`, `signal_forge/features/__init__.py`):

`RealtimePipeline.process()` now bundles window emissions into `WindowedFeatureVectorEvent`s and exposes them on a new `ProcessingResult.features: list[WindowedFeatureVectorEvent]` field. Bundling groups emissions by `(partition_key, window_start)` and produces one feature event per group, with `feature_values` containing every aggregation's value from that group. Repair emissions produce additional feature events for the same group with updated values.

No `FeatureSink` protocol — features are returns, not side effects. The caller routes to production sinks (Phase 4 dataset writer), test assertions, or replay-isolated buckets. Trace propagation follows the Phase 2 pattern (D-9): best-effort inheritance from the first non-`None` `last_contributing_trace_id`, falling back to a fresh `TraceContext()`.

`FEATURE_SCHEMA_VERSION = "v1"` lives in `signal_forge/features/__init__.py` as a module-level constant. Versioning describes the feature set (which named features the pipeline produces), not the payload schema (which is versioned at the upstream contract layer).

**Two upstream contributions** (`event-schema-contracts` v0.3.0 and v0.4.0):

- **v0.3.0** (PR #2, `5fc7980`): added required `store_id` field to `DeviceRegistrationPayload`. Without this, `DeviceRegistry` couldn't project device→store membership from the event stream. The gap surfaced during DeviceRegistry's design conversation, was caught early enough to be contained, and is documented as D-10's worked example in working-notes.

- **v0.4.0** (PR #3, `456269b`): added `WindowedFeatureVectorPayload` alongside the existing entity-centric `FeatureVectorPayload`. The entity-centric variant requires `entity_id: UUID` and `source_event_id: UUID`, neither of which fits a windowed aggregation across many source events with a string-typed partition key. Rather than shoehorn into the wrong shape, a sibling variant was added. The two payloads now cover semantically distinct use cases (entity-centric for ML feature stores; partition-window-centric for streaming aggregations).

**Reference documentation** (`docs/feature-pipelines.md`):

117-line write-up of the feature-emission design. Explains the bundling semantics, the entity-vs-windowed payload distinction, replay-determinism guarantees, repair-emission handling, trace propagation, and feature schema versioning. Mirrors the structure of `detection-models.md` (per-section design + "what this deliberately does not do" + cross-references).

## What's deliberately deferred

- **`DeviceRegistry` deregistration support.** The brief doesn't describe device deletion. If a device is decommissioned, the registration persists so historical detections still resolve cleanly. A future `device.deregistration v1` event type would be the right addition; not in Phase 3.

- **`QuantileAggregation` and richer aggregations.** Phase 3 ships `MeanAggregation`. Quantiles, exponentially-weighted means, and other statistical aggregations are follow-up work if needed. The aggregation protocol shape supports them without changes.

- **Statistical anomaly models.** Phase 2's `AnomalyDetector` is threshold-driven. Statistical models (z-scores, EWMA, isolation forest) remain a follow-up project per the roadmap.

- **Watermark-observer-driven offline detection.** Phase 2 uses scan-on-event. Phase 2.5 may add watermark observers if measurement shows it's a bottleneck. Not exercised in Phase 3.

- **A Phase 4 dataset sink for `ProcessingResult.features`.** Bundling produces the events; routing them to S3-partitioned Parquet is Phase 4's job. The function-shaped contract means Phase 4 will be a downstream consumer of `result.features`, not a pipeline-internal change.

## Open issues

- **`structured-logging-python` stdlib-logging warnings during `error()` calls** continue to surface in tests. Pre-existing since Phase 1; deterministic; not addressed in Phase 3. Likely fix during Phase 4 or as a focused upstream cleanup PR.

- **`event-schema-contracts` ruff backlog**: now slightly larger after Phase 3's two PRs added new files. The new files themselves are ruff-clean; the backlog is in the existing files. Cleanup PR still queued.

- **`event-schema-contracts` test directory ruff backlog**: surfaced for the first time in Phase 3 (Dict→dict migrations, `dict()` literal C408 warnings, `timezone.utc` → `datetime.UTC` UP017 warnings). The new test file added in PR #3 is clean; the existing `test_feature_vector.py` and possibly others carry the same warnings. Cleanup is part of the broader upstream ruff backlog.

## Estimate vs actual

Phase 3 was estimated at 10-14 hours in the roadmap. Actual was around 14-16 hours of focused work across multiple sessions, plus ~4 hours of upstream PR work that wasn't in the original Phase 3 plan but was the right call. Detailed breakdown in `docs/phases/phase-3-plan.md`'s "What Phase 3 delivered" addendum.

The overrun is consistent with the roadmap's "Underestimating Phase 4" risk being real for any first-deep-integration phase. The pattern will likely persist into Phase 4 (S3 mock infrastructure) and Phase 5 (alert routing's replay-aware sink discipline).

## Process observations worth carrying forward

- **The "paste the contract before writing tests against it" rule extends to semantic contracts, not just type signatures.** Phase 3's feature-emission tests initially used watermark-naïve timestamps (`event_timestamp=110` to close a window at `event_timestamp=105`). The watermark math (`watermark = high_event_timestamp - lateness`) was a contract I should have read before writing the tests. One diagnostic round-trip caught it; codified now.

- **The `Aggregation.name` vs `register_aggregator(name)` footgun is real.** Two aggregations registered under different names but with the same `name` attribute collapse in feature-bundling. Documented in `docs/feature-pipelines.md`. Worth knowing for future Phase-N aggregator additions.

- **Two upstream PRs in one consumer phase is normal for first-deep-integration phases.** Captured as D-11 in working-notes. Future phases should budget for it; if a phase ships without any upstream evolution, review for local workarounds that shoehorned a contract gap rather than fixing it upstream.

- **Status-snapshot writing while-on-branch works.** This snapshot was written before the PR merged, framed as if-already-merged. Phase 2's `f7b4d1e` followed the same pattern. The slight time-travel in the writing is fine; the snapshot is what the post-merge `main` state will be.

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

Expected: branch `main`, working tree clean, top commit is the Phase 3 merge, 166/166 tests, tags include `phase-1-complete`, `phase-2-complete`, and `phase-3-complete`.

Then start Phase 4: create `docs/phases/phase-4-plan.md` and work through the design questions from `docs/roadmap.md`'s Phase 4 section before any implementation begins. The roadmap flags dataset partitioning, S3 export with `moto` or `localstack`, schema-evolution handling for the dataset layer, and compaction strategy as the major pieces. The S3 mock infrastructure is the most plausibly time-consuming part.

Budget for at least one upstream PR per D-11 — Phase 4's dataset layer may need contract evolution as it integrates with the upstream payloads at depth.
