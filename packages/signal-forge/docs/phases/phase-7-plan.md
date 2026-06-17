# Phase 7 — Replay and backfill orchestration

> **Status.** Branch `feat/phase-7-replay-orchestration` to be created; no commits yet. `main` sits at the Phase 6 merge, tagged `phase-6-complete`. See the most recent file in `docs/status/` for current state. This is the final phase before `v1.0.0`.

## What Phase 7 delivers

The replay driver that turns the project's determinism guarantees into a runnable capability: re-run an archived event sequence through the same processing code, producing outputs in replay-isolated sinks, byte-for-byte identical to the original run. This is the capstone — DDIA Chapter 11/12 reprocessing — and the reason every prior phase held to its determinism discipline (no `datetime.now()` in pure components, epoch-aligned windows, UUIDv5-derived identity, the function-shaped pipeline, `for_replay()` sink swaps, idempotent folds).

The deliverables are:

- **`run_replay`** — the function-shaped driver. Takes platform settings, an injected event source, and an injected pipeline-builder; builds a pipeline from `settings.for_replay()`, feeds the source through `process_batch`, logs the replay lineage, and returns the results. The driver adds orchestration and observability, not logic; determinism is entirely inherited. Recorded as D-20.
- **The event-source contract** — the driver consumes an `Iterable[TelemetryEvent]`. Production backs it with an EventBridge-archive reader; the determinism test backs it with a fixed list. The archive reader is kept thin or deferred (below).
- **The CLI shell** — a `__main__` entry point that parses a JSON replay config (the shape Step Functions passes), constructs settings and an event source, and delegates to `run_replay`. Parse-and-delegate only; no logic.
- **The determinism integration test** — the headline deliverable the definition of done calls out. One `build_pipeline` wiring the dataset writer *and* projections; invoked once with live settings and once through `run_replay`; asserts byte-identical dataset Parquet *and* content-identical projection-table rows, with the live sinks untouched by the replay run.
- **`docs/replay-workflows.md`** — updated from its Phase 1 stub to the as-built driver. The stub already pins the contracts; this phase fills in the realised shape.

One design question is settled (the driver shape, below). The rest of the phase is orchestration over seams that already exist.

## Context and prerequisites

Phase 7 starts from the most favourable position of any phase: the replay-isolation groundwork is already laid in every consumer phase. Phase 4 made the dataset Parquet byte-reproducible (the UUIDv5 identity work was done precisely for this); Phase 5 gated the alert sink on environment; Phase 6 swapped the projection table via `for_replay()`. Phase 7 orchestrates over these seams rather than building new isolation machinery, which is why it is small.

The pipeline is already function-shaped (D-4): `process_batch` takes an iterable and the event source lives outside. The driver inherits that shape one level up — it is function-shaped over an injected source and builder (D-20). No new determinism machinery is introduced.

No new runtime dependencies. `boto3` (already in the `datasets` and `dashboards` extras) is all the archive reader would need, and that reader is thin-or-deferred. No upstream contract evolution is anticipated — unlike Phases 3-5, Phase 7 consumes existing contracts at the orchestration layer, not at depth. If a phase ships without an upstream PR, D-11 says to check for shoehorned workarounds; here the absence is genuine, because the driver is composition over finished components.

## The driver shape — settled design

Settled in the design conversation opening Phase 7; recorded here, with the cross-phase principle in working note D-20.

**1. `run_replay` is function-shaped over an injected source and builder.**

```python
def run_replay(
    settings: PlatformSettings,
    *,
    event_source: Iterable[TelemetryEvent],
    build_pipeline: Callable[[PlatformSettings], RealtimePipeline],
) -> list[ProcessingResult]: ...
```

The body builds a pipeline from `settings.for_replay()`, runs the source through `process_batch`, logs the replay lineage (window, event count, environment tag), and returns the results.

**2. The driver takes a builder, not a pre-built pipeline.**

A pre-built pipeline has already constructed its sinks against some settings, so the driver could not re-point them at replay targets. `for_replay()` must thread into sink *construction*, which happens during registration, so the caller supplies a builder that performs the registration sequence against whatever settings it is handed. The same builder constructs both the live and replay pipelines; only the settings differ. That "same builder, different settings" property is what makes replay verifiable — see the determinism test below.

**3. The event source is injected; the archive reader is thin-or-deferred.**

The driver consumes an `Iterable[TelemetryEvent]`. The real EventBridge-archive reader is AWS plumbing the definition of done explicitly places out of scope ("not deployed to AWS", "not validated against production traffic") and it proves nothing about determinism. It is either a thin, lightly-tested production adapter or deferred to a follow-up; the determinism and orchestration value lives in `run_replay` over an injected source.

**4. Isolation is structural — no flag, no gating.**

The driver builds its own pipeline, so it has its own `WatermarkManager` and aggregator state; there is no shared mutable state with any live pipeline. The only isolation needed is that sinks point at replay targets, which `for_replay()` guarantees (D-12). Isolation is the absence of shared state.

## The determinism test — settled scope

The headline test drives the fullest production-shaped builder, not the narrow dataset-only builder Phase 4's `test_replay_isolation.py` used. One `build_pipeline` wires the dataset writer (Phase 4) and the projections routed via `route_detections` (Phase 6). It is invoked twice over one fixed event source: once with live settings, once through `run_replay`. The assertions:

- **Byte-identical dataset Parquet** across the live and replay buckets, per object key — the Phase 4 canonical artefact, elevated from "the writer is isolated" to "the driver reproduces the run".
- **Content-identical projection-table rows** across the live and replay DynamoDB tables — the anomaly `detection_id` is UUIDv5-derived, so the persisted values match.
- **Live sinks untouched** — a replay-only run leaves the live bucket and live table empty.

This elevates the determinism proof from "the dataset writer is deterministic" (Phase 4) to "the whole control plane reproduces" — which is what the definition of done's replay-determinism item points at.

## Deferred as out-of-brief

- **The EventBridge-archive reader** beyond a thin adapter. Real archive iteration is deployment plumbing in `aws-event-pipeline-infra`, not analytics-control-plane logic.
- **The `replay_audit_table` record.** The root-trace-tagged audit row (per `replay-workflows.md`) is a DynamoDB write to a table provisioned upstream; it proves nothing about determinism and is infra-adjacent. Deferred with rationale, mirroring how Phase 2 deferred watermark observers. A minimal audit hook is a small later addition if wanted.
- **Step Functions wiring.** The CLI accepts the JSON shape Step Functions passes; the workflow definition itself lives upstream.

## Plan

Phase 7 is structured as a sequence of small, individually-bisectable commits:

| # | Commit | Status |
|---|---|---|
| 1 | `docs(project): add Phase 7 plan and D-20 working note` | This commit |
| 2 | `feat(replay): add run_replay driver over injected source and builder` | Pending |
| 3 | `feat(replay): add CLI shell parsing JSON replay config` | Pending |
| 4 | `test(replay): live-vs-replay determinism over the full control plane` | Pending |
| 5 | `docs(replay): update replay-workflows.md to the as-built driver` | Pending |
| 6 | `chore(ci): bump test-count baseline for Phase 7 close` | Pending |
| 7 | `docs(status): add phase-7-complete snapshot` | Pending |

Seven commits, at the low end of the roadmap's 8-12 estimate — appropriate, because the isolation groundwork is done and the phase is composition over finished seams rather than new machinery. Commit 4 is the headline; commits 2 and 3 are the driver and its shell; the rest is documentation and close.

## Acceptance criteria

Phase 7 is done when:

- `run_replay` exists, function-shaped over an injected event source and an injected `build_pipeline`, building from `settings.for_replay()`.
- A CLI entry point parses a JSON replay config and delegates to `run_replay`.
- The determinism integration test runs one fixed event sequence through a live-configured pipeline and through `run_replay`, asserting byte-identical dataset Parquet, content-identical projection-table rows, and live sinks untouched.
- `docs/replay-workflows.md` describes the as-built driver, superseding its Phase 1 stub.
- CI green: ruff, `mypy --strict`, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian.
- PR merged to main via a merge commit (preserving the commit narrative, the Option A convention); tag `phase-7-complete` pushed. With this, the definition of done is satisfied and `v1.0.0` is tagged.

## Estimated test count delta

Per the roadmap: ~360 → ~370 was the original frame, but the suite is already at 371 after Phase 6. Phase 7 is orchestration-heavy and test-light relative to its importance — the determinism integration test is a handful of high-value methods, plus a few for the CLI parse and the driver's lineage logging. Estimate: 371 → ~385.

- `run_replay` driver: ~4 tests (builds with replay settings, runs the source, returns results, logs lineage).
- CLI parse-and-delegate: ~4 tests (valid config, malformed config, the JSON shape round-trips to a `run_replay` call).
- Determinism integration test: ~3 tests (byte-identical Parquet, content-identical projection rows, live sinks untouched) — mirroring the three-test shape of the Phase 4 and Phase 6 replay-isolation tests.

Total estimated growth: 371 → ~385.

## Cross-references

- `docs/working-notes.md` — D-20 (the replay driver shape); D-4 (function-shaped pipeline, the precedent the driver follows); D-12 (`for_replay()` as the live-vs-replay switch); D-5 (detections-as-returns, which `route_detections` and the dataset writer both consume).
- `docs/replay-workflows.md` — the Phase 1 stub this phase realises; the contracts the streaming layer guarantees for replay.
- `docs/roadmap.md` — Phase 7 scope, the definition of done's replay-determinism item, and the `v1.0.0` milestone.
- `tests/datasets/test_replay_isolation.py` and `tests/dashboards/test_replay_isolation.py` — the writer- and projection-level isolation tests the Phase 7 determinism test composes into a whole-control-plane proof.
- `signal_forge/config/platform_settings.py` — the `for_replay()` method the driver threads through the builder.
- `signal_forge/streaming/realtime_pipeline.py` — the function-shaped `process_batch` the driver feeds, and the `RealtimePipeline` the builder returns.
