# Replay workflows

> **Status:** As-built (Phase 7). The replay driver is implemented in `stream_pipeline/replay/`. This document describes the realised shape; the determinism contracts the streaming layer guarantees (below) are unchanged from the Phase 1 stub, because they held exactly.

## Why replay exists

The brief mandates deterministic replay/backfill workflows for:

- dataset rebuilds — regenerating historical features after a feature definition change.
- window recomputation — re-emitting aggregates after fixing an aggregation bug.
- feature regeneration — Phase 3.
- alert recomputation — Phase 5.
- schema migration backfills — re-running historical events through new schema-aware handlers.

Replay must integrate with the Step Functions workflow defined in `aws-event-pipeline-infra/modules/eventbridge_replay_workflow`. The infrastructure side is in place upstream; the analytics side is the driver documented here.

## What the streaming layer guarantees

The streaming layer provides the following determinism contracts the replay driver relies on. These were pinned in Phase 1 and held without amendment through to the Phase 7 integration test:

1. **Watermark trajectories are reproducible.** Given the same event sequence in the same order, `WatermarkManager` produces a byte-identical sequence of `WatermarkObservation` values. No `datetime.now()`, no system clock.
2. **Window emissions are reproducible.** Given the same event sequence in the same order, `WindowAggregator` produces a byte-identical sequence of `WindowEmission` values, including any repair emissions. Window boundaries are aligned to the Unix epoch, not to the first event seen, so two replay runs on different machines or at different times produce identical boundaries.
3. **Handler dispatch order is deterministic.** Within a single event's dispatch, handlers run in registration order. Across multiple events in a batch, processing order is the input order.
4. **Aggregator emission order is deterministic.** Multiple aggregators registered on the same pipeline emit in registration order; multiple windows closing on the same observation emit in chronological window-start order.
5. **Settings are reproducible.** `PlatformSettings.for_replay()` produces a deterministic settings snapshot tagged for a replay environment, so the analytical parameters of the original run and the replay run are byte-identical.

Detection and feature identity were made reproducible downstream of these contracts: `detection_id`, `source_event_id`, and the envelope `event_id` are derived via UUIDv5 over their semantic keys, not minted with `uuid4`, so the serialised outputs carry no per-run randomness. This is what lifts "the *sequence* of outputs is reproducible" to "the *bytes* of outputs are identical".

## The driver

The replay driver is `run_replay`, in `stream_pipeline/replay/driver.py`. It is function-shaped over an injected event source and an injected pipeline-builder (working note D-20) — the same form the pipeline itself took (D-4), one level up:

```python
def run_replay(
    settings: PlatformSettings,
    *,
    event_source: Iterable[TelemetryEvent],
    build_pipeline: Callable[[PlatformSettings], RealtimePipeline],
    logger: StructuredLoggerLike | None = None,
) -> list[ProcessingResult]: ...
```

It builds a pipeline from `settings.for_replay()` via `build_pipeline`, feeds the event source through `process_batch` in arrival order, emits one `replay.completed` lineage log line (environment, event count, aggregate result counts), and returns one `ProcessingResult` per event.

Two properties carry the design:

**The driver takes a builder, not a pre-built pipeline.** A pre-built pipeline has already constructed its sinks against some settings, so the driver could not re-point them at replay targets. `for_replay()` must thread into sink *construction*, which happens during registration, so the caller supplies a builder that performs the registration sequence against whatever settings it is handed. The *same* builder constructs the live and replay pipelines; only the settings differ. That "same builder, different settings" property is what makes replay verifiable rather than merely plausible.

**The driver owns the `for_replay()` swap.** The caller passes *live* settings; the driver applies `for_replay()` itself before building. A caller cannot accidentally hand the builder live settings and leak replay output into live sinks — replay-target selection is the driver's, not the caller's.

## The CLI

`python -m stream_pipeline.replay <config.json>` is a thin parse-and-delegate shell over `run_replay`. It reads a JSON config, constructs settings and an event source from it, and delegates. The config shape is the one a Step Functions invocation passes:

```json
{
  "window": {
    "start_time": "2026-06-01T00:00:00+00:00",
    "end_time":   "2026-06-01T01:00:00+00:00",
    "event_pattern": { }
  },
  "settings": {
    "SF_DATASET_BUCKET": "sf-live-dataset",
    "SF_REPLAY_DATASET_BUCKET": "sf-replay-dataset",
    "SF_PROJECTION_TABLE": "sf-live-projections",
    "SF_REPLAY_PROJECTION_TABLE": "sf-replay-projections",
    "SF_ENV": "production"
  }
}
```

The `settings` block is, deliberately, the same `SF_*` vocabulary a live deployment reads from its environment: it is passed straight to `PlatformSettings.from_env(env=...)`, reusing the one validated construction path rather than introducing a second. An operator who knows the deployment's environment variables already knows the replay config.

The config carries *live* names alongside their replay counterparts. It describes the *environment*; the driver performs the *replay isolation*. There is no way for the config to express a broken "replay that writes to the live bucket" state.

## How the two sink types reach isolation

The replay run reproduces the whole control plane, and the two sink types reach their replay isolation by two different mechanisms — the asymmetry D-5 created between sinks that live on the pipeline and projections that live beside it:

- **The dataset writer is registered on the pipeline** by `build_pipeline`. `run_replay` calls `build_pipeline(settings.for_replay())` internally, so the writer reads the already-swapped `dataset_bucket` — isolation happens *inside* the driver, via the builder.
- **Projections are routed by the caller**, after `run_replay` returns its results, via `route_detections`. A replay caller builds its projections from `for_replay()` settings, pointing them at the replay table — isolation happens *outside* the driver, in the routing step.

Both end up isolated; the determinism integration test (`tests/replay/test_replay_determinism.py`) drives both at once through a single `run_replay` call and asserts byte-identical dataset Parquet *and* content-identical projection-table rows, with the live sinks untouched. That is the definition-of-done's replay-determinism item, realised across the full control plane rather than a single sink.

## Deferred as out-of-brief

The definition of done places deployment and production-traffic validation out of scope ("not deployed to AWS", "not validated against production-volume traffic"). Two seams are therefore typed-and-stubbed rather than built:

- **The EventBridge-archive reader** — the production `build_event_source` that iterates the archive over `[start_time, end_time)` filtered by `event_pattern`. The typed seam exists; its AWS body raises `NotImplementedError` with a pointer. Replay runs inject an event source in its place (the integration test feeds a fixed list).
- **The production pipeline builder** — the wiring that registers the dataset writer and projections for a live deployment. The integration test defines the builder that proves the control plane reproduces; a deployed CLI would reference that wiring here.
- **The `replay_audit_table` record** — the root-trace-tagged audit row provisioned upstream. It is infra-adjacent and proves nothing about determinism; deferred with rationale, a small later addition if wanted.

## Non-goals for replay

- Replay does **not** rewrite history. The original run's outputs remain immutable; replay produces new outputs in a separate sink.
- Replay does **not** advance the live watermark. The driver builds its own pipeline with its own `WatermarkManager`, so there is no shared state with any live pipeline. Isolation is the absence of shared state, not a flag.
- Replay does **not** trigger downstream alerts on the live bus. Alert routing is gated on environment; replay runs in a `replay` environment with the alert bus swapped by `for_replay()`.

## See also

- [`architecture.md`](architecture.md) — full pipeline overview.
- [`streaming-internals.md`](streaming-internals.md) — determinism contracts in detail.
- `stream_pipeline/replay/driver.py` — the `run_replay` driver.
- `stream_pipeline/replay/__main__.py` — the CLI shell and the config shape.
- `tests/replay/test_replay_determinism.py` — the whole-control-plane determinism proof.
- `docs/working-notes.md` — D-20 (the replay driver shape), D-5 (projections routed beside the pipeline), D-12 (`for_replay()` as the live-vs-replay switch).
- Upstream `aws-event-pipeline-infra/docs/replay-strategy.md` — Step Functions, archive, and replay infrastructure.
