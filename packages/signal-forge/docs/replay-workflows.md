# Replay workflows

> **Status:** Phase 1 stub. The replay system itself is built in Phase 7. This document pins the contracts the streaming layer (Phase 1) provides for replay to consume, so Phase 7 starts from a known surface.

## Why replay exists

The brief mandates deterministic replay/backfill workflows for:

- dataset rebuilds — regenerating historical features after a feature definition change.
- window recomputation — re-emitting aggregates after fixing an aggregation bug.
- feature regeneration — Phase 3.
- alert recomputation — Phase 5.
- schema migration backfills — re-running historical events through new schema-aware handlers.

Replay must integrate with the Step Functions workflow defined in `aws-event-pipeline-infra/modules/eventbridge_replay_workflow`. The infrastructure side is already in place; the analytics side is Phase 7.

## What the streaming layer guarantees

The Phase 1 streaming layer provides the following determinism contracts that the Phase 7 replay system will rely on:

1. **Watermark trajectories are reproducible.** Given the same event sequence in the same order, `WatermarkManager` produces a byte-identical sequence of `WatermarkObservation` values. No `datetime.now()`, no system clock.
2. **Window emissions are reproducible.** Given the same event sequence in the same order, `WindowAggregator` produces a byte-identical sequence of `WindowEmission` values, including any repair emissions. Window boundaries are aligned to the Unix epoch, not to the first event seen, so two replay runs on different machines or at different times produce identical boundaries.
3. **Handler dispatch order is deterministic.** Within a single event's dispatch, handlers run in registration order. Across multiple events in a batch, processing order is the input order.
4. **Aggregator emission order is deterministic.** Multiple aggregators registered on the same pipeline emit in registration order; multiple windows closing on the same observation emit in chronological window-start order.
5. **Settings are reproducible.** `PlatformSettings.for_replay()` produces a deterministic settings snapshot tagged for a replay environment, so the analytical parameters of the original run and the replay run are byte-identical.

## What replay needs to do (Phase 7 scope, not yet built)

The Phase 7 replay driver will:

1. take a `(start_time, end_time, event_pattern)` window from the Step Functions input,
2. iterate archived events from the EventBridge archive in original arrival order,
3. construct a `RealtimePipeline` with `for_replay()` settings and the same partition extractor / aggregator / handler registrations as the live pipeline,
4. feed events through `pipeline.process_batch()`,
5. route emissions and detection events to a sealed replay sink (S3 bucket distinct from the live pipeline's outputs).

The replay run produces an audit record in the `replay_audit_table` (provisioned upstream) tagged with the `root_trace_id` of the original run, so lineage is preserved.

## Non-goals for replay

- Replay does **not** rewrite history. The original run's outputs remain immutable; replay produces new outputs in a separate sink.
- Replay does **not** advance the live watermark. It runs against an isolated `WatermarkManager` so the live pipeline is unaffected.
- Replay does **not** trigger downstream alerts. Alert routing (Phase 5) is gated on environment; replay runs in a `replay` environment that does not propagate to EventBridge.

## See also

- [`architecture.md`](architecture.md) — full pipeline overview.
- [`streaming-internals.md`](streaming-internals.md) — determinism contracts in detail.
- Upstream `aws-event-pipeline-infra/docs/replay-strategy.md` — Step Functions, archive, and replay infrastructure.