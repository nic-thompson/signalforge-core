# Feature pipelines

The feature layer turns window emissions into typed `WindowedFeatureVectorEvent`s that downstream consumers (Phase 4 dataset layer, Phase 6 dashboard projections, future ML feature stores) act on. Each `RealtimePipeline.process()` call bundles the emissions it produces into feature events and returns them alongside detections and raw emissions, on a single `ProcessingResult`.

**Output type:** `event_schema_contracts.features.windowed_feature_vector.WindowedFeatureVectorEvent` (event type `feature.vector.windowed`, schema version `v1`).
**Replay determinism:** sequence-level (same trade-off as `DetectionEvent`).
**Sink protocol:** none — features are returns.
**Feature schema version:** `stream_pipeline.features.FEATURE_SCHEMA_VERSION` (currently `"v1"`).

## The pipeline produces feature events

Every `RealtimePipeline.process()` call may produce zero or more `WindowedFeatureVectorEvent`s on `ProcessingResult.features`. The list is parallel to `ProcessingResult.detections` and `ProcessingResult.emissions`: three coordinated outputs from the same per-event flow.

Features are returns, not side effects. The pipeline owns no sink registry, no callback list, no environment-aware dispatcher. The caller decides what to do with the returned events — write to a Phase 4 dataset sink in production, assert in tests, route to a replay-isolated bucket during replay. This is the same function-shaped contract used by detections (working note D-5).

## Bundling

### What is bundled

Every emission produced by `process()` carries a `(partition_key, window_start)` tuple. The feature layer groups emissions by that tuple and produces **one** `WindowedFeatureVectorEvent` per group, with `feature_values` containing every aggregation's value from the group. A worked example: a pipeline with two aggregators registered against the same window geometry — `CountAggregation(name="distinct_devices")` and `MeanAggregation(name="mean_latency")` — produces two emissions for store-1's `[100, 105)` window when it closes. The feature layer bundles them into a single event:

```
feature_values = {"distinct_devices": 47, "mean_latency": 312.5}
partition_key  = "store-1"
window_start   = 2026-04-30T12:01:40+00:00
window_end     = 2026-04-30T12:01:45+00:00
```

One event per `(partition, window)` pair regardless of how many aggregations contribute. Downstream consumers see a coherent feature snapshot for the partition-window, not N separate events that need to be re-joined.

### Aggregation names become dict keys

The `aggregation_name` field on each `WindowEmission` becomes the key in `feature_values`. That field is set by the `Aggregation` instance's own `name` attribute, **not** by the name passed to `pipeline.register_aggregator(name, aggregator)`. Two `CountAggregation()` instances registered under different names will both produce emissions with `aggregation_name="count"` (the default) and collide in the bundled dict.

The fix is to construct aggregations with distinct names — `CountAggregation(name="device_count")`, `CountAggregation(name="error_count")` — when bundling multiple of the same type. This is a real footgun; the example in `test_feature_emissions.py` uses `CountAggregation(name="count_b")` to make the point explicit.

### When bundling happens

Inside `RealtimePipeline.process()`, just before `ProcessingResult` is constructed. The helper `_bundle_feature_events(emissions: list[WindowEmission]) -> list[WindowedFeatureVectorEvent]` is module-private and is the only caller of `WindowedFeatureVectorPayload` and `WindowedFeatureVectorEvent` in the stream-pipeline codebase. Bundling is a pure function of the per-call emissions list; it has no state, no clock, no side effects.

A `result.features` accessor (lazy computation) was considered and rejected: parallel materialised lists is more honest about cost, easier to test, and consistent with how `detections` works.

## Two upstream payload variants

Upstream `event_schema_contracts.features` ships two payload variants. The distinction matters because the names are similar and the use cases are not.

| Payload | Lives at | Purpose | Partition / Entity field |
|---|---|---|---|
| `FeatureVectorPayload` | `features/feature_vector.py` | Entity-centric: ML feature stores, online inference, offline dataset generation. One feature snapshot describing one entity at one timestamp. | `entity_id: UUID` (plus `source_event_id: UUID`) |
| `WindowedFeatureVectorPayload` | `features/windowed_feature_vector.py` | Partition-centric: streaming aggregations, dataset projectors, dashboard rollups. One feature snapshot describing a partition over a bounded event-time window. | `partition_key: str` (no `source_event_id`) |

The streaming pipeline uses `WindowedFeatureVectorPayload` exclusively. Phase 3 added it via upstream PR #3 (event-schema-contracts v0.4.0) after the entity-centric variant proved a poor fit — windowed aggregations have no single source event and partition by string keys, not UUIDs. The upstream commit message (`d72f279`) carries the full rationale.

Future ML feature pipelines (none planned in this brief) would use `FeatureVectorPayload`; they are out of scope for the streaming control plane.

## Replay determinism

Sequence-level: the same input event stream always produces the same number of `WindowedFeatureVectorEvent`s in the same order with the same `partition_key`, `window_start`, `window_end`, `feature_values`, and `feature_version` payload fields.

Per-event identity is **not** replay-deterministic. `WindowedFeatureVectorEvent.event_id` uses `uuid4()` (the `BaseEvent` default). Two runs of the same input will produce events with different `event_id`s but otherwise-identical payloads. This is the same trade-off accepted for `DetectionEvent` in working note D-5; the brief's replay-determinism requirement is satisfied by sequence-level determinism, and per-event identity is incidental.

If byte-identical replay outputs ever become a requirement, deriving `event_id` from `(partition_key, window_start, feature_version)` via UUIDv5 would be a contained refinement. Not needed for Phase 7's planned replay verification.

## Repair emissions

A late-arriving event (classified `LATE_TOLERATED` by the watermark manager) whose timestamp falls inside an already-closed window triggers a repair: the aggregator re-emits the window with updated state and `is_repair=True`. The feature layer bundles repair emissions the same way it bundles first-closures, producing an additional `WindowedFeatureVectorEvent` for the same `(partition_key, window_start)` pair with updated `feature_values`.

Downstream consumers will see two events for the same partition-window: one from the original closure, one from each repair. Idempotency, replacement semantics, or last-write-wins behaviour is the consumer's concern — exactly as it is for `WindowEmission.is_repair` itself. The feature layer surfaces the repair faithfully; it does not deduplicate.

For the Phase 4 dataset writer, this likely means writing both events and letting the downstream query layer handle deduplication via partition-and-window keys plus a timestamp tie-breaker. For Phase 6 dashboards, repairs may update materialised projections in place. The right answer is layer-specific and not decidable here.

## Trace propagation

Each `WindowedFeatureVectorEvent` carries a `TraceContext` chained back to a contributing input. The bundling helper applies a best-effort policy:

1. Scan the group's emissions in order. Find the first emission whose `last_contributing_trace_id` is not `None`.
2. If found, construct the event's `TraceContext` with that trace_id (parsed back into a `UUID`).
3. If no emission has a trace, construct a fresh `TraceContext()` with a new UUID.

The fallback path (case 3) is unreachable through the public pipeline API: every event reaching `process()` has a `trace.trace_id`, and the aggregator records it on the window state. The fallback is defensive code against a hypothetical future event source that produces traceless events, kept for the same reasons as `MeanAggregation`'s defensive `count > 0` guard.

The pattern mirrors emission detectors' trace handling in Phase 2 (working note D-9): an operator looking at a windowed feature event can chase its trace_id backwards through tracing tools to find the events that contributed to it, even though the feature is an aggregation of many.

## Feature schema versioning

`stream_pipeline.features.FEATURE_SCHEMA_VERSION` is the constant written into every bundled event's `feature_version` field. It stays at `"v1"` until the produced feature set changes incompatibly — for example, renaming an aggregation, changing the semantics of an existing feature value, or changing the dict key conventions.

The version describes the **feature set**, not the **payload schema**. Payload-schema versioning lives at the upstream contract layer (`WindowedFeatureVectorEvent.__schema_version__`) and would only change if the wrapping event's fields changed. The two version strings travel together but mean different things; documenting both here so a future reader doesn't conflate them.

Adding a new aggregation does **not** require a feature-version bump: existing consumers see the new key in `feature_values` and either ignore it (forward compatibility) or use it. Removing an aggregation, renaming one, or changing its semantics **does** require a feature-version bump.

## What this layer deliberately does not do

The feature layer is small on purpose. Behaviours that look like feature-layer concerns but live elsewhere:

- **No sink protocol.** Features are returns. Production callers wire `result.features` to a Phase 4 dataset writer; test callers assert on them; replay callers route them to a sealed replay bucket. The pipeline does not know which is which.

- **No persistence.** No S3, no DynamoDB, no in-memory cache of past feature events. The Phase 4 dataset layer owns persistence; the feature layer owns construction.

- **No filtering.** Every closed window with emissions produces a feature event. Filtering by partition, by aggregation, by feature-value threshold is downstream — Phase 4 might filter for dataset partitioning; Phase 6 dashboards might filter by store-of-interest. The feature layer surfaces everything and lets consumers narrow.

- **No anomaly logic.** A `WindowedFeatureVectorEvent` carries the value of an aggregation; whether that value is anomalous is `AnomalyDetector`'s job (running in parallel and producing its own `DetectionEvent`).

- **No replay-isolated routing.** The same `result.features` list comes out of live and replay runs of the pipeline. Replay isolation happens at the caller's routing layer — in production, the caller wires to a live sink; in replay, the caller wires to a sealed bucket. The pipeline stays oblivious.

- **No reconciliation across calls.** Each `process()` call's `result.features` is the events produced by **that call only**. If a window closes in call N and is repaired in call N+5, those produce events in different `ProcessingResult`s; the feature layer does not maintain a buffer or merge across calls.

## Cross-references

- `docs/working-notes.md` — design decisions D-5 (detections-as-returns, the precedent feature emissions follows) and D-9 (trace propagation through window emissions, the pattern mirrored here).
- `docs/architecture.md` — where feature emission fits in the streaming pipeline; the architectural map.
- `docs/roadmap.md` — Phase 4 dataset layer (the production consumer of these features) and Phase 6 dashboard projections (a likely additional consumer).
- `stream_pipeline/streaming/realtime_pipeline.py` — `_bundle_feature_events` helper and the two `ProcessingResult` construction sites.
- `stream_pipeline/features/__init__.py` — the `FEATURE_SCHEMA_VERSION` constant.
- `event_schema_contracts/features/windowed_feature_vector.py` — the upstream payload contract.
- Upstream commit `d72f279` (event-schema-contracts v0.4.0) — the contract evolution that introduced `WindowedFeatureVectorPayload`. Consumer SHA-bump landed in stream-pipeline `9dcee74`.
- `tests/streaming/test_feature_emissions.py` — seven integration tests covering bundling, multi-aggregation collapse, partition separation, repair re-emission, trace propagation.
