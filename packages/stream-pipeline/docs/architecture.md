# stream-pipeline Architecture

## Purpose

`stream-pipeline` is the analytics, detection, replay and dataset control plane for the SignalForge telemetry intelligence platform. It consumes validated telemetry events produced by `telemetry-parser` and transforms them into:

- detection events (`detection_event`)
- versioned feature vectors (`feature_vector`)
- partitioned analytics datasets (`dataset_record`)
- routed alerts (`alert_event`)
- realtime dashboard KPI views

Logic owned by the upstream contract, parser, logging, and infrastructure repositories is **never** redefined here. `stream-pipeline` integrates with them via stable contracts.

## Position in the SignalForge stack

```
                ┌──────────────────────────────────────────┐
                │              stream-pipeline                │
                │        (this repository — Phase 1)       │
                ├──────────────────────────────────────────┤
                │  realtime pipeline                       │
                │  detection engines (Phase 2)             │
                │  feature pipelines (Phase 3)             │
                │  dataset partitioning (Phase 4)          │
                │  alert routing (Phase 5)                 │
                │  dashboard projections (Phase 6)         │
                │  replay & backfill (Phase 7)             │
                └─────────────────────┬────────────────────┘
                                      │ consumes
                                      │ telemetry_event (BaseEvent[Payload])
              ┌───────────────────────┴───────────────────────┐
              │              telemetry-parser                 │
              │  (raw → validated structured event extraction)│
              └───────────────────────┬───────────────────────┘
                                      │ binds against
              ┌───────────────────────┴───────────────────────┐
              │           event-schema-contracts              │
              │  (BaseEvent envelope, schema registry,        │
              │   semver enforcement, payload schemas)        │
              └───────────────────────────────────────────────┘

  ┌──────────────────────────────────┐    ┌─────────────────────────────────┐
  │   structured-logging-python      │    │   aws-event-pipeline-infra      │
  │  trace propagation, structured   │    │  EventBridge bus, archives,     │
  │  logging across all components   │    │  Step Functions replay, SQS     │
  └──────────────────────────────────┘    └─────────────────────────────────┘
```

`stream-pipeline` depends on all four upstream repositories. The streaming layer documented below is the foundation on top of which detection, features, datasets, alerts, dashboards, and replay are built in subsequent phases.

## Engineering principles

Every module in this repository is:

- **typed** — `mypy --strict` clean.
- **modular** — single-responsibility components with explicit seams.
- **replay-safe** — same inputs in the same order produce byte-identical outputs.
- **schema-aware** — handlers and aggregators register against `(event_type, schema_version)`.
- **deterministic** — no `datetime.now()` in pure components, no nondeterministic iteration affecting outputs.
- **testable** — pure components have no side effects; the orchestration layer takes injectable loggers and partition extractors.
- **observable** — every meaningful step emits a structured log line carrying the inbound event's `trace_id`.
- **version-aware** — schema evolution is explicit; consumer-favouring fallback within the same major version is supported, major-version mismatches are a deliberate boundary.
- **infrastructure-compatible** — usable from Lambda, ECS, Step Functions replay drivers, and unit tests.
- **streaming- and batch-compatible** — the realtime pipeline is function-shaped, not daemon-shaped.

## Realtime pipeline (Phase 1)

The realtime pipeline transforms a stream of `BaseEvent` records into windowed aggregate emissions and dispatched handler invocations. Five components compose it.

```
   inbound BaseEvent
          │
          ▼
   ┌──────────────────────┐
   │ partition extractor  │   (caller-supplied; e.g. by store_id)
   └──────────┬───────────┘
              │ partition_key
              ▼
   ┌──────────────────────┐
   │  WatermarkManager    │   classifies ON_TIME / LATE_TOLERATED / LATE_DROPPED
   └──────────┬───────────┘
              │ WatermarkObservation
              ▼
   ┌──────────────────────┐
   │  WindowAggregator(s) │   tumbling + sliding event-time windows
   └──────────┬───────────┘   late-event window repair
              │ WindowEmission[]
              ▼
   ┌──────────────────────┐
   │  EventRouter         │   schema-aware (event_type, schema_version) dispatch
   └──────────┬───────────┘   consumer-favouring fallback within same major
              │
              ▼
       handler invocations
       (detectors, feature builders,
        dashboard projectors)
```

The `RealtimePipeline` orchestrator wires these together, owns trace propagation, and is the only component in this layer that performs side effects (structured logging).

### Component contracts

#### `EventRouter` — `stream_pipeline.streaming.event_router`

Schema-aware dispatcher. Handlers register against `(event_type, schema_version)`, never against `event_type` alone. Major-version mismatches do not fall back; they are a deliberate breaking-change boundary. Within the same major, fallback is **consumer-favouring**: a v1 handler accepts v1.1 events. This is the opposite direction from the upstream `event_schema_contracts.SchemaRegistry`, which serves producers — the asymmetry is intentional and reflects the role each component plays.

Handler order is registration order. Failures are isolated by default (a bad handler does not poison the stream); strict mode re-raises immediately for replay-validation runs.

#### `WatermarkManager` — `stream_pipeline.streaming.watermark_manager`

Per-key event-time progression. Each partition key (typically `store_id` or `(store_id, device_id)`) maintains its own watermark, defined as `max(event_timestamp_seen) - lateness_tolerance`. Watermarks never retract under any arrival order. Each `observe()` call returns one of:

- `ON_TIME` — at or after the watermark; safe for active aggregations.
- `LATE_TOLERATED` — older than the watermark but within `lateness_tolerance`; included in aggregations and triggers window repair downstream.
- `LATE_DROPPED` — older than `watermark - lateness_tolerance`; dropped from aggregations.

A `lateness_tolerance` of zero is supported and disables late-event correction entirely (strict-ordering mode). The default in production is 60 seconds, configurable via `PlatformSettings.late_event_tolerance_seconds`.

#### `WindowAggregator` — `stream_pipeline.streaming.window_aggregator`

Tumbling and sliding event-time windows with late-event window repair. Tumbling is the special case `size == slide`; sliding is the general case. `slide > size` is rejected because it would create gaps where events belong to no window.

Window boundaries are aligned to the Unix epoch — `floor((event_timestamp - epoch) / slide) * slide` — never to the first event seen. This guarantees:

- replay determinism: two pipeline runs over the same event stream produce identical window boundaries,
- cross-shard joinability: store A and store B emit windows on the same boundaries, so downstream datasets can join them without coordination.

Emission is **watermark-driven**, not event-driven: a window emits when the watermark advances past its right edge. State is retained until the watermark passes `window_end + lateness_tolerance`, after which the window is *sealed* and evicted. Memory is bounded; replays remain deterministic.

A `LATE_TOLERATED` event whose timestamp falls inside a still-retained window updates that window's state and triggers a re-emission tagged `is_repair=True`. Repairs use the same `combine()` code path as initial aggregation — there is no special case.

Aggregations are pluggable via the `Aggregation` strategy. Phase 1 ships `CountAggregation` and `SumAggregation`. Phase 3 (features) adds `MeanAggregation`; Phase 6 (dashboards) adds `DistinctCountAggregation` and quantile aggregations.

#### `RealtimePipeline` — `stream_pipeline.streaming.realtime_pipeline`

Orchestration layer. The only component in the streaming layer that performs side effects, by design — lower layers stay pure so they can be replayed deterministically.

For each event, the pipeline:

1. extracts a partition key via the configured `PartitionExtractor`,
2. observes the watermark manager,
3. feeds `(event, classification, watermark)` into each registered window aggregator,
4. dispatches the event through the router,
5. emits a single structured log line summarising the lineage.

The pipeline is function-shaped (`process(event)` and `process_batch(events)`) rather than daemon-shaped. Event sources — SQS pollers, replay iterators, in-memory test lists — live outside. This is what makes the same pipeline usable in production, replay, and tests without modification.

### Configuration

`stream_pipeline.config.platform_settings.PlatformSettings` exposes the five platform parameters mandated by the brief:

| Setting | Env var | Default | Purpose |
|---|---|---|---|
| `realtime_window_seconds` | `SF_REALTIME_WINDOW_SECONDS` | 5 | Tumbling window size for dashboard freshness. |
| `offline_threshold_seconds` | `SF_OFFLINE_THRESHOLD_SECONDS` | 300 | Device offline detection horizon (Phase 2). |
| `outage_threshold_ratio` | `SF_OUTAGE_THRESHOLD_RATIO` | 0.5 | Fraction of devices offline that constitutes a store outage. |
| `late_event_tolerance_seconds` | `SF_LATE_EVENT_TOLERANCE_SECONDS` | 60 | Watermark lateness budget. |
| `data_retention_days` | `SF_DATA_RETENTION_DAYS` | 730 | Analytics retention horizon (2 years). |

Settings are immutable, range-validated at construction, and expose a `for_replay()` factory that produces a deterministic settings snapshot tagged for a replay environment.

### Trace propagation

Every log line emitted by the realtime pipeline carries `trace_id = event.trace.trace_id`. Stamping is centralised in `RealtimePipeline.process()` — lower layers (router, watermark, aggregator) do not log directly. The structured-logging facade prefers `structured-logging-python`'s `StructuredLogger` when available and degrades to a stdlib backend with the same surface for unit tests.

### Determinism guarantees

The streaming layer makes the following determinism claims, all enforced by tests:

- Given the same event sequence in the same order, the watermark trajectory is byte-identical across runs.
- Given the same event sequence in the same order, the set of window emissions (including repairs) is byte-identical across runs.
- Handler invocations within a single dispatch happen in registration order.
- Multiple aggregators registered on the same pipeline emit in registration order.
- Window boundaries are independent of the first event seen — they are always aligned to the Unix epoch.

These properties are what make replay (Phase 7) viable.

## Future phases

| Phase | Scope | Status |
|---|---|---|
| 2 | Detection engines (offline, outage, anomaly) | not started |
| 3 | Feature pipelines & feature registry | not started |
| 4 | Dataset partitioning, versioning, S3 export | not started |
| 5 | Alert routing (EventBridge, SNS) | not started |
| 6 | Dashboard materialised views | not started |
| 7 | Replay & backfill orchestration | not started |

Each phase will add a corresponding section to this document.

## See also

- [`streaming-internals.md`](streaming-internals.md) — engineering-reference depth on the watermark/window arithmetic, including boundary cases and incident-response notes.
- [`replay-workflows.md`](replay-workflows.md) — how the streaming layer's determinism guarantees feed into Phase 7 replay.
- Upstream `aws-event-pipeline-infra/docs/architecture.md` — EventBridge, SQS, archive, and Step Functions topology.
- Upstream `event-schema-contracts/README.md` — schema versioning and registry semantics.