# Dashboard projections

The dashboard layer turns the detection stream into materialised views suitable for a sub-5-second dashboard. Where the detection layer answers "what just changed?" one event at a time, the projection layer answers "what is the state of the fleet right now?" — current offline counts, active outages, anomaly rates — by folding detections into denormalised, read-optimised views (DDIA Chapter 11). Three projections ship in Phase 6, each a different view *shape*:

- **`OfflineCountProjection`** — a partitioned gauge: how many devices are currently offline, per store.
- **`ActiveOutageProjection`** — a global current-set: which stores are in outage right now, and how many.
- **`AnomalyRateProjection`** — a rolling rate: how many anomaly onsets per signal type over a recent window.

**Input:** `ProcessingResult.detections`, fed in by `route_detections` (below). The projections do not subscribe to the pipeline; they consume its returned detections.
**Storage:** the `ProjectionStore` protocol — `InMemoryProjectionStore` for tests and no-AWS callers, `DynamoDbProjectionStore` in production. The two are interchangeable behind the protocol.
**Update model:** per-emission. Every detection updates the view immediately, so the dashboard is always fresh rather than batch-stale (working note D-15).
**Replay determinism:** every fold is deterministic. The one place a wall clock is needed — "the rate over the last N minutes" — is fenced to the read path via an `as_of` parameter, never read inside a fold.

## The fold must be idempotent

Detections reach a projection over an at-least-once channel: EventBridge re-delivers, and a replay re-runs the same event sequence (the reason the Phase 5 alert idempotency keys exist, D-14). A fold over such a stream must be idempotent or the view rots — a naive `+1 / -1` counter permanently inflates on a duplicated delivery and never heals.

So every projection folds via set membership rather than an accumulating counter (working note D-17). Adding an element already present is a no-op; removing one already absent is a no-op. The view is correct under duplicate delivery, under replay, and under flapping. The element differs by projection — offline device ids, outaged store keys, anomaly detection ids per bucket — but the discipline is the same in all three, and the value is always serialised as a sorted JSON array so the persisted bytes are independent of arrival order.

## `OfflineCountProjection` — a partitioned gauge

A partitioned view: one independent gauge per store, looked up by store id. It holds, per store, the *set* of currently-offline device ids. `device.offline` adds the device to its store's set; `device.online` removes it; the gauge is the set's cardinality.

```
view   = "offline_count"
key    = store_id
value  = sorted JSON array of currently-offline device ids
count  = len(value)
```

When a store's set empties (its last offline device recovers), the key is deleted, bounding the view to "stores with currently-offline devices" rather than "stores ever seen" — the same defensive cleanup `DeviceRegistry` does. Cold start is the store's `None` sentinel: an unqueried store reports zero. The query surface is `offline_count(store_id)`, `offline_device_ids(store_id)`, and `stores_with_offline()` (sorted, for a deterministic rollup).

## `ActiveOutageProjection` — a global current-set

A global view: a single question answered by a fold over the whole stream. It is stored as **one key per outaged store** under a shared view — key-per-presence (working note D-19). `store.outage` writes the store's key; `store.recovered` deletes it; the active set *is* `keys(view)` and the active-outage count is its length.

```
view   = "active_outage"
key    = store_id (one key per store currently in outage)
value  = the outage's detected_at, as free "in outage since" provenance
count  = len(keys(view))
```

The tempting alternative — one fixed key holding the whole JSON set of store ids — was rejected: every transition would be a read-modify-write on that one key, and under a fleet-wide event many stores transition at once, making it a contention point and, on DynamoDB, a hot partition (the classic DDIA Chapter 6 footgun). Key-per-presence makes each transition an independent point write, contends with nothing, and reuses the `keys(view)`-enumerates-the-set pattern `OfflineCountProjection` already uses. The cost accepted is that counting the set is a `keys(view)` enumeration rather than a single read — a bounded scan, since the number of currently-outaged stores is small even in a bad event. Query surface: `active_outage_count()`, `stores_in_outage()`, `is_in_outage(store_id)`.

## `AnomalyRateProjection` — a rolling rate

The third and most interesting shape: a rate of `signal.anomaly` onsets by signal type over a rolling window. Anomalies are once-per-transition, bucketed by `details["signal_name"]` (fleet-wide, aggregating across stores — "anomaly rate by signal type"), with `detected_at` as event-time and `detection_id` as the idempotency token.

### Event time, not wall-clock

A "rate over the last N minutes" needs a notion of *now*, which is in tension with the rule that pure components never read the wall clock (replay determinism, D-2/D-3). The tension is resolved by splitting the clock to the read path:

- `observe()` is pure event-time bucketing. Each anomaly is placed in the bucket its `detected_at` falls in, with bucket boundaries aligned to the Unix epoch exactly as the streaming `WindowAggregator` aligns windows (D-3). No clock, no nondeterminism.
- `count_in_window` and `rate_per_minute` take the caller's *now* as an explicit `as_of` argument and sum the buckets overlapping `[as_of - window, as_of)`. Production passes `datetime.now(UTC)` from the dashboard layer; tests and replay pass a fixed instant.

This mirrors D-14's discipline: the deterministic projection is the bucketed counts; the wall-clock read is fenced into the query.

```
view   = "anomaly_rate"
key    = "{signal_name}|{bucket_start_iso}"   (60s epoch-aligned buckets)
value  = sorted JSON array of detection_ids in that (signal, bucket)
rate   = sum of bucket cardinalities over the buckets overlapping the window
```

Each `(signal_name, bucket)` holds the set of detection_ids that landed in it. `AnomalyDetector` derives `detection_id` over `(partition_key, signal_name, window_start, window_end)`, so anomalies from different stores are distinct ids in the same signal bucket and redeliveries collapse — the set cardinality is the distinct-onset count. The read computes exactly which bucket keys overlap the window and point-gets them (`window / bucket_seconds` gets), so read cost is bounded by the window width regardless of how many historical buckets exist.

### No eviction in the fold

Unlike the other two projections, anomaly-rate buckets do not empty themselves — they age out. This projection does **not** evict (the eviction-(i) decision): `observe()` is a pure bucketing fold with no high-water tracking and no prune-on-write, to keep it consistent with the other folds and free of watermark machinery. The storage bound is owned by the backend — the DynamoDB store sets a TTL on bucket keys. The in-memory store does not evict and grows without bound in a long-running non-DynamoDB process; this is acceptable for tests and for the DynamoDB-backed production path, and is tracked as a known limitation.

## Recovery emissions make the folds possible

Two of the folds need a *clear* signal the detectors did not originally emit. The Phase 2 detectors followed D-7 — emit on entry into a detection state, clear silently on recovery — which was correct when the only consumer (alert routing) could infer recovery from the absence of further detections. A current-state projection cannot: a gauge needs an explicit signal to decrement on.

So two detector enablers were added ahead of the projections that need them:

- `OfflineDetector` emits `device.online` on the offline → seen transition (working note D-16).
- `OutageDetector` emits `store.recovered` on the outage → not-outage transition (working note D-18).

Both were zero-cost upstream changes under the discriminator-pattern schema (D-8): a new `detection_type` string matching the existing constraint, not a contract bump. The pattern is now established twice — a detector's silent clear becomes an emitted recovery event the moment a current-state consumer needs the transition as a signal.

## Routing: beside the pipeline, not in it

Projections are fed by `route_detections(result, projections)` — a thin, bulkheaded helper that lives beside the pipeline. For each detection in a `ProcessingResult`, it offers the detection to every projection in turn; each projection self-filters by `detection_type`, so the router fans out without knowing which projection consumes which type.

This is deliberately *not* pipeline registration (D-5). `AlertRouter` and `DatasetWriter` *are* registered on the pipeline — but only because their output feeds back onto the `ProcessingResult` (alerts) or needs the assembled result (features). A projection produces nothing the result carries and writes to its own store, so it has no reason to run inside `process()`. Keeping it outside is also what makes replay isolation trivial: a replay caller routes to a replay-scoped store, with no pipeline state to gate. Registering projections on the pipeline would have reintroduced exactly the side-effect-on-the-pipeline shape D-5 rejected, and a replay run would mutate live projection state unless gated.

Each `observe()` call is bulkheaded, the same discipline the pipeline applies to aggregators, detectors, the alert router, and the dataset writer: a raising projection is logged (`dashboards.projection_error`) with the detection's lineage and skipped, so one faulty projection cannot stop the others from seeing the detection. `strict=True` re-raises, for replay-validation and integration tests.

## Storage: the `ProjectionStore` protocol

The fold logic never touches AWS. The `ProjectionStore` protocol is a minimal key-value contract keyed by `(view, key)` holding opaque serialised string values — the projection owns its own serialisation, so the store never needs to know the value shape. `get` returns `None` for an absent key (the cold-start sentinel); `keys(view)` returns the keys in a view, sorted. `InMemoryProjectionStore` (dict-backed) and `DynamoDbProjectionStore` are interchangeable implementations, so a projection tested against the in-memory store behaves identically against DynamoDB.

### DynamoDB schema

`view` is the partition key, `key` is the sort key, the serialised value is a `value` attribute. This makes `keys(view)` a `Query` on one partition (bounded, ordered) rather than a full-table `Scan` — the read pattern `ActiveOutageProjection.active_outage_count()` and the anomaly-rate reads depend on. A composite `"view|key"` single partition key was rejected for exactly that reason: it would force `keys` to `Scan`. boto3 lives only in the DynamoDB store module; the in-memory store, which shares the package, imports without it.

### TTL anchored on event-time

The current-state views empty themselves on recovery and must never expire. The anomaly-rate view's buckets age out instead, and that is what DynamoDB TTL is for. The expiry must not come from a wall-clock read in the store — that would put nondeterminism in the persistence layer. Instead it is anchored on the event-time already embedded in the key: an `anomaly_rate` key is `"signal|bucket_iso"`, so the bucket's own event-time is the anchor and `expiry = bucket_time + retention`. The store stays generic: it holds a `TtlResolver` — `(view, key) -> epoch_expiry | None` — and writes the `ttl` attribute only when the resolver returns a value. The default resolver understands the anomaly-rate key shape and leaves the current-state views untouched.

## Replay isolation

A replay must update the views in a replay-isolated table, never touching the live one (DDIA Chapter 11 reprocessing: re-derive history without corrupting the present). The mechanism is the D-12 pattern: `PlatformSettings.for_replay()` swaps `projection_table` to `replay_projection_table` on the returned settings, *unconditionally*. A replay store reads `projection_table` and is replay-oblivious.

The unconditional swap is the safety property. A replay run whose replay table was never configured gets `projection_table=None`, which the store turns into a no-op — writing nothing, anywhere — rather than falling through to the live table. A misconfigured replay fails safe, not dangerous. Because projections are routed beside the pipeline (not registered on it), isolation needs no per-component gating: the replay caller simply points its projections at a store built from `for_replay()` settings.

## What this layer deliberately does not do

The projections are small on purpose. Behaviours that look like projection concerns but live elsewhere:

- **No detection logic.** A projection folds detections into a view; whether a value is anomalous, or a device offline, is the detection layer's job. The projection counts what the detectors decided.

- **No cross-detection correlation.** `OfflineCountProjection` and `ActiveOutageProjection` may both react to the same fleet-wide event; they do not de-duplicate or correlate. Each is an independent view; correlation, if ever needed, is a higher-layer concern.

- **No alerting.** A rising anomaly rate or a growing outage count produces no alert. Alert routing is Phase 5, consuming `ProcessingResult.detections` directly; the projections are read by a dashboard, not by an alerter.

- **No wall clock in the fold.** The only wall-clock read is the `as_of` passed into an anomaly-rate query, at the read edge. Every `observe()` is deterministic.

- **No eviction in the anomaly-rate fold.** Bucket expiry is the backend's job (DynamoDB TTL), not the fold's. The fold only ever adds.

- **No replay-aware routing.** The same `route_detections` feeds live and replay runs. Replay isolation happens at the store the caller points the projections at, not in the router or the projections.

## Cross-references

- `docs/working-notes.md` — design decisions D-15 (per-emission update model), D-16 and D-18 (recovery emissions), D-17 (idempotent set-fold), D-19 (key-per-presence for global views); D-5 (detections-as-returns, the precedent routing follows) and D-12 (`PlatformSettings` as the live-vs-replay switch home).
- `docs/detection-models.md` — the detectors whose detections these projections fold, including the `device.online` and `store.recovered` recovery emissions.
- `docs/architecture.md` — where the projection layer sits in the streaming pipeline.
- `docs/roadmap.md` — Phase 6 scope and the dashboard-freshness requirement.
- `stream_pipeline/dashboards/` — the projection package: `projection_store.py` (protocol + in-memory), the three projection modules, `dynamodb_projection_store.py`, and `routing.py`.
- `stream_pipeline/config/platform_settings.py` — the `projection_table` / `replay_projection_table` fields and the `for_replay()` swap.
- `tests/dashboards/` — per-projection unit tests, the routing integration test, the DynamoDB store test, and the replay-isolation test.
