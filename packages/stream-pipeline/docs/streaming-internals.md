# Streaming layer internals

This document is engineering-reference depth on the realtime pipeline. It assumes you have already read [`architecture.md`](architecture.md). Use this when debugging incidents or implementing a new aggregation.

## Watermark arithmetic

For a partition key `K` and observed events with timestamps `t_1, t_2, …, t_n`:

```
high(K, n)      = max(t_1, ..., t_n)
watermark(K, n) = high(K, n) - lateness_tolerance
```

Watermarks are derived, not stored. The state held per key is the high event-timestamp; the watermark is computed lazily.

### Classification

For an event with timestamp `t` arriving at a key with current watermark `W` and high event-timestamp `H`:

```
if t >= W:                          ON_TIME
elif (W - t) <= lateness:           LATE_TOLERATED       (and lateness > 0)
else:                               LATE_DROPPED
```

The `lateness > 0` clause matters: with `lateness == 0` every event predating the watermark is `LATE_DROPPED`, by design — that is strict-ordering mode.

After classification, `H` is updated to `max(H, t)`. The previous watermark is used for classification; the new watermark reflects the post-observation state. This is what guarantees monotonicity: the classification of event `n` is decided against the watermark *before* event `n` is incorporated, so an arrival of an old event cannot retroactively reclassify subsequent events.

### Boundary cases pinned by tests

- `t == W` is `ON_TIME`. Two events with the same timestamp arriving in either order receive the same classification — required for replay determinism.
- `(W - t) == lateness` is `LATE_TOLERATED`. The boundary is inclusive on the tolerated side; one second further back is `LATE_DROPPED`.
- A first observation for a key (no prior events) produces watermark `H - lateness` for the new high. The previous watermark for classification is the epoch baseline (`datetime.min` UTC), so the first event is always `ON_TIME`.

### Per-key isolation

Each key has its own state. One key's stalled watermark does not affect another key's emission cadence. The `global_watermark()` accessor returns `min(per_key_watermarks)` and is used by the dataset layer (Phase 4) to decide when historical partitions can be sealed.

## Window arithmetic

For window spec `(size, slide)` where `slide` divides `size`:

```
windows containing event at t = {
  start = (floor(t_seconds / slide) - k) * slide
  for k in 0 .. (size/slide - 1)
  if start <= t < start + size
}
```

Where `t_seconds = (t - epoch).total_seconds()`. The number of overlapping windows containing any event is exactly `size / slide`. Tumbling is the case `size == slide`, giving exactly one window.

### Why epoch-relative alignment

Aligning to the Unix epoch (rather than to the first event seen, or to a window-creation moment) gives two operationally critical properties:

1. **Replay determinism.** If you re-run the pipeline over the same event stream, you produce identical window boundaries — byte-for-byte. No "first event seen" state is involved in the alignment computation.
2. **Cross-shard joinability.** Two pipeline shards (one per AWS partition or one per consumer worker) processing different stores emit windows on the same boundaries. The dataset layer (Phase 4) can merge them without coordination.

The cost is that windows do not align to "round" wall-clock times unless you happen to launch the pipeline at one. This is not a real cost — dashboards and reports live downstream of the dataset layer, which can re-window at display time if needed.

### Emission lifecycle

For window `[start, end)`:

```
opened       — first event whose timestamp falls in [start, end)
emitted      — watermark first satisfies watermark >= end
                (emission tagged is_repair=False)
repaired     — LATE_TOLERATED event with timestamp in [start, end)
                arrives while watermark <= end + lateness
                (emission tagged is_repair=True; one repair per arrival)
sealed       — watermark > end + lateness; state evicted
```

Emissions across multiple windows closing on the same observation are returned in chronological window-start order. A repair emission for window W arrives in the same `observe()` call as the late-tolerated event that triggered it, so consumers see the corrected aggregate immediately.

### Boundary cases pinned by tests

- A window emits on the first call where `watermark >= window_end`, even if that's the same call where the window was opened. (Common when an event arrives with a timestamp far ahead of any prior event.)
- An event exactly on a slide boundary (`t == start`) belongs to the window `[start, start+size)` — strictly inclusive of the left edge.
- An event exactly on the right edge (`t == start + size`) does **not** belong to that window — strictly exclusive of the right edge.
- After a window has been emitted but before it has been sealed, a `LATE_TOLERATED` event still updates state and produces a repair emission. The same `combine()` is used; there is no special-case repair code path.

## Aggregation strategies

An `Aggregation` is `(initial, combine, finalise, name)`. State and result types are independent — `MeanAggregation` (Phase 3) will hold `(sum, count)` as state and finalise to `sum / count`.

`combine` must be:

- pure — no side effects, no `datetime.now()`,
- order-independent for commutative aggregations (sum, count, distinct-count),
- strictly typed in its `contribution` argument — `SumAggregation` rejects non-numeric contributions with `TypeError`.

Order independence matters because late-event repair re-applies events out of arrival order. A non-commutative aggregation would produce different results depending on which order repairs arrived; that violates determinism.

## Failure isolation

The pipeline isolates failures at the per-event level by default. The contract is:

| Layer | Default behaviour | Strict mode |
|---|---|---|
| Partition extractor | log `pipeline.extraction_error`, return `extraction_failed=True`, continue with next event | re-raise |
| Watermark observation | not caught — contract violations (empty key, naive timestamp) indicate programming errors | not caught |
| Aggregator | log `pipeline.aggregation_error`, continue with remaining aggregators and dispatch | re-raise |
| Router handler | log `router.handler_error`, continue with next handler | re-raise (router-level strict mode) |

Strict mode is intended for replay-validation runs and integration tests where the goal is to catch silent regressions. Production runs use the default (failure-isolated) mode.

## Memory bounds

- **Watermark manager**: O(number of distinct keys) — one timestamp per key. There is no eviction policy in Phase 1; long-running stores accumulate one entry indefinitely. Phase 7 will add an eviction policy keyed off the global watermark for true backfill workloads.
- **Window aggregator**: O(distinct keys × open windows per key). Windows are evicted past the sealing horizon, so for a steady stream the open count per key is bounded by `(size + lateness) / slide`. For a 5s tumbling window with 60s lateness the bound is 13 open windows per key.
- **Router**: O(registered handlers). No per-event state.

## Incident playbook (sketch)

| Symptom | First check |
|---|---|
| Dashboard freshness > 5s | Are watermarks advancing? Look for `pipeline.processed` log lines per key over the last minute; if a key is missing, investigate event-source delivery for that store. |
| Detection double-firing | Look for `is_repair=True` emissions feeding the detector. The detector may need to be made repair-aware in Phase 2. |
| Watermark stalled for one store | Per-key isolation guarantees other stores are unaffected, but the stalled store needs an explicit advance — Phase 7 adds an admin replay tool. For now, check whether events for that store are being `LATE_DROPPED` upstream. |
| Mass `extraction_failures` in batch summary | Almost always a payload-shape mismatch after a schema bump. Check that the partition extractor's field name still exists on the new schema version. |

## See also

- [`architecture.md`](architecture.md) — start here.
- [`replay-workflows.md`](replay-workflows.md) — how these determinism guarantees support Phase 7 replay.