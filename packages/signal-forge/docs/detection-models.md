# Detection models

The detection layer turns telemetry events and windowed aggregations into typed `DetectionEvent`s that downstream consumers (Phase 5 alert routing, Phase 4 datasets, Phase 6 dashboard projections) act on. Three detectors ship in Phase 2:

- **`OfflineDetector`** — emits when a device falls silent past a configured threshold.
- **`OutageDetector`** — emits when more than a configured fraction of a store's devices are simultaneously offline.
- **`AnomalyDetector`** — emits when a windowed signal value crosses a configured threshold.

Each detector is small, single-purpose, and stateless about the world outside its own state machine. Alert lifecycle (acknowledgement, reminder cadence, escalation) lives in Phase 5; detector output is the raw signal that lifecycle is built on.

## Common design contract

All three detectors share the same architectural shape. Documenting it once here keeps the per-detector sections focused on what differs.

### Protocols

Two protocols, defined in `signal_forge.detection.protocols`:

- **`EventDetector`** consumes raw `TelemetryEvent`s. Used by detectors that need to see every event, not just windowed aggregates. `OfflineDetector` is the only Phase 2 example.
- **`EmissionDetector`** consumes `WindowEmission`s from a named aggregation. Used by detectors that operate on windowed values. `OutageDetector` and `AnomalyDetector` are the examples.

Both protocols are `typing.Protocol` types (structural typing). A detector class doesn't inherit from the protocol; it just declares the right methods and class variables, and `mypy --strict` verifies conformance at every call site. We deliberately do not use `@runtime_checkable` — `isinstance(obj, Protocol)` only checks method existence, not signature compatibility, so the runtime check would mislead.

### State machine

Every detector tracks one or more entities (devices for `OfflineDetector`, stores for `OutageDetector` and `AnomalyDetector`) through a state machine with two observable states:

- **not-in-detection-state** — represented implicitly by the absence of the entity from the detector's internal state dict.
- **in-detection-state** — represented by the entity's presence in the state dict with a state value like `"offline"`, `"outage"`, or `"anomalous"`.

Transitions:

```
(implicit not-in-state) -- threshold crossed --> in-state -- recovery --> (implicit not-in-state)
                                                    |
                                                    | same threshold-cross condition still holds
                                                    | (already in state)
                                                    v
                                                no emission
```

A transition *into* an in-state state emits one `DetectionEvent`. Subsequent observations that still satisfy the condition emit nothing — once per transition is the contract. Recovery (the entity falling back below the threshold or becoming reachable again) clears state silently. The entity is removed from the state dict on recovery, bounding the dict's size to "entities currently in detection state" rather than "entities ever in detection state".

The state machine deliberately produces **no recovery event**. Reminder cadence, recovery notification, and acknowledgement lifecycle are Phase 5 concerns. The detector's contract is "tell me about state changes"; the alert routing layer's contract is "tell the right human at the right time about the state changes that matter".

A consequence of this design: a device or store that *flaps* (rapidly entering and exiting detection state) produces a fresh `DetectionEvent` on each new entry. The roadmap describes this as the "once per offline transition event" semantics from working note D-7. Long-running fleet operations benefit because the detector is honest about each flap rather than suppressing repeats; humans further down the pipeline get to choose whether flaps are noisy alerts or quiet metrics.

### Replay determinism

Same input sequence always produces the same output sequence of `DetectionEvent`s. Concretely:

- No `datetime.now()`. No system-clock reads.
- All time-arithmetic is derived from event timestamps or window boundaries.
- State machine transitions are determined entirely by inputs.
- Iteration over internal state dicts is in insertion order (Python 3.7+ guarantee).

One piece of detector output is *not* replay-deterministic: `detection_id` and `source_event_id` use `uuid4()`. The output *sequence* is reproducible; the per-detection identity is not. This is acceptable for Phase 7 replay testing because the sequence is the contract; if byte-identical replay outputs become a requirement, derivation from `(detection_type, store_id, window_start)` via UUIDv5 is a possible refinement.

### Trace propagation

Detection events carry a `trace_id` that chains back to the contributing input:

- **`EventDetector`** uses the source event's `trace.trace_id` directly.
- **`EmissionDetector`** uses the emission's `last_contributing_trace_id` (a Phase 1 dataclass extension on `WindowEmission`), falling back to a fresh `uuid4()` only if the emission carries no trace.

The "most recent contributor wins" semantics — pinned by the trace-propagation test in `EmissionDetectorDispatchTest` — means an operator looking at a detection can chase trace IDs backwards through tracing tools to find the events that produced it.

### Failure isolation

A detector raising an exception during `observe_event` or `observe_emission` does not propagate to the pipeline. `RealtimePipeline.process()` catches the exception, logs it with full lineage (`event_id`, detector name, kind, and `aggregation_name` for emission detectors), and continues with the remaining detectors. Strict mode (`RealtimePipeline(strict=True)`) re-raises immediately; used only in replay-validation tests.

This is the pattern called *bulkheading*: bounded blast radius for component failures. A buggy detector can't poison a batch.

## `OfflineDetector`

**Type:** `EventDetector`. **Severity:** `WARNING`.

### Purpose

Emits when a previously-seen device has been silent for longer than `threshold_seconds`. Detects device-level offline conditions: a single headset that's lost connectivity, run out of battery, or been powered down.

### Inputs

- `threshold_seconds: int` — typically `PlatformSettings.offline_threshold_seconds` (default 300s).
- `device_id_extractor: Callable[[TelemetryEvent], UUID | None]` — maps events to their device identifier. Events for which the extractor returns `None` (events that don't identify a specific device) are skipped entirely; they neither register devices nor trigger scans.
- `store_lookup: Callable[[UUID], str | None]` — maps a device to its registered store. Returns `None` for unregistered devices, in which case no detection is emitted (the `DetectionEvent` schema requires non-empty `store_id`). Same shape the future `DeviceRegistry` component will satisfy (working note D-7).

### State machine

```
unseen -- first event --> seen -- silent_gap > threshold --> offline -- any event --> seen
                          (no emission)                      (emit)                   (no emission)
```

The detector's per-device state is `_last_seen: dict[UUID, datetime]` (most recent event timestamp) and `_state: dict[UUID, "seen" | "offline"]` (current state).

### Scan-on-event semantics

Every observed event with an extractable `device_id` triggers two operations:

1. **Update** the device's `_last_seen` to the event's timestamp.
2. **Scan** all known seen devices. For each whose silent gap exceeds the threshold, transition to offline and emit a `DetectionEvent`.

The scanning event becomes the detection's `source_event_id` — the event that *discovered* the offline condition, not the silent device's last event. This is the only way the detector can know "time has advanced" without a wall clock: an event with a later timestamp is the signal.

A consequence: detection latency is bounded by event arrival. In a quiet fleet, a device that goes offline at t=0 may not be detected until any event arrives at t=threshold+1. For the brief's 300s threshold and a 50,000-device fleet, this is operationally fine; for tighter latency requirements, a watermark-observer-driven approach (deferred to Phase 2.5) would be the right answer.

### Edge cases

- **Event without device_id**: extractor returns `None`. Detector skips entirely — no scan, no emission. The event's timestamp does not advance the detector's view of time.
- **Unregistered device**: store_lookup returns `None`. Even after threshold cross, no detection is emitted. The state transition still happens, so the device won't repeatedly fail to emit.
- **Device that flaps**: each `seen -> offline` transition emits a fresh detection with a distinct `detection_id`. Recovery (`offline -> seen`) clears the offline state.

## `OutageDetector`

**Type:** `EmissionDetector` (`aggregation_name="distinct_devices"`). **Severity:** `CRITICAL`.

### Purpose

Emits when more than `threshold_ratio` of a store's registered devices are not reporting in a window. Detects store-level outages: a venue where most or all headsets have stopped reporting, indicating a local network failure, power outage, or similar systemic problem.

Severity is deliberately `CRITICAL` — one step above `OfflineDetector`'s `WARNING`. Single-device offline is normal operational noise; store-level outage is operationally serious and pages on-call. The two-tier severity gives Phase 5 alert routing a natural triage point.

### Inputs

- `threshold_ratio: float` — open interval `(0.0, 1.0)`, validated at construction. Typically `PlatformSettings.outage_threshold_ratio` (default 0.5).
- `registered_count_lookup: Callable[[str], int | None]` — maps a store to its registered device count. Returns `None` for unregistered stores; the detector skips those silently.

The detector requires the pipeline to be configured with a `DistinctCountAggregation` registered under the name `distinct_devices`, keyed by `device_id`. The aggregation counts unique devices reporting per `(store, window)`; the detector's job is to compare that count against the registered fleet size.

### State machine

```
not_outage -- offline_ratio > threshold_ratio --> outage -- offline_ratio <= threshold --> not_outage
              (emit)                                         (no emission, state cleared)
```

State is `_state: dict[str, "outage"]` keyed by store_id. Stores not in the dict are implicitly not-in-outage.

### Threshold semantics

`offline_ratio = 1 - (reporting / registered)`.

The comparison `offline_ratio > threshold_ratio` is strict greater-than. With `threshold_ratio=0.5` and 50 registered devices:

- 25 reporting, 25 offline → `offline_ratio = 0.5`. Does not trigger.
- 24 reporting, 26 offline → `offline_ratio = 0.52`. Triggers.

### Edge cases

- **Unregistered store** (lookup returns `None`): no detection, no state mutation. Catches partition-extraction misconfiguration.
- **Zero registered devices** (lookup returns 0): same as unregistered — no meaningful ratio to compute.
- **More devices reporting than registered** (transient registry drift after new device registration): `offline_count` clamps to 0. No false-positive outage.
- **Sustained outage** across multiple emissions: only the first emits. Subsequent emissions with the store still in outage state are observed but not re-emitted.
- **Flapping store**: each fresh entry into outage state emits a new detection with a distinct `detection_id`.

## `AnomalyDetector`

**Type:** `EmissionDetector` (`aggregation_name="signal_value"` by default). **Severity:** `WARNING`.

### Purpose

Emits when a windowed signal value crosses a configured threshold in a configured direction. The most generic of the three detectors: any aggregation that emits a numeric value per window can be watched by an `AnomalyDetector`.

### Inputs

- `threshold: float` — the value compared against.
- `comparison: "above" | "below"` (default `"above"`) — direction of the anomaly. `"above"` emits when `value > threshold`; `"below"` emits when `value < threshold`. Strict in both directions.
- `signal_name: str` (default `"signal"`) — free-form label surfaced in the detection's `threshold_breached` string and `details`. Operators reading an alert see `"latency_ms 312.0 above threshold 300.0"` rather than the generic `"value 312.0 above threshold 300.0"`.

The `aggregation_name = "signal_value"` default is a class-level constant. Operators tracking multiple signals (latency, error rate, heartbeat count, etc.) subclass `AnomalyDetector` and override `aggregation_name` per signal; each subclass routes independently through the pipeline's emission-detector dispatch.

### State machine

Same shape as `OutageDetector`:

```
not_anomalous -- value crosses threshold --> anomalous -- recovery --> not_anomalous
                  (emit)                                  (no emission, state cleared)
```

State is `_state: dict[str, "anomalous"]` keyed by `partition_key` (typically a store_id).

### Why both directions

"Anomaly" isn't unidirectional. Latency anomalies are *high* values (slow responses are bad); heartbeat anomalies are *low* values (silent devices are bad). The detector handles both with the `comparison` flag rather than splitting into separate `HighAnomalyDetector` and `LowAnomalyDetector` classes. Two reasons: the state machine and dispatch logic are identical; and operators reading code shouldn't have to remember which class is which when both express the same operational pattern.

### Why uniform `WARNING` severity

No escalation logic by overshoot magnitude. A signal 5× over threshold isn't classified as more severe than 1.1× over. Reasons:

- Severity escalation is operationally subjective. The same overshoot ratio means different things for different signals (5× over a latency threshold is bad; 5× over a battery-level threshold is fine).
- The right place to apply escalation is Phase 5 alert routing, where on-call rotations, customer SLAs, and time-of-day context are available.
- A signal that genuinely needs higher severity can subclass `AnomalyDetector` and override `severity` directly.

## What these detectors deliberately do not do

The detectors are small on purpose. Behaviours that look like detector concerns but live elsewhere:

- **Recovery notifications.** When a device comes back online or a store recovers from outage, no detection is emitted. The recovery is observable in the absence of further offline detections; explicit "device recovered" events are Phase 5's job, where acknowledgement state lets us tell the difference between "fixed itself" and "ack'd by a human".

- **Reminder cadence.** A device that has been offline for 8 hours is reported once, not 96 times. The detector tracks state transitions; the question "should we keep reminding the on-call engineer that this device is still offline?" is Phase 5's, where rotation and acknowledgement context exist.

- **Cross-detector correlation.** If `OfflineDetector` fires for 40 devices in store-1 and `OutageDetector` fires for store-1 in the same window, these are two events; the detectors do not de-duplicate or correlate. Correlation is a Phase 6 dashboard projection concern, where rolling aggregations across detection types live.

- **Statistical anomaly models.** `AnomalyDetector` is threshold-driven, not statistical. No rolling z-scores, no EWMA, no isolation forest. The brief's anomaly requirement is satisfied by threshold detection; statistical models are a follow-up project if and when needed (see roadmap).

- **Acknowledgement state.** No "this detection has been acknowledged" flag, no suppression of follow-up detections after ack. Acknowledgement is a Phase 5 concern.

## Cross-references

- `docs/working-notes.md` — design decisions D-5 through D-9 explain the reasoning behind protocols, state machines, schema home, and trace propagation in more depth than this document.
- `docs/architecture.md` — where detection fits in the streaming pipeline; the architectural map.
- `docs/roadmap.md` — Phase 5 alert routing (the consumer of these detections) and Phase 6 dashboard projections (the aggregator).
- `signal_forge/detection/` — the detection package; `protocols.py` for the two protocols, `detectors/` for the three implementations, `types.py` for detection-type constants.
- Commit history on `feat/phase-2-detection-engines`: each detector landed as a single commit with a detailed message describing implementation choices, edge cases, and trade-offs.
