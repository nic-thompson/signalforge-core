# Store outage detection

This document is the dedicated reference for store-level outage detection — the `OutageDetector` and the operational scenario it exists to catch. It complements [`detection-models.md`](detection-models.md), which covers all three Phase 2 detectors at the level of their shared contract; this document goes deep on the store-outage case specifically: the threshold arithmetic, the registry dependency, the severity choice, and the boundary between what the detector decides and what the alerting and dashboard layers decide.

## The scenario

A SignalForge store is a retail venue running a fleet of telemetry headsets. Individual headsets drop offline routinely — a flat battery, a headset powered down at end of shift, a transient wireless glitch. That device-level noise is the `OfflineDetector`'s domain, and it is expected, low-severity, and operationally normal.

A *store outage* is categorically different: a venue where most or all headsets stop reporting at once. The signature is correlated silence across a store's fleet, and the causes are systemic — a local network failure, a power outage, an upstream connectivity break, a misconfigured site router. One headset offline is noise; forty of a store's fifty headsets offline simultaneously is an event a human needs to know about quickly. The `OutageDetector` exists to separate that signal from the device-level noise and to raise it at a severity that pages on-call.

## Where it sits

`OutageDetector` implements the `EmissionDetector` protocol. It does not see raw telemetry events; it sees `WindowEmission`s from a `DistinctCountAggregation` registered under the name `distinct_devices`, keyed by `device_id`. That aggregation counts how many distinct devices reported in each `(store, window)`. The detector's job is to compare that count against the store's registered fleet size and decide whether the shortfall constitutes an outage.

This division is deliberate. Counting distinct reporters per window is a streaming-aggregation concern and lives in the window layer; deciding whether a count is alarming is a detection concern and lives here. The detector carries no windowing logic of its own — it is a pure function of `(emission, registered_count)` plus a small state machine.

The registered fleet size arrives through a `registered_count_lookup: Callable[[str], int | None]` — a store_id to registered-device-count mapping. In production this is backed by the `DeviceRegistry` (Phase 3); in tests it is any callable of the right shape. The callable returns `None` for stores the registry has never seen.

## Threshold arithmetic

The detector computes, per emission:

```
offline_count = max(0, registered - reporting)
offline_ratio = offline_count / registered
```

and transitions a store into outage when:

```
offline_ratio > threshold_ratio          (strict greater-than)
```

`threshold_ratio` is validated at construction to the open interval `(0.0, 1.0)` — a ratio of exactly 0 or 1 is rejected as a misconfiguration. It is typically sourced from `PlatformSettings.outage_threshold_ratio`, whose production default is `0.5`.

The comparison is **strict greater-than**, which matters at the boundary. With `threshold_ratio = 0.5` and 50 registered devices:

- 25 reporting → 25 offline → `offline_ratio = 0.50` → does **not** trigger (0.50 is not greater than 0.50).
- 24 reporting → 26 offline → `offline_ratio = 0.52` → triggers.

Exactly half the fleet offline is not an outage; more than half is. The strictness is pinned by tests so the boundary cannot drift silently.

### The defensive clamp

`offline_count` is clamped to a floor of zero: `max(0, registered - reporting)`. In correct operation `reporting` never exceeds `registered`, but the registry and the aggregation can drift apart transiently — a device registers mid-window and reports before the registry projection catches up, so for one window `reporting` momentarily exceeds `registered`. Without the clamp that would produce a negative offline count and a nonsensical negative ratio. The clamp treats the transient as "zero offline", which is the correct interpretation: more devices reporting than the registry knows about is not an outage. No false-positive outage is raised from registry drift.

## Severity: CRITICAL, and why

`OutageDetector` emits at `DetectionSeverity.CRITICAL` — one step above `OfflineDetector`'s `WARNING`. This is the most consequential single choice in the detector, and it is deliberate.

Single-device offline is operational noise; it should not page a human at 3am. Store-level outage is operationally serious — a whole venue is dark — and should. The two-tier severity (`WARNING` for device-level, `CRITICAL` for store-level) gives the Phase 5 alert-routing layer a natural triage point: route `CRITICAL` to the immediate page path, `WARNING` to a digest. The detector does not itself decide *how* a `CRITICAL` is delivered — that is alert-routing's job — but it provides the severity signal that makes the triage decidable downstream.

The severity is fixed, not scaled by magnitude. A store 90% offline and a store 51% offline both emit `CRITICAL`; the detector does not classify "more offline" as "more severe". Overshoot-based escalation is operationally subjective and belongs in the alert layer where on-call rotation, customer SLA, and time-of-day context exist — the same reasoning the `AnomalyDetector` applies to its uniform `WARNING`.

## State machine

A store moves through a two-state machine, identical in shape to the other Phase 2 detectors:

```
not_outage  -- offline_ratio > threshold --> outage  -- offline_ratio <= threshold --> not_outage
              (emit one DetectionEvent)                (no emission, state cleared)
```

`not_outage` is represented implicitly by the store's absence from the internal `_state: dict[str, "outage"]`. A store in outage is present in the dict; a store not in outage is absent. The dict's size is bounded by "stores currently in outage", not "stores ever in outage", because recovery deletes the entry.

The contract is **once per transition into outage**. The window that first crosses the threshold emits one `DetectionEvent`; subsequent windows that remain over threshold emit nothing. When a store's offline ratio falls back to or below threshold, the state is cleared silently — recovery is **not** a detection event in Phase 2. A store that flaps (crossing the threshold, recovering, crossing again) emits a fresh detection on each new entry into outage, each with its own derived identity.

This is the D-7 semantics shared across the detectors: the detector reports state *changes*, not sustained state. A store in outage for eight hours produces one detection, not one per window. The questions "should we keep reminding on-call that this store is still dark?" and "tell us when it recovers" are alert-routing concerns (Phase 5), where acknowledgement state and reminder cadence are the right context — not the detector's.

## Detection output

A store-outage detection is a `DetectionEvent` with:

- `detection_type = "store.outage"` (the `DETECTION_TYPE_STORE_OUTAGE` constant).
- `severity = CRITICAL`.
- `store_id` set; `device_id` `None` (a store outage is not about one device).
- `detected_at` and `event_timestamp` set to the emission's `window_end`.
- `threshold_breached` — a human-readable summary, e.g. `"26 of 50 devices not reporting (52% offline, threshold 50%)"`.
- `details` — the structured numbers: `offline_count`, `registered_count`, `offline_ratio`, `threshold_ratio`, and the window bounds as ISO strings.

### Identity is replay-deterministic

`detection_id`, `source_event_id`, and the envelope `event_id` are derived via `event_schema_contracts.base.identity.derive(role, *parts)` (UUIDv5) from stable coordinates — the store_id and window bounds — not minted as `uuid4`. Two runs over the same emission sequence produce byte-identical identities, which is what lets the Phase 4 dataset layer assert byte-identical Parquet across live and replay, and what lets Phase 5 derive a stable alert key from `detection_id`. (This is the post-Phase-4 behaviour; the detector's identity was `uuid4`-based through Phases 2–3 and was made deterministic when the dataset layer's replay byte-identity test required it.)

The trace is propagated from the emission's `last_contributing_trace_id` — the most-recent contributing event's trace — so an operator looking at a store-outage detection can chase the trace backwards to the events that produced the window. If the emission carries no trace (unreachable through the public pipeline API, kept as defensive code), a fresh trace is minted.

## Edge cases

- **Unregistered store** (`registered_count_lookup` returns `None`): skipped silently, no emission, no state mutation. Catches partition-extractor misconfiguration — a partition key the registry has never seen.
- **Zero registered devices** (lookup returns `0`): same treatment as unregistered. `not registered` is true for both `None` and `0`, and there is no meaningful ratio to compute against a zero fleet.
- **Registry drift** (more devices reporting than registered, transient after a new device registers mid-window): the `max(0, …)` clamp makes `offline_count` zero. No false-positive outage.
- **Sustained outage** across many windows: only the first window over threshold emits. The store stays in the state dict; subsequent over-threshold windows are observed but not re-emitted.
- **Flapping store**: each fresh entry into outage emits a new detection with a distinct derived identity. Recovery between entries clears state so the next entry is a genuine transition.
- **Boundary exactly at threshold**: `offline_ratio == threshold_ratio` does not trigger (strict greater-than).

## What store-outage detection deliberately does not do

- **Recovery notifications.** When a store's fleet comes back, no detection is emitted; the recovery is observable in the absence of further outage detections. Explicit "store recovered" signalling is Phase 5's job, where acknowledgement state distinguishes "fixed itself" from "ack'd by a human".
- **Reminder cadence.** A store dark for eight hours is reported once, not once per window. Re-notification cadence is a Phase 5 / operational-scheduler concern.
- **Correlation with device-level offline.** If `OfflineDetector` fires for forty devices in a store and `OutageDetector` fires for that store in the same window, those are two independent detections. The detectors do not de-duplicate or correlate; rolling cross-detection-type correlation is a Phase 6 dashboard projection concern.
- **Cause attribution.** The detector reports that a store is dark, not *why* — network vs power vs upstream connectivity. Root-cause inference is out of scope; the detection is the signal a human (or a richer downstream system) investigates.
- **Magnitude-scaled severity.** Every store outage is `CRITICAL` regardless of how far over threshold it is. Escalation by overshoot is an alert-layer concern, not a detector one.

## Cross-references

- [`detection-models.md`](detection-models.md) — the three Phase 2 detectors and their shared design contract; this document is the store-outage deep-dive that complements it.
- `docs/working-notes.md` — D-7 (once-per-transition state machine), D-8 (`DetectionEvent` schema lives upstream).
- [`architecture.md`](architecture.md) — where detection sits in the streaming pipeline.
- `docs/roadmap.md` — Phase 5 alert routing (the consumer of these detections, where `CRITICAL` becomes a page) and Phase 6 dashboard projections (cross-detection correlation).
- `stream_pipeline/detection/detectors/outage_detector.py` — the implementation.
- `stream_pipeline/detection/types.py` — the `DETECTION_TYPE_STORE_OUTAGE` constant.
- `event_schema_contracts.base.identity` — the `derive` helper backing replay-deterministic detection identity. It lived in `stream_pipeline/identity.py` until August 2026, when it was published upstream so the schema library and its consumers share one definition rather than two that happen to agree.
