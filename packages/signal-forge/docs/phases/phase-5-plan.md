# Phase 5 — Alert routing

> **Status.** Branch `feat/phase-5-alert-routing` exists locally; no commits yet. `main` sits at `2482316` (the store-outage-detection doc merge, above the `phase-4-complete` tag on `a58722c`), green at 247 tests across Python 3.11 and 3.12. See the most recent file in `docs/status/` for current state.

## What Phase 5 delivers

A consumer of `ProcessingResult.detections` that turns detections into routed alerts, severity-aware, replay-isolated, and acknowledgement-aware. The deliverables:

- **`alert.event v1` and `alert.acknowledgement v1` schemas** — upstream in `event-schema-contracts`. Routed alerts and acks are platform-wide contracts: they flow through EventBridge, get archived, and are replayed. The D-8 reasoning that put `detection.event` upstream applies identically.
- **`AlertRouter`** — registered into `RealtimePipeline` via `register_alert_router`, fed `ProcessingResult` after each `process()`, reading `result.detections` and producing routed `AlertEvent`s. Severity-aware: `CRITICAL` to the page path, `WARNING` to the digest path. Per-router failure isolation matching the detector/aggregator/dataset-writer pattern.
- **`AcknowledgementRegistry`** — a router-subscribed projection of `alert.acknowledgement` events into live ack state, exactly the `DeviceRegistry` pattern. Replay-deterministic because acks ride the event stream.
- **Alert identity** — a stable alert key derived via the Phase 4 `signal_forge.identity` spine (`derive("alert", str(detection_id))`), carried as a first-class field on the `AlertEvent` payload so the ack event can reference it.
- **`PlatformSettings` extension** — `alert_bus` and `alert_topic` (`str | None`) name fields, swapped by `for_replay()` to a sealed/`None` target, with the boto3/EventBridge/SNS client constructed in the router. D-12 to the letter.
- **`docs/alert-routing.md`** (or `docs/store-outage-detection.md` if the brief's document name is taken literally — confirm against the definition-of-done list) — reference document matching the shape of `docs/dataset-export.md`, `docs/feature-pipelines.md`, and `docs/detection-models.md`.

Several design points are settled below. Three remain open and will be settled in focused design conversations before the relevant commit.

## Context and prerequisites

Phase 5 starts from a position similar to Phase 4's: the upstream contracts are mature, but at least one upstream PR is expected (D-11). For Phase 5 the upstream work is larger than usual — two new schemas (`alert.event v1`, `alert.acknowledgement v1`) rather than a field relaxation — so budget the upstream PR as a first-class piece of the phase, not an afterthought. Upstream tip is `v0.5.0`; the new schemas land as a minor bump (additive, no existing schema changes).

The pipeline's `ProcessingResult.detections` already carries what the router consumes — D-5 ("detections flow back as a return value, not as side effects") was specifically designed so Phase 5 alert routing could be a thin consumer. This phase exercises that intent, the same way Phase 4's writer exercised it for datasets.

The Phase 4 identity work is a quiet prerequisite-already-met: `detection_id` is now UUIDv5-derived and replay-deterministic, so any alert key derived from it is stable across live and replay without extra machinery. Phase 5 inherits that for free.

`for_replay()` already centralises the live-vs-replay switch (D-12). Phase 5's job is to add `alert_bus`/`alert_topic` to the fields it swaps, not to invent a new switching mechanism.

## Push routing via the pipeline — settled design

Three consumer models were considered: push (the pipeline calls the router after each `process()`), pull (a separate process polls detections from a queue), and a hybrid.

**Decision: push, via `register_alert_router`.**

**1. Push matches the established consumer shape.** Phase 4's dataset writer set the precedent: a consumer registered into the pipeline, fed `ProcessingResult` after each `process()`, with replay isolation achieved by swapping the sink *target* via `for_replay()` rather than by keeping the consumer out of the pipeline. `register_alert_router` mirrors `register_dataset_writer` exactly — same registration shape (lines 366–392 of `realtime_pipeline.py`), same per-consumer failure isolation, same strict-mode re-raise.

**2. Pull's only real advantage is already bought.** The roadmap noted pull "has merit for replay isolation". That merit is already delivered by the config swap: a replay run routes to a sealed/`None` sink and never touches the live on-call, satisfying the `replay-workflows.md` non-goal ("replay does not trigger downstream alerts"). Pull's remaining advantage — deployment decoupling — costs the function-shaped-pipeline simplicity (D-4) and adds queue infrastructure for no determinism benefit.

**3. The router is a thin adapter, by design.** It reads `result.detections`, consults ack state, and produces `AlertEvent`s. It owns no detection logic, no windowing, no state beyond what the ack registry projects. This is exactly the role D-5 was written to enable.

## Acknowledgement state and the cadence boundary — settled design

This is the phase's central determinism decision, and it splits cleanly into two halves with a hard line between them.

**1. Acknowledgement state is a replay-deterministic event projection.**

An `alert.acknowledgement v1` event type flows through the same event stream as everything else. An `AcknowledgementRegistry` subscribes to `("alert.acknowledgement", "v1")` via the `EventRouter` — the same mechanism `DeviceRegistry` uses — and projects acks into live state. Because acks are events in the stream, ack state is a deterministic function of input order: a replay reconstructs byte-identical ack state, and the determinism contracts in `replay-workflows.md` hold without exception. D-5's "same input sequence produces same detection sequence" extends to "same input sequence produces same ack-resolved routing decisions".

**2. Reminder cadence is wall-clock and lives outside the deterministic path.**

"Should we re-page about this still-unacked alert every N minutes?" is a wall-clock question. It cannot be replay-deterministic — cadence fires on elapsed real time, not on event arrival — and putting a timer inside `RealtimePipeline` would reintroduce both the daemon shape D-4 forbids and the `datetime.now()` the spine forbids everywhere else.

So Phase 5 **names and bounds** the cadence scheduler but does not build the wall-clock loop inside the pipeline. The reference document describes the seam where an operational scheduler attaches (consuming routed alerts and current ack state, firing reminders on its own clock); the scheduler itself is a follow-up (Phase 5.5 or a dedicated ops component), explicitly outside the replay path. A replay run, routing to a sealed sink, never exercises cadence at all — there is nothing to re-page.

This is decision **D-14** (lands in commit 1 with this plan): *acknowledgement state is a replay-deterministic event projection; reminder cadence is wall-clock and lives outside the deterministic path.* It belongs in the decision log because "complete the alerting layer" naturally tempts a future phase to put a reminder timer in the pipeline, and that temptation is exactly the determinism-boundary violation the spine has avoided everywhere else.

## Idempotency keys via the identity spine — settled design

**1. The alert key derives from `detection_id` via the Phase 4 identity helper.**

`derive("alert", str(detection_id))` — a UUIDv5 in the `signal_forge.identity` namespace. Because `detection_id` is already replay-deterministic (Phase 4), the alert key is automatically stable across live and replay runs, so downstream dedup in EventBridge/SNS works without bespoke key machinery.

**2. The alert key is its own coordinate, not an overload of `detection_id`.**

Deriving a distinct `alert`-namespaced key rather than reusing `detection_id` directly keeps the alert's identity a first-class thing rather than borrowing the detection's. It also gives the ack event a clean, stable target to reference: the ack carries the alert key, not the detection id, so "this ack resolves this alert" is expressed at the alert layer where it belongs.

## Configuration as PlatformSettings name fields — settled design

Follows D-12 exactly, the same shape Phase 4 used for `dataset_bucket`/`replay_dataset_bucket`.

**1. `alert_bus` and `alert_topic` are `str | None` fields on `PlatformSettings`**, defaulting to `None` ("no alert routing configured"). `for_replay()` swaps them to their replay-isolated targets (or `None`), so the router reads `settings.alert_bus` and does not care whether it is running live or replay.

**2. Names, not clients.** The settings carry the EventBridge bus name and SNS topic ARN as strings; the boto3 client is constructed in the router. The module's stdlib-only discipline survives, as it did through the Phase 4 bucket fields.

**3. Validation at construction.** ARN/name shape validated in `__post_init__` the same way the bucket fields validate against AWS S3 naming rules — fail fast at startup, not three hours into a replay.

## Acknowledgement gating vs annotation — settled design

Does the router *gate* on ack state (suppress routing of an already-acked detection's alert) or *annotate* (always route, stamp `acknowledged: true/false` and let the downstream scheduler suppress)?

- **Gating** is more useful at the router but couples the router to ack state and raises an out-of-order question: an ack arriving before the alert it acknowledges has been routed.
- **Annotation** keeps the router a pure function of `(detection, ack_state)` with no suppression logic, pushing the suppress-or-not decision to the cadence scheduler — which is already the home of wall-clock suppression under D-14.

**Decision: annotate.** The `AlertRouter` always produces an `AlertEvent` per detection, stamped with acknowledgement status, and never suppresses. Suppression — declining to re-page about an already-acknowledged alert — lives in the downstream cadence scheduler, the wall-clock component D-14 already fences outside the deterministic path.

This keeps the router a pure function of `(detection, ack_state)`: same inputs, same `AlertEvent`, every time, which is the determinism property the phase rests on. It also dissolves the out-of-order problem rather than fighting it — an ack arriving before its alert is routed needs no special handling, because the router simply stamps current ack state at routing time and a later change is the scheduler's concern, not a routing-correctness one. And it loses no information: the annotation carries the full ack signal downstream, where the cadence, rotation, and time-of-day context needed to act on it actually live.

Mechanism: the acknowledgement status is carried in the `AlertEventPayload.details` dict (e.g. `details={"acknowledged": false, ...}`), not as a dedicated payload field. The `alert.event v1` contract is already fixed and has no acknowledgement field; `details` is the opaque, forward-compatible pass-through for exactly this kind of consumer-facing annotation, consistent with how the detection contract uses its own `details`. No schema change is needed.

## Alert-key payload field — question to settle

The ack event must reference the alert it acknowledges, so the alert key has to be (a) on the `AlertEvent` payload as a first-class, stable field, and (b) reproducible by whoever emits the ack. The identity spine gives reproducibility; this question is the schema shape — is the alert key the payload's identity field, or a separate `alert_key` field alongside the envelope `event_id`? Pins down part of the upstream `alert.event v1` schema. To be settled before the upstream schema commit. Estimated 15 minutes of design.

## Severity-to-sink mapping — question to settle

`CRITICAL` pages immediately; `WARNING` digests. The open question is the concrete sink topology: separate SNS topics per severity, one topic with a severity attribute for subscriber filtering, or EventBridge rules keyed on severity. Probably EventBridge-rules-on-severity (keeps the routing declarative and upstream-of-the-router), but worth confirming against what `aws-event-pipeline-infra` already provisions. To be settled before the router's sink-dispatch commit. Estimated 20 minutes of design.

## Plan

Phase 5 is structured as a sequence of small, individually-bisectable commits. The implementation order:

| # | Commit | Status |
|---|---|---|
| 1 | `docs(project): add Phase 5 plan and D-14 working note` | This commit |
| 2 | `feat(config): add alert_bus and alert_topic to PlatformSettings` | Pending |
| 3 | `chore(deps): bump event-schema-contracts to <sha>` | Pending (after upstream PR) |
| 4 | `feat(alerts): add AlertRouter and ProcessingResult plumbing` | Pending (design TBD) |
| 5 | `feat(alerts): add AcknowledgementRegistry projection` | Pending |
| 6 | `feat(alerts): derive alert keys via the identity spine` | Pending |
| 7 | `feat(alerts): severity-aware sink dispatch` | Pending (design TBD) |
| 8 | `feat(alerts): integrate AlertRouter into RealtimePipeline` | Pending |
| 9 | `feat(alerts): replay isolation via PlatformSettings` | Pending |
| 10 | `test(alerts): replay byte-identity of routing decisions` | Pending |
| 11 | `docs(alerts): add alert-routing reference document` | Pending |
| 12 | `chore(project): close Phase 5 housekeeping` | Pending |
| 13 | `docs(status): add YYYY-MM-DD phase-5-complete snapshot` | Pending |

The upstream PR (two schemas: `alert.event v1`, `alert.acknowledgement v1`) lands before commit 3, the consumer SHA bump. It may be two commits within one PR (one schema each) or one — decide when drafting it.

Commit ordering mirrors Phase 4's "build in isolation, then wire in, then add the AWS edge": the router and registry land as pure components (4–6), integration into the pipeline follows (8), and replay isolation gets its own commit (9) so the determinism assertion is bisectable, exactly as Phase 4's commit 9 was.

## What's deliberately not in Phase 5

- **The wall-clock reminder-cadence scheduler.** Named and bounded (D-14); built as a follow-up outside the deterministic path. Phase 5 ships the routing core and ack projection; cadence is Phase 5.5 or an ops component.
- **Acknowledgement UI / human-facing ack mechanism.** The `alert.acknowledgement` event is the contract; how a human emits one (a button, a CLI, a Slack action) is not this repo's job.
- **DynamoDB operational store.** Ack state is an in-memory event projection (replay-deterministic). Any operational persistence of ack/cadence state belongs to the scheduler, outside the pipeline.
- **Cross-detector alert correlation.** Two detections for the same store produce two alerts; de-duplication/correlation is a Phase 6 dashboard projection concern, as established for detections in Phase 2.

## Acceptance criteria

Phase 5 is done when:

- `alert.event v1` and `alert.acknowledgement v1` schemas exist upstream and are pinned into this repo via SHA bump.
- `AlertRouter` is registered into `RealtimePipeline`, reads `result.detections`, produces severity-aware routed alerts, and is failure-isolated.
- `AcknowledgementRegistry` projects `alert.acknowledgement` events into live state via the router, replay-deterministically.
- Alert keys derive from `detection_id` via `signal_forge.identity` and are stable across live and replay.
- `PlatformSettings` exposes `alert_bus` and `alert_topic`; `for_replay()` swaps them.
- A replay-isolation test runs one event sequence through live and `for_replay()` pipelines and asserts byte-identical routing decisions, with the replay run routing to a sealed sink that never reaches the live on-call.
- `docs/alert-routing.md` exists, describing the design decisions, the D-14 determinism boundary, the cadence-scheduler seam, and deferred questions.
- Test count grew from 247 to roughly 285 (estimate; refine as we go).
- CI green: ruff, `mypy --strict`, pytest matrix on 3.11 and 3.12, test-count regression guard, GitGuardian.
- PR merged to main; tag `phase-5-complete` pushed on the merge commit.

## Estimated test count delta

The roadmap's ~240→~280 estimate predates Phase 4's overrun (which landed at 247, not ~220). From the real 247 baseline, a realistic Phase 5 delta:

- `AlertRouter` routing + severity dispatch: ~12 tests
- `AcknowledgementRegistry` projection (registration, queries, cold start, out-of-order ack, re-ack): ~10 tests
- Alert-key derivation + stability: ~5 tests
- Pipeline integration + failure isolation: ~6 tests
- Replay byte-identity of routing decisions: ~3 tests
- `PlatformSettings` extension (defaults, `for_replay()` swap, validation): ~6 tests

Total estimated growth: 247 → ~285. Consistent with the roadmap's per-phase shape once rebased on the true Phase 4 count.

## Cross-references

- `docs/working-notes.md` — D-4 (function-shaped pipeline), D-5 (detections-as-return-value, the consumer pattern this phase exercises), D-8 (schemas live upstream), D-11 (budget an upstream PR per deep-integration phase), D-12 (`PlatformSettings` as the live-vs-replay switch home). D-14 lands in commit 1.
- `docs/roadmap.md` — Phase 5 design questions (push-vs-pull, acknowledgement state, idempotency keys) and the DDIA cross-references (Chapters 9 and 10).
- `docs/detection-models.md` — the detectors whose output this phase routes; the deferred recovery/reminder/ack lifecycle noted there as "Phase 5's job" is now this phase.
- `docs/dataset-export.md` — Phase 4's reference document; Phase 5's matches its shape.
- `signal_forge/streaming/realtime_pipeline.py` — `register_dataset_writer` (lines 366–392), the registration shape `register_alert_router` mirrors; `ProcessingResult.detections`, what the router consumes.
- `signal_forge/config/platform_settings.py` — the `for_replay()` pattern and bucket-field validation Phase 5 extends.
- `signal_forge/identity.py` — the `derive` helper Phase 5 reuses for alert keys.
- `replay-workflows.md` — the "replay does not trigger downstream alerts" non-goal that the config swap satisfies.

## What Phase 5 delivered

The phase landed close to plan, with the design questions resolved as the plan anticipated. Nine commits past the store-outage documentation work on `main`:

- `docs(project): add Phase 5 plan and D-14 working note`
- `chore(deps): bump event-schema-contracts to 0c3b48b`
- `docs(project): settle Phase 5 ack gating-vs-annotation as annotate`
- `feat(alerts): add AcknowledgementRegistry and AlertRouter`
- `feat(alerts): integrate AlertRouter into RealtimePipeline`
- `feat(alerts): add alert-bus config and the AlertSink boundary`
- `feat(alerts): add EventBridgeAlertSink via moto`
- `test(alerts): replay byte-identity of routing decisions`
- `docs(alerts): add alert-routing reference document`

Plus the upstream `event-schema-contracts` v0.6.0 PR (`alert.event v1` and `alert.acknowledgement v1`), the D-11 upstream contribution this phase budgeted for.

**How the design questions resolved.** All three open questions settled as the plan leaned. Acknowledgement gating-vs-annotation went to **annotate** (a pure router; suppression deferred to the cadence scheduler), recorded as the settled-design section and D-14. The alert-key payload shape was fixed by the upstream schema (`alert_id` a first-class UUIDv4-or-v5 field), closing that question at contract-design time. Severity-to-sink topology resolved against the real `aws-event-pipeline-infra`: it provisions one environment-scoped EventBridge bus with detail-type routing rules, so severity rides on `DetailType` and routing is an infra-side rule concern — simpler than the plan's three candidate options.

**Caller-wired publication.** The plan left open whether the sink is pipeline-registered or caller-wired; it settled on **caller-wired** (the pipeline returns `ProcessingResult.alerts`; the caller picks the sink), because publication is an environment-dependent side effect and the live-vs-replay sink choice belongs where the environment context lives — consistent with how the feature and detection layers stay pure returns.

**Test count:** 247 → 295. The growth is the router and registry units, the pipeline-integration tests, the config and sink tests, the moto-backed EventBridge tests, and the replay byte-identity proof.
