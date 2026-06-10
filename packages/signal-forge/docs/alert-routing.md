# Alert routing

The alert layer turns the detections a `process()` call produces into routed, acknowledgement-aware alerts. It is the consumer of `ProcessingResult.detections` that the detection layer was built to feed (working note D-5): detections flow back as return values, and alert routing is the thin layer that reads them, derives stable alert identities, annotates acknowledgement state, and hands the resulting alerts to a publication sink.

The layer is deliberately split across a clean determinism boundary. Everything that *can* be reconstructed from the event stream — the routing decision, the acknowledgement state — lives inside the deterministic, replay-safe pipeline. The one thing that cannot — wall-clock reminder cadence — is fenced out into an operational scheduler that this layer names but does not build. That boundary is decision D-14, and it is the organising idea of the whole layer.

## The components

Four pieces, each single-purpose, composing into the routing path:

**`AlertRouter`** (`signal_forge/alerts/alert_router.py`) consumes a call's detections and produces one `AlertEvent` per detection. It is a pure function of `(detections, acknowledgement-state)`: it holds no state of its own, derives every alert identity rather than minting it, and reads acknowledgement state from an injected registry. Given the same detections and the same registry, it produces the same alerts, every time.

**`AcknowledgementRegistry`** (`signal_forge/alerts/acknowledgement_registry.py`) is a router-subscribed projection of `alert.acknowledgement` events into a live set of acknowledged `alert_id`s. It mirrors `DeviceRegistry` exactly — a deterministic fold over an event stream into a materialised view, parameter-less, idempotent, with no event-time discipline. Because acknowledgements ride the event stream, acknowledgement *state* is a deterministic function of input order, so a replay reconstructs it identically.

**`AlertSink`** (`signal_forge/alerts/alert_sink.py`) is the structural protocol for alert publication — the boundary between the pure pipeline and external delivery — with `InMemoryAlertSink` as the no-AWS implementation. Publication is *caller-wired*, not pipeline-registered: the pipeline returns alerts on `ProcessingResult.alerts`, and a caller hands them to whichever sink the current environment calls for. The production driver wires a live sink; the replay driver wires a sealed one. This keeps the live-versus-replay choice where the environment context lives, the same way the feature and detection layers stay pure returns.

**`EventBridgeAlertSink`** (`signal_forge/alerts/eventbridge_alert_sink.py`) is the boto3 implementation, publishing alerts to an environment-scoped EventBridge bus via `put_events`. It mirrors `S3DatasetWriter`: a client-factory seam for test injection, the bus name read from `PlatformSettings.alert_bus`, a no-op when no bus is configured, and boto3 confined to this one module.

## Alert identity and idempotency

Each alert's `alert_id` is derived as a UUIDv5 from the originating detection's `detection_id` via the identity spine (`derive("alert", str(detection_id))`). Because `detection_id` is itself derived and replay-stable (the Phase 4 identity work), the alert key is automatically identical across live and replay runs.

This is an idempotency key in the precise sense. Publication to an external bus is at-least-once — `put_events` can be retried, and a batch can partially fail and be re-sent — so a downstream consumer may see the same alert twice. A consumer that deduplicates on `alert_id` gets effectively-once processing on top of at-least-once delivery. The sink does not deduplicate; it delivers faithfully and lets the stable key do the work downstream. The alert key is also what an acknowledgement references, so an `alert.acknowledgement` emitted against an `alert_id` resolves the right alert across replays.

The alert is its own identity, not an overload of the detection's: deriving a distinct `alert`-namespaced key keeps the alert a first-class thing and gives the acknowledgement a clean target to point at.

## Acknowledgement and the cadence boundary

This is the layer's central determinism decision (D-14), and it splits a single conceptual feature — "alerting lifecycle" — along the line of *which state can be reconstructed from the event stream*.

Acknowledgement state can be, because acknowledgements are events. An `alert.acknowledgement v1` event flows through the same stream as everything else; the `AcknowledgementRegistry` projects it into live state through the same router every other consumer uses. A replay reconstructs byte-identical acknowledgement state, so acknowledgement-resolved routing decisions are themselves replay-deterministic.

Reminder cadence cannot be, because it fires on elapsed wall-clock time, not on event arrival. "Re-page about this still-unacknowledged alert every N minutes" has no event-shaped form, and a timer inside the pipeline would reintroduce both the daemon shape the pipeline forbids (D-4) and the `datetime.now()` the determinism spine forbids everywhere. So cadence lives in an operational scheduler *outside* the deterministic path — a component that consumes routed alerts and current acknowledgement state and fires reminders on its own clock. This layer names and bounds that seam but does not build the loop; the scheduler is a follow-up. A replay run, routing to a sealed sink, never exercises cadence at all — there is nothing to re-page.

The router reflects this split by **annotating, never suppressing**. An already-acknowledged detection still produces an alert; the router stamps acknowledgement status into the alert's `details` rather than withholding it. Suppression — declining to re-notify about an acknowledged alert — is the scheduler's wall-clock concern. Annotating keeps the router a pure function, dissolves the out-of-order case (an acknowledgement arriving before its alert is routed needs no special handling, because the router stamps current state at routing time), and loses no information: the full acknowledgement signal travels downstream to where the context to act on it lives.

## Severity and the infra boundary

Each alert carries the detector-assigned severity unchanged — the router does not reclassify. `CRITICAL` is meant to page immediately; `WARNING` is meant to digest. But the router does not implement page-versus-digest dispatch, because that decision lives in infrastructure, not in the analytics control plane.

The upstream `aws-event-pipeline-infra` provisions one environment-scoped EventBridge bus and routes events by detail-type through rules. So the `EventBridgeAlertSink` publishes well-formed alerts carrying their severity on the event's `DetailType` (`"CRITICAL:store.outage"`), and the bus rules — owned by infra — fan CRITICAL and WARNING to their respective targets. signal-forge owns "produce the alert"; infra owns "where the alert goes". This keeps the severity-routing topology out of the analytics code and lets it evolve in infra without redeploying the pipeline.

## Configuration and replay isolation

`PlatformSettings` carries `alert_bus` and `replay_alert_bus` as `str | None` name fields, validated as EventBridge bus names at construction. `for_replay()` swaps the active bus to the replay bus unconditionally — a replay run publishes to the replay bus, or to nothing if no replay bus is configured, but never to the live bus (the safer failure mode). The settings carry the bus *name*, never a client; the boto3 client is constructed in the sink. This is decision D-12 applied to alerting, identical in shape to how the dataset layer's bucket fields work.

Replay isolation therefore has two complementary mechanisms. For a pipeline-registered sink the swap would happen in settings; for the caller-wired alert sink it happens at the caller's routing layer — the production driver builds a live sink, the replay driver a sealed one. Either way, the determinism that makes isolation safe is the same: the alerts themselves are byte-identical across runs, so routing them to a separate sink is the only difference between live and replay.

## Determinism, proven

The replay byte-identity test (`tests/alerts/test_alert_replay_isolation.py`) is the headline guarantee: one fixed event sequence run through a live pipeline and a `for_replay()` pipeline produces byte-identical `AlertEvent` payloads, each run's alerts routed to its own sink. This is the alerting analogue of the Phase 4 dataset byte-identity test and the same underlying property — re-running the event log reproduces identical derived output, the guarantee that makes replay and backfill safe.

The reproducibility rests entirely on derived identity: `alert_id` from `detection_id`, the envelope `event_id` from `alert_id`, and the trace propagated from the detection's trace from the event's trace. With identical events fed to both runs — exactly as the EventBridge archive replays unchanged events — the serialised alert payload carries no per-run randomness.

## What this layer deliberately does not do

- **Reminder cadence.** The wall-clock re-notification loop is named and bounded here but built outside the deterministic path, as an operational scheduler. A replay never exercises it.
- **Severity-to-target dispatch.** The router carries severity; the EventBridge bus rules (infra) route on it. signal-forge does not decide which target a CRITICAL reaches.
- **Deduplication.** The sink delivers at-least-once and faithfully; consumers dedupe on the replay-stable `alert_id`. The layer provides the idempotency key, not the dedup.
- **Acknowledgement capture.** The `alert.acknowledgement` event is the contract; how a human emits one — a button, a CLI, a chat action — is not this repository's concern.
- **Operational persistence of acknowledgement state.** Acknowledgement state is an in-memory event projection, replay-deterministic. Any durable operational store of acknowledgement or cadence state belongs to the scheduler, outside the pipeline.
- **Cross-alert correlation.** Two detections for the same store produce two alerts; correlating or collapsing them is a Phase 6 dashboard-projection concern, as it is for detections.

## Cross-references

- `docs/working-notes.md` — D-4 (function-shaped pipeline), D-5 (detections-as-return-value, the consumer pattern this layer exercises), D-8 (schemas live upstream), D-12 (`PlatformSettings` as the live-vs-replay switch home), D-14 (the acknowledgement/cadence determinism boundary).
- `docs/detection-models.md` — the detectors whose output this layer routes; the recovery/reminder/acknowledgement lifecycle noted there as "Phase 5's job" is this layer.
- `docs/store-outage-detection.md` — the store-outage detection whose `CRITICAL` severity becomes an immediately-paged alert here.
- `docs/dataset-export.md` — Phase 4's reference document; this one matches its shape, and the replay byte-identity tests are siblings.
- `signal_forge/alerts/` — the layer: `alert_router.py`, `acknowledgement_registry.py`, `alert_sink.py`, `eventbridge_alert_sink.py`.
- `event_schema_contracts.alerts` — the upstream `alert.event v1` and `alert.acknowledgement v1` contracts (v0.6.0).
- `tests/alerts/test_alert_replay_isolation.py` — the determinism proof.
- Upstream `aws-event-pipeline-infra/docs/architecture.md` — the EventBridge bus and detail-type routing rules this layer publishes into.
