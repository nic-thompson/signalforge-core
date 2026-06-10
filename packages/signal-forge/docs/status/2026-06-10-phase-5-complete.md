# Status snapshot — 2026-06-10 — Phase 5 complete

> Snapshot captured before the merge of the PR for `feat/phase-5-alert-routing`. The branch is nine commits past the store-outage documentation work on `main`, and ready to merge. Once merged and tagged `phase-5-complete`, this snapshot describes the on-`main` state. The as-if-merged framing matches the Phase 2–4 snapshots.

## Where we are

`main` will sit at the merge commit for Phase 5's PR. Nine commits past `2482316` (the store-outage doc merge):

```
chore(project): close Phase 5 housekeeping
docs(alerts): add alert-routing reference document
test(alerts): replay byte-identity of routing decisions
feat(alerts): add EventBridgeAlertSink via moto
feat(alerts): add alert-bus config and the AlertSink boundary
feat(alerts): integrate AlertRouter into RealtimePipeline
feat(alerts): add AcknowledgementRegistry and AlertRouter
docs(project): settle Phase 5 ack gating-vs-annotation as annotate
chore(deps): bump event-schema-contracts to 0c3b48b
docs(project): add Phase 5 plan and D-14 working note
```

295 tests passing. ruff and `mypy --strict` clean across 34 source files. CI is expected green at merge (ruff check, `mypy --strict` on `signal_forge`, pytest matrix on Python 3.11 and 3.12, test-count regression guard now at 295). This is the first CI run of the phase — the branch has been local-green throughout — so it is also the first time the moto-backed EventBridge tests run on 3.12.

## What landed in Phase 5

**Alert routing core** (`signal_forge/alerts/`):

The consumer of `ProcessingResult.detections` that turns detections into routed, acknowledgement-aware alerts. The pieces:

- **`AlertRouter`** (`alert_router.py`) — consumes a call's detections and produces one `AlertEvent` per detection. A pure function of `(detections, ack-state)`: no state of its own, every alert identity derived (not minted), ack state read from an injected registry. The detection-to-alert stream-table join.
- **`AcknowledgementRegistry`** (`acknowledgement_registry.py`) — a router-subscribed projection of `alert.acknowledgement` events into a live set of acknowledged `alert_id`s, mirroring `DeviceRegistry`. A deterministic fold over the event stream; replay-deterministic because acks ride the stream.
- **`AlertSink`** + **`InMemoryAlertSink`** (`alert_sink.py`) — the structural protocol for alert publication and its no-AWS implementation. Publication is caller-wired: the pipeline returns `ProcessingResult.alerts`, the caller hands them to whichever sink the environment calls for.
- **`EventBridgeAlertSink`** (`eventbridge_alert_sink.py`) — the boto3 implementation, publishing via `put_events` to an environment-scoped bus, severity on `DetailType`, chunked to the 10-entry cap. Mirrors `S3DatasetWriter`: client-factory seam, bus from settings, no-op when unconfigured, boto3 confined to the module.

**Pipeline integration** (`signal_forge/streaming/realtime_pipeline.py`):

`register_alert_router` (single router, second registration raises), a `route()` dispatch step turning each call's detections into alerts, and a new `ProcessingResult.alerts` field parallel to `detections`. Bulkheaded like every other observer; the extraction-failure path carries `alerts=[]`.

**Configuration** (`signal_forge/config/platform_settings.py`):

`alert_bus` and `replay_alert_bus` (`str | None`), validated as EventBridge bus names, read from `SF_ALERT_BUS` / `SF_REPLAY_ALERT_BUS`. `for_replay()` swaps the active bus to the replay bus unconditionally — a replay run publishes to the replay bus, or to nothing, never to the live bus (D-12).

**Replay byte-identity** (`tests/alerts/test_alert_replay_isolation.py`):

The phase's headline property: one fixed event sequence through a live and a `for_replay()` pipeline produces byte-identical `AlertEvent` payloads, each run's alerts routed to its own sink. The alerting analogue of the Phase 4 dataset test; the Chapter-11 reprocessing guarantee for alerts.

**Upstream contribution** (`event-schema-contracts` v0.6.0, PR #5, `0c3b48b`):

`alert.event v1` and `alert.acknowledgement v1` schemas. `alert.event` carries `alert_id` (UUIDv4-or-v5, derivable), lineage `detection_id`, reused `DetectionSeverity`, and denormalised routing context; `alert.acknowledgement` references the alert by `alert_id` with a v4-only `acknowledgement_id`. The D-11 upstream contribution this phase budgeted for. Pinned into this repo at `0c3b48b`.

**Decision D-14** (`docs/working-notes.md`):

Acknowledgement state is a replay-deterministic event projection; reminder cadence is wall-clock and lives outside the deterministic path. The organising idea of the layer — everything reconstructable from the event stream stays inside the deterministic pipeline; the irreducibly-wall-clock remainder (cadence) is fenced into an operational scheduler this phase names but does not build.

**Reference documentation** (`docs/alert-routing.md`):

The brief-mandated alert reference, matching the shape of `dataset-export.md` and `detection-models.md`.

## How the design questions resolved

All three of the plan's open questions settled as the plan leaned:

- **Acknowledgement gating vs annotation → annotate.** The router always emits an alert, stamped with ack status in `details`; suppression is deferred to the cadence scheduler (D-14). Keeps the router a pure function and dissolves the out-of-order case.
- **Alert-key payload shape → settled by the schema.** `alert_id` is a first-class UUIDv4-or-v5 field on `alert.event`, referenced by the acknowledgement. Closed at contract-design time.
- **Severity-to-sink topology → infra-side rules.** `aws-event-pipeline-infra` provisions one environment-scoped EventBridge bus with detail-type routing rules, so severity rides on `DetailType` and routing is an infra concern — simpler than the plan's three candidate options.

Plus the publication model: **caller-wired**, not pipeline-registered, because publication is an environment-dependent side effect and the live-vs-replay sink choice belongs where the environment context lives.

## What's deliberately deferred

- **The wall-clock reminder-cadence scheduler.** Named and bounded (D-14); built as a follow-up outside the deterministic path. Phase 5 ships the routing core and ack projection; cadence is a later operational component.
- **Acknowledgement capture mechanism.** The `alert.acknowledgement` event is the contract; how a human emits one (button, CLI, chat action) is not this repo's concern.
- **Operational persistence of ack/cadence state.** Ack state is an in-memory event projection. Durable operational storage belongs to the scheduler, outside the pipeline.
- **Cross-alert correlation.** Two detections for one store produce two alerts; correlation is a Phase 6 dashboard-projection concern.

## Open issues

- **CI runner Node 20 deprecation.** `actions/checkout@v4` and `actions/setup-python@v5` run on Node 20, forced to Node 24 from 2026-06-16 and removed 2026-09-16. CI is green and unaffected for now; fix is a small CI PR before the September removal. Tracked in working-notes.
- **`structured-logging-python` stdlib-logging warnings** during `error()` calls still surface in tests. Pre-existing since Phase 1; deterministic; not addressed.

## Estimate vs actual

The roadmap estimated Phase 5 at 12–16 hours. The phase landed close to plan: the design questions resolved as anticipated, the upstream PR was the budgeted single D-11 contribution, and the unit-by-unit cadence (router, integration, config+sink, EventBridge sink, replay test, docs) ran without the upstream-contract surprises Phase 3 and Phase 4 hit. Test count grew 247 → 295, near the plan's ~285 estimate.

## Process observations worth carrying forward

- **Larger unit boundaries kept cadence up.** After the fine-grained Phase 4 close, Phase 5 worked at the unit level — a coherent component plus its tests drafted, sandbox-verified, and committed as one reviewable piece, with fine-grained stepping reserved for genuinely risky spots (contract changes, the determinism assertion). Faster without losing the deliberateness where it earns its keep.
- **The test caught the bug.** The `for_replay()` alert-bus swap was initially missing (Edit 4 didn't land); the config test failed loudly, which is exactly the incident D-12/D-14 are written to prevent — a replay run publishing to the live bus. Verifying the swap before trusting it paid off.
- **Verify the real fixture before writing tests against it.** The first integration-test draft assumed `FakeEventDetector(store_id=..., emit_count=...)`; the real fixture takes `detections_per_observation` and hardcodes the store. Reading the fixture before finalising avoided a failure cascade.

## How to resume

Next session, the standard sequence:

```bash
cd ~/Code/signal-forge
source .venv/bin/activate
git status
git --no-pager log --oneline | head -3
python3 -m unittest discover -s tests 2>&1 | tail -3
git tag --list
```

Expected: branch `main`, working tree clean, top commit is the Phase 5 merge, 295/295 tests, tags include `phase-1-complete` through `phase-5-complete`.

Then start Phase 6 (dashboard projections): create `docs/phases/phase-6-plan.md` and work the roadmap's Phase 6 design questions before any implementation. The roadmap flags materialised views of detections/alerts by store, type, and severity over rolling windows, a low-latency store (DynamoDB), and the update model (per-emission vs batched). Phase 6 is the consumer of the detections and alerts this and prior phases produce; per D-12, any projection-store config defaults to name fields on `PlatformSettings` swapped by `for_replay()`. Budget for an upstream PR per D-11.
