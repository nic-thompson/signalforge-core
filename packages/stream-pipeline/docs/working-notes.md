# SignalForge — working notes

> **Purpose.** This document captures the design decisions, working agreements, conventions, and known issues that don't fit cleanly into commit messages or per-phase plans but matter for anyone working on the project. It's the long-lived index of "how we think about this codebase".
>
> **Scope.** Evergreen project state. Per-phase plans live in `docs/phases/`. Point-in-time status snapshots live in `docs/status/`. The architectural map of the streaming layer lives in `docs/architecture.md`.
>
> **Audience.** A platform engineer (or AI assistant) joining the project — including future-me coming back after a gap. Read this before reading any other doc except `RESUMING.md`.

## What SignalForge is

A telemetry intelligence platform for a retail headset fleet. Core requirements from the brief:

- 50,000 devices across 8,000 stores
- Sub-5-second dashboard freshness
- 2-year retention
- Replay-deterministic dataset regeneration
- Schema-evolution support without consumer disruption

The platform decomposes into five repositories. `stream-pipeline` (this repo) is the analytics control plane. The other four are upstream contract and infrastructure packages: `event-schema-contracts`, `structured-logging-python`, `telemetry-parser`, `aws-event-pipeline-infra`.

## Engineering principles

These aren't aspirational — every commit on `main` honours them. If a future change is about to break one of these, that's a signal to either revisit the principle openly or reshape the change.

### Replay determinism

Same input sequence always produces the same output sequence. Concretely:

- No `datetime.now()`, no random IDs, no environment-dependent behaviour in pure components.
- Window boundaries are aligned to the Unix epoch, not to the first event seen.
- Iteration order over registered handlers, aggregators, and detectors is the registration order.
- Detection results flow back as a typed return value (`ProcessingResult.detections`), not as side effects, so replays produce identical detection sequences.

This is the big-ticket property the brief calls out. Every other principle either supports it or is at least consistent with it. Kleppmann (DDIA) calls this *deterministic stream processing* — Chapter 12, "Stream Processing", makes the case for why this matters for replay and exactly-once semantics.

### Schema-aware everywhere

Every consumer registers against `(event_type, schema_version)`. The `EventRouter` honours this. So does the schema registry in `event-schema-contracts`. Major-version mismatches don't fall back; minor-version mismatches do (consumer-favouring fallback within a major).

The asymmetry between us and the upstream registry matters: `event-schema-contracts.SchemaRegistry` serves *producers* (use the highest version a producer supports); we serve *consumers* (accept any minor version a registered handler can handle). That's not a contradiction; it's two correct answers to two different questions. Document it explicitly because the asymmetry is otherwise easy to read as a bug.

### Function-shaped, not daemon-shaped

`RealtimePipeline.process()` is a function: events in, results out. No background threads, no event loops, no `while True`. The caller owns the event source — SQS poller, replay iterator, test list — and the same pipeline works for streaming and batch.

This is what makes the brief's "replay-and-streaming-compatible" requirement actually achievable. A daemon-shaped service would need parallel non-daemon plumbing for replay; we just feed the replay events to the same `process_batch()` and get the same outputs.

### Failure isolation at orchestration boundaries

The pipeline is the only component that performs side effects (structured logging). The pipeline catches exceptions from handlers, aggregators, and detectors with `except Exception:`, logs them with full lineage (event_id, trace_id, partition_key, the failing component's name), and continues. A bad event or a buggy detector cannot poison a batch.

Strict mode (`RealtimePipeline(strict=True)`) re-raises immediately. Used in replay-validation tests to catch silent-corruption regressions that production would tolerate.

This is the pattern called *bulkheading* in resilience-engineering literature; Kleppmann references the idea in DDIA Chapter 8 ("The Trouble With Distributed Systems") in the context of bounded blast radius.

### Type-first

`mypy --strict` is a hard CI gate, not a nice-to-have. Every signature is annotated. `Any` appears only where a value is genuinely opaque (event payloads pre-validation, aggregation state in the windowing layer). `from __future__ import annotations` is enforced at the top of every module.

The cost of this discipline is roughly 5% extra typing time. The benefit is that refactors land on the first try and CI catches contract drift before review.

### Replay safety meets type safety in `Protocol`

We use `typing.Protocol` rather than abstract base classes. Three reasons:

1. Structural typing means a class doesn't need to inherit from a Protocol to satisfy it; if it has the right shape, it conforms.
2. `mypy --strict` checks Protocol conformance at every call site, so the contract is verified statically without runtime ceremony.
3. `Protocol` doesn't bring an `__init__`, so test fakes don't need to call a parent's constructor; they just declare the right methods and class variables.

We deliberately do NOT use `@runtime_checkable`. `isinstance(obj, MyProtocol)` only checks method existence, not signature compatibility — partial protection that misleads. Static checking is the real safety; runtime checking buys ceremony that doesn't catch real bugs.

## Working agreements

These are how we work, not what we're building. They've evolved through Phase 1 and the start of Phase 2.

### Option A — fine-grained commits, full hygiene

Every commit is a single coherent piece of thinking. The history reads as a narrative of design decisions. Reviewer-friendly even when the reviewer is just future-you.

The cost is wall-clock time — Phase 1 took longer than a "ship the whole streaming layer in one big commit" approach would have. The benefit is that anyone reading the history can `git log --first-parent main` and see the sequence of decisions, not just the final state.

### Verification checkpoints

Before any commit:

- `git status --short` to see exactly what's modified or untracked
- `wc -l` on every newly created file to confirm content actually landed (twice in this project, an empty file slipped through)
- `ruff check .` (full repo) before staging
- `mypy stream_pipeline` before staging
- `python3 -m unittest discover -s tests` before staging

After any commit:

- `git --no-pager show --stat HEAD` to confirm what landed matches what was intended

These aren't optional. Twice in Phase 1 a commit nearly shipped with missing files because we skipped the post-commit verification. The cost of running these is seconds; the cost of not running them is debugging "why did this break in CI" cycles.

### Pre-merge CI

`gh pr checks --watch` before any `gh pr merge`. Three times in this project we've nearly merged before CI confirmed green. The pattern is now: open PR, run `gh pr checks --watch`, merge only after explicit green confirmation.

### Pasting type definitions before writing tests against them

When writing tests that construct Phase 1 types (`WindowEmission`, `RealtimePipeline`, `ProcessingResult`, etc.), I (the AI) ask you to paste the type's current definition before I write the test. Memory-based test code shipped two bugs in Phase 2 (`FakeTrace` failing pydantic validation, `WindowEmission`'s `aggregator_name` vs `aggregation_name` field rename) that paste-then-write would have caught.

### Diff a file against its current version before overwriting it from a snapshot

When applying a whole-file change — especially one generated from an earlier snapshot of the repo — diff it against the working tree before overwriting. A later commit may have moved the file on: during Phase 4, `realtime_pipeline.py` was overwritten from a pre-commit-7 snapshot and silently lost `register_dataset_writer` and its dispatch, which only the dataset-writer tests caught. The three detectors were safe only because nothing had touched them since Phase 2. The rule: for any file a later commit might have changed, prefer a targeted edit to the current file over wholesale replacement; and if replacing, `git diff` before staging so a reverted addition shows up immediately.

### Conversational hygiene with `gh` and `git`

Both tools default to using a pager (`less`). Empty terminal output after a `gh` or `git` command's first hypothesis should be "the pager opened and I closed it". Either disable globally:

```bash
gh config set pager cat
git config --global core.pager ""
```

Or use explicit no-pager flags per command (`git --no-pager`, `GH_PAGER= gh`).

### Extended regex for structural greps

`grep -En "^(class|def |if __name__)"` is the canonical form for finding Python structure. Basic-regex alternation (`\|`) varies between BSD and GNU; extended regex (`-E`) is portable and more readable.

### VS Code workspace settings

`.vscode/settings.json` enforces `files.insertFinalNewline` and `files.trimTrailingWhitespace`. These align with ruff's `W291` and `W292` rules. Settings that align with project lint rules are project policy, not personal preference, so the file is committed.

## Decisions, with reasoning

This section is a chronological record of the consequential design decisions, with the reasoning behind each. New entries get appended; old entries don't get rewritten.

### Phase 1 — streaming layer architecture

#### D-1: `EventRouter` uses consumer-favouring fallback within a major version

A v1 handler accepts v1.1 events. A v1 handler does NOT accept v2 events.

**Why:** Consumers are the asymmetric side of the schema-evolution problem. A producer publishing v1.1 events shouldn't break a v1-aware consumer's day; the v1 fields are still there, the new v1.1 fields are simply ignored. Major-version bumps are by convention breaking, so we don't fall back across them.

**Trade-off considered:** Strict-version routing (only exact `(type, version)` matches dispatch). Rejected because schema evolution would require lockstep upgrades across all consumers, which doesn't scale across a fleet.

**Cross-reference:** DDIA Chapter 5 ("Encoding and Evolution") discusses this exact problem under "backward and forward compatibility". Our v1 handler accepting v1.1 events is *forward compatibility on the consumer side*.

#### D-2: Watermarks are wall-clock-free and per-key

`WatermarkManager` derives time only from event timestamps. There's no `datetime.now()` and no system-clock dependency. Watermarks are tracked per partition key (typically `(store_id, device_id)`), not globally.

**Why:** Replay determinism requires the watermark trajectory to be reproducible from the input event sequence alone. Per-key watermarks prevent fleet-wide stalls when a single store's stream goes quiet.

**Trade-off considered:** Global watermark with per-key offsets, as some streaming systems use. Rejected because the operational model — devices going offline regularly, store-level outages happening — means per-key isolation is the right granularity.

**Cross-reference:** DDIA Chapter 12 ("Stream Processing") introduces watermarks in Sub Section "Reasoning About Time"; the per-key approach matches Apache Beam's "windowing per key" pattern, which DDIA refers to but doesn't fully unpack.

#### D-3: Window boundaries align to Unix epoch, not first-event-time

Window starts are computed as `floor((event_timestamp - epoch) / slide) * slide`. They are NOT computed relative to whichever event arrives first.

**Why:** Replay determinism *and* cross-shard joinability. If two consumer shards processing the same fleet's events compute window boundaries differently, downstream datasets can't join their outputs. Epoch-relative alignment guarantees that any two pipelines processing any subset of the same event stream produce identical window boundaries.

**Trade-off considered:** First-event alignment (window starts at the first event's timestamp). Rejected because two consumer shards starting at slightly different times would compute different boundaries for the "same" window. Replay reproducibility breaks.

#### D-4: `RealtimePipeline` is the only component that performs side effects

Specifically: structured logging emission. Routers, aggregators, watermark managers, and (in Phase 2) detectors are pure. They take input, return values, and don't write to anywhere external.

**Why:** Pure components are trivially replay-deterministic. The pipeline owns the side-effect surface so we can audit it in one place — every log line is emitted from one of five sites in `realtime_pipeline.py`, all with the same trace-id propagation pattern.

**Trade-off considered:** Per-component logging. Rejected because trace-id propagation, log-event-type consistency, and metadata schema would be duplicated in every component. Centralisation is cheaper.

### Phase 2 — detection layer architecture

#### D-5: Detections flow back as a return value, not as side effects

`ProcessingResult` gains a `detections: list[DetectionEvent]` field. Detectors return their detections; the pipeline collects them. They do NOT call into a sink layer themselves.

**Why:** Replay determinism — same input sequence produces same detection sequence. Phase 5 alert routing becomes a thin adapter that reads `result.detections`. Detectors stay testable in isolation.

**Trade-off considered:** Detectors call directly into a sink registry. Rejected because it couples detection to sink infrastructure (EventBridge, SNS, etc.) and breaks replay — replays would re-emit alerts to live systems unless the sink registry was switched, which is exactly the kind of "easy to forget" flag that causes incidents.

#### D-6: Two protocols, not one — `EventDetector` and `EmissionDetector`

`EventDetector` consumes raw `TelemetryEvent`s. `EmissionDetector` consumes `WindowEmission`s from a named aggregation.

**Why:** Each protocol's input type is exact, no generics or unions. A reader of `OfflineDetector` (which only consumes events) sees `def observe_event(self, event: TelemetryEvent)` — clear contract, no narrowing.

**Trade-off considered:**

- A single `Detector[InputT]` generic protocol — clean for dispatch but pushes the type narrowing onto every detector.
- A single `observe(event_or_emission: TelemetryEvent | WindowEmission)` union — every detector starts with `if isinstance(...)`. Ugly.

Two protocols cost more lines but read better. Detectors that need both subscribe to both.

#### D-7: `OfflineDetector` emits once per offline transition event

State machine: `unseen → seen → offline`. Transition `seen → offline` emits one detection. Transition `offline → seen` resets state. Threshold crossed again is a new transition, hence a new emission.

**Why:** A device offline for 8 hours with a 5-minute threshold and "once per silent window" semantics would emit 96 detections — and in a fleet outage of 50,000 devices, that's 4.8M detections from a single quiet morning. The detector's job is to flag state transitions; reminder cadence belongs in Phase 5 alert routing where on-call rotations and acknowledgements are the actual context.

**Trade-off considered:** Once on threshold cross. Rejected because it doesn't reset on recovery — a device that flaps would emit only its first transition, then stay silent forever even if it went offline again. Transition-event semantics is the strict superset.

#### D-8: `DetectionEvent` schema lives upstream in `event-schema-contracts`

Discriminator pattern: single schema, `detection_type: str` + `details: dict[str, Any]`.

**Why:** Detection events are platform-wide contracts. They flow through EventBridge, get archived, are replayed. Every consumer (alert routing, dashboards, dataset projectors) needs the schema. Putting it upstream alongside `DeviceRegistrationEvent`, `SessionStartEvent`, etc. is the natural home.

**Trade-off considered:** Per-detector subschemas (`DeviceOfflineEvent`, `StoreOutageEvent`, `SignalAnomalyEvent`). Rejected because the three Phase 2 detection types (and foreseeable future types) share substantial common structure (`severity`, `store_id`, `source_event_id`, `threshold_breached`) and only differ in `details`. Per-subclass would force a schema PR for every new detector.

#### D-9: Trace propagation through window emissions

`WindowEmission` carries `last_contributing_trace_id: str | None`. The aggregator updates `_WindowState.last_contributing_trace_id` on every contribution. Emission detectors derive their detection's trace from this, falling back to a fresh trace when `None`.

**Why:** Lineage. An operator looking at a "store.outage" detection should be able to chase backwards to the contributing events. `source_event_id` on the payload provides instance-level lineage; `last_contributing_trace_id` provides trace-level lineage suitable for distributed-tracing tools.

**Trade-off considered:** Fresh trace per emission detection (no lineage). Cheaper but loses information. We made the more correct call.

**Alternative considered:** Track *all* contributing trace_ids on the window state. Rejected because memory cost (a list per window) and we never lookup by trace anyway — the most-recent-contributor is the right narrowing.

#### D-10: Callable injection is the right pattern for cross-phase deferrals, but the production data path must be sketched first

When a future-phase component (e.g. `DeviceRegistry`) isn't ready yet, the current phase's consumers (e.g. `OfflineDetector`, `OutageDetector`) take a constructor `Callable[..., ...]` for the missing capability. Tests fill the callable with a hand-built dict or lambda; production wiring happens in the later phase. This is what we did in Phase 2 with `store_lookup` and `registered_count_lookup`, deferring `DeviceRegistry` to Phase 3.

**Why:** Callable injection decouples phases. The consumer's interface is fixed by the abstract shape (e.g. `Callable[[UUID], str | None]`), not by the concrete implementation that comes later. Detectors stay testable in isolation and the later phase can build the real implementation without rewriting the consumer.

**The hidden cost we encountered:** Test fakes can fill interfaces that production data cannot. Phase 2's tests built `store_lookup` from `{"store-1": 50}` dicts; we never asked "where does this dict come from in production?". When Phase 3 began designing `DeviceRegistry`, we discovered `DeviceRegistrationPayload` didn't carry `store_id` at all — the production projection wasn't expressible from the upstream event stream as it stood. The cost was small (upstream PR, SHA bump) because it surfaced before any further work depended on it. Caught later — after Phase 5 alert routing and Phase 6 dashboards had been built — it would have meant rewriting the registration mechanism, re-running every producer, and re-validating downstream consumers.

**The lesson:** Before accepting a callable-injection deferral, do a 5-minute "design the production path enough to confirm it's possible" check. Specifically: trace the data the callable would need in production back to its source. If that source doesn't exist (the field isn't on the upstream schema, the event type isn't defined, the projection isn't deterministically computable), fix the gap *before* shipping the consumer — even though the consumer can technically be shipped with the test fake intact.

**Trade-off considered:** Refusing all deferrals — every phase ships the production implementations end-to-end. Rejected because it serialises work that can run in parallel and forces upstream contract evolution earlier than necessary. The callable-injection pattern remains correct; only the discipline around accepting it changes.

**Alternative considered:** Documenting the deferred-design risk in working-notes at the time of deferral, so the future phase's first task is to verify the production path. Equivalent in effect to the lesson above but more administrative. Either form works; what matters is that the verification happens before the deferred work is committed against.

This entry exists because the Phase 3 / `DeviceRegistry` design caught the gap at a non-catastrophic moment. The pattern of "callable injection without production-path verification" is the kind of disciplined-looking-but-actually-risky default that's worth flagging in the decision log so future phases don't repeat it.

#### D-11: Upstream contract evolution during a consumer phase is normal for the first deep integration

Phase 3 produced two upstream PRs against `event-schema-contracts`: PR #2 (v0.3.0) adding required `store_id` to `DeviceRegistrationPayload`, and PR #3 (v0.4.0) adding `WindowedFeatureVectorPayload` as a sibling of the existing entity-centric variant. Neither was in the original Phase 3 plan; both were the right call at the moment they surfaced.

**Why:** Phases 1 and 2 used the upstream contracts shallowly — read events, emit detections — and the contracts' existing shape was sufficient. Phase 3 is the first phase to *integrate* the upstream contracts with consumer logic at depth: a live device-to-store projection requires `store_id` on the registration payload; bundled feature emissions require a partition-window-centric payload variant. Surfacing two contract gaps during this phase isn't a failure of the upstream design — it's the predictable result of being the first deep consumer.

**The pattern to recognise:** When a consumer phase's design conversation produces "the upstream doesn't quite fit our use case here", the right move is usually an upstream PR rather than a local workaround. Workarounds (shoehorning into an existing schema, defining a parallel local type, building UUIDv5-from-string hacks) compound: each one obscures the contract for future readers and complicates future evolution. Contract evolution upstream is contained, reviewed, and visible.

**The cost is real but bounded.** Each Phase 3 upstream PR cost roughly an extra session — design, implementation, CI, PR, merge, tag, consumer SHA-bump. Phase 3 grew from a planned 7 commits to 9 to accommodate the two SHA bumps. Worth the cost: the consumer code stays clean and the contract carries the right semantics.

**Trade-off considered:** Defer contract evolution to a dedicated "schema evolution" phase. Rejected because it serialises work that can run in parallel (the consumer can't ship cleanly without the contract change, so blocking on it doesn't save time), and because the contract evolution is best designed by the consumer who has the use case in hand.

**The discipline to carry forward:** Future deep-integration phases (Phase 4 dataset layer, Phase 5 alert routing) should budget for *at least one* upstream PR. If a phase ships without any upstream evolution, that's either a sign the contracts are mature for that integration depth (good) or a sign that local workarounds slipped in (bad — review the consumer changes for shoehorning before declaring victory).

This entry exists because the pattern is now a Phase-N constant, not a Phase-3 anomaly. New phases should plan for it, not be surprised by it.

#### D-12: `PlatformSettings` is the canonical home for live-vs-replay switches — names and labels, not connection objects

Phase 4's dataset layer needed a way to route writes to a live S3 bucket in production and to a sealed replay bucket during replay runs. The decision was to add two `str | None` fields to `PlatformSettings` — `dataset_bucket` and `replay_dataset_bucket` — and have `for_replay()` swap the active bucket on the returned copy. The writer reads `settings.dataset_bucket` and does not care whether it's running live or replay.

**Why:** `for_replay()` already exists *specifically* to centralise the live-vs-replay distinction. Its Phase 1 job was rebranding the `environment` label so log streams don't collide; its Phase 4 job is swapping the active dataset bucket. The shape of the change is the same in both phases, in the same place, for the same reason. Adding a parallel live-vs-replay switching mechanism elsewhere would create exactly the "easy to forget flag that causes incidents" hazard D-5 was written to avoid — except symmetrically, applied to the writer side rather than the detector side.

**The boundary that matters:** `PlatformSettings` carries the bucket *name* (a `str`), not a boto3 client, not credentials, not a region resolver. The discipline established in the existing module docstring — "stdlib-only — importable from any context without pydantic/AWS deps" — survives the addition. The boto3 client gets constructed by the writer from the bucket name; that's where AWS dependencies are allowed to live. Anything richer than a name or a label is the wrong shape for `PlatformSettings` and belongs in the consuming component.

**Trade-off considered:** Per-writer config passed at construction (`DatasetWriter(bucket=..., replay_bucket=...)`). Rejected because it creates a second live-vs-replay switching mechanism in parallel to `for_replay()`. A future engineer wiring up a replay driver would have to remember to flip both — the settings *and* the writer config. One source of truth is cheaper than two.

**Alternative considered:** A separate `DatasetSettings` object alongside `PlatformSettings`. Rejected because the existing `data_retention_days` field already crosses the dataset-layer boundary (its docstring explicitly says "consumed in Phase 4"). Splitting would scatter dataset-layer config across two homes for no clear benefit, and the same argument would recur for every future phase — `AlertSettings`, `DashboardSettings`, `ReplaySettings` — none of which carries enough weight to justify its own object.

**The discipline to carry forward:** Phase 5 alert routing will face the same question (EventBridge bus name, SNS topic ARN, replay-isolated alert sink). Phase 6 dashboard projections will face it again (DynamoDB table name, replay-isolated table). The default answer for each is: name fields on `PlatformSettings`, swap on `for_replay()`, construct the AWS client in the consuming component. Departures from this default need explicit justification, not just "feels cleaner to keep it local".

This entry exists because the right place to put a live-vs-replay switch is non-obvious — local config feels lighter at the moment of writing, but the cost of getting it wrong (a replay run accidentally writing to the live bucket, or alerting the live on-call) is high enough that the convention deserves to be a documented default rather than a per-phase rediscovery.

#### D-13: A contract change is "local" only if the validators — not just the field types — permit the value you intend to produce

Commit 9's replay byte-identity assertion required `detection_id`, `source_event_id`, and the envelope `event_id` to be deterministic across runs. Deriving them via UUIDv5 from stable coordinates looked like a purely local `stream-pipeline` change: the upstream fields are typed `UUID`, and a UUIDv5 *is* a `UUID`. It was not local. `event_schema_contracts`'s `UUIDv4Model` enforces UUIDv4 on those fields through a `field_validator` keyed off `__uuid_v4_fields__`, so a derived v5 id was rejected at construction. The change required relaxing the upstream contract (PR #4, `event-schema-contracts` v0.5.0, adding a `__uuid_v4_or_v5_fields__` policy) plus a consumer SHA bump — two commits and an upstream PR — before the consumer change could land.

**Why the misjudgement happened:** reading the field *annotation* (`UUID`) and concluding "any UUID is accepted". The annotation describes the type; the *validator* describes the accepted values, and the two diverge precisely at the kind of constraint — version, range, format, `extra="forbid"` — that makes a change non-local.

**The discipline:** before judging a contract change local, read the upstream model's validators and config — `field_validator`s, `model_config` (`extra=`, `frozen=`), and any custom base class (here `UUIDv4Model`) — not just the field annotations. Then construct the value you intend to produce against the *real* contract early: a five-line "does this even validate?" check is cheaper than discovering mid-implementation that the consumer change can't land without an upstream PR and SHA-bump cycle you have already built on top of.

**Relationship to D-10 and D-11:** the same family of lesson. D-10 says trace a deferred callable's production data path before accepting the deferral; D-11 says budget for at least one upstream PR per deep-integration phase; D-13 says verify the upstream *constraint*, not just its type signature, before deciding where a change lives. All three are "confirm the real constraint before committing to an approach", surfaced at three different boundaries.

This entry exists because "it's just a UUID" is exactly the kind of type-level reasoning that feels safe and skips the validator — and the cost of the wrong call is a mid-flight pivot from "local edit" to "upstream PR, release, re-pin", far more expensive caught late than the five-minute check that prevents it.

#### D-14: Acknowledgement state is a replay-deterministic event projection; reminder cadence is wall-clock and lives outside the deterministic path

Phase 5 alert routing needs to know whether an alert has been acknowledged (to decide how to route it) and needs to remind on-call about still-unacked alerts on some cadence. Both look like "alerting state", but they sit on opposite sides of the determinism boundary, and the decision is to split them there rather than treat them as one feature.

**Acknowledgement state rides the event stream.** An `alert.acknowledgement v1` event type flows through the same stream as telemetry and device-registration events. An `AcknowledgementRegistry` subscribes to `("alert.acknowledgement", "v1")` via the `EventRouter` — the exact mechanism `DeviceRegistry` uses (settled in the Phase 3 plan) — and projects acks into live state. Because acks are events in the ordered stream, ack state is a deterministic function of input order: a replay reconstructs byte-identical ack state, and every determinism contract in `replay-workflows.md` holds. D-5's "same input sequence produces the same detection sequence" extends cleanly to "the same input sequence produces the same ack-resolved routing decisions".

**Reminder cadence is wall-clock and is *not* in the pipeline.** "Re-page about this still-unacked alert every N minutes" fires on elapsed real time, not on event arrival. It cannot be replay-deterministic, and a timer inside `RealtimePipeline` would reintroduce both the daemon shape (D-4) and the `datetime.now()` the spine forbids everywhere. So cadence lives in an operational scheduler explicitly outside the deterministic path: it consumes routed alerts and current ack state and fires reminders on its own clock. Phase 5 *names and bounds* this seam in `docs/alert-routing.md` but does not build the loop; the scheduler is a follow-up. A replay run, routing to a sealed sink, never exercises cadence — there is nothing to re-page.

**The boundary that matters:** the same conceptual feature ("alerting lifecycle") straddles the determinism line, and the split is *which state is reconstructable from the event stream*. Ack state is, because acks are events; cadence state is not, because elapsed wall-clock time is not an event. The discipline is to push everything that *can* be an event projection onto the deterministic side, and fence the irreducibly-wall-clock remainder into a clearly-labelled operational component — rather than letting the wall-clock part contaminate the pipeline because it happened to be bundled with the deterministic part in the brief.

**Trade-off considered:** an operational store (DynamoDB) holding *both* ack and cadence state, outside the replay path entirely. Rejected for ack state because it would forfeit replay determinism of routing decisions for no reason — acks *can* be events, so making them events is strictly better. Accepted for cadence, which has no event-shaped form. The split is the point: same store would have been simpler to write and wrong about half of what it held.

**Relationship to D-4, D-5, D-12:** D-4 keeps the pipeline function-shaped (no event loop); D-5 makes detections a return value so replays reproduce them; D-12 keeps the live-vs-replay switch in one place. D-14 is those three applied to alerting: the deterministic part (routing, ack projection) stays in the function-shaped pipeline and reproduces under replay; the non-deterministic part (cadence) is fenced out, and the replay isolation that protects it is the same `for_replay()` sink swap D-12 established.

This entry exists because "finish the alerting layer" naturally tempts a future phase to add a reminder timer to the pipeline — and that single, reasonable-looking addition would be the first wall-clock dependency in the deterministic core. The boundary is worth recording as a decision so it is defended deliberately, not rediscovered after a replay run starts behaving non-reproducibly.

### D-15 — Dashboard projections update per-emission, inside the deterministic path

Dashboard projections (Phase 6) update on every relevant event, not on a
wall-clock timer or per-window batch. A per-emission fold over the ordered
event stream is replay-deterministic — it reconstructs the same view on
replay, the property DeviceRegistry and AcknowledgementRegistry already
have — whereas wall-clock-batched flushing would not be.

This is the mirror of D-14. There, reminder cadence was fenced *out* of the
deterministic path because it is inherently wall-clock. Here, projection
updates are kept *in* the deterministic path because they can be: they are
a function of the events, not of elapsed time. Same boundary, opposite side.

The cost is write amplification — every event touches the projection store —
which is DDIA's central materialised-view trade-off (write cost vs read
freshness). For this platform's scale and the sub-5-second dashboard
requirement, always-fresh is the right call; the cost is named, not hidden.

### D-16 — OfflineDetector emits a device.online recovery event, revisiting D-7

D-7 established "once per offline transition; recovery clears state silently, no recovery event" — correct for Phase 2, whose only prospective consumer (alert routing) could infer recovery from the absence of further offline detections. Phase 6's OfflineCountProjection cannot: a current offline-count-per-store gauge needs an explicit signal to decrement on, and "absence of further offline events" is not a signal a fold can consume.

So the detector now emits a device.online DetectionEvent (severity INFO) on the offline -> seen transition, mirroring the offline emission's shape and derived identity (derive("detection.device_online", store_id, device_id, source_event.event_id)). A seen -> seen or unseen -> seen transition is not a recovery and emits nothing; an unregistered store at recovery time skips emission, the same contract the offline path follows.

This does not contradict D-7 so much as meet the condition D-7 implicitly waited for: recovery becomes an event when a consumer genuinely needs it as one. The discriminator-pattern schema (D-8) made this a zero-cost upstream change — device.online matches the detection_type pattern constraint, so no contract bump was required, only a new type constant and the emission. This is the D-11 "budget for an upstream PR" turning out unspent, exactly as the Phase 6 plan allowed for.

Relationship to D-10: the same lesson at a different boundary. D-10 said trace a deferred callable's production data path before accepting the deferral; D-16 is what happens when a later phase's consumer needs a signal an earlier phase chose not to emit — caught cleanly because the projection's data path was traced before the projection was built, not after.


### D-17 — Dashboard projections fold via idempotent set membership, not counters

OfflineCountProjection needs a current-offline-count-per-store gauge. The naive fold is an integer counter: increment on device.offline, decrement on device.online. It is wrong. Detections reach a projection over an at-least-once channel — EventBridge re-delivers, and replay re-runs the same event sequence (the reason Phase 5's alert idempotency keys exist, D-14). A duplicated device.offline permanently inflates a counter and the drift never heals.

So the projection holds, per store, the set of currently-offline device ids: device.offline adds the id, device.online discards it, and the gauge is the set's cardinality. Adding a present id is a no-op; discarding an absent id is a no-op. The fold is therefore idempotent under duplicate delivery, and correct under flapping (offline -> online -> offline collapses to cardinality 1) and under a device going offline twice without an intervening recovery. The set is serialised as a sorted JSON array so the persisted bytes are arrival-order-independent; the key is deleted when the set empties, bounding the view to currently-affected stores.

This is the DDIA Chapter 11 materialised-view-over-a-Chapter-9 at-least-once-stream discipline: a fold over an at-least-once stream must be idempotent or the view rots. The principle is not specific to offline counts — the other two Phase 6 projections (active-outage set cardinality, anomaly-rate rolling window) face the same duplicate-delivery condition and inherit the same default: prefer an idempotent set/membership fold over an accumulating counter wherever the input is a redeliverable event. Counters are admissible only where the increment is itself keyed by something that makes redelivery a no-op.


### D-18 — OutageDetector emits a store.recovered event on clear, mirroring D-16

D-7 made the same call for OutageDetector that it made for OfflineDetector: transition into outage emits, transition back to not-outage clears state silently. Correct for Phase 2, whose only consumer (alert routing) could infer recovery from the absence of further outage detections. Phase 6's ActiveOutageProjection cannot: a current-active-outage view needs an explicit signal to remove a store from the active set on, and "absence of further outage events" is not a signal a fold can consume — the identical gap D-16 closed for offline.

So the detector now emits a store.recovered DetectionEvent (severity INFO) on the outage -> not-outage transition. The clear is even cleaner to emit than offline's was: the transition is already a discrete window emission the detector handles and computes everything a recovery event needs (store_id, window bounds, the now-sub-threshold ratio); it simply declined to emit. Identity is derived the same way the outage event's is (derive over store_id and window bounds), so it stays replay-deterministic. A below-threshold emission for a store not in outage is not a recovery and emits nothing — the same "once per transition" contract the outage entry follows.

As with D-16 this was a zero-cost upstream change: the discriminator pattern (D-8) means store.recovered is a new detection_type string matching the existing constraint, not a contract bump — a new type constant and the emission, nothing more. The pattern is now established twice (device.online, store.recovered): a detector's silent-clear becomes an emitted recovery event the moment a current-state consumer needs the transition as a signal. The third detector (AnomalyDetector) will face the same question if and when a projection needs its recovery; the move is the same and cheap.


### D-19 — Global current-set projections use key-per-presence, not a single hot key

OfflineCountProjection is a partitioned view: one gauge per store, read by store id. ActiveOutageProjection is the first global view — "which stores are in outage right now, and how many" — and global views invite a tempting layout: one fixed key holding the whole set, read in a single round-trip. Rejected. Every transition would be a read-modify-write on that one key, and under a fleet-wide event many stores transition at once, making it a contention point and, on the DynamoDB backend, a hot partition (DDIA Chapter 6).

So a global current-set is stored as one key per present member under a shared view: store.outage writes the store's key, store.recovered deletes it, the set is keys(view) and its cardinality is the count. Each transition is an independent point write that contends with nothing, and it reuses the exact keys(view)-enumerates-the-set pattern OfflineCountProjection already uses, so partitioned and global projections read consistently. The idempotency property from D-17 carries over unchanged: a point write is last-write-wins (duplicate delivery is a no-op), a delete of an absent key is a no-op.

The cost accepted is that counting the set is a keys(view) enumeration rather than a single read — a bounded scan, since the number of currently-affected members is small even in a bad event. The stored value carries free provenance (the outage's detected_at) rather than a bare sentinel; the fold's own logic only reads presence. The third projection (anomaly rate) faces the same global-view question and inherits this default: prefer key-per-presence over a single accumulating key wherever the view is a current set rather than a per-entity gauge.

### D-20 — The replay driver is function-shaped over an injected source and an injected pipeline-builder

Phase 7's replay driver takes the same function-shaped form the pipeline itself took (D-4), one level up. `run_replay(settings, *, event_source, build_pipeline)` builds a pipeline from `settings.for_replay()`, feeds the injected event source through `process_batch`, logs the replay lineage, and returns the results. The event source is an injected `Iterable[TelemetryEvent]`: production backs it with an EventBridge-archive reader, the determinism test backs it with a fixed list. The archive reader itself is kept thin or deferred — it is AWS plumbing the definition of done explicitly places out of scope ("not deployed to AWS", "not validated against production traffic"), and it proves nothing about determinism.

The load-bearing decision is that the driver takes a `build_pipeline: Callable[[PlatformSettings], RealtimePipeline]`, not a pre-built pipeline. A pre-built pipeline has already constructed its sinks against some settings, so the driver could not re-point them at replay targets; `for_replay()` has to be threaded into sink *construction*, and sinks are constructed during registration. So the caller supplies a builder that performs the full registration sequence against whatever settings it is handed, and the driver calls it with `settings.for_replay()`. The same builder constructs both the live and replay pipelines; the only difference is the settings passed in.

That "same builder, different settings" property is what makes replay verifiable rather than merely plausible. The determinism integration test the definition of done calls out defines one builder — dataset writer plus projections routed via `route_detections` — and invokes it twice: once with live settings, once through `run_replay` with the same fixed event source. Byte-identical dataset Parquet and content-identical projection-table rows follow from the builder being identical and the settings differing only in their replay-isolated sink targets. If the two runs diverged, the divergence could only be in settings-driven sink routing or in a determinism regression upstream — both of which the test localises.

Replay isolation needs no flag and no gating. The driver builds its own pipeline via the builder, so it has its own `WatermarkManager`, its own aggregator state, its own everything; there is no shared mutable state with any live pipeline to isolate. The only isolation required is that the sinks point at replay targets, which `for_replay()` already guarantees (D-12). Isolation is the *absence of shared state*, which is what function-shaped bought the project in Phase 1. This is why Phase 7 is small: every prior phase cut the seam it needs — Phase 4's byte-reproducible Parquet, Phase 5's environment-gated alert sink, Phase 6's replay-table swap — and the driver orchestrates over them rather than building new isolation machinery.

## Known issues

Things we know about and have decided how to handle.

### Active

- **`AnomalyRateProjection` in-memory store grows without bound.** Eviction-(i): the projection's `observe()` is a pure bucketing fold with no prune, by design. Bucket storage is bounded by the backend, not the fold - the DynamoDB store (Phase 6 commit 8) will set a TTL on bucket keys from the retention horizon. The `InMemoryProjectionStore` does not evict, so a long-running non-DynamoDB process accumulates one key per signal per time bucket indefinitely. Acceptable for tests (short-lived) and for the DynamoDB-backed production path; flagged so the TTL is not forgotten when the DynamoDB store lands.

- **`structured-logging-python` emits stdlib-logging warnings during `error()` calls.** Every test run produces four `--- Logging error ---` lines from `tests/streaming/test_observability.py`. Tests pass; the artefact pollutes test output. Pre-existing since Phase 1; deterministic; reproducer is a single 4-line `python3 -c` snippet. Decision pending: fix upstream now (small PR + SHA bump) vs defer to a later cleanup pass.

- **`event-schema-contracts` ruff backlog: 161 warnings, 127 auto-fixable.** Pre-existing on the upstream main branch; not in our new code. Cleanup PR queued for after Phase 2 lands.

- **`event-schema-contracts` top-level `__init__.py` doesn't eagerly import domains.** Consumers must import each domain explicitly (`event_schema_contracts.detection.X`) for schemas to register on the registry. Convention works; ergonomic wart. Possible future tidying.

- **CI runner Node 20 deprecation.** GitHub Actions emits a deprecation warning that `actions/checkout@v4` and `actions/setup-python@v5` run on Node 20, which will be forced to Node 24 from 2026-06-16 and removed from runners 2026-09-16. CI is green and unaffected for now; the fix (bump the action versions) is a small CI PR, due before the September removal.

### Deferred

- **`DeviceRegistry` component** for `OutageDetector`'s registered-device-count lookup. Phase 2 takes a constructor callable; Phase 3 or 4 introduces a real registry component when feature pipelines also need it.

- **Watermark observers in `RealtimePipeline`.** Phase 2 uses scan-on-event for offline detection. Phase 2.5 may add watermark observers as a focused follow-up if scan-on-event measures poorly at fleet scale.

- **`mypy --strict` on the `tests/` directory.** Currently mypy scans `stream_pipeline/` only. Test code is type-hinted but not strict-checked. Defer until we have concrete cases of test type-bugs that strict checking would have caught.

## How resumption works

If you're picking this up after a gap (or you're a fresh AI session), do this:

1. Read `RESUMING.md` at the repo root.
2. Read this file (`docs/working-notes.md`).
3. Read the latest file in `docs/status/` for the most recent state snapshot.
4. Read `docs/phases/phase-N-plan.md` for the active phase, where N is the highest-numbered plan file.
5. `git --no-pager log --oneline | head -20` to see the most recent commits.

That should rebuild full context in 15 minutes of reading. Then run the resume protocol from `RESUMING.md` to verify your environment is in a known-good state before doing any work.
