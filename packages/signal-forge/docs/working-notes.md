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

The platform decomposes into five repositories. `signal-forge` (this repo) is the analytics control plane. The other four are upstream contract and infrastructure packages: `event-schema-contracts`, `structured-logging-python`, `telemetry-parser`, `aws-event-pipeline-infra`.

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
- `mypy signal_forge` before staging
- `python3 -m unittest discover -s tests` before staging

After any commit:

- `git --no-pager show --stat HEAD` to confirm what landed matches what was intended

These aren't optional. Twice in Phase 1 a commit nearly shipped with missing files because we skipped the post-commit verification. The cost of running these is seconds; the cost of not running them is debugging "why did this break in CI" cycles.

### Pre-merge CI

`gh pr checks --watch` before any `gh pr merge`. Three times in this project we've nearly merged before CI confirmed green. The pattern is now: open PR, run `gh pr checks --watch`, merge only after explicit green confirmation.

### Pasting type definitions before writing tests against them

When writing tests that construct Phase 1 types (`WindowEmission`, `RealtimePipeline`, `ProcessingResult`, etc.), I (the AI) ask you to paste the type's current definition before I write the test. Memory-based test code shipped two bugs in Phase 2 (`FakeTrace` failing pydantic validation, `WindowEmission`'s `aggregator_name` vs `aggregation_name` field rename) that paste-then-write would have caught.

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

## Known issues

Things we know about and have decided how to handle.

### Active

- **`structured-logging-python` emits stdlib-logging warnings during `error()` calls.** Every test run produces four `--- Logging error ---` lines from `tests/streaming/test_observability.py`. Tests pass; the artefact pollutes test output. Pre-existing since Phase 1; deterministic; reproducer is a single 4-line `python3 -c` snippet. Decision pending: fix upstream now (small PR + SHA bump) vs defer to a later cleanup pass.

- **`event-schema-contracts` ruff backlog: 161 warnings, 127 auto-fixable.** Pre-existing on the upstream main branch; not in our new code. Cleanup PR queued for after Phase 2 lands.

- **`event-schema-contracts` top-level `__init__.py` doesn't eagerly import domains.** Consumers must import each domain explicitly (`event_schema_contracts.detection.X`) for schemas to register on the registry. Convention works; ergonomic wart. Possible future tidying.

### Deferred

- **`DeviceRegistry` component** for `OutageDetector`'s registered-device-count lookup. Phase 2 takes a constructor callable; Phase 3 or 4 introduces a real registry component when feature pipelines also need it.

- **Watermark observers in `RealtimePipeline`.** Phase 2 uses scan-on-event for offline detection. Phase 2.5 may add watermark observers as a focused follow-up if scan-on-event measures poorly at fleet scale.

- **`mypy --strict` on the `tests/` directory.** Currently mypy scans `signal_forge/` only. Test code is type-hinted but not strict-checked. Defer until we have concrete cases of test type-bugs that strict checking would have caught.

## How resumption works

If you're picking this up after a gap (or you're a fresh AI session), do this:

1. Read `RESUMING.md` at the repo root.
2. Read this file (`docs/working-notes.md`).
3. Read the latest file in `docs/status/` for the most recent state snapshot.
4. Read `docs/phases/phase-N-plan.md` for the active phase, where N is the highest-numbered plan file.
5. `git --no-pager log --oneline | head -20` to see the most recent commits.

That should rebuild full context in 15 minutes of reading. Then run the resume protocol from `RESUMING.md` to verify your environment is in a known-good state before doing any work.
