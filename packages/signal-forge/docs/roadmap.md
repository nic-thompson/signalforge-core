# SignalForge — roadmap

> **Purpose.** Map the full scope of the project with realistic time estimates and explicit milestones. Capture the definition of done so we can recognise the destination when we get there.
>
> **Scope.** This document is for human planning, not automated tooling. Estimates are honest expectations, not commitments. Per-phase plans (`docs/phases/phase-N-plan.md`) carry detailed task lists; this document is the long-range view.
>
> **Status.** Phases 1–7 are complete and `v1.0.0` is tagged. The estimates and risks below were written during Phase 2 and are left as they were: they are a record of what was expected, which is more useful than a retrospective tidy-up. "Beyond v1.0.0" at the end covers what comes next, and is written to a different standard — it gives shape rather than hours, because these phases depend on AWS behaviour that has repeatedly turned out to differ from what the documentation implied.

## Definition of done

The project is complete when all of the following are true:

1. **All seven phases merged to `main` via reviewed PRs.** Each phase tagged with `phase-N-complete`.
2. **CI green at the tip of `main`.** ruff, `mypy --strict`, pytest matrix on Python 3.11 and 3.12, regression guards passing.
3. **Test coverage demonstrably exercises the brief's requirements.** Each requirement traceable to specific tests via the per-phase acceptance criteria.
4. **Documentation complete.** All brief-mandated documents exist (`architecture.md`, `streaming-internals.md`, `replay-workflows.md`, `detection-models.md`, `dataset-lifecycle.md`, `store-outage-detection.md`, plus per-phase plans). Each major component has a module docstring explaining its contract.
5. **Replay determinism verified.** A replay test exists that runs an identical event sequence twice and asserts byte-identical outputs across both runs.
6. **Operational ergonomics achieved.** A new contributor can clone the repo, run `pip install -e ".[dev]"`, run the test suite, and have everything pass within ten minutes. CI replicates this exactly.

What "done" deliberately does NOT mean:

- Deployed to AWS. The infrastructure code lives in `aws-event-pipeline-infra`; this repo's job is the analytics control plane logic, not the deployment.
- Validated against production-volume traffic. Performance tuning at fleet scale is a separate engagement.
- Multi-region or HA. Not in the brief; not in scope.
- Statistical or ML-based anomaly detection. Threshold-driven anomaly detection in Phase 2 is sufficient for the brief; richer models are a follow-up project if and when needed.

## Phases

Estimates are in working hours of focused engineering. They assume Option A workflow (fine-grained commits, full hygiene, careful design conversations). Multiply by 1.5–2.0 for elapsed calendar time including breaks and review cycles.

### Phase 1 — Streaming foundation ✅ Complete

**Delivered:** Event router, watermark manager, window aggregator, realtime pipeline; platform settings; three documents; GitHub Actions CI; test-count regression guard.

**Test count:** 100. **Commits:** 10.

**Estimate vs actual:** Estimated 6–8 hours. Actual ~10 hours.

### Phase 2 — Detection engines ✅ Complete

**Delivers:** `OfflineDetector`, `OutageDetector`, `AnomalyDetector`. Pipeline integration for detector dispatch and detection collection. Trace propagation through window emissions (Phase 1 dataclass extension). Detection-event schema upstreamed.

**Estimated test count delta:** 100 → ~140. **Estimated commits:** 12 (including the upstream PR's commits).

**Estimate:** 8–10 hours. **Tracked progress:** ~6 hours done, ~3 hours remaining.

### Phase 3 — Feature pipelines ✅ Complete

**Delivers:** Online feature aggregations (counts, sums, means, distincts) for use by detectors and downstream consumers. A `DeviceRegistry` component to replace the constructor-callable hack in Phase 2's `OutageDetector`. Feature emissions written to a sink the dataset layer (Phase 4) can pick up.

**Design questions to settle when we get there:**

- Where does feature-vector schema live? Probably alongside `detection.event` upstream — `event-schema-contracts.features` already has `feature.vector v1`. Likely we extend it rather than create a parallel structure.
- How does `DeviceRegistry` synchronise with `device.registration` events? Probably as a router-subscribed handler maintaining a `dict[store_id, set[device_id]]`. Replay-deterministic.
- What aggregation strategies beyond Phase 1's `CountAggregation` and `SumAggregation`? `MeanAggregation`, `DistinctCountAggregation` likely; `QuantileAggregation` a maybe.

**Estimated test count delta:** ~140 → ~190. **Estimated commits:** 8–12.

**Estimate:** 10–14 hours.

### Phase 4 — Dataset partitioning and S3 export ✅ Complete

**Delivers:** A dataset layer that partitions emissions and detections by `(store_id, hour)` (or similar) and writes them to S3 in Parquet. Replay-aware: separate sinks for live and replay outputs. Honours the brief's 2-year retention requirement via S3 lifecycle policies (declared in this layer's docs; configured upstream in `aws-event-pipeline-infra`).

**Design questions to settle:**

- File size targeting (Parquet row groups, hourly partitions vs daily) — DDIA Chapter 11 ("Batch Processing") gives useful framing on file size for downstream batch consumers.
- Schema evolution at the dataset layer — when an upstream contract bumps from v1 to v1.1, what does the dataset writer do? Probably accept both, write the union schema, and let consumers ignore unknown fields. This is the classic data-lake forward-compatibility problem.
- Compaction. Do we compact on write, on read, or run a separate compaction job? Probably "compact on read" via Parquet's predicate-pushdown for v1; revisit if performance demands.

**Estimated test count delta:** ~190 → ~240. **Estimated commits:** 10–14.

**Estimate:** 14–18 hours. The S3 mock infrastructure is the time sink — `moto` or `localstack` setup is non-trivial.

### Phase 5 — Alert routing ✅ Complete

**Delivers:** A consumer of `ProcessingResult.detections` that routes detections to alert sinks (EventBridge, SNS, conceptually also Slack/PagerDuty). Severity-aware: CRITICAL paged immediately, WARNING digested. Acknowledgement and reminder cadence policies (the bit we deferred from Phase 2's `OfflineDetector` design).

**Design questions to settle:**

- Push vs pull consumer model. Probably push (the pipeline calls into a router after each `process()`), but a pull model has merit for replay isolation. The "function-shaped pipeline" principle from Phase 1 suggests push.
- Acknowledgement state. Where does it live? Possibly DynamoDB (operational store, low latency); possibly a feature of the pipeline itself for replay determinism. Replay-aware acknowledgement is a real complication.
- Idempotency keys for downstream alert systems. EventBridge has its own; we should produce alerts with stable keys derived from `detection_id`.

**Estimated test count delta:** ~240 → ~280. **Estimated commits:** 8–12.

**Estimate:** 12–16 hours.

### Phase 6 — Dashboard projections ✅ Complete

**Delivers:** Materialised views suitable for a sub-5-second dashboard. Aggregations of detections by store, by detection_type, by severity, over rolling time windows. Probably written to DynamoDB or a similar low-latency store. The dashboard itself isn't in scope; the projections that feed it are.

**Design questions to settle:**

- Which projections does the dashboard actually need? Probably "current offline count per store", "active outage count", "anomaly rate by signal type". Confirm with stakeholders before building.
- Storage: DynamoDB is the obvious choice for AWS-native low-latency. Consider also keeping projections materialised in-memory for tests.
- Update model: every emission updates the projection (write amplification but always-fresh) or batched per-window (less write traffic, slightly stale). Probably the former for sub-5s freshness.

**Estimated test count delta:** ~280 → ~320. **Estimated commits:** 6–10.

**Estimate:** 10–14 hours.

### Phase 7 — Replay and backfill orchestration ✅ Complete

**Delivers:** The Step Functions workflow integration referenced in `docs/replay-workflows.md`. Takes a `(start_time, end_time, event_pattern)` window, iterates archived events from the EventBridge archive, feeds them through a `RealtimePipeline` configured for replay (`PlatformSettings.for_replay()`), routes outputs to a sealed replay sink (separate S3 bucket, separate alert sink in Phase 5).

**Design questions to settle:**

- How is the replay driver invoked? Probably a CLI entry-point that takes JSON config (the same shape Step Functions passes). Tests run the CLI directly with synthetic configs.
- Replay isolation. The replay run must NOT advance the live watermark or trigger live alert routing. Achieved via `PlatformSettings.for_replay()` (already in Phase 1) and the alert-routing layer's environment-aware gating (Phase 5).
- Replay verification. A test that runs the same event sequence through both the live pipeline and the replay driver, asserting byte-identical outputs. This is the determinism integration test the project's "definition of done" calls out.

**Estimated test count delta:** ~320 → ~360. **Estimated commits:** 8–12.

**Estimate:** 12–16 hours.

## Estimated total

| Phase | Estimate (hours) | Cumulative |
|---|---|---|
| 1 | 6–8 (actual ~10) | ~10 |
| 2 | 8–10 | 18–20 |
| 3 | 10–14 | 28–34 |
| 4 | 14–18 | 42–52 |
| 5 | 12–16 | 54–68 |
| 6 | 10–14 | 64–82 |
| 7 | 12–16 | 76–98 |

**Realistic total: 80–100 working hours.** Calendar time depending on cadence — at three to five focused hours per week, that's 16–33 weeks (4–8 months).

This is consistent with the brief's complexity and a production-quality bar. A "demo" version of this work would be ten to twenty hours and look superficially similar but lack replay determinism, schema evolution, CI gates, and the documentation discipline that makes the work actually maintainable.

## Milestones

A milestone is a commit on `main` plus a tag plus a status snapshot. The tags map directly to the phase-completion checkpoints.

| Milestone | Tag | Achieved when |
|---|---|---|
| Streaming foundation | `phase-1-complete` | ✅ Achieved |
| Detection engines | `phase-2-complete` | Phase 2 PR merged |
| Feature pipelines | `phase-3-complete` | Phase 3 PR merged |
| Dataset layer | `phase-4-complete` | Phase 4 PR merged |
| Alert routing | `phase-5-complete` | Phase 5 PR merged |
| Dashboard projections | `phase-6-complete` | Phase 6 PR merged |
| Replay orchestration | `phase-7-complete` | Phase 7 PR merged |
| Project complete | `v1.0.0` | All phases merged, definition-of-done satisfied |

## Beyond v1.0.0

The definition of done above deliberately excluded deployment. That exclusion held while this repository was the analytics control plane and nothing else existed to deploy against. It no longer holds: `aws-event-pipeline-infra` has a bus, five stage queues, an archive and a working replay workflow deployed to dev, and as of 30 August 2026 real events flow into it.

The two halves have never met. Everything below follows from that.

These phases are described in shape rather than hours. The estimates in the section above were made about code in this repository, where the unknowns were design ones. The phases below depend on AWS behaviour, and this project's recent experience is that AWS behaviour differs from what the documentation implies more often than is comfortable — a replay workflow deployed to three environments since 21 August turned out to have four independent defects, none of which could have been found without running it.

### Phase 8 — Connecting the streaming path

The callers. Nothing polls the ingestion queue; nothing reads the archive. `signal_forge.replay`'s production event source still raises `NotImplementedError`. See `docs/phases/phase-8-plan.md`.

The design question is settled by the architecture rather than by preference: the pipeline holds watermark and open-window state across `process_batch` calls, so its caller must be a long-running process rather than a function invocation. A Lambda would lose open windows on recycle, and windows would silently never close.

### Phase 9 — A second producer

The platform claim is currently untested. This repository consumes one event type from one producer, and "adding a source requires no change to the middle" is a property we believe from the design rather than from evidence.

Customer call points are the natural first: a button press and its acknowledgement are two schemas, one producer, and a response-time metric that means something operationally. If the bus, archive, replay and audit machinery genuinely need no change, the claim holds. If they do, better to know.

Note that this producer is API-sourced rather than packet-sourced. It needs no reassembly, no framing, no protocol parsing — which makes it a much smaller component than `telemetry-parser`, and a fair test of whether the ingestion path generalises.

### Phase 10 — Widening the domain

Shelf alerts, EPOS override requests, lone-worker alarms — each a schema and a producer following the pattern Phase 9 establishes. This is where the event vocabulary stops being about one protocol.

### Phase 11 — Warehouse

Athena over the dataset-export bucket, once several event types exist to join. Response times by store, alert clustering by time of day, override rates by till. Deliberately after Phase 10: a query layer over a single event type gives you a table of registrations and little to ask of it.

### Phase 12 — Reconsider the SIP path

The headsets are DECT. They communicate on localised wireless frequencies rather than the store network, which means a SIP REGISTER is not a headset checking in — at most it is a base station or PBX bridge doing so.

So `sip.registration` measures the health of one integration point, not of the fleet, and `device_label` carrying values like `headset-12` implies the parser sees headsets when it probably does not. That is a problem with what the data means rather than with any code, and it should either be narrowed to claim what it actually observes or retired.

### The standing caveat

Every event in this system is synthetic, generated from a contract that was authored rather than observed, describing devices that do not exist. `telemetry-parser`'s ADR-001 records why: with no fleet to observe, the edge producer contract could only be written, not discovered.

That is defensible and documented. It gets less defensible with each phase that builds further on it, and Phases 9 and 10 in particular involve inventing what a button press or a shelf alert reports. Modelling a documented product feature is a smaller fiction than inventing protocol internals — but it is still a fiction, and the distance between this system and one fed by real traffic grows rather than shrinks as it is extended.

## Risks and mitigations

The actual risks to delivery, called out so they don't surprise us.

### Scope creep at the upstream boundary

We've already done two unscheduled upstream contributions: adding `detection.event` to `event-schema-contracts` and adding CI to it. Both were the right call. But every phase will tempt us to "just fix this small thing upstream while we're here". The mitigation: any upstream change beyond a SHA bump gets explicitly justified in the phase plan. If the change is more than a small PR, it gets its own time budget rather than being squeezed into the consumer phase.

### Underestimating Phase 4 (dataset layer)

S3 mock infrastructure (`moto` or `localstack`) is the most plausibly-time-consuming piece in the project. The current 14–18 hour estimate assumes a smooth setup. If we hit fights with mock-AWS test infrastructure, that estimate could double. Mitigation: scope Phase 4's first commits as scaffolding and verification of the test infrastructure before any dataset logic gets written.

### Reaching Phase 7 with insufficient determinism testing earlier

Replay-determinism is the project's most distinctive property and a definition-of-done item. If the discipline of "no `datetime.now()` in pure components" slips earlier, Phase 7's replay test will fail and we'll be retrofitting determinism back into earlier phases. Mitigation: each phase's plan should include a "what could break replay determinism here?" review before its first commit.

### Learning curve outpacing the schedule

Building a production-quality platform while learning Platform Engineering is reasonable but will mean some phases take longer than the estimate as concepts land. The mitigation isn't to compress the estimates; it's to recognise that the estimates assume baseline competence in the underlying domain and that part of every phase is also developing that competence. Be honest about which phase costs are "implementation" vs "learning"; both are valid uses of time.

## Cross-references

- `RESUMING.md` for orientation.
- `docs/working-notes.md` for engineering principles, decisions, and the cumulative project diary.
- `docs/phases/phase-8-plan.md` for the active phase. Each phase gets its own plan file when started.
- DDIA chapters most relevant to upcoming phases: 11 (Batch Processing) for Phase 4, 9 (Distributed Systems) and 10 (Consistency and Consensus) for Phase 5, 12 (Stream Processing, Reasoning About Time) throughout.
