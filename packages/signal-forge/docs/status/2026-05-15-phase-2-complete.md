# Status snapshot — 2026-05-15 — Phase 2 complete

> Snapshot captured at the merge of PR #2. Phase 2 is on `main` and tagged `phase-2-complete`. The branch `feat/phase-2-detection-engines` no longer exists locally or remotely.

## Where we are

`main` sits at the merge commit for Phase 2's PR. The branch is twelve commits past `phase-1-complete`:

```
chore(ci): bump test-count baseline from 100 to 144 for Phase 2
docs(detection): add detection-models reference document
feat(detection): AnomalyDetector emits on threshold crossings in either direction
feat(detection): OutageDetector emits on store-level offline-ratio crossings
feat(streaming): add DistinctCountAggregation
feat(detection): OfflineDetector emits on seen->offline transitions
feat(detection): wire detector dispatch into RealtimePipeline
docs(project): add working notes, phase plan, status snapshot, roadmap, RESUMING
feat(streaming): extend WindowEmission with last_contributing_trace_id
feat(detection): add EventDetector and EmissionDetector protocols
feat(detection): add detection-type constants and package layout
chore(deps): bump event-schema-contracts to 8643cef
```

144 tests passing. ruff and `mypy --strict` clean across 24 source files. CI green on `main` at the merge commit (ruff, mypy, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian security scan).

Working tree clean. Tag `phase-2-complete` pushed to origin.

## What landed in Phase 2

**Three detectors** (`signal_forge/detection/detectors/`):

- `OfflineDetector` — `EventDetector` protocol, severity WARNING. Emits when a previously-seen device falls silent past `OFFLINE_THRESHOLD_SECONDS`. Scan-on-event semantics; state-machine `unseen → seen → offline` with silent recovery transitions. Takes a `device_id_extractor` and `store_lookup` callable in its constructor for decoupling from event schemas.
- `OutageDetector` — `EmissionDetector` protocol, severity CRITICAL. Emits when more than `OUTAGE_THRESHOLD_RATIO` of a store's devices are not reporting in a window. Subscribes to a `DistinctCountAggregation` named `distinct_devices`. Takes a `registered_count_lookup` callable.
- `AnomalyDetector` — `EmissionDetector` protocol, severity WARNING. Emits when a windowed signal value crosses a threshold in either direction (`comparison="above" | "below"`). Subclass per signal for routing.

All three share the state-machine pattern: transition into detection state emits one `DetectionEvent`; sustained state emits nothing more; recovery clears state silently. A flapping entity produces a fresh detection on each new entry.

**Pipeline integration** (`signal_forge/streaming/realtime_pipeline.py`):

- `ProcessingResult.detections: list[DetectionEvent]` — detections flow back as a typed return value, not as side effects.
- `register_event_detector()` and `register_emission_detector()` methods.
- Dispatch loops in `process()` with per-detector failure isolation (raising detectors logged with full lineage and skipped, others continue).
- Log-line metadata updates: `pipeline.processed` and `pipeline.batch_summary` carry detection counts; `pipeline.detector_error` captures failures.

**Two protocols** (`signal_forge/detection/protocols.py`):

- `EventDetector` consumes raw `TelemetryEvent`s.
- `EmissionDetector` consumes `WindowEmission`s routed by `aggregation_name`.
- `typing.Protocol` (structural typing), not abstract base classes. Static-checked via `mypy --strict`. Deliberately not `@runtime_checkable`.

**Streaming-layer additions:**

- `WindowEmission.last_contributing_trace_id` — Phase 1 dataclass extension carrying the most-recent contributor's `trace_id` through to emission detectors. Enables detection lineage.
- `DistinctCountAggregation` — counts cardinality of an extracted key. Used by `OutageDetector`. Set-mutation-in-place chosen over allocation-per-contribution for fleet-scale GC pressure.

**Upstream contribution** (`event-schema-contracts` v0.2.0 at `8643cef`):

- `DetectionEvent` and `DetectionEventPayload` schemas via the discriminator pattern (`detection_type: str` + `details: dict[str, Any]`).
- CI added to the upstream (didn't exist before).
- `mypy --strict` cleanup across 10 pydantic validator signatures.

**Documentation:**

- `docs/detection-models.md` (216 lines) — the brief-mandated detector reference document. Common design contract, per-detector sections, explicit non-goals.
- `docs/working-notes.md` — long-lived engineering principles, decision log (D-1 through D-9), working agreements, known issues.
- `docs/roadmap.md` — seven-phase plan with estimates, milestones, and definition of done.
- `docs/phases/phase-2-plan.md` — Phase 2's plan, retained as historical record.
- `docs/status/2026-05-08-phase-2-mid-flight.md` — earlier snapshot capturing mid-flight state.
- `RESUMING.md` — orientation file at the repo root.

**CI:**

- Test-count regression guard baseline bumped from 100 to 144.

## What's deliberately not in Phase 2

- **Recovery notifications.** Devices coming back online or stores recovering from outage produce no detection. Phase 5 alert routing owns the recovery lifecycle alongside acknowledgement state.
- **Reminder cadence.** A device offline for 8 hours produces one detection, not 96. Phase 5's concern.
- **Cross-detector correlation.** No deduplication between `OfflineDetector` and `OutageDetector` firing for the same store. Phase 6 dashboard projections concern.
- **Statistical anomaly models.** `AnomalyDetector` is threshold-driven. Statistical models (z-scores, EWMA, isolation forest) are a follow-up project per the roadmap.
- **`DeviceRegistry` component.** `OutageDetector` and `OfflineDetector` take constructor callables for store/device lookups. The real registry component lands in Phase 3 or 4 when feature pipelines also need it.
- **Watermark-observer-driven offline detection.** Phase 2 uses scan-on-event. Phase 2.5 may add watermark observers if measurement shows it's a bottleneck.

## Open issues

- **`structured-logging-python` emits stdlib-logging warnings during `error()` calls** in `tests/streaming/test_observability.py`. Four `--- Logging error ---` lines per test run. Pre-existing since Phase 1; deterministic; reproducer is a 4-line `python3 -c` snippet. Decision deferred: a fix would be a small upstream PR plus another SHA bump, but the bug doesn't propagate to production behaviour. Likely fix during Phase 3 or as a focused cleanup PR.
- **`event-schema-contracts` ruff backlog**: 161 warnings, 127 auto-fixable, pre-existing on the upstream main branch. Cleanup PR queued for after Phase 2 lands — which is now.
- **`event-schema-contracts` top-level `__init__.py` doesn't eagerly import domains.** Consumers must import each domain explicitly (`event_schema_contracts.detection.X`) for schemas to register. Ergonomic wart; possible future tidying.

## Estimate vs actual

Phase 2 was estimated at 8-10 hours in the roadmap. Actual was around 12-15 hours of focused work across multiple sessions, plus the upstream PR (~3 hours) that wasn't on the original Phase 2 plan but was the right call.

The overrun is consistent with the roadmap's "Learning curve outpacing the schedule" risk: a meaningful fraction of Phase 2's time was Platform Engineering learning rather than implementation pace. Both are valid uses of time per the agreed working principles.

## Process observations worth carrying forward

- **The WIP-commit-then-fold pattern** worked well for the B.3 test sequence. Six WIP commits collapsed cleanly via `git reset --soft <docs commit>` into one coherent "wire detector dispatch into RealtimePipeline" commit. The history reads as one decision, not seven.
- **The verification-after-commit discipline** (`git --no-pager show --stat HEAD`) saved us once and slipped once. The slip cost no real time — the AnomalyDetector commit just happened a session later than intended — but it's evidence that the rule only works if it actually runs.
- **The "paste fixture before using it" rule** caught several misalignments early. We slipped on it a few times (`StorePayload.event(...)` and `event.metadata.source` defaults) and each slip cost one round-trip. The rule is right; the discipline of applying it is worth holding to.
- **Ruff `I001` (import organisation) fires consistently on new files** because PEP 8 wants blank lines between import groups and I kept omitting them. Pattern noted and the AnomalyDetector module finally landed without the warning. Worth keeping in muscle memory for Phase 3.

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

Expected: branch `main`, working tree clean, top commit is the Phase 2 merge, 144/144 tests, tags include `phase-1-complete` and `phase-2-complete`.

Then start Phase 3: create `docs/phases/phase-3-plan.md` and work through the design questions from `docs/roadmap.md`'s Phase 3 section before any implementation begins. The roadmap flags `MeanAggregation`, the `DeviceRegistry` component, and feature emissions to a sink as the major pieces.
