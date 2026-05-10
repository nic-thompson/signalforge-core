# Phase 2 — Detection engines

> **Status.** In progress. Six commits landed on `feat/phase-2-detection-engines`, branch not yet pushed. See the most recent file in `docs/status/` for current state.

## What Phase 2 delivers

Three detection engines built on Phase 1's streaming foundation:

- **`OfflineDetector`** — emits when a device transitions from seen to silent past `OFFLINE_THRESHOLD_SECONDS`.
- **`OutageDetector`** — emits when more than `OUTAGE_THRESHOLD_RATIO` of a store's devices are simultaneously offline.
- **`AnomalyDetector`** — emits when a windowed signal-quality field crosses a configured threshold.

Detection events flow back through `RealtimePipeline.process()` as a typed return value (`ProcessingResult.detections: list[DetectionEvent]`), not as side effects. Phase 5 alert routing reads detections; Phase 4 dataset projectors consume them; replay reproduces them deterministically.

## Why detection lives in `signal-forge`, not in upstream packages

A reasonable alternative would be to put detection logic in `event-schema-contracts` or in a separate `signalforge-detectors` repo. We don't, for two reasons:

1. **Detection is analytics-platform-specific.** The contracts are platform-wide (every consumer needs the same `DetectionEvent` schema), but the detection *logic* is the analytics control plane's responsibility. Mixing platform contracts with consumer logic upstream blurs the boundary.

2. **`signal-forge` is where the streaming primitives live.** Detectors subscribe to events and emissions through the `RealtimePipeline`, which lives here. Splitting the detector implementations across repositories would mean either reproducing the pipeline contracts upstream or taking a circular dependency. Neither is good.

The schemas live upstream. The implementations live here. That's the right boundary.

## Design contract

The seven decisions locked at the start of Phase 2 (with reasoning in `docs/working-notes.md` under D-5 through D-9):

1. **Detections flow back as a return value, not as side effects.** Replay-deterministic. Phase 5 alert routing is a thin consumer of `result.detections`.
2. **Watermark observers deferred to Phase 2.5.** Offline detection uses scan-on-event for now. Revisit if measurement shows it's the bottleneck.
3. **`DetectionEvent` lives upstream** in `event-schema-contracts`, with the discriminator pattern (single schema, `detection_type: str` + `details: dict[str, Any]`).
4. **`OfflineDetector` emits once per offline transition event.** State machine `unseen → seen → offline`; transition to offline emits, transition back to seen resets state.
5. **`OutageDetector` takes a constructor callable** `Callable[[str], int]` for registered device counts. Real `DeviceRegistry` deferred to Phase 3 or 4.
6. **Detection-type constants** as `Final[str]` in `signal_forge/detection/types.py`.
7. **Two protocols** — `EventDetector` and `EmissionDetector` — not one. Routing by `aggregation_name`.

Plus the design choices made during implementation:

8. **Aggregate → router → detect ordering** in `RealtimePipeline.process()`. Detection observes the fully-processed event after every other subscriber.
9. **Per-detector failure isolation** matching Phase 1's pattern — a raising detector is logged and skipped, others continue.
10. **Trace propagation through `WindowEmission.last_contributing_trace_id`** — Phase 1 dataclass extension to support emission-detector lineage.

## Plan

The phase is structured as a sequence of small, individually-bisectable commits. Each commit either ships a new piece of the contract or adds a detector that builds on previously-shipped pieces.

### Stream A — upstream `event-schema-contracts` ✅ Merged

| # | Commit | Status |
|---|---|---|
| 1 | `feat(detection): add detection.event v1 schema` | Merged in upstream PR #1 |
| 2 | `chore(ci): add GitHub Actions workflow with mypy and pytest` | Merged in upstream PR #1 |

Upstream now at v0.2.0, SHA `8643cef`. CI operational on the upstream for the first time.

### Stream B — `signal-forge` Phase 2

| # | Commit | Status |
|---|---|---|
| 1 | `chore(deps): bump event-schema-contracts to 8643cef` | ✅ Done |
| 2 | `feat(detection): add detection-type constants and package layout` | ✅ Done |
| 3 | `feat(detection): add EventDetector and EmissionDetector protocols` | ✅ Done |
| 4 | `feat(streaming): extend WindowEmission with last_contributing_trace_id` | ✅ Done — Phase 1 dataclass extension |
| 5 | `feat(detection): wire detector dispatch into RealtimePipeline` | 🟡 In progress — sub-steps B.1 and B.2 done locally, B.3 pending |
| 6 | `feat(detection): OfflineDetector` | ⏳ Pending |
| 7 | `feat(detection): OutageDetector` | ⏳ Pending |
| 8 | `feat(detection): AnomalyDetector` | ⏳ Pending |
| 9 | `docs(detection): add docs/detection-models.md` | ⏳ Pending |
| 10 | `chore(ci): bump test-count baseline` | ⏳ Pending |

After commit 10: push, open PR, watch CI, merge.

### Sub-steps within commit 5 (the pipeline integration)

The commit is large enough to verify in pieces:

- **B.1** — `tests/_fixtures/detectors.py` (reusable test doubles). Done locally.
- **B.2** — `signal_forge/streaming/realtime_pipeline.py` modifications: `ProcessingResult.detections`, `register_event_detector()`, `register_emission_detector()`, dispatch loops, log-line metadata. Done locally.
- **B.3** — extend `tests/streaming/test_realtime_pipeline.py` with new tests covering registration, dispatch, failure isolation, trace propagation, log metadata. Pending.

All three land in one commit when complete.

## What's deliberately not in Phase 2

- **Watermark observers.** Deferred to Phase 2.5. Offline detection uses scan-on-event meanwhile.
- **`DeviceRegistry` component.** `OutageDetector` takes a constructor callable; real registry component arrives in Phase 3 or 4.
- **Alert routing.** Phase 5. Detections accumulate in `ProcessingResult`; nobody routes them anywhere yet.
- **Detection persistence.** Phase 4 will project detections into the dataset layer.
- **Statistical anomaly models.** `AnomalyDetector` in Phase 2 is threshold-driven. Statistical models (rolling z-score, EWMA, isolation-forest) are a Phase 6 or later concern if needed at all.

## Acceptance criteria

Phase 2 is done when:

1. All 10 Stream B commits land on `main` via merged PR.
2. CI green on the merged PR (ruff, mypy --strict, pytest matrix).
3. Test count regression guard reflects the new baseline.
4. All three detectors have unit tests covering: typical case, threshold-just-not-crossed, threshold-just-crossed, edge cases (e.g. device that flaps, store with one device, signal exactly equal to threshold).
5. `RealtimePipeline` integration tests cover detector registration, dispatch order, failure isolation, trace propagation, and log-line metadata.
6. `docs/detection-models.md` exists and explains each detector's contract, threshold semantics, false-positive considerations, and the offline-transition state machine.
7. Branch `feat/phase-2-detection-engines` deleted; tag `phase-2-complete` pushed.

## Cross-references

- DDIA Chapter 12 ("Stream Processing"), specifically "Reasoning About Time" — the watermark and event-time material that underpins our `WindowEmission` and the trace-propagation extension.
- DDIA Chapter 5 ("Encoding and Evolution") — the discriminator-pattern decision (D-8) is a forward-compatibility choice for the detection-event schema.
- `docs/architecture.md` for the streaming-layer architecture this builds on.
- `docs/working-notes.md` for the engineering principles, decision log, and known issues.
