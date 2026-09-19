# Status snapshot — 2026-05-08 — Phase 2 mid-flight

> Snapshot of project state at the point where the working notes, phase plans, status, roadmap, and resumption guide were first committed. Captured because the chat session producing Phase 2 is approaching its context limit and the project is being moved into a Claude Project for continued work.

## Where we are

Six commits ahead of `main` on `feat/phase-2-detection-engines`, none yet pushed. The branch sits at `fe7034f`:

```
fe7034f  feat(streaming): extend WindowEmission with last_contributing_trace_id
85f2bc7  feat(detection): add EventDetector and EmissionDetector protocols
8fb684c  feat(detection): add detection-type constants and package layout
2b15422  chore(deps): bump event-schema-contracts to 8643cef
4fdc56b  Merge pull request #1 from nic-thompson/feat/phase-1-streaming-skeleton  ← main
```

109/109 tests pass locally. ruff clean. `mypy --strict` clean across 21 source files. Working tree shows two untracked items (`tests/_fixtures/detectors.py`, the start of B.1) and uncommitted modifications to `stream_pipeline/streaming/realtime_pipeline.py` (B.2 — the pipeline integration that's been verified but not yet committed).

This in-flight work belongs to commit 5 of Phase 2's plan and is documented as sub-steps B.1 and B.2 in `docs/phases/phase-2-plan.md`.

## What's verified locally but not yet committed

- `tests/_fixtures/detectors.py` (~157 lines) — `FakeEventDetector`, `FakeEmissionDetector`, `RaisingEventDetector`, `RaisingEmissionDetector`. Smoke test confirms all four satisfy the protocols structurally; full suite at 109 still passes.
- Modifications to `stream_pipeline/streaming/realtime_pipeline.py`:
  - imports of `DetectionEvent`, `EventDetector`, `EmissionDetector`
  - new `_LOG_EVENT_DETECTOR_ERROR` constant
  - `ProcessingResult.detections: list[DetectionEvent]` field
  - `_event_detectors: list[EventDetector]` and `_emission_detectors: dict[str, list[EmissionDetector]]` storage
  - `register_event_detector()` and `register_emission_detector()` methods
  - `aggregator.observe(trace_id=trace_id)` call-site update
  - detector dispatch loops in `process()` (event detectors, then emission detectors per emission)
  - log-line metadata updates (`event_type=_LOG_EVENT_DETECTOR_ERROR`, `detections` count in `pipeline.processed`, `total_detections` in `pipeline.batch_summary`)
  - return path updates (`detections=[]` on extraction failure, `detections=all_detections` on success)
- Verified: ruff clean (after `--fix` removed two unused `# noqa: BLE001` directives my code shouldn't have included), mypy --strict clean across 21 source files, all 109 existing tests still pass.

The existing tests passing without modification is a real signal: the new detector dispatch is purely additive code, none of the existing tests construct `ProcessingResult` directly so the new required field doesn't break them. New tests covering the new behaviour are sub-step B.3.

## What's pending

- **B.3** — six or seven new tests in `tests/streaming/test_realtime_pipeline.py` covering: detector registration mechanics, event-detector dispatch order, emission-detector routing by `aggregation_name`, per-detector failure isolation, trace propagation end-to-end, log-line metadata. Once these pass, B.1+B.2+B.3 commit together as a single logical "wire detector dispatch into RealtimePipeline" change.
- **Commits 6, 7, 8** — the three detector implementations.
- **Commits 9, 10** — docs and CI baseline bump.
- **Push, PR, CI, merge.**

## Open issue

`structured-logging-python` emits stdlib-logging warnings during `error()` calls in `tests/streaming/test_observability.py`. Four `--- Logging error ---` lines per test run. Pre-existing since Phase 1. Reproducer is a 4-line `python3 -c` snippet. Decision pending on whether to fix upstream now (small PR plus a third SHA bump cycle) or defer to a later cleanup. Currently leaning "fix now" because the bug is contained and shipping clean test output is hygiene worth paying for; not yet decided.

## How to resume

Read `RESUMING.md`, `docs/working-notes.md`, `docs/phases/phase-2-plan.md`, then this file. Then:

```bash
cd ~/Code/stream-pipeline
source .venv/bin/activate
git status
git --no-pager log --oneline | head -5
python3 -m unittest discover -s tests 2>&1 | tail -3
```

Expected: branch `feat/phase-2-detection-engines`, working tree shows the in-flight modifications above, top commit `fe7034f`, 109/109 tests.

If working tree is clean (changes lost): replay sub-steps B.1 and B.2 from the message history. The work is small enough to redo from scratch in 30 minutes if necessary.
