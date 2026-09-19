# Status snapshot — 2026-06-18 — Phase 7 complete

> Snapshot captured before the merge of PR for `feat/phase-7-replay-orchestration`. The branch is seven commits past `phase-6-complete` and ready to merge. Once merged and tagged `phase-7-complete` — and then `v1.0.0` — this snapshot describes the on-`main` state. This is the final phase; with it, the definition of done is satisfied.

## Where we are

`main` will sit at the merge commit for Phase 7's PR. Seven commits past `phase-6-complete`:

```
docs(status): add 2026-06-18 phase-7-complete snapshot
chore(ci): bump test-count baseline to 384 for Phase 7 close
docs(replay): update replay-workflows to the as-built driver
test(replay): live-vs-replay determinism across the full control plane
feat(replay): add CLI shell parsing JSON replay config
feat(replay): add run_replay driver over injected source and builder
docs(project): add Phase 7 plan and D-20 working note
```

384 tests passing. ruff and `mypy --strict` clean across 42 source files. CI is expected green at merge (ruff, mypy, pytest matrix on Python 3.11 and 3.12, test-count regression guard, GitGuardian security scan).

## What landed in Phase 7

**`run_replay`** (`stream_pipeline/replay/driver.py`):

The replay driver — function-shaped over an injected event source and an injected pipeline-builder (D-20), the same form the pipeline itself took (D-4) one level up. It builds a pipeline from `settings.for_replay()` via the builder, feeds the event source through `process_batch` in arrival order, emits one `replay.completed` lineage log line, and returns one `ProcessingResult` per event. Determinism is entirely inherited from the streaming layer's contracts; the driver adds orchestration and observability, not logic.

The load-bearing decision is that the driver takes a `build_pipeline` callable, not a pre-built pipeline: `for_replay()` must thread into sink construction, which happens during registration, so the same builder constructs both the live and replay pipelines and only the settings differ. The driver owns the `for_replay()` swap — the caller passes live settings, so a replay cannot accidentally write to live sinks.

**The CLI shell** (`stream_pipeline/replay/__main__.py`):

`python -m stream_pipeline.replay <config.json>` — a thin parse-and-delegate entry point. It reads a JSON replay config (the shape a Step Functions invocation passes), constructs settings and an event source from it, and delegates to `run_replay`. The config's `settings` block is the same `SF_*` vocabulary a live deployment reads from its environment, passed straight to `PlatformSettings.from_env(env=...)` — reusing the one validated construction path rather than introducing a second. The config carries live names alongside their replay counterparts and describes the environment; the driver performs the isolation. `build_event_source` and `build_pipeline` are injected with production defaults, so the parse-and-delegate surface is testable with fakes without touching AWS.

**The determinism integration test** (`tests/replay/test_replay_determinism.py`):

The headline deliverable the definition of done calls out. One production-shaped builder wires the dataset writer (on the pipeline) and projections (routed beside it via `route_detections`), invoked once with live settings and once through `run_replay`. It asserts byte-identical dataset Parquet *and* content-identical projection-table rows across the live and replay sinks, with the live sinks untouched by a replay-only run. This elevates the determinism proof from "the dataset writer is deterministic" (Phase 4) to "the whole control plane reproduces".

The two sink types reach isolation by two different mechanisms, and the asymmetry is the point: the dataset writer is registered on the pipeline, so `run_replay` calling `build_pipeline(settings.for_replay())` isolates it *inside* the driver; projections are routed by the caller after the driver returns, so the caller building them from `for_replay()` settings isolates them *outside* the driver. Both reproduce; proving both in one driver-run is the whole-control-plane determinism property.

## Design decisions

Recorded as working note D-20: the replay driver is function-shaped over an injected source and an injected pipeline-builder. The note captures four threads — the function-shaped form, the builder-not-pre-built-pipeline decision (so `for_replay()` threads into sink construction), the "same builder, different settings" property that makes replay verifiable, and isolation as the absence of shared state (the driver builds its own pipeline, so no flag or gating is needed).

## What's deliberately deferred

The definition of done places deployment and production-traffic validation out of scope. Three seams are therefore typed-and-stubbed or deferred rather than built:

- **The EventBridge-archive reader.** The production `build_event_source` that iterates the archive over `[start_time, end_time)` filtered by `event_pattern`. The typed seam exists; its AWS body raises `NotImplementedError` with a pointer. Replay runs inject an event source in its place.

- **The production pipeline builder.** The wiring that registers the dataset writer and projections for a live deployment. The determinism test defines the builder that proves the control plane reproduces; a deployed CLI would reference that wiring.

- **The `replay_audit_table` record.** The root-trace-tagged audit row provisioned upstream — infra-adjacent, proves nothing about determinism. Deferred with rationale, a small later addition if wanted.

These are honest deferrals: the determinism and orchestration value — the actual Phase 7 brief — lives in `run_replay` over an injected source, and that is built and proven. The deferred pieces are AWS plumbing the brief explicitly excludes.

## Open issues

- **`structured-logging-python` stdlib-logging warnings** continue to surface in tests, now reproduced from the replay path (the driver's `replay.completed` line and the determinism test both emit through the real backend). Pre-existing since Phase 1; deterministic; cosmetic output pollution, not a failure. Still queued for a focused upstream cleanup.

- **CI runner Node 20 deprecation.** `actions/checkout@v4` and `actions/setup-python@v5` run on Node 20, removed from runners 2026-09-16. CI is green and unaffected for now, but the deadline is the one piece of housekeeping that outlives the project's phases — the fix (bump the action versions) is a small CI PR that should land before September. Worth tracking as the first post-`v1.0.0` maintenance item.

- **`event-schema-contracts` ruff backlog.** Pre-existing upstream; not in our code. Phase 7 added no upstream PRs (it consumes existing contracts at the orchestration layer, not at depth), so the backlog is unchanged. The genuine absence of an upstream PR this phase is the expected signal for an orchestration-over-finished-components phase, not a sign of shoehorning (D-11).

## Estimate vs actual

Phase 7 was estimated at 12-16 hours in the roadmap. Actual came in at the lower end — the phase was orchestration over seams every prior phase had already cut. Phase 4 made the dataset Parquet byte-reproducible, Phase 5 gated the alert sink on environment, Phase 6 swapped the projection table via `for_replay()`; the driver composes these rather than building new isolation machinery. The determinism test, the phase's centre of gravity, was largely the two existing single-sink isolation tests combined into one whole-control-plane proof. The roadmap's framing that Phase 7 would be small precisely because the determinism discipline was paid down earlier proved correct.

## Process observations worth carrying forward

- **The sandbox-vs-repo ruff gap is real and worth pre-empting.** PTH123 (`open()` → `Path.open()`) was the repeat offender — the repo enforces `flake8-use-pathlib`, which a bare ruff run elsewhere does not flag. The discipline that emerged: write file I/O with `pathlib` from the start, and treat the repo's `ruff check --fix` as the authority rather than any external pass. The lesson generalises — the repo's lint config is stricter than a default ruff, so match the repo, not the tool's defaults.

- **The richer log line has an ongoing test tax.** Keeping `detections`/`emissions` counts in `replay.completed` means every fake pipeline in every test must return results carrying those lists. The driver tests and the CLI tests both needed `_FakeResult` doubles with the lists; a bare `object()` result raised `AttributeError` on the count-sum. The counts earn their place (the replay-level "what did this produce" record), but the tax is real and worth knowing when writing future fakes against the driver.

- **Merge convention restored.** Phase 7 merges with `gh pr merge --merge`, preserving the seven-commit narrative — the Option A convention prior phases used. Phase 6 diverged to a squash; Phase 7 corrects back. The fine-grained-commit history is the project's record of how each decision was reached, and the convention is to preserve it.

## How to resume

This is the final phase. There is no Phase 8. The resume protocol is the definition-of-done checklist:

1. **All seven phases merged to `main` via reviewed PRs, each tagged `phase-N-complete`.** After this merge: tags `phase-1-complete` through `phase-7-complete` all present.
2. **CI green at the tip of `main`.** ruff, `mypy --strict`, pytest matrix on 3.11 and 3.12, regression guards passing.
3. **Test coverage exercises the brief's requirements**, traceable to per-phase acceptance criteria.
4. **Documentation complete.** All brief-mandated documents exist; `replay-workflows.md` is now as-built rather than a stub.
5. **Replay determinism verified.** `tests/replay/test_replay_determinism.py` runs an identical event sequence through the live pipeline and the replay driver and asserts byte-identical outputs across the full control plane. This was the last outstanding definition-of-done item; Phase 7 satisfies it.
6. **Operational ergonomics** — clone, `pip install -e ".[dev]"`, test suite passes within ten minutes; CI replicates this.

With all six satisfied, tag `v1.0.0`:

```bash
git checkout main && git pull
git tag v1.0.0
git push origin v1.0.0
```

The standard environment-verification sequence still applies before any post-`v1.0.0` maintenance:

```bash
cd ~/Code/stream-pipeline
source .venv/bin/activate
git status
git --no-pager log --oneline | head -3
python3 -m unittest discover -s tests 2>&1 | tail -3
git tag --list
```

Expected: branch `main`, working tree clean, top commit the Phase 7 merge, 384/384 tests, tags including `phase-1-complete` through `phase-7-complete` and `v1.0.0`.

The first post-`v1.0.0` maintenance item is the CI Node 20 action bump, due before 2026-09-16.
