# Resuming stream-pipeline

> Read this first. You're picking up an in-progress production engineering project.

## What this is

SignalForge is a telemetry intelligence platform for a large-scale retail
headset fleet. **`stream-pipeline`** — this package, at
`packages/stream-pipeline/` — is its analytics control plane: detection,
alerting, dashboard projections, dataset export, and replay.

It is one of four packages inside **`signal-forge`**, a `uv` workspace
monorepo (not a standalone repository). The repository itself is named
`signal-forge`; the package inside it is `stream-pipeline`, deliberately
distinct, so a reader can always tell whether something refers to the
whole workspace or to this one analytics package. The other three
packages — `event-schema-contracts`, `telemetry-parser`,
`structured-logging-python` — resolve as local workspace dependencies,
not pinned SHAs or tags. There is no cross-package pin to go stale inside
the workspace; a change to any of the four is visible to the others in
the same commit.

## Read these before doing anything else

In order:

1. **This file.**
2. **`docs/working-notes.md`** — the long-lived index of engineering principles, design decisions, working agreements, and known issues. Read all of it; future re-reads will be quick once you know the structure.
3. **The latest file in `docs/status/`** — alphabetically last, by date. The most recent point-in-time snapshot of where the project is.
4. **`docs/roadmap.md`** for the full long-range plan — "Beyond v1.0.0" covers the current phase and everything planned after it.
5. **`docs/phases/phase-N-plan.md`** for the active phase, where N is the highest-numbered plan file.

That should rebuild context in 20–30 minutes of reading.

## Then verify your environment

This package lives inside the `signal-forge` workspace root, not in
its own standalone virtualenv — `uv sync` at the workspace root installs
all four packages together.

```bash
cd ~/Code/signal-forge    # the monorepo root, not this package's own directory
uv sync --group dev

git status
git --no-pager log --oneline | head -10
uv run pytest packages/stream-pipeline/tests -q
uv run mypy packages/stream-pipeline/stream_pipeline
```

Run this package's tests on their own, not as part of a combined
`packages/*/tests` glob — collecting more than one package's `tests/`
directory in one pytest invocation raises `ModuleNotFoundError: No
module named 'tests.alerts'` (or similar), because every package's
tests become the same top-level `tests` package. mypy has no equivalent
issue and can check all four packages together; see the workspace
root's `.github/workflows/ci.yml` for how CI does both.

You should see:

- Working tree clean (or with whatever in-flight modifications the latest status snapshot describes).
- Recent commits in the order documented in the status snapshot.
- All tests passing.
- mypy clean.

If any of those is wrong, **stop**. Compare against the status snapshot. Don't make changes until you understand why your local state differs from the documented state.

## Working agreements

Documented in detail in `docs/working-notes.md`. The headline rules:

- **Option A workflow.** Fine-grained commits, careful design conversations, full hygiene at every step. We are not optimising for speed; we are optimising for production quality.
- **Verification checkpoints before every commit:** `git status --short`, `uv run pytest packages/stream-pipeline/tests -q`, `uv run mypy packages/stream-pipeline/stream_pipeline`. After every commit: `git --no-pager show --stat HEAD`.
- **No merge before `gh pr checks` shows explicit green** — checked as its own step, never chained into the same command as the merge itself. A run can still be in progress when checked too early; re-check rather than assume.
- **`grep -En "^(class|def |if __name__)"`** is the canonical structural-grep form.

## When working with an AI assistant on this project

A few specific patterns that have proven valuable:

- **Paste type definitions and real command output before asking the AI to write code or draw conclusions.** Memory-based code shipping, and trusting a summary over the actual pasted output, have both been sources of real bugs and real wasted time in this project. Read-then-write, and verify-then-conclude, both beat their assumed alternatives.
- **Run the verification commands before any commit**, and **in the environment the change actually needs to run in** — this project has repeatedly found real discrepancies between one virtualenv's result and another's (a workspace mixed with another repo's venv, a repo's own isolated venv, a config file mypy only reads from one location). `which python3` and confirming the working directory are cheap; assuming an environment is the right one has not been.
- **Ask the AI to explain trade-offs.** This is a learning project as well as a delivery project. The AI should be making design choices visibly, not silently.
- **Watch for confident claims stated without having actually checked.** This project has caught fabricated citations, an incorrectly-diagnosed mypy failure later found to be an environment mismatch, and "all tests pass" claimed before the real command had actually been run. If a claim of "verified" or "confirmed" isn't accompanied by the actual command output, ask to see it.
- **The AI doesn't have memory across separate conversations.** It re-reads the project documentation at the start of each new conversation. The thoroughness of `docs/working-notes.md`, `docs/roadmap.md`, and the latest `docs/status/` file is what makes resumption work.

## Repository layout

This package's own layout, inside the larger `signal-forge` workspace:

```
signal-forge/                     # the monorepo root
├── .github/workflows/ci.yml          # CI for all four packages: pytest matrix per package, unified mypy
├── pyproject.toml                    # workspace definition, [tool.uv.sources], shared dev dependency group
├── packages/
│   ├── event-schema-contracts/
│   ├── telemetry-parser/
│   ├── structured-logging-python/
│   └── stream-pipeline/              # this package
│       ├── docs/
│       │   ├── architecture.md       # Streaming-layer architectural map
│       │   ├── streaming-internals.md  # Watermark/window arithmetic, incident playbook
│       │   ├── replay-workflows.md
│       │   ├── working-notes.md      # Engineering principles and decision log
│       │   ├── roadmap.md            # Long-range plan, definition of done
│       │   ├── phases/
│       │   │   └── phase-N-plan.md   # Per-phase plans
│       │   └── status/
│       │       └── YYYY-MM-DD-*.md   # Point-in-time snapshots
│       ├── stream_pipeline/          # Production code (import name: stream_pipeline)
│       │   ├── streaming/            # Watermarks, windows, the realtime pipeline
│       │   ├── detection/            # Detectors, device registry
│       │   ├── alerts/               # Alert routing and sinks
│       │   ├── dashboards/           # DynamoDB-backed projections
│       │   ├── datasets/             # S3/Parquet dataset export
│       │   ├── replay/               # Replay CLI and driver
│       │   └── config/               # PlatformSettings
│       ├── tests/
│       ├── pyproject.toml            # This package's own name, version, extras
│       └── RESUMING.md               # This file
```

## Upstream packages

Three of the four other packages this one depends on resolve as local
workspace members via `[tool.uv.sources]` in the workspace root's
`pyproject.toml` — `event-schema-contracts`, `telemetry-parser`,
`structured-logging-python`. There is no SHA or tag to bump for these;
a change to any of them is visible here the moment it lands on `main`,
in the same `uv sync`.

`aws-event-pipeline-infra` is not part of the workspace — it is a
separate, private repository (real AWS account details, not suitable
for a public monorepo) that consumes this workspace's packages by
pinning a `signal-forge` git tag, the same way any external
consumer would. It is referenced in `docs/replay-workflows.md` but is
not a Python dependency of this package.

When `signal-forge` needs a new tag to reflect a change here (for
`aws-event-pipeline-infra` or any other external consumer to pick up),
that is a deliberate, separate step — not something that happens
automatically on every commit to this package.
