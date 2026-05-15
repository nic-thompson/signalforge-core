# Resuming SignalForge

> Read this first. You're picking up an in-progress production engineering project.

## What this is

SignalForge is a telemetry intelligence platform for a 50,000-device retail headset fleet. This repository (`signal-forge`) is the analytics control plane. Four upstream packages contribute schemas, logging, parsing, and infrastructure references.

## Read these before doing anything else

In order:

1. **This file.**
2. **`docs/working-notes.md`** — the long-lived index of engineering principles, design decisions, working agreements, and known issues. Read all of it; future re-reads will be quick once you know the structure.
3. **The latest file in `docs/status/`** — alphabetically last, by date. The most recent point-in-time snapshot of where the project is.
4. **`docs/phases/phase-N-plan.md`** for the active phase, where N is the highest-numbered plan file.
5. **`docs/roadmap.md`** for the full long-range plan and the definition of done.

That should rebuild context in 20–30 minutes of reading.

## Then verify your environment

```bash
cd ~/Code/signal-forge       # or wherever you've cloned it
source .venv/bin/activate    # if you don't have a venv: python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]"

git status
git --no-pager log --oneline | head -10
python3 -m unittest discover -s tests 2>&1 | tail -3
ruff check .
mypy signal_forge 2>&1 | tail -3
```

You should see:

- Working tree clean (or with whatever in-flight modifications the latest status snapshot describes).
- Recent commits in the order documented in the status snapshot.
- All tests passing.
- ruff clean.
- mypy clean.

If any of those is wrong, **stop**. Compare against the status snapshot. Don't make changes until you understand why your local state differs from the documented state.

## Working agreements

Documented in detail in `docs/working-notes.md`. The headline rules:

- **Option A workflow.** Fine-grained commits, careful design conversations, full hygiene at every step. We are not optimising for speed; we are optimising for production quality.
- **Verification checkpoints before every commit:** `git status --short`, `ruff check .`, `mypy signal_forge`, `python3 -m unittest discover -s tests`. After every commit: `git --no-pager show --stat HEAD`.
- **No merge before `gh pr checks --watch` shows explicit green.**
- **`grep -En "^(class|def |if __name__)"`** is the canonical structural-grep form.
- **VS Code workspace settings** (`.vscode/settings.json`) are checked in. They configure `files.insertFinalNewline` and `files.trimTrailingWhitespace`, eliminating the most common ruff lint slips.

## When working with an AI assistant on this project

A few specific patterns that have proven valuable:

- **Paste type definitions before asking the AI to write code against them.** Memory-based code shipping has been the source of multiple bugs in this project. Read-then-write beats remember-then-write.
- **Run all four verification commands before any commit.** AI-generated code is occasionally lint-clean but introduces subtle issues; the four checks catch most of them.
- **Ask the AI to explain trade-offs.** This is a learning project as well as a delivery project. The AI should be making design choices visibly, not silently.
- **Watch for confident citations.** The AI was caught fabricating DDIA chapter numbers early in Phase 2. If a specific chapter, page, or section title is cited without a recent verification step, ask explicitly whether it's been verified.
- **The AI doesn't have memory across separate conversations.** It re-reads the project documentation at the start of each new conversation. The thoroughness of `docs/working-notes.md`, `docs/roadmap.md`, and the latest `docs/status/` file is what makes resumption work.

## Repository layout

```
signal-forge/
├── .github/workflows/        # CI: ruff, mypy --strict, pytest matrix, test-count regression guard
├── .vscode/                  # Workspace settings for lint hygiene
├── docs/
│   ├── architecture.md       # Streaming-layer architectural map
│   ├── streaming-internals.md  # Watermark/window arithmetic, incident playbook
│   ├── replay-workflows.md   # Phase 7 contract stub
│   ├── working-notes.md      # Engineering principles and decision log
│   ├── roadmap.md            # Long-range plan, definition of done
│   ├── phases/
│   │   └── phase-N-plan.md   # Per-phase plans
│   └── status/
│       └── YYYY-MM-DD-*.md   # Point-in-time snapshots
├── signal_forge/             # Production code
│   ├── streaming/            # Phase 1 streaming primitives
│   ├── detection/            # Phase 2 detection engines (in progress)
│   └── config/               # PlatformSettings
├── tests/
│   ├── _fixtures/            # Reusable test fakes
│   ├── ci/                   # Tests for CI scripts
│   ├── streaming/            # Phase 1 tests
│   └── detection/            # Phase 2 tests
├── pyproject.toml            # Pinned dependencies, ruff config, mypy config
└── RESUMING.md               # This file
```

## Upstream packages

Four GitHub repositories contribute via SHA-pinned dependencies in `pyproject.toml`:

- `nic-thompson/event-schema-contracts` — pydantic envelopes, schema registry, semver. Currently pinned at `8643cef` (v0.2.0 with detection.event v1).
- `nic-thompson/structured-logging-python` — trace-aware structured logging. **Has a known bug** producing stdlib-logging warnings during `error()` calls; documented in `docs/working-notes.md` under "Known issues".
- `nic-thompson/telemetry-parser` — raw-to-validated event extraction.
- `nic-thompson/aws-event-pipeline-infra` — Terraform; not a Python dependency, but referenced in `docs/replay-workflows.md`.

When you bump an upstream SHA, the bump goes in its own commit (`chore(deps): bump <package> to <sha>`) before any logic that depends on the new SHA. This makes the dependency change bisectable.
