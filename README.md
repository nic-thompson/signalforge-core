# signal-forge

The four co-released cloud packages of the SignalForge telemetry platform,
as a single [uv](https://docs.astral.sh/uv/) workspace.

| Package | What it does |
|---|---|
| [`event-schema-contracts`](packages/event-schema-contracts/) | Canonical telemetry event schema contracts |
| [`telemetry-parser`](packages/telemetry-parser/) | Parses raw TCP packet streams into validated events |
| [`structured-logging-python`](packages/structured-logging-python/) | Structured JSON logging and trace propagation |
| [`stream-pipeline`](packages/stream-pipeline/) | Analytics, detection, replay and dataset control plane |

Each package keeps its own `pyproject.toml`, version, and test suite —
see its own README for details. What the workspace adds is that they
resolve against each other's *working tree*, not a pinned git tag.

## Why one workspace

These four packages were four separate repositories until 2026-09-18.
Each one pinned the others by git tag (`event-schema-contracts @
git+...@v0.8.2`), and those pins repeatedly went stale relative to one
another — a change merged in one repo would not reach a sibling until
someone remembered to bump a pin and cut a new tag, discovered only
when a later, unrelated change failed to install. `stream_pipeline`'s
`_production_build_pipeline` depending on a specific
`event-schema-contracts` export is a representative case: the export
was added, tagged, and still took three further pin bumps across two
more repos before everything agreed.

Inside this workspace, `stream-pipeline` depending on
`event-schema-contracts` resolves to `packages/event-schema-contracts`
directly. A change to one is visible to the others in the same commit,
the same pull request, the same CI run. There is no tag to go stale,
because there is nothing to pin.

## What stayed separate, and why

- **`aws-event-pipeline-infra`** (private) — names a real, live AWS
  account, its region, and its resource topology. It consumes this
  workspace by pinning one tag here instead of three separate
  package tags.
- **`signalforge-integration-tests`** — its one real test exercises the
  seam between `telemetry-parser` (here) and `greengrass-publisher`
  (edge-side, its own repo), which inherently spans a boundary this
  workspace does not cross.
- **`greengrass-publisher`** — runs on-premises, on the Controller
  hardware, not in the cloud. A different deployment target with no
  coupling to the schema/parser/pipeline chain here.

## Development

```bash
uv sync --group dev
uv run pytest packages/event-schema-contracts/tests -q
uv run mypy packages/stream-pipeline/stream_pipeline
```

Run each package's tests separately, not as one combined
`packages/*/tests` glob — collecting more than one package's `tests/`
directory in a single pytest invocation raises
`ModuleNotFoundError: No module named 'tests.alerts'` (or similar),
because every package's tests become the same top-level `tests`
package. `mypy` has no equivalent issue and can run across all four at
once; see `.github/workflows/ci.yml`.

## History

Each package's git history was preserved through the merge — file-level
`git blame` and `git log -- packages/<name>/<file>` still show the
original commits, authors, and dates from before 2026-09-18. Each
repository's own tags (`v0.8.2`, `v1.0.3`, etc.) remain valid on the
now-archived original repositories; this workspace's own tags going
forward are prefixed per package (`event-schema-contracts-v0.8.3`, and
so on), since there is no longer one shared version number that means
anything.
