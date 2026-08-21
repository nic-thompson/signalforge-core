# signalforge-integration-tests

Cross-repo integration tests for SignalForge — the seams
between components, which no single component repo can test on its own.

## Why a separate repo

These tests span repos with no dependency edges between them
(`telemetry-parser`, `structured-logging-python`, `greengrass-publisher`,
and later `aws-event-pipeline-infra` and `signal-forge`). No existing
repo is a natural home; putting them in one would mean it arbitrarily
owns tests about code it doesn't contain, and would drag cross-repo
concerns — sibling checkouts now, AWS credentials later — into a
focused component's CI.

The known cost is staleness: a change to `telemetry-parser` won't run
these tests as part of its own CI. Both mitigations for that are built
in — see `docs/wiring-component-repos.md`.

## What's covered

**Layer 2 — edge chain** (`tests/test_edge_chain_integration.py`)

```
raw SIP bytes -> TCPPacket stream -> ParserPipeline -> GreengrassEventPublisher
```

No AWS account or network interface needed; the publisher runs against
a fake Greengrass IPC client. Covers TCP reassembly across fragmented
packets, multi-message streams, non-REGISTER methods being skipped
without aborting the stream, trace propagation, publish-failure
surfacing, and end-of-stream flushing.

**Layer 3 — cloud chain**: not yet built. Will need AWS credentials and
the `iot_core_telemetry_rule` module applied to `dev`.

## Running locally

```bash
pip install -e ".[dev]"

PYTHONPATH="../telemetry-parser:../structured-logging-python:../greengrass-publisher:." \
  pytest -v
```

Expect **12 passed, 3 xfailed**.

## The three xfails

Not flakiness — three real replay-determinism defects in
`telemetry-parser`, found by this suite on its first run. See
`DEFECTS.md`.

They assert the behaviour SignalForge *claims* rather than the broken
behaviour it currently has, and are `strict=True`: fixing the defects
turns this suite red on unexpected-pass, which is the signal to remove
the markers. Fixing the defect silently isn't possible.

## Staleness mitigations

| Trigger | When |
|---|---|
| `push` / `pull_request` | Changes to the tests themselves |
| `repository_dispatch` | Minutes after a component repo merges to main |
| `schedule` | Nightly, 06:00 UTC — backstop |
| `workflow_dispatch` | Manual, with optional per-component ref overrides |

Setup for the dispatch path (it needs a PAT — the default
`GITHUB_TOKEN` can't trigger cross-repo workflows) is in
`docs/wiring-component-repos.md`.

## Layout

```
fixtures/
  sip_register_fixtures.py    synthetic SIP REGISTER messages
  packet_builder.py           raw bytes -> TCPPacket (stands in for the
                              unbuilt capture component)
tests/
  test_edge_chain_integration.py
docs/
  wiring-component-repos.md      setup for both staleness mitigations
  component-repo-dispatch-job.yml  snippet to paste into each component repo
DEFECTS.md                    what this suite found
```

## Fidelity caveat

The SIP fixtures are synthetic, built to exercise exactly the headers
`FieldMapper` reads. A real PCAP capture would be higher fidelity and
would catch malformed-input cases these don't — worth producing once a
capture component exists to record one. At that point
`packet_builder.py` is replaced by a PCAP reader and the tests
themselves shouldn't need to change.
