"""
signal_forge.replay.__main__ — the replay CLI shell.

A thin parse-and-delegate entry point: ``python -m signal_forge.replay
config.json`` reads a JSON replay config, constructs settings and an event
source from it, and hands them to ``run_replay``. The CLI holds no replay
logic — the driver owns all of it (working note D-20). This module's only job
is to turn the JSON shape a Step Functions invocation passes into the
arguments ``run_replay`` expects.

Config shape
------------

::

    {
      "window": {
        "start_time": "2026-06-01T00:00:00+00:00",
        "end_time":   "2026-06-01T01:00:00+00:00",
        "event_pattern": { ... }
      },
      "settings": {
        "SF_DATASET_BUCKET": "sf-live-dataset",
        "SF_REPLAY_DATASET_BUCKET": "sf-replay-dataset",
        "SF_PROJECTION_TABLE": "sf-live-projections",
        "SF_REPLAY_PROJECTION_TABLE": "sf-replay-projections",
        "SF_ENV": "production"
      }
    }

The ``settings`` block is, deliberately, the same ``SF_*`` vocabulary a live
deployment reads from its environment: it is passed straight to
``PlatformSettings.from_env(env=...)``, reusing the one validated construction
path rather than introducing a second. An operator who knows the deployment's
environment variables already knows the replay config.

The config carries *live* names (``SF_DATASET_BUCKET``, ``SF_PROJECTION_TABLE``)
alongside their replay counterparts. It describes the *environment*; the driver
performs the *replay isolation* by calling ``settings.for_replay()`` itself
(D-20). There is no way for the config to express a broken "replay that writes
to the live bucket" state — replay-target selection is the driver's, not the
operator's.

Seams
-----

``build_event_source`` and ``build_pipeline`` are injected into ``main`` with
production defaults, so the parse-and-delegate surface is testable with fakes
without touching AWS — the same dependency-injection shape the driver uses, one
level up. The production ``build_event_source`` reads the EventBridge archive;
that read is AWS plumbing the definition of done places out of scope, so its
body is deferred (it raises ``NotImplementedError`` with a pointer) while its
typed seam exists now. ``build_pipeline`` defaults to the production builder
that wires the dataset writer and projections; the determinism integration
test exercises that builder end-to-end.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.replay.driver import run_replay
from signal_forge.streaming.event_protocol import TelemetryEvent
from signal_forge.streaming.realtime_pipeline import ProcessingResult, RealtimePipeline

EventSourceBuilder = Callable[[Mapping[str, Any]], Iterable[TelemetryEvent]]
PipelineBuilder = Callable[[PlatformSettings], RealtimePipeline]


def build_event_source(window: Mapping[str, Any]) -> Iterable[TelemetryEvent]:
    """
    Construct the replay event source from the config's ``window`` block.

    The production path reads the EventBridge archive over
    ``[start_time, end_time)`` filtered by ``event_pattern``, in original
    arrival order. That read is deployment plumbing the definition of done
    places out of scope ("not deployed to AWS", "not validated against
    production traffic"), so it is deferred: the typed seam exists now, the
    AWS-specific body is a follow-up. Tests inject a fake source in its place;
    the determinism integration test feeds a fixed list.
    """
    raise NotImplementedError(
        "The production EventBridge-archive reader is deferred as out-of-brief "
        "(see docs/replay-workflows.md). Inject an event source via "
        "build_event_source= to run a replay before it lands."
    )


def _production_build_pipeline(settings: PlatformSettings) -> RealtimePipeline:
    """
    The production pipeline builder — wires the full control plane.

    Deferred to the same follow-up as the archive reader: the determinism
    integration test (tests/replay/test_replay_determinism.py) defines the
    builder that wires the dataset writer and projections, and that is the
    builder a production CLI invocation would reference here. Stubbed now so
    the CLI's parse-and-delegate surface is complete and testable with an
    injected builder.
    """
    raise NotImplementedError(
        "The production pipeline builder is deferred; inject one via "
        "build_pipeline= (see tests/replay/test_replay_determinism.py for the "
        "control-plane wiring)."
    )


def run_from_config(
    config: Mapping[str, Any],
    *,
    build_event_source: EventSourceBuilder = build_event_source,
    build_pipeline: PipelineBuilder = _production_build_pipeline,
) -> list[ProcessingResult]:
    """
    Turn a parsed replay config into a ``run_replay`` invocation.

    Constructs settings from the ``settings`` block via
    ``PlatformSettings.from_env`` (the one validated construction path),
    builds the event source from the ``window`` block, and delegates to the
    driver. The driver applies ``for_replay()`` — this function passes *live*
    settings, exactly as they came from the config.
    """
    settings = PlatformSettings.from_env(env=dict(config.get("settings", {})))
    window = config.get("window", {})
    event_source = build_event_source(window)

    return run_replay(
        settings,
        event_source=event_source,
        build_pipeline=build_pipeline,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    build_event_source: EventSourceBuilder = build_event_source,
    build_pipeline: PipelineBuilder = _production_build_pipeline,
) -> int:
    """
    CLI entry point. Reads a JSON config file named on the command line,
    parses it, and delegates to ``run_from_config``. Returns a process exit
    code: 0 on success, 2 on a usage error (mirroring argparse's convention).
    """
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) != 1:
        print("usage: python -m signal_forge.replay <config.json>", file=sys.stderr)
        return 2

    with Path(args[0]).open(encoding="utf-8") as handle:
        config = json.load(handle)

    run_from_config(
        config,
        build_event_source=build_event_source,
        build_pipeline=build_pipeline,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
