"""
stream_pipeline.replay.__main__ — the replay CLI shell.

A thin parse-and-delegate entry point: ``python -m stream_pipeline.replay
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
        "end_time":   "2026-06-01T01:00:00+00:00"
      },
      "settings": {
        "SF_DATASET_BUCKET": "sf-live-dataset",
        "SF_REPLAY_DATASET_BUCKET": "sf-replay-dataset",
        "SF_PROJECTION_TABLE": "sf-live-projections",
        "SF_REPLAY_PROJECTION_TABLE": "sf-replay-projections",
        "SF_ENV": "production"
      }
    }

``window`` originally documented a third key, ``event_pattern``, on the
assumption that a replay could be filtered by event type. EventBridge's
``StartReplay`` API has no such parameter — it takes only
``EventSourceArn``, ``EventStartTime``, ``EventEndTime`` and
``Destination`` — confirmed against the real API while building the
Step Functions replay workflow in ``aws-event-pipeline-infra``, which
does not filter either. A replay is a time window, in full; narrowing
what it contains happens some other way (a second archive with its own
narrower ``event_pattern`` set at creation, or filtering downstream of
delivery), never at ``StartReplay`` itself. Removed from the documented
shape rather than left as an aspiration nothing implements.

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

from stream_pipeline.config.platform_settings import PlatformSettings
from stream_pipeline.detection.detectors import OfflineDetector
from stream_pipeline.detection.device_registry import DeviceRegistry
from stream_pipeline.replay.driver import run_replay
from stream_pipeline.streaming.event_protocol import TelemetryEvent
from stream_pipeline.streaming.event_router import EventRouter
from stream_pipeline.streaming.realtime_pipeline import (
    ProcessingResult,
    RealtimePipeline,
    by_payload_field,
)
from stream_pipeline.streaming.watermark_manager import WatermarkManager

EventSourceBuilder = Callable[[Mapping[str, Any]], Iterable[TelemetryEvent]]
PipelineBuilder = Callable[[PlatformSettings], RealtimePipeline]


def build_event_source(window: Mapping[str, Any]) -> Iterable[TelemetryEvent]:
    """
    Construct the replay event source from the config's ``window`` block.

    Deliberately not implemented here, and never will be: this function
    is AWS-specific plumbing, and this package stays infra-agnostic by
    design (D-4's boundary applied one level up). The real
    implementation lives in aws-event-pipeline-infra, which:

    1. Calls ``StartReplay`` for ``[start_time, end_time)`` against the
       archive's own bus. There is no ``event_pattern`` parameter —
       ``StartReplay`` takes only ``EventSourceArn``, ``EventStartTime``,
       ``EventEndTime`` and ``Destination`` (confirmed against the real
       API; see docs/replay-workflows.md). A replay is a time window,
       in full.
    2. Polls ``DescribeReplay`` until the replay reaches a terminal
       state, mirroring the Step Functions workflow's own polling.
    3. Drains a dedicated SQS queue fed by a rule matching
       ``{"replay-name": [{"exists": true}]}`` — the exact inverse of
       the live-ingestion rules' exclusion filter, so replayed events
       reach this queue and only this queue, never the live one.
    4. Returns the deserialised, deleted-on-success events as a list.

    ``run_replay`` materialises ``event_source`` via ``list()`` before
    processing (D-20), so this is a bounded, one-shot batch fetch for a
    single window — not a long-running consumer, unlike the live
    ingestion path, which does need to be one (see phase-8-plan.md).

    Tests inject a fake source in its place; the determinism
    integration test feeds a fixed list.
    """
    raise NotImplementedError(
        "The production EventBridge-archive reader lives in "
        "aws-event-pipeline-infra, not this package — see this "
        "function's own docstring. Inject an event source via "
        "build_event_source= to run a replay without it."
    )


# The identity of DeviceRegistry's provisioning-schema registration.
# Nothing currently emits this event type; registering it costs
# nothing today and means this builder keeps working unmodified if
# device provisioning ever does arrive on the bus. See
# DeviceRegistry's own module docstring.
DEVICE_REGISTRATION = ("device.registration", "v1")

# The event type this builder actually wires DeviceRegistry against
# today. See the class docstring below for why both are registered.
SIP_REGISTRATION = ("sip.registration", "v1")


def _production_build_pipeline(settings: PlatformSettings) -> RealtimePipeline:
    """
    The production pipeline builder — wires the full control plane.

    This is the one place SignalForge's actual analytics — which
    detectors run, what they watch, how partitioning works — is
    defined. Both the live ingestion consumer
    (aws-event-pipeline-infra's scripts/consume_telemetry.py) and this
    replay CLI call this same function, deliberately: run_replay's own
    docstring states the invariant this exists to satisfy — "the same
    builder constructs the live and replay pipelines; only the settings
    differ... that property is what makes replay verifiable rather than
    merely plausible." Two independently-written builders could only
    ever agree by coincidence; one shared builder makes agreement
    structural. This was previously a stub, which meant that invariant
    had never actually been true in production.

    Partitioning is by ``store_id``. That field is on every telemetry
    payload in this system by construction — see telemetry-parser's
    ADR-001 and event-schema-contracts' ADR-002 — so it partitions any
    event type this builder might ever carry, not only sip.registration.

    No aggregator is registered. ``OfflineDetector`` is an event
    detector: it is driven directly from ``process()``'s per-event call
    to every registered ``EventDetector``, not from window emissions, so
    it needs no window — a window is the right tool for a rate or count
    over time (the store-heartbeat rollup, once it exists), not for "has
    this specific device gone quiet".

    ``DeviceRegistry`` — the device-to-store projection
    ``OfflineDetector.store_lookup`` depends on — is registered for both
    ``device.registration`` (the schema its own module docstring names)
    and ``sip.registration`` (what actually flows today). Registering
    only the former would leave every device this builder ever sees
    resolving to no store, and ``OfflineDetector`` silently skips
    emission when a store cannot be resolved — a detector that runs
    forever and never once fires, with no error anywhere.
    """

    router = EventRouter()
    watermarks = WatermarkManager(
        lateness_tolerance_seconds=settings.late_event_tolerance_seconds
    )

    pipeline = RealtimePipeline(
        router=router,
        watermark_manager=watermarks,
        partition_extractor=by_payload_field("store_id"),
    )

    registry = DeviceRegistry()
    router.register(*DEVICE_REGISTRATION, registry.observe_registration)
    router.register(*SIP_REGISTRATION, registry.observe_registration)

    pipeline.register_event_detector(
        OfflineDetector(
            threshold_seconds=settings.offline_threshold_seconds,
            device_id_extractor=lambda event: getattr(
                event.payload, "device_id", None
            ),
            store_lookup=registry.store_for,
        )
    )

    return pipeline


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
        print("usage: python -m stream_pipeline.replay <config.json>", file=sys.stderr)
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
