"""
The replay driver — Phase 7.

``run_replay`` re-runs an archived event sequence through the same processing
code that produced it, writing to replay-isolated sinks, byte-for-byte
identical to the original run. It is the capstone the whole project's
determinism discipline was built toward (DDIA Chapter 11/12 reprocessing):
no ``datetime.now()`` in pure components, epoch-aligned windows, UUIDv5-derived
identity, the function-shaped pipeline, ``for_replay()`` sink swaps, and
idempotent folds all exist so that this function can reproduce a run exactly.

The driver is function-shaped over an injected event source and an injected
pipeline-builder (working note D-20), mirroring the form the pipeline itself
took (D-4) one level up:

- The **event source** is an ``Iterable[TelemetryEvent]``. Production backs it
  with an EventBridge-archive reader; the determinism test backs it with a
  fixed list. The driver does not know or care which.
- The **builder** is a ``Callable[[PlatformSettings], RealtimePipeline]`` that
  performs the full registration sequence against whatever settings it is
  handed. The driver calls it with ``settings.for_replay()``. The *same*
  builder constructs the live and replay pipelines; only the settings differ.
  That "same builder, different settings" property is what makes replay
  verifiable rather than merely plausible.

The driver owns the ``for_replay()`` swap. A caller passes *live* settings; the
driver swaps them before building, so isolation cannot be forgotten — there is
no path by which a caller accidentally hands the builder live settings and
leaks replay output into live sinks. Replay isolation needs no flag and no
gating: the driver builds its own pipeline (its own ``WatermarkManager``, its
own aggregator state), so there is no shared mutable state with any live
pipeline. The only isolation required is that sinks point at replay targets,
which ``for_replay()`` guarantees (D-12).

The driver is an orchestration-layer side-effect site (D-4): it emits one
``replay.completed`` structured log line stamping the replay lineage — the
environment tag, the event count, and the aggregate result counts. It does
*not* echo per-event or batch-summary lines; ``RealtimePipeline.process_batch``
already emits ``pipeline.batch_summary`` for the run's internals. The driver's
line records only what the pipeline cannot know: that this was a replay, in
this environment, over this many events.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.streaming.event_protocol import TelemetryEvent
from signal_forge.streaming.observability import StructuredLoggerLike, get_logger
from signal_forge.streaming.realtime_pipeline import ProcessingResult, RealtimePipeline

__all__ = ["run_replay"]


def run_replay(
    settings: PlatformSettings,
    *,
    event_source: Iterable[TelemetryEvent],
    build_pipeline: Callable[[PlatformSettings], RealtimePipeline],
    logger: StructuredLoggerLike | None = None,
) -> list[ProcessingResult]:
    """
    Replay an event sequence through a replay-isolated pipeline.

    Builds a pipeline from ``settings.for_replay()`` via ``build_pipeline``,
    feeds ``event_source`` through it in arrival order, logs the replay
    lineage, and returns one ``ProcessingResult`` per event.

    The caller passes *live* ``settings``; the driver applies ``for_replay()``
    itself. This is deliberate — the driver owning the swap is what guarantees
    a replay run cannot accidentally write to live sinks (D-20). The builder
    receives the replay-swapped settings and constructs its sinks against the
    replay-isolated bucket / table / bus accordingly.

    ``event_source`` is materialised once into a list before processing, so a
    single-use generator cannot be exhausted between the count and the run, and
    the event count is knowable for the lineage log line.
    """
    log = logger if logger is not None else get_logger("signal_forge.replay.driver")

    replay_settings = settings.for_replay()
    pipeline = build_pipeline(replay_settings)

    events = list(event_source)
    results = pipeline.process_batch(events)

    total_detections = sum(len(result.detections) for result in results)
    total_emissions = sum(len(result.emissions) for result in results)

    log.info(
        "replay completed",
        event_type="replay.completed",
        metadata={
            "environment": replay_settings.environment,
            "events": len(events),
            "results": len(results),
            "detections": total_detections,
            "emissions": total_emissions,
        },
    )

    return results
