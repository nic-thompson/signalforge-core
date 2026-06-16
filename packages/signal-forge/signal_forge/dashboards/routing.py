"""
signal_forge.dashboards.routing

Routing of pipeline detections into dashboard projections.

Phase 6 projections are pure consumers of ``ProcessingResult.detections``
(D-5): the pipeline produces detections as a return value and knows nothing
about projections. This module is the thin consumer that feeds a result's
detections to a set of projections - it lives *beside* the pipeline, not
inside it.

Keeping projection routing out of the pipeline is what makes replay
isolation trivial: a replay caller routes to a replay-scoped
``ProjectionStore`` (or does not route at all), with no pipeline state to
gate. Contrast ``AlertRouter`` and ``DatasetWriter``, which *are* registered
on the pipeline - but only because their output feeds back onto the
``ProcessingResult`` (alerts) or needs the assembled result (features). A
projection produces nothing the result carries and writes to its own store,
so it has no reason to run inside ``process()``; registering it there would
reintroduce the side-effect-on-the-pipeline shape D-5 deliberately rejected,
and a replay run would mutate live projection state unless gated.

Failure isolation: each ``projection.observe(detection)`` call is
bulkheaded, the same discipline the pipeline applies to aggregators,
detectors, the alert router, and the dataset writer. A projection that
raises is logged with lineage (``dashboards.projection_error``) and skipped;
the remaining projections still observe the detection. ``strict=True``
re-raises immediately, for replay-validation and integration tests.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Final, Protocol

from signal_forge.streaming.observability import StructuredLoggerLike, get_logger

if TYPE_CHECKING:
    from event_schema_contracts.detection import DetectionEvent

    from signal_forge.streaming.realtime_pipeline import ProcessingResult


_LOG_EVENT_PROJECTION_ERROR: Final[str] = "dashboards.projection_error"


class DetectionProjection(Protocol):
    """
    Structural protocol for a dashboard projection that folds detections.

    The three Phase 6 projections (offline-count, active-outage,
    anomaly-rate) satisfy this by exposing ``observe``. Each projection
    self-filters by ``detection_type``, so the router fans every detection
    out to every projection without knowing which projection consumes which
    type. Statically checked, not ``@runtime_checkable`` - the same
    convention the detector protocols follow.
    """

    def observe(self, detection: DetectionEvent) -> None: ...


def route_detections(
    result: ProcessingResult,
    projections: Iterable[DetectionProjection],
    *,
    logger: StructuredLoggerLike | None = None,
    strict: bool = False,
) -> None:
    """
    Fold every detection in ``result`` into each projection.

    Detections are fanned out: each detection is offered to every
    projection in turn, and each projection ignores the types it does not
    handle. The extraction-failure result carries no detections, so routing
    it is a no-op.

    Each ``observe`` call is bulkheaded: a raising projection is logged
    (``dashboards.projection_error``) with the detection's lineage and
    skipped, so one faulty projection cannot stop the others from seeing the
    detection. ``strict=True`` re-raises immediately.

    ``projections`` is materialised once so a generator argument is not
    exhausted after the first detection - every detection sees every
    projection.
    """
    log = (
        logger
        if logger is not None
        else get_logger("signal_forge.dashboards.routing")
    )
    projection_list = list(projections)
    for detection in result.detections:
        for projection in projection_list:
            try:
                projection.observe(detection)
            except Exception as exc:
                log.error(
                    "Projection raised during observe()",
                    event_type=_LOG_EVENT_PROJECTION_ERROR,
                    trace_id=str(detection.trace.trace_id),
                    metadata={
                        "event_id": result.event_id,
                        "detection_id": str(detection.payload.detection_id),
                        "detection_type": detection.payload.detection_type,
                        "projection": type(projection).__name__,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if strict:
                    raise
