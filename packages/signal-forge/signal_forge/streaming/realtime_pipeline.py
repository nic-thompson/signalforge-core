"""
signal_forge.streaming.realtime_pipeline

Orchestration layer that wires the streaming primitives together.

The realtime pipeline is the only component in Phase 1 that performs
side effects. Its responsibilities are:

- extract a partition key from each inbound event via a caller-supplied
  ``PartitionExtractor``,
- observe the watermark manager to classify the event,
- feed (event, classification, watermark) into each registered window
  aggregator and collect emissions,
- dispatch the event through the router so registered handlers
  (detectors, feature builders, dashboard projectors) fire,
- emit a single structured log line per event with the full lineage,
- isolate per-event failures so one bad event cannot poison a batch.

Design properties
-----------------

- **Function-shaped, not daemon-shaped**. ``process()`` and
  ``process_batch()`` are caller-driven. The event source (SQS poll,
  replay iterator, test list) lives outside the pipeline. This is what
  satisfies the brief's "streaming-compatible AND batch-compatible"
  requirement.
- **Fixed execution order**: extract → watermark → aggregate → dispatch
  → log. Watermark before aggregator is mandatory. Router after
  aggregator is deliberate so detector handlers can subscribe to either
  raw events (via the router) or windowed emissions (returned from
  ``process()``).
- **Centralised trace propagation**: trace_id is read off the inbound
  event once and stamped on every log line emitted during processing
  of that event. Lower layers do not log; this is by design.
- **Per-event failure isolation**: an exception in extraction,
  classification, aggregation, or dispatch is logged and the pipeline
  moves to the next event. Strict mode re-raises immediately and is
  intended for replay-validation and integration tests.
- **Wall-clock-free**: no ``datetime.now()``, no system clock. The
  pipeline is fully deterministic when fed the same event sequence.
- **No persistence, no AWS coupling**. Emissions are returned from
  ``process()``. Phase 5 and 6 wrap this layer with sink adapters.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Final
from uuid import UUID

from event_schema_contracts.alerts.alert_event import AlertEvent
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection import DetectionEvent
from event_schema_contracts.features.windowed_feature_vector import (
    FeatureValue,
    WindowedFeatureVectorEvent,
    WindowedFeatureVectorPayload,
)

from signal_forge.alerts.alert_router import AlertRouter
from signal_forge.detection.protocols import EmissionDetector, EventDetector
from signal_forge.features import FEATURE_SCHEMA_VERSION
from event_schema_contracts.base.identity import derive
from signal_forge.streaming.event_protocol import TelemetryEvent
from signal_forge.streaming.event_router import EventRouter
from signal_forge.streaming.observability import StructuredLoggerLike, get_logger
from signal_forge.streaming.watermark_manager import (
    EventClassification,
    WatermarkManager,
)
from signal_forge.streaming.window_aggregator import (
    WindowAggregator,
    WindowEmission,
)

if TYPE_CHECKING:
    # Imported under TYPE_CHECKING to avoid a circular import:
    # signal_forge.datasets.writer imports ProcessingResult from this
    # module. DatasetWriter is a Protocol, so nothing here needs the
    # concrete class at runtime — the annotation is enough for mypy and
    # `from __future__ import annotations` keeps it a string at runtime.
    from signal_forge.datasets.writer import DatasetWriter

# Type alias: a partition extractor takes an event and returns a string
# key. Production extractors will read payload fields (store_id,
# device_id, etc.); the pipeline does not assume a particular shape.
PartitionExtractor = Callable[[TelemetryEvent], str]


# Logical event types used by the pipeline for its own structured-log
# emissions. Distinct from the telemetry event types it processes.
_LOG_EVENT_PROCESSED: Final[str] = "pipeline.processed"
_LOG_EVENT_EXTRACTION_ERROR: Final[str] = "pipeline.extraction_error"
_LOG_EVENT_AGGREGATION_ERROR: Final[str] = "pipeline.aggregation_error"
_LOG_EVENT_DETECTOR_ERROR: Final[str] = "pipeline.detector_error"
_LOG_EVENT_ALERT_ROUTER_ERROR = "alerts.router_error"
_LOG_EVENT_WRITER_ERROR: Final[str] = "pipeline.writer_error"
_LOG_EVENT_BATCH_SUMMARY: Final[str] = "pipeline.batch_summary"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ProcessingResult:
    """
    Outcome of processing a single event.

    Returned to the caller for routing emissions and for tests asserting
    pipeline behaviour without inspecting the logger. Includes the
    watermark observation and any emissions produced across all
    registered aggregators and any detection events produced by
    registered detectors.

    The ``detections`` list collects detections in deterministic order:
    event-detector outputs first (in registration order), then
    emission-detector outputs (per emission, then registration order
    within that emission). Replay determinism is preserved provided
    every detector itself observes the determinism contract documented
    in ``signal_forge.detection.protocols``.

    The ``features`` list collects bundled feature emissions: one
    ``WindowedFeatureVectorEvent`` per ``(partition_key, window_start)``
    group from this call's emissions. A window with multiple aggregations
    registered against it produces a single feature event carrying all
    values in its ``feature_values`` dict. Repair emissions
    (``is_repair=True``) produce additional feature events for the same
    group — downstream consumers see updated values via a second event.
    """

    event_id: str
    partition_key: str | None
    classification: EventClassification | None
    handlers_invoked: int
    handler_failures: int
    emissions: list[WindowEmission]
    detections: list[DetectionEvent]
    alerts: list[AlertEvent]
    extraction_failed: bool
    features: list[WindowedFeatureVectorEvent]

def _bundle_feature_events(
    emissions: list[WindowEmission],
) -> list[WindowedFeatureVectorEvent]:
    """
    Group emissions by ``(partition_key, window_start)`` and construct
    one ``WindowedFeatureVectorEvent`` per group.

    Each event's ``feature_values`` map carries the
    ``{aggregation_name: value}`` pairs from emissions in that group.
    A single window with multiple aggregations registered (e.g.
    distinct-device-count plus mean-latency) bundles into one event
    with both values rather than emitting separately — downstream
    consumers see a coherent feature snapshot per partition-window.

    Repair emissions (``is_repair=True``) are bundled alongside
    first-closure emissions for the same ``(partition_key,
    window_start)`` if both happen to be present in a single call's
    emissions list. The typical case is that a single call produces
    either first-closures or repairs for a given window, not both;
    when both appear, the bundled event carries all their values in
    the dict (last-write-wins on duplicate aggregation_names, which
    is sound because a repair re-emits the same aggregation with
    updated state).

    Ordering: groups emit in the order their FIRST emission appears
    in the input list. Within a group, ``feature_values`` is
    assembled via dict construction; iteration order is insertion
    order (Python 3.7+ guarantee). Replay-deterministic provided
    the input emission ordering is.

    Trace propagation is best-effort: if any emission in the group
    has a non-``None`` ``last_contributing_trace_id``, the feature
    event inherits the FIRST such trace. Otherwise a fresh
    ``TraceContext`` is constructed. This matches the lineage-
    propagation pattern used by emission detectors for
    ``DetectionEvent``s in Phase 2.

    ``event_id`` is left to ``BaseEvent``'s default (``uuid4()``).
    Per-event identity is non-deterministic across replays; sequence-
    level determinism (same input -> same number of events in the
    same order with the same payloads) is preserved. This mirrors
    the trade-off accepted for ``DetectionEvent``.
    """
    if not emissions:
        return []

    # Group by (partition_key, window_start), preserving first-seen order.
    groups: dict[tuple[str, datetime], list[WindowEmission]] = {}
    for emission in emissions:
        key = (emission.partition_key, emission.window_start)
        groups.setdefault(key, []).append(emission)

    events: list[WindowedFeatureVectorEvent] = []
    for (partition_key, window_start), group in groups.items():
        first = group[0]
        feature_values: dict[str, FeatureValue] = {
            emission.aggregation_name: emission.value for emission in group
        }
        trace_id_str = next(
            (
                emission.last_contributing_trace_id
                for emission in group
                if emission.last_contributing_trace_id is not None
            ),
            None,
        )
        trace = (
            TraceContext(trace_id=UUID(trace_id_str))
            if trace_id_str is not None
            else TraceContext()
        )
        payload = WindowedFeatureVectorPayload(
            partition_key=partition_key,
            window_start=window_start,
            window_end=first.window_end,
            feature_values=feature_values,
            feature_version=FEATURE_SCHEMA_VERSION,
        )
        events.append(
            WindowedFeatureVectorEvent(
                event_id=derive(
                    "event.feature",
                    partition_key,
                    window_start,
                    first.window_end,
                    FEATURE_SCHEMA_VERSION,
                ),
                event_timestamp=first.window_end,
                trace=trace,
                payload=payload,
            )
        )
    return events


# ---------------------------------------------------------------------------
# Default partition extractors
# ---------------------------------------------------------------------------


def by_event_source(event: TelemetryEvent) -> str:
    """
    Default partition extractor: group events by their source service.

    Suitable for early-phase wiring and tests. Production registrations
    will use payload-aware extractors (see ``by_payload_field``).
    """
    return event.metadata.source


def by_payload_field(field_name: str) -> PartitionExtractor:
    """
    Build an extractor that reads a string-typed payload attribute.

    Most production telemetry partitions on ``store_id`` or
    ``device_id`` — both live on the payload. The extractor coerces to
    string so UUID and integer keys are handled uniformly.
    """

    def extract(event: TelemetryEvent) -> str:
        payload = event.payload
        if payload is None:
            raise ValueError(f"event payload is None; cannot extract '{field_name}'")
        if not hasattr(payload, field_name):
            raise ValueError(
                f"event payload of type {type(payload).__name__} has no attribute '{field_name}'"
            )
        return str(getattr(payload, field_name))

    return extract


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class RealtimePipeline:
    """
    Orchestrates router + watermark manager + aggregators for a single
    consumer shard.

    Parameters
    ----------
    router:
        The event router that fans out to registered handlers.
    watermark_manager:
        Per-key event-time progression. The pipeline calls
        ``observe(key, event_timestamp)`` exactly once per event.
    partition_extractor:
        Callable mapping ``TelemetryEvent`` to a string key.
    strict:
        When True, exceptions in extraction, aggregation, or dispatch
        propagate immediately (use for replay-validation and tests).
        When False (default — production), per-event errors are logged
        and the pipeline continues with the next event.
    logger:
        Optional structured-logger override. Tests use this to inspect
        emitted log lines.

    Aggregators are registered with ``register_aggregator(name, aggregator)``
    after construction. Multiple aggregators may be registered;
    emissions from each are returned in a stable order.
    """

    def __init__(
        self,
        *,
        router: EventRouter,
        watermark_manager: WatermarkManager,
        partition_extractor: PartitionExtractor,
        strict: bool = False,
        logger: StructuredLoggerLike | None = None,
    ) -> None:
        self._router = router
        self._watermarks = watermark_manager
        self._extract = partition_extractor
        self._strict = strict
        self._logger = logger if logger is not None else get_logger(
            "signal_forge.streaming.realtime_pipeline"
        )

        # Aggregator registrations are kept as a list of (name, aggregator)
        # tuples so emission order is the registration order. Stable
        # ordering is required for replay determinism.
        self._aggregators: list[tuple[str, WindowAggregator]] = []

        # Event detectors observe every event in registration order.
        # Emission detectors are keyed by ``aggregation_name`` for O(1)
        # dispatch lookup per emission; within a key, they observe in
        # registration order.
        self._event_detectors: list[EventDetector] = []
        self._emission_detectors: dict[str, list[EmissionDetector]] = {}

        # A single optional dataset writer. Structurally singular: the
        # replay-isolation model swaps one active bucket via
        # PlatformSettings.for_replay(), so "which of several writers is
        # the replay-isolated one?" has no coherent answer. Registered
        # post-construction like the other observers; see
        # register_dataset_writer.
        self._dataset_writer: DatasetWriter | None = None
        self._alert_router: AlertRouter | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_aggregator(
        self,
        name: str,
        aggregator: WindowAggregator,
    ) -> None:
        """
        Register a window aggregator under ``name``.

        ``name`` identifies the aggregator in emission logs and lets
        downstream consumers (Phase 6 dashboards) demultiplex emissions
        from a pipeline that runs several aggregators in parallel.
        """
        if not name:
            raise ValueError("name must be non-empty")
        if any(existing == name for existing, _ in self._aggregators):
            raise ValueError(f"aggregator already registered: {name}")
        self._aggregators.append((name, aggregator))

    def register_event_detector(self, detector: EventDetector) -> None:
        """
        Register an event detector.

        The detector observes every event dispatched through the
        pipeline, after the router has run. Multiple event detectors
        may be registered; each observes every event in registration
        order. A failing detector is logged and skipped; other
        detectors still observe the same event.
        """
        self._event_detectors.append(detector)

    def register_emission_detector(self, detector: EmissionDetector) -> None:
        """
        Register an emission detector.

        The detector observes ``WindowEmission``s whose
        ``aggregation_name`` matches ``detector.aggregation_name``.
        Multiple emission detectors may be registered against the same
        ``aggregation_name``; within that group, they observe each
        emission in registration order. A failing detector is logged
        and skipped; other detectors still observe the same emission.
        """
        bucket = self._emission_detectors.setdefault(detector.aggregation_name, [])
        bucket.append(detector)

    def register_dataset_writer(self, writer: DatasetWriter) -> None:
        """
        Register the dataset writer.

        At most one writer per pipeline: a second registration raises,
        the same fail-fast posture ``register_aggregator`` takes on a
        duplicate name. The single-writer constraint is the honest shape
        of the replay-isolation model — ``PlatformSettings.for_replay()``
        swaps one active bucket, so a list of writers would have no
        well-defined replay-isolated member.

        The writer receives each successful ``ProcessingResult`` (see
        ``process``). It is *not* called on the extraction-failure path,
        whose result carries no emissions and would buffer nothing. A
        writer that raises is logged and skipped (strict mode re-raises),
        matching the per-observer failure isolation applied to
        aggregators and detectors.
        """
        if self._dataset_writer is not None:
            raise ValueError("dataset writer already registered")
        self._dataset_writer = writer

    def register_alert_router(self, router: AlertRouter) -> None:
        """
        Register the alert router.

        At most one router per pipeline: a second registration raises,
        the same fail-fast posture as register_dataset_writer. The router
        consumes each successful result's detections and produces alerts
        onto ProcessingResult.alerts. It is not run on the extraction-
        failure path, which carries no detections.
        """
        if self._alert_router is not None:
            raise ValueError("alert router already registered")
        self._alert_router = router

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process(self, event: TelemetryEvent) -> ProcessingResult:
        """
        Process a single event through the full pipeline.

        Returns a ``ProcessingResult`` summarising what happened.
        Emissions in the result come from all registered aggregators,
        in registration order. Detections come from all registered
        detectors: event-detector outputs first (registration order),
        then emission-detector outputs (per emission, then registration
        order within that emission).
        """

        trace_id = str(event.trace.trace_id)
        event_id = str(event.event_id)

        # ── 1. Partition extraction ─────────────────────────────────────
        try:
            partition_key = self._extract(event)
            if not partition_key:
                raise ValueError("partition extractor returned empty key")
        except Exception as exc:
            self._logger.error(
                "Partition extraction failed",
                event_type=_LOG_EVENT_EXTRACTION_ERROR,
                trace_id=trace_id,
                metadata={
                    "event_id": event_id,
                    "telemetry_event_type": event.metadata.event_type,
                    "telemetry_schema_version": event.metadata.schema_version,
                    "exception_type": type(exc).__name__,
                    "exception_message": str(exc),
                },
            )
            if self._strict:
                raise
            return ProcessingResult(
                event_id=event_id,
                partition_key=None,
                classification=None,
                handlers_invoked=0,
                handler_failures=0,
                emissions=[],
                detections=[],
                alerts=[],
                extraction_failed=True,
                features=[],
            )

        # ── 2. Watermark observation ───────────────────────────────────
        # The watermark manager's observe() can only fail on contract
        # violations (empty key, naive timestamp). We don't catch those;
        # they indicate a programming error worth surfacing immediately.
        observation = self._watermarks.observe(partition_key, event.event_timestamp)

        # ── 3. Aggregation across registered aggregators ───────────────
        all_emissions: list[WindowEmission] = []
        for agg_name, aggregator in self._aggregators:
            try:
                emissions = aggregator.observe(
                    partition_key=partition_key,
                    event_timestamp=event.event_timestamp,
                    contribution=event.payload,
                    classification=observation.classification,
                    watermark=observation.watermark,
                    trace_id=trace_id,
                )
                all_emissions.extend(emissions)
            except Exception as exc:
                self._logger.error(
                    "Aggregator raised during observe()",
                    event_type=_LOG_EVENT_AGGREGATION_ERROR,
                    trace_id=trace_id,
                    metadata={
                        "event_id": event_id,
                        "aggregator": agg_name,
                        "partition_key": partition_key,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if self._strict:
                    raise

        # ── 4. Router dispatch ─────────────────────────────────────────
        # The router has its own failure isolation; in strict mode it
        # re-raises, which we propagate via its strict flag (configured
        # at the router level, not here).
        dispatch_result = self._router.dispatch(event)

        # ── 5. Detector dispatch ───────────────────────────────────────
        # Event detectors observe the event after the router has run;
        # detection is conceptually a synthesis step downstream of
        # dispatch. Emission detectors observe each window emission
        # produced in step 3, routed by aggregation_name. Per-detector
        # failure isolation: a raising detector is logged and skipped,
        # other detectors continue to observe the same input.
        all_detections: list[DetectionEvent] = []

        for event_detector in self._event_detectors:
            try:
                all_detections.extend(event_detector.observe_event(event))
            except Exception as exc:
                self._logger.error(
                    "Event detector raised during observe_event()",
                    event_type=_LOG_EVENT_DETECTOR_ERROR,
                    trace_id=trace_id,
                    metadata={
                        "event_id": event_id,
                        "detector": event_detector.name,
                        "detector_kind": "event",
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if self._strict:
                    raise

        for emission in all_emissions:
            for emission_detector in self._emission_detectors.get(
                emission.aggregation_name, []
            ):
                try:
                    all_detections.extend(emission_detector.observe_emission(emission))
                except Exception as exc:
                    self._logger.error(
                        "Emission detector raised during observe_emission()",
                        event_type=_LOG_EVENT_DETECTOR_ERROR,
                        trace_id=trace_id,
                        metadata={
                            "event_id": event_id,
                            "detector": emission_detector.name,
                            "detector_kind": "emission",
                            "aggregation_name": emission.aggregation_name,
                            "exception_type": type(exc).__name__,
                            "exception_message": str(exc),
                        },
                    )
                    if self._strict:
                        raise

        # ── 6. Single structured log line summarising the event ────────
        self._logger.info(
            "Processed event through pipeline",
            event_type=_LOG_EVENT_PROCESSED,
            trace_id=trace_id,
            metadata={
                "event_id": event_id,
                "telemetry_event_type": event.metadata.event_type,
                "telemetry_schema_version": event.metadata.schema_version,
                "partition_key": partition_key,
                "classification": observation.classification.value,
                "handlers_invoked": dispatch_result.handlers_invoked,
                "handler_failures": dispatch_result.handler_failures,
                "emissions": len(all_emissions),
                "detections": len(all_detections),
            },
        )
        all_alerts: list[AlertEvent] = []
        if self._alert_router is not None:
            try:
                all_alerts = self._alert_router.route(all_detections)
            except Exception as exc:
                self._logger.error(
                    "Alert router raised during route()",
                    event_type=_LOG_EVENT_ALERT_ROUTER_ERROR,
                    trace_id=trace_id,
                    metadata={
                        "event_id": event_id,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if self._strict:
                    raise
        result = ProcessingResult(
            event_id=event_id,
            partition_key=partition_key,
            classification=observation.classification,
            handlers_invoked=dispatch_result.handlers_invoked,
            handler_failures=dispatch_result.handler_failures,
            emissions=all_emissions,
            detections=all_detections,
            alerts=all_alerts,
            extraction_failed=False,
            features=_bundle_feature_events(all_emissions),
        )

        # ── 7. Dataset writer dispatch ─────────────────────────────────
        # Hand the successful result to the registered dataset writer, if
        # any. Bulkheaded like every other observer: a raising writer is
        # logged with lineage and skipped so it cannot poison the batch;
        # strict mode re-raises for replay-validation runs. Only the
        # success path writes — the extraction-failure result carries no
        # emissions and would buffer nothing.
        if self._dataset_writer is not None:
            try:
                self._dataset_writer.write(result)
            except Exception as exc:
                self._logger.error(
                    "Dataset writer raised during write()",
                    event_type=_LOG_EVENT_WRITER_ERROR,
                    trace_id=trace_id,
                    metadata={
                        "event_id": event_id,
                        "partition_key": partition_key,
                        "emissions": len(all_emissions),
                        "detections": len(all_detections),
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                )
                if self._strict:
                    raise

        return result

    def process_batch(
        self,
        events: Iterable[TelemetryEvent],
    ) -> list[ProcessingResult]:
        """
        Process a batch of events in arrival order.

        Returns one ``ProcessingResult`` per event. Emits a single
        ``pipeline.batch_summary`` log line at the end with aggregate
        counters — useful for SQS-batch metrics without spamming logs.
        """

        results: list[ProcessingResult] = []
        on_time = 0
        late_tolerated = 0
        late_dropped = 0
        extraction_failures = 0
        total_emissions = 0
        total_detections = 0

        for event in events:
            result = self.process(event)
            results.append(result)

            if result.extraction_failed:
                extraction_failures += 1
            elif result.classification is EventClassification.ON_TIME:
                on_time += 1
            elif result.classification is EventClassification.LATE_TOLERATED:
                late_tolerated += 1
            elif result.classification is EventClassification.LATE_DROPPED:
                late_dropped += 1

            total_emissions += len(result.emissions)
            total_detections += len(result.detections)

        self._logger.info(
            "Processed event batch",
            event_type=_LOG_EVENT_BATCH_SUMMARY,
            trace_id=None,
            metadata={
                "events": len(results),
                "on_time": on_time,
                "late_tolerated": late_tolerated,
                "late_dropped": late_dropped,
                "extraction_failures": extraction_failures,
                "emissions": total_emissions,
                "detections": total_detections,
            },
        )

        return results
