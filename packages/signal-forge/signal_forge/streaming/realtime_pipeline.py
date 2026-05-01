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
from typing import Final

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

# Type alias: a partition extractor takes an event and returns a string
# key. Production extractors will read payload fields (store_id,
# device_id, etc.); the pipeline does not assume a particular shape.
PartitionExtractor = Callable[[TelemetryEvent], str]


# Logical event types used by the pipeline for its own structured-log
# emissions. Distinct from the telemetry event types it processes.
_LOG_EVENT_PROCESSED: Final[str] = "pipeline.processed"
_LOG_EVENT_EXTRACTION_ERROR: Final[str] = "pipeline.extraction_error"
_LOG_EVENT_AGGREGATION_ERROR: Final[str] = "pipeline.aggregation_error"
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
    registered aggregators.
    """

    event_id: str
    partition_key: str | None
    classification: EventClassification | None
    handlers_invoked: int
    handler_failures: int
    emissions: list[WindowEmission]
    extraction_failed: bool


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

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def process(self, event: TelemetryEvent) -> ProcessingResult:
        """
        Process a single event through the full pipeline.

        Returns a ``ProcessingResult`` summarising what happened.
        Emissions in the result come from all registered aggregators,
        in registration order.
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
                extraction_failed=True,
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

        # ── 5. Single structured log line summarising the event ────────
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
            },
        )

        return ProcessingResult(
            event_id=event_id,
            partition_key=partition_key,
            classification=observation.classification,
            handlers_invoked=dispatch_result.handlers_invoked,
            handler_failures=dispatch_result.handler_failures,
            emissions=all_emissions,
            extraction_failed=False,
        )

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
            },
        )

        return results
