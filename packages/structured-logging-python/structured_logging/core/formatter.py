from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from dataclasses import asdict
from typing import Any, Mapping

from structured_logging.core.context import ServiceContext
from structured_logging.trace.trace_context import TraceContext
from structured_logging.schema.log_event_schema import (
    LogEventSchema,
    LogLevel,
    StructuredError,
)


_STANDARD_LEVELS: dict[int, LogLevel] = {
    logging.DEBUG: "DEBUG",
    logging.INFO: "INFO",
    logging.WARNING: "WARNING",
    logging.ERROR: "ERROR",
    logging.CRITICAL: "CRITICAL",
}


def _narrow_level(record: logging.LogRecord) -> LogLevel:
    """
    Narrows a LogRecord's level name to the schema's LogLevel.

    ``levelname`` is a plain ``str``, and the schema requires one of five
    literals. For the standard levels they coincide, but a caller may
    register a custom level — ``logging.addLevelName(25, "NOTICE")`` — and
    that name would fail schema validation at emit time, turning a log
    call into an exception.

    A custom level is therefore mapped to the highest standard level at or
    below its numeric severity, so NOTICE(25) is recorded as INFO(20)
    rather than crashing or being silently dropped. The name is lost; the
    severity ordering is not, which is what a consumer filters on.
    """

    standard = _STANDARD_LEVELS.get(record.levelno)

    if standard is not None:
        return standard

    below = [level for level in _STANDARD_LEVELS if level <= record.levelno]

    return _STANDARD_LEVELS[max(below)] if below else "DEBUG"


class StructuredJSONFormatter(logging.Formatter):
    """
    JSON log formatter backed by LogEventSchema.

    Guarantees schema-stable output across all services using this library.
    """

    def format(self, record: logging.LogRecord) -> str:
        event = self._build_log_event(record)
        return json.dumps(
            self._serialise_event(event),
            separators=(",", ":"),
        )

    def _build_log_event(self, record: logging.LogRecord) -> LogEventSchema:

        metadata: Mapping[str, Any] = getattr(record, "metadata", {}) or {}

        trace_state = TraceContext.get()

        error = getattr(record, "error", None)

        return LogEventSchema(
            timestamp=datetime.fromtimestamp(record.created, timezone.utc),
            level=_narrow_level(record),
            service=ServiceContext.service_name(),
            environment=ServiceContext.environment(),
            event_type=getattr(record, "event_type", "log.event"),
            message=record.getMessage(),
            trace_id=(
                getattr(record, "trace_id", None)
                or (trace_state.trace_id if trace_state else None)
            ),
            parent_trace_id=(
                trace_state.parent_span_id if trace_state else None
            ),
            correlation_id=(
                trace_state.correlation_id if trace_state else None
            ),
            pipeline_stage=(
                trace_state.pipeline_stage if trace_state else None
            ),
            error=error,
            metadata=metadata,
    )

    def _serialise_event(self, event: LogEventSchema) -> Mapping[str, Any]:
        """
        Convert schema object into JSON-safe dictionary.
        """

        payload = asdict(event)

        # JSON does not support datetime → convert to ISO-8601
        payload["timestamp"] = event.timestamp.isoformat()

        return payload