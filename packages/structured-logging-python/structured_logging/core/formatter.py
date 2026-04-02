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
    StructuredError
)


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
            level=record.levelname,
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