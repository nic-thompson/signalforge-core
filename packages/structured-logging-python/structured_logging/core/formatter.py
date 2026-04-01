from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from structured_logging.core.context import ServiceContext
from structured_logging.trace.trace_context import TraceContext


class StructuredJSONFormatter(logging.Formatter):
    """
    JSON log formatter.

    Guarantees schema-stable output across all services using this library.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_event = self._build_log_event(record)
        return json.dumps(log_event, separators=(",", ":"), default=str)
    
    def _build_log_event(self, record: logging.LogRecord) -> Dict[str, Any]:
        """
        Construct structured log payload.
        """

        metadata = getattr(record, "metadata", {})

        trace_state = TraceContext.get()

        return {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "service": ServiceContext.service_name(),
            "environment": ServiceContext.environment(),
            "trace_id": (
                getattr(record, "trace_id", None)
                or (trace_state.trace_id if trace_state else None)
            ),
            "parent_trace_id": (
                trace_state.parent_span_id if trace_state else None
            ),
            "correlation_id": (
                trace_state.correlation_id if trace_state else None
            ),
            "pipeline_stage": (
                trace_state.pipeline_stage if trace_state else None
            ),
            "event_type": getattr(record, "event_type", "log.event"),
            "message": record.getMessage(),
            "metadata": metadata,
        }