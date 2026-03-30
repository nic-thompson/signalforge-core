from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from structured_logging.core.context import ServiceContext

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

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "service": ServiceContext.service_name(),
            "environment": ServiceContext.environment(),
            "trace_id": getattr(record, "trace_id", None),
            "event_type": getattr(record, "event_type", "log.event"),
            "message": record.getMessage(),
            "metadata": metadata,
        }