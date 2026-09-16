from __future__ import annotations

import logging
import os
from typing import Any, Mapping

from structured_logging.core.formatter import StructuredJSONFormatter
from structured_logging.schema.log_event_schema import StructuredError, LogLevel


class StructuredLogger:
    """
    Production structured logger wrapper.

    Provides schema-compliant structured logging across services.
    """

    def __init__(self, name: str) -> None:
        self.logger = logging.getLogger(name)
        self.configure_logger()

    def configure_logger(self) -> None:
        """
        Configure logging backend.
        """

        # Ensure StructuredJSONFormatter exists exactly once
        if any(
            isinstance(h.formatter, StructuredJSONFormatter)
            for h in self.logger.handlers
        ):
            return

        log_level = getattr(
            logging,
            os.getenv("LOG_LEVEL", "INFO").upper(),
            logging.INFO,
        )

        handler = logging.StreamHandler()
        handler.setFormatter(StructuredJSONFormatter())

        self.logger.setLevel(log_level)
        self.logger.addHandler(handler)
        self.logger.propagate = False

    def log(
        self,
        level: int,
        message: str,
        event_type: str,
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
        error: StructuredError | None = None,
    ) -> None:

        extra = {
            "event_type": event_type,
            "metadata": metadata or {},
            "trace_id": trace_id,
            "error": error,
        }

        self.logger.log(level, message, extra=extra)

    def info(
        self,
        message: str,
        event_type: str = "log.info",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:

        self.log(
            logging.INFO,
            message,
            event_type,
            metadata,
            trace_id,
        )

    def warning(
        self,
        message: str,
        event_type: str = "log.warning",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:

        self.log(
            logging.WARNING,
            message,
            event_type,
            metadata,
            trace_id,
        )

    def error(
        self,
        message: str,
        event_type: str = "log.error",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
        error: StructuredError | None = None,
    ) -> None:

        self.log(
            logging.ERROR,
            message,
            event_type,
            metadata,
            trace_id,
            error,
        )

    def debug(
        self,
        message: str,
        event_type: str = "log.debug",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:

        self.log(
            logging.DEBUG,
            message,
            event_type,
            metadata,
            trace_id,
        )

    def emit_error(
        self,
        error_code: str,
        message: str,
        event_type: str = "log.error",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
        severity: LogLevel | None = None,
        retryable: bool | None = None,
        origin: str | None = None,
        exception_type: str | None = None,
        stack_trace: str | None = None,
    ) -> None:
        """
        Emit a structured error log with a canonical StructuredError payload.

        Convenience wrapper around error() that constructs the StructuredError
        from keyword arguments, so callers do not need to import StructuredError
        directly.
        """

        structured_error = StructuredError(
            error_code=error_code,
            message=message,
            exception_type=exception_type,
            stack_trace=stack_trace,
            severity=severity,
            retryable=retryable,
            origin=origin,
            metadata=metadata,
        )

        self.log(
            logging.ERROR,
            message,
            event_type,
            metadata,
            trace_id,
            structured_error,
        )