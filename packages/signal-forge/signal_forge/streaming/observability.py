"""
signal_forge.streaming.observability

Thin observability facade that wraps ``structured-logging-python`` when it
is available and degrades cleanly to the stdlib ``logging`` module when it
is not.

Why a facade?
-------------

The streaming layer must always emit structured logs with ``trace_id``
propagation, but unit tests should not require the upstream
``structured-logging-python`` package to be installed. The facade lets the
production runtime use the canonical logger while tests run against an
identical interface backed by stdlib.

The facade is intentionally tiny — it exposes only the methods the
streaming layer actually uses: ``info``, ``warning``, ``error``. Anything
more belongs in the upstream library itself.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol, cast


class StructuredLoggerLike(Protocol):
    """Minimum logging surface used by signal-forge streaming components."""

    def info(
        self,
        message: str,
        event_type: str = ...,
        metadata: Mapping[str, Any] | None = ...,
        trace_id: str | None = ...,
    ) -> None: ...

    def warning(
        self,
        message: str,
        event_type: str = ...,
        metadata: Mapping[str, Any] | None = ...,
        trace_id: str | None = ...,
    ) -> None: ...

    def error(
        self,
        message: str,
        event_type: str = ...,
        metadata: Mapping[str, Any] | None = ...,
        trace_id: str | None = ...,
    ) -> None: ...


class _StdlibFallbackLogger:
    """
    Fallback logger used when ``structured_logging`` is not importable.

    Emits the same conceptual fields as the upstream logger but via the
    stdlib ``logging`` backend. Sufficient for local development and unit
    tests; production deployments should always have the upstream package
    installed.
    """

    def __init__(self, name: str) -> None:
        self._logger = logging.getLogger(name)

    def _emit(
        self,
        level: int,
        message: str,
        event_type: str,
        metadata: Mapping[str, Any] | None,
        trace_id: str | None,
    ) -> None:
        extra = {
            "event_type": event_type,
            "metadata": dict(metadata) if metadata else {},
            "trace_id": trace_id,
        }
        self._logger.log(level, message, extra=extra)

    def info(
        self,
        message: str,
        event_type: str = "log.info",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self._emit(logging.INFO, message, event_type, metadata, trace_id)

    def warning(
        self,
        message: str,
        event_type: str = "log.warning",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self._emit(logging.WARNING, message, event_type, metadata, trace_id)

    def error(
        self,
        message: str,
        event_type: str = "log.error",
        metadata: Mapping[str, Any] | None = None,
        trace_id: str | None = None,
    ) -> None:
        self._emit(logging.ERROR, message, event_type, metadata, trace_id)


def get_logger(name: str) -> StructuredLoggerLike:
    """
    Return a structured logger.

    Prefers the upstream ``structured-logging-python`` ``StructuredLogger``
    when importable; falls back to a stdlib-backed implementation that
    exposes the same surface otherwise.
    """

    try:
        from structured_logging.core.logger import StructuredLogger
    except ImportError:
        return _StdlibFallbackLogger(name)

    return cast(StructuredLoggerLike, StructuredLogger(name))
