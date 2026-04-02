from __future__ import annotations

import time
from contextlib import ContextDecorator
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, TypeVar

from structured_logging.core.logger import StructuredLogger
from structured_logging.trace.trace_context import TraceContext


F = TypeVar("F", bound=Callable[..., Any])


# Shared module-level logger (prevents handler duplication)
_DEFAULT_LOGGER = StructuredLogger(__name__)


class PipelineLatency(ContextDecorator):
    """
    Context manager and decorator for measuring pipeline-stage latency.

    Emits structured latency telemetry events automatically.

    Example:

        with PipelineLatency("feature_generation"):
            build_features()

    Or:

        @PipelineLatency("dataset_export")
        def export():
            ...
    """

    def __init__(
        self,
        stage: str,
        logger: StructuredLogger | None = None,
        metadata: Dict[str, Any] | None = None,
        event_type: str | None = None,
    ) -> None:

        self.stage = stage
        self.metadata = dict(metadata) if metadata else {}
        self.event_type = event_type or f"pipeline.{stage}.latency"

        self.start_time: float | None = None
        self.start_timestamp: str | None = None

        # Stores previous trace context for restoration
        self._trace_token = None

        self.logger = logger or _DEFAULT_LOGGER

    def __enter__(self) -> "PipelineLatency":
        self.start_time = time.perf_counter()
        self.start_timestamp = datetime.now(timezone.utc).isoformat()

        # Capture current trace context before creating child span
        self._trace_token = TraceContext.get()

        # Enter stage-specific child span
        TraceContext.child_trace(pipeline_stage=self.stage)

        return self

    def __exit__(self, exc_type, exc, exc_tb) -> bool:
        if self.start_time is None:
            return False

        end_time = time.perf_counter()
        end_timestamp = datetime.now(timezone.utc).isoformat()

        duration_ms = round((end_time - self.start_time) * 1000, 3)

        status = "error" if exc else "success"

        payload = {
            "pipeline_stage": self.stage,
            "duration_ms": duration_ms,
            "start_time": self.start_timestamp,
            "end_time": end_timestamp,
            "status": status,
        }

        if exc is not None:
            payload["exception_type"] = exc_type.__name__

        payload.update(self.metadata)

        log_method = self.logger.error if exc else self.logger.info

        log_method(
            message="pipeline stage completed",
            event_type=self.event_type,
            metadata=payload,
        )

        # Restore previous trace context
        TraceContext.restore(self._trace_token)

        return False


def pipeline_latency(
    stage: str,
    logger: StructuredLogger | None = None,
    metadata: Dict[str, Any] | None = None,
) -> PipelineLatency:
    """
    Convenience helper for pipeline latency measurements.

    Example:

        with pipeline_latency("dataset_export"):
            export_dataset()
    """

    return PipelineLatency(
        stage=stage,
        logger=logger,
        metadata=metadata,
    )


def pipeline_latency_decorator(
    stage: str,
    logger: StructuredLogger | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Callable[[F], F]:
    """
    Function decorator for measuring execution latency.

    Example:

        @pipeline_latency_decorator("feature_generation")
        def build_features():
            ...
    """

    def wrapper(func: F) -> F:
        @wraps(func)
        def inner(*args: Any, **kwargs: Any):
            with pipeline_latency(stage, logger, metadata):
                return func(*args, **kwargs)

        return inner  # type: ignore[return-value]

    return wrapper