from __future__ import annotations

import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, Optional, TypeVar, cast

from structured_logging.core.logger import StructuredLogger
from structured_logging.trace.trace_context import TraceContext
from structured_logging.trace.trace_propagation import TracePropagation


# Module-level cold-start detector
_COLD_START = True

# Preserves the decorated handler's own signature through the decorator,
# so a caller keeps the types they wrote rather than having them widened
# to Callable[..., Any] on the way through.
LambdaHandler = TypeVar("LambdaHandler", bound=Callable[..., Any])


def lambda_logging_handler(
    logger: Optional[StructuredLogger] = None,
    log_event_metadata: bool = True,
) -> Callable[[LambdaHandler], LambdaHandler]:
    """
    Decorator enabling structured telemetry for AWS Lambda handlers.

    Features:

    - trace propagation restoration
    - automatic trace initialisation
    - invocation latency measurement
    - structured exception emission
    - optional event metadata logging
    """

    logger = logger or StructuredLogger("lambda-service")

    def decorator(handler: LambdaHandler) -> LambdaHandler:

        @wraps(handler)
        def wrapper(event: Dict[str, Any], context: Any) -> Any:

            global _COLD_START

            start_time = time.perf_counter()
            start_timestamp = datetime.now(timezone.utc).isoformat()

            # Attempt upstream trace restoration
            if isinstance(event, dict):
                TracePropagation.extract_headers(event)

            # Initialise trace context
            if TraceContext.get() is None:
                TraceContext.start_trace(
                    pipeline_stage="lambda_invocation"
                )
            else:
                TraceContext.child_trace(
                    pipeline_stage="lambda_invocation"
                )

            # Cold-start detection
            if _COLD_START:

                logger.info(
                    message="Lambda cold start detected",
                    event_type="lambda.cold_start",
                    metadata={
                        "function_name": getattr(
                            context,
                            "function_name",
                            None,
                        ),
                    },
                )

                _COLD_START = False

            # Optional event metadata logging (safe subset only)
            if log_event_metadata and isinstance(event, dict):

                logger.info(
                    message="Lambda invocation started",
                    event_type="lambda.invocation.started",
                    metadata=_safe_event_metadata(event),
                )

            try:

                result = handler(event, context)

            except Exception as exc:

                duration_ms = round(
                    (time.perf_counter() - start_time) * 1000,
                    3,
                )

                logger.emit_error(
                    error_code="LAMBDA_UNHANDLED_EXCEPTION",
                    message=str(exc),
                    event_type="lambda.invocation.failure",
                    metadata={
                        "duration_ms": duration_ms,
                        "function_name": getattr(
                            context,
                            "function_name",
                            None,
                        ),
                        "exception_type": type(exc).__name__,
                    },
                )

                raise

            duration_ms = round(
                (time.perf_counter() - start_time) * 1000,
                3,
            )

            logger.info(
                message="Lambda invocation completed",
                event_type="lambda.invocation.completed",
                metadata={
                    "duration_ms": duration_ms,
                    "function_name": getattr(
                        context,
                        "function_name",
                        None,
                    ),
                    "start_time": start_timestamp,
                },
            )

            return result

        # functools.wraps preserves the handler's identity at runtime but
        # not its signature for a type checker, so the cast is what tells
        # mypy the decorator returns what it was given.
        return cast(LambdaHandler, wrapper)

    return decorator


def _safe_event_metadata(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract safe metadata from Lambda event payload.

    Avoid logging full payloads to prevent:

    - PII leakage
    - oversized logs
    - schema instability
    """

    safe_keys = [
        "source",
        "detail-type",
        "id",
        "time",
        "region",
        "account",
    ]

    return {
        key: event[key]
        for key in safe_keys
        if key in event
    }