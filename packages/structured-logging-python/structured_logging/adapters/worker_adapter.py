from __future__ import annotations

import asyncio
import inspect
import time
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Dict, Optional

from structured_logging.core.logger import StructuredLogger
from structured_logging.trace.trace_context import TraceContext
from structured_logging.trace.trace_propagation import TracePropagation

def worker_logging_handler(
        queue_name: str,
        logger: StructuredLogger | None = None,
) -> Callable:
    """
    Decorator enabling structured telemetry for async/sync worker handlers.

    Features:

    - queue trace restoration 
    - execution latency measurement
    - retry observability 
    - failure classification 
    - lifecycle logging
    """

    logger = logger or StructuredLogger("worker-service")

    def decorator(handler: Callable):

        if inspect.iscoroutinefunction(handler):

            @wraps(handler)
            async def async_wrapper(message: Dict[str, Any], *args, **kwargs):

                start_time = time.perf_counter()
                start_timestamp = datetime.now(timezone.utc).isoformat()

                # Restore upstream trace context
                if isinstance(message, dict):
                    TracePropagation.extract_headers(message)

                # Initialise trace if missing
                if TraceContext.get() is None:
                    TraceContext.start_trace(
                        pipeline_stage="worker_execution"
                    )
                else:
                    TraceContext.child_trace(
                        pipeline_stage="worker_execution"
                    )


                logger.info(
                    message="Worker execution started",
                    event_type="worker.execution.started",
                    metadata={
                        "queue_name": queue_name,
                        "start_time": start_timestamp,
                    },
                )

                try:

                    result = await handler(message, *args, **kwargs)

                except asyncio.CancelledError:
                    
                    duration_ms = round(
                        (time.perf_counter() - start_time) * 1000,
                        3,
                    )

                    logger.info(
                        message="Worker execution cancelled",
                        event_type="worker.execution.cancelled",
                        metadata={
                            "queue_name": queue_name,
                            "duration_ms": duration_ms,    
                        },
                    )

                    raise

                except Exception as exc:

                    duration_ms = round(
                        (time.perf_counter() - start_time) * 1000,
                        3,
                    )

                    retry_count = _extract_retry_count(message)

                    logger.emit_error(
                        error_code="WORKER_EXECUTION_FAILED",
                        message=str(exc),
                        event_type="worker.execution.failure",
                        metadata={
                            "queue_name": queue_name,
                            "duration_ms": duration_ms,
                            "retry_count": retry_count,
                            "exception_type": type(exc).__name__
                        },
                    )


                    raise 

                finally: 

                    TraceContext.end_trace()

                duration_ms = round(
                    (time.perf_counter() - start_time) * 1000,
                    3,
                )

                logger.info(
                    message="Worker execution completed",
                    event_type="worker.execution.completed",
                    metadata={
                        "queue_name": queue_name,
                        "duration_ms": duration_ms,
                    },
                )

                return result
            
            return async_wrapper
    
        else:

            @wraps(handler)
            def sync_wrapper(message: Dict[str, Any], *args, **kwargs):

                start_time = time.perf_counter()
                start_timestamp = datetime.now(timezone.utc).isoformat()

                if isinstance(message, dict):
                    TracePropagation.extract_headers(message)

                if TraceContext.get() is None:
                    TraceContext.start_trace(
                        pipeline_stage="worker_execution" 
                    )
                else:
                    TraceContext.child_trace(
                        pipeline_stage="worker_execution"
                    )

                logger.info(
                    message="Worker execution started",
                    event_type="worker.execution.started",
                    metadata={
                        "queue_name": queue_name,
                        "start_time": start_timestamp,
                    },
                )

                try: 

                    result = handler(message, *args, **kwargs)

                except Exception as exc:

                    duration_ms = round(
                        (time.perf_counter() - start_time) * 1000,
                        3,
                    )

                    retry_count = _extract_retry_count(message)

                    logger.emit_error(
                        error_code="WORKER_EXECUTION_FAILED",
                        message=str(exc),
                        event_type="worker.execution.failure",
                        metadata={
                            "queue_name": queue_name,
                            "duration_ms": duration_ms,
                            "retry_count": retry_count,
                            "exception_type": type(exc).__name__,
                        },
                    )

                    raise

                finally: 

                    TraceContext.end_trace()
                
                duration_ms = round(
                    (time.perf_counter() - start_time) * 1000,
                    3,
                )

                logger.info(
                    message="Worker execution completed",
                    event_type="worker.execution.completed",
                    metadata={
                        "queue_name": queue_name,
                        "duration_ms": duration_ms,
                    },
                )

                return result
            
            return sync_wrapper
        
    return decorator


def log_dead_letter_event(
        message: Dict[str, Any],
        queue_name: str,
        logger: StructuredLogger | None = None,
) -> None:
    """
    Emit structured dead-letter queue telemetry event.
    """

    logger = logger or StructuredLogger("worker-service")

    TracePropagation.extract_headers(message)

    logger.emit_error(
        error_code="MESSAGE_SENT_TO_DLQ",
        message="Message routed to dead-letter queue",
        event_type="worker.dead_letter",
        metadata={
            "queue_name": queue_name,
            "message_id": message.get("id"),
        },
    )


def log_retry_event(
        message: Dict[str, Any],
        queue_name: str,
        logger: StructuredLogger | None = None,
) -> None:
    """
    Emit retry telemetry event for worker reprocessing.
    """

    logger = logger or StructuredLogger("worker-service")

    retry_count = _extract_retry_count(message)

    logger.info(
        message="Worker retry scheduled",
        event_type="worker.retry",
        metadata={
            "queue_name": queue_name,
            "retry_count": retry_count,
            "message_id": message.get("id"),
        },
    )


def _extract_retry_count(message) -> int:
    """
    Attempt to extract retry count from metadata.

    Compatible with:

    - SQS ApproximateReceivedCount
    - custom retry envelopes
    """

    if not isinstance(message, dict):
        return 0

    attributes = message.get("attributes", {})

    if isinstance(attributes, dict):
        retry_value = attributes.get("ApproximateReceiveCount")

        if retry_value is not None:
            try:
                return int(retry_value)
            except (ValueError, TypeError):
                pass

    return 0
