from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Callable, Awaitable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from structured_logging.core.logger import StructuredLogger
from structured_logging.trace.trace_context import TraceContext
from structured_logging.trace.trace_propagation import TracePropagation


class FastAPILoggingMiddleware(BaseHTTPMiddleware):
    """
    FastAPI middleware enabling structured request telemetry.

    Features:

    - trace context propagation
    - automatic request trace initialisation
    - latency measurement
    - structured exception logging
    - response status logging
    """

    def __init__(
        self,
        app: ASGIApp,
        logger: StructuredLogger | None = None,
    ):
        super().__init__(app)
        self.logger = logger or StructuredLogger("fastapi-service")

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:

        start_time = time.perf_counter()
        start_timestamp = datetime.now(timezone.utc).isoformat()

        # Restore upstream trace if present
        TracePropagation.extract_headers(request.headers)

        # Initialise trace if not already present
        if TraceContext.get() is None:
            TraceContext.start_trace(
                pipeline_stage="api_request"
            )
        else:
            TraceContext.child_trace(
                pipeline_stage="api_request"
            )

        try:
            response = await call_next(request)

        except Exception as exc:

            duration_ms = round(
                (time.perf_counter() - start_time) * 1000,
                3,
            )

            self.logger.emit_error(
                error_code="FASTAPI_UNHANDLED_EXCEPTION",
                message=str(exc),
                event_type="api.request.failure",
                metadata={
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": duration_ms,
                    "start_time": start_timestamp,
                },
            )

            raise

        else:

            duration_ms = round(
                (time.perf_counter() - start_time) * 1000,
                3,
            )

            self.logger.info(
                message="API request completed",
                event_type="api.response",
                metadata={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": response.status_code,
                    "duration_ms": duration_ms,
                    "start_time": start_timestamp,
                },
            )

            return response

        finally:
            TraceContext.end_trace()


def configure_fastapi_logging(
    app: ASGIApp,
    logger: StructuredLogger | None = None,
) -> None:
    """
    Attach structured logging middleware to FastAPI app.

    Example:

        configure_fastapi_logging(app)
    """

    app.add_middleware(
        FastAPILoggingMiddleware,
        logger=logger,
    )