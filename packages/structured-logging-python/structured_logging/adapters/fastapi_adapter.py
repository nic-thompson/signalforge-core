from typing import Callable, Awaitable
from starlette.types import ASGIApp


class FastAPILoggingMiddleware(BaseHTTPMiddleware):

    def __init__(
        self,
        app: ASGIApp,
        logger: StructuredLogger | None = None,
    ):
        super().__init__(app)
        self.logger = logger or StructuredLogger(__name__)

    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:

        start_time = time.perf_counter()
        request_start_time = datetime.now(timezone.utc).isoformat()

        TracePropagation.extract_headers(request.headers)

        if TraceContext.get() is None:
            TraceContext.start_trace(pipeline_stage="api_request")
        else:
            TraceContext.child_trace(pipeline_stage="api_request")

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
                },
            )

            raise

        finally:
            TraceContext.end_trace()

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
                "start_time": request_start_time,
            },
        )

        return response