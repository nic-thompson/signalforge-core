import pytest
from unittest.mock import MagicMock, ANY

from fastapi import Request, Response

from structured_logging.adapters.fastapi_adapter import (
    FastAPILoggingMiddleware,
    configure_fastapi_logging
)


@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def mock_trace_context(monkeypatch):
    mock = MagicMock()

    monkeypatch.setattr(
        "structured_logging.adapters.fastapi_adapter.TraceContext",
        mock,
    )

    return mock


@pytest.fixture
def mock_trace_propagation(monkeypatch):
    mock = MagicMock()

    monkeypatch.setattr(
        "structured_logging.adapters.fastapi_adapter.TracePropagation",
        mock,
    )

    return mock


@pytest.fixture
def mock_request():
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/items",
        "headers": [],
    }

    return Request(scope)


@pytest.fixture
def mock_response():
    return Response(status_code=200)


@pytest.fixture
def call_next(mock_response):

    async def _call_next(request):
        return mock_response
    
    return _call_next


@pytest.mark.anyio
async def test_success_response_logging(
    mock_request,
    call_next,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation
):
    
    mock_trace_context.get.return_value = None

    middleware = FastAPILoggingMiddleware(
        app=lambda scope, recieve, send: None,
        logger=mock_logger,
    )

    response = await middleware.dispatch(
        mock_request,
        call_next
    )

    assert response.status_code == 200

    mock_logger.info.assert_called_once()

    mock_trace_context.start_trace.assert_called_once()

    mock_trace_context.end_trace.assert_called_once()


@pytest.mark.anyio
async def test_child_trace_created_if_existing_trace(
    mock_request,
    call_next,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = object()

    middleware = FastAPILoggingMiddleware(
        app=lambda scope, receive, send: None,
        logger=mock_logger,
    )

    await middleware.dispatch(mock_request, call_next)

    mock_trace_context.child_trace.assert_called_once()


@pytest.mark.anyio
async def test_trace_headers_extracted(
    mock_request,
    call_next,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    middleware = FastAPILoggingMiddleware(
        app=lambda scope, receive, send: None,
        logger=mock_logger,
    )

    await middleware.dispatch(mock_request, call_next)

    mock_trace_propagation.extract_headers.assert_called_once()


@pytest.mark.anyio
async def test_exception_logging(
    mock_request,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    async def failing_call_next(request):
        raise RuntimeError("boom")
    
    middleware = FastAPILoggingMiddleware(
        app=lambda scope, receive, send: None,
        logger=mock_logger,
    )

    with pytest.raises(RuntimeError):
        await middleware.dispatch(
            mock_request,
            failing_call_next,
        )

    mock_logger.emit_error.assert_called_once()

    mock_trace_context.end_trace_assert_called_once()


@pytest.mark.anyio
async def test_trace_end_always_called(
    mock_request,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    async def failing_call_next(request):
        raise RuntimeError("boom")

    middleware = FastAPILoggingMiddleware(
        app=lambda scope, receive, send: None,
        logger=mock_logger,
    )

    with pytest.raises(RuntimeError):
        await middleware.dispatch(
            mock_request,
            failing_call_next,
        )

    mock_trace_context.end_trace_assert_called_once()


def test_configure_fastapi_logging(monkeypatch):

    app = MagicMock()

    configure_fastapi_logging(app)

    app.add_middleware.assert_called_once()