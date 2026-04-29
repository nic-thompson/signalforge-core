import asyncio
import pytest
from unittest.mock import MagicMock, ANY

from structured_logging.adapters.worker_adapter import (
    worker_logging_handler,
    log_dead_letter_event,
    log_retry_event,
)

@pytest.fixture
def mock_logger():
    return MagicMock()


@pytest.fixture
def message():
    return {
        "id": "msg-123",
        "attributes": {
            "ApproximateReceiveCount": "3"
        }
    }


@pytest.fixture
def mock_trace_context(monkeypatch):
    mock = MagicMock()

    monkeypatch.setattr(
        "structured_logging.adapters.worker_adapter.TraceContext",
        mock
    )

    return mock


@pytest.fixture
def mock_trace_propagation(monkeypatch):
    mock = MagicMock()

    monkeypatch.setattr(
        "structured_logging.adapters.worker_adapter.TracePropagation",
        mock
    )

    return mock


def test_sync_handler_success(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation
):
    
    mock_trace_context.get.return_value = None

    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        return "ok"
    
    result = handler(message)

    assert result == "ok"

    mock_logger.info.assert_any_call(
        message="Worker execution started",
        event_type="worker.execution.started",
        metadata=ANY,
    )

    mock_trace_context.start_trace.assert_called_once()

    mock_trace_context.end_trace.assert_called_once()


@pytest.mark.anyio
async def test_async_handler_success(
    message,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = None

    @worker_logging_handler("queue-A", logger=mock_logger)
    async def handler(msg):
        return "ok"
    
    result = await handler(message)

    assert result == "ok"

    mock_logger.info.assert_any_call(
        message="Worker execution completed",
        event_type="worker.execution.completed",
        metadata=ANY,
    )


def test_failure_logging(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = None

    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        raise RuntimeError("boom")
    

    with pytest.raises(RuntimeError):
        handler(message)

    mock_logger.emit_error.assert_called_once()


@pytest.mark.anyio
async def test_async_cancellation_logging(
    message,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = None

    @worker_logging_handler("queue-A", logger=mock_logger)
    async def handler(msg):
        raise asyncio.CancelledError()
    
    with pytest.raises(asyncio.CancelledError):
        await handler(message)

    mock_logger.info.assert_any_call(
        message="Worker execution cancelled",
        event_type="worker.execution.cancelled",
        metadata=ANY,
    )


def test_child_trace_created_if_existing_trace(
    message,
    mock_logger,
    mock_trace_context,
    mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = object()

    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        return True
    
    handler(message)

    mock_trace_context.child_trace.assert_called_once()


def test_trace_restoration_called(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation,
):
    
    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        return True
    
    handler(message)

    mock_trace_propagation.extract_headers.assert_called_once_with(message)


def test_duration_metadata_present(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation,
):
    
    mock_trace_context.get.return_value = None

    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        return True
    
    handler(message)

    assert mock_logger.info.call_count >= 2


def test_return_value_preserved(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation,
):
    
    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        return 42
    
    assert handler(message) == 42


def test_exception_preserved(
        message,
        mock_logger,
        mock_trace_context,
        mock_trace_propagation,
):
    @worker_logging_handler("queue-A", logger=mock_logger)
    def handler(msg):
        raise ValueError("fail")

    with pytest.raises(ValueError):
        handler(message)

def test_dead_letter_event(message, mock_logger, monkeypatch):

    monkeypatch.setattr(
        "structured_logging.adapters.worker_adapter.StructuredLogger",
        lambda *_: mock_logger,
    )

    log_dead_letter_event(message, "queue-A")

    mock_logger.emit_error.assert_called_once()


def test_retry_event(message, mock_logger, monkeypatch):

    monkeypatch.setattr(
        "structured_logging.adapters.worker_adapter.StructuredLogger",
        lambda *_: mock_logger,
    )

    log_retry_event(message, "queue-A")

    mock_logger.info.assert_called_once()
