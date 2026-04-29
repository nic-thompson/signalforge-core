import structured_logging.adapters.lambda_adapter as module_under_test
from structured_logging.adapters.lambda_adapter import lambda_logging_handler
from unittest.mock import MagicMock, patch
import types
 
import pytest

from structured_logging.adapters.lambda_adapter import lambda_logging_handler

from structured_logging.adapters.lambda_adapter import _safe_event_metadata

class DummyContext:
    function_name = "test-function"
    aws_request_id = "test-request-id"


def test_cold_start_logged_once():
    """
    Ensure lambda.cold_start is emitted exactly once per container lifecycle.
    """

    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    @lambda_logging_handler(logger=mock_logger)
    def handler(event, context):
        return {"ok": True}

    event = {}

    context = DummyContext()

    # First invocation (should trigger cold start)
    handler(event, context)


    # Second invocation (should NOT trigger cold start)
    handler(event, context)


    # Extract emitted event types
    emitted_event_types = [
        call.kwargs.get("event_type")
        for call in mock_logger.info.call_args_list
    ]

    # Assert cold start emitted exactly once
    assert emitted_event_types.count("lambda.cold_start") == 1


def test_invocation_started_logged_each_call():
    """
    Ensure lambda.invocation.started is emitted on every invocation,
    not only during cold start.
    """

    # Reset module-level cold start state for deterministic behaviour
    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    @lambda_logging_handler(logger=mock_logger, log_event_metadata=True)
    def handler(event, context):
        return {"ok": True}
    
    event = {
        "source": "aws.events",
        "id": "event-id",
    }

    context = DummyContext()

    # Invoked twice
    handler(event, context)
    handler(event, context)

    emitted_event_types = [
        call.kwargs.get("event_type")
        for call in mock_logger.info.call_args_list
    ]

    assert emitted_event_types.count("lambda.invocation.started") == 2


def test_invocation_completed_logged():
    """
    Ensure lambda.invocation.completed is emitted once per invocation
    with expected metadata fields.
    """

    # Reset cold-start state for deterministic behaviour
    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    @lambda_logging_handler(logger=mock_logger)
    def handler(event, context):
        return {"ok": True}
    
    event = {}
    context = DummyContext()

    handler(event, context)

    completion_calls = [
        call
        for call in mock_logger.info.call_args_list
        if call.kwargs.get("event_type") == "lambda.invocation.completed"
    ]

    assert len(completion_calls) == 1

    metadata = completion_calls[0].kwargs.get("metadata", {})

    assert "duration_ms" in metadata
    assert "function_name" in metadata
    assert "start_time" in metadata

    assert metadata["function_name"] == "test-function"


def test_exception_emits_failure_event():
    """
    Ensure lambda.invocation.failure is emitted when handler raises,
    and the exception is re-raised afer logging.
    """

    # Reset cold-start state for deterministic behaviour
    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    class TestException(Exception):
        pass

    @lambda_logging_handler(logger=mock_logger)
    def handler(event, context):
        raise TestException("boom")
    
    event = {}
    context = DummyContext()

    with pytest.raises(TestException):
        handler(event, context)

    # Verify emit_error called exaclty once
    assert mock_logger.emit_error.call_count == 1

    call = mock_logger.emit_error.call_args

    assert call.kwargs["event_type"] == "lambda.invocation.failure"
    assert call.kwargs["error_code"] == "LAMBDA_UNHANDLED_EXCEPTION"

    metadata = call.kwargs["metadata"]

    assert metadata["function_name"] == "test-function"
    assert metadata["exception_type"] == "TestException"
    assert "duration_ms" in metadata


def test_trace_started_when_missing():
    """
    Ensure TraceContext.start_trace() is called when no existing trace
    content is present.
    """

    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    with patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.get",
        return_value=None,
    ) as mock_get, patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.start_trace"
    ) as mock_start_trace, patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.child_trace"
    ) as mock_child_trace:
        
        @lambda_logging_handler(logger=mock_logger)
        def handler(event, context):
            return {"ok": True}
        
        handler({}, DummyContext())

        mock_get.assert_called_once()
        mock_start_trace.assert_called_once_with(
            pipeline_stage="lambda_invocation"
        )

        mock_child_trace.assert_not_called()


def test_child_trace_when_present():
    """
    Ensure TraceContext.child_trace() is called when an existing trace
    context is already present.
    """

    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    with patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.get",
        return_value=object(), # Simulate existing trace context
    ) as mock_get, patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.start_trace"
    ) as mock_start_trace, patch(
        "structured_logging.adapters.lambda_adapter.TraceContext.child_trace"
    ) as mock_child_trace:
        
        @lambda_logging_handler(logger=mock_logger)
        def handler(event, context):
            return {"ok": True}
        
        handler({}, DummyContext)

        mock_get.assert_called_once()
        mock_child_trace.assert_called_once_with(
            pipeline_stage="lambda_invocation"
        )
        mock_start_trace.assert_not_called


def test_trace_propagation_attempt_for_dict_event():
    """
    Ensure TracePropagation.extract_headers() is called when the
    Lambda event payload is a dictionary.
    """

    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    with patch(
        "structured_logging.adapters.lambda_adapter.TracePropagation.extract_headers"
    ) as mock_extract_headers:
        
        @lambda_logging_handler(logger=mock_logger)
        def handler(event, context):
            return {"ok": True}
        
        event = {"headers": {"x-trace-id": "abc"}}
        handler(event, DummyContext())

        mock_extract_headers.assert_called_once_with(event)


def test_trace_propagation_not_attempted_for_non_dict_event():
    """
    Ensure TracePropagation.extract_headers() is NOT called when the
    Lambda event is not a dictionary.
    """

    module_under_test._COLD_START = True

    mock_logger = MagicMock()

    with patch(
        "structured_logging.adapters.lambda_adapter.TracePropagation.extract_headers"
    ) as mock_axctract_headers:
        
        @lambda_logging_handler(logger=mock_logger)
        def handler(event, context):
            return("ok", True)

        event = "not-a-dict-event"

        handler(event, DummyContext())

        mock_axctract_headers.assert_not_called()


def test_safe_event_metadata_filters_payload():
    """
    Ensure only whitelisted metadata keys are extracted from the event payload.
    """

    event = {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "id": "event-id",
        "time": "2026-04-10T12:00:00Z",
        "region": "eu-west-2",
        "account": "123456789012",
        # Unsafe / unexpected keys
        "password": "secret",
        "token": "abc123",
        "payload": {"sensitive": True}
    }

    metadata = _safe_event_metadata(event)

    assert metadata == {
        "source": "aws.events",
        "detail-type": "Scheduled Event",
        "id": "event-id",
        "time": "2026-04-10T12:00:00Z",
        "region": "eu-west-2",
        "account": "123456789012",
    }

    # Explicitly verify sensitivie fields were exluded
    assert "password" not in metadata
    assert "token" not in metadata
    assert "payload" not in metadata