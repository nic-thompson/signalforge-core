from datetime import datetime
import json
import logging

import pytest

from structured_logging.core.formatter import StructuredJSONFormatter
from structured_logging.trace.trace_context import TraceContext


@pytest.fixture(autouse=True)
def clear_trace_context():
    """
    Ensure TraceContext does not leak between tests.
    """
    TraceContext.clear()
    yield
    TraceContext.clear()


# ------------------------------------------------------------------
# BASIC FORMATTER STRUCTURE
# ------------------------------------------------------------------


def test_formatter_outputs_valid_json(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="hello",
        args=(),
        exc_info=None,
    )

    result = formatter.format(record)
    parsed = json.loads(result)

    assert parsed["message"] == "hello"
    assert parsed["service"] == "test-service"
    assert parsed["environment"] == "test"


def test_required_fields_present(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    parsed = json.loads(formatter.format(record))

    assert "timestamp" in parsed
    assert "level" in parsed
    assert "service" in parsed
    assert "environment" in parsed
    assert "event_type" in parsed
    assert "metadata" in parsed
    assert "trace_id" in parsed
    assert "parent_trace_id" in parsed
    assert "correlation_id" in parsed
    assert "pipeline_stage" in parsed


def test_defaults_when_optional_fields_missing(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] is None
    assert parsed["parent_trace_id"] is None
    assert parsed["correlation_id"] is None
    assert parsed["pipeline_stage"] is None
    assert parsed["event_type"] == "log.event"
    assert parsed["metadata"] == {}


def test_metadata_passthrough(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    record.metadata = {"user_id": 42}

    parsed = json.loads(formatter.format(record))

    assert parsed["metadata"]["user_id"] == 42


def test_event_fields_override_defaults(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)
    record.trace_id = "abc123"
    record.event_type = "auth.login"

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] == "abc123"
    assert parsed["event_type"] == "auth.login"


def test_timestamp_is_isoformat(service_context):

    formatter = StructuredJSONFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    parsed = json.loads(formatter.format(record))

    datetime.fromisoformat(parsed["timestamp"])
    assert parsed["timestamp"].endswith("+00:00")


def test_schema_is_stable(service_context):

    formatter = StructuredJSONFormatter()
    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    parsed = json.loads(formatter.format(record))

    assert set(parsed.keys()) == {
        "timestamp",
        "level",
        "service",
        "environment",
        "trace_id",
        "parent_trace_id",
        "correlation_id",
        "pipeline_stage",
        "event_type",
        "message",
        "metadata",
    }


# ------------------------------------------------------------------
# TRACE CONTEXT INTEGRATION
# ------------------------------------------------------------------


def test_formatter_injects_trace_context(service_context):

    TraceContext.start_trace(
        correlation_id="corr-123",
        pipeline_stage="feature-gen",
    )

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        10,
        "trace-aware log",
        (),
        None,
    )

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] is not None
    assert parsed["correlation_id"] == "corr-123"
    assert parsed["pipeline_stage"] == "feature-gen"


def test_formatter_respects_record_trace_override(service_context):

    TraceContext.start_trace()

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        10,
        "override log",
        (),
        None,
    )

    record.trace_id = "manual-trace-id"

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] == "manual-trace-id"


def test_formatter_uses_child_span(service_context):

    root = TraceContext.start_trace()
    child = TraceContext.child_trace()

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        10,
        "child span log",
        (),
        None,
    )

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] == child.trace_id


def test_formatter_includes_parent_span(service_context):

    root = TraceContext.start_trace()
    TraceContext.child_trace()

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        10,
        "child event",
        (),
        None,
    )

    parsed = json.loads(formatter.format(record))

    assert parsed["parent_trace_id"] == root.span_id


def test_formatter_propagates_correlation_id(service_context):

    TraceContext.start_trace(correlation_id="dataset-run-42")

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "trainer",
        logging.INFO,
        __file__,
        10,
        "epoch start",
        (),
        None,
    )

    parsed = json.loads(formatter.format(record))

    assert parsed["correlation_id"] == "dataset-run-42"


def test_formatter_includes_pipeline_stage(service_context):

    TraceContext.start_trace(pipeline_stage="training")

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        "trainer",
        logging.INFO,
        __file__,
        10,
        "epoch start",
        (),
        None,
    )

    parsed = json.loads(formatter.format(record))

    assert parsed["pipeline_stage"] == "training"