from datetime import datetime
import json, logging

from structured_logging.core.formatter import StructuredJSONFormatter


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


def test_defaults_when_optional_fields_missing(service_context):

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord("test", logging.INFO, "", 0, "msg", (), None)

    parsed = json.loads(formatter.format(record))

    assert parsed["trace_id"] is None
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
        "event_type",
        "message",
        "metadata",
    }