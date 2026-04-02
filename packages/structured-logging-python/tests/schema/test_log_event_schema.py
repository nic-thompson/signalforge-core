import json
import logging
from datetime import datetime, timezone
from dataclasses import FrozenInstanceError

import pytest

from structured_logging.schema.log_event_schema import (
    LogEventSchema,
    StructuredError,
    SCHEMA_VERSION,
)

from structured_logging.core.formatter import StructuredJSONFormatter
from structured_logging.core.logger import StructuredLogger


# --------------------------------------------------
# Schema unit tests
# --------------------------------------------------


def test_log_event_schema_creation():

    event = LogEventSchema(
        timestamp=datetime.now(timezone.utc),
        level="INFO",
        service="svc",
        environment="test",
        event_type="log.test",
        message="hello",
    )

    assert event.level == "INFO"
    assert event.service == "svc"
    assert event.schema_version == SCHEMA_VERSION


def test_log_event_schema_metadata_defaults_to_empty_dict():

    event = LogEventSchema(
        timestamp=datetime.now(timezone.utc),
        level="INFO",
        service="svc",
        environment="dev",
        event_type="log.test",
        message="msg",
    )

    assert event.metadata == {}


def test_log_event_schema_is_immutable():

    event = LogEventSchema(
        timestamp=datetime.now(timezone.utc),
        level="INFO",
        service="svc",
        environment="dev",
        event_type="log.test",
        message="msg",
    )

    with pytest.raises(FrozenInstanceError):
        event.level = "ERROR"


def test_structured_error_embeds_correctly():

    error = StructuredError(
        error_code="TEST_ERR",
        message="failure occurred",
        retryable=False,
    )

    event = LogEventSchema(
        timestamp=datetime.now(timezone.utc),
        level="ERROR",
        service="svc",
        environment="prod",
        event_type="pipeline.failure",
        message="failure",
        error=error,
    )

    assert event.error.error_code == "TEST_ERR"
    assert event.error.retryable is False


# --------------------------------------------------
# Formatter integration tests
# --------------------------------------------------


def test_formatter_outputs_schema_compliant_json():

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="formatter message",
        args=(),
        exc_info=None,
    )

    record.event_type = "log.integration"
    record.metadata = {"step": 1}

    output = formatter.format(record)

    parsed = json.loads(output)

    assert parsed["event_type"] == "log.integration"
    assert parsed["metadata"]["step"] == 1
    assert parsed["level"] == "INFO"
    assert parsed["schema_version"] == SCHEMA_VERSION
    assert "timestamp" in parsed


def test_formatter_serializes_structured_error():

    formatter = StructuredJSONFormatter()

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=20,
        msg="failure",
        args=(),
        exc_info=None,
    )

    record.event_type = "pipeline.failure"

    record.error = StructuredError(
        error_code="PIPE_FAIL",
        message="pipeline crashed",
        retryable=True,
    )

    output = formatter.format(record)

    parsed = json.loads(output)

    assert parsed["error"]["error_code"] == "PIPE_FAIL"
    assert parsed["error"]["retryable"] is True


# --------------------------------------------------
# Logger integration tests
# --------------------------------------------------


def test_structured_logger_emits_schema_compliant_event(logger, capture_handler):

    logger.logger.addHandler(capture_handler)

    logger.info(
        "integration test message",
        event_type="log.integration",
        metadata={"stage": "unit-test"},
    )

    record = capture_handler.records[0]

    formatted = logger.logger.handlers[0].formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["event_type"] == "log.integration"
    assert parsed["metadata"]["stage"] == "unit-test"
    assert parsed["level"] == "INFO"

def test_structured_logger_error_supports_structured_error(logger, capture_handler):

    logger.logger.addHandler(capture_handler)

    error = StructuredError(
        error_code="FEATURE_FAIL",
        message="feature pipeline failed",
        retryable=False,
    )

    logger.error(
        "pipeline failure",
        event_type="pipeline.error",
        error=error,
    )

    record = capture_handler.records[0]

    formatted = logger.logger.handlers[0].formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["error"]["error_code"] == "FEATURE_FAIL"
    assert parsed["error"]["retryable"] is False