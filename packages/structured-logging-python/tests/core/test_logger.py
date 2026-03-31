import logging
import os

def test_logger_initialises_without_errors(logger):
    assert isinstance(logger.logger, logging.Logger)


def test_configure_logger_adds_stream_handler(logger):
    handler = logger.logger.handlers
    assert any(isinstance(h, logging.StreamHandler) for h in handler)


def test_logger_does_not_duplicate_handlers(logger):

    initial_handler_count = len(logger.logger.handlers)

    from structured_logging.core.logger import StructuredLogger
    StructuredLogger("test.logger")
    
    assert len(logger.logger.handlers) == initial_handler_count


def test_log_emits_structured_fields(logger, capture_handler):
    
    logger.logger.handlers = [capture_handler]

    logger.log(
        logging.INFO,
        "Hello",
        event_type="test_event",
        metadata={"key": "value"},
        trace_id="12345",
    )

    log_record = capture_handler.records[0]

    assert log_record.msg == "Hello"
    assert log_record.event_type == "test_event"
    assert log_record.metadata == {"key": "value"}
    assert log_record.trace_id == "12345"


def test_info_sets_defaults(logger, capture_handler):
    
    logger.logger.handlers = [capture_handler]

    logger.info("Info message")

    log_record = capture_handler.records[0]

    assert log_record.msg == "Info message"
    assert log_record.event_type == "log.info"
    assert log_record.metadata == {}
    assert log_record.trace_id is None  


def test_warning_sets_defaults(logger, capture_handler):
    
    logger.logger.handlers = [capture_handler]

    logger.warning("Warning message")

    log_record = capture_handler.records[0]

    assert log_record.msg == "Warning message"
    assert log_record.levelno == logging.WARNING
    assert log_record.event_type == "log.warning"
    assert log_record.metadata == {}
    assert log_record.trace_id is None


def test_error_sets_defaults(logger, capture_handler):
    
    logger.logger.handlers = [capture_handler]

    logger.error("Error message")

    log_record = capture_handler.records[0]

    assert log_record.msg == "Error message"
    assert log_record.levelno == logging.ERROR
    assert log_record.event_type == "log.error"
    assert log_record.metadata == {}
    assert log_record.trace_id is None


def test_debug_sets_defaults(logger, capture_handler):
    
    logger.logger.setLevel(logging.DEBUG)

    logger.logger.handlers = [capture_handler]

    logger.debug("Debug message")

    log_record = capture_handler.records[0]

    assert log_record.msg == "Debug message"
    assert log_record.levelno == logging.DEBUG
    assert log_record.event_type == "log.debug"
    assert log_record.metadata == {}
    assert log_record.trace_id is None