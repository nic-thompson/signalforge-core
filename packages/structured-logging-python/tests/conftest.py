import logging
import pytest

from structured_logging.core.context import ServiceContext
from structured_logging.core.logger import StructuredLogger

class DummyHandler(logging.Handler):
    """
    Captures emitted LogRecord objects for assertions in tests
    without writing to stdout/stderr.
    """
    
    def __init__(self):
        super().__init__()
        self.records = []

    def emit(self, record):
        self.records.append(record)

@pytest.fixture(autouse=True)
def reset_service_context():
    """
    Reset service context before each test.

    ServiceContext is intentionally immutable after initialisation
    in production, but tests must isolate state between runs.
    """

    ServiceContext._service_name = None
    ServiceContext._environment = None
    ServiceContext._initialised = False

    yield


@pytest.fixture
def service_context():
    """
    Provide an initialised ServiceContext for tests that need it.
    """

    ServiceContext.initialise(
        service_name="test-service",
        environment="test"
    )

    return ServiceContext


@pytest.fixture
def logger(service_context):
    """
    Provide a configured StructuredLogger instance.

    Depends on service_context so logs always contain
    valid service/environment metadata.
    """

    return StructuredLogger("test.logger")

@pytest.fixture
def capture_handler():
    """
    Provide a handler that captures emitted LogRecord objects for assertions in tests.
    """
    return DummyHandler()

@pytest.fixture(autouse=True)
def clean_root_logger():
    """
    Prevent handler leakage between tests by clearing handlers and resetting log level on the root logger.

    Ensures that each test starts with a clean logging configuration, 
    avoiding unintended side effects from previous tests, staying deterministic.
    """

    logging.getLogger().handlers.clear()
    logging.getLogger().setLevel(logging.NOTSET)

    yield

    logging.getLogger().handlers.clear()