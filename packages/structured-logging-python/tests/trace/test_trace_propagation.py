import pytest

from structured_logging.trace.trace_context import TraceContext
from structured_logging.trace.trace_propagation import (
    TracePropagation,
    TRACE_HEADER_TRACE_ID,
    TRACE_HEADER_SPAN_ID,
    TRACE_HEADER_PARENT_ID,
    TRACE_HEADER_CORRELATION_ID,
    TRACE_HEADER_PIPELINE_STAGE,
)


@pytest.fixture(autouse=True)
def clear_trace_context():
    """
    Ensure clean trace context before and after every test.
    """
    TraceContext.clear()
    yield
    TraceContext.clear()


# ------------------------------------------------------------------
# INJECT HEADERS
# ------------------------------------------------------------------


def test_inject_headers_with_active_trace():
    TraceContext.start_trace(
        correlation_id="corr-123",
        pipeline_stage="ingest",
    )

    headers = {}

    result = TracePropagation.inject_headers(headers)

    state = TraceContext.get()

    assert result[TRACE_HEADER_TRACE_ID] == state.trace_id
    assert result[TRACE_HEADER_SPAN_ID] == state.span_id
    assert result[TRACE_HEADER_CORRELATION_ID] == "corr-123"
    assert result[TRACE_HEADER_PIPELINE_STAGE] == "ingest"


def test_inject_headers_without_trace_context():
    headers = {}

    result = TracePropagation.inject_headers(headers)

    assert result == {}


def test_inject_headers_with_parent_span():
    root = TraceContext.start_trace()
    child = TraceContext.child_trace()

    headers = {}

    result = TracePropagation.inject_headers(headers)

    assert result[TRACE_HEADER_TRACE_ID] == child.trace_id
    assert result[TRACE_HEADER_SPAN_ID] == child.span_id
    assert result[TRACE_HEADER_PARENT_ID] == root.span_id


# ------------------------------------------------------------------
# EXTRACT HEADERS
# ------------------------------------------------------------------


def test_extract_headers_creates_trace_context():
    headers = {
        TRACE_HEADER_TRACE_ID: "trace-abc",
        TRACE_HEADER_SPAN_ID: "span-parent",
        TRACE_HEADER_CORRELATION_ID: "corr-xyz",
        TRACE_HEADER_PIPELINE_STAGE: "parse",
    }

    TracePropagation.extract_headers(headers)

    state = TraceContext.get()

    assert state is not None
    assert state.trace_id == "trace-abc"
    assert state.parent_span_id == "span-parent"
    assert state.correlation_id == "corr-xyz"
    assert state.pipeline_stage == "parse"


def test_extract_headers_without_trace_id_does_nothing():
    headers = {
        TRACE_HEADER_SPAN_ID: "span-parent",
    }

    TracePropagation.extract_headers(headers)

    assert TraceContext.get() is None


def test_extract_headers_generates_new_local_span():
    headers = {
        TRACE_HEADER_TRACE_ID: "trace-123",
        TRACE_HEADER_SPAN_ID: "upstream-span",
    }

    TracePropagation.extract_headers(headers)

    state = TraceContext.get()

    assert state.trace_id == "trace-123"
    assert state.span_id != "upstream-span"
    assert state.parent_span_id == "upstream-span"


# ------------------------------------------------------------------
# ROUNDTRIP PROPAGATION
# ------------------------------------------------------------------


def test_roundtrip_trace_propagation():
    """
    Simulate service-to-service propagation:
    Service A -> headers -> Service B
    """

    TraceContext.start_trace(
        correlation_id="corr-roundtrip",
        pipeline_stage="feature-gen",
    )

    outbound_headers = {}
    TracePropagation.inject_headers(outbound_headers)

    TraceContext.clear()

    TracePropagation.extract_headers(outbound_headers)

    state = TraceContext.get()

    assert state is not None
    assert state.trace_id == outbound_headers[TRACE_HEADER_TRACE_ID]
    assert state.parent_span_id == outbound_headers[TRACE_HEADER_SPAN_ID]
    assert state.correlation_id == "corr-roundtrip"
    assert state.pipeline_stage == "feature-gen"