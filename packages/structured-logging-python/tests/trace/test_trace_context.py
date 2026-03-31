import pytest
import anyio

from structured_logging.trace.trace_context import TraceContext


@pytest.mark.anyio
async def test_async_context_is_isolated():
    TraceContext.start_trace(
        correlation_id="job-123",
        pipeline_stage="ingest"
    )

    async def worker():
        state = TraceContext.get()
        return state.pipeline_stage if state else None

    result = await worker()

    assert result == "ingest"


@pytest.mark.anyio
async def test_async_context_propagates_across_tasks():
    TraceContext.start_trace(pipeline_stage="ingest")

    async def worker():
        state = TraceContext.get()
        return state.pipeline_stage if state else None

    result_holder = {}

    async with anyio.create_task_group() as tg:

        async def run():
            result_holder["value"] = await worker()

        tg.start_soon(run)

    assert result_holder["value"] == "ingest"


def test_start_trace_sets_context():
    state = TraceContext.start_trace(
        correlation_id="job-123",
        pipeline_stage="ingest"
    )

    current = TraceContext.get()

    assert current is not None
    assert current.trace_id == state.trace_id
    assert current.span_id == state.span_id
    assert current.parent_span_id is None
    assert current.correlation_id == "job-123"
    assert current.pipeline_stage == "ingest"


def test_child_trace_inherits_parent():
    root = TraceContext.start_trace(
        correlation_id="job-123",
        pipeline_stage="ingest"
    )

    child = TraceContext.child_trace("feature_engineering")

    assert child.trace_id == root.trace_id
    assert child.parent_span_id == root.span_id
    assert child.span_id != root.span_id
    assert child.pipeline_stage == "feature_engineering"


def test_child_trace_without_existing_trace_creates_new():
    TraceContext.clear()

    child = TraceContext.child_trace("parse")

    assert child.trace_id is not None
    assert child.span_id is not None
    assert child.parent_span_id is None
    assert child.pipeline_stage == "parse"


def test_get_returns_none_when_not_set():
    TraceContext.clear()

    assert TraceContext.get() is None


def test_clear_resets_context():
    TraceContext.start_trace()

    TraceContext.clear()

    assert TraceContext.get() is None


def test_generate_trace_id_returns_string():
    trace_id = TraceContext.generate_trace_id()

    assert isinstance(trace_id, str)
    assert len(trace_id) > 0


def test_generate_span_id_returns_string():
    span_id = TraceContext.generate_span_id()

    assert isinstance(span_id, str)
    assert len(span_id) > 0


def test_span_chain_structure():
    root = TraceContext.start_trace("trace-1", pipeline_stage="ingest")

    stage_2 = TraceContext.child_trace("parse")
    stage_3 = TraceContext.child_trace("features")

    assert stage_2.parent_span_id == root.span_id
    assert stage_3.parent_span_id == stage_2.span_id
    assert stage_3.trace_id == root.trace_id


def test_span_context_manager_restores_previous_state():
    root = TraceContext.start_trace(pipeline_stage="ingest")

    with TraceContext.span("features") as span:
        current = TraceContext.get()
        assert current.span_id == span.span_id
        assert current.parent_span_id == root.span_id

    restored = TraceContext.get()

    assert restored.span_id == root.span_id
    assert restored.parent_span_id is None


def test_span_context_manager_creates_root_if_missing():
    TraceContext.clear()

    with TraceContext.span("ingest") as span:
        current = TraceContext.get()

        assert current is not None
        assert current.span_id == span.span_id
        assert current.parent_span_id is None