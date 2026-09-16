import pytest

from structured_logging.metrics.latency import (
    PipelineLatency,
    pipeline_latency,
    pipeline_latency_decorator,
)

from structured_logging.trace.trace_context import TraceContext


# ------------------------------------------------------------------
# Test logger stub
# ------------------------------------------------------------------


class DummyLogger:
    def __init__(self):
        self.info_calls = []
        self.error_calls = []

    def info(self, **kwargs):
        self.info_calls.append(kwargs)

    def error(self, **kwargs):
        self.error_calls.append(kwargs)


# ------------------------------------------------------------------
# SUCCESS PATH
# ------------------------------------------------------------------


def test_pipeline_latency_success_logs_info():
    logger = DummyLogger()

    TraceContext.clear()

    with PipelineLatency("feature_generation", logger=logger):
        pass

    assert len(logger.info_calls) == 1
    assert len(logger.error_calls) == 0

    call = logger.info_calls[0]

    assert call["event_type"] == "pipeline.feature_generation.latency"
    assert call["metadata"]["pipeline_stage"] == "feature_generation"
    assert call["metadata"]["status"] == "success"
    assert "duration_ms" in call["metadata"]


# ------------------------------------------------------------------
# FAILURE PATH
# ------------------------------------------------------------------


def test_pipeline_latency_exception_logs_error():
    logger = DummyLogger()

    TraceContext.clear()

    with pytest.raises(ValueError):
        with PipelineLatency("dataset_export", logger=logger):
            raise ValueError("boom")

    assert len(logger.error_calls) == 1

    metadata = logger.error_calls[0]["metadata"]

    assert metadata["status"] == "error"
    assert metadata["exception_type"] == "ValueError"


# ------------------------------------------------------------------
# METADATA MERGING
# ------------------------------------------------------------------


def test_pipeline_latency_metadata_merge():
    logger = DummyLogger()

    with PipelineLatency(
        "validation",
        logger=logger,
        metadata={"dataset_version": "v5"},
    ):
        pass

    metadata = logger.info_calls[0]["metadata"]

    assert metadata["dataset_version"] == "v5"


# ------------------------------------------------------------------
# EVENT TYPE OVERRIDE
# ------------------------------------------------------------------


def test_pipeline_latency_custom_event_type():
    logger = DummyLogger()

    with PipelineLatency(
        "training",
        logger=logger,
        event_type="custom.latency.event",
    ):
        pass

    assert logger.info_calls[0]["event_type"] == "custom.latency.event"


# ------------------------------------------------------------------
# HELPER FUNCTION WRAPPER
# ------------------------------------------------------------------


def test_pipeline_latency_helper_function():
    logger = DummyLogger()

    with pipeline_latency("dataset_export", logger=logger):
        pass

    assert len(logger.info_calls) == 1


# ------------------------------------------------------------------
# DECORATOR SUCCESS
# ------------------------------------------------------------------


def test_pipeline_latency_decorator_success():
    logger = DummyLogger()

    @pipeline_latency_decorator("feature_build", logger=logger)
    def build():
        return 42

    result = build()

    assert result == 42
    assert len(logger.info_calls) == 1


# ------------------------------------------------------------------
# DECORATOR FAILURE
# ------------------------------------------------------------------


def test_pipeline_latency_decorator_exception():
    logger = DummyLogger()

    @pipeline_latency_decorator("feature_build", logger=logger)
    def explode():
        raise RuntimeError("fail")

    with pytest.raises(RuntimeError):
        explode()

    assert len(logger.error_calls) == 1


# ------------------------------------------------------------------
# TRACE CREATION (CHILD SPAN)
# ------------------------------------------------------------------


def test_pipeline_latency_creates_child_trace():
    TraceContext.clear()

    root = TraceContext.start_trace(pipeline_stage="root")

    with PipelineLatency("child_stage"):
        current = TraceContext.get()

        assert current is not None
        assert current.parent_span_id == root.span_id
        assert current.pipeline_stage == "child_stage"


# ------------------------------------------------------------------
# TRACE RESTORATION
# ------------------------------------------------------------------


def test_pipeline_latency_restores_previous_trace():
    TraceContext.clear()

    root = TraceContext.start_trace(pipeline_stage="root")

    with PipelineLatency("child_stage"):
        pass

    restored = TraceContext.get()

    assert restored is not None
    assert restored.span_id == root.span_id


# ------------------------------------------------------------------
# TRACE RESTORE WHEN NO PRIOR CONTEXT
# ------------------------------------------------------------------


def test_pipeline_latency_restores_to_none_when_no_prior_trace():
    TraceContext.clear()

    with PipelineLatency("stage"):
        pass

    assert TraceContext.get() is None


# ------------------------------------------------------------------
# NESTED PIPELINE LATENCY CONTEXTS
# ------------------------------------------------------------------


def test_nested_pipeline_latency_contexts_restore_correctly():
    TraceContext.clear()

    root = TraceContext.start_trace(pipeline_stage="root")

    with PipelineLatency("stage_1"):
        stage_1 = TraceContext.get()

        with PipelineLatency("stage_2"):
            stage_2 = TraceContext.get()

            assert stage_2.parent_span_id == stage_1.span_id

        restored = TraceContext.get()

        assert restored.span_id == stage_1.span_id

    final = TraceContext.get()

    assert final.span_id == root.span_id


# ------------------------------------------------------------------
# METADATA IMMUTABILITY
# ------------------------------------------------------------------


def test_metadata_input_not_mutated():
    logger = DummyLogger()

    metadata = {"dataset": "v1"}

    with PipelineLatency("stage", logger=logger, metadata=metadata):
        pass

    assert metadata == {"dataset": "v1"}


# ------------------------------------------------------------------
# DURATION FIELD VALIDITY
# ------------------------------------------------------------------


def test_duration_is_positive():
    logger = DummyLogger()

    with PipelineLatency("stage", logger=logger):
        pass

    duration = logger.info_calls[0]["metadata"]["duration_ms"]

    assert duration >= 0