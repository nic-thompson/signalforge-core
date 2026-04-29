from __future__ import annotations

import uuid
from contextvars import ContextVar
from dataclasses import dataclass
from contextlib import contextmanager


@dataclass
class TraceState:
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    correlation_id: str | None = None
    pipeline_stage: str | None = None


_trace_context: ContextVar[TraceState | None] = ContextVar(
    "trace_context",
    default=None,
)


class TraceContext:
    """
    Distributed trace context manager backed by contextvars.

    Supports hierarchical span-based tracing compatible with
    OpenTelemetry-style execution lineage.
    """

    # ---------- ID GENERATION ----------

    @staticmethod
    def generate_trace_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def generate_span_id() -> str:
        return str(uuid.uuid4())

    # ---------- ROOT TRACE ----------

    @classmethod
    def start_trace(
        cls,
        trace_id: str | None = None,
        correlation_id: str | None = None,
        pipeline_stage: str | None = None,
    ) -> TraceState:

        trace_state = TraceState(
            trace_id=trace_id or cls.generate_trace_id(),
            span_id=cls.generate_span_id(),
            parent_span_id=None,
            correlation_id=correlation_id,
            pipeline_stage=pipeline_stage,
        )

        _trace_context.set(trace_state)
        return trace_state

    # ---------- CHILD SPAN ----------

    @classmethod
    def child_trace(
        cls,
        pipeline_stage: str | None = None,
    ) -> TraceState:

        current = _trace_context.get()

        if not current:
            return cls.start_trace(pipeline_stage=pipeline_stage)

        trace_state = TraceState(
            trace_id=current.trace_id,
            span_id=cls.generate_span_id(),
            parent_span_id=current.span_id,
            correlation_id=current.correlation_id,
            pipeline_stage=pipeline_stage or current.pipeline_stage,
        )

        _trace_context.set(trace_state)
        return trace_state

    # ---------- ACCESS CURRENT CONTEXT ----------

    @classmethod
    def get(cls) -> TraceState | None:
        return _trace_context.get()

    # ---------- CLEAR CONTEXT ----------

    @classmethod
    def clear(cls) -> None:
        _trace_context.set(None)

    @classmethod
    def end_trace(cls) -> None:
        """
        Alias for clear(). Clears the current trace context.

        Provided for adapter code that uses lifecycle-oriented naming
        (start_trace / end_trace) rather than the lower-level clear().
        """
        cls.clear()

    # ---------- SPAN CONTEXT MANAGER ----------

    @classmethod
    @contextmanager
    def span(cls, pipeline_stage: str | None = None):
        previous = cls.get()
        new_span = cls.child_trace(pipeline_stage)

        try:
            yield new_span
        finally:
            if previous:
                _trace_context.set(previous)
            else:
                cls.clear()


    # ---------- RESTORE PREVIOUS CONTEXT ----------

    @classmethod
    def restore(cls, previous: TraceState | None) -> None:
        """
        Restore a previously captured trace context.

        Used by instrumentation utilities that temporarily
        create child spans and must safely revert afterwards.
        """
        if previous is not None:
            _trace_context.set(previous)
        else:
            cls.clear()

