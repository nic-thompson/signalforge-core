from __future__ import annotations

from dataclasses import replace
from typing import Dict

from structured_logging.trace.trace_context import TraceContext, _trace_context


TRACE_HEADER_TRACE_ID = "x-trace-id"
TRACE_HEADER_SPAN_ID = "x-trace-span-id"
TRACE_HEADER_PARENT_ID = "x-trace-parent-id"
TRACE_HEADER_CORRELATION_ID = "x-trace-correlation-id"
TRACE_HEADER_PIPELINE_STAGE = "x-trace-pipeline-stage"


class TracePropagation:
    """
    Utilities for injecting and extracting distributed trace context
    across service boundaries.

    Supports hierarchical span-based tracing compatible with
    OpenTelemetry-style execution lineage.
    """

    # ------------------------------------------------------------------
    # OUTBOUND PROPAGATION
    # ------------------------------------------------------------------

    @staticmethod
    def inject_headers(headers: Dict[str, str]) -> Dict[str, str]:
        """
        Inject current trace context into outbound request headers.
        """

        state = TraceContext.get()

        if not state:
            return headers

        headers[TRACE_HEADER_TRACE_ID] = state.trace_id
        headers[TRACE_HEADER_SPAN_ID] = state.span_id

        if state.parent_span_id:
            headers[TRACE_HEADER_PARENT_ID] = state.parent_span_id

        if state.correlation_id:
            headers[TRACE_HEADER_CORRELATION_ID] = state.correlation_id

        if state.pipeline_stage:
            headers[TRACE_HEADER_PIPELINE_STAGE] = state.pipeline_stage

        return headers

    # ------------------------------------------------------------------
    # INBOUND PROPAGATION
    # ------------------------------------------------------------------

    @staticmethod
    def extract_headers(headers: Dict[str, str]) -> None:
        """
        Restore trace context from inbound request headers.

        Creates a new local span whose parent_span_id is the upstream span_id.
        """

        trace_id = headers.get(TRACE_HEADER_TRACE_ID)

        if not trace_id:
            return

        upstream_span_id = headers.get(TRACE_HEADER_SPAN_ID)

        # Start a new span locally while continuing upstream trace lineage
        state = TraceContext.start_trace(
            trace_id=trace_id,
            correlation_id=headers.get(TRACE_HEADER_CORRELATION_ID),
            pipeline_stage=headers.get(TRACE_HEADER_PIPELINE_STAGE),
        )

        # Preserve span hierarchy: record the upstream span as our parent,
        # then write the updated state back into the ContextVar so that
        # TraceContext.get() returns the correct object.
        if upstream_span_id:
            updated = replace(state, parent_span_id = upstream_span_id)
            _trace_context.set(updated)