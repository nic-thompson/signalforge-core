"""
Distributed trace context propagated on every event.

Carries the identifiers that let a single logical operation be followed across
service boundaries, and the pipeline stage the event was at when emitted.
"""

from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class PipelineStage(str, Enum):
    """
    The pipeline stage an event was emitted from.

    Ordered here as the canonical flow, though events do not necessarily visit
    every stage. Inherits from ``str`` so members serialise as their values.
    """

    INGESTION = "ingestion"
    VALIDATION = "validation"
    ENRICHMENT = "enrichment"
    FEATURE_BUILDING = "feature_building"
    EXPORT = "export"
    INFERENCE = "inference"


class TraceContext(BaseModel):
    """
    Distributed trace propagation metadata.

    Enables cross-service correlation, replay tracking, pipeline debugging,
    and latency attribution across ML infrastructure components.
    """

    trace_id: UUID = Field(
        default_factory=uuid4,
        description="Global trace identifier shared across pipeline stages",
    )

    root_trace_id: UUID | None = Field(
        default=None,
        description="Root lineage identifier for replay tracking",
    )

    pipeline_stage: PipelineStage = Field(
        default=PipelineStage.INGESTION,
        description="Pipeline processing stage",
    )

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    @model_validator(mode="before")
    @classmethod
    def default_root_trace(cls, data: Any) -> Any:
        """
        Seed ``root_trace_id`` from ``trace_id`` when it is not supplied.
        """

        # An event with no explicit root is the root of its own lineage: seed
        # root_trace_id from trace_id so every event has a stable lineage
        # anchor for replay tracking, without the producer having to
        # special-case the first event in a chain.
        if isinstance(data, dict) and data.get("root_trace_id") is None:
            data["root_trace_id"] = data.get("trace_id")
        return data