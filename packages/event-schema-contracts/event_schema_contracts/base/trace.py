from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class PipelineStage(str, Enum):
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
        if isinstance(data, dict) and data.get("root_trace_id") is None:
            data["root_trace_id"] = data.get("trace_id")
        return data