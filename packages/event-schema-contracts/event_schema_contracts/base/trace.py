from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

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
        description="Global trace identifier shared across pipeline stages"
    )

    root_trace_id: UUID = Field(
        default_factory=uuid4,
        description="Root lineage identifier for replay tracking"
    )

    pipeline_stage: PipelineStage = Field(
        default=PipelineStage.INGESTION,
        description="Pipeline processing stage"
    )
