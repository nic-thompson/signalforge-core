from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

class TraceContext(BaseModel):
    """
    Distributed trace propagation metadata.

    Enables cross-service correlation, replay tracking, pipeline debugging,
    and latency attribution across ML infrastructure components.
    """

    trace_id: UUID = Field(
        ...,
        description="Global trace identifier shared across pipeline stages"
    )

    parent_trace_id: Optional[UUID] = Field(
        None,
        description="Parent trace identifier if derived from upstream event"
    )

    pipeline_stage: Optional[str] = Field(
        None,
        description="Pipeline processing stage (ingestion, validation, enrichment, feature_building, export)"
    )
