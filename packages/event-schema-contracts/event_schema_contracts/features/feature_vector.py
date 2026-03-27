from datetime import datetime
from typing import Dict, Any, ClassVar
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload

class FeatureVectorPayload(DomainEventPayload):
    """
    Canonical payload representing a feature vector snapshot.

    Used by:

    - feature builders
    - offline dataset generators
    - online inference services
    - feature stores integration boundary
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "entity_id",
        "source_event_id",

    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "feature_timestamp",
    )

    entity_id: UUID = Field(
        ...,
        description="Primary entity identifier (device_id, user_id, session_id, etc.)"
    )

    feature_timestamp: datetime = Field(
        ...,
        description="Timestamp representing feature computation time"
    )

    feature_values: Dict[str, Any] = Field(
        ...,
        description="Dictionary of feature name -> feature value"
    )

    feature_version: str = Field(
        ...,
        description="Feature schema version identifier"
    )

    source_event_id: UUID = Field(
        ..., 
        description="Upstream telemetry event used to derive features"
    )
    
# Schema identity
EVENT_TYPE = "feature.vector"
SCHEMA_VERSION_V1 = "v1"
    
class FeatureVectorEvent(BaseEvent[FeatureVectorPayload]):
    """
    feature.vector v1

    Canonical feature vector contract.

    Defines the boundary between telemetry-derived signals and
    model-consumable datasets.

    Metadata is auto injected from schema identity.
    """
    __event_type__ = EVENT_TYPE
    __schema_version__ = SCHEMA_VERSION_V1
