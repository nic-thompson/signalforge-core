from datetime import datetime
from typing import ClassVar

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload
from event_schema_contracts.features.feature_vector import FeatureValue

PARTITION_KEY_PATTERN = r"^[a-z0-9]+([.\-_][a-z0-9]+)*$"


class WindowedFeatureVectorPayload(DomainEventPayload):
    """
    Feature vector aggregated over a partition-window pair.

    Distinct from ``FeatureVectorPayload`` (entity-centric, one feature
    snapshot for one entity at one timestamp). This payload describes
    a snapshot of features for a partition (typically a store) over a
    bounded event-time window. Used by aggregation pipelines that
    project windowed features for downstream consumers (dataset
    builders, dashboards, ML feature stores).
    """

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "window_start",
        "window_end",
    )

    partition_key: str = Field(
        ...,
        description="Partition identifier these features describe (typically a store_id).",
        pattern=PARTITION_KEY_PATTERN,
        min_length=1,
    )

    window_start: datetime = Field(
        ...,
        description="Inclusive event-time start of the aggregation window.",
    )

    window_end: datetime = Field(
        ...,
        description="Exclusive event-time end of the aggregation window.",
    )

    feature_values: dict[str, FeatureValue] = Field(
        ...,
        description="Dictionary of feature name to scalar feature value "
        "(int, float, bool, or str).",
    )

    feature_version: str = Field(
        ...,
        pattern=r"^v\d+(\.\d+)*$",
        description="Feature schema version identifier (e.g. v1, v1.2, v1.2.3).",
    )


EVENT_TYPE = "feature.vector.windowed"
SCHEMA_VERSION_V1 = "v1"


class WindowedFeatureVectorEvent(BaseEvent[WindowedFeatureVectorPayload]):
    """
    feature.vector.windowed v1

    Canonical wrapper for windowed feature vector emissions.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1
