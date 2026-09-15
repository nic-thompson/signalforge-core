"""
Feature vector events for dataset export and streaming aggregation.

``FeatureVectorEvent`` is entity-centric; ``WindowedFeatureVectorEvent`` is
partition-window-centric. Importing this subpackage registers its schemas.
"""

from event_schema_contracts.features.feature_vector import FeatureVectorEvent, FeatureVectorPayload, FeatureValue
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
    WindowedFeatureVectorPayload,
)

__all__ = [
    "FeatureValue",
    "FeatureVectorEvent",
    "FeatureVectorPayload",
    "WindowedFeatureVectorEvent",
    "WindowedFeatureVectorPayload",
]
