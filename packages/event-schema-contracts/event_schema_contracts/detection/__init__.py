"""
Detection events emitted when a monitored condition is identified.

Importing this subpackage registers its schemas.
"""

from event_schema_contracts.detection.detection_event import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

__all__ = [
    "DetectionEvent",
    "DetectionEventPayload",
    "DetectionSeverity",
]