"""
detection_event.py

Canonical schema for detection events emitted by the analytics control
plane (e.g. signal-forge) when a monitored condition is identified.

This contract uses the **discriminator pattern**: a single schema covers
every detection type, distinguished by ``detection_type``. Detector-
specific structured data lives in ``details``. This trades the strict
typing of per-detector subschemas for a single stable contract that
absorbs new detector types without schema-version bumps.

Designed for:

- alert routing pipelines (EventBridge, SNS, Slack, PagerDuty)
- detection lineage tracking via ``source_event_id``
- replay-safe regeneration of historical detection streams
- dashboard projections of detection volume and severity
"""

from datetime import datetime
from enum import Enum
from typing import Any, ClassVar
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload


class DetectionSeverity(str, Enum):
    """
    Severity classification for detection events.

    INFO    - informational, no operational action required.
    WARNING - operational attention needed within normal cadence.
    CRITICAL - immediate operational response required.

    Severity is set by the detector, not the alert router. The alert
    router consumes severity to drive escalation policy in Phase 5+.
    """

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class DetectionEventPayload(DomainEventPayload):
    """
    Payload schema for detection events.

    Discriminator-pattern contract. The ``detection_type`` field
    identifies which detector emitted the event and what structure the
    ``details`` field carries. Conventional values follow a
    ``<domain>.<state>`` shape; see the consuming repository
    (signal-forge) for canonical constants.

    Examples:

        detection_type="device.offline"
        details={"silent_seconds": 320, "threshold_seconds": 300}

        detection_type="store.outage"
        details={"offline_count": 7, "registered_count": 10, "ratio": 0.7}

        detection_type="signal.anomaly"
        details={"field": "signal_quality_index", "observed": 0.18,
                 "threshold": 0.5, "direction": "below"}
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "detection_id",
        "source_event_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "detected_at",
    )

    detection_id: UUID = Field(
        ...,
        description="Globally unique identifier for this detection instance.",
    )

    detection_type: str = Field(
        ...,
        min_length=1,
        pattern=r"^[a-z0-9]+(\.[a-z0-9]+)+$",
        description=(
            "Logical detection type identifier (e.g. 'device.offline'). "
            "Follows the same dotted-lowercase convention as event_type."
        ),
    )

    severity: DetectionSeverity = Field(
        ...,
        description="Detector-assigned severity classification.",
    )

    detected_at: datetime = Field(
        ...,
        description=(
            "Event-time timestamp when the detector identified the "
            "condition. Distinct from the envelope's event_timestamp."
        ),
    )

    store_id: str = Field(
        ...,
        min_length=1,
        description=(
            "Logical store identifier scoped to the detection. "
            "Modelled as a string to match the platform's store-id "
            "convention; not a UUID."
        ),
    )

    device_id: UUID | None = Field(
        default=None,
        description=(
            "Device identifier for device-scoped detections. None for "
            "store-level detections (e.g. store.outage)."
        ),
    )

    source_event_id: UUID = Field(
        ...,
        description=(
            "The upstream telemetry event_id that triggered the "
            "detection. Establishes lineage for replay and audit."
        ),
    )

    threshold_breached: str = Field(
        ...,
        min_length=1,
        description=(
            "Human-readable description of the threshold breach, e.g. "
            "'7/10 devices offline'. For dashboards and alert messages."
        ),
    )

    details: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Detector-specific structured details. Shape depends on "
            "detection_type. Consumers should treat unknown keys as "
            "opaque to preserve forward compatibility."
        ),
    )


# Schema identity
EVENT_TYPE = "detection.event"
SCHEMA_VERSION_V1 = "v1"


class DetectionEvent(BaseEvent[DetectionEventPayload]):
    """
    detection.event v1

    Canonical detection event contract. Emitted by the analytics
    control plane when a monitored condition is identified. Consumed
    by alert routing pipelines, dashboard projectors, and dataset
    exporters.

    Metadata is auto-injected from schema identity.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1