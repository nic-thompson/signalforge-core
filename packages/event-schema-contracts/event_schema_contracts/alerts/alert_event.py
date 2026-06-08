"""
alert_event.py

Canonical schema for routed alert events emitted by the analytics
control plane's alert-routing layer (e.g. signal-forge Phase 5) when a
detection is turned into an alert destined for an operational sink
(EventBridge, SNS, and conceptually Slack / PagerDuty).

An alert is downstream of a detection. The detection says "a monitored
condition occurred"; the alert says "this condition has been routed for
operational attention, at this severity, identified by this stable key".
The separation lets alerting carry its own identity (``alert_id``) that
downstream systems dedupe on and that an acknowledgement references,
while preserving lineage back to the originating detection via
``detection_id``.

``alert_id`` is permitted to be UUIDv5 (deterministically derived) so a
replay produces byte-identical alert identities, the same trade-off the
detection contract makes for ``detection_id``. ``severity`` reuses
``DetectionSeverity`` so detection and alerting share one severity
vocabulary; the alert router passes the detector-assigned severity
through rather than reclassifying it.

Designed for:

- alert routing pipelines (EventBridge, SNS, Slack, PagerDuty)
- acknowledgement correlation via ``alert_id``
- replay-safe regeneration of historical alert streams
- dashboard projections of alert volume and severity
"""

from datetime import datetime
from typing import Any, ClassVar
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload
from event_schema_contracts.detection.detection_event import DetectionSeverity


class AlertEventPayload(DomainEventPayload):
    """
    Payload schema for routed alert events.

    Carries the alert's own identity (``alert_id``), lineage to the
    originating detection (``detection_id``), the detector-assigned
    severity, and enough denormalised context (``store_id``,
    ``device_id``, ``detection_type``, ``summary``) for an alert sink to
    route and render the alert without re-fetching the detection.

    ``alert_id`` and ``detection_id`` may be UUIDv4 (random) or UUIDv5
    (derived); the alert-routing layer derives ``alert_id`` from the
    detection id so it is stable across replays.
    """

    __uuid_v4_or_v5_fields__: ClassVar[tuple[str, ...]] = (
        "alert_id",
        "detection_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "routed_at",
    )

    alert_id: UUID = Field(
        ...,
        description=(
            "Globally unique identifier for this alert instance. "
            "Derived (UUIDv5) by the alert router from detection_id so "
            "it is stable across replays; referenced by acknowledgements "
            "and used for downstream deduplication."
        ),
    )

    detection_id: UUID = Field(
        ...,
        description=(
            "The detection_id of the detection this alert was raised "
            "from. Establishes lineage back to the originating detection."
        ),
    )

    detection_type: str = Field(
        ...,
        min_length=1,
        pattern=r"^[a-z0-9]+(\.[a-z0-9]+)+$",
        description=(
            "Logical detection type the alert was raised for (e.g. "
            "'store.outage'). Carried through from the detection so "
            "sinks can route or filter without re-fetching it."
        ),
    )

    severity: DetectionSeverity = Field(
        ...,
        description=(
            "Severity carried through from the detection. The router "
            "does not reclassify; CRITICAL drives immediate paging, "
            "WARNING drives digesting, in the consuming layer."
        ),
    )

    routed_at: datetime = Field(
        ...,
        description=(
            "Event-time timestamp when the router raised the alert. "
            "Distinct from the envelope's event_timestamp."
        ),
    )

    store_id: str = Field(
        ...,
        min_length=1,
        description=(
            "Logical store identifier scoped to the alert. Modelled as "
            "a string to match the platform's store-id convention; not "
            "a UUID."
        ),
    )

    device_id: UUID | None = Field(
        default=None,
        description=(
            "Device identifier for device-scoped alerts. None for "
            "store-level alerts (e.g. store.outage). Carried through "
            "from the detection."
        ),
    )

    summary: str = Field(
        ...,
        min_length=1,
        description=(
            "Human-readable description of the alert, for the page or "
            "digest message. Mirrors the detection's threshold_breached."
        ),
    )

    details: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Alert-specific structured details. Consumers should treat "
            "unknown keys as opaque to preserve forward compatibility."
        ),
    )


# Schema identity
EVENT_TYPE = "alert.event"
SCHEMA_VERSION_V1 = "v1"


class AlertEvent(BaseEvent[AlertEventPayload]):
    """
    alert.event v1

    Canonical routed-alert contract. Emitted by the analytics control
    plane's alert-routing layer when a detection is routed for
    operational attention. Consumed by alert sinks (EventBridge, SNS),
    acknowledgement correlation, and dashboard projectors.

    Metadata is auto-injected from schema identity.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1
