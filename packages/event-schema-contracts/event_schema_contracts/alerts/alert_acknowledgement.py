"""
alert_acknowledgement.py

Canonical schema for alert acknowledgement events — a human (or an
automated responder acting for one) acknowledging a routed alert.

Acknowledgements ride the event stream so that acknowledgement *state*
is a replay-deterministic projection: a consumer (e.g. signal-forge's
Phase 5 AcknowledgementRegistry) subscribes to this event type through
the same router every other consumer uses and reconstructs identical
ack state from the same ordered input. This is why an ack is modelled as
a first-class event rather than as mutable state on the alert.

An acknowledgement references the alert it resolves by ``alert_id`` —
the stable, derivable key carried on ``alert.event``. Reminder cadence
(re-paging about a still-unacknowledged alert) is deliberately NOT part
of this contract: cadence is wall-clock-driven and lives in an
operational scheduler outside the deterministic event path.

Designed for:

- replay-deterministic acknowledgement-state projection
- alert lifecycle correlation via ``alert_id``
- audit of who acknowledged what, and when
"""

from datetime import datetime
from typing import ClassVar
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload


class AlertAcknowledgementPayload(DomainEventPayload):
    """
    Payload schema for alert acknowledgement events.

    ``acknowledgement_id`` is a genuinely new random identity (UUIDv4):
    an acknowledgement is a real new occurrence, not a derived artefact,
    so it is not replay-reproducible and does not need to be.
    ``alert_id`` references the alert being acknowledged and is permitted
    to be UUIDv4 or the UUIDv5-derived key carried on ``alert.event``.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "acknowledgement_id",
    )

    __uuid_v4_or_v5_fields__: ClassVar[tuple[str, ...]] = (
        "alert_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "acknowledged_at",
    )

    acknowledgement_id: UUID = Field(
        ...,
        description=(
            "Globally unique identifier for this acknowledgement "
            "instance. Random (UUIDv4): an acknowledgement is a new "
            "occurrence, not a derived artefact."
        ),
    )

    alert_id: UUID = Field(
        ...,
        description=(
            "The alert_id of the alert being acknowledged. References "
            "the stable key carried on alert.event; the join between an "
            "acknowledgement and the alert it resolves."
        ),
    )

    acknowledged_at: datetime = Field(
        ...,
        description=(
            "Event-time timestamp when the acknowledgement occurred. "
            "Distinct from the envelope's event_timestamp."
        ),
    )

    acknowledged_by: str = Field(
        ...,
        min_length=1,
        description=(
            "Identifier of the operator or on-call responder who "
            "acknowledged the alert."
        ),
    )

    note: str | None = Field(
        default=None,
        description=(
            "Optional free-text note accompanying the acknowledgement."
        ),
    )


# Schema identity
EVENT_TYPE = "alert.acknowledgement"
SCHEMA_VERSION_V1 = "v1"


class AlertAcknowledgementEvent(BaseEvent[AlertAcknowledgementPayload]):
    """
    alert.acknowledgement v1

    Canonical acknowledgement contract. Emitted when a routed alert is
    acknowledged by a human or an automated responder. Consumed by
    acknowledgement-state projections and lifecycle audit.

    Metadata is auto-injected from schema identity.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1
