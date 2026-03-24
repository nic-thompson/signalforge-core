from datetime import datetime
from typing import ClassVar
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.base.domain import DomainEventPayload

class SessionStartPayload(DomainEventPayload):
    """
    Payload schema for session lifecycle start events.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "session_id",
        "actor_id"
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "started_at",
    )

    session_id: UUID = Field(
        ...,
        description="Unique session identifier"
    )

    actor_id: UUID = Field(
        ...,
        description="User or device initiating the session"
    )

    started_at: datetime = Field(
        ...,
        description="Session start timestamp"
    )

    client_version: str | None = Field(
        None,
        description="Client software version"
    )

class SessionStartEvent(
    BaseEvent[SessionStartPayload]
):
    """
    Canonical session start telemetry contract.
    """

    metadata: EventMetadata = Field(
        default_factory=lambda: EventMetadata(
            schema_version="v1",
            event_type="session.start",
            source="unknown"
        )
    )

    trace: TraceContext