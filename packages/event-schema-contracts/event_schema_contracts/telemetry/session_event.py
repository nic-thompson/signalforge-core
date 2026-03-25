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
        pattern=r"^\d+\.\d+\.\d+$",
        description="Client software version"
    )

# Schema identity
EVENT_TYPE = "session.start"
SCHEMA_VERSION_V1 = "v1"

class SessionStartEvent(BaseEvent[SessionStartPayload]):
    """
    session.start v1

    Canonical session start telemetry contract.
    """
    __event_type__ = EVENT_TYPE
    __schema_version__ = SCHEMA_VERSION_V1
