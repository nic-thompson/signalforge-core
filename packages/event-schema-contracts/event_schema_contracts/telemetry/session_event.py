"""
session_event.py

Canonical telemetry contract for session lifecycle start events.

This schema is designed for:

- analytics pipelines
- dataset generation workflows
- feature store joins
- replay/backfill compatibility
- distributed trace correlation
- experiment attribution
"""

from datetime import datetime
from typing import ClassVar, Literal
from uuid import UUID

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload


EVENT_TYPE = "session.start"
SCHEMA_VERSION = "v1"


class SessionStartPayload(DomainEventPayload):
    """
    Payload schema for session lifecycle start events.

    This contract represents the canonical analytic session boundary event
    and is intended to be stable across ingestion, storage, replay, and
    dataset generation pipelines.
    """

    # Enforced centrally by DomainEventPayload validators
    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "session_id",
        "actor_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "started_at",
    )

    session_id: UUID = Field(
        ...,
        description="Globally unique session identifier",
    )

    actor_id: UUID = Field(
        ...,
        description="Stable identifier for user, device, or service actor initiating the session",
    )

    started_at: datetime = Field(
        ...,
        description="UTC timestamp when the session began (event-time, not ingestion-time)",
    )

    client_version: str | None = Field(
        default=None,
        pattern=r"^\d+\.\d+\.\d+([\-+][A-Za-z0-9\.]+)?$",
        description="Semantic version of originating client (semver-compatible)",
    )

    platform: Literal["ios", "android", "web", "backend", "cli"] | None = Field(
        default=None,
        description="Originating platform for the session",
    )

    entrypoint: str | None = Field(
        default=None,
        description="Surface or mechanism that initiated the session (e.g. deep_link, homepage, notification)",
    )

    region: str | None = Field(
        default=None,
        description="ISO region code inferred at session start time",
    )

    network_type: Literal["wifi", "cellular", "ethernet", "unknown"] | None = Field(
        default=None,
        description="Network class observed at session start",
    )

    experiment_id: str | None = Field(
        default=None,
        description="Active experiment identifier if session is part of an experiment cohort",
    )


class SessionStartEvent(BaseEvent[SessionStartPayload]):
    """
    session.start v1

    Canonical session start telemetry contract.

    Used as:

    - session boundary marker
    - feature engineering anchor event
    - replay-safe lifecycle signal
    - analytics join key generator
    """

    __event_type__ = EVENT_TYPE
    __schema_version__ = SCHEMA_VERSION