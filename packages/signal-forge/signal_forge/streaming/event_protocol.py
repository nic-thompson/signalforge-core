"""
signal_forge.streaming.event_protocol

Structural protocols describing the shape of telemetry events as consumed by
the streaming layer.

Why protocols, not imports?
---------------------------

The streaming layer must not perform redundant validation of events it
receives — events arriving here have already been validated by
``telemetry-parser`` against the schemas in ``event-schema-contracts``.

Protocols let us:

- describe exactly which fields the streaming layer touches
- run the streaming hot path without pydantic re-validation overhead
- unit-test routers, watermarks, and aggregators with lightweight test
  doubles that do not require pydantic
- keep the upstream ``BaseEvent`` as the single source of truth for the
  full envelope contract

Real upstream ``BaseEvent`` instances satisfy these protocols structurally,
so the public API is unchanged.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class EventMetadataLike(Protocol):
    """Subset of ``EventMetadata`` consumed by the streaming layer."""

    schema_version: str
    event_type: str
    source: str


@runtime_checkable
class TraceContextLike(Protocol):
    """Subset of ``TraceContext`` consumed by the streaming layer."""

    trace_id: UUID
    root_trace_id: UUID | None


@runtime_checkable
class TelemetryEvent(Protocol):
    """
    Structural contract for events flowing through the streaming layer.

    Any object exposing these attributes is acceptable. Upstream
    ``BaseEvent`` instances satisfy this protocol naturally.
    """

    event_id: UUID
    metadata: EventMetadataLike
    trace: TraceContextLike
    event_timestamp: datetime
    ingest_timestamp: datetime
    payload: Any
