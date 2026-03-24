from datetime import datetime
from typing import Union, ClassVar
from uuid import UUID
from enum import Enum
from ipaddress import IPv4Address, IPv6Address

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.base.domain import DomainEventPayload

IPAddress = Union[IPv4Address, IPv6Address]

class ConnectionDirection(str, Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"

class TransportProtocol(str, Enum):
    TCP = "TCP"
    UDP = "UDP"
    TLS = "TLS"

class NetworkConnectionPayload(DomainEventPayload):
    """
    Payload schema for network connection telemetry events.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "connection_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "connected_at",
    )

    connection_id: UUID = Field(
        ...,
        description="Unique connection session identifier"
    )

    source_ip: IPAddress

    destination_ip: IPAddress

    protocol: TransportProtocol

    connected_at: datetime = Field(
        ...,
        description="Connection establishment timestamp"
    )

    latency_ms: float | None = Field(
        None,
        ge=0,
        description="Observed connection latency"
    )

    direction: ConnectionDirection

class NetworkConnectionEvent(
    BaseEvent[NetworkConnectionPayload]
):
    """
    Canonical network connection telemetry contract.
    """

    metadata: EventMetadata = Field(
        default_factory=lambda: EventMetadata(
            schema_version="v1",
            event_type="network.connection",
            source="unknown"
        )
    )

    trace: TraceContext