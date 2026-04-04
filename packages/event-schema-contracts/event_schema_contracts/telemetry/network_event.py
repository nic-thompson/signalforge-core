from datetime import datetime
from typing import Union, ClassVar
from uuid import UUID
from enum import Enum
from ipaddress import IPv4Address, IPv6Address

from pydantic import Field

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload


IPAddress = Union[IPv4Address, IPv6Address]


class ConnectionDirection(str, Enum):
    """
    Direction relative to the observing node or telemetry boundary.
    """

    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class TransportProtocol(str, Enum):
    """
    Transport-layer protocol observed for the connection.
    """

    TCP = "TCP"
    UDP = "UDP"
    TLS = "TLS"


class NetworkConnectionPayload(DomainEventPayload):
    """
    Payload schema for network connection telemetry events.

    Represents a single observed connection session between two endpoints.
    The connection_id MUST uniquely identify one logical connection lifecycle.
    A reconnect using the same 5-tuple MUST generate a new connection_id.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "connection_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "connected_at",
    )

    connection_id: UUID = Field(
        ...,
        description="Unique identifier for the logical connection session",
    )

    source_ip: IPAddress = Field(
        ...,
        description="Source IP address of the connection",
    )

    source_port: int | None = Field(
        None,
        ge=0,
        le=65535,
        description="Source transport-layer port number",
    )

    destination_ip: IPAddress = Field(
        ...,
        description="Destination IP address of the connection",
    )

    destination_port: int | None = Field(
        None,
        ge=0,
        le=65535,
        description="Destination transport-layer port number",
    )

    protocol: TransportProtocol = Field(
        ...,
        description="Observed transport protocol",
    )

    connected_at: datetime = Field(
        ...,
        description="UTC timestamp when the connection was established",
    )

    latency_ms: int | None = Field(
        None,
        ge=0,
        le=60_000,
        description="Observed connection establishment latency in milliseconds",
    )

    direction: ConnectionDirection = Field(
        ...,
        description="Direction relative to the observing node",
    )


# Schema identity
EVENT_TYPE = "network.connection"
SCHEMA_VERSION_V1 = "v1"


class NetworkConnectionEvent(BaseEvent[NetworkConnectionPayload]):
    """
    network.connection v1

    Canonical telemetry contract describing an observed network connection.
    Suitable for replay pipelines, feature engineering workflows,
    anomaly detection systems, and service graph construction.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1