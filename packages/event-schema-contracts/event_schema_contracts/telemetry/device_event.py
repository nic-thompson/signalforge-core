from datetime import datetime
from typing import ClassVar, Annotated
from uuid import UUID
from enum import Enum

from pydantic import Field, StringConstraints
import re

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload
from event_schema_contracts.base.versioning import SemVerModel

class DeviceType(str, Enum):
    SENSOR = "SENSOR"
    GATEWAY = "GATEWAY"
    EDGE_NODE = "EDGE_NODE"

class DeviceRegistrationPayload(
    DomainEventPayload,
    SemVerModel
):
    """
    Payload schema for device registration telemetry events.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = (
        "device_id",
    )

    __utc_fields__: ClassVar[tuple[str, ...]] = (
        "registered_at",
    )

    __semver_fields__: ClassVar[tuple[str, ...]] = (
        "firmware_version"
    )

    device_id: UUID = Field(
        ...,
        description="Globally unique device identifier."
    )

    device_type: DeviceType

    firmware_version: str | None = Field(
        None,
        description="Installed firmware version (semver-compliant)."
    )

    registered_at: datetime = Field(
        ...,
        description="Timestamp when device completed registration."
    )

# Schema identity
EVENT_TYPE = "device.registration"
SCHEMA_VERSION_V1 = "v1"

class DeviceRegistrationEvent(BaseEvent[DeviceRegistrationPayload]):
    """
    device.registration v1

    Canonical device registration event contract.

    Metadata is auto injected from schema identity.
    """
    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1
