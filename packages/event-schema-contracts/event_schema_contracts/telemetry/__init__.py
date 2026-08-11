"""
Raw structured telemetry: device, network and session events.

The events the parser emits and the streaming layer consumes. Importing this
subpackage registers its schemas.
"""

from event_schema_contracts.telemetry.device_event import DeviceRegistrationEvent, DeviceRegistrationPayload, DeviceType
from event_schema_contracts.telemetry.network_event import NetworkConnectionEvent, NetworkConnectionPayload, ConnectionDirection, TransportProtocol
from event_schema_contracts.telemetry.session_event import SessionStartEvent, SessionStartPayload

__all__ = [
    "ConnectionDirection",
    "DeviceRegistrationEvent",
    "DeviceRegistrationPayload",
    "DeviceType",
    "NetworkConnectionEvent",
    "NetworkConnectionPayload",
    "SessionStartEvent",
    "SessionStartPayload",
    "TransportProtocol",
]
