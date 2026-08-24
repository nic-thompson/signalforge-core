"""
Raw structured telemetry: device, network, session and SIP registration events.

The events the parser emits and the streaming layer consumes. Importing this
subpackage registers its schemas.
"""

from event_schema_contracts.telemetry.device_event import DeviceRegistrationEvent, DeviceRegistrationPayload, DeviceType
from event_schema_contracts.telemetry.network_event import NetworkConnectionEvent, NetworkConnectionPayload, ConnectionDirection, TransportProtocol
from event_schema_contracts.telemetry.session_event import SessionStartEvent, SessionStartPayload
from event_schema_contracts.telemetry.sip_registration_event import (
    RegistrationStatus,
    SipRegistrationEvent,
    SipRegistrationPayload,
    SipTransportProtocol,
)

__all__ = [
    "ConnectionDirection",
    "DeviceRegistrationEvent",
    "DeviceRegistrationPayload",
    "DeviceType",
    "NetworkConnectionEvent",
    "NetworkConnectionPayload",
    "RegistrationStatus",
    "SessionStartEvent",
    "SessionStartPayload",
    "SipRegistrationEvent",
    "SipRegistrationPayload",
    "SipTransportProtocol",
    "TransportProtocol",
]
