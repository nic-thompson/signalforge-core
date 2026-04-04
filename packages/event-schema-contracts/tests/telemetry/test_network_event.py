from datetime import datetime, timezone
from uuid import uuid4
from ipaddress import IPv4Address

import pytest

from event_schema_contracts.telemetry.network_event import (
    NetworkConnectionPayload,
    NetworkConnectionEvent,
    ConnectionDirection,
    TransportProtocol
)

from event_schema_contracts.base.trace import TraceContext

def valid_payload_kwargs(**overrides):
    """
    Factory helper for contstructing valid payload inputs.
    Allows override injection for validation tests. 
    """
    base = dict(
        connection_id=uuid4(),
        source_ip="192.168.1.10",
        source_port=443,
        destination_ip="10.0.0.5",
        destination_port=51515,
        protocol=TransportProtocol.TCP,
        connected_at=datetime.now(timezone.utc),
        latency_ms=25,
        direction=ConnectionDirection.OUTBOUND,
    )
    base.update(overrides)
    return base


# -----------------------
# Payload construction
# -----------------------


def test_valid_payload_construction():
    payload = NetworkConnectionPayload(**valid_payload_kwargs())

    assert payload.protocol == TransportProtocol.TCP
    assert payload.direction == ConnectionDirection.OUTBOUND


# -----------------------
# IP coercion
# -----------------------


def test_ip_address_parsed_to_ipv4_object():
    payload = NetworkConnectionPayload(**valid_payload_kwargs())

    assert isinstance(payload.source_ip, IPv4Address)


# -----------------------
# Port validation
# -----------------------


def test_invalid_source_port_rejected():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(source_port=70000)
        )


def test_invalid_destination_port_rejected():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(destination_port=-1)
        )


# -------------------------
# Latency validation
# -------------------------


def test_negative_latency_rejected():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(latency_ms=-1)
        )

def test_latency_upper_bound_enforced():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(latency_ms=100000)
        )


# ------------------------
# UUID validation
# ------------------------


def test_connection_id_requires_uuid():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(connection_id="not-a-uuid")
        )


# ---------------------------
# UTC timestamp enforcement
# ---------------------------


def test_connected_at_requires_utc():
    naive_time = datetime.now() # intentionally missing timezone

    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(connected_at=naive_time)
        )


# --------------------------        
# Enum validation        
# --------------------------


def test_invalid_protocol_rejected():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(protocol="HTTP")
        )

def test_invalid_direction_detected():
    with pytest.raises(Exception):
        NetworkConnectionPayload(
            **valid_payload_kwargs(direction="SIDEWAYS")
        )


# ---------------------------
# Event wrapper validation
# ---------------------------


def test_event_wrapper_accepts_payload():
    payload = NetworkConnectionPayload(**valid_payload_kwargs())

    event = NetworkConnectionEvent(
        payload=payload,
        trace=TraceContext(trace_id=uuid4()),
        event_timestamp=datetime.now(timezone.utc),
    )

    assert event.payload == payload
    assert event.__event_type__ == "network.connection"
    assert event.__schema_version__ == "v1"
