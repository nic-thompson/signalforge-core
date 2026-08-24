from datetime import datetime, timezone
from uuid import NAMESPACE_DNS, uuid1, uuid4, uuid5

import pytest
from pydantic import ValidationError

from event_schema_contracts.telemetry.sip_registration_event import (
    EVENT_TYPE,
    SCHEMA_VERSION_V1,
    RegistrationStatus,
    SipRegistrationEvent,
    SipRegistrationPayload,
    SipTransportProtocol,
)
from event_schema_contracts.versioning.schema_registry import schema_registry


NAMESPACE = uuid5(NAMESPACE_DNS, "signalforge.analytics")


def derived_device_id(store_id: str, device_label: str):
    """Mirrors signal_forge.identity.derive for test purposes."""
    return uuid5(NAMESPACE, f"device|{store_id}|{device_label}")


def minimal_payload(**overrides):
    fields = {
        "device_id": derived_device_id("store-1", "headset-0001"),
        "device_label": "headset-0001",
        "store_id": "store-1",
        "registration_status": RegistrationStatus.REGISTERED,
        "observed_at": datetime.now(timezone.utc),
    }
    fields.update(overrides)
    return SipRegistrationPayload(**fields)


# ---------------------------------------------------------------
# Required fields — the set a detector cannot function without
# ---------------------------------------------------------------


def test_minimal_payload_valid_with_only_required_fields():
    payload = minimal_payload()
    assert payload.latency_ms is None
    assert payload.retry_count is None
    assert payload.transport_protocol is None
    assert payload.source_ip is None
    assert payload.registration_call_id is None


@pytest.mark.parametrize(
    "missing",
    ["device_id", "device_label", "store_id", "registration_status", "observed_at"],
)
def test_required_fields_rejected_when_absent(missing):
    fields = {
        "device_id": derived_device_id("store-1", "headset-0001"),
        "device_label": "headset-0001",
        "store_id": "store-1",
        "registration_status": RegistrationStatus.REGISTERED,
        "observed_at": datetime.now(timezone.utc),
    }
    del fields[missing]
    with pytest.raises(ValidationError):
        SipRegistrationPayload(**fields)


# ---------------------------------------------------------------
# Identity policy — derived (v5) ids must be accepted, v1 must not
# ---------------------------------------------------------------


def test_device_id_accepts_derived_uuid_v5():
    device_id = derived_device_id("store-1", "headset-0001")
    assert device_id.version == 5
    assert minimal_payload(device_id=device_id).device_id == device_id


def test_device_id_accepts_random_uuid_v4():
    """v4 stays permitted so a future producer with a genuine random id isn't locked out."""
    assert minimal_payload(device_id=uuid4()).device_id.version == 4


def test_device_id_rejects_uuid_v1():
    """v1 encodes host and time — no place on an identity field."""
    with pytest.raises(ValidationError):
        minimal_payload(device_id=uuid1())


def test_derivation_is_stable_across_calls():
    """The property replay determinism depends on."""
    assert derived_device_id("store-1", "headset-0001") == derived_device_id(
        "store-1", "headset-0001"
    )


def test_same_label_in_different_stores_yields_different_ids():
    """Labels are only unique within a store, so the store must be part of the derivation."""
    assert derived_device_id("store-1", "headset-0001") != derived_device_id(
        "store-2", "headset-0001"
    )


def test_device_label_is_retained_alongside_derived_id():
    """
    UUIDv5 derivation is one-way. Without the label the original is
    unrecoverable, leaving an operator holding a physical headset with
    nothing to match against.
    """
    assert minimal_payload().device_label == "headset-0001"


def test_device_label_rejects_empty_string():
    with pytest.raises(ValidationError):
        minimal_payload(device_label="")


# ---------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------


def test_observed_at_rejects_naive_datetime():
    with pytest.raises(ValidationError):
        minimal_payload(observed_at=datetime(2026, 8, 22, 9, 0, 0))


def test_observed_at_accepts_utc():
    ts = datetime(2026, 8, 22, 9, 0, 0, tzinfo=timezone.utc)
    assert minimal_payload(observed_at=ts).observed_at == ts


# ---------------------------------------------------------------
# store_id grammar — must agree with DeviceRegistrationPayload
# ---------------------------------------------------------------


@pytest.mark.parametrize("store_id", ["store-1", "store.1", "store_1", "store1"])
def test_store_id_accepts_valid_grammar(store_id):
    assert minimal_payload(store_id=store_id).store_id == store_id


@pytest.mark.parametrize("store_id", ["Store-1", "store 1", "-store", "store-", ""])
def test_store_id_rejects_invalid_grammar(store_id):
    with pytest.raises(ValidationError):
        minimal_payload(store_id=store_id)


# ---------------------------------------------------------------
# Optional operational fields
# ---------------------------------------------------------------


def test_latency_ms_accepts_zero_and_upper_bound():
    assert minimal_payload(latency_ms=0).latency_ms == 0
    assert minimal_payload(latency_ms=60_000).latency_ms == 60_000


@pytest.mark.parametrize("latency", [-1, 60_001])
def test_latency_ms_rejects_out_of_range(latency):
    with pytest.raises(ValidationError):
        minimal_payload(latency_ms=latency)


def test_retry_count_rejects_negative():
    with pytest.raises(ValidationError):
        minimal_payload(retry_count=-1)


def test_source_ip_accepts_ipv4_and_ipv6():
    assert str(minimal_payload(source_ip="10.20.0.14").source_ip) == "10.20.0.14"
    assert minimal_payload(source_ip="2001:db8::1").source_ip is not None


def test_source_ip_rejects_non_address():
    with pytest.raises(ValidationError):
        minimal_payload(source_ip="not-an-ip")


def test_transport_protocol_accepts_enum_members():
    for member in SipTransportProtocol:
        assert minimal_payload(transport_protocol=member).transport_protocol == member


def test_transport_protocol_rejects_unknown():
    with pytest.raises(ValidationError):
        minimal_payload(transport_protocol="SCTP")


# ---------------------------------------------------------------
# Status enum — pinned to what the parser can actually emit
# ---------------------------------------------------------------


def test_registration_status_has_only_producible_values():
    """
    Pins the deliberate decision to declare only REGISTERED. The parser
    reads REGISTER requests and never responses or the Expires header,
    so UNREGISTERED and FAILED cannot be produced. If parser support
    lands, extend the enum and update this test consciously rather than
    discovering the mismatch downstream.
    """
    assert [m.value for m in RegistrationStatus] == ["REGISTERED"]


def test_registration_status_rejects_undeclared_value():
    with pytest.raises(ValidationError):
        minimal_payload(registration_status="FAILED")


# ---------------------------------------------------------------
# Envelope and registry
# ---------------------------------------------------------------


def test_schema_registered_under_its_own_identity():
    assert schema_registry.get_schema(EVENT_TYPE, SCHEMA_VERSION_V1) is SipRegistrationEvent


def test_does_not_collide_with_device_registration():
    """
    The parser previously emitted event_type 'device.registration',
    clashing with the provisioning schema. These must stay distinct.
    """
    device_schema = schema_registry.get_schema("device.registration", "v1")
    assert device_schema is not SipRegistrationEvent
    assert EVENT_TYPE == "sip.registration"


def test_payload_forbids_extra_fields():
    """Strictness at the ingestion boundary — an unknown field is a contract mismatch."""
    with pytest.raises(ValidationError):
        minimal_payload(session_duration=3600.0)
