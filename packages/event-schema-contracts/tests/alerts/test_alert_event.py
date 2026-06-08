"""
Tests for event_schema_contracts.alerts.alert_event.

Mirrors the structure of test_detection_event.py. Covers: schema
identity, registration, metadata auto-injection, the UUID-version
policy on alert_id and detection_id, UTC enforcement, field
constraints, the reused DetectionSeverity enum, device_id optionality,
and envelope behaviour (future-timestamp rejection, immutability,
serialisation round-trip).
"""

from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_DNS, uuid1, uuid4, uuid5

import pytest
from pydantic import ValidationError

from event_schema_contracts.alerts.alert_event import (
    AlertEvent,
    AlertEventPayload,
)
from event_schema_contracts.detection.detection_event import DetectionSeverity
from event_schema_contracts.versioning.schema_registry import schema_registry


# ---------------------------------------------------------------------------
# Fixtures local to this module
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_alert_payload(utc_now):
    return AlertEventPayload(
        alert_id=uuid4(),
        detection_id=uuid4(),
        detection_type="store.outage",
        severity=DetectionSeverity.CRITICAL,
        routed_at=utc_now,
        store_id="store-001",
        device_id=None,
        summary="26 of 50 devices not reporting",
        details={"offline_count": 26, "registered_count": 50},
    )


@pytest.fixture
def valid_alert_event(trace_context, utc_now, valid_alert_payload):
    return AlertEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=valid_alert_payload,
    )


# ---------------------------------------------------------------------------
# Schema identity and registration
# ---------------------------------------------------------------------------


def test_event_type_identity():
    assert AlertEvent.__event_type__ == "alert.event"
    assert AlertEvent.__schema_version__ == "v1"


def test_metadata_auto_injected(trace_context, valid_alert_payload):
    event = AlertEvent(
        event_timestamp=datetime.now(timezone.utc),
        trace=trace_context,
        payload=valid_alert_payload,
    )

    assert event.metadata.event_type == "alert.event"
    assert event.metadata.schema_version == "v1"


def test_schema_registered():
    cls = schema_registry.get_schema("alert.event", "v1")
    assert cls is AlertEvent


# ---------------------------------------------------------------------------
# UUID-version policy
# ---------------------------------------------------------------------------


def _payload_with(*, alert_id, detection_id, utc_now):
    return AlertEventPayload(
        alert_id=alert_id,
        detection_id=detection_id,
        detection_type="store.outage",
        severity=DetectionSeverity.CRITICAL,
        routed_at=utc_now,
        store_id="store-001",
        summary="some summary",
        details={},
    )


def test_alert_id_accepts_v4_and_v5(utc_now):
    # The alert router derives alert_id as a UUIDv5 from the detection
    # id so it survives a replay byte-for-byte; v4 is also accepted.
    for alert_id in (
        uuid4(),
        uuid5(NAMESPACE_DNS, "alert|detection-xyz"),
    ):
        payload = _payload_with(
            alert_id=alert_id, detection_id=uuid4(), utc_now=utc_now
        )
        assert payload.alert_id == alert_id


def test_alert_id_rejects_non_v4_v5(utc_now):
    with pytest.raises(ValidationError) as exc:
        _payload_with(alert_id=uuid1(), detection_id=uuid4(), utc_now=utc_now)

    assert exc.value.errors()[0]["loc"] == ("alert_id",)


def test_detection_id_accepts_v4_and_v5(utc_now):
    for detection_id in (
        uuid4(),
        uuid5(NAMESPACE_DNS, "detection.store_outage|store-001"),
    ):
        payload = _payload_with(
            alert_id=uuid4(), detection_id=detection_id, utc_now=utc_now
        )
        assert payload.detection_id == detection_id


def test_detection_id_rejects_non_v4_v5(utc_now):
    with pytest.raises(ValidationError) as exc:
        _payload_with(alert_id=uuid4(), detection_id=uuid1(), utc_now=utc_now)

    assert exc.value.errors()[0]["loc"] == ("detection_id",)


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------


def test_routed_at_must_be_utc():
    with pytest.raises(ValidationError) as exc:
        AlertEventPayload(
            alert_id=uuid4(),
            detection_id=uuid4(),
            detection_type="store.outage",
            severity=DetectionSeverity.CRITICAL,
            routed_at=datetime.now(),  # naive
            store_id="store-001",
            summary="some summary",
        )

    assert "routed_at" in str(exc.value)


def test_detection_type_must_match_pattern(utc_now):
    for bad in ("", "Store.Outage", "outage"):
        with pytest.raises(ValidationError):
            AlertEventPayload(
                alert_id=uuid4(),
                detection_id=uuid4(),
                detection_type=bad,
                severity=DetectionSeverity.CRITICAL,
                routed_at=utc_now,
                store_id="store-001",
                summary="some summary",
            )


def test_store_id_must_be_non_empty(utc_now):
    with pytest.raises(ValidationError):
        AlertEventPayload(
            alert_id=uuid4(),
            detection_id=uuid4(),
            detection_type="store.outage",
            severity=DetectionSeverity.CRITICAL,
            routed_at=utc_now,
            store_id="",
            summary="some summary",
        )


def test_summary_must_be_non_empty(utc_now):
    with pytest.raises(ValidationError):
        AlertEventPayload(
            alert_id=uuid4(),
            detection_id=uuid4(),
            detection_type="store.outage",
            severity=DetectionSeverity.CRITICAL,
            routed_at=utc_now,
            store_id="store-001",
            summary="",
        )


def test_device_id_optional_for_store_scoped_alerts(utc_now):
    payload = AlertEventPayload(
        alert_id=uuid4(),
        detection_id=uuid4(),
        detection_type="store.outage",
        severity=DetectionSeverity.CRITICAL,
        routed_at=utc_now,
        store_id="store-001",
        device_id=None,
        summary="26 of 50 devices not reporting",
    )
    assert payload.device_id is None


def test_device_id_carried_for_device_scoped_alerts(utc_now):
    device_id = uuid4()
    payload = AlertEventPayload(
        alert_id=uuid4(),
        detection_id=uuid4(),
        detection_type="device.offline",
        severity=DetectionSeverity.WARNING,
        routed_at=utc_now,
        store_id="store-001",
        device_id=device_id,
        summary="silent for 320s",
    )
    assert payload.device_id == device_id


def test_details_defaults_to_empty_dict(utc_now):
    payload = AlertEventPayload(
        alert_id=uuid4(),
        detection_id=uuid4(),
        detection_type="store.outage",
        severity=DetectionSeverity.CRITICAL,
        routed_at=utc_now,
        store_id="store-001",
        summary="some summary",
    )
    assert payload.details == {}


# ---------------------------------------------------------------------------
# Severity (reused DetectionSeverity)
# ---------------------------------------------------------------------------


def test_severity_is_detection_severity(utc_now):
    payload = AlertEventPayload(
        alert_id=uuid4(),
        detection_id=uuid4(),
        detection_type="store.outage",
        severity="CRITICAL",  # accepted as the enum value
        routed_at=utc_now,
        store_id="store-001",
        summary="some summary",
    )
    assert payload.severity is DetectionSeverity.CRITICAL


def test_invalid_severity_rejected(utc_now):
    with pytest.raises(ValidationError):
        AlertEventPayload(
            alert_id=uuid4(),
            detection_id=uuid4(),
            detection_type="store.outage",
            severity="MAJOR",  # not a valid DetectionSeverity
            routed_at=utc_now,
            store_id="store-001",
            summary="some summary",
        )


# ---------------------------------------------------------------------------
# Envelope behaviour (event-level)
# ---------------------------------------------------------------------------


def test_event_timestamp_future_rejected(trace_context, valid_alert_payload):
    with pytest.raises(ValidationError):
        AlertEvent(
            event_timestamp=datetime.now(timezone.utc) + timedelta(minutes=10),
            trace=trace_context,
            payload=valid_alert_payload,
        )


def test_event_is_immutable(valid_alert_event):
    with pytest.raises(ValidationError):
        valid_alert_event.event_id = uuid4()


def test_serialisation_round_trip(trace_context, utc_now):
    event = AlertEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=AlertEventPayload(
            alert_id=uuid4(),
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            routed_at=utc_now,
            store_id="store-001",
            device_id=uuid4(),
            summary="silent for 320s",
            details={"silent_seconds": 320},
        ),
    )

    serialised = event.model_dump(mode="json")
    reconstructed = AlertEvent(**serialised)

    assert reconstructed == event
