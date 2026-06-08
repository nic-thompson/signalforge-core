"""
Tests for event_schema_contracts.alerts.alert_acknowledgement.

Mirrors the structure of test_detection_event.py. Covers: schema
identity, registration, metadata auto-injection, the UUID-version
policy (acknowledgement_id is v4-only; alert_id is v4-or-v5), UTC
enforcement, field constraints, optional note, and envelope behaviour.
"""

from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_DNS, uuid1, uuid4, uuid5

import pytest
from pydantic import ValidationError

from event_schema_contracts.alerts.alert_acknowledgement import (
    AlertAcknowledgementEvent,
    AlertAcknowledgementPayload,
)
from event_schema_contracts.versioning.schema_registry import schema_registry


# ---------------------------------------------------------------------------
# Fixtures local to this module
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_ack_payload(utc_now):
    return AlertAcknowledgementPayload(
        acknowledgement_id=uuid4(),
        alert_id=uuid4(),
        acknowledged_at=utc_now,
        acknowledged_by="oncall-nic",
        note="investigating site router",
    )


@pytest.fixture
def valid_ack_event(trace_context, utc_now, valid_ack_payload):
    return AlertAcknowledgementEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=valid_ack_payload,
    )


# ---------------------------------------------------------------------------
# Schema identity and registration
# ---------------------------------------------------------------------------


def test_event_type_identity():
    assert AlertAcknowledgementEvent.__event_type__ == "alert.acknowledgement"
    assert AlertAcknowledgementEvent.__schema_version__ == "v1"


def test_metadata_auto_injected(trace_context, valid_ack_payload):
    event = AlertAcknowledgementEvent(
        event_timestamp=datetime.now(timezone.utc),
        trace=trace_context,
        payload=valid_ack_payload,
    )

    assert event.metadata.event_type == "alert.acknowledgement"
    assert event.metadata.schema_version == "v1"


def test_schema_registered():
    cls = schema_registry.get_schema("alert.acknowledgement", "v1")
    assert cls is AlertAcknowledgementEvent


# ---------------------------------------------------------------------------
# UUID-version policy
# ---------------------------------------------------------------------------


def test_alert_id_accepts_v4_and_v5(utc_now):
    # alert_id references the alert.event key, which may be a derived v5.
    for alert_id in (
        uuid4(),
        uuid5(NAMESPACE_DNS, "alert|detection-xyz"),
    ):
        payload = AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=alert_id,
            acknowledged_at=utc_now,
            acknowledged_by="oncall-nic",
        )
        assert payload.alert_id == alert_id


def test_alert_id_rejects_non_v4_v5(utc_now):
    with pytest.raises(ValidationError) as exc:
        AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=uuid1(),
            acknowledged_at=utc_now,
            acknowledged_by="oncall-nic",
        )

    assert exc.value.errors()[0]["loc"] == ("alert_id",)


def test_acknowledgement_id_accepts_v4(utc_now):
    ack_id = uuid4()
    payload = AlertAcknowledgementPayload(
        acknowledgement_id=ack_id,
        alert_id=uuid4(),
        acknowledged_at=utc_now,
        acknowledged_by="oncall-nic",
    )
    assert payload.acknowledgement_id == ack_id


def test_acknowledgement_id_rejects_v5(utc_now):
    # acknowledgement_id is v4-only: an ack is a genuinely new occurrence,
    # not a derived artefact, so a derived (v5) id is rejected.
    with pytest.raises(ValidationError) as exc:
        AlertAcknowledgementPayload(
            acknowledgement_id=uuid5(NAMESPACE_DNS, "ack|alert-xyz"),
            alert_id=uuid4(),
            acknowledged_at=utc_now,
            acknowledged_by="oncall-nic",
        )

    assert exc.value.errors()[0]["loc"] == ("acknowledgement_id",)


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------


def test_acknowledged_at_must_be_utc():
    with pytest.raises(ValidationError) as exc:
        AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=uuid4(),
            acknowledged_at=datetime.now(),  # naive
            acknowledged_by="oncall-nic",
        )

    assert "acknowledged_at" in str(exc.value)


def test_acknowledged_by_must_be_non_empty(utc_now):
    with pytest.raises(ValidationError):
        AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=uuid4(),
            acknowledged_at=utc_now,
            acknowledged_by="",
        )


def test_note_is_optional(utc_now):
    payload = AlertAcknowledgementPayload(
        acknowledgement_id=uuid4(),
        alert_id=uuid4(),
        acknowledged_at=utc_now,
        acknowledged_by="oncall-nic",
    )
    assert payload.note is None


# ---------------------------------------------------------------------------
# Envelope behaviour (event-level)
# ---------------------------------------------------------------------------


def test_event_timestamp_future_rejected(trace_context, valid_ack_payload):
    with pytest.raises(ValidationError):
        AlertAcknowledgementEvent(
            event_timestamp=datetime.now(timezone.utc) + timedelta(minutes=10),
            trace=trace_context,
            payload=valid_ack_payload,
        )


def test_event_is_immutable(valid_ack_event):
    with pytest.raises(ValidationError):
        valid_ack_event.event_id = uuid4()


def test_serialisation_round_trip(trace_context, utc_now):
    event = AlertAcknowledgementEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=uuid4(),
            acknowledged_at=utc_now,
            acknowledged_by="oncall-nic",
            note="investigating site router",
        ),
    )

    serialised = event.model_dump(mode="json")
    reconstructed = AlertAcknowledgementEvent(**serialised)

    assert reconstructed == event
