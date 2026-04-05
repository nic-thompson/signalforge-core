from event_schema_contracts.telemetry.session_event import (
    SessionStartEvent, 
    SessionStartPayload
) 
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4

from pydantic import ValidationError

import pytest

from event_schema_contracts.versioning.schema_registry import schema_registry

def test_session_start_event_valid(trace_context):
    event = SessionStartEvent(
        event_timestamp=datetime.now(timezone.utc),
        trace=trace_context,
        payload=SessionStartPayload(
            session_id=uuid4(),
            actor_id=uuid4(),
            started_at=datetime.now(timezone.utc),
        ),
    )

    assert event.metadata.event_type == "session.start"


def test_metadata_auto_injected(trace_context, valid_session_payload):
    event = SessionStartEvent(
        event_timestamp=datetime.now(timezone.utc),
        trace=trace_context,
        payload=valid_session_payload,
    )

    assert event.metadata.event_type == "session.start"
    assert event.metadata.schema_version == "v1"


def test_invalid_uuid_rejected():
    with pytest.raises(ValidationError) as exc:
        SessionStartPayload(
            session_id=UUID(int=0),
            actor_id=uuid4(),
            started_at=datetime.now(timezone.utc),
        )

    assert exc.value.errors()[0]["loc"] == ("session_id",)


def test_started_at_must_be_utc():
    with pytest.raises(ValidationError) as exc:
        SessionStartPayload(
            session_id=uuid4(),
            actor_id=uuid4(),
            started_at=datetime.now()
        )

    assert "started_at" in str(exc.value)


def test_event_timestamp_future_rejected(trace_context, valid_session_payload):
    with pytest.raises(ValidationError) as exc:
        SessionStartEvent(
            event_timestamp=datetime.now(timezone.utc) + timedelta(minutes=10),
            trace=trace_context,
            payload=valid_session_payload
        )

    errors = exc.value.errors()
    assert errors[0]["loc"] == ()


def test_schema_registered():
    cls = schema_registry.get_schema("session.start", "v1")
    assert cls is SessionStartEvent


def test_event_is_imutable(valid_event):
    with pytest.raises(ValidationError):
        valid_event.event_id = uuid4()


def test_session_event_serialisation_round_trip(trace_context, utc_now):
    event = SessionStartEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=SessionStartPayload(
            session_id=uuid4(),
            actor_id=uuid4(),
            started_at=utc_now,
        )
    )

    serialised = event.model_dump(mode="json")
    reconstructed = SessionStartEvent(**serialised)

    assert reconstructed == event


def test_optional_fields_supported(trace_context, utc_now):
    payload = SessionStartPayload(
        session_id=uuid4(),
        actor_id=uuid4(),
        started_at=utc_now,
        platform="ios",
        network_type="wifi",
    )

    event = SessionStartEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=payload,
    )
    
    assert event.payload.platform == "ios"


def test_metadata_identity_mismatch_rejected(trace_context, utc_now):
    from event_schema_contracts.base.metadata import EventMetadata

    with pytest.raises(ValidationError):
        SessionStartEvent(
            event_timestamp=utc_now,
            trace=trace_context,
            metadata=EventMetadata(
                event_type="wrong.event",
                schema_version="v1",
                source="test",
            ),
            payload=SessionStartPayload(
                session_id=uuid4(),
                actor_id=uuid4(),
                started_at=utc_now,
            ),
        )


def test_client_version_semver_valid(utc_now):
    payload = SessionStartPayload(
        session_id=uuid4(),
        actor_id=uuid4(),
        started_at=utc_now,
        client_version="1.2.3-beta",
    )

    assert payload.client_version == "1.2.3-beta"


def test_event_timestamp_within_clock_skew_allowed(trace_context, valid_session_payload, utc_now):
    event = SessionStartEvent(
        event_timestamp=utc_now + timedelta(minutes=4),
        trace=trace_context,
        payload=valid_session_payload,
    )

    assert event is not None