import pytest
from uuid import uuid4
from datetime import datetime, timezone
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.telemetry.session_event import (
    SessionStartPayload,
    SessionStartEvent
)

@pytest.fixture
def utc_now():
    return datetime.now(timezone.utc)


@pytest.fixture
def trace_context():
    return TraceContext(
        trace_id=uuid4(),
    )


@pytest.fixture
def valid_session_payload(utc_now):
    return SessionStartPayload(
        session_id=uuid4(),
        actor_id=uuid4(),
        started_at=utc_now
    )

@pytest.fixture
def valid_event(trace_context, utc_now):
    return SessionStartEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=SessionStartPayload(
            session_id=uuid4(),
            actor_id=uuid4(),
            started_at=utc_now
        ),
    )