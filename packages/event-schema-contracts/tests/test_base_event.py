from datetime import datetime, timezone
from uuid import uuid4

import pytest

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext

class DummyPayload:
    pass

class DummyEvent(BaseEvent[dict]):
    __event_type__ = "test.event"
    __schema_version__ = "v1"

def test_valid_base_event_creation():
    event = DummyEvent(
        metadata=EventMetadata(
            schema_version="v1",
            event_type="test.event",
            source="pytest"
        ),
        trace=TraceContext(trace_id=uuid4()),
        event_timestamp=datetime.now(timezone.utc),
        payload={"key": "value"}
    )

    assert event.event_id is not None
    assert event.metadata.event_type == "test.event"

def test_event_timestamp_requires_timezone():
    with pytest.raises(ValueError):
        DummyEvent(
            metadata=EventMetadata(
                schema_version="v1",
                event_type="test.event",
                source="pytest"
            ),
            trace=TraceContext(trace_id=uuid4),
            event_timestamp=datetime.now(),
            payload={"key": "value"},
        )