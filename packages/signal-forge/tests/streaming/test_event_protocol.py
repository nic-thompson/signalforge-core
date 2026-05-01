"""
Tests for signal_forge.streaming.event_protocol.

Confirms that the protocol contracts:

- accept structurally-conforming objects (the FakeEvent test double)
- expose all fields that downstream modules will read
- pass isinstance() checks at runtime via @runtime_checkable

These tests guard against accidental regressions where a future refactor
removes a field or weakens a type without realising the streaming layer
depends on it.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime

from signal_forge.streaming.event_protocol import (
    EventMetadataLike,
    TelemetryEvent,
    TraceContextLike,
)
from tests._fixtures.events import FakeEvent


class TelemetryEventProtocolTest(unittest.TestCase):
    def test_fake_event_is_structurally_a_telemetry_event(self):
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=datetime(2026, 4, 30, tzinfo=UTC),
        )
        self.assertIsInstance(event, TelemetryEvent)

    def test_fake_event_metadata_is_structurally_event_metadata(self):
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=datetime(2026, 4, 30, tzinfo=UTC),
        )
        self.assertIsInstance(event.metadata, EventMetadataLike)

    def test_fake_event_trace_is_structurally_trace_context(self):
        event = FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=datetime(2026, 4, 30, tzinfo=UTC),
        )
        self.assertIsInstance(event.trace, TraceContextLike)

    def test_object_missing_fields_is_not_a_telemetry_event(self):
        # A bare object should NOT satisfy the protocol. This guards against
        # the streaming layer accepting under-specified inputs.
        class Bare:
            pass

        self.assertNotIsInstance(Bare(), TelemetryEvent)


if __name__ == "__main__":
    unittest.main()
