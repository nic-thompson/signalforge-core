"""
Tests for signal_forge.alerts.alert_sink: the AlertSink protocol and
the InMemoryAlertSink no-AWS implementation.

Covers: protocol conformance (structural + runtime_checkable), recording
published alerts across calls, order preservation, the empty-batch
no-op-but-counted contract, and that a plain object missing publish()
does not satisfy the protocol.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from uuid import uuid4

from event_schema_contracts.alerts.alert_event import (
    AlertEvent,
    AlertEventPayload,
)
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection.detection_event import DetectionSeverity

from signal_forge.alerts.alert_sink import AlertSink, InMemoryAlertSink
from signal_forge.identity import derive


def _alert(store_id: str = "store-1") -> AlertEvent:
    now = datetime.now(UTC)
    detection_id = derive("detection.store_outage", store_id)
    alert_id = derive("alert", str(detection_id))
    return AlertEvent(
        event_timestamp=now,
        trace=TraceContext(trace_id=uuid4()),
        payload=AlertEventPayload(
            alert_id=alert_id,
            detection_id=detection_id,
            detection_type="store.outage",
            severity=DetectionSeverity.CRITICAL,
            routed_at=now,
            store_id=store_id,
            device_id=None,
            summary="26 of 50 devices not reporting",
        ),
    )


class AlertSinkProtocolTest(unittest.TestCase):
    def test_in_memory_sink_satisfies_protocol(self):
        self.assertIsInstance(InMemoryAlertSink(), AlertSink)

    def test_object_without_publish_does_not_satisfy_protocol(self):
        class NotASink:
            pass

        self.assertNotIsInstance(NotASink(), AlertSink)


class InMemoryAlertSinkTest(unittest.TestCase):
    def test_starts_empty(self):
        sink = InMemoryAlertSink()
        self.assertEqual(sink.published, [])
        self.assertEqual(sink.publish_calls, 0)

    def test_publish_records_alerts(self):
        sink = InMemoryAlertSink()
        a, b = _alert("store-1"), _alert("store-2")
        sink.publish([a, b])
        self.assertEqual(sink.published, [a, b])
        self.assertEqual(sink.publish_calls, 1)

    def test_publish_preserves_order_across_calls(self):
        sink = InMemoryAlertSink()
        a, b, c = _alert("store-1"), _alert("store-2"), _alert("store-3")
        sink.publish([a])
        sink.publish([b, c])
        self.assertEqual(sink.published, [a, b, c])
        self.assertEqual(sink.publish_calls, 2)

    def test_empty_batch_is_noop_but_counts_as_a_call(self):
        sink = InMemoryAlertSink()
        sink.publish([])
        self.assertEqual(sink.published, [])
        self.assertEqual(sink.publish_calls, 1)


if __name__ == "__main__":
    unittest.main()
