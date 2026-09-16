"""
Tests for signal_forge.alerts.eventbridge_alert_sink.

Uses moto's @mock_aws to back a real boto3 EventBridge client. The tests
assert on what the sink sends — that put_events accepts every entry with
no failures, that the entry shape is correct, that severity rides on
DetailType, and that alerts are chunked to EventBridge's 10-entry cap —
rather than observing delivery through a rule-to-target hop (moto's
target templating needs an optional dependency the suite does not pull
in, and target delivery is infra's concern, not this sink's).

Also covers the no-op path: an unconfigured bus constructs no client and
publishes nothing.
"""

from __future__ import annotations

import json
import unittest
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from event_schema_contracts.alerts.alert_event import (
    AlertEvent,
    AlertEventPayload,
)
from event_schema_contracts.base.identity import derive
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection.detection_event import DetectionSeverity
from moto import mock_aws

from signal_forge.alerts.eventbridge_alert_sink import EventBridgeAlertSink
from signal_forge.config.platform_settings import PlatformSettings

_BUS = "sf-test-bus"
_REGION = "us-east-1"


def _alert(
    store_id: str = "store-1",
    severity: DetectionSeverity = DetectionSeverity.CRITICAL,
) -> AlertEvent:
    now = datetime.now(UTC)
    detection_id = derive("detection.store_outage", store_id)
    return AlertEvent(
        event_timestamp=now,
        trace=TraceContext(trace_id=uuid4()),
        payload=AlertEventPayload(
            alert_id=derive("alert", str(detection_id)),
            detection_id=detection_id,
            detection_type="store.outage",
            severity=severity,
            routed_at=now,
            store_id=store_id,
            device_id=None,
            summary="26 of 50 devices not reporting",
        ),
    )


class _CapturingClient:
    """Wraps a real moto events client, recording the Entries sent."""

    def __init__(self, inner):  # type: ignore[no-untyped-def]
        self._inner = inner
        self.batches: list[list[dict]] = []

    def put_events(self, *, Entries):  # type: ignore[no-untyped-def]
        self.batches.append(Entries)
        return self._inner.put_events(Entries=Entries)


class EventBridgeAlertSinkNoopTest(unittest.TestCase):
    def test_unconfigured_bus_is_noop(self):
        # No bus configured: no client constructed, publish does nothing
        # and does not raise.
        sink = EventBridgeAlertSink(settings=PlatformSettings(alert_bus=None))
        sink.publish([_alert()])  # must not raise
        sink.publish([])


class EventBridgeAlertSinkPublishTest(unittest.TestCase):
    @mock_aws
    def test_put_events_accepts_all_entries(self):
        events = boto3.client("events", region_name=_REGION)
        events.create_event_bus(Name=_BUS)
        capturing = _CapturingClient(events)

        sink = EventBridgeAlertSink(
            settings=PlatformSettings(alert_bus=_BUS),
            client_factory=lambda: capturing,
        )
        sink.publish([_alert("store-1"), _alert("store-2")])

        # One batch of two entries, accepted with no failures.
        self.assertEqual(len(capturing.batches), 1)
        self.assertEqual(len(capturing.batches[0]), 2)

    @mock_aws
    def test_entry_shape_and_detail_type(self):
        events = boto3.client("events", region_name=_REGION)
        events.create_event_bus(Name=_BUS)
        capturing = _CapturingClient(events)

        sink = EventBridgeAlertSink(
            settings=PlatformSettings(alert_bus=_BUS),
            client_factory=lambda: capturing,
        )
        sink.publish([_alert("store-1", DetectionSeverity.WARNING)])

        entry = capturing.batches[0][0]
        self.assertEqual(
            sorted(entry.keys()),
            ["Detail", "DetailType", "EventBusName", "Source"],
        )
        self.assertEqual(entry["Source"], "signalforge.alerts")
        self.assertEqual(entry["DetailType"], "WARNING:store.outage")
        self.assertEqual(entry["EventBusName"], _BUS)
        # Detail is the full alert serialised as JSON.
        detail = json.loads(entry["Detail"])
        self.assertEqual(detail["payload"]["store_id"], "store-1")

    @mock_aws
    def test_alerts_chunked_to_ten_per_batch(self):
        events = boto3.client("events", region_name=_REGION)
        events.create_event_bus(Name=_BUS)
        capturing = _CapturingClient(events)

        sink = EventBridgeAlertSink(
            settings=PlatformSettings(alert_bus=_BUS),
            client_factory=lambda: capturing,
        )
        alerts = [_alert(f"store-{i}") for i in range(23)]
        sink.publish(alerts)

        batch_sizes = [len(b) for b in capturing.batches]
        self.assertEqual(batch_sizes, [10, 10, 3])
        self.assertEqual(sum(batch_sizes), 23)

    @mock_aws
    def test_empty_batch_sends_nothing(self):
        events = boto3.client("events", region_name=_REGION)
        events.create_event_bus(Name=_BUS)
        capturing = _CapturingClient(events)

        sink = EventBridgeAlertSink(
            settings=PlatformSettings(alert_bus=_BUS),
            client_factory=lambda: capturing,
        )
        sink.publish([])

        self.assertEqual(capturing.batches, [])


if __name__ == "__main__":
    unittest.main()
