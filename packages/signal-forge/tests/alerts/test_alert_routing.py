"""
Tests for the alert-routing unit: AcknowledgementRegistry (the
acknowledgement-state projection) and AlertRouter (the detection-to-
alert stream-table join).

Covers, for the registry: empty default, recording an acknowledgement,
the unknown-alert-is-not-acknowledged sentinel, idempotency on duplicate
and multiple acks. For the router: one alert per detection, alert_id
derived as a stable UUIDv5, severity and device_id carried through,
summary mapped from threshold_breached, trace propagation, the annotate-
not-suppress contract, and byte-identical determinism across runs.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from uuid import uuid4

from event_schema_contracts.alerts.alert_acknowledgement import (
    AlertAcknowledgementEvent,
    AlertAcknowledgementPayload,
)
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.detection.detection_event import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)

from signal_forge.alerts.acknowledgement_registry import AcknowledgementRegistry
from signal_forge.alerts.alert_router import AlertRouter
from event_schema_contracts.base.identity import derive


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _detection(
    *,
    detection_id=None,
    detection_type: str = "store.outage",
    severity: DetectionSeverity = DetectionSeverity.CRITICAL,
    store_id: str = "store-1",
    device_id=None,
    threshold_breached: str = "26 of 50 devices not reporting",
) -> DetectionEvent:
    now = _utc_now()
    det_id = detection_id if detection_id is not None else derive(
        "detection.store_outage", store_id
    )
    return DetectionEvent(
        event_timestamp=now,
        trace=TraceContext(trace_id=uuid4()),
        payload=DetectionEventPayload(
            detection_id=det_id,
            detection_type=detection_type,
            severity=severity,
            detected_at=now,
            store_id=store_id,
            device_id=device_id,
            source_event_id=derive("source.store_outage", store_id),
            threshold_breached=threshold_breached,
            details={"offline_count": 26, "registered_count": 50},
        ),
    )


def _ack_event(alert_id) -> AlertAcknowledgementEvent:
    now = _utc_now()
    return AlertAcknowledgementEvent(
        event_timestamp=now,
        trace=TraceContext(trace_id=uuid4()),
        payload=AlertAcknowledgementPayload(
            acknowledgement_id=uuid4(),
            alert_id=alert_id,
            acknowledged_at=now,
            acknowledged_by="oncall-nic",
        ),
    )


class AcknowledgementRegistryTest(unittest.TestCase):
    def test_empty_registry_acknowledges_nothing(self):
        reg = AcknowledgementRegistry()
        self.assertFalse(reg.is_acknowledged(uuid4()))
        self.assertEqual(reg.acknowledged_count(), 0)

    def test_observe_records_acknowledgement(self):
        reg = AcknowledgementRegistry()
        alert_id = uuid4()
        reg.observe_acknowledgement(_ack_event(alert_id))
        self.assertTrue(reg.is_acknowledged(alert_id))
        self.assertEqual(reg.acknowledged_count(), 1)

    def test_unknown_alert_not_acknowledged(self):
        reg = AcknowledgementRegistry()
        reg.observe_acknowledgement(_ack_event(uuid4()))
        self.assertFalse(reg.is_acknowledged(uuid4()))

    def test_duplicate_acknowledgement_is_idempotent(self):
        reg = AcknowledgementRegistry()
        alert_id = uuid4()
        reg.observe_acknowledgement(_ack_event(alert_id))
        reg.observe_acknowledgement(_ack_event(alert_id))  # second ack, same alert
        self.assertTrue(reg.is_acknowledged(alert_id))
        self.assertEqual(reg.acknowledged_count(), 1)


class AlertRouterTest(unittest.TestCase):
    def setUp(self):
        self.registry = AcknowledgementRegistry()
        self.router = AlertRouter(self.registry)

    def test_one_alert_per_detection(self):
        detections = [_detection(store_id="store-1"), _detection(store_id="store-2")]
        alerts = self.router.route(detections)
        self.assertEqual(len(alerts), 2)

    def test_empty_detections_produces_no_alerts(self):
        self.assertEqual(self.router.route([]), [])

    def test_alert_id_is_derived_v5(self):
        [alert] = self.router.route([_detection()])
        self.assertEqual(alert.payload.alert_id.version, 5)

    def test_alert_id_derived_from_detection_id(self):
        detection = _detection()
        [alert] = self.router.route([detection])
        expected = derive("alert", str(detection.payload.detection_id))
        self.assertEqual(alert.payload.alert_id, expected)

    def test_severity_carried_through(self):
        [alert] = self.router.route(
            [_detection(severity=DetectionSeverity.WARNING)]
        )
        self.assertEqual(alert.payload.severity, DetectionSeverity.WARNING)

    def test_device_id_carried_through(self):
        device_id = uuid4()
        [alert] = self.router.route(
            [_detection(detection_type="device.offline", device_id=device_id)]
        )
        self.assertEqual(alert.payload.device_id, device_id)

    def test_summary_from_threshold_breached(self):
        [alert] = self.router.route(
            [_detection(threshold_breached="silent for 320s")]
        )
        self.assertEqual(alert.payload.summary, "silent for 320s")

    def test_trace_propagated_from_detection(self):
        detection = _detection()
        [alert] = self.router.route([detection])
        self.assertEqual(alert.trace.trace_id, detection.trace.trace_id)

    def test_unacknowledged_alert_stamped_false(self):
        [alert] = self.router.route([_detection()])
        self.assertFalse(alert.payload.details["acknowledged"])

    def test_acknowledged_detection_still_produces_alert(self):
        # Annotate, never suppress: an acked alert is still routed,
        # stamped acknowledged=True.
        detection = _detection()
        alert_id = derive("alert", str(detection.payload.detection_id))
        self.registry.observe_acknowledgement(_ack_event(alert_id))

        alerts = self.router.route([detection])
        self.assertEqual(len(alerts), 1)
        self.assertTrue(alerts[0].payload.details["acknowledged"])

    def test_routing_is_byte_identical_across_runs(self):
        # The determinism property: the same detection routed twice
        # produces an identical alert payload (alert_id, severity,
        # everything) — the basis for replay byte-identity.
        detection = _detection()
        [a] = self.router.route([detection])
        [b] = self.router.route([detection])
        self.assertEqual(
            a.payload.model_dump_json(), b.payload.model_dump_json()
        )


if __name__ == "__main__":
    unittest.main()
