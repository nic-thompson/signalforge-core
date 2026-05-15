"""
Tests for signal_forge.detection.detectors.OfflineDetector.

Verifies the EventDetector contract for offline detection:

- A device's first event registers it as seen.
- A subsequent event that crosses the silence threshold emits exactly
  one DetectionEvent for the silent device.
- The detection carries store_id (via store_lookup), device_id,
  source_event_id (the event that triggered the scan, not the
  silent device's last event), and a threshold_breached string
  describing the gap.
- Events without a device_id (extractor returns None) are skipped
  entirely, including the silence scan.
- Devices for which store_lookup returns None are skipped at
  emission time — the DetectionEvent schema requires non-empty
  store_id, so unrooted devices produce no detection.
- A device that goes offline and then receives an event transitions
  back to seen silently. No emission on recovery.
- A device that flaps (seen -> offline -> seen -> offline) emits a
  fresh detection on each new offline transition. Pins the
  'once per offline transition event' semantics from D-7.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from signal_forge.detection.detectors import OfflineDetector
from signal_forge.streaming.event_protocol import TelemetryEvent
from tests._fixtures.events import FakeEvent
from tests._fixtures.payloads import FakeDevicePayload


def _event_at(seconds: int, *, device_id: UUID | None = None) -> TelemetryEvent:
    """Build a FakeEvent at a deterministic timestamp, optionally with a device payload."""
    payload = FakeDevicePayload(device_id=device_id) if device_id is not None else None
    return FakeEvent(
        event_type="device.registration",
        schema_version="v1",
        event_timestamp=datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)
        + timedelta(seconds=seconds),
        payload=payload,
    )


def _extract(event: TelemetryEvent) -> UUID | None:
    """Extractor for tests: read device_id from FakeDevicePayload, or None."""
    payload = event.payload
    return payload.device_id if payload is not None else None


class OfflineDetectorTest(unittest.TestCase):
    def setUp(self) -> None:
        # Threshold 300s matches PlatformSettings's production default.
        self.device_a = uuid4()
        self.device_b = uuid4()
        self.stores = {self.device_a: "store-1", self.device_b: "store-2"}
        self.detector = OfflineDetector(
            threshold_seconds=300,
            device_id_extractor=_extract,
            store_lookup=lambda d: self.stores.get(d),
        )

    def test_first_event_registers_device_as_seen_no_emission(self):
        event = _event_at(0, device_id=self.device_a)
        detections = self.detector.observe_event(event)
        self.assertEqual(detections, [])

    def test_device_crossing_threshold_emits_one_detection(self):
        # Device A seen at t=0. Device B's event at t=350 advances time
        # past the threshold for A.
        self.detector.observe_event(_event_at(0, device_id=self.device_a))
        detections = self.detector.observe_event(
            _event_at(350, device_id=self.device_b)
        )

        # Exactly one detection — for device A, not B (B's last_seen
        # was just updated).
        self.assertEqual(len(detections), 1)
        d = detections[0]
        self.assertEqual(d.payload.device_id, self.device_a)
        self.assertEqual(d.payload.store_id, "store-1")
        self.assertEqual(d.payload.detection_type, "device.offline")
        # source_event_id is the SCANNING event (B's), not A's last event.
        # This is the "scan-on-event" semantics: time advances when an
        # event arrives, so that event is the one that "discovered"
        # the offline condition.
        self.assertEqual(d.payload.threshold_breached, "no events for 350s (threshold 300s)")
        self.assertEqual(d.payload.details, {"silent_seconds": 350, "threshold_seconds": 300})

    def test_event_without_device_id_is_skipped(self):
        # Event with no payload, no device. Even though its timestamp
        # would normally cross device A's threshold, the detector
        # cannot attribute time to a device and skips the entire
        # observation — no scan, no emission.
        self.detector.observe_event(_event_at(0, device_id=self.device_a))
        detections = self.detector.observe_event(_event_at(350))
        self.assertEqual(detections, [])

    def test_unregistered_device_does_not_emit(self):
        # Device U is not in self.stores; store_lookup returns None.
        # Even after crossing the threshold, no detection emits (the
        # schema requires non-empty store_id).
        device_u = uuid4()
        self.detector.observe_event(_event_at(0, device_id=device_u))
        detections = self.detector.observe_event(
            _event_at(350, device_id=self.device_a)
        )
        self.assertEqual(detections, [])

    def test_device_returning_to_seen_after_offline_emits_nothing(self):
        # Device A goes offline, then sends an event. The recovery
        # transition is silent — no DetectionEvent.
        self.detector.observe_event(_event_at(0, device_id=self.device_a))
        offline_detections = self.detector.observe_event(
            _event_at(350, device_id=self.device_b)
        )
        self.assertEqual(len(offline_detections), 1)  # confirm setup
        # Device A returns at t=400. No detection.
        recovery_detections = self.detector.observe_event(
            _event_at(400, device_id=self.device_a)
        )
        self.assertEqual(recovery_detections, [])

    def test_flapping_device_emits_on_each_offline_transition(self):
        # The headline test: device A goes offline, recovers, goes
        # offline again — emits twice, once per fresh offline
        # transition. This is what makes alert reminder cadence
        # (Phase 5's concern) the right place for "still offline"
        # rather than baking it into the detector.
        self.detector.observe_event(_event_at(0, device_id=self.device_a))
        first_offline = self.detector.observe_event(
            _event_at(350, device_id=self.device_b)
        )
        # A recovers.
        self.detector.observe_event(_event_at(400, device_id=self.device_a))
        # B advances time again, A has been silent since 400.
        second_offline = self.detector.observe_event(
            _event_at(750, device_id=self.device_b)
        )

        self.assertEqual(len(first_offline), 1)
        self.assertEqual(len(second_offline), 1)
        # Both detections are for device A.
        self.assertEqual(first_offline[0].payload.device_id, self.device_a)
        self.assertEqual(second_offline[0].payload.device_id, self.device_a)
        # Distinct detection_ids — each transition is its own event.
        self.assertNotEqual(
            first_offline[0].payload.detection_id,
            second_offline[0].payload.detection_id,
        )
