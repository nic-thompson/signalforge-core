"""
Tests for signal_forge.detection.detectors.OutageDetector.

Verifies the EmissionDetector contract for store-level outage
detection:

- Strict-greater-than threshold semantics: a store with exactly the
  threshold ratio of devices offline does NOT trigger; one device
  more offline DOES trigger.
- The detection carries store_id (from emission.partition_key),
  detection_type = "store.outage", severity = CRITICAL, and a
  details payload with offline_count, registered_count,
  offline_ratio, threshold_ratio, and window bounds.
- 'Once per transition into outage' semantics: a store that stays
  offline across multiple window emissions emits exactly one
  detection. Recovery and re-entry into outage emit again.
- Unregistered stores (registered_count_lookup returns None or 0)
  are skipped silently — no emission, no state mutation.
- The defensive clamp: reporting_count > registered_count is treated
  as 0 offline rather than negative.

The detector's registered_count_lookup callable is wired to a real
DeviceRegistry populated from registration events, replacing the
hand-built dict that Phase 2's tests used. Behaviour is unchanged;
the wiring is what's being verified. The outage detector doesn't
actually need a real registry (any callable satisfies it), but
using the registry here keeps the codebase consistent in how
detector tests express their "device-store world".
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from signal_forge.detection.detectors import OutageDetector
from signal_forge.detection.device_registry import DeviceRegistry
from signal_forge.streaming.window_aggregator import WindowEmission
from tests._fixtures.registration import make_registration_event


def _emission(
    *,
    store_id: str,
    reporting_count: int,
    window_seconds_offset: int = 0,
) -> WindowEmission:
    """
    Construct a WindowEmission as it would be produced by a
    DistinctCountAggregation over distinct device_ids per store.

    Tests typically need only partition_key, value, and the window
    bounds (used for detected_at). Other fields take sensible
    defaults.
    """
    window_start = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC) + timedelta(
        seconds=window_seconds_offset
    )
    window_end = window_start + timedelta(seconds=300)
    return WindowEmission(
        partition_key=store_id,
        aggregation_name="distinct_devices",
        window_start=window_start,
        window_end=window_end,
        value=reporting_count,
        event_count=reporting_count,  # one event per reporting device for tests
        is_repair=False,
        last_contributing_trace_id=None,
    )


def _populate_registry(
    registry: DeviceRegistry,
    *,
    counts: dict[str, int],
) -> None:
    """
    Register ``count`` anonymous devices in each named store.

    The outage detector cares about device counts per store, not
    about specific device identities, so anonymous fresh UUIDs are
    sufficient. Using the registry rather than a dict keeps the
    "build the device-store world from registration events" pattern
    consistent across detector tests, even where it's not strictly
    necessary.
    """
    for store_id, count in counts.items():
        for _ in range(count):
            registry.observe_registration(
                make_registration_event(device_id=uuid4(), store_id=store_id)
            )


class OutageDetectorTest(unittest.TestCase):
    def setUp(self) -> None:
        # 50 registered devices in store-1, 10 in store-2.
        self.registry = DeviceRegistry()
        _populate_registry(self.registry, counts={"store-1": 50, "store-2": 10})
        # Threshold 0.5 = "more than 50% offline triggers".
        self.detector = OutageDetector(
            threshold_ratio=0.5,
            registered_count_lookup=self.registry.device_count,
        )

    def test_below_threshold_does_not_emit(self):
        # 30 of 50 reporting = 20 offline = 40% offline. Below 50%.
        detections = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=30)
        )
        self.assertEqual(detections, [])

    def test_at_exact_threshold_does_not_emit(self):
        # 25 of 50 reporting = 25 offline = exactly 50%. Strict-greater
        # means this does not trigger.
        detections = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=25)
        )
        self.assertEqual(detections, [])

    def test_above_threshold_emits_once(self):
        # 20 of 50 reporting = 30 offline = 60%. Triggers.
        detections = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=20)
        )
        self.assertEqual(len(detections), 1)
        d = detections[0]
        self.assertEqual(d.payload.detection_type, "store.outage")
        self.assertEqual(d.payload.store_id, "store-1")
        self.assertIsNone(d.payload.device_id)
        self.assertEqual(d.payload.details["offline_count"], 30)
        self.assertEqual(d.payload.details["registered_count"], 50)
        self.assertEqual(d.payload.details["offline_ratio"], 0.6)
        self.assertEqual(d.payload.details["threshold_ratio"], 0.5)

    def test_consecutive_emissions_below_threshold_then_crossing_emits_once(self):
        # Two emissions below threshold (no detections), then one
        # above (one detection). Verifies the transition is the
        # trigger, not just 'any above-threshold observation'.
        self.assertEqual(
            self.detector.observe_emission(
                _emission(store_id="store-1", reporting_count=40)
            ),
            [],
        )
        self.assertEqual(
            self.detector.observe_emission(
                _emission(
                    store_id="store-1",
                    reporting_count=35,
                    window_seconds_offset=300,
                )
            ),
            [],
        )
        detections = self.detector.observe_emission(
            _emission(
                store_id="store-1",
                reporting_count=15,
                window_seconds_offset=600,
            )
        )
        self.assertEqual(len(detections), 1)

    def test_sustained_outage_emits_only_on_transition(self):
        # Three consecutive emissions, all above threshold. The first
        # transitions into outage and emits; the next two are
        # already-in-outage observations and emit nothing.
        first = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=10)
        )
        second = self.detector.observe_emission(
            _emission(
                store_id="store-1",
                reporting_count=5,
                window_seconds_offset=300,
            )
        )
        third = self.detector.observe_emission(
            _emission(
                store_id="store-1",
                reporting_count=8,
                window_seconds_offset=600,
            )
        )
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.assertEqual(third, [])

    def test_recovery_then_re_outage_emits_twice(self):
        # Outage, recovery (silent), outage again — two detections.
        # Pins the 'flapping store re-emits' semantics from D-7
        # mirrored to emission-based detection. Critical for long-
        # running operation: a store that toggles in and out of
        # outage state must produce a fresh detection on each entry.
        first = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=10)
        )
        # Recovery: 40 of 50 reporting = 20% offline, well below.
        recovery = self.detector.observe_emission(
            _emission(
                store_id="store-1",
                reporting_count=40,
                window_seconds_offset=300,
            )
        )
        second = self.detector.observe_emission(
            _emission(
                store_id="store-1",
                reporting_count=10,
                window_seconds_offset=600,
            )
        )
        self.assertEqual(len(first), 1)
        self.assertEqual(recovery, [])
        self.assertEqual(len(second), 1)
        # Distinct detection_ids — each transition is its own event.
        self.assertNotEqual(
            first[0].payload.detection_id,
            second[0].payload.detection_id,
        )

    def test_unregistered_store_does_not_emit(self):
        # Store "store-unknown" is not in the registry; device_count
        # returns 0. No detection, no state mutation.
        detections = self.detector.observe_emission(
            _emission(store_id="store-unknown", reporting_count=0)
        )
        self.assertEqual(detections, [])

    def test_zero_registered_devices_does_not_emit(self):
        # A store with 0 registered devices in the registry is
        # indistinguishable from an unregistered store — both return
        # device_count = 0. The dict-based fixture in Phase 2 could
        # distinguish "known but empty" from "unknown"; with a real
        # registry they collapse to the same case. The detector's
        # behaviour is correct: device_count returning 0 short-
        # circuits the ratio computation.
        detections = self.detector.observe_emission(
            _emission(store_id="store-empty", reporting_count=0)
        )
        self.assertEqual(detections, [])

    def test_reporting_exceeds_registered_clamps_to_zero_offline(self):
        # 60 reporting against 50 registered (registry hasn't caught
        # up after new device registration). offline_count clamps to
        # 0, ratio is 0, no outage.
        detections = self.detector.observe_emission(
            _emission(store_id="store-1", reporting_count=60)
        )
        self.assertEqual(detections, [])

    def test_threshold_validation_rejects_zero(self):
        with self.assertRaises(ValueError):
            OutageDetector(
                threshold_ratio=0.0,
                registered_count_lookup=self.registry.device_count,
            )

    def test_threshold_validation_rejects_one(self):
        with self.assertRaises(ValueError):
            OutageDetector(
                threshold_ratio=1.0,
                registered_count_lookup=self.registry.device_count,
            )
