"""
Tests for the deterministic identity fields produced at the detection
construction sites, and for this project's expectations of the derivation
they use.

The derivation itself now lives in event-schema-contracts. It was
implemented here first — Phase 4 needed replay-deterministic ids before
the schema library had anything to offer — and independently in that
library's own test fixtures, which is two definitions of one derivation
with nothing comparing them. They agreed, but nothing made them agree:
had either drifted, the same logical record would have resolved to two
ids and every join across them would have split silently.

DeriveHelperTest is kept even though the library tests its own function,
because these are a consumer's assertions rather than duplicates. If a
future version re-based the namespace, every id this project has ever
produced would change, and the replay byte-identity guarantee would
break. Asserting it here catches that when the dependency is bumped
rather than when a dataset comparison fails.

The rest covers this project's own behaviour:

- OfflineDetector, OutageDetector, AnomalyDetector each produce the same
  detection_id / source_event_id / envelope event_id when fed identical
  (stable) coordinates across two independent runs
- the envelope event_id and the detection_id differ (role separation)
- different windows / devices yield different detection ids

The full byte-identical live-vs-replay assertion over serialised Parquet
is the Phase 4 replay-isolation test.
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_DNS, UUID, uuid5

from event_schema_contracts.base.identity import NAMESPACE, derive

from signal_forge.detection.detectors import (
    AnomalyDetector,
    OfflineDetector,
    OutageDetector,
)
from signal_forge.detection.device_registry import DeviceRegistry
from tests._fixtures.dataset import emission
from tests._fixtures.events import FakeEvent
from tests._fixtures.payloads import FakeDevicePayload
from tests._fixtures.registration import make_registration_event

_WS = datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
_WE = datetime(2026, 5, 29, 14, 0, 5, tzinfo=UTC)


class DeriveHelperTest(unittest.TestCase):
    def test_namespace_is_dns_derived_and_frozen(self) -> None:
        # Pins the constant: any drift in the namespace re-bases every id.
        self.assertEqual(NAMESPACE, uuid5(NAMESPACE_DNS, "signalforge.analytics"))

    def test_same_role_and_parts_is_stable(self) -> None:
        self.assertEqual(derive("role.x", "store-1", 5), derive("role.x", "store-1", 5))

    def test_returns_uuid5(self) -> None:
        self.assertEqual(derive("role.x", "store-1").version, 5)

    def test_role_distinguishes_identical_parts(self) -> None:
        # A detection_id and an envelope event_id built from the same
        # coordinates must not collide — the role keeps them apart.
        self.assertNotEqual(derive("detection", "x"), derive("event", "x"))

    def test_parts_distinguish_identical_role(self) -> None:
        self.assertNotEqual(derive("r", "store-1"), derive("r", "store-2"))


class AnomalyDeterminismTest(unittest.TestCase):
    def _detect(self) -> object:
        detector = AnomalyDetector(threshold=300.0, signal_name="latency_ms")
        out = detector.observe_emission(
            emission(
                partition_key="store-1",
                aggregation_name="signal_value",
                value=312.0,
                window_start=_WS,
                window_end=_WE,
            )
        )
        self.assertEqual(len(out), 1)
        return out[0]

    def test_ids_stable_across_independent_runs(self) -> None:
        a = self._detect()
        b = self._detect()
        self.assertEqual(a.payload.detection_id, b.payload.detection_id)
        self.assertEqual(a.payload.source_event_id, b.payload.source_event_id)
        self.assertEqual(a.event_id, b.event_id)

    def test_event_id_differs_from_detection_id(self) -> None:
        d = self._detect()
        self.assertNotEqual(d.event_id, d.payload.detection_id)

    def test_detection_id_and_source_event_id_differ(self) -> None:
        d = self._detect()
        self.assertNotEqual(d.payload.detection_id, d.payload.source_event_id)

    def test_different_window_yields_different_detection_id(self) -> None:
        first = self._detect()
        detector = AnomalyDetector(threshold=300.0, signal_name="latency_ms")
        later = detector.observe_emission(
            emission(
                partition_key="store-1",
                aggregation_name="signal_value",
                value=312.0,
                window_start=_WS + timedelta(seconds=5),
                window_end=_WE + timedelta(seconds=5),
            )
        )[0]
        self.assertNotEqual(first.payload.detection_id, later.payload.detection_id)


class OutageDeterminismTest(unittest.TestCase):
    def _detect(self) -> object:
        detector = OutageDetector(
            threshold_ratio=0.5,
            registered_count_lookup=lambda _store: 50,
        )
        out = detector.observe_emission(
            emission(
                partition_key="store-1",
                aggregation_name="distinct_devices",
                value=10,  # 10 of 50 reporting -> 0.8 offline, triggers
                window_start=_WS,
                window_end=_WE,
            )
        )
        self.assertEqual(len(out), 1)
        return out[0]

    def test_ids_stable_across_independent_runs(self) -> None:
        a = self._detect()
        b = self._detect()
        self.assertEqual(a.payload.detection_id, b.payload.detection_id)
        self.assertEqual(a.payload.source_event_id, b.payload.source_event_id)
        self.assertEqual(a.event_id, b.event_id)


class OfflineDeterminismTest(unittest.TestCase):
    def setUp(self) -> None:
        self.device_a = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        self.device_b = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
        # Fixed triggering-event id so source_event_id is stable across runs
        # (in real replay the archived event carries this id unchanged).
        self.trigger_event_id = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")

    def _registry(self) -> DeviceRegistry:
        registry = DeviceRegistry()
        registry.observe_registration(
            make_registration_event(device_id=self.device_a, store_id="store-1")
        )
        registry.observe_registration(
            make_registration_event(device_id=self.device_b, store_id="store-2")
        )
        return registry

    def _event(self, seconds: int, device_id: UUID, event_id: UUID | None = None):
        kwargs = {}
        if event_id is not None:
            kwargs["event_id"] = event_id
        return FakeEvent(
            event_type="device.registration",
            schema_version="v1",
            event_timestamp=datetime(2026, 5, 29, 14, 0, 0, tzinfo=UTC)
            + timedelta(seconds=seconds),
            payload=FakeDevicePayload(device_id=device_id),
            **kwargs,
        )

    def _detect(self) -> object:
        registry = self._registry()
        detector = OfflineDetector(
            threshold_seconds=300,
            device_id_extractor=lambda e: (
                e.payload.device_id if e.payload is not None else None
            ),
            store_lookup=registry.store_for,
        )
        detector.observe_event(self._event(0, self.device_a))
        out = detector.observe_event(
            self._event(350, self.device_b, event_id=self.trigger_event_id)
        )
        self.assertEqual(len(out), 1)
        return out[0]

    def test_ids_stable_across_independent_runs(self) -> None:
        a = self._detect()
        b = self._detect()
        self.assertEqual(a.payload.detection_id, b.payload.detection_id)
        self.assertEqual(a.event_id, b.event_id)
        # source_event_id is the (fixed) triggering event's id.
        self.assertEqual(a.payload.source_event_id, self.trigger_event_id)


if __name__ == "__main__":
    unittest.main()
