"""
Tests for signal_forge.detection.device_registry.DeviceRegistry.

Verifies the projection contract for the device -> store mapping:

- The empty registry returns None / 0 for unknown queries.
- A first registration populates both internal dicts and is visible
  from both query directions (store_for and device_count).
- Multiple devices in a single store are aggregated correctly.
- Devices across multiple stores are partitioned correctly.
- Re-observing an identical registration is idempotent.
- Re-registering a device to a different store moves it between
  stores' device sets and removes empty stores from known_stores.
- The diagnostic methods (registered_device_count, known_stores)
  reflect the state accurately and return defensive copies.

The tests construct real DeviceRegistrationEvent objects rather than
fakes because the registry's whole purpose is to read specific fields
(device_id, store_id) from the upstream payload. Faking the payload
would require either a Protocol-shaped registry handler (premature
abstraction) or a fake that satisfies pydantic's class identity check
(fragile).
"""

from __future__ import annotations

import unittest
from datetime import UTC, datetime
from uuid import UUID, uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.telemetry.device_event import (
    DeviceRegistrationEvent,
    DeviceRegistrationPayload,
    DeviceType,
)

from signal_forge.detection.device_registry import DeviceRegistry


def _registration_event(
    *,
    device_id: UUID | None = None,
    store_id: str = "store-1",
    device_type: DeviceType = DeviceType.SENSOR,
    firmware_version: str | None = None,
    registered_at: datetime | None = None,
    event_timestamp: datetime | None = None,
) -> DeviceRegistrationEvent:
    """
    Build a real DeviceRegistrationEvent with sensible defaults.

    Tests override only the fields they care about; everything else
    takes a default. The helper is inline in this test file rather
    than in tests/_fixtures/events.py because that fixture file is
    deliberately stdlib-only (it avoids pydantic imports). Promote
    to a shared fixture if Phase 4 or 5 tests start needing real
    registration events too.
    """
    if device_id is None:
        device_id = uuid4()
    if registered_at is None:
        registered_at = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)
    if event_timestamp is None:
        event_timestamp = registered_at

    payload = DeviceRegistrationPayload(
        device_id=device_id,
        store_id=store_id,
        device_type=device_type,
        firmware_version=firmware_version,
        registered_at=registered_at,
    )
    return DeviceRegistrationEvent(
        event_timestamp=event_timestamp,
        trace=TraceContext(),
        payload=payload,
    )


class DeviceRegistryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = DeviceRegistry()

    def test_empty_registry_returns_none_for_unknown_device(self):
        self.assertIsNone(self.registry.store_for(uuid4()))

    def test_empty_registry_returns_zero_for_unknown_store(self):
        self.assertEqual(self.registry.device_count("store-1"), 0)

    def test_first_registration_populates_both_query_directions(self):
        device_id = uuid4()
        self.registry.observe_registration(
            _registration_event(device_id=device_id, store_id="store-1")
        )
        self.assertEqual(self.registry.store_for(device_id), "store-1")
        self.assertEqual(self.registry.device_count("store-1"), 1)

    def test_multiple_devices_in_one_store_aggregate(self):
        device_a, device_b, device_c = uuid4(), uuid4(), uuid4()
        for device_id in (device_a, device_b, device_c):
            self.registry.observe_registration(
                _registration_event(device_id=device_id, store_id="store-1")
            )
        self.assertEqual(self.registry.device_count("store-1"), 3)
        # Each device resolves to the same store.
        self.assertEqual(self.registry.store_for(device_a), "store-1")
        self.assertEqual(self.registry.store_for(device_b), "store-1")
        self.assertEqual(self.registry.store_for(device_c), "store-1")

    def test_multiple_stores_partition_correctly(self):
        device_a, device_b, device_c = uuid4(), uuid4(), uuid4()
        self.registry.observe_registration(
            _registration_event(device_id=device_a, store_id="store-1")
        )
        self.registry.observe_registration(
            _registration_event(device_id=device_b, store_id="store-2")
        )
        self.registry.observe_registration(
            _registration_event(device_id=device_c, store_id="store-2")
        )

        self.assertEqual(self.registry.store_for(device_a), "store-1")
        self.assertEqual(self.registry.store_for(device_b), "store-2")
        self.assertEqual(self.registry.store_for(device_c), "store-2")
        self.assertEqual(self.registry.device_count("store-1"), 1)
        self.assertEqual(self.registry.device_count("store-2"), 2)

    def test_duplicate_registration_is_idempotent(self):
        device_id = uuid4()
        for _ in range(3):
            self.registry.observe_registration(
                _registration_event(device_id=device_id, store_id="store-1")
            )
        # device_count still 1, not 3.
        self.assertEqual(self.registry.device_count("store-1"), 1)
        self.assertEqual(self.registry.store_for(device_id), "store-1")

    def test_re_registration_to_different_store_moves_device(self):
        # Device A starts at store-1.
        device_a = uuid4()
        self.registry.observe_registration(
            _registration_event(device_id=device_a, store_id="store-1")
        )
        self.assertEqual(self.registry.store_for(device_a), "store-1")
        self.assertEqual(self.registry.device_count("store-1"), 1)

        # Re-registered to store-2.
        self.registry.observe_registration(
            _registration_event(device_id=device_a, store_id="store-2")
        )

        # store_for reflects the new store.
        self.assertEqual(self.registry.store_for(device_a), "store-2")
        # Old store's device_count drops to 0; store-2's is 1.
        self.assertEqual(self.registry.device_count("store-1"), 0)
        self.assertEqual(self.registry.device_count("store-2"), 1)

    def test_re_registration_drops_empty_store_from_known_stores(self):
        # Two devices at store-1, then both move to store-2.
        device_a, device_b = uuid4(), uuid4()
        self.registry.observe_registration(
            _registration_event(device_id=device_a, store_id="store-1")
        )
        self.registry.observe_registration(
            _registration_event(device_id=device_b, store_id="store-1")
        )
        self.assertEqual(self.registry.known_stores(), {"store-1"})

        # Both move to store-2.
        self.registry.observe_registration(
            _registration_event(device_id=device_a, store_id="store-2")
        )
        # After the first move, store-1 still has device_b, so it's
        # still in known_stores.
        self.assertEqual(self.registry.known_stores(), {"store-1", "store-2"})
        self.registry.observe_registration(
            _registration_event(device_id=device_b, store_id="store-2")
        )
        # Now store-1 is empty and removed.
        self.assertEqual(self.registry.known_stores(), {"store-2"})

    def test_registered_device_count_sums_across_stores(self):
        self.registry.observe_registration(
            _registration_event(device_id=uuid4(), store_id="store-1")
        )
        self.registry.observe_registration(
            _registration_event(device_id=uuid4(), store_id="store-2")
        )
        self.registry.observe_registration(
            _registration_event(device_id=uuid4(), store_id="store-2")
        )
        self.assertEqual(self.registry.registered_device_count(), 3)

    def test_known_stores_returns_defensive_copy(self):
        self.registry.observe_registration(
            _registration_event(device_id=uuid4(), store_id="store-1")
        )
        snapshot = self.registry.known_stores()
        # Mutating the returned set must not affect the registry.
        snapshot.add("store-fake")
        self.assertEqual(self.registry.known_stores(), {"store-1"})
