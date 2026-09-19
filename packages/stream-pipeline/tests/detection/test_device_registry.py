"""
Tests for stream_pipeline.detection.device_registry.DeviceRegistry.

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
(device_id, store_id) from the upstream payload. The construction
helper lives in ``tests/_fixtures/registration.py`` since the
detector tests also need it.
"""

from __future__ import annotations

import unittest
from uuid import uuid4

from stream_pipeline.detection.device_registry import DeviceRegistry
from tests._fixtures.registration import make_registration_event


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
            make_registration_event(device_id=device_id, store_id="store-1")
        )
        self.assertEqual(self.registry.store_for(device_id), "store-1")
        self.assertEqual(self.registry.device_count("store-1"), 1)

    def test_multiple_devices_in_one_store_aggregate(self):
        device_a, device_b, device_c = uuid4(), uuid4(), uuid4()
        for device_id in (device_a, device_b, device_c):
            self.registry.observe_registration(
                make_registration_event(device_id=device_id, store_id="store-1")
            )
        self.assertEqual(self.registry.device_count("store-1"), 3)
        # Each device resolves to the same store.
        self.assertEqual(self.registry.store_for(device_a), "store-1")
        self.assertEqual(self.registry.store_for(device_b), "store-1")
        self.assertEqual(self.registry.store_for(device_c), "store-1")

    def test_multiple_stores_partition_correctly(self):
        device_a, device_b, device_c = uuid4(), uuid4(), uuid4()
        self.registry.observe_registration(
            make_registration_event(device_id=device_a, store_id="store-1")
        )
        self.registry.observe_registration(
            make_registration_event(device_id=device_b, store_id="store-2")
        )
        self.registry.observe_registration(
            make_registration_event(device_id=device_c, store_id="store-2")
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
                make_registration_event(device_id=device_id, store_id="store-1")
            )
        # device_count still 1, not 3.
        self.assertEqual(self.registry.device_count("store-1"), 1)
        self.assertEqual(self.registry.store_for(device_id), "store-1")

    def test_re_registration_to_different_store_moves_device(self):
        # Device A starts at store-1.
        device_a = uuid4()
        self.registry.observe_registration(
            make_registration_event(device_id=device_a, store_id="store-1")
        )
        self.assertEqual(self.registry.store_for(device_a), "store-1")
        self.assertEqual(self.registry.device_count("store-1"), 1)

        # Re-registered to store-2.
        self.registry.observe_registration(
            make_registration_event(device_id=device_a, store_id="store-2")
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
            make_registration_event(device_id=device_a, store_id="store-1")
        )
        self.registry.observe_registration(
            make_registration_event(device_id=device_b, store_id="store-1")
        )
        self.assertEqual(self.registry.known_stores(), {"store-1"})

        # Both move to store-2.
        self.registry.observe_registration(
            make_registration_event(device_id=device_a, store_id="store-2")
        )
        # After the first move, store-1 still has device_b, so it's
        # still in known_stores.
        self.assertEqual(self.registry.known_stores(), {"store-1", "store-2"})
        self.registry.observe_registration(
            make_registration_event(device_id=device_b, store_id="store-2")
        )
        # Now store-1 is empty and removed.
        self.assertEqual(self.registry.known_stores(), {"store-2"})

    def test_registered_device_count_sums_across_stores(self):
        self.registry.observe_registration(
            make_registration_event(device_id=uuid4(), store_id="store-1")
        )
        self.registry.observe_registration(
            make_registration_event(device_id=uuid4(), store_id="store-2")
        )
        self.registry.observe_registration(
            make_registration_event(device_id=uuid4(), store_id="store-2")
        )
        self.assertEqual(self.registry.registered_device_count(), 3)

    def test_known_stores_returns_defensive_copy(self):
        self.registry.observe_registration(
            make_registration_event(device_id=uuid4(), store_id="store-1")
        )
        snapshot = self.registry.known_stores()
        # Mutating the returned set must not affect the registry.
        snapshot.add("store-fake")
        self.assertEqual(self.registry.known_stores(), {"store-1"})
