"""
Tests for signal_forge.dashboards.projection_store: the ProjectionStore
protocol and the InMemoryProjectionStore implementation.

Covers protocol conformance, the None cold-start sentinel, put/get
round-trip and overwrite, view scoping, sorted keys, the no-op delete
contract, and that values are persisted opaquely (the store never parses
them).
"""

from __future__ import annotations

import unittest

from signal_forge.dashboards.projection_store import (
    InMemoryProjectionStore,
    ProjectionStore,
)


class ProjectionStoreProtocolTest(unittest.TestCase):
    def test_in_memory_store_satisfies_protocol(self):
        self.assertIsInstance(InMemoryProjectionStore(), ProjectionStore)

    def test_object_without_methods_does_not_satisfy_protocol(self):
        class NotAStore:
            pass

        self.assertNotIsInstance(NotAStore(), ProjectionStore)


class InMemoryProjectionStoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.store = InMemoryProjectionStore()

    def test_absent_key_returns_none(self):
        self.assertIsNone(self.store.get("offline_count", "store-1"))

    def test_put_then_get_round_trips(self):
        self.store.put("offline_count", "store-1", "3")
        self.assertEqual(self.store.get("offline_count", "store-1"), "3")

    def test_put_overwrites(self):
        self.store.put("offline_count", "store-1", "3")
        self.store.put("offline_count", "store-1", "5")
        self.assertEqual(self.store.get("offline_count", "store-1"), "5")

    def test_views_are_scoped(self):
        # The same key in two views is two independent values.
        self.store.put("offline_count", "store-1", "3")
        self.store.put("active_outages", "store-1", "1")
        self.assertEqual(self.store.get("offline_count", "store-1"), "3")
        self.assertEqual(self.store.get("active_outages", "store-1"), "1")

    def test_keys_returns_sorted_view_keys(self):
        self.store.put("offline_count", "store-3", "1")
        self.store.put("offline_count", "store-1", "1")
        self.store.put("offline_count", "store-2", "1")
        self.assertEqual(
            self.store.keys("offline_count"),
            ["store-1", "store-2", "store-3"],
        )

    def test_keys_of_empty_view_is_empty(self):
        self.assertEqual(self.store.keys("nonexistent"), [])

    def test_delete_removes_key(self):
        self.store.put("offline_count", "store-1", "3")
        self.store.delete("offline_count", "store-1")
        self.assertIsNone(self.store.get("offline_count", "store-1"))

    def test_delete_absent_key_is_noop(self):
        # Neither an absent key nor an absent view raises.
        self.store.delete("offline_count", "store-1")
        self.store.delete("nonexistent", "store-1")

    def test_value_is_stored_opaquely(self):
        # The store persists the serialised string verbatim; it does not
        # parse or interpret structure.
        payload = '{"count": 4, "window_start": 100}'
        self.store.put("anomaly_rate", "latency", payload)
        self.assertEqual(self.store.get("anomaly_rate", "latency"), payload)


if __name__ == "__main__":
    unittest.main()
