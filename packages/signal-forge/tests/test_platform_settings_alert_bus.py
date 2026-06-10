"""
Tests for the alert-bus fields added to PlatformSettings in Phase 5:
alert_bus and replay_alert_bus, their validation, env-reading, and the
for_replay() swap.

These mirror the existing dataset-bucket coverage: defaults are None,
non-None values are validated at construction, from_env reads the
SF_ALERT_BUS / SF_REPLAY_ALERT_BUS variables, and for_replay() swaps the
active bus to the replay bus unconditionally (a missing replay bus
surfaces as None rather than silently retaining the live bus).
"""

from __future__ import annotations

import unittest

from signal_forge.config.platform_settings import PlatformSettings


class AlertBusDefaultsTest(unittest.TestCase):
    def test_alert_bus_fields_default_to_none(self):
        settings = PlatformSettings()
        self.assertIsNone(settings.alert_bus)
        self.assertIsNone(settings.replay_alert_bus)


class AlertBusValidationTest(unittest.TestCase):
    def test_valid_bus_names_accepted(self):
        settings = PlatformSettings(
            alert_bus="signalforge-prod-bus",
            replay_alert_bus="signalforge.replay_bus-1",
        )
        self.assertEqual(settings.alert_bus, "signalforge-prod-bus")
        self.assertEqual(settings.replay_alert_bus, "signalforge.replay_bus-1")

    def test_empty_alert_bus_rejected(self):
        with self.assertRaises(ValueError):
            PlatformSettings(alert_bus="")

    def test_illegal_character_rejected(self):
        with self.assertRaises(ValueError):
            PlatformSettings(alert_bus="bad bus name")  # space not allowed

    def test_overlong_bus_name_rejected(self):
        with self.assertRaises(ValueError):
            PlatformSettings(alert_bus="a" * 257)


class AlertBusFromEnvTest(unittest.TestCase):
    def test_reads_bus_vars(self):
        settings = PlatformSettings.from_env(
            {
                "SF_ALERT_BUS": "live-bus",
                "SF_REPLAY_ALERT_BUS": "replay-bus",
            }
        )
        self.assertEqual(settings.alert_bus, "live-bus")
        self.assertEqual(settings.replay_alert_bus, "replay-bus")

    def test_absent_bus_vars_default_to_none(self):
        settings = PlatformSettings.from_env({})
        self.assertIsNone(settings.alert_bus)
        self.assertIsNone(settings.replay_alert_bus)

    def test_empty_string_bus_var_is_none(self):
        # "" or None -> None, matching the dataset-bucket env idiom.
        settings = PlatformSettings.from_env({"SF_ALERT_BUS": ""})
        self.assertIsNone(settings.alert_bus)


class AlertBusForReplayTest(unittest.TestCase):
    def test_for_replay_swaps_active_bus_to_replay_bus(self):
        settings = PlatformSettings(
            alert_bus="live-bus",
            replay_alert_bus="replay-bus",
        )
        replay = settings.for_replay()
        self.assertEqual(replay.alert_bus, "replay-bus")
        self.assertEqual(replay.environment, "replay")

    def test_for_replay_with_no_replay_bus_yields_none(self):
        # Unconditional swap: a missing replay bus surfaces as None (a
        # no-op sink) rather than silently publishing replay alerts to
        # the live bus.
        settings = PlatformSettings(alert_bus="live-bus")
        replay = settings.for_replay()
        self.assertIsNone(replay.alert_bus)


if __name__ == "__main__":
    unittest.main()
