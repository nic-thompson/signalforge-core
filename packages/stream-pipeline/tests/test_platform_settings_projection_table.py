"""
Tests for the projection-table fields added to PlatformSettings in
Phase 6: projection_table and replay_projection_table, their validation,
env-reading, and the for_replay() swap.

Mirrors test_platform_settings_alert_bus.py exactly: defaults are None,
non-None values are validated at construction, from_env reads the
SF_PROJECTION_TABLE / SF_REPLAY_PROJECTION_TABLE variables, and
for_replay() swaps the active table to the replay table unconditionally
(a missing replay table surfaces as None rather than silently retaining
the live table — the safer failure mode that keeps replay projections
off the live store).
"""

from __future__ import annotations

import unittest

from stream_pipeline.config.platform_settings import PlatformSettings


class ProjectionTableDefaultsTest(unittest.TestCase):
    def test_projection_table_fields_default_to_none(self):
        settings = PlatformSettings()
        self.assertIsNone(settings.projection_table)
        self.assertIsNone(settings.replay_projection_table)


class ProjectionTableValidationTest(unittest.TestCase):
    def test_valid_table_names_accepted(self):
        settings = PlatformSettings(
            projection_table="signalforge-prod-projections",
            replay_projection_table="signalforge.replay_projections-1",
        )
        self.assertEqual(
            settings.projection_table, "signalforge-prod-projections"
        )
        self.assertEqual(
            settings.replay_projection_table,
            "signalforge.replay_projections-1",
        )

    def test_too_short_table_name_rejected(self):
        # DynamoDB requires >= 3 chars.
        with self.assertRaises(ValueError):
            PlatformSettings(projection_table="ab")

    def test_illegal_character_rejected(self):
        with self.assertRaises(ValueError):
            PlatformSettings(projection_table="bad table name")  # spaces

    def test_overlong_table_name_rejected(self):
        with self.assertRaises(ValueError):
            PlatformSettings(projection_table="a" * 256)


class ProjectionTableFromEnvTest(unittest.TestCase):
    def test_reads_table_vars(self):
        settings = PlatformSettings.from_env(
            {
                "SF_PROJECTION_TABLE": "live-projections",
                "SF_REPLAY_PROJECTION_TABLE": "replay-projections",
            }
        )
        self.assertEqual(settings.projection_table, "live-projections")
        self.assertEqual(
            settings.replay_projection_table, "replay-projections"
        )

    def test_absent_table_vars_default_to_none(self):
        settings = PlatformSettings.from_env({})
        self.assertIsNone(settings.projection_table)
        self.assertIsNone(settings.replay_projection_table)

    def test_empty_string_table_var_is_none(self):
        settings = PlatformSettings.from_env({"SF_PROJECTION_TABLE": ""})
        self.assertIsNone(settings.projection_table)


class ProjectionTableForReplayTest(unittest.TestCase):
    def test_for_replay_swaps_active_table_to_replay_table(self):
        settings = PlatformSettings(
            projection_table="live-projections",
            replay_projection_table="replay-projections",
        )
        replay = settings.for_replay()
        self.assertEqual(replay.projection_table, "replay-projections")
        self.assertEqual(replay.environment, "replay")

    def test_for_replay_with_no_replay_table_yields_none(self):
        # Unconditional swap: a missing replay table surfaces as None (a
        # no-op store) rather than silently writing replay projections to
        # the live table.
        settings = PlatformSettings(projection_table="live-projections")
        replay = settings.for_replay()
        self.assertIsNone(replay.projection_table)


if __name__ == "__main__":
    unittest.main()
