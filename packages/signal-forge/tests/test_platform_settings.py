"""
Tests for signal_forge.config.platform_settings.

Covers:

- production-safe defaults
- range validation at construction
- environment-variable parsing
- replay-snapshot semantics
- immutability (frozen dataclass)
- equality / hashing — required for replay-determinism assertions
"""

from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from signal_forge.config.platform_settings import (
    DEFAULT_DATA_RETENTION_DAYS,
    DEFAULT_LATE_EVENT_TOLERANCE_SECONDS,
    DEFAULT_OFFLINE_THRESHOLD_SECONDS,
    DEFAULT_OUTAGE_THRESHOLD_RATIO,
    DEFAULT_REALTIME_WINDOW_SECONDS,
    PlatformSettings,
)


class PlatformSettingsDefaultsTest(unittest.TestCase):
    def test_defaults_match_platform_brief(self):
        settings = PlatformSettings()

        self.assertEqual(
            settings.realtime_window_seconds, DEFAULT_REALTIME_WINDOW_SECONDS
        )
        self.assertEqual(
            settings.offline_threshold_seconds, DEFAULT_OFFLINE_THRESHOLD_SECONDS
        )
        self.assertEqual(
            settings.outage_threshold_ratio, DEFAULT_OUTAGE_THRESHOLD_RATIO
        )
        self.assertEqual(
            settings.late_event_tolerance_seconds,
            DEFAULT_LATE_EVENT_TOLERANCE_SECONDS,
        )
        # 730 = 2 years retention as required by the platform brief
        self.assertEqual(settings.data_retention_days, DEFAULT_DATA_RETENTION_DAYS)
        self.assertEqual(settings.data_retention_days, 730)

    def test_dataset_bucket_defaults_to_none(self) -> None:
        settings = PlatformSettings()
        self.assertIsNone(settings.dataset_bucket)

    def test_replay_dataset_bucket_defaults_to_none(self) -> None:
        settings = PlatformSettings()
        self.assertIsNone(settings.replay_dataset_bucket)


class PlatformSettingsValidationTest(unittest.TestCase):
    def test_rejects_zero_realtime_window(self):
        with self.assertRaises(ValueError):
            PlatformSettings(realtime_window_seconds=0)

    def test_rejects_negative_offline_threshold(self):
        with self.assertRaises(ValueError):
            PlatformSettings(offline_threshold_seconds=-1)

    def test_rejects_outage_ratio_above_one(self):
        with self.assertRaises(ValueError):
            PlatformSettings(outage_threshold_ratio=1.5)

    def test_rejects_outage_ratio_zero(self):
        # Ratio must be strictly positive — a zero ratio would mean any
        # store with at least one offline device is in outage, which is the
        # dashboard / detection equivalent of /dev/null.
        with self.assertRaises(ValueError):
            PlatformSettings(outage_threshold_ratio=0.0)

    def test_accepts_outage_ratio_one(self):
        # Exactly one is permitted — only-when-everything-is-down policy.
        PlatformSettings(outage_threshold_ratio=1.0)

    def test_accepts_zero_lateness(self):
        # Zero lateness disables late-event correction. That's a valid mode
        # for strict-ordering pipelines and must be permitted.
        PlatformSettings(late_event_tolerance_seconds=0)

    def test_rejects_negative_lateness(self):
        with self.assertRaises(ValueError):
            PlatformSettings(late_event_tolerance_seconds=-1)

class PlatformSettingsDatasetBucketValidationTest(unittest.TestCase):
    """
    Validation of the dataset_bucket and replay_dataset_bucket fields
    against the loose AWS S3 bucket-naming rules.
    """

    def test_accepts_valid_bucket_name(self) -> None:
        settings = PlatformSettings(dataset_bucket="signal-forge-prod")
        self.assertEqual(settings.dataset_bucket, "signal-forge-prod")

    def test_rejects_uppercase_in_bucket_name(self) -> None:
        with self.assertRaises(ValueError):
            PlatformSettings(dataset_bucket="Signal-Forge-Prod")

    def test_rejects_bucket_name_starting_with_hyphen(self) -> None:
        with self.assertRaises(ValueError):
            PlatformSettings(dataset_bucket="-signal-forge")

    def test_rejects_bucket_name_ending_with_hyphen(self) -> None:
        with self.assertRaises(ValueError):
            PlatformSettings(dataset_bucket="signal-forge-")

    def test_rejects_bucket_name_too_short(self) -> None:
        with self.assertRaises(ValueError):
            PlatformSettings(dataset_bucket="sf")

    def test_rejects_bucket_name_too_long(self) -> None:
        with self.assertRaises(ValueError):
            # 64 chars, one over the limit
            PlatformSettings(dataset_bucket="a" * 64)

    def test_validates_replay_dataset_bucket_too(self) -> None:
        with self.assertRaises(ValueError):
            PlatformSettings(replay_dataset_bucket="INVALID")


class PlatformSettingsImmutabilityTest(unittest.TestCase):
    def test_frozen_settings_cannot_mutate(self):
        settings = PlatformSettings()

        with self.assertRaises(FrozenInstanceError):
            settings.realtime_window_seconds = 99  # type: ignore[misc]

    def test_settings_are_hashable(self):
        # Hashability is required so replay drivers can use settings as a
        # dict key when caching deterministic computation results.
        settings_a = PlatformSettings()
        settings_b = PlatformSettings()
        self.assertEqual(hash(settings_a), hash(settings_b))
        self.assertEqual(settings_a, settings_b)

    def test_distinct_configs_are_unequal(self):
        a = PlatformSettings(realtime_window_seconds=5)
        b = PlatformSettings(realtime_window_seconds=10)
        self.assertNotEqual(a, b)


class PlatformSettingsFromEnvTest(unittest.TestCase):
    def test_from_env_uses_defaults_when_unset(self):
        settings = PlatformSettings.from_env(env={})
        self.assertEqual(
            settings.realtime_window_seconds, DEFAULT_REALTIME_WINDOW_SECONDS
        )

    def test_from_env_parses_integer_overrides(self):
        settings = PlatformSettings.from_env(
            env={
                "SF_REALTIME_WINDOW_SECONDS": "10",
                "SF_DATA_RETENTION_DAYS": "365",
                "SF_ENV": "staging",
            }
        )
        self.assertEqual(settings.realtime_window_seconds, 10)
        self.assertEqual(settings.data_retention_days, 365)
        self.assertEqual(settings.environment, "staging")

    def test_from_env_loads_dataset_buckets(self) -> None:
        settings = PlatformSettings.from_env(
            env={
                "SF_DATASET_BUCKET": "signal-forge-prod",
                "SF_REPLAY_DATASET_BUCKET": "signal-forge-replay",
            }
        )
        self.assertEqual(settings.dataset_bucket, "signal-forge-prod")
        self.assertEqual(settings.replay_dataset_bucket, "signal-forge-replay")

    def test_from_env_treats_empty_bucket_as_none(self) -> None:
        settings = PlatformSettings.from_env(
            env={"SF_DATASET_BUCKET": "", "SF_REPLAY_DATASET_BUCKET": ""}
        )
        self.assertIsNone(settings.dataset_bucket)
        self.assertIsNone(settings.replay_dataset_bucket)

    def test_from_env_parses_float_overrides(self):
        settings = PlatformSettings.from_env(
            env={"SF_OUTAGE_THRESHOLD_RATIO": "0.75"}
        )
        self.assertEqual(settings.outage_threshold_ratio, 0.75)

    def test_from_env_rejects_garbage_int(self):
        with self.assertRaises(ValueError):
            PlatformSettings.from_env(env={"SF_REALTIME_WINDOW_SECONDS": "not-an-int"})

    def test_from_env_rejects_garbage_float(self):
        with self.assertRaises(ValueError):
            PlatformSettings.from_env(env={"SF_OUTAGE_THRESHOLD_RATIO": "nope"})


class PlatformSettingsReplayTest(unittest.TestCase):
    def test_for_replay_preserves_analytical_parameters(self):
        original = PlatformSettings(realtime_window_seconds=7, environment="prod")
        replayed = original.for_replay()

        # Analytical parameters MUST be byte-identical for replay
        # determinism. Only the environment label changes.
        self.assertEqual(
            replayed.realtime_window_seconds, original.realtime_window_seconds
        )
        self.assertEqual(replayed.environment, "replay")
        self.assertEqual(original.environment, "prod")

    def test_for_replay_accepts_custom_environment(self):
        original = PlatformSettings()
        replayed = original.for_replay(replay_environment="replay-2026-04-30")
        self.assertEqual(replayed.environment, "replay-2026-04-30")

    def test_for_replay_swaps_dataset_bucket(self) -> None:
        live = PlatformSettings(
            dataset_bucket="signal-forge-prod",
            replay_dataset_bucket="signal-forge-replay",
        )
        replay = live.for_replay()
        self.assertEqual(replay.dataset_bucket, "signal-forge-replay")

    def test_for_replay_swaps_to_none_when_no_replay_bucket(self) -> None:
        live = PlatformSettings(dataset_bucket="signal-forge-prod")
        replay = live.for_replay()
        self.assertIsNone(replay.dataset_bucket)


if __name__ == "__main__":
    unittest.main()
