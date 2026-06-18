"""
Tests for the replay CLI shell.

The CLI holds no replay logic, so these tests pin its parse-and-delegate
surface:

1. ``run_from_config`` constructs settings from the ``settings`` block via
   ``from_env`` and passes them through to the driver with the config's *live*
   names intact (the driver, not the CLI, applies for_replay);
2. it builds the event source from the ``window`` block and feeds it through;
3. ``main`` reads a JSON file, parses it, and delegates;
4. ``main`` returns a usage error (exit 2) when not given exactly one config
   path, without attempting a run;
5. the production ``build_event_source`` and ``build_pipeline`` are deferred —
   they raise NotImplementedError with a pointer, so an un-injected production
   invocation fails loudly rather than silently doing nothing.

Settings and pipeline are exercised through real ``PlatformSettings.from_env``
(it is stdlib-only, no AWS) but a faked driver path: the event source and
pipeline builder are injected, so no archive read and no real pipeline are
needed to prove the CLI wires its arguments correctly.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from signal_forge.config.platform_settings import PlatformSettings
from signal_forge.replay import __main__ as cli


def _config(**settings: str) -> dict[str, Any]:
    return {
        "window": {
            "start_time": "2026-06-01T00:00:00+00:00",
            "end_time": "2026-06-01T01:00:00+00:00",
            "event_pattern": {"source": ["telemetry"]},
        },
        "settings": settings,
    }


@dataclass
class _FakeResult:
    detections: list[Any] = field(default_factory=list)
    emissions: list[Any] = field(default_factory=list)


class _CapturingPipeline:
    """Captures the settings its builder was handed and the events processed."""

    def __init__(self, settings: PlatformSettings) -> None:
        self.settings = settings
        self.seen: list[Any] = []

    def process_batch(self, events: Iterable[Any]) -> list[Any]:
        self.seen = list(events)
        return [_FakeResult() for _ in self.seen]


class RunFromConfigTest(unittest.TestCase):
    def test_settings_come_from_the_settings_block_via_from_env(self) -> None:
        built: list[_CapturingPipeline] = []

        def build_pipeline(settings: PlatformSettings) -> Any:
            pipeline = _CapturingPipeline(settings)
            built.append(pipeline)
            return pipeline

        cli.run_from_config(
            _config(
                SF_DATASET_BUCKET="sf-live-dataset",
                SF_REPLAY_DATASET_BUCKET="sf-replay-dataset",
                SF_ENV="production",
            ),
            build_event_source=lambda window: [object(), object()],
            build_pipeline=build_pipeline,
        )

        (pipeline,) = built
        # The driver applies for_replay(), so the builder sees the replay
        # environment and the swapped bucket — proof the live names from the
        # config flowed through from_env and the driver did the swap.
        self.assertEqual(pipeline.settings.environment, "replay")
        self.assertEqual(pipeline.settings.dataset_bucket, "sf-replay-dataset")

    def test_event_source_is_built_from_the_window_block(self) -> None:
        seen_windows: list[Mapping[str, Any]] = []

        def build_event_source(window: Mapping[str, Any]) -> Iterable[Any]:
            seen_windows.append(window)
            return [object(), object(), object()]

        captured: list[_CapturingPipeline] = []

        def build_pipeline(settings: PlatformSettings) -> Any:
            pipeline = _CapturingPipeline(settings)
            captured.append(pipeline)
            return pipeline

        cli.run_from_config(
            _config(SF_DATASET_BUCKET="sf-live-dataset"),
            build_event_source=build_event_source,
            build_pipeline=build_pipeline,
        )

        (window,) = seen_windows
        self.assertEqual(window["start_time"], "2026-06-01T00:00:00+00:00")
        self.assertEqual(window["event_pattern"], {"source": ["telemetry"]})
        (pipeline,) = captured
        self.assertEqual(len(pipeline.seen), 3)  # the three source events ran


class MainTest(unittest.TestCase):
    def test_main_reads_json_file_and_delegates(self) -> None:
        captured: list[_CapturingPipeline] = []

        def build_pipeline(settings: PlatformSettings) -> Any:
            pipeline = _CapturingPipeline(settings)
            captured.append(pipeline)
            return pipeline

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "replay.json"
            path.write_text(
                json.dumps(_config(SF_DATASET_BUCKET="sf-live-dataset")),
                encoding="utf-8",
            )
            code = cli.main(
                [str(path)],
                build_event_source=lambda window: [object()],
                build_pipeline=build_pipeline,
            )

        self.assertEqual(code, 0)
        self.assertEqual(len(captured), 1)

    def test_main_usage_error_on_wrong_argument_count(self) -> None:
        ran: list[Any] = []

        # No args -> usage error, exit 2, no run attempted.
        code = cli.main(
            [],
            build_event_source=lambda window: ran.append(window) or [],
            build_pipeline=lambda settings: ran.append(settings),
        )
        self.assertEqual(code, 2)
        self.assertEqual(ran, [])

        # Two args -> same.
        code = cli.main(
            ["a.json", "b.json"],
            build_event_source=lambda window: ran.append(window) or [],
            build_pipeline=lambda settings: ran.append(settings),
        )
        self.assertEqual(code, 2)
        self.assertEqual(ran, [])


class DeferredProductionSeamsTest(unittest.TestCase):
    def test_production_event_source_is_deferred(self) -> None:
        with self.assertRaises(NotImplementedError):
            list(cli.build_event_source({"start_time": "x"}))

    def test_production_pipeline_builder_is_deferred(self) -> None:
        with self.assertRaises(NotImplementedError):
            cli._production_build_pipeline(PlatformSettings.from_env(env={}))


if __name__ == "__main__":
    unittest.main()
