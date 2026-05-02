"""
Tests for .github/workflows/scripts/check_test_count.py.

A CI guard that silently exits 0 when it should exit 1 is worse than no
guard at all. These tests pin the script's exit-code contract so we can
catch regressions in the guard itself.

The script writes diagnostic messages to stdout/stderr by design - those
messages are what a human reads in CI logs. During tests we redirect
those streams into buffers so test output stays clean and any real
failure is easy to spot.
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

# Locate the script relative to the repository root, irrespective of where
# the test runner was invoked from. resolve() mirrors what the script
# itself does internally, so the test sees the same path the CI runner sees.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / ".github" / "workflows" / "scripts" / "check_test_count.py"


def _load_module():
    """Load the script as an importable module without running it."""
    spec = importlib.util.spec_from_file_location("check_test_count", _SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load script at {_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["check_test_count"] = mod
    spec.loader.exec_module(mod)
    return mod


@contextlib.contextmanager
def _silenced():
    """Redirect stdout and stderr to throwaway buffers for the duration."""
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        yield


class CheckTestCountAtBaselineTest(unittest.TestCase):
    def test_match_returns_zero(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            baseline.write_text(json.dumps({"tests": 92}))
            with _silenced():
                result = mod.check(92, baseline)
            self.assertEqual(result, 0)


class CheckTestCountBelowBaselineTest(unittest.TestCase):
    def test_below_returns_one(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            baseline.write_text(json.dumps({"tests": 92}))
            # This is the case that silently exited 0 before. It MUST
            # return 1 - the entire guard's value depends on it.
            with _silenced():
                result = mod.check(91, baseline)
            self.assertEqual(result, 1)

    def test_far_below_returns_one(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            baseline.write_text(json.dumps({"tests": 100}))
            with _silenced():
                result = mod.check(0, baseline)
            self.assertEqual(result, 1)


class CheckTestCountAboveBaselineTest(unittest.TestCase):
    def test_above_returns_zero(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            baseline.write_text(json.dumps({"tests": 92}))
            # Growth is fine - we only fail on regression.
            with _silenced():
                result = mod.check(93, baseline)
            self.assertEqual(result, 0)


class CheckTestCountFirstRunTest(unittest.TestCase):
    def test_missing_baseline_records_and_passes(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            self.assertFalse(baseline.exists())
            with _silenced():
                result = mod.check(50, baseline)
            self.assertEqual(result, 0)
            self.assertTrue(baseline.exists())
            self.assertEqual(json.loads(baseline.read_text())["tests"], 50)


class CheckTestCountInvalidInputTest(unittest.TestCase):
    def test_negative_returns_two(self):
        mod = _load_module()
        with tempfile.TemporaryDirectory() as tmp:
            baseline = Path(tmp) / ".test_count.json"
            baseline.write_text(json.dumps({"tests": 92}))
            with _silenced():
                result = mod.check(-1, baseline)
            self.assertEqual(result, 2)


class CheckTestCountCLITest(unittest.TestCase):
    def test_main_with_no_args_returns_two(self):
        mod = _load_module()
        with _silenced():
            result = mod.main(["check_test_count.py"])
        self.assertEqual(result, 2)

    def test_main_with_non_integer_returns_two(self):
        mod = _load_module()
        with _silenced():
            result = mod.main(["check_test_count.py", "not-a-number"])
        self.assertEqual(result, 2)


if __name__ == "__main__":
    unittest.main()
