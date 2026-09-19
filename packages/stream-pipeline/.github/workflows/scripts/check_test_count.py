"""
Test-count regression guard.

Reads the previous run's test count from .github/workflows/scripts/.test_count.json
(checked into the repo) and compares against the count from the current run,
which is supplied as the only positional argument.

Fails CI if the current count is lower than the recorded count. The recorded
count is updated by committing a new .test_count.json - silent test loss
(an empty test file, an import error skipping a module, a fixture rename
breaking discovery) is therefore impossible without a deliberate, visible
change to the recorded baseline.

Legitimate reductions (a feature is removed and its tests deleted alongside
it) are fine: the contributor commits an updated baseline as part of the
same PR. The point is to make any reduction visible, not to forbid it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_BASELINE = Path(__file__).resolve().parent / ".test_count.json"


def _usage() -> int:
    print("Usage: check_test_count.py <current_count>", file=sys.stderr)
    return 2


def check(current: int, baseline_path: Path = _BASELINE) -> int:
    """
    Compare ``current`` against the recorded baseline.

    Returns the exit code the CLI should propagate:
      0 - current >= baseline (or baseline did not exist; first run records it)
      1 - current < baseline (regression)
      2 - invalid input

    Splitting the comparison out of ``main`` lets us unit-test the logic
    without invoking subprocesses.
    """

    if current < 0:
        print(f"current count {current} is negative; aborting", file=sys.stderr)
        return 2

    if not baseline_path.exists():
        # First-ever run: record and pass. The baseline is committed in
        # the same PR as the workflow, so this branch only fires once.
        baseline_path.write_text(json.dumps({"tests": current}, indent=2) + "\n")
        print(f"recorded initial baseline: {current} tests")
        return 0

    data = json.loads(baseline_path.read_text())
    baseline = int(data["tests"])

    if current < baseline:
        print(
            f"FAIL: collected {current} tests, baseline is {baseline}.\n"
            "The test count has decreased. If this is intentional, update\n"
            f"{baseline_path} in this PR.",
            file=sys.stderr,
        )
        return 1

    if current > baseline:
        print(
            f"NOTE: collected {current} tests vs baseline {baseline}.\n"
            f"Test count has grown - please update {baseline_path.name}\n"
            "in this PR so the baseline reflects current state."
        )
        return 0

    print(f"OK: {current} tests, matches baseline.")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        return _usage()

    try:
        current = int(argv[1])
    except ValueError:
        return _usage()

    return check(current)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
