"""
Detection type identifiers.

These constants are the canonical values for the ``detection_type`` field
on ``event_schema_contracts.detection.DetectionEventPayload``. They follow
the upstream contract's ``<domain>.<state>`` convention (lowercase,
dotted, multi-segment) and are validated by the schema's pattern
constraint at construction time.

Defining them as module-level ``Final[str]`` constants rather than
hard-coding the strings at each detector's call site catches typos at
import time, makes refactoring straightforward, and gives consumers
(dashboards, alert routers) a single source of truth to import from.

The discriminator pattern in the upstream schema means new detection
types added in later phases are payload changes, not schema-version
bumps — but they should still be added here so signal-forge has a
canonical list of every detection type it produces.
"""

from __future__ import annotations

from typing import Final

# Phase 2 detection types
DETECTION_TYPE_DEVICE_OFFLINE: Final[str] = "device.offline"
DETECTION_TYPE_DEVICE_ONLINE: Final[str] = "device.online"
DETECTION_TYPE_STORE_OUTAGE: Final[str] = "store.outage"
DETECTION_TYPE_STORE_RECOVERED: Final[str] = "store.recovered"
DETECTION_TYPE_SIGNAL_ANOMALY: Final[str] = "signal.anomaly"


# Set of all known detection types signal-forge emits. Useful for
# exhaustive iteration in tests and for downstream consumers that want
# to assert they handle every type.
ALL_DETECTION_TYPES: Final[frozenset[str]] = frozenset(
    {
        DETECTION_TYPE_DEVICE_OFFLINE,
        DETECTION_TYPE_DEVICE_ONLINE,
        DETECTION_TYPE_STORE_OUTAGE,
        DETECTION_TYPE_STORE_RECOVERED,
        DETECTION_TYPE_SIGNAL_ANOMALY,
    }
)
