"""
Detection engines for signal-forge.

Phase 2 introduces detection logic atop the Phase 1 streaming primitives.
Detectors consume events from the router and/or windowed emissions from
the realtime pipeline's aggregators, and emit ``DetectionEvent`` records
that flow back as a typed return value on ``ProcessingResult.detections``.

The package is layered:

- ``protocols`` defines the ``EventDetector`` and ``EmissionDetector``
  protocols every detector implements.
- ``types`` defines canonical ``DETECTION_TYPE_*`` string constants.
- ``detectors`` contains the concrete implementations:
  ``OfflineDetector``, ``OutageDetector``, ``AnomalyDetector``.

Detectors are pure: they receive input, optionally update internal state,
and return a list of detections. They do not perform side effects. The
pipeline orchestrates dispatch and is the only component that does.
"""

from __future__ import annotations

from signal_forge.detection.types import (
    ALL_DETECTION_TYPES,
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_SIGNAL_ANOMALY,
    DETECTION_TYPE_STORE_OUTAGE,
)

__all__ = [
    "ALL_DETECTION_TYPES",
    "DETECTION_TYPE_DEVICE_OFFLINE",
    "DETECTION_TYPE_SIGNAL_ANOMALY",
    "DETECTION_TYPE_STORE_OUTAGE",
]
