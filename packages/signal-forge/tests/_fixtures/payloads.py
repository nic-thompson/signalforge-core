"""
Reusable payload fixtures for tests that need structured event payloads.

Phase 2's detectors are the first components to require payload
structure; ``FakeEvent`` (in events.py) leaves payload as Any/None
because Phase 1's streaming layer is payload-agnostic. This module
supplies the structural payloads detector tests need.

The fakes are duck-typed against the upstream telemetry payload
shapes (UUID device_id, etc.) without inheriting from the upstream
classes. This keeps the test fixtures stdlib-only and avoids
coupling tests to pydantic validation.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True)
class FakeDevicePayload:
    """
    Minimal device-shaped payload exposing a UUID device_id.

    Mirrors a subset of fields from
    ``event_schema_contracts.telemetry.DeviceRegistrationPayload``
    relevant to detector extractor tests. The real payload now also
    carries ``store_id`` (since v0.3.0) and other fields; tests that
    need those construct the real payload directly, or use a
    different fake.
    """

    device_id: UUID
