"""
Real-event fixtures for tests that need pydantic-validated upstream
events rather than the stdlib-only fakes in events.py.

events.py is deliberately stdlib-only: the streaming, router, and
aggregator tests run without exercising upstream pydantic types,
which keeps those tests fast and isolated from upstream changes.

This file is for the other case — tests that genuinely need to
exercise upstream payload contracts (notably ``DeviceRegistry`` tests
and the detector tests that wire to a real registry). It imports
upstream types and constructs validated events.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.telemetry.device_event import (
    DeviceRegistrationEvent,
    DeviceRegistrationPayload,
    DeviceType,
)


def make_registration_event(
    *,
    device_id: UUID | None = None,
    store_id: str = "store-1",
    device_type: DeviceType = DeviceType.SENSOR,
    firmware_version: str | None = None,
    registered_at: datetime | None = None,
    event_timestamp: datetime | None = None,
) -> DeviceRegistrationEvent:
    """
    Build a real ``DeviceRegistrationEvent`` with sensible defaults.

    Tests override only the fields they care about; everything else
    takes a default. Pydantic validation runs at construction time,
    so format violations (e.g. an invalid ``store_id``) surface
    immediately in tests that pass them.

    Defaults:

    - ``device_id``: a fresh ``uuid4()``.
    - ``store_id``: ``"store-1"`` (matches detector-test convention).
    - ``device_type``: ``DeviceType.SENSOR``.
    - ``firmware_version``: ``None`` (the field is optional upstream).
    - ``registered_at``: a fixed UTC timestamp at 2026-04-30 12:00:00.
      Fixed rather than ``datetime.now(UTC)`` for replay determinism
      in tests.
    - ``event_timestamp``: defaults to ``registered_at``.
    """
    if device_id is None:
        device_id = uuid4()
    if registered_at is None:
        registered_at = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)
    if event_timestamp is None:
        event_timestamp = registered_at

    payload = DeviceRegistrationPayload(
        device_id=device_id,
        store_id=store_id,
        device_type=device_type,
        firmware_version=firmware_version,
        registered_at=registered_at,
    )
    return DeviceRegistrationEvent(
        event_timestamp=event_timestamp,
        trace=TraceContext(),
        payload=payload,
    )
