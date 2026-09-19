"""
stream_pipeline.detection.device_registry

Live device -> store projection built from device.registration events.

The registry subscribes to the EventRouter as a handler for
("device.registration", "v1"). Each observed registration event updates
an internal dual mapping (device -> store and store -> {devices}) that
backs two O(1) queries used by detectors and feature pipelines:

- ``store_for(device_id)`` — given a device, find its store.
- ``device_count(store_id)`` — given a store, how many devices it has.

These match the constructor-callable shapes that Phase 2 detectors
(``OfflineDetector``, ``OutageDetector``) already accept, so wiring the
registry into existing detectors is a substitution rather than a
refactor of their interface.

Design notes captured in ``docs/working-notes.md`` and
``docs/phases/phase-3-plan.md``:

- Router-subscribed rather than fed explicitly (Phase 3 plan, D-10).
- Dual internal state for O(1) queries on both directions; the
  iteration alternative is untenable at fleet scale.
- No event-time discipline; the registry is a state projection, not a
  time-windowed computation.
- Replay-deterministic by construction: state depends only on the
  observed event sequence.

Order of registration with the EventRouter matters: register this
component's handler before any consumer that depends on lookups
against the registry, so the registry's state is updated for each
event before downstream consumers run.
"""

from __future__ import annotations

from uuid import UUID

from stream_pipeline.streaming.event_protocol import TelemetryEvent


class DeviceRegistry:
    """
    Projection of device.registration events into a live
    ``device_id -> store_id`` mapping.

    Construction is parameter-less; the registry begins empty and
    accumulates state as registration events arrive through
    ``observe_registration``. Queries against unknown devices or stores
    return ``None`` / 0 respectively — the same "unknown" sentinel
    Phase 2 detectors already handle.

    The registry is idempotent against duplicate registration events
    (re-observing an identical registration is a no-op) and handles
    the case of a device re-registering to a different store by
    updating both internal mappings consistently.
    """

    def __init__(self) -> None:
        self._device_to_store: dict[UUID, str] = {}
        self._store_to_devices: dict[str, set[UUID]] = {}

    def observe_registration(self, event: TelemetryEvent) -> None:
        """
        Handler for ``device.registration`` events. Updates the
        projection from the event's payload.

        The router guarantees this is called only for matching event
        types; the payload is therefore a ``DeviceRegistrationPayload``
        in practice and the access of ``device_id`` and ``store_id``
        below is type-safe in production. The static type of
        ``event.payload`` here is ``Any`` because the router's handler
        signature is the structural ``TelemetryEvent`` protocol.
        """
        payload = event.payload
        device_id: UUID = payload.device_id
        store_id: str = payload.store_id

        # Idempotent: re-observing an identical registration is a no-op.
        if self._device_to_store.get(device_id) == store_id:
            return

        # If the device was previously registered to a different store,
        # remove it from that store's set before re-binding. The brief
        # does not describe device-store as mutable, but defensive
        # correctness costs us nothing and avoids a future surprise if
        # a re-registration event ever arrives in production.
        previous_store = self._device_to_store.get(device_id)
        if previous_store is not None:
            self._store_to_devices[previous_store].discard(device_id)
            if not self._store_to_devices[previous_store]:
                del self._store_to_devices[previous_store]

        self._device_to_store[device_id] = store_id
        self._store_to_devices.setdefault(store_id, set()).add(device_id)

    def store_for(self, device_id: UUID) -> str | None:
        """
        Return the ``store_id`` for the device, or ``None`` if the
        device has not been registered. Matches the shape of Phase 2's
        ``OfflineDetector.store_lookup`` constructor callable.
        """
        return self._device_to_store.get(device_id)

    def device_count(self, store_id: str) -> int:
        """
        Return the number of registered devices in the store, or 0 if
        the store is unknown. Matches the shape of Phase 2's
        ``OutageDetector.registered_count_lookup`` constructor
        callable.
        """
        devices = self._store_to_devices.get(store_id)
        return len(devices) if devices is not None else 0

    def registered_device_count(self) -> int:
        """Total registered devices across all stores. Diagnostic."""
        return len(self._device_to_store)

    def known_stores(self) -> set[str]:
        """
        Snapshot of stores currently with at least one registered
        device. Returns a copy of the internal key set, not a live
        reference. Diagnostic.
        """
        return set(self._store_to_devices.keys())
