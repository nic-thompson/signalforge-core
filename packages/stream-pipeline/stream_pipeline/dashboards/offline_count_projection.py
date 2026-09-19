"""
stream_pipeline.dashboards.offline_count_projection

A materialised view of *how many devices are currently offline per store* —
the first of Phase 6's three dashboard projections, and the simplest view
shape (a per-store gauge). It folds the ``device.offline`` and
``device.online`` detections emitted by ``OfflineDetector`` into a current
count, persisted through a ``ProjectionStore`` (DDIA Chapter 11: a
denormalised read-optimised view over the event stream).

Why a *set* of offline device ids, not a bare integer counter
-------------------------------------------------------------
The fold has to be idempotent. The detections reach this projection over an
at-least-once channel — EventBridge re-delivers, and replay re-runs the same
event sequence (the very reason Phase 5's alert idempotency keys exist, D-14).
A naive ``+1 / -1`` counter is *not* idempotent: a duplicated ``device.offline``
permanently inflates the count, and the drift never heals. So instead the
projection holds, per store, the *set* of currently-offline ``device_id``s:

- ``device.offline`` adds the id to the store's set,
- ``device.online`` discards it,
- the dashboard "count" is the set's cardinality.

Adding an id already present is a no-op; discarding one already absent is a
no-op. The fold is therefore idempotent under duplicate delivery and correct
under flapping (offline -> online -> offline collapses to cardinality 1) and
under a device going offline twice without an intervening recovery. This is
the DDIA Chapter 11 materialised-view-over-a-Chapter-9 at-least-once-stream
discipline: the fold must be idempotent or the view rots. Recorded as D-17;
the other two Phase 6 projections inherit the same default.

Storage contract
----------------
The per-store set is serialised to a JSON array of device-id strings, sorted
so the persisted bytes are independent of arrival order (the same
replay-determinism discipline the rest of the platform holds to). When a
store's set empties (its last offline device recovers) the key is deleted,
bounding the view to "stores with currently-offline devices" rather than
"stores ever seen" — the same defensive cleanup ``DeviceRegistry`` does when
a store loses all its devices.

Cold start is the ``ProjectionStore``'s ``None`` sentinel: an absent key reads
as the empty set, so an unqueried store reports a count of zero.
"""

from __future__ import annotations

import json
from typing import Final

from event_schema_contracts.detection import DetectionEvent

from stream_pipeline.dashboards.projection_store import ProjectionStore
from stream_pipeline.detection.types import (
    DETECTION_TYPE_DEVICE_OFFLINE,
    DETECTION_TYPE_DEVICE_ONLINE,
)


class OfflineCountProjection:
    """
    Per-store current-offline-device-count projection.

    Construct with a ``ProjectionStore`` (in-memory for tests and
    no-AWS callers, DynamoDB-backed in production). Feed every
    ``DetectionEvent`` to :meth:`observe`; the projection filters to the
    two detection types it cares about and ignores the rest, the same way
    a router-subscribed registry ignores event types it does not handle.
    """

    #: Namespaces this projection within the store.
    VIEW: Final[str] = "offline_count"

    def __init__(self, store: ProjectionStore) -> None:
        self._store = store

    def observe(self, detection: DetectionEvent) -> None:
        """
        Fold one detection into the view.

        ``device.offline`` marks the device offline for its store;
        ``device.online`` clears it. All other detection types are
        ignored. A detection with no ``device_id`` is ignored (the two
        handled types always carry one, but the projection does not
        assume the producer's internals).
        """
        payload = detection.payload
        detection_type = payload.detection_type
        if detection_type not in (
            DETECTION_TYPE_DEVICE_OFFLINE,
            DETECTION_TYPE_DEVICE_ONLINE,
        ):
            return

        device_id = payload.device_id
        if device_id is None:
            return

        store_id = payload.store_id
        offline = self._load(store_id)
        if detection_type == DETECTION_TYPE_DEVICE_OFFLINE:
            offline.add(str(device_id))
        else:  # DETECTION_TYPE_DEVICE_ONLINE
            offline.discard(str(device_id))
        self._save(store_id, offline)

    def offline_count(self, store_id: str) -> int:
        """Number of devices currently offline in ``store_id`` (0 if none)."""
        return len(self._load(store_id))

    def offline_device_ids(self, store_id: str) -> set[str]:
        """The set of currently-offline device ids in ``store_id``."""
        return self._load(store_id)

    def stores_with_offline(self) -> list[str]:
        """
        Stores with at least one device currently offline, sorted.

        Sorted for determinism, so a dashboard rollup over all affected
        stores reads in a stable order across runs.
        """
        return self._store.keys(self.VIEW)

    def _load(self, store_id: str) -> set[str]:
        raw = self._store.get(self.VIEW, store_id)
        if raw is None:
            return set()
        return set(json.loads(raw))

    def _save(self, store_id: str, offline: set[str]) -> None:
        if offline:
            self._store.put(self.VIEW, store_id, json.dumps(sorted(offline)))
        else:
            # Last offline device recovered; drop the key so the view
            # stays bounded to currently-affected stores.
            self._store.delete(self.VIEW, store_id)
