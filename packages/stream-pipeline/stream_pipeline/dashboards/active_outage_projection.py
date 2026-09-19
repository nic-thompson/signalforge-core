"""
stream_pipeline.dashboards.active_outage_projection

A materialised view of *which stores are currently in outage* — the second
of Phase 6's three dashboard projections, and the second view shape: a
global current-set whose cardinality is the dashboard's active-outage
count. It folds the ``store.outage`` and ``store.recovered`` detections
emitted by ``OutageDetector`` (the latter added in D-18 specifically so
this projection has a clear signal to fold).

Where ``OfflineCountProjection`` is a *partitioned* view — one independent
gauge per store, looked up by store id — this is a *global* view: a single
question ("which stores are in outage right now, and how many") answered by
a fold over the whole detection stream. The read pattern is "enumerate the
set / count it", not "look up one store", and that is what drives the
layout.

Layout: key-per-store-presence
------------------------------
Each store currently in outage is its own key under ``view="active_outage"``;
the set of outaged stores *is* ``store.keys(view)`` and the active-outage
count is its length. ``store.outage`` writes the store's key;
``store.recovered`` deletes it.

The alternative — one fixed key holding the whole JSON set of store ids —
matches the "current-set" shape more literally but makes every transition a
read-modify-write on a single hot key. Under a fleet-wide event many stores
transition at once, so that single key becomes a contention point and, on
the DynamoDB backend, a hot partition (the classic DDIA Chapter 6 footgun).
Key-per-presence makes each transition an independent point write, contends
with nothing, and reuses the exact ``keys(view)``-enumerates-the-set pattern
``OfflineCountProjection`` already established, so the two projections read
consistently. The cost it accepts is that counting the active set is a
``keys(view)`` enumeration rather than a single read — a bounded scan (the
number of *currently* outaged stores is small even in a bad event), noted as
a known trade-off and revisited only if it measures poorly.

Idempotency (D-17) holds trivially under this layout. ``store.outage`` over
an at-least-once channel redelivers the same detection with the same
``detected_at``, so the point write is last-write-wins with an identical
value — a no-op in effect. ``store.recovered`` for a store not present is a
delete of an absent key — also a no-op. The fold is correct under duplicate
delivery, replay re-runs, and flapping (outage -> recovered -> outage
collapses to a single present key).

The stored value is the outage's ``detected_at`` (its window-end), not a
bare sentinel. It costs the same as storing ``"1"`` but gives free "in
outage since" provenance to anyone reading the store directly; the
projection's own logic only cares whether the key is present.

Cold start is the ``ProjectionStore``'s ``None`` sentinel: a store with no
key is not in outage, and an empty view counts zero.
"""

from __future__ import annotations

from typing import Final

from event_schema_contracts.detection import DetectionEvent

from stream_pipeline.dashboards.projection_store import ProjectionStore
from stream_pipeline.detection.types import (
    DETECTION_TYPE_STORE_OUTAGE,
    DETECTION_TYPE_STORE_RECOVERED,
)


class ActiveOutageProjection:
    """
    Global current-set projection of stores presently in outage.

    Construct with a ``ProjectionStore`` (in-memory for tests and no-AWS
    callers, DynamoDB-backed in production). Feed every ``DetectionEvent``
    to :meth:`observe`; the projection folds the two store-level outage
    transitions and ignores everything else.
    """

    #: Namespaces this projection within the store.
    VIEW: Final[str] = "active_outage"

    def __init__(self, store: ProjectionStore) -> None:
        self._store = store

    def observe(self, detection: DetectionEvent) -> None:
        """
        Fold one detection into the view.

        ``store.outage`` marks the store present in the active set;
        ``store.recovered`` removes it. All other detection types are
        ignored. Store-level detections carry ``device_id=None``, so —
        unlike the device-level offline projection — routing is on
        ``detection_type`` alone and ``device_id`` is not consulted.
        """
        payload = detection.payload
        detection_type = payload.detection_type
        if detection_type == DETECTION_TYPE_STORE_OUTAGE:
            # Value is the outage's detected_at, for free provenance; the
            # projection only cares that the key is present.
            self._store.put(
                self.VIEW, payload.store_id, payload.detected_at.isoformat()
            )
        elif detection_type == DETECTION_TYPE_STORE_RECOVERED:
            self._store.delete(self.VIEW, payload.store_id)

    def active_outage_count(self) -> int:
        """Number of stores currently in outage (0 if none)."""
        return len(self._store.keys(self.VIEW))

    def stores_in_outage(self) -> list[str]:
        """
        The stores currently in outage, sorted.

        Sorted for determinism, so a dashboard listing active outages
        reads in a stable order across runs.
        """
        return self._store.keys(self.VIEW)

    def is_in_outage(self, store_id: str) -> bool:
        """Whether ``store_id`` is currently in outage."""
        return self._store.get(self.VIEW, store_id) is not None
