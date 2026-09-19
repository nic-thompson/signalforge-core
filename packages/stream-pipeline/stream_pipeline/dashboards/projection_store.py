"""
stream_pipeline.dashboards.projection_store

The persistence boundary for dashboard projections.

A projection is a materialised view — a denormalised, read-optimised
fold over the event stream (DDIA Chapter 11). The *fold* lives in the
projection; the *persistence* lives behind this protocol. Keeping the
two apart means the projection logic — where the determinism and the
aggregation semantics live — is testable against an in-memory store with
no AWS, and the DynamoDB backend (a later commit) is a dumb key-value
persister that knows nothing about offline counts or anomaly rates.

The contract is deliberately minimal and untyped about *meaning*: the
store reads and writes opaque string values keyed by ``(view, key)``.
``view`` namespaces a projection ("offline_count", "active_outages");
``key`` identifies an entity within it (a store id, a signal type). The
value is a string the projection has already serialised — the projection
owns its own value shape (an int, a set, a rolling-window structure) and
serialises it to a string at its edge, so the store never needs to know
the shape. This is what lets the in-memory and DynamoDB stores persist
the *same* primitive contract: a string in, the same string out, with no
type-coercion surface where a number round-trip could diverge between
Python and DynamoDB.

``get`` returns ``None`` for an absent ``(view, key)`` — the cold-start
sentinel every projection handles (a missing count reads as zero, a
missing set reads as empty), the same "unknown returns None" convention
``DeviceRegistry`` and ``AcknowledgementRegistry`` use.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ProjectionStore(Protocol):
    """
    Structural protocol for projection persistence.

    Implementations persist opaque string values keyed by ``(view, key)``.
    They neither parse nor interpret the values; serialisation is the
    projection's concern.
    """

    def get(self, view: str, key: str) -> str | None:
        """Return the stored value, or ``None`` if absent."""
        ...

    def put(self, view: str, key: str, value: str) -> None:
        """Store ``value`` under ``(view, key)``, overwriting any prior."""
        ...

    def delete(self, view: str, key: str) -> None:
        """Remove ``(view, key)``. A no-op if absent."""
        ...

    def keys(self, view: str) -> list[str]:
        """
        Return the keys present in ``view``, sorted.

        Sorted so a projection that reads a whole view (a dashboard
        rollup over all stores, say) sees a deterministic order — the
        same replay-determinism discipline the rest of the platform
        holds to.
        """
        ...


class InMemoryProjectionStore:
    """
    Dict-backed ``ProjectionStore``. The no-AWS implementation: backs all
    projection-logic tests and any caller wanting projections without
    DynamoDB. Holds the same opaque-string contract the DynamoDB store
    persists, so a projection tested against this behaves identically
    against the cloud store.
    """

    def __init__(self) -> None:
        # view -> key -> serialised value
        self._data: dict[str, dict[str, str]] = {}

    def get(self, view: str, key: str) -> str | None:
        return self._data.get(view, {}).get(key)

    def put(self, view: str, key: str, value: str) -> None:
        self._data.setdefault(view, {})[key] = value

    def delete(self, view: str, key: str) -> None:
        view_data = self._data.get(view)
        if view_data is not None:
            view_data.pop(key, None)

    def keys(self, view: str) -> list[str]:
        return sorted(self._data.get(view, {}))
