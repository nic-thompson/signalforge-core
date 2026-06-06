"""
signal_forge.identity

Deterministic identity derivation for produced events.

Detections and windowed feature vectors carry identity fields that
default to ``uuid4`` — the envelope ``event_id``, and a detection's
``detection_id`` and ``source_event_id``. ``uuid4`` makes per-record
identity non-reproducible across runs: the project's determinism
contract is sequence-level, not byte-level (see ``docs/detection-models.md``
and ``docs/feature-pipelines.md``). The Phase 4 dataset layer serialises
these ids into Parquet, and the replay-isolation test asserts
byte-identical output across a live run and its replay; that requires the
ids to be derived deterministically from stable inputs rather than minted
randomly.

This module provides a frozen project namespace and one derivation
helper. Ids are UUIDv5 over ``(role, *coordinates)``: the ``role`` string
keeps two fields built from overlapping coordinates — a detection's
``detection_id`` and its envelope ``event_id``, say — from colliding to
the same value.

Stability contract
-------------------
The namespace and every ``role`` string are **frozen**. Once shipped,
changing either silently re-bases every derived id, which would make a
replay diverge from the original run it is meant to reproduce. Treat both
as append-only: add new roles for new record kinds, never rename existing
ones.

The namespace is derived from the DNS namespace rather than minted as an
opaque literal, so it is self-documenting and reproducible from nothing —
there is no magic UUID to copy around, and the derivation can be
re-checked by anyone.
"""

from __future__ import annotations

from uuid import NAMESPACE_DNS, UUID, uuid5

# Frozen project namespace. The root of every derived id; do not change.
NAMESPACE: UUID = uuid5(NAMESPACE_DNS, "signalforge.analytics")

# Separator between the role and coordinate parts in the UUIDv5 name. A
# pipe cannot appear in a UUID, an ISO timestamp, a detection-type
# constant, or a partition key (the upstream partition grammar forbids
# it), so it cannot blur the boundary between two parts.
_SEP = "|"


def derive(role: str, *parts: object) -> UUID:
    """
    Derive a stable UUIDv5 from a role and its coordinate parts.

    ``role`` names what the id is for (e.g. ``"detection.device_offline"``
    or ``"event.detection"``) so distinct fields built from overlapping
    coordinates do not collide. ``parts`` are the stable coordinates that
    uniquely identify the logical record; each is stringified and joined
    with a separator that cannot occur inside a part.

    Identical ``(role, parts)`` always yields the same UUID, on any
    machine and across runs — this is what lets a replay reproduce the
    original run's ids byte-for-byte. Callers must pass replay-stable
    coordinates only (window bounds, store ids, the triggering event's
    id), never wall-clock time or a fresh ``uuid4``.
    """
    name = _SEP.join([role, *(str(p) for p in parts)])
    return uuid5(NAMESPACE, name)
