"""
UUID version policy, and the derivation that produces conforming ids.

Two complementary halves of one concern. ``UUIDv4Model`` says which
fields may hold a derived (v5) id; ``derive`` is how such an id is
produced. Keeping them together means a schema that permits a derived
identifier also ships the only sanctioned way to derive one.
"""

from typing import Any, ClassVar
from collections.abc import Sequence
from uuid import NAMESPACE_DNS, UUID, uuid5

from pydantic import BaseModel, ValidationInfo, field_validator

# Frozen project namespace. The root of every derived id; do not change.
#
# Derived from the DNS namespace rather than minted as an opaque literal,
# so it is self-documenting and reproducible from nothing — there is no
# magic UUID to copy around, and anyone can re-check the derivation.
NAMESPACE: UUID = uuid5(NAMESPACE_DNS, "signalforge.analytics")

# Separator between the role and coordinate parts in the UUIDv5 name. A
# pipe cannot appear in a UUID, an ISO timestamp, a detection-type
# constant, or a store id (the store_id grammar forbids it), so it cannot
# blur the boundary between two parts.
_SEP = "|"

# The role for a device identity. Pinned as a constant because the string
# is part of the derivation and therefore part of the contract: changing
# it re-bases every device id in the system.
_DEVICE_ROLE = "device"


def derive(role: str, *parts: object) -> UUID:
    """
    Derive a stable UUIDv5 from a role and its coordinate parts.

    ``role`` names what the id is for (e.g. ``"device"`` or
    ``"detection.device_offline"``) so distinct fields built from
    overlapping coordinates do not collide. ``parts`` are the stable
    coordinates that uniquely identify the logical record; each is
    stringified and joined with a separator that cannot occur inside a
    part.

    Identical ``(role, parts)`` always yields the same UUID, on any
    machine and across runs — this is what lets a replay reproduce the
    original run's ids byte-for-byte. Callers must pass replay-stable
    coordinates only (store ids, device labels, window bounds, the
    triggering event's id), never wall-clock time or a fresh ``uuid4``.

    Stability contract
    ------------------
    The namespace and every ``role`` string are **frozen**. Once shipped,
    changing either silently re-bases every derived id, which would make a
    replay diverge from the original run it is meant to reproduce. Treat
    both as append-only: add new roles for new record kinds, never rename
    existing ones.
    """

    name = _SEP.join([role, *(str(p) for p in parts)])
    return uuid5(NAMESPACE, name)


def derive_device_id(store_id: str, device_label: str) -> UUID:
    """
    Derive a device's stable identity from the store it belongs to and
    the label it was configured with.

    A device label is unique only within a store — two stores may each
    have a ``headset-12``, and they are different devices — so the store
    is part of the identity rather than merely an attribute of it. This
    is why ``SipRegistrationPayload`` carries ``store_id``,
    ``device_label`` and a derived ``device_id`` rather than treating the
    label as an identifier.

    Derivation is one-way: the label is carried alongside because it
    cannot be recovered from the id.

    A device moving between stores acquires a new identity. That is
    intended — its telemetry belongs to the store it is in.

    See docs/ADR-002-sip-registration-schema.md.
    """

    return derive(_DEVICE_ROLE, store_id, device_label)


class UUIDv4Model(BaseModel):
    """
    Base model enforcing UUID-version policy on selected fields.

    Subclasses declare:

    - ``__uuid_v4_fields__``: fields that MUST be UUIDv4 (random). For
      identifiers that must not be derivable or guessable.
    - ``__uuid_v4_or_v5_fields__``: fields that may be UUIDv4 (random) OR
      UUIDv5 (deterministically derived from a namespace + name). For
      identifiers a consumer may regenerate reproducibly — e.g. detection
      ids that must come out identical on replay — while still rejecting
      v1/v3, which encode host/time or an opaque MD5 and have no place on
      these fields.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = ()
    __uuid_v4_or_v5_fields__: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        for attr in ("__uuid_v4_fields__", "__uuid_v4_or_v5_fields__"):
            if not isinstance(getattr(cls, attr), tuple):
                raise TypeError(f"{attr} must be tuple[str, ...]")

    @field_validator("*", check_fields=False)
    @classmethod
    def validate_uuid_fields(cls, value: Any, info: ValidationInfo) -> Any:
        allowed: tuple[int, ...]
        if info.field_name in cls.__uuid_v4_fields__:
            allowed = (4,)
        elif info.field_name in cls.__uuid_v4_or_v5_fields__:
            allowed = (4, 5)
        else:
            return value

        if value is None:
            return value

        expected = " or ".join(f"UUIDv{v}" for v in allowed)
        if isinstance(value, UUID):
            if value.version not in allowed:
                raise ValueError(f"{info.field_name} must be {expected}")

        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for item in value:
                if not isinstance(item, UUID) or item.version not in allowed:
                    raise ValueError(
                        f"{info.field_name} must contain {expected} values only"
                    )

        return value
