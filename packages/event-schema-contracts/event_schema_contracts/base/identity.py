from typing import Any, ClassVar
from collections.abc import Sequence
from uuid import UUID

from pydantic import BaseModel, ValidationInfo, field_validator

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
