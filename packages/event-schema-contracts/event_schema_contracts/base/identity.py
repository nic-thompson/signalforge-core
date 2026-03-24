from typing import Any, ClassVar
from uuid import UUID

from pydantic import BaseModel, field_validator

class UUIDv4Model(BaseModel):
    """
    Base model enforcing UUIDv4 policy on selected fields.

    Subclasses declare UUID fields using __uuid_v4_fields__.
    """

    __uuid__v4_fields__: ClassVar[tuple[str, ...]] = ()

    @field_validator
    @classmethod
    def validate_uuid_fields(cls, value: Any, info):
        if (
            isinstance(value, UUID)
            and info.field_name in cls.__uuid__v4_fields__
        ):
            if value.version != 4:
                raise ValueError(
                    f"{info.field_name} must be UUIDv4"
                )
            
        return value