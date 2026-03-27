from typing import ClassVar
from collections.abc import Sequence
from uuid import UUID

from pydantic import BaseModel, field_validator

class UUIDv4Model(BaseModel):
    """
    Base model enforcing UUIDv4 policy on selected fields.

    Subclasses declare UUID fields using __uuid_v4_fields__.
    """

    __uuid_v4_fields__: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not isinstance(cls.__uuid_v4_fields__, tuple):
            raise TypeError("__uuid_v4_fields__ must be tuple[str, ...]")

    @field_validator("*")
    @classmethod
    def validate_uuid_fields(cls, value, info):
        
        if info.field_name not in cls.__uuid_v4_fields__:
            return value
        
        if value is None:
            return value
        
        if isinstance(value, UUID):
            if value.version != 4:
                raise ValueError(f"{info.field_name} must be UUIDv4")
        
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for item in value:
                if not isinstance(item, UUID) or item.version != 4:
                    raise ValueError(f"{info.field_name} must contain UUIDv4 values only")
            
        return value