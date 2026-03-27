from datetime import datetime, timezone
from typing import Any, ClassVar

from pydantic import BaseModel, field_validator

class UTCTimestampModel(BaseModel):
    """
    Base model enforcing UTC timezone-aware datetime fields.

    Subclasses define which fields must be UTC using __utc_fields__. 
    """

    __utc_fields__: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not isinstance(cls.__utc_fields__, tuple):
            raise TypeError("__utc_fields__ must be tuple[str, ...]")

    @field_validator("*")
    @classmethod
    def validate_utc_fields(cls, value: Any, info):
        
        if (
            isinstance(value, datetime)
            and info.field_name in cls.__utc_fields__
        ):
            if not isinstance(value, datetime):
                return value
            
            if value.tzinfo is None:
                raise ValueError(
                    f"{info.field_name} must be timezone-aware"
                )

            if value.utcoffset() != timezone.utc.utcoffset(value):
                raise ValueError(f"{info.field_name} must be UTC")
            
        return value