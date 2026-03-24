from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, field_validator

class UTCTimestampModel(BaseModel):
    """
    Base model enforcing UTC timezone-aware datetime fields.

    Subclasses define which fields must be UTC using __utc_fields__. 
    """

    __utc_fields__: tuple [str, ...] = ()

    @field_validator("*")
    @classmethod
    def validate_utc_fields(cls, value: Any, info):
        if (
            isinstance(value, datetime)
            and info.field_name in cls.__utc_fields__
        ):
            if value.tzinfo is None:
                raise ValueError(
                    f"{info.field_name} must be timezone-aware"
                )
            
            if value.tzinfo != timezone.utc:
                raise ValueError(
                    f"{info.field_name} must be UTC"
                ) 
        return value