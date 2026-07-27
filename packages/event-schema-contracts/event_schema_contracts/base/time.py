"""
UTC enforcement for datetime fields on payload schemas.

Timestamps crossing a process boundary need an unambiguous offset, so nominated
fields must be both timezone-aware and actually UTC rather than merely aware.
"""

from datetime import datetime, timezone
from typing import Any, ClassVar

from pydantic import BaseModel, ValidationInfo, field_validator

class UTCTimestampModel(BaseModel):
    """
    Base model enforcing UTC timezone-aware datetime fields.

    Subclasses define which fields must be UTC using __utc_fields__. 
    """

    __utc_fields__: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Reject a subclass whose ``__utc_fields__`` is not a tuple.

        Catches the easy mistake of writing a bare string, which would
        otherwise iterate character by character and silently match nothing.
        """

        super().__init_subclass__(**kwargs)
        if not isinstance(cls.__utc_fields__, tuple):
            raise TypeError("__utc_fields__ must be tuple[str, ...]")

    @field_validator("*", check_fields=False)
    @classmethod
    def validate_utc_fields(cls, value: Any, info: ValidationInfo) -> Any:
        """
        Require any field named in ``__utc_fields__`` to be a UTC datetime.

        Checks both that an offset is present and that it is zero, so a
        correctly-aware but non-UTC timestamp is still rejected. Non-datetime
        values pass through untouched.
        """

        
        if (
            isinstance(value, datetime)
            and info.field_name in cls.__utc_fields__
        ):
            
            if value.tzinfo is None:
                raise ValueError(
                    f"{info.field_name} must be timezone-aware"
                )

            if value.utcoffset() != timezone.utc.utcoffset(value):
                raise ValueError(f"{info.field_name} must be UTC")
            
        return value