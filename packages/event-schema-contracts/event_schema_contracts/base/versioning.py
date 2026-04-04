from typing import ClassVar
from pydantic import BaseModel, field_validator
import re


class SemVerModel(BaseModel):
    """
    Base model enforcing semver formatting on selected fields.
    """

    __semver_fields__: ClassVar[tuple[str, ...]] = ()

    __SEMVER_PATTERN: ClassVar[re.Pattern[str]] = re.compile(
        r"^(0|[1-9]\d*)\."
        r"(0|[1-9]\d*)\."
        r"(0|[1-9]\d*)"
        r"(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
        r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"

    )

    @field_validator("*", check_fields=False)
    @classmethod
    def validate_semver_fields(cls, value, info):

        if info.field_name not in cls.__semver_fields__:
            return value
        
        if value is None:
            return value
        
        if not isinstance(value, str):
            raise TypeError(f"{info.field_name} must be a string")
        
        if not cls.__SEMVER_PATTERN.match(value):
            raise ValueError(
                f"{info.field_name} must follow semantic versioning (MAJOR.MINOR.PATCH)"    
            )
        
        return value