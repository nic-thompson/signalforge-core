"""
Semver enforcement for payload fields that carry a version string.

Distinct from ``EventMetadata.schema_version``, which uses the registry's
``v<major>[.<minor>]`` form. This enforces strict ``MAJOR.MINOR.PATCH`` semver
for version fields inside payloads, such as a model or feature version.
"""

from typing import Any, ClassVar
from pydantic import BaseModel, ValidationInfo, field_validator
import re


class SemVerModel(BaseModel):
    """
    Base model enforcing strict semver on nominated fields.

    Subclasses opt fields in by listing their names in ``__semver_fields__``;
    fields not listed are untouched. ``None`` is allowed through, so optional
    version fields work without special handling.

    Accepts the full semver grammar including pre-release and build metadata
    (``1.2.3-rc.1+build.5``), and rejects leading zeros.
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
    def validate_semver_fields(cls, value: Any, info: ValidationInfo) -> Any:
        """
        Apply the semver pattern to any field named in ``__semver_fields__``.

        Registered against ``"*"`` so it sees every field on the subclass, then
        filters by name — see ``docs/base-model-conventions.md``.
        """

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