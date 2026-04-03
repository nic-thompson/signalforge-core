from pydantic import BaseModel, Field, field_validator
import re

SEMVER_PATTERN = re.compile(r"^v\d+(\.\d+)*$")
EVENT_TYPE_PATTERN = re.compile(r"^[a-z0-9]+(\.[a-z0-9]+)+$")
SOURCE_PATTERN = re.compile(r"^[a-z0-9]+$")

class EventMetadata(BaseModel):
    """
    Cannonical schema metadata envelope shared across all event contracts

    Defines schema identity and routing semantics across ingestion pipelines,
    validation workers, feature builders, exporters, and inference services.
    """

    schema_version: str = Field(
        ...,
        description="Schema version identifier (e.g., v1, v1.1, v2)"
    )

    event_type: str = Field(
            ...,
            description="Logical event type identifier (e.g., device.registration)"
    )

    source: str = Field(
        ...,
        description="Originating service emitting the event"
    )

    model_config = {
        "frozen": True,
        "extra": "forbid"
    }

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if not SEMVER_PATTERN.match(value):
            raise ValueError("schema_version must match patteren v<major>[.<minor>...]")
    
    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        if not EVENT_TYPE_PATTERN.match(value):
            raise ValueError(
                "event_type must must match pattern <domain>.<actioni>[.<subaction>...]"
            )
        return value
    
    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        if not SOURCE_PATTERN.match(value):
            raise ValueError("source must match pattern [a-z0-9]+")
        return value
