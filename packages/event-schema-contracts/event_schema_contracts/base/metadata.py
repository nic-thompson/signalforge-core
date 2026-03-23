from pydantic import BaseModel, Field, field_validator

class EventMetadata(BaseModel):
    """
    Cannonical schema metadata envelope shared across all event contracts

    Defines schema identity and routing semantics across ingestion pipelines,
    validation workers, feature builders, exporters, and inference services.
    """

    schema_version: str = Field(
        ...,
        description="Semantic schema version identifier (e.g., v1, v1.1, v2)"
    )

    event_type: str = Field(
            ...,
            description="Logical event type identifier (e.g., device.registration)"
    )

    source: str = Field(
        ...,
        description="Originating service emitting the event"
    )

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        if not value.startswith("v"):
            raise ValueError("schema version must start with 'v'")
        return value
