from datetime import datetime, timezone
from typing import Generic, TypeVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext

from event_schema_contracts.versioning.schema_registry import schema_registry

PayloadT = TypeVar("PayloadT")

class BaseEvent(BaseModel, Generic[PayloadT]):
    """
    Canonical event envelope shared across all telemetry schemas.

    This contract defines ingenstion boundries and guaruntees:

    - schema version tracebility
    - reply saftey
    - distributed trace propagation
    - dataset reproducibility guaruntees
    - pipeline observability compatibility
    """

    __event_type__: ClassVar[str]
    __schema_version__: ClassVar[str]

    model_config = {
        "validate_assignment": True,
        "frozen": True
    }

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        if cls is BaseEvent:
            return
        
        if not hasattr(cls, "__event_type__"):
            raise TypeError(f"{cls.__name__} missing __event_type__")
        
        if not hasattr(cls, "__schema_version__"):
            raise TypeError(f"{cls.__name__} missing __schema_version__")
        
        meta = getattr(cls, "__pydantic_generic_metadata__", None)
        
        if not meta or not meta.get("args"):
            raise TypeError(
                f"{cls.__name__} must specify payload type BaseEvent[Payload]"
            )

        try:
            schema_registry.register(
                cls.__event_type__,
                cls.__schema_version__,
                cls,
            )
        except ValueError as exc:
            raise TypeError(
                f"{cls.__name__} duplicates schema identity "
                f"{cls.__event_type__} {cls.__schema_version__} "
            ) from exc

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Globally unique identifier for event instance"
    )

    metadata: EventMetadata = Field(
        ...,
        description="Schema identity metadata"
    )

    trace: TraceContext = Field(
        ...,
        description="Distributed trace propagation context"
    )

    event_timestamp: datetime = Field(
        ...,
        description="Timestamp when the event occured at the source system"
    )

    ingest_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Tiimestamp when event entered ingestion boundry"
    )

    payload: PayloadT = Field(
        ...,
        description="Typed payload specific to event type"
    )

    @field_validator("event_timestamp")
    @classmethod
    def validate_event_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_timestamp must be timezone aware")
        return value
    
    @field_validator("ingest_timestamp")
    @classmethod
    def validate_ingest_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("ingest_timestamp must be timezone aware")
        return value
    
    @model_validator(mode="after")
    def validate_timestamp_order(self):
        if self.ingest_timestamp < self.event_timestamp:
            raise ValueError(
                "ingest_timestamp cannot be earlier than event_timestamp"
            )
        return self
