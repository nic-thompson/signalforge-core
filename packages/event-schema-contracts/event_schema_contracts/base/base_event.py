from datetime import datetime, timezone, timedelta
from typing import Generic, TypeVar, ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.versioning.schema_registry import schema_registry


PayloadT = TypeVar("PayloadT")

MAX_CLOCK_SKEW = timedelta(minutes=5)


class BaseEvent(BaseModel, Generic[PayloadT]):
    """
    Canonical event envelope shared across all telemetry schemas.

    This contract defines ingestion boundaries and guarantees:

    - schema version traceability
    - replay safety
    - distributed trace propagation
    - dataset reproducibility guarantees
    - pipeline observability compatibility
    """

    __event_type__: ClassVar[str]
    __schema_version__: ClassVar[str]

    model_config = {
        "validate_assignment": True,
        "frozen": True,
        "extra": "forbid",
    }

    # ------------------------------------------------------------------
    # Schema registration enforcement
    # ------------------------------------------------------------------

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Ignore abstract base + generic wrapper classes
        if cls is BaseEvent or cls.__name__.startswith("BaseEvent["):
            return

        if "__event_type__" not in cls.__dict__:
            raise TypeError(f"{cls.__name__} missing __event_type__")

        if "__schema_version__" not in cls.__dict__:
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
                f"{cls.__event_type__} {cls.__schema_version__}"
            ) from exc

    # ------------------------------------------------------------------
    # Envelope fields
    # ------------------------------------------------------------------

    event_id: UUID = Field(
        default_factory=uuid4,
        description="Globally unique identifier for event instance",
    )

    metadata: EventMetadata = Field(
        ...,
        description="Schema identity metadata",
    )

    trace: TraceContext = Field(
        ...,
        description="Distributed trace propagation context",
    )

    event_timestamp: datetime = Field(
        ...,
        description="Timestamp when the event occurred at the source system",
    )

    ingest_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp when event entered ingestion boundary",
    )

    payload: PayloadT = Field(
        ...,
        description="Typed payload specific to event type",
    )

    # ------------------------------------------------------------------
    # Timestamp validation
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Metadata auto-injection
    # ------------------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def inject_metadata(cls, data):
        """
        Automatically inject metadata if missing.
        """

        if not isinstance(data, dict):
            return data

        if "metadata" not in data or data["metadata"] is None:
            data["metadata"] = EventMetadata(
                event_type=cls.__event_type__,
                schema_version=cls.__schema_version__,
                source="unknown",
            )

        return data

    # ------------------------------------------------------------------
    # Cross-field validation
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def validate_model(self):
        """
        Validate schema identity + timestamp ordering.
        """

        if self.ingest_timestamp + MAX_CLOCK_SKEW < self.event_timestamp:
            raise ValueError(
                "ingest_timestamp cannot be earlier than event_timestamp"
            )

        if self.metadata.event_type != self.__class__.__event_type__:
            raise ValueError("metadata.event_type mismatch")

        if self.metadata.schema_version != self.__class__.__schema_version__:
            raise ValueError("metadata.schema_version mismatch")

        return self