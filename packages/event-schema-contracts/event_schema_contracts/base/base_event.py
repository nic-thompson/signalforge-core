"""
The canonical event envelope every telemetry schema is built on.

``BaseEvent`` supplies the fields common to all events — identity, metadata,
trace context, timestamps — and leaves the domain-specific portion to the
``PayloadT`` type parameter, so a concrete schema is declared as
``BaseEvent[SomePayload]``.

Defining a subclass has a side effect: it registers itself in the global
``schema_registry`` under its ``(__event_type__, __schema_version__)`` pair.
A schema is therefore only resolvable once the module defining it has been
imported. See ``versioning/schema_registry.py`` for what that means in
practice.
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Generic, Self, TypeVar, ClassVar
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.versioning.schema_registry import schema_registry


PayloadT = TypeVar("PayloadT")

# Reject events dated implausibly far in the future: a source clock ahead of
# ingest by more than this is treated as a fault, not a valid event. 5 minutes
# tolerates ordinary NTP drift between source and ingest hosts while still
# catching clearly-broken clocks.
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

    # Envelopes are immutable once constructed: an event is a fact that
    # happened, and replay must reproduce it byte-for-byte, so nothing
    # downstream may mutate it. extra="forbid" makes the ingestion boundary
    # strict — an unrecognised field is a producer/consumer contract mismatch,
    # not something to silently drop and lose on replay.
    model_config = {
        "validate_assignment": True,
        "frozen": True,
        "extra": "forbid",
    }

    # ------------------------------------------------------------------
    # Schema registration enforcement
    # ------------------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Enforce the schema contract and register the subclass.

        Runs when a subclass is *defined*, and raises ``TypeError`` unless the
        subclass declares ``__event_type__``, declares ``__schema_version__``,
        and parameterises its payload as ``BaseEvent[Payload]``. On success the
        class is registered in the global ``schema_registry``; a clash with an
        already-registered ``(event_type, schema_version)`` is also a
        ``TypeError``.

        Abstract and generic-wrapper classes are skipped, so ``BaseEvent``
        itself and intermediate ``BaseEvent[...]`` forms are not registered.
        """

        # Enforce the schema contract at class-definition (import) time, not at
        # first-event time. A schema class that forgets its identity or its
        # payload type is a defect in the contract library itself — we want it
        # to fail the moment the module is imported (i.e. in CI / at service
        # startup), never mid-stream in production ingestion.
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
        """
        Require ``event_timestamp`` to be timezone aware.

        A naive datetime has no single correct interpretation once it crosses a
        process boundary, so it is rejected at the edge rather than silently
        assumed to be UTC.
        """

        if value.tzinfo is None:
            raise ValueError("event_timestamp must be timezone aware")
        return value

    @field_validator("ingest_timestamp")
    @classmethod
    def validate_ingest_timestamp(cls, value: datetime) -> datetime:
        """
        Require ``ingest_timestamp`` to be timezone aware.

        Same reasoning as ``validate_event_timestamp``: the two are compared
        against each other in ``validate_model``, which is only meaningful if
        both carry an explicit offset.
        """

        if value.tzinfo is None:
            raise ValueError("ingest_timestamp must be timezone aware")
        return value

    # ------------------------------------------------------------------
    # Metadata auto-injection
    # ------------------------------------------------------------------

    @model_validator(mode="before")
    @classmethod
    def inject_metadata(cls, data: Any) -> Any:
        """
        Synthesise ``metadata`` when the caller omits it.

        ``event_type`` and ``schema_version`` are taken from the class, which is
        the authoritative source for both. ``source`` is defaulted to the
        literal string ``"unknown"``.

        Note that ``"unknown"`` satisfies the ``source`` pattern on
        ``EventMetadata``, so a synthesised value is indistinguishable
        downstream from one a producer supplied deliberately. Events therefore
        reach consumers attributed to no service whenever metadata is omitted.

        Runs in ``mode="before"``, so it only sees raw input; non-dict input is
        passed through untouched for pydantic to reject.
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
    def validate_model(self) -> Self:
        """
        Check timestamp ordering and schema identity after construction.

        Raises ``ValueError`` if ``event_timestamp`` leads ``ingest_timestamp``
        by more than ``MAX_CLOCK_SKEW``, or if the metadata disagrees with the
        class about ``event_type`` or ``schema_version``.
        """

        if self.event_timestamp > self.ingest_timestamp + MAX_CLOCK_SKEW:
            raise ValueError(
                "event_timestamp is too far in the future relative to ingest_timestamp"
            )

        # The schema class is the single source of truth for its own identity.
        # A caller-supplied metadata.event_type / schema_version that disagrees
        # with the class is a corrupted or misrouted event, so we reject rather
        # than coerce — silently "fixing" it would let mislabelled events into
        # datasets and break replay determinism.
        if self.metadata.event_type != self.__class__.__event_type__:
            raise ValueError("metadata.event_type mismatch")

        if self.metadata.schema_version != self.__class__.__schema_version__:
            raise ValueError("metadata.schema_version mismatch")

        return self