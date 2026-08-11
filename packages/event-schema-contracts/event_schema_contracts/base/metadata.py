"""
Schema identity metadata carried by every event.

``EventMetadata`` answers three questions about an event: what kind it is,
which version of that contract it conforms to, and which service emitted it.
The first two are what the registry routes on.
"""

from pydantic import BaseModel, Field, field_validator
import re


SEMVER_PATTERN = re.compile(r"^v\d+(\.\d+)*$")
EVENT_TYPE_PATTERN = re.compile(r"^[a-z0-9]+(\.[a-z0-9]+)+$")
SOURCE_PATTERN = re.compile(r"^[a-z0-9]+([.\-_][a-z0-9]+)*$")


class EventMetadata(BaseModel):
    """
    Canonical schema metadata envelope shared across all event contracts.

    Defines schema identity and routing semantics across ingestion pipelines,
    validation workers, feature builders, exporters, and inference services.
    """

    schema_version: str = Field(
        ...,
        description="Schema version identifier (e.g., v1, v1.1, v2)",
    )

    event_type: str = Field(
        ...,
        description="Logical event type identifier (e.g., device.registration)",
    )

    source: str = Field(
        ...,
        description="Originating service emitting the event",
    )

    model_config = {
        "frozen": True,
        "extra": "forbid",
    }

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, value: str) -> str:
        """
        Require the ``v<major>[.<minor>...]`` form, e.g. ``v1`` or ``v1.1``.

        Note this is the registry's version format, which is not the same as
        the strict ``MAJOR.MINOR.PATCH`` semver enforced by ``SemVerModel``.
        """

        if not SEMVER_PATTERN.match(value):
            raise ValueError(
                "schema_version must match pattern v<major>[.<minor>...]"
            )
        return value

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        """
        Require the dotted ``<domain>.<action>`` form, e.g. ``device.registration``.

        The namespacing is what keeps event types from colliding as domains are
        added to the registry.
        """

        if not EVENT_TYPE_PATTERN.match(value):
            raise ValueError(
                "event_type must match pattern <domain>.<action>[.<subaction>...]"
            )
        return value

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        """
        Require a lowercase alphanumeric service name, optionally separated by
        ``.``, ``-`` or ``_``.

        Note the pattern admits the literal ``"unknown"``, which is also what
        ``BaseEvent.inject_metadata`` substitutes when metadata is omitted, so
        a defaulted source cannot be told apart from a declared one.
        """

        if not SOURCE_PATTERN.match(value):
            raise ValueError(
                "source must match pattern [a-z0-9]+"
            )
        return value