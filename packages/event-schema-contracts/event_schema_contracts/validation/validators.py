"""
validators.py

Ingestion boundary validation helpers.

These utilities sit between raw inbound payloads and the schema registry,
providing lightweight pre-validation before full Pydantic model construction.

They are intentionally stateless and side-effect-free so they can be used
safely in replay, backfill, and streaming contexts.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Required envelope field names
# ---------------------------------------------------------------------------

_REQUIRED_ENVELOPE_FIELDS: frozenset[str] = frozenset(
    {
        "event_timestamp",
        "payload",
        "trace",
    }
)

_REQUIRED_METADATA_FIELDS: frozenset[str] = frozenset(
    {
        "event_type",
        "schema_version",
        "source",
    }
)


# ---------------------------------------------------------------------------
# Public validators
# ---------------------------------------------------------------------------


def validate_envelope_fields(event: dict[str, Any]) -> list[str]:
    """
    Check that all required top-level envelope fields are present.

    Returns a list of missing field names.  An empty list means the envelope
    is structurally complete.

    Does not perform type or value validation — use the schema registry for
    full Pydantic validation.
    """
    if not isinstance(event, dict):
        raise TypeError(f"Expected dict, got {type(event).__name__}")

    return sorted(_REQUIRED_ENVELOPE_FIELDS - event.keys())


def validate_metadata_fields(event: dict[str, Any]) -> list[str]:
    """
    Check that all required metadata fields are present and non-empty.

    Returns a list of missing or empty field names.

    Expects ``event`` to be the raw envelope dict containing a ``metadata``
    sub-dict.  Returns ``["metadata"]`` immediately if the metadata key is
    absent or is not a dict.
    """
    if not isinstance(event, dict):
        raise TypeError(f"Expected dict, got {type(event).__name__}")

    metadata = event.get("metadata")

    if not isinstance(metadata, dict):
        return ["metadata"]

    missing = []

    for field in sorted(_REQUIRED_METADATA_FIELDS):
        value = metadata.get(field)
        if not value:
            missing.append(f"metadata.{field}")

    return missing


def assert_valid_envelope(event: dict[str, Any]) -> None:
    """
    Raise ``ValueError`` if the event envelope is structurally incomplete.

    Combines envelope and metadata field checks into a single call suitable
    for use at ingestion boundaries where an exception is the desired outcome.
    """
    envelope_missing = validate_envelope_fields(event)

    if envelope_missing:
        raise ValueError(
            f"Event envelope missing required fields: {envelope_missing}"
        )

    metadata_missing = validate_metadata_fields(event)

    if metadata_missing:
        raise ValueError(
            f"Event metadata missing required fields: {metadata_missing}"
        )
