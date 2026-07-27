"""
Standalone envelope validation helpers.

Checks the envelope's shape independently of the registry, for callers that
need to validate structure without resolving a schema class.
"""

from event_schema_contracts.validation.validators import (
    assert_valid_envelope,
    validate_envelope_fields,
    validate_metadata_fields,
)

__all__ = [
    "assert_valid_envelope",
    "validate_envelope_fields",
    "validate_metadata_fields",
]
