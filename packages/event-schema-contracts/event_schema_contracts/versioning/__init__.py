"""
Schema version parsing, compatibility rules, and the schema registry.

``schema_registry`` maps ``(event_type, schema_version)`` to schema classes and
is the lookup used at the ingestion boundary. ``parse_version`` and
``ensure_compatibility`` implement the version arithmetic behind it.
"""

from event_schema_contracts.versioning.compatibility import ensure_compatibility, parse_version, SchemaVersion
from event_schema_contracts.versioning.schema_registry import SchemaRegistry, schema_registry

__all__ = [
    "SchemaRegistry",
    "SchemaVersion",
    "ensure_compatibility",
    "parse_version",
    "schema_registry",
]
