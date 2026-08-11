"""
Canonical, versioned event schema contracts for the SignalForge platform.

Every event schema is a ``BaseEvent[Payload]`` subclass: a shared envelope
(identity, metadata, trace context, timestamps) wrapped around a domain-specific
payload. Schemas are grouped by domain into the ``telemetry``, ``detection``,
``features`` and ``alerts`` subpackages, with the shared machinery in ``base``,
``versioning`` and ``validation``.

Importing this package exposes the registry and the version helpers, but does
NOT import the domain subpackages — so ``schema_registry`` starts empty.
Schemas register themselves when their defining module is imported, so resolve
events only after importing the relevant domain subpackage, e.g.
``import event_schema_contracts.telemetry``.
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
