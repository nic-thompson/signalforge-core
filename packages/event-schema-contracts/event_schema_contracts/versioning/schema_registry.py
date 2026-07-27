"""
Resolution of ``(event_type, schema_version)`` pairs to schema classes.

The registry is the lookup used at the ingestion boundary: given a raw event
dict off the wire, find the schema class that should validate it.

Population is implicit. Schema classes register themselves as a side effect of
``BaseEvent.__init_subclass__`` running, which happens when the module defining
them is imported. A schema is therefore invisible to the registry until
something has imported its module — importing this module alone registers
nothing, and importing the ``event_schema_contracts`` package root does not
import the domain subpackages. Callers resolving events off the wire need the
relevant domain module imported first; note that ``get_schema`` reports an
unimported schema and a genuinely unregistered one with the same ``KeyError``.

``schema_registry`` at the bottom of this module is the process-wide instance
that ``BaseEvent`` writes to by name. Instantiating ``SchemaRegistry`` directly
produces an empty registry that no schema class will ever populate.
"""

from typing import Dict, Tuple, Type, Any

from pydantic import ValidationError

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.versioning.compatibility import (
    ensure_compatibility, 
    parse_version
)

SchemaKey = Tuple[str, str]

class SchemaRegistry:
    """
    Central schema registry for canonical event contracts.

    Responsibilities:

    - map (event_type, schema_version) -> schema class
    - enforce version availability
    - support ingestion-time validation
    - enable replay-safe schema resolution
    """

    def __init__(self) -> None:
        self._registry: Dict[
            SchemaKey, 
            Type["BaseEvent[Any]"]
        ] = {}

    def is_registered(
        self,
        event_type: str,
        schema_version: str,
    ) -> bool:
        """
        Report whether an exact ``(event_type, schema_version)`` is registered.

        Exact match only — unlike ``get_schema``, this does not consider
        compatible fallbacks, so a version that would resolve successfully can
        still report ``False`` here.
        """

        return (event_type, schema_version) in self._registry

    def register(
            self,
            event_type: str,
            schema_version: str,
            schema: Type["BaseEvent[Any]"],
    ) -> None:
        """
        Register ``schema`` against ``(event_type, schema_version)``.

        Raises ``ValueError`` if the pair is already taken. Called from
        ``BaseEvent.__init_subclass__``, which re-raises that as ``TypeError``.
        """

        key = (event_type, schema_version)

        # Two classes claiming the same (event_type, schema_version) is an
        # ambiguous contract — resolution could not be deterministic — so we
        # refuse the second registration rather than silently overwrite.
        if key in self._registry:
            raise ValueError(
                f"Schema already registered for {event_type} {schema_version}"
            )
        
        self._registry[key] = schema
    
    def get_schema(
            self,
            event_type: str,
            schema_version: str,
    ) -> Type["BaseEvent[Any]"]:
        """
        Resolve the schema class for ``event_type`` at ``schema_version``.

        An exact match wins. Failing that, falls back to the lowest registered
        version >= requested within the same major version. Lowest-not-highest
        is deliberate: it exposes the consumer to the fewest fields added after
        the version it asked for, keeping resolution as close as possible to
        the requested contract.

        Raises ``KeyError`` if nothing compatible is registered — which
        includes the case where the defining module has simply not been
        imported yet.
        """

        key = (event_type, schema_version)

        if key in self._registry:
            return self._registry[key]
        
        # attempt compatible fallback
        compatible_versions = []

        requested = parse_version(schema_version)

        for (etype, version) in self._registry.keys():
            if etype != event_type:
                continue

            candidate = parse_version(version)

            if candidate.major != requested.major:
                continue

            try:
                ensure_compatibility(schema_version, version)
                if candidate >= requested:
                    compatible_versions.append((candidate, version))
            except ValueError:
                continue

        if compatible_versions:
            # Select the lowest compatible version >= requested.
            # This gives the tightest forward-compatible match, avoiding
            # unnecessary exposure to fields added in later minor versions.
            closest = min(compatible_versions, key=lambda item: item[0])[1]
            return self._registry[(event_type, closest)]
        
        raise KeyError(
            f"No compatible schema registered for {event_type} {schema_version}"
        )
    
    def validate(
            self, 
            event: Dict[str, Any],
    ) -> "BaseEvent[Any]":
        """
        Resolve the schema for a raw event dict and validate against it.

        Reads ``metadata.event_type`` and ``metadata.schema_version`` to pick
        the schema, then delegates to it. Raises ``ValueError`` for malformed
        input or failed validation, and ``KeyError`` (from ``get_schema``) when
        no compatible schema is registered.

        Expected structure:

        {
            metadata: {
                event_type: ...
                schema_version: ...
            }
        }
        """

        if not isinstance(event, dict):
            raise ValueError("Event must be a dictionary")

        metadata = event.get("metadata")

        if not isinstance(metadata, dict):
            raise ValueError("Event metadata must be a dictionary")

        event_type = metadata.get("event_type")
        schema_version = metadata.get("schema_version")

        if not event_type or not schema_version:
            raise ValueError(
                "Event missing metadata.event_type or metadata.schema_version fields"
            )
        
        schema = self.get_schema(event_type, schema_version)

        try:
            return schema.model_validate(event)
        except ValidationError as exc:
            raise ValueError(
                f"Schema validation failed for {event_type} {schema_version}"
            ) from exc
        
    def list_versions(self, event_type: str) -> list[str]:
        """
        Return every registered version string for ``event_type``, sorted.

        Sorting is lexical, not semantic, so this is a listing aid rather than
        a way to find the newest version.
        """

        return sorted(
            version
            for (etype, version) in self._registry
            if etype == event_type
        )
    
    def list_registered(self) -> Dict[SchemaKey, Type["BaseEvent[Any]"]]:
        """
        Return a shallow copy of the full registry mapping.

        Reflects only what has been imported so far, so it is a view of the
        current process rather than of every schema the library defines.
        """

        return dict(self._registry)
        
schema_registry = SchemaRegistry()
        