from typing import Dict, Tuple, Type, Any

from pydantic import ValidationError

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
    - support ingenstion-time validation
    - enable replay-safe schema resolution
    """

    def __init__(self) -> None:
        self._registry: Dict[
            SchemaKey, 
            Type[BaseEvent[Any]]
        ] = {}

    def is_registered(
        self,
        event_type: str,
        schema_version: str,
    ) -> bool:
        return (event_type, schema_version) in self._registry

    def register(
            self,
            event_type: str,
            schema_version: str,
            schema: Type[BaseEvent[Any]],
    ) -> None:
        """
        Register schema for event_type + version.
        """

        key = (event_type, schema_version)

        if key in self._registry:
            raise ValueError(
                f"Schema already registered for {event_type} {schema_version}"
            )
        
        self._registry[key] = schema
    
    def get_schema(
            self,
            event_type: str,
            schema_version: str,
    ) -> Type[BaseEvent[Any]]:
        """
        Resolve schema class for event_type + version.

        Support forward-compatible fallback lookup using the
        closest available compatible schema version.
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
            closest = min(compatible_versions, key=lambda item: item[0])[1]
            return self._registry[(event_type, closest)]
        
        raise KeyError(
            f"No compatible schema registered for {event_type} {schema_version}"
        )
    
    def validate(
            self, 
            event: Dict[str, Any],
    ) -> BaseEvent[Any]:
        """
        Validate raw event dict against registered schema.
        
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
        return sorted(
            version
            for (etype, version) in self._registry
            if etype == event_type
        )
    
    def list_registered(self) -> Dict[SchemaKey, Type[BaseEvent[Any]]]:
        return dict(self._registry)
        
schema_registry = SchemaRegistry()
        