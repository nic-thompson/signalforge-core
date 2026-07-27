"""
Shared envelope and field-policy machinery underlying every schema.

``BaseEvent`` is the event envelope; ``DomainEventPayload`` is the base every
payload inherits from. ``UUIDv4Model``, ``UTCTimestampModel`` and ``SemVerModel``
provide opt-in field policies — a subclass names fields in a ``__..._fields__``
ClassVar to bring them under a rule. See ``docs/base-model-conventions.md``.
"""

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload
from event_schema_contracts.base.identity import UUIDv4Model
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.time import UTCTimestampModel
from event_schema_contracts.base.trace import TraceContext, PipelineStage
from event_schema_contracts.base.versioning import SemVerModel

__all__ = [
    "BaseEvent",
    "DomainEventPayload",
    "EventMetadata",
    "PipelineStage",
    "SemVerModel",
    "TraceContext",
    "UTCTimestampModel",
    "UUIDv4Model",
]
