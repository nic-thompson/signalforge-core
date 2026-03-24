from event_schema_contracts.base.identity import UUIDv4Model
from event_schema_contracts.base.time import UTCTimestampModel

class DomainEventPayload(
    UUIDv4Model,
    UTCTimestampModel
):
    """
    Shared base class for domain payload schemas.
    
    Provides:
    - UUIDv4
    - UTC timestamp enforcement
    """

    pass