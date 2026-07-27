"""
The base class every domain payload inherits from.
"""

from event_schema_contracts.base.identity import UUIDv4Model
from event_schema_contracts.base.time import UTCTimestampModel

class DomainEventPayload(
    UUIDv4Model,
    UTCTimestampModel
):
    """
    Shared base class for domain payload schemas.

    Combines the two field-policy bases, so a payload subclass can opt its
    fields into either by naming them:

    - ``__uuid_v4_fields__`` / ``__uuid_v4_or_v5_fields__`` from
      ``UUIDv4Model`` — UUID version policy
    - ``__utc_fields__`` from ``UTCTimestampModel`` — UTC enforcement

    Note this carries the UUID *policy* machinery, not a blanket UUIDv4 rule:
    fields listed in ``__uuid_v4_or_v5_fields__`` accept v5 as well, which is
    what makes replay-deterministic identifiers possible. See
    ``docs/base-model-conventions.md``.
    """

    pass