from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any


@dataclass(frozen=True)
class StructuredEvent:

    schema_version: str
    event_id: str
    trace_id: str
    event_timestamp: datetime
    ingest_timestamp: datetime
    event_type: str
    source: str
    payload: Dict[str, Any]


    def __post_init__(self):

        if self.event_timestamp.tzinfo is None:
            raise ValueError("event_timestamp must be timezone-aware")

        if self.ingest_timestamp.tzinfo is None:
            raise ValueError("ingest_timestamp must be timezone-aware")

        if not self.event_id:
            raise ValueError("event_id cannot be empty")


    def to_dict(self) -> Dict[str, Any]:

        data = asdict(self)

        data["event_timestamp"] = self.event_timestamp.isoformat()
        data["ingest_timestamp"] = self.ingest_timestamp.isoformat()

        return data


    def to_json_safe(self) -> Dict[str, Any]:

        return self.to_dict()