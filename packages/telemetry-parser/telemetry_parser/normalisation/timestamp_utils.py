from datetime import datetime, timezone
from typing import Optional 

class TimestampUtils:
    """
    Provides deterministic timestamp normalisation utilities.

    Ensures compatibility with:
    - analytics pipelines
    - replay workflows
    - dataset regeneration
    """

    @staticmethod
    def normalise_event_timestamp(
        extracted_timestamp: datetime | None,
        fallback_timestamp: datetime | None = None,
    ) -> datetime:
        """
        Uses extracted timestamp if present,
        otherwise falls back to provided timestamp,
        otherwise uses current UTC time.
        """

        if extracted_timestamp is not None:
            return TimestampUtils._ensure_utc(extracted_timestamp)
        
        if fallback_timestamp is not None:
            return TimestampUtils._ensure_utc(fallback_timestamp)
        
        return datetime.now(timezone.utc)
    

    @staticmethod
    def ingest_timestamp() -> datetime:
        """
        Return ingestion timestamp UTC.
        """

        return datetime.now(timezone.utc)
    

    @staticmethod
    def _ensure_utc(ts: datetime) -> datetime:

        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        
        return ts.astimezone(timezone.utc)