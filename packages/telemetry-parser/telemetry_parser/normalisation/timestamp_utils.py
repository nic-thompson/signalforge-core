from datetime import datetime, timezone

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
        observed_at: datetime,
    ) -> datetime:
        """
        Returns the observation time in UTC.

        This used to walk a fallback chain ending at datetime.now(), which
        meant an event with no timestamp of its own was dated by when it
        happened to be processed — different on every run, so replay could
        not reproduce anything. Observation time is now the only source and
        is always supplied, so there is no chain and no clock read.
        """

        return TimestampUtils._ensure_utc(observed_at)
    

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