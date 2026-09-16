from typing import Optional, Dict, Any
from datetime import datetime


class ParserObserver:
    """
    Structured observability hook interface for telemetry parsing lifecycle.

    Designed for integration with:
    - structured-logging-python
    - OpenTelemetry exporters
    - ingestion monitoring dashboards
    """

    def on_session_start(
        self,
        session_id: str,
        metadata: Dict[str, Any],
    ) -> None:
        pass

    def on_session_end(
        self,
        session_id: str,
        duration_seconds: float,
    ) -> None:
        pass

    def on_packet_dropped(
        self,
        reason: str,
        metadata: Dict[str, Any] | None = None,
    ) -> None:
        pass

    def on_message_reconstructed(
        self,
        size_bytes: int,
        timestamp: datetime,
    ) -> None:
        pass

    def on_parse_error(
        self,
        error_type: str,
        raw_size: Optional[int] = None,
    ) -> None:
        pass

    def on_event_emitted(
        self,
        event_type: str,
        event_id: str,
    ) -> None:
        pass