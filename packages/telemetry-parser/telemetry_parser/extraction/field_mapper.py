from dataclasses import dataclass
from typing import Dict, Literal
from datetime import datetime, timezone


@dataclass(frozen=True)
class ExtractedEventFields:
    device_id: str | None
    registration_status: Literal["registered", "unknown"] | None
    latency: float | None
    retry_count: int | None
    transport_protocol: str | None
    session_duration: float | None
    call_id: str | None
    source_ip: str | None
    event_timestamp: datetime | None


class FieldMapper:
    """
    Maps SIP message headers into structured telemetry attributes.

    Designed to support downstream:
    - analytics pipelines
    - feature stores
    - dataset generation workflows
    """

    def map_registration_status(
        self,
        headers: Dict[str, str],
    ) -> Literal["registered", "unknown"] | None:
        
        cseq = headers.get("cseq")

        if not cseq:
            return None
        
        if "REGISTER" in cseq:
            return "registered"
        
        return "unknown"
    
    def map_retry_count(
        self,
        headers: Dict[str, str],
    ) -> int | None:
        
        retry_header = headers.get("retry-after")

        if retry_header is None:
            return None
        
        try:
            return int(retry_header)
        except ValueError:
            return None
        
    def map_latency(
        self,
        headers: Dict[str, str],
    ) -> float | None:
        
        latency_header = headers.get("x-latency")

        if latency_header is None:
            return None
        
        try: 
            return float(latency_header)
        except ValueError:
            return None
        
    
    def map_session_duration(
        self,
        headers: Dict[str, str],
    ) -> float | None:
        
        duration_header = headers.get("x-session-duration")

        if duration_header is None:
            return None
        
        try:
            return float(duration_header)
        except ValueError:
            return None
        
    
    def map_timestamp(
        self,
        headers: Dict[str, str],
    ) -> datetime | None:
        
        ts = headers.get("x-timestamp")

        if ts is None:
            return None
        
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc)
        except Exception:
            return None
        