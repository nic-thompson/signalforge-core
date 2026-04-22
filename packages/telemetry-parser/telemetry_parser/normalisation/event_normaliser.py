import uuid
from typing import Dict, Any

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.output.structured_event import StructuredEvent
from telemetry_parser.normalisation.timestamp_utils import TimestampUtils

class EventNormaliser:
    """
    Converts extracted telemetry attributes
    into schema-versioned structured events.

    Compatible with:
    - analytics ingestion pipelines
    - feature stores
    - replay pipelines
    - dataset export workflows
    """

    DEFAULT_SCHEMA_VERSION = "v1"
    DEFAULT_SOURCE = "telemetry-parser"

    def normalise(
            self,
            extracted: ExtractedEventFields,
            trace_id: str | None = None
    ) -> StructuredEvent:
        
        event_timestamp = TimestampUtils.normalise_event_timestamp(
            extracted.event_timestamp
        )

        ingest_timestamp = TimestampUtils.ingest_timestamp()

        event_type = self._map_event_type(extracted)

        payload = self._build_payload(extracted)

        return StructuredEvent(
            schema_version=self.DEFAULT_SCHEMA_VERSION,
            event_id=str(uuid.uuid4()),
            trace_id=trace_id or str(uuid.uuid4()),
            event_timestamp=event_timestamp,
            ingest_timestamp=ingest_timestamp,
            event_type=event_type,
            source=self.DEFAULT_SOURCE,
            payload=payload,
        )
    

    def _map_event_type(
            self,
            extracted: ExtractedEventFields,
    ) -> str:
        
        if extracted.registration_status == "registered":
            return "device.registration"
        
        return "device.unknown"
    

    def _build_payload(
            self,
            extracted: ExtractedEventFields,
    ) -> Dict[str, Any]:
        
        return {
            "device_id": extracted.device_id,
            "registration_status": extracted.registration_status,
            "latency": extracted.latency,
            "retry_count": extracted.retry_count,
            "transport_protocol": extracted.transport_protocol,
            "session_duration": extracted.session_duration,
            "call_id": extracted.call_id,
            "source_ip": extracted.source_ip,
        }