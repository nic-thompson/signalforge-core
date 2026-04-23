import uuid
from typing import Dict, Any

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.output.structured_event import StructuredEvent
from telemetry_parser.normalisation.timestamp_utils import TimestampUtils


class EventNormaliser:
    """
    Converts extracted telemetry attributes into schema-versioned structured events.

    Compatible with:
    - analytics ingestion pipelines
    - feature stores
    - replay pipelines
    - dataset export workflows
    """

    DEFAULT_SCHEMA_VERSION = "v1"
    DEFAULT_SOURCE = "telemetry-parser"

    def __init__(
        self,
        replay_mode: bool = False,
        preserve_event_ids: bool = False,
    ) -> None:
        """
        Parameters
        ----------
        replay_mode:
            Enables deterministic replay behaviour.

        preserve_event_ids:
            Prevents regeneration of event identifiers during dataset backfills.
        """

        self.replay_mode = replay_mode
        self.preserve_event_ids = preserve_event_ids

    def normalise(
        self,
        extracted: ExtractedEventFields,
        trace_id: str | None = None,
    ) -> StructuredEvent:

        event_timestamp = TimestampUtils.normalise_event_timestamp(
            extracted.event_timestamp
        )

        ingest_timestamp = (
            extracted.ingest_timestamp
            if self.replay_mode and hasattr(extracted, "ingest_timestamp")
            else TimestampUtils.ingest_timestamp()
        )

        event_id = (
            extracted.event_id
            if self.preserve_event_ids and hasattr(extracted, "event_id")
            else str(uuid.uuid4())
        )

        resolved_trace_id = (
            trace_id
            if trace_id is not None
            else (
                extracted.trace_id
                if self.replay_mode and hasattr(extracted, "trace_id")
                else str(uuid.uuid4())
            )
        )

        event_type = self._map_event_type(extracted)

        payload = self._build_payload(extracted)

        return StructuredEvent(
            schema_version=self.DEFAULT_SCHEMA_VERSION,
            event_id=event_id,
            trace_id=resolved_trace_id,
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