from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.protocol.sip_parser import SIPMessage
from .field_mapper import FieldMapper, ExtractedEventFields

class UnsupportedProtocolEvent(Exception):
    pass

class EventExtractor:
    """
    Extracts analytics-ready structured attributes
    from parsed SIP protocol messages.
    """

    def __init__(
        self,
        observer: ParserObserver | None = None
    ) -> None:
        self.mapper = FieldMapper()
        self.observer = observer

    def extract(
        self,
        message: SIPMessage,
    ) -> ExtractedEventFields | None:
        
        if message.method != "REGISTER":
            raise UnsupportedProtocolEvent(
                f"Unsupported SIP method: {message.method}"
            )
        
        headers = message.headers

        registration_status = self.mapper.map_registration_status(
            headers
        )

        retry_count = self.mapper.map_retry_count(headers)

        latency = self.mapper.map_latency(headers)

        session_duration = self.mapper.map_session_duration(headers)

        timestamp = self.mapper.map_timestamp(headers)

        return ExtractedEventFields(
            device_id=message.device_id,
            registration_status=registration_status,
            latency=latency,
            retry_count=retry_count,
            transport_protocol=message.transport,
            session_duration=session_duration,
            call_id=message.call_id,
            source_ip=message.source_ip,
            event_timestamp=timestamp,
        )