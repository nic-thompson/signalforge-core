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
        """
        Returns the extracted fields, or None if the message is a REGISTER
        that contradicts itself and cannot be described.

        A missing or disagreeing CSeq used to produce an event typed
        "sip.unknown". That was a parse failure dressed as an event type:
        no schema was ever registered for it, and the event it produced
        carried a null registration_status into a payload whose schema
        declares that field required — so it could not have validated
        anywhere. Malformed input is dropped and reported, as it is
        everywhere else in this pipeline. See
        docs/ADR-001-edge-producer-contract.md.
        """

        if message.method != "REGISTER":
            raise UnsupportedProtocolEvent(
                f"Unsupported SIP method: {message.method}"
            )

        headers = message.headers

        registration_status = self.mapper.map_registration_status(
            headers
        )

        if registration_status is None:
            if self.observer:
                self.observer.on_parse_error(
                    "register_cseq_mismatch",
                )
            return None

        # A REGISTER whose From header carries no user part names no
        # device. Device identity is derived from (store_id,
        # device_label), so without the label there is nothing to
        # identify and no event worth emitting.
        if message.device_label is None:
            if self.observer:
                self.observer.on_parse_error(
                    "register_missing_device_label",
                )
            return None

        return ExtractedEventFields(
            device_label=message.device_label,
            registration_status=registration_status,
            transport_protocol=message.transport,
            call_id=message.call_id,
            source_ip=message.source_ip,
        )