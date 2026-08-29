import re
import uuid
from datetime import datetime
from ipaddress import ip_address

from event_schema_contracts.base.identity import derive_device_id
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import PipelineStage, TraceContext
from event_schema_contracts.telemetry.sip_registration_event import (
    STORE_ID_PATTERN,
    RegistrationStatus,
    SipRegistrationEvent,
    SipRegistrationPayload,
    SipTransportProtocol,
)

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.normalisation.timestamp_utils import TimestampUtils


class EventNormaliser:
    """
    Converts extracted telemetry attributes into validated domain events.

    Produces ``SipRegistrationEvent`` from ``event-schema-contracts``
    rather than a locally-defined envelope. That library owns the event
    schemas; constructing its types here means the schema validates this
    parser's output at the moment it is produced, instead of a downstream
    component discovering a mismatch — or not discovering it.

    The conversions below are the ones a hand-built payload dictionary
    left implicit, and each was a real defect waiting to happen: a status
    in the wrong case, a transport token the enum does not accept, a
    device label sitting in a field named for an identifier. A
    ``dict[str, Any]`` accepts all of them silently. ``SipRegistrationPayload``
    accepts none.
    """

    SOURCE = "telemetry-parser"

    # This parser observes traffic at the ingestion boundary, so every
    # event it produces enters the pipeline at that stage.
    PIPELINE_STAGE = PipelineStage.INGESTION

    def __init__(
        self,
        store_id: str,
        observer: ParserObserver | None = None,
    ) -> None:
        """
        Parameters
        ----------
        store_id:
            Identity of the store this controller serves, taken from its
            provisioned configuration.

            Required. It is the first value this parser carries that it did
            not parse, and it has to come from configuration because it is
            not in the traffic: a headset registering to a local PBX is
            inside one store's network, so there is exactly one store in
            scope and nothing in the protocol names it. See
            docs/ADR-001-edge-producer-contract.md.

            It is also load-bearing beyond its own field — the ingestion
            boundary derives each device's stable UUIDv5 identity from
            (store_id, device_label), so a controller configured with the
            wrong store silently re-identifies every device it observes.
        """

        self.store_id = self._validate_store_id(store_id)
        self.observer = observer

    def normalise(
        self,
        extracted: ExtractedEventFields,
        observed_at: datetime,
        trace_id: uuid.UUID | None = None,
    ) -> SipRegistrationEvent:
        """
        Parameters
        ----------
        observed_at:
            Packet capture time for the message this event came from — the
            timestamp of the packet carrying its first byte, per ADR-001.

            Required, and the only source of event time. Devices send plain
            RFC 3261, which carries no timestamp, so there is nothing to
            fall back from. An event with no observation time cannot be
            dated at all, and inventing one from the clock would make
            replay non-reproducible.
        """

        event_timestamp = TimestampUtils.normalise_event_timestamp(
            observed_at
        )

        return SipRegistrationEvent(
            event_id=uuid.uuid4(),
            event_timestamp=event_timestamp,
            ingest_timestamp=TimestampUtils.ingest_timestamp(),
            trace=TraceContext(
                trace_id=trace_id if trace_id is not None else uuid.uuid4(),
                pipeline_stage=self.PIPELINE_STAGE,
            ),
            # Supplied rather than left to BaseEvent.inject_metadata, which
            # defaults source to "unknown" — a value that satisfies the
            # field's own pattern and is therefore indistinguishable
            # downstream from a producer that genuinely declared itself
            # unknown. event_type and schema_version come from the class,
            # so they cannot disagree with the schema being constructed.
            metadata=EventMetadata(
                event_type=SipRegistrationEvent.__event_type__,
                schema_version=SipRegistrationEvent.__schema_version__,
                source=self.SOURCE,
            ),
            payload=self._build_payload(extracted, event_timestamp),
        )

    def _build_payload(
        self,
        extracted: ExtractedEventFields,
        observed_at: datetime,
    ) -> SipRegistrationPayload:
        """
        Every field here is a conversion, not a copy.

        - ``device_id`` is derived, not parsed. Nothing in a REGISTER
          carries a device identifier; the label it does carry is unique
          only within a store. See event-schema-contracts ADR-002.
        - ``registration_status`` is uppercased into the enum. The parser
          works in lowercase because that is what its own mapper returns.
        - ``transport_protocol`` comes off the Via header as a raw token.
          The enum accepts TCP, UDP and TLS; anything else raises here,
          at the boundary, rather than being carried onward.
        - ``registration_call_id`` renames ``call_id``. On a REGISTER,
          SIP ``Call-ID`` identifies the registration transaction and not
          a voice call, and the short name reads as call telemetry.
        - ``observed_at`` duplicates the envelope's ``event_timestamp``.
          The schema wants observation time on the payload so a consumer
          holding only the payload can still date it.
        """

        return SipRegistrationPayload(
            device_id=derive_device_id(self.store_id, extracted.device_label),
            device_label=extracted.device_label,
            store_id=self.store_id,
            registration_status=RegistrationStatus(
                extracted.registration_status.upper()
            ),
            observed_at=observed_at,
            transport_protocol=(
                SipTransportProtocol(extracted.transport_protocol.upper())
                if extracted.transport_protocol is not None
                else None
            ),
            # Converted here rather than left to pydantic's coercion, so
            # a malformed address fails in the parser — where the Via
            # header it came from is still in scope — instead of inside
            # schema validation.
            source_ip=(
                ip_address(extracted.source_ip)
                if extracted.source_ip is not None
                else None
            ),
            registration_call_id=extracted.call_id,
        )

    @staticmethod
    def _validate_store_id(store_id: str) -> str:
        """
        Rejects a store identity the schema would reject, at construction.

        This used to check only for a blank value, with a comment
        explaining that the parser could not import the real grammar. It
        can now: ``STORE_ID_PATTERN`` is the schema's own constant, so
        there is one definition rather than a copy free to drift from it.

        Checking at construction rather than per event means a controller
        with a malformed store identity refuses to start, instead of
        emitting a stream rejected one event at a time far from the cause.
        """

        if not isinstance(store_id, str) or not store_id.strip():
            raise ValueError(
                "store_id is required and must be a non-empty string; "
                "it comes from the controller's provisioned configuration"
            )

        # fullmatch, not match: Python's ``$`` also matches before a
        # trailing newline, so re.match would accept "store-1\\n" while
        # the schema rejects it. The point of importing the pattern is
        # to agree with the schema exactly.
        if not re.fullmatch(STORE_ID_PATTERN, store_id):
            raise ValueError(
                f"store_id does not satisfy the sip.registration grammar "
                f"({STORE_ID_PATTERN}): {store_id!r}"
            )

        return store_id
