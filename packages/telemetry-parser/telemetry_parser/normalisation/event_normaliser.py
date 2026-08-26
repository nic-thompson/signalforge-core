import uuid
from datetime import datetime
from typing import Dict, Any

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.observability.parser_observer import ParserObserver
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

    # A constant rather than a choice. This used to select between
    # "sip.registration" and "sip.unknown", but the extractor now rejects
    # the malformed messages that produced the latter, so there is one
    # event type. "sip.registration" rather than "device.registration":
    # that identity was already registered by event-schema-contracts for
    # device provisioning, a different domain with an incompatible field
    # set. See event-schema-contracts ADR-002.
    EVENT_TYPE = "sip.registration"

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
        trace_id: str | None = None,
    ) -> StructuredEvent:
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

        ingest_timestamp = TimestampUtils.ingest_timestamp()

        event_id = str(uuid.uuid4())

        resolved_trace_id = (
            trace_id if trace_id is not None else str(uuid.uuid4())
        )

        payload = self._build_payload(extracted)

        return StructuredEvent(
            schema_version=self.DEFAULT_SCHEMA_VERSION,
            event_id=event_id,
            trace_id=resolved_trace_id,
            event_timestamp=event_timestamp,
            ingest_timestamp=ingest_timestamp,
            event_type=self.EVENT_TYPE,
            source=self.DEFAULT_SOURCE,
            payload=payload,
        )

    def _build_payload(
        self,
        extracted: ExtractedEventFields,
    ) -> Dict[str, Any]:

        return {
            "store_id": self.store_id,
            "device_label": extracted.device_label,
            "registration_status": extracted.registration_status,
            "transport_protocol": extracted.transport_protocol,
            "call_id": extracted.call_id,
            "source_ip": extracted.source_ip,
        }

    @staticmethod
    def _validate_store_id(store_id: str) -> str:
        """
        Rejects a store identity that is missing or malformed enough to be
        certainly a misconfiguration.

        This deliberately stops short of the full grammar that
        ``sip.registration v1`` enforces. This parser does not depend on
        ``event-schema-contracts``, so it cannot import that pattern, and
        copying the regex here would create a second definition free to
        drift from the first — which is the failure this whole line of work
        has been unpicking. The authoritative check belongs at the ingestion
        boundary, which owns the contract.

        What is caught here is the case worth catching early: an unset or
        blank configuration value. A controller with no store identity
        should refuse to start rather than emit a stream of events that are
        rejected one at a time, far from the cause.
        """

        if not isinstance(store_id, str) or not store_id.strip():
            raise ValueError(
                "store_id is required and must be a non-empty string; "
                "it comes from the controller's provisioned configuration"
            )

        if store_id != store_id.strip() or any(c.isspace() for c in store_id):
            raise ValueError(
                f"store_id must not contain whitespace, got {store_id!r}"
            )

        return store_id
