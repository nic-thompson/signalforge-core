from typing import Iterable, Iterator
from uuid import UUID

from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.stream.tcp_reassembler import TCPReassembler, TCPPacket
from telemetry_parser.protocol.message_decoder import MessageDecoder
from telemetry_parser.protocol.sip_parser import SIPParser
from telemetry_parser.extraction.event_extractor import EventExtractor, UnsupportedProtocolEvent
from telemetry_parser.normalisation.event_normaliser import EventNormaliser
from telemetry_parser.output.event_emitter import EventEmitter
from event_schema_contracts.telemetry.sip_registration_event import (
    SipRegistrationEvent,
)


class ParserPipeline:
    """
    End-to-end telemetry parsing pipeline.

    Supports:
    - streaming ingestion
    - deterministic replay (when upstream components are configured accordingly)
    - dataset regeneration
    - feature pipeline compatibility
    """

    def __init__(
        self,
        store_id: str,
        observer: ParserObserver | None = None
    ) -> None:
        """
        Constructs a streaming-safe telemetry parsing pipeline.

        Parameters
        ----------
        store_id:
            Identity of the store this controller serves, from its
            provisioned configuration. Required — it is not present in the
            traffic and cannot be inferred from it. See
            docs/ADR-001-edge-producer-contract.md.

        """

        self.store_id = store_id
        self.observer = observer

        self.reassembler = TCPReassembler(observer)
        self.decoder = MessageDecoder(observer)
        self.parser = SIPParser(observer)
        self.extractor = EventExtractor(observer)

        self.normaliser = EventNormaliser(
            store_id=store_id,
            observer=observer,
        )

        self.emitter = EventEmitter(
            observer=observer
        )

    def parse_stream(
        self,
        packets: Iterable[TCPPacket],
        trace_id: UUID | None = None,
    ) -> Iterator[SipRegistrationEvent]:
        """
        Parses a TCP packet stream into structured telemetry events.

        Ensures:

        - streaming-safe incremental processing
        - deterministic buffer flushing at end-of-stream
        - compatibility with dataset regeneration workflows
        """

        for packet in packets:

            for chunk in self.reassembler.process_packet(packet):

                for framed_message in self.decoder.feed(chunk):

                    sip_message = self.parser.parse(framed_message.data)

                    if sip_message is None:
                        continue

                    try:
                        extracted = self.extractor.extract(sip_message)
                    except UnsupportedProtocolEvent:
                        continue

                    if extracted is None:
                        continue

                    structured_event = self.normaliser.normalise(
                        extracted,
                        observed_at=framed_message.timestamp,
                        trace_id=trace_id,
                    )

                    yield self.emitter.emit(structured_event)

        # Ensure deterministic termination behaviour
        self._discard_incomplete_buffers()

    def _discard_incomplete_buffers(self) -> None:
        """
        Discards whatever remains buffered at end-of-stream, and reports
        it to the observer. Emits nothing.

        This method used to parse the leftovers and emit events from them.
        Both sources were unsound. The reassembler held segments stranded
        behind gaps that never filled, so joining them produced bytes that
        were never contiguous on the wire. The decoder held a fragment with
        no header terminator, which is by definition an incomplete SIP
        message. Events built from either were indistinguishable downstream
        from events built from clean captures.

        Dropping them silently would be no better, so what was discarded is
        reported instead: the count is a signal that a capture was lossy,
        which is worth knowing and was previously hidden inside plausible
        looking events.

        See docs/ADR-001-edge-producer-contract.md.
        """

        segments, segment_bytes = self.reassembler.discard_incomplete()

        if segments and self.observer:
            self.observer.on_packet_dropped(
                "incomplete_reassembly_at_end_of_stream",
                {
                    "segments": segments,
                    "bytes": segment_bytes,
                },
            )

        remainder = self.decoder.flush()

        if remainder is not None and self.observer:
            self.observer.on_packet_dropped(
                "incomplete_message_at_end_of_stream",
                {
                    "bytes": len(remainder.data),
                    "observed_at": remainder.timestamp.isoformat(),
                },
            )
