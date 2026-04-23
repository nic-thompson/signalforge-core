from typing import Iterable, Iterator

from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.stream.tcp_reassembler import TCPReassembler, TCPPacket
from telemetry_parser.protocol.message_decoder import MessageDecoder
from telemetry_parser.protocol.sip_parser import SIPParser
from telemetry_parser.extraction.event_extractor import EventExtractor
from telemetry_parser.normalisation.event_normaliser import EventNormaliser
from telemetry_parser.output.event_emitter import EventEmitter
from telemetry_parser.output.structured_event import StructuredEvent


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
        replay_mode: bool = False,
        preserve_event_ids: bool = False,
        observer: ParserObserver | None = None
    ) -> None:
        """
        Constructs a streaming-safe telemetry parsing pipeline.

        Parameters
        ----------
        replay_mode:
            Enables deterministic replay semantics where supported by downstream components.

        preserve_event_ids:
            Prevents regeneration of event identifiers during dataset backfills.
        """

        self.replay_mode = replay_mode
        self.preserve_event_ids = preserve_event_ids
        self.observer = observer

        self.reassembler = TCPReassembler(observer)
        self.decoder = MessageDecoder(observer)
        self.parser = SIPParser(observer)
        self.extractor = EventExtractor(observer)

        self.normaliser = EventNormaliser(
            replay_mode=replay_mode,
            preserve_event_ids=preserve_event_ids,
            observer=observer,
        )

        self.emitter = EventEmitter(
            preserve_event_ids=preserve_event_ids,
            observer=observer
        )

    def parse_stream(
        self,
        packets: Iterable[TCPPacket],
        trace_id: str | None = None,
    ) -> Iterator[StructuredEvent]:
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

                    sip_message = self.parser.parse(framed_message)

                    if sip_message is None:
                        continue

                    extracted = self.extractor.extract(sip_message)

                    if extracted is None:
                        continue

                    structured_event = self.normaliser.normalise(
                        extracted,
                        trace_id=trace_id,
                    )

                    yield self.emitter.emit(structured_event)

        # Ensure deterministic termination behaviour
        yield from self._flush_buffers(trace_id)

    def _flush_buffers(
        self,
        trace_id: str | None = None,
    ) -> Iterator[StructuredEvent]:
        """
        Flushes buffered state from the TCP reassembler and message decoder.

        Required for:

        - deterministic replay completion
        - SIP frame boundary correctness
        - offline PCAP dataset regeneration
        """

        # Flush TCP reassembly buffers first
        for chunk in self.reassembler.flush():

            for framed_message in self.decoder.feed(chunk):

                sip_message = self.parser.parse(framed_message)

                if sip_message is None:
                    continue

                extracted = self.extractor.extract(sip_message)

                if extracted is None:
                    continue

                structured_event = self.normaliser.normalise(
                    extracted,
                    trace_id=trace_id,
                )

                yield self.emitter.emit(structured_event)

        # Flush decoder remainder
        remainder = self.decoder.flush()

        if not remainder:
            return

        sip_message = self.parser.parse(remainder)

        if sip_message is None:
            return

        extracted = self.extractor.extract(sip_message)

        if extracted is None:
            return

        structured_event = self.normaliser.normalise(
            extracted,
            trace_id=trace_id,
        )

        yield self.emitter.emit(structured_event)