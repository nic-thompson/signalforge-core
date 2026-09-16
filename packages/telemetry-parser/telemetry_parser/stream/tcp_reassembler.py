from dataclasses import dataclass
from datetime import datetime
from typing import Iterator

from telemetry_parser.stream.session_tracker import SessionTracker, TCPSession
from telemetry_parser.observability.parser_observer import ParserObserver
from telemetry_parser.stream.observation import TimestampedChunk

@dataclass
class TCPPacket:
    src_ip: str
    dst_ip: str
    src_port: int
    dst_port: int
    sequence_number: int
    payload: bytes
    timestamp: datetime
    fin: bool = False


class TCPReassemblyError(Exception):
    pass


class TCPReassembler:
    """
    Incremental TCP stream reconstruction engine.

    Handles:
    - out of order packets
    - missing fragments
    - session lifecycle tracking
    - deterministic replay-safe ordering
    """

    def __init__(
            self,
            observer: ParserObserver | None = None,
    ) -> None:

        self.session_tracker = SessionTracker(observer)
        self.observer = observer

    
    def process_packet(
        self,
        packet: TCPPacket,
    ) -> Iterator[TimestampedChunk]:

        session = self.session_tracker.get_or_create_session(
            packet.src_ip,
            packet.src_port,
            packet.dst_ip,
            packet.dst_port,
            packet.timestamp,
        )

        yield from self._handle_packet(session, packet)

        if packet.fin:
            self.session_tracker.close_session(
                packet.src_ip,
                packet.src_port,
                packet.dst_ip,
                packet.dst_port,
                packet.timestamp,
            )

    def _handle_packet(
        self,
        session: TCPSession,
        packet: TCPPacket,
    ) -> Iterator[TimestampedChunk]:

        seq = packet.sequence_number

        if session.expected_sequence == 0:
            session.expected_sequence = seq

        if seq < session.expected_sequence:

            if self.observer:
                self.observer.on_packet_dropped(
                    "out_of_order_retransmit",
                    {"sequence": seq},
                )
            # duplicate / retransmitted segment ignored
            return

        if seq > session.expected_sequence:
            session.buffered_segments[seq] = TimestampedChunk(
                timestamp=packet.timestamp,
                data=packet.payload,
            )
            return

        if not packet.payload:
            return

        yield TimestampedChunk(
            timestamp=packet.timestamp,
            data=packet.payload,
        )

        session.expected_sequence += len(packet.payload)

        yield from self._flush_buffer(session)


    def _flush_buffer(
            self,
            session: TCPSession,
    ) -> Iterator[TimestampedChunk]:
        """
        Releases buffered segments that the newly arrived packet has
        made contiguous. Each keeps the observation time of the packet
        it arrived in, not the time of the packet that unblocked it.
        """

        while session.expected_sequence in session.buffered_segments:

            chunk = session.buffered_segments.pop(
                session.expected_sequence
            )

            yield chunk

            session.expected_sequence += len(chunk.data)


    def discard_incomplete(self) -> tuple[int, int]:
        """
        Drops out-of-order segments still held behind gaps that never
        filled, and reports how much was dropped as (segments, bytes).

        These segments are not a stream. Each sits after a hole in the
        sequence space, so joining them would fabricate contiguity that
        was never observed — which is what the previous flush() did before
        handing the result on to be parsed and emitted. Bytes across a gap
        cannot be framed into a SIP message honestly, so they are
        discarded and counted rather than reconstructed.

        See docs/ADR-001-edge-producer-contract.md.
        """

        segments = 0
        discarded_bytes = 0

        for session in self.session_tracker.sessions.values():

            for chunk in session.buffered_segments.values():
                segments += 1
                discarded_bytes += len(chunk.data)

            session.buffered_segments.clear()

        return segments, discarded_bytes
