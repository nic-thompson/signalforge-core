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


    def flush(self) -> Iterator[TimestampedChunk]:
        """
        Flushes all remaining out-of-order buffered payloads across every
        tracked session. Called by the pipeline at end-of-stream to ensure
        deterministic termination.
        """

        for session in list(self.session_tracker.sessions.values()):
            data = self.flush_session(session)
            if data:
                yield data

    def flush_session(
            self,
            session: TCPSession,
    ) -> TimestampedChunk | None:
        """
        Flushes any remaining out-of-order buffered payload when session
        terminates or capture ends.

        Note that this joins segments across a gap that was never filled,
        so the result is not a contiguous stream. The timestamp is that of
        the lowest-sequence segment held, per the first-byte rule.
        """

        if not session.buffered_segments:
            return None

        ordered_sequences = sorted(session.buffered_segments.keys())

        data = b"".join(
            session.buffered_segments[seq].data
            for seq in ordered_sequences
        )

        timestamp = session.buffered_segments[ordered_sequences[0]].timestamp

        session.buffered_segments.clear()

        return TimestampedChunk(timestamp=timestamp, data=data)
