"""
Turns raw SIP message bytes into TCPPacket streams.

This stands in for the *unbuilt* capture component (Gap 1). It is
deliberately written against the same output contract that component
will need to satisfy — an Iterable[TCPPacket] fed into
ParserPipeline.parse_stream() — so that when the real libpcap-based
capture exists, this builder can be swapped out without the
integration test changing shape.

Fragmentation is intentional: messages are split across multiple
packets at non-message boundaries so the test exercises TCPReassembler
rather than handing the parser conveniently pre-assembled messages.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterator

from telemetry_parser.stream.tcp_reassembler import TCPPacket

DEFAULT_SRC_IP = "10.20.0.14"
DEFAULT_DST_IP = "10.20.0.1"
DEFAULT_SRC_PORT = 51234
DEFAULT_DST_PORT = 5060


def packets_from_payload(
    payload: bytes,
    *,
    fragment_size: int | None = None,
    src_ip: str = DEFAULT_SRC_IP,
    dst_ip: str = DEFAULT_DST_IP,
    src_port: int = DEFAULT_SRC_PORT,
    dst_port: int = DEFAULT_DST_PORT,
    start_time: datetime | None = None,
    start_sequence: int = 1000,
    fin: bool = False,
) -> Iterator[TCPPacket]:
    """
    Splits a byte payload into a sequence of TCPPackets on one connection.

    fragment_size=None sends the whole payload in a single packet.
    A small fragment_size forces the reassembler to stitch fragments
    back together before the decoder can frame a complete message.
    """
    if start_time is None:
        start_time = datetime(2026, 8, 21, 9, 15, 0, tzinfo=timezone.utc)

    if fragment_size is None:
        chunks = [payload]
    else:
        chunks = [
            payload[i : i + fragment_size]
            for i in range(0, len(payload), fragment_size)
        ]

    sequence = start_sequence

    for index, chunk in enumerate(chunks):
        is_last = index == len(chunks) - 1

        yield TCPPacket(
            src_ip=src_ip,
            dst_ip=dst_ip,
            src_port=src_port,
            dst_port=dst_port,
            sequence_number=sequence,
            payload=chunk,
            # Each fragment lands a few ms after the previous one, as
            # real packets on a connection would.
            timestamp=start_time + timedelta(milliseconds=index * 5),
            fin=fin and is_last,
        )

        sequence += len(chunk)


def packets_from_messages(
    messages: list[bytes],
    *,
    fragment_size: int | None = None,
    src_port: int = DEFAULT_SRC_PORT,
    **kwargs: object,
) -> list[TCPPacket]:
    """
    Concatenates several SIP messages onto one TCP connection, as they
    would genuinely arrive — back-to-back on the same stream, with
    message boundaries not aligned to packet boundaries.
    """
    combined = b"".join(messages)

    return list(
        packets_from_payload(
            combined,
            fragment_size=fragment_size,
            src_port=src_port,
            **kwargs,  # type: ignore[arg-type]
        )
    )
