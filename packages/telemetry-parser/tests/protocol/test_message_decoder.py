from datetime import datetime, timedelta, timezone

import pytest

from telemetry_parser.protocol.message_decoder import MessageDecoder
from telemetry_parser.stream.observation import TimestampedChunk

BASE_TIME = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)


def chunk(data: bytes, offset_seconds: float = 0.0) -> TimestampedChunk:
    """A chunk observed ``offset_seconds`` after BASE_TIME."""

    return TimestampedChunk(
        timestamp=BASE_TIME + timedelta(seconds=offset_seconds),
        data=data,
    )


def payloads(messages) -> list[bytes]:
    return [message.data for message in messages]

HEADER_1 = (
    b"INVITE sip:user@example.com SIP/2.0\r\n"
    b"Via: SIP/2.0/TCP host\r\n"
    b"Content-Length: 0\r\n"
    b"\r\n"
)

HEADER_2 = (
    b"SIP/2.0 200 OK\r\n"
    b"Via: SIP/2.0/TCP host\r\n"
    b"Content-Length: 0\r\n"
    b"\r\n"
)

def test_single_complete_header():
    decoder = MessageDecoder()

    messages = list(decoder.feed(chunk(HEADER_1)))

    assert payloads(messages) == [HEADER_1]


def test_multiple_headers_single_chunk():
    decoder = MessageDecoder()

    payload = HEADER_1 + HEADER_2

    messages = list(decoder.feed(chunk(payload)))

    assert payloads(messages) == [HEADER_1, HEADER_2]


@pytest.mark.parametrize("split_point", [1, 5, 17, 42, len(HEADER_1) - 1])
def test_header_split_across_multiple_chunks(split_point):
    decoder = MessageDecoder()

    part_1 = HEADER_1[:split_point] 
    part_2 = HEADER_1[split_point:]

    message_1 = list(decoder.feed(chunk(part_1, 0)))
    message_2 = list(decoder.feed(chunk(part_2, 5)))

    assert message_1 == []
    assert payloads(message_2) == [HEADER_1]

    # First byte arrived in part_1, so the message is observed then —
    # not when the chunk that completed it turned up five seconds later.
    assert message_2[0].timestamp == BASE_TIME


def test_multiple_headers_split_across_chunks():
    decoder = MessageDecoder()

    combined = HEADER_1 + HEADER_2

    split_point = len(HEADER_1) + 10

    message_1 = list(decoder.feed(chunk(combined[:split_point], 0)))
    message_2 = list(decoder.feed(chunk(combined[split_point:], 5)))

    assert payloads(message_1) == [HEADER_1]
    assert payloads(message_2) == [HEADER_2]

    # HEADER_2 begins inside the first chunk, so it is observed at
    # BASE_TIME even though it was only completed by the second.
    assert message_1[0].timestamp == BASE_TIME
    assert message_2[0].timestamp == BASE_TIME


def test_partial_header_remains_buffered():
    decoder = MessageDecoder()

    partial =  HEADER_1[:-10]

    messages = list(decoder.feed(chunk(partial)))

    assert messages == []

    remainder = decoder.flush()

    assert remainder is not None
    assert remainder.data == partial
    assert remainder.timestamp == BASE_TIME


def test_flush_returns_none_when_empty():
    decoder = MessageDecoder()

    assert decoder.flush() is None


def test_flush_clears_buffer():
    decoder = MessageDecoder()

    partial = HEADER_1[:-5]

    list(decoder.feed(chunk(partial)))

    remainder = decoder.flush()

    assert remainder is not None
    assert remainder.data == partial
    assert decoder.flush() is None


def test_incremental_multiple_feeds():
    decoder = MessageDecoder()

    stream = HEADER_1 + HEADER_2

    outputs = []

    for index, byte in enumerate(stream):
        outputs.extend(decoder.feed(chunk(bytes([byte]), index)))

    assert payloads(outputs) == [HEADER_1, HEADER_2]

    # Fed one byte per second, each message is observed at the second its
    # own first byte arrived.
    assert outputs[0].timestamp == BASE_TIME
    assert outputs[1].timestamp == BASE_TIME + timedelta(seconds=len(HEADER_1))

def test_decoder_handles_empty_input():
    decoder = MessageDecoder()

    messages = list(decoder.feed(chunk(b"")))

    assert messages == []
