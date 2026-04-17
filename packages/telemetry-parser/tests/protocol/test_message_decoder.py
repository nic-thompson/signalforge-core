import pytest

from telemetry_parser.protocol.message_decoder import MessageDecoder

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

    messages = list(decoder.feed(HEADER_1))

    assert messages == [HEADER_1]


def test_multiple_headers_single_chunk():
    decoder = MessageDecoder()

    payload = HEADER_1 + HEADER_2

    messages = list(decoder.feed(payload))

    assert messages == [HEADER_1, HEADER_2]


@pytest.mark.parametrize("split_point", [1, 5, 17, 42, len(HEADER_1) - 1])
def test_header_split_across_multiple_chunks(split_point):
    decoder = MessageDecoder()

    part_1 = HEADER_1[:split_point] 
    part_2 = HEADER_1[split_point:]

    message_1 = list(decoder.feed(part_1))
    message_2 = list(decoder.feed(part_2))

    assert message_1 == []
    assert message_2 == [HEADER_1]


def test_multiple_headers_split_across_chunks():
    decoder = MessageDecoder()

    combined = HEADER_1 + HEADER_2

    split_point = len(HEADER_1) + 10

    message_1 = list(decoder.feed(combined[:split_point]))
    message_2 = list(decoder.feed(combined[split_point:]))

    assert message_1 == [HEADER_1]
    assert message_2 == [HEADER_2]


def test_partial_header_remains_buffered():
    decoder = MessageDecoder()

    partial =  HEADER_1[:-10]

    messages = list(decoder.feed(partial))

    assert messages == []

    remainder = decoder.flush()

    assert remainder == partial


def test_flush_returns_none_when_empty():
    decoder = MessageDecoder()

    assert decoder.flush() is None


def test_flush_clears_buffer():
    decoder = MessageDecoder()

    partial = HEADER_1[:-5]

    list(decoder.feed(partial))

    remainder = decoder.flush()

    assert remainder == partial
    assert decoder.flush() is None


def test_incremental_multiple_feeds():
    decoder = MessageDecoder()

    stream = HEADER_1 + HEADER_2

    outputs = []

    for byte in stream:
        outputs.extend(decoder.feed(bytes([byte])))

    assert outputs == [HEADER_1, HEADER_2]

def test_decoder_handles_empty_input():
    decoder = MessageDecoder()

    messages = list(decoder.feed(b""))

    assert messages == []
