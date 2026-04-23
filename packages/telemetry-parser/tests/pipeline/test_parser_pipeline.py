from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from telemetry_parser.pipeline.parser_pipeline import ParserPipeline
from telemetry_parser.output.structured_event import StructuredEvent


class DummyPacket:
    pass


class DummyChunk:
    pass


class DummyFramedMessage:
    pass


class DummySIPMessage:
    pass


class DummyExtracted:
    pass


def make_structured_event() -> StructuredEvent:
    """
    Creates a minimal valid StructuredEvent for testing.
    """

    return StructuredEvent(
        schema_version="v1",
        event_id="event-1",
        trace_id="trace-1",
        event_timestamp=datetime.now(timezone.utc),
        ingest_timestamp=datetime.now(timezone.utc),
        event_type="device.registration",
        source="test",
        payload={},
    )


@pytest.fixture
def pipeline():
    pipeline = ParserPipeline()

    pipeline.reassembler = MagicMock()
    pipeline.decoder = MagicMock()
    pipeline.parser = MagicMock()
    pipeline.extractor = MagicMock()
    pipeline.normaliser = MagicMock()
    pipeline.emitter = MagicMock()

    return pipeline


def test_parse_stream_single_event(pipeline):
    packet = DummyPacket()
    chunk = DummyChunk()
    framed = DummyFramedMessage()
    sip = DummySIPMessage()
    extracted = DummyExtracted()
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = [chunk]
    pipeline.decoder.feed.return_value = [framed]
    pipeline.parser.parse.return_value = sip
    pipeline.extractor.extract.return_value = extracted
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([packet]))

    assert events == [structured]


def test_parse_stream_skips_none_sip_messages(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = ["framed"]
    pipeline.parser.parse.return_value = None

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([DummyPacket()]))

    assert events == []


def test_parse_stream_skips_none_extracted(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = ["framed"]
    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = None

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([DummyPacket()]))

    assert events == []


def test_trace_id_propagated_to_normaliser(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = ["framed"]

    sip = DummySIPMessage()
    extracted = DummyExtracted()
    structured = make_structured_event()

    pipeline.parser.parse.return_value = sip
    pipeline.extractor.extract.return_value = extracted
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([DummyPacket()], trace_id="trace-123"))

    pipeline.normaliser.normalise.assert_called_with(
        extracted,
        trace_id="trace-123",
    )


def test_decoder_flush_emits_remaining_message(pipeline):
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.flush.return_value = []

    pipeline.decoder.flush.return_value = "remainder"

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    events = list(pipeline.parse_stream([]))

    assert events == [structured]


def test_reassembler_flush_processed(pipeline):
    chunk = "chunk"
    framed = "framed"
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.flush.return_value = [chunk]

    pipeline.decoder.feed.return_value = [framed]
    pipeline.decoder.flush.return_value = None

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    events = list(pipeline.parse_stream([]))

    assert events == [structured]


def test_multiple_packets_processed_in_order(pipeline):
    packets = [DummyPacket(), DummyPacket()]

    pipeline.reassembler.process_packet.side_effect = [
        ["chunk1"],
        ["chunk2"],
    ]

    pipeline.decoder.feed.side_effect = [
        ["msg1"],
        ["msg2"],
    ]

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()

    event1 = make_structured_event()
    event2 = make_structured_event()

    pipeline.normaliser.normalise.side_effect = [event1, event2]
    pipeline.emitter.emit.side_effect = [event1, event2]

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream(packets))

    assert events == [event1, event2]


def test_emitter_called_once_per_event(pipeline):
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = ["msg"]

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.flush.return_value = []
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([DummyPacket()]))

    pipeline.emitter.emit.assert_called_once_with(structured)


def test_no_packets_still_flushes_buffers(pipeline):
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.flush.return_value = []

    pipeline.decoder.flush.return_value = "remainder"

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    events = list(pipeline.parse_stream([]))

    assert events == [structured]


def test_none_decoder_flush_produces_no_event(pipeline):
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.flush.return_value = []

    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([]))

    assert events == []