from datetime import datetime, timedelta, timezone
from uuid import uuid4
from unittest.mock import MagicMock

import pytest

from telemetry_parser.pipeline.parser_pipeline import ParserPipeline
from event_schema_contracts.base.identity import derive_device_id
from event_schema_contracts.base.metadata import EventMetadata
from event_schema_contracts.base.trace import PipelineStage, TraceContext
from event_schema_contracts.telemetry.sip_registration_event import (
    RegistrationStatus,
    SipRegistrationEvent,
    SipRegistrationPayload,
)
from telemetry_parser.stream.observation import (
    TimestampedChunk,
    TimestampedMessage,
)

BASE_TIME = datetime(2026, 8, 24, 12, 0, 0, tzinfo=timezone.utc)
STORE_ID = "store-0042"


def framed(data: bytes = b"msg", offset_seconds: float = 0.0) -> TimestampedMessage:
    """A framed message observed ``offset_seconds`` after BASE_TIME."""

    return TimestampedMessage(
        timestamp=BASE_TIME + timedelta(seconds=offset_seconds),
        data=data,
    )


class DummyPacket:
    pass


class DummyChunk:
    pass


class DummySIPMessage:
    pass


class DummyExtracted:
    pass


def make_structured_event() -> SipRegistrationEvent:
    """
    A minimal valid event for wiring tests.

    Note that "minimal" now means every required field, correctly typed —
    the previous version passed an empty payload dict, which the schema
    would reject. That the fixture has to be this specific is the point
    of the change it tests.
    """

    now = datetime.now(timezone.utc)

    return SipRegistrationEvent(
        event_id=uuid4(),
        event_timestamp=now,
        ingest_timestamp=now,
        trace=TraceContext(trace_id=uuid4(), pipeline_stage=PipelineStage.INGESTION),
        metadata=EventMetadata(
            event_type=SipRegistrationEvent.__event_type__,
            schema_version=SipRegistrationEvent.__schema_version__,
            source="test",
        ),
        payload=SipRegistrationPayload(
            device_id=derive_device_id(STORE_ID, "headset-12"),
            device_label="headset-12",
            store_id=STORE_ID,
            registration_status=RegistrationStatus.REGISTERED,
            observed_at=now,
        ),
    )


@pytest.fixture
def pipeline():
    pipeline = ParserPipeline(store_id=STORE_ID)

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
    framed_message = framed()
    sip = DummySIPMessage()
    extracted = DummyExtracted()
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = [chunk]
    pipeline.decoder.feed.return_value = [framed_message]
    pipeline.parser.parse.return_value = sip
    pipeline.extractor.extract.return_value = extracted
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([packet]))

    assert events == [structured]


def test_parse_stream_skips_none_sip_messages(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = [framed()]
    pipeline.parser.parse.return_value = None

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([DummyPacket()]))

    assert events == []


def test_parse_stream_skips_none_extracted(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = [framed()]
    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = None

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream([DummyPacket()]))

    assert events == []


def test_trace_id_propagated_to_normaliser(pipeline):
    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = [framed()]

    sip = DummySIPMessage()
    extracted = DummyExtracted()
    structured = make_structured_event()

    pipeline.parser.parse.return_value = sip
    pipeline.extractor.extract.return_value = extracted
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([DummyPacket()], trace_id="trace-123"))

    pipeline.normaliser.normalise.assert_called_with(
        extracted,
        observed_at=BASE_TIME,
        trace_id="trace-123",
    )


def test_multiple_packets_processed_in_order(pipeline):
    packets = [DummyPacket(), DummyPacket()]

    pipeline.reassembler.process_packet.side_effect = [
        ["chunk1"],
        ["chunk2"],
    ]

    pipeline.decoder.feed.side_effect = [
        [framed(b"msg1", 0)],
        [framed(b"msg2", 1)],
    ]

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()

    event1 = make_structured_event()
    event2 = make_structured_event()

    pipeline.normaliser.normalise.side_effect = [event1, event2]
    pipeline.emitter.emit.side_effect = [event1, event2]

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    events = list(pipeline.parse_stream(packets))

    assert events == [event1, event2]


def test_emitter_called_once_per_event(pipeline):
    structured = make_structured_event()

    pipeline.reassembler.process_packet.return_value = ["chunk"]
    pipeline.decoder.feed.return_value = [framed()]

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = structured
    pipeline.emitter.emit.return_value = structured

    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([DummyPacket()]))

    pipeline.emitter.emit.assert_called_once_with(structured)


# ---------------------------------------------------------------
# End-of-stream leftovers are discarded, not emitted
# ---------------------------------------------------------------

# These previously asserted the opposite: that a decoder remainder and
# stranded reassembly segments were parsed and emitted as events. Both
# sources were unsound — a remainder has no header terminator, and
# stranded segments sit behind a gap that never filled — so the events
# they produced were fabricated but indistinguishable downstream from
# real ones. ADR-001 refuses them.


def test_decoder_remainder_is_not_emitted(pipeline):
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = framed(b"truncated")

    pipeline.parser.parse.return_value = DummySIPMessage()
    pipeline.extractor.extract.return_value = DummyExtracted()
    pipeline.normaliser.normalise.return_value = make_structured_event()
    pipeline.emitter.emit.return_value = make_structured_event()

    events = list(pipeline.parse_stream([]))

    assert events == []
    pipeline.emitter.emit.assert_not_called()


def test_stranded_segments_are_not_emitted(pipeline):
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.discard_incomplete.return_value = (2, 40)
    pipeline.decoder.flush.return_value = None

    pipeline.emitter.emit.return_value = make_structured_event()

    events = list(pipeline.parse_stream([]))

    assert events == []
    pipeline.emitter.emit.assert_not_called()


def test_discarded_reassembly_is_reported_to_the_observer():
    observer = MagicMock()
    pipeline = ParserPipeline(store_id=STORE_ID, observer=observer)

    pipeline.reassembler = MagicMock()
    pipeline.decoder = MagicMock()
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.discard_incomplete.return_value = (2, 40)
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([]))

    observer.on_packet_dropped.assert_called_once_with(
        "incomplete_reassembly_at_end_of_stream",
        {"segments": 2, "bytes": 40},
    )


def test_discarded_remainder_is_reported_to_the_observer():
    observer = MagicMock()
    pipeline = ParserPipeline(store_id=STORE_ID, observer=observer)

    pipeline.reassembler = MagicMock()
    pipeline.decoder = MagicMock()
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = framed(b"truncated")

    list(pipeline.parse_stream([]))

    observer.on_packet_dropped.assert_called_once_with(
        "incomplete_message_at_end_of_stream",
        {"bytes": len(b"truncated"), "observed_at": BASE_TIME.isoformat()},
    )


def test_clean_stream_reports_no_drops():
    observer = MagicMock()
    pipeline = ParserPipeline(store_id=STORE_ID, observer=observer)

    pipeline.reassembler = MagicMock()
    pipeline.decoder = MagicMock()
    pipeline.reassembler.process_packet.return_value = []
    pipeline.reassembler.discard_incomplete.return_value = (0, 0)
    pipeline.decoder.flush.return_value = None

    list(pipeline.parse_stream([]))

    observer.on_packet_dropped.assert_not_called()
