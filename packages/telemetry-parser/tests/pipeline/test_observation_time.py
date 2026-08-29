"""
End-to-end checks that event time comes from packet capture.

The pipeline tests alongside this file drive mocked components, so they
verify wiring rather than behaviour. These run the real reassembler,
decoder, parser, extractor and normaliser over raw bytes, because the
thing worth proving is a property of the whole chain: a message with no
timestamp of its own is dated by when it was observed, not by when it
happened to be processed.

Before this, event time fell back to ingestion wall-clock, so parsing the
same capture twice produced different events. See DEFECT-2 and
docs/ADR-001-edge-producer-contract.md.
"""

from datetime import datetime, timedelta, timezone

from telemetry_parser.pipeline.parser_pipeline import ParserPipeline
from telemetry_parser.stream.tcp_reassembler import TCPPacket

CAPTURED_AT = datetime(2026, 8, 24, 9, 30, 0, tzinfo=timezone.utc)
STORE_ID = "store-0042"


def register_message(device: str = "headset-12", call_id: str = "abc123") -> bytes:
    """A plain RFC 3261 REGISTER, carrying no non-standard headers."""

    return (
        b"REGISTER sip:pbx.store.local SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"From: <sip:" + device.encode() + b"@store.local>\r\n"
        b"Call-ID: " + call_id.encode() + b"@10.0.0.5\r\n"
        b"CSeq: 1 REGISTER\r\n"
        b"Content-Length: 0\r\n"
        b"\r\n"
    )


def packet(
    payload: bytes,
    sequence_number: int,
    timestamp: datetime,
) -> TCPPacket:
    return TCPPacket(
        src_ip="10.0.0.5",
        dst_ip="10.0.0.1",
        src_port=5060,
        dst_port=5060,
        sequence_number=sequence_number,
        payload=payload,
        timestamp=timestamp,
    )


def test_event_time_comes_from_packet_capture():
    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [packet(register_message(), 1000, CAPTURED_AT)]
        )
    )

    assert len(events) == 1
    assert events[0].event_timestamp == CAPTURED_AT


def test_event_time_is_not_processing_time():
    """
    The distinguishing test. A capture from the past must produce an
    event dated in the past — if event time were wall-clock, this would
    be dated now and the assertion below would fail by years.
    """

    old_capture = datetime(2019, 3, 1, 14, 0, 0, tzinfo=timezone.utc)

    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [packet(register_message(), 1000, old_capture)]
        )
    )

    assert events[0].event_timestamp == old_capture
    assert events[0].event_timestamp < datetime.now(timezone.utc) - timedelta(days=365)


def test_reparsing_the_same_capture_gives_the_same_event_time():
    """
    Replay determinism, stated as a property: the same bytes and the same
    capture times must yield the same event times on every pass. Wall-clock
    fallback made this false by construction.
    """

    packets = [packet(register_message(), 1000, CAPTURED_AT)]

    first = list(ParserPipeline(store_id=STORE_ID).parse_stream(packets))
    second = list(ParserPipeline(store_id=STORE_ID).parse_stream(packets))

    assert [event.event_timestamp for event in first] == [
        event.event_timestamp for event in second
    ]


def test_message_split_across_packets_takes_the_first_packets_time():
    """
    Per ADR-001, a message is observed when its first byte arrives. The
    second half turning up later must not move the event forward.
    """

    message = register_message()
    split = len(message) // 2

    later = CAPTURED_AT + timedelta(seconds=3)

    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [
                packet(message[:split], 1000, CAPTURED_AT),
                packet(message[split:], 1000 + split, later),
            ]
        )
    )

    assert len(events) == 1
    assert events[0].event_timestamp == CAPTURED_AT


def test_out_of_order_packets_keep_their_own_observation_times():
    """
    Two messages, the second arriving before the first is complete. Each
    is dated by the packet carrying its own first byte, so the event that
    arrived later on the wire keeps the later time even though the gap in
    the stream meant it was released in one go.
    """

    first_message = register_message(device="headset-01", call_id="call-a")
    second_message = register_message(device="headset-02", call_id="call-b")

    second_time = CAPTURED_AT + timedelta(seconds=2)

    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [
                packet(first_message, 1000, CAPTURED_AT),
                packet(second_message, 1000 + len(first_message), second_time),
            ]
        )
    )

    assert len(events) == 2
    assert events[0].event_timestamp == CAPTURED_AT
    assert events[1].event_timestamp == second_time


def test_buffered_segment_keeps_arrival_time_not_release_time():
    """
    A segment held behind a gap is released when the gap fills. It must
    keep the time it arrived, not the time it was unblocked — otherwise a
    dropped packet silently shifts observation times forward.
    """

    message = register_message()
    third = len(message) // 3

    arrived_first = CAPTURED_AT
    arrived_second = CAPTURED_AT + timedelta(seconds=1)
    filled_gap = CAPTURED_AT + timedelta(seconds=9)

    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [
                packet(message[:third], 1000, arrived_first),
                # tail arrives early and is buffered behind the gap
                packet(message[third * 2 :], 1000 + third * 2, arrived_second),
                # middle arrives last and releases the tail
                packet(message[third : third * 2], 1000 + third, filled_gap),
            ]
        )
    )

    assert len(events) == 1
    assert events[0].event_timestamp == arrived_first


def test_store_id_reaches_the_emitted_payload():
    """
    End to end: configuration supplied at the pipeline's construction
    appears on the event, having travelled through a chain that parsed
    nothing of the sort out of the traffic.
    """

    events = list(
        ParserPipeline(store_id="store-0042").parse_stream(
            [packet(register_message(), 1000, CAPTURED_AT)]
        )
    )

    assert events[0].payload.store_id == "store-0042"


def test_pipeline_refuses_to_start_without_a_store_id():
    import pytest

    with pytest.raises(ValueError):
        ParserPipeline(store_id="")


# ---------------------------------------------------------------
# the parser emits the contract type, validated
# ---------------------------------------------------------------

# These are the assertions that replace a whole class of downstream
# surprise. Before this, the parser produced a dict[str, Any] that no
# schema checked, and every mismatch below was found by reading the two
# repositories side by side rather than by anything failing.


def test_pipeline_emits_a_validated_contract_event():
    from event_schema_contracts.telemetry.sip_registration_event import (
        SipRegistrationEvent,
    )

    events = list(
        ParserPipeline(store_id=STORE_ID).parse_stream(
            [packet(register_message(), 1000, CAPTURED_AT)]
        )
    )

    assert isinstance(events[0], SipRegistrationEvent)


def test_emitted_event_declares_its_own_identity():
    """
    ADR-002 in aws-event-pipeline-infra: the publisher passes the
    envelope dump as `detail` and adds nothing, because the identity is
    already in `metadata`. That is only true if the parser puts it there.
    """

    event = next(
        iter(
            ParserPipeline(store_id=STORE_ID).parse_stream(
                [packet(register_message(), 1000, CAPTURED_AT)]
            )
        )
    )

    assert event.metadata.event_type == "sip.registration"
    assert event.metadata.schema_version == "v1"


def test_source_is_declared_not_defaulted():
    """
    BaseEvent defaults metadata.source to "unknown", which satisfies the
    field's pattern and is therefore indistinguishable downstream from a
    producer that genuinely declared itself unknown. Every event this
    parser emits must name it.
    """

    event = next(
        iter(
            ParserPipeline(store_id=STORE_ID).parse_stream(
                [packet(register_message(), 1000, CAPTURED_AT)]
            )
        )
    )

    assert event.metadata.source == "telemetry-parser"


def test_device_id_is_derived_from_store_and_label():
    from event_schema_contracts.base.identity import derive_device_id

    event = next(
        iter(
            ParserPipeline(store_id=STORE_ID).parse_stream(
                [packet(register_message(device="headset-12"), 1000, CAPTURED_AT)]
            )
        )
    )

    assert event.payload.device_id == derive_device_id(STORE_ID, "headset-12")


def test_conversions_the_payload_dict_used_to_leave_implicit():
    """
    Each of these was a real mismatch found by inspection rather than by
    a failure: a lowercase status, a raw Via token, and Call-ID under a
    name that reads as call telemetry.
    """

    from event_schema_contracts.telemetry.sip_registration_event import (
        RegistrationStatus,
        SipTransportProtocol,
    )

    event = next(
        iter(
            ParserPipeline(store_id=STORE_ID).parse_stream(
                [packet(register_message(call_id="abc123"), 1000, CAPTURED_AT)]
            )
        )
    )

    assert event.payload.registration_status is RegistrationStatus.REGISTERED
    assert event.payload.transport_protocol is SipTransportProtocol.TCP
    assert event.payload.registration_call_id == "abc123@10.0.0.5"


def test_pipeline_stage_is_ingestion():
    from event_schema_contracts.base.trace import PipelineStage

    event = next(
        iter(
            ParserPipeline(store_id=STORE_ID).parse_stream(
                [packet(register_message(), 1000, CAPTURED_AT)]
            )
        )
    )

    assert event.trace.pipeline_stage is PipelineStage.INGESTION
