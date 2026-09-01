"""
Edge-chain integration test: TCP packets -> ParserPipeline ->
GreengrassEventPublisher.

This is a SEAM test, not a unit test. Every component here already has
its own passing unit suite; what was never verified is that they
actually connect — that ParserPipeline's output is genuinely accepted
by GreengrassEventPublisher's input, over a realistic multi-message,
fragmented packet stream.

No AWS dependency: the publisher is wired to a fake IPC client.
"""

from __future__ import annotations

import json
from uuid import UUID

import pytest

from fixtures.packet_builder import packets_from_messages, packets_from_payload
from fixtures.sip_register_fixtures import (
    HEALTHY_REGISTER,
    INVITE_MESSAGE,
    SECOND_DEVICE_REGISTER,
    SPARSE_REGISTER,
)
from greengrass_publisher.publisher import GreengrassEventPublisher, PublishError
from telemetry_parser.pipeline.parser_pipeline import ParserPipeline

# Fixed rather than generated, so assertions can compare against it.
# parse_stream takes a UUID: it used to accept any string, and this was
# "trace-integration-001" until TraceContext began validating the type.
TRACE_ID = UUID("11111111-2222-3333-4444-555555555555")


class FakeIPCClient:
    """Records publishes instead of talking to real IoT Core."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[dict] = []

    def publish_to_iot_core(self, topic: str, payload: bytes, qos: int) -> None:
        if self.fail:
            raise RuntimeError("simulated IPC failure")
        self.published.append({"topic": topic, "payload": payload, "qos": qos})


def run_chain(
    messages: list[bytes],
    *,
    fragment_size: int | None = None,
    store_id: str = "store-0042",
    client: FakeIPCClient | None = None,
) -> FakeIPCClient:
    """
    Runs the full edge chain: raw SIP bytes -> packets -> parser ->
    publisher, returning the fake client holding whatever was published.
    """
    client = client or FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id=store_id, ipc_client=client)
    pipeline = ParserPipeline(store_id=store_id)

    packets = packets_from_messages(messages, fragment_size=fragment_size)

    for event in pipeline.parse_stream(packets, trace_id=TRACE_ID):
        publisher.publish(event)

    return client


def decoded_payloads(client: FakeIPCClient) -> list[dict]:
    return [json.loads(p["payload"].decode("utf-8")) for p in client.published]


# --------------------------------------------------------------------
# The core seam: does a packet stream actually reach IoT Core publishing?
# --------------------------------------------------------------------


def test_single_register_flows_end_to_end() -> None:
    client = run_chain([HEALTHY_REGISTER])

    assert len(client.published) == 1
    assert client.published[0]["topic"] == "edge/store-0042/telemetry"


def test_published_payload_carries_extracted_sip_fields() -> None:
    client = run_chain([HEALTHY_REGISTER])
    payload = decoded_payloads(client)[0]

    # The device_id the SIP parser pulled out of the From header must
    # survive all the way through normalisation into the published JSON.
    assert "headset-0001" in json.dumps(payload)


def test_trace_id_propagates_from_pipeline_to_published_event() -> None:
    """
    The trace id given to parse_stream must reach the published JSON.

    Two things about it changed with the move to contract types, and both
    are the sort a consumer only discovers by running: it is a UUID
    rather than an arbitrary string, and it is nested under `trace`
    rather than sitting flat on the envelope.
    """

    client = run_chain([HEALTHY_REGISTER])
    payload = decoded_payloads(client)[0]

    assert payload["trace"]["trace_id"] == str(TRACE_ID)


# --------------------------------------------------------------------
# Realistic stream conditions: fragmentation and multiple messages
# --------------------------------------------------------------------


def test_message_split_across_packets_is_reassembled() -> None:
    """
    A single SIP message fragmented across many small packets must still
    produce exactly one published event — this is the TCP reassembly
    seam, and it only shows up when fragments don't align to message
    boundaries.
    """
    client = run_chain([HEALTHY_REGISTER], fragment_size=37)

    assert len(client.published) == 1


def test_multiple_messages_on_one_connection_each_produce_an_event() -> None:
    client = run_chain(
        [HEALTHY_REGISTER, SECOND_DEVICE_REGISTER, SPARSE_REGISTER]
    )

    assert len(client.published) == 3


def test_multiple_fragmented_messages_produce_correct_count() -> None:
    """
    Several messages, back-to-back, fragmented at boundaries that fall
    mid-message. Exercises reassembly and framing together — the case
    most likely to silently drop or duplicate an event.
    """
    client = run_chain(
        [HEALTHY_REGISTER, SECOND_DEVICE_REGISTER, SPARSE_REGISTER],
        fragment_size=53,
    )

    assert len(client.published) == 3


def test_distinct_devices_are_not_collapsed() -> None:
    client = run_chain([HEALTHY_REGISTER, SECOND_DEVICE_REGISTER])
    blob = json.dumps(decoded_payloads(client))

    assert "headset-0001" in blob
    assert "headset-0002" in blob


# --------------------------------------------------------------------
# Documented skip behaviour: non-REGISTER methods
# --------------------------------------------------------------------


def test_invite_is_skipped_without_breaking_the_stream() -> None:
    """
    telemetry_parser explicitly supports REGISTER only; other methods
    raise UnsupportedProtocolEvent and are skipped. The important
    property is that a skipped message doesn't abort the stream —
    surrounding REGISTERs must still flow through.
    """
    client = run_chain([HEALTHY_REGISTER, INVITE_MESSAGE, SECOND_DEVICE_REGISTER])

    assert len(client.published) == 2

    blob = json.dumps(decoded_payloads(client))
    assert "headset-0001" in blob
    assert "headset-0002" in blob


# --------------------------------------------------------------------
# Event identity and publishing contract
# --------------------------------------------------------------------


def test_every_published_event_has_a_distinct_event_id() -> None:
    client = run_chain(
        [HEALTHY_REGISTER, SECOND_DEVICE_REGISTER, SPARSE_REGISTER]
    )
    event_ids = [p["event_id"] for p in decoded_payloads(client)]

    assert len(set(event_ids)) == len(event_ids)


def test_all_events_publish_at_least_once_qos() -> None:
    client = run_chain([HEALTHY_REGISTER, SECOND_DEVICE_REGISTER])

    assert all(
        p["qos"] == GreengrassEventPublisher.QOS_AT_LEAST_ONCE
        for p in client.published
    )


def test_publish_failure_surfaces_rather_than_silently_dropping() -> None:
    """
    If IoT Core publishing fails mid-stream, the chain must raise rather
    than silently losing telemetry — the pipeline caller decides the
    retry/skip policy, not the publisher.
    """
    with pytest.raises(PublishError):
        run_chain([HEALTHY_REGISTER], client=FakeIPCClient(fail=True))


# --------------------------------------------------------------------
# Stream termination
# --------------------------------------------------------------------


def test_complete_message_with_no_trailing_traffic_is_emitted() -> None:
    """
    A complete message needs no flush. It carries its own header
    terminator, so the decoder frames it on arrival rather than leaving
    it buffered — and it is emitted whether or not further traffic
    follows.

    This test was named ...still_flushes and described end-of-stream
    buffer flushing as what rescued it. That was never what happened
    here, and flushing has since been removed: it joined reassembly
    segments across gaps that never filled and framed remainders with no
    terminator, fabricating events indistinguishable from real ones. The
    pipeline now discards and reports those instead. The assertion below
    is unchanged, because it never depended on the flush.
    """
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)
    pipeline = ParserPipeline(store_id="store-0042")

    packets = list(packets_from_payload(HEALTHY_REGISTER, fin=True))

    for event in pipeline.parse_stream(packets):
        publisher.publish(event)

    assert len(client.published) == 1


# --------------------------------------------------------------------
# Replay determinism — SignalForge's headline property.
#
# These tests are xfail because they currently FAIL. They assert the
# behaviour SignalForge claims; leaving them here (rather than
# asserting the broken behaviour, or deleting them) means they will
# start passing the moment the defects in DEFECTS.md are fixed, and
# will show up as unexpected passes rather than being forgotten.
# --------------------------------------------------------------------


def _run_twice(messages: list[bytes]) -> tuple[list, list]:
    """
    Parses the same messages twice.

    This used to pass replay_mode=True and preserve_event_ids=True.
    Neither flag did anything — both guarded on fields
    ExtractedEventFields has never carried — and both have been removed.
    Determinism is no longer a mode to opt into.
    """
    runs = []
    for _ in range(2):
        pipeline = ParserPipeline(store_id="store-0042")
        runs.append(
            list(pipeline.parse_stream(packets_from_messages(messages)))
        )
    return runs[0], runs[1]


@pytest.mark.xfail(
    reason="DEFECT-1: event_id is still uuid.uuid4() per event. The "
    "preserve_event_ids flag this originally blamed has been removed — it "
    "never worked — but nothing replaced it. event-schema-contracts now "
    "publishes derive(role, *parts), so deriving the id from replay-stable "
    "coordinates is possible; it has not been done.",
    strict=True,
)
def test_event_ids_are_stable_across_replay() -> None:
    first, second = _run_twice([HEALTHY_REGISTER, SECOND_DEVICE_REGISTER])

    assert [e.event_id for e in first] == [e.event_id for e in second]


def test_event_timestamps_are_stable_across_replay() -> None:
    """
    DEFECT-2, fixed. Event time now comes from the packet carrying a
    message's first byte, with no fallback — the X-Timestamp header this
    once preferred was never defined by anything, and the wall-clock
    fallback beneath it made two parses of one capture differ.
    """
    first, second = _run_twice([HEALTHY_REGISTER, SPARSE_REGISTER])

    assert [e.event_timestamp for e in first] == [
        e.event_timestamp for e in second
    ]


@pytest.mark.xfail(
    reason="DEFECT-3: ingest_timestamp is still wall-clock. The replay_mode "
    "flag this originally blamed has been removed — it never worked. Unlike "
    "event_timestamp, ingest time is arguably meant to differ per run: it "
    "records when this parse happened, not when the traffic was observed. "
    "Whether it belongs in a byte-identity comparison is an open question.",
    strict=True,
)
def test_ingest_timestamps_are_stable_across_replay() -> None:
    first, second = _run_twice([HEALTHY_REGISTER])

    assert [e.ingest_timestamp for e in first] == [
        e.ingest_timestamp for e in second
    ]
