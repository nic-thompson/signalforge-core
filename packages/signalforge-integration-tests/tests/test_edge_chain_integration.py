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
    pipeline = ParserPipeline()

    packets = packets_from_messages(messages, fragment_size=fragment_size)

    for event in pipeline.parse_stream(packets, trace_id="trace-integration-001"):
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
    client = run_chain([HEALTHY_REGISTER])
    payload = decoded_payloads(client)[0]

    assert payload["trace_id"] == "trace-integration-001"


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


def test_message_without_trailing_data_still_flushes() -> None:
    """
    ParserPipeline flushes reassembler and decoder buffers at
    end-of-stream. A message arriving with no subsequent traffic must
    still be emitted rather than stranded in a buffer.
    """
    client = FakeIPCClient()
    publisher = GreengrassEventPublisher(store_id="store-0042", ipc_client=client)
    pipeline = ParserPipeline()

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
    runs = []
    for _ in range(2):
        pipeline = ParserPipeline(replay_mode=True, preserve_event_ids=True)
        runs.append(
            list(pipeline.parse_stream(packets_from_messages(messages)))
        )
    return runs[0], runs[1]


@pytest.mark.xfail(
    reason="DEFECT-1: preserve_event_ids has no effect — ExtractedEventFields "
    "has no event_id attribute, so the hasattr() guard in EventNormaliser "
    "always falls through to uuid.uuid4()",
    strict=True,
)
def test_event_ids_are_stable_across_replay() -> None:
    first, second = _run_twice([HEALTHY_REGISTER, SECOND_DEVICE_REGISTER])

    assert [e.event_id for e in first] == [e.event_id for e in second]


@pytest.mark.xfail(
    reason="DEFECT-2: a REGISTER with no X-Timestamp header falls back to "
    "datetime.now(), so event_timestamp differs on every run. The packet's "
    "own capture timestamp is available but never passed as the fallback.",
    strict=True,
)
def test_event_timestamps_are_stable_across_replay() -> None:
    first, second = _run_twice([HEALTHY_REGISTER, SPARSE_REGISTER])

    assert [e.event_timestamp for e in first] == [
        e.event_timestamp for e in second
    ]


@pytest.mark.xfail(
    reason="DEFECT-3: replay_mode has no effect on ingest_timestamp — "
    "ExtractedEventFields has no ingest_timestamp attribute, so the "
    "hasattr() guard always falls through to wall-clock time",
    strict=True,
)
def test_ingest_timestamps_are_stable_across_replay() -> None:
    first, second = _run_twice([HEALTHY_REGISTER])

    assert [e.ingest_timestamp for e in first] == [
        e.ingest_timestamp for e in second
    ]
