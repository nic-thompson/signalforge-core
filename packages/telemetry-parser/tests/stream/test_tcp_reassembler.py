from datetime import datetime, timedelta, timezone

from telemetry_parser.stream.tcp_reassembler import (
    TCPPacket,
    TCPReassembler,
)


def payloads(chunks) -> list[bytes]:
    return [chunk.data for chunk in chunks]


def timestamps(chunks) -> list[datetime]:
    return [chunk.timestamp for chunk in chunks]

def make_packet(
    seq: int,
    payload: bytes,
    timestamp: datetime,
    *,
    src_ip="10.0.0.1",
    dst_ip="10.0.0.2",
    src_port=1234,
    dst_port=80,
    fin=False,
):
    return TCPPacket(
        src_ip=src_ip,
        dst_ip=dst_ip,
        src_port=src_port,
        dst_port=dst_port,
        sequence_number=seq,
        payload=payload,
        timestamp=timestamp,
        fin=fin,
    )


def test_in_order_packet_delivery():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"hello", ts)

    output = list(r.process_packet(pkt))

    assert payloads(output) == [b"hello"]
    assert timestamps(output) == [ts]


def test_initial_sequence_number_is_respected():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(5000, b"abc", ts)

    output = list(r.process_packet(pkt))

    assert payloads(output) == [b"abc"]


def test_out_of_order_packet_buffering_then_flush():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt1 = make_packet(1000, b"hello", ts)
    pkt2 = make_packet(1010, b"world", ts + timedelta(seconds=1))
    pkt_mid = make_packet(1005, b"12345", ts + timedelta(seconds=2))

    out1 = list(r.process_packet(pkt1))
    out2 = list(r.process_packet(pkt2)) # buffered
    out3 = list(r.process_packet(pkt_mid)) # resolved gap

    assert payloads(out1) == [b"hello"]
    assert out2 == []
    assert payloads(out3) == [b"12345", b"world"]

    # "world" was buffered when it arrived at +1s and released at +2s.
    # It keeps its own arrival time, not the time of the packet that
    # unblocked it — otherwise a gap in the stream would silently move
    # observation times forward.
    assert timestamps(out3) == [
        ts + timedelta(seconds=2),
        ts + timedelta(seconds=1),
    ]


def test_duplicate_packet_is_ignored():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"hello", ts)

    first = list(r.process_packet(pkt))
    second = list(r.process_packet(pkt))

    assert payloads(first) == [b"hello"]
    assert second == []


def test_empty_payload_packet_is_ignored():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"", ts)

    output = list(r.process_packet(pkt))

    assert output == []


def test_buffered_packets_flush_in_order():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt1 = make_packet(1000, b"A", ts)
    pkt3 = make_packet(1002, b"C", ts + timedelta(seconds=1))
    pkt2 = make_packet(1001, b"B", ts + timedelta(seconds=2))

    out1 = list(r.process_packet(pkt1))
    out2 = list(r.process_packet(pkt3)) # buffered
    out3 = list(r.process_packet(pkt2)) # resolves gap

    assert payloads(out1) == [b"A"]
    assert out2 == []
    assert payloads(out3) == [b"B", b"C"]

    # C arrived before B but is released after it; each keeps its own
    # observation time, so the released order is not time-ordered.
    assert timestamps(out3) == [
        ts + timedelta(seconds=2),
        ts + timedelta(seconds=1),
    ]


def test_fin_close_session():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"end", ts, fin=True)

    list(r.process_packet(pkt))

    sessions = list(r.session_tracker.sessions.values())

    assert len(sessions) == 1
    assert sessions[0].end_timestamp == ts


def test_discard_incomplete_drops_and_counts_stranded_segments():
    """
    A segment sitting behind a gap that never filled is dropped, not
    released. It is not contiguous with anything, so there is no honest
    way to frame it — but the count must survive so a lossy capture is
    visible rather than silent.
    """

    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    list(r.process_packet(make_packet(1000, b"A", ts)))
    list(r.process_packet(make_packet(1002, b"CC", ts + timedelta(seconds=1))))

    session = next(iter(r.session_tracker.sessions.values()))
    assert session.buffered_segments != {}

    segments, discarded_bytes = r.discard_incomplete()

    assert segments == 1
    assert discarded_bytes == 2
    assert session.buffered_segments == {}


def test_discard_incomplete_reports_nothing_when_stream_is_clean():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    list(r.process_packet(make_packet(1000, b"A", ts)))

    assert r.discard_incomplete() == (0, 0)


def test_flush_paths_that_fabricated_contiguity_are_gone():
    """
    flush() and flush_session() joined segments across a gap and handed
    the result on to be parsed. Both are removed (ADR-001); these assert
    they stay removed.
    """

    r = TCPReassembler()

    assert not hasattr(r, "flush")
    assert not hasattr(r, "flush_session")
