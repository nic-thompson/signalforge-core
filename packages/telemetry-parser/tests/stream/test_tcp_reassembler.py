from datetime import datetime, timedelta, timezone

from telemetry_parser.stream.tcp_reassembler import (
    TCPPacket,
    TCPReassembler,
)

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

    assert output == [b"hello"]


def test_initial_sequence_number_is_respected():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(5000, b"abc", ts)

    output = list(r.process_packet(pkt))

    assert output == [b"abc"]


def test_out_of_order_packet_buffering_then_flush():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt1 = make_packet(1000, b"hello", ts)
    pkt2 = make_packet(1010, b"world", ts + timedelta(seconds=1))
    pkt_mid = make_packet(1005, b"12345", ts + timedelta(seconds=2))

    out1 = list(r.process_packet(pkt1))
    out2 = list(r.process_packet(pkt2)) # buffered
    out3 = list(r.process_packet(pkt_mid)) # resolved gap

    assert out1 == [b"hello"]
    assert out2 == []
    assert out3 == [b"12345", b"world"]


def test_duplicate_packet_is_ignored():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"hello", ts)

    first = list(r.process_packet(pkt))
    second = list(r.process_packet(pkt))

    assert first == [b"hello"]
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

    assert out1 == [b"A"]
    assert out2 == []
    assert out3 == [b"B", b"C"]


def test_fin_close_session():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"end", ts, fin=True)

    list(r.process_packet(pkt))

    sessions = list(r.session_tracker.sessions.values())

    assert len(sessions) == 1
    assert sessions[0].end_timestamp == ts


def test_flush_session_returns_remaining_buffer():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt1 = make_packet(1000, b"A", ts)
    pkt_gap = make_packet(1002, b"C", ts + timedelta(seconds=1)) 

    list(r.process_packet(pkt1))
    list(r.process_packet(pkt_gap)) # buffered

    session = next(iter(r.session_tracker.sessions.values()))

    flushed = r.flush_session(session)

    assert flushed == b"C"
    assert session.buffered_segments == {}


def test_flush_session_returns_none_when_empty():
    r = TCPReassembler()
    ts = datetime.now(timezone.utc)

    pkt = make_packet(1000, b"A", ts)

    list(r.process_packet(pkt))

    session = next(iter(r.session_tracker.sessions.values()))

    flushed = r.flush_session(session)

    assert flushed is None