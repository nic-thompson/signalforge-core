from datetime import datetime, timedelta, timezone

from telemetry_parser.stream.session_tracker import SessionTracker


def test_creates_session_with_expected_fields():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    session = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    assert session.source_ip == "10.0.0.1"
    assert session.destination_ip == "10.0.0.2"
    assert session.source_port == 1234
    assert session.destination_port == 80

    assert session.start_timestamp == ts
    assert session.last_activity_timestamp == ts
    assert session.end_timestamp is None


def test_session_id_is_deterministic_string():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    session = tracker.get_or_create_session(
        src_ip="10.0.0.1",
        src_port=1234,
        dst_ip="10.0.0.2",
        dst_port=80,
        timestamp=ts,
    )
                        
    expected_prefix = "('10.0.0.1', 1234, '10.0.0.2', 80)-"

    assert session.session_id.startswith(expected_prefix)


def test_reuses_existing_session_object():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    session_a = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    session_b = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    assert session_a is session_b


def test_updates_last_activity_timestamp():
    tracker = SessionTracker()

    ts1 = datetime.now(timezone.utc)
    ts2 = ts1 + timedelta(seconds=5)

    session = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts1,
    )

    session = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts2,
    )


    assert session.last_activity_timestamp == ts2


def test_bidirectional_packets_map_to_same_session():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    forward = tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    reverse = tracker.get_or_create_session(
        "10.0.0.2",
        80,
        "10.0.0.1",
        1234,
        ts,
    )

    assert forward is reverse


def test_close_session_sets_end_timestamp():
    tracker = SessionTracker()

    ts_start = datetime.now(timezone.utc)
    ts_end = ts_start + timedelta(seconds=10)

    tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts_start,
    )


    tracker.close_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts_end,
    )

    session = next(iter(tracker.sessions.values()))

    assert session.end_timestamp == ts_end


def test_close_session_is_noop_if_session_missing():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    tracker.close_session(
        "192.168.1.1",
        1111,
        "192.168.1.2",
        2222,
        ts,
    )

    assert len(tracker.sessions) == 0


def test_active_sessions_returns_only_open_sessions():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    tracker.get_or_create_session(
        "10.0.0.3",
        5555,
        "10.0.0.4",
        443,
        ts,
    )

    tracker.close_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts + timedelta(seconds=1),
    )

    active = list(tracker.active_sessions())

    assert len(active) == 1
    assert active[0].source_ip == "10.0.0.3"


def test_active_sessions_return_generator():
    tracker = SessionTracker()

    ts = datetime.now(timezone.utc)

    tracker.get_or_create_session(
        "10.0.0.1",
        1234,
        "10.0.0.2",
        80,
        ts,
    )

    active = tracker.active_sessions()

    assert hasattr(active, "__iter__")
    assert not isinstance(active, list)

