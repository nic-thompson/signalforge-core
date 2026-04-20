import pytest
from datetime import datetime, UTC

from telemetry_parser.extraction.field_mapper import (
    FieldMapper, ExtractedEventFields
)

@pytest.fixture
def mapper():
    return FieldMapper()

# ------------------------------------------------
# registration_status mapping
# ------------------------------------------------

def test_registration_status_register_detected(mapper):
    headers = {"cseq": "1 REGISTER"}

    result = mapper.map_registration_status(headers)

    assert result == "registered"


def test_registration_status_non_register_returns_unknown(mapper):
    headers = {"cseq": "1 INVITE"}

    result = mapper.map_registration_status(headers)
    
    assert result == "unknown"


def test_registration_status_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_registration_status(headers)

    assert result is None


# ---------------------------------------------------------------
# retry_count mapping
# ---------------------------------------------------------------

def test_retry_after_valid_integer(mapper):
    headers = {"retry-after": "3"}

    result = mapper.map_retry_count(headers)

    assert result == 3


def test_retry_after_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_retry_count(headers)

    assert result is None


def test_retry_after_invalid_returns_none(mapper):
    headers = {"retry-after": "banana"}

    result = mapper.map_retry_count(headers)

    assert result is None


# -----------------------------------------------------------
# latency mapping
# -----------------------------------------------------------

def test_latency_valid_float(mapper):
    headers = {"x-latency": "12.5"}

    result = mapper.map_latency(headers)

    assert result == 12.5


def test_latency_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_latency(headers)

    assert result is None


def test_latency_invalid_returns_none(mapper):
    headers = {"x-latency": "NAN????"}

    result = mapper.map_latency(headers)

    assert result is None


# ----------------------------------------------------
# session_duration mapping
# ----------------------------------------------------


def test_session_duration_valid(mapper):
    headers = {"x-session-duration": "42.0"}

    result = mapper.map_session_duration(headers)

    assert result == 42.0


def test_session_duration_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_session_duration(headers)

    assert result is None


def test_session_duration_invalid_returns_none(mapper):
    headers = {"x-session-duration": "invalid"}

    result = mapper.map_session_duration(headers)

    assert result is None


# ---------------------------------------------------
# timestamp parsing
# ---------------------------------------------------

def test_timestamp_parses_zulu_time(mapper):
    headers = {"x-timestamp": "2026-04-20T10:15:30Z"}

    result = mapper.map_timestamp(headers)

    assert result == datetime(2026, 4, 20, 10, 15, 30, tzinfo=UTC)
    assert result.tzinfo is not None
    assert result.utcoffset().total_seconds() == 0


def test_timestamp_parses_offset_time(mapper):
    headers = {"x-timestamp": "2026-04-20T11:15:30+01:00"}

    result = mapper.map_timestamp(headers)

    assert result == datetime(2026, 4, 20, 10, 15, 30, tzinfo=UTC)
    assert result.tzinfo is not None
    assert result.utcoffset().total_seconds() == 0
    


def test_timestamp_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_timestamp(headers)

    assert result is None


def test_timestamp_invalid_returns_none(mapper):
    headers = {"x-timestamp": "not-a-timestamp"}

    result = mapper.map_timestamp(headers)

    assert result is None
