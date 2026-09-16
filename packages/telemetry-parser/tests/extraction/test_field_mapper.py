import pytest

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
    
    assert result is None


def test_registration_status_missing_returns_none(mapper):
    headers = {}

    result = mapper.map_registration_status(headers)

    assert result is None


# ---------------------------------------------------------------
# Removed mappings
# ---------------------------------------------------------------

# map_retry_count, map_latency, map_session_duration and map_timestamp
# were removed with the edge producer contract (ADR-001). They read
# X-Latency, X-Session-Duration, X-Timestamp and Retry-After: three
# non-standard headers no device sends, and one response header that
# should never appear on a request. These assert the mappings stay gone,
# so reinstating one is a deliberate act rather than an oversight.


@pytest.mark.parametrize(
    "removed",
    [
        "map_retry_count",
        "map_latency",
        "map_session_duration",
        "map_timestamp",
    ],
)
def test_removed_header_mappings_are_absent(mapper, removed):
    assert not hasattr(mapper, removed)


@pytest.mark.parametrize(
    "removed",
    ["latency", "retry_count", "session_duration", "event_timestamp"],
)
def test_removed_fields_are_absent(removed):
    assert removed not in ExtractedEventFields.__dataclass_fields__
