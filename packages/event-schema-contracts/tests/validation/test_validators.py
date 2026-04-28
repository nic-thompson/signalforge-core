"""
Tests for ingestion boundary validators.
"""

import pytest

from event_schema_contracts.validation.validators import (
    assert_valid_envelope,
    validate_envelope_fields,
    validate_metadata_fields,
)


def _valid_event() -> dict:
    return {
        "event_timestamp": "2024-01-01T00:00:00Z",
        "payload": {"device_id": "abc"},
        "trace": {"trace_id": "xyz"},
        "metadata": {
            "event_type": "device.registration",
            "schema_version": "v1",
            "source": "pytest",
        },
    }


# ---------------------------------------------------------------------------
# validate_envelope_fields
# ---------------------------------------------------------------------------

def test_valid_envelope_returns_empty_list():
    assert validate_envelope_fields(_valid_event()) == []


def test_missing_payload_detected():
    event = _valid_event()
    del event["payload"]
    assert "payload" in validate_envelope_fields(event)


def test_missing_trace_detected():
    event = _valid_event()
    del event["trace"]
    assert "trace" in validate_envelope_fields(event)


def test_missing_event_timestamp_detected():
    event = _valid_event()
    del event["event_timestamp"]
    assert "event_timestamp" in validate_envelope_fields(event)


def test_non_dict_raises_type_error():
    with pytest.raises(TypeError):
        validate_envelope_fields("not a dict")


# ---------------------------------------------------------------------------
# validate_metadata_fields
# ---------------------------------------------------------------------------

def test_valid_metadata_returns_empty_list():
    assert validate_metadata_fields(_valid_event()) == []


def test_missing_metadata_key_returns_metadata_error():
    event = _valid_event()
    del event["metadata"]
    assert validate_metadata_fields(event) == ["metadata"]


def test_metadata_not_dict_returns_metadata_error():
    event = _valid_event()
    event["metadata"] = "not-a-dict"
    assert validate_metadata_fields(event) == ["metadata"]


def test_missing_event_type_in_metadata():
    event = _valid_event()
    del event["metadata"]["event_type"]
    missing = validate_metadata_fields(event)
    assert "metadata.event_type" in missing


def test_missing_schema_version_in_metadata():
    event = _valid_event()
    del event["metadata"]["schema_version"]
    missing = validate_metadata_fields(event)
    assert "metadata.schema_version" in missing


def test_missing_source_in_metadata():
    event = _valid_event()
    del event["metadata"]["source"]
    missing = validate_metadata_fields(event)
    assert "metadata.source" in missing


# ---------------------------------------------------------------------------
# assert_valid_envelope
# ---------------------------------------------------------------------------

def test_assert_valid_envelope_passes_for_valid_event():
    assert_valid_envelope(_valid_event())  # must not raise


def test_assert_valid_envelope_raises_for_missing_payload():
    event = _valid_event()
    del event["payload"]
    with pytest.raises(ValueError, match="payload"):
        assert_valid_envelope(event)


def test_assert_valid_envelope_raises_for_missing_metadata_field():
    event = _valid_event()
    del event["metadata"]["event_type"]
    with pytest.raises(ValueError, match="event_type"):
        assert_valid_envelope(event)
