"""
Tests for WindowedFeatureVectorPayload and WindowedFeatureVectorEvent contracts.
"""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.features.windowed_feature_vector import (
    WindowedFeatureVectorEvent,
    WindowedFeatureVectorPayload,
)
from event_schema_contracts.versioning.schema_registry import schema_registry

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_payload(**overrides) -> WindowedFeatureVectorPayload:
    base = {
        "partition_key": "store-1",
        "window_start": datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC),
        "window_end": datetime(2026, 4, 30, 12, 5, 0, tzinfo=UTC),
        "feature_values": {"distinct_devices": 47, "mean_latency": 312.5},
        "feature_version": "v1",
    }
    base.update(overrides)
    return WindowedFeatureVectorPayload(**base)

def make_event(**overrides) -> WindowedFeatureVectorEvent:
    defaults = {
        "trace": TraceContext(trace_id=uuid4()),
        "event_timestamp": datetime(2026, 4, 30, 12, 5, 0, tzinfo=UTC),
        "payload": make_payload(),
    }
    defaults.update(overrides)
    return WindowedFeatureVectorEvent(**defaults)

# ---------------------------------------------------------------------------
# Payload field validation
# ---------------------------------------------------------------------------


def test_valid_payload_construction():
    payload = make_payload()
    assert payload.partition_key == "store-1"
    assert payload.feature_version == "v1"
    assert payload.feature_values == {"distinct_devices": 47, "mean_latency": 312.5}


def test_feature_values_accept_mixed_scalar_types():
    payload = make_payload(
        feature_values={
            "count": 42,
            "score": 0.95,
            "is_active": True,
            "label": "outage",
        }
    )
    assert payload.feature_values["count"] == 42
    assert payload.feature_values["is_active"] is True


def test_feature_values_reject_non_scalar():
    with pytest.raises(ValidationError):
        make_payload(feature_values={"nested": {"inner": 1}})


def test_feature_version_accepts_v1():
    payload = make_payload(feature_version="v1")
    assert payload.feature_version == "v1"


def test_feature_version_accepts_minor():
    payload = make_payload(feature_version="v1.2")
    assert payload.feature_version == "v1.2"


def test_feature_version_accepts_patch():
    payload = make_payload(feature_version="v1.2.3")
    assert payload.feature_version == "v1.2.3"


def test_feature_version_rejects_bare_integer():
    with pytest.raises(ValidationError):
        make_payload(feature_version="1")


def test_feature_version_rejects_arbitrary_string():
    with pytest.raises(ValidationError):
        make_payload(feature_version="latest")


def test_partition_key_accepts_canonical_form():
    payload = make_payload(partition_key="store-42")
    assert payload.partition_key == "store-42"


def test_partition_key_accepts_compound_separators():
    for valid_key in ("store.42", "store_main", "store-london-flagship", "a.b-c_d"):
        payload = make_payload(partition_key=valid_key)
        assert payload.partition_key == valid_key


@pytest.mark.parametrize(
    "invalid_partition_key",
    [
        "Store-1",        # uppercase
        "store 1",        # whitespace
        "store#1",        # special character
        "-store",         # leading separator
        "store-",         # trailing separator
        "store..main",    # double separator
    ],
)
def test_partition_key_rejects_invalid_format(invalid_partition_key):
    with pytest.raises(ValidationError):
        make_payload(partition_key=invalid_partition_key)


def test_partition_key_rejects_empty_string():
    with pytest.raises(ValidationError):
        make_payload(partition_key="")


def test_window_start_must_be_utc():
    with pytest.raises(ValidationError):
        make_payload(
            window_start=datetime(2026, 4, 30, 12, 0, 0),  # naive
        )


def test_window_end_must_be_utc():
    with pytest.raises(ValidationError):
        make_payload(
            window_end=datetime(2026, 4, 30, 12, 5, 0),  # naive
        )


# ---------------------------------------------------------------------------
# Event-level contracts
# ---------------------------------------------------------------------------


def test_event_construction_succeeds():
    event = make_event()
    assert event.payload.partition_key == "store-1"
    assert event.__event_type__ == "feature.vector.windowed"
    assert event.__schema_version__ == "v1"


def test_event_registered_in_schema_registry():
    cls = schema_registry.get_schema("feature.vector.windowed", "v1")
    assert cls is WindowedFeatureVectorEvent


def test_event_is_immutable():
    event = make_event()
    with pytest.raises(ValidationError):
        event.payload = make_payload(partition_key="store-2")  # type: ignore[misc]


def test_event_serialisation_round_trip():
    event = make_event()
    as_dict = event.model_dump(mode="json")
    rebuilt = WindowedFeatureVectorEvent.model_validate(as_dict)
    assert rebuilt.payload.partition_key == event.payload.partition_key
    assert rebuilt.payload.feature_values == event.payload.feature_values
    assert rebuilt.payload.window_start == event.payload.window_start
    assert rebuilt.payload.window_end == event.payload.window_end
