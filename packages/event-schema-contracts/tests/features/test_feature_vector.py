"""
Tests for FeatureVectorPayload and FeatureVectorEvent contracts.
"""

from datetime import datetime, timezone
from uuid import uuid4, UUID

import pytest
from pydantic import ValidationError

from event_schema_contracts.base.trace import TraceContext
from event_schema_contracts.features.feature_vector import (
    FeatureVectorEvent,
    FeatureVectorPayload,
)
from event_schema_contracts.versioning.schema_registry import schema_registry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_payload(**overrides) -> FeatureVectorPayload:
    base = dict(
        entity_id=uuid4(),
        feature_timestamp=datetime.now(timezone.utc),
        feature_values={"score": 0.95, "count": 42},
        feature_version="v1",
        source_event_id=uuid4(),
    )
    base.update(overrides)
    return FeatureVectorPayload(**base)


def make_event(**overrides) -> FeatureVectorEvent:
    defaults = dict(
        trace=TraceContext(trace_id=uuid4()),
        event_timestamp=datetime.now(timezone.utc),
        payload=make_payload(),
    )
    defaults.update(overrides)
    return FeatureVectorEvent(**defaults)


# ---------------------------------------------------------------------------
# Payload construction
# ---------------------------------------------------------------------------

def test_valid_payload_construction():
    payload = make_payload()
    assert isinstance(payload.entity_id, UUID)
    assert payload.feature_version == "v1"


def test_feature_values_accept_mixed_scalar_types():
    payload = make_payload(
        feature_values={
            "rate": 0.42,
            "count": 7,
            "label": "active",
            "flag": True,
        }
    )
    assert payload.feature_values["label"] == "active"


def test_feature_values_reject_non_scalar():
    with pytest.raises(ValidationError):
        make_payload(feature_values={"nested": {"a": 1}})


# ---------------------------------------------------------------------------
# feature_version format enforcement
# ---------------------------------------------------------------------------

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
        make_payload(feature_version="1.0.0")


def test_feature_version_rejects_arbitrary_string():
    with pytest.raises(ValidationError):
        make_payload(feature_version="latest")


# ---------------------------------------------------------------------------
# UUID enforcement
# ---------------------------------------------------------------------------

def test_entity_id_must_be_uuidv4():
    with pytest.raises(ValidationError):
        make_payload(entity_id=UUID(int=0))


def test_source_event_id_must_be_uuidv4():
    with pytest.raises(ValidationError):
        make_payload(source_event_id=UUID(int=0))


# ---------------------------------------------------------------------------
# UTC timestamp enforcement
# ---------------------------------------------------------------------------

def test_feature_timestamp_must_be_utc():
    with pytest.raises(ValidationError):
        make_payload(feature_timestamp=datetime.now())  # naive


# ---------------------------------------------------------------------------
# Event wrapper
# ---------------------------------------------------------------------------

def test_event_construction_succeeds():
    event = make_event()
    assert event.__event_type__ == "feature.vector"
    assert event.__schema_version__ == "v1"


def test_event_registered_in_schema_registry():
    cls = schema_registry.get_schema("feature.vector", "v1")
    assert cls is FeatureVectorEvent


def test_event_is_immutable():
    event = make_event()
    with pytest.raises(ValidationError):
        event.event_id = uuid4()


def test_event_serialisation_round_trip():
    event = make_event()
    serialised = event.model_dump(mode="json")
    reconstructed = FeatureVectorEvent(**serialised)
    assert reconstructed == event
