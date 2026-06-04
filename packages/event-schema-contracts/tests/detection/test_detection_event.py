"""
Tests for event_schema_contracts.detection.detection_event.

Mirrors the structure of test_session_event.py and test_feature_vector.py.
Covers: schema identity, registration, validation, immutability,
serialisation round-trip, and the discriminator-pattern contract on
detection_type and details.
"""

from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_DNS, uuid1, uuid4, uuid5

import pytest
from pydantic import ValidationError

from event_schema_contracts.detection.detection_event import (
    DetectionEvent,
    DetectionEventPayload,
    DetectionSeverity,
)
from event_schema_contracts.versioning.schema_registry import schema_registry


# ---------------------------------------------------------------------------
# Fixtures local to this module
# ---------------------------------------------------------------------------


@pytest.fixture
def valid_detection_payload(utc_now):
    return DetectionEventPayload(
        detection_id=uuid4(),
        detection_type="device.offline",
        severity=DetectionSeverity.WARNING,
        detected_at=utc_now,
        store_id="store-001",
        device_id=uuid4(),
        source_event_id=uuid4(),
        threshold_breached="silent for 320s (threshold 300s)",
        details={"silent_seconds": 320, "threshold_seconds": 300},
    )


@pytest.fixture
def valid_detection_event(trace_context, utc_now, valid_detection_payload):
    return DetectionEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=valid_detection_payload,
    )


# ---------------------------------------------------------------------------
# Schema identity and registration
# ---------------------------------------------------------------------------


def test_event_type_identity():
    assert DetectionEvent.__event_type__ == "detection.event"
    assert DetectionEvent.__schema_version__ == "v1"


def test_metadata_auto_injected(trace_context, valid_detection_payload):
    event = DetectionEvent(
        event_timestamp=datetime.now(timezone.utc),
        trace=trace_context,
        payload=valid_detection_payload,
    )

    assert event.metadata.event_type == "detection.event"
    assert event.metadata.schema_version == "v1"


def test_schema_registered():
    cls = schema_registry.get_schema("detection.event", "v1")
    assert cls is DetectionEvent


# ---------------------------------------------------------------------------
# Field validation
# ---------------------------------------------------------------------------


def _payload_with(*, detection_id, source_event_id, utc_now):
    return DetectionEventPayload(
        detection_id=detection_id,
        detection_type="device.offline",
        severity=DetectionSeverity.WARNING,
        detected_at=utc_now,
        store_id="store-001",
        source_event_id=source_event_id,
        threshold_breached="some breach",
        details={},
    )


def test_detection_id_accepts_v4_and_v5(utc_now):
    # v4 (random) and v5 (deterministically derived) are both valid. v5
    # lets a consumer regenerate detection ids reproducibly so they
    # survive a replay byte-for-byte.
    for detection_id in (
        uuid4(),
        uuid5(NAMESPACE_DNS, "detection.device_offline|store-001"),
    ):
        payload = _payload_with(
            detection_id=detection_id, source_event_id=uuid4(), utc_now=utc_now
        )
        assert payload.detection_id == detection_id


def test_detection_id_rejects_non_v4_v5(utc_now):
    # v1 encodes host/time; disallowed on these fields.
    with pytest.raises(ValidationError) as exc:
        _payload_with(detection_id=uuid1(), source_event_id=uuid4(), utc_now=utc_now)

    assert exc.value.errors()[0]["loc"] == ("detection_id",)


def test_source_event_id_accepts_v4_and_v5(utc_now):
    for source_event_id in (
        uuid4(),
        uuid5(NAMESPACE_DNS, "source.store_outage|store-001"),
    ):
        payload = _payload_with(
            detection_id=uuid4(), source_event_id=source_event_id, utc_now=utc_now
        )
        assert payload.source_event_id == source_event_id


def test_source_event_id_rejects_non_v4_v5(utc_now):
    with pytest.raises(ValidationError) as exc:
        _payload_with(detection_id=uuid4(), source_event_id=uuid1(), utc_now=utc_now)

    assert exc.value.errors()[0]["loc"] == ("source_event_id",)


def test_detected_at_must_be_utc():
    with pytest.raises(ValidationError) as exc:
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            detected_at=datetime.now(),  # naive
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )

    assert "detected_at" in str(exc.value)


def test_detection_type_must_match_pattern(utc_now):
    # Empty rejected
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )

    # Uppercase rejected (must be lowercase dotted)
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="Device.Offline",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )

    # Single-segment rejected (must contain at least one dot)
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="offline",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )


def test_store_id_must_be_non_empty(utc_now):
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )


def test_threshold_breached_must_be_non_empty(utc_now):
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="",
            details={},
        )


def test_device_id_optional_for_store_scoped_detections(utc_now):
    # Store outage detection has no device_id.
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type="store.outage",
        severity=DetectionSeverity.CRITICAL,
        detected_at=utc_now,
        store_id="store-001",
        device_id=None,
        source_event_id=uuid4(),
        threshold_breached="7/10 devices offline",
        details={"offline_count": 7, "registered_count": 10, "ratio": 0.7},
    )
    assert payload.device_id is None


def test_details_defaults_to_empty_dict(utc_now):
    # The `details` field is optional; absent at construction means empty.
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type="device.offline",
        severity=DetectionSeverity.WARNING,
        detected_at=utc_now,
        store_id="store-001",
        source_event_id=uuid4(),
        threshold_breached="some breach",
    )
    assert payload.details == {}


# ---------------------------------------------------------------------------
# Discriminator pattern contract
# ---------------------------------------------------------------------------


def test_discriminator_pattern_supports_all_phase_2_detector_types(utc_now):
    """
    The discriminator pattern must accept the three Phase 2 detection
    types without modification: device.offline, store.outage,
    signal.anomaly.
    """
    base_args = dict(
        detection_id=uuid4(),
        severity=DetectionSeverity.WARNING,
        detected_at=utc_now,
        store_id="store-001",
        source_event_id=uuid4(),
    )

    offline = DetectionEventPayload(
        **base_args,
        detection_type="device.offline",
        device_id=uuid4(),
        threshold_breached="silent for 320s",
        details={"silent_seconds": 320},
    )
    assert offline.detection_type == "device.offline"

    outage = DetectionEventPayload(
        **{**base_args, "detection_id": uuid4(), "source_event_id": uuid4()},
        detection_type="store.outage",
        threshold_breached="7/10 devices offline",
        details={"offline_count": 7, "registered_count": 10},
    )
    assert outage.detection_type == "store.outage"

    anomaly = DetectionEventPayload(
        **{**base_args, "detection_id": uuid4(), "source_event_id": uuid4()},
        detection_type="signal.anomaly",
        device_id=uuid4(),
        threshold_breached="signal_quality 0.18 below threshold 0.5",
        details={"field": "signal_quality_index", "observed": 0.18,
                 "threshold": 0.5, "direction": "below"},
    )
    assert anomaly.detection_type == "signal.anomaly"


def test_unknown_detection_type_accepted(utc_now):
    """
    The schema must NOT enforce a closed enum of detection types — the
    discriminator pattern's whole purpose is to absorb new detector
    types as payload changes, not schema changes.
    """
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type="firmware.degradation",  # not yet implemented
        severity=DetectionSeverity.WARNING,
        detected_at=utc_now,
        store_id="store-001",
        source_event_id=uuid4(),
        threshold_breached="firmware drift detected",
        details={"version": "v3.4.1"},
    )
    assert payload.detection_type == "firmware.degradation"


# ---------------------------------------------------------------------------
# Severity classification
# ---------------------------------------------------------------------------


def test_severity_is_a_string_enum(utc_now):
    payload = DetectionEventPayload(
        detection_id=uuid4(),
        detection_type="device.offline",
        severity="WARNING",  # accepted as the enum value
        detected_at=utc_now,
        store_id="store-001",
        source_event_id=uuid4(),
        threshold_breached="some breach",
        details={},
    )
    assert payload.severity is DetectionSeverity.WARNING


def test_invalid_severity_rejected(utc_now):
    with pytest.raises(ValidationError):
        DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity="MAJOR",  # not a valid DetectionSeverity
            detected_at=utc_now,
            store_id="store-001",
            source_event_id=uuid4(),
            threshold_breached="some breach",
            details={},
        )


# ---------------------------------------------------------------------------
# Envelope behaviour (event-level)
# ---------------------------------------------------------------------------


def test_event_timestamp_future_rejected(trace_context, valid_detection_payload):
    with pytest.raises(ValidationError):
        DetectionEvent(
            event_timestamp=datetime.now(timezone.utc) + timedelta(minutes=10),
            trace=trace_context,
            payload=valid_detection_payload,
        )


def test_event_is_immutable(valid_detection_event):
    with pytest.raises(ValidationError):
        valid_detection_event.event_id = uuid4()


def test_serialisation_round_trip(trace_context, utc_now):
    event = DetectionEvent(
        event_timestamp=utc_now,
        trace=trace_context,
        payload=DetectionEventPayload(
            detection_id=uuid4(),
            detection_type="device.offline",
            severity=DetectionSeverity.WARNING,
            detected_at=utc_now,
            store_id="store-001",
            device_id=uuid4(),
            source_event_id=uuid4(),
            threshold_breached="silent for 320s",
            details={"silent_seconds": 320},
        ),
    )

    serialised = event.model_dump(mode="json")
    reconstructed = DetectionEvent(**serialised)

    assert reconstructed == event