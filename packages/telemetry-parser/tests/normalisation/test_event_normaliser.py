from datetime import datetime, timezone

import pytest

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.normalisation.event_normaliser import EventNormaliser


OBSERVED_AT = datetime(2026, 8, 24, 9, 30, 0, tzinfo=timezone.utc)


def make_extracted(**overrides) -> ExtractedEventFields:
    fields = {
        "device_id": "headset-0001",
        "registration_status": "registered",
        "transport_protocol": "TCP",
        "call_id": "c8f3a91e",
        "source_ip": "10.20.0.14",
    }
    fields.update(overrides)
    return ExtractedEventFields(**fields)


def normalise(normaliser: EventNormaliser, extracted, **kwargs):
    return normaliser.normalise(extracted, observed_at=OBSERVED_AT, **kwargs)


# ---------------------------------------------------------------
# event_type mapping — previously had zero test coverage
# ---------------------------------------------------------------


def test_registered_status_maps_to_sip_registration():
    """
    Pins the ADR-002 rename. Previously emitted "device.registration",
    which collided with event-schema-contracts' identity for device
    *provisioning* — an incompatible schema. "sip.registration" is the
    identity actually registered for this parser's output.
    """
    normaliser = EventNormaliser()
    extracted = make_extracted(registration_status="registered")

    event = normalise(normaliser, extracted)

    assert event.event_type == "sip.registration"


def test_missing_registration_status_maps_to_sip_unknown():
    """
    A REGISTER message the extractor accepted (method matched) but
    whose CSeq header was missing or malformed, so no status could be
    read. Renamed alongside "device.registration" for the same reason:
    "device.unknown" was an equally orphaned identity with no schema.
    """
    normaliser = EventNormaliser()
    extracted = make_extracted(registration_status=None)

    event = normalise(normaliser, extracted)

    assert event.event_type == "sip.unknown"


def test_event_type_never_uses_device_prefix():
    """
    Broad regression guard: nothing this normaliser emits should use
    the "device."-prefixed identities, which belong to
    event-schema-contracts' provisioning schemas, not this parser's
    telemetry.
    """
    normaliser = EventNormaliser()

    registered = normalise(normaliser, make_extracted(registration_status="registered"))
    unknown = normalise(normaliser, make_extracted(registration_status=None))

    assert not registered.event_type.startswith("device.")
    assert not unknown.event_type.startswith("device.")


# ---------------------------------------------------------------
# observation time is the only source of event time
# ---------------------------------------------------------------


def test_event_time_is_the_observation_time():
    event = normalise(EventNormaliser(), make_extracted())

    assert event.event_timestamp == OBSERVED_AT


def test_observation_time_is_required():
    """
    Devices send plain RFC 3261, which carries no timestamp, so there is
    nothing to fall back from. Omitting observation time is a programming
    error, not a case to paper over with the clock.
    """

    with pytest.raises(TypeError):
        EventNormaliser().normalise(make_extracted())


def test_payload_carries_no_removed_fields():
    payload = normalise(EventNormaliser(), make_extracted()).payload

    for removed in ("latency", "retry_count", "session_duration"):
        assert removed not in payload
