from datetime import datetime, timezone

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.normalisation.event_normaliser import EventNormaliser


def make_extracted(**overrides) -> ExtractedEventFields:
    fields = {
        "device_id": "headset-0001",
        "registration_status": "registered",
        "latency": 42.5,
        "retry_count": 0,
        "transport_protocol": "TCP",
        "session_duration": None,
        "call_id": "c8f3a91e",
        "source_ip": "10.20.0.14",
        "event_timestamp": datetime.now(timezone.utc),
    }
    fields.update(overrides)
    return ExtractedEventFields(**fields)


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

    event = normaliser.normalise(extracted)

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

    event = normaliser.normalise(extracted)

    assert event.event_type == "sip.unknown"


def test_event_type_never_uses_device_prefix():
    """
    Broad regression guard: nothing this normaliser emits should use
    the "device."-prefixed identities, which belong to
    event-schema-contracts' provisioning schemas, not this parser's
    telemetry.
    """
    normaliser = EventNormaliser()

    registered = normaliser.normalise(make_extracted(registration_status="registered"))
    unknown = normaliser.normalise(make_extracted(registration_status=None))

    assert not registered.event_type.startswith("device.")
    assert not unknown.event_type.startswith("device.")
