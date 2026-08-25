from datetime import datetime, timezone

import pytest

from telemetry_parser.extraction.field_mapper import ExtractedEventFields
from telemetry_parser.normalisation.event_normaliser import EventNormaliser


OBSERVED_AT = datetime(2026, 8, 24, 9, 30, 0, tzinfo=timezone.utc)
STORE_ID = "store-0042"


def make_extracted(**overrides) -> ExtractedEventFields:
    fields = {
        "device_label": "headset-0001",
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
    normaliser = EventNormaliser(store_id=STORE_ID)
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
    normaliser = EventNormaliser(store_id=STORE_ID)
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
    normaliser = EventNormaliser(store_id=STORE_ID)

    registered = normalise(normaliser, make_extracted(registration_status="registered"))
    unknown = normalise(normaliser, make_extracted(registration_status=None))

    assert not registered.event_type.startswith("device.")
    assert not unknown.event_type.startswith("device.")


# ---------------------------------------------------------------
# observation time is the only source of event time
# ---------------------------------------------------------------


def test_event_time_is_the_observation_time():
    event = normalise(EventNormaliser(store_id=STORE_ID), make_extracted())

    assert event.event_timestamp == OBSERVED_AT


def test_observation_time_is_required():
    """
    Devices send plain RFC 3261, which carries no timestamp, so there is
    nothing to fall back from. Omitting observation time is a programming
    error, not a case to paper over with the clock.
    """

    with pytest.raises(TypeError):
        EventNormaliser(store_id=STORE_ID).normalise(make_extracted())


def test_payload_carries_no_removed_fields():
    payload = normalise(EventNormaliser(store_id=STORE_ID), make_extracted()).payload

    for removed in ("latency", "retry_count", "session_duration"):
        assert removed not in payload


# ---------------------------------------------------------------
# store identity comes from configuration, not from the message
# ---------------------------------------------------------------


def test_payload_carries_the_configured_store_id():
    payload = normalise(EventNormaliser(store_id="store-0042"), make_extracted()).payload

    assert payload["store_id"] == "store-0042"


def test_store_id_is_independent_of_the_message():
    """
    Nothing in a SIP REGISTER names a store. Two identical messages parsed
    on controllers provisioned for different stores must produce events
    attributed to different stores — that is the whole point of taking it
    from configuration.
    """

    extracted = make_extracted()

    first = normalise(EventNormaliser(store_id="store-0001"), extracted).payload
    second = normalise(EventNormaliser(store_id="store-0002"), extracted).payload

    assert first["store_id"] == "store-0001"
    assert second["store_id"] == "store-0002"


def test_store_id_is_required():
    with pytest.raises(TypeError):
        EventNormaliser()


@pytest.mark.parametrize("bad", ["", "   ", "\t"])
def test_blank_store_id_is_rejected_at_construction(bad):
    """
    An unset configuration value is the realistic misconfiguration. A
    controller with no store identity should refuse to start, rather than
    emit a stream of events rejected one at a time at the far end.
    """

    with pytest.raises(ValueError, match="non-empty"):
        EventNormaliser(store_id=bad)


@pytest.mark.parametrize("bad", ["store 0042", " store-0042", "store-0042\n"])
def test_store_id_with_whitespace_is_rejected(bad):
    with pytest.raises(ValueError):
        EventNormaliser(store_id=bad)


def test_construction_fails_before_any_event_is_produced():
    """
    The check is at construction, not at first event. A misconfigured
    controller fails at startup, where the cause is visible.
    """

    with pytest.raises(ValueError):
        EventNormaliser(store_id="")


def test_payload_uses_device_label_not_device_id():
    """
    The From header carries a name the device was configured with, unique
    only within a store. Calling it device_id invited a join across stores
    that would silently merge two different devices sharing a label —
    no error, just wrong numbers that look like identical behaviour.

    The stable identity is derived downstream from (store_id, device_label);
    this parser has no id to emit and now says so.
    """

    payload = normalise(EventNormaliser(store_id=STORE_ID), make_extracted()).payload

    assert "device_label" in payload
    assert "device_id" not in payload


def test_device_labels_can_collide_across_stores():
    """
    States the reason the rename matters. The same label in two stores is
    two devices, and only store_id tells them apart.
    """

    extracted = make_extracted(device_label="headset-12")

    bristol = normalise(EventNormaliser(store_id="store-bristol"), extracted).payload
    leeds = normalise(EventNormaliser(store_id="store-leeds"), extracted).payload

    assert bristol["device_label"] == leeds["device_label"]
    assert bristol["store_id"] != leeds["store_id"]
