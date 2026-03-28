import pytest

from event_schema_contracts.telemetry.device_event import (
    DeviceRegistrationEvent,
)

from event_schema_contracts.versioning.schema_registry import (
    schema_registry,
)

# ----------------------------
# Happy-path lookup tests
# ----------------------------

def test_lookup_returns_expected_schema_class():
    """
    Registry returns the correct schema class 
    for a valid (event_type, version) pair.
    """

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert schema is DeviceRegistrationEvent


def test_lookup_returns_class_not_instance():
    """
    Registry must return schema *type* not instance.
    """

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert isinstance(schema, type)


def test_lookup_unknown_event_type_raises():
    """
    Unknown event types must fail clearly
    """

    with pytest.raises(KeyError):
        schema_registry.get_schema(
            "device.unknown",
            "v1",
        )


def test_lookup_unknown_version_raises():
    """
    Known event_types with unknown versions
    must raise KeyError.
    """

    with pytest.raises(KeyError):
        schema_registry.get_schema(
            "device.registration",
            "v999",
        )


# -------------------------------
# Schema identity consistency
# -------------------------------

def test_schema_event_type_matches_registry_key():
    """
    Schema metadata must match registry key.
    Prevents silent mismatches.
    """

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert schema.__event_type__ == "device.registration"


def test_schema_version_matches_registry_key():
    """
    Schema version metadata must match
    registry lookup version.
    """

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert schema.__schema_version__ == "v1"
