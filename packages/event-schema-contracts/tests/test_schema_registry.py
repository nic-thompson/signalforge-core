import pytest

from event_schema_contracts.telemetry.device_event import (
    DeviceRegistrationEvent,
)

from event_schema_contracts.versioning.schema_registry import (
    SchemaRegistry,
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


    # ----------------------------------
    # Registry structural guarantees 
    # ----------------------------------

def test_registry_contains_expected_schema():
    """
    Registry must expose expected mapping internally.
    Guards accidental deregistration.
    """

    assert ("device.registration", "v1",) in schema_registry._registry


def test_registry_maps_to_correct_schema_class():
    """
    Internal registry mapping must match public lookup result.
    """

    internal_schema = schema_registry._registry[
        (
            "device.registration",
            "v1",
        )
    ]

    public_schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert internal_schema is public_schema


# -------------------------------------
# Duplicate registration protection
# -------------------------------------

def test_duplicate_schema_registrarion_fails():
    """
    Registry must prevent duplicate schema registration
    """

    registry = schema_registry

    with pytest.raises(ValueError):
        registry.register(
            "device.registration",
            "v1",
            DeviceRegistrationEvent,
        )


# --------------------------
# Type-saftey guarantees
# --------------------------

def test_registry_returns_base_event_subclass():
    """
    All registry schemas must inherit BaseEvent
    """

    from event_schema_contracts.base.base_event import BaseEvent

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert issubclass(schema, BaseEvent)


# ----------------------------------
# Versioning contract guarantees
# ----------------------------------

def test_schema_version_format_is_valid():
    """
    Schema versions must follow vN convention
    """

    schema = schema_registry.get_schema(
        "device.registration",
        "v1",
    )

    assert schema.__schema_version__.startswith("v")
    assert schema.__schema_version__[1:].isdigit()


# -----------------------------
# Registry completeness check
# -----------------------------

def test_registry_entries_are_consistant():
    """
    Every registry entry must be internally consitent.
    Prevents drift between metadata and keys.
    """

    for (event_type, version), schema in schema_registry._registry.items():

            assert schema.__event_type__ == event_type
            assert schema.__schema_version__ == version