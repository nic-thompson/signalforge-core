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