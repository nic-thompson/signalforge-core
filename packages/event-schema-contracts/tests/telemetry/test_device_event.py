from event_schema_contracts.telemetry.device_event import DeviceRegistrationPayload
from uuid import uuid4
from datetime import datetime, timezone
from pydantic import ValidationError

import pytest

def test_firmware_version_accepts_semver():

    payload = DeviceRegistrationPayload(
        device_id=uuid4(),
        device_type="SENSOR",
        firmware_version="1.2.3",
        registered_at=datetime.now(timezone.utc),
    )

    assert payload.firmware_version == "1.2.3"


def test_firmware_version_rejects_invalid_semver():

    with pytest.raises(ValidationError):
        DeviceRegistrationPayload(
            device_id=uuid4(),
            device_type="SENSOR",
            firmware_version="latest",
            registered_at=datetime.now(timezone.utc),
        )


def test_firmware_version_optional():
    
    payload = DeviceRegistrationPayload(
        device_id=uuid4(),
        device_type="SENSOR",
        firmware_version=None,
        registered_at=datetime.now(timezone.utc),
    )

    assert payload.firmware_version is None


def test_semver_validation_not_applied_to_other_fields():

    payload = DeviceRegistrationPayload(
        device_id=uuid4(),
        device_type="SENSOR",
        firmware_version="1.2.3",
        registered_at=datetime.now(timezone.utc),
    )

    assert payload.device_type == "SENSOR"


def test_firmware_version_accepts_semver_with_prerelease_and_build():

    payload = DeviceRegistrationPayload(
        device_id=uuid4(),
        device_type="SENSOR",
        firmware_version="1.2.3-alpha.1+build.7",
        registered_at=datetime.now(timezone.utc),
    )

    assert payload.firmware_version == "1.2.3-alpha.1+build.7"