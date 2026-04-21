import pytest
from datetime import datetime, timezone

from telemetry_parser.protocol.sip_parser import SIPMessage
from telemetry_parser.extraction.event_extractor import (
    EventExtractor,
    UnsupportedProtocolEvent,
)
from telemetry_parser.extraction.field_mapper import ExtractedEventFields


_DEFAULT_HEADERS = object()

def make_register_message(
    headers: dict | None = _DEFAULT_HEADERS,
    device_id: str | None = "handset-42",
    call_id: str | None = "abc123",
    transport: str | None = "TCP",
    source_ip: str | None = "10.0.0.5",
) -> SIPMessage:
    if headers is _DEFAULT_HEADERS:
        headers = {"cseq": "1 REGISTER"}
    return SIPMessage(
        method="REGISTER",
        headers=headers,
        device_id=device_id,
        call_id=call_id,
        transport=transport,
        source_ip=source_ip,
    )


@pytest.fixture
def extractor() -> EventExtractor:
    return EventExtractor()


# ----------------------------------------------------------------
# happy path
# ----------------------------------------------------------------

def test_extract_returns_extracted_event_fields(extractor):
    msg = make_register_message()

    result = extractor.extract(msg)

    assert isinstance(result, ExtractedEventFields)


def test_extract_maps_device_id(extractor):
    msg = make_register_message(device_id="handset-99")

    result = extractor.extract(msg)

    assert result.device_id == "handset-99"


def test_extract_maps_call_id(extractor):
    msg = make_register_message(call_id="xyz-789")

    result = extractor.extract(msg)

    assert result.call_id == "xyz-789"


def test_extract_maps_transport_protocol(extractor):
    msg = make_register_message(transport="UDP")

    result = extractor.extract(msg)

    assert result.transport_protocol == "UDP"


def test_extract_maps_source_ip(extractor):
    msg = make_register_message(source_ip="192.168.1.10")

    result = extractor.extract(msg)

    assert result.source_ip == "192.168.1.10"


def test_extract_maps_registration_status(extractor):
    msg = make_register_message(headers={"cseq": "1 REGISTER"})

    result = extractor.extract(msg)

    assert result.registration_status == "registered"


def test_extract_maps_latency(extractor):
    msg = make_register_message(headers={"cseq": "1 REGISTER", "x-latency": "25.5"})

    result = extractor.extract(msg)

    assert result.latency == 25.5


def test_extract_maps_retry_count(extractor):
    msg = make_register_message(headers={"cseq": "1 REGISTER", "retry-after": "3"})

    result = extractor.extract(msg)

    assert result.retry_count == 3


def test_extract_maps_session_duration(extractor):
    msg = make_register_message(
        headers={"cseq": "1 REGISTER", "x-session-duration": "120.0"}
    )

    result = extractor.extract(msg)

    assert result.session_duration == 120.0


def test_extract_maps_event_timestamp(extractor):
    msg = make_register_message(
        headers={"cseq": "1 REGISTER", "x-timestamp": "2026-04-20T10:15:30Z"}
    )

    result = extractor.extract(msg)

    assert result.event_timestamp == datetime(2026, 4, 20, 10, 15, 30, tzinfo=timezone.utc)


# ----------------------------------------------------------------
# missing / null fields
# ----------------------------------------------------------------

def test_extract_with_no_optional_headers_returns_none_fields(extractor):
    msg = make_register_message(
        headers={},
        device_id=None,
        call_id=None,
        transport=None,
        source_ip=None,
    )

    result = extractor.extract(msg)

    assert result.device_id is None
    assert result.call_id is None
    assert result.transport_protocol is None
    assert result.source_ip is None
    assert result.registration_status is None
    assert result.latency is None
    assert result.retry_count is None
    assert result.session_duration is None
    assert result.event_timestamp is None


# ----------------------------------------------------------------
# unsupported method
# ----------------------------------------------------------------

def test_extract_raises_for_invite_method(extractor):
    msg = SIPMessage(
        method="INVITE",
        headers={},
        device_id=None,
        call_id=None,
        transport=None,
        source_ip=None,
    )

    with pytest.raises(UnsupportedProtocolEvent, match="INVITE"):
        extractor.extract(msg)


def test_extract_raises_for_options_method(extractor):
    msg = SIPMessage(
        method="OPTIONS",
        headers={},
        device_id=None,
        call_id=None,
        transport=None,
        source_ip=None,
    )

    with pytest.raises(UnsupportedProtocolEvent):
        extractor.extract(msg)