import pytest

from telemetry_parser.protocol.sip_parser import SIPMessage
from telemetry_parser.extraction.event_extractor import (
    EventExtractor,
    UnsupportedProtocolEvent,
)
from telemetry_parser.extraction.field_mapper import ExtractedEventFields


_DEFAULT_HEADERS = object()

def make_register_message(
    headers: dict | None = _DEFAULT_HEADERS,
    device_label: str | None = "handset-42",
    call_id: str | None = "abc123",
    transport: str | None = "TCP",
    source_ip: str | None = "10.0.0.5",
) -> SIPMessage:
    if headers is _DEFAULT_HEADERS:
        headers = {"cseq": "1 REGISTER"}
    return SIPMessage(
        method="REGISTER",
        headers=headers,
        device_label=device_label,
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


def test_extract_maps_device_label(extractor):
    msg = make_register_message(device_label="handset-99")

    result = extractor.extract(msg)

    assert result.device_label == "handset-99"


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


def test_non_standard_headers_are_ignored(extractor):
    """
    A message carrying the old X- headers extracts exactly as one without
    them. Nothing reads them any more, so their presence changes nothing.
    """

    plain = extractor.extract(make_register_message())

    annotated = extractor.extract(
        make_register_message(
            headers={
                "cseq": "1 REGISTER",
                "x-latency": "25.5",
                "x-session-duration": "120.0",
                "x-timestamp": "2026-04-20T10:15:30Z",
                "retry-after": "3",
            }
        )
    )

    assert plain == annotated


def test_extract_with_no_optional_headers_returns_none_fields(extractor):
    msg = make_register_message(
        headers={},
        device_label=None,
        call_id=None,
        transport=None,
        source_ip=None,
    )

    result = extractor.extract(msg)

    assert result.device_label is None
    assert result.call_id is None
    assert result.transport_protocol is None
    assert result.source_ip is None
    assert result.registration_status is None


# ----------------------------------------------------------------
# unsupported method
# ----------------------------------------------------------------

def test_extract_raises_for_invite_method(extractor):
    msg = SIPMessage(
        method="INVITE",
        headers={},
        device_label=None,
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
        device_label=None,
        call_id=None,
        transport=None,
        source_ip=None,
    )

    with pytest.raises(UnsupportedProtocolEvent):
        extractor.extract(msg)