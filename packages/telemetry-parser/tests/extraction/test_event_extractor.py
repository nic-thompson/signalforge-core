from unittest.mock import MagicMock

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


def test_extract_with_optional_headers_absent_keeps_required_fields(extractor):
    """
    Everything optional may be absent. Two things may not: CSeq, without
    which the message contradicts its own request line, and the From
    header's user part, without which the message names no device.
    """

    msg = make_register_message(
        headers={"cseq": "1 REGISTER"},
        device_label="headset-12",
        call_id=None,
        transport=None,
        source_ip=None,
    )

    result = extractor.extract(msg)

    assert result is not None
    assert result.device_label == "headset-12"
    assert result.call_id is None
    assert result.transport_protocol is None
    assert result.source_ip is None
    assert result.registration_status == "registered"


# ----------------------------------------------------------------
# self-contradicting REGISTER
# ----------------------------------------------------------------

# A request line saying REGISTER with a CSeq saying otherwise is a
# malformed message, not a different kind of message. It used to become
# an event typed "sip.unknown" — an identity no schema was ever
# registered for, carrying a null registration_status into a payload
# whose schema declares that field required, so it could never have
# validated anywhere. It is now dropped and reported, as malformed input
# is everywhere else in this pipeline.


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"cseq": ""},
        {"cseq": "1 INVITE"},
        {"cseq": "malformed"},
    ],
)
def test_contradicting_cseq_is_rejected(extractor, headers):
    result = extractor.extract(make_register_message(headers=headers))

    assert result is None


def test_rejection_is_reported_to_the_observer():
    observer = MagicMock()
    extractor = EventExtractor(observer=observer)

    extractor.extract(make_register_message(headers={"cseq": "1 INVITE"}))

    observer.on_parse_error.assert_called_once_with("register_cseq_mismatch")


def test_accepted_message_reports_no_parse_error():
    observer = MagicMock()
    extractor = EventExtractor(observer=observer)

    extractor.extract(make_register_message(headers={"cseq": "1 REGISTER"}))

    observer.on_parse_error.assert_not_called()


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


def test_register_without_a_device_label_is_rejected(extractor):
    """
    Device identity is derived from (store_id, device_label). A REGISTER
    whose From header carries no user part has nothing to derive from, so
    there is no device to attribute the event to and no event worth
    emitting.
    """

    result = extractor.extract(
        make_register_message(headers={"cseq": "1 REGISTER"}, device_label=None)
    )

    assert result is None


def test_missing_device_label_is_reported_to_the_observer():
    observer = MagicMock()
    extractor = EventExtractor(observer=observer)

    extractor.extract(
        make_register_message(headers={"cseq": "1 REGISTER"}, device_label=None)
    )

    observer.on_parse_error.assert_called_once_with("register_missing_device_label")
