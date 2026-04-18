import pytest

from telemetry_parser.protocol.sip_parser import SIPParser, SIPMessage

def build_register_message(extra_headers: bytes = b"") -> bytes:
    return (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"From: <sip:handset-42@test>\r\n"
        b"Call-ID: abc123\r\n"
        + extra_headers +
        b"\r\n"
    )


def test_parse_valid_register_message():

    parser = SIPParser()

    msg = parser.parse(build_register_message())

    assert isinstance(msg, SIPMessage)
    assert msg.method == "REGISTER"
    assert msg.device_id == "handset-42"
    assert msg.call_id == "abc123"
    assert msg.transport == "TCP" 
    assert msg.source_ip == "10.0.0.5"


def test_parse_missing_via_header():

    parser = SIPParser()

    raw = (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"From: <sip: handset-42@test>\r\n"
        b"Call-ID: abc123\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.transport is None   
    assert msg.source_ip is None   


def test_test_missing_from_header():

    parser = SIPParser()

    raw = (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"Call-ID: abc123\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.device_id is None


def test_test_missing_call_id():

    parser = SIPParser()

    raw = (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"From: <sip: handset-42@test>\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.call_id is None


def test_parse_invalid_request_line_returns_none():

    parser = SIPParser()

    raw = b"INVALID\r\nHeader: value\r\n"

    msg = parser.parse(raw)

    assert msg is None


def test_parse_empty_message_returns_none():

    parser = SIPParser()

    msg = parser.parse(b"")

    assert msg is None


def test_parse_non_register_method_still_parses():

    parser = SIPParser()

    raw = (
        b"INVITE sip:test SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"From: <sip:device@test>\r\n"
        b"Call-ID: xyz789\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.method == "INVITE"
    assert msg.device_id == "device"


def test_parse_malformed_headers_ignored_safely():

    parser = SIPParser()

    raw = (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"MalformedHeaderWithoutColon\r\n"
        b"Via: SIP/2.0/TCP 10.0.0.5:5060\r\n"
        b"From: <sip:handset-42@test>\r\n"
        b"Call-ID: abc123\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.device_id == "handset-42"
    assert msg.transport == "TCP"


def test_partial_via_header_returns_transport():

    parser = SIPParser()

    raw = (
        b"REGISTER sip:test SIP/2.0\r\n"
        b"Via: SIP/2.0/TCP\r\n"
        b"From: <sip:handset-42@test>\r\n"
        b"Call-ID: abc123\r\n"
        b"\r\n"
    )

    msg = parser.parse(raw)

    assert msg.transport is None
    assert msg.source_ip is None