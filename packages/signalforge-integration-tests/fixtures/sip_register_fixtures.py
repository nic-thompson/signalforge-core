"""
Raw SIP REGISTER message fixtures for edge-chain integration testing.

These are *synthetic* SIP messages, hand-built to exercise the exact
headers telemetry_parser's FieldMapper reads:

    cseq                 -> registration_status
    retry-after          -> retry_count
    x-latency            -> latency
    x-session-duration   -> session_duration
    x-timestamp          -> event_timestamp
    from                 -> device_id (the part between "sip:" and "@")
    via                  -> transport_protocol, source_ip
    call-id              -> call_id

Note on fidelity: a real PCAP capture of production SIP traffic would
be higher fidelity than these hand-built messages, and is worth
producing when a capture component exists to record one. These
fixtures deliberately mirror the header set the parser reads rather
than being a complete, RFC-exhaustive SIP corpus — they test the
chain, not SIP conformance.

Messages use CRLF line endings and terminate with a blank line
(\\r\\n\\r\\n), which is what MessageDecoder frames on.
"""

CRLF = "\r\n"


def _build_register(
    *,
    device_id: str,
    domain: str = "edge.local",
    source_ip: str = "10.20.0.14",
    transport: str = "TCP",
    call_id: str,
    latency: str | None = "42.5",
    retry_after: str | None = "0",
    session_duration: str | None = "3600.0",
    timestamp: str | None = "2026-08-21T09:15:00Z",
) -> bytes:
    """Builds one raw SIP REGISTER message as it would appear on the wire."""
    lines = [
        f"REGISTER sip:{domain} SIP/2.0",
        f"Via: SIP/2.0/{transport} {source_ip}:5060;branch=z9hG4bK{call_id[:8]}",
        f"From: <sip:{device_id}@{domain}>;tag=a1b2c3",
        f"To: <sip:{device_id}@{domain}>",
        f"Call-ID: {call_id}",
        "CSeq: 1 REGISTER",
        f"Contact: <sip:{device_id}@{source_ip}:5060>",
        "Max-Forwards: 70",
        "Expires: 3600",
    ]

    if latency is not None:
        lines.append(f"X-Latency: {latency}")
    if retry_after is not None:
        lines.append(f"Retry-After: {retry_after}")
    if session_duration is not None:
        lines.append(f"X-Session-Duration: {session_duration}")
    if timestamp is not None:
        lines.append(f"X-Timestamp: {timestamp}")

    # Blank line terminates the header block — MessageDecoder frames on this.
    return (CRLF.join(lines) + CRLF + CRLF).encode("utf-8")


# A healthy registration: every mapped field present and well-formed.
HEALTHY_REGISTER = _build_register(
    device_id="headset-0001",
    call_id="c8f3a91e-4b22-4d0e-9f77-1a2b3c4d5e6f",
)

# A second, distinct device — proves multiple devices in one stream are
# each extracted separately rather than collapsing into one event.
SECOND_DEVICE_REGISTER = _build_register(
    device_id="headset-0002",
    call_id="d9e4b02f-5c33-4e1f-a088-2b3c4d5e6f70",
    latency="118.0",
    retry_after="3",
)

# A registration with the optional X- headers absent — the mapper should
# return None for those fields rather than failing, so the event still
# flows through the chain.
SPARSE_REGISTER = _build_register(
    device_id="headset-0003",
    call_id="e0f5c13a-6d44-4f20-b199-3c4d5e6f7081",
    latency=None,
    retry_after=None,
    session_duration=None,
    timestamp=None,
)

# A SIP INVITE — telemetry_parser explicitly does NOT support this
# (EventExtractor raises UnsupportedProtocolEvent for any non-REGISTER
# method). Included so the integration test can assert it is skipped
# without breaking the stream, which is the documented behaviour.
INVITE_MESSAGE = (
    CRLF.join(
        [
            "INVITE sip:reception@edge.local SIP/2.0",
            "Via: SIP/2.0/TCP 10.20.0.14:5060;branch=z9hG4bKinvite01",
            "From: <sip:headset-0001@edge.local>;tag=x1y2z3",
            "To: <sip:reception@edge.local>",
            "Call-ID: f1a6d24b-7e55-4031-c2aa-4d5e6f708192",
            "CSeq: 2 INVITE",
            "Max-Forwards: 70",
        ]
    )
    + CRLF
    + CRLF
).encode("utf-8")
