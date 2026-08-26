from dataclasses import dataclass
from typing import Dict, Literal


@dataclass(frozen=True)
class ExtractedEventFields:
    """
    Telemetry attributes taken from a SIP REGISTER.

    Every field here comes from a standard RFC 3261 header. Four fields
    were removed when the edge producer contract was written: latency,
    retry_count and session_duration read non-standard X- headers that no
    device sends and no document defines, and event_timestamp read
    X-Timestamp, which observation time replaced. See
    docs/ADR-001-edge-producer-contract.md.
    """

    # The From header's user part: a name the device was configured
    # with, not an identifier this system issued. It is unique only
    # within a store, which is why the stable device identity is
    # derived downstream from (store_id, device_label).
    device_label: str | None
    # Not optional. A REGISTER whose CSeq does not agree with its request
    # line is malformed and is rejected during extraction, so if one of
    # these exists at all, it describes a registration.
    registration_status: Literal["registered"]
    transport_protocol: str | None
    call_id: str | None
    source_ip: str | None


class FieldMapper:
    """
    Maps SIP message headers into structured telemetry attributes.

    Designed to support downstream:
    - analytics pipelines
    - feature stores
    - dataset generation workflows
    """

    def map_registration_status(
        self,
        headers: Dict[str, str],
    ) -> Literal["registered"] | None:
        """
        Returns "registered" for a well-formed REGISTER, or None when the
        CSeq header is absent or names a different method.

        None means the message contradicts itself: the request line said
        REGISTER — the extractor rejects anything else before this runs —
        while CSeq says otherwise. That is a malformed message, not a
        different kind of message, which is why the caller drops it rather
        than labelling it.
        """

        cseq = headers.get("cseq")

        if not cseq:
            return None

        if "REGISTER" in cseq:
            return "registered"

        return None
