"""
sip_registration_event.py

Canonical telemetry contract for SIP registration observations.

Distinct from ``device_event.DeviceRegistrationEvent``, despite the
similar name: that schema describes a device being *provisioned* onto
the fleet (device type, firmware version, registration completion),
whereas this one describes an ongoing *SIP registration observation* —
whether an in-store device is currently reachable, how long its
registration took, and how many retries it needed.

The two were conflated at one point: ``telemetry-parser`` emitted
``event_type="device.registration"``, clashing with the other schema's
registered identity while producing an incompatible field set. This
schema exists so the parser's actual output has a home. See
docs/ADR-002-sip-registration-schema.md.

Field naming deliberately diverges from the parser's internal names in
three places, each noted inline below.
"""

from datetime import datetime
from enum import Enum
from typing import ClassVar
from uuid import UUID

from pydantic import Field, IPvAnyAddress

from event_schema_contracts.base.base_event import BaseEvent
from event_schema_contracts.base.domain import DomainEventPayload

# Matches DeviceRegistrationPayload's store_id constraint, so the two
# schemas agree on what a store identifier looks like.
STORE_ID_PATTERN = r"^[a-z0-9]+([.\-_][a-z0-9]+)*$"


class RegistrationStatus(str, Enum):
    """
    Outcome of an observed SIP registration attempt.

    Only REGISTERED is defined, because it is the only value
    ``telemetry-parser`` can currently produce: ``map_registration_status``
    tests whether the CSeq header contains REGISTER and returns
    ``"registered"`` or nothing. The parser reads REGISTER *requests*
    only — it never parses responses, and never reads the Expires
    header (verified: no reference to it anywhere in the package).

    Two states are therefore deliberately absent rather than forgotten:

    - a clean deregistration (REGISTER with ``Expires: 0``) — needs
      Expires parsing
    - a failed or challenged registration (401 / 403) — needs response
      parsing

    Both matter operationally: a store closing for the night
    deregisters its devices, and treating that as a fault would fire
    false offline detections nightly. They are omitted here because
    declaring enum values no producer can emit describes a parser that
    does not exist, and invites consumers to write dead branches.

    Adding values later is a payload change, not a required-field
    introduction, so it does not force a major version bump — but
    consumers should still treat an unrecognised value as "not
    currently registered" rather than matching exhaustively.
    """

    REGISTERED = "REGISTERED"


class SipTransportProtocol(str, Enum):
    """
    Transport observed in the SIP Via header.

    Deliberately declared here rather than reusing
    ``network_event.TransportProtocol``, despite currently identical
    members. Each schema in this library is independently versioned, and
    importing another schema's enum would mean a change made for
    ``network.connection``'s benefit silently alters ``sip.registration``'s
    contract. The cost is a duplicated three-member enum; the benefit is
    that the two schemas can diverge without one breaking the other.
    """

    TCP = "TCP"
    UDP = "UDP"
    TLS = "TLS"


class SipRegistrationPayload(DomainEventPayload):
    """
    Payload schema for SIP registration telemetry.

    One instance represents a single observed REGISTER transaction from
    one in-store device.
    """

    # device_id is UUIDv5, derived from (store_id, device_label) rather
    # than minted randomly — a device must resolve to the same id on
    # every run, or replay diverges from the run it reproduces. v4 is
    # also accepted so a future producer with a genuine random device
    # id is not locked out.
    __uuid_v4_or_v5_fields__: ClassVar[tuple[str, ...]] = ("device_id",)

    __utc_fields__: ClassVar[tuple[str, ...]] = ("observed_at",)

    device_id: UUID = Field(
        ...,
        description=(
            "Stable device identifier, derived as UUIDv5 from "
            "(store_id, device_label). The join key for detection and "
            "aggregation."
        ),
    )

    # Carried alongside the derived id because UUIDv5 derivation is
    # one-way: without this, device_label is unrecoverable from the
    # event, and anyone debugging a store has a physical headset label
    # and nothing to match it against. Never used as a join key.
    device_label: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description=(
            "Human-readable device identifier as it appeared in the SIP "
            "From header. For display and support lookup only."
        ),
    )

    store_id: str = Field(
        ...,
        pattern=STORE_ID_PATTERN,
        min_length=1,
        description="Store the observed device belongs to.",
    )

    registration_status: RegistrationStatus = Field(
        ...,
        description="Outcome of the observed registration attempt.",
    )

    observed_at: datetime = Field(
        ...,
        description=(
            "UTC event-time of the observation. Sourced from the "
            "X-Timestamp header where present, otherwise the packet "
            "capture time — never ingestion wall-clock, which would "
            "break replay determinism."
        ),
    )

    # Named latency_ms, not latency: the parser's field is a bare number
    # with no unit in its name, and NetworkConnectionPayload already
    # establishes latency_ms as the convention. Unnamed units are how a
    # silent factor-of-1000 error gets in.
    latency_ms: int | None = Field(
        None,
        ge=0,
        le=60_000,
        description="Observed registration latency in milliseconds.",
    )

    retry_count: int | None = Field(
        None,
        ge=0,
        description="Retries before this registration attempt resolved.",
    )

    transport_protocol: SipTransportProtocol | None = Field(
        None,
        description="Transport observed in the SIP Via header.",
    )

    source_ip: IPvAnyAddress | None = Field(
        None,
        description="Source address the registration was observed from.",
    )

    # Named registration_call_id, not call_id: on a REGISTER the SIP
    # Call-ID identifies the registration transaction, not a phone call.
    # Left as call_id it reads as call telemetry, which this is not.
    registration_call_id: str | None = Field(
        None,
        max_length=256,
        description=(
            "SIP Call-ID of the REGISTER transaction. Correlates retries "
            "of one registration; unrelated to voice calls."
        ),
    )


# Schema identity
EVENT_TYPE = "sip.registration"
SCHEMA_VERSION_V1 = "v1"


class SipRegistrationEvent(BaseEvent[SipRegistrationPayload]):
    """
    sip.registration v1

    Canonical contract for SIP registration telemetry observed at the
    edge and ingested centrally.
    """

    __event_type__: ClassVar[str] = EVENT_TYPE
    __schema_version__: ClassVar[str] = SCHEMA_VERSION_V1
