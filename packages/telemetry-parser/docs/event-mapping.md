# Event Mapping Specification

## Overview

This document defines the mapping between SIP REGISTER telemetry and structured analytics events.

The mapping layer converts protocol headers into schema-versioned telemetry attributes.

Every header read here is a standard RFC 3261 header. That is a constraint, not an
observation: devices send plain SIP and annotate nothing, so anything the parser cannot
find in the protocol does not exist to be mapped. See
[ADR-001](ADR-001-edge-producer-contract.md).

---

## Event Type Mapping

SIP Method:

REGISTER

Maps to:

sip.registration

A REGISTER whose CSeq is missing or unparseable maps to:

sip.unknown

Non-REGISTER methods are rejected before mapping and produce no event.

---

## Header Mapping

From header:

sip:\<device_label\>@domain

Maps to:

payload.device_id

Note that this is the device's own label — a name it was configured with, not an
identifier this system issued. The ingestion boundary carries it as `device_label` and
derives a UUIDv5 `device_id` from it together with the store identity. See
event-schema-contracts ADR-002.

---

Call-ID header:

Maps to:

payload.call_id

On a REGISTER this identifies the registration transaction, not a voice call.

---

Via header:

Extracted fields:

transport protocol
source IP address

Maps to:

payload.transport_protocol
payload.source_ip

---

## Event Time

Event time comes from packet capture: the timestamp of the packet carrying the message's
first byte.

There is no fallback. A message with no observation time cannot be dated, and reading the
clock instead would make two parses of the same capture produce different events.

Where the message spans several packets, the first packet's time is used throughout — a
later segment arriving, or a retransmission, does not move the event. A segment held
behind a sequence gap keeps the time it arrived, not the time the gap filled.

---

## Headers this parser does not read

Four mappings were removed with ADR-001. They are listed here because their absence is a
decision, and because this document previously specified all four.

X-Latency, X-Session-Duration and X-Timestamp are non-standard headers. Nothing defines
them, nothing emits them, and no unit or meaning was ever recorded for any of them. They
were consumed on the assumption that devices annotate their own telemetry, which devices
do not do.

Retry-After is a standard header, but RFC 3261 defines it as a *response* header. This
parser reads REGISTER requests only, so a conforming request never carries it.

`payload.latency`, `payload.retry_count` and `payload.session_duration` are gone with
them, as are the corresponding fields on `sip.registration v1` — see
event-schema-contracts ADR-002 Amendment 1.

---

## Incomplete captures

Two kinds of leftover can remain when a stream ends. Neither produces an event.

A message with no header terminator is incomplete by definition. Segments stranded behind
a sequence gap that never filled are not contiguous, so joining them would fabricate a
stream that was never observed.

Both are discarded and reported to the observer as dropped, with counts. A lossy capture
is worth knowing about; it used to be hidden inside events that looked no different from
events parsed cleanly.

---

## Output Schema

Example structured event:

{
  "schema_version": "v1",
  "event_type": "sip.registration",
  "event_timestamp": "2026-08-24T09:30:00+00:00",
  "payload": {
    "device_id": "handset-42",
    "registration_status": "registered",
    "transport_protocol": "TCP",
    "call_id": "abc123@10.0.0.5",
    "source_ip": "10.0.0.5"
  }
}

This schema is compatible with downstream analytics and ML dataset pipelines.
