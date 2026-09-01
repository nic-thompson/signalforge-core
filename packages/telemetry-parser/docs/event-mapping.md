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

This is the only event type this parser emits.

Non-REGISTER methods are rejected before mapping and produce no event. That is a
filter, not a failure — a capture carries whatever SIP traffic is on the wire, and
INVITE is not malformed just because this parser does not describe it.

A REGISTER whose CSeq is missing, empty, or names a different method is a different
case: the message contradicts its own request line, so it is malformed. It is dropped
and reported to the observer as `register_cseq_mismatch`. It used to produce an event
typed `sip.unknown` — an identity no schema was ever registered for, carrying a null
`registration_status` into a payload whose schema declares that field required. Such
an event could not have validated anywhere, so it was a parse failure wearing an
event type.

---

## Header Mapping

From header:

sip:\<device_label\>@domain

Maps to:

payload.device_label

The name is deliberate. This is the device's own label — a name it was configured with,
not an identifier this system issued — and it is unique only within a store. Two stores
may both have a `headset-12`, and they are different devices.

Calling it `device_id` would invite a join across stores that silently merges them: no
error, just wrong numbers that look like two devices behaving identically. The parser has
no device identifier to emit, and now says so.

The stable identity is derived at the ingestion boundary as a UUIDv5 over
(store_id, device_label), with the label carried alongside because derivation is one-way.
See event-schema-contracts ADR-001.

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

## Store Identity

payload.store_id

Not mapped from any header. Nothing in a SIP REGISTER names a store, and nothing should:
a headset registering to a local PBX is inside one store's network, so there is exactly
one store in scope and naming it in every message would be redundant.

It comes instead from the controller's provisioned configuration, supplied when the
pipeline is constructed. This is the only value the parser emits that it did not parse.

Two consequences worth stating. A controller configured with the wrong store attributes
every device it observes to that store, and nothing downstream can detect it, because the
traffic never made a claim to contradict. And store identity is load-bearing beyond its
own field: the ingestion boundary derives each device's stable UUIDv5 from
(store_id, device_label), so the wrong store silently re-identifies every device.

A missing or blank store identity is rejected when the pipeline is constructed, so a
misconfigured controller fails at startup rather than emitting events that are rejected
one at a time at the far end. The full grammar is enforced at the ingestion boundary,
which owns the contract.

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
event-schema-contracts ADR-001 Amendment 1.

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
    "store_id": "store-0042",
    "device_label": "handset-42",
    "registration_status": "registered",
    "transport_protocol": "TCP",
    "call_id": "abc123@10.0.0.5",
    "source_ip": "10.0.0.5"
  }
}

This schema is compatible with downstream analytics and ML dataset pipelines.
