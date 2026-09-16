# Parsing Lifecycle

## Overview

The telemetry parser processes telemetry streams incrementally using a staged transformation pipeline.

Each stage operates independently and emits structured intermediate representations.

---

## Stage 1: TCP Stream Reconstruction

Input:

TCPPacket stream

Responsibilities:

- fragment buffering
- out-of-order correction
- session tracking
- FIN detection
- lifecycle timestamps

Output:

ordered byte stream segments

---

## Stage 2: Message Framing

Input:

reassembled byte stream

Responsibilities:

- message boundary detection
- header termination discovery
- partial buffering
- multi-message extraction

Output:

complete SIP message frames

---

## Stage 3: Protocol Parsing

Input:

SIP message frames

Responsibilities:

- REGISTER method detection
- header extraction
- device identity parsing
- transport inference

Output:

SIPMessage objects

---

## Stage 4: Field Extraction

Input:

SIPMessage

Responsibilities:

semantic attribute mapping

Produces:

device_label
registration_status
transport_protocol
call_id
source_ip

Output:

ExtractedEventFields, or None

None is returned for a REGISTER that contradicts its own request line —
a CSeq that is absent or names another method — or that carries no user
part in its From header and so names no device. Both are reported to the
observer rather than raised.

latency, retry_count and session_duration were produced here until the
edge producer contract was written. They read non-standard headers no
document defines and no component emits. See ADR-001.

event_timestamp is no longer extracted from the message. Event time comes
from packet capture, which is the only trustworthy record of when a
message was seen.

---

## Stage 5: Event Normalisation

Input:

ExtractedEventFields

Responsibilities:

- device identity derivation from (store_id, device_label)
- protocol values converted into the schema's enums and types
- envelope construction, including the metadata that names the schema

Output:

SipRegistrationEvent, validated on construction

The event type is from event-schema-contracts rather than defined here.
Constructing it means the schema checks this parser's output at the point
it is produced, instead of a downstream component discovering a mismatch
— or not discovering it.

---

## Stage 6: Event Emission

Input:

SipRegistrationEvent

Responsibilities:

- iterator-safe streaming output
- JSON-safe serialisation
- observability signaling

Output:

analytics-ready structured telemetry events