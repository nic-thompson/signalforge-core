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

device_id
registration_status
latency
retry_count
transport_protocol
session_duration
call_id
source_ip
event_timestamp

Output:

ExtractedEventFields

---

## Stage 5: Event Normalisation

Input:

ExtractedEventFields

Responsibilities:

- schema version assignment
- timestamp normalisation
- event type mapping
- payload structuring

Output:

StructuredEvent

---

## Stage 6: Event Emission

Input:

StructuredEvent

Responsibilities:

- iterator-safe streaming output
- JSON-safe serialisation
- observability signaling

Output:

analytics-ready structured telemetry events