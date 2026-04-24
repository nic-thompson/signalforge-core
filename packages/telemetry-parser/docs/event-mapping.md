# Event Mapping Specification

## Overview

This document defines the mapping between SIP REGISTER telemetry and structured analytics events.

The mapping layer converts protocol headers into schema-versioned telemetry attributes.

---

## Event Type Mapping

SIP Method:

REGISTER

Maps to:

device.registration

Unsupported methods map to:

device.unknown

---

## Header Mapping

From header:

sip:<device_id>@domain

Maps to:

payload.device_id

---

Call-ID header:

Maps to:

payload.call_id

---

Via header:

Extracted fields:

transport protocol
source IP address

Maps to:

payload.transport_protocol
payload.source_ip

---

Retry-After header:

Maps to:

payload.retry_count

---

X-Latency header:

Maps to:

payload.latency

---

X-Session-Duration header:

Maps to:

payload.session_duration

---

X-Timestamp header:

Maps to:

event_timestamp

Fallback:

packet timestamp

Fallback:

ingest timestamp

---

## Output Schema

Example structured event:

{
  "schema_version": "v1",
  "event_type": "device.registration",
  "payload": {
    "device_id": "handset-42",
    "transport_protocol": "TCP",
    "retry_count": 0
  }
}

This schema is compatible with downstream analytics and ML dataset pipelines.