# Logging Contract

## Required Fields

Every log event must include:

| Field | Description |
| ----- | ----------- |
| timestamp | UTC ISO8601 timestamp with timezone designator (e.g. 2026-04-13T09:41:22Z) |
| level | log severity |
| service | service name |
| environment | deployment environment |
| trace_id | distributed trace identifier |
| event_type | structured event category |
| message | human-readable description |
| metadata | structured event payload |

--- 

## Optional Fields

These fields may appear when available:

| Field | Description |
| parent_trace_id | upstream execution context |
| correlation_id | workflow identifier |
| pipeline_stage | logical execution stage |
| error_code | machine-readable error identifier |

---

## Event Type Naming Convention

Event types follow dotted hierarchy:

Examples:

- api.response
- lambda.invocation.completed
- worker.execution.started
- dataset.export.completed
- validation.failure
- pipeline.latency

---

## Error Logging Standard

Errors must include:

- error_code
- event_type
- metadata
- trace_id

Recommended error code format:

DOMAIN_ERROR_REASON

Examples:

SCHEMA_VERSION_UNSUPPORTED
FEATURE_VECTOR_MISSING
DATASET_PARTITION_WRITE_FAILED

---

## Metadata Guidelines

Metadata must:

- be JSON serialisable
- avoid PII
- remain schema-stable
- avoid large payloads

Recommended size:

< 5 KB per event

---

## Cloudwatch Query Compatibility

Example:

fields event_type, error_code
| filter error_code = "FEATURE_TIMEOUT"

---

## OpenTelemetry Compatibility

Mapping Strategy:

| Logging Field | OTel Field |
| ------------- | ---------- |
| trace_id      | trace_id   |
| pipeline_stage | span.name |
| duration_ms | span.duration |
| correlation_id | baggage |


