# Structured Logging Architecture

## Overview

`structured-logging-python` provides a schema-stable trace-aware logging layer
for distributed ML platform services.

It standardises telemetry across:

- ingestion pipelines
- feature builders
- dataset exporters
- inference APIs
- async workers
- Lambda orchestration workers

The library ensures that all logs are:

- JSON structured
- trace-propagated
- pipeline-stage-aware
- Cloudwatch compatible
- machine-readable

---

## Architecture Layers

The system is composed of five primary layers:

core/
trace/
metrics/
schema/
adapters/

### Core Layer

Provides:

- JSON formatter
- service metadata injection
- structured logger interface

Responsible for enforcing canonical log structure.

---

### Trace Layer

Implements distributed trace propagation using `contextvars`.

Supports:

- trace_id lifecycle
- parent trace chaining
- correlation_id linking
- pipeline stage transitions

trace_id = single execution path
correlation_id = dataset job / replay batch / user request group

Trace context propagates automatically within async execution boundaries and is rehydrated across service boundaries via explicit adapters.

- FastAPI middleware
- Lambda handlers
- async worker decorators

---

### Metrics Layer

Provides latency instrumentation primitives.

Supports measurement of:

- ingestion latency
- queue delay
- feature generation duration
- dataset export execution time
- end-to-end pipeline latency

Latency events are emitted as structured telemetry. Emits latency measurement events as structured log telemetry rather than time-series metrics.

---

### Schema Layer

Defines canonical log structure contracts. Includes schema versioning guarantees for backward-compatible ingestion pipelines.

Ensures compatibility with:

- telemetry ingestion pipelines
- replay debugging workflows
- dataset lineage reconstruction
- automated alert routing

Includes structured error definitions.

---

### Adapter Layer

Adapters integrate structured logging into runtime environments:

| Adapter | Environment |
| ------- | ----------- |
| FastAPI | Inference APIs |
| Lambda  | event-driven pipeline stages |
| Worker  | queue consumers |

Adapters automatically:

- initialise trace context
- emit lifecycle telemetry
- capture failures
- measure execution latency

---

## Pipeline Integration Flow

Typical execution lifecycle:

API Request
↓
Trace Initialised
↓
Queue Event Published (trace context serialised)
↓
Worker Execution (trace context restored)
↓
Dataset Export

Trace continuity is preserved across each boundary.

---

## Latency Instrumentation Strategy

Latency telemetry is emitted using scoped timers:

measure_pipeline_latency("feature_generalisation") 

with measure_pipeline_latency("feature_generation"):

Emitted metrics include: 

- duration_ms
- pipeline stage
- start_time
- end_time

This enables reconstruction of pipeline timing graphs.

---

## Cloudwatch Compatibility

All log events:

- single line JSON
- stable schema
- application timestamp encoded as ISO8601 UTC in the event payload

Example query:

fields event_type, duration_ms
| filter event_type = "pipeline.latency"

---

## Future OpenTelemetry Alignment

Schema fields map directly to OTel equivalents:

| Library Fields | OTel Equivalent |
| -------------- | --------------- |
| trace_id | trace_id |
| correlation_id | baggage |
| pipeline_stage | span.name |
| duration_ms | span.duration |

