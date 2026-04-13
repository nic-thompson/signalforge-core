# Trace Model

## Overview

The trace model provides distributed execution visibility across ML pipelines.

Each execution context includes:

- trace_id
- span_id
- parent_trace_id
- correlation_id
- pipeline_stage

These fields enable reconstruction of pipeline lineage. A trace represents a pipeline execution segment rather than a span-level operation. Parent trace identifiers therefore link execution segments across services instead of representing intra-trace spans.

---

## Trace ID lifecycle 

A trace begins at the first service boundary:

Examples:

- API request
- Lambda trigger
- worker trigger
- replay job

Example: 

TraceContext.start_trace()

This creates a globally unique trace identifier.

---

## Parent trace chaining

Parent trace IDs track transitions between pipeline stages.

Example:

TraceContext.child_trace(pipeline_stage="feature_generalisation")

This enables hierarchical execution reconstruction.

---

## Correlation ID

Correlation IDs link independent traces belonging to the same workflow.

Example workflows:

- dataset export jobs
- replay pipelines
- feature backfills
- batch inference runs


Example:

TraceContext.start_trace(
    correlation_id="dataset-export-2026-03-24"
)

---

## Pipeline Stage Attribution

Pipeline stage identifies logical execution boundries.

Examples:

| Stage | Meaning |
| ----- | ------- |
| api_request | HTTP Entrypoint |
| lambda_invocation | serverless execution |
| worker_execution | queue consumer |
| feature_generation | feature pipeline | 
| dataset_export | snapshot materialisation |

---

## Cross-Service Propagation Strategy

Trace context propagation through headers:

x-trace-id
x-parent-trace-id
x-correlation-id
x-pipeline-stage

Propagation occurs automatically via:

TracePropagation.inject_headers()
TracePropagation.extract_headers()

---

## Replay Pipeline Support

Replay jobs reuse correlation IDs while generating new trace IDs.

This enables comparison between:

- original execution
- replay execution  

