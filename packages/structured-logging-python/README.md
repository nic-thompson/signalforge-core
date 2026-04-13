# structured-logging-python

Structured JSON logging and distributed trace propagation for the **SignalForge telemetry intelligence platform**.

This repository provides the canonical Python implementation of SignalForge’s structured telemetry logging contract. It enables consistent observability across ingestion pipelines, feature builders, dataset exporters, replay workflows, and inference services.

It is not a generic logging wrapper. It is a platform infrastructure component that standardises telemetry emission across service boundaries.

---

## Role in the SignalForge Platform

SignalForge is composed of multiple cooperating repositories forming a telemetry analytics and dataset generation system.

`structured-logging-python` provides the observability substrate shared across those services.

Platform relationship:

event-schema-contracts
        ↓
telemetry-parser
        ↓
signal-forge
        ↓
dataset export + replay pipelines

structured-logging-python provides the shared observability substrate across all layers.
```

This repository ensures:

* schema-stable telemetry events
* trace continuity across services
* pipeline-stage attribution
* latency instrumentation across ingestion boundaries
* machine-readable failure diagnostics

---

## Platform Architecture Context

`structured-logging-python` is part of the SignalForge telemetry intelligence platform and provides the shared observability substrate used across ingestion pipelines, feature generation services, dataset exporters, and replay workflows.

Within SignalForge, the repository occupies the telemetry contract layer responsible for:

* schema-stable structured log emission
* distributed trace propagation across execution boundaries
* pipeline-stage attribution for feature workflows
* latency instrumentation across dataset generation stages
* machine-readable diagnostics for replay debugging

Conceptually, it sits between event schema definitions and analytics orchestration:

```
event-schema-contracts
        ↓
structured-logging-python
        ↓
signal-forge
```

This allows SignalForge services to reconstruct execution timelines such as:

```
API request
    → queue ingestion
        → parser execution
            → feature generation
                → dataset export
                    → replay pipeline
```

using a shared telemetry model.

Although developed as part of SignalForge, the library is designed as a reusable structured logging SDK for distributed Python services participating in event-driven data pipelines.

## What This Library Provides

### Canonical Structured Telemetry

Every log event emitted by SignalForge services conforms to a stable schema:

```
timestamp
level
service
environment
trace_id
event_type
message
metadata
```

The `service` and `environment` fields are injected automatically via ServiceContext, ensuring consistent metadata across services.

Optional lineage fields:

```
parent_trace_id
correlation_id
pipeline_stage
error_code
```

These guarantees enable:

* deterministic replay debugging
* dataset lineage reconstruction
* ingestion pipeline filtering
* CloudWatch Insights queries
* Athena analytics workflows

---

### Distributed Trace Propagation

Trace context propagates automatically across:

* FastAPI inference endpoints
* AWS Lambda orchestration steps
* async queue workers
* feature generation stages
* dataset export boundaries

Trace identifiers:

```
trace_id
parent_trace_id
correlation_id
pipeline_stage
```

Propagation headers:

```
x-trace-id
x-parent-trace-id
x-correlation-id
x-pipeline-stage
```

This allows reconstruction of execution graphs across the full SignalForge pipeline.

---

### Pipeline Latency Instrumentation

Measure execution time across telemetry processing stages:

```python
from structured_logging.metrics.latency import measure_pipeline_latency

with measure_pipeline_latency("feature_generation"):
    generate_embeddings(records)
```

Emits structured telemetry:

```
pipeline.latency
```

Used for:

* ingestion delay diagnostics
* dataset freshness monitoring
* exporter performance tracking
* replay comparison baselines
* SLA enforcement

---

### Machine-Readable Error Diagnostics

Emit structured failures with stable error identifiers:

```python
logger.emit_error(
    error_code="SCHEMA_VERSION_UNSUPPORTED",
    message="Unsupported schema version detected",
)
```

Supports:

* retry classification
* dead-letter routing
* anomaly clustering
* alert automation
* ingestion filtering

---

## Runtime Integration Adapters

Adapters instrument service boundaries automatically.

### FastAPI

```
configure_fastapi_logging(app)
```

Provides:

* request trace initialisation
* structured exception telemetry
* response lifecycle logging
* request latency measurement

---

### AWS Lambda

```
@lambda_logging_handler()
```

Provides:

* invocation trace restoration
* cold start detection
* execution latency telemetry
* structured failure emission

---

### Async Workers

```
@worker_logging_handler(queue_name="feature-build-queue")
```

Provides:

* queue trace propagation
* retry instrumentation
* dead-letter classification
* lifecycle telemetry

Supports both synchronous and asynchronous handlers.

---

## Example Usage

Structured telemetry event:

```python
from structured_logging.core.logger import StructuredLogger

logger = StructuredLogger(__name__)

logger.info(
    "Feature vector stored",
    event_type="feature.generated",
)
```

Latency instrumentation:

```python
with measure_pipeline_latency("dataset_export"):
    export_dataset()
```

Structured diagnostics:

```python
logger.emit_error(
    error_code="FEATURE_TIMEOUT",
    message="Embedding service exceeded latency threshold",
)
```

---

## Repository Structure

```
structured_logging/

core/
trace/
metrics/
schema/
adapters/
config/

docs/
tests/
scripts/
```

Layer responsibilities:

| Layer    | Responsibility                   |
| -------- | -------------------------------- |
| core     | structured logger + formatter    |
| trace    | distributed trace lifecycle      |
| metrics  | latency instrumentation          |
| schema   | canonical telemetry contract     |
| adapters | service boundary instrumentation |

---

## Documentation

Architecture overview:

```
docs/architecture.md
```

Trace lifecycle model:

```
docs/trace-model.md
```

Integration patterns:

```
docs/integration-patterns.md
```

Telemetry schema contract:

```
docs/logging-contract.md
```

---

## Development Setup

Create environment:

```
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```
pip install -e ".[dev]"
```

Run tests:

```
pytest
```

Bootstrap script available:

```
scripts/bootstrap.sh
```

---

## Design Goals

This repository enables SignalForge services to produce telemetry that is:

* schema-stable
* trace-propagated
* pipeline-stage aware
* replay-compatible
* ingestion-safe
* CloudWatch-native

It serves as the observability foundation for dataset generation and telemetry intelligence workflows across the SignalForge platform.


