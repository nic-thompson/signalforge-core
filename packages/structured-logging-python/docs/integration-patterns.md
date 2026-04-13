# Integration Patterns

## FastAPI Integration

Attach middleware:

configure_fastapi_logging(app)

Provides:

- request trace initialisation
- latency telemetry
- structured exception logging
- response status logging

---

## Lambda Integration

Wrap handler:

@lambda_logging_handler()
def handler(event, context):

Provides:
- cold start detection
- invocation timing
- trace restoration
- structured failure logging

---

## Worker Integration 

Wrap queue handler:

@worker_logging_handler(queue_name="feature-build-queue")

Provides:
- retry telemetry
- DLQ classification
- execution latency tracking
- queue trace propagation

---

## Pipeline Latency Measurement

Measure stage execution:

with measure_pipeline_latency("dataset_export"):

Used for:

- SLA monitoring
- feature freshness tracking
- ingestion delay diagnostics


---

## Structured Error Emission

Emit machine-readable errors:

logger.emit_error(
    error_code="FEATURE_TIMEOUT",
    message="Embedding service timeout"
)

Supports automated alert routing.