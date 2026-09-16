# structured-logging-python

Structured JSON logging and distributed trace propagation for the **SignalForge** telemetry intelligence platform.

This library provides the canonical Python implementation of SignalForge's structured logging contract, giving every service — the parser, the analytics control plane, dataset exporters, and replay workflows — one consistent, trace-aware, machine-readable logging surface. It is a platform infrastructure component, not a generic logging wrapper.

## Role in the platform

SignalForge is composed of cooperating repositories forming a telemetry analytics and dataset-generation system. `structured-logging-python` is the shared observability substrate beneath them:

```
event-schema-contracts
        ↓
telemetry-parser
        ↓
signal-forge
        ↓
dataset export + replay
```

Every layer emits through the same logging contract, so an execution timeline — ingestion → parse → feature generation → dataset export → replay — can be reconstructed from a shared, trace-linked log model. Although developed for SignalForge, it is designed as a reusable structured-logging SDK for distributed Python services in event-driven pipelines.

## What it provides

- **Canonical structured log events** — every log line conforms to a stable schema (`LogEventSchema`): timestamp, level, service, environment, event_type, message, plus optional `trace_id`, `parent_trace_id`, and `correlation_id` lineage fields. `service` and `environment` are injected automatically from configuration.
- **Distributed trace propagation** — trace context flows across service and execution boundaries via standard propagation headers, so execution graphs can be reconstructed across the pipeline.
- **Pipeline latency instrumentation** — measure execution time across processing stages.
- **Machine-readable error diagnostics** — structured failures with stable error codes, suitable for retry classification, dead-letter routing, and alerting.
- **Runtime adapters** — drop-in instrumentation for FastAPI, AWS Lambda, and async queue workers.

## Core logging

```python
from structured_logging.core.logger import StructuredLogger

logger = StructuredLogger(__name__)

logger.info(
    "Feature vector stored",
    event_type="feature.generated",
)
```

`StructuredLogger` exposes `info`, `warning`, `error`, `debug`, the lower-level `log`, and `emit_error`. Each takes a `message`, an optional `event_type`, optional `metadata`, and an optional `trace_id`.

Structured error diagnostics carry a stable error code via `emit_error`:

```python
logger.emit_error(
    error_code="FEATURE_TIMEOUT",
    message="Embedding service exceeded latency threshold",
)
```

`emit_error` builds a canonical `StructuredError` payload (error code, message, optional exception type, stack trace, severity, retryable flag, and origin) so callers need not construct one directly.

## Latency instrumentation

```python
from structured_logging.metrics.latency import pipeline_latency

with pipeline_latency("dataset_export"):
    export_dataset()
```

`pipeline_latency(stage, logger=None, metadata=None)` is a context manager; `pipeline_latency_decorator` is the equivalent function decorator. Both emit a structured latency event on exit, used for ingestion-delay diagnostics, dataset-freshness monitoring, and replay comparison baselines.

## Trace propagation

Trace context propagates across FastAPI endpoints, Lambda invocations, async queue workers, and pipeline stages. The carried identifiers are `trace_id`, `parent_trace_id`, and `correlation_id`, exchanged over `x-trace-id`, `x-parent-trace-id`, and `x-correlation-id` headers, allowing execution-graph reconstruction across the full pipeline.

## Runtime adapters

Adapters instrument service boundaries with minimal wiring.

**FastAPI** — `configure_fastapi_logging(app)` installs `FastAPILoggingMiddleware`, providing request trace initialisation, structured exception telemetry, response-lifecycle logging, and request-latency measurement.

**AWS Lambda** — the `@lambda_logging_handler()` decorator provides invocation trace restoration, cold-start detection, execution-latency telemetry, and structured failure emission.

**Async workers** — the `@worker_logging_handler(queue_name=...)` decorator provides queue trace propagation, retry instrumentation, and dead-letter classification, with `log_dead_letter_event` and `log_retry_event` helpers. Both synchronous and asynchronous handlers are supported.

## Repository structure

```
structured_logging/
├── core/        structured logger, formatter, context
├── trace/       distributed trace lifecycle and propagation
├── metrics/     latency instrumentation
├── schema/      canonical log-event schema
├── adapters/    FastAPI, Lambda, and worker instrumentation
└── config/      service/environment settings
```

| Layer      | Responsibility                   |
| ---------- | -------------------------------- |
| `core`     | structured logger + formatter    |
| `trace`    | distributed trace lifecycle      |
| `metrics`  | latency instrumentation          |
| `schema`   | canonical telemetry log contract |
| `adapters` | service-boundary instrumentation |
| `config`   | service and environment settings |

## Documentation

- `docs/architecture.md` — system topology
- `docs/trace-model.md` — trace lifecycle model
- `docs/integration-patterns.md` — integration patterns
- `docs/logging-contract.md` — telemetry schema contract

## Development

```
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```
