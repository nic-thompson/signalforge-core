# signal-forge

Analytics, detection, replay and dataset control plane for the **SignalForge** telemetry intelligence platform.

This repository sits at the top of the SignalForge stack and consumes validated `telemetry_event` records produced by `telemetry-parser`, transforming them into:

- `detection_event` — outage, offline, and anomaly signals
- `feature_vector` — versioned features for ML pipelines
- `dataset_record` — partitioned, replay-safe analytics datasets
- `alert_event` — routed alerts (EventBridge / SNS today; Slack / PagerDuty later)
- dashboard KPI views — sub-5-second realtime aggregates

## Upstream dependencies

| Repository | Role |
|---|---|
| `event-schema-contracts` | canonical event envelopes, schema registry, semver enforcement |
| `structured-logging-python` | trace-aware structured logging |
| `telemetry-parser` | raw → validated `BaseEvent` extraction |
| `aws-event-pipeline-infra` | EventBridge bus, archives, replay Step Functions, SQS queues |

Logic owned upstream is **never** redefined here.

## Engineering principles

Every module in this repository must be **typed, modular, replay-safe, schema-aware, deterministic, testable, observable, version-aware, infrastructure-compatible, and both streaming- and batch-compatible.**

See `docs/architecture.md` for the full pipeline view.

## Phase status

| Phase | Scope | Status |
|---|---|---|
| 1 | Realtime event routing & window aggregation skeleton | in progress |
| 2 | Detection engines (offline, outage, anomaly) | not started |
| 3 | Feature pipelines & feature registry | not started |
| 4 | Dataset partitioning, versioning, S3 export | not started |
| 5 | Alert routing (EventBridge, SNS) | not started |
| 6 | Dashboard materialised views | not started |
| 7 | Replay & backfill orchestration | not started |

## Development

```bash
pip install -e ".[dev]"
pytest
ruff check .
mypy signal_forge
```