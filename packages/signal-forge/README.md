# signal-forge

Analytics, detection, replay and dataset control plane for the **SignalForge** telemetry intelligence platform.

This repository sits at the top of the SignalForge stack and consumes validated `telemetry_event` records produced by `telemetry-parser`, transforming them into:

- `detection_event` — outage, offline, and anomaly signals
- `feature_vector` — versioned windowed features for downstream consumers
- `dataset_record` — partitioned, replay-safe analytics datasets in S3/Parquet
- `alert_event` — routed alerts (EventBridge / SNS)
- dashboard projections — sub-5-second materialised KPI views

**Status:** `v1.0.0` — all seven phases complete. 384 tests; `ruff` and `mypy --strict` clean across 42 source files; CI green on Python 3.11 and 3.12.

## Upstream dependencies

| Repository                  | Role                                                           |
| --------------------------- | -------------------------------------------------------------- |
| `event-schema-contracts`    | canonical event envelopes, schema registry, semver enforcement |
| `structured-logging-python` | trace-aware structured logging                                 |
| `telemetry-parser`          | raw → validated `BaseEvent` extraction                         |
| `aws-event-pipeline-infra`  | EventBridge bus, archives, replay Step Functions, SQS queues   |

Logic owned upstream is **never** redefined here.

## Engineering principles

Every module in this repository is **typed, modular, replay-safe, schema-aware, deterministic, testable, observable, version-aware, infrastructure-compatible, and both streaming- and batch-compatible.**

The headline property is **replay determinism**: the same event sequence in the same order produces byte-identical outputs, so an archived stream can be reprocessed through the same code to regenerate derived state. This is verified end-to-end by `tests/replay/test_replay_determinism.py`, which drives one event sequence through a live pipeline and the replay driver and asserts byte-identical dataset Parquet and content-identical projection rows across isolated sinks.

See `docs/architecture.md` for the full pipeline view.

## Phase status

| Phase | Scope                                                | Status      |
| ----- | ---------------------------------------------------- | ----------- |
| 1     | Realtime event routing & window aggregation          | ✅ complete |
| 2     | Detection engines (offline, outage, anomaly)         | ✅ complete |
| 3     | Feature pipelines & device registry                  | ✅ complete |
| 4     | Dataset partitioning, versioning, S3/Parquet export  | ✅ complete |
| 5     | Alert routing (EventBridge, SNS)                     | ✅ complete |
| 6     | Dashboard materialised views                         | ✅ complete |
| 7     | Replay & backfill orchestration                      | ✅ complete |

Each phase is tagged `phase-N-complete`; the project release is tagged `v1.0.0`. Per-phase plans live in `docs/phases/`, point-in-time snapshots in `docs/status/`, and the cumulative design-decision log in `docs/working-notes.md`.

## What each layer provides

- **Streaming foundation** (`signal_forge/streaming/`) — `EventRouter`, `WatermarkManager`, `WindowAggregator`, and the function-shaped `RealtimePipeline`. Epoch-aligned windows and wall-clock-free watermarks make the streaming output reproducible.
- **Detection** (`signal_forge/detection/`) — `OfflineDetector`, `OutageDetector`, `AnomalyDetector`, plus the `DeviceRegistry` projection. Detections flow back as a typed `ProcessingResult.detections` return value, not as side effects.
- **Features** (`signal_forge/features/`) — window emissions bundled into `WindowedFeatureVectorEvent`s on `ProcessingResult.features`.
- **Datasets** (`signal_forge/datasets/`) — `(store_id, hour)` partitioning to S3/Parquet with schema-per-file evolution, via a `moto`-verified `S3DatasetWriter`.
- **Dashboards** (`signal_forge/dashboards/`) — three materialised projections (offline-count gauge, active-outage set, anomaly-rate window) over a `ProjectionStore` with an in-memory and a DynamoDB backend, routed beside the pipeline.
- **Replay** (`signal_forge/replay/`) — `run_replay`, a function-shaped driver that reprocesses an archived event sequence through a replay-isolated pipeline, plus a thin CLI (`python -m signal_forge.replay <config.json>`).

## Development

```
pip install -e ".[dev]"
python -m unittest discover -s tests
ruff check .
mypy signal_forge
```

Optional extras: `.[datasets]` (pyarrow, pandas, boto3) for the S3/Parquet layer, `.[dashboards]` (boto3) for the DynamoDB projection store. The `.[dev]` group carries the test and lint toolchain (pytest, mypy, ruff, hypothesis, moto).
