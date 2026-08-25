# event-schema-contracts

**Canonical telemetry event schema contracts for the SignalForge telemetry intelligence platform**

`event-schema-contracts` defines the authoritative, versioned event schemas shared across every SignalForge service — parsers, the streaming analytics control plane, feature pipelines, dataset exporters, alert routing, and replay workflows. It is the schema authority layer: no service defines event schemas independently of this repository.

**Current version:** `v0.7.0` — the first tagged release. Backward-compatible within the `v0` line; while the major version is 0, a breaking change bumps the minor (see the schema-evolution policy below). See [CHANGELOG.md](CHANGELOG.md) for what each version says.

---

## Overview

The repository provides:

- a canonical `BaseEvent` envelope with identity, timestamp, and trace mixins
- typed domain payloads — telemetry, detection, features, and alerts
- a schema registry with `(event_type, schema_version)` resolution
- semantic-version compatibility enforcement
- deterministic trace-lineage propagation
- replay-safe schema-evolution guarantees

It defines the compatibility boundary between ingestion, parsing, streaming analytics, feature building, dataset export, alert routing, and replay.

---

## Installation

Install a pinned schema version — pinning is what gives downstream services deterministic replay and dataset reproducibility:

```
pip install "event-schema-contracts @ git+https://github.com/nic-thompson/event-schema-contracts@v0.7.0"
```

Pin a tag, never a branch. A tag resolves to one immutable commit, which is what
makes it possible to say which version of the contract a component validated
against — and to get the same answer again on replay.

Do not copy this package's source into a consuming repository. A copied schema is
a fork the moment it lands: nothing constrains it, nothing notices when it drifts,
and two plausible definitions of the same event can coexist indefinitely. Keeping
one definition is the entire purpose of this repository.

For development:

```
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Releasing

The version in `pyproject.toml` is the single source of truth. Merging a version
bump to `main` cuts the release: CI tags `v<version>`, builds the distributions,
and publishes a GitHub release with that version's `CHANGELOG.md` section as its
notes. Tags are never pushed by hand, so the tag and the packaged version cannot
disagree.

A release with no `CHANGELOG.md` entry for its version fails the build rather than
publishing. A version consumers cannot read about is one they cannot reason about.

---

## Event domains

The contracts are organised into four payload domains, each carried by the shared `BaseEvent` envelope.

### Telemetry (`telemetry/`)

Device, network, and session events — the raw structured telemetry the parser emits and the streaming layer consumes. `DeviceRegistrationPayload` carries `device_id`, `store_id`, `device_type`, optional `firmware_version`, and `registered_at`; the `store_id` field is what lets downstream consumers project device→store membership.

### Detection (`detection/`)

`DetectionEvent` — a single discriminator-pattern schema for every detection type. `detection_type` (a dotted-lowercase string like `device.offline`, `store.outage`, `signal.anomaly`) selects the shape of the free-form `details` dict; `severity` (a `DetectionSeverity` enum) drives downstream escalation; `store_id` and `threshold_breached` carry denormalised context. One schema covers all detectors, so a new detector type needs no schema change.

### Features (`features/`)

Two feature-vector variants for two distinct uses:

- `FeatureVectorEvent` — entity-centric (`entity_id`, `source_event_id`), for ML feature stores and online inference.
- `WindowedFeatureVectorEvent` — partition-window-centric (`partition_key`, `window_start`, `window_end`, `feature_values`, `feature_version`), for streaming aggregations and dashboard rollups. This is the variant the SignalForge streaming pipeline emits.

### Alerts (`alerts/`)

- `AlertEvent` — carries the alert's own identity (`alert_id`, permitted to be UUIDv5 so re-deliveries collapse), lineage to the originating detection (`detection_id`), and the detector-assigned `severity` (reusing `DetectionSeverity`, so detection and alerting share one severity vocabulary).
- `AlertAcknowledgementEvent` — an acknowledgement modelled as an event, so acknowledgement *state* can be projected replay-deterministically by folding the ordered stream. References the alert it resolves by `alert_id`.

---

## Event envelope contract

All canonical events share a structure:

```
{
  "schema_version": "v1",
  "event_id": "uuid",
  "trace": { "trace_id": "uuid", "root_trace_id": "uuid", "pipeline_stage": "..." },
  "event_timestamp": "...",
  "event_type": "device.registration",
  "payload": { }
}
```

The envelope enforces schema identity, timestamp ordering, ingestion-boundary validation, cross-service compatibility, and replay safety.

---

## Example usage

Construct a canonical telemetry event:

```python
from uuid import uuid4
from datetime import datetime, timezone

from event_schema_contracts.telemetry.device_event import (
    DeviceRegistrationEvent,
    DeviceRegistrationPayload,
    DeviceType,
)
from event_schema_contracts.base.trace import TraceContext, PipelineStage

event = DeviceRegistrationEvent(
    trace=TraceContext(
        root_trace_id=uuid4(),
        pipeline_stage=PipelineStage.INGESTION,
    ),
    event_timestamp=datetime.now(timezone.utc),
    payload=DeviceRegistrationPayload(
        device_id=uuid4(),
        store_id="store-1",
        device_type=DeviceType.SENSOR,
        registered_at=datetime.now(timezone.utc),
    ),
)
```

Resolve and validate via the schema registry:

```python
from event_schema_contracts.versioning.schema_registry import schema_registry

schema = schema_registry.get_schema(
    event_type="device.registration",
    schema_version="v1",
)
validated = schema_registry.validate(event.model_dump())
```

---

## Schema registry

Schemas register against `(event_type, schema_version)` via subclass identity metadata. The registry resolves a schema for a given event type and version, lists registered versions, and validates payloads. Resolution is deterministic; compatibility fallback within a major version is supported (a consumer registered for `v1` accepts `v1.1` events).

---

## Compatibility model

Schemas follow semantic versioning (`vMAJOR.MINOR.PATCH`).

| Change                  | Requires      |
| ----------------------- | ------------- |
| optional field addition | minor version |
| metadata / trace extension | minor version |
| new event domain        | minor version |
| field removal           | major version |
| type modification       | major version |
| payload restructuring   | major version |

Backward compatibility is guaranteed within a major version, enforced by the registry. The `v0.x` line has accreted the detection, windowed-feature, and alerts domains as minor additions without breaking existing consumers.

---

## Repository structure

```
event_schema_contracts/
├── base/
│   ├── base_event.py
│   ├── domain.py
│   ├── identity.py
│   ├── metadata.py
│   ├── time.py
│   ├── trace.py
│   └── versioning.py
├── telemetry/
│   ├── device_event.py
│   ├── network_event.py
│   └── session_event.py
├── detection/
│   └── detection_event.py
├── features/
│   ├── feature_vector.py
│   └── windowed_feature_vector.py
├── alerts/
│   ├── alert_event.py
│   └── alert_acknowledgement.py
├── validation/
│   └── validators.py
└── versioning/
    ├── schema_registry.py
    └── compatibility.py
```

Supporting documentation:

```
docs/
├── schema-versioning.md
├── event-lifecycle.md
└── compatibility-policy.md
```

---

## Schema evolution policy

Breaking changes require a major-version increment, migration documentation, replay validation, and dataset-regeneration verification. Backward compatibility holds within a major version.

See `docs/schema-versioning.md` and `docs/compatibility-policy.md`.

---

## Development

```
pip install -e ".[dev]"
pytest
```

Tests validate schema identity enforcement, registry resolution, compatibility guarantees, timestamp-ordering rules, and payload-type integrity.
