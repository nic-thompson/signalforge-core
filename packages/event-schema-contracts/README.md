# event-schema-contracts

**Canonical telemetry event schema contracts for distributed ML platform infrastructure**

`event-schema-contracts` defines the authoritative schema interface layer used across ingestion services, parsers, feature pipelines, dataset exporters, replay systems, inference APIs, and experiment tracking layers.

This repository serves as the **schema authority layer** for platform-wide data compatibility and enforces deterministic schema evolution guarantees across all services that produce or consume telemetry.

---

## Overview

Modern ML platforms require stable, versioned telemetry contracts shared across multiple services. This repository provides:

- canonical event envelopes
- typed telemetry schemas
- feature vector contracts
- schema registry resolution
- semantic version compatibility enforcement
- deterministic trace lineage propagation
- replay-safe schema evolution guarantees

It defines the compatibility boundary between:

- ingestion services
- telemetry parsers
- enrichment pipelines
- feature builders
- dataset exporters
- inference APIs
- replay jobs
- experiment tracking layers

No service should define event schemas independently of this repository.

---

## Architecture Role

`event-schema-contracts` functions as:

> the schema authority layer

All platform services depend on it transitively.

It guarantees:

- ingestion boundary validation
- dataset reproducibility
- replay determinism
- schema evolution safety
- cross-service trace alignment
- feature lineage integrity

---

## Installation

Install a pinned schema version:

```bash
pip install event-schema-contracts==0.1.0
```

Version pinning enables:

- deterministic replay
- dataset reproducibility
- safe schema rollout
- compatibility guarantees across pipelines

For development:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Example Usage

Create a canonical telemetry event:

```python
from uuid import uuid4
from datetime import datetime, timezone

from event_schema_contracts.telemetry.device_event import DeviceRegistrationEvent
from event_schema_contracts.base.trace import TraceContext, PipelineStage

event = DeviceRegistrationEvent(
    trace=TraceContext(
        root_trace_id=uuid4(),
        pipeline_stage=PipelineStage.INGESTION,
    ),
    event_timestamp=datetime.now(timezone.utc),
    payload={
        "device_id": uuid4(),
        "device_type": "sensor",
        "registered_at": datetime.now(timezone.utc),
    },
)
```

Validate dynamically via the schema registry:

```python
from event_schema_contracts.versioning.schema_registry import schema_registry

validated = schema_registry.validate(event.model_dump())
```

---

## Event Envelope Contract

All canonical events follow a shared structure:

```json
{
  "schema_version": "v1",
  "event_id": "uuid",
  "trace_id": "uuid",
  "event_timestamp": "...",
  "ingest_timestamp": "...",
  "event_type": "device.registration",
  "source": "ingestion.lambda",
  "payload": {}
}
```

Envelope guarantees:

- schema identity enforcement
- timestamp ordering correctness
- ingestion boundary validation
- cross-service compatibility
- replay safety

---

## Trace Propagation Model

Each event includes deterministic lineage metadata:

| Field | Purpose |
|------|---------|
| trace_id | stage-local processing identifier |
| root_trace_id | pipeline lineage anchor |
| pipeline_stage | typed processing stage enum |

Pipeline stages include:

- INGESTION
- VALIDATION
- ENRICHMENT
- FEATURE_BUILDING
- EXPORT
- INFERENCE

Trace metadata enables:

- cross-service correlation
- latency attribution
- replay graph reconstruction
- dataset lineage tracking

---

## Schema Registry

Schemas are registered automatically via subclass identity metadata.

Resolve schemas dynamically:

```python
from event_schema_contracts.versioning.schema_registry import schema_registry

schema = schema_registry.get_schema(
    event_type="device.registration",
    schema_version="v1",
)
```

Registry guarantees:

- deterministic schema resolution
- metadata alignment enforcement
- compatibility fallback support
- version correctness validation

---

## Compatibility Model

Schemas follow semantic versioning:

```
vMAJOR
vMAJOR.MINOR
vMAJOR.MINOR.PATCH
```

Allowed changes:

| Change | Allowed |
|-------|---------|
| optional field addition | yes (minor version) |
| metadata extension | yes (minor version) |
| trace extension | yes (minor version) |

Breaking changes:

| Change | Requires |
|-------|----------|
| field removal | major version |
| type modification | major version |
| payload restructuring | major version |

Compatibility is enforced automatically by the schema registry.

---

## Feature Vector Contracts

Feature pipelines emit canonical feature snapshots:

```python
from event_schema_contracts.features.feature_vector import FeatureVectorEvent
```

Feature vectors include:

| Field | Purpose |
|------|---------|
| entity_id | dataset join key |
| feature_timestamp | feature computation time |
| feature_values | model inputs |
| feature_version | schema lineage |
| source_event_id | telemetry ancestry anchor |

Guarantees:

- deterministic dataset joins
- replay-safe feature reconstruction
- feature lineage traceability

---

## Repository Structure

```
event_schema_contracts/

├── base/
│   ├── base_event.py
│   ├── metadata.py
│   ├── identity.py
│   ├── time.py
│   └── trace.py
│
├── telemetry/
│   ├── device_event.py
│   ├── network_event.py
│   └── session_event.py
│
├── features/
│   └── feature_vector.py
│
├── validation/
│   └── validators.py
│
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

## Schema Lifecycle

Events move through deterministic processing stages:

```
creation → validation → enrichment → feature extraction → dataset export → inference
```

Each boundary enforces:

- schema identity correctness
- timestamp ordering validation
- compatibility guarantees
- lineage propagation integrity

---

## Validation Guarantees

Contracts enforce:

| Validation | Layer |
|-----------|------|
| UUIDv4 enforcement | identity mixin |
| UTC timestamp enforcement | time mixin |
| schema identity alignment | BaseEvent |
| trace lineage integrity | TraceContext |
| registry correctness | schema_registry |
| version compatibility | compatibility engine |

---

## Development Workflow

Run tests:

```bash
pytest
```

Tests validate:

- schema identity enforcement
- registry resolution correctness
- compatibility guarantees
- timestamp ordering rules
- payload type integrity

---

## Version Pinning Policy

All downstream services must pin schema versions:

```bash
pip install event-schema-contracts==0.1.0
```

This ensures:

- reproducible datasets
- deterministic replay
- safe schema rollout
- cross-service compatibility stability

---

## Schema Evolution Policy

Breaking changes require:

1. major version increment
2. migration documentation
3. replay validation
4. dataset regeneration verification

Backward compatibility is guaranteed within major versions.

See:

- docs/schema-versioning.md
- docs/compatibility-policy.md

---

## Platform Contract Guarantee

`event-schema-contracts` defines the compatibility boundary between platform services.

It guarantees:

- ingestion boundary correctness
- feature pipeline stability
- dataset reproducibility
- inference input contract integrity
- replay-safe schema evolution

All platform telemetry must conform to schemas defined in this repository.
