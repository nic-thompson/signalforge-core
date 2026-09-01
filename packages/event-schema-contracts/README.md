# event-schema-contracts

**Canonical telemetry event schema contracts for the SignalForge telemetry intelligence platform**

`event-schema-contracts` defines the authoritative, versioned event schemas shared across every SignalForge service — parsers, the streaming analytics control plane, feature pipelines, dataset exporters, alert routing, and replay workflows. It is the schema authority layer: no service defines event schemas independently of this repository.

**Current version:** `v0.8.1`. Backward-compatible within the `v0` line; while the major version is 0, a breaking change bumps the minor (see the schema-evolution policy below). See [CHANGELOG.md](CHANGELOG.md) for what each version says.

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
pip install "event-schema-contracts @ git+https://github.com/nic-thompson/event-schema-contracts@v0.8.1"
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

Device, network, session and SIP registration events.

`SipRegistrationPayload` (`sip.registration`) is what `telemetry-parser` emits —
one observed SIP REGISTER from one in-store device. `device_id` is a UUIDv5
derived from `(store_id, device_label)`, with the label carried alongside because
derivation is one-way; a device label is unique only within a store, so the store
is part of the identity rather than an attribute of it.

`DeviceRegistrationPayload` (`device.registration`) is a different thing despite
the similar name: it describes device *provisioning* — `device_id`, `store_id`,
`device_type`, optional `firmware_version`, `registered_at` — and has an
incompatible field set. Nothing in the SIP path produces it. The two were confused
once, which is why `sip.registration` exists; see
[ADR-002](docs/ADR-002-sip-registration-schema.md).

In both, `store_id` is what lets downstream consumers project device→store
membership.

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

All canonical events share a structure. This is a real `model_dump(mode="json")`, not a sketch:

```json
{
  "event_id": "4a20e3e5-3951-4c3b-bb75-29316d0d56c7",
  "metadata": {
    "schema_version": "v1",
    "event_type": "device.registration",
    "source": "unknown"
  },
  "trace": {
    "trace_id": "b4b3f52c-cf8c-4cd4-bc7d-e9a81488ef97",
    "root_trace_id": "c659a6fd-b143-460d-a4cc-8d97a7f4cbbe",
    "pipeline_stage": "ingestion"
  },
  "event_timestamp": "2026-09-01T18:06:36.927809Z",
  "ingest_timestamp": "2026-09-01T18:06:36.928206Z",
  "payload": { }
}
```

**`schema_version` and `event_type` are inside `metadata`, not at the top level.**
This document showed them flat until September 2026, which is worth stating plainly
because the mistake is not obvious from the class definition either: they are
declared as `__event_type__` and `__schema_version__` ClassVars, which Pydantic
treats as class metadata rather than model fields, so they are absent from
`model_fields` while being present in every dump. A reader checking `model_fields`
and finding nothing concludes the envelope does not name its own schema. It does —
through `metadata`, populated at construction from those ClassVars.

That matters to any consumer reading the JSON: an event is identifiable from
`detail.metadata.event_type` without inspecting payload keys to guess.

`metadata.source` defaults to `"unknown"`, and that default satisfies the field's
own pattern. A producer that does not set it is therefore indistinguishable from
one that genuinely declared itself unknown. Producers should set it.

### What the envelope does and does not enforce

Enforced at construction:

- **Schema identity** — `__event_type__` and `__schema_version__` are required on
  every subclass, and a duplicate `(event_type, schema_version)` pair is rejected.
- **Timestamp ordering** — `event_timestamp` may not exceed `ingest_timestamp` by
  more than the permitted clock skew.
- **Payload typing** — `payload` is a `DomainEventPayload` subclass with
  `extra="forbid"`, so an unknown field fails rather than being silently dropped.

Not enforced here:

- **Ingestion-boundary validation** is a property of the producer, not the
  envelope. This library validates whatever is constructed from it; it cannot
  make a service construct one. `telemetry-parser` began doing so in August 2026,
  which is what made the guarantee real for the telemetry domain — before that,
  `signal_forge.streaming.event_protocol` documented a validation step that no
  component performed.
- **Replay safety** is permitted rather than guaranteed. The schemas allow
  derived UUIDv5 identifiers and carry observation time, which is what makes a
  reproducible replay possible; whether any given producer derives its ids
  deterministically is that producer's decision.

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
│   ├── session_event.py
│   └── sip_registration_event.py
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
├── ADR-002-sip-registration-schema.md
├── base-model-conventions.md
├── compatibility-policy.md
├── event-lifecycle.md
└── schema-versioning.md
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
