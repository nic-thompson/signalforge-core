# Event Lifecycle

This document defines the lifecycle of canonical telemetry and feature events across the ML platform.
Events move through deterministic processing stages that preserve traceability, reproducibility, and schema integrity.

---

## Stage 1: Event Creation

Events originate from:

- ingestion services 
- device agents
- API gateways
- telemetry collectors
- feature builders

Each event must include:

schema version
event_id
trace_id
event_timestamp
ingest_timestamp
event_type
source
payload

---

## Stage 2: Validation Boundry

Validation workers enforce:

- schema structure correcetness
- timestamp ordering
- UUID validity
- payload type integrity
- compatability constraints

Invalid events must never cross the validation boundry.

---

## Stage 3: Enrichment

Optional enrichement pipelines may attach:

- derived attributes
- geo resolution
- device classification
- session linkage 
- aggregation context

Enrichment must not mutate original payload fields.

Only extensions are allowed.

---

## Stage 4: Feature Extraction

Feature builders convert telemetry events into feature vectors.

Transformation includes:

entity mapping
window aggregation
signal normalaisation
feature version tagging

Output:

feature.vector events

These form the training dataset boundary.

---

## Stage 5: Dataset Export

Dataset exporters assemble feature vectors into:

training datasets
evaluation datasets 
replay datasets
monitoring snapshots

Export pipelines rely on schema version determinism.

---

## Stage 6: Inference Consumption

Inference service consume feature.vector events for:

online prediction
shadow evaluation
model comparison
drift detection

Trace metadata enables prediction lineage tracking.