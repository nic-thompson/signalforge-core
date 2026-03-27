# Compatibility Policy

This document defines schema compatibility guarantees across versions.
Compatibility enforcement ensures ingestion stability, replay correctness, and dataset reproducability.

---

## Compatability Guarantees

Within the same version:

forward compatability is guaranteed
backward compatibility is guaranteed

Across major versions:

compatibility is not guaranteed

Migration is required.

---

## Allowed Changes (Minor Versions)

The following changes are permitted:

optional field addition
metadata extension
trace extention
new feature annotations

These changes must not break existing consumers.

Example:

v1 -> v1.1

Allowed.

---

## Disallowed Changes (Minor Versions)

The following changes require a major version bump:

field removal
field rename
type mutation
required field introduction
payload restructuring

Example:

v1 -> v1.1 removing device_id

NOT ALLOWED

---

Deprication Lifecycle

Field deprication follows a structured lifecyle:

Phase 1: Introduce replacement field
Phase 2: Mark legacy field deprecated
Phase 3: Maintain compatability window
Phase 4: Remove in major version

Example: 

device_model -> hardware_model

device_model marked deprecated in v1.2
removed in v2

---

## Replay Compatibility Requirement

Schema evolution must preserve replay capability.

This guarantees:

historical dataset regeneration
training reproducibility
audit traceability
model rollback support 

Breaking replay compatibility is prohibited without a major version

---

## Registry Enforcement

Compatibility is enforced automatically through:

SchemaRegistry
compatibility.ensure_compatibility()

Invalid upgrades are rejected at validation time.
