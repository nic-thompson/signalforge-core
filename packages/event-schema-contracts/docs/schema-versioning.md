# Schema Versioning Strategy

This repositotry defines canonical telemetry and feature contracts shares across ingestion services, validation workers, enrichment pipelines, feature builders, dataset exporters, and inference APIs. Schema versioning ensures safe evolution while preserving dataset reproducibility and replay compatibility.

---

## Version Format

v<major>
v<major>.<minor>
v<major>.<minor>.<patch>

Examples:

v1
v1.1
v1.2.3

---

## Major Versions

Major versions introduce breaking changes.

Examples:

- field removal
- field rename
- type mutation
- payload restructuring
- required field additions

Major upgrades require:

- migration adapters
- dataset regression validation
- replay verification

Example:

device.regression v1 -> v2

NOT backward compatible.

---

## Minor Versions

Minor Versions introduce backward-compatible extensions.

Allowed:

- optional field additions
- metadata extensions
- trace extensions
- payload annotations

Example:

v1 -> v1.1

Backward compatible.

Consumers using v1 remain valid.

---

## Patch Versions

Patch versions support:

- validation tightening
- documentation alignment
- metadata corrections
- non-breaking constraints

Example:

v1.1 -> v1.1.1

Always safe.

---

## Forward Compatibility

Schemas support forward compatibility within the same major version.

Example:

Consumer expects v1
Producer emits v1.2

Allowed.

Registry resolves compatible schema automatically.

---

## Backward Compatibility

Backward compatibility is guaranteed within the same major version when:

- fields are not removed
- types are not modified
- required remain unchanged

Example:

Producer emits v1
Consumer emits v1.2

Allowed through registry fallback.

---

## Breaking Change Policy

Breaking changes require:

1. major version increment
2. migration documentation
3. replay validation
4. dataset verification

Breaking changes must never be introduced silently.