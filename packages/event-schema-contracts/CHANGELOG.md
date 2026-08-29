# Changelog

Consumers pin this library by git tag, so each entry here is what a component
gets when it pins that version. Every schema change belongs in this file,
because a version consumers cannot read about is one they cannot reason about.

Versions 0.3.0 through 0.6.0 were tagged by hand and are pinnable, but predate
this file, so their contents are recorded only in their commit history. They are
not listed. 0.7.0 is the first version published as a GitHub release and the
first with an entry here.

While the major version is 0, a breaking change bumps the minor. See
docs/compatibility-policy.md for what counts as breaking.

## 0.8.1 — 2026-08-29

### Fixed

- Ship a PEP 561 `py.typed` marker. Without it, a consumer running mypy against
  this library is told it has no type information, and every import from it
  fails as `import-not-found` — despite the library being `mypy --strict` clean
  itself. `telemetry-parser` hit this the moment it took its first dependency
  on this package. The marker is what makes the types usable by anyone other
  than this repository.

## 0.8.0 — 2026-08-29

### Added

- `event_schema_contracts.base.identity.derive(role, *parts)` and
  `derive_device_id(store_id, device_label)`, with the frozen project
  `NAMESPACE`. The UUIDv5 derivation that produces `sip.registration`'s
  `device_id` previously existed only as a helper inside this library's own
  test file, and as an independent implementation in `signal-forge`. Two
  definitions of the same derivation, with nothing comparing them: had either
  drifted, the same physical device would have resolved to two identities and
  every join across them would have silently split.

  The published version is byte-identical to both. `base/identity.py` already
  owned the UUID *version* policy — which fields may hold a derived id — so it
  now also owns how one is produced.

  The namespace, the separator and every role string are frozen; changing any
  re-bases every derived id in the system. Tests pin all three against
  hardcoded literals, because asserting the derivation against its own inputs
  would pass even if those inputs changed.

### Changed

- This library's `sip.registration` tests import `derive_device_id` rather than
  defining their own. The ids are unchanged.

### Notes for consumers

- `signal-forge` still carries its own `signal_forge.identity`. The two agree
  today; the durable fix is for it to import this one, which is a change to
  that repository and not yet made.

## 0.7.0 — 2026-08-25

First version cut by the release workflow, and the first with a changelog entry.
Earlier versions were tagged by hand.

### Removed

- `SipRegistrationPayload.latency_ms` and `SipRegistrationPayload.retry_count`.
  Neither can be correctly populated by `telemetry-parser`, which reads REGISTER
  requests only. `X-Latency` has no documented unit anywhere and a request cannot
  state its own round-trip time; `Retry-After` is a response header under
  RFC 3261 and never appears on a conforming request. Removed from v1 in place
  rather than cut as v2, because the schema had no producers, no consumers and
  no persisted events at the time — see ADR-002 Amendment 1.

### Added

- `sip.registration v1` (`SipRegistrationPayload`). Covers what
  `telemetry-parser` actually emits for a SIP REGISTER, which the previously
  registered `device.registration` did not — that identity belongs to device
  provisioning and has an incompatible schema. `device_id` is a UUIDv5 derived
  from `(store_id, device_label)`, with the label carried alongside because
  derivation is one-way. `RegistrationStatus` declares only `REGISTERED`: the
  parser never reads responses or the `Expires` header, so no producer can emit
  anything else. See ADR-002.

### Changed

- `DomainEventPayload` now sets `extra="forbid"`, library-wide. Unknown fields
  are rejected at construction rather than silently dropped, so a producer
  sending a field the schema does not carry fails loudly at the ingestion
  boundary instead of losing data quietly.

### Notes for consumers

`extra="forbid"` is the change most likely to break an existing producer. Any
component sending fields beyond a payload's declared set will now raise on
construction rather than having them dropped. That is the intent — silently
discarded fields are how a producer and a schema drift apart without either
side noticing — but it is a behavioural change worth checking against your own
output before pinning.
