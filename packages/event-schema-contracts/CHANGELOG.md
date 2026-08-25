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
