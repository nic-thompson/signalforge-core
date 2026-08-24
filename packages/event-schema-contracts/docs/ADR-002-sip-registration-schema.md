# ADR-002: A dedicated schema for SIP registration telemetry

- **Date:** 2026-08-22
- **Status:** Accepted

## Context

`telemetry-parser` emits events with `event_type = "device.registration"` — the identity already registered by `DeviceRegistrationEvent`. The two were assumed to correspond. They do not.

`DeviceRegistrationPayload` describes device **provisioning**: a device joining the fleet, carrying `device_type`, `firmware_version`, and a UUID `device_id`. The parser produces **operational SIP telemetry**: registration status, latency, retry count, and a `device_id` taken from the SIP `From` header, which is a human-assigned label such as `headset-0001` — not a UUID.

The field sets are incompatible in both directions:

| Parser produces | `DeviceRegistrationPayload` requires |
|---|---|
| `device_id` — a label string | `device_id` — UUID, enforced |
| — | `device_type`, `firmware_version` |
| `latency`, `retry_count`, `session_duration`, `call_id`, `source_ip`, `transport_protocol`, `registration_status` | no field accepts any of these |

`NetworkConnectionPayload` and `SessionStartPayload` were both evaluated and rejected: the former mandates `destination_ip`, `connection_id` and `direction`, none of which the parser extracts, and has no home for `retry_count` or `registration_status`; the latter is web/mobile session analytics (`platform: ios|android|web`, `experiment_id`), an unrelated domain.

The consequence is that the parser's output has never had a valid target schema, which is why no ingestion path was ever wired up.

## Decision

Add `sip.registration v1` as a distinct schema.

**Identity.** `device_id` is a UUIDv5 derived from `(store_id, device_label)`, opted into the library's existing `__uuid_v4_or_v5_fields__` policy. Deriving rather than minting keeps the same physical device resolving to the same id on every run, which the platform's replay-determinism guarantee requires and `uuid4` would break. v4 remains permitted so a future producer with a genuinely random device id is not excluded.

**`device_label` is carried alongside the derived id.** UUIDv5 derivation is one-way: without the label, the original is unrecoverable from the event, and an operator holding a physical headset has no route back to its telemetry. It is diagnostic only and never a join key, being unique only within a store.

Deriving from `store_id` means a device moving between stores acquires a new identity. This is correct for store-fixed devices, which is the current deployment model. It would be wrong for pooled hardware circulating between sites; such a fleet would need a derivation keyed on something intrinsic to the device.

**Three fields are renamed from the parser's internal names**, each because the original invites a specific error:

- `latency` → `latency_ms`. The parser's field is a bare number. `NetworkConnectionPayload` already establishes `latency_ms`. Unnamed units are how a factor-of-1000 error survives review.
- `call_id` → `registration_call_id`. On a REGISTER, SIP `Call-ID` identifies the registration transaction, not a voice call. The short name reads as call telemetry, which this is not.
- `session_duration` dropped. No session exists at registration time; the field carries no meaning here.

**Required fields are `device_id`, `device_label`, `store_id`, `registration_status` and `observed_at`.** Every field the parser produces is individually optional, so a payload requiring nothing would validate anything and detect nothing. The required set is exactly what a detector cannot function without; an event missing any of them is unusable and should be rejected at ingestion rather than passed downstream.

**`RegistrationStatus` declares only `REGISTERED`.** It is the only value the parser can currently produce: `map_registration_status` tests whether `CSeq` contains `REGISTER`, and the parser never reads responses or the `Expires` header (verified — no reference to `Expires` exists anywhere in the package). A clean deregistration (`Expires: 0`) and a failed or challenged registration (401/403) are both operationally significant and both deliberately absent, because declaring values no producer can emit describes a parser that does not exist and invites dead branches in consumers.

**`SipTransportProtocol` is declared locally** rather than reusing `network_event.TransportProtocol`, despite identical members. Each schema is independently versioned; importing another schema's enum would let a change made for `network.connection` silently alter this contract.

## Consequential change: payload strictness

Writing the tests surfaced a separate defect. `extra="forbid"` was set on `BaseEvent`, `EventMetadata` and `TraceContext`, but not on `DomainEventPayload`, which inherited pydantic's default `extra="ignore"`. Every payload in the library therefore **accepted and silently discarded** unknown fields — demonstrated against `DeviceRegistrationPayload`, which happily consumed an invented field and dropped it.

This directly contradicts `BaseEvent`'s own documented rationale: *"an unrecognised field is a producer/consumer contract mismatch, not something to silently drop and lose on replay."* The envelope enforced that; the payloads did not.

`extra="forbid"` is now set on `DomainEventPayload`. Silent loss is the worst available failure here — the event validates, the field never reaches storage, and replay cannot recover what was never persisted.

## Consequences

- The parser's output has a valid target schema, unblocking the ingestion path.
- `device.registration` and `sip.registration` are now unambiguously separate identities. The parser must be updated to emit the new `event_type`; until then it emits an identity whose registered schema rejects its payload.
- All payloads across the library now reject unknown fields. The full suite (224 existing tests plus 37 new) passes unchanged, and nothing currently produces events, so this correction is free today and would not be later.
- `signal-forge` is unaffected: its `TelemetryEvent` protocol declares `payload: Any` and its detectors take a `device_id_extractor` callable, so payload shape is not constrained downstream.
- Adding `RegistrationStatus` values later remains possible without a major bump, but consumers must treat an unrecognised value as "not currently registered" rather than matching exhaustively.

## A related finding, not addressed here

`signal_forge.streaming.event_protocol` states that events reaching the streaming layer have *"already been validated by `telemetry-parser` against the schemas in `event-schema-contracts`"*, and skips re-validation on that basis. This is untrue: `telemetry-parser` does not import `event-schema-contracts` at all. No validation boundary currently exists anywhere in the pipeline.

The planned ingestion Lambda should become that boundary, which makes the claim true rather than aspirational. Until it exists, `signal-forge`'s decision to skip validation rests on a guarantee nothing provides.
