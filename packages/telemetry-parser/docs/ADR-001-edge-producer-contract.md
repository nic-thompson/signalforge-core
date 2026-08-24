# ADR-001: The edge producer contract

- **Date:** 2026-08-24
- **Status:** Accepted

## Context

This parser consumes three non-standard SIP headers — `X-Latency`, `X-Session-Duration` and `X-Timestamp` — and, via `map_retry_count`, one standard header read in the wrong direction: `Retry-After`. None of the three is defined anywhere. No document in any repository states what they contain, in what unit, or what writes them.

That was not an oversight in documentation. **No edge producer has ever been specified.** The parser was written against fixtures that assumed a device annotating its own telemetry, and the fixtures were the only thing those headers ever came from.

The consequences surfaced while wiring the ingestion path against `event-schema-contracts`:

- `X-Latency` had no unit, and the `latency_ms` rename in that repo's ADR-002 asserted one rather than establishing it. Both that field and `retry_count` were subsequently removed from `sip.registration v1` (ADR-002 Amendment 1).
- `Retry-After` is a response header (RFC 3261). This parser reads REGISTER requests only, so it would never legitimately appear.
- `store_id` is required by `sip.registration v1` and exists nowhere in this parser — not in `SIPMessage`, not in `ExtractedEventFields`, not in `StructuredEvent`.
- `X-Timestamp` is the source of `observed_at`, and when absent the parser substitutes wall-clock (DEFECT-2). With nothing defining the header, that fallback is not an edge case — it is the only path, so the platform's replay-determinism property has no foundation at this layer.

There is no fleet to observe. The contract cannot be discovered; it has to be authored. This ADR authors it.

## Decision

**Devices emit standard RFC 3261 SIP. Nothing else.**

A commercial SIP handset sends a plain REGISTER. It does not annotate its own telemetry, does not know its round-trip latency, and does not carry vendor headers describing the observation being made of it. Specifying otherwise would describe hardware that does not exist and could not be bought.

Three consequences follow directly.

### 1. The parser reads only what SIP provides

`From`, `Call-ID`, `Via` and `CSeq`. Every non-standard header mapping is removed:

| Removed | Reason |
|---|---|
| `map_latency` (`X-Latency`) | invented header; the field it fed no longer exists in `sip.registration v1` |
| `map_session_duration` (`X-Session-Duration`) | invented header; no session exists at registration time |
| `map_retry_count` (`Retry-After`) | response header; unreachable on a request-only parser |
| `map_timestamp` (`X-Timestamp`) | invented header; superseded by capture time, below |

`ExtractedEventFields` loses `latency`, `retry_count` and `session_duration` accordingly. The repository gets smaller, which is the correct direction: most of what goes was written to consume a fiction.

### 2. Event-time comes from packet capture

The controller observing the traffic timestamps the packet. That observation time is `observed_at` — the only event-time source that exists once the device stops inventing one.

`TCPPacket.timestamp` already carries it. DEFECT-2 records that it is never threaded through to the normaliser. Under this contract that stops being an ambiguity and becomes the specified behaviour, so the defect is now a required fix rather than a discretionary one.

**Two questions this raises are not yet resolved and are deliberately left open here** (see Open questions):

- `TCPReassembler.process_packet` yields `bytes`, discarding the packet. A SIP message reassembled from several packets has several candidate timestamps and the parser currently keeps none of them.
- `_flush_buffers` emits events with no packet in scope at all.

### 3. Store identity comes from the controller, not the message

A headset registering to a local PBX is inside one store's network. There is exactly one store in scope, so a store identifier in the message would be redundant — real deployments do not put the building's address on an internal memo.

The component that knows the store is the on-site controller, provisioned with that identity at install time. The parser runs on that controller and takes `store_id` from its configuration.

This is also the stronger guarantee. In AWS IoT Core a device authenticates with a certificate, and an IoT policy can restrict a thing to publishing only beneath its own topic prefix. Store attribution derived from the topic is therefore certificate-attested and broker-enforced, whereas a self-declared header is a claim any misconfigured device could make incorrectly.

`store_id` becomes a required constructor argument to `ParserPipeline`. It is the first value this parser carries that it did not parse, and that is deliberate: it is environmental fact, not protocol content.

## Consequences

- Four header mappings and three `ExtractedEventFields` fields are deleted. `docs/event-mapping.md` documents exactly the mappings being removed and is rewritten in the same change.
- `observed_at` gains a real source. DEFECT-2's fix is now specified rather than discretionary.
- `ParserPipeline` gains a required `store_id`. Every existing construction site and test changes.
- `sip.unknown` is unaffected by this ADR and remains unresolved: `_map_event_type` emits it when `registration_status` is `None`, and no schema of that identity exists in `event-schema-contracts`. Tracked separately.
- A SIP traffic generator becomes buildable, producing plain RFC 3261 REGISTER streams and PCAP fixtures. It is smaller than a generator for annotated traffic would have been, and it is what the unbuilt packet-capture component will be tested against.
- The parser's test fixtures currently carry the invented headers. They are rewritten to plain SIP, which is what makes this ADR verifiable rather than aspirational.

## Open questions

**Which packet timestamps a multi-packet SIP message.** First packet of the message is the defensible answer — it is when the observation began, and it is stable under retransmission of later segments. Last packet is the alternative and would be when the message became complete. This needs deciding before the capture timestamp can be threaded through, because reassembly currently discards the information entirely.

**What timestamps a flushed message.** `_flush_buffers` runs at end-of-stream with no packet in scope. The candidates are the last timestamp seen on that session, or refusing to emit at all. Since these are messages that never completed cleanly, refusing may be more honest than assigning them a time they were not observed at.

**How `store_id` reaches the controller's configuration.** Greengrass thing name, an explicit config file, or the certificate CN are three different mechanisms with different failure modes. This ADR fixes that the value is environmental; it does not fix which environment supplies it.

## Note on provenance

This contract is authored, not observed. There is no device fleet, and no capture of real traffic exists to validate against. What makes it defensible is that it specifies *less* than the code it replaces: standard protocol only, no invented headers, identity from provisioning rather than self-declaration. Every previous assumption in this repository was a claim about hardware; this one is a claim about a protocol specification that can be checked against RFC 3261.
