# telemetry-parser

Structured telemetry extraction for the **SignalForge** telemetry platform. It sits at the ingestion boundary, turning raw TCP packet streams into validated `sip.registration` events from [`event-schema-contracts`](https://github.com/nic-thompson/event-schema-contracts).

## What it does

`telemetry-parser` reconstructs application-level messages from a raw packet stream and extracts them into typed, validated telemetry events. The pipeline runs in six stages, each a single-purpose package:

1. **Stream reassembly** (`stream/`) — `TCPReassembler` reconstructs an ordered byte stream from TCP packets; `session_tracker` tracks per-connection state. Each chunk carries the observation time of the packet that supplied its first byte.
2. **Protocol decoding** (`protocol/`) — `MessageDecoder` frames the reassembled bytes into discrete messages; `SIPParser` parses them. Framing does not align with packet boundaries, so the decoder tracks which packet supplied each byte in order to date a message correctly.
3. **Extraction** (`extraction/`) — `EventExtractor` turns a parsed message into extracted fields. SIP `REGISTER` is the only supported method; other methods are filtered out, which is not a failure — a capture carries whatever traffic is on the wire.
4. **Normalisation** (`normalisation/`) — `EventNormaliser` builds a validated `SipRegistrationEvent`, converting protocol values into the schema's types and deriving the device identity.
5. **Emission** (`output/`) — `EventEmitter` finalises each event.
6. **Observability** (`observability/`) — `ParserObserver` is threaded through every stage as an optional instrumentation hook.

The `pipeline/` package composes these into `ParserPipeline`.

## Usage

```python
from telemetry_parser.pipeline.parser_pipeline import ParserPipeline

pipeline = ParserPipeline(store_id="store-0042")

for event in pipeline.parse_stream(packets):
    # event is a SipRegistrationEvent, already validated
    handle(event)
```

`parse_stream(packets, trace_id=None)` is a generator: it consumes an iterable of `TCPPacket`s and yields events incrementally, so a stream is processed without buffering the whole input.

### Store identity is required

`store_id` comes from the controller's provisioned configuration and is the only value this parser emits that it did not parse. Nothing in a SIP `REGISTER` names a store — a headset registering to a local PBX is inside one store's network, so there is exactly one store in scope and the protocol has no reason to say which.

It is load-bearing beyond its own field. Each device's stable identity is derived from `(store_id, device_label)`, because a device label is unique only within a store: two stores may each have a `headset-12`, and they are different devices. A controller configured with the wrong store silently re-identifies every device it observes, and nothing downstream can detect it — the traffic never made a claim to contradict.

A missing or malformed store identity is rejected when the pipeline is constructed, against the schema's own grammar, so a misconfigured controller fails at startup rather than emitting a stream rejected one event at a time.

## Output

`parse_stream` yields `SipRegistrationEvent` — the schema from `event-schema-contracts`, not a locally-defined shape. That library owns the event schemas, so constructing its types here means the schema validates this parser's output at the moment it is produced.

The payload is a `SipRegistrationPayload` rather than a dictionary, which makes several conversions explicit that a dictionary left implicit:

| Protocol source | Payload field | Conversion |
| --- | --- | --- |
| — | `device_id` | derived UUIDv5 over `(store_id, device_label)` |
| `From` user part | `device_label` | carried as a label, never as an identifier |
| configuration | `store_id` | supplied, not parsed |
| `CSeq` | `registration_status` | uppercased into `RegistrationStatus` |
| packet capture | `observed_at` | observation time |
| `Via` | `transport_protocol` | raw token into `SipTransportProtocol` |
| `Via` | `source_ip` | parsed into an IP address |
| `Call-ID` | `registration_call_id` | renamed: on a REGISTER this identifies the registration transaction, not a voice call |

The envelope names its own schema in `metadata`, so a serialised event is self-describing and a publisher need add nothing to it.

## Event time comes from packet capture

Every event is dated by the packet carrying its message's first byte. There is no fallback.

A later segment arriving, or a retransmission, does not move the event; a segment held behind a sequence gap keeps the time it arrived rather than the time the gap filled. Parsing the same capture twice therefore produces identical event times, which is what makes a replay reproduce the run it is meant to reproduce.

This was not always true. Event time previously fell back to ingestion wall-clock whenever a message carried no timestamp of its own, which made two parses of the same capture differ. Two constructor flags, `replay_mode` and `preserve_event_ids`, appeared to control this and did nothing at all — both guarded on fields the extracted event has never had. Determinism is not a mode to opt into; it is a property of dating events by when they were observed.

## What it refuses

Malformed input is dropped and reported to the observer rather than emitted:

- A `REGISTER` whose `CSeq` is absent or names another method contradicts its own request line. This once produced an event typed `sip.unknown` — an identity no schema was ever registered for, carrying a null registration status into a payload that requires one, so it could never have validated anywhere.
- A `REGISTER` whose `From` header carries no user part names no device, and device identity is derived from that label.
- At end-of-stream, a message with no header terminator is incomplete, and reassembly segments stranded behind a gap that never filled are not contiguous. Joining them would fabricate a stream that was never observed.

Non-`REGISTER` methods are filtered silently, which is different: a capture carries whatever SIP traffic is on the wire, and an `INVITE` is not malformed for being undescribed here.

## Devices speak plain RFC 3261

The parser reads `From`, `Call-ID`, `Via` and `CSeq`. Nothing else.

It once read `X-Latency`, `X-Session-Duration`, `X-Timestamp` and `Retry-After`. The first three are non-standard headers that no document defined and no component emits; the fourth is a response header that a conforming request never carries. They were consumed on the assumption that devices annotate their own telemetry, which devices do not do.

See [ADR-001](docs/ADR-001-edge-producer-contract.md) for the contract, and [`docs/event-mapping.md`](docs/event-mapping.md) for the field-by-field mapping.

## Role in the SignalForge platform

`telemetry-parser` is the ingestion-boundary component between raw telemetry signals and the analytics control plane. Event schemas are owned upstream by `event-schema-contracts`; this repository extracts raw signals into that contract's types and validates them at the boundary.

## Development

```
pip install -e ".[dev]"
pytest
mypy telemetry_parser
```
