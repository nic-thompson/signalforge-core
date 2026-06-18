# telemetry-parser

Structured telemetry extraction engine for the **SignalForge** telemetry intelligence platform. It sits at the ingestion boundary, converting raw TCP packet streams into schema-versioned `StructuredEvent`s that the downstream analytics control plane (`signal-forge`) consumes.

## What it does

`telemetry-parser` reconstructs application-level messages from a raw packet stream and extracts them into typed, structured telemetry events. The pipeline runs in six stages, each a single-purpose package:

1. **Stream reassembly** (`stream/`) — `TCPReassembler` reconstructs an ordered byte stream from TCP packets; `session_tracker` tracks per-connection state.
2. **Protocol decoding** (`protocol/`) — `MessageDecoder` frames the reassembled bytes into discrete messages; `SIPParser` parses SIP-style protocol messages.
3. **Extraction** (`extraction/`) — `EventExtractor` turns a parsed message into an extracted event; `field_mapper` maps protocol fields (registration status, latency, retry count, session duration) to semantic ones. SIP `REGISTER` is the supported method today; other methods raise `UnsupportedProtocolEvent` and are skipped rather than failing the stream.
4. **Normalisation** (`normalisation/`) — `EventNormaliser` assigns identity, timestamps, and event type, producing a canonical shape. `timestamp_utils` handles timestamp coercion.
5. **Emission** (`output/`) — `EventEmitter` finalises each event; `StructuredEvent` is the output contract.
6. **Observability** (`observability/`) — `ParserObserver` is threaded through every stage as an optional instrumentation hook.

The `pipeline/` package composes these into `ParserPipeline`.

## Usage

```python
from telemetry_parser.pipeline.parser_pipeline import ParserPipeline

pipeline = ParserPipeline()

for event in pipeline.parse_stream(packets, trace_id=trace_id):
    # event is a StructuredEvent
    handle(event)
```

`parse_stream(packets, trace_id=None)` is a generator: it consumes an iterable of `TCPPacket`s and yields `StructuredEvent`s incrementally, so a stream is processed without buffering the whole input. Buffer flushing is deterministic at end-of-stream.

## Output contract

Each `StructuredEvent` carries the envelope the analytics layer expects:

| Field             | Purpose                                  |
| ----------------- | ---------------------------------------- |
| `schema_version`  | schema identity                          |
| `event_id`        | per-event identifier                     |
| `trace_id`        | lineage anchor for cross-service tracing |
| `event_timestamp` | event time                               |
| `ingest_timestamp`| ingestion time                           |
| `event_type`      | dotted-lowercase type discriminator      |
| `source`          | emitting source                          |
| `payload`         | extracted, normalised fields             |

## Replay determinism

Replay determinism is **opt-in**, controlled by two `ParserPipeline` constructor flags:

- `replay_mode` — reuses the upstream `ingest_timestamp` and `trace_id` carried on the extracted event rather than generating new ones, so a re-parse of the same input reproduces the same timestamps and trace lineage.
- `preserve_event_ids` — reuses upstream `event_id`s rather than minting new ones, for dataset backfills where event identity must be stable across runs.

By default (both flags `False`), the normaliser mints fresh `uuid4()` identifiers and uses processing-time values — the live-ingestion path. Deterministic replay is the configured path, not the default, so a replay driver constructs the pipeline with the flags set. This mirrors the determinism discipline the rest of the SignalForge platform follows: reproducibility is an explicit mode, and the components that need it configure it deliberately.

## Role in the SignalForge platform

`telemetry-parser` is the ingestion-boundary component between raw telemetry signals and the analytics control plane. It produces the `StructuredEvent`s that `signal-forge` routes, aggregates, and turns into detections, features, datasets, alerts, and dashboard projections. Event schemas themselves are owned upstream by `event-schema-contracts`; this repository extracts raw signals into that contract's shape.

## Development

```
pip install -e ".[dev]"
pytest
```
