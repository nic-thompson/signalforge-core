# Replay Strategy

## Overview

Replay mode enables deterministic regeneration of structured telemetry datasets from recorded packet streams.

This supports:

offline training dataset reconstruction
feature-store backfills
pipeline regression testing
schema migration validation
telemetry drift detection

---

## Replay Guarantees

Replay mode preserves:

packet ordering
session boundaries
event timestamps
message framing
protocol extraction semantics

This ensures training datasets remain reproducible across pipeline versions.

---

## Replay Execution Model

Replay input:

Iterable[TCPPacket]

Processing model:

sequential deterministic iteration

No concurrency is introduced during replay execution.

This guarantees stable ordering across executions.

---

## Timestamp Preservation

Event timestamps are derived from:

X-Timestamp header

Fallback:

packet timestamp

Fallback:

ingestion timestamp

Replay mode prioritises original event timestamps whenever available.

---

## Buffer Flush Safety

Replay pipelines explicitly flush decoder buffers at stream termination.

This prevents silent message loss at dataset boundaries.

---

## Dataset Regeneration Workflow

Example usage:

pipeline = ParserPipeline(replay_mode=True)

for event in pipeline.parse_stream(packet_stream):
    write(event)

Output datasets are reproducible across executions given identical input streams.