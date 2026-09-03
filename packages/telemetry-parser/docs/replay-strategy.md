# Replay Strategy

## Overview

Replay is the deterministic regeneration of telemetry events from a
recorded packet stream: the same packets, parsed again, producing the
same events.

This supports:

offline training dataset reconstruction
feature-store backfills
pipeline regression testing
schema migration validation
telemetry drift detection

---

## Replay is not a mode

There is no replay flag. Parsing is deterministic, and a second parse of
the same capture reproduces the first.

This document previously described replay as opt-in, controlled by
`replay_mode` and `preserve_event_ids` constructor arguments. Neither did
anything: both guarded on fields that `ExtractedEventFields` has never
carried, so every guard was permanently false. They were removed rather
than implemented — a flag named `replay_mode` that silently does nothing
is worse than no flag, particularly on a pipeline whose determinism
story it appears to support.

---

## What is deterministic

Given identical input packets, a re-parse reproduces:

packet ordering
session boundaries
message framing
event timestamps
observed_at
device identities
protocol extraction semantics

`event_id` is derived rather than generated, under the role
`event.sip_registration`, from the event's own coordinates: the device
identity, the observation time, and the registration Call-ID. All three
are properties of the observed traffic rather than of the run, so the
same capture yields the same ids on every parse.

It was `uuid4()` until 2026-09-02. A replay therefore reproduced an
event's content but never its identity — and identity is what a consumer
deduplicates and joins on, so two parses of one capture produced two
populations of events indistinguishable in every field except the one
meant to distinguish them.

One thing remains **not** reproducible: `trace_id`, when no trace is
supplied to `parse_stream`. It is `uuid4()` in that case, and arguably
should be — a trace correlates one processing run, and two runs are two
runs. A caller wanting a byte-identical envelope supplies the trace id,
which is what makes it a parameter. `ingest_timestamp` is wall-clock for
the same reason: it records when this parse happened, not when the
traffic was observed.

---

## Event time

Every event is dated by the packet that carried its message's first byte.
There is no fallback.

A message spanning several packets takes the first packet's time
throughout, so a later segment arriving — or a retransmission — does not
move it. A segment held behind a sequence gap keeps the time it arrived
rather than the time the gap filled, so a dropped packet does not shift
observation times forward.

This document previously described a fallback chain: `X-Timestamp`
header, then packet timestamp, then ingestion timestamp. `X-Timestamp` is
a non-standard header that nothing defines and nothing emits, and the
ingestion fallback meant that parsing the same capture twice produced
different events. Determinism was claimed while the mechanism made it
impossible.

---

## Execution model

Replay input:

Iterable[TCPPacket]

Processing model:

sequential deterministic iteration

No concurrency is introduced. This guarantees stable ordering across
executions.

---

## Incomplete captures are refused, not flushed

At end-of-stream the pipeline discards whatever remains buffered and
reports it to the observer. It emits nothing from it.

Two things can remain. A decoder buffer with no header terminator is an
incomplete message. Reassembly segments stranded behind a gap that never
filled are not contiguous, and joining them fabricates a stream that was
never observed.

This document previously described flushing those buffers as preventing
"silent message loss at dataset boundaries". It did the opposite. The
events it produced were fabricated from bytes that were never a message,
and were indistinguishable downstream from events parsed cleanly — which
is a worse failure than dropping them, because it is invisible. The
counts are reported instead, so a lossy capture is visible as a lossy
capture.

---

## Dataset regeneration

```python
pipeline = ParserPipeline(store_id="store-0042")

for event in pipeline.parse_stream(packet_stream):
    write(event)
```

Output datasets are reproducible across executions given identical input
streams, with the `trace_id` and `ingest_timestamp` caveat above.
