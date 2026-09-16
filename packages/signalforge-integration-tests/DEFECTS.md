# Defects found by the edge-chain integration test

Surfaced, **not fixed** — this work was scoped to building the seam
test, not to changing `telemetry-parser`. Each is captured by an
`xfail(strict=True)` test in `test_edge_chain_integration.py`, so they
will flip to unexpected-pass the moment they're fixed rather than being
quietly forgotten.

All three defects live in `telemetry_parser/normalisation/event_normaliser.py`
and all three undermine **replay determinism** — the property SignalForge
is architecturally built around.

---

## DEFECT-1 — `preserve_event_ids=True` has no effect

```python
event_id = (
    extracted.event_id
    if self.preserve_event_ids and hasattr(extracted, "event_id")
    else str(uuid.uuid4())
)
```

`extracted` is an `ExtractedEventFields` frozen dataclass. Its fields are:

```
device_id, registration_status, latency, retry_count,
transport_protocol, session_duration, call_id, source_ip,
event_timestamp
```

There is **no `event_id` field**, so `hasattr(extracted, "event_id")` is
always `False` and the expression always falls through to
`str(uuid.uuid4())`. The `preserve_event_ids` flag — plumbed all the way
through `ParserPipeline.__init__` and documented as *"Prevents
regeneration of event identifiers during dataset backfills"* — cannot
ever take effect.

**Impact:** every replay produces new random event IDs. Any downstream
deduplication or lineage tracking keyed on `event_id` sees a replayed
event as a brand-new event.

**Verified empirically:** two runs over identical input with
`preserve_event_ids=True` produced entirely different event IDs.

---

## DEFECT-2 — Missing `X-Timestamp` falls back to wall-clock time

`TimestampUtils.normalise_event_timestamp` has a three-stage fallback:
extracted timestamp → supplied fallback → `datetime.now(timezone.utc)`.

`ParserPipeline` never passes the middle argument, so a SIP `REGISTER`
without an `X-Timestamp` header is stamped with the wall-clock time *at
parse time*.

**Impact:** the same event replayed a day later gets a different
`event_timestamp`. Because `signal-forge`'s `WatermarkManager` derives
watermarks purely from event timestamps (deliberately, to keep replay
deterministic), a shifted `event_timestamp` shifts the entire watermark
trajectory — changing which events are classified `ON_TIME` vs
`LATE_DROPPED`, which changes window contents, which changes the
exported dataset. The determinism guarantee `signal-forge` carefully
maintains is broken upstream of it.

**Worth noting:** the fix is readily available — `TCPPacket` already
carries a `timestamp` field (the packet's capture time), which is
exactly the right deterministic fallback. It is simply never threaded
through to the normaliser.

---

## DEFECT-3 — `replay_mode=True` has no effect on `ingest_timestamp` or `trace_id`

Same root cause as DEFECT-1 — `hasattr()` guards against attributes
that `ExtractedEventFields` does not have:

```python
ingest_timestamp = (
    extracted.ingest_timestamp
    if self.replay_mode and hasattr(extracted, "ingest_timestamp")
    else TimestampUtils.ingest_timestamp()      # always taken
)
```

and, when no `trace_id` is passed to `parse_stream()`:

```python
extracted.trace_id
if self.replay_mode and hasattr(extracted, "trace_id")
else str(uuid.uuid4())                          # always taken
```

**Impact:** `replay_mode` is effectively a no-op flag throughout
`telemetry-parser`. This is arguably the most serious of the three,
because the flag's *existence* signals that replay determinism was
considered and handled at this layer, when in fact nothing behind it
works.

---

## Common root cause

All three are the same mistake: a `hasattr()` guard used as a type
check against a `@dataclass` whose fields are known statically. Because
`hasattr()` silently returns `False` rather than raising, the dead
branches never announce themselves — no test failed, no type error was
raised, and `mypy --strict` would not flag it either, since the
expression is well-typed.

A structural fix would be to add the three fields
(`event_id`, `ingest_timestamp`, `trace_id`) to `ExtractedEventFields` as
optional, populated on the replay path — at which point the existing
guards would start working as written. That is a design decision for
whoever owns `telemetry-parser`, not something to change from inside a
test suite.

---

## Why the integration test found this and 384 unit tests did not

Each component's unit tests verify that component against its own
contract. Nothing tested the *chain* running the same input twice and
comparing. Replay determinism is a property of the whole pipeline, so
it is invisible at the unit level by construction — which is precisely
the argument for building this seam test before building more isolated
components.
