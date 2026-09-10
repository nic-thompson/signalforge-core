# SignalForge — Architecture, Gaps, and End-to-End Test Strategy

**Status of this document.** Everything under "What is built" was verified by reading the source of all repositories directly. Everything under "Gaps" was verified as absent — searched for and not found, not merely assumed missing. Gap statuses were revised on 2026-09-03; several are now closed.

Section 1.1 was added on 2026-09-03 from product knowledge that had not previously been written down anywhere. It changes what several existing components are understood to be measuring, so it is worth reading before the sections that follow.

---

## 1. What SignalForge is

A telemetry pipeline that turns SIP registration signals observed on in-store controllers into real-time operational awareness and reproducible datasets, across an estate of sites.

Note the wording. This document previously said "from an edge device fleet", which implied the devices themselves speak SIP. They do not — see 1.1.

The defining engineering property is **replay determinism**: any historical window can be re-run through the same pipeline and produce byte-identical output. This is not a bolt-on feature — immutability (`frozen=True`), strict validation (`extra="forbid"`), epoch-aligned windows, UUIDv5 identity derivation, and wall-clock-free watermarks all exist to make that guarantee hold.

Architecturally this is **Kappa**, not Lambda: one processing pipeline serves both live traffic and reprocessing, with an immutable event archive as the source of truth. There is no separate batch codebase computing a parallel "correct" view — `run_replay` uses the same pipeline builder as production, differing only in settings.

---

## 1.1 The physical system

SignalForge is the telemetry-and-analytics half of the cloud tier of a three-tier retail communication product. Understanding the other two tiers matters, because it determines what the events actually describe.

### The edge — headsets and base stations

Staff wear lightweight wireless headsets for hands-free group audio. These run on **DECT**, a localised radio protocol, deliberately not on store Wi-Fi or cellular. Base stations across the site form a multi-cell radio network, and audio switching, channel mixing and handover between cells all happen locally. The product's defining property is that communication keeps working when the internet does not.

**Headsets do not speak SIP.** This is the single most important correction in this document: nothing a headset does produces a SIP REGISTER.

### On-premises — the Controller

Each site has a hardware Controller managing up to a thousand wireless endpoints. It orchestrates message routing between shelf sensors, customer call points and cash registers, and — relevant here — **bridges local audio into external SIP/VoIP telephone systems**.

That bridge is where SIP lives. A `sip.registration` event is the Controller's PBX bridge registering, not a headset checking in. That is a real thing worth monitoring: if the bridge drops, staff lose landline calls while headset-to-headset audio carries on working, so the failure is invisible from inside the store until someone tries to answer a phone.

`telemetry-parser` and `greengrass-publisher` run on the Controller. That is why `store_id` comes from the Controller's provisioned configuration rather than from the traffic — the Controller *is* the store, as far as this system is concerned.

**One Controller per store.** Recorded because the entire identity model rests on it: `store_id` is both the site and the publisher, it is the partition key for every window, and device identities derive from it. A site needing two Controllers would break that, and nothing in any repository currently states the assumption.

### Central — the cloud tier

Cloud infrastructure across the estate handles over-the-air firmware upgrades, configuration distribution, telemetry and analytics.

**SignalForge is the telemetry and analytics part only.** Firmware push, configuration distribution and permissions management are the downward path, cloud to edge, and are out of scope — not a gap, not ours.

### What this makes the analytics

The cloud tier sees what no single store can. Device liveness within a site is the Controller's job and is handled locally; a headset dropping out of DECT range does not need a round trip to AWS to be noticed.

What needs the estate-wide view is comparison:

- which sites have degrading Controller-to-PBX bridges, before staff report it
- response times to customer call points, ranked by site — a regional manager's metric
- alert volume by type and site: shelf-sensor triggers clustering somewhere may be theft or a miscalibrated sensor, and only the cross-site view distinguishes them
- fleet health at scale: firmware versions in the field, battery-swap frequency, headsets failing early

This is why the roadmap places customer call points as the next producer. Response time to a fitting-room call, by site, is an operational KPI. Device liveness is infrastructure monitoring.

---

## 2. Repositories

| Repo | Role | Deployment |
|---|---|---|
| `event-schema-contracts` | Versioned Pydantic event contracts — base envelope, schema registry, domain payloads (telemetry, detection, features, alerts) | Central (AWS) |
| `structured-logging-python` | Structured JSON logging and trace propagation, shared across services | Both edge and central |
| `telemetry-parser` | TCP reassembly, SIP protocol parsing, field extraction, normalisation → `StructuredEvent` | Edge (controller) |
| `greengrass-publisher` | Publishes `StructuredEvent` to IoT Core over Greengrass IPC | Edge (controller) |
| `aws-event-pipeline-infra` | Terraform: EventBridge bus, per-stage SQS queues, archive, replay/export Step Functions, audit tables | Central (AWS) |
| `signal-forge` | Streaming (watermarks, windowing), detection, alerts, dashboard projections, dataset export, replay driver | Central (AWS) |

---

## 3. The intended flow, device to dataset

```
[EDGE — per site, on the controller]
  network interface
       ↓  raw packets
  capture component                          ← GAP 1 (not built)
       ↓  TCPPacket
  telemetry-parser (ParserPipeline)
       ↓  StructuredEvent
  greengrass-publisher
       ↓  MQTT (Greengrass IPC → IoT Core), topic: edge/{store_id}/telemetry

[CENTRAL — AWS]
  AWS IoT Core
       ↓  iot_core_telemetry_rule                ← BUILT this session, not yet applied
  EventBridge bus (${name_prefix}-telemetry-bus)
       ↓  detail-type: telemetry.ingested
  SQS ingestion queue
       ↓
  ingestion consumer                         ← GAP 2 (not built)
       ↓  translate to event-schema-contracts domain types
  signal-forge RealtimePipeline
       ├→ watermarks + windowed aggregation
       ├→ detection (offline / outage / anomaly)
       ├→ alert routing (EventBridge/SNS)
       ├→ dashboard projections (DynamoDB)
       └→ dataset export (S3/Parquet, Hive-partitioned)

  EventBridge Archive → replay Step Function → same RealtimePipeline
```

---

## 4. What is built

### 4.1 `event-schema-contracts`
Base event envelope with strict immutability and validation. Schema registry with explicit compatibility policy (new optional field = minor; breaking change = major). Domain payloads for telemetry, detection, features, and alerts. Documented conventions for the field-injection pattern.

### 4.2 `structured-logging-python`
`StructuredLogger` emitting JSON log events with service context and trace propagation. Adapters for FastAPI, Lambda, and async queue workers. Latency metrics helpers. Requires `ServiceContext.initialise()` once at process startup.

### 4.3 `telemetry-parser`
TCP stream reassembly keyed by connection 4-tuple. SIP message decoding. Field extraction and normalisation into `StructuredEvent`. **SIP `REGISTER` only** — other methods raise `UnsupportedProtocolEvent` and are skipped; there is an explicit test locking this in (`test_extract_raises_for_invite_method`). Transport-agnostic: `EventEmitter` takes an `on_emit` callback and knows nothing about MQTT or AWS.

### 4.4 `greengrass-publisher` (built this session)
Implements `on_emit`, publishing each `StructuredEvent` as JSON to `edge/{store_id}/telemetry` via Greengrass IPC `PublishToIoTCore` at QoS AT_LEAST_ONCE. Logs via `StructuredLogger` with the originating event's `trace_id`. Six tests, verified passing against the real `telemetry_parser` and `structured_logging` packages, using a fake IPC client so no AWS dependency is needed.

### 4.5 `aws-event-pipeline-infra`
Terraform, layered `shared` → `modules` → per-environment composition, with remote state bootstrapped separately. Custom EventBridge bus. Per-stage SQS queues, each with its own DLQ, for `telemetry.ingested`, `telemetry.validated`, `telemetry.enriched`, `feature.generated`, `dataset.export-requested`. EventBridge Archive (730-day retention in dev). Replay Step Function with a real poll loop (`WaitForReplay` → `DescribeReplay` → `CheckReplayState`) and a replay audit table. KMS encryption and default tagging applied across every environment.

### 4.6 `signal-forge`
v1.0.0, all seven phases complete. 384 tests; ruff and mypy `--strict` clean across 42 source files; CI green on Python 3.11 and 3.12.

- **Streaming**: `WatermarkManager` (per-key, monotonic, event-time only — never wall-clock, so replay reproduces the watermark trajectory exactly), classifying events `ON_TIME` / `LATE_TOLERATED` / `LATE_DROPPED`. `WindowAggregator` with tumbling and sliding event-time windows, watermark-driven emission, and late-event repair emitting corrections tagged `is_repair=True`. Windows are epoch-aligned so independent shards compute identical boundaries without coordinating.
- **Detection**: offline detector, outage detector, threshold-based anomaly detector (explicitly *not* statistical or ML — that is a documented, deferred follow-up).
- **Alerts**: alert routing with acknowledgement registry, EventBridge sink.
- **Dashboards**: idempotent projection folds into DynamoDB — offline counts, active outages, anomaly rates.
- **Datasets**: Hive-style partitioning, byte-deterministic Parquet serialisation, S3 writer.
- **Replay**: replay driver with the same-builder guarantee for live and replay pipelines.

---

## 5. Gaps

### Gap 1 — Packet capture component (not built)
`ParserPipeline.parse_stream()` takes `Iterable[TCPPacket]`. Nothing in any repository binds to a network interface, uses libpcap, or produces `TCPPacket` objects. Verified by search: no `socket`, `AF_PACKET`, `pcap`, or `scapy` usage anywhere outside comments.

**Consequence:** `telemetry-parser` has no real input source. Everything downstream is currently exercised only by synthetic data.

**Shape of the fix:** a Greengrass component using libpcap (via `scapy` or a lower-level binding) that captures SIP traffic on the controller's interface and feeds `TCPPacket` objects into `ParserPipeline` in-process.

### Gap 2 — Central-side ingestion consumer (partly closed; the remainder is Phase 8)
**The translation half is closed.** `telemetry-parser` now depends on `event-schema-contracts` and emits validated `SipRegistrationEvent` directly. `StructuredEvent` was deleted. There is no translation step because there is nothing left to translate — the parser constructs the contract types, so the schema validates its output at the point of production.

**The consumer half is open.** Nothing reads the ingestion queue. Thirty messages sit in it unread.

**Shape of the fix, revised:** not a Lambda. `RealtimePipeline` holds watermark and open-window state across `process_batch` calls, so a function invocation would lose open windows on recycle and they would silently never close. A long-running process is what the architecture requires. See `docs/phases/phase-8-plan.md`.

**And it emits nothing downstream.** The original fix said "emitting `telemetry.validated`". Nothing in any repository produces that event type, or `telemetry.enriched`, or `feature.generated` — verified by search across `signal-forge` and `telemetry-parser`. The five stage queues describe a decomposition that was never built: `RealtimePipeline` performs validation, enrichment, features and detection in one function-shaped process, so there is nothing to hand between stages. Four of the five queues, their DLQs and their routing rules can never receive anything.

That is a real design disagreement between the infrastructure and the control plane, and it is resolved for now in favour of the control plane: one queue, ingestion. The decomposition would matter at hundreds of Controllers with real volume; building it now would mean designing for a scale that cannot be tested, from scaffolding that predates the pipeline being written function-shaped.

### Gap 3 — IoT Core rule not yet applied
The `iot_core_telemetry_rule` module referred to here was written against an EventBridge target that IoT topic rules cannot use, and the branch carrying it was deleted. The publishing path currently in use is `scripts/publish_telemetry.py`, which runs locally and calls `PutEvents` directly — proving the pipeline without the IoT hop. A real edge deployment would still need this rule, as IoT rule → Lambda → `PutEvents`.

### Gap 4 — Dataset export Step Function is a stub
The Terraform-defined export workflow writes a placeholder `{"status": "exported"}` object to S3. The real export logic (`S3DatasetWriter`, Parquet serialisation, partitioning) lives in `signal-forge` and is not wired to this workflow.

### Gap 5 — Replay CLI's AWS plumbing is stubbed (open; Phase 8)
`build_event_source` (the EventBridge archive reader) and the production pipeline builder raise `NotImplementedError` by design, documented as deliberately out-of-brief. The replay *driver* and its determinism guarantees are fully implemented and tested; only the connection to a real archive is missing.

The archive is now worth reading from. The replay workflow was executed for the first time on 2026-09-02 and had four independent defects, all since fixed: `StartReplay` was passed an `EventPattern` parameter the API does not have; that field was a required JSONPath, so omitting it failed too; the IAM policy granted `StartReplay` on two of the three resources EventBridge evaluates it against; and the audit table recorded intent rather than outcome, leaving a row from 27 April in `STARTED` for four months. Fifteen events have now been archived and replayed successfully.

### Gap 6 — No CI on `aws-event-pipeline-infra` ✅ Closed
Added 2026-08-27: `terraform fmt -check` and `terraform validate` across dev, staging, prod and bootstrap, plus a Python job for the publisher scripts. No credentials required — no root declares a remote backend, so `init -backend=false` suffices.

### Gap 7 — Documentation drift (substantially addressed, but recurring)
Cleared across every repository between 25 August and 3 September: the parser's README, `replay-strategy.md` and `parsing-lifecycle.md` described a parser that no longer existed; the contracts README documented an envelope shape that was wrong about where schema identity lives; `wiring-component-repos.md` described a dispatch mechanism no component repo uses.

Treat this gap as permanently open rather than closable. The recurring finding across this project is that a document asserting something no mechanism verifies will drift, and drift silently. The mitigation that works is running the thing — every defect above was found by executing something for the first time, not by reading it.

### Gap 8 — The SIP path's framing (new)
`sip.registration` carries `device_label` values like `headset-12`, implying the parser observes headsets. Per 1.1, it does not: headsets are DECT and the SIP traffic belongs to the Controller's PBX bridge. The path is correctly *scoped* — bridge health is worth monitoring — and mislabelled.

The fix is naming rather than removal: the label should identify the Controller or its bridge. Small, and not yet done.

---

## 6. Testing the full device-to-dataset flow

The strategy is four layers, each independently valuable and each buildable before the layer above it works. Layers 1 and 2 need no AWS account at all.

### Layer 1 — Component tests (largely exist already)
Each repository's own unit tests, run in isolation. `signal-forge` has 384; `greengrass-publisher` has 6; `telemetry-parser` and the others have their own suites. These already prove each unit behaves correctly against its own contract.

**Missing at this layer:** nothing significant, except tests for the two unbuilt components (Gaps 1 and 2).

### Layer 2 — Local edge-chain integration test (buildable now, no AWS)
Prove the edge chain works end-to-end without a network interface or a cloud account:

1. Feed a **captured PCAP file** of real SIP `REGISTER` traffic through a small adapter producing `TCPPacket` objects. (A PCAP fixture is worth creating regardless — it also becomes the test fixture for the capture component when it's built.)
2. Run it through `ParserPipeline`, asserting the expected `StructuredEvent` instances come out.
3. Wire `greengrass-publisher` with a fake IPC client, asserting the correct MQTT topic and payload for each.

This proves capture → parse → publish as a chain, not just as three separately-tested units. It is the single highest-value test to build next, because it closes the exact class of gap this system has repeatedly hit: units verified individually, seams never verified at all.

### Layer 3 — Cloud integration test, synthetic input (needs AWS dev environment)
Once the IoT Core rule is applied to `dev`:

1. Publish a synthetic `StructuredEvent` JSON directly to `edge/test-store/telemetry` using the AWS IoT MQTT test client (or `aws iot-data publish`).
2. Assert it lands on the EventBridge bus with `detail-type: telemetry.ingested` — check the ingestion SQS queue's message count, or a temporary CloudWatch Logs target on the rule.
3. Once Gap 2 is closed, assert the ingestion consumer translates and re-emits `telemetry.validated`.
4. Assert a dashboard projection row appears in DynamoDB.
5. Assert a Parquet object appears in the dataset S3 bucket.

Each numbered step is independently assertable, so this can be built incrementally rather than as one all-or-nothing test.

### Layer 4 — Replay determinism test (the system's headline claim)
This is the test that proves the property SignalForge is actually built around, and the one most worth being able to demonstrate:

1. Push a known, fixed set of events through the pipeline; capture the resulting dataset objects and their byte hashes.
2. Trigger the replay Step Function over the same time window.
3. Assert the replayed output is **byte-identical** — same hashes, same partition layout, same record ordering.
4. Repeat with deliberately out-of-order and late-arriving events, asserting the watermark trajectory and any `is_repair=True` corrections are identical across both runs.

Note this test is currently blocked by Gap 5 (`build_event_source` raises `NotImplementedError`) — closing that gap is a prerequisite for proving the system's central claim end-to-end, which arguably makes it higher priority than its "out-of-brief" label suggests.

### Recommended order

1. **Layer 2** — no AWS needed, closes the seam-verification gap, produces a PCAP fixture reusable by the capture component later.
2. **Gap 3** — apply the IoT Core rule to `dev` (needs AWS credentials configured).
3. **Layer 3, steps 1–2** — proves edge-to-EventBridge works with synthetic input.
4. **Gap 2** — build the ingestion consumer; then Layer 3, steps 3–5 become assertable.
5. **Gap 1** — build the capture component, tested against the PCAP fixture from step 1.
6. **Gap 5 + Layer 4** — close the replay archive reader, then prove determinism end-to-end.

Building the capture component earlier is tempting but is the wrong order: it adds another well-tested isolated unit without proving anything connects, which is the failure mode this system has already hit repeatedly.
