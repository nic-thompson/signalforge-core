# SignalForge — Architecture, Gaps, and End-to-End Test Strategy

**Status of this document.** Everything under "What is built" was verified by reading the source of all six repositories directly. Everything under "Gaps" was verified as absent — searched for and not found, not merely assumed missing. The test strategy is proposed, not yet implemented.

---

## 1. What SignalForge is

A telemetry pipeline that turns raw SIP device-registration signals from an edge device fleet into real-time operational awareness and reproducible datasets.

The defining engineering property is **replay determinism**: any historical window can be re-run through the same pipeline and produce byte-identical output. This is not a bolt-on feature — immutability (`frozen=True`), strict validation (`extra="forbid"`), epoch-aligned windows, UUIDv5 identity derivation, and wall-clock-free watermarks all exist to make that guarantee hold.

Architecturally this is **Kappa**, not Lambda: one processing pipeline serves both live traffic and reprocessing, with an immutable event archive as the source of truth. There is no separate batch codebase computing a parallel "correct" view — `run_replay` uses the same pipeline builder as production, differing only in settings.

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

### Gap 2 — Central-side ingestion consumer and type translation (not built)
`telemetry-parser` emits its own `StructuredEvent` type. `signal-forge` and `event-schema-contracts` use a different set of domain types. Nothing anywhere translates between them, and `telemetry-parser` does not import `event-schema-contracts` at all (verified by search).

**Consequence:** even with MQTT publishing and the IoT Core rule in place, events landing on the ingestion SQS queue have no consumer, and no defined path into `signal-forge`'s pipeline.

**Shape of the fix:** a Lambda consuming the ingestion queue, validating incoming JSON against `event-schema-contracts`, translating the edge `StructuredEvent` shape into the central domain payload, and emitting `telemetry.validated`.

### Gap 3 — IoT Core rule not yet applied
The `iot_core_telemetry_rule` Terraform module was written this session and wired into `environments/dev`, but has not been `terraform apply`'d (blocked on AWS credentials not being configured locally). It is also not yet added to `staging` or `prod`.

### Gap 4 — Dataset export Step Function is a stub
The Terraform-defined export workflow writes a placeholder `{"status": "exported"}` object to S3. The real export logic (`S3DatasetWriter`, Parquet serialisation, partitioning) lives in `signal-forge` and is not wired to this workflow.

### Gap 5 — Replay CLI's AWS plumbing is stubbed
`build_event_source` (the EventBridge archive reader) and the production pipeline builder raise `NotImplementedError` by design, documented as deliberately out-of-brief. The replay *driver* and its determinism guarantees are fully implemented and tested; only the connection to a real archive is missing.

### Gap 6 — No CI on `aws-event-pipeline-infra`
Every other repository has a `.github/workflows/ci.yml` running mypy and pytest. This one has none — no automated `terraform fmt`, `validate`, or `plan` on pull requests.

### Gap 7 — Documentation drift
`docs/observability.md` states replay completion polling is not implemented; the actual `state_machine.json` has a full poll loop. Stale in the pessimistic direction, but stale.

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
