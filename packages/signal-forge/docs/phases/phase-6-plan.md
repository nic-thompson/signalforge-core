# Phase 6 — Dashboard projections

> **Status.** Branch `feat/phase-6-dashboard-projections` exists locally; no commits yet. `main` sits at `18be8a7` (the Phase 5 merge), tagged `phase-5-complete`, green at 295 tests across Python 3.11 and 3.12. See the most recent file in `docs/status/` for current state.

## What Phase 6 delivers

Materialised views suitable for a sub-5-second dashboard: query-optimised, denormalised aggregates of the detections and alerts the pipeline produces, kept current as events flow. The dashboard itself is not in scope; the projections that feed it are.

The deliverables:

- **Three projections**, chosen to span three distinct view *shapes* rather than three variants of one:
  - **`OfflineCountProjection`** — current offline device count per store. A *gauge* keyed by store: it rises and falls as devices go offline and recover.
  - **`ActiveOutageProjection`** — the count of stores currently in outage. A *current-set cardinality*: how many stores are in a state right now.
  - **`AnomalyRateProjection`** — anomaly rate by signal type over a rolling window. A *windowed rate*: events per unit time, by key.
- **`ProjectionStore`** — a protocol for the read store, with an `InMemoryProjectionStore` (the no-AWS implementation backing tests and the deterministic core) and a `DynamoDbProjectionStore` (the AWS edge, moto-tested), mirroring the dataset and alert layers' protocol + in-memory + cloud pattern.
- **Pipeline integration** — the projections consume `ProcessingResult` (detections, and the alerts/emissions they need) and update the store per event.
- **`docs/dashboard-projections.md`** — reference document matching the shape of `alert-routing.md`, `dataset-export.md`, and `detection-models.md`.

Two design areas are settled below. One sub-question — how the rolling-window projection sources its time-windowing — is left open until the relevant commit.

## The idea: materialised views, completed

Phase 6 is where Chapter 11's materialised-view pattern reaches its natural form. The project has already built two read-only folds over the event stream — `DeviceRegistry` and `AcknowledgementRegistry` — each maintaining current state by consuming events. A dashboard projection is the same pattern, but the view is shaped for *reads*: instead of "is this alert acknowledged?", it answers "how many stores are in outage right now?".

A materialised view, in DDIA's framing, is a denormalised, query-optimised cache of derived data, kept current by the event stream rather than recomputed on read. That trades write amplification (every event touches the view) for read latency (the answer is precomputed). This is the project's first read-optimised store: distinct from the event flow (OLTP-shaped) and from the dataset layer (batch/analytics-shaped), a dashboard projection is a low-latency point-lookup store, which is why DynamoDB is the natural backend.

## Per-emission updates — settled design

The update model: every relevant event updates the projection immediately, rather than batching updates per window or on a timer.

**Decision: per-emission update.** This is recorded as **D-15**, landing in commit 1 with this plan.

**1. It is the faithful materialised-view implementation.** The book's model is that the view updates as each event arrives; the projection is a fold over the event sequence. Per-emission update is that model directly.

**2. It is replay-deterministic.** A per-emission fold over an ordered event sequence reconstructs the same view on replay — the same property `DeviceRegistry` and `AcknowledgementRegistry` already have. A wall-clock-batched alternative ("flush the view every N seconds") would not be replay-deterministic, the same reason D-14 fenced reminder cadence out of the deterministic path. D-15 is the mirror image of D-14: there, wall-clock cadence was fenced *out*; here, per-emission updates keep the projection *in* the deterministic path. Same boundary, opposite side.

**3. The cost is real and worth naming.** Per-emission update is write-amplifying — every event touches the store. That is DDIA's central materialised-view tension (write cost vs read freshness), and for a production system at scale it would warrant measurement. For this platform's scale and the brief's sub-5-second freshness requirement, always-fresh wins; the write cost is acceptable and the determinism benefit is decisive. The trade-off is documented rather than hidden.

## Storage: protocol, in-memory, DynamoDB — settled design

Mirrors the dataset (`InMemoryDatasetWriter` / `S3DatasetWriter`) and alert (`InMemoryAlertSink` / `EventBridgeAlertSink`) layers exactly.

**1. `ProjectionStore` is a structural protocol.** The projections read and write current view state through it, never touching a concrete backend directly. The protocol is the boundary between the deterministic projection logic and the storage backend.

**2. `InMemoryProjectionStore` is the no-AWS implementation.** It backs all projection-logic tests and any caller that wants projections without AWS. The fold — the part where the DDIA learning lives — is exercised entirely against this, with no cloud dependency.

**3. `DynamoDbProjectionStore` is the AWS edge.** boto3 confined to one module, moto-tested, constructed from a table name on `PlatformSettings`, no-op when unconfigured — the same client-factory-seam pattern the S3 writer and EventBridge sink use. It lands in its own commit, isolating moto's complexity, as the Phase 4/5 cloud writers did.

**4. Configuration follows D-12.** A `projection_table` / `replay_projection_table` name pair on `PlatformSettings`, `str | None`, swapped by `for_replay()`, validated at construction. Names, not clients. Replay projections write to the replay table, or to nothing, never to the live table.

## Rolling-window sourcing — question to settle

The two current-state projections (offline count, active outages) are straightforward folds: a detection updates a count or a set. The **rolling-window** projection (anomaly rate by signal type) needs a notion of *time* — "rate over the last N minutes" — and there are two honest ways to source it:

- **Reuse the streaming `WindowAggregator`.** The pipeline already computes event-time windows; the projection could consume window emissions and read a rate off them. This keeps all windowing in one place (the streaming layer) and stays event-time-driven, hence replay-deterministic.
- **Window in the projection layer.** The projection maintains its own rolling buffer of recent anomaly detections keyed by signal, evicting by event-time as newer detections arrive. More self-contained, but reintroduces windowing logic the streaming layer already owns.

The first is almost certainly right — it reuses proven, replay-deterministic windowing and avoids a second windowing implementation — but it depends on the emission shape the anomaly path produces, which is worth confirming against the real `WindowEmission` before committing. To be settled in a focused design conversation before the `AnomalyRateProjection` commit. Estimated 20 minutes of design.

## Plan

Phase 6 is structured as a sequence of small, individually-bisectable commits:

| # | Commit | Status |
|---|---|---|
| 1 | `docs(project): add Phase 6 plan and D-15 working note` | This commit |
| 2 | `feat(config): add projection_table and replay_projection_table to PlatformSettings` | Pending |
| 3 | `feat(dashboards): add ProjectionStore protocol and InMemoryProjectionStore` | Pending |
| 4 | `feat(dashboards): add OfflineCountProjection` | Pending |
| 5 | `feat(dashboards): add ActiveOutageProjection` | Pending |
| 6 | `feat(dashboards): add AnomalyRateProjection` | Pending (design TBD) |
| 7 | `feat(dashboards): integrate projections into the pipeline` | Pending |
| 8 | `feat(dashboards): add DynamoDbProjectionStore via moto` | Pending |
| 9 | `feat(dashboards): replay isolation via PlatformSettings` | Pending |
| 10 | `docs(dashboards): add dashboard-projections reference document` | Pending |
| 11 | `chore(project): close Phase 6 housekeeping` | Pending |
| 12 | `docs(status): add YYYY-MM-DD phase-6-complete snapshot` | Pending |

The order mirrors Phase 4/5: config first, then the protocol and in-memory store, then the projections built and tested against the in-memory store (the deterministic core), then pipeline integration, then the AWS edge (DynamoDB) in its own commit, then replay isolation, then docs and close-out. Whether Phase 6 needs an upstream PR is unclear — projections consume existing contracts, so possibly not; but per D-11, budget for one in case a projection needs a contract shape the upstream does not yet carry.

## What's deliberately not in Phase 6

- **The dashboard UI.** Out of scope per the roadmap; Phase 6 delivers the projections that feed a dashboard, not the dashboard.
- **Wall-clock-batched updates.** Per-emission only (D-15); batched flushing would break replay determinism.
- **Projections beyond the three shapes.** Offline count, active outages, anomaly rate cover three distinct view shapes; more projections are follow-up work, added against the same `ProjectionStore` protocol without structural change.
- **Cross-projection consistency / transactions.** Each projection is an independent fold; no cross-view transactional guarantees. The dashboard reads each view independently.

## Acceptance criteria

Phase 6 is done when:

- Three projections (`OfflineCountProjection`, `ActiveOutageProjection`, `AnomalyRateProjection`) exist, each a per-emission fold tested against `InMemoryProjectionStore`.
- `ProjectionStore` protocol exists with in-memory and DynamoDB implementations; the DynamoDB one is moto-tested.
- `PlatformSettings` exposes `projection_table` / `replay_projection_table`; `for_replay()` swaps them.
- The pipeline updates registered projections per event.
- A replay-isolation test confirms projections built live and via `for_replay()` are identical, with replay writing to a separate table.
- `docs/dashboard-projections.md` exists.
- Test count grew from 295 to roughly 330 (estimate; refine as we go).
- CI green: ruff, `mypy --strict`, pytest matrix on 3.11 and 3.12, test-count guard, GitGuardian.
- PR merged to main; tag `phase-6-complete` pushed on the merge commit.

## Estimated test count delta

The roadmap's ~280 → ~320 predates the Phase 4/5 overruns (actuals landed at 247 and 295). From the real 295 baseline, a realistic Phase 6 delta:

- `ProjectionStore` + `InMemoryProjectionStore`: ~6 tests
- `OfflineCountProjection`: ~7 tests
- `ActiveOutageProjection`: ~6 tests
- `AnomalyRateProjection`: ~8 tests
- Pipeline integration: ~6 tests
- `DynamoDbProjectionStore` via moto: ~8 tests
- Replay isolation: ~3 tests
- `PlatformSettings` extension: ~6 tests

Total estimated growth: 295 → ~330.

## Cross-references

- `docs/working-notes.md` — D-4 (function-shaped pipeline), D-5 (results as return values), D-12 (`PlatformSettings` as the live-vs-replay switch home), D-14 (wall-clock fenced out of the deterministic path). D-15 lands in commit 1, the mirror of D-14 for projections.
- `docs/roadmap.md` — Phase 6 scope and design questions; the DDIA cross-references (Chapter 11 materialised views, Chapter 3 storage for reads).
- `docs/detection-models.md`, `docs/alert-routing.md` — the detections and alerts these projections aggregate.
- `signal_forge/detection/device_registry.py`, `signal_forge/alerts/acknowledgement_registry.py` — the existing folds-over-a-stream the projections generalise.
- `signal_forge/streaming/realtime_pipeline.py` — `ProcessingResult`, what the projections consume.
- `signal_forge/config/platform_settings.py` — the `for_replay()` pattern Phase 6 extends.

## What Phase 6 delivered

> Closing reconciliation written at housekeeping, before the PR merges, matching the as-if-merged framing of the Phase 2–5 snapshots.

(To be filled in at housekeeping, before the PR merges.)
