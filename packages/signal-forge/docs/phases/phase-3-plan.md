# Phase 3 — Feature pipelines

> **Status.** In progress. One commit landed on `feat/phase-3-feature-pipelines` (`e26eb16`, the upstream SHA bump for `event-schema-contracts` v0.3.0). Branch pushed to origin. See the most recent file in `docs/status/` for current state.

## What Phase 3 delivers

Three pieces of online feature infrastructure that detectors and downstream consumers (Phase 4 datasets) build on:

- **`DeviceRegistry`** — a router-subscribed projection of `device.registration` events into a live `device_id → store_id` mapping. Replaces the constructor-callable hack the Phase 2 detectors used (`store_lookup`, `registered_count_lookup`).
- **`MeanAggregation`** — a new aggregation alongside Phase 1's `CountAggregation` and `SumAggregation` and Phase 2's `DistinctCountAggregation`. Computes the windowed mean of a numeric contribution.
- **Feature emissions to a sink** — a protocol and an in-memory implementation that downstream consumers (Phase 4 dataset partitioning) can use to receive computed features from the pipeline.

The first piece is mostly settled by the design conversation that opened Phase 3. The other two have open design questions captured below.

## Context and prerequisites

Phase 3 starts from a position more concrete than Phase 2's plan was at equivalent time. The upstream contract correction landed in `event-schema-contracts` v0.3.0 (PR #2 in that repo, merged 2026-05-16). It added the required `store_id` field to `DeviceRegistrationPayload`, without which `DeviceRegistry` couldn't have projected device→store membership from event streams.

The SHA bump to `5fc79809` is already in this branch (`e26eb16`). No Phase 2 code or tests needed updating in response — the abstraction discipline of using `FakeDevicePayload` rather than the real upstream class meant the breaking contract change was invisible to existing tests.

The lesson that surfaced during this design work is captured in `docs/working-notes.md` under D-10 ("Callable injection lets us defer design questions, but only safely if the production path is at least sketched first"). The phase plan references it because it informed how Phase 3 unfolds, but the lesson itself is cross-phase process and lives in working-notes.

## DeviceRegistry — settled design

The design conversation on 2026-05-16 settled six decisions, with reasoning recorded here for future reference.

**1. Registry receives events via the `EventRouter`, not via explicit feeds or detector-protocol mimicry.**

The registry registers as a handler for `("device.registration", "v1")` through the same dispatch mechanism the detectors use. Three alternatives considered:

- Explicit `registry.observe(event)` calls from the pipeline owner — rejected because the registry would duplicate the router's filtering logic.
- Registry implements `EventDetector` returning empty detection lists — rejected because it mixes "stateful projection" with "side-effect-free detection".
- The chosen pattern — consistent with how the rest of the project routes events, leverages the consumer-favouring schema fallback from D-1 (a v1 registry handler accepts v1.1 events forward-compatibly), and keeps the registry single-purpose.

**2. Internal state is dual: `device_to_store` and `store_to_devices` kept in lockstep.**

A single `device_to_store: dict[UUID, str]` would force `device_count(store_id)` to iterate over values. At 50,000 devices, 8,000 stores, and `OutageDetector` calling `registered_count_lookup` once per window emission per store, that's ~5 billion iterations per hour. Untenable. The dual representation is `O(1)` for both queries at the cost of a second dict. Memory cost: ~10MB at fleet scale, trivial.

**3. API shape:**

```python
class DeviceRegistry:
    def __init__(self) -> None: ...

    # Handler for the EventRouter.
    def observe_registration(self, event: DeviceRegistrationEvent) -> None: ...

    # Query API (matches Phase 2 detector constructor-callable shapes).
    def store_for(self, device_id: UUID) -> str | None: ...
    def device_count(self, store_id: str) -> int: ...

    # Diagnostics.
    def registered_device_count(self) -> int: ...
    def known_stores(self) -> set[str]: ...
```

**4. Cold start: unknown queries return `None`/0.**

Events arriving before the corresponding registration produce no registry entries; queries against unregistered devices and stores return the same "unknown" sentinel as the Phase 2 callable hack. Phase 2 detectors already handle this case (skip emission rather than raise); no detector-side changes needed.

**5. No time-window discipline.**

Unlike streaming aggregators, the registry processes registration events as they arrive. There's no watermark, no event-time correctness, no late-event repair. The registry is a *state projection*, not a *time-windowed computation*. Replay determinism is preserved because event order is preserved across replays.

**6. Deregistration deferred.**

The brief doesn't describe device deletion. If a device is decommissioned, the registration is intended to persist (so historical detections still resolve cleanly). If deregistration becomes a requirement, it gets its own event type (`device.deregistration v1` upstream) and the registry subscribes to both. Not in Phase 3.

## MeanAggregation — questions to settle

The aggregation type is simple in shape (`(sum, count)` state, returns `sum/count`) but two design questions need explicit answers before implementation:

1. **Empty-window semantics.** What does `finalise` return when no contributions have arrived? Candidates: `0.0` (suggests value of zero), `NaN` (technically correct but propagates oddly), raising (harsh). Likely `0.0` paired with `event_count=0` as the disambiguator — but worth deciding deliberately rather than defaulting.

2. **Type coercion on non-numeric contributions.** `SumAggregation` raises `TypeError` for non-numerics. Does `MeanAggregation` follow that precedent, or be more permissive (e.g. skip non-numerics silently)? The strictness precedent is established; departing from it needs justification.

To be settled in a focused design conversation before the implementation commit. Estimated 15 minutes of design, 30 minutes of implementation + tests.

## Feature emissions — questions to settle

This is where Phase 3 starts touching Phase 4's surface and the design questions are larger:

1. **What does a feature emission look like?** The upstream `event-schema-contracts.features` package has `feature.vector v1`. Likely we use it rather than creating a parallel structure. Confirm by inspecting the upstream schema.

2. **What's the sink protocol shape?** Probably `FeatureSink` with `observe_emission(emission, feature_vector) -> None`, mirroring the detector-protocol pattern. Phase 4's S3 sink implements the same protocol.

3. **How does feature emission integrate with `RealtimePipeline`?** Two options: a separate sink-registration mechanism (like detector registration) or extending `ProcessingResult` with a `features` field (mirroring `detections`). The latter is more uniform with the detection layer; the former is more decoupled. Worth thinking through carefully.

4. **Replay isolation.** Replay runs must not emit features to the live sink. The Phase 1 `PlatformSettings.for_replay()` pattern probably gives us this — sink-registration is environment-aware in the same way alert routing will be in Phase 5.

To be settled in a focused design conversation before implementation. Estimated 30-45 minutes of design, 60-90 minutes of implementation + tests.

## Plan

Phase 3 is structured as a sequence of small, individually-bisectable commits. The implementation order is:

| # | Commit | Status |
|---|---|---|
| 1 | `chore(deps): bump event-schema-contracts to 5fc7980` | ✅ Landed (`e26eb16`) |
| 2 | `docs(project): add Phase 3 plan and D-10 working note` | This commit |
| 3 | `feat(detection): add DeviceRegistry component` | Pending |
| 4 | `refactor(detection): wire OfflineDetector and OutageDetector to DeviceRegistry in tests` | Pending |
| 5 | `feat(streaming): add MeanAggregation` | Pending (design TBD) |
| 6 | `feat(features): add FeatureSink protocol and InMemoryFeatureSink` | Pending (design TBD) |
| 7 | `feat(features): wire feature emission into RealtimePipeline` | Pending (design TBD) |
| 8 | `docs(features): add feature-pipelines reference document` | Pending |
| 9 | `chore(ci): bump test-count baseline from 144 to ~190` | Pending |

Commit 4 is a refactor rather than a feat because it changes how tests construct detectors (passing the registry instead of bespoke callables) without changing detector behaviour. Whether to also refactor production users of the detectors at the same time depends on whether any exist yet; in Phase 3 the production users are still hypothetical and the refactor is test-side only.

## Acceptance criteria

Phase 3 is done when:

- `DeviceRegistry` is implemented, tested, and the existing Phase 2 detectors have at least their tests demonstrating they work against a real registry rather than a hand-built dict callable.
- `MeanAggregation` is implemented and tested alongside the other three aggregation types.
- A `FeatureSink` protocol exists with at least an `InMemoryFeatureSink` implementation, and the pipeline emits features to registered sinks during `process()`.
- A `docs/features/` reference document or extension to `docs/streaming-internals.md` describes the feature-emission path.
- Test count grew from 144 to roughly 190 (estimate per the roadmap; refine as we go).
- CI green; ruff, `mypy --strict`, pytest matrix on 3.11 and 3.12 all passing.
- PR merged to main; tag `phase-3-complete` pushed.

## Estimated test count delta

Per the roadmap: 144 → ~190. Refined breakdown:

- `DeviceRegistry`: ~12 tests (registration flow, queries, dual-state consistency, cold start, multiple stores)
- `MeanAggregation`: ~5 tests (matching the shape of `SumAggregationTest`)
- `FeatureSink` + integration: ~15-20 tests
- Refactor commit (test-side rewiring of detectors): no net change

Total estimated growth: 144 → ~180. Slightly lower than the roadmap's ~190; we'll see.

## Cross-references

- `docs/working-notes.md` — design decisions D-1 through D-9 establish the architectural patterns Phase 3 builds on; D-10 captures the process lesson from the upstream contract correction work.
- `docs/roadmap.md` — Phase 4 (datasets) is the immediate downstream consumer of feature emissions; Phase 5 (alert routing) consumes detections that DeviceRegistry indirectly supports.
- `event-schema-contracts` PR #2 (merged 2026-05-16) — the contract correction adding required `store_id` to `DeviceRegistrationPayload`.
- Commit `e26eb16` — the SHA bump that made the new field available in this repo.
