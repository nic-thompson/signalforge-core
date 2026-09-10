# Phase 8 — Connecting the streaming path

> **Status.** Not started. `main` sits at `v1.0.0`, Phase 7 complete. This is the first phase after the original definition of done, which placed "deployed to AWS" and "validated against production traffic" out of scope. Phase 8 is where that scope reopens.

## What Phase 8 delivers

The callers.

`signal-forge` is a library. `RealtimePipeline.process_batch` is function-shaped by design (D-4) — the event source lives outside it, and the docstring is explicit that a caller supplies one. Every consumer layer beneath it is finished and tested: watermarks, windowing, detection, features, alerts, projections, dataset export, replay isolation. 384 tests.

What does not exist is anything that calls it against a real event source. There is one entry point in the repository, `signal_forge.replay.__main__`, and its production event source raises `NotImplementedError` — deferred as "deployment plumbing the definition of done places out of scope". Nothing polls a queue. Nothing reads the archive.

Meanwhile `aws-event-pipeline-infra` has, since 21 August, deployed a bus, five stage queues, an archive and a replay workflow. As of 30 August real events flow into it: synthetic SIP registrations, generated, parsed, validated against `sip.registration v1`, published to the dev bus, routed to the ingestion queue and archived. Thirty messages sit in that queue. Nothing has ever read one.

The two halves have never met. Phase 8 introduces the component that joins them.

The deliverables are:

- **The live consumer** — receives from the ingestion queue, deserialises to contract types, calls `process_batch`, handles emissions and deletes on success.
- **The archive reader** — the deferred body of `build_event_source`, so `signal_forge.replay` can read the archive that Phase 7 was built to replay from.
- **A deployment decision, recorded** — where the consumer runs, and what that costs while idle.
- **`docs/streaming-deployment.md`** — the as-built shape, matching how `docs/replay-workflows.md` documents the driver.

## Context and prerequisites

Phase 8 differs from every prior phase in one respect: it is the first that cannot be finished without AWS. Phases 1 to 7 were deliberately buildable and testable in isolation, and that discipline is why the consumer layers are finished. It is also why nothing has ever run against a real queue.

The prerequisites are in place and were not when Phase 7 closed:

- `telemetry-parser` emits validated `SipRegistrationEvent` rather than a locally-defined envelope, so events on the bus already satisfy the contracts this pipeline consumes.
- `event_id` is derived rather than generated, so replay reproduces identity as well as content.
- The replay workflow works. Four defects were found and fixed on 2 September, in a component that had been deployed to three environments since 21 August without ever executing.
- The dev estate is deployed, verified against its configuration, and holds fifteen archived events.

No new runtime dependencies beyond `boto3`, already present in the `datasets` and `dashboards` extras.

## The consumer shape — settled by the architecture, not by preference

**The pipeline holds state across calls, so the consumer must be a long-running process rather than a function invocation.**

This is not a deployment preference. `WatermarkManager` holds `_high_event_ts` per partition key. `WindowAggregator` holds `_states` and `_open_starts` for windows that are open and not yet closed. A window spans many events and closes when the watermark advances past its end plus lateness tolerance — which may be many batches later, or in a subsequent poll.

A Lambda triggered by SQS would therefore lose every open window when its instance is recycled, and concurrent instances would each hold partial state for the same partition key. Windows would never close. Aggregations would vanish without an error, because nothing in the pipeline distinguishes "this window never closed" from "this window had no events".

That failure is silent, which is the kind this project has spent a fortnight removing.

Three ways out, in the order they should be considered:

1. **A long-running consumer.** One process, holding pipeline state, polling the queue and calling `process_batch`. Matches the architecture exactly and requires no change to any finished component. **Recommended.**
2. **Externalise the state.** Move watermarks and open windows to DynamoDB so a function invocation can rehydrate them. This makes Lambda viable and is what a high-volume production system would do — but it is a substantial change to two finished, tested components, and it trades in-memory correctness for a distributed-state problem. Not for this phase.
3. **Single-concurrency Lambda with a long timeout.** Neither one thing nor the other. Rejected.

### Where it runs, and what it costs

The recommendation above says what shape the consumer is, not where it executes. Those are separable, and the cost difference is the whole of it.

The dev estate currently costs about £1/month at rest — one KMS key, with EventBridge, SQS, DynamoDB and Step Functions all effectively free at these volumes. A long-running consumer breaks that: the smallest always-on Fargate task is roughly £10-15/month, an order of magnitude more than everything else combined.

Against that, the working discipline established on 27 August is to tear down at session end and leave nothing accruing unnecessarily — a discipline adopted after finding a NAT gateway that had run for ten days at £25/month.

**So Phase 8 should run the consumer locally first.** The same reasoning that put the synthetic publisher in `scripts/` rather than a Lambda: it proves the path end to end, costs nothing, and defers the packaging decision until there is something worth packaging. A local consumer reading a real SQS queue exercises everything except where the process runs.

Deploying it — Fargate, its task definition, its IAM role, its scaling behaviour — is a later phase, and one worth taking only when there is a reason for the pipeline to be running while nobody is watching. There is not yet.

## What "connected" means

Phase 8 is done when a generated REGISTER produces a detection, and every step between is observable.

Concretely, one run should demonstrate:

1. `make publish` puts events on the dev bus
2. the ingestion rule routes them to the ingestion queue
3. the consumer receives, deserialises and calls `process_batch`
4. the watermark advances and a window closes
5. a detector fires, or provably does not fire for a reason the data explains
6. emissions reach their sinks
7. the messages are deleted from the queue

Step 5 deserves attention. The detectors look for device liveness — a device that stops registering. The generator currently produces regular, healthy traffic from every device, so **no detection will fire**. That is correct behaviour and a poor demonstration.

The generator therefore needs a way to produce a device that goes quiet: a `--silent-after` option, or a scenario flag. This is a small change to `aws-event-pipeline-infra/scripts/telemetry_generator.py` and it belongs in this phase, because without it the pipeline can be connected and still prove nothing.

## Open questions

- **Which queue does the consumer read?** The infrastructure has five stage queues — ingestion, validation, enrichment, feature, dataset-export — implying a consumer per stage, each publishing to the next. `RealtimePipeline` does all of that in one process. Either the stage queues describe a decomposition the control plane does not implement, or the consumer reads ingestion and the rest are unused. This needs deciding before the consumer is written, and the answer may be that the queue topology was designed for a different shape than the one that was built.
- **What happens to messages that fail to deserialise?** Each queue has a DLQ. Nothing currently sets a redrive policy that would route to it.
- **Does `process_batch` need the whole batch or can it stream?** SQS delivers up to ten messages per receive; the pipeline takes an iterable. Whether a receive maps to a batch, or the consumer accumulates, affects when windows close.

## Not in scope

- Deploying the consumer. Local execution only.
- Externalising pipeline state.
- Any new event type. Phase 9 introduces the second producer; this phase connects what exists.
- The SIP path's framing. The headsets are DECT, so `sip.registration` measures a PBX integration rather than the fleet — that is a real problem with what the data means, and it is not this phase's.
