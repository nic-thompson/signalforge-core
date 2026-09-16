# Wiring component repos to the integration suite

Two mechanisms are meant to keep this suite from going stale. They are
complementary: dispatch gives fast feedback, the schedule is the backstop
for when dispatch does not fire.

> **Only the schedule is wired.** No component repo sends a dispatch —
> the job in `component-repo-dispatch-job.yml` has not been added to any
> of them, and no token exists. A break in a component therefore surfaces
> at the next nightly run rather than within minutes of the merge that
> caused it. Everything under "repository_dispatch" below is a setup
> guide for work not yet done, not a description of what happens today.
>
> That is a real gap but a bounded one: up to 24 hours of latency on a
> project with one committer. It is recorded here rather than left for a
> reader to infer from the absence of dispatches.

| | Latency | Fails safe? |
|---|---|---|
| `repository_dispatch` | Minutes after a component merges | No — a missing token or a repo not yet wired means nothing fires. **Not currently wired.** |
| `schedule` (nightly) | Up to 24 hours | Yes — runs regardless of what any other repo does. **This is what runs today.** |

Build both. The schedule alone is too slow to be useful during active
work; dispatch alone silently stops working the moment a token expires.
The row above is exactly why the schedule was built first: dispatch not
being wired is invisible, and the nightly run is what covers it.

---

## 1. Scheduled runs

Already configured in `.github/workflows/integration.yml` — nightly at
06:00 UTC. Nothing to set up.

Two things worth knowing:

- **GitHub disables scheduled workflows in repos with no activity for
  60 days.** If this repo goes quiet, the nightly run stops without
  announcing itself. Worth checking the Actions tab occasionally, or
  keeping the repo active.
- A failing nightly run **opens an issue** (labelled `nightly-failure`),
  because nobody owns a cron the way an author owns a PR. Repeat
  failures comment on the existing issue rather than opening duplicates,
  and a subsequent passing run comments and closes it. Until 2026-09-03
  nothing closed it on recovery, so an issue outlived the failure it
  described and had to be closed by hand.

---

## 2. `repository_dispatch` from component repos

### The token requirement

`repository_dispatch` needs a **personal access token**. The default
`GITHUB_TOKEN` cannot trigger workflows in another repository — a
deliberate GitHub restriction to prevent workflow-triggering loops, not
something that can be configured around.

Create a **fine-grained PAT**:

1. GitHub → Settings → Developer settings → Personal access tokens →
   Fine-grained tokens → Generate new token
2. Repository access: select `signalforge-integration-tests`
3. Permissions:
   - `signalforge-integration-tests` → **Contents: Read and write**
     (required to send a dispatch)

   The component repos no longer need to be listed. They were private
   when this was written, so the same token also granted the integration
   workflow read access to check them out. All four are public now and
   the checkouts use no token at all — which is what fixed this suite's
   first eleven days of failures, where an unset `COMPONENT_REPO_TOKEN`
   resolved to an empty string and `actions/checkout` rejected it.
4. Set an expiry you'll actually notice. A one-year token that expires
   silently reintroduces exactly the staleness problem this is
   preventing — the nightly run is what covers you when it does.

### Adding the secrets

In **each component repo** (`telemetry-parser`,
`structured-logging-python`, `greengrass-publisher`):

```bash
gh secret set INTEGRATION_DISPATCH_TOKEN --repo nic-thompson/<repo>
```

Nothing needs to be set in this repo. `COMPONENT_REPO_TOKEN` was required
while the component repos were private; they are public, the checkouts
pass no token, and the secret is no longer referenced by the workflow.

### Adding the dispatch job

Append the contents of `docs/component-repo-dispatch-job.yml` to each
component repo's `.github/workflows/ci.yml`, changing `needs:` to name
that repo's actual test job.

The job **degrades gracefully**: if `INTEGRATION_DISPATCH_TOKEN` isn't
set it logs a warning and exits 0 rather than failing the component
repo's build. A missing integration trigger shouldn't block a component
from merging — the nightly run still covers it.

---

## How a dispatched run differs

When `telemetry-parser` dispatches, the integration workflow pins
`telemetry-parser` to **the exact commit that triggered it** and leaves
the other components on `main`. That way a failure points at one
changed component rather than an ambiguous combination.

The refs actually used are written to the run summary, so you can see
what was tested without reading the logs.

---

## Verifying it works

Trigger manually first, before relying on the automation:

```bash
gh workflow run integration.yml --repo nic-thompson/signalforge-integration-tests
```

Then test the dispatch path end to end:

```bash
curl -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer $YOUR_PAT" \
  https://api.github.com/repos/nic-thompson/signalforge-integration-tests/dispatches \
  -d '{"event_type":"component-updated","client_payload":{"repository":"nic-thompson/telemetry-parser","sha":"main"}}'
```

A `204 No Content` means it fired. Check the Actions tab for the run.

---

## Expected result

All tests pass, with **one `xfail`** — DEFECT-3.

The three replay-determinism defects in `DEFECTS.md` were all `xfail`
when written, and the markers are `strict=True`, so fixing a defect turns
its test into an **unexpected pass** and the suite goes red. That is
intentional: it makes fixing a defect impossible to do silently, and it
worked exactly as designed.

- **DEFECT-2** fixed 2026-08-30. Event time comes from packet capture
  with no fallback.
- **DEFECT-1** fixed 2026-09-02. `event_id` is derived rather than
  generated.
- **DEFECT-3** remains `xfail`, but not because it is unfixed.
  `ingest_timestamp` is wall-clock deliberately — it records when a parse
  happened, not when traffic was observed, so two runs differing is
  arguably correct. The marker is kept so that a change of behaviour
  fails loudly rather than passing unnoticed, which would mean the
  decision had been reversed without being revisited.

A count is deliberately not given here. This document claimed "12 passed,
3 xfailed" for a fortnight during which the suite never ran at all.
