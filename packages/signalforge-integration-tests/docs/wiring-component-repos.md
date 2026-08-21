# Wiring component repos to the integration suite

Two mechanisms keep this suite from going stale. They're complementary:
dispatch gives fast feedback, the schedule is the backstop for when
dispatch doesn't fire.

| | Latency | Fails safe? |
|---|---|---|
| `repository_dispatch` | Minutes after a component merges | No — a missing token or a repo not yet wired means nothing fires |
| `schedule` (nightly) | Up to 24 hours | Yes — runs regardless of what any other repo does |

Build both. The schedule alone is too slow to be useful during active
work; dispatch alone silently stops working the moment a token expires.

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
  failures comment on the existing issue rather than opening duplicates.

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
2. Repository access: select `signalforge-integration-tests`,
   `telemetry-parser`, `structured-logging-python`, `greengrass-publisher`
3. Permissions:
   - `signalforge-integration-tests` → **Contents: Read and write**
     (required to send a dispatch)
   - the component repos → **Contents: Read** (so the integration
     workflow can check them out — these are private repos)
4. Set an expiry you'll actually notice. A one-year token that expires
   silently reintroduces exactly the staleness problem this is
   preventing — the nightly run is what covers you when it does.

### Adding the secrets

In **each component repo** (`telemetry-parser`,
`structured-logging-python`, `greengrass-publisher`):

```bash
gh secret set INTEGRATION_DISPATCH_TOKEN --repo nic-thompson/<repo>
```

In **this repo**, so the workflow can check out the private components:

```bash
gh secret set COMPONENT_REPO_TOKEN --repo nic-thompson/signalforge-integration-tests
```

Both can be the same PAT.

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

**12 passed, 3 xfailed.**

The three `xfail`s are the replay-determinism defects in `DEFECTS.md`.
They're `strict=True`, so when someone fixes the underlying defects in
`telemetry-parser` this suite goes **red on unexpected-pass** — the
signal to remove the `xfail` markers. That's intentional: it makes
fixing the defect impossible to do silently.
