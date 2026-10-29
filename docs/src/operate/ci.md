# CI runners (GitHub Actions)

vmon CI runs GitHub Actions jobs on vmon. A job with `runs-on: vmon` (or another vmon size label) gets a fresh sandbox, a microVM booted from the [`ci` template](../concepts/templates.md#the-ci-runner-template), which runs one ephemeral just-in-time (JIT) GitHub runner. The sandbox is killed when the job ends. Jobs can use Docker and `/dev/kvm` inside the sandbox.

```yaml
jobs:
  test:
    runs-on: vmon-large          # 8 vCPUs, 16 GiB
    steps:
      - uses: actions/checkout@v4
      - run: make test
```

## How it works

1. A GitHub App owned by the operator sends `workflow_job` webhooks to the driver's `POST /ci/github/webhook` on its public listener, directly or through a relay that forwards the raw body unchanged with the `X-GitHub-*` and `X-Hub-Signature-256` headers. The driver checks `X-Hub-Signature-256` (HMAC-SHA256 of the raw body, constant time) with the webhook secret it is configured with, and records the job. A relay either passes GitHub's signature through (vmon holds the app's webhook secret) or verifies it and re-signs the body with a secret of its own (vmon holds the relay's secret).
2. The raft leader's CI controller checks the job: its labels, the GitHub account's linked team, and for public repositories the account's policy. A refused job never runs, and the reason is shown in the dashboard and `vmon admin ci`.
3. The controller creates a sandbox in the linked team, registers a repository-level JIT runner with the job's vmon label, and starts `run.sh --jitconfig …` in the sandbox as `user`. GitHub hands the runner a queued job with that label.
4. When the job completes, the controller removes the runner and kills the sandbox. A failed job with the `debug` option keeps its sandbox for 30 minutes.

Sandboxes count against the linked team's limits like any other sandbox. They get no environment variables or vmon credentials; jobs use GitHub secrets. Each sandbox carries the metadata `vmon.ci.job`, `vmon.ci.repo`, and `vmon.ci.runner`.

## Labels

A job uses exactly one vmon label, `<size>[/<option>…]`. Besides it, only `self-hosted`, `linux`, and `x64` are allowed (case-insensitive). Jobs without a vmon label are ignored, so other runner pools keep working.

| Label | vCPUs | Memory |
|---|---|---|
| `vmon-small` | 2 | 4 GiB |
| `vmon` | 4 | 8 GiB |
| `vmon-large` | 8 | 16 GiB |
| `vmon-xlarge` | 16 | 32 GiB |

| Option | Effect |
|---|---|
| `from=<template>` | Boot another template: any reference the linked team resolves (ID, alias, `team-slug/alias`, optional `:tag`). It must derive from `ci` (`Template().from_template("ci")`) so the runner layout holds. In `from=<namespace>/<alias>` the alias is the next segment unless that segment is an option. Default: `--ci-template` (`ci`). |
| `debug` | Keep the sandbox of a failed job for 30 minutes (job state `held`), so the team can inspect it with the E2B SDKs or CLI. |

Examples: `runs-on: vmon`, `runs-on: vmon-large/from=omp-ci`, `runs-on: vmon-xlarge/from=omp-ci/debug`, `runs-on: [self-hosted, vmon-small]`.

Unknown sizes or options, a duplicate option, two vmon labels, or another label refuse the job. A size that differs from the template build's size boots the template disk fresh at that size (the start command runs again; about the template's cold boot time) instead of forking its snapshot. Workflows can detect vmon runners with `VMON_CI=1`.

## Setup

1. **Template.** Build `ci` as a public template of a system team. Its disk is 64 GiB, so raise the team's disk limit first:

   ```sh
   vmon admin set-limits system --max-disk-mb 65536
   uv run templates/build.py ci
   ```

2. **Team.** Pick or create the team the jobs run in, with limits that fit the sizes it should run (`vmon-xlarge` needs `--max-vcpus 16 --max-memory-mb 32768`) and enough `--max-concurrent-sandboxes`. Jobs are capped at 6 hours; keep `--max-length-hours` at 7 or more.
3. **GitHub App.** Open the dashboard's CI page (`vmon admin ci connect` prints its URL) and create the app. Enter the public webhook URL: this driver's `https://api.<domain>/ci/github/webhook`, or the relay's. GitHub creates the app with the permissions vmon needs (repository administration: write, actions: read, metadata: read; event `workflow_job`) and returns to the dashboard, which stores its credentials. An existing app can be configured with `vmon admin ci app set --app-id <id> --private-key-file app.pem --webhook-secret-file secret`, where `secret` is the secret that signs what vmon receives: the app's webhook secret when GitHub or a pass-through relay delivers, the relay's own secret when the relay re-signs. `vmon admin ci app secret` prints that secret (for a pass-through relay that verifies with it).

   The app may be shared with other services (its webhook URL then points at their relay, which forwards the CI events). vmon answers every other event with 200 and ignores it, never asks GitHub to redeliver it, and only touches GitHub runners named `vmon-…`. The app and each installation need Administration: write (runner registration) and Actions: read (jobs, logs); `vmon admin ci` lists a setup problem for an app or installation lacking them, but `app set` does not refuse such an app.
4. **Install** the app on the user or organization accounts whose repositories should use vmon. GitHub returns to the dashboard, which links the installation to a team. From the CLI:

   ```sh
   vmon admin ci sync
   vmon admin ci link acme --team acme-ci
   ```

5. Check `vmon admin ci`: it lists setup problems (no app, unlinked accounts, team limits below a size, an unresolvable template) until there are none.

The app's private key and secrets are stored encrypted with a key derived from the cluster secret. Changing the cluster secret makes them unreadable; configure the app again afterwards.

## Public repositories

Anyone who can open a pull request can make a public repository's workflow run code. Public repositories are therefore off by default per account. `vmon admin ci public <account> on --users "alice, @bob"` allows runs whose actor and triggering actor (a re-run's initiator) are both listed; `--anyone` allows every run. Users are checked against GitHub when the policy is saved.

The policy is enforced twice. The controller refuses jobs that fail it, so they get no runner. But JIT runners are registered per repository, and GitHub hands a runner any queued job with matching labels, so a refused job could still start on a runner provisioned for an allowed job. Every runner therefore carries a job gate: before `run.sh` starts, its start command writes `/home/user/actions-runner/.vmon/job-started.sh` (read-only) and sets `ACTIONS_RUNNER_HOOK_JOB_STARTED` to it, with the repository and the account's policy at that moment baked in. The runner runs the hook before the job's first step and waits for it. Unless the checks below pass, the hook prints a `vmon CI refused this job: …` error annotation and ends the sandbox's VM on the spot (`sudo reboot -ff`, which is a VM exit on vmon; without sudo it kills the runner processes and never returns), because a merely failing hook would still let `always()` steps and action pre-scripts run. The checks:

- `GITHUB_REPOSITORY` is the runner's repository, and
- for a public repository, public repositories are on and either anyone may run or both `GITHUB_ACTOR` and `GITHUB_TRIGGERING_ACTOR` (`GITHUB_ACTOR` when unset) are listed (case-insensitive).

Private repositories only check the repository. When GitHub reports a refused job running on a vmon runner, the controller kills the runner's sandbox, removes the runner from GitHub, and provisions a fresh runner for the job the runner was meant for. A job vmon has not checked yet (its `queued` webhook was lost, for example during a relay outage) that starts on a vmon runner is adopted instead: GitHub matched it to the runner's label, and the gate has checked it. The job the runner was meant for gets another runner.

## Lifecycle and limits

| Event | Behaviour |
|---|---|
| No capacity (team concurrency limit, no node with room) or GitHub rate limit | The job is `waiting` with the reason; retried every 15 s until it fits or GitHub cancels the job. |
| Template missing, size over team limits | The job is refused. |
| Other provisioning failures | Retried up to 3 times, then the job is `failed`. |
| Job completes | Sandbox killed, runner removed from GitHub (retried while GitHub is unavailable). |
| Job fails with `debug` | Sandbox kept 30 minutes (`held`), then killed. |
| Runner idle for 10 minutes (its job ran elsewhere or was cancelled) | Runner removed, sandbox killed. |
| Runner running for 6 hours | Runner removed, sandbox killed, job marked `failed`. |

Every minute the leader also asks GitHub to redeliver `workflow_job`, `installation`, and `installation_repositories` deliveries that failed in the last 24 hours (for example while the relay could not reach vmon; at most 3 times each). It reads only GitHub's failed deliveries, up to 10 pages of 100, and skips deliveries vmon itself answered with 2xx (it remembers their delivery IDs for 25 hours), so a relay failing after forwarding does not cause redeliveries. A `workflow_job` delivery that never reached vmon is redelivered even when it was another runner pool's job; vmon then ignores it. The leader also polls jobs whose webhooks seem lost, kills sandboxes with CI metadata that no runner record owns, and removes offline `vmon-` runners left registered on GitHub. Webhook handling is idempotent: a redelivered or late webhook never starts a second sandbox for a job, and a leader change resumes work where it stopped.

Job states: `queued` (recorded), `waiting` (retrying; see the reason), `starting` (sandbox up, runner registering), `running`, `held`, `completed` (with GitHub's conclusion), `refused` (never ran; see the reason), `failed` (vmon-side failure).

## Observing

- `vmon admin ci`: setup problems, 7-day statistics, latest jobs. `vmon admin ci jobs --state failed`, `vmon admin ci job <id>` (with its history), `vmon admin ci logs <id>`. See the [command reference](../reference/admin.md#ci).
- The dashboard's CI page shows the same, plus repositories and account settings.
- A job's sandbox is an ordinary sandbox of the linked team while it runs (`GET /v2/sandboxes?metadata=vmon.ci.job%3D<id>`).

## Admin API

The dashboard and `vmon admin ci` use the JSON API under `/admin/ci` (`X-Admin-Token`) on the public listener and `/driver/v1/ci` (cluster secret) on the cluster listener: `GET /` (overview), `GET /jobs?account=&repo=&state=all|active|failed|refused&limit=&nextToken=` (`X-Next-Token`), `GET /jobs/{id}`, `GET /jobs/{id}/logs` (409 `not_available` while running), `GET /repos?account=`, `GET|PUT|DELETE /app`, `POST /app/manifest`, `POST /app/conversions`, `POST /accounts/sync`, `POST|DELETE /accounts/{installationId}/link`, `PUT /accounts/{installationId}/public`, and on the cluster listener only `GET /app/webhook-secret`. Errors use the API's `{code, message, error_code}` shape.
