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

## Actions cache

Jobs on vmon runners use the GitHub Actions cache unchanged (`actions/cache`, `Swatinem/rust-cache`, the `cache:` options of `setup-node`/`setup-python`/`setup-go`, docker buildx `type=gha`, anything built on `@actions/cache` v4 or later), but the archives stay on your nodes' disks. Nothing is stored at GitHub, and GitHub's 10 GB repository quota does not apply.

How it works:

- The `ci` template's runner is patched (one string in `Runner.Worker.dll`, pinned by sha256) so the cache client talks to the `ACTIONS_RESULTS_URL` the runner was started with instead of GitHub's. vmon starts the runner with `ACTIONS_RESULTS_URL=http://<sandbox gateway>:7948/`, a service of the node the sandbox runs on. Templates built `from=` `ci` inherit this.
- The node attributes each request to the sandbox by its network attachment, never by anything the job sends, and serves only CI runner sandboxes; other sandboxes get 403. A runner moved to another node (node drain) keeps using the URL it started with: every node answers on the gateway address of any lease in its `--net-pool` (use the same pool on all nodes).
- Scopes follow GitHub's rules, taken from GitHub's record of the workflow run: a job saves to its own ref (`refs/heads/<branch>`, `refs/pull/<n>/merge`, `refs/tags/<tag>`) and restores from its own ref, then (pull requests) the base branch, then the default branch. A pull request never writes to its base or default branch, and a branch never reads another branch except the default. `pull_request_target` runs save to the base branch, as on GitHub.
- Lookup: exact key, then each `restore-keys` prefix (newest match first), per ref in that order; the cache version (paths and compression) must match. Entries are immutable: saving an existing key logs GitHub's "another job may be creating this cache" message.
- A restore on a node without the archive copies it from a node that has it while streaming it to the job.
- Other calls to GitHub's results service (`upload-artifact`/`download-artifact` v4) are forwarded to GitHub unchanged, so artifacts keep working.

Storage and eviction:

| Limit | Default | Behaviour |
|---|---|---|
| Unused entries | 7 days | Removed (GitHub's rule). |
| Per repository | 50 GiB | Least recently used entries are removed beyond it. |
| Per node (`--ci-cache-mb`) | 10% of the data filesystem, at most a fifth of its free space at start | Least recently used local copies are removed beyond it; an archive whose last copy goes takes its entries with it. |
| Unfinished uploads | 1 hour | The reservation is released. |

Archives live in `<data-dir>/ci-cache/`, the index in the cluster store.

```sh
vmon admin ci caches                              # usage per repository
vmon admin ci caches --repo acme/app              # entries of one repository
vmon admin ci caches delete acme/app --key-prefix cargo-   # drop entries
```

The cache needs Linux nodes with TAP networking. Elsewhere the runner falls back to GitHub's results service (GitHub's own cache).

### Bazel remote cache

The same node service is a [Bazel HTTP remote cache](https://bazel.build/remote/caching#http-caching). When the node serves it, the runner exports `VMON_CACHE_URL=http://<sandbox gateway>:7948` to every step (unset otherwise):

```sh
# push, tag, workflow_dispatch, schedule, … jobs: read and write
bazel build --remote_cache="$VMON_CACHE_URL/bazel" --remote_local_fallback //...
# pull request jobs: read only
bazel build --remote_cache="$VMON_CACHE_URL/bazel" --remote_upload_local_results=false --remote_local_fallback //...
```

- Entries (`/bazel/ac/<sha256>`, `/bazel/cas/<sha256>`) belong to the job's repository; other repositories never see them. CAS uploads must hash to their key.
- Only jobs saving to a branch or tag (`refs/heads/*`, `refs/tags/*`) may write. Pull request jobs read the repository's entries, but their uploads get 403 (Bazel prints a warning per upload), so pass `--remote_upload_local_results=false` on pull requests. `pull_request_target` jobs save to the base branch and may write; workflows that build pull request code in `pull_request_target` should also disable uploads.
- Entries stay on the node that stored them; a miss asks the other nodes (2 s) and keeps a copy. Least recently used entries are evicted beyond `--ci-bazel-cache-mb` (default 5% of the data filesystem, at most a fifth of its free space at start). Unlike the Actions cache there is no cluster index and no admin listing; `<data-dir>/ci-cache/bazel/<repo id>/` can be deleted while the node is stopped.
- Use `--remote_local_fallback` so a cache outage never fails a build, and prefer downloading all outputs (no `--remote_download_toplevel`/`minimal`): an evicted entry cannot be refetched.

Container actions (`uses: docker://…` or a Dockerfile action) and steps of `container:` jobs receive only the variables the runner hands them: they see GitHub's results URL as `ACTIONS_RESULTS_ORL` and no `ACTIONS_RESULTS_URL`, so caching and artifact uploads from inside a container need `ACTIONS_RESULTS_URL: ${{ env.ACTIONS_RESULTS_URL }}` passed explicitly (or the step moved out of the container). Script and JavaScript steps on the runner itself are unaffected.

## Observing

- `vmon admin ci`: setup problems, 7-day statistics, latest jobs. `vmon admin ci jobs --state failed`, `vmon admin ci job <id>` (with its history), `vmon admin ci logs <id>`. See the [command reference](../reference/admin.md#ci).
- The dashboard's CI page shows the same, plus repositories, Actions caches (usage per repository, entries, deletion by key prefix), and account settings.
- A job's sandbox is an ordinary sandbox of the linked team while it runs (`GET /v2/sandboxes?metadata=vmon.ci.job%3D<id>`).

## Admin API

The dashboard and `vmon admin ci` use the JSON API under `/admin/ci` (`X-Admin-Token`) on the public listener and `/driver/v1/ci` (cluster secret) on the cluster listener: `GET /` (overview), `GET /jobs?account=&repo=&state=all|active|failed|refused&limit=&nextToken=` (`X-Next-Token`), `GET /jobs/{id}`, `GET /jobs/{id}/logs` (409 `not_available` while running), `GET /repos?account=`, `GET|PUT|DELETE /app`, `POST /app/manifest`, `POST /app/conversions`, `POST /accounts/sync`, `POST|DELETE /accounts/{installationId}/link`, `PUT /accounts/{installationId}/public`, `GET /caches` (`{repos}` with usage per repository), `GET /caches/{owner}/{name}` (`{repo, entries}`), `DELETE /caches/{owner}/{name}?keyPrefix=` (`{deleted, freedBytes}`), and on the cluster listener only `GET /app/webhook-secret`. Errors use the API's `{code, message, error_code}` shape.
