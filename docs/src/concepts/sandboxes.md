# Sandboxes

How a sandbox lives: create, timeouts, pause and resume, fork, snapshots, kill, and the metrics, logs, events, and webhooks it produces. Every operation is the upstream E2B API, so the official SDKs work as documented. This page states the exact vibemon semantics. Examples use the Python `e2b` SDK, with TypeScript where the call shape differs.

## States

`GET /sandboxes/{id}` and `Sandbox.list()` report a sandbox as `running` or `paused`. Killed sandboxes disappear from both.

| From | Operation | To |
|---|---|---|
| — | create, fork | running |
| running | pause, or timeout with `autoPause` | paused |
| paused | connect, resume, or ingress traffic with `autoResume` | running |
| running | snapshot, fork (source) | running (briefly `snapshotting` internally) |
| running | kill, or timeout without `autoPause` | killed |
| paused | kill | killed |

Transitions of one sandbox are serialized by a per-sandbox lock in the raft KV, except that an uncontended running-sandbox kill atomically removes the route without acquiring that lock. A request that finds another transition in progress waits for it, up to 2 minutes. A running sandbox has a route in the KV. A paused sandbox has only its sandbox record in the driver store and its paused state, on its node or in the object store. Lifecycle operations finish even if the client disconnects. See [Architecture](architecture.md).

## Create

```python
from e2b import Sandbox

sbx = Sandbox.create("base", timeout=600, metadata={"job": "42"}, envs={"FOO": "bar"})
```

```ts
import { Sandbox } from 'e2b'

const sbx = await Sandbox.create('base', { timeoutMs: 600_000, metadata: { job: '42' } })
```

- A create normally is a copy-on-write fork of the template's memory snapshot, captured with any configured start command already running. The rootfs is a block overlay of the template's disk. See [Templates](templates.md).
- vCPUs, memory, and disk come from the template build, not the request. The create fails with 400 if the build exceeds the team's `max_vcpus`, `max_memory_mb`, or `max_disk_mb`.
- GPUs come from `metadata["vmon.gpus"]`, else the template's `VMON_GPUS` environment variable, else none. GPU sandboxes always cold-boot, and their pauses and snapshots keep only the filesystem. See [GPUs](../operate/gpus.md).
- Admission checks the team's concurrent-sandbox limit atomically in the KV and answers 429 at the limit. Paused sandboxes do not count. See [Authentication and teams](auth.md#limits).
- Creates with node-local `volumeMounts` cold-boot the template rootfs instead of forking the snapshot. Object-store volumes and `vmonS3Mounts` can reuse memory only when their device tags match the snapshot; otherwise they also cold-boot. See [Storage, object store, and volumes](../operate/storage.md#volumes).
- On memory restores, the node requests 64 bytes of fresh host entropy and a kernel CRNG reseed before envd is reinitialized. It also reseeds the source after a snapshot. Reseed failures are logged as warnings rather than failing the operation.
- The response carries `envdAccessToken` (required by envd), plus `trafficAccessToken` when `network.allowPublicTraffic` is false. See [Authentication and teams](auth.md#envd-access-token).
- Sandbox IDs are `i` followed by 20 characters of `[a-z0-9]`.

### Idempotency

`POST /v2/sandboxes` accepts an `Idempotency-Key` header matching `^[a-z0-9]{1,255}$`. The record lives 24 hours in the KV.

| Repeat with the same key | Answer |
|---|---|
| Same body, first request finished | Replays the original 201 response |
| Different body | 409 `idempotency_request_mismatch` |
| First request still running | 409 `idempotency_in_progress` |
| First request pending for more than 10 minutes | 422 `idempotency_outcome_unknown` |

A failed create deletes the record, so the key can be retried. The SDKs do not send this header; use raw HTTP.

## Timeouts

| Rule | Value |
|---|---|
| Default timeout, `POST /v2/sandboxes` and v2 connect | 300 s |
| Default timeout, `POST /sandboxes` (v1), resume, fork | 15 s |
| Allowed timeout on create, resume, v2 connect, fork | `> 0` and ≤ the team's `max_length_hours` (400 otherwise) |
| Maximum lifetime | `max_length_hours` per execution, counted from the create or the latest resume |

When the timeout expires, the raft leader's reaper (every 1 s) pauses the sandbox if it has `autoPause`, otherwise kills it. If an auto-pause fails, the sandbox is killed.

| Call | Semantics |
|---|---|
| `sbx.set_timeout(seconds)` → `POST /sandboxes/{id}/timeout` | Sets the end to `now + timeout`. May shorten. Capped at the maximum lifetime. 400 `Max instance length exceeded` once that has elapsed. 404 for paused sandboxes. |
| `POST /sandboxes/{id}/refreshes` | Extends to `now + duration`, never shortens. `duration` defaults to and is floored at 15 s, maximum 3600 s. |
| connect on a running sandbox | Extends to `now + timeout`, never shortens. |

A process in the guest can extend its own sandbox with its envd access token. See [Sandbox self-keepalive](../get-started/clients.md#sandbox-self-keepalive).

## Pause

```python
sbx.pause()                      # memory + filesystem
# Alternatively: sbx.pause(mode="filesystem")  # filesystem only
```

`POST /sandboxes/{id}/pause` answers 204. It answers 409 when the sandbox is already paused (the SDK then returns `False`) or is in another transition.

- **Memory pause** (`memory` true, the default): the VMM snapshots the guest. When a fork base is available, RAM is written as a delta against it, so the RAM capture scales with pages written since the fork; otherwise it is a full snapshot. Resume restores guest processes and memory, but GC4 tunnel connections are reset.
- **Filesystem pause** (`memory: false`, SDK `mode="filesystem"`): the guest agent requests a guest page-cache flush, the VM stops, and only the disk is kept. Resume cold-boots from the disk: processes and in-memory state are lost.
- **Sandboxes running VMs** (x86_64 nodes with nested virtualization give every sandbox `/dev/kvm`; see [Nested virtualization](../operate/kernel.md#nested-virtualization-devkvm)): while VMs run inside the sandbox (Intel: while any VM exists; AMD: while one is executing at that moment), a memory pause answers 409 with `error_code` `nested_virtualization_active` and the sandbox keeps running. Memory snapshots and forks answer the same. Stop the VMs and retry, or pause filesystem-only. A timeout with `autoPause` kills such a sandbox, because a failed auto-pause kills.

The paused state stays on the node that ran the sandbox. With `--object-store`, it is also uploaded in the background so another node can resume it. See [Pause artifacts](../operate/storage.md#pause-artifacts) and [Portable paused sandboxes](../operate/storage.md#portable-paused-sandboxes).

## Resume and connect

```python
sbx = Sandbox.connect(sandbox_id, timeout=300)             # resume if paused
# Alternatively: Sandbox.connect(sandbox_id, on_resume="reboot")  # cold boot if paused
```

```ts
const sbx = await Sandbox.connect(sandboxId, { timeoutMs: 300_000 })
```

| Endpoint | Running sandbox | Paused sandbox | Unknown or killed |
|---|---|---|---|
| `POST /v2/sandboxes/{id}/connect` | 200, extends the timeout only | resumes, 201 | 404 |
| `POST /sandboxes/{id}/connect` (v1) | same as v2; `timeout` required, non-positive values keep the current end | resumes, 201 | 404 |
| `POST /sandboxes/{id}/resume` (deprecated) | 409 | resumes, 201 | 404 |

- `memory: false` in the body (SDK `on_resume="reboot"`) cold-boots from the paused disk instead of restoring memory. A sandbox paused filesystem-only always cold-boots.
- A resume goes back to the node holding the paused state. If that node is gone and no object store holds a copy, the resume fails with 503. With an object store, another node of the same architecture resumes it. A node with the same backend and CPU baseline restores memory. Any other same-arch node cold-boots from the disk.
- A resume starts a new execution. It emits `sandbox.lifecycle.resumed`, and the maximum lifetime restarts from it.
- Resume counts against the concurrent-sandbox limit (429 at the limit).

## Auto-pause and auto-resume

The SDK's `lifecycle` option sets the create fields `autoPause`, `autoPauseMemory`, and `autoResume`:

```python
sbx = Sandbox.create("base", timeout=300,
                     lifecycle={"on_timeout": "pause", "auto_resume": True})
```

```ts
const sbx = await Sandbox.create('base', {
  timeoutMs: 300_000,
  lifecycle: { onTimeout: 'pause', autoResume: true },
})
```

| Field | Default | Effect |
|---|---|---|
| `autoPause` | `false` | Timeout pauses instead of kills. |
| `autoPauseMemory` | `true` | `false` makes the auto-pause filesystem-only. Requires `autoPause`, and cannot be combined with `autoResume` (400). |
| `autoResume.enabled` | `false` | Ingress traffic to a paused sandbox resumes it. Allowed without `autoPause`, so a manually paused sandbox also wakes on traffic. |

An explicit connect resumes any paused sandbox, whether or not `autoResume` is set. A traffic-triggered resume uses the create timeout raised to at least 300 s, capped at the team's maximum lifetime. `GET /sandboxes/{id}` reports the policy as `lifecycle: {autoResume, onTimeout: "pause"|"kill"}`. Ingress behavior: [Auto-resume](../operate/ingress.md#auto-resume).

## Fork

```python
children = sbx.fork(count=3, timeout=600)
for child in children:
    if isinstance(child, Exception):
        print("fork failed:", child)
```

`POST /sandboxes/{id}/fork` with `{timeout, count}` answers 201 with one entry per child: either `{sandbox}` or `{error}`.

- The source must be running. It is snapshotted once, with memory, into a hidden build. It is briefly paused, then resumed with its ID and end time unchanged.
- All children start on the source's node from that snapshot and inherit its files, env vars, metadata, lifecycle flags, network policy, and mounts. Processes are preserved only when the child can restore the memory snapshot; node-local volume mounts force a cold boot. Each child succeeds or fails on its own.
- `count` defaults to 1. It must be 1–100 and lower than the team's concurrent-sandbox limit (400 otherwise). The SDK documents 1–20. `timeout` defaults to 15 s.
- The hidden template is retired once the children have started. Its build stays while any running or paused child uses it, then artifact GC removes it.

## Snapshots

A snapshot turns a running sandbox into a template that new sandboxes start from.

```python
info = sbx.create_snapshot("my-snapshot:v1")     # or no name; mode="filesystem" for disk only
clone = Sandbox.create(info.snapshot_id)
Sandbox.delete_snapshot(info.snapshot_id)
```

- `POST /sandboxes/{id}/snapshots` with `{name?, memory?}` answers 201 `{snapshotID, names}`. The source is paused, snapshotted, and resumed, and keeps running. The capture emits `sandbox.lifecycle.checkpointed`.
- `name` is `[namespace/]alias[:tag]`. The namespace must be the team's slug (400 otherwise). An existing snapshot name owned by the team gets a new build under that tag. A name used by another kind of template answers 409.
- `snapshotID` is `<slug>/<alias>:<tag>` for named snapshots and `<templateID>:default` otherwise. Pass it, or the name, as the template of a create.
- `memory: false` (SDK `mode="filesystem"`) captures only the disk. Sandboxes created from it cold-boot.
- `GET /snapshots` lists the team's snapshots, filtered by `sandboxID` or `name` and paginated. Delete one with `DELETE /templates/{id}`.
- Snapshot builds live on the capturing node and are fetched by other nodes like any build. See [Templates](templates.md#deletion-and-artifact-gc).

## Kill

```python
sbx.kill()                       # or Sandbox.kill(sandbox_id)
```

`DELETE /sandboxes/{id}` answers 204. It kills a running sandbox and deletes paused state, including the object-store copy. It answers 404 when neither exists, and the SDK then returns `False`. The reply comes once the VMM is killed and the route is gone. Disk, network, and process cleanup finish in the background. Logs and metrics stay readable for 7 days.

## Metrics

Each node scrapes envd `GET /metrics` of every running sandbox every 5 s into the sandbox's `metrics.jsonl`. Samples are kept for 7 days after the sandbox ends.

```python
for m in sbx.get_metrics():                       # start/end default to the sandbox's lifetime
    print(m.timestamp, m.cpu_used_pct, m.mem_used)
```

- `GET /sandboxes/{id}/metrics?start&end` (Unix seconds) works for running, paused, and recently killed sandboxes. The samples are thinned to one per step: 5 s under 1 h, 30 s under 6 h, 60 s under 12 h, 120 s under 24 h, 300 s under 7 days, else 900 s.
- `GET /sandboxes/metrics?sandbox_ids=a,b` returns the latest sample (from the last 60 s) of up to 100 running sandboxes.
- `e2b sandbox metrics <id>` (`-f` to follow) prints the same samples.

## Logs

Sandbox logs are the VM's console output: kernel messages and the agent's and envd's JSON log lines. envd logs every process start and end as upstream does (`logger=process`, `event_type=process_start|process_end`). Logs stay readable for 7 days after a kill.

| Endpoint | Parameters |
|---|---|
| `GET /v2/sandboxes/{id}/logs` | `cursor`, `limit` (≤ 1000, default 1000), `direction` (`backward` default, `forward`), `level` (`debug`, `info`, `warn`, `error`), `search` (≤ 256 chars) |
| `GET /sandboxes/{id}/logs` | `start`, `limit`. Each `logs[].line` is the raw JSON entry. |

```sh
e2b sandbox logs <sandbox-id> --level info -f
```

`fields` values are strings, so numeric envd fields arrive as strings. See [E2B compatibility](../reference/e2b-compat.md#5-deliberate-deviations).

## Events

Every lifecycle change is recorded in the driver store (kept for 30 days) as an event of type `sandbox.lifecycle.created`, `killed` (with `kill_reason`: `request`, `timeout`, `admin`, or `orphaned`), `updated` (timeout or network change), `paused`, `resumed`, or `checkpointed`.

```sh
curl -s "$E2B_API_URL/events/sandboxes/$SANDBOX_ID?limit=50&orderAsc=true" -H "X-API-Key: $E2B_API_KEY"
```

`GET /events/sandboxes` (team) and `GET /events/sandboxes/{id}` accept `offset`, `limit` (1–100, default 10), `orderAsc`, and `types` (repeated or comma-separated).

## Webhooks

Webhooks deliver the same events to your HTTP endpoint. Manage them with `GET|POST /events/webhooks` and `GET|PATCH|DELETE /events/webhooks/{id}`. Inspect deliveries with `GET /events/webhooks/{id}/deliveries` and `GET /events/webhooks/{id}/stats`.

```sh
curl -sX POST "$E2B_API_URL/events/webhooks" -H "X-API-Key: $E2B_API_KEY" \
  -H 'Content-Type: application/json' -d '{"name": "lifecycle",
  "url": "https://hooks.example.com/e2b", "signatureSecret": "s3cret",
  "events": ["sandbox.lifecycle.created", "sandbox.lifecycle.killed"]}'
```

- `url` must be an absolute `http` or `https` URL (≤ 2048 chars). `events` is a non-empty subset of the six types. `signatureSecret` is 1–512 characters, stored encrypted.
- The raft-leader driver delivers events in order. Each event gets up to 3 attempts, 500 ms then 1 s apart, with a 10 s timeout each. After the last attempt, delivery moves on to the next event. Every attempt is recorded.
- Each request is a JSON `POST` with the headers `e2b-webhook-id`, `e2b-delivery-id`, `e2b-signature-version: v1`, and `e2b-signature`. The body is snake_case: `id`, `version`, `type`, `timestamp`, `event_category`, `event_label`, `event_data`, `sandbox_id`, `sandbox_execution_id`, `sandbox_template_id`, `sandbox_build_id`, `sandbox_team_id`, `events_ttl_days`.
- `e2b-signature` is the upstream scheme: unpadded standard base64 of `SHA-256(signatureSecret + body)`.

```python
import base64, hashlib, hmac

def verify(secret: str, body: bytes, signature: str) -> bool:
    expected = base64.b64encode(hashlib.sha256(secret.encode() + body).digest()).decode().rstrip("=")
    return hmac.compare_digest(expected, signature)
```
