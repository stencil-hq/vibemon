# Administration

Day-to-day operator tasks: creating teams, keys, and CLI users, setting limits, blocking teams, publishing default templates, watching the fleet in the admin dashboard, and using the admin HTTP API for keys, sandboxes, builds, and nodes. Every `vmon admin` flag is listed in [vmon admin commands](../reference/admin.md); the credentials themselves are explained in [Authentication and teams](../concepts/auth.md).

## Three tools

| Tool | Talks to | Use it for |
|---|---|---|
| `vmon admin …` | A running driver's cluster listener | Teams, users, memberships, limits, blocking, system teams, minting keys |
| Admin dashboard (`/dashboard/`) | A running driver, in a browser | Fleet capacity and activity charts; every team's sandboxes with a terminal, file browser, metrics, and logs; pause, resume, kill, timeouts; node draining |
| Admin HTTP API (`X-Admin-Token`) | A running driver | Listing, minting, and deleting keys; killing a team's sandboxes; cancelling builds; node status and draining |

## Running vmon admin

`vmon admin` needs the cluster listener of a running driver and the cluster secret:

```sh
export VMON_CLUSTER_URL=http://10.0.0.1:7947,http://10.0.0.2:7947   # drivers' --cluster-listen
export VMON_CLUSTER_SECRET=…        # the drivers' --cluster-secret
vmon admin list-teams
```

- **Cluster URL** (`--cluster-url`, `VMON_CLUSTER_URL`; default `http://127.0.0.1:7947`, the local driver): one or more driver cluster-listener URLs, comma-separated; requests go to the first that answers. The drivers keep all durable state in their raft KV, so `vmon admin` works only once a driver has started.
- **Cluster secret** (`--cluster-secret`, `VMON_CLUSTER_SECRET`): API keys are stored encrypted with AES-256-GCM under a key derived from the cluster secret (next to a SHA-256 hash used for lookups). `vmon admin` derives that key itself, so it must use the drivers' secret. When the flag is unset it reads `<data-dir>/cluster-secret` (`--data-dir`, `VMON_HOME`; default `/var/lib/vmon` as root on Linux, else `~/.vmon`), which is where `vmon serve` stores a generated secret. Running on a server host with the server's data dir therefore needs no secret flag. `vmon admin` never generates a secret; without one it fails with `no cluster secret: pass --cluster-secret (VMON_CLUSTER_SECRET) or --data-dir of a server (<path> missing)`.

Global options go before the subcommand. Commands that take a team accept its ID or its slug.

## First team and key

```sh
vmon admin create-team acme
```

```text
team    acme (6f0c3c1e-…)
slug    acme
api key e2b_…
```

The printed key (named `Default API key`) is shown once. Hand it to the team as `E2B_API_KEY`; client setup is in [Connecting SDKs and the CLI](../get-started/clients.md). Pass `--max-*` flags to `create-team` to start with non-default [limits](../concepts/auth.md#limits).

## More keys and rotation

Mint another key at any time; only the key is printed:

```sh
vmon admin create-api-key acme --name ci
```

There is no in-place rotation. Rotate by minting a new key, switching clients to it, then deleting the old one through the API (see [Keys](#keys)). Drivers cache key lookups for 30 seconds, so a deleted key can keep working on other drivers for up to that long.

## CLI login users

`e2b auth login` signs in with an email and password that you create:

```sh
VMON_ADMIN_PASSWORD='…' vmon admin create-user dev@example.com --team acme
```

`--team` makes the user a member with that team as default. The password must be at least 8 UTF-8 bytes; reading it from `VMON_ADMIN_PASSWORD` instead of `--password` keeps it out of the process list. After login, the CLI's `GET /teams` returns one API key per team for the user, minting a key named `CLI (<email>)` the first time.

Add an existing user to more teams:

```sh
vmon admin add-member dev@example.com other-team             # ordinary membership
vmon admin add-member dev@example.com other-team --default   # and make it the default
```

`add-member` on an existing membership rewrites its default flag: running it without `--default` clears that membership's default flag.

> [!NOTE]
> `vmon admin` has no commands to delete teams or users, remove members, or change passwords.

## Limits

```sh
vmon admin set-limits acme --max-concurrent-sandboxes 20 --max-length-hours 4
```

Only the flags you pass change; the others keep their current values. The command prints the team's updated line. What each limit enforces is in [Authentication and teams](../concepts/auth.md#limits). Drivers pick up new limits immediately for user-token and admin callers and within 30 seconds for API-key callers.

## Blocking a team

```sh
vmon admin block acme
vmon admin unblock acme
```

A blocked team can still read, but every mutating team-scoped request is 403 `team is blocked`. Running sandboxes keep running; to stop them, use [`POST /admin/teams/{teamID}/sandboxes/kill`](#sandboxes-and-builds). As with limits, API-key callers see the change within 30 seconds.

## System team for the default templates

The SDKs' default aliases (`base`, `code-interpreter-v1`, `desktop`, `mcp-gateway`) resolve for every team only when a system team publishes them. `just templates` (`templates/build.py`) builds and publishes them. When `E2B_API_KEY` is unset or empty, it runs `vmon admin` to find the team with slug `--team` (default `system`), marks it as a system team if needed or creates it with `create-team --system`, and mints a key; a supplied key is used without checking whether its team is a system team. `vmon admin` reads `VMON_CLUSTER_URL` and `VMON_CLUSTER_SECRET` / `VMON_HOME` from the environment as usual, and needs a running driver. Doing it by hand:

```sh
vmon admin create-team system --system      # or: vmon admin set-system <team>
```

`set-system` also claims free bare aliases for the team's already public templates; `unset-system` releases all of them. Build and publishing details are in [Templates](../concepts/templates.md).

## Admin dashboard

Every driver serves a web console at `/dashboard/` on its public listener (`--listen`, and `--tls-listen` when set), next to the API; `/` redirects there. Start the driver with `--admin-token` (`VMON_ADMIN_TOKEN`), open `http://127.0.0.1:3000/` (or `https://api.<domain>/`), and sign in with that token. The token stays in the browser's session storage (local storage with "Remember on this device") and is sent only as `X-Admin-Token` to the same origin.

| View | Shows | Actions |
|---|---|---|
| Overview | Running sandboxes, node health, vCPU and memory allocation, template builds; charts of running sandboxes, starts per minute, vCPU, and memory; cluster members with roles and raft state | None |
| Sandboxes | Running and paused sandboxes of every team, filterable by state, team, and text (ID, template, metadata) | New sandbox (any team, a template that team can access); pause, resume, kill |
| Sandbox | Team, node, template, size, lifetime, lifecycle settings, metadata | Terminal (a login shell, as the template's user or root), file browser (navigate, preview, download, upload by drag and drop, rename, delete, new folder), metrics, logs, pause, resume, kill, set timeout |
| Nodes | Host name, status, platform, sandboxes, vCPU and memory allocation, free disk, create outcomes; per node its sandboxes, features, and host details (CPU model, advertised IPs) | Mark ready, drain, mark unhealthy |
| Teams | Limits and running/paused counts | Kill all of a team's sandboxes |

The terminal and the file browser reach the sandbox through the driver's ingress with the sandbox's envd access token, exactly like an SDK, so they work on any node of the cluster. Charts come from the serving driver's in-memory fleet sampler (one sample every 5 seconds, last hour), so they start empty after a driver restart and can differ slightly between drivers. Node create counters are per driver as well.

> [!WARNING]
> The admin token grants every team's sandboxes, including shells and files. Serve the dashboard over TLS (`--tls-listen`) whenever the driver is reachable from other hosts.

## Admin HTTP API

Admin routes are served by every driver on its public listener and require `X-Admin-Token` equal to that driver's `--admin-token` (`VMON_ADMIN_TOKEN`). Without `--admin-token` they answer 401. Team IDs in paths and in `X-Team-ID` are UUIDs (`vmon admin list-teams`, first column), not slugs.

```sh
API=https://api.example.com          # or http://127.0.0.1:3000
AUTH="X-Admin-Token: $VMON_ADMIN_TOKEN"
TEAM=6f0c3c1e-…
```

### Keys

| Method | Path | Result |
|---|---|---|
| `POST` | `/admin/teams/{teamID}/api-keys` | Body `{"name": "…"}` (trimmed, 1–255 UTF-8 bytes). 201 with the key object including the plaintext `key`. |
| `DELETE` | `/admin/teams/{teamID}/api-keys/{keyID}` | 204; 404 when the team has no such key. |
| `GET` | `/api-keys` with `X-Team-ID` | The team's keys (ID, name, mask, `createdAt`, `createdBy`, `lastUsed`), no plaintext. `lastUsed` is stamped at most once a minute. |
| `PATCH` | `/api-keys/{keyID}` with `X-Team-ID` | Rename: body `{"name": "…"}`. |
| `DELETE` | `/api-keys/{keyID}` with `X-Team-ID` | Same as the admin delete. |

```sh
curl -s -H "$AUTH" -H "X-Team-ID: $TEAM" "$API/api-keys"
curl -s -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"name": "ci-2"}' "$API/admin/teams/$TEAM/api-keys"
curl -s -X DELETE -H "$AUTH" "$API/admin/teams/$TEAM/api-keys/<keyID>"
```

`X-Admin-Token` with `X-Team-ID` also authenticates the team routes of the E2B API (all except `POST /api-keys` and `GET /teams`, which need a user token), so an operator can act as a team, for example list its sandboxes or templates, without one of its keys.

### Sandboxes and builds

| Method | Path | Result |
|---|---|---|
| `GET` | `/admin/sandboxes/running-counts` | `{"<teamID>": <count>, …}` from the cluster's admission index |
| `GET` | `/admin/sandboxes/{sandboxID}` | `{"sandboxID", "teamID", "state"}` with `state` `running`, `paused`, or `killed` |
| `POST` | `/admin/sandboxes/{sandboxID}/migrate` | vmon extension. Moves a running sandbox, or a paused one whose state is only on its node, to another node with the same ID, processes, memory, and disk. Optional body `{"nodeID": "<16 hex>"}`; without it the scheduler picks a compatible node. 200 `{"sandboxID", "nodeID", "state"}` (`state` stays `running` or `paused`); 404 unknown or killed; 409 while another operation runs or when the paused state is already in the object store; 503 `sandbox_no_compatible_node` / `sandbox_capacity_unavailable` without a target |
| `POST` | `/admin/teams/{teamID}/sandboxes/kill` | Kills every sandbox of the team: `{"killedCount", "failedCount"}` |
| `POST` | `/admin/teams/{teamID}/builds/cancel` | Cancels the team's unfinished template builds: `{"cancelledCount", "failedCount"}` |

```sh
curl -s -H "$AUTH" "$API/admin/sandboxes/running-counts"
curl -s -X POST -H "$AUTH" "$API/admin/teams/$TEAM/sandboxes/kill"
curl -s -X POST -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"nodeID": "<nodeID>"}' "$API/admin/sandboxes/<sandboxID>/migrate"
```

The five `/clusters/{clusterID}/rigs…` operations (upstream's autoscaled node pools) check the admin token and then answer `501 Not implemented by this deployment`, the spec's response for deployments without rigs.

### Dashboard data

The dashboard reads three vmon-specific routes, usable from scripts as well:

| Method | Path | Result |
|---|---|---|
| `GET` | `/admin/cluster` | Cluster ID, the serving driver's ID, every gossip member (host name, advertised IPs, the cluster URL this driver reaches it at, state, roles, version, raft view, node capacity, CPU model and status, create counters, GPUs with their holder or problem), and `history`: up to one hour of 5 s fleet samples (nodes, sandboxes, builds, vCPU, memory, starts) |
| `GET` | `/admin/teams` | Every team with limits, flags, and running/paused sandbox counts |
| `GET` | `/admin/sandboxes` | Running and paused sandboxes of every team, newest first, with `teamID`, `teamName`, `nodeID`, and `gpus` (count). Query: `state` (`running`, `paused`, or both), `team`, `id` (one sandbox), `limit` (1–1000, default 500), `nextToken` (from `X-Next-Token`) |

```sh
curl -s -H "$AUTH" "$API/admin/sandboxes?state=running&limit=50"
```

### Nodes

| Method | Path | Result |
|---|---|---|
| `GET` | `/nodes` | Every gossip member with the node role, sorted by ID: version, `clusterID`, machine info (`cpuModel` and `cpuModelName` are the host CPU model), `status`, sandbox counts, capacity metrics, create successes/failures |
| `GET` | `/nodes/{nodeID}` | The same plus the node's sandboxes (when the node is alive) and `cachedBuilds` |
| `POST` | `/nodes/{nodeID}` | Body `{"status": "ready" \| "draining" \| "unhealthy"}`; 204. `draining` also migrates the node's sandboxes to other nodes ([Draining a node](#draining-a-node)) |

Node IDs are 16 lowercase hex characters. The reported `status` is derived in this order:

1. Gossip liveness: a dead member is `unhealthy`, one that left is `shutting_down`, a suspect one is `connecting`.
2. The operator status set with `POST /nodes/{nodeID}`, stored in the raft KV (`node/<id>/status`).
3. `draining` when the node itself announces that it is draining.

`connecting`, `standby`, and `shutting_down` cannot be set (400).

## Draining a node

1. Find the node: `curl -s -H "$AUTH" "$API/nodes"`.
2. Mark it draining:

   ```sh
   curl -s -X POST -H "$AUTH" -H 'Content-Type: application/json' \
     -d '{"status": "draining"}' "$API/nodes/<nodeID>"
   ```

   The scheduler places new sandboxes and builds only on `ready` nodes, and the driver starts evacuating the node: every few seconds it migrates the node's running sandboxes with more than a minute left, two at a time, plus its paused sandboxes when the node keeps paused state only locally (without an object store). A migrated sandbox keeps its ID, processes, memory, and disk; the move pauses it, transfers its written disk and dirtied memory to the target, and resumes it there, so the blackout grows with that amount. Connections open during the move drop; clients reconnect to the same sandbox on the new node.

   Each sandbox needs a compatible `ready` node: same architecture, hypervisor backend, and CPU baseline, enough capacity, the features its network settings need, and no volume on node-local storage pinning it to the draining node. Sandboxes without one stay where they are and are retried every minute (the driver logs why). Sandboxes with less than a minute left expire in place. One sandbox can be moved by hand with `POST /admin/sandboxes/{sandboxID}/migrate`.
3. Watch `sandboxCount` in `GET /nodes/<nodeID>` fall as sandboxes move or end.
4. Return it to service with `{"status": "ready"}`, which deletes the stored override.

`unhealthy` has the same placement effect but does not evacuate. What happens to paused sandboxes, failover, and node shutdown is covered in [Clusters](cluster.md#node-status).
