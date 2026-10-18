# E2B compatibility matrix

Target: the official E2B SDKs (`e2b`, `@e2b/code-interpreter` /
`e2b_code_interpreter`, `@e2b/desktop` / `e2b_desktop`) and the `e2b` CLI work
unmodified against vibemon when pointed at it with `E2B_DOMAIN` /
`E2B_API_URL` / `E2B_SANDBOX_URL` / `E2B_API_KEY`.

Sources: [`spec/e2b/`](https://github.com/stencil-hq/vibemon/tree/main/spec/e2b)
(vendored upstream specs; provenance in `spec/e2b/SOURCES.md`). Design:
[`ARCHITECTURE.md`](https://github.com/stencil-hq/vibemon/blob/main/ARCHITECTURE.md).

Legend: **Core** is what the stripped core (`vmm` + `vmon-agent`) provided
before the rework. **Implementation** is what vibemon adds, and where. Proof
for each area is in §4a (official SDK suites), §6 (CLI),
[Distributed, Linux/KVM, and TLS verification](../develop/cluster-verification.md)
and [`e2e/README.md`](https://github.com/stencil-hq/vibemon/blob/main/e2e/README.md).

## 1. What the core VM gives us

| Capability | Core status | Implementation |
|---|---|---|
| Direct-kernel boot, virtio blk/net/console/fs/rng | ✅ | — |
| Guest kernel features E2B templates rely on (nf_tables + iptables legacy/nft, conntrack/NAT, IPv6, bridge/veth, cgroup v2 controllers, user namespaces, BPF/BTF, NFS/`rpc_pipefs`, Docker-in-sandbox) | ❌ in the cloud-hypervisor kernel: `nftables.service` and `run-rpc_pipefs.mount` fail, no Docker | ✅ with the vmon kernel (`kernel/`, `just kernel`), the node's default: `vmon-kernel-<arch>` from the release assets tarball next to `vmon`, or `target/kernel/` in a dev tree (the cloud-hypervisor download is only a last resort, with a WARN): `systemctl --failed` empty in `base` and a Docker template, `nft`, `iptables-legacy`/`-nft`/`ip6tables`, `ip -6`, `rpc_pipefs`, moby `check-config.sh` (nothing necessary missing), `docker run hello-world` + bridge NAT verified via the Python SDK on macOS/HVF (aarch64). x86_64 image boot-smoked under QEMU only; Linux/KVM end-to-end not yet run. |
| Pause / snapshot / restore / CoW fork (`--fork-from`; lazily filled 2 MiB hugetlb RAM on Linux when huge pages are available, else MAP_PRIVATE memory) | ✅ | Per-sandbox orchestration (node). |
| CoW rootfs overlay (`--disk-overlay-of`) | ✅ block-level overlay over a hard-linked raw base on any host filesystem (Linux, macOS); Windows full copy | Overlay clones reflink (`FICLONE`/`fclonefileat`) when possible, else copy only written extents (vmm). |
| Networking | ✅ Linux TAP; macOS/Windows libslirp user-net | Host TAP/NAT setup (node); egress policy (node proxy on Linux, VMM user-net packet filter on macOS). |
| Guest agent channel (GC4 over virtio-console) | ✅ exec, PTY, fs read/write/list/stat/mkdir/remove, net_config, mount | Byte-stream tunnels with flow control, OCI unpack, provisioning, poweroff, time set. |
| In-guest E2B `envd` (port 49983, Connect + REST) | ❌ | Implement in `vmon-agent` (§3). |
| Guest port reachability from host | ⚠️ TAP-only `TunnelSet` in the old server; none on macOS | GC4 `tcp_connect` tunnels (all platforms). |
| Control plane, auth, persistence, scheduling, ingress | ❌ (old server removed) | `driver` crate + raft KV (all durable state under `db/…`) + gossip. |
| Template build (OCI image → bootable rootfs → snapshot) | ❌ | Node build pipeline with in-guest OCI unpack (§4). |

## 2. Control-plane REST API (`spec/e2b/infra/spec/openapi.yml`)

Every operation is routed. "SDK" = called by the JS/Python SDKs, "CLI" =
called by the `e2b` CLI, "Admin" = operator-only (`X-Admin-Token`).

| Method | Path | Used by | Implementation |
|---|---|---|---|
| GET | `/health` | SDK | 204 when the driver can serve linearizable KV reads. |
| GET | `/teams` | CLI | User JWT → `[{teamID, name, apiKey, isDefault}]` (API key decrypted from `key_enc`). |
| GET | `/teams/{teamID}/metrics` | dashboard | From lifecycle event aggregates (concurrent sandboxes, start rate). |
| GET | `/teams/{teamID}/metrics/max` | dashboard | Same source, max over window. |
| GET | `/sandboxes` | deprecated | Running sandboxes of the team (KV catalog + store detail). |
| POST | `/sandboxes` | deprecated | Same handler as v2 with v1 defaults (`timeout` 15 s). |
| POST | `/v2/sandboxes` | SDK | Full `NewSandboxV2`: template/alias/tag resolution, team limits (KV txn), placement, node create, `/init`, envd + traffic tokens, `Idempotency-Key`. |
| GET | `/v2/sandboxes` | SDK | `state` (CSV), `metadata` filter, `order`, `startedAfter`, `template`, pagination, `X-Total-Running`. |
| GET | `/sandboxes/metrics` | SDK | Latest sample per ID from nodes. |
| GET | `/sandboxes/{id}/logs` | CLI | Node log store (console + agent logs); `logs[].line` is the raw JSON line (`{…fields, level, message}`) that `e2b sandbox logs` parses. |
| GET | `/v2/sandboxes/{id}/logs` | SDK | Cursor/direction/level/search over node log store. |
| GET | `/sandboxes/{id}` | SDK | Running → KV route + store record; paused → store record. |
| DELETE | `/sandboxes/{id}` | SDK | Kill running and/or delete paused state; 404 if neither. |
| GET | `/sandboxes/{id}/metrics` | SDK | Node `metrics.jsonl`, stepped by range. |
| POST | `/sandboxes/{id}/pause` | SDK | Node pause (memory snapshot, or disk-only when `memory=false`); 409 if paused. |
| POST | `/sandboxes/{id}/resume` | deprecated | Resume on the owning node; 409 if running. |
| POST | `/sandboxes/{id}/fork` | SDK | Snapshot source into hidden build, create `count` children on the same node. |
| POST | `/sandboxes/{id}/connect` | deprecated | As v2 connect. |
| POST | `/v2/sandboxes/{id}/connect` | SDK | Running → 200 (extend only); paused → resume, 201; unknown → 404. |
| POST | `/sandboxes/{id}/timeout` | SDK | Set `endAt` (may shorten) → 204. |
| PUT | `/sandboxes/{id}/network` | SDK | Replace egress policy live (node) → 204; 409 unless running. |
| POST | `/sandboxes/{id}/refreshes` | SDK | Extend by `duration` (default 15 s) → 204. |
| POST | `/sandboxes/{id}/snapshots` | SDK | Snapshot-as-template (`[ns/]alias[:tag]` names) → 201 `SnapshotInfo`. |
| GET | `/snapshots` | SDK | Team snapshot templates, filter by `sandboxID`/`name`, paginated. |
| POST | `/v3/templates` | SDK, CLI | Create/reuse template + new build (`waiting`) → 202. |
| GET | `/v2/templates` | SDK | Paginated templates. |
| GET | `/templates/{id}/files/{hash}` | SDK | `{present, url}`; signed PUT URL on the driver, stored then pushed to the builder node. |
| GET | `/templates` | CLI | Team templates (deprecated shape). |
| GET | `/templates/{id}` | SDK | Template + builds, paginated. |
| DELETE | `/templates/{id}` | SDK, CLI | Soft delete + artifact GC. |
| PATCH | `/templates/{id}` | deprecated | `public` toggle; bare aliases only for system teams (§5). |
| POST | `/v2/templates/{id}/builds/{buildID}` | SDK | Validate steps, place on a node, start build → 202. |
| PATCH | `/v2/templates/{id}` | CLI | `public` toggle → `TemplateUpdateResponse`; a system team's publish claims the bare aliases (§5). |
| GET | `/templates/{id}/builds/{buildID}/status` | SDK | Stored status + log page (`logsOffset`, `limit`, `level`). |
| GET | `/templates/{id}/builds/{buildID}/logs` | SDK | Cursor/direction/level/source log API. |
| POST | `/templates/tags` | SDK | Assign tags to a build. |
| DELETE | `/templates/tags` | SDK | Remove tags. |
| GET | `/templates/{id}/tags` | SDK | List tags. |
| GET | `/templates/aliases/{alias}` | SDK | 200 owner / 403 other team / 404. |
| GET | `/nodes` | Admin | Gossip members with `roles.node` + capacity. `machineInfo`: `cpuFamily` = hypervisor backend, `cpuModel` and `cpuModelName` = host CPU model name, `cpuArchitecture` = guest arch. |
| GET | `/nodes/{nodeID}` | Admin | Member detail + sandboxes + cached builds. |
| POST | `/nodes/{nodeID}` | Admin | `ready` / `draining` / `unhealthy` via KV `node/<id>/status`. |
| POST | `/admin/teams/{teamID}/sandboxes/kill` | Admin | Kill every team sandbox. |
| GET | `/admin/sandboxes/running-counts` | Admin | Counts from KV catalog. |
| GET | `/admin/sandboxes/{id}` | Admin | Team + state. |
| POST | `/admin/sandboxes/{id}/migrate` | Admin | vmon extension: offline migration of a running or node-local paused sandbox to `{"nodeID"}` (optional; else placed on a compatible node) → `{"sandboxID", "nodeID", "state"}`. |
| GET | `/admin/cluster` | Admin | vmon extension (dashboard): gossip members with host name, advertised IPs, roles, raft view, node capacity/CPU model/status/create counters/GPUs, plus 1 h of 5 s fleet samples. |
| GET | `/admin/teams` | Admin | vmon extension (dashboard): every team with limits and stored running/paused counts, by name. |
| GET | `/admin/sandboxes` | Admin | vmon extension (dashboard): every team's running/paused sandboxes (`ListedSandbox` + `teamID`, `teamName`, `nodeID`, `gpus`), newest first; `state`, `team`, `limit` ≤ 1000 (default 500), `nextToken`/`X-Next-Token`; `id` returns just that sandbox (`[]` when killed/unknown). |
| POST | `/admin/teams/{teamID}/builds/cancel` | Admin | Cancel team builds. |
| POST | `/admin/teams/{teamID}/api-keys` | Admin | Create key. |
| DELETE | `/admin/teams/{teamID}/api-keys/{id}` | Admin | Delete key. |
| GET/POST | `/api-keys` | dashboard | User JWT + `X-Team-ID`. |
| PATCH/DELETE | `/api-keys/{id}` | dashboard | Same. |
| GET/POST | `/volumes` | SDK | Store record + JWT volume token. With `--object-store`: the volume is the prefix `volumes/<id>/` in the object store, mountable on any node with feature `object-store` (write-back S3 mount); otherwise a node directory. |
| GET/DELETE | `/volumes/{id}` | SDK | Same; `DELETE` removes the whole prefix (object store) or directory. |
| GET/POST | `/secrets` | SDK | Encrypted versions in the store. |
| GET/POST/DELETE | `/secrets/{id}` | SDK | Same. |
| GET/PUT/DELETE | `/clusters/{clusterID}/rigs…` (5 ops) | Admin | **501** `Not implemented by this deployment` (the spec's documented answer for deployments without rigs/autoscaling). |
| GET | `/events/sandboxes`, `/events/sandboxes/{id}` | dashboard | Stored lifecycle events (kept 30 days). |
| * | `/events/webhooks…` (7 ops) | dashboard | Webhooks with signed (`e2b-signature` = base64 SHA-256(secret + body)) lifecycle event delivery by the raft leader, ≤ 3 attempts, delivery log/stats. |
| POST/DELETE | `/access-tokens…` | removed upstream | **410** with the upstream deprecation message. |
| POST | `/templates`, `/templates/{id}`, `/templates/{id}/builds/{buildID}` (legacy v1) | removed upstream | Not served (404), matching current upstream; current CLIs use v2. |

Volume-content API (`spec/e2b/sdk/spec/openapi-volumecontent.yml`):
`GET|PATCH|DELETE /volumecontent/{id}/path`, `GET|POST /volumecontent/{id}/dir`,
`GET|PUT /volumecontent/{id}/file` — volume JWT auth; object-store volumes are
served by the driver directly against S3 (same objects the mounts see),
node-local ones are proxied to the owning node. Point the SDK's
`E2B_VOLUME_API_URL` at the driver when not using `https://api.<domain>`.

## 3. envd (`spec/e2b/infra/packages/envd/spec/`)

| Surface | Core agent | Implementation (`vmon-agent` envd) |
|---|---|---|
| `process.Process/Start` (stream) | `exec` (no pid/tag/user/cwd defaults) | Process registry; `start{pid}`, `data{stdout,stderr,pty}`, `keepalive`, `end`; `Connect-Timeout-Ms` lifetime; users; PTY. |
| `Connect` (stream) | `pty_attach` only | Reattach by pid/tag; exited processes connectable for 30 s. |
| `List` | `pty_list` only | All tracked processes with config + tag. |
| `Update` (PTY resize) | `resize` | Same semantics. |
| `SendInput` / `StreamInput` / `CloseStdin` | STDIN frames | Implemented. |
| `SendSignal` | KILL frame | SIGTERM/SIGKILL by pid. |
| `filesystem.Filesystem/Stat`, `ListDir(depth)`, `MakeDir`, `Move`, `Remove` | partial (no move/depth/owner) | Full `EntryInfo` (owner, group, permissions, symlink target, mtime). |
| `WatchDir` (stream), `CreateWatcher`/`GetWatcherEvents`/`RemoveWatcher` | ❌ | inotify-backed watchers (recursive optional). |
| `GET /health`, `GET /envs`, `GET /metrics` | ❌ | Implemented. |
| `POST /init` | ❌ | Env vars, access token, default user/workdir, clock set, sandbox/template IDs, volume mounts, CA bundle. |
| `GET /files`, `POST /files`, `POST /files/compose` | `fs_read`/`fs_write` (buffered, no HTTP) | Streaming download (gzip), multipart + octet-stream upload, signatures, xattr metadata, compose. |
| `/freeze`, `/unfreeze`, `/fsfreeze`, `/fsthaw`, `/collapse` | ❌ | Implemented for the host pause pipeline; blocked on public ingress. |
| Auth (`X-Access-Token`, Basic user, `?username=`, signatures) | ❌ | Implemented. |

Transport: Connect v1 (JSON and proto; unary + enveloped server streams +
client stream for `StreamInput`), HTTP/1.1 + h2c.

## 4. Infrastructure behaviors

| Behavior | Implementation |
|---|---|
| `https://<port>-<id>.<domain>` and `E2b-Sandbox-Id/-Port` routing | Driver ingress (§9.5 of ARCHITECTURE.md). Optional TLS listener with operator-provided wildcard cert. Proven with the Python SDK (h2 via ALPN) and the JS SDK without `E2B_SANDBOX_URL`, against `--tls-listen` with a `*.vmon.test` cert ([TLS verification](../develop/cluster-verification.md#3-tls-and-true-subdomain-routing-no-e2b_sandbox_url)). Exception: the Python SDK's volume content API always targets `https://api.<domain>`. |
| 502 `was not found` / `port is not open` | Ingress. |
| Public envd control paths blocked | Ingress. |
| `allowPublicTraffic=false` + `e2b-traffic-access-token` | Ingress. |
| `maskRequestHost`, `httpsPorts` | Ingress. |
| Auto-pause on timeout, auto-resume on traffic/connect | Driver reaper + ingress. |
| Egress `allow_internet_access`, `allowOut` (CIDR/IP/domain), `denyOut`, private ranges | Linux: transparent egress proxy (Host/SNI domain match, re-dial by name, resolved IP must be public). macOS: VMM user-net filter; domains admitted only on TCP 80/443 to IPs learned from slirp-DNS (10.0.2.3) answers, TTL-bounded; DoH/other resolvers cannot unlock domains. |
| Egress `rules` (HTTPS header transforms), `egressProxy` (SOCKS5 ± TLS) | Linux egress proxy (MITM CA installed via `/init`). Nodes without the feature are not eligible. |
| Template build v2 (steps, file context, `fromImage`/`fromTemplate`/registry auth, start/ready cmd, step cache) | Driver build API + node pipeline. |
| Default templates `base`, `code-interpreter-v1`, `desktop`, `mcp-gateway` | `templates/` build scripts; `mcp-gateway` is an in-house image implementing the documented CLI contract. `code-interpreter-v1` proven on macOS/HVF (aarch64) with a non-system team's key: code-interpreter Python SDK suite 83/83, JS SDK suite 41/41 (Node 22.18.0, bun, deno, Cloudflare legs), template HTTP tests 86/86 including the R, Java (Temurin aarch64), JS/TS and bash kernels (`e2e/README.md` § code-interpreter). |
| MCP gateway: `Sandbox.create(mcp=…)`, `getMcpUrl()`, `getMcpToken()` (incl. after `connect()`), `Template().fromTemplate('mcp-gateway').addMcpServer([...])` | `templates/mcp-gateway/` (ARCHITECTURE.md §7): Docker engine in the guest + docker/mcp-gateway for Docker MCP Catalog servers (local images and remote servers), `github/<owner>/<repo>` stdio servers (`runCmd`/`installCmd`/`envs` and `run_cmd`/`install_cmd`), one Streamable HTTP endpoint on `:50005/mcp` behind the bearer token. Proven on HVF with the `kernel/` guest kernel: Python SDK + official `mcp` client through the TLS ingress (`https://50005-<id>.<domain>/mcp`) for `duckduckgo`, `fetch`, `context7` (remote) and the docs' `github/modelcontextprotocol/servers` example, tool calls included; JS SDK (camelCase `runCmd`/`envs`) with the in-sandbox `http://localhost:50005/mcp`; `addMcpServer` pre-pull; invalid configs fail `create` with `Failed to start MCP gateway: …`. |
| CLI login (`e2b auth login`) | Driver login page + JWT/refresh endpoints (§6). |
| `envdVersion`, `envdAccessToken`, `trafficAccessToken`, `clientID`, sandbox ID format | Driver. |
| Metrics (5 s envd scrape) and logs | Node stores; driver queries. |

## 4a. Proven by the official SDK suites

`e2e/run.py` runs upstream's unmodified `packages/python-sdk/tests` and
`packages/js-sdk/tests` (pinned to the `spec/e2b/` snapshot) against a
monolithic `vmon serve`. Final macOS/HVF run (2026-10-12): Python 1444 passed
/ 8 failed / 34 skipped, JS 1006 passed / 2 failed / 36 skipped; skips match
upstream CI. Remaining failures: egress header `rules` (Linux-only feature, 3)
and signed download URLs under the macOS Python routing workaround (6). The
run's one JS auto-resume regression was fixed afterwards in the ingress; a JS
re-run then gave 1007 passed / 1 failed (egress `rules`) / 36 skipped.
Details and the fixes made are listed in [`e2e/README.md`](https://github.com/stencil-hq/vibemon/blob/main/e2e/README.md).

## 5. Deliberate deviations

- **Bring-your-own-bucket mounts (`vmonS3Mounts`, extension).** `POST
  /sandboxes` and `POST /v2/sandboxes` accept
  `vmonS3Mounts: [{path, uri, mode?, endpoint?, region?, credentials?:
  {accessKeyId, secretAccessKey, sessionToken?}, credentialsSecret?}]`:
  `uri` is `s3://bucket[/prefix]` or `http(s)://endpoint/bucket[/prefix]`;
  `mode` is `ro` (default; lazy read-through), `overlay` (lazy lower +
  writes kept on the sandbox disk, gone at kill) or `writeback` (writes
  persisted to the bucket on `close`/`fsync`); `credentialsSecret` names a
  team secret whose value is the credentials JSON (re-read at every resume
  and fork). Paths must be absolute, clean, and must not overlap each other
  or `volumeMounts`; at most 8 S3-backed mounts per sandbox. Without
  credentials requests are anonymous (never the node's identity). Endpoints
  resolving to private/loopback addresses are refused unless the node runs
  with `--s3-mounts-allow-private`. Credentials are never logged or
  returned; the store keeps them only encrypted. Unmodified SDKs cannot send
  extension fields; use raw HTTP and then `Sandbox.connect`:

  ```sh
  curl -sX POST "$E2B_API_URL/v2/sandboxes" -H "X-API-Key: $E2B_API_KEY" \
    -H 'Content-Type: application/json' -d '{"templateID": "base",
    "vmonS3Mounts": [{"path": "/mnt/data", "uri": "s3://my-bucket/datasets",
      "mode": "ro", "credentialsSecret": "my-bucket-keys"}]}'
  ```

  Consistency: last writer wins per object, attributes are cached ≤ 1 s
  (write-back) / 5 s (ro, overlay), no cross-node locking; directory renames
  are copy + delete (not atomic, ≤ 10 000 keys, else `EXDEV`); symlinks and
  hard links are `EPERM`.

- **GPU sandboxes (extension).** Upstream E2B has no GPUs. Here a sandbox
  gets whole NVIDIA GPUs (or vGPU VFs) passed through when
  `metadata["vmon.gpus"]` is a decimal `0`–`8` (works with stock SDKs:
  `Sandbox.create(metadata={"vmon.gpus": "1"})`), else when its template's
  environment sets `VMON_GPUS` (`Template().set_envs({"VMON_GPUS": "1"})`
  or Dockerfile `ENV VMON_GPUS=1`), else it gets none; metadata wins, so
  `"0"` opts a GPU template out. Any other value is 400 naming the key.
  Placement needs a node with that many free assignable GPUs:
  `sandbox_no_compatible_node` when no node has that many at all,
  `sandbox_capacity_unavailable` while they are all held. GPU sandboxes
  always cold-boot, and **pauses and snapshots keep the filesystem only**:
  `pause()` (with or without `memory`), `autoPause`, and
  `create_snapshot()` stop or capture the disk, and the next resume (manual,
  `connect`, or `autoResume` traffic) boots it again with the GPUs
  re-reserved, losing guest processes and memory. The requested
  `autoPauseMemory`/`autoResume` flags are validated as upstream and kept as
  asked. GPU sandboxes are not migrated, so draining a node leaves them
  running there. The `cuda` default template (CUDA runtime, one GPU)
  is a vmon addition. See [GPUs](../operate/gpus.md).

- **Sandbox self-keepalive.** `X-Access-Token: <envdAccessToken>` also
  authenticates `GET /sandboxes/{id}`, `POST /sandboxes/{id}/timeout`, and
  `POST /sandboxes/{id}/refreshes`, only for the sandbox that owns that token.
  The token is compared in constant time before a sandbox lookup; wrong
  tokens, killed sandboxes, and unknown sandboxes return 401. It grants no
  access to create, list, kill, pause, connect, snapshots, or any other API
  route. Existing team blocking and maximum-lifetime rules still apply:
  timeout and refresh clamp expiry to the owning team's per-execution maximum,
  exactly as with API-key authentication. This is a vmon extension, not an
  upstream E2B capability.

- **Admin dashboard.** `/dashboard/` on the public listener and its
  `GET /admin/cluster`, `GET /admin/teams`, `GET /admin/sandboxes` endpoints
  (`X-Admin-Token`) are vmon extensions with no upstream counterpart.

- **Sandbox log fields are strings.** `SandboxLogEntry.fields` is
  `map<string,string>`, so non-string envd fields (`pid`, `process_result`)
  reach `e2b sandbox logs` as strings; upstream's v1 `line` keeps their JSON
  types.
- **Init system follows upstream provisioning.** Debian/Ubuntu/RHEL/Arch
  templates boot systemd as PID 1 and Alpine boots busybox init + OpenRC, with
  `vmon-agent` installed as the `envd` service at `/usr/bin/envd` (upstream
  package set and init setup, vendored under
  `spec/e2b/infra/packages/orchestrator/pkg/template/build/phases/`). Images
  with an unknown distribution, which upstream rejects, boot the agent as
  PID 1 instead. When the apt install fails on sources using `deb.debian.org`
  or `security.debian.org`, provisioning switches them to `archive.debian.org`
  and retries once, so end-of-life Debian releases still provision; upstream
  fails the build.
- **No Firecracker MMDS.** Identity (`E2B_SANDBOX_ID`, `E2B_TEMPLATE_ID`,
  access-token hash) is delivered through `/init`; there is no
  `169.254.169.254` service.
- **Legacy v1 template builds and the `docker.<domain>` registry** are not
  served; upstream removed them and current CLIs never call them.
- **MCP gateway internals are ours.** Upstream's `mcp-gateway` image is not
  public. Ours maps every SDK catalog id except `circleci` (now an OAuth-only
  remote server in the Docker catalog; its `token`/`url` fields have no
  counterpart) onto the Docker MCP Catalog v3. Catalog servers needing
  Docker Desktop OAuth, multi-line secret values, and per-server tool
  prefixes are not supported; on a tool-name clash the first configured
  server wins. `mcp-gateway --config` returns after at most 55 s; servers
  still starting then (runtime image pulls, long `installCmd`s) appear later
  with `notifications/tools/list_changed`. Needs a guest kernel that runs
  Docker (`kernel/`); the pinned cloud-hypervisor kernel download cannot.
- **Rigs** (cluster autoscaling objects) answer 501, the response the spec
  defines for deployments that do not implement them.
- **Bare aliases belong to system teams.** Upstream's v1 publish lets any team
  claim a bare (non-namespaced) alias. Here only a system team's public
  templates hold bare aliases (claimed on v1 or v2 publish), so the SDK
  defaults (`base`, `code-interpreter-v1`, `desktop`, `mcp-gateway`) resolve
  for every team and cannot be squatted; ordinary teams are addressed as
  `<slug>/<alias>` (ARCHITECTURE.md §7).
- **`minFreeDiskMb` is validated, not applied.** Builds accept it (0 to the
  team's `max_disk_mb`) and record it, but every build disk is already a
  sparse ext4 image of the team's `max_disk_mb`. Free space after the steps is
  that size minus what they wrote; upstream instead grows the filesystem to
  the requested free space and keeps it smaller otherwise.
- **Default templates are multi-arch.** `templates/` mirrors the upstream
  definitions but picks architecture-specific packages at build time (JDK 11:
  OpenJDK x64 / Temurin aarch64; desktop: Google Chrome on amd64, Chromium
  as `google-chrome-stable` on arm64; VS Code repo for the native arch).
- **vibemon branding** on cosmetic surfaces only: the default guest hostname
  is `vibemon` (upstream `e2b`), the `desktop` wallpaper and the noVNC page
  title/icons are vibemon's. Protocol identifiers (`E2B_*` env vars, `/.e2b`,
  `/run/e2b*`, `e2b/*` MIME types, `events.e2b.local`, `user`, aliases) are
  unchanged.

## 6. `e2b` CLI (`@e2b/cli` 2.21.2)

Configuration: `E2B_API_URL`, `E2B_DOMAIN`, and (without TLS on the sandbox
domain) `E2B_SANDBOX_URL`. Commands authenticate with `E2B_API_KEY` or with the
project key that `e2b auth login` stores in `~/.e2b/config.json`. The CLI opens
the login page at `${E2B_DOCS_BASE:-https://$E2B_DOMAIN/docs}/api/cli`. The
driver serves that page on any Host that does not address a sandbox, so `E2B_DOCS_BASE=http://<api>/docs` is
only needed when the apex domain does not resolve to the driver over HTTPS. The
CLI never reads `E2B_ACCESS_TOKEN`.

All commands below were proven against a single `vmon serve` on macOS/HVF,
driven by scripts (browser simulator; pexpect for the inquirer prompts and for
the PTY).

| Command | Status | Notes |
|---|---|---|
| `auth login` | ✅ | Login page → form POST → 302 to `http://localhost:<port>/?email&accessToken&refreshToken&tokenEndpoint&revokeEndpoint&clientId` → CLI 302s back to `?state=success` (rendered). `GET /teams` with the Bearer token picks the `isDefault` team. Wrong password → 401 form with error. Non-loopback `next` → 400. |
| `auth info` | ✅ | Local. |
| `auth configure` | ✅ | Lists every team of the user; selection is persisted. An expired access token is refreshed through `POST /auth/token` (rotation). A reused/revoked refresh token → `invalid_grant`, so the CLI deletes the config ("session has expired"). |
| `auth logout` | ✅ | `POST /auth/revoke`; the refresh token stops working. |
| `template init` (ts / python-sync) | ✅ | Generated `build.dev.ts` / `build_dev.py` build against vibemon. |
| `template create` (`-d`, `-c` + `--ready-cmd`, `--cpu-count`, `--memory-mb`) | ✅ | Dockerfile → v2 steps (`FROM`/`ENV`/`RUN`/`WORKDIR`/`COPY` with file upload/`USER`). Step cache works on rebuild. Start/ready commands run. |
| `template migrate` | ✅ | `e2b.toml` + Dockerfile → SDK code; the generated build script builds. |
| `template list` (`-t`, `-f json`) | ✅ | Foreign `--project` → 403; bad key → 401. |
| `template publish` / `unpublish` (`-y`) | ✅ | By alias or ID. Published templates are usable by other teams as `<slug>/<alias>`; after unpublish they get 403. |
| `template delete` (`-y`) | ✅ | A second delete returns 404. |
| `template build` | n/a | Removed from the CLI upstream (legacy V1 API, §5). |
| `sandbox create` / `spawn` (interactive; `-u`, `-c`, `--timeout`, `--lifecycle.*`) | ✅ | PTY: prompt, `tty`, initial size, SIGWINCH resize, `^C` → 130, UTF-8, `exit` → status 0, and the sandbox ends ~1 s later. `--lifecycle.ontimeout pause --lifecycle.autoresume` pauses at timeout and auto-resumes on connect. |
| `sandbox create -d`, `connect` | ✅ | Same PTY checks on `connect`. |
| `sandbox exec` (`-u`, `-c`, `-e`, `-b`, piped stdin) | ✅ | Exit codes propagate. |
| `sandbox list` (`-s`, `-f json`), `info`, `kill` (IDs, `-a`), `pause`, `resume` | ✅ | Unknown IDs → "wasn't found". |
| `sandbox snapshot create` / `list` / `delete`, `fork -n` | ✅ | Sandboxes created from a snapshot and fork children keep the files. |
| `sandbox logs` (`--level`, `--format json`, `--loggers`, `-f`) | ✅ | envd emits upstream `process_start` / `process_end` lines (logger `process`). Follow stops with "sandbox is closed" after a kill. Logs stay readable after the kill. |
| `sandbox metrics` (+ `-f`) | ✅ | 5 s samples. |
