# Architecture

How a vibemon deployment is put together: which processes run, where state lives, which ports they open, and what happens on the three requests that matter most. Read it before planning a topology. The normative design, with every internal detail, is [`ARCHITECTURE.md`](https://github.com/stencil-hq/vibemon/blob/main/ARCHITECTURE.md).

## Components

The `vmon` binary provides the host components; `vmon-agent` is a separate guest binary.

| Component | Runs as | Does |
|---|---|---|
| Driver | `vmon serve --role driver` | E2B REST API, authentication, placement, sandbox lifecycle and timeouts, build supervision, sandbox ingress proxy. Raft voter. |
| Node | `vmon serve --role node` | Hosts microVMs, builds templates, stores template artifacts, paused sandboxes and volumes, serves the node API and guest tunnels. |
| VMM | `vmon vmm …`, one process per VM | The virtual machine monitor (KVM on Linux, Hypervisor.framework on macOS). Normally spawned by a node; also usable directly. |
| Guest agent | `vmon-agent`, installed in the guest as `/usr/bin/envd` | Speaks GC4 to the node over a virtio-console port and serves an envd-compatible HTTP/Connect API on guest port `49983`. |

A process may run both roles (`--role node,driver`, the default). One such process is a complete **monolithic** deployment. A **distributed** deployment is any mix of driver-only, node-only, and combined processes. They find each other through a gossip memberlist seeded with `--join`. Topologies and flags: [Running vmon serve](../operate/serve.md#roles-and-topologies).

The guest boots its distro init: systemd on Debian, Ubuntu, Fedora, CentOS, Rocky, AlmaLinux and Arch, and OpenRC on Alpine. envd runs under that init as a service. Images without a matching `ID` or `ID_LIKE` distro profile use the agent as PID 1 instead. VMMs run in their own process group. The restarted node reconnects to VMMs that are still running.

## Where state lives

| Store | Holds | Owner |
|---|---|---|
| MySQL | Durable system of record: teams, users, API keys, templates, aliases, tags, builds and build logs, sandboxes (running, paused, killed), snapshots, volumes, secrets, lifecycle events, webhooks. | Drivers (`--mysql-url`). `vmon admin` writes to it directly. |
| Raft KV | Hot, cluster-coordinated state: the live sandbox catalog (`sbx/<id>`, the route to the owning node), per-team admission counters, per-sandbox and per-build locks, which nodes hold which build artifacts, node status overrides, `Idempotency-Key` records. | Replicated across drivers (raft voters). Node-only processes forward KV calls to a driver. Stored under `<data-dir>/raft/`. |
| Gossip metadata | Membership, liveness, roles, addresses, node capacity (arch, backend, CPUs, memory, running sandboxes, draining flag, features). | Every process. |
| Node disk | Template artifacts (`builds/<buildID>/`: rootfs + memory snapshot), running sandboxes' disk overlays and logs, paused sandboxes (`paused/<id>/`), OCI layer and step caches, volumes. | Each node, under `--data-dir`. |
| Object store (optional) | Published template builds, portable paused sandboxes, object-store volumes. | Nodes and drivers with `--object-store`. |

A paused sandbox has no KV route. It exists as a `paused` MySQL row plus the paused state on its node, or in the object store. Data layout and the object store: [Storage, object store, and volumes](../operate/storage.md). Cluster mechanics: [Clusters](../operate/cluster.md).

## Listeners

| Listener | Default | Who connects | Serves |
|---|---|---|---|
| `--listen` | `0.0.0.0:3000` | SDKs, CLI, browsers | E2B API and sandbox ingress (HTTP/1.1 + h2c). Driver only. |
| `--tls-listen` | off | Same | The same router over HTTPS (h2 / HTTP/1.1). |
| `--cluster-listen` | `0.0.0.0:7947` | Other vmon processes | Raft and KV (`/cluster/v1/*`), node API and guest tunnels (`/node/v1/*`). |
| `--gossip-listen` | `0.0.0.0:7946` | Other vmon processes | Gossip: UDP probes and TCP push-pull. |

The cluster listener and gossip are authenticated with the shared cluster secret: a bearer token on cluster HTTP, and an HMAC tag on every gossip packet. Never expose them publicly. Guest ports are never reached over the guest network. Ingress always goes through GC4 tunnels, so no inbound NAT is needed. Details: [Running vmon serve](../operate/serve.md#listeners).

The driver routes each public request by its Host header or its `E2b-Sandbox-*` headers:

1. Host `<port>-<sandboxID>.<domain>` → ingress to that sandbox port.
2. Headers `E2b-Sandbox-Id` and `E2b-Sandbox-Port` → ingress.
3. Requests without a sandbox target → the E2B API (including `/health` and the CLI login pages). Incomplete or malformed routing headers get 400.

See [Domains, TLS, and ingress](../operate/ingress.md#routing).

```text
 E2B SDK / e2b CLI
        │  HTTPS: api.<domain>, <port>-<id>.<domain>
        ▼
 ┌─────────────┐  raft (cluster HTTP)  ┌─────────────┐
 │   driver    │◄─────────────────────►│   driver    │──► MySQL
 └──────┬──────┘   gossip (all procs)  └─────────────┘
        │ cluster HTTP: node API, tunnels
        ▼
 ┌─────────────┐  spawn  ┌───────────┐  virtio-console (GC4)  ┌──────────────────────┐
 │    node     │────────►│ vmon vmm  │◄──────────────────────►│ guest: init + envd   │
 └─────────────┘         └───────────┘                        │ (vmon-agent, :49983) │
                                                              └──────────────────────┘
```

## Flow: creating a sandbox

`Sandbox.create()` sends `POST /v2/sandboxes`.

1. The driver authenticates the API key and resolves the template reference (template ID, `alias`, or `namespace/alias`, with an optional `:tag`) to a ready build. It then checks the build's vCPUs, memory, and disk against the team limits.
2. After selecting the initial node, it admits the sandbox with one KV transaction. The transaction checks the team's concurrent-sandbox limit and writes the route `sbx/<id>` plus the admission index entry. Over the limit, the create fails with 429.
3. Placement picks a node from the gossip members. Candidates are alive, not draining, of the build's architecture, with the required features. Nodes with free headroom and nodes that already hold the build's artifacts score higher. The driver calls the node's `POST /node/v1/sandboxes`. An unreachable or busy node is skipped and another is tried, up to 3 attempts; other node errors fail the create immediately.
4. If the node lacks the artifacts, it fetches them from a peer that holds the build, or from the object store. When a reusable memory snapshot and matching mount devices are available, it starts `vmon vmm` as a copy-on-write fork over a block overlay of the rootfs. Otherwise it cold-boots; node-local volume mounts always require a cold boot.
5. The node requests a guest kernel RNG reseed on memory restores (failures only warn), configures the network, and attaches the egress policy. It then calls envd `POST /init` through the tunnel with the env vars, the sandbox's envd access token, and the default user and workdir.
6. The driver records the MySQL row and a `sandbox.lifecycle.created` event. It returns 201 with `sandboxID`, `envdVersion`, `envdAccessToken`, `trafficAccessToken` (when public traffic is off), and `domain`.

Every create runs to completion even if the client disconnects. A failure after admission rolls back the route and the admission slot. Lifecycle details: [Sandboxes](sandboxes.md).

## Flow: an SDK call reaching envd

`sbx.commands.run("ls")` is a Connect RPC to envd on guest port `49983`.

1. The SDK sends it to `https://49983-<id>.<domain>`. When `E2B_SANDBOX_URL` is set, it sends it there with `E2b-Sandbox-Id` and `E2b-Sandbox-Port` headers. See [Connecting SDKs and the CLI](../get-started/clients.md#routing-modes).
2. Any driver accepts it. Ingress reads the route `sbx/<id>` from its local KV replica to find the owning node. If the sandbox is paused and has auto-resume, ingress resumes it first.
3. The driver opens `GET /node/v1/sandboxes/{id}/tunnel/49983` on the node's cluster listener (HTTP upgrade). It then proxies the request over that tunnel, streaming bodies and passing WebSocket upgrades through.
4. The node opens a GC4 `tcp_connect` stream to the agent over the VM's virtio-console port. GC4 is credit-flow-controlled, so streams do not block each other.
5. The agent connects to `127.0.0.1:49983` in the guest. envd checks the `X-Access-Token` header and runs the command.

The same path serves any guest port. For ports other than `49983`, ingress also enforces the traffic access token when `allowPublicTraffic` is false. envd's control paths (`/init`, `/freeze`, …) are not reachable through ingress. See [Domains, TLS, and ingress](../operate/ingress.md).

## Flow: building a template

`Template.build(...)` or `e2b template create` drives this sequence.

1. `POST /v3/templates` registers the template (or reuses the existing one for the alias) and creates a `waiting` build. It answers 202.
2. For every `COPY` step, the client asks `GET /templates/{id}/files/{hash}`. If the archive is missing, the client uploads it to the signed URL the driver returns. The driver streams each upload to the alive, non-draining node with the most free disk.
3. `POST /v2/templates/{id}/builds/{buildID}` validates the steps and picks a builder node. Builders are alive and not draining, of the parent template's architecture, else of the cluster's majority architecture. The most free capacity wins. The driver then calls `POST /node/v1/builds`.
4. The node pulls the OCI image and unpacks and provisions it inside a build VM. It runs the steps with the step cache, boots the result with its own init, runs the start and ready commands, and snapshots the VM.
5. Whichever driver holds the build's lease polls the builder every 500 ms. It copies log lines into MySQL, and on success records which node holds the artifacts and assigns the tags. Clients poll `GET /templates/{id}/builds/{buildID}/status`.

Pipeline details: [Templates](templates.md).

## Background work

The raft leader runs the cluster-wide loops:

| Loop | Interval | Does |
|---|---|---|
| Reaper | 1 s | Pauses (`autoPause`) or kills sandboxes past their end time. Kills sandboxes a dead driver left mid-transition. |
| Reconciler | 10 s | Compares each node's sandbox list with the KV routes. Marks sandboxes of dead nodes killed. Kills VMs that have no route. |
| Housekeeping | 60 s | Fails stuck snapshot builds and retires abandoned fork templates. Runs artifact GC for deleted templates. |
| Webhook delivery | 1 s | Delivers lifecycle events to team webhooks. |

Build supervision runs on every driver, one lease per build, so a dead driver's builds are adopted by another driver.
