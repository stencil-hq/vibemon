# Clusters

A distributed deployment runs separate driver and node processes that find each other through gossip. The drivers replicate all state, live and durable, in a raft KV; there is no external database. This page covers how to start such a cluster, how work is placed, and what happens when processes fail. For a single host, see [Running vmon serve](serve.md).

## Shape

```text
            clients (SDKs, CLI, browsers)
                       │  --listen / --tls-listen
       ┌───────────────┼───────────────┐
   driver d1       driver d2       driver d3     raft voters, E2B API, ingress
       │ ╲             │             ╱ │
       │   ╲  raft KV (all state)      │
       │                               │
   ──────── gossip :7946 (every process) ────────
   ──────── cluster HTTP :7947 (raft, KV, node API, tunnels) ────────
       │               │               │
    node n1         node n2   …     node nN       VMs, builds, artifacts
```

| Layer | Members | Holds |
|---|---|---|
| Gossip memberlist (SWIM) | every process | Membership, liveness, per-member metadata: roles, addresses, capacity, features, cached builds, raft status. |
| Raft KV (openraft) | voters = drivers; node-only processes and `vmon admin` are clients | Live sandbox routes, admission counters, locks, artifact locations, admin node status, and, under `db/…`, the durable records: teams, users, API keys, refresh tokens, templates, aliases, tags, builds and build logs, sandboxes, events, webhooks and deliveries, secrets, volumes. |

All processes need the same `--cluster-secret`. All drivers should share `--domain`, `--admin-token`, `--shared-volumes`, and `--object-store` so that every driver behaves the same.

## Gossip

Every process joins gossip on `--gossip-listen` (default `0.0.0.0:7946`, UDP and TCP). Pass the seeds with `--join host:port,host:port`. Seed host names are resolved through DNS, and the process's own address is skipped, so you can pass the same seed list to every process. Listing all drivers is a good choice. If no seed answers at startup, the process logs a warning and retries every 2 s until one does.

Peers reach a process at its advertised addresses:

- `--gossip-advertise` (`ip:port`), defaulting to the detected IP with the `--gossip-listen` port.
- `--cluster-advertise` (a URL), defaulting to `http://<detected-ip>:<cluster-listen port>`.

The detected IP is the `--cluster-listen` IP when it is specific, or else the host's outbound IPv4 address. Set both advertise flags on multi-homed hosts, in containers, and behind NAT.

Failure detection: each member probes one peer per second. An unanswered probe (500 ms timeout, then 3 indirect probes) marks the peer `Suspect`, and it becomes `Dead` after `5 × max(1, ⌈log10(n+1)⌉)` seconds. A process that shuts down cleanly announces `Left` instead. Every 30 s, members exchange full state with a random peer (and with a random dead one), which heals partitions. Packets are authenticated with an HMAC keyed from the cluster secret, and unauthenticated ones are dropped.

## Raft

Drivers are the raft voters. Raft state lives in `<data-dir>/raft/`: every driver holds the whole KV in memory and snapshots it there. Sandbox events, webhook deliveries, build logs, and records of killed sandboxes expire after 7 days, which bounds its size.

- **Bootstrap:** start the initial drivers with `--bootstrap-expect N`, where N is the number of initial drivers. A driver with uninitialized raft waits until gossip shows N alive drivers that have no cluster ID yet. The driver with the smallest node ID among them then initializes raft with exactly those N voters.
- **Joining later:** a driver that sees any peer with a cluster ID never bootstraps. The leader reconciles membership every 2 s, adding new alive drivers as learners and promoting them to voters once they have caught up.
- **Removal:** a voter that has been `Dead` or `Left` in gossip for longer than `--raft-reap-after` seconds (default 600) is removed. The voter set never drops below 1.
- **Node-only processes** run no raft. Their KV calls go to a live driver over cluster HTTP, and they retry the next driver when a connection fails. Followers forward writes and consistent reads to the leader.

> [!WARNING]
> `--bootstrap-expect` defaults to 1. A lone driver with the default bootstraps as soon as it sees itself, so if several drivers start with the default before gossip connects them, more than one raft cluster can form. Give every initial driver the same `--bootstrap-expect`. It only matters while `raft/` is empty.

Raft needs a majority of voters to elect a leader and commit writes, so three drivers tolerate the loss of one. KV calls wait up to about 8 s for a reachable leader and then fail. Ingress reads sandbox routes from the driver's local replica. API reads of durable records are linearizable on every driver: a follower first fetches the leader's applied revision and waits until its replica has applied it. Writes are optimistic KV transactions, guarded on the revisions of the records they read and rerun on a conflict.

## Placement

The driver that handles a create picks the node (`driver/src/scheduler.rs`):

1. **Filter:** alive node members only, excluding nodes that are draining or unhealthy. The node's architecture must match the build's, and it must have the required features: `egress-proxy` for egress `rules` and `egressProxy`, `egress-domains` for domain allow entries when no proxy is needed, and `object-store` for object-store volumes and relocated resumes. A volume on node-local storage (no `--shared-volumes`, no object store) pins the sandbox to the node that holds the volume. Capacity: allocated vCPUs up to 4 × advertised CPUs, allocated memory up to 2 × advertised memory, and at least 512 MiB free disk.
2. **Score:** free vCPU and memory headroom. Add 1.0 when the node already holds the build's artifacts, and another 0.5 when its snapshot is reusable there (same backend and CPU baseline, so the create is a copy-on-write fork). Subtract 0.25 per create already in flight on that node from this driver.
3. **Pick:** two random candidates from the top half; the one with the higher score wins. If the node is busy or unreachable, the driver tries the next candidate, up to 3 attempts in total.

When nothing fits, the create fails with 503: `sandbox_no_compatible_node` when no node matches, or `sandbox_capacity_unavailable` when matching nodes are full. Template builds go to alive nodes that are not draining, with the parent template's architecture, else the majority architecture (ties go to `x86_64`). Their score is free memory (MiB) plus 1024 per free advertised vCPU, minus 2048 per running build; higher scores come first. Advertised capacity is set with `--max-vcpus` and `--max-memory-mb`; see [Capacity](serve.md#capacity).

## Node status

Node status combines gossip liveness with an admin override stored in KV (`node/<id>/status`). `GET /nodes` and `GET /nodes/{nodeID}` return it. Both require `X-Admin-Token`, and the detail view also lists the node's sandboxes and cached builds.

| Status | Meaning | New sandboxes and builds |
|---|---|---|
| `ready` | Alive, no override. | yes |
| `draining` | Admin override. | no |
| `unhealthy` | Admin override, or gossip `Dead`. | no |
| `connecting` | Gossip `Suspect`. | no (placement uses `Alive` members only) |
| `shutting_down` | Gossip `Left`. | no |

Set the override with `POST /nodes/{nodeID}` on any driver:

```sh
curl -X POST "https://api.$DOMAIN/nodes/$NODE_ID" \
  -H "X-Admin-Token: $VMON_ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"status": "draining"}'        # 204; "ready" removes the override
```

Only `ready`, `draining`, and `unhealthy` can be set. Other values get 400, and an unknown node gets 404. A draining node keeps running its sandboxes. With an object store, resumes of sandboxes that were paused on it move to other nodes. Without one, a node-local pause can only resume on the node that holds it. For the operator workflow, see [Administration](admin.md).

## Failure handling

The raft leader runs the cluster-wide loops: the reaper (every 1 s: timeouts and auto-pause, and sandboxes that a dead driver left mid-transition), the reconciler (every 10 s), housekeeping and artifact GC (every 60 s), and webhook delivery. Build supervision is spread across drivers through 20 s KV leases.

| Event | Behaviour |
|---|---|
| Driver (leader) killed | The surviving drivers elect a new leader, which takes over the leader-only loops. The other drivers keep serving the API and ingress. Builds supervised by the dead driver are adopted once their lease lapses. After `--raft-reap-after`, the dead driver is removed from the voters. A restarted driver rejoins with its `raft/` state and does not bootstrap again. |
| Node crashes or is unreachable (gossip `Dead`) | Its routes count as lost on the next reconciler pass. The reconciler marks its sandboxes killed with a `killed` event whose reason is `orphaned`. New sandboxes go to other nodes. If the node comes back, VMMs it still runs without a route are killed. |
| Node stopped with SIGTERM (gossip `Left`) | VMMs keep running. If the node rejoins within 60 s, it adopts surviving VMMs. A Linux sandbox with lazily restored memory loses its page server and fails its next remote page fault. After 60 s its routes are treated as lost, as for a crash. |
| Running sandbox's node lost | The sandbox is gone. Only paused state can move between nodes (next section). |
| Builder node lost during a build | The build fails. |

[Distributed, KVM, and TLS verification](../develop/cluster-verification.md) describes the gated test that exercises these cases with 3 drivers and 2 nodes.

## Artifacts and portability

Each node keeps template build artifacts (rootfs and memory snapshot) in `<data-dir>/builds/`, and drivers record which nodes hold each build (`art/<buildID>/<nodeID>`). When a sandbox lands on a node without the build, the node fetches it from a holder over cluster HTTP. The transfer is extent-aware, so sparse disks stay sparse, and it is verified by SHA-256. A same-architecture node whose backend or CPU baseline differs from the holder's fetches only the rootfs and regenerates the template snapshot locally.

Without `--object-store`, all state is tied to nodes: builds survive only while some node holds them, and a paused sandbox resumes only on its own node (503 while that node is unavailable). With `--object-store` on every node and driver:

- finished builds are also published to the store, so sandboxes can start from a template after every node that held it is gone;
- paused sandboxes are uploaded in the background and resume on another compatible node when their own node is gone or draining (a memory restore with the same backend and CPU baseline, else a rootfs-only cold boot);
- volumes live in the store and mount on any node.

Details are in [Storage, object store, and volumes](storage.md).

## Example: 3 drivers and N nodes

Hosts: drivers `10.0.0.1`–`10.0.0.3`, nodes `10.0.1.1`…`10.0.1.N`, and an S3 bucket. Generate the cluster secret once (for example `openssl rand -hex 32`) and set the same environment on every host:

```sh
export VMON_CLUSTER_SECRET='<64 hex chars>'
export VMON_JOIN='10.0.0.1:7946,10.0.0.2:7946,10.0.0.3:7946'
export VMON_OBJECT_STORE='s3://vmon-state/prod'    # credentials from the AWS environment or role
```

Each driver (here `10.0.0.1`):

```sh
export VMON_ADMIN_TOKEN='<admin token, same on every driver>'

vmon serve --role driver \
  --bootstrap-expect 3 \
  --cluster-listen 10.0.0.1:7947 \
  --gossip-listen 10.0.0.1:7946 \
  --domain sbx.example.com \
  --tls-listen 0.0.0.0:443 \
  --tls-cert /etc/vmon/tls/fullchain.pem \
  --tls-key /etc/vmon/tls/privkey.pem
```

Each node (here `10.0.1.1`, running as root on Linux/KVM):

```sh
vmon serve --role node \
  --cluster-listen 10.0.1.1:7947 \
  --gossip-listen 10.0.1.1:7946 \
  --vmm-user 64000:64000
```

Binding the internal listeners to the host's private IP also makes it the advertised address. Put the drivers' public listeners behind DNS or a load balancer for `api.sbx.example.com` and `*.sbx.example.com` (see [Domains, TLS, and ingress](ingress.md)). Then check that `GET /nodes` on any driver lists every node as `ready`:

```sh
curl -s https://api.sbx.example.com/nodes -H "X-Admin-Token: $VMON_ADMIN_TOKEN"
```

Run `vmon admin` against the drivers' cluster listeners with the same `VMON_CLUSTER_SECRET`:

```sh
VMON_CLUSTER_URL=http://10.0.0.1:7947,http://10.0.0.2:7947,http://10.0.0.3:7947 vmon admin list-teams
```
