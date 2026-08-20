# Sandboxes

A sandbox is a daemon-owned guest managed by a running `vmon serve` daemon or mesh. SDK sandbox objects are bound handles to that server-side resource, not local VM runtimes. They carry a stable ID, cache the latest known server view, and expose lifecycle actions and guest services.

For endpoint selection and client ownership, see [Connect](connect.md). For daemon, transport, and protocol errors, see [Shared Concepts](shared-concepts.md), [Connection Strings and Contexts](connection-strings.md), and [Error Codes](../reference/errors.md).

## Create a sandbox

Creation sends a request to the daemon and returns a stable-ID handle whose mesh routing remains private to the SDK. The daemon decides which images, templates, architectures, and resource combinations it accepts. Environment values are for non-secret configuration; use the SDK's secret and volume request values where appropriate. See [Volumes and Secrets](volumes-and-secrets.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Select one creation source with `image`, `template`, or `dockerfile` plus `context`, as appropriate for the daemon. Python accepts optional `name`, `cpus`, `memory`, `disk_mb`, `timeout`, `workdir`, `env`, `secrets`, `credentials`, volume mounts, tags, and a startup command. `credentials` contains host-brokered names only; see [Volumes and Secrets](volumes-and-secrets.md#host-brokered-credential-names).

```python
from vmon import Secret
import vmon

with vmon.connect() as client:
    work = client.volumes.create("work")
    sandbox = client.sandboxes.create(
        image="alpine:latest",
        name="worker",
        cpus=2,
        memory=1024,
        disk_mb=2048,
        timeout=600,
        workdir="/workspace",
        env={"APP_ENV": "production"},
        secrets=[Secret.from_env("API_TOKEN")],
        volumes={"/workspace": work},
        ports=[8080],
        tags={"service": "build"},
        command=["sh", "-lc", "sleep 600"],
    )
```

The request also supports `timeout_secs`, `fs_dir`, `block_network`, `egress_allow`, `egress_allow_domains`, `inbound_cidr_allowlist`, `readiness_probe`, `pool_size`, `ha`, `arch`, and `idempotency_key`. These configure the daemon request, not the local Python runtime.

</div>
<div data-sdk-language="go">

`SandboxCreateRequest` carries the image or template choice and optional resource, environment, network, port, and storage settings. `TimeoutSeconds` is the sandbox idle timeout; it is distinct from `Timeout`, the create-request timeout in seconds. Use `Secrets` for secret bundles, `Credentials` for host-brokered credential names, and `S3Mounts` for S3-backed guest filesystems.

```go
idleTimeout := uint64(900)
ctx := context.Background()

sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
    Image:          "alpine:3.21",
    Name:           "build-agent",
    CPUs:           2,
    MemoryMiB:      1024,
    DiskMiB:        4096,
    TimeoutSeconds: &idleTimeout,
    Workdir:        "/workspace",
    Env:            map[string]string{"CI": "true"},
    Ports:          []uint16{8080},
    Tags:           map[string]string{"purpose": "build"},
})
if err != nil {
    return err
}
fmt.Printf("%s is %s on %s\n", sandbox.ID, sandbox.Status, sandbox.Node)
```

All Go operations take a `context.Context`. Cancellation or expiry ends the in-flight operation and returns the context error where applicable; use per-operation deadlines for bounded requests.

</div>
<div data-sdk-language="typescript">

TypeScript sends a `SandboxCreateRequestWithSecrets`. It accepts fields including `image`, `command`, `env`, `workdir`, resource values, `ports`, `volumes`, `credentials`, tags, and creation-time network fields such as `block_network`, `egress_allow`, and `egress_allow_domains`.

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect("vmon://127.0.0.1:7777");
const sandbox = await client.sandboxes.create({
  image: "alpine",
  command: ["sh", "-lc", "sleep 600"],
  cpus: 1,
  memory: 512,
  ports: [8080],
  tags: { service: "preview" },
});

console.log(sandbox.id, sandbox.node, sandbox.info);
```

</div>
</div>

## Get, reference, and list

Listing queries live mesh endpoints, merges successful results, and de-duplicates entries by sandbox ID. Results may be filtered by exact tag pairs.

Handles returned by creation, lookup, and listing retain a private, stable-ID endpoint resolver that handles transparent re-routing in a mesh. See [Shared Concepts](shared-concepts.md#resource-handles-and-namespaces).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
with vmon.connect() as client:
    current = client.sandboxes.get("sandbox-id")
    deferred = client.sandboxes.ref("sandbox-id")  # no request here
    builds = client.sandboxes.list(tags={"service": "build"})
```

The latest typed `SandboxInfo` is available as `sandbox.info`; `sandbox.node` identifies the reporting mesh node when present. Call `refresh()` to fetch and update it.

</div>
<div data-sdk-language="go">

`Ref` constructs a local handle. `Refresh` updates that same pointer and returns it, so every caller holding the handle observes the refreshed fields.

```go
sandbox := client.Sandboxes.Ref("sandbox-id")
if _, err := sandbox.Refresh(ctx); err != nil {
    return err
}
if sandbox.Status != "running" && sandbox.Status != "ready" {
    return fmt.Errorf("sandbox %s is %s", sandbox.ID, sandbox.Status)
}
```

The returned `Sandbox` exposes typed fields including `ID`, `Name`, `Status`, `DesiredState`, `ObservedState`, `StateGeneration`, `LifecycleFailure`, `HA`, `RestartPolicy`, `Node`, timestamps, tags, `ReturnCode`, and `ErrorMessage`. Unknown response fields remain available in `Details` as `json.RawMessage`. Do not construct a `Sandbox` directly: obtain one through `Create`, `Get`, `List`, or `Ref` so its `Files` and `Ports` services are initialized.

</div>
<div data-sdk-language="typescript">

```ts
const current = await client.sandboxes.get(sandbox.id);
const deferred = client.sandboxes.ref("sbx_123"); // no request here
const previews = await client.sandboxes.list({
  tags: { service: "preview" },
});

console.log(current.info.observed_state);
await deferred.stop();
```

`Sandbox.info` is the handle's latest read-only cached view. `id` is stable, and `node` is the reporting mesh node or `null`. `refresh()` fetches a current `SandboxInfo` and merges it into the handle.

</div>
</div>

Current servers return the following lifecycle fields on every sandbox view. Python and TypeScript use the wire names below; Go exposes the corresponding `DesiredState`, `ObservedState`, `StateGeneration`, `LifecycleFailure`, `HA`, and `RestartPolicy` fields.

| Field | Meaning |
| --- | --- |
| `status` | Serving-status summary retained for compatibility. Use the desired and observed fields to inspect lifecycle progress. |
| `desired_state` | Durable target accepted by the server, such as `running`, `paused`, `suspended`, or `stopped`. |
| `observed_state` | State the current owner has materialized. It can lag the desired state while an operation is in progress. |
| `state_generation` | Monotonic transition generation. A newer accepted lifecycle request has a larger generation. |
| `lifecycle_failure` | Failure for the current transition, or `null` when no transition failure is recorded. |
| `ha` | Selected durability tier: `off`, `async`, `rerun`, or `async+rerun`. |
| `restart_policy` | Server-derived owner-loss policy, normally `none` or `rerun`. |

## Lifecycle and placement

Lifecycle operations act on the identified server sandbox. Stopping execution, immediately terminating resources, and removing the server record are distinct operations. Use the least destructive action that matches the intent. Named volumes have their own lifecycle and are not removed with the sandbox.

Pause keeps the VM allocated and quiesces its virtual CPUs. `resume()` resumes
that in-memory VM when paused; when the sandbox is durably suspended, the same
method restores its exact committed suspend checkpoint and preserves its ID.
`suspend()` is transactional: it captures a checkpoint, commits the suspended
state, releases the live VM, and keeps the identity manageable. A capture or
publication failure leaves the prior VM and observed state intact.

`history()` returns immutable points from oldest to newest. A `disk` point
contains durable disk state and cold-boots on rollback, so guest processes do
not survive. A `checkpoint` point contains VM execution state and resumes
captured processes. `rollback()` stages and validates a replacement before
cutover; a failed replacement does not destroy the current source. Production
meshes additionally fence these operations through PostgreSQL ownership leases
and publish encrypted portable history to S3. See
[Snapshots, Restore, and Fork](../platform/snapshots.md) and
[Mesh and High Availability](../platform/mesh.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

| Method | Effect |
| --- | --- |
| `refresh()` | Fetch and cache the latest `SandboxInfo`. |
| `stop()` | Stop execution while retaining the server record. |
| `terminate()` | Terminate the sandbox; repeated calls on the same handle are idempotent. |
| `remove()` | Delete the record; repeated calls on the same handle are idempotent. |
| `pause()` / `resume()` | Pause virtual CPUs, resume a paused VM, or restore a durably suspended VM. |
| `suspend()` | Checkpoint durably, release the live VM, and retain the sandbox ID. |
| `history()` | List retained recovery points from oldest to newest. |
| `rollback(recovery_point)` | Restore this ID to the named recovery point and return updated `SandboxInfo`. |
| `extend(secs)` | Extend the deadline by a non-negative number of seconds and return updated `SandboxInfo`. |
| `migrate(target)` | Move to a non-empty mesh-node target, rebind the handle, and return updated `SandboxInfo`. |

The `wait` argument on `stop()` and `terminate()` remains in the Python signature, but the current client does not use it to change request behavior.

```python
sandbox.stop()
sandbox.refresh()
print(sandbox.info.status)

checkpoint = sandbox.suspend()
print(checkpoint.desired_state, checkpoint.observed_state)
resumed = sandbox.resume()
point = sandbox.history()[-1]
restored = sandbox.rollback(point.name)
print(resumed.observed_state, restored.observed_state)


updated = sandbox.migrate("node-b")
print(updated.node)

sandbox.terminate()
sandbox.remove()
```

</div>
<div data-sdk-language="go">

| Method | Effect | Return |
| --- | --- | --- |
| `Refresh(ctx)` | Fetch and cache current metadata. | Updated `*Sandbox` |
| `Stop(ctx)` | Perform the API's ordinary graceful stop. | Updated `*Sandbox` |
| `Terminate(ctx)` | Immediately halt and release sandbox resources. | `error` |
| `Remove(ctx)` | Remove the sandbox record. | `error` |
| `Pause(ctx)` / `Resume(ctx)` | Pause virtual CPUs, resume a paused VM, or restore a durably suspended VM. | Updated `*Sandbox` |
| `Suspend(ctx)` | Checkpoint durably and release the live VM. | Updated `*Sandbox` |
| `History(ctx)` | List retained recovery points from oldest to newest. | `[]RecoveryPoint`, `error` |
| `Rollback(ctx, recoveryPoint)` | Restore this ID to one retained recovery point. | Updated `*Sandbox` |
| `Extend(ctx, seconds)` | Increase the lease duration. | Updated `*Sandbox` |
| `Migrate(ctx, target)` | Relocate and rebind to the resolved endpoint. | Updated `*Sandbox` |

```go
if _, err := sandbox.Suspend(ctx); err != nil {
    return err
}
if sandbox.DesiredState != "suspended" || sandbox.ObservedState != "suspended" {
    return fmt.Errorf("suspend did not converge: %#v", sandbox)
}
if _, err := sandbox.Resume(ctx); err != nil {
    return err
}
points, err := sandbox.History(ctx)
if err != nil {
    return err
}
if len(points) == 0 {
    return fmt.Errorf("no recovery point retained")
}
if _, err := sandbox.Rollback(ctx, points[len(points)-1].Name); err != nil {
    return err
}
if _, err := sandbox.Extend(ctx, 600); err != nil {
    return err
}
if _, err := sandbox.Migrate(ctx, "node-b"); err != nil {
    return err
}
if err := sandbox.Terminate(ctx); err != nil {
    return err
}
```

`Stop`, `Pause`, `Resume`, `Extend`, and `Migrate` replace the receiver's decoded metadata while preserving its client binding. `MigrationTiming()` exposes the migration response's pre-copy, downtime, and total durations when present.

</div>
<div data-sdk-language="typescript">

| Method | Effect | Return |
| --- | --- | --- |
| `refresh()` | Fetch and cache the current view. | `Promise<SandboxInfo>` |
| `stop(wait = true)` | Request graceful shutdown and, by default, wait for `stopped` or `exited`. | `Promise<SandboxInfo>` |
| `terminate(wait = true)` | Force termination and, by default, wait for `terminated`, `stopped`, or `exited`. | `Promise<void>` |
| `remove()` | Remove the sandbox record. | `Promise<void>` |
| `pause()` / `resume()` | Pause virtual CPUs, resume a paused VM, or restore a durably suspended VM. | `Promise<SandboxInfo>` |
| `suspend()` | Transactionally checkpoint, release the live VM, and preserve its ID. | `Promise<SandboxInfo>` |
| `history()` | List retained recovery points from oldest to newest. | `Promise<RecoveryPoint[]>` |
| `rollback(recoveryPoint)` | Restore this ID to one retained recovery point. | `Promise<SandboxInfo>` |
| `extend(secs)` | Extend the lease in seconds. | `Promise<SandboxInfo>` |
| `migrate(target)` | Migrate and re-resolve the serving endpoint. | `Promise<SandboxInfo>` |

The implicit state wait in `stop()` and `terminate()` is bounded at five minutes. Pass `false` to return after the request instead. A successful request is not a guarantee of a particular final state; inspect the returned view when state matters.

```ts
await sandbox.stop();
await sandbox.extend(600);
const suspended = await sandbox.suspend();
console.log(suspended.desired_state, suspended.observed_state);
const resumed = await sandbox.resume();
const points = await sandbox.history();
const point = points.at(-1);
if (!point) throw new Error("no recovery point retained");
const restored = await sandbox.rollback(point.name);
console.log(resumed.observed_state, restored.observed_state);
const moved = await sandbox.migrate("node-b");
console.log(moved.node);
await sandbox.terminate(false);
await sandbox.remove();
```

</div>
</div>

A named volume remains a separate `client.volumes` resource. Delete it deliberately when it is no longer needed; see [Volumes and Secrets](volumes-and-secrets.md).

## Wait for readiness

Readiness waiting is an SDK-side polling operation. Port and command probes are alternatives: a port probe checks a reachable exposed service, while a command probe succeeds only when the guest command exits with status zero. Probe failure is retried until the configured deadline.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`wait_ready(probe, timeout=300)` accepts an integer port, a mapping containing `port`, or another value rendered as a guest-shell command. Passing `None` returns immediately. Port probes resolve the tunnel target and attempt a TCP connection. Command probes run `sh -lc <probe>`. Expiry raises `TimeoutError`.

```python
sandbox.wait_ready({"port": 8080}, timeout=60)
sandbox.wait_ready("test -f /workspace/ready", timeout=60)
```

</div>
<div data-sdk-language="go">

`WaitReady` first polls `Refresh` until the state is `running` or `ready`. It defaults to a five-minute timeout and a 250 ms interval. `Port` performs a TCP dial against the daemon-reported tunnel target; `Command` runs a captured argument vector. These probes are mutually exclusive.

A state of `failed`, `terminated`, or `exited` produces a `not_running` API error. Readiness expiry produces a `timeout` API error, while cancellation of the caller's context takes precedence.

```go
ready, err := sandbox.WaitReady(ctx, vmon.WaitReadyOptions{
    Timeout:  45 * time.Second,
    Interval: 500 * time.Millisecond,
    Command:  []string{"test", "-f", "/workspace/ready"},
})
if err != nil {
    return err
}
fmt.Println(ready.Status)
```

</div>
<div data-sdk-language="typescript">

With no `probe`, `waitReady()` immediately returns cached `SandboxInfo`; it does not infer readiness from state or wait for boot. A probe can be a port number, `{ port }`, or a shell command string.

`timeout` is in seconds and defaults to `300`; `interval` is in milliseconds and defaults to `100`. A port probe sends `GET` requests through the port proxy and requires an HTTP `ok` response. A command probe runs `sh -lc <command>` with a five-second execution timeout. Deadline expiry throws an `Error` whose cause is the latest probe failure.

```ts
await sandbox.waitReady({ probe: 8080, timeout: 30, interval: 250 });
await sandbox.waitReady({ probe: { port: 8080 }, timeout: 30 });
await sandbox.waitReady({ probe: "test -f /tmp/ready", timeout: 30 });
```

</div>
</div>

## Network policy and tunnels

Network policy is daemon state. Reading returns the current policy; updating is presence-aware. Omitted fields remain unchanged, while an explicitly supplied empty allowlist replaces that allowlist with an empty list. These settings do not make ports public and do not mean the SDK itself performs filtering; the daemon's policy and deployment controls define the effective boundary. Invalid CIDRs or domains, unapproved domain resolution, and host policy setup failures reject the operation rather than allowing unrestricted egress. See [Networking](../platform/networking.md).

Tunnel discovery returns exposed guest-port targets and may supply a short-lived connect token. Applications normally use the sandbox's HTTP or WebSocket port service, which obtains and caches the token as needed. Expose the port in the creation request and configure it on the server. See [Files and Ports](files-and-ports.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`network()` returns `SandboxNetworkPolicy`. `set_network()` accepts that type or a mapping with `block_network`, `cidr_allow`, and `domain_allow`. `tunnels()` returns typed targets and the connect token used by the bound port proxy.

```python
policy = sandbox.set_network(
    {
        "block_network": False,
        "cidr_allow": ["10.0.0.0/8"],
        "domain_allow": ["packages.example"],
    }
)
assert policy.block_network is False

tunnels = sandbox.tunnels()
```

</div>
<div data-sdk-language="go">

`Network(ctx)` reads policy. `SetNetwork(ctx, policy)` updates only fields represented by non-`nil` pointers; a non-`nil` empty slice deliberately clears that allowlist. `Tunnels(ctx)` returns targets and may update the handle's connect token.

```go
blocked := false
cidrs := []string{"203.0.113.0/24"}
state, err := sandbox.SetNetwork(ctx, vmon.NetworkPolicy{
    BlockNetwork: &blocked,
    CIDRAllow:    &cidrs,
})
if err != nil {
    return err
}
fmt.Println(state.EgressAllow)
```

</div>
<div data-sdk-language="typescript">

`network()` reads `NetworkPolicy`. `setNetwork()` treats omitted or `null` `block_network`, `cidr_allow`, and `domain_allow` as unchanged; supplied arrays replace their allowlists. `tunnels()` returns `TunnelSet` and caches any `connect_token` for port proxies.

```ts
await sandbox.setNetwork({
  block_network: false,
  cidr_allow: ["10.0.0.0/8"],
  domain_allow: ["packages.example.test"],
});

const policy = await sandbox.network();
const tunnels = await sandbox.tunnels();
```

</div>
</div>

## Metrics, logs, and console access

Metrics are snapshots of daemon-reported runtime data. Log and console streaming capabilities differ by SDK; close live streams when they are no longer needed.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`metrics()` returns typed `SandboxMetrics`. `logs()` returns buffered text by default; `logs(follow=True)` returns a closeable live `LogStream`. `attach()` returns a closeable live `ConsoleStream`.

```python
print(sandbox.metrics())
print(sandbox.logs())

with sandbox.logs(follow=True) as logs:
    for line in logs:
        print(line)
        break
```

</div>
<div data-sdk-language="go">

`Metrics(ctx)` returns `SandboxMetrics`, an open set of raw JSON values. Use `Float64(name)` only when a metric is present and numeric. `Logs(ctx)` combines the currently available log chunks into one string. Use `FollowLogs` for incremental streaming as described in [Processes](processes.md).

```go
metrics, err := sandbox.Metrics(ctx)
if err != nil {
    return err
}
value, ok := metrics.Float64("cpu_usage")

logs, err := sandbox.Logs(ctx)
if err != nil {
    return err
}
fmt.Println(value, ok, logs)
```

</div>
<div data-sdk-language="typescript">

`metrics()` returns the daemon's `SandboxMetrics` view.

```ts
const metrics = await sandbox.metrics();
console.log(metrics);
```

Use the process APIs for command output and streaming behavior; see [Processes](processes.md).

</div>
</div>

## Snapshots and guest commands

Full snapshots capture VM state; filesystem snapshots capture a restorable filesystem image. Each SDK returns the resulting server identifier. Restoration and pool operations are covered in [Snapshots and Pools](snapshots.md). Guest command execution is covered in [Processes](processes.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
snapshot_name = sandbox.snapshot()
image_name = sandbox.snapshot_filesystem()

result = sandbox.run("sh", "-lc", "echo ready")
assert result.returncode == 0
```

</div>
<div data-sdk-language="go">

```go
snapshotID, err := sandbox.Snapshot(ctx, vmon.SnapshotRequest{Name: "checkpoint"})
if err != nil {
    return err
}
imageID, err := sandbox.SnapshotFilesystem(ctx, vmon.FilesystemSnapshotRequest{Name: "rootfs"})
```

</div>
<div data-sdk-language="typescript">

`snapshot(name?, stop = false)` returns a full snapshot name. `snapshotFilesystem(name?)` returns a filesystem image name.

```ts
const snapshotName = await sandbox.snapshot("checkpoint");
const imageName = await sandbox.snapshotFilesystem("rootfs");
```

</div>
</div>
