# Snapshots and Pools

Snapshots and warm pools belong to the daemon reached by the SDK. The SDK submits requests to a running `vmon serve` API and returns daemon-owned names, resource handles, and statistics; it does not snapshot VMs, allocate clones, prewarm capacity, or provide local durability itself.

Vibemon exposes two different capture operations:

- A **full VM snapshot** captures resumable machine state and is used with the snapshot collection's restore and fork operations.
- A **filesystem snapshot** produces an image/template reference for later sandbox creation. It does not contain CPU, memory, or device state.

The two result names are not interchangeable. See [Snapshots, Restore, and Fork](../platform/snapshots.md) for the authoritative capture contents, external-resource behavior, compatibility rules, and copy-on-write implementation.

## Capture a sandbox

Both capture operations require a running sandbox. A full snapshot accepts an optional name and a stop flag; enabling the flag asks the daemon to stop the sandbox immediately after capture. The returned string is the full snapshot name.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect() as client:
    sandbox = client.sandboxes.create(image="alpine")
    try:
        sandbox.run("sh", "-lc", "printf checkpoint > /tmp/state")
        snapshot_name = sandbox.snapshot("worker-checkpoint", stop=False)
    finally:
        sandbox.terminate()
```

For a filesystem-only image, call `snapshot_filesystem(name=None)`:

```python
with vmon.connect() as client:
    sandbox = client.sandboxes.create(image="alpine")
    try:
        sandbox.run("sh", "-lc", "mkdir -p /app && printf hello > /app/message")
        image_name = sandbox.snapshot_filesystem("example-app")
    finally:
        sandbox.terminate()
```

</div>
<div data-sdk-language="go">

```go
snapshotName, err := sandbox.Snapshot(ctx, vmon.SnapshotRequest{
    Name: "worker-checkpoint",
    Stop: false,
})
if err != nil {
    return err
}
```

For a filesystem-only image, use a separate request type:

```go
imageName, err := sandbox.SnapshotFilesystem(ctx, vmon.FilesystemSnapshotRequest{
    Name: "example-app",
})
if err != nil {
    return err
}
```

</div>
<div data-sdk-language="typescript">

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect();
const sandbox = await client.sandboxes.create({ image: "alpine:latest" });
const snapshotName = await sandbox.snapshot("worker-checkpoint", false);
```

For a filesystem-only image, call `snapshotFilesystem(name?)`:

```ts
const imageName = await sandbox.snapshotFilesystem("example-app");
```

</div>
</div>

Use a filesystem snapshot result as a sandbox template rather than passing it to the snapshot collection.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
from_template = client.sandboxes.create(template=image_name)
```

</div>
<div data-sdk-language="go">

Supply `imageName` through the template field supported by the sandbox create request used by the deployment.

</div>
<div data-sdk-language="typescript">

```ts
const fromTemplate = await client.sandboxes.create({ template: imageName });
```

</div>
</div>

See [Sandboxes](sandboxes.md) for the complete creation API. Use restore or fork only with a name returned by the full snapshot operation or by the snapshot list operation.

## List and restore full snapshots

Listing returns the full snapshot names registered with the selected daemon. Restoring a name creates and returns a new bound sandbox; it does not mutate the sandbox that was captured.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect() as client:
    print(client.snapshots.list())
    restored = client.snapshots.restore("worker-checkpoint")
    try:
        assert restored.run("cat", "/tmp/state").stdout == b"checkpoint"
    finally:
        restored.terminate()
```

The Python restore signature is `restore(snapshot, *, s3_mounts=...)`. It does not expose general create-style restore overrides such as resources, tags, timeout, or name.

</div>
<div data-sdk-language="go">

```go
names, err := client.Snapshots.List(ctx)
if err != nil {
    return err
}

restored, err := client.Snapshots.Restore(ctx, "worker-checkpoint", vmon.RestoreRequest{
    Name: "worker-from-checkpoint",
    Overrides: map[string]any{
        "env": map[string]string{"JOB": "compile"},
    },
})
if err != nil {
    return err
}
fmt.Println(names, restored.ID)
```

`RestoreRequest` has typed `Name` and `Agent` fields. `Overrides` accepts runtime-only fields: `env`, `workdir`, `tags`, `timeout`, `timeout_secs`, `readiness_probe`, `secrets`, `s3_mounts`, and `command`. Do not put `"name"` or `"agent"` in `Overrides`; the SDK rejects those conflicts. CPU, memory, disk, network, volume, host-share, image, and placement fields cannot change the devices already captured in a full VM snapshot and are rejected.

</div>
<div data-sdk-language="typescript">

```ts
const names = await client.snapshots.list();
const restored = await client.snapshots.restore("worker-checkpoint", {
  name: "worker-from-checkpoint",
  env: { JOB: "compile" },
  timeout_secs: 300,
});

console.log(names, restored.id);
```

The optional `RestoreRequest` body accepts `name`, `agent`, and the runtime-only override fields listed above. Device shape and placement remain properties of the captured snapshot.

</div>
</div>

## Delete a full snapshot

`delete()` permanently removes a full snapshot and its encrypted daemon
storage. It does not remove a sandbox restored or forked from that snapshot.
Delete a delta's descendants before deleting its base; otherwise those
descendants cannot be restored. An unknown name returns `not_found`.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
client.snapshots.delete("worker-checkpoint")
```

</div>
<div data-sdk-language="go">

```go
if err := client.Snapshots.Delete(ctx, "worker-checkpoint"); err != nil {
    return err
}
```

</div>
<div data-sdk-language="typescript">

```ts
await client.snapshots.delete("worker-checkpoint");
```

</div>
</div>

### External resources and S3 mounts

A full VM snapshot does not copy arbitrary host resources. Named volumes, ordinary host shares, network backends, and managed S3 storage remain external dependencies. The restoring or forking host must satisfy the requirements documented in the [platform snapshot guide](../platform/snapshots.md).

Sandboxes configured with secrets can be captured, restored, forked, and
migrated. A full VM snapshot preserves raw guest RAM, including secret bytes
that remain after a process exits. The daemon encrypts snapshot storage with
the owning tenant's customer key ID. Restore, fork, and rollback fail if that
key is unavailable; protect the artifact and its key as secret-bearing data.
Local restore and fork do not infer the host-side secret binding from guest
memory. Go and TypeScript callers can use the runtime-only `secrets` override
when future exec calls need that binding; live mesh migration carries the
source sandbox's active binding to the destination.

For a sandbox created with managed S3 mounts, the daemon records credential-free mount metadata beside the snapshot. Restore and fork reconstruct those recorded mounts using credentials available to the destination daemon. The snapshot never contains access keys, secret keys, session tokens, or the remote S3 objects themselves. A mount that originally used inline credentials or daemon environment credentials therefore requires suitable `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values at the destination, plus `AWS_SESSION_TOKEN` when applicable; anonymous mounts require no credentials.

An `s3_mounts` override supplies replacement connection or credential settings for the mountpoints already recorded in the snapshot; it cannot add or remove remote filesystem devices. The daemon preserves the recorded virtio-fs tags and rejects a mismatched mountpoint set. Do not put credentials in application source.

## Fork copy-on-write clones

Fork asks the daemon to create between 1 and 32 independent sandboxes from one full snapshot using copy-on-write sharing. The batch is atomic: if any clone fails, the daemon removes every clone created by that request. Every returned sandbox has its own lifecycle and must be terminated or removed independently.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect() as client:
    clones = client.snapshots.fork("worker-checkpoint", count=2)
    try:
        for clone in clones:
            print(clone.id)
    finally:
        for clone in clones:
            clone.terminate()
```

The Python signature also accepts `s3_mounts=...`; it does not expose other create-style fork overrides such as tags or resources.

</div>
<div data-sdk-language="go">

```go
clones, err := client.Snapshots.Fork(ctx, "worker-checkpoint", vmon.ForkRequest{
    Count: 3,
    Overrides: map[string]any{
        "tags": map[string]string{"role": "builder"},
    },
})
if err != nil {
    return err
}
for _, clone := range clones {
    fmt.Printf("clone %s on %s\n", clone.ID, clone.Node)
}
```

`ForkRequest.Count` is required. `Overrides` accepts the same runtime-only fields as restore; each value applies to every clone.

</div>
<div data-sdk-language="typescript">

```ts
const clones = await client.snapshots.fork("worker-checkpoint", {
  count: 3,
  tags: { role: "builder" },
  timeout_secs: 300,
});

for (const clone of clones) {
  console.log(clone.id);
}
```

`ForkRequest` requires `count` and accepts the same runtime-only fields as `RestoreRequest`.

</div>
</div>

Runtime-only overrides do not change snapshot compatibility or placement. Fetch a sandbox again when current owner or status matters. Mesh eligibility and affinity are described in [Mesh and High Availability](../platform/mesh.md).

## Manage warm pools

Warm pools are named daemon-side allocations of ready sandbox capacity. They are not language worker pools, and the SDK does not run or schedule VMs locally. Setting a pool requests a desired size for an image or template reference; it does not guarantee immediate readiness.

All three SDKs can set, list, delete, and clear pools. A listed or newly set pool carries the configured count and server statistics. The exact statistics available are daemon-owned; common fields include configured size, ready capacity, hits, and misses.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect() as client:
    pool = client.pools.set("alpine:latest", 8, memory=256)
    stats = pool.stats()
    print(stats.ready, stats.hits, stats.misses, stats.size)

    for configured in client.pools.list():
        print(configured.ref, configured.count)

    pool.delete()
```

`set(ref, count, **template)` accepts a non-negative desired count and image-pipeline template fields. `clear()` lists and deletes every pool visible to the client. A Python `Pool` is also a context manager that deletes its pool on exit.

</div>
<div data-sdk-language="go">

```go
stats, err := client.Pools.Set(ctx, "alpine:latest", vmon.PoolRequest{
    Size: 8,
    Template: map[string]any{"memory": 256},
})
if err != nil {
    return err
}
fmt.Printf("ready=%d hits=%d misses=%d size=%d\n", stats.Ready, stats.Hits, stats.Misses, stats.Size)
```

Use `client.Pools.List` to inspect all pools, `Delete` to remove one reference, and `Clear` to list then delete every configured reference. `PoolRequest.Size` is the desired size and `Template` contains image-pipeline arguments.

</div>
<div data-sdk-language="typescript">

```ts
const pool = await client.pools.set("alpine:latest", 8, { memory: 256 });
console.log(pool.ref, pool.count, pool.stats);

for (const configured of await client.pools.list()) {
  console.log(configured.ref, configured.count);
}

await pool.delete();
```

`set(ref, count, template?)` sends `count` as the request's `size` and merges the optional template object into the request. `delete(ref)` removes one pool, while `clear()` lists and deletes all visible pools.

</div>
</div>

Read the pool inventory when observed capacity matters, and use daemon scheduling telemetry to diagnose misses. Warm-pool scoring never overrides architecture, snapshot compatibility, agent, kernel, or other placement eligibility checks described in [Mesh and High Availability](../platform/mesh.md).
