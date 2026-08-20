# Client and service namespaces

A client is the root object for one logical connection to a running daemon or mesh. It owns the transport used by daemon-wide operations, service namespaces, and resources returned by those services. Keep one client for the lifetime of related work so bound resources retain their endpoint affinity; creating a client does not provide a local VMM or server runtime.

## Create and close a client

Use the SDK's connection helper for normal construction and release the client when its owner is finished with it. Closing a client releases local transports; it does not stop, terminate, or remove server-side sandboxes.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

`vmon.connect()` returns a `vmon.Client`. The client is a synchronous context manager, and `close()` is idempotent. `aclose()` provides the corresponding awaitable close path.

```python
import vmon

with vmon.connect("https://vmon.example") as client:
    health = client.health()
    assert health.ok
```

For explicit ownership:

```python
client = vmon.connect()
try:
    client.health()
finally:
    client.close()
```

</div>
<div data-sdk-language="go">

`vmon.Connect` returns a `*vmon.Client` and an error. Close the client once; `Close` returns an error after releasing cached gRPC connections and the backing driver.

```go
client, err := vmon.Connect(dsn)
if err != nil {
    return err
}
defer client.Close()
```

Remote methods take a `context.Context` and return an error. Check that error before using the result.

</div>
<div data-sdk-language="typescript">

`connect()` returns a `Client`. `close()` may be synchronous or asynchronous depending on the driver, so awaiting it is safe.

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect("https://vmon.example");
try {
  const health = await client.health();
  console.log(health.ok);
} finally {
  await client.close();
}
```

</div>
</div>

See [Connect](connect.md) for DSNs, options, and endpoint selection.

## Daemon-wide operations

The root client exposes daemon health and identity directly. Metrics are returned as Prometheus exposition text rather than a parsed SDK model. Streaming operations remain open until the application stops and closes them.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

| Method | Result | Purpose |
| --- | --- | --- |
| `health()` | `vmon.Health` | Reads daemon health; `Health.ok` is the health flag. |
| `info()` | `vmon.ServerInfo` | Reads typed daemon and node information. |
| `metrics()` | `str` | Reads the Prometheus metrics document. |
| `events()` | `vmon.EventStream` | Opens a daemon engine-event stream. |
| `shell(...)` | `vmon.Process` | Starts an interactive shell in an existing or ephemeral sandbox. |
| `close()` / `aclose()` | `None` / awaitable | Releases the underlying driver. |

```python
with vmon.connect() as client:
    info = client.info()
    prometheus_text = client.metrics()
    print(info, prometheus_text)
```

`events()` returns a closeable context manager of `vmon.EventRecord` values. Do not assume the stream is finite:

```python
with vmon.connect() as client:
    with client.events() as events:
        for event in events:
            print(event)
            break
```

`shell()` uses `/bin/sh` when no command is supplied. Pass command pieces as positional strings or one iterable; `ref`, `image`, `cpus`, `memory`, `disk_mb`, and `timeout` configure the request. If `ref` resolves to a sandbox, the shell runs there; otherwise the ephemeral shell sandbox is removed when its stream ends. The returned `vmon.Process` exposes streaming input, output, `wait()`, and `close()`; see [Processes](processes.md).

</div>
<div data-sdk-language="go">

| Method | Result | Purpose |
| --- | --- | --- |
| `Health(ctx)` | `vmon.Health` | Reads daemon health; `Health.OK` is the health flag. |
| `Info(ctx)` | `vmon.ServerInfo` | Reads version, platform, architecture, backend, and capabilities. |
| `Metrics(ctx)` | `string` | Reads the Prometheus metrics document. |
| `Events(ctx)` | `*vmon.EventStream` | Opens a lifecycle-event stream. |
| `Driver()` | `vmon.Driver` | Returns the transport backing the client. |
| `Close()` | `error` | Releases cached connections and the backing driver. |

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

info, err := client.Info(ctx)
if err != nil {
    return err
}
if !info.Capabilities["snapshots"] {
    return errors.New("connected daemon does not advertise snapshots")
}
log.Printf("vmon %s using %s", info.Version, info.Backend)
```

Capability names are daemon-defined; a missing map key reads as `false`. `Events` returns a stream of lossless `vmon.Event` objects. Read with `Next`, use `Value(name)` to access a field as `json.RawMessage`, and close the stream explicitly. Cancel the context to stop a blocked `Next` call.

```go
stream, err := client.Events(ctx)
if err != nil {
    return err
}
defer stream.Close()

for {
    event, err := stream.Next(ctx)
    if err != nil {
        return err
    }
    if rawID, ok := event.Value("id"); ok {
        log.Printf("event id: %s", rawID)
    }
}
```

</div>
<div data-sdk-language="typescript">

| Method | Result | Purpose |
| --- | --- | --- |
| `health()` | `Promise<Health>` | Reads the daemon health document from `GET /healthz`. |
| `info()` | `Promise<ServerInfo>` | Reads daemon and node information. |
| `metrics()` | `Promise<string>` | Reads Prometheus metrics text from `GET /metrics`. |
| `events()` | `Promise<EventStream>` | Opens the daemon event RPC stream. |
| `shell(request?)` | `Promise<Process>` | Opens an interactive shell and waits for its ready frame. |
| `close()` | `void \| Promise<void>` | Releases the underlying driver. |

```ts
const events = await client.events();
try {
  for await (const event of events) {
    console.log(event);
  }
} finally {
  events.close();
}
```

`EventStream.close()` cancels that stream. `shell()` returns a `Process` with writable `stdin`, asynchronous output iteration, `wait()`, and `close()`; see [Processes](processes.md).

</div>
</div>

## Resource and service namespaces

Namespaces retain their root client; do not instantiate namespace or service classes directly. Resources returned by creation or lookup retain that client and manage mesh routing privately by stable ID.

The SDKs share services for sandboxes, snapshots, volumes, pools, and mesh topology. TypeScript additionally exposes deployed durable functions and applications from the root client.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

| Property | Operations |
| --- | --- |
| `client.sandboxes` | Create, fetch, reference, and list sandboxes. |
| `client.snapshots` | List, restore, and fork snapshots. |
| `client.volumes` | Create, list, and delete persistent named volumes. |
| `client.pools` | Inspect, resize, and delete server-owned warm pools. |
| `client.mesh` | Inspect mesh status and nodes. |

```python
with vmon.connect() as client:
    work = client.volumes.create("work")
    sandbox = client.sandboxes.create(
        image="alpine:latest",
        volumes={"/work": work},
    )
    try:
        print([volume.name for volume in client.volumes.list()])
    finally:
        sandbox.terminate()
        sandbox.remove()
```

</div>
<div data-sdk-language="go">

| Field | Service | Operations |
| --- | --- | --- |
| `client.Sandboxes` | `*vmon.SandboxService` | Create, get, list, and manage sandboxes. |
| `client.Snapshots` | `*vmon.SnapshotService` | List, restore, and fork snapshots. |
| `client.Volumes` | `*vmon.VolumeService` | List, create, and delete persistent volumes. |
| `client.Pools` | `*vmon.PoolService` | List, configure, and clear warm-pool allocations. |
| `client.Mesh` | `*vmon.MeshService` | Read mesh status and known nodes. |

```go
status, err := client.Mesh.Status(ctx)
if err != nil {
    return err
}
log.Printf("node %s has %d peers; quorum is %d", status.Self.NodeID, len(status.Peers), status.Quorum)

nodes, err := client.Mesh.Nodes(ctx)
if err != nil {
    return err
}
for _, node := range nodes {
    log.Printf("node %s advertises %s", node.NodeID, node.Advertise)
}
```

`Mesh.Nodes` is a convenience view of `Mesh.Status`: it returns the local node followed by its peers.

</div>
<div data-sdk-language="typescript">

| Property | Operations |
| --- | --- |
| `client.sandboxes` | Create, get, reference, and list sandboxes. |
| `client.snapshots` | List, restore, and fork snapshots. |
| `client.volumes` | List, create, and delete persistent volumes. |
| `client.pools` | Inspect and configure warm pools. |
| `client.mesh` | Read mesh status and nodes. |
| `client.functions` | Look up a deployed durable function by name. |
| `client.apps` | Look up a deployed application by name. |

```ts
const sandbox = await client.sandboxes.get("sandbox-id");
const snapshotNames = await client.snapshots.list();
const deployed = await client.functions.fromName("thumbnail");

console.log(sandbox.id, snapshotNames, deployed);
```

</div>
</div>

See [Sandboxes](sandboxes.md), [Snapshots](snapshots.md), [Volumes and Secrets](volumes-and-secrets.md), [Processes](processes.md), [Files and Ports](files-and-ports.md), and [Durable Functions](functions.md) for operation-specific APIs. [Shared Concepts](shared-concepts.md) describes the common resource and endpoint-affinity model.

## Direct driver construction and lifecycle

Normal applications should use the connection helper. Direct construction is an integration seam for applications that own the transport. The client assumes ownership of its driver, so do not share one driver across clients unless that driver's lifecycle supports it.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Create clients with `vmon.connect()` and close the client through its context manager, `close()`, or `aclose()`. Close each event stream or process when its application-level work ends; closing the root client is not a substitute for ending stream work deliberately.

</div>
<div data-sdk-language="go">

`vmon.NewClient(driver, options...)` binds the public object model to an existing `vmon.Driver` without parsing a DSN or constructing the default mesh driver:

```go
func newServiceClient(driver vmon.Driver, token string) *vmon.Client {
    return vmon.NewClient(driver, vmon.WithToken(token))
}
```

`Client.Close` closes the injected driver. `WithToken` and `WithMaxResponseBytes` configure the client. Transport-construction options such as `WithHTTPClient`, `WithDiscovery`, `WithTimeout`, and `WithUserAgent` do not reconfigure an injected driver; pass them to `Connect` when constructing the default transport.

For a client made by `Connect`, `Driver().Endpoints()` reports each endpoint URL, health, and source. Discovery refresh is lazy and throttled. Selection favors an operation's endpoint hint, then the preferred endpoint, then the remaining roster. Only transport-level connection failures trigger failover; API errors and HTTP status responses are returned without retrying another node.

</div>
<div data-sdk-language="typescript">

`connect(dsn, options)` constructs `new Client(new MeshDriver(dsn, options))`. `MeshDriver` owns endpoint discovery, selection, HTTP requests, and WebSocket RPC transport. Its `endpoints()` method returns the current endpoint URLs, health, and seed-or-discovered source; `refresh()` requests discovery when enabled.

Closing the client marks a `MeshDriver` closed, so later operations fail instead of opening new requests. Close each `EventStream` or `Process` when its application-level work ends; closing the root client is not a substitute for ending stream work deliberately.

</div>
</div>

The client does not promise application-level retries. See [Shared Concepts](shared-concepts.md) for transport failover and affinity, [Errors](errors.md) for SDK failure types, and [Mesh and High Availability](../platform/mesh.md) for server topology.
