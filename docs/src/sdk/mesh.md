# Mesh

A `Client` represents one logical connection to a vmon mesh. It starts from the DSN endpoints and, unless discovery is disabled, discovers mesh-advertised endpoints lazily. The SDKs expose the same status and node roster through `client.mesh` or `client.Mesh`.

For DSN host lists, discovery parameters, contexts, and authentication, see [Connection Strings and Contexts](connection-strings.md). Disabling discovery keeps the configured endpoint roster fixed; it does not turn one endpoint into a mesh router.

## Inspect membership

Mesh status contains the reporting node, its advertised peers, and the number of replicas it currently holds. A node has a stable node identifier plus its advertised API URL and placement region when supplied by the server.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
from vmon import connect

with connect("vmon://node-a.example,node-b.example?discover=on") as client:
    status = client.mesh.status()
    print(status.self_node.node_id, status.replicas_held)
    for node in client.mesh.nodes():
        print(node.node_id, node.advertise, node.region)
```

`status.self_node` is the reporting node. `client.mesh.nodes()` returns `[status.self_node, *status.peers]`.

</div>
<div data-sdk-language="go">

```go
client, err := vmon.Connect("vmon://node-a.example,node-b.example?discover=on")
if err != nil {
    return err
}
defer client.Close()

status, err := client.Mesh.Status(ctx)
if err != nil {
    return err
}
fmt.Println(status.Self.NodeID, status.ReplicasHeld)

nodes, err := client.Mesh.Nodes(ctx)
if err != nil {
    return err
}
for _, node := range nodes {
    fmt.Println(node.NodeID, node.Advertise, node.Region)
}
```

`Nodes` returns the reporting node followed by its peers. `MeshStatus` also exposes `ExpectedMembers`, `Quorum`, and the original JSON payload through `Raw`.

</div>
<div data-sdk-language="typescript">

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect("vmon://node-a.example,node-b.example?discover=on");
try {
  const status = await client.mesh.status();
  console.log(status.self.node_id, status.replicas_held);
  for (const node of await client.mesh.nodes()) {
    console.log(node.node_id, node.advertise, node.region);
  }
} finally {
  await client.close();
}
```

`nodes()` returns `[status.self, ...status.peers]`. Mesh response objects retain additional server fields for forward-compatible placement data.

</div>
</div>

## Driver roster and discovery

The client driver maintains seed and discovered endpoints. Its endpoint snapshot is an advanced diagnostic surface, not an instruction to manually choose a node for ordinary resource calls.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
for endpoint in client.driver.endpoints():
    print(endpoint.url, endpoint.healthy, endpoint.source)

client.driver.refresh(force=True)
```

</div>
<div data-sdk-language="go">

```go
for _, endpoint := range client.Driver().Endpoints() {
    fmt.Println(endpoint.URL, endpoint.Healthy, endpoint.Source)
}

if err := client.Driver().Refresh(ctx, true); err != nil {
    return err
}
```

</div>
<div data-sdk-language="typescript">

```ts
for (const endpoint of client.driver.endpoints()) {
  console.log(endpoint.url, endpoint.healthy, endpoint.source);
}

await client.driver.refresh(true);
```

</div>
</div>

Discovery is throttled and lazy. The roster keeps configured seeds and merges healthy advertised peers. Closing the client releases or closes every transport owned by its driver.

## Routing and stable-ID reconnect

The mesh driver selects a reachable endpoint for client-wide requests. A sandbox returned by create, lookup, restore, fork, or list is identified by its stable sandbox ID; any routing state is private to the SDK.

When a sandbox-scoped request reports `not_found` and several endpoints exist, each SDK resolves the current owner once and retries the request. Callers retain the sandbox ID and do not select a worker node or manage an endpoint.

Migration is a server operation. Call `migrate(target)` in Python or TypeScript, or `Migrate(ctx, target)` in Go, with a mesh node ID. The SDK updates the cached sandbox view and resolves its serving endpoint after the move; the stable sandbox ID does not change. See [Sandboxes](sandboxes.md#lifecycle-actions).

## Failover boundaries

Drivers fail over only on transport failures that did not successfully complete an API request. They do not retry API errors, validation failures, HTTP status failures, or commands that have already started. Streaming calls can fail over while establishing the stream; failures after establishment belong to that stream.

This policy does not provide at-most-once or exactly-once mutation semantics. Use explicit API controls such as sandbox creation `idempotency_key`, and make durable-function side effects idempotent. See [Shared Concepts](shared-concepts.md), [Errors](errors.md), and [Mesh and High Availability](../platform/mesh.md).

Sandbox list operations query the live roster, merge rows by sandbox ID, and support tag filtering. A partial result can remain useful when another endpoint has a transport failure; if every attempted endpoint fails at the transport layer, the operation returns that failure.
