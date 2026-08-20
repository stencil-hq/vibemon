# Getting Started

The Python, Go, and TypeScript SDKs are clients for a running Rust `vmon serve` daemon. They create and control server-side resources through the daemon API; installing an SDK does not install or start the daemon, CLI, VMM, image runtime, or an in-process sandbox server.

Install the SDK where the application runs, start the daemon separately, and configure the application with a daemon DSN. Images, guest execution, persistent-volume storage, networking, scheduling, and mesh membership remain daemon responsibilities.

Start with [Install](install.md), then read [Connect](connect.md) for endpoint selection, credentials, discovery, and transport behavior. Canonical DSN syntax, environment precedence, and context-file rules are documented in [Connection Strings and Contexts](connection-strings.md), with lower-level details in the [DSN reference](../reference/dsn.md).

## Import the SDK

Use each SDK's public package boundary:

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python 3.14 or newer is required. The `vmon` package provides a typed, synchronous client:

```python
import vmon
```

</div>
<div data-sdk-language="go">

The Go module declares Go 1.25. Import its root package rather than internal generated packages:

```go
import     vmon "github.com/stencil-hq/vibemon/sdk/go"
```

</div>
<div data-sdk-language="typescript">

The ESM-only package targets Bun and browser applications that can reach the daemon:

```ts
import { connect, type Client } from "@stencil-hq/vibemon";
```

The root entry point exports `connect`, `Client`, `MeshDriver`, resource classes, models, driver and DSN types, and errors. Durable-function helpers are available from `@stencil-hq/vibemon/functions`, and function value codecs from `@stencil-hq/vibemon/function-values`.

</div>
</div>

## Connect and create a sandbox

Creating a client does not start a daemon. The first remote operation contacts the configured endpoint. The examples below connect, run a command in an Alpine sandbox, and explicitly clean up the server-side sandbox before closing the client transport.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect("vmon://node-a,node-b") as client:
    sandbox = client.sandboxes.create(image="alpine:latest")
    try:
        result = sandbox.run("sh", "-lc", "printf hello")
        assert result.returncode == 0
        assert result.stdout == b"hello"
    finally:
        sandbox.terminate()
        sandbox.remove()
```

The `with` block closes the client transport. It does not remove resources created through that client.

</div>
<div data-sdk-language="go">

```go
package main

import (
    "context"
    "log"

    vmon "github.com/stencil-hq/vibemon/sdk/go"
)

func main() {
    ctx := context.Background()

    client, err := vmon.Connect("vmons://vmon.example.internal")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    sandbox, err := client.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
        Image: "alpine:latest",
    })
    if err != nil {
        log.Fatal(err)
    }

    result, err := sandbox.Run(ctx, vmon.ExecRequest{
        Command: []string{"sh", "-lc", "printf hello"},
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("sandbox %s exited %d: %s", sandbox.ID, result.ExitCode, result.Stdout)

    if err := sandbox.Terminate(ctx); err != nil {
        log.Fatal(err)
    }
}
```

Pass a `context.Context` to remote calls and blocking reads. Reuse a request or worker context when available, or derive one with a timeout so cancellation and deadlines propagate. Local accessors and lifetime methods such as `Client.Driver`, `Client.Close`, and `EventStream.Close` do not take a context. Sandbox removal is documented in [Sandboxes](sandboxes.md); termination alone does not erase the server-side record.

</div>
<div data-sdk-language="typescript">

```ts
import { connect, type Client } from "@stencil-hq/vibemon";

const client: Client = connect("https://vmon.example");
try {
  const sandbox = await client.sandboxes.create({ image: "alpine:latest" });
  try {
    const result = await sandbox.run(["sh", "-lc", "printf hello"]);
    if (result.exit !== 0) {
      throw new Error(`command failed with exit ${result.exit}`);
    }
    const stdout = new TextDecoder().decode(
      Uint8Array.from(atob(result.stdout_b64), (c) => c.charCodeAt(0)),
    );
    console.log(stdout);
  } finally {
    await sandbox.terminate();
    await sandbox.remove();
  }
} finally {
  await client.close();
}
```

Closing the client releases its transport but does not remove server-side resources.

</div>
</div>

See [Sandboxes](sandboxes.md) for creation, bound references, readiness, migration, network policy, and the distinction between stopping, terminating, and removing a sandbox.

## Client capabilities

Every SDK exposes a root client with service namespaces for daemon resources. The shared guides cover [Client](client.md), [Sandboxes](sandboxes.md), [Processes](processes.md), [Files and Ports](files-and-ports.md), [Volumes and Secrets](volumes-and-secrets.md), [Snapshots and Pools](snapshots.md), [Durable Functions](functions.md), [Errors](errors.md), and [Testing](testing.md).

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

The synchronous Python client includes sandbox, snapshot, volume, pool, mesh, and function APIs, plus daemon inspection, events, and an ephemeral shell. Bound sandbox helpers expose thread-backed `.aio` facades, and durable-function async usage is documented in [Durable Functions](functions.md#python-async-facade).

Python's remaining language-specific guide is [Stateful Actors and Apps](python/actors.md).

</div>
<div data-sdk-language="go">

`*vmon.Client` exposes `Sandboxes`, `Snapshots`, `Volumes`, `Pools`, and `Mesh`, along with root methods for daemon health, build information, Prometheus metrics, and lifecycle events.

Methods return Go values plus an `error`. Structured failures include `APIError`, `ProtocolError`, `TransportError`, and `ResponseTooLargeError`; inspect them with `errors.As` rather than parsing error text.

</div>
<div data-sdk-language="typescript">

The root client exposes connection and resource APIs, daemon health, and the package's exported models and errors. Network DSNs work in Bun and browsers when the deployment's TLS, CORS, and WebSocket policies permit them.

The TypeScript SDK uses HTTP(S) for health, metrics, and HTTP-backed resource operations, and the daemon's `/grpc` WebSocket bridge for RPCs. It does not provide native HTTP/2 gRPC or Unix-domain-socket transport, so `vmon+unix://` is unsupported.

</div>
</div>

## Connection and lifecycle boundaries

A client connection and a sandbox have separate lifecycles. Closing a client releases local transport resources; a created sandbox remains on the daemon until its lifecycle is explicitly changed. This also means an application can reconnect to an existing resource when the API supports a bound reference.

Transport support differs by SDK:

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python accepts the DSNs and discovery mechanisms described in [Connect](connect.md), including named contexts and mesh discovery.

</div>
<div data-sdk-language="go">

`vmon.Connect` supports remote HTTP(S), `vmon`/`vmons` mesh rosters, named contexts, local Unix-domain sockets, and environment or local-state resolution from an empty DSN. A Unix socket still connects to a separate daemon.

</div>
<div data-sdk-language="typescript">

TypeScript supports reachable network endpoints through HTTP(S) and the WebSocket RPC bridge. It cannot connect through a Unix-domain socket.

</div>
</div>

Endpoint affinity, transport-only failover, and other behavior shared across SDKs are described in [Shared Concepts](shared-concepts.md). For daemon setup and network exposure, see [Server Operation](../platform/server.md).
