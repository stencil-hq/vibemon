# Connect

Create one client and keep it for the lifetime of the component that uses it. Client construction parses configuration and prepares the transport, but does not contact the daemon; make an operation such as `health` when the application needs a readiness check. Do not create a new client for every request.

## Create and close a client

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Use `vmon.connect()` to construct a `vmon.Client`. The returned client is a context manager:

```python
import vmon

with vmon.connect("https://vmon.example") as client:
    health = client.health()
    assert health.ok
```

Without a context manager, close the client explicitly:

```python
client = vmon.connect("vmons://vmon-a.example,vmon-b.example")
try:
    print(client.info())
finally:
    client.close()
```

In async code, `await client.aclose()` closes the underlying transport. It does not make the synchronous resource API asynchronous; bound sandbox helpers provide thread-backed `.aio` facades, while durable-function async usage is covered in [Durable Functions](functions.md#python-async-facade).

</div>
<div data-sdk-language="go">

`vmon.Connect` returns a `*vmon.Client` and may report configuration errors. Pass a `context.Context` to remote calls and apply deadline and cancellation policy at the call site:

```go
package main

import (
    "context"
    "errors"
    "time"

    vmon "github.com/stencil-hq/vibemon/sdk/go"
)

func check() error {
    client, err := vmon.Connect("vmons://vmon.example.internal")
    if err != nil {
        return err
    }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    health, err := client.Health(ctx)
    if err != nil {
        return err
    }
    if !health.OK {
        return errors.New("vmon daemon reported unhealthy")
    }
    return nil
}
```

`Client.Close` closes cached gRPC connections and the backing driver and returns an error from the driver close path. When that error matters, handle it explicitly:

```go
client, err := vmon.Connect(dsn)
if err != nil {
    return err
}
defer func() {
    if err := client.Close(); err != nil {
        log.Printf("close vmon client: %v", err)
    }
}()
```

Do not issue operations after closing the client.

</div>
<div data-sdk-language="typescript">

Use `connect` to create the root client, and await `close()` when finished:

```ts
import { connect } from "@stencil-hq/vibemon";

const client = connect("vmons://vmon-a.example,vmon-b.example?timeout=20");
try {
  const info = await client.info();
  console.log(info.version);
} finally {
  await client.close();
}
```

</div>
</div>

## DSNs and defaults

A DSN selects one endpoint, a mesh seed roster, a Unix socket where supported, or a saved context. Common explicit forms are:

| Form | Example | Use |
| --- | --- | --- |
| HTTP endpoint | `http://127.0.0.1:8000` | One plaintext daemon endpoint. |
| HTTPS endpoint | `https://vmon.example.internal/api` | One secure endpoint, optionally with a path prefix. |
| Mesh roster | `vmon://node-a,node-b:9000/api` | Plaintext seed endpoints; `vmons://` uses HTTPS. |
| Unix socket | `vmon+unix:///absolute/path/to/vmond.sock` | A local daemon over an absolute Unix-domain socket path. |
| Named context | `vmon+context://prod` | Endpoints loaded from the saved `prod` context. |

A named context is a local profile, not a server-side context. For the exact grammar, normalization, query parameters, environment precedence, token precedence, and context-file locations, see [Connection Strings and Contexts](connection-strings.md) and the [DSN reference](../reference/dsn.md).

An omitted or blank DSN first resolves `VMON_DSN`, then `VMON_CONTEXT`. The final default and available transports depend on the SDK runtime:

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

When neither environment variable is set, Python uses the local default Unix socket. `vmon+unix://` is supported.

```python
import vmon

local = vmon.connect("vmon+unix:///absolute/path/vmond.sock")
remote = vmon.connect("https://vmon.example")
mesh = vmon.connect("vmon://node-a,node-b?discover=off")
profile = vmon.connect("vmon+context://staging")
```

</div>
<div data-sdk-language="go">

When neither environment variable is set, Go uses the local default Unix socket. `vmon+unix://` is supported. An empty string is therefore meaningful: `Connect("")` deliberately selects the fallback policy.

Applications can validate or inspect configuration without making a request:

```go
config, err := vmon.ParseDSN("vmon+context://prod")
if err != nil {
    return err
}
for _, endpoint := range config.Endpoints {
    log.Printf("configured endpoint: %s", endpoint)
}
```

</div>
<div data-sdk-language="typescript">

After the environment variables, TypeScript uses the current page origin in an HTTP(S) browser page, or `http://127.0.0.1:8000` outside a browser, including Bun. These defaults select an address; they do not start a daemon.

Use `vmon://`, `vmons://`, `http://`, or `https://`. RPCs use the daemon's WebSocket bridge. The SDK has no Unix-domain-socket transport, so it rejects `vmon+unix://` even in Bun.

Saved contexts require filesystem access to read `contexts.json` and an optional credential from `VMON_HOME` or the user's home directory. Bun can use them; browsers cannot and must use a network DSN. Browser deployments must also permit the required HTTP and WebSocket connections. A page that contacts a different daemon origin needs the server's appropriate browser-origin policy; the SDK does not proxy through the page origin.

</div>
</div>

## Tokens, timeouts, and discovery

Prefer short-lived or deployment-scoped bearer tokens supplied outside source code and logs. Explicit factory options override values resolved from the DSN or environment. Without an explicit token, the shared resolution rules apply: a DSN token takes precedence over `VMON_API_TOKEN`, with a named context's saved credential as the final fallback after the environment variable.

Timeout values in Python and TypeScript are seconds. Go uses `time.Duration`. Discovery is enabled by default unless the DSN or an explicit option disables it.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
vmon.connect(dsn=None, *, token=None, timeout=60.0, discover=True)
```

`timeout` must be positive and defaults to `60.0`. `token`, `timeout`, and `discover` are explicit connection overrides.

```python
client = vmon.connect(
    "vmons://vmon.example.internal",
    token=token,
    timeout=30.0,
    discover=False,
)
```

</div>
<div data-sdk-language="go">

`Connect` accepts variadic `vmon.Option` functions:

| Option | Effect |
| --- | --- |
| `vmon.WithToken(token)` | Sets the client's bearer token. |
| `vmon.WithHTTPClient(httpClient)` | Supplies the mesh driver's HTTP client; a nil value is ignored. |
| `vmon.WithUserAgent(value)` | Sets the HTTP `User-Agent` when `Connect` constructs the driver. |
| `vmon.WithMaxResponseBytes(limit)` | Replaces the 64 MiB buffered successful-response limit when positive. |
| `vmon.WithDiscovery(enabled)` | Overrides the DSN discovery setting. |
| `vmon.WithTimeout(timeout)` | Overrides the DSN request timeout when positive. |

```go
httpClient := &http.Client{}
client, err := vmon.Connect(
    "vmons://vmon.example.internal?discover=off",
    vmon.WithToken(token),
    vmon.WithHTTPClient(httpClient),
    vmon.WithMaxResponseBytes(8<<20),
    vmon.WithTimeout(30*time.Second),
)
if err != nil {
    return err
}
defer client.Close()
```

`WithMaxResponseBytes` limits successful response bodies buffered by the SDK, not an open event stream.

</div>
<div data-sdk-language="typescript">

`connect(dsn?, options?)` accepts `token`, `timeout`, and `discover` overrides. It also accepts `fetch`, `websocket`, and `transport` injection points for compatible application-owned implementations; ordinary Bun and browser applications normally use their globals.

```ts
const client = connect("https://vmon.example", {
  token: process.env.VMON_API_TOKEN,
  timeout: 30,
  discover: false,
});
```

When an application owns driver construction, pass a `MeshDriver` or another exported `Driver` implementation directly to `Client`:

```ts
import { Client, MeshDriver } from "@stencil-hq/vibemon";

const driver = new MeshDriver("https://vmon.example", { discover: false });
const client = new Client(driver);
try {
  await client.health();
} finally {
  await client.close();
}
```

</div>
</div>

## Mesh behavior and request lifecycle

A multi-host `vmon://` or `vmons://` DSN provides seed endpoints. The driver can discover advertised peers lazily after a successful request. It fails over for transport failures, not API errors. Sandboxes returned by create, get, or list are pinned to the endpoint that answered; sandbox-scoped operations retain that affinity. If a bound sandbox is missing from a mesh endpoint, the client can resolve its current host and retry there.

This behavior is not a general retry guarantee. Design request handling around each operation's semantics; see [Shared Concepts](shared-concepts.md) for the cross-SDK rules.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python resource calls are synchronous. Apply the connection-level timeout when constructing the client; use bound objects' thread-backed `.aio` facades where available, and `asyncio.to_thread` for blocking root-client operations.

</div>
<div data-sdk-language="go">

Every remote client or service call takes a `context.Context`; local accessors and lifetime methods such as `Client.Driver` and `Client.Close` do not. Blocking stream reads also take a context, and an event stream remains tied to the context passed to `Events`:

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
    if kind, ok := event.Value("type"); ok {
        log.Printf("event type: %s", kind)
    }
}
```

`Event.Value` returns `json.RawMessage`; decode it only when the application owns the event schema. `EventStream.Close` is local and does not take a context.

</div>
<div data-sdk-language="typescript">

Client operations are asynchronous and use the configured transport timeout. Always await operations and `client.close()` so transport resources finish closing before the surrounding lifecycle ends.

</div>
</div>

Continue with [Client](client.md) for the root API and service namespaces.
