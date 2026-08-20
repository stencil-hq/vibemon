# Install an SDK

Each SDK is a client for a running Rust `vmon serve` daemon. Installing an SDK does not install the daemon, CLI, VMM, or image runtime. Install and run the daemon separately, then configure the application with a daemon DSN.

## Add the SDK

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Python requires version 3.14 or later. Add the `vmon` package to a `uv` project:

```sh
uv add vmon
```

Or create an isolated environment:

```sh
uv venv --python 3.14
uv pip install vmon
```

</div>
<div data-sdk-language="go">

Add the Go module from an existing application module:

```sh
go get github.com/stencil-hq/vibemon/sdk/go
```

The SDK module declares Go 1.25. Check the application toolchain with `go version`.

</div>
<div data-sdk-language="typescript">

The TypeScript SDK targets ESM Bun applications. It is currently a private package in this repository, so add a local checkout:

```sh
bun add @stencil-hq/vibemon@file:../vibemon/sdk/ts
bun add --dev typescript
```

</div>
</div>

## Import and connect

Use the public package boundary. Creating a client does not start a daemon; the first operation contacts the configured daemon.

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

```python
import vmon

with vmon.connect("https://vmon.example") as client:
    health = client.health()
    assert health.ok
```

</div>
<div data-sdk-language="go">

```go
package main

import (
    "context"
    "time"

    vmon "github.com/stencil-hq/vibemon/sdk/go"
)

func check() error {
    client, err := vmon.Connect("vmons://vmon.example")
    if err != nil {
        return err
    }
    defer client.Close()

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _, err = client.Health(ctx)
    return err
}
```

</div>
<div data-sdk-language="typescript">

```ts
import { connect, type Client } from "@stencil-hq/vibemon";

const client: Client = connect("https://vmon.example");
try {
  await client.health();
} finally {
  await client.close();
}
```

</div>
</div>

## Development checks

<div class="sdk-snippets" data-sdk-snippets>
<div data-sdk-language="python">

Synchronize this repository's Python development environment from `sdk/py`:

```sh
uv sync
```

</div>
<div data-sdk-language="go">

The Go SDK resolves its gRPC and protobuf dependencies through the module. Applications should import `sdk/go`, not its internal generated packages.

</div>
<div data-sdk-language="typescript">

Type-check this repository's SDK package without emitting JavaScript:

```sh
cd sdk/ts
bun run typecheck
```

Do not import implementation files such as `@stencil-hq/vibemon/src/client.ts`. The package exposes `@stencil-hq/vibemon`, `@stencil-hq/vibemon/functions`, and `@stencil-hq/vibemon/function-values`.

</div>
</div>

Continue with [Connect](connect.md) for endpoint selection. The canonical DSN syntax, environment precedence, and context-file rules are in [Connection Strings and Contexts](connection-strings.md).
