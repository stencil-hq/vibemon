# vmon Python SDK

Thin Python client SDK for the Rust `vmon` API. The Rust `vmon` binary owns the CLI, `vmon serve` gRPC/HTTP/WebSocket server, and VMM runtime; this package only provides Python objects for talking to that API.

## Install

For development from the repository root:

```sh
uv sync --project sdk/py --python 3.14
```

The `vmon` distribution supports Python 3.14 and newer. The SDK expects a running Rust daemon/server. By default it talks to the local Unix socket at `$VMON_HOME/vmond.sock` (or `~/.vmon/vmond.sock`). Named remote contexts are stored under `$VMON_HOME/contexts.json`, and bearer tokens can come from `VMON_API_TOKEN` or `$VMON_HOME/credentials/<context>.token`.

## Local release package

From the repository root, build and validate the wheel and source distribution:

```sh
./scripts/package-python-sdk.sh
```

The script cleans `sdk/py/dist/`, builds `vmon` with Python 3.14, checks both distributions' metadata, installs the wheel and its dependencies into an isolated Python 3.14 environment, and verifies that `import vmon` resolves from that environment's installed wheel with a package version matching the distribution metadata. It does not publish or read upload credentials.

The publishable wheel and source distribution remain in `sdk/py/dist/`. The script also creates the deterministic release bundle and checksum consumed by the release workflow:

```text
dist/vmon-python-sdk-<version>.tar.gz
dist/vmon-python-sdk-<version>.tar.gz.sha256
```

To inspect or install the locally built wheel:

```sh
uvx --python 3.14 twine check sdk/py/dist/*
uv pip install --python 3.14 sdk/py/dist/vmon-*.whl
```

Publishing to a package index is outside this local packaging workflow. The script does not invoke an upload command or read upload credentials.

## Quick start

```python
import vmon

with vmon.connect("vmon://node-a,node-b") as client:
    assert client.health().ok is True

    work = client.volumes.create("work")
    with client.sandboxes.create(
        image="alpine:latest",
        env={"APP_ENV": "dev"},
        secrets=[vmon.Secret.from_env("API_TOKEN")],
        volumes={"/data": work},
    ) as sandbox:
        captured = sandbox.run("sh", "-lc", "printf captured")
        assert captured.returncode == 0
        assert captured.stdout == b"captured"

        process = sandbox.exec("sh", "-lc", "echo hello > /data/out && cat /data/out")
        assert process.wait(timeout=30).code == 0

        template = sandbox.snapshot_filesystem("alpine-ready")

    restored = client.sandboxes.create(template=template)
    restored.terminate()
```

## Public surface

- `connect()` parses local, HTTP(S), multi-host mesh, Unix-socket, and named-context DSNs and returns a `Client`. `MeshDriver` discovers peer advertise URLs lazily, fails over only on transport errors, and keeps sandbox calls pinned to the node that owns them.
- `Client` exposes `sandboxes`, `snapshots`, `volumes`, `pools`, and `mesh` resource namespaces. Health, server info, and event calls return the exported `Health`, `ServerInfo`, and `EventRecord` models; Prometheus metrics remain text.
- `Sandbox` is a bound resource. `run()` captures a command; `exec()` opens a streaming `Process`; `files` and `ports` expose bound filesystem and proxy operations. Runtime metrics, network policy, and tunnels return `SandboxMetrics`, `SandboxNetworkPolicy`, and `TunnelSet`. Lifecycle methods return typed `SandboxInfo` views with desired/observed state, transition generation and failure, HA tier, and restart policy; `RecoveryPoint.kind` is `disk` or `checkpoint`.
- `Process.wait()` returns `ExecExit`; stdin is available through `process.stdin`, and stdout/stderr remain closeable byte streams. Console, event, log, and file streams are closeable context managers.
- `Volume` and `Secret` are validated request values. Server-side volume lifecycle lives under `client.volumes`; secret values stay in memory and are sent only in create and exec requests.
- `APIError`, `TransportError`, and `ProtocolError` distinguish server envelopes, failover-eligible I/O failures, and malformed wire data.
- `sandbox.aio`, `sandbox.files.aio`, and `sandbox.ports.aio` provide thread-backed async forms of the synchronous object hierarchy.
- `client.function()` and `@vmon.function` package source-available callables into server-native durable functions. Calls are recorded before execution, expose stable `FunctionCall` IDs, resumable events/results, `spawn()` and bounded `map()` forms, and server-owned retries with at-least-once attempt semantics. Portable values use JSON/CBOR envelopes; explicitly trusted Python serialization supports richer values. `@vmon.cls` defines durable actors with `@vmon.method`/`@vmon.enter`/`@vmon.exit` lifecycle hooks, and `vmon.is_remote()` reports worker-side execution.

## Durable lifecycle

`pause()` retains the live VM. `resume()` resumes a paused VM or restores the
exact committed checkpoint of a durably suspended sandbox. `suspend()` releases
the live VM only after the checkpoint and lifecycle state commit; failure leaves
the previous VM authoritative. Recovery history is oldest to newest: `disk`
points cold-boot and `checkpoint` points restore VM execution state.

```python
suspended = sandbox.suspend()
assert suspended.desired_state == suspended.observed_state == "suspended"

resumed = sandbox.resume()
point = sandbox.history()[-1]
restored = sandbox.rollback(point.name)
assert resumed.observed_state == restored.observed_state == "running"
```

## Real-VM SDK smoke test

From the repository root, after building the Rust binary and enabling real-VM e2e prerequisites:

```sh
VMON_BIN=$PWD/target/debug/vmon VMON_E2E=1 uv run --python 3.14 python sdk/py/e2e.py
```
