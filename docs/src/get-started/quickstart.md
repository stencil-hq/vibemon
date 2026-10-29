# Quickstart

Run a single-process vibemon (node and driver roles in one `vmon serve`) on one machine, build the default `base` template, and drive a sandbox with the official E2B SDKs. Works on macOS 15+ on Apple Silicon and on Linux with KVM.

## Prerequisites

| Tool | Why |
|---|---|
| [rustup](https://rustup.rs) | Installs the pinned nightly toolchain and targets from [`rust-toolchain.toml`](https://github.com/stencil-hq/vibemon/blob/main/rust-toolchain.toml) on first `cargo` run. |
| [just](https://github.com/casey/just) | Runs the recipes in the [`justfile`](https://github.com/stencil-hq/vibemon/blob/main/justfile). |
| Docker | On macOS `just kernel` builds in a container. |
| [uv](https://docs.astral.sh/uv/) | `just templates` runs `templates/build.py` with `uv run`. |
| [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild) + [Zig](https://ziglang.org) (optional) | `just agent-musl` uses `cargo zigbuild` for the static guest agent when it is installed, else plain `cargo build`. |

Linux hosts also need `/dev/kvm`, `ip` (iproute2), and `iptables`; see [Installation](installation.md#host-requirements).

No database is needed: the driver keeps all state in its raft KV under the data directory.

## 1. Build the guest kernel (recommended)

```sh
just kernel --arch aarch64          # macOS (in Docker), or an aarch64 Linux host
just kernel --arch x86_64           # x86_64 Linux host
```

Outputs land in `target/kernel/`, where a dev-tree `vmon` finds them. Without this step the node downloads the pinned cloud-hypervisor kernel and logs a warning: that kernel has no netfilter, IPv6, bridge, or NFS, so Docker and some units of the default templates fail. Templates capture the kernel they were built with, so build it before step 3. Details: [Guest kernel](../operate/kernel.md).

## 2. Start the server

Leave the server running in its own terminal.

**macOS**: HVF needs no root, and networking is user-mode NAT.

```sh
just serve
```

`just serve` builds and ad-hoc signs `vmon`, builds the static guest agent into `target/test-assets/`, and runs:

```text
vmon serve --agent target/test-assets/vmon-agent-<arch> --listen 127.0.0.1:3000 \
  --public-url http://127.0.0.1:3000 --domain localhost
```

`--public-url` matters with a loopback listener: signed template-file upload URLs point at it, and its default is derived from the cluster listener's address, not from `--listen`.

Extra arguments are forwarded, e.g. add `--registry-mirror https://mirror.gcr.io`. State goes to `~/.vmon`: the raft KV with every team, key, template, and sandbox record (`raft/`), and the generated `cluster-secret` that `vmon admin` reads.

**Linux**: the node creates TAP devices and NAT rules, which needs root or `CAP_NET_ADMIN`. `just serve` runs unprivileged, so the node starts but rejects sandbox creates and builds. Run the same command under `sudo`. Use an explicit cluster secret so the unprivileged `vmon admin` and `just templates` can use it (as root, the generated secret is written to `/var/lib/vmon/cluster-secret` with mode `0600`):

```sh
just build agent-musl
export VMON_CLUSTER_SECRET="$(openssl rand -hex 32)"
sudo "$(just bin)" serve --cluster-secret "$VMON_CLUSTER_SECRET" \
  --agent "$PWD/target/test-assets/vmon-agent-$(uname -m)" \
  --listen 127.0.0.1:3000 --domain localhost --public-url http://127.0.0.1:3000
```

Export the same `VMON_CLUSTER_SECRET` in every shell that runs the commands below. Alternatives to running everything as root: [Linux privileges](../operate/serve.md#linux-privileges).

Check that the driver is up. `/health` returns `204` once the raft KV serves reads:

```sh
curl -i http://127.0.0.1:3000/health
```

## 3. Build the `base` template

```sh
just templates base
```

`templates/build.py` creates (or reuses) the system team `system` through `vmon admin`, mints a key for it, builds `base` with the E2B `Template` builder, and publishes it. A system team's public templates claim bare aliases, so `base` then resolves for every team. Run `just templates` without arguments to build all defaults (`base`, `code-interpreter-v1`, `desktop`, `mcp-gateway`, `ubuntu-systemd`, `ci`); `ci` first needs a 64 GiB disk limit (see [The `ci` runner template](../concepts/templates.md#the-ci-runner-template)).

> [!IMPORTANT]
> `build.py` uses `E2B_API_KEY` if it is set. Run it in a shell without your own team's key, or the templates land in that team's namespace and the bare alias stays unclaimed.

> [!TIP]
> Docker Hub's anonymous pull limit shows up as `HTTP 429 TOOMANYREQUESTS`. Restart the server with `--registry-mirror https://mirror.gcr.io` (or `VMON_REGISTRY_MIRROR`).

More: [Templates](../concepts/templates.md).

## 4. Create a team and API key

```sh
"$(just bin)" admin create-team dev
```

`vmon admin` talks to the running server's cluster listener (`http://127.0.0.1:7947` by default).

The output has `team`, `slug`, and `api key` lines. Export the key and point the SDKs at the server:

```sh
export E2B_API_KEY=e2b_...                  # the `api key` line
export E2B_DOMAIN=localhost
export E2B_API_URL=http://127.0.0.1:3000
export E2B_SANDBOX_URL=http://127.0.0.1:3000
```

`E2B_SANDBOX_URL` sends sandbox (envd) traffic to the API listener, which routes it by the `E2b-Sandbox-*` headers. [Connecting SDKs and the CLI](clients.md) covers the TLS setup that needs only `E2B_DOMAIN`.

## 5. Use a sandbox

Python:

```python
# uv run --with e2b python quickstart.py
from e2b import Sandbox

sbx = Sandbox.create()                              # forks the `base` template snapshot
print(sbx.commands.run("uname -a").stdout)
sbx.files.write("/home/user/hello.txt", "hi")
sbx.pause()                                         # memory + disk snapshot
sbx = Sandbox.connect(sbx.sandbox_id)               # resumes it
print(sbx.files.read("/home/user/hello.txt"))
sbx.kill()
```

TypeScript:

```ts
// npm install e2b && npx tsx quickstart.ts
import { Sandbox } from 'e2b'

const sbx = await Sandbox.create()
console.log((await sbx.commands.run('uname -a')).stdout)
await sbx.files.write('/home/user/hello.txt', 'hi')
console.log(await sbx.files.read('/home/user/hello.txt'))
await sbx.kill()
```

Lifecycle details (timeouts, auto-pause, fork, snapshots): [Sandboxes](../concepts/sandboxes.md).

## With release binaries (Linux)

Tagged releases ship Linux binaries only (x86_64 and aarch64, static musl); macOS uses the dev path above. Install `vmon`, `vmon-kernel-<arch>`, and `vmon-agent-<arch>` into one directory as described in [Installation](installation.md#install-a-release), here `/opt/vmon`. Then:

```sh
export VMON_CLUSTER_SECRET="$(openssl rand -hex 32)"
sudo /opt/vmon/vmon serve --cluster-secret "$VMON_CLUSTER_SECRET" \
  --listen 127.0.0.1:3000 --domain localhost --public-url http://127.0.0.1:3000
```

No `--kernel` or `--agent` is needed: the node picks up the files next to `vmon`. The template definitions are not in the release tarballs; build them from a checkout of the repository (no Rust toolchain needed):

```sh
VMON_BIN=/opt/vmon/vmon uv run templates/build.py base
/opt/vmon/vmon admin create-team dev
```

Export the printed key and the client variables as in step 4, then continue at step 5.
