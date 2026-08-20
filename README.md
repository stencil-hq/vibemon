<p align="center">
  <img src="assets/hero.png" alt="Vibemon — KVM, HVF, and WHP microVM monitor" width="100%" />
</p>

# Vibemon

`vmon` is a small KVM, HVF, and WHP virtual machine monitor for Linux guests. One Rust binary owns the user CLI, `vmon serve` server, and low-level `vmon vmm` monitor. It boots containers as hardware-isolated microVMs, snapshots them to disk, and forks copy-on-write clones.

<p align="center">
  <img src="assets/lifecycle.png" alt="Snapshot, restore, and fork lifecycle" width="100%" />
</p>

## Quickstart

```sh
# Build the single Rust binary
just release                      # target/release/vmon

# Run a container as a microVM (Linux/KVM or Apple Silicon macOS/HVF)
./target/release/vmon run alpine -- sh -c 'echo hello from a microVM; uname -a'

# Snapshot, warm-boot, and fork
./target/release/vmon snapshot myvm tpl --stop
./target/release/vmon restore tpl --name warm      # ~120 ms
./target/release/vmon fork tpl --count 5           # ~3 ms per CoW clone
```

The commands above run on Linux/KVM and Apple Silicon macOS/HVF. The Windows build currently exposes the low-level `vmon vmm` WHP monitor; the user CLI and `vmon serve` have not been ported to Windows. `just release` codesigns the macOS binary. To launch the embedded web panel and gRPC/HTTP API:

```sh
cd ui && bun install && bun run build   # writes vmond/web/
cd ..
./target/release/vmon serve --host 127.0.0.1 --port 8000 --token secret
# open http://127.0.0.1:8000
```

## Architecture

<p align="center">
  <img src="assets/architecture.png" alt="Vibemon runtime architecture" width="100%" />
</p>

The single Rust `vmon` binary has three roles: the user-facing CLI, `vmon serve` (the Rust server from the `vmond` crate), and `vmon vmm` (the per-VM monitor from the `vmm` crate). The server owns the sandbox registry and spawns one `vmon vmm` child per microVM; the guest agent runs inside the VM and talks back over a virtio-console channel.

```
Web UI / Rust CLI / Python SDK / TypeScript SDK / Go SDK
   │ gRPC (native h2c or WebSocket bridge); HTTP health, metrics, and port proxy
vmon serve (Rust axum + tonic API, vmond crate; local UDS supported)
   │ Engine registry, image pipeline, pools, mesh, volumes
   │ spawns `vmon vmm ... --api-sock <sock>` per VM
vmon vmm (Rust VMM crate)
   │ virtio-console, length-prefixed binary frames
vmon-agent (guest agent, Linux guest only)
```

**Rust boot path:** `Config::from_args()` → `vmm::run()` → `Vmm::build()` (boot or restore/fork) → allocate guest memory, instantiate virtio device backends, register on the device `Bus` → `Vmm::start()` spawns one thread per vCPU and one worker thread per device. vCPU threads run the hypervisor loop (`KVM_RUN`, HVF, or WHP), trap MMIO/PortIO to the `Bus`, and notify virtio queues; device workers wait for queue, backend, and control events before signaling completion interrupts.

**Control plane:** JSON lifecycle protocol (`ping`, `info`, `pause`, `resume`, `snapshot`, `quit`, `metrics`, `extend`) over Unix sockets on Linux/macOS and local named pipes on Windows. Requests cross a `flume` channel to the owner thread; the transport thread never touches the `Vmm` directly. `PauseGate` quiesces vCPUs with an RT signal on Linux and backend exit kickers on HVF and WHP.

## Support matrix

| Area | Supported | Notes |
| --- | --- | --- |
| Host OS | Linux with `/dev/kvm`; macOS 15+ on Apple Silicon with Hypervisor.framework; x86_64 Windows with Windows Hypervisor Platform | The backend is selected at compile time: KVM, HVF, or WHP. macOS requires a codesigned binary with `com.apple.security.hypervisor`; Windows requires the Hyper-V/Windows Hypervisor Platform feature and currently supports the low-level `vmon vmm` monitor only. |
| Host CPU architecture | `x86_64`, `aarch64` | Linux guests follow the host hypervisor architecture. macOS/HVF supports `aarch64` guests only; Windows/WHP supports `x86_64` guests only. |
| Guest OS | Linux | Direct-kernel boot and operator-supplied UEFI firmware are supported; non-Linux guests are not a target. |
| x86_64 direct kernel format | uncompressed ELF `vmlinux` or `bzImage` | Loaded directly by vmon on KVM or WHP. |
| aarch64 direct kernel format | uncompressed `Image` | The demo can extract an `Image` from a host `vmlinuz` on arm64 Linux. |
| UEFI firmware | QEMU/EDK2 firmware supplied by the operator | Pass `--boot-mode uefi --firmware <path>`; vmon maps OVMF firmware on x86_64 WHP and does not vendor firmware blobs. |
| Devices | serial console, virtio-blk, virtio-net, virtio-console agent, virtio-rng, writable/read-only virtio-fs, remote virtio-fs | Linux networking uses TAP. macOS/HVF and Windows/WHP support `--net user` through libslirp. Windows control, agent, and remote-filesystem endpoints use local named pipes. Snapshot/restore covers device state and user-net state; host-side TCP flows reset after user-net restore. |

Fast CI runs Rust formatting, checks, clippy, tests, aarch64 checks, and `cargo audit` on Ubuntu, plus macOS arm64 build and no-run test coverage with HVF codesigning. A native Windows job checks the x86_64 `vmon` binary and runs WHP clippy and unit tests. Real guest-boot coverage currently runs on KVM and HVF hosts.

## Build

```sh
just release
```

The resulting binary is `target/release/vmon` unless `CARGO_TARGET_DIR` or Cargo `build.target-dir` redirects the target directory. On macOS 15+ Apple Silicon, `just build` and `just release` automatically ad-hoc codesign the binary with `hvf.entitlements`, which grants `com.apple.security.hypervisor` (the only entitlement ad-hoc signing can carry; restricted entitlements such as `com.apple.vm.networking` cause the kernel to refuse to launch an ad-hoc-signed binary). The entitlement-free `--net user` backend also needs native `libslirp` + `pkg-config` installed locally, for example `brew install libslirp pkg-config`. If building by hand on macOS, run:

```sh
cargo build --release
codesign --sign - --entitlements hvf.entitlements --force target/release/vmon
```

`--net user` works on macOS/HVF without `com.apple.vm.networking`; `--tap` still requires host vmnet-style networking support and fails clearly on the ad-hoc-signed binary.

## Local release packaging

The local release entry point builds all consumable release artifacts without
uploading them or publishing to GitHub, npm, or PyPI. Select the Linux musl
platform explicitly:

```sh
just local-release x86_64-unknown-linux-musl
# or
just local-release aarch64-unknown-linux-musl
```

The equivalent script interface is:

```sh
./scripts/package-local-release.sh --target x86_64-unknown-linux-musl
```

It composes the same component packagers used by the release workflow:

```sh
./scripts/package-rust-release.sh --target <triple>
./scripts/package-guest-assets.sh --target <triple>
./scripts/package-python-sdk.sh
(cd sdk/ts && bun run package)
./scripts/package-release.sh
```

Install the locked TypeScript dependencies with
`cd sdk/ts && bun install --frozen-lockfile` before running the local entry
point. Rust cross-compilation also requires the configured Rust target, Zig,
and `cargo-zigbuild`; Python packaging and deterministic archives require
`uv`. The binary packager executes both `vmon` and `vmon-agent` rather than
only checking that they link. A Linux host can execute its native architecture
directly. Cross-architecture Linux packaging requires `qemu-x86_64` or
`qemu-aarch64` (or `VMON_RELEASE_RUNNER`); non-Linux hosts must set
`VMON_RELEASE_RUNNER`. Packaging fails explicitly if the selected target
cannot be executed.

A successful run produces:

| Component | Local artifacts |
| --- | --- |
| Rust binaries | `dist/vmon-<triple>.tar.gz`, `dist/vmon-<triple>.tar.gz.sha256` |
| Guest kernel and agent | `dist/vmon-assets-<arch>.tar.gz`, `dist/vmon-assets-<arch>.tar.gz.sha256` |
| Python SDK | wheel and sdist in `sdk/py/dist/`; `dist/vmon-python-sdk-<version>.tar.gz`, `dist/vmon-python-sdk-<version>.tar.gz.sha256` |
| TypeScript SDK | `sdk/ts/dist/package/stencil-hq-vibemon-<version>.tgz` containing built JavaScript and declarations |
| Deployment files | `dist/vmon-deploy-assets.tar.gz`, `dist/SHA256SUMS-deploy.txt` |

Pushing a `v*` tag starts `.github/workflows/release.yml`, which runs these
same local packaging interfaces on Linux (using QEMU for the cross-architecture
smoke), uploads their outputs as workflow artifacts, and attaches them to the
tagged GitHub release. A manual workflow dispatch only builds workflow
artifacts. Neither path publishes an SDK to a package registry.

## Low-level VMM commands

Boot a Linux kernel with an initramfs:

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Boot with a virtio-blk root disk:

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --rootfs <disk.img> \
  --cmdline "console=ttyS0 root=/dev/vda rw"
```

Add a virtio-net device after creating a Linux TAP interface (Linux/KVM only for `--tap`):

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --tap tap0 \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

On macOS/HVF without `com.apple.vm.networking`, use entitlement-free user-mode NAT instead:

```sh
./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --net user \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Use the JSON control socket for pause/resume/snapshot/quit:

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --api-sock /tmp/vmon/control.sock \
  --snapshot-root /tmp/vmon-snapshots \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# The server writes one banner line first:
# {"vmm":"0.1.0","api":1}
printf '%s\n' \
  '{"id":1,"method":"pause","params":{}}' \
  '{"id":2,"method":"snapshot","params":{"name":"demo"}}' \
  '{"id":3,"method":"resume","params":{}}' \
  '{"id":4,"method":"quit","params":{}}' \
  | socat - UNIX-CONNECT:/tmp/vmon/control.sock
```

Restore or fork a snapshot:

```sh
sudo ./target/release/vmon vmm --restore /tmp/vmon-snapshots/demo
sudo ./target/release/vmon vmm --fork-from /tmp/vmon-snapshots/demo --count 4
```

Use PCI virtio transport on x86_64 only:

```sh
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --transport pci \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Boot via operator-supplied UEFI firmware:

```sh
# aarch64: point this at a QEMU_EFI.fd build for the host architecture.
VMON_AARCH64_UEFI=/path/to/QEMU_EFI.fd

# x86_64: point this at an OVMF_CODE.fd/EDK2 firmware image.
VMON_X86_UEFI=/path/to/OVMF_CODE.fd

sudo ./target/release/vmon vmm \
  --boot-mode uefi \
  --firmware "$VMON_X86_UEFI" \
  --rootfs <uefi-bootable-disk.img> \
  --transport pci
```

Pinned UEFI assets can be fetched using the optional best-effort script `demo/fetch-test-assets.sh`. It downloads a pinned TianoCore EDK2 release (`edk2-stable202511-r1-bin.tar.xz`, SHA256 `79841c5dcac6d4bb71ead5edb6ca2a251237330be3c0b166bdc8a8fec0ce760d`) and extracts code and vars for both x86_64 (`OVMF_CODE.fd`/`OVMF_VARS.fd`) and aarch64 (`QEMU_EFI.fd`), as well as Ubuntu focal cloud images when `VMON_UEFI_IMAGES=1` is supplied:
- x86_64 Cloud Image: `http://cloud-images-archive.ubuntu.com/releases/focal/release-20230209/ubuntu-20.04-server-cloudimg-amd64.img` (SHA256 `eb20cd25da5d2193283951953f6a0f5bdbd57474ac19fd1c36b9b77e6b68bbfc`)
- aarch64 Cloud Image: `http://cloud-images-archive.ubuntu.com/releases/focal/release-20230209/ubuntu-20.04-server-cloudimg-arm64.img` (SHA256 `f607f625568e004831fe7daf799bdd50def22d83e87d82a40f717a09c11a772c`)

Expose host directories with virtio-fs:

```sh
# Read-only shared directory.
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --fs-tag shared --fs-dir /path/to/share \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"

# Named volume, writable by default. Add :ro for a read-only volume.
sudo ./target/release/vmon vmm \
  --kernel <kernel-image> \
  --initrd <initramfs.cpio.gz> \
  --volume data:/var/lib/vmon-volumes/data \
  --volume cache:/srv/cache:ro \
  --cmdline "console=ttyS0 reboot=t panic=-1 rdinit=/init"
```

Snapshots record the virtio-fs mode and are tagged with the capturing backend. Named volume data is not copied into snapshots; the SDK re-attaches volumes by name on restore or fork. Restores are backend- and architecture-specific: KVM snapshots restore on KVM, HVF snapshots on HVF, and WHP snapshots on WHP. Cross-hypervisor and cross-architecture migration is out of scope.

### Production platform flags

The low-level `vmon vmm` subcommand accepts the production lifecycle, agent, jail, networking, and logging flags used by the SDK and server:

- `--snapshot-root <dir>`: root for named JSON lifecycle snapshots.
- `--timeout-secs <n>`: VMM-enforced wall-clock deadline, from 1 second to 24 hours. On timeout the VMM writes `status.json` with `reason:"timeout"` and return code `124`.
- `--mem-target-mib <n>`: Linux-only transparent guest-RAM paging target. vmm needs root/CAP_SYS_PTRACE or `vm.unprivileged_userfaultfd=1` so userfaultfd can handle KVM faults.
- `--zram-store-max-mib <n>`: cap for the in-process compressed page store before pager overflow spills to swap.
- `--zram-swap-file <path>`: operator-provided pager overflow file; default is an anonymous temporary file in `$TMPDIR`.
- `--ksm`: mark guest RAM `MADV_MERGEABLE` so host KSM can merge identical pages across co-resident guests. The operator must enable `/sys/kernel/mm/ksm/run`; metrics report advised regions, not per-process byte savings.
- `--rng`: attach a virtio-rng entropy device exposed to the guest as `/dev/hwrng`, sourced from the host `/dev/urandom`. Seeds the guest kernel CRNG early so first-boot `getrandom(2)` (TLS, key generation, language runtimes) does not block. MMIO on all architectures, PCI on x86_64; captured and restored across snapshot/fork.
- `--agent-sock <path>`: guest-agent byte bridge over virtio-console; also enables the console agent device.
- `--jail`, `--id <name>`, `--jail-root <dir>`: Linux namespace/cgroup/pivot-root jail identity and root.
- `--volume <tag>:<host_dir>[:ro]`: attach a named virtio-fs volume. Tags use `[a-z0-9_]{1,32}`; volumes are writable unless `:ro` is present.
- `--cgroup-cpu-max <value>`, `--cgroup-mem-max <value>`, `--cgroup-pids-max <n>`, `--cgroup-mode v2|off`: cgroup controls.
- `--seccomp-action kill|errno|log`: seccomp default action (default: `errno` for safety). Note that CLI `kill` maps internally to a seccomp `Trap` (triggering SIGSYS) for diagnostics instead of a silent, unlogged process kill.
- `--netns <path>`: operator-supplied network namespace entered before TAP open.
- `--log-format text|json`, `--log-level <level>`: tracing output controls.
- `--no-sandbox`: opt out of the default-on Stage-B process filters (seccomp + Landlock + `no_new_privs` + resource-limit tightening) for local development; cannot be combined with `--jail`.
- `--sandbox-uid <uid>`, `--sandbox-gid <gid>`: UID/GID to drop to after the filters are applied. Required only under `--jail`; for default-on standalone filters they are optional, and the privilege drop runs only when vmon starts as root and both are supplied.

## Self-hosted sandbox API

The sandbox control plane is Rust-owned. `vmon serve` starts the `vmond` engine, serves the `vmon.v1` gRPC services over native h2c and a `/grpc` WebSocket bridge, keeps HTTP routes for health, metrics, and port proxying, embeds the React panel from `vmond/web/`, and exposes the local UDS endpoint used by the CLI plus the Python and Go SDKs. The top-level CLI (`vmon run`, `ps`, `logs`, `exec`, `stop`, `mesh`, `context`, and friends) is Rust code in the same binary; `vmon vmm` remains the low-level escape hatch for direct kernel/rootfs boots.

| Command | What it does |
| --- | --- |
| `vmon suspend NAME` / `history NAME` / `rollback NAME POINT` | Durably suspend a sandbox, list its retained disk/checkpoint recovery points, or restore its identity to one point. `vmon resume NAME` restores the committed suspend checkpoint. |
| `vmon doctor` | Prints the local prerequisite checklist (vmon binary, macOS codesign entitlement, HVF/KVM, `skopeo`, `umoci`, `mkfs.ext4`, guest kernel, guest agent, daemon, and host environment) and exits non-zero on hard failures. `vmon doctor --serve --config PATH` validates the resolved `vmon serve` config surface. |
| `vmon completion [bash|zsh|fish]` | Prints a sourceable shell-completion script; load it with `eval "$(vmon completion zsh)"` (or `bash`/`fish`). |

The Python, TypeScript, and Go packages are thin clients for the Rust API. Each exposes the same object hierarchy: a root client, resource namespaces (`sandboxes`, `snapshots`, `volumes`, `pools`, and `mesh`), and sandbox-bound process, file, and port objects. They do not ship a CLI, daemon, server, web bundle, guest-agent bundle, or VMM implementation.

SDK resource operations use the generated `vmon.v1` gRPC services. Browser and TypeScript clients carry the same protobuf calls over the server's `/grpc` WebSocket bridge; only health, Prometheus metrics, and sandbox port proxying remain HTTP. Public responses are exported model types, and malformed response envelopes raise each SDK's `ProtocolError`.

Sandbox views expose `desired_state`, `observed_state`, `state_generation`,
`lifecycle_failure`, `ha`, and `restart_policy` in all three SDK model types.
Pause retains the VM; resume handles either an in-memory pause or an exact
durable suspend marker. Recovery history identifies `disk` cold-boot points
and `checkpoint` execution-state points, and rollback keeps the source
authoritative until its replacement is ready.

Production clusters are explicit: `cluster_mode=production` requires
PostgreSQL ownership/lifecycle authority, S3 portable object storage, and one
non-`default` portable-history key provisioned identically at
`$VMON_HOME/security/keys/<id>.key` on every node. Owner lease expiry
self-fences the local VMM; the server never degrades to local authority. See
the platform configuration, mesh, and snapshot guides.

All three SDKs accept the network forms below. Python and Go additionally support the local UDS form; browser builds of the TypeScript SDK are deliberately network-only.

| DSN | Meaning |
| --- | --- |
| `vmon://host-a,host-b:9000/prefix` | Plaintext gRPC/HTTP endpoints; port defaults to `8000`. |
| `vmons://host-a,host-b` | TLS gRPC/HTTPS/WSS endpoints. |
| `http://host:8000` or `https://host` | One explicit endpoint. |
| `vmon+unix:///absolute/path/vmond.sock` | Local gRPC/HTTP-over-UDS endpoint (Python and Go). |
| `vmon+context://prod` | Endpoints and optional token from the named vmon context. |

`token`, `discover=on|off`, and `timeout=<seconds>` are optional query parameters. An empty Python or Go DSN resolves `$VMON_DSN`, then `$VMON_CONTEXT`, then the local `$VMON_HOME/vmond.sock`; TypeScript resolves those environment settings when available, then defaults to the page origin in a browser or `http://127.0.0.1:8000` elsewhere. Client construction performs no network I/O. Mesh discovery is lazy: after the first successful request, the driver learns advertised peers and fails over only on transport-level connection failures. Daemon gRPC statuses and HTTP responses are never replayed.

```python
from vmon import Secret, connect

with connect("vmon+context://prod") as client:
    volume = client.volumes.create("agent_data")
    sandbox = client.sandboxes.create(
        image="alpine",
        timeout=300,
        volumes={"/data": volume},
        secrets=[Secret.from_env("TOKEN"), Secret.from_dict({"MODE": "ci"})],
        tags={"kind": "oneshot"},
        ports=[8080],
        egress_allow_domains=["api.github.com"],
        pool_size=2,
    )

    process = sandbox.exec(["sh", "-lc", "echo hello from the guest"], tty=True)
    exit_status = process.wait()
    print(process.stdout.read().decode(), exit_status.code)

    image = sandbox.snapshot_filesystem("img1")
    clone = client.sandboxes.create(template=image)
    same = client.sandboxes.get(sandbox.id)
```

Named volumes persist outside snapshots and are protected by the Rust server's single-writer host lock (or mesh lease on clustered writable mounts). Secrets are merged into exec environments and are not written to VM metadata. Sandbox creation also supports `block_network`, CIDR egress rules, DNS-pinned egress domains, inbound CIDR allowlists, `ha`, and `arch`; the domain allowlist resolves to IP rules and is not live TLS-SNI filtering.

Exposed ports are available through each sandbox's `tunnels` and `ports` APIs. Runtime deadlines can be extended through the bound sandbox object; polling reports the entry-process exit code when known, otherwise VMM status codes such as `124` for timeout and `137` for termination.

Durable functions are server-native calls recorded before execution, with stable call IDs, resumable events/results, server-owned retries, and at-least-once attempt semantics. Python packages source-aware `@vmon.function` definitions and applications through `vmon.App`; TypeScript resolves pinned deployments through `client.functions.fromName()` and `client.apps.fromName()`; Go uses `vmon.LookupFunction` and reconstructs calls with `vmon.FunctionCallFromID`. Portable inputs and results use checksummed JSON or CBOR envelopes, while Python additionally supports explicitly trusted Python serialization.

The TypeScript SDK lives in `sdk/ts` and uses bun. Run `just sdk-ts` for install and type checking, and `just sdk-ts-smoke` for its live API smoke. The Go SDK lives in `sdk/go` as module `github.com/stencil-hq/vibemon/sdk/go`; run `just sdk-go` for its gRPC, HTTP, and WebSocket tests. Real-VM remote-function tests require the language-specific smoke environment variables documented in each package.

## Cluster

Use `vmon serve` gateways to pool multiple machines behind one CLI/SDK context. Every node shares the full operator bearer token (`--token` or `VMON_API_TOKEN`); clients pass either that token or a scoped `VMON_CLIENT_TOKEN` value through `VMON_API_TOKEN`.

`vmon serve` has one config surface. Defaults come from `ServeConfig`, then an optional TOML file (`vmon serve --config file.toml` or `$VMON_CONFIG`), then `VMON_*` environment variables, then CLI flags. Unknown config keys are rejected.

```toml
[serve]
host = "0.0.0.0"
port = 8000
token = "T"
replicas = 1
# replicate_sec defaults to 60 on mesh-enabled nodes; explicit 0 disables it.
```

### Form a cluster

On the seed node, start the gateway:

```sh
vmon serve --config serve.toml
```

In another shell on the seed host, initialize the mesh and copy the printed `vmon mesh join <blob>` command:

```sh
VMON_API_TOKEN=T vmon mesh setup --advertise http://<seed-ip>:8000
```

On each other node, start the gateway with the same token, then join with the blob printed by the seed:

```sh
vmon serve --config serve.toml
VMON_API_TOKEN=T vmon mesh join <blob>
```

`vmon mesh status` renders members, health, capacity, and local sandboxes with durability tier plus checkpoint age/RPO. The underlying `GET /v1/mesh/status` payload also includes top-level `replicas_held`, status warnings, and per-node HA counters (`stats.replication`, `stats.restore`, `stats.fence`):

```sh
VMON_API_TOKEN=T vmon mesh status
```

### Connect a client

Create a cluster context from any reachable gateway. The CLI fetches the full roster and stores the ordered endpoint list. Tokens are not stored unless you opt in with `--save-token`, which writes a private file under `$VMON_HOME/credentials/`.

```sh
export VMON_API_TOKEN=T
vmon context create prod --server http://<any-node>:8000 --save-token
vmon context use prod

vmon run alpine -- echo hello
vmon ps
vmon exec <sandbox> -- uname -a
```

Manage contexts with:

```sh
vmon context ls
vmon context inspect prod
vmon context refresh prod
vmon context rm prod
vmon context use local
```

After `vmon context use prod`, ordinary `vmon run`, `vmon exec`, and `vmon ps` route across the cluster through the shared `Transport` plane (`LocalTransport` for the daemon, `MeshTransport` for gateway rosters). Failover happens only before delivery: idempotent detached `run`/`restore` calls carry a stable key and may walk the roster, while attached/interactive operations probe `/healthz` once and then run exactly once. `vmon context use local` returns the CLI to the local `vmond` daemon.

The SDKs resolve named contexts through the same DSN:

```python
import vmon

with vmon.connect("vmon+context://prod") as client:
    sandbox = client.sandboxes.create(image="alpine")
    same_plane = client.sandboxes.get(sandbox.id)
```

An explicit missing context is an error; it does not fall back to local.

### Connectivity prerequisite

Each node's advertised URL must be routable by every other node and by the client. On a LAN or cloud VPC with reachable IPs, that is usually automatic. There is no built-in NAT traversal or relay: for machines behind NAT, including a laptop or home server, put the nodes on a WireGuard or Tailscale overlay and advertise each node's overlay IP:

```sh
VMON_API_TOKEN=T vmon mesh setup --advertise http://<overlay-ip>:8000
```

### Placement

Placement is request-scoped. The optional `arch` selector is accepted on create/run/restore/fork requests and is never defaulted from the ingress machine. If `arch` is omitted, the coordinator derives compatible arches from the image manifest (`skopeo inspect`, cached) intersected with live node arches. A single live arch is used directly; mixed live arches with an underivable image return `arch_required`, and no live match returns `unplaceable`.

```sh
vmon run --arch aarch64 alpine -- uname -m
vmon restore tpl --arch x86_64 --name restored
vmon fork tpl --arch aarch64 --count 2
```

### Durability and downtime

In `cluster_mode=production`, PostgreSQL transactionally stores the create
record, owner epoch, lifecycle state, and owner lease before acknowledgement.
S3 stores encrypted portable checkpoints and replicas after verification; a
PostgreSQL manifest commit makes them visible. In the default
`cluster_mode=single-node`, development meshes copy create records and
checkpoints to required peers and provide only best-effort epoch fencing.

The per-sandbox durability tier is
`ha=off|async|rerun|async+rerun`. Mesh creates default to `async`; local daemon
creates default to `off`. `async` captures every 60 seconds by default;
`VMON_REPLICATE_SEC=0` disables it. `rerun` permits a fenced successor to
re-execute the durable create record at least once when no checkpoint exists.
`async+rerun` prefers checkpoint restore and falls back to rerun.

```python
with vmon.connect("vmon+context://prod") as client:
    sandbox = client.sandboxes.create(
        image="alpine",
        ha="async+rerun",
        idempotency_key="nightly-build",
    )
```

Automatic orphan restore is quorum-gated by default with at least three
expected members. A two-node mesh cannot form a post-failure majority, so that
guard defaults off and `mesh status` reports a warning.

Networked sandboxes are HA/migration-eligible. Linux restores allocate a fresh TAP on the destination; macOS/HVF restores reopen user-net and replay guest-visible libslirp state (DHCP lease, ARP/NAT tables). Host-side TCP flows are not preserved. `fs_dir` host shares are rejected on mesh creates; use a volume.

Writable mesh volumes use epoch-fenced leases with TTL self-fencing. Production grants them through PostgreSQL; development meshes use strict-majority peer votes. Writable volumes require at least three expected members and are rejected otherwise. Read-only volumes are unrestricted; a local daemon uses only host `flock`.

Before planned downtime, move work or drain the node:

```sh
VMON_API_TOKEN=T vmon mesh migrate <name> <node>
VMON_API_TOKEN=T vmon mesh leave --drain
```

Production owner and volume leases are authoritative: missed renewal self-fences the local VMM or writer before a successor activates. Development-mode non-volume epochs remain best-effort and converge through gossip.

### Scoped client token

Set `VMON_CLIENT_TOKEN` when clients should run sandboxes without full mesh administration. The client token authorizes normal sandbox RPCs but rejects mesh administration and `SandboxService.Migrate`; the full `VMON_API_TOKEN` retains those operations. Give SDK clients their scoped token through the usual `VMON_API_TOKEN` client environment variable.

For rotation, `VMON_API_TOKEN` and `VMON_CLIENT_TOKEN` may each be a comma-separated list. During rollover, run gateways with `old,new`; any listed value authorizes for that tier, and the client tier remains blocked from mesh-admin and migrate routes.

### TLS

Run `vmon serve --tls-cert PATH --tls-key PATH` (or set `VMON_TLS_CERT` and `VMON_TLS_KEY`) to serve the gateway over HTTPS. Advertise the matching `https://...` URL in mesh setup/join data; when peers advertise `https` URLs, the inter-node exec WebSocket proxy uses `wss`.

### Testing the cluster

`just cluster-e2e` runs the gated Rust cluster end-to-end suite on KVM/HVF hosts (`VMON_CLUSTER_E2E=1 VMON_E2E=1 cargo test --test cluster_e2e -- --test-threads=1`). The nightly `mesh-soak.yml` workflow loops the same Rust suite under host-level tc-netem, and the fault/invariant tests cover the failure-mode contracts without booting guests.

### Security

The full bearer token is shared by all nodes and grants full control. Keep it secret. Contexts persist tokens only with `--save-token`; otherwise clients read either the full token or a scoped client token from `VMON_API_TOKEN` when they connect.

## Compared with Modal sandboxes

vmon's wedge is local ownership of the sandbox stack. It can create from memory snapshots using copy-on-write fork, and the warm pool keeps those clones ready for near-instant starts. Modal's VM runtime does not expose memory snapshots. vmon also ships a self-hosted REST API; Modal's control plane is SDK-only over proprietary gRPC.

The platform includes writable named volumes, secrets, pty exec, egress controls, authenticated port tunnels, tags, snapshot-to-image flows, and VMM-enforced timeouts. GPU passthrough is a non-goal for this project.

## Host integration testing

The Rust integration suite under `tests/` runs end-to-end on KVM and HVF. The WHP backend currently has native Windows unit, lint, and binary-build coverage but no automated real-guest CI runner.

| Environment | Backend / arch | Run it with | Networking exercised |
| --- | --- | --- | --- |
| Linux host | KVM, x86_64 or aarch64 | `just integration` | TAP (`--tap`) |
| macOS host | HVF, aarch64 (Apple Silicon) | `just integration` | user-mode NAT (`--net user`) |
| Lima on macOS | KVM, aarch64 (nested) | `just lima-integration` | TAP (`--tap`) |

Set `VMON_E2E=1` to opt in to booting guests; without it the boot tests early-return so a plain `cargo test` stays hermetic. The Linux and macOS recipes fetch the pinned per-architecture guest assets (`demo/fetch-test-assets.sh` selects the x86_64 `bzImage` or aarch64 `Image` for the host). A Windows run needs the same x86_64 assets under `target/test-assets/`. On macOS, the recipe routes each test binary through `demo/hvf-test-runner.sh`, which ad-hoc codesigns the spawned `vmon` with the hypervisor entitlement immediately before it runs. Building the macOS assets needs `brew install libslirp pkg-config e2fsprogs cpio`.

What each environment exercises:

- **Boot, virtio-blk, virtio-fs, JSON control (pause/snapshot/resume/quit), metrics, timeout, snapshot/restore/fork** run in the KVM/HVF integration jobs; the corresponding WHP paths have compile and unit coverage.
- **TAP networking and throughput** require a host TAP and run on Linux/KVM only; export `VMON_TAP=<iface>` (and optionally `VMON_HOST_IP`).
- **User-mode NAT** (DHCP lease + outbound TCP through the slirp gateway) runs on macOS/HVF and Windows/WHP; the current real-guest CI case runs on macOS.
- **userfaultfd paging, jail, and the seccomp audit** are Linux-only.
- **The CLI capability matrix** (`tests/cli_matrix.rs`) needs no hypervisor and checks platform flag combinations, including PCI on unsupported architectures, `--net user` outside macOS/Windows, TAP conflicts, UEFI firmware requirements, and remote-filesystem validation.

The delta snapshot code path supports KVM, HVF, and WHP; automated real-guest coverage currently runs on KVM and HVF.

## Demo commands

The checked-in demos are host-side scripts. They expect Linux tooling listed in each script and may need `sudo`.

```sh
# On an arm64 Linux host with /dev/kvm: boot a busybox initramfs with virtio-blk and virtio-net.
VMON_BIN=./target/release/vmon demo/run-arm64-demo.sh [arm64-Image]

# From macOS/Apple silicon: run the arm64 demo inside a Lima VM with nested KVM enabled.
limactl start --vm-type=vz --set='.nestedVirtualization=true' --name=kvm template:default
demo/run-on-lima.sh demo kvm

# Build an OCI image into an ext4 root disk; this does not use /dev/kvm.
demo/build-oci-rootfs.sh docker://busybox /tmp/vmon-demo/oci-rootfs.img 256M

# Boot a real Ubuntu 24.04 cloud image through UEFI firmware to a serial login.
demo/run-uefi-ubuntu.sh
```

## Current limitations

- This is not a production isolation boundary. It has not had a security audit.
- The trusted computing base includes the vmon process, KVM, HVF, or WHP, the host kernel, guest kernel/image inputs, disk images, snapshots, and host paths passed on the command line.
- Snapshot restore requires a matching build architecture, hypervisor backend, and supported snapshot version (currently version 3).
- Bare VMM networking uses host TAP devices on Linux. On macOS/HVF and Windows/WHP, `--net user` provides user-mode NAT through libslirp. macOS `--tap` still requires vmnet-style host networking support that is unavailable to the ad-hoc-signed binary. User-mode NAT currently provides outbound/DHCP/DNS guest connectivity, not same-LAN bridging or inbound host port forwarding.
- Host paths exposed through virtio-fs should be dedicated directories. `--fs-dir` is read-only; named `--volume` mounts are writable unless `:ro` is set.
- Linux applies Stage-B process filters (seccomp syscall filtering, Landlock path policy, `no_new_privs`, and resource-limit tightening) by default; pass `--no-sandbox` to disable them for local development. `--jail` adds cgroup v2, namespaces, pivot-root, and uid/gid drop. Windows and macOS currently run without this host-process sandbox.
- Launch-time caps are enforced for accidental fanout: up to 64 vCPUs, 64 GiB RAM, and 32 fork children.

## Security model status

Treat guests, guest-controlled virtqueue data, and restored snapshot files as untrusted. Treat kernel/initrd/rootfs images and host paths supplied by an operator as trusted configuration. On Linux, keep the default Stage-B filters enabled and use `--jail` for production namespace, cgroup, pivot-root, and uid/gid isolation. Unix control and agent sockets are operator-owned, mode `0600`, live in private parent directories, and on Linux accept only root or the launch uid. Windows uses local named pipes with remote-client rejection.

Do not expose vmon control sockets, host filesystem shares, TAP devices, vmnet attachments, or user-mode forwarded ports across trust boundaries without the jail and external host network policy. See [`SECURITY.md`](SECURITY.md) for the vulnerability reporting policy.

## License

Copyright © 2026 Stencil Labs, Inc.

| Scope | License |
| --- | --- |
| `vmon`, `vmm`, `vmond`, `vmon-agent`, `vmon-cloud`, the web UI, deployment stacks, documentation, demos, and scripts | [MIT](LICENSE) |
| `vmon-proto`, `proto/vmon/v1/*.proto`, `openapi.json`, the Python, TypeScript, and Go SDKs, and their generated bindings | [MIT](LICENSE) OR [Apache-2.0](LICENSE-APACHE), at your option |

This licensing applies only to first-party Vibemon work in the scopes above. Third-party dependencies, vendored code, logos, assets, guest kernels, images, BusyBox, firmware, and other separately licensed material remain under their respective terms. See [THIRD-PARTY-NOTICES.txt](THIRD-PARTY-NOTICES.txt) for attribution and source details.

Contributions are accepted under the license of the affected package; see [CONTRIBUTING.md](CONTRIBUTING.md).
