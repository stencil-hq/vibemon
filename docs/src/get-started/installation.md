# Installation

What to install on each host: the release assets, platform support, host requirements, driver state, and building from source. For a single-machine trial, the [Quickstart](quickstart.md) is shorter.

## Platform support

| | Linux / KVM | macOS 15+ / HVF (Apple Silicon) | Windows / WHP |
|---|---|---|---|
| Guest arch | x86_64, aarch64 | aarch64 | x86_64 |
| node role | ✅ TAP + NAT, transparent egress proxy | ✅ user-mode NAT, in-VMM egress filter | — |
| driver role | ✅ | ✅ | — |
| `vmon vmm` | ✅ | ✅ | ✅ (only component that builds) |
| Release binaries | ✅ x86_64, aarch64 (static musl) | — (build from source) | — |

Egress features differ per platform; see [Networking and egress](../operate/networking.md).

## Release assets

Tags matching `v*` run [`.github/workflows/release.yml`](https://github.com/stencil-hq/vibemon/blob/main/.github/workflows/release.yml), which attaches these files to the GitHub release (each with a `.sha256` sidecar):

| Asset | Contents |
|---|---|
| `vmon-<target>.tar.gz` | Directory `vmon-<target>/`: `vmon`, `vmon-agent-<arch>`, `LICENSE`, `LICENSE-APACHE`, `THIRD-PARTY-NOTICES.txt`. |
| `vmon-assets-<arch>.tar.gz` | Directory `vmon-assets-<arch>/`: `vmon-kernel-<arch>` (guest kernel), `vmon-kernel-<arch>.config` (its resolved config), `vmon-agent-<arch>` (static guest agent), `MANIFEST.txt` (sources, checksums, licenses), `LICENSE`, `LICENSE-APACHE`, `THIRD-PARTY-NOTICES.txt`. |
| `bzImage-x86_64`, `vmlinux-x86_64`, `config-x86_64` | Raw x86_64 kernel images and config from the `kernel` job. |
| `Image-aarch64`, `config-aarch64` | Raw aarch64 kernel image and config. |

`<target>` is `x86_64-unknown-linux-musl` or `aarch64-unknown-linux-musl`; `<arch>` is `x86_64` or `aarch64`.

> [!NOTE]
> Releases up to 0.3.2 are the previous product (`vmond`) and do not have this layout. See [`CHANGELOG.md`](https://github.com/stencil-hq/vibemon/blob/main/CHANGELOG.md).

## Install a release

Nodes look for `vmon-kernel-<arch>` and `vmon-agent-<arch>` in the directory that holds `vmon`, so put all three in one place. Both tarballs contain a `vmon-agent-<arch>`; either works.

```sh
TAG=vX.Y.Z                       # a release tag
ARCH=$(uname -m)                 # x86_64 or aarch64
BASE=https://github.com/stencil-hq/vibemon/releases/download/$TAG
for f in vmon-$ARCH-unknown-linux-musl.tar.gz vmon-assets-$ARCH.tar.gz; do
  curl -fLO "$BASE/$f" && curl -fLO "$BASE/$f.sha256"
done
sha256sum --check vmon-$ARCH-unknown-linux-musl.tar.gz.sha256 vmon-assets-$ARCH.tar.gz.sha256
tar -xzf vmon-$ARCH-unknown-linux-musl.tar.gz
tar -xzf vmon-assets-$ARCH.tar.gz
sudo install -d /opt/vmon
sudo install -m 0755 vmon-$ARCH-unknown-linux-musl/vmon vmon-$ARCH-unknown-linux-musl/vmon-agent-$ARCH /opt/vmon/
sudo install -m 0644 vmon-assets-$ARCH/vmon-kernel-$ARCH /opt/vmon/
```

Driver-only hosts need just `vmon`. `--kernel` (`VMON_KERNEL`) and `--agent` (`VMON_AGENT`) override the lookup; the full search order is in [Running vmon serve](../operate/serve.md#guest-kernel-and-agent).

## Host requirements

### Linux nodes

- `/dev/kvm`.
- Root or `CAP_NET_ADMIN` for TAP networking (the only Linux mode; `--net user` is rejected on Linux). Without it, the default `--net auto` starts the node but rejects sandbox creates; explicit `--net tap` fails at startup. See [Linux privileges](../operate/serve.md#linux-privileges) for running VMMs as an unprivileged uid with `--vmm-user`.
- `ip` (iproute2), `iptables`, and `iptables-restore`, which the node runs to set up TAPs and NAT rules.

### macOS nodes

- macOS 15 or later on Apple Silicon.
- `vmon` must be ad-hoc signed with the hypervisor entitlement in [`hvf.entitlements`](https://github.com/stencil-hq/vibemon/blob/main/hvf.entitlements) (`com.apple.security.hypervisor`). `just build` and `just release` do this; by hand:

  ```sh
  codesign --sign - --entitlements hvf.entitlements --force target/release/vmon
  ```

- No root: HVF needs none, and guest networking is user-mode NAT inside the VMM.

### Drivers

Drivers need no external database: they keep all durable state (teams, keys, templates, builds, sandboxes, events, webhooks, secrets, volumes) in the raft KV they replicate among themselves, in memory and snapshotted under `<data-dir>/raft/`. Keep that directory on persistent storage. Every driver of a cluster uses the same cluster secret; see [Running vmon serve](../operate/serve.md#cluster-secret).

## Build from source

Requires rustup (the toolchain is pinned in [`rust-toolchain.toml`](https://github.com/stencil-hq/vibemon/blob/main/rust-toolchain.toml)) and `just`.

```sh
just release                     # target/release/vmon (+ HVF codesign on macOS)
just agent-musl                  # target/test-assets/vmon-agent-<arch>
just kernel --arch "$(uname -m | sed 's/arm64/aarch64/')"   # target/kernel/
```

A `vmon` built at `target/release/vmon` finds the agent in `target/test-assets/` and the kernel in `target/kernel/` relative to its executable path, not the working directory. To install it elsewhere, copy the agent and kernel next to it as `vmon-agent-<arch>` and `vmon-kernel-<arch>` (the kernel file is `target/kernel/Image-aarch64` or `target/kernel/bzImage-x86_64`).

`just local-release <target>` builds the release tarballs for a Linux musl target into `dist/` without uploading anything. It needs `cargo-zigbuild` and the target arch's kernel in `target/kernel/` (`just kernel`), and it runs the packaged `vmon` to smoke-check it, so a cross-arch target on Linux needs `qemu-user` or a `VMON_RELEASE_RUNNER` wrapper. On macOS, `VMON_RELEASE_RUNNER` must name an executable VM/container wrapper even when the CPU architecture matches. Toolchains, tests, and the other recipes: [Building and testing](../develop/building.md).

## Verify the install

```sh
vmon --version                   # prints `vmon <version>`
curl -i http://127.0.0.1:3000/health
```

`GET /health` on a driver's `--listen` address returns `204 No Content` when the driver can serve linearizable reads of the raft KV, and `500` or `503` otherwise.
