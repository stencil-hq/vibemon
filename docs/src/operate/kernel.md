# Guest kernel

Every sandbox and template build boots one guest kernel chosen by the node. This page covers the vmon kernel (the default), what it adds over the plain cloud-hypervisor kernel, how to build it, and how a node picks its kernel. Read it when installing nodes from source or when a template needs Docker, `nft`, IPv6, or NFS in the guest.

## Why the vmon kernel

The vmon kernel is the cloud-hypervisor kernel source and `ch_defconfig` plus the features E2B guests rely on. The plain cloud-hypervisor kernel, which a node downloads only as a last resort, has no netfilter, no IPv6, no bridge, and no NFS support, so guest features such as Docker, `nft`, `ip6tables`, and NFS cannot work fully under it. The `mcp-gateway` template runs Docker in the guest and needs the vmon kernel.

Release assets ship it: `vmon-assets-<arch>.tar.gz` contains `vmon-kernel-<arch>` (and its resolved config `vmon-kernel-<arch>.config`) next to `vmon-agent-<arch>`; the raw images are also attached to each GitHub release. See [Installation](../get-started/installation.md).

## What it adds

Everything is built in (`CONFIG_MODULES=n`, as upstream): guests never need `/lib/modules` or `modprobe`.

| Area | Features |
|---|---|
| Firewalling / NAT | nf_tables (inet/ip/ip6/arp/bridge/netdev, ct, nat, masq, redir, reject, fib, socket, tproxy, `nft_compat`); xtables matches and targets used by Docker, Kubernetes, and firewalld; iptables legacy and nft (`iptables-legacy`, `iptables-nft`, ip6tables, arptables, ebtables); conntrack (+ netlink, events, zones, labels, FTP/TFTP helpers); ipset; IPVS |
| Networking | IPv6, policy routing, multipath, bridge (+ VLAN filtering, br_netfilter), veth, macvlan/macvtap, ipvlan, vxlan, geneve, WireGuard, 802.1q, dummy, GRE/IPIP, MPTCP, SCTP, XFRM/ESP, traffic control (htb, fq, fq_codel, netem, ingress, bpf/u32/flower classifiers, mirred) |
| Containers | user/pid/net/ipc/uts/time namespaces; every cgroup v2 controller (cpu, cpuset, memory, io incl. io.max/io.cost/io.latency, pids, freezer via `cgroup.freeze`, hugetlb, misc, perf_event); PSI; seccomp; `CHECKPOINT_RESTORE`; overlayfs |
| BPF | JIT (always on), cgroup BPF, BPF LSM, kprobes/uprobes/ftrace events, BTF (`/sys/kernel/btf/vmlinux` for CO-RE tools) |
| Security | Landlock and BPF LSMs (`lsm=landlock,bpf`) |
| Filesystems | NFS v3/v4.x client (+ `rpc_pipefs`, RPCSEC_GSS), CIFS, FUSE/virtiofs, squashfs (all compressors), EROFS, XFS, Btrfs, ISO 9660, quota, fanotify permission events, loop, NBD, device-mapper (thin, snapshot, crypt) |
| Introspection | `/proc/config.gz` |

Removed from `ch_defconfig`: the ktimer PPS test client and `PPS_DEBUG` (a wakeup and two dmesg lines every second in every sandbox), and the RAID6 boot benchmark pulled in by Btrfs (~1.1 s of a ~2 s boot under HVF).

The boot path is unchanged from the cloud-hypervisor kernel: virtio-mmio and virtio-pci; virtio blk/net/console/fs/rng/vsock/balloon; 8250 and PL011 consoles; GICv3 + PSCI on arm64; kvm-clock/PVH on x86_64. Source: [`cloud-hypervisor/linux`](https://github.com/cloud-hypervisor/linux) tag `ch-release-v6.16.9-20260508`.

## Building

```sh
just kernel                      # both arches → target/kernel/
just kernel --arch aarch64       # one arch
kernel/build.sh --config-only    # resolve and verify the .config only
```

`just kernel` passes its arguments to [`kernel/build.sh`](https://github.com/stencil-hq/vibemon/blob/main/kernel/build.sh):

| Option | Meaning |
|---|---|
| `--arch aarch64\|x86_64` | Build one arch; repeatable (default: both) |
| `--out DIR` | Output directory (default `target/kernel/`) |
| `--cache DIR` | Source tarball and build trees (default `$XDG_CACHE_HOME/vmon-kernel`, else `~/.cache/vmon-kernel`) |
| `--jobs N` | Parallel jobs |
| `--config-only` | Resolve and verify the `.config`, write `config-<arch>`, skip compiling |

**Linux** (aarch64 or x86_64): `build.sh` runs natively. The host arch builds natively; the other arch is cross-compiled with `gcc-<arch>-linux-gnu`. On Debian/Ubuntu install the toolchain plus the cross compiler for the other arch (`gcc-aarch64-linux-gnu` on x86_64 hosts, `gcc-x86-64-linux-gnu` on aarch64 hosts):

```sh
sudo apt-get install build-essential flex bison bc libelf-dev libssl-dev \
  dwarves python3 cpio xz-utils curl
```

**macOS**: `just kernel` runs `build.sh` in one Ubuntu 24.04 container built from [`kernel/Dockerfile`](https://github.com/stencil-hq/vibemon/blob/main/kernel/Dockerfile), capped at 4 CPUs and niced, with the source and build trees in the Docker volume `vmon-kernel-cache` (the kernel tree needs a case-sensitive filesystem). Docker must be running. Outputs still land in the checkout's `target/kernel/`.

Every line in the config fragments is verified after `olddefconfig`: an option that was dropped (unmet dependency, renamed symbol) fails the build instead of silently producing a weaker kernel. Builds are reproducible: with the same toolchain (the Ubuntu 24.04 toolchain used by the macOS container and the release job), images are bit-identical regardless of `--jobs`. A cross build embeds a different compiler name in `/proc/version` and the config, so compare like with like.

### Outputs

| File in `target/kernel/` | Use |
|---|---|
| `Image-aarch64` | arm64 boot image |
| `bzImage-x86_64` | x86_64 boot image |
| `vmlinux-x86_64` | x86_64 ELF (DWARF stripped, BTF kept) |
| `config-<arch>` | Resolved `.config` (also `/proc/config.gz` in the guest) |
| `SHA256SUMS` | Checksums |

## How a node picks its kernel

At startup the node resolves the guest kernel in this order and logs the result:

1. `--kernel <path>` (`VMON_KERNEL`). The file must exist.
2. `vmon-kernel-<arch>` next to the `vmon` binary, where the release assets tarball puts it.
3. `target/kernel/Image-aarch64` or `target/kernel/bzImage-x86_64` in the nearest ancestor of the binary's directory: a dev tree after `just kernel`.
4. The pinned cloud-hypervisor kernel, downloaded once into `<data-dir>/assets/` and verified by SHA-256.

The fallback logs a warning:

```text
no vmon guest kernel (vmon-kernel-<arch> next to vmon, or target/kernel/ from `just kernel`); falling back to the pinned cloud-hypervisor kernel, which lacks netfilter (nftables/iptables, Docker), IPv6, bridge and NFS
```

> [!WARNING]
> Template snapshots capture the running kernel, so templates built under one kernel keep booting it. Rebuild templates after switching kernels.

See [Running `vmon serve`](serve.md#guest-kernel-and-agent) for how the node finds the guest agent binary.
