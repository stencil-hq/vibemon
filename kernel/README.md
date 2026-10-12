# vmon guest kernel

`vmon serve` boots every sandbox and template build with one guest kernel.
This directory builds the **vmon kernel**, the default guest kernel: the
cloud-hypervisor source tree and `ch_defconfig`, plus the features E2B guests
rely on. The plain cloud-hypervisor kernel (the node's last-resort download)
has no netfilter, no IPv6, no bridge and no network filesystems, so Docker,
`nft`, `ip6tables`, NFS and the `nftables.service` / `run-rpc_pipefs.mount`
units in the default templates fail under it.

| File | Purpose |
|---|---|
| `build.sh` | Reproducible build (pinned source tarball + sha256, fixed build metadata). |
| `config/common.config` | Fragment for both arches (see the groups in the file). |
| `config/aarch64.config`, `config/x86_64.config` | Arch fragments (boot path pins, IA32 emulation). |
| `Dockerfile` | Ubuntu 24.04 toolchain image `just kernel` uses on macOS. |

## Building

```sh
just kernel                      # both arches → target/kernel/
just kernel --arch aarch64       # one arch
kernel/build.sh --config-only    # resolve + verify the .config only (seconds)
```

`build.sh` runs on Linux. On macOS `just kernel` runs it in one Ubuntu 24.04
container (`kernel/Dockerfile`, `--cpus 4`, niced) with the source and build
trees in the Docker volume `vmon-kernel-cache` (the kernel tree needs a
case-sensitive filesystem); outputs still land in `target/kernel/`. The host
arch builds natively; the other arch is cross-compiled
with `gcc-<arch>-linux-gnu`. Tools: `build-essential flex bison bc libelf-dev
libssl-dev dwarves cpio xz-utils curl` (+ `gcc-aarch64-linux-gnu` /
`gcc-x86-64-linux-gnu`). The source tarball and build trees are cached in
`~/.cache/vmon-kernel` (`--cache`); an incremental rebuild takes about a
minute, a cold build 3–4 minutes per arch on 8 cores (about twice that in the
4-CPU macOS container).

Outputs in `target/kernel/` (`--out`):

| File | Use |
|---|---|
| `Image-aarch64` | arm64 boot image for `--kernel` |
| `bzImage-x86_64` | x86_64 boot image for `--kernel` |
| `vmlinux-x86_64` | x86_64 ELF (DWARF stripped, BTF kept) |
| `config-<arch>` | resolved `.config` (also at `/proc/config.gz` in the guest) |
| `SHA256SUMS` | checksums |

Source: `cloud-hypervisor/linux` tag `ch-release-v6.16.9-20260508` — the tag
the default aarch64 pin was released from, so the boot path (virtio-mmio and
virtio-pci, virtio blk/net/console/fs/rng/vsock/balloon, 8250 + PL011 consoles,
GICv3 + PSCI on arm64, kvm-clock/PVH on x86_64) is unchanged.
`KBUILD_BUILD_TIMESTAMP/USER/HOST/VERSION` are fixed and BTF is encoded
single-threaded (`JOBS=1` for pahole), so the same toolchain (Ubuntu 24.04
gcc 13.3 + pahole 1.25, as in the macOS container and the release job) produces
bit-identical images regardless of `--jobs`. A cross build embeds a different
compiler name (`x86_64-linux-gnu-gcc` vs `gcc`) in `/proc/version` and the
config, so compare like with like. The release workflow (`.github/workflows/release.yml`,
job `kernel`) builds both arches on native Ubuntu 24.04 runners; the
`binaries` job packs each into `vmon-assets-<arch>.tar.gz` as
`vmon-kernel-<arch>` (+ `vmon-kernel-<arch>.config`) next to `vmon-agent-<arch>`
(`scripts/package-guest-assets.sh`, which takes `VMON_RELEASE_KERNEL` or
`target/kernel/`), and the raw images are attached to the GitHub release.

Every line in the fragments is verified after `olddefconfig`: an option that
was dropped (unmet dependency, renamed symbol) fails the build instead of
silently producing a weaker kernel.

## What it adds over the default kernel

All built in (`CONFIG_MODULES=n`, as upstream): guests never need
`/lib/modules` or `modprobe`.

- **Firewalling / NAT**: nf_tables (inet/ip/ip6/arp/bridge/netdev, ct, nat,
  masq, redir, reject, fib, socket, tproxy, `nft_compat`), xtables matches and
  targets used by Docker, Kubernetes and firewalld, iptables **legacy and nft**
  (`iptables-legacy`, `iptables-nft`, ip6tables, arptables, ebtables),
  conntrack (+ netlink, events, zones, labels, FTP/TFTP helpers), ipset, IPVS.
- **Networking**: IPv6, policy routing, multipath, bridge (+ VLAN filtering,
  br_netfilter), veth, macvlan/macvtap, ipvlan, vxlan, geneve, WireGuard,
  802.1q, dummy, GRE/IPIP, MPTCP, SCTP, XFRM/ESP (encrypted overlay
  networks), traffic control (htb, fq, fq_codel, netem, ingress, bpf/u32/flower
  classifiers, mirred).
- **Containers**: user/pid/net/ipc/uts/time namespaces, every cgroup v2
  controller (cpu, cpuset, memory, io incl. io.max/io.cost/io.latency, pids,
  freezer via `cgroup.freeze`, hugetlb, misc, perf_event), PSI,
  seccomp, `CHECKPOINT_RESTORE` (kcmp), overlayfs. Moby's
  `contrib/check-config.sh` reports nothing missing under "Generally
  Necessary"; only SELinux/AppArmor (deliberately off) and ZFS userland are
  optional misses.
- **BPF**: JIT (always on), cgroup BPF, BPF LSM, kprobes/uprobes/ftrace
  events, BTF (`/sys/kernel/btf/vmlinux` for CO-RE tools).
- **Security**: Landlock + BPF LSMs (`lsm=landlock,bpf`).
- **Filesystems**: NFS v3/v4.x client (+ `rpc_pipefs`, RPCSEC_GSS), CIFS,
  FUSE/virtiofs, squashfs (all compressors), EROFS, XFS, Btrfs, ISO 9660,
  quota, fanotify permission events, loop, NBD, device-mapper
  (thin, snapshot, crypt).
- **Introspection**: `/proc/config.gz`.

Removed from `ch_defconfig`: the ktimer PPS test client and `PPS_DEBUG` (a
wakeup and two dmesg lines every second in every sandbox) and the RAID6
boot benchmark pulled in by Btrfs (~1.1 s of a ~2 s boot under HVF).

## Using it

The node (`node/src/assets.rs`) picks the guest kernel in this order:

1. `--kernel <path>` (`VMON_KERNEL`).
2. `vmon-kernel-<arch>` next to the `vmon` binary — where the release assets
   tarball puts it.
3. `target/kernel/Image-aarch64` / `bzImage-x86_64` in the nearest ancestor
   of the binary's directory — a dev tree after `just kernel`.
4. The pinned cloud-hypervisor kernel, downloaded into `<data-dir>/assets/`
   with a WARN that netfilter (nftables/iptables, Docker), IPv6, bridge and
   NFS are missing.

Template snapshots capture the running kernel, so templates built under
another kernel keep booting it: rebuild templates after switching kernels.
