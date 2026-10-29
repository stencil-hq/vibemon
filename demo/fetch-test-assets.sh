#!/bin/bash
# Fetch and build the pinned guest assets used by the e2e integration tests.
# Assets are selected for the host architecture (override VMON_ASSET_ARCH):
#   * x86_64  -> ELF `vmlinux` + static busybox (busybox.net)
#   * aarch64 -> uncompressed arm64 `Image` + busybox/musl from Alpine minirootfs
# All outputs live under target/test-assets/ (ignored by git).
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/.." && pwd)

# --- target architecture -----------------------------------------------------
ARCH=${VMON_ASSET_ARCH:-$(uname -m)}
case "$ARCH" in
  arm64 | aarch64) ARCH=aarch64 ;;
  amd64 | x86_64) ARCH=x86_64 ;;
  *) echo "error: unsupported asset arch '$ARCH' (want x86_64 or aarch64)" >&2; exit 1 ;;
esac

# --- asset paths and pins ----------------------------------------------------
ASSET_DIR=${ASSET_DIR:-"$REPO/target/test-assets"}

# busybox's musl loader is bundled into the guest tree for dynamically linked
# builds; it stays empty for the statically linked x86_64 busybox.
MUSL_LOADER=""

if [ "$ARCH" = x86_64 ]; then
  KERNEL_NAME="bzImage-x86_64"
  KERNEL_URL=${KERNEL_URL:-"https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.12.8-20250613/bzImage-x86_64"}
  KERNEL_SHA256=${KERNEL_SHA256:-"d4af401aa859e4659d4b08a153ac608eb6a315c6918e567daa46981af5d2e5ef"}

  BUSYBOX_PATH=${BUSYBOX_PATH:-"$ASSET_DIR/busybox-x86_64"}
  BUSYBOX_URL=${BUSYBOX_URL:-"https://busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox"}
  BUSYBOX_SHA256=${BUSYBOX_SHA256:-"6e123e7f3202a8c1e9b1f94d8941580a25135382b99e8d3e34fb858bba311348"}

  INITRAMFS_PATH=${INITRAMFS_PATH:-"$ASSET_DIR/busybox-initramfs-x86_64.cpio.gz"}
  ROOTFS_PATH=${ROOTFS_PATH:-"$ASSET_DIR/rootfs-x86_64.ext4"}
else
  # The Cloud Hypervisor release kernel (raw arm64 Image, v6.16.9): boots cleanly
  # under KVM and HVF with our 8250/earlycon UART and ships CONFIG_VIRTIO_FS=y so
  # virtio-fs shares/volumes work. (The firecracker-ci kernel lacked virtio-fs;
  # the quickstart aarch64 kernel hangs for want of an 8250 console.)
  KERNEL_NAME="Image-aarch64"
  KERNEL_URL=${KERNEL_URL:-"https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.16.9-20260508/Image-arm64"}
  KERNEL_SHA256=${KERNEL_SHA256:-"69d1b1235381ec50f1b45cf771a7dff4a9013d452833ab34682d6283e2114010"}

  # busybox.net publishes no aarch64 binary; extract a working busybox + its
  # musl loader from a pinned Alpine aarch64 minirootfs instead.
  BUSYBOX_PATH=${BUSYBOX_PATH:-"$ASSET_DIR/busybox-aarch64"}
  MINIROOTFS_URL=${MINIROOTFS_URL:-"https://dl-cdn.alpinelinux.org/alpine/v3.20/releases/aarch64/alpine-minirootfs-3.20.3-aarch64.tar.gz"}
  MINIROOTFS_SHA256=${MINIROOTFS_SHA256:-"041fa34a81788242df9e78fa69b97ab45b8ec47ddbf88864755610414a7bf3de"}
  MUSL_LOADER=${MUSL_LOADER:-"$ASSET_DIR/ld-musl-aarch64.so.1"}

  INITRAMFS_PATH=${INITRAMFS_PATH:-"$ASSET_DIR/busybox-initramfs-aarch64.cpio.gz"}
  ROOTFS_PATH=${ROOTFS_PATH:-"$ASSET_DIR/rootfs-aarch64.ext4"}
fi

KERNEL_PATH=${KERNEL_PATH:-"$ASSET_DIR/$KERNEL_NAME"}
# The guest agent baked into the initramfs (agent smoke tests skip when absent).
SMOKE_AGENT=${VMON_SMOKE_AGENT:-"$ASSET_DIR/vmon-agent-$ARCH"}
ROOTFS_SIZE=${ROOTFS_SIZE:-"64M"}
ROOTFS_VERSION=${ROOTFS_VERSION:-"2026-06-26.2-$ARCH"}

# mkfs.ext4 lives in /sbin or /usr/sbin on Linux, and in the keg-only
# e2fsprogs formula on macOS (`brew install e2fsprogs`).
export PATH="$PATH:/usr/sbin:/sbin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/opt/e2fsprogs/sbin:/opt/homebrew/sbin:/usr/local/sbin"

# --- preconditions ----------------------------------------------------------
missing=0
need() {  # need <command> <install-hint>
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: required tool '$1' not found on PATH" >&2
    echo "       hint: $2" >&2
    missing=1
  fi
}

need curl "apt-get install curl  |  dnf install curl  |  brew install curl"
need mkfs.ext4 "apt-get install e2fsprogs  |  dnf install e2fsprogs  |  brew install e2fsprogs"
need cpio "apt-get install cpio  |  dnf install cpio  |  brew install cpio"
need gzip "apt-get install gzip  |  dnf install gzip  |  brew install gzip"
need tar "apt-get install tar  |  dnf install tar  |  preinstalled on macOS"

if command -v sha256sum >/dev/null 2>&1; then
  SHA256_TOOL=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  SHA256_TOOL=shasum
else
  echo "error: required tool 'sha256sum' or 'shasum' not found on PATH" >&2
  echo "       hint: apt-get install coreutils  |  dnf install coreutils" >&2
  missing=1
  SHA256_TOOL=
fi

if [ "$missing" -ne 0 ]; then
  echo "error: install the missing tool(s) listed above, then re-run" >&2
  exit 1
fi

mkdir -p "$ASSET_DIR"

TMP_DIR=
cleanup() {
  if [ -n "${TMP_DIR:-}" ]; then rm -rf "$TMP_DIR"; fi
}
trap cleanup EXIT

calc_sha256() {
  local sum ignored
  if [ "$SHA256_TOOL" = sha256sum ]; then
    read -r sum ignored < <(sha256sum "$1")
  else
    read -r sum ignored < <(shasum -a 256 "$1")
  fi
  printf '%s\n' "$sum"
}

# The initramfs bakes in the guest agent when present, so the agent content
# participates in the cache key: rebuilding the agent regenerates the image.
if [ -z "${INITRAMFS_VERSION:-}" ]; then
  INITRAMFS_VERSION="2026-10-29.1-$ARCH"
  if [ -f "$SMOKE_AGENT" ]; then
    INITRAMFS_VERSION="$INITRAMFS_VERSION-a$(calc_sha256 "$SMOKE_AGENT" | cut -c1-12)"
  fi
fi

verify_sha256() {  # verify_sha256 <path> <expected-sha256> <label>
  local path=$1 expected=$2 label=$3 actual
  if [ ! -f "$path" ]; then
    echo "error: missing $label at $path" >&2
    return 1
  fi
  actual=$(calc_sha256 "$path")
  if [ "$actual" != "$expected" ]; then
    echo "error: checksum mismatch for $label" >&2
    echo "       path:     $path" >&2
    echo "       expected: $expected" >&2
    echo "       actual:   $actual" >&2
    return 1
  fi
}

fetch_verified() {  # fetch_verified <label> <url> <path> <sha256>
  local label=$1 url=$2 path=$3 expected=$4 tmp actual
  if [ -f "$path" ]; then
    if verify_sha256 "$path" "$expected" "$label"; then
      echo "[assets] $label: using existing $path"
      return 0
    fi
    echo "[assets] $label: existing file is corrupt; re-downloading" >&2
    rm -f "$path"
  fi

  echo "[assets] $label: downloading $url"
  tmp=$(mktemp "$ASSET_DIR/.${label}.XXXXXX")
  curl --fail --location --show-error --retry 3 --connect-timeout 20 --output "$tmp" "$url"
  actual=$(calc_sha256 "$tmp")
  if [ "$actual" != "$expected" ]; then
    rm -f "$tmp"
    echo "error: checksum mismatch for downloaded $label" >&2
    echo "       url:      $url" >&2
    echo "       expected: $expected" >&2
    echo "       actual:   $actual" >&2
    exit 1
  fi
  mv "$tmp" "$path"
  verify_sha256 "$path" "$expected" "$label"
}

generated_ok() {  # generated_ok <path> <version>
  local path=$1 version=$2 sum_file version_file expected actual recorded_version ignored
  sum_file="$path.sha256"
  version_file="$path.version"
  [ -f "$path" ] || return 1
  [ -f "$sum_file" ] || return 1
  [ -f "$version_file" ] || return 1
  recorded_version=$(cat "$version_file")
  [ "$recorded_version" = "$version" ] || return 1
  read -r expected ignored < "$sum_file" || return 1
  actual=$(calc_sha256 "$path")
  [ "$actual" = "$expected" ] || return 1
}

write_generated_metadata() {  # write_generated_metadata <path> <version>
  local path=$1 version=$2 sum
  sum=$(calc_sha256 "$path")
  printf '%s  %s\n' "$sum" "$(basename "$path")" > "$path.sha256"
  printf '%s\n' "$version" > "$path.version"
}

write_test_init() {  # write_test_init <path>
  cat > "$1" <<'INIT_EOF'
#!/bin/busybox sh
/bin/busybox --install -s /bin 2>/dev/null || true
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export PATH

# `sync` flushes filesystems, not the serial console: let the tty drain before
# the kernel's halt message, or it can land in the middle of the last result
# line (seen as `MAC=aa:bb:cc:de:[ 0.97] reboot: …` on a slow host).
drain_console() {
  sync
  sleep 0.2 2>/dev/null || sleep 1
}

fail() {
  echo "VMON_TEST_FAIL: $*" >&2
  drain_console
  poweroff -f 2>/dev/null || reboot -f 2>/dev/null || exit 1
  exit 1
}

finish() {
  drain_console
  poweroff -f 2>/dev/null || reboot -f 2>/dev/null || exit 0
  exit 0
}

cmdline_get() {
  key=$1
  default=$2
  for arg in $(cat /proc/cmdline 2>/dev/null); do
    case "$arg" in
      ${key}=*) printf '%s\n' "${arg#*=}"; return 0 ;;
    esac
  done
  printf '%s\n' "$default"
}

mount -t proc proc /proc || fail "mount /proc"
mount -t sysfs sys /sys || fail "mount /sys"
mount -t devtmpfs dev /dev 2>/dev/null || true
mkdir -p /mnt /tmp

mode=$(cmdline_get vmon.test boot)

case "$mode" in
  boot)
    echo "VMON_OK"
    finish
    ;;

  rng)
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      [ -c /dev/hwrng ] && break
      sleep 1
    done
    [ -c /dev/hwrng ] || fail "wait for /dev/hwrng"
    n=$(dd if=/dev/hwrng bs=32 count=1 2>/dev/null | wc -c)
    [ "$n" -eq 32 ] || fail "short /dev/hwrng read: $n"
    echo "RNG_OK"
    finish
    ;;

  blk)
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      [ -e /dev/vda ] && break
      sleep 1
    done
    [ -e /dev/vda ] || fail "wait for /dev/vda"
    mount -t ext4 /dev/vda /mnt || fail "mount /dev/vda"
    marker=$(cat /mnt/MARKER.txt 2>/dev/null) || fail "read /mnt/MARKER.txt"
    [ "$marker" = "vmon rootfs fixture" ] || fail "unexpected MARKER.txt: $marker"
    printf '%s\n' "guest wrote this" > /mnt/WRITE.txt || fail "write /mnt/WRITE.txt"
    sync || fail "sync after write"
    umount /mnt || fail "umount after write"
    mount -t ext4 /dev/vda /mnt || fail "remount /dev/vda"
    readback=$(cat /mnt/WRITE.txt 2>/dev/null) || fail "read back /mnt/WRITE.txt"
    [ "$readback" = "guest wrote this" ] || fail "unexpected WRITE.txt: $readback"
    umount /mnt || fail "final umount"
    echo "BLK_OK"
    finish
    ;;

  net)
    host_ip=$(cmdline_get vmon.host_ip 192.168.249.1)
    guest_ip=$(cmdline_get vmon.guest_ip 192.168.249.2)
    prefix=$(cmdline_get vmon.prefix 24)
    iface=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      for dev in /sys/class/net/*; do
        [ -d "$dev" ] || continue
        name=${dev##*/}
        [ "$name" = lo ] && continue
        iface=$name
        break
      done
      [ -n "$iface" ] && break
      sleep 1
    done
    [ -n "$iface" ] || fail "no non-loopback network interface"
    ip link set "$iface" up || fail "bring up $iface"
    if ! ip addr add "$guest_ip/$prefix" dev "$iface" 2>/dev/null; then
      if [ "$prefix" = 24 ]; then
        ifconfig "$iface" "$guest_ip" netmask 255.255.255.0 up || fail "assign $guest_ip to $iface"
      else
        fail "assign $guest_ip/$prefix to $iface"
      fi
    fi
    ok=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      ping -c1 -W1 "$host_ip" >/dev/null 2>&1 && { ok=1; break; }
      sleep 1
    done
    [ -n "$ok" ] || fail "ping $host_ip"
    echo "NET_OK"
    finish
    ;;

  net_throughput)
    host_ip=$(cmdline_get vmon.host_ip 192.168.249.1)
    guest_ip=$(cmdline_get vmon.guest_ip 192.168.249.2)
    prefix=$(cmdline_get vmon.prefix 24)
    port=$(cmdline_get vmon.tput_port 5050)
    mib=$(cmdline_get vmon.tput_mib 64)
    floor=$(cmdline_get vmon.tput_floor 100)
    iface=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      for dev in /sys/class/net/*; do
        [ -d "$dev" ] || continue
        name=${dev##*/}
        [ "$name" = lo ] && continue
        iface=$name
        break
      done
      [ -n "$iface" ] && break
      sleep 1
    done
    [ -n "$iface" ] || fail "no non-loopback network interface"
    ip link set "$iface" up || fail "bring up $iface"
    if ! ip addr add "$guest_ip/$prefix" dev "$iface" 2>/dev/null; then
      if [ "$prefix" = 24 ]; then
        ifconfig "$iface" "$guest_ip" netmask 255.255.255.0 up || fail "assign $guest_ip to $iface"
      else
        fail "assign $guest_ip/$prefix to $iface"
      fi
    fi
    ok=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      ping -c1 -W1 "$host_ip" >/dev/null 2>&1 && { ok=1; break; }
      sleep 1
    done
    [ -n "$ok" ] || fail "ping $host_ip"
    start=$(cut -d' ' -f1 /proc/uptime)
    dd if=/dev/zero bs=1048576 count="$mib" 2>/dev/null | nc -w 20 "$host_ip" "$port" \
      || fail "throughput transfer to $host_ip:$port"
    end=$(cut -d' ' -f1 /proc/uptime)
    mibps=$(awk -v m="$mib" -v s="$start" -v e="$end" 'BEGIN{d=e-s; if(d<=0)d=0.001; printf "%d", m/d}')
    echo "THROUGHPUT_MIBPS=$mibps"
    if [ "$mibps" -ge "$floor" ]; then
      echo "THROUGHPUT_OK"
    else
      fail "throughput $mibps MiB/s below floor $floor MiB/s"
    fi
    finish
    ;;

  usernet | usernet_dhcp | usernet_snapshot)
    host_ip=$(cmdline_get vmon.host_ip 10.0.2.2)
    port=$(cmdline_get vmon.tput_port 5050)
    mib=$(cmdline_get vmon.tput_mib 8)
    iface=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      for dev in /sys/class/net/*; do
        [ -d "$dev" ] || continue
        name=${dev##*/}
        [ "$name" = lo ] && continue
        iface=$name
        break
      done
      [ -n "$iface" ] && break
      sleep 1
    done
    [ -n "$iface" ] || fail "no non-loopback network interface"
    ip link set "$iface" up || fail "bring up $iface"
    if [ "$mode" = usernet_dhcp ] || [ "$mode" = usernet_snapshot ]; then
      # DHCP is mandatory here: a slirp DHCP-server regression must fail.
      udhcpc -i "$iface" -n -q -t 8 -T 1 >/dev/null 2>&1 || fail "udhcpc lease on $iface"
      lease=$(ip addr show dev "$iface" | awk '/inet / {print $2; exit}' | cut -d/ -f1)
      [ -n "$lease" ] || fail "read DHCP lease on $iface"
      if [ "$mode" = usernet_snapshot ]; then
        echo "USERNET_SNAPSHOT_READY lease=$lease"
        sleep "$(cmdline_get vmon.snapshot_delay 30)"
        lease_after=$(ip addr show dev "$iface" | awk '/inet / {print $2; exit}' | cut -d/ -f1)
        echo "USERNET_SNAPSHOT_RESTORED lease=$lease_after"
        [ "$lease_after" = "$lease" ] || fail "DHCP lease changed after restore: $lease_after != $lease"
        echo "USERNET_SAME_LEASE"
      else
        echo "USERNET_DHCP_OK"
      fi
    else
      # Deterministic static config exercising only the NAT data path.
      ip addr add 10.0.2.15/24 dev "$iface" 2>/dev/null \
        || ifconfig "$iface" 10.0.2.15 netmask 255.255.255.0 up \
        || fail "static config on $iface"
      ip route add default via 10.0.2.2 2>/dev/null || route add default gw 10.0.2.2 || true
    fi
    # Outbound TCP through user-mode NAT: slirp maps the gateway 10.0.2.2 to the
    # host loopback, so a host listener on 127.0.0.1:$port receives this stream.
    dd if=/dev/zero bs=1048576 count="$mib" 2>/dev/null | nc -w 20 "$host_ip" "$port" \
      || fail "usernet transfer to $host_ip:$port"
    echo "USERNET_OK"
    [ "$mode" = usernet_snapshot ] && echo "USERNET_SNAPSHOT_OK"
    finish
    ;;

  lifecycle)
    echo "LIFECYCLE_READY"
    while true; do sleep 60; done
    ;;

  snapshot)
    echo "SNAPSHOT_READY"
    sleep "$(cmdline_get vmon.snapshot_delay 5)"
    echo "SNAPSHOT_AFTER_BASE"
    sleep "$(cmdline_get vmon.snapshot_after_base_hold 0)"
    echo "SNAPSHOT_RESTORED"
    echo "SNAPSHOT_AFTER_RESTORE"
    finish
    ;;

  soak)
    echo "SOAK_READY"
    while true; do sleep 60; done
    ;;

  busy_smp)
    expect=$(cmdline_get vmon.expect_cpus 2)
    cpus=0
    tries=0
    while [ "$tries" -lt 50 ]; do
      cpus=$(grep -c '^processor' /proc/cpuinfo 2>/dev/null || echo 0)
      [ "$cpus" -ge "$expect" ] && break
      tries=$((tries + 1))
      sleep 1
    done
    echo "CPUS=$cpus"
    [ "$cpus" -ge "$expect" ] || fail "expected $expect CPUs online, saw $cpus"
    i=0
    while [ "$i" -lt "$cpus" ]; do
      while true; do :; done &
      i=$((i + 1))
    done
    echo "BUSY_READY"
    wait
    ;;

  shell)
    exec /bin/sh
    ;;

  rootfs_ro)
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      [ -e /dev/vda ] && break
      sleep 1
    done
    [ -e /dev/vda ] || fail "wait for /dev/vda"
    # The kernel silently downgrades a rw mount of a read-only virtio-blk
    # device to ro, so assert the device-level RO flag and a failing write
    # instead of a failing mount.
    ro=$(cat /sys/block/vda/ro 2>/dev/null)
    [ "$ro" = 1 ] || fail "/sys/block/vda/ro is '$ro', expected 1"
    mount -t ext4 /dev/vda /mnt || fail "mount /dev/vda"
    marker=$(cat /mnt/MARKER.txt 2>/dev/null) || fail "read /mnt/MARKER.txt"
    [ "$marker" = "vmon rootfs fixture" ] || fail "unexpected MARKER.txt: $marker"
    if echo deny > /mnt/DENY.txt 2>/dev/null; then
      fail "write to read-only rootfs unexpectedly succeeded"
    fi
    umount /mnt || fail "umount ro rootfs"
    echo "ROOTFS_RO_OK"
    finish
    ;;

  mac)
    iface=
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      for dev in /sys/class/net/*; do
        [ -d "$dev" ] || continue
        name=${dev##*/}
        [ "$name" = lo ] && continue
        iface=$name
        break
      done
      [ -n "$iface" ] && break
      sleep 1
    done
    [ -n "$iface" ] || fail "no non-loopback network interface"
    ip link set "$iface" up || fail "bring up $iface"
    mac=$(cat "/sys/class/net/$iface/address" 2>/dev/null) || fail "read $iface MAC"
    echo "MAC=$mac"
    echo "MAC_OK"
    finish
    ;;

  fsshare)
    tag=$(cmdline_get vmon.fs_tag host)
    if ! out=$(mount -t virtiofs "$tag" /mnt 2>&1); then
      case "$out" in
        *"No such device"*) echo "FSSHARE_NODEV"; finish ;;
      esac
      fail "mount virtiofs $tag: $out"
    fi
    share=$(cat /mnt/SHARE.txt 2>/dev/null) || fail "read /mnt/SHARE.txt"
    [ "$share" = "vmon share fixture" ] || fail "unexpected SHARE.txt: $share"
    if (echo deny > /mnt/DENY.txt) 2>/dev/null; then
      fail "write to read-only virtiofs share unexpectedly succeeded"
    fi
    echo "FSSHARE_OK"
    finish
    ;;

  volume)
    mkdir -p /data /ro
    if ! out=$(mount -t virtiofs data /data 2>&1); then
      case "$out" in
        *"No such device"*) echo "VOLUME_RO_NODEV"; finish ;;
      esac
      fail "mount virtiofs data: $out"
    fi
    printf 'hello' > /data/OUT.txt || fail "write /data/OUT.txt"
    sync || fail "sync volume write"
    echo "VOLUME_OK"
    mount -t virtiofs snap /ro || fail "mount virtiofs snap"
    if (echo deny > /ro/DENY.txt) 2>/dev/null; then
      fail "write to read-only volume unexpectedly succeeded"
    fi
    echo "VOLUME_RO_OK"
    finish
    ;;

  agent)
    # vmon-agent waits for its virtio-console port (vmon.gc4) itself.
    if [ ! -x /usr/bin/vmon-agent ]; then
      echo "AGENT_MISSING"
      finish
    fi
    echo "AGENT_INIT_OK"
    exec /usr/bin/vmon-agent
    ;;

  *)
    fail "unknown vmon.test mode: $mode"
    ;;
esac
INIT_EOF
  chmod 0755 "$1"
}

install_busybox_tree() {  # install_busybox_tree <root-dir>
  local root=$1 app loader
  mkdir -p "$root/bin" "$root/sbin" "$root/etc" "$root/proc" "$root/sys" "$root/dev" "$root/mnt" "$root/tmp"
  cp "$BUSYBOX_PATH" "$root/bin/busybox"
  chmod 0755 "$root/bin/busybox"
  # Dynamically linked (aarch64) busybox needs its musl loader on the guest.
  if [ -n "${MUSL_LOADER:-}" ]; then
    loader=$(basename "$MUSL_LOADER")
    mkdir -p "$root/lib"
    cp "$MUSL_LOADER" "$root/lib/$loader"
    chmod 0755 "$root/lib/$loader"
  fi
  for app in sh mount umount cat echo sleep poweroff reboot sync mkdir uname ls tr ping ip ifconfig route grep sed awk cut true false dd hexdump chmod chown mknod nc udhcpc; do
    ln -sf busybox "$root/bin/$app"
  done
  ln -sf ../bin/busybox "$root/sbin/poweroff"
  ln -sf ../bin/busybox "$root/sbin/reboot"
  # busybox udhcpc applies the lease through this script (used by --net user).
  mkdir -p "$root/usr/share/udhcpc"
  cat > "$root/usr/share/udhcpc/default.script" <<'UDHCPC_EOF'
#!/bin/busybox sh
case "$1" in
  bound | renew)
    ip addr add "$ip/${mask:-24}" dev "$interface" 2>/dev/null \
      || ifconfig "$interface" "$ip" netmask "${subnet:-255.255.255.0}"
    [ -n "$router" ] && { ip route add default via "$router" 2>/dev/null \
      || route add default gw "$router"; }
    ;;
esac
exit 0
UDHCPC_EOF
  chmod 0755 "$root/usr/share/udhcpc/default.script"
}

# Place the busybox binary (and, for aarch64, its musl loader) at $BUSYBOX_PATH.
provision_busybox() {
  local tar_path
  if [ "$ARCH" = x86_64 ]; then
    fetch_verified "busybox-x86_64" "$BUSYBOX_URL" "$BUSYBOX_PATH" "$BUSYBOX_SHA256"
    chmod 0755 "$BUSYBOX_PATH"
    return 0
  fi
  if [ -f "$BUSYBOX_PATH" ] && [ -f "$MUSL_LOADER" ]; then
    echo "[assets] busybox-aarch64: using existing $BUSYBOX_PATH"
    return 0
  fi
  echo "[assets] busybox-aarch64: extracting from Alpine aarch64 minirootfs"
  TMP_DIR=$(mktemp -d "$ASSET_DIR/.busybox.XXXXXX")
  tar_path="$TMP_DIR/minirootfs.tar.gz"
  fetch_verified "alpine-minirootfs-aarch64" "$MINIROOTFS_URL" "$tar_path" "$MINIROOTFS_SHA256"
  tar -xzf "$tar_path" -C "$TMP_DIR" bin/busybox lib/ld-musl-aarch64.so.1 2>/dev/null \
    || tar -xzf "$tar_path" -C "$TMP_DIR" ./bin/busybox ./lib/ld-musl-aarch64.so.1
  cp "$TMP_DIR/bin/busybox" "$BUSYBOX_PATH"
  cp "$TMP_DIR/lib/ld-musl-aarch64.so.1" "$MUSL_LOADER"
  chmod 0755 "$BUSYBOX_PATH" "$MUSL_LOADER"
  cleanup
  TMP_DIR=
}

build_initramfs() {
  local root tmp_img
  if generated_ok "$INITRAMFS_PATH" "$INITRAMFS_VERSION"; then
    echo "[assets] initramfs: using existing $INITRAMFS_PATH"
  else
    echo "[assets] initramfs: building $INITRAMFS_PATH"
    TMP_DIR=$(mktemp -d "$ASSET_DIR/.initramfs.XXXXXX")
    root="$TMP_DIR/root"
    install_busybox_tree "$root"
    write_test_init "$root/init"
    if [ -f "$SMOKE_AGENT" ]; then
      echo "[assets] initramfs: embedding guest agent $SMOKE_AGENT"
      mkdir -p "$root/usr/bin"
      cp "$SMOKE_AGENT" "$root/usr/bin/vmon-agent"
      chmod 0755 "$root/usr/bin/vmon-agent"
    else
      echo "[assets] notice: no guest agent at $SMOKE_AGENT; agent smoke tests will skip (build with 'just agent-musl')"
    fi
    # Make the cpio stable enough that a rebuild changes only when contents do.
    find "$root" -exec touch -h -d '@0' {} + 2>/dev/null || true
    tmp_img="$ASSET_DIR/.initramfs.cpio.gz.$$"
    ( cd "$root" && find . -print | LC_ALL=C sort | cpio -o -H newc -R 0:0 2>/dev/null | gzip -n -1 > "$tmp_img" )
    mv "$tmp_img" "$INITRAMFS_PATH"
    write_generated_metadata "$INITRAMFS_PATH" "$INITRAMFS_VERSION"
    cleanup
    TMP_DIR=
  fi

  if ! generated_ok "$INITRAMFS_PATH" "$INITRAMFS_VERSION"; then
    echo "error: generated initramfs failed checksum verification: $INITRAMFS_PATH" >&2
    exit 1
  fi

}

write_rootfs_init() {  # write_rootfs_init <path>
  cat > "$1" <<'ROOT_INIT_EOF'
#!/bin/busybox sh
/bin/busybox --install -s /bin 2>/dev/null || true
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export PATH
mount -t proc proc /proc 2>/dev/null || true
mount -t sysfs sys /sys 2>/dev/null || true
mount -t devtmpfs dev /dev 2>/dev/null || true
echo "ROOTFS_OK"
exec /bin/sh
ROOT_INIT_EOF
  chmod 0755 "$1"
}

build_rootfs() {
  local root tmp_img
  if generated_ok "$ROOTFS_PATH" "$ROOTFS_VERSION"; then
    echo "[assets] rootfs: using existing $ROOTFS_PATH"
    return 0
  fi

  echo "[assets] rootfs: building $ROOTFS_PATH ($ROOTFS_SIZE)"
  TMP_DIR=$(mktemp -d "$ASSET_DIR/.rootfs.XXXXXX")
  root="$TMP_DIR/root"
  install_busybox_tree "$root"
  write_rootfs_init "$root/init"
  printf '%s\n' "vmon rootfs fixture" > "$root/MARKER.txt"
  mkdir -p "$root/root"
  printf '%s\n' "This ext4 image is generated by demo/fetch-test-assets.sh." > "$root/etc/issue"

  tmp_img="$ROOTFS_PATH.$$"
  rm -f "$tmp_img"
  mkfs.ext4 -F -q -d "$root" "$tmp_img" "$ROOTFS_SIZE"
  mv "$tmp_img" "$ROOTFS_PATH"
  write_generated_metadata "$ROOTFS_PATH" "$ROOTFS_VERSION"
  cleanup
  TMP_DIR=

  if ! generated_ok "$ROOTFS_PATH" "$ROOTFS_VERSION"; then
    echo "error: generated rootfs failed checksum verification: $ROOTFS_PATH" >&2
    exit 1
  fi
}

fetch_verified "$KERNEL_NAME" "$KERNEL_URL" "$KERNEL_PATH" "$KERNEL_SHA256"
provision_busybox
build_initramfs
build_rootfs

verify_sha256 "$KERNEL_PATH" "$KERNEL_SHA256" "$KERNEL_NAME"
generated_ok "$INITRAMFS_PATH" "$INITRAMFS_VERSION" || { echo "error: initramfs verification failed" >&2; exit 1; }
generated_ok "$ROOTFS_PATH" "$ROOTFS_VERSION" || { echo "error: rootfs verification failed" >&2; exit 1; }

echo "[assets] ready ($ARCH):"
echo "  kernel:    $KERNEL_PATH"
echo "  initramfs: $INITRAMFS_PATH"
echo "  rootfs:    $ROOTFS_PATH"

# --- best-effort UEFI firmware & fixtures ------------------------------------
UEFI_DIR="$ASSET_DIR/uefi"
mkdir -p "$UEFI_DIR"

EDK2_URL="https://github.com/rust-osdev/ovmf-prebuilt/releases/download/edk2-stable202511-r1/edk2-stable202511-r1-bin.tar.xz"
EDK2_SHA256="79841c5dcac6d4bb71ead5edb6ca2a251237330be3c0b166bdc8a8fec0ce760d"

fetch_uefi_optional() {
  local label=$1 url=$2 path=$3 expected=$4
  echo "[assets][uefi] checking optional asset $label..."
  if [ -f "$path" ]; then
    if verify_sha256 "$path" "$expected" "$label"; then
      echo "[assets][uefi] $label: using existing $path"
      return 0
    fi
    echo "[assets][uefi] $label: existing file is corrupt; removing" >&2
    rm -f "$path"
  fi

  echo "[assets][uefi] downloading optional $url"
  local tmp
  tmp=$(mktemp "$UEFI_DIR/.${label}.XXXXXX")
  if ! curl --fail --location --show-error --retry 2 --connect-timeout 10 --output "$tmp" "$url"; then
    echo "[assets][uefi] warning: failed to download optional $label" >&2
    rm -f "$tmp"
    return 1
  fi

  local actual
  actual=$(calc_sha256 "$tmp")
  if [ "$actual" != "$expected" ]; then
    echo "[assets][uefi] warning: checksum mismatch for optional $label" >&2
    rm -f "$tmp"
    return 1
  fi
  mv "$tmp" "$path"
  return 0
}

TAR_PATH="$UEFI_DIR/edk2-bin.tar.xz"
if fetch_uefi_optional "edk2-prebuilt" "$EDK2_URL" "$TAR_PATH" "$EDK2_SHA256"; then
  echo "[assets][uefi] extracting OVMF/UEFI firmware code and vars..."
  rm -rf "$UEFI_DIR/x64" "$UEFI_DIR/aarch64" 2>/dev/null || true
  if tar -xf "$TAR_PATH" -C "$UEFI_DIR" --strip-components=1 \
    "edk2-stable202511-r1-bin/x64/code.fd" \
    "edk2-stable202511-r1-bin/x64/vars.fd" \
    "edk2-stable202511-r1-bin/aarch64/code.fd" >/dev/null 2>&1; then
    mv "$UEFI_DIR/x64/code.fd" "$UEFI_DIR/OVMF_CODE.fd" 2>/dev/null || true
    mv "$UEFI_DIR/x64/vars.fd" "$UEFI_DIR/OVMF_VARS.fd" 2>/dev/null || true
    mv "$UEFI_DIR/aarch64/code.fd" "$UEFI_DIR/QEMU_EFI.fd" 2>/dev/null || true
  fi
  if [ -f "$UEFI_DIR/OVMF_CODE.fd" ] && [ -f "$UEFI_DIR/OVMF_VARS.fd" ] && [ -f "$UEFI_DIR/QEMU_EFI.fd" ]; then
    echo "[assets][uefi] successfully extracted all UEFI firmware files"
  else
    echo "[assets][uefi] warning: UEFI firmware extraction failed. Some files are missing." >&2
  fi
  rm -rf "$UEFI_DIR/x64" "$UEFI_DIR/aarch64" 2>/dev/null || true
fi

if [ "${VMON_UEFI_IMAGES:-0}" = "1" ]; then
  UBUNTU_AMD64_URL="http://cloud-images-archive.ubuntu.com/releases/focal/release-20230209/ubuntu-20.04-server-cloudimg-amd64.img"
  UBUNTU_AMD64_SHA256="eb20cd25da5d2193283951953f6a0f5bdbd57474ac19fd1c36b9b77e6b68bbfc"
  fetch_uefi_optional "ubuntu-amd64-uefi" "$UBUNTU_AMD64_URL" "$UEFI_DIR/ubuntu-focal-amd64.img" "$UBUNTU_AMD64_SHA256" || true

  UBUNTU_ARM64_URL="http://cloud-images-archive.ubuntu.com/releases/focal/release-20230209/ubuntu-20.04-server-cloudimg-arm64.img"
  UBUNTU_ARM64_SHA256="f607f625568e004831fe7daf799bdd50def22d83e87d82a40f717a09c11a772c"
  fetch_uefi_optional "ubuntu-arm64-uefi" "$UBUNTU_ARM64_URL" "$UEFI_DIR/ubuntu-focal-arm64.img" "$UBUNTU_ARM64_SHA256" || true

  for arch in amd64 arm64; do
    img="$UEFI_DIR/ubuntu-focal-$arch.img"
    raw="$UEFI_DIR/ubuntu-focal-$arch.raw"
    if [ -f "$img" ] && [ ! -f "$raw" ]; then
      if command -v qemu-img >/dev/null 2>&1; then
        echo "[assets][uefi] converting qcow2 image to raw: $img -> $raw"
        qemu-img convert -O raw "$img" "$raw" || true
      else
        echo "[assets][uefi] warning: 'qemu-img' not found. Cannot convert qcow2 image to raw." >&2
      fi
    fi
  done
fi
