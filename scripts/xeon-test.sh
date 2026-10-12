#!/bin/sh
# Run the real Linux/KVM verification suites on xeon.internal.
set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
host=${VMON_TEST_HOST:-xeon.internal}

if [ -z "${VMON_TEST_REPO:-}" ]; then
    remote_user=$(ssh -G "$host" 2>/dev/null | awk '/^user / {print $2}' || true)
    remote_user=${remote_user:-$USER}
    if [ "$remote_user" = "root" ]; then
        remote_home="/root"
    else
        remote_home="/home/$remote_user"
    fi
else
    remote_home=""
fi

remote_repo=${VMON_TEST_REPO:-$remote_home/vibevmm-e2e}
mode=${1:-all}
skip_prepare=${VMON_TEST_SKIP_PREPARE:-0}
tap=${VMON_TEST_TAP:-vmon0}
host_ip=${VMON_TEST_HOST_IP:-192.168.249.1}
mysql_url=${VMON_TEST_MYSQL_URL:-mysql://root:vmon@127.0.0.1:13306}
registry_mirror=${VMON_TEST_REGISTRY_MIRROR:-https://mirror.gcr.io}

case "$mode" in
    prepare | rust | cluster | e2e | all) ;;
    *)
        echo "usage: $0 [prepare|rust|cluster|e2e|all]" >&2
        exit 2
        ;;
esac

if [ "${VMON_TEST_NO_SYNC:-0}" != "1" ]; then
    "$script_dir/xeon-sync.sh"
fi

exec ssh "$host" bash -s -- \
    "$remote_repo" "$mode" "$skip_prepare" "$tap" "$host_ip" "$mysql_url" \
    "$registry_mirror" <<'REMOTE'
set -euo pipefail

repo=$1
mode=$2
skip_prepare=$3
tap=$4
host_ip=$5
mysql_url=$6
registry_mirror=$7
kernel="$repo/target/kernel/bzImage-x86_64"

cd "$repo"

prepare_host() {
    echo '==> preparing guest agent, assets, and vmon'
    just agent-musl
    # fetch-test-assets caches the initramfs independently of the embedded
    # agent. Remove only that generated archive so it receives the new agent.
    rm -f target/test-assets/busybox-initramfs-x86_64.cpio.gz
    just fetch-assets
    [ -f "$kernel" ] || just kernel --arch x86_64
    cargo build --locked
}

rust_e2e() {
    echo '==> Rust Linux/KVM integration suite'
    VMON_TAP="$tap" \
    VMON_HOST_IP="$host_ip" \
    VMON_KERNEL="$repo/target/test-assets/bzImage-x86_64" \
    VMON_E2E=1 \
        cargo test --tests -- --test-threads=1
}

cluster_e2e() {
    echo '==> distributed failover suite (3 drivers + 2 nodes)'
    VMON_CLUSTER_E2E=1 \
    VMON_E2E=1 \
    VMON_CLUSTER_MYSQL_URL="$mysql_url" \
    VMON_KERNEL="$kernel" \
    VMON_AGENT="$repo/target/test-assets/vmon-agent-x86_64" \
        cargo test --test cluster -- --test-threads=1
}

sdk_e2e() {
    echo '==> official E2B Python + JS SDK suites (TAP networking, TLS ingress)'
    python3 e2e/run.py all \
        --ingress tls \
        --sudo \
        --mysql-url "$mysql_url" \
        --kernel "$kernel" \
        --agent "$repo/target/test-assets/vmon-agent-x86_64" \
        --serve-arg="--registry-mirror=$registry_mirror"
}

if [ "$mode" = prepare ]; then
    prepare_host
    exit 0
fi
if [ "$skip_prepare" != 1 ]; then
    prepare_host
fi

case "$mode" in
    rust)
        rust_e2e
        ;;
    cluster)
        cluster_e2e
        ;;
    e2e)
        sdk_e2e
        ;;
    all)
        rust_e2e
        cluster_e2e
        sdk_e2e
        ;;
esac
REMOTE
