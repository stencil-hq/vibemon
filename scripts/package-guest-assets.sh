#!/usr/bin/env bash
# Build a guest agent and package it with the architecture-matched vmon guest
# kernel built by kernel/build.sh (`just kernel`).
#
# Kernel input: $VMON_RELEASE_KERNEL (with its resolved config at
# $VMON_RELEASE_KERNEL_CONFIG, default config-<arch> beside it), else
# target/kernel/<image> + target/kernel/config-<arch>.
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
DIST_DIR="${ROOT_DIR}/dist"

usage() {
    echo "usage: $0 --target <x86_64-unknown-linux-musl|aarch64-unknown-linux-musl>" >&2
}

fail() {
    echo "error: $*" >&2
    exit 1
}

hash_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    else
        fail "sha256sum or shasum is required"
    fi
}

TARGET=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            TARGET=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage
            fail "unknown argument: $1"
            ;;
    esac
done

[[ -n "${TARGET}" ]] || { usage; exit 2; }
case "${TARGET}" in
    x86_64-unknown-linux-musl)
        ARCH=x86_64
        ELF_MACHINE=62
        KERNEL_FILE=bzImage-x86_64
        ;;
    aarch64-unknown-linux-musl)
        ARCH=aarch64
        ELF_MACHINE=183
        KERNEL_FILE=Image-aarch64
        ;;
    *) fail "unsupported release target '${TARGET}'; only x86_64-unknown-linux-musl and aarch64-unknown-linux-musl are supported" ;;
esac

PACKAGE="vmon-assets-${ARCH}"
ARCHIVE="${DIST_DIR}/${PACKAGE}.tar.gz"
CHECKSUM="${ARCHIVE}.sha256"
mkdir -p "${DIST_DIR}"
# Never leave a prior target artifact looking like the result of a failed run.
rm -f "${ARCHIVE}" "${CHECKSUM}"

for required in LICENSE LICENSE-APACHE THIRD-PARTY-NOTICES.txt; do
    [[ -f "${ROOT_DIR}/${required}" ]] || fail "missing required release payload ${required}"
done
[[ -f "${ROOT_DIR}/scripts/deterministic-tar.py" ]] || fail "missing scripts/deterministic-tar.py"
command -v cargo >/dev/null 2>&1 || fail "cargo is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

WORK_DIR=$(mktemp -d "${DIST_DIR}/.${PACKAGE}.XXXXXX")
cleanup() {
    rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

# Building on every invocation lets Cargo reuse only artifacts it can prove are
# current. It also makes this script independently composable with, but not
# dependent on, package-rust-release.sh.
(
    cd "${ROOT_DIR}"
    cargo zigbuild --locked --release --target "${TARGET}" -p vmon-agent
)

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "${CARGO_TARGET_DIR}" in
        /*) TARGET_DIR=${CARGO_TARGET_DIR} ;;
        *) TARGET_DIR="${ROOT_DIR}/${CARGO_TARGET_DIR}" ;;
    esac
else
    TARGET_DIR="${ROOT_DIR}/target"
fi
AGENT_BIN="${TARGET_DIR}/${TARGET}/release/vmon-agent"
[[ -f "${AGENT_BIN}" && -x "${AGENT_BIN}" && -s "${AGENT_BIN}" ]] || fail "Cargo did not produce an executable vmon-agent at ${AGENT_BIN}"

# Confirm that the agent selected from the target directory actually has the
# architecture encoded by --target, rather than trusting its path or filename.
python3 - "${ELF_MACHINE}" "${AGENT_BIN}" <<'PY'
import struct
import sys
from pathlib import Path

expected = int(sys.argv[1])
path = Path(sys.argv[2])
with path.open("rb") as stream:
    header = stream.read(20)
if len(header) < 20 or header[:4] != b"\x7fELF":
    raise SystemExit(f"error: release agent is not ELF: {path}")
if header[4] != 2 or header[5] != 1:
    raise SystemExit(f"error: release agent is not 64-bit little-endian ELF: {path}")
machine = struct.unpack_from("<H", header, 18)[0]
if machine != expected:
    raise SystemExit(f"error: release agent has ELF machine {machine}, expected {expected}: {path}")
PY

# The vmon kernel: an explicit release build (CI downloads the `kernel` job's
# artifact) or the dev build from `just kernel`. Its resolved config must be
# beside it and must identify the vmon kernel build.
KERNEL_PATH=${VMON_RELEASE_KERNEL:-"${ROOT_DIR}/target/kernel/${KERNEL_FILE}"}
KERNEL_CONFIG=${VMON_RELEASE_KERNEL_CONFIG:-"$(dirname -- "${KERNEL_PATH}")/config-${ARCH}"}
[[ -f "${KERNEL_PATH}" && -s "${KERNEL_PATH}" ]] || fail "missing ${ARCH} vmon kernel ${KERNEL_PATH}; build it with \`just kernel --arch ${ARCH}\` or set VMON_RELEASE_KERNEL"
[[ -f "${KERNEL_CONFIG}" ]] || fail "missing kernel config ${KERNEL_CONFIG} (written by kernel/build.sh)"
grep -qx 'CONFIG_LOCALVERSION="-vmon"' "${KERNEL_CONFIG}" || fail "${KERNEL_CONFIG} is not a vmon kernel config (kernel/build.sh)"
# Boot-image magic per arch: arm64 Image "ARMd" at 0x38, x86 bzImage "HdrS" at 0x202.
python3 - "${ARCH}" "${KERNEL_PATH}" <<'PY'
import sys
arch, path = sys.argv[1], sys.argv[2]
offset, magic = (0x38, b"ARM\x64") if arch == "aarch64" else (0x202, b"HdrS")
with open(path, "rb") as kernel:
    kernel.seek(offset)
    if kernel.read(4) != magic:
        raise SystemExit(f"error: {path} is not an {arch} boot image")
PY
KERNEL_SHA256=$(hash_file "${KERNEL_PATH}")
KERNEL_CONFIG_SHA256=$(hash_file "${KERNEL_CONFIG}")
KERNEL_TAG=$(sed -n 's/^KERNEL_TAG=//p' "${ROOT_DIR}/kernel/build.sh")
[[ -n "${KERNEL_TAG}" ]] || fail "cannot read KERNEL_TAG from kernel/build.sh"
AGENT_SHA256=$(hash_file "${AGENT_BIN}")

STAGE_DIR="${WORK_DIR}/${PACKAGE}"
# deterministic-tar.py preserves mode bits, so set the archive root explicitly
# rather than inheriting the caller's umask.
install -d -m 0755 "${STAGE_DIR}"
install -m 0644 "${KERNEL_PATH}" "${STAGE_DIR}/vmon-kernel-${ARCH}"
install -m 0644 "${KERNEL_CONFIG}" "${STAGE_DIR}/vmon-kernel-${ARCH}.config"
install -m 0755 "${AGENT_BIN}" "${STAGE_DIR}/vmon-agent-${ARCH}"
install -m 0644 \
    "${ROOT_DIR}/LICENSE" \
    "${ROOT_DIR}/LICENSE-APACHE" \
    "${ROOT_DIR}/THIRD-PARTY-NOTICES.txt" \
    "${STAGE_DIR}/"
cat > "${STAGE_DIR}/MANIFEST.txt" <<EOF
Vibemon guest runtime assets

release target: ${TARGET}
architecture: ${ARCH}

kernel file: vmon-kernel-${ARCH} (${KERNEL_FILE} from kernel/build.sh)
kernel source: https://github.com/cloud-hypervisor/linux tag ${KERNEL_TAG}
kernel configuration: ch_defconfig + kernel/config/ of this Vibemon release;
  resolved config in vmon-kernel-${ARCH}.config
kernel sha256: ${KERNEL_SHA256}
kernel config sha256: ${KERNEL_CONFIG_SHA256}
kernel license: GPL-2.0-only WITH Linux-syscall-note

agent file: vmon-agent-${ARCH}
agent source: Vibemon workspace package vmon-agent (Cargo.lock locked release build)
agent target: ${TARGET}
agent sha256: ${AGENT_SHA256}
agent license: MIT

The Linux guest kernel is not covered by Vibemon's first-party licenses. See
THIRD-PARTY-NOTICES.txt for the corresponding provenance and licensing notice.
EOF
chmod 0644 "${STAGE_DIR}/MANIFEST.txt"

# Validate the exact staged inputs after the manifest is written and before the
# deterministic archive consumes them.
[[ "$(hash_file "${STAGE_DIR}/vmon-kernel-${ARCH}")" == "${KERNEL_SHA256}" ]] || fail "staged kernel checksum changed"
[[ "$(hash_file "${STAGE_DIR}/vmon-kernel-${ARCH}.config")" == "${KERNEL_CONFIG_SHA256}" ]] || fail "staged kernel config checksum changed"
[[ "$(hash_file "${STAGE_DIR}/vmon-agent-${ARCH}")" == "${AGENT_SHA256}" ]] || fail "staged agent checksum changed"

TEMP_ARCHIVE="${WORK_DIR}/${PACKAGE}.tar.gz"
python3 "${ROOT_DIR}/scripts/deterministic-tar.py" "${TEMP_ARCHIVE}" "${STAGE_DIR}"
[[ -s "${TEMP_ARCHIVE}" ]] || fail "deterministic archive creation produced no data"
ARCHIVE_SHA256=$(hash_file "${TEMP_ARCHIVE}")
TEMP_CHECKSUM="${WORK_DIR}/${PACKAGE}.tar.gz.sha256"
printf '%s  %s\n' "${ARCHIVE_SHA256}" "${PACKAGE}.tar.gz" > "${TEMP_CHECKSUM}"
# The archive is the completion marker; publish it only after its sidecar exists.
mv "${TEMP_CHECKSUM}" "${CHECKSUM}"
mv "${TEMP_ARCHIVE}" "${ARCHIVE}"

echo "Guest asset release (${ARCH}, ${TARGET}):"
echo "  ${ARCHIVE}"
echo "  ${CHECKSUM}"
