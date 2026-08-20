#!/usr/bin/env bash
# Build a guest agent and package it with the pinned architecture-matched kernel.
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
        KERNEL_RELEASE=ch-release-v6.12.8-20250613
        KERNEL_URL=https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.12.8-20250613/bzImage-x86_64
        KERNEL_SHA256=d4af401aa859e4659d4b08a153ac608eb6a315c6918e567daa46981af5d2e5ef
        ;;
    aarch64-unknown-linux-musl)
        ARCH=aarch64
        ELF_MACHINE=183
        KERNEL_FILE=Image-aarch64
        KERNEL_RELEASE=ch-release-v6.16.9-20260508
        KERNEL_URL=https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.16.9-20260508/Image-arm64
        KERNEL_SHA256=69d1b1235381ec50f1b45cf771a7dff4a9013d452833ab34682d6283e2114010
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
CACHE_TEMP=""
cleanup() {
    rm -rf "${WORK_DIR}"
    [[ -z "${CACHE_TEMP}" ]] || rm -f "${CACHE_TEMP}"
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

# An explicitly supplied kernel is never replaced or silently accepted. Without
# an override, reuse a verified pinned test/cache copy or fetch the immutable URL
# into the release-only cache and validate it before use.
KERNEL_PATH=""
if [[ -n "${VMON_RELEASE_KERNEL:-}" ]]; then
    [[ -f "${VMON_RELEASE_KERNEL}" && -s "${VMON_RELEASE_KERNEL}" ]] || fail "VMON_RELEASE_KERNEL does not name a non-empty file: ${VMON_RELEASE_KERNEL}"
    ACTUAL_KERNEL_SHA=$(hash_file "${VMON_RELEASE_KERNEL}")
    [[ "${ACTUAL_KERNEL_SHA}" == "${KERNEL_SHA256}" ]] || fail "kernel checksum mismatch for ${VMON_RELEASE_KERNEL}: expected ${KERNEL_SHA256}, got ${ACTUAL_KERNEL_SHA}"
    KERNEL_PATH=${VMON_RELEASE_KERNEL}
else
    CACHE_DIR=${VMON_RELEASE_ASSET_CACHE_DIR:-"${ROOT_DIR}/target/release-assets"}
    case "${CACHE_DIR}" in
        /*) ;;
        *) CACHE_DIR="${ROOT_DIR}/${CACHE_DIR}" ;;
    esac
    mkdir -p "${CACHE_DIR}"
    for candidate in "${ROOT_DIR}/target/test-assets/${KERNEL_FILE}" "${CACHE_DIR}/${KERNEL_FILE}"; do
        if [[ -f "${candidate}" && -s "${candidate}" ]] && [[ "$(hash_file "${candidate}")" == "${KERNEL_SHA256}" ]]; then
            KERNEL_PATH=${candidate}
            break
        fi
    done
    if [[ -z "${KERNEL_PATH}" ]]; then
        command -v curl >/dev/null 2>&1 || fail "curl is required to fetch the pinned ${ARCH} kernel"
        DOWNLOAD="${WORK_DIR}/${KERNEL_FILE}.download"
        curl --fail --location --retry 3 --output "${DOWNLOAD}" "${KERNEL_URL}"
        [[ -s "${DOWNLOAD}" ]] || fail "downloaded kernel is empty: ${KERNEL_URL}"
        ACTUAL_KERNEL_SHA=$(hash_file "${DOWNLOAD}")
        [[ "${ACTUAL_KERNEL_SHA}" == "${KERNEL_SHA256}" ]] || fail "downloaded kernel checksum mismatch: expected ${KERNEL_SHA256}, got ${ACTUAL_KERNEL_SHA}"
        # Create beside the cache destination so the final rename is atomic.
        # The EXIT trap removes this unique temporary file after interruption.
        CACHE_TEMP=$(mktemp "${CACHE_DIR}/.${KERNEL_FILE}.XXXXXX")
        install -m 0644 "${DOWNLOAD}" "${CACHE_TEMP}"
        mv "${CACHE_TEMP}" "${CACHE_DIR}/${KERNEL_FILE}"
        CACHE_TEMP=""
        KERNEL_PATH="${CACHE_DIR}/${KERNEL_FILE}"
    fi
fi

ACTUAL_KERNEL_SHA=$(hash_file "${KERNEL_PATH}")
[[ "${ACTUAL_KERNEL_SHA}" == "${KERNEL_SHA256}" ]] || fail "verified kernel changed while packaging: ${KERNEL_PATH}"
AGENT_SHA256=$(hash_file "${AGENT_BIN}")

STAGE_DIR="${WORK_DIR}/${PACKAGE}"
# deterministic-tar.py preserves mode bits, so set the archive root explicitly
# rather than inheriting the caller's umask.
install -d -m 0755 "${STAGE_DIR}"
install -m 0644 "${KERNEL_PATH}" "${STAGE_DIR}/${KERNEL_FILE}"
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

kernel file: ${KERNEL_FILE}
kernel upstream: Cloud Hypervisor Linux release ${KERNEL_RELEASE}
kernel source: ${KERNEL_URL}
kernel sha256: ${KERNEL_SHA256}
kernel license: GPL-2.0-only WITH Linux-syscall-note

agent file: vmon-agent-${ARCH}
agent source: Vibemon workspace package vmon-agent (Cargo.lock locked release build)
agent target: ${TARGET}
agent sha256: ${AGENT_SHA256}
agent license: MIT

The external kernel is not covered by Vibemon's first-party licenses. See
THIRD-PARTY-NOTICES.txt for the corresponding provenance and licensing notice.
EOF
chmod 0644 "${STAGE_DIR}/MANIFEST.txt"

# Validate the exact staged inputs after the manifest is written and before the
# deterministic archive consumes them.
[[ "$(hash_file "${STAGE_DIR}/${KERNEL_FILE}")" == "${KERNEL_SHA256}" ]] || fail "staged kernel checksum changed"
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
