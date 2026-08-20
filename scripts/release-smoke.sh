#!/usr/bin/env bash
# Execute the release CLI through a real target runtime and validate the agent ELF.
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

usage() {
    cat >&2 <<'EOF'
usage: release-smoke.sh --target <triple> [--vmon <path>] [--agent <path>] [--check]

The vmon CLI is executed through the selected target runtime. The guest-only
vmon-agent cannot be safely run as a host process, so its ELF format and target
architecture are validated without claiming it was runtime-smoked.

Native execution is supported only on Linux with the same CPU architecture.
Linux cross-target execution uses qemu-<arch> when available. Set
VMON_RELEASE_RUNNER to one executable wrapper (no arguments) for another real
target runtime. In particular, macOS cannot directly execute Linux/musl output
and must use such a VM/container wrapper; this script never treats exec-format
failure as a successful smoke test.
EOF
}

fail() {
    echo "error: $*" >&2
    exit 1
}

TARGET=""
VMON_BIN=""
AGENT_BIN=""
CHECK_ONLY=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            TARGET=$2
            shift 2
            ;;
        --vmon)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            VMON_BIN=$2
            shift 2
            ;;
        --agent)
            [[ $# -ge 2 ]] || { usage; exit 2; }
            AGENT_BIN=$2
            shift 2
            ;;
        --check)
            CHECK_ONLY=1
            shift
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
    x86_64-unknown-linux-musl) TARGET_ARCH=x86_64 ;;
    aarch64-unknown-linux-musl) TARGET_ARCH=aarch64 ;;
    *) fail "unsupported release target '${TARGET}'; only x86_64-unknown-linux-musl and aarch64-unknown-linux-musl are supported" ;;
esac

normalize_arch() {
    case "$1" in
        x86_64|amd64) echo x86_64 ;;
        aarch64|arm64) echo aarch64 ;;
        *) echo "$1" ;;
    esac
}

RUNNER=""
if [[ -n "${VMON_RELEASE_RUNNER:-}" ]]; then
    case "${VMON_RELEASE_RUNNER}" in
        *[[:space:]]*) fail "VMON_RELEASE_RUNNER must name one executable; put arguments in a wrapper script" ;;
    esac
    command -v "${VMON_RELEASE_RUNNER}" >/dev/null 2>&1 || fail "VMON_RELEASE_RUNNER is not executable or not in PATH: ${VMON_RELEASE_RUNNER}"
    RUNNER=${VMON_RELEASE_RUNNER}
else
    HOST_OS=$(uname -s)
    HOST_ARCH=$(normalize_arch "$(uname -m)")
    if [[ "${HOST_OS}" == Linux && "${HOST_ARCH}" == "${TARGET_ARCH}" ]]; then
        RUNNER=""
    elif [[ "${HOST_OS}" == Linux ]]; then
        for candidate in "qemu-${TARGET_ARCH}" "qemu-${TARGET_ARCH}-static"; do
            if command -v "${candidate}" >/dev/null 2>&1; then
                RUNNER=${candidate}
                break
            fi
        done
        [[ -n "${RUNNER}" ]] || fail "cannot execute ${TARGET} on Linux/${HOST_ARCH}; install qemu-user for ${TARGET_ARCH} or set VMON_RELEASE_RUNNER to an executable wrapper"
    else
        fail "cannot directly execute Linux/musl target ${TARGET} on ${HOST_OS}/${HOST_ARCH}; set VMON_RELEASE_RUNNER to an executable VM/container wrapper"
    fi
fi

if [[ ${CHECK_ONLY} -eq 1 ]]; then
    if [[ -n "${RUNNER}" ]]; then
        echo "release smoke runtime: ${RUNNER} (${TARGET})"
    else
        echo "release smoke runtime: native (${TARGET})"
    fi
    exit 0
fi

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "${CARGO_TARGET_DIR}" in
        /*) TARGET_DIR=${CARGO_TARGET_DIR} ;;
        *) TARGET_DIR="${ROOT_DIR}/${CARGO_TARGET_DIR}" ;;
    esac
else
    TARGET_DIR="${ROOT_DIR}/target"
fi
VMON_BIN=${VMON_BIN:-"${TARGET_DIR}/${TARGET}/release/vmon"}
AGENT_BIN=${AGENT_BIN:-"${TARGET_DIR}/${TARGET}/release/vmon-agent"}
[[ -f "${VMON_BIN}" && -x "${VMON_BIN}" && -s "${VMON_BIN}" ]] || fail "missing executable vmon: ${VMON_BIN}"
[[ -f "${AGENT_BIN}" && -x "${AGENT_BIN}" && -s "${AGENT_BIN}" ]] || fail "missing executable vmon-agent: ${AGENT_BIN}"
command -v python3 >/dev/null 2>&1 || fail "python3 is required to validate release ELF headers"

# vmon-agent has no host-side CLI: it is guest PID 1 and opening /dev/hvc0 is
# its first useful action. Executing it on a host can hang if that device exists,
# so validate its ELF architecture here without representing that as an agent
# runtime smoke. The vmon CLI is exercised through the target runner below.
python3 - "${TARGET_ARCH}" "${VMON_BIN}" "${AGENT_BIN}" <<'PY'
import struct
import sys
from pathlib import Path

arch, *names = sys.argv[1:]
expected_machine = {"x86_64": 62, "aarch64": 183}[arch]
for name in names:
    path = Path(name)
    with path.open("rb") as stream:
        header = stream.read(20)
    if len(header) < 20 or header[:4] != b"\x7fELF":
        raise SystemExit(f"error: release executable is not ELF: {path}")
    if header[4] != 2 or header[5] != 1:
        raise SystemExit(f"error: release executable is not 64-bit little-endian ELF: {path}")
    machine = struct.unpack_from("<H", header, 18)[0]
    if machine != expected_machine:
        raise SystemExit(
            f"error: release executable has ELF machine {machine}, expected {expected_machine} for {arch}: {path}"
        )
PY

run_target() {
    if [[ -n "${RUNNER}" ]]; then
        "${RUNNER}" "$@"
    else
        "$@"
    fi
}

VERSION_OUTPUT=$(run_target "${VMON_BIN}" --version 2>&1) || fail "${TARGET} vmon --version did not execute successfully: ${VERSION_OUTPUT}"
case "${VERSION_OUTPUT}" in
    vmon\ *) ;;
    *) fail "${TARGET} vmon --version returned unexpected output: ${VERSION_OUTPUT}" ;;
esac

HELP_OUTPUT=$(run_target "${VMON_BIN}" vmm --help 2>&1) || fail "${TARGET} vmon vmm --help did not execute successfully: ${HELP_OUTPUT}"
case "${HELP_OUTPUT}" in
    *"vmon vmm ("*"--kernel <image>"*) ;;
    *) fail "${TARGET} vmon vmm --help did not expose the expected VMM CLI" ;;
esac

if [[ -n "${RUNNER}" ]]; then
    echo "release smoke passed via ${RUNNER}: ${VERSION_OUTPUT} (${TARGET})"
else
    echo "release smoke passed natively: ${VERSION_OUTPUT} (${TARGET})"
fi
