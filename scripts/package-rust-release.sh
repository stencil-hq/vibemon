#!/usr/bin/env bash
# Build and package the supported Linux/musl release binaries locally.
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
    x86_64-unknown-linux-musl) ARCH=x86_64 ;;
    aarch64-unknown-linux-musl) ARCH=aarch64 ;;
    *) fail "unsupported release target '${TARGET}'; only x86_64-unknown-linux-musl and aarch64-unknown-linux-musl are supported" ;;
esac

PACKAGE="vmon-${TARGET}"
ARCHIVE="${DIST_DIR}/${PACKAGE}.tar.gz"
CHECKSUM="${ARCHIVE}.sha256"
mkdir -p "${DIST_DIR}"
# Once an invocation has selected this target, an older artifact must not remain
# plausible if any prerequisite, build, or smoke step fails.
rm -f "${ARCHIVE}" "${CHECKSUM}"

for required in LICENSE LICENSE-APACHE THIRD-PARTY-NOTICES.txt; do
    [[ -f "${ROOT_DIR}/${required}" ]] || fail "missing required release payload ${required}"
done
[[ -f "${ROOT_DIR}/scripts/deterministic-tar.py" ]] || fail "missing scripts/deterministic-tar.py"
command -v cargo >/dev/null 2>&1 || fail "cargo is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"

# Refuse an expensive build before learning that the resulting target cannot be
# executed. Cross builds require qemu-user on Linux or an explicit executable
# wrapper in VMON_RELEASE_RUNNER; macOS cannot directly run Linux/musl output.
"${ROOT_DIR}/scripts/release-smoke.sh" --target "${TARGET}" --check

# Always ask Cargo to build both packages. Cargo may reuse valid artifacts, but
# its dependency tracking prevents an arbitrary stale binary from being packed.
(
    cd "${ROOT_DIR}"
    cargo zigbuild --locked --release --target "${TARGET}" -p vmon -p vmon-agent
)

if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    case "${CARGO_TARGET_DIR}" in
        /*) TARGET_DIR=${CARGO_TARGET_DIR} ;;
        *) TARGET_DIR="${ROOT_DIR}/${CARGO_TARGET_DIR}" ;;
    esac
else
    TARGET_DIR="${ROOT_DIR}/target"
fi
VMON_BIN="${TARGET_DIR}/${TARGET}/release/vmon"
AGENT_BIN="${TARGET_DIR}/${TARGET}/release/vmon-agent"
[[ -f "${VMON_BIN}" && -x "${VMON_BIN}" && -s "${VMON_BIN}" ]] || fail "Cargo did not produce an executable vmon at ${VMON_BIN}"
[[ -f "${AGENT_BIN}" && -x "${AGENT_BIN}" && -s "${AGENT_BIN}" ]] || fail "Cargo did not produce an executable vmon-agent at ${AGENT_BIN}"

WORK_DIR=$(mktemp -d "${DIST_DIR}/.${PACKAGE}.XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT
STAGE_DIR="${WORK_DIR}/${PACKAGE}"
# deterministic-tar.py preserves mode bits, so set the archive root explicitly
# rather than inheriting the caller's umask.
install -d -m 0755 "${STAGE_DIR}"
install -m 0755 "${VMON_BIN}" "${STAGE_DIR}/vmon"
install -m 0755 "${AGENT_BIN}" "${STAGE_DIR}/vmon-agent"
install -m 0644 \
    "${ROOT_DIR}/LICENSE" \
    "${ROOT_DIR}/LICENSE-APACHE" \
    "${ROOT_DIR}/THIRD-PARTY-NOTICES.txt" \
    "${STAGE_DIR}/"

TEMP_ARCHIVE="${WORK_DIR}/${PACKAGE}.tar.gz"
python3 "${ROOT_DIR}/scripts/deterministic-tar.py" "${TEMP_ARCHIVE}" "${STAGE_DIR}"
[[ -s "${TEMP_ARCHIVE}" ]] || fail "deterministic archive creation produced no data"

# Execute the exact staged vmon through the target runner before publishing the
# archive into dist. A loader/linker/architecture failure therefore cannot leave
# a plausible current release artifact behind.
"${ROOT_DIR}/scripts/release-smoke.sh" \
    --target "${TARGET}" \
    --vmon "${STAGE_DIR}/vmon" \
    --agent "${STAGE_DIR}/vmon-agent"

ARCHIVE_SHA256=$(hash_file "${TEMP_ARCHIVE}")
TEMP_CHECKSUM="${WORK_DIR}/${PACKAGE}.tar.gz.sha256"
printf '%s  %s\n' "${ARCHIVE_SHA256}" "${PACKAGE}.tar.gz" > "${TEMP_CHECKSUM}"
# Publish the checksum first and the archive last: the presence of the archive is
# the completion marker, so an interrupted run cannot expose an unchecked file.
mv "${TEMP_CHECKSUM}" "${CHECKSUM}"
mv "${TEMP_ARCHIVE}" "${ARCHIVE}"

echo "Rust release artifact (${ARCH}, ${TARGET}):"
echo "  ${ARCHIVE}"
echo "  ${CHECKSUM}"
