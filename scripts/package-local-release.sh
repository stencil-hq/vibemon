#!/usr/bin/env bash
# Build every locally consumable release artifact without publishing it.
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)

target=""
usage() {
    cat <<'EOF'
Usage: scripts/package-local-release.sh --target <triple>

Package the Rust binaries, architecture-matched guest assets, Python SDK,
TypeScript SDK, and deployment assets locally. Nothing is uploaded or published.

Supported release targets:
  x86_64-unknown-linux-musl
  aarch64-unknown-linux-musl

The Rust packaging step executes the target vmon CLI and validates the guest
agent's ELF architecture. Cross-target packaging therefore requires a compatible
target runtime (for example, qemu-user), and fails explicitly when one is
unavailable.
EOF
}

while (($#)); do
    case "$1" in
        --target)
            if (($# < 2)); then
                echo "error: --target requires a Rust target triple" >&2
                usage >&2
                exit 2
            fi
            target=$2
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

if [[ -z "${target}" ]]; then
    echo "error: --target is required" >&2
    usage >&2
    exit 2
fi

case "${target}" in
    x86_64-unknown-linux-musl|aarch64-unknown-linux-musl) ;;
    *)
        echo "error: unsupported local release target: ${target}" >&2
        usage >&2
        exit 2
        ;;
esac

cd "${ROOT_DIR}"
./scripts/package-rust-release.sh --target "${target}"
./scripts/package-guest-assets.sh --target "${target}"
./scripts/package-python-sdk.sh
(
    cd sdk/ts
    bun run package
)
./scripts/package-release.sh

cat <<EOF
Local release complete for ${target}.
Artifacts are under dist/, sdk/py/dist/, and sdk/ts/dist/package/.
No artifact was uploaded or published.
EOF
