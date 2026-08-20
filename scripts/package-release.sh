#!/usr/bin/env bash
# Packaging and release script for Vibemon (vmon) deployment assets.
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
DIST_DIR="${ROOT_DIR}/dist"
PACKAGE_NAME="vmon-deploy-assets"
STAGE_DIR="${DIST_DIR}/${PACKAGE_NAME}"
TAR_FILE="${DIST_DIR}/${PACKAGE_NAME}.tar.gz"

echo "=== Packaging Vibemon Deployment Assets ==="

if command -v just >/dev/null 2>&1; then
    echo "Running validation checks..."
    (cd "${ROOT_DIR}" && just validate-deploy)
else
    echo "Warning: 'just' command not found, skipping validation."
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv is required for reproducible release packaging" >&2
    exit 1
fi

for required in LICENSE LICENSE-APACHE THIRD-PARTY-NOTICES.txt; do
    if [[ ! -f "${ROOT_DIR}/${required}" ]]; then
        echo "error: missing required release payload ${required}" >&2
        exit 1
    fi
done

mkdir -p "${DIST_DIR}"
rm -rf "${STAGE_DIR}"
trap 'rm -rf "${STAGE_DIR}"' EXIT
mkdir -p "${STAGE_DIR}"
uv run --no-project python \
    "${ROOT_DIR}/scripts/copy-release-tree.py" \
    "${ROOT_DIR}/deploy" \
    "${STAGE_DIR}/deploy"
cp \
    "${ROOT_DIR}/LICENSE" \
    "${ROOT_DIR}/LICENSE-APACHE" \
    "${ROOT_DIR}/THIRD-PARTY-NOTICES.txt" \
    "${STAGE_DIR}/"

echo "Creating reproducible release archive: ${TAR_FILE}"
uv run --no-project python "${ROOT_DIR}/scripts/deterministic-tar.py" "${TAR_FILE}" "${STAGE_DIR}"

echo "Generating SHA256 checksums..."
(
    cd "${DIST_DIR}"
    archive=$(basename "${TAR_FILE}")
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "${archive}" > "SHA256SUMS-deploy.txt"
    else
        shasum -a 256 "${archive}" > "SHA256SUMS-deploy.txt"
    fi
)

echo "=== Packaging completed successfully ==="
echo "Artifacts generated:"
echo "  - ${TAR_FILE}"
echo "  - ${DIST_DIR}/SHA256SUMS-deploy.txt"
