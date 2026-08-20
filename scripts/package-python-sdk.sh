#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
SDK_DIR="${ROOT_DIR}/sdk/py"
SDK_DIST="${SDK_DIR}/dist"
DIST_DIR="${ROOT_DIR}/dist"
readonly PYTHON_VERSION=3.14

if (( $# != 0 )); then
    echo "usage: $0" >&2
    exit 2
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "error: uv is required for Python SDK packaging" >&2
    exit 1
fi

export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-0}
export PYTHONHASHSEED=0
export TZ=UTC
export LC_ALL=C

WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/vmon-python-sdk.XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT

rm -rf "${SDK_DIST}" "${SDK_DIR}/build" "${SDK_DIR}/vmon.egg-info"
mkdir -p "${SDK_DIST}" "${DIST_DIR}"
for stale in \
    "${DIST_DIR}"/vmon-python-sdk-*.tar.gz \
    "${DIST_DIR}"/vmon-python-sdk-*.tar.gz.sha256; do
    if [[ -e "${stale}" ]]; then
        rm -f "${stale}"
    fi
done

(
    cd "${SDK_DIR}"
    uv build --python "${PYTHON_VERSION}" --out-dir "${SDK_DIST}"
)

shopt -s nullglob
wheels=("${SDK_DIST}"/vmon-*.whl)
sdists=("${SDK_DIST}"/vmon-*.tar.gz)
shopt -u nullglob
if (( ${#wheels[@]} != 1 || ${#sdists[@]} != 1 )); then
    echo "error: expected exactly one vmon wheel and one source distribution in ${SDK_DIST}" >&2
    exit 1
fi
WHEEL=${wheels[0]}
SDIST=${sdists[0]}

uvx --python "${PYTHON_VERSION}" --from twine twine check "${WHEEL}" "${SDIST}"

VERSION=$(
    uv run --no-project --python "${PYTHON_VERSION}" python - "${WHEEL}" "${SDIST}" <<'PY'
from __future__ import annotations

import email
import sys
import tarfile
import zipfile
from pathlib import Path

wheel = Path(sys.argv[1])
sdist = Path(sys.argv[2])
required_wheel_files = {
    "vmon/__init__.py",
    "vmon/py.typed",
    "vmon/v1/api_pb2.pyi",
    "vmon/v1/bridge_pb2.pyi",
}

with zipfile.ZipFile(wheel) as archive:
    names = set(archive.namelist())
    missing = sorted(required_wheel_files - names)
    if missing:
        raise SystemExit(f"wheel is missing required package data: {', '.join(missing)}")
    metadata_paths = sorted(name for name in names if name.endswith(".dist-info/METADATA"))
    if len(metadata_paths) != 1:
        raise SystemExit("wheel must contain exactly one .dist-info/METADATA file")
    wheel_metadata = email.message_from_bytes(archive.read(metadata_paths[0]))

with tarfile.open(sdist, mode="r:gz") as archive:
    pkg_info = [
        member
        for member in archive.getmembers()
        if member.name.endswith("/PKG-INFO") and member.name.count("/") == 1
    ]
    if len(pkg_info) != 1:
        raise SystemExit("source distribution must contain exactly one top-level PKG-INFO file")
    extracted = archive.extractfile(pkg_info[0])
    if extracted is None:
        raise SystemExit("could not read source distribution PKG-INFO")
    sdist_metadata = email.message_from_binary_file(extracted)

wheel_name = wheel_metadata.get("Name")
wheel_version = wheel_metadata.get("Version")
if wheel_name != "vmon" or not wheel_version:
    raise SystemExit(f"unexpected wheel identity: {wheel_name!r} {wheel_version!r}")
if sdist_metadata.get("Name") != wheel_name or sdist_metadata.get("Version") != wheel_version:
    raise SystemExit("wheel and source distribution metadata identities do not match")
print(wheel_version)
PY
)

VENV_DIR="${WORK_DIR}/venv"
uv venv --python "${PYTHON_VERSION}" "${VENV_DIR}"
uv pip install --python "${VENV_DIR}/bin/python" "${WHEEL}"
(
    unset PYTHONPATH
    export PYTHONNOUSERSITE=1
    cd "${WORK_DIR}"
    "${VENV_DIR}/bin/python" - "${VERSION}" <<'PY'
from __future__ import annotations

import importlib.metadata
import sys
import sysconfig
from pathlib import Path

import vmon

expected = sys.argv[1]
installed = importlib.metadata.version("vmon")
module_version = vmon.__version__
if installed != expected or module_version != expected:
    raise SystemExit(
        f"installed/imported version mismatch: expected={expected!r}, "
        f"metadata={installed!r}, module={module_version!r}"
    )
module_path = Path(vmon.__file__).resolve()
site_roots = {
    Path(sysconfig.get_path(name)).resolve()
    for name in ("purelib", "platlib")
}
if not any(module_path.is_relative_to(root) for root in site_roots):
    raise SystemExit(f"vmon was not imported from the isolated wheel install: {module_path}")
if not callable(vmon.connect):
    raise SystemExit("installed vmon package does not expose callable connect")
PY
)

PACKAGE_NAME="vmon-python-sdk-${VERSION}"
STAGE_DIR="${WORK_DIR}/${PACKAGE_NAME}"
ARCHIVE="${DIST_DIR}/${PACKAGE_NAME}.tar.gz"
CHECKSUM="${ARCHIVE}.sha256"
mkdir -p "${STAGE_DIR}"
cp "${WHEEL}" "${SDIST}" "${STAGE_DIR}/"

uv run --no-project --python "${PYTHON_VERSION}" python - "${STAGE_DIR}" <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

stage = Path(sys.argv[1])
lines = []
for artifact in sorted(path for path in stage.iterdir() if path.is_file()):
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    lines.append(f"{digest}  {artifact.name}\n")
(stage / "SHA256SUMS").write_text("".join(lines), encoding="utf-8", newline="\n")
PY

uv run --no-project --python "${PYTHON_VERSION}" python \
    "${ROOT_DIR}/scripts/deterministic-tar.py" \
    "${ARCHIVE}" \
    "${STAGE_DIR}"

uv run --no-project --python "${PYTHON_VERSION}" python - "${ARCHIVE}" "${CHECKSUM}" <<'PY'
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

archive = Path(sys.argv[1])
checksum = Path(sys.argv[2])
digest = hashlib.sha256(archive.read_bytes()).hexdigest()
checksum.write_text(f"{digest}  {archive.name}\n", encoding="utf-8", newline="\n")
PY

printf 'Python SDK release artifacts:\n'
printf '  %s\n' "${WHEEL}" "${SDIST}" "${ARCHIVE}" "${CHECKSUM}"
