# /// script
# requires-python = ">=3.10"
# dependencies = ["e2b>=2.20.3,<3"]
# ///
"""Build vibemon's default templates as public templates of the system team.

The E2B SDKs default to the unqualified aliases `base`, `code-interpreter-v1`,
`desktop` and `mcp-gateway`. Publishing a system team's template gives it the
bare alias, which every team resolves after its own namespace
(ARCHITECTURE.md §7), so these builds make the defaults work for all teams.
`ubuntu-systemd` (stock Ubuntu 24.04 with systemd as PID 1) is vibemon's own
reference template for `fromImage` builds on a plain distro image; `cuda`
(NVIDIA CUDA runtime, one GPU by default) is its template for GPU sandboxes;
`ci` (GitHub Actions runner image) backs `runs-on: vmon…` CI jobs and needs
the team's `max_disk_mb` raised to 65536 first.

    uv run templates/build.py [NAME ...] [--jobs N] [--skip-cache]

Credentials: `E2B_API_KEY` of a system team, or else `vmon admin` (`$VMON_BIN`,
default `vmon` on PATH, with `VMON_CLUSTER_URL` (default: the local driver's
`http://127.0.0.1:7947`), `VMON_CLUSTER_SECRET` / `VMON_HOME` as for
`vmon admin`) finds or creates the system team `--team` and mints a key.
`E2B_API_URL` defaults to `http://127.0.0.1:3000` (`just serve`).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Template definitions are imported from the source tree; keep it clean.
sys.dont_write_bytecode = True

HERE = Path(__file__).resolve().parent
# Build order: alias -> definition directory under templates/.
TEMPLATES = {
    "base": "base",
    "code-interpreter-v1": "code-interpreter",
    "desktop": "desktop",
    "mcp-gateway": "mcp-gateway",
    "ubuntu-systemd": "ubuntu-systemd",
    "cuda": "cuda",
    "ci": "ci",
}
DEFAULT_API_URL = "http://127.0.0.1:3000"

_print_lock = threading.Lock()


def log(name: str, message: str) -> None:
    with _print_lock:
        print(f"[{name}] {message}", flush=True)


def available() -> dict[str, Path]:
    """Default templates whose definition exists in this checkout."""
    return {
        alias: HERE / directory
        for alias, directory in TEMPLATES.items()
        if (HERE / directory / "template.py").is_file()
    }


def load_definition(alias: str, directory: Path):
    spec = importlib.util.spec_from_file_location(
        f"vmon_template_{alias.replace('-', '_')}", directory / "template.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def vmon_admin(*args: str) -> str:
    vmon = os.environ.get("VMON_BIN") or shutil.which("vmon")
    if not vmon:
        sys.exit("build.py: set E2B_API_KEY (system team) or VMON_BIN / put `vmon` on PATH")
    result = subprocess.run(
        [vmon, "admin", *args], check=True, capture_output=True, text=True
    )
    return result.stdout


def system_api_key(team: str) -> str:
    """API key of the system team `team` (slug), creating the team if needed."""
    for line in vmon_admin("list-teams").splitlines():
        fields = line.split("\t")
        if len(fields) >= 3 and fields[1] == team:
            if "SYSTEM" not in fields[4:]:
                vmon_admin("set-system", team)
            return vmon_admin("create-api-key", team, "--name", "templates/build.py").strip()
    out = vmon_admin("create-team", "--system", team)
    for line in out.splitlines():
        if line.startswith("api key "):
            return line.removeprefix("api key ").strip()
    sys.exit(f"build.py: unexpected `vmon admin create-team` output:\n{out}")


def publish(template_id: str) -> list[str]:
    """`PATCH /v2/templates/{id}` `{public: true}`; returns the template's names."""
    import httpx
    from e2b import ConnectionConfig

    config = ConnectionConfig()
    response = httpx.patch(
        f"{config.api_url}/v2/templates/{template_id}",
        json={"public": True},
        headers={"X-API-KEY": config.api_key},
        timeout=30,
    )
    response.raise_for_status()
    return response.json().get("names", [])


def build(alias: str, directory: Path, skip_cache: bool) -> str:
    from e2b import Template

    definition = load_definition(alias, directory)
    started = time.monotonic()
    log(alias, f"building from {directory.relative_to(HERE.parent)}")
    info = Template.build(
        definition.make_template(),
        alias,
        cpu_count=definition.CPU_COUNT,
        memory_mb=definition.MEMORY_MB,
        min_free_disk_mb=getattr(definition, "MIN_FREE_DISK_MB", None),
        skip_cache=skip_cache,
        on_build_logs=lambda entry: log(alias, str(entry)),
    )
    names = publish(info.template_id)
    log(
        alias,
        f"ready in {time.monotonic() - started:.0f}s: template {info.template_id}, "
        f"build {info.build_id}, public names {names}",
    )
    return info.template_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("names", nargs="*", help="aliases to build (default: all present)")
    parser.add_argument("--team", default="system", help="system team slug (default: system)")
    parser.add_argument("--jobs", "-j", type=int, default=1, help="concurrent builds")
    parser.add_argument("--skip-cache", action="store_true", help="rebuild every layer")
    args = parser.parse_args()

    found = available()
    names = args.names or list(found)
    unknown = [name for name in names if name not in found]
    if unknown:
        sys.exit(f"build.py: no definition for {unknown}; available: {list(found)}")
    missing = [alias for alias in TEMPLATES if alias not in found]
    if missing and not args.names:
        print(f"build.py: skipping {missing} (no template.py in this checkout)", flush=True)

    os.environ.setdefault("E2B_API_URL", DEFAULT_API_URL)
    if not os.environ.get("E2B_API_KEY"):
        os.environ["E2B_API_KEY"] = system_api_key(args.team)

    failed = []
    with ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        futures = {
            pool.submit(build, name, found[name], args.skip_cache): name for name in names
        }
        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
            except Exception as error:  # report every failed template, then exit 1
                log(name, f"FAILED: {error}")
                failed.append(name)
    if failed:
        print(f"build.py: failed: {failed}", file=sys.stderr)
        return 1
    print(f"build.py: built and published {names}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
