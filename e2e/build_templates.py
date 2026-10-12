"""Build the templates the upstream E2B SDK test suites expect.

Run by `e2e/run.py` inside the pinned upstream Python SDK environment, so the
templates are built by the same SDK version the suites exercise. Mirrors
upstream `.github/workflows/templates.yml`:

- `base`    — the `e2bdev/base` image (upstream builds the identical
              `templates/base/e2b.Dockerfile`).
- `httpbin` — upstream `templates/httpbin/e2b.Dockerfile` with go-httpbin as
              the start command and its health endpoint as the ready command.
- `e2e-warm-ubuntu` — warm-up only: `ubuntu:22.04`, which many template tests
              build from.

Usage: python build_templates.py <upstream E2B checkout> [name ...]
Templates whose alias already exists are skipped.
"""

import sys
import time
from pathlib import Path

from e2b import Template


def log(entry) -> None:
    print(f"    {entry.message}", flush=True)


def build_base(_checkout: Path) -> None:
    template = Template().from_image("e2bdev/base")
    Template.build(template, "base", cpu_count=2, memory_mb=1024, on_build_logs=log)


def build_httpbin(checkout: Path) -> None:
    dockerfile = checkout / "templates" / "httpbin" / "e2b.Dockerfile"
    template = (
        Template()
        .from_dockerfile(dockerfile.read_text())
        .set_start_cmd(
            "go-httpbin -host 0.0.0.0 -port 8080",
            "curl -fsS http://localhost:8080/status/200 > /dev/null",
        )
    )
    Template.build(template, "httpbin", cpu_count=2, memory_mb=512, on_build_logs=log)


def build_warm_ubuntu(_checkout: Path) -> None:
    # Not a suite template: pulls and provisions ubuntu:22.04 once so the
    # template tests that start from it (with a 180 s budget each) hit warm
    # layer caches, as they do on upstream's long-lived cluster.
    template = Template().from_image("ubuntu:22.04")
    Template.build(template, "e2e-warm-ubuntu", cpu_count=2, memory_mb=1024, on_build_logs=log)


BUILDERS = {"base": build_base, "httpbin": build_httpbin, "e2e-warm-ubuntu": build_warm_ubuntu}


def main() -> int:
    checkout = Path(sys.argv[1])
    names = sys.argv[2:] or list(BUILDERS)
    for name in names:
        if Template.exists(name):
            print(f"e2e: template {name!r} already exists, skipping", flush=True)
            continue
        print(f"e2e: building template {name!r}", flush=True)
        started = time.monotonic()
        BUILDERS[name](checkout)
        print(f"e2e: template {name!r} built in {time.monotonic() - started:.0f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
