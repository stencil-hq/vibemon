#!/usr/bin/env python3
"""Copy a source release tree without local build and dependency caches."""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

EXCLUDED = (".DS_Store", ".pulumi", ".venv", "__pycache__", "dist", "node_modules")


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: copy-release-tree.py SOURCE DESTINATION")
    source = Path(sys.argv[1]).resolve()
    destination = Path(sys.argv[2]).resolve()
    if not source.is_dir():
        raise SystemExit(f"source directory does not exist: {source}")
    shutil.copytree(source, destination, ignore=shutil.ignore_patterns(*EXCLUDED))


if __name__ == "__main__":
    main()
