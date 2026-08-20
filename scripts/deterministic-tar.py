#!/usr/bin/env python3
"""Create a byte-reproducible gzip-compressed tar archive from one tree."""

from __future__ import annotations

import gzip
import sys
import tarfile
from pathlib import Path

EXCLUDED_PARTS = {
    ".DS_Store",
    ".pulumi",
    ".venv",
    "__pycache__",
    "dist",
    "node_modules",
}


def normalize(info: tarfile.TarInfo) -> tarfile.TarInfo:
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = 0
    return info


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: deterministic-tar.py OUTPUT SOURCE_DIRECTORY")
    output = Path(sys.argv[1]).resolve()
    source = Path(sys.argv[2]).resolve()
    if not source.is_dir():
        raise SystemExit(f"source directory does not exist: {source}")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive:
                archive.add(source, arcname=source.name, recursive=False, filter=normalize)
                paths = (
                    path
                    for path in source.rglob("*")
                    if not EXCLUDED_PARTS.intersection(path.relative_to(source).parts)
                )
                for path in sorted(paths, key=lambda item: item.relative_to(source).as_posix()):
                    archive.add(
                        path,
                        arcname=(Path(source.name) / path.relative_to(source)).as_posix(),
                        recursive=False,
                        filter=normalize,
                    )


if __name__ == "__main__":
    main()
