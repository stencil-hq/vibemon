from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
import tarfile
from dataclasses import dataclass, replace
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import ClassVar, Literal, Self


class ImageError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class ImageSource:
    kind: Literal["python", "registry", "dockerfile", "template"]
    value: str
    context: str | None = None
    revision: str | None = None


@dataclass(frozen=True, slots=True)
class ImageArtifact:
    key: str
    data: bytes
    sha256: str
    destination: str | None = None
    media_type: str = "application/octet-stream"


ImageStepKind = Literal[
    "uv_sync",
    "uv_install",
    "apt_install",
    "env",
    "run_commands",
    "add_local_file",
    "add_local_python_source",
]


@dataclass(frozen=True, slots=True)
class ImageStep:
    kind: ImageStepKind
    values: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Image:
    source: ImageSource
    steps: tuple[ImageStep, ...] = ()

    _PYTHON_VERSION: ClassVar[re.Pattern[str]] = re.compile(r"^\d+\.\d+(?:\.\d+)?$")

    @classmethod
    def python(cls, version: str = "3.14", *, slim: bool = True) -> Self:
        if not cls._PYTHON_VERSION.fullmatch(version):
            raise ImageError(f"invalid Python version: {version!r}")
        variant = "slim" if slim else "bookworm"
        return cls(ImageSource("python", f"{version}-{variant}"))

    @classmethod
    def from_registry(cls, reference: str) -> Self:
        return cls(ImageSource("registry", _nonempty(reference, "registry reference")))

    @classmethod
    def from_dockerfile(
        cls,
        dockerfile: str | os.PathLike[str] = "Dockerfile",
        *,
        context: str | os.PathLike[str] = ".",
    ) -> Self:
        return cls(
            ImageSource(
                "dockerfile",
                os.fspath(dockerfile),
                context=os.fspath(context),
            )
        )

    @classmethod
    def from_template(cls, template: str, *, revision: str | None = None) -> Self:
        name = _nonempty(template, "template")
        if revision is None and "@" in name:
            name, revision = name.rsplit("@", 1)
        if revision is None or not re.fullmatch(r"[0-9a-f]{64}", revision):
            raise ImageError("template revision must be an immutable 64-character SHA-256")
        return cls(ImageSource("template", _nonempty(name, "template"), revision=revision))

    def uv_sync(
        self,
        *,
        frozen: bool = True,
        project: str | os.PathLike[str] = ".",
        groups: tuple[str, ...] = (),
    ) -> Self:
        values = (
            os.fspath(project),
            "frozen" if frozen else "unlocked",
            *_strings(groups, "group"),
        )
        return self._step("uv_sync", values)

    def uv_install(self, *packages: str) -> Self:
        return self._step("uv_install", _strings(packages, "package", required=True))

    def apt_install(self, *packages: str) -> Self:
        return self._step("apt_install", _strings(packages, "package", required=True))

    def env(self, values: dict[str, str] | None = None, /, **variables: str) -> Self:
        merged = dict(values or {})
        merged.update(variables)
        if not merged:
            raise ImageError("env requires at least one variable")
        entries: list[str] = []
        for key, value in sorted(merged.items()):
            if not key or "=" in key or "\x00" in key:
                raise ImageError(f"invalid environment variable name: {key!r}")
            if not isinstance(value, str) or "\x00" in value:
                raise ImageError(f"invalid value for environment variable {key!r}")
            entries.extend((key, value))
        return self._step("env", tuple(entries))

    def run_commands(self, *commands: str) -> Self:
        return self._step("run_commands", _strings(commands, "command", required=True))

    def add_local_file(
        self,
        source: str | os.PathLike[str],
        destination: str | os.PathLike[str],
    ) -> Self:
        return self._step(
            "add_local_file",
            (os.fspath(source), _container_path(destination)),
        )

    def add_local_python_source(
        self,
        source: str | os.PathLike[str],
        *,
        destination: str | os.PathLike[str] = "/opt/vmon/src",
    ) -> Self:
        return self._step(
            "add_local_python_source",
            (os.fspath(source), _container_path(destination)),
        )

    def artifacts(self) -> tuple[ImageArtifact, ...]:
        artifacts: list[ImageArtifact] = []
        if self.source.kind == "dockerfile":
            context = Path(self.source.context or ".").resolve()
            dockerfile = Path(self.source.value)
            if not dockerfile.is_absolute():
                dockerfile = (context / dockerfile).resolve()
            try:
                dockerfile.relative_to(context)
            except ValueError as exc:
                raise ImageError("Dockerfile must be inside its build context") from exc
            data = _archive(context)
            artifacts.append(_artifact("dockerfile_context", data, media_type="application/x-tar"))
        for index, step in enumerate(self.steps):
            if step.kind == "uv_sync":
                data = _archive(Path(step.values[0]))
                artifacts.append(
                    _artifact(
                        f"step:{index}",
                        data,
                        destination=f"/opt/vmon/projects/{hashlib.sha256(data).hexdigest()}",
                        media_type="application/x-tar",
                    )
                )
            elif step.kind in {"add_local_file", "add_local_python_source"}:
                source = Path(step.values[0])
                data = _archive(source) if source.is_dir() else _read_file(source)
                artifacts.append(
                    _artifact(
                        f"step:{index}",
                        data,
                        destination=step.values[1],
                        media_type="application/x-tar"
                        if source.is_dir()
                        else "application/octet-stream",
                    )
                )
        return tuple(artifacts)

    def canonical_dict(self) -> dict[str, object]:
        artifact_digests: dict[str, str] = {}
        try:
            artifact_digests = {item.key: item.sha256 for item in self.artifacts()}
        except FileNotFoundError, NotADirectoryError, ImageError:
            pass
        return {
            "source": {
                "context": None if self.source.kind == "dockerfile" else self.source.context,
                "kind": self.source.kind,
                "revision": self.source.revision,
                "value": (
                    _dockerfile_relative(self.source)
                    if self.source.kind == "dockerfile"
                    else self.source.value
                ),
            },
            "artifacts": artifact_digests,
            "steps": [
                {
                    "kind": step.kind,
                    "values": [
                        value
                        for index, value in enumerate(step.values)
                        if not (
                            step.kind in {"uv_sync", "add_local_file", "add_local_python_source"}
                            and index == 0
                        )
                    ],
                }
                for step in self.steps
            ],
        }

    @property
    def digest(self) -> str:
        encoded = json.dumps(self.canonical_dict(), sort_keys=True, separators=(",", ":")).encode()
        return hashlib.sha256(encoded).hexdigest()

    def _step(self, kind: ImageStepKind, values: tuple[str, ...]) -> Self:
        return replace(self, steps=(*self.steps, ImageStep(kind, values)))


def _dockerfile_relative(source: ImageSource) -> str:
    context = Path(source.context or ".").resolve()
    dockerfile = Path(source.value)
    if not dockerfile.is_absolute():
        dockerfile = context / dockerfile
    return dockerfile.resolve().relative_to(context).as_posix()


def _artifact(
    key: str,
    data: bytes,
    *,
    destination: str | None = None,
    media_type: str,
) -> ImageArtifact:
    return ImageArtifact(key, data, hashlib.sha256(data).hexdigest(), destination, media_type)


def _read_file(path: Path) -> bytes:
    if not path.is_file():
        raise ImageError(f"local image input is not a regular file: {path}")
    return path.read_bytes()


def _archive(path: Path) -> bytes:
    path = path.resolve()
    entries: tuple[Path, ...]
    if path.is_file():
        root, entries = path.parent, (path,)
    elif path.is_dir():
        root = path
        entries = tuple(
            sorted(
                (item for item in path.rglob("*") if item.is_file()),
                key=lambda p: p.relative_to(root).as_posix(),
            )
        )
    else:
        raise ImageError(f"local image input does not exist: {path}")
    output = BytesIO()
    with tarfile.open(fileobj=output, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for entry in entries:
            relative = entry.relative_to(root).as_posix()
            info = tarfile.TarInfo(relative)
            data = entry.read_bytes()
            info.size = len(data)
            info.mode = 0o755 if entry.stat().st_mode & 0o111 else 0o644
            info.mtime = info.uid = info.gid = 0
            info.uname = info.gname = ""
            archive.addfile(info, BytesIO(data))
    return output.getvalue()


def command_argv(command: str) -> tuple[str, ...]:
    try:
        argv = tuple(shlex.split(command))
    except ValueError as exc:
        raise ImageError(f"invalid build command: {exc}") from exc
    if not argv:
        raise ImageError("build command must contain an executable")
    return argv


def _nonempty(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ImageError(f"{label} must be a non-empty string")
    return value


def _strings(
    values: tuple[str, ...],
    label: str,
    *,
    required: bool = False,
) -> tuple[str, ...]:
    if required and not values:
        raise ImageError(f"at least one {label} is required")
    return tuple(_nonempty(value, label) for value in values)


def _container_path(value: str | os.PathLike[str]) -> str:
    path = os.fspath(value)
    if not path.startswith("/") or ".." in PurePosixPath(path).parts:
        raise ImageError(f"container destination must be an absolute normalized path: {path!r}")
    return path
