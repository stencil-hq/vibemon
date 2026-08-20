"""Client-side storage for named cluster contexts."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

__all__ = [
    "LOCAL",
    "Context",
    "contexts_path",
    "context_token_path",
    "ContextStore",
]

#: Reserved built-in context name for using the local vmond daemon.
LOCAL = "local"

#: Context names double as credential filenames; keep them path-safe.
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def state_dir() -> Path:
    """Return the vmon state directory without importing server internals."""
    return Path(os.environ.get("VMON_HOME", str(Path.home() / ".vmon")))


def _require_safe_name(name: str) -> str:
    """Reject context names that could escape the credentials directory."""
    if not _NAME_RE.match(name):
        raise ValueError(
            f"invalid context name {name!r}; use letters, digits, '.', '_', or '-' "
            "(starting with a letter or digit)"
        )
    return name


@dataclass
class Context:
    """A named gateway roster; bearer tokens are stored outside contexts.json."""

    name: str
    endpoints: list[str]
    token: str | None = None
    region: str = ""
    updated: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "endpoints": list(self.endpoints),
            "region": self.region,
            "updated": self.updated,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Context:
        """Build a context from tolerant JSON data, defaulting missing fields."""
        endpoints_value = data.get("endpoints", [])
        if isinstance(endpoints_value, list):
            endpoints = [value for value in endpoints_value if isinstance(value, str) and value]
        else:
            endpoints = []

        name_value = data.get("name", "")
        region_value = data.get("region", "")
        updated_value = data.get("updated", 0.0)

        if not isinstance(updated_value, int | float):
            updated_value = 0.0

        return cls(
            name=name_value if isinstance(name_value, str) else "",
            endpoints=endpoints,
            token=None,
            region=region_value if isinstance(region_value, str) else "",
            updated=float(updated_value),
        )


def contexts_path(home: Path | None = None) -> Path:
    """Return the JSON store path under the given or default vmon state directory."""
    return (home or state_dir()) / "contexts.json"


def context_token_path(name: str, home: Path | None = None) -> Path:
    """Return the bearer-token path for a context under the credentials store."""
    return (home or state_dir()) / "credentials" / f"{_require_safe_name(name)}.token"


class ContextStore:
    """Load, persist, and select CLI cluster contexts."""

    def __init__(self, path: Path | None = None) -> None:
        """Create an empty store backed by the given contexts file path."""
        self._path = path or contexts_path()
        self._current: str | None = None
        self._contexts: dict[str, Context] = {}

    def load(self) -> None:
        """Load contexts from disk, replacing memory and tolerating absent/bad JSON."""
        self._current = None
        self._contexts = {}

        try:
            with self._path.open(encoding="utf-8") as file:
                data = json.load(file)
        except OSError, json.JSONDecodeError:
            return

        if not isinstance(data, Mapping):
            return

        current = data.get("current")
        self._current = current if isinstance(current, str) and current else None

        contexts = data.get("contexts", {})
        if not isinstance(contexts, Mapping):
            return

        loaded: dict[str, Context] = {}
        for key, value in contexts.items():
            if not isinstance(key, str) or not isinstance(value, Mapping):
                continue
            context = Context.from_dict(value)
            if not context.name:
                context.name = key
            loaded[context.name] = context
        self._contexts = loaded

    def save(self) -> None:
        """Atomically persist current selection and contexts to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        data = {
            "current": self._current,
            "contexts": {name: ctx.to_dict() for name, ctx in self._contexts.items()},
        }
        with tmp.open("w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
        os.replace(tmp, self._path)

    def list(self) -> list[Context]:
        """Return all persisted contexts sorted by name."""
        return [self._contexts[name] for name in sorted(self._contexts)]

    def get(self, name: str) -> Context | None:
        """Return a context by name, or None when it does not exist."""
        return self._contexts.get(name)

    def current_name(self) -> str | None:
        """Return the active context name, honoring the VMON_CONTEXT override."""
        override = os.environ.get("VMON_CONTEXT")
        if override:
            return override
        return self._current or None

    def current(self) -> Context | None:
        """Return the active remote context, excluding the built-in local context."""
        name = self.current_name()
        if not name or name == LOCAL:
            return None
        return self.get(name)

    def use(self, name: str) -> None:
        """Select an existing context, or the reserved local context, and persist it."""
        if name != LOCAL and name not in self._contexts:
            raise KeyError(name)
        self._current = name
        self.save()

    def put(self, ctx: Context) -> None:
        """Insert or replace a context by name and persist the store.

        Raises ValueError for names unsafe as credential filenames.
        """
        self._contexts[_require_safe_name(ctx.name)] = ctx
        self.save()

    def remove(self, name: str) -> None:
        """Remove a context and its saved token, clearing it as current when selected."""
        self._contexts.pop(name, None)
        self.remove_token(name)
        if self._current == name:
            self._current = None
        self.save()

    def token_path(self, name: str) -> Path:
        """Return the token file path that shares this store's state root."""
        return self._path.parent / "credentials" / f"{_require_safe_name(name)}.token"

    def save_token(self, name: str, token: str) -> None:
        """Persist a context token with private directory and file permissions."""
        directory = self.token_path(name).parent
        directory.mkdir(parents=True, exist_ok=True)
        os.chmod(directory, 0o700)
        tmp = directory / f".{name}.token.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(token)
            file.write("\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, self.token_path(name))
        os.chmod(self.token_path(name), 0o600)

    def load_token(self, name: str) -> str | None:
        """Return a saved context token, or None when absent/empty/unreadable."""
        try:
            token = self.token_path(name).read_text(encoding="utf-8").strip()
        except OSError, ValueError:
            return None
        return token or None

    def has_token(self, name: str) -> bool:
        """Return whether a context has an opt-in saved token file."""
        return self.load_token(name) is not None

    def remove_token(self, name: str) -> None:
        """Delete a saved token file, tolerating missing files and unsafe names."""
        try:
            self.token_path(name).unlink()
        except FileNotFoundError, ValueError:
            pass

    def resolve_token(self, name: str) -> str | None:
        """Resolve a mesh bearer token: environment override, then saved file."""
        return os.environ.get("VMON_API_TOKEN") or self.load_token(name)
