"""The `mcp` object of `Sandbox.create(mcp=…)`, as passed to `--config`.

Keys are either catalog server ids (`spec/mcp-server.json`, e.g. `exa`,
`githubOfficial`) mapped to their settings, or `github/<owner>/<repo>` custom
stdio servers. The JS SDK spells the custom-server fields `runCmd` /
`installCmd`, the Python SDK `run_cmd` / `install_cmd`; both are accepted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .catalog import Catalog, CatalogError, Resolved

_REPO_PART = re.compile(r"^[A-Za-z0-9_.-]+$")


class ConfigError(ValueError):
	"""The `mcp` configuration is invalid."""


@dataclass(frozen=True)
class GitHubServer:
	"""A custom MCP server cloned from a public GitHub repository."""

	key: str
	owner: str
	repo: str
	run_cmd: str
	install_cmd: str | None
	envs: dict[str, str]

	@property
	def clone_url(self) -> str:
		return f"https://github.com/{self.owner}/{self.repo}.git"


@dataclass
class GatewayConfig:
	"""Validated gateway configuration."""

	github: list[GitHubServer] = field(default_factory=list)
	catalog: list[Resolved] = field(default_factory=list)


def _str_field(key: str, entry: dict[str, Any], *names: str) -> str | None:
	present = [n for n in names if entry.get(n) is not None]
	if len(present) > 1 and len({entry[n] for n in present}) > 1:
		raise ConfigError(f"{key}: conflicting {' and '.join(present)}")
	if not present:
		return None
	value = entry[present[0]]
	if not isinstance(value, str):
		raise ConfigError(f"{key}.{present[0]}: must be a string")
	return value


def parse_github(key: str, entry: Any) -> GitHubServer:
	parts = key.split("/")
	if len(parts) != 3 or not all(_REPO_PART.match(p) for p in parts[1:]) or parts[2] in (".", ".."):
		raise ConfigError(f"{key}: expected github/<owner>/<repo>")
	if not isinstance(entry, dict):
		raise ConfigError(f"{key}: configuration must be an object")
	known = {"runCmd", "run_cmd", "installCmd", "install_cmd", "envs"}
	unknown = sorted(set(entry) - known)
	if unknown:
		raise ConfigError(f"{key}: unknown field(s) {', '.join(unknown)}")
	run_cmd = _str_field(key, entry, "runCmd", "run_cmd")
	if not run_cmd or not run_cmd.strip():
		raise ConfigError(f"{key}: runCmd is required")
	install_cmd = _str_field(key, entry, "installCmd", "install_cmd")
	envs = entry.get("envs") or {}
	if not isinstance(envs, dict) or not all(
		isinstance(k, str) and isinstance(v, str) for k, v in envs.items()
	):
		raise ConfigError(f"{key}.envs: must map strings to strings")
	return GitHubServer(key, parts[1], parts[2], run_cmd, install_cmd or None, dict(envs))


def parse(raw: Any, catalog: Catalog | None) -> GatewayConfig:
	"""Validate the whole configuration; raises `ConfigError` on any problem.

	`catalog` may be None when no catalog data is installed; catalog entries
	then fail validation.
	"""
	if not isinstance(raw, dict):
		raise ConfigError("configuration must be a JSON object")
	out = GatewayConfig()
	for key, entry in raw.items():
		if key.startswith("github/"):
			out.github.append(parse_github(key, entry))
			continue
		if catalog is None:
			raise ConfigError(f"{key}: the MCP server catalog is not installed")
		try:
			out.catalog.append(catalog.resolve(key, entry if entry is not None else {}))
		except CatalogError as e:
			raise ConfigError(str(e)) from None
	names = [r.name for r in out.catalog]
	dupes = sorted({n for n in names if names.count(n) > 1})
	if dupes:
		raise ConfigError(f"configured more than once: {', '.join(dupes)}")
	return out
