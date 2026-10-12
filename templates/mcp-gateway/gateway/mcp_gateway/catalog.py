"""E2B MCP server ids → Docker MCP Catalog entries.

E2B's `McpServer` type (`spec/mcp-server.json` in the E2B SDK repo) keys
catalog servers by camelCase ids (`awsCdk`, `githubOfficial`, …) and their
settings by camelCase field names (`apiKey`, `personalAccessToken`, …). Every
entry carries `x-dockerHubUrl` = `https://hub.docker.com/mcp/server/<name>/…`,
naming the Docker MCP Catalog server that implements it. The Docker catalog
splits a server's settings into secrets (`<server>.<secret>` → container env)
and config properties (`config[block][path…]`, templated into env, command
and volumes). This module maps one onto the other.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_HUB_URL = re.compile(r"/mcp/server/([^/]+)")
_TEMPLATE = re.compile(r"\{\{\s*([^}|\s]+)")


def _norm(name: str) -> str:
	return re.sub(r"[^a-z0-9]", "", name.lower())


def _kebab(camel: str) -> str:
	return re.sub(r"(?<=[a-z0-9])([A-Z])", r"-\1", camel).lower()


@dataclass(frozen=True)
class Slot:
	"""One setting of a Docker catalog server."""

	kind: str  # "secret" | "config"
	key: tuple[str, ...]  # secret: (name,); config: (block, prop, …)
	candidates: tuple[str, ...]  # normalized names a field may match


@dataclass
class Resolved:
	"""A server selected for the Docker gateway with its settings."""

	name: str
	secrets: dict[str, str] = field(default_factory=dict)
	config: dict[str, dict[str, Any]] = field(default_factory=dict)


class CatalogError(ValueError):
	"""The E2B configuration cannot be mapped onto the catalog."""


class Catalog:
	"""The E2B id table plus the Docker MCP Catalog (`registry:` map)."""

	def __init__(self, e2b_spec: dict[str, Any], docker_catalog: dict[str, Any]):
		self.e2b = e2b_spec.get("properties", {})
		self.docker = docker_catalog.get("registry", {})

	@classmethod
	def load(cls, e2b_spec_path: Path, docker_catalog_path: Path) -> Catalog:
		with open(e2b_spec_path) as f:
			spec = json.load(f)
		with open(docker_catalog_path) as f:
			docker = yaml.safe_load(f)
		return cls(spec, docker)

	def docker_name(self, server_id: str) -> str:
		"""Docker catalog name for an E2B id (or a Docker catalog name)."""
		entry = self.e2b.get(server_id)
		if entry is not None:
			m = _HUB_URL.search(entry.get("x-dockerHubUrl", ""))
			if m and m.group(1) in self.docker:
				return m.group(1)
		if server_id in self.docker:
			return server_id
		kebab = _kebab(server_id)
		if kebab in self.docker:
			return kebab
		known = "unknown MCP server" if entry is None else "not in the Docker MCP Catalog"
		raise CatalogError(f"{server_id}: {known}")

	def image(self, server_id: str) -> str | None:
		"""Container image of a catalog server (None for remote servers)."""
		entry = self.docker[self.docker_name(server_id)]
		return entry.get("image") if entry.get("type") in ("server", "poci") else None

	def slots(self, docker_name: str) -> list[Slot]:
		entry = self.docker[docker_name]
		slots: list[Slot] = []
		prefix = docker_name + "."
		for secret in entry.get("secrets") or []:
			name = secret["name"]
			short = name[len(prefix) :] if name.startswith(prefix) else name.split(".", 1)[-1]
			cands = {_norm(name), _norm(short)}
			if secret.get("env"):
				cands.add(_norm(secret["env"]))
			slots.append(Slot("secret", (name,), tuple(sorted(cands))))
		# Env vars whose value is a `{{block.path}}` template name the config leaf.
		env_names: dict[str, list[str]] = {}
		for env in entry.get("env") or []:
			for ref in _TEMPLATE.findall(str(env.get("value", ""))):
				env_names.setdefault(ref, []).append(env["name"])
		for block in entry.get("config") or []:
			self._config_slots(block.get("name", docker_name), block, (), env_names, slots)
		return slots

	def _config_slots(self, block, schema, path, env_names, out) -> None:
		props = schema.get("properties") or {}
		for prop, sub in props.items():
			sub_path = (*path, prop)
			if (sub or {}).get("type") == "object" and (sub or {}).get("properties"):
				self._config_slots(block, sub, sub_path, env_names, out)
				continue
			cands = {_norm("".join(sub_path)), _norm(prop)}
			for env in env_names.get(".".join((block, *sub_path)), []):
				cands.add(_norm(env))
			out.append(Slot("config", (block, *sub_path), tuple(sorted(cands))))

	def match_fields(self, docker_name: str, fields: list[str]) -> dict[str, Slot]:
		"""Assign every E2B field of a server to one catalog slot."""
		slots = self.slots(docker_name)
		free = list(slots)
		assigned: dict[str, Slot] = {}

		def take(fname: str, slot: Slot) -> None:
			assigned[fname] = slot
			free.remove(slot)

		# Pass 1: exact (normalized) name matches.
		for fname in fields:
			n = _norm(fname)
			hits = [s for s in free if n in s.candidates]
			if len(hits) >= 1:
				take(fname, hits[0])
		# Pass 2: one name is a suffix of the other (`apiKey` ↔ `atlan_api_key`,
		# `airtableApiKey` ↔ `api_key`); closest length wins.
		for fname in fields:
			if fname in assigned:
				continue
			n = _norm(fname)
			best: tuple[int, Slot] | None = None
			for s in free:
				for c in s.candidates:
					if c and (c.endswith(n) or n.endswith(c)):
						d = abs(len(c) - len(n))
						if best is None or d < best[0]:
							best = (d, s)
			if best is not None:
				take(fname, best[1])
		# Pass 3: the only setting left (`beagleSecurityApiToken` ↔
		# `Beagle-Security-Personal-access-token`).
		rest = [f for f in fields if f not in assigned]
		if len(rest) == 1 and len(free) == 1:
			take(rest[0], free[0])
		return assigned

	def resolve(self, server_id: str, settings: dict[str, Any]) -> Resolved:
		"""Translate one E2B `mcp` entry into Docker gateway secrets/config."""
		if not isinstance(settings, dict):
			raise CatalogError(f"{server_id}: configuration must be an object")
		# `required` in the SDK types is advisory: like docker/mcp-gateway, a
		# server starts without unset settings and reports its own errors.
		name = self.docker_name(server_id)
		mapping = self.match_fields(name, list(settings))
		unknown = [f for f in settings if f not in mapping]
		if unknown:
			raise CatalogError(f"{server_id}: unknown field(s) {', '.join(unknown)}")
		out = Resolved(name)
		for fname, value in settings.items():
			slot = mapping[fname]
			if slot.kind == "secret":
				text = value if isinstance(value, str) else json.dumps(value)
				if "\n" in text:
					raise CatalogError(f"{server_id}.{fname}: multi-line secrets are not supported")
				out.secrets[slot.key[0]] = text
			else:
				node = out.config.setdefault(slot.key[0], {})
				for part in slot.key[1:-1]:
					node = node.setdefault(part, {})
				node[slot.key[-1]] = value
		return out
