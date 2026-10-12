"""Mapping of E2B `mcp` configurations onto Docker MCP Catalog settings.

Run (from `templates/mcp-gateway/gateway`):
`uv run --no-project --with pytest --with pyyaml --with mcp==1.30.0 python -m pytest -p no:cacheprovider tests`
"""

import json
from pathlib import Path

import pytest
import yaml

from mcp_gateway.catalog import Catalog
from mcp_gateway.config import ConfigError, parse

REPO = Path(__file__).resolve().parents[4]
E2B_SPEC = json.loads((REPO / "spec/e2b/sdk/spec/mcp-server.json").read_text())

# Excerpts of the Docker MCP Catalog (v3) with the shapes that matter.
DOCKER_CATALOG = yaml.safe_load(
	"""
registry:
  airtable-mcp-server:
    type: server
    image: mcp/airtable-mcp-server@sha256:3d15
    secrets:
      - {name: airtable-mcp-server.api_key, env: AIRTABLE_API_KEY}
    env:
      - {name: NODE_ENV, value: '{{airtable-mcp-server.nodeenv}}'}
    config:
      - name: airtable-mcp-server
        type: object
        properties: {nodeenv: {type: string}}
  browserbase:
    type: server
    image: mcp/browserbase@sha256:f90c
    secrets:
      - {name: browserbase.browserbase_api_key, env: BROWSERBASE_API_KEY}
      - {name: browserbase.browserbase_project_id, env: BROWSERBASE_PROJECT_ID}
      - {name: browserbase.gemini_api_key, env: GEMINI_API_KEY}
  atlassian:
    type: server
    image: mcp/atlassian@sha256:ecec
    secrets:
      - {name: atlassian.confluence.api_token, env: CONFLUENCE_API_TOKEN}
      - {name: atlassian.jira.api_token, env: JIRA_API_TOKEN}
      - {name: atlassian.confluence.personal_token, env: CONFLUENCE_PERSONAL_TOKEN}
      - {name: atlassian.jira.personal_token, env: JIRA_PERSONAL_TOKEN}
    env:
      - {name: CONFLUENCE_URL, value: '{{atlassian.confluence.url}}'}
      - {name: JIRA_URL, value: '{{atlassian.jira.url}}'}
    config:
      - name: atlassian
        type: object
        properties:
          confluence: {type: object, properties: {url: {type: string}, username: {type: string}}}
          jira: {type: object, properties: {url: {type: string}, username: {type: string}}}
  filesystem:
    type: server
    image: mcp/filesystem@sha256:35fc
    command: ['{{filesystem.paths|volume-target|into}}']
    config:
      - name: filesystem
        type: object
        properties: {paths: {type: array, items: {type: string}}}
  fetch:
    type: server
    image: mcp/fetch@sha256:1a7a
  context7:
    type: remote
    remote: {transport_type: streamable-http, url: 'https://mcp.context7.com/mcp'}
    secrets:
      - {name: context7.api_key, env: CONTEXT7_API_KEY}
  mcp-hackernews:
    type: server
    image: mcp/hackernews@sha256:aaaa
  brave:
    type: server
    image: mcp/brave-search@sha256:cccc
    secrets:
      - {name: brave.api_key, env: BRAVE_API_KEY}
"""
)


@pytest.fixture
def catalog() -> Catalog:
	return Catalog(E2B_SPEC, DOCKER_CATALOG)


def test_camel_case_ids_and_fields_map_to_catalog_secrets_and_config(catalog):
	cfg = parse(
		{
			"airtable": {"airtableApiKey": "pat1", "nodeenv": "production"},
			"browserbase": {"apiKey": "bb", "geminiApiKey": "gm", "projectId": "p1"},
			"atlassian": {"confluenceUrl": "https://c", "jiraApiToken": "jt"},
			"filesystem": {"paths": ["/home/user"]},
			"hackernews": {},
		},
		catalog,
	)
	got = {r.name: (r.secrets, r.config) for r in cfg.catalog}
	assert got["airtable-mcp-server"] == (
		{"airtable-mcp-server.api_key": "pat1"},
		{"airtable-mcp-server": {"nodeenv": "production"}},
	)
	assert got["browserbase"][0] == {
		"browserbase.browserbase_api_key": "bb",
		"browserbase.gemini_api_key": "gm",
		"browserbase.browserbase_project_id": "p1",
	}
	assert got["atlassian"] == (
		{"atlassian.jira.api_token": "jt"},
		{"atlassian": {"confluence": {"url": "https://c"}}},
	)
	assert got["filesystem"] == ({}, {"filesystem": {"paths": ["/home/user"]}})
	assert got["mcp-hackernews"] == ({}, {})


def test_remote_servers_have_no_image(catalog):
	assert catalog.image("context7") is None
	assert catalog.image("fetch") == "mcp/fetch@sha256:1a7a"


def test_github_servers_accept_js_and_python_spellings(catalog):
	cfg = parse(
		{
			"github/acme/js-server": {"runCmd": "node index.js", "installCmd": "npm install", "envs": {"A": "1"}},
			"github/acme/py-server": {"run_cmd": "python server.py", "install_cmd": "pip install ."},
		},
		catalog,
	)
	js, py = cfg.github
	assert (js.owner, js.repo, js.run_cmd, js.install_cmd, js.envs) == (
		"acme", "js-server", "node index.js", "npm install", {"A": "1"},
	)
	assert (py.run_cmd, py.install_cmd, py.envs) == ("python server.py", "pip install .", {})
	assert js.clone_url == "https://github.com/acme/js-server.git"


@pytest.mark.parametrize(
	("raw", "message"),
	[
		({"github/acme/x": {"installCmd": "npm i"}}, "runCmd is required"),
		({"github/acme": {"runCmd": "x"}}, "expected github/<owner>/<repo>"),
		({"github/acme/..": {"runCmd": "x"}}, "expected github/<owner>/<repo>"),
		({"github/acme/x": {"runCmd": "x", "cmd": "y"}}, "unknown field(s) cmd"),
		({"fetch": {"bogus": 1}}, "unknown field(s) bogus"),
		({"notAServer": {}}, "unknown MCP server"),
		({"brave": {"apiKey": "a\nb"}}, "multi-line secrets"),
		([], "must be a JSON object"),
	],
)
def test_invalid_configurations_are_rejected(catalog, raw, message):
	with pytest.raises(ConfigError) as e:
		parse(raw, catalog)
	assert message in str(e.value)
