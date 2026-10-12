# Connecting SDKs and the CLI

Configure the official E2B SDKs (`e2b`, `e2b_code_interpreter` / `@e2b/code-interpreter`, `e2b_desktop` / `@e2b/desktop`) and the `e2b` CLI to talk to vibemon instead of E2B's cloud. They need no code changes, only environment variables. This page also shows how to call vibemon's API extensions, which the SDKs cannot send.

## Environment variables

| Variable | Read by | Value for vibemon |
|---|---|---|
| `E2B_API_KEY` | SDKs, CLI | A team key (`e2b_` + 40 hex) from `vmon admin create-team` or `create-api-key`. See [Authentication and teams](../concepts/auth.md). |
| `E2B_DOMAIN` | SDKs, CLI | The driver's `--domain`. Without other overrides, the SDKs use `https://api.<domain>` for the API and `https://<port>-<sandboxId>.<domain>` for sandbox traffic. |
| `E2B_API_URL` | SDKs, CLI | Base URL of the API when it is not `https://api.<domain>`, e.g. `http://127.0.0.1:3000`. |
| `E2B_SANDBOX_URL` | SDKs, CLI | Send all sandbox (envd) traffic to this URL, routed by the `E2b-Sandbox-Id` / `E2b-Sandbox-Port` headers. Set it to the same URL as `E2B_API_URL` when the sandbox subdomains are not served over HTTPS. |
| `E2B_VOLUME_API_URL` | SDKs | Base URL of the volume-content API (`/volumecontent/…`). Set it to the driver (same value as `E2B_API_URL`) when the API is not at `https://api.<domain>`. The Python SDK's volume calls ignore `E2B_API_URL`. |
| `E2B_DOCS_BASE` | CLI | Base of the login page `e2b auth login` opens (default `https://$E2B_DOMAIN/docs`). See [CLI login](#cli-login). |
| `SSL_CERT_FILE`, `NODE_EXTRA_CA_CERTS` | Python, Node | CA bundle for a private TLS certificate on the driver (see [TLS with a private CA](#tls-with-a-private-ca)). |

Leave `E2B_DEBUG`, `E2B_HTTP_VERSION`, and `E2B_ACCESS_TOKEN` unset; the e2e harness unsets all three. `E2B_ACCESS_TOKEN` is deprecated upstream: the CLI never reads it, and the driver answers the old `/access-tokens` routes with `410`.

## Routing modes

The driver serves the API and sandbox traffic on the same listener. A request goes to a sandbox when its Host is `<port>-<sandboxId>.<anything>`, or when it carries both `E2b-Sandbox-Id` and `E2b-Sandbox-Port`; requests without either target go to the API. Incomplete or malformed routing headers get 400. Clients pick one of two setups. The server side (domains, certificates, DNS) is in [Domains, TLS, and ingress](../operate/ingress.md).

### Header routing (no TLS, development)

Point everything at the plain-HTTP listener:

```sh
export E2B_API_KEY=e2b_...
export E2B_DOMAIN=localhost                    # the driver's --domain
export E2B_API_URL=http://127.0.0.1:3000
export E2B_SANDBOX_URL=http://127.0.0.1:3000
export E2B_VOLUME_API_URL=http://127.0.0.1:3000
```

SDK calls (commands, files, PTY, code execution, volumes) work. Domain-based hosts and URLs still require the sandbox subdomains; signed file URLs also need routing information when opened outside the SDK:

- `get_host()` / `getHost()` results, opened in a browser or HTTP client;
- signed file URLs (`download_url()`, `upload_url()`): Python honors `E2B_SANDBOX_URL`, but requests to that URL still need the sandbox routing headers;
- `get_mcp_url()` / `getMcpUrl()`;
- the desktop stream URL (`stream.get_url()`).

To reach a guest port by hand in this mode, set the Host or the routing headers yourself:

```sh
curl -H 'E2b-Sandbox-Id: <sandboxId>' -H 'E2b-Sandbox-Port: 8080' http://127.0.0.1:3000/
curl -H 'Host: 8080-<sandboxId>.localhost' http://127.0.0.1:3000/
```

### Subdomain routing (TLS, production)

With the driver on `--tls-listen`, a certificate for `*.<domain>` and `api.<domain>`, and DNS for both pointing at the driver, the SDKs need only the upstream variables:

```sh
export E2B_API_KEY=e2b_...
export E2B_DOMAIN=vmon.example.com
```

The API is `https://api.vmon.example.com`, and sandbox port 8080 is `https://8080-<sandboxId>.vmon.example.com`. This supports SDK calls and the URLs listed above (unset any plain-HTTP URL overrides). This is the topology of upstream E2B and of the e2e suites' `--ingress tls` mode.

### TLS with a private CA

- **Node** (JS SDKs, CLI): `NODE_EXTRA_CA_CERTS=/path/ca.pem`.
- **Python** on Linux: `SSL_CERT_FILE` pointing at a bundle of the system roots plus your CA.
- **Python on macOS**: the SDK's transport verifies through the macOS keychain and ignores `SSL_CERT_FILE`. Trust the CA in the keychain, or keep header routing (`E2B_API_URL` / `E2B_SANDBOX_URL`) for the Python SDK while other clients use the TLS subdomains. The e2e harness does the latter.

To check that the TLS listener negotiates HTTP/2:

```sh
curl -v --cacert ca.pem https://api.vmon.example.com/health    # expect "ALPN: server accepted h2"
```

## SDK notes

Default templates are resolved by bare alias (`base`, `code-interpreter-v1`, `desktop`, `mcp-gateway`). They exist only after an operator builds them with `just templates`; see [Templates](../concepts/templates.md).

**Code interpreter**: `Sandbox.create()` uses `code-interpreter-v1`. Jupyter on guest port 49999 is reached through the same ingress, in either routing mode.

```python
from e2b_code_interpreter import Sandbox

with Sandbox.create() as sbx:
    print(sbx.run_code("1 + 1").text)
```

**Desktop**: `Sandbox.create()` uses `desktop`. Viewing the stream from `stream.get_url()` needs subdomain routing.

```python
from e2b_desktop import Sandbox

desktop = Sandbox.create()
desktop.stream.start()
print(desktop.stream.get_url())   # https://6080-<sandboxId>.<domain>/vnc.html?autoconnect=true&resize=scale
```

**MCP gateway**: `Sandbox.create(mcp=…)` starts sandboxes from the `mcp-gateway` template. `get_mcp_url()` returns `https://50005-<sandboxId>.<domain>/mcp`, so it needs subdomain routing; send `get_mcp_token()` as the bearer token. The template runs Docker in the guest and therefore needs the vmon guest kernel ([Guest kernel](../operate/kernel.md)). Supported catalog entries and limits: [E2B compatibility](../reference/e2b-compat.md), §4 and §5.

**Egress header rules and SOCKS5 upstreams** (`network` rules, `egressProxy`) work only on Linux nodes; creates that need them fail with `503` when no Linux node is available. See [Networking and egress](../operate/networking.md).

## CLI login

The `e2b` CLI authenticates with `E2B_API_KEY`, or with the key that `e2b auth login` stores in `~/.e2b/config.json`. Login needs a user, created by an operator:

```sh
vmon admin --mysql-url mysql://root:vmon@127.0.0.1:13306/vmon \
  create-user dev@example.com --password 'change-me' --team dev
```

`--team` takes the team's id or slug and makes it the user's default team. Then:

```sh
npm install -g @e2b/cli
export E2B_DOMAIN=localhost
export E2B_API_URL=http://127.0.0.1:3000
export E2B_SANDBOX_URL=http://127.0.0.1:3000          # only without TLS subdomains
export E2B_DOCS_BASE=http://127.0.0.1:3000/docs       # only if https://$E2B_DOMAIN/docs is not the driver
e2b auth login
e2b sandbox create base
```

The CLI opens `${E2B_DOCS_BASE:-https://$E2B_DOMAIN/docs}/api/cli`. The driver serves this login page on any Host that routes to the API rather than to a sandbox. After you sign in, it redirects back to the CLI's localhost callback, and the CLI picks the user's default team. Command-by-command status: [E2B compatibility](../reference/e2b-compat.md), §6.

## Vibemon extensions over raw HTTP

The SDKs only send upstream fields. For vibemon extensions, call the REST API directly with `X-API-Key`, then attach the SDK to the sandbox by id. Full semantics: [E2B compatibility](../reference/e2b-compat.md), §5.

### Bucket mounts (`vmonS3Mounts`)

```sh
curl -sX POST "$E2B_API_URL/v2/sandboxes" -H "X-API-Key: $E2B_API_KEY" \
  -H 'Content-Type: application/json' -d '{"templateID": "base",
  "vmonS3Mounts": [{"path": "/mnt/data", "uri": "s3://my-bucket/datasets",
    "mode": "ro", "credentialsSecret": "my-bucket-keys"}]}'
```

The response carries `sandboxID`. Attach to it:

```python
from e2b import Sandbox

sbx = Sandbox.connect("<sandboxID>")
print(sbx.commands.run("ls /mnt/data").stdout)
```

`mode` is `ro` (default), `overlay`, or `writeback`. `credentialsSecret` names a team secret holding the credentials JSON; inline `credentials` work too. Operator settings (`--s3-mounts-allow-private`): [Storage, object store, and volumes](../operate/storage.md).

### Sandbox self-keepalive

Every sandbox response (`POST /v2/sandboxes`, `GET /sandboxes/{id}`) includes `envdAccessToken`. Sent as `X-Access-Token`, it authorizes three routes, and only for that sandbox: `GET /sandboxes/{id}`, `POST /sandboxes/{id}/timeout`, and `POST /sandboxes/{id}/refreshes`. Give it to code inside the sandbox so the sandbox can extend its own lifetime without a team key:

```sh
curl -sX POST "$E2B_API_URL/sandboxes/$SANDBOX_ID/timeout" \
  -H "X-Access-Token: $ENVD_ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"timeout": 300}'                                  # seconds from now -> 204
curl -sX POST "$E2B_API_URL/sandboxes/$SANDBOX_ID/refreshes" \
  -H "X-Access-Token: $ENVD_ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"duration": 60}'                                  # extend by seconds -> 204
```

Wrong tokens, and killed or unknown sandboxes, get `401`. The team's maximum sandbox length still caps the expiry. From inside the sandbox, the API URL must be reachable under the sandbox's egress policy; private ranges are denied unless the operator allows them with `--egress-internal-allow` ([Networking and egress](../operate/networking.md)).
