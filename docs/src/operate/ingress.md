# Domains, TLS, and ingress

Each driver serves the E2B API and proxies sandbox traffic on the same public listener. This page covers the sandbox domain, how requests are routed, TLS and DNS setup, and the per-sandbox ingress settings. You need it when you expose vibemon beyond `localhost`. Client-side configuration is in [Connecting SDKs and the CLI](../get-started/clients.md).

## Sandbox URLs

`--domain` (`VMON_DOMAIN`, default `localhost`) is the sandbox domain:

| Address | Serves |
|---|---|
| `https://<port>-<sandboxID>.<domain>` | Guest TCP `<port>` of a sandbox. envd is port `49983`, so the SDKs talk to `49983-<id>.<domain>`. |
| `https://api.<domain>` | The E2B REST API, CLI login (`/docs/api/cli`), `/health`, and the volume content API (`/volumecontent/…`). |

The router never requires the `api.` host: any request that does not address a sandbox goes to the API. Sandbox API responses carry `domain` with the `--domain` value, which the SDKs use to build sandbox hosts. Sandbox IDs never contain `-`, so the first `-` in the left-most label separates the port from the ID.

Guest ports are reached through a GC4 tunnel to the guest agent, never through the guest network. Services bound to the guest's `127.0.0.1` are reachable, and no inbound NAT or firewall rule is needed on the node.

## Routing

A driver dispatches every public request in this order:

1. **Host:** the left-most label of `Host` (HTTP/1.1) or `:authority` (HTTP/2) is `<port>-<id>`, where the port is a number up to 65535 and the ID consists of lowercase letters and digits. The request goes to ingress. Labels after the ID (`8080-<id>-<anything>.…`) are ignored, and so is the rest of the host name: any domain works. IP literals and hosts without a dot never match.
2. **Headers:** both `E2b-Sandbox-Id` and `E2b-Sandbox-Port` are present. The request goes to ingress, whatever its Host. A host match takes precedence over the headers.
3. **Otherwise:** the request goes to the E2B API router.

Header routing lets a client send sandbox traffic to a single address, such as `http://127.0.0.1:3000`, without wildcard DNS. This is what `E2B_SANDBOX_URL` does (see [Connecting SDKs and the CLI](../get-started/clients.md)). Malformed routing headers get a plain-text 400: `missing header` when only one of them is set, `Invalid sandbox port`, or `Invalid sandbox ID`.

Any driver serves any sandbox. It reads the sandbox's route (`sbx/<id>`) from its local raft KV replica, finds the owning node, and opens a tunnel to the guest port over that node's `--cluster-listen`. A load balancer can therefore spread public traffic across all drivers with no stickiness.

## TLS

`--tls-listen` opens an HTTPS listener with the same router as `--listen`. It needs `--tls-cert` (PEM chain) and `--tls-key` (PEM key), and offers `h2` and `http/1.1` through ALPN. `--listen` stays open alongside it, serving plain HTTP/1.1 and h2c, and cannot be disabled. Bind it to loopback or firewall it if all clients should use HTTPS.

The certificate must cover `*.<domain>` (sandbox hosts) and `api.<domain>`. The verification setup also adds the bare `<domain>`. vibemon does not obtain or renew certificates: it reads the files once at startup, so restart the driver after renewing.

```sh
vmon serve --role driver \
  --domain sbx.example.com \
  --tls-listen 0.0.0.0:443 \
  --tls-cert /etc/vmon/tls/fullchain.pem \
  --tls-key /etc/vmon/tls/privkey.pem \
  --mysql-url "$VMON_MYSQL_URL"
```

With TLS on port 443 and DNS in place, the SDKs need only `E2B_API_KEY` and `E2B_DOMAIN`. They then address `https://<port>-<id>.<domain>` directly.

> [!NOTE]
> The Python SDK volume content API (`Volume.read_file`, …) ignores `E2B_API_URL`. It defaults to `https://api.<domain>` outside debug mode, but accepts `E2B_VOLUME_API_URL` or a per-operation `api_url` override, so a plain-HTTP listener can also be used.

To terminate TLS on an external load balancer instead, forward both `api.<domain>` and `*.<domain>` to `--listen` with the original `Host` header, and set `--public-url`. WebSocket upgrades must pass through the load balancer.

## Public URL

`--public-url` is the base URL clients use to reach this driver. The driver puts it in:

- signed template file upload URLs (`GET /templates/{id}/files/{hash}` returns a `PUT …/upload?expires=…&signature=…` URL, valid for 30 minutes and authenticated by its signature alone);
- `tokenEndpoint` and `revokeEndpoint` of `e2b auth login`.

The default is `https://api.<domain>` with `--tls-listen`, and `http://<advertise-ip>:<listen port>` otherwise. Set it explicitly when clients reach the driver through another host, a port other than 443, or a load balancer. Otherwise template builds that upload files fail from outside. In a cluster, each driver advertises its own public URL.

## DNS

Point these names at the drivers, or at a load balancer in front of them:

```text
api.sbx.example.com.   A     203.0.113.10
*.sbx.example.com.     A     203.0.113.10
```

For local testing without DNS, use `--domain localhost` with header routing, or the resolver approach in [Distributed, KVM, and TLS verification](../develop/cluster-verification.md).

## Traffic access tokens

A sandbox created with `network.allowPublicTraffic: false` (Python: `network={"allow_public_traffic": False}`) accepts requests on its ports only with an `e2b-traffic-access-token` header. The create response returns the token as `trafficAccessToken`. It is derived from the cluster secret and the sandbox ID, so it stays the same across pause and resume and is accepted by every driver.

```python
from e2b import Sandbox
import httpx

sbx = Sandbox.create(network={"allow_public_traffic": False})
sbx.commands.run("python3 -m http.server 8080", background=True)
r = httpx.get(f"https://{sbx.get_host(8080)}",
              headers={"e2b-traffic-access-token": sbx.traffic_access_token})
```

- Port `49983` (envd) is exempt. envd checks its own `X-Access-Token`.
- A missing or wrong token gets 403. The comparison is constant-time, and a private sandbox without a stored token refuses every request.
- The default is `true`: sandbox ports are public to anyone who knows the URL.

## Host masking and HTTPS ports

Both are set at create time in the sandbox's `network` config, and apply to every port except `49983`:

| Setting | Effect |
|---|---|
| `maskRequestHost` | The upstream `Host` sent to the guest is replaced with this value, with `${PORT}` replaced by the target port (for example `localhost:${PORT}`). The original host goes to `X-Forwarded-Host`. An empty string means no masking. |
| `httpsPorts` | The proxy speaks TLS to these guest ports (ALPN `http/1.1`) and does not verify the guest certificate, so self-signed certificates work. SNI is the request host, or the masked host. The public URL is HTTPS or HTTP depending on the driver listener, not on this setting. Up to 128 unique ports, and `49983` is rejected. |

## Proxy behaviour

- **Protocol:** requests reach the guest as HTTP/1.1 over the tunnel, whatever the client spoke. Bodies stream in both directions without buffering. Hop-by-hop headers are removed. If the client disconnects, the guest connection closes.
- **WebSockets and upgrades:** an HTTP/1.1 `Upgrade` request is passed through. After the guest answers 101, bytes are spliced in both directions.
- **Dial retry:** when the guest port refuses the connection, the proxy makes up to 5 attempts with a 100–400 ms linear backoff between them before it answers 502.
- **CORS:** API and sandbox responses allow any origin. The driver answers API preflight requests itself (cached for 86400 s). Sandbox preflights go to the guest, and the driver answers them only when routing fails. Upgrade responses (101) get no CORS headers.
- **Blocked envd paths:** on port `49983`, `/init`, `/freeze`, `/unfreeze`, `/collapse`, `/fsfreeze`, `/fsthaw`, and `/upgrade` return 404 for every method. These paths are reserved for the control plane. Paths are matched after percent-decoding and cleaning, so `/%69nit` and `/a/../init` are blocked too.

Errors are JSON (`{"sandboxId", "message", "code"}`), or a small HTML page when a browser navigates to the URL:

| Status | Message | Cause |
|---|---|---|
| 502 | `The sandbox was not found` | No live route and no auto-resume, sandbox killed or pausing, owning node unavailable, or the node no longer runs the VM. |
| 502 | `The sandbox is running but port is not open` (with `port`) | Nothing listens on the guest port after the retries, guest TLS failed, or the upstream request failed. |
| 403 | `Sandbox is secured with traffic access token. …` | Missing or invalid `e2b-traffic-access-token`. |
| 409 | `The sandbox is still transitioning. Try again in a moment.` | The sandbox is resuming for longer than 1 minute, or another transition holds it. |
| 429 | team limit message | Auto-resume would exceed the team's concurrent sandbox limit. |
| 404 | `This endpoint is reserved for the E2B control plane …` | Blocked envd path. |
| 500 | `Unexpected error when routing request: …` (plain text) | KV unavailable or a corrupt route. |

## Auto-resume

Traffic to a paused sandbox resumes it when the sandbox was created with auto-resume (`lifecycle={"on_timeout": "pause", "auto_resume": True}` in Python). The rules:

- A sandbox that is `pausing` with auto-resume enabled is resumed once the pause completes.
- While another request is resuming the sandbox, ingress polls every 200 ms for up to 1 minute.
- A paused sandbox without auto-resume, or a killed one, gets 502 `The sandbox was not found`.
- The resumed sandbox gets the timeout remembered at create time, raised to at least 300 s and capped at the team's maximum sandbox length.
- A pause ends every proxied stream to the sandbox (open command or log streams, long downloads). Ingress keeps such a response open, sending nothing, while the sandbox is pausing or paused, and reports the broken stream only once the sandbox runs again, is killed, or after 620 s. Without this, the SDKs react to the failed stream with an envd health check through the ingress, which would auto-resume the sandbox right after it paused.

Resume may move a sandbox to another node when its own node is gone and an object store is configured; see [Storage, object store, and volumes](storage.md). Lifecycle semantics are in [Sandboxes](../concepts/sandboxes.md).
