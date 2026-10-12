# Authentication and teams

Every credential vibemon accepts, where it is accepted, and how it is derived or stored, plus the team model (members, limits, blocking, system teams) that the credentials resolve to. Read this before you issue keys or expose the API.

## Credentials

| Credential | Sent as | Accepted by | Issued by |
|---|---|---|---|
| Team API key | `X-API-Key: e2b_…` | Every team route of the E2B API, except `/api-keys` | `vmon admin create-team` / `create-api-key`, `POST /api-keys`, `POST /admin/teams/{teamID}/api-keys`, `GET /teams` (CLI login) |
| User access token (JWT) | `Authorization: Bearer <jwt>`, optional `X-Team-ID` | Team routes (as the member of a team), `GET /teams`, `/api-keys` | CLI login (`GET`/`POST /docs/api/cli`), `POST /auth/token` |
| Admin token | `X-Admin-Token: <token>` | `/nodes*`, `/admin/*`; team routes together with `X-Team-ID` | The `--admin-token` flag (`VMON_ADMIN_TOKEN`) of each driver |
| envd access token | `X-Access-Token: <token>` | envd inside the sandbox; on the driver only `GET /sandboxes/{id}`, `POST /sandboxes/{id}/timeout`, `POST /sandboxes/{id}/refreshes` of that sandbox | `envdAccessToken` in sandbox create/connect/detail responses |
| Traffic access token | `e2b-traffic-access-token: <token>` | Sandbox ingress, non-envd ports of sandboxes created with `allowPublicTraffic: false` | `trafficAccessToken` in sandbox responses |
| Volume token (JWT) | `Authorization: Bearer <jwt>` | `/volumecontent/{volumeID}/…` for that one volume | `token` in `POST /volumes` and `GET /volumes/{volumeID}` responses |

Team API errors normally use the E2B body `{"code": <status>, "message": "…"}`. The volume-content API uses string error codes, and OAuth token errors use `error` and `error_description`. Team routes without credentials get 401 `authorization header is missing`.

## Team routes and precedence

Team-scoped routes resolve the caller to exactly one team. The driver checks the headers in this order and uses the first one present; a recognized but invalid credential fails the request instead of falling through to the next. Empty headers and an `Authorization` value without a non-empty Bearer token count as absent:

1. `X-API-Key`: the key's team.
2. `Authorization: Bearer <access token>`: the team named by `X-Team-ID` (a team ID the user is a member of), or the user's default team when `X-Team-ID` is absent. A team the user is not a member of is 401.
3. `X-Admin-Token`: requires `X-Team-ID` with a team ID (not a slug). 401 `X-Team-ID header is required with X-Admin-Token` without it.
4. `X-Access-Token`: only on the three sandbox routes listed above, only for the sandbox whose ID is in the path.

Some routes narrow this further. `/api-keys` refuses `X-API-Key`: listing, renaming, and deleting keys take a user token or the admin token (with `X-Team-ID`); creating a key through `POST /api-keys` takes a user token only. `GET /teams` takes a user token only.

## Team API keys

Generated keys are `e2b_` followed by 40 lowercase hex characters. Keys without the `e2b_` prefix are rejected before any lookup with the upstream "API key is malformed" message.

The driver stores each key twice in MySQL (`api_keys`):

- `key_hash`: SHA-256 of the hex-decoded key bytes, used for lookups.
- `key_enc`: the key encrypted with AES-256-GCM under the data key derived from the cluster secret (see [Key material](#key-material)). This is what lets `GET /teams` hand the CLI a usable key.

Only the first 2 and last 4 characters after `e2b_` are kept in clear (`mask` in `GET /api-keys`). The plaintext is returned on creation and can also be returned by `GET /teams`; `/api-keys` listings return the mask.

Each driver caches the key → team lookup for 30 seconds. Deleting a key through the API evicts it from the cache of the driver that served the delete; other drivers may accept it until their cache entry expires. The same 30-second window applies to block and limit changes for callers that authenticate with an API key.

## Users and CLI login

Users exist for the `e2b` CLI login flow and the `/api-keys` routes. Operators create them with [`vmon admin create-user`](../reference/admin.md#create-user); passwords are stored as Argon2id hashes. There is no self-signup.

`e2b auth login` opens `GET /docs/api/cli`, served on any Host that routes to the API rather than to a sandbox. The form posts email and password; on success the driver redirects to the CLI's loopback callback (`http://localhost`, `127.0.0.1`, or `[::1]` only) with:

| Parameter | Value |
|---|---|
| `accessToken` | HS256 JWT, audience `vmon-access`, issuer `vmon`, valid 1 hour |
| `refreshToken` | 64 hex characters, valid 30 days, stored as SHA-256 |
| `tokenEndpoint`, `revokeEndpoint` | `<public URL>/auth/token`, `<public URL>/auth/revoke` |
| `clientId` | `vmon-cli` |
| `email` | The user's email |

- `POST /auth/token` (form, `grant_type=refresh_token`) returns a new access token and a new refresh token. Every refresh rotates the refresh token; presenting an already rotated token revokes all of the user's refresh tokens and returns `invalid_grant`.
- `POST /auth/revoke` (form, `token=<refresh token>`) revokes that refresh token and always answers 200.
- `GET /teams` with the access token lists the user's teams with `isDefault` and an `apiKey` per team: the newest key that user created in the team, or a new key named `CLI (<email>)` minted when no decryptable user-owned key is available. The CLI stores that key and uses it as `X-API-Key`.
- `/access-tokens` answers 410 with the upstream deprecation message; `E2B_ACCESS_TOKEN` is not supported.

Client configuration for the login is in [Connecting SDKs and the CLI](../get-started/clients.md).

## Admin token

The admin token is the value of `--admin-token` (`VMON_ADMIN_TOKEN`) on a driver, compared in constant time against `X-Admin-Token`. It is not stored in MySQL, and each driver checks its own flag, so set the same value on every driver. Without `--admin-token` the admin routes reject every request with 401.

The admin routes are served on the public listener next to the E2B API. Operator workflows and `curl` examples are in [Administration](../operate/admin.md#admin-http-api).

## envd access token

Every sandbox has an envd access token: `hex(HMAC-SHA256(seed, sandboxID))` with `seed = HKDF(cluster secret, "envd-token")`. It is deterministic, so paused and resumed sandboxes keep it, and it is always set (the deprecated `secure` create flag is ignored). The driver delivers it to the guest through envd `/init`; the SDKs read it from `envdAccessToken` and send it as `X-Access-Token`.

Inside the guest, envd requires `X-Access-Token` on every route except `GET /health`, `GET`/`POST /files`, and `POST /init`. `/files` accepts either the header or an SDK-style URL signature (`signature`, optional `signature_expiration`) computed from the token. The ingress answers 404 for envd's control paths (`/init` and the pause hooks) on port 49983, so `/init` is not reachable from outside.

**Sandbox self-keepalive (vibemon extension).** The same token also authenticates three driver routes, for the sandbox that owns it only:

| Route | Effect |
|---|---|
| `GET /sandboxes/{sandboxID}` | Sandbox detail |
| `POST /sandboxes/{sandboxID}/timeout` | Set the expiry |
| `POST /sandboxes/{sandboxID}/refreshes` | Extend the expiry |

The token is compared in constant time before the sandbox is looked up; wrong tokens, killed sandboxes, and unknown sandboxes all get 401. Team blocking and the team's maximum sandbox length still apply. It grants nothing else. Upstream E2B has no equivalent, so the SDKs do not use it; call it with raw HTTP from inside the sandbox:

```sh
# API: the driver's API URL; SBX: the sandbox ID; TOKEN: its envdAccessToken
curl -sX POST "$API/sandboxes/$SBX/timeout" \
  -H "X-Access-Token: $TOKEN" -H 'Content-Type: application/json' \
  -d '{"timeout": 600}'
```

## Traffic access token

Sandboxes created with `allowPublicTraffic: false` get a traffic access token, `hex(HMAC-SHA256(seed, "sandbox-traffic-" + sandboxID))` with the same seed as the envd token, returned as `trafficAccessToken`. The ingress then requires `e2b-traffic-access-token` on every port except envd's 49983. Routing and the related options are covered in [Domains, TLS, and ingress](../operate/ingress.md#traffic-access-tokens).

## Volume tokens

`POST /volumes` and `GET /volumes/{volumeID}` return a volume token: an HS256 JWT with audience `vmon-volume`, scoped to one volume of one team, valid 24 hours. The volume-content API (`/volumecontent/{volumeID}/path|dir|file`) takes it as `Authorization: Bearer`; a missing or expired token is 401, a token for another volume is 403. Volumes themselves are covered in [Storage, object store, and volumes](../operate/storage.md).

## Key material

All derived secrets come from the shared cluster secret (`--cluster-secret`, `VMON_CLUSTER_SECRET`; generated into `<data-dir>/cluster-secret` when unset, see [Running vmon serve](../operate/serve.md#cluster-secret)) through HKDF-SHA256 with a fixed label:

| Label | Used for |
|---|---|
| `envd-token` | envd and traffic access tokens (HMAC-SHA256 seed) |
| `jwt` | HS256 key of user access tokens and volume tokens |
| `data-key` | AES-256-GCM key for stored API keys, sandbox env vars, secret values, and webhook secrets (ciphertext = 12-byte nonce ‖ AES-GCM output) |
| `upload-url` | HMAC key of signed template file upload URLs (`expires=<unix>&signature=<hex>`, valid 30 minutes) |

The internal cluster listener and gossip are authenticated with the cluster secret as well ([Clusters](../operate/cluster.md)).

> [!WARNING]
> Changing the cluster secret makes every stored ciphertext undecryptable and changes every derived token: envd and traffic tokens of existing sandboxes, outstanding JWTs, and signed upload URLs. API key lookups use the unencrypted hash and keep working. `vmon admin` encrypts the keys it mints with the secret it is given, so it must use the drivers' secret.

## Teams

A team is the tenant every API key, template, sandbox, volume, secret, and webhook belongs to. Teams are created with [`vmon admin create-team`](../reference/admin.md#create-team); there is no API route that creates one.

| Field | Meaning |
|---|---|
| ID | UUID v4. Used in `X-Team-ID` and in `/admin/teams/{teamID}/…`. |
| Name | Free text given at creation. |
| Slug | Unique, lowercase `[a-z0-9-]` form of the name (`My Team!` → `my-team`; `team` when nothing is left), suffixed `-2`, `-3`, … on collision. It is the team's template namespace (`<slug>/<alias>`). `vmon admin` accepts the ID or the slug wherever it takes a team. |
| Limits | See below. |
| Blocked | See [Blocking](#blocking). |
| System | See [System teams](#system-teams). |

### Members and the default team

Users join teams through `vmon admin create-user --team` or `add-member`. A membership can be the user's default team; making one default clears the flag on the user's other memberships. When none is flagged, the oldest membership counts as the default. Bearer requests without `X-Team-ID`, and the CLI's team pick after login, use the default team.

### Limits

Limits are stored on the team row and enforced by the driver:

| Limit | Default | Enforcement |
|---|---|---|
| `max_concurrent_sandboxes` | 100 | Running sandboxes per team, checked atomically at admission on create, resume, and auto-resume: 429 at the limit. A fork's `count` must be lower than this value. |
| `max_concurrent_builds` | 20 | Concurrent template builds: 429 at the limit. |
| `max_vcpus` | 8 | Per-sandbox and per-build vCPU ceiling: 400 when a template or build request exceeds it. |
| `max_memory_mb` | 8192 | Per-sandbox and per-build memory ceiling (MiB): 400 beyond. |
| `max_disk_mb` | 20480 | Per-sandbox disk ceiling (MiB): 400 beyond. Template builds also get a disk of this size. |
| `max_length_hours` | 24 | Maximum lifetime per execution: a create, resume, or connect timeout longer than this is 400 `Timeout cannot be greater than <n> hours`; timeout changes and extensions are capped at start + this length. |

Set them at creation or later with [`vmon admin set-limits`](../reference/admin.md#set-limits).

### Blocking

On team-authenticated API routes, a blocked team keeps read access but every mutating request (any method other than `GET`, `HEAD`, `OPTIONS`, `TRACE`) gets 403 `team is blocked`, whichever credential authenticated it. Running sandboxes are not stopped, and existing volume-content tokens are not checked against the block flag. Block and unblock with [`vmon admin block` / `unblock`](../reference/admin.md#block).

### System teams

The SDKs default to the bare aliases `base`, `code-interpreter-v1`, `desktop`, and `mcp-gateway`. In vibemon only public templates of a system team hold bare aliases: publishing such a template claims each of its names that no other template holds as a bare alias, and every team resolves an unqualified reference as a template ID first if it has the ID shape, then in its own namespace, then among bare aliases. Ordinary teams' public templates are addressed as `<slug>/<alias>`. Mark a team with `vmon admin create-team --system` or `set-system`; `unset-system` releases its bare aliases. The build pipeline for the default templates is in [Templates](templates.md).
