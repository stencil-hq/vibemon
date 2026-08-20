# Environment Variables

This page lists the current documented `VMON_*` variables used by `vmon serve`, the SDK connection resolvers, and the documented UEFI fixture flow. Variables are partitioned by the process that consumes them; setting a server variable in an SDK process does not reconfigure a remote server.

## Precedence rules

For `vmon serve`, a value is resolved in this order, with each later source overriding the earlier one:

1. built-in `ServeConfig` default;
2. an optional TOML configuration file;
3. the corresponding `VMON_*` environment variable;
4. an explicit `vmon serve` flag.

Select the file with `vmon serve --config PATH`; when that flag is absent, `VMON_CONFIG` selects it. A non-empty `--config` wins over `VMON_CONFIG`. The service rejects unknown configuration keys and non-UTF-8 configured environment values. For the TOML shape and flags, see [Configuration](../platform/configuration.md).

SDK connection selection is different: an absent DSN resolves `VMON_DSN`, then `VMON_CONTEXT`, then that language's default. `VMON_API_TOKEN` is a client credential fallback, not a way to change a remote server's configured token. See [DSN Reference](dsn.md).

## Operator: service location and credentials

These values are read by `vmon serve`. `VMON_HOME` is also used by local clients and contexts for state files.

| Variable | Value and default | Effect |
| --- | --- | --- |
| `VMON_CONFIG` | Path to a TOML file; no default | Chooses the serve configuration file if `--config` is absent. `~` is expanded in this path. |
| `VMON_HOME` | Directory; default `$HOME/.vmon` | Server home/state directory. `~` is expanded. Local Python and Go default UDS paths use `$VMON_HOME/vmond.sock`. |
| `VMON_SERVE_HOST` | Non-empty host; default `127.0.0.1` | TCP bind host. |
| `VMON_SERVE_PORT` | Integer `0`–`65535`; default `8000` | TCP bind port. |
| `VMON_API_TOKEN` | Trimmed token or empty; default unset | Full operator bearer token on the server. It is also the normal client credential variable; a client may supply the scoped client-token value here instead. |
| `VMON_CLIENT_TOKEN` | Trimmed token or empty; default unset | Restricted server-side client bearer token. It permits normal sandbox work but not mesh administration or sandbox migration. |
| `VMON_TLS_CERT` | Certificate path; default unset | TLS certificate path. Configure together with `VMON_TLS_KEY` to serve HTTPS/TLS gRPC. |
| `VMON_TLS_KEY` | Private-key path; default unset | TLS private-key path. Configure together with `VMON_TLS_CERT`. |
| `VMON_FUNCTION_ARTIFACT_MAX_BYTES` | Integer `1` through `2^50`; default `4294967296` (4 GiB) | Maximum accepted size of one streamed function artifact. |
| `VMON_IDLE_TIMEOUT` | Positive seconds; default `300` | Idle sandbox timeout. |

A gateway bound beyond loopback needs an operator token. The operator and client tokens may each be comma-separated rotation lists; any listed value authenticates at its own tier. The client tier remains unable to perform mesh-admin operations and migration. Treat both token variables and TLS key paths as secrets. Do not put a token in a DSN query string, commit it to TOML, or expose it in shell history or logs. See [Security](../platform/security.md).

## Operator: pool, replication, and mesh tuning

All values in this table are consumed by `vmon serve`; they are not SDK client options. Numeric durations are seconds.

| Variable | Accepted value and default | Effect |
| --- | --- | --- |
| `VMON_REPLICATE_SEC` | Non-negative number; default is derived: `60` on a mesh, `0` otherwise | Replica checkpoint cadence. Explicit `0` disables checkpoint replication. |
| `VMON_REPLICAS` | Non-negative integer; default `1` | Desired asynchronous replica count. |
| `VMON_REPLICATE_CONCURRENCY` | Positive integer; default `2` | Maximum simultaneous replica transfers. |
| `VMON_RESTORE_QUORUM` | Boolean: `1`, `true`, `yes`, `on`, `0`, `false`, `no`, or `off`; default is derived | Overrides automatic restore quorum. Without an override, it is enabled when expected mesh membership is at least three. |
| `VMON_WARM_POOL_SIZE` | Non-negative integer; default `1` | Default number of ready instances for each warm image. |
| `VMON_WARM_IMAGES` | Comma- or whitespace-separated image entries; default empty | Images to prewarm. An entry may be `reference=count` or `reference:count` when the suffix is an integer; otherwise it uses `VMON_WARM_POOL_SIZE`. |
| `VMON_MESH_HEARTBEAT_SEC` | Positive number; default `3` | Mesh heartbeat interval. |
| `VMON_MESH_REAP_SEC` | Positive number; default `300` | Age at which a mesh member is reaped. |
| `VMON_MESH_IDEM_TTL_SEC` | Positive number; default `900` | Mesh idempotency-cache TTL. |
| `VMON_MESH_CREATE_TIMEOUT_SEC` | Positive number; default `120` | Mesh create-dispatch timeout. |
| `VMON_MESH_W_WARM` | Positive number; default `1000` | Placement score weight for warm capacity. |
| `VMON_MESH_W_FREE` | Positive number; default `100` | Placement score weight for free capacity. |
| `VMON_MESH_W_LOCAL` | Positive number; default `50` | Placement score weight for local-node bias. |
| `VMON_MESH_W_REGION` | Positive number; default `30` | Placement score weight for region affinity. |
| `VMON_MESH_W_INFLIGHT` | Positive number; default `80` | Placement penalty for in-flight creates. |

Use the default mesh settings unless there is an observed operational reason to tune them. Disabling replication changes guest-state protection, not the durable-create record behavior. Quorum changes affect automatic restore safety; see [Mesh and High Availability](../platform/mesh.md).

## Client: endpoint and context selection

These variables are read by the Python, Go, and TypeScript DSN resolvers. They do not start a daemon or set its bind address.

| Variable | Consumer and precedence | Effect |
| --- | --- | --- |
| `VMON_DSN` | Python/Go/TypeScript, after an explicit DSN | Connection string for the gateway roster and policy. |
| `VMON_CONTEXT` | Python/Go/TypeScript, after `VMON_DSN` when no DSN is supplied | Selects a saved named context. It also overrides the CLI's saved active-context selection when non-empty. |
| `VMON_API_TOKEN` | Python/Go/TypeScript, after a DSN token; TypeScript also checks it before a saved context token | Bearer credential for the selected gateway. Supply the scoped server `VMON_CLIENT_TOKEN` value here when clients should not have operator access. |
| `VMON_HOME` | Local Python/Go and context storage | Determines local state, including the default socket path and saved context/credential location. |

Python and Go default to `vmon+unix://$VMON_HOME/vmond.sock`; TypeScript instead defaults to an HTTP(S) browser origin or `http://127.0.0.1:8000`. TypeScript browser builds do not support UDS. The exact DSN grammar, query parameter precedence, and context-token order are in the [DSN Reference](dsn.md).

## Cloud image and registry credentials

These variables are consumed by cloud disk publication, lazy remote rootfs
reads, and automatic private-registry authentication. They are not persisted
in image sidecars or passed as command-line arguments.

| Variable | Effect |
| --- | --- |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Standard static or temporary AWS credentials; used before the workload metadata chain. |
| `VMON_S3_ACCESS_KEY` / `VMON_S3_SECRET_KEY` | S3-compatible static credentials when standard AWS names are unavailable. |
| `AWS_SESSION_TOKEN` / `VMON_S3_SESSION_TOKEN` | Optional session token paired with the corresponding credential names. |
| `AWS_REGION` / `AWS_DEFAULT_REGION` / `VMON_S3_REGION` | AWS signing region; metadata identity is used when none is set. |
| `AWS_ENDPOINT_URL_S3` / `VMON_S3_ENDPOINT` | Optional S3-compatible endpoint; requests use path-style addressing. It does not relax lazy-rootfs identity checks: objects still require a non-null version ID and a strong quoted ETag. |
| `GCE_METADATA_HOST` | Optional Google metadata host override for workload OAuth tokens; remote token-bearing requests are restricted to provider-trusted HTTPS GCS object endpoints. |
| `VMON_REGISTRY_AUTH_FILE` | Operator-managed Docker auth file passed to `skopeo`; otherwise GCR/Artifact Registry and ECR use short-lived provider tokens. |

AWS lazy rootfs requires S3 bucket versioning and object metadata containing
both a non-null `x-amz-version-id` and a strong quoted ETag. Enabling a generic
S3-compatible endpoint does not waive either requirement. GCS lazy rootfs
requires both a positive object generation and an ETag. Dynamic AWS workload
credentials and GCS metadata OAuth tokens are sent only to provider-trusted
HTTPS object endpoints; configure credentials explicitly for other endpoints.

## Development and test fixtures

These documented variables are for explicitly supplied UEFI assets or the optional fixture-download script. They are not `vmon serve` configuration and are not sent to a client or guest automatically.

| Variable | Use |
| --- | --- |
| `VMON_AARCH64_UEFI` | Convenience shell variable naming an operator-supplied, host-architecture-compatible `QEMU_EFI.fd` path for an aarch64 UEFI boot command. |
| `VMON_X86_UEFI` | Convenience shell variable naming an operator-supplied `OVMF_CODE.fd`/EDK2 firmware path for an x86_64 UEFI boot command. |
| `VMON_UEFI_IMAGES` | Set to `1` when running `demo/fetch-test-assets.sh` to also download the pinned Ubuntu focal cloud images; without it, the script fetches the pinned firmware assets only. |

UEFI firmware is operator supplied; vmon does not vendor firmware blobs. These variables only make a path or optional script branch convenient, so pass the resulting firmware path explicitly to `vmon vmm --boot-mode uefi --firmware ...`.
