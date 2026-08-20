# Command line

The `vmon` executable is the normal Rust control-plane client. It talks to the local daemon or a saved remote context through the `vmon.v1` gRPC API. It also contains `vmon serve` and the direct `vmon vmm` escape hatch; those are different operating modes, not replacements for ordinary sandbox management.

## Global transport selection

Put transport options before the subcommand:

```sh
vmon --context production ps
vmon --context production --token "$VMON_API_TOKEN" run alpine -- echo hello
vmon --context local ps
```

`--context NAME` selects a saved remote context; `local` selects the local Unix-domain-socket daemon. `--token TOKEN` supplies the bearer token for a selected remote context. For a saved context, token lookup is: explicit `--token`, then `VMON_API_TOKEN`, then that context's saved token. `VMON_CONTEXT` overrides the saved active-context selection when it is non-empty.

## Everyday sandbox commands

The primary commands are:

- `vmon run [IMAGE] [CMD...]` boots an image, streams its output unless `--detach` is used, and accepts `--name`, `--mem`, `--cpus`, `--disk-mb`, `--timeout`, `--block-network`, and `--arch`.
- `vmon ps`, `logs NAME [-f]`, `stop NAME`, `rm NAME`, `pause NAME`, `resume NAME`, `suspend NAME`, `history NAME`, `rollback NAME POINT`, `extend NAME SECS`, and `stats NAME` inspect or change a sandbox. Pause retains the live VM; suspend releases it only after committing a durable checkpoint.
- `vmon exec NAME [CMD...]` runs a command in an agent-enabled sandbox; add `--tty` for a PTY. `vmon shell` opens or attaches a shell and can create a fresh shell with `--image`; `-e KEY=VALUE` sets a variable and bare `-e KEY` copies it from the host.
- `vmon cp SRC DST` transfers a file between the host and guest. `vmon fs list NAME[:PATH]` and `vmon fs stat NAME[:PATH]` inspect guest paths.
- `vmon snapshot NAME SNAPSHOT [--stop]`, `vmon restore SNAPSHOT [--name NAME] [--agent] [--detach] [--arch ...]`, and `vmon fork SNAPSHOT [--count N] [--arch ...]` operate managed snapshots. See [Snapshots, Restore, and Fork](snapshots.md).
- `vmon volume ls|rm NAME` and `vmon pool ls|set|rm` operate managed volumes and warm pools. See [Storage and Volumes](storage.md).

`vmon deploy TARGET` packages, registers, and activates a durable application. `vmon function ls|shell REF` handles function revisions; `vmon call get ID` retrieves a durable call and `vmon call logs ID --follow` follows its reconnectable logs.

`vmon image publish-rootfs gs://bucket/export.tar.gz` or `vmon image publish-rootfs s3://bucket/export.raw` converts a cloud disk export into a lazily consumed rootfs artifact. See [OCI Images](images.md#cloud-disk-exports) for supported formats and workload-identity requirements.

## Contexts

A context records an endpoint roster, optional region, and update time in the state directory's `contexts.json`. Add one from a gateway URL (a scheme is optional and defaults to `http://`):

```sh
export VMON_API_TOKEN=<operator-token>
vmon context add production --server https://gateway.example --region eu-west
vmon context use production
vmon --context production ps
```

`vmon context create` is an alias for `add`. Add `--token TOKEN --save-token` only when the token should be persisted. Saved tokens live at `credentials/NAME.token` beneath the context file's directory; the credentials directory and file are created with modes `0700` and `0600`, respectively. Without `--save-token`, provide the credential through `VMON_API_TOKEN` or `--token`.

Use `vmon context ls`, `vmon context use NAME`, and `vmon context rm NAME`; `vmon context use local` returns to the local daemon. Removing a context also removes its saved token. A nonexistent selected context is an error.

## Mesh, server, and diagnostics

`vmon mesh setup [--advertise URL] [--region REGION] [--max-vcpus N] [--max-mem-mib N]` initializes a node and prints a join blob. Use `vmon mesh join BLOB [--advertise URL] [--region REGION]`, `vmon mesh status`, and `vmon mesh leave [--drain]` for cluster lifecycle. See [Mesh and High Availability](mesh.md).

`vmon serve` starts the Rust gateway; its configuration is documented in [Server Operation](server.md) and [Configuration](configuration.md). `vmon daemon status` reports local daemon health and `vmon daemon stop` sends it `SIGTERM`. `vmon doctor` checks local prerequisites; `vmon doctor --serve --config PATH` also resolves and validates server configuration.

## Shell completion

Generate a completion script and load it in the current shell:

```sh
# bash or zsh
eval "$(vmon completion zsh)"

# fish
vmon completion fish | source
```

The supported completion targets are `bash`, `zsh`, and `fish`.

## Direct monitor boundary

Do not use `vmon vmm` to manage sandboxes that the server owns. The server creates each monitor with its own control socket and maintains the registry, lifecycle, image, volume, and gRPC state above it. `vmon vmm` is for an operator launching one monitor directly; its flags and newline-delimited JSON control socket are documented in [Low-Level VMM](low-level-vmm.md).
