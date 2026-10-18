# Templates

A template is what every sandbox starts from: a root filesystem and, normally, a memory snapshot of a booted VM with any configured start command already running. This page covers how vibemon builds templates from OCI images, how you define them with the E2B SDK or CLI, the default templates, naming and publishing, and when artifacts are deleted.

## What a template is

| Object | Shape | Holds |
|---|---|---|
| Template | ID of 20 `[a-z0-9]` characters | Owner team, `public` flag, aliases, tags |
| Build | UUID, status `waiting` → `building` → `ready` or `error` | vCPUs, memory, disk, start and ready commands, envd defaults (user, workdir, env), and the artifacts |
| Artifacts | `builds/<buildID>/` on a node | `rootfs.ext4`, the optional memory snapshot `snapshot/`, and `meta.json` |

A sandbox create normally forks the snapshot copy-on-write, so the start command's processes are already up, and boots over a block overlay of the rootfs. See [Sandboxes](sandboxes.md#create). Other nodes fetch artifacts on first use from a node that holds them, transferring only allocated extents, or from the object store when builds are published there. See [Clusters](../operate/cluster.md#artifacts-and-portability) and [Published templates](../operate/storage.md#published-templates).

A snapshot is reused only on a node whose architecture, hypervisor backend, and CPU baseline match the builder's. Any other node of the same architecture regenerates the snapshot locally from the rootfs: it boots the rootfs and reruns the start and ready commands before the first sandbox starts.

## Build pipeline

An uncached `fromImage` build runs on one node, in VMs, through these phases. `fromTemplate` instead clones the parent rootfs and skips unpacking and provisioning; cache hits also skip completed layers:

1. **Pull.** For `fromImage`, the node's own OCI registry client (Docker Registry v2 / OCI distribution, no Docker or skopeo) resolves the reference. It runs the bearer-token flow, picks the `linux/<arch>` manifest for the node's architecture, and downloads layers into the shared cache `cache/oci/blobs/sha256/`, verifying their digests. Tags are resolved with `HEAD`, which Docker Hub does not count against pull limits, and manifests are cached by digest. Interrupted layer downloads resume with HTTP `Range`. A registry 429 is retried for up to 6 attempts when `Retry-After` is absent or a numeric value of at most 60 s; other values fail immediately. Errors include the registry message and `Retry-After` when present. For `fromTemplate`, the node fetches the parent build's artifacts instead.
2. **Unpack.** The node creates an empty sparse ext4 disk of the build's disk size. It boots a VM with the agent as `/init`, sharing the layers read-only over virtio-fs. The agent applies the layers inside the guest, so ownership, setuid bits, xattrs, device nodes, and whiteouts are handled by the guest kernel, never the host. It installs the agent as `/usr/bin/envd` and sets the hostname to `vibemon`.
3. **Provision.** The rootfs boots with the agent as PID 1. The distro is detected from `/etc/os-release` and provisioned with upstream E2B's script and package set:

   | `ID` | Init | Package manager |
   |---|---|---|
   | `debian`, `ubuntu` | systemd | apt |
   | `fedora`, `centos`, `rocky`, `almalinux` | systemd | dnf / microdnf / yum |
   | `arch`, `archarm` | systemd | pacman |
   | `alpine` | OpenRC (busybox init) | apk |

   Other IDs fall back to a matching `ID_LIKE` entry, best effort and with a warning. Profiles install the init system, sudo, chrony, OpenSSH, curl, git, iptables/nftables, and related tools, and register the agent as the `envd` service. If neither `ID` nor `ID_LIKE` matches a profile, provisioning selects the agent as PID 1 and skips package setup.

   apt runs `apt-get update` before installing. If the install fails and the image's apt sources use `deb.debian.org` or `security.debian.org`, provisioning switches those sources to `archive.debian.org` and runs `apt-get update` and the install once more, with a warning in the build log. Debian moves end-of-life releases to the archive, while the default hosts can keep serving indexes for packages they no longer carry. Debian 11 after its August 2026 end of life fails with `404 Not Found` on `bullseye-security` packages, for example. The switched sources stay in the template, so its own `apt-get` steps use the archive too.
4. **Steps.** In the same VM, with networking and the guest clock set, the template's steps run against the build's file context mounted read-only. Unless the client is an E2B SDK older than 2.3.0 (template version 2.0, selected from the `User-Agent`), a first layer creates the user `user` with passwordless sudo and makes it the default user.
5. **Finalize.** A fresh VM boots the rootfs with the distro init, networking, and the build's vCPUs and memory. Upstream's `configure.sh` creates `/.e2b`, user `user` with sudo, and `/code`. envd is initialized, the start command runs in the background, and the ready command is polled. The node then syncs the filesystem and snapshots the VM.

A node runs at most `--max-builds` builds at once (default 2). A build that runs longer than 1 hour fails with `build status polling timed out. Maximum build time is 1h0m0s.` A build also fails if its builder node dies. The build's disk is the team's `max_disk_mb`.

### Registry authentication and mirror

| SDK call | `fromImageRegistry` | Credentials used for |
|---|---|---|
| `from_image(image, username=…, password=…)` | `{type: "registry", username, password}` | Any registry (Docker Hub, GHCR, private) |
| `from_aws_registry(image, access_key_id=…, secret_access_key=…, region=…)` | `{type: "aws", …}` | ECR, via `GetAuthorizationToken` |
| `from_gcp_registry(image, service_account_json=…)` | `{type: "gcp", serviceAccountJson}` | GCR / Artifact Registry, via service-account OAuth |

Credentials are not stored with the build spec. `vmon serve --registry-mirror https://mirror.gcr.io` (`VMON_REGISTRY_MIRROR`) is tried first for anonymous `docker.io` pulls, with Docker Hub as the fallback. Credentials are never sent to the mirror. See [vmon serve options](../reference/serve.md#templates-and-registries).

## Steps and the step cache

The SDK lowers its builder calls to v2 steps. The driver validates them:

| Step | Args | Notes |
|---|---|---|
| `RUN` | `command, user?` | `bash -l -c` when `/bin/bash` exists, else `sh -c`, as the step user |
| `COPY` (`ADD`) | `src, dest, owner?, mode?` | Needs a `filesHash` of an uploaded context archive. `mode` is 3–4 octal digits |
| `ENV` (`ARG`) | `key, value, …` | Recorded into the template's envd environment |
| `WORKDIR` | `path` | Becomes the default workdir |
| `USER` | `user, sudo?` | Creates the user if missing and becomes the default user |

Other step types fail with 400 `command type … is not implemented`. Context archives are uploaded before the build starts. The client asks `GET /templates/{id}/files/{hash}`, then uploads to the signed URL it returns (valid 30 minutes) if the archive is not yet `present`. The driver's `--public-url` must be reachable by the client for this.

Every layer has a cache key chained by SHA-256. The base key covers the image digest (or the parent build), disk size, provisioning script, and agent binary. Each step's key covers the previous key plus the step's type, args, and files hash. A rebuild resumes from the deepest cached layer. `force` on a step, or `skip_cache` (Python SDK), `skipCache` (JS SDK), or `--no-cache` (`e2b template create`) for the whole template, rebuilds that layer and every layer after it. The cache is per node (`cache/steps/`). It keeps at most 48 layers and at most `--step-cache-mb` of disk (default 10% of the data directory's filesystem), evicting the least recently used layers first. Sizes count allocated blocks, so a sparse 20 GiB rootfs counts only its data; a layer larger than the whole budget is not kept.

## Start and ready commands

- The start command runs in the background during finalize, and the snapshot captures it running.
- The ready command is polled every 2 s until it exits 0, for at most 10 minutes. Without a ready command, a template with a start command waits `sleep 20`.
- A `fromTemplate` build that sets neither command inherits the parent's pair.

## Building with the SDK

```python
from e2b import Template, default_build_logger, wait_for_port

template = (
    Template()
    .from_image("python:3.13")
    .apt_install(["curl"])
    .copy("requirements.txt", "/home/user/")
    .run_cmd("pip install -r /home/user/requirements.txt")
    .set_workdir("/home/user")
    .set_start_cmd("python -m http.server 8000", wait_for_port(8000))
)

info = Template.build(template, "my-server:v1", cpu_count=2, memory_mb=1024,
                      on_build_logs=default_build_logger())
print(info.template_id, info.build_id)
```

```ts
import { Template, defaultBuildLogger, waitForPort } from 'e2b'

const template = Template()
  .fromImage('python:3.13')
  .setStartCmd('python -m http.server 8000', waitForPort(8000))

await Template.build(template, 'my-server:v1', { cpuCount: 2, memoryMB: 1024, onBuildLogs: defaultBuildLogger() })
```

`Template.build` calls `POST /v3/templates` (202, build `waiting`), uploads file contexts, calls `POST /v2/templates/{id}/builds/{buildID}` (202), and polls status. Resource rules:

| Field | Default | Rule |
|---|---|---|
| `cpuCount` | 2 | 1 or even, ≤ 32, ≤ team `max_vcpus` |
| `memoryMB` | 1024 | even, ≥ 128, ≤ team `max_memory_mb` |
| `minFreeDiskMb` | — | 0 … team `max_disk_mb`; validated and recorded only (see below) |

Every build disk is a sparse ext4 image of the team's `max_disk_mb`, so the free space after the steps is that size minus what the steps wrote, whatever `minFreeDiskMb` asks for. Upstream grows the filesystem to the requested free space instead; here it is already as large as the team allows.

The team's concurrent-build limit answers 429. Starting a build cancels the template's other running builds that target an overlapping tag.

The builder node must have the parent's architecture for `fromTemplate`. Otherwise the driver uses the cluster's majority architecture (ties prefer `x86_64`), and the node with the most free memory and vCPUs, minus running builds, wins. A template is therefore built for one architecture, and its sandboxes run only on nodes of that architecture. See [Placement](../operate/cluster.md#placement).

## Building from a Dockerfile with the CLI

```sh
e2b template create my-image -d Dockerfile -c "/start.sh" --ready-cmd "test -f /tmp/ready" \
  --cpu-count 2 --memory-mb 2048
```

The CLI converts the Dockerfile into v2 steps (`FROM`, `ENV`, `RUN`, `WORKDIR`, `COPY` with file upload, `USER`) and builds it like the SDK. `e2b template init` and `e2b template migrate` generate SDK build scripts that build against vibemon too. The legacy `e2b template build` (v1 API) is not served. See [E2B compatibility](../reference/e2b-compat.md#6-e2b-cli-e2bcli-2212).

## Build status and logs

| Endpoint | Parameters | Returns |
|---|---|---|
| `GET /templates/{id}/builds/{buildID}/status` | `logsOffset`, `limit` (≤ 100), `level` | `status`, `logEntries`, and for errors `reason {message, step, logEntries}` |
| `GET /templates/{id}/builds/{buildID}/logs` | `cursor` (ms), `limit` (≤ 100), `direction`, `level`, `source` | `logs[]` |

The supervising driver copies the builder's log into the driver store (the raft-replicated journal) every 500 ms, so logs outlive the builder; they are kept for 30 days. `Template.get_build_status(...)` and `on_build_logs` read these endpoints.

## Names, aliases, and tags

- A name is `[namespace/]alias[:tag]`. Aliases and namespaces match `^[a-z0-9-_]+$`, tags match `^[a-z0-9-_.]+$` and cannot be UUIDs, and names are lowercased. `default` means no tag.
- A build's alias is registered in the team's namespace, its slug. The template's `names` are `<slug>/<alias>`. Passing a namespace other than your slug fails with 400.
- A build gets the tag from its name plus any `tags`, or `default`. Tags move to the build when it becomes `ready`. `POST /templates/tags` `{target: "name:tag", tags}` points more tags at an existing build. `DELETE /templates/tags` removes them, and `GET /templates/{id}/tags` lists them.
- A reference without a tag resolves the `default` tag, falling back to the latest ready build. An explicit tag that does not exist answers 404.
- `GET /templates/aliases/{alias}` answers 200 for your template, 403 for another team's, and 404 otherwise.

### Resolution order

An unqualified reference such as `base` is looked up in this order:

1. As a template ID, if it has the ID shape.
2. In the caller's namespace (`<slug>/base`).
3. As a bare alias (`base`), held only by public templates of system teams.

A reference with an explicit namespace is looked up only there. Another team's template resolves only if it is public. A team's own `base` therefore shadows the default `base`.

### Publishing and system teams

```sh
e2b template publish my-image -y      # PATCH /v2/templates/{id} {"public": true}
e2b template unpublish my-image -y
```

A public template is usable by every team as `<slug>/<alias>`, for sandboxes and as a `fromTemplate` parent. After unpublishing, other teams get 403.

Bare aliases are reserved for system teams (`vmon admin create-team --system`, `set-system`, `unset-system`). When a system team publishes a template, each of the template's aliases also claims the free bare alias. Unpublishing, deleting the template, or unsetting the system flag releases them, and setting the flag claims them for the team's already-public templates. An ordinary team's publish only toggles `public`. Reserving bare aliases keeps `base` from being claimed by an ordinary team. See [System teams](auth.md#system-teams) and [Administration](../operate/admin.md#system-team-for-the-default-templates).

## Default templates

The SDKs default to bare aliases: `Sandbox` uses `base`, the code-interpreter SDK uses `code-interpreter-v1`, the desktop SDK uses `desktop`, and `Sandbox.create(mcp=…)` uses `mcp-gateway`. [`templates/`](https://github.com/stencil-hq/vibemon/tree/main/templates) holds their definitions as E2B v2 build scripts:

| Alias | Directory | Base | vCPUs / MiB |
|---|---|---|---|
| `base` | `base/` | `e2bdev/base` (`from_base_image()`), nothing added | 2 / 1024 |
| `code-interpreter-v1` | `code-interpreter/` | Upstream e2b-dev/code-interpreter template on `python:3.13` | 2 / 2048 |
| `desktop` | `desktop/` | Upstream e2b-dev/desktop template on `ubuntu:22.04` | 8 / 8192 |
| `mcp-gateway` | `mcp-gateway/` | `e2bdev/base` + Docker engine + docker/mcp-gateway + vibemon's gateway (`MIN_FREE_DISK_MB` 8192) | 2 / 2048 |
| `ubuntu-systemd` | `ubuntu-systemd/` | `ubuntu:24.04` with systemd as PID 1, a probe unit, and a heartbeat start command; vibemon's reference `fromImage` template | 2 / 1024 |

The definitions pick architecture-specific packages at build time, so they build on aarch64 and x86_64 guests. JDK 11 is OpenJDK on x64 and Temurin on aarch64. Desktop uses Google Chrome on amd64 and Chromium as `google-chrome-stable` on arm64. `mcp-gateway` runs Docker in the guest and needs the [vmon guest kernel](../operate/kernel.md). Provenance and licenses: [`templates/SOURCES.md`](https://github.com/stencil-hq/vibemon/blob/main/templates/SOURCES.md).

Sandboxes take the template's resources, so a team needs limits of at least 8 vCPUs and 8192 MiB to start `desktop`. Those are the default team limits.

Build and publish them against a running server:

```sh
just templates                       # all five, as the system team
just templates -j 2 base desktop     # selected aliases, 2 concurrent builds
uv run templates/build.py --skip-cache code-interpreter-v1
```

`templates/build.py` options:

| Option | Default | Meaning |
|---|---|---|
| `NAME …` | every directory present | Aliases to build |
| `--team` | `system` | Slug of the system team to build as |
| `--jobs`, `-j` | 1 | Concurrent builds |
| `--skip-cache` | off | Rebuild every layer |

It uses `E2B_API_KEY` if set; that key must belong to a system team. Otherwise it runs `vmon admin` (`$VMON_BIN`, else `vmon` on `PATH`, against a running driver's cluster listener, `VMON_CLUSTER_URL` defaulting to `http://127.0.0.1:7947`) to find or create the system team, mark it system, and mint a key. `E2B_API_URL` defaults to `http://127.0.0.1:3000`. After each build it publishes the template with `PATCH /v2/templates/{id}`, which claims the bare alias.

## Deletion and artifact GC

```sh
e2b template delete my-image -y      # DELETE /templates/{id}
```

Deleting soft-deletes the template: its aliases and tags are freed at once, its running builds are cancelled, and a second delete answers 404. Running and paused sandboxes keep working. The raft leader's housekeeping (every 60 s) collects artifacts of finished builds of deleted templates that no running or paused sandbox uses. It deletes them on every holder node and, with `--object-store`, removes the published copy under `builds/<buildID>/`. Snapshot templates are deleted the same way, and hidden fork templates are collected once no child uses them.

## Upgrades and the snapshot format

Memory snapshots carry a format version, currently 4. A VMM rejects snapshots of an older version with `recapture required`. When a release bumps the version, it is listed under Breaking Changes in [`CHANGELOG.md`](https://github.com/stencil-hq/vibemon/blob/main/CHANGELOG.md), and every template must be rebuilt: rerun `just templates` and your own builds.
