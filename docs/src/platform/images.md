# OCI Images

Vibemon turns a Linux OCI image into a bootable microVM template. The image is
not run through a container runtime: its OCI filesystem is converted into an
ext4 root filesystem, then Vibemon boots and verifies that root filesystem
before saving the template's VMM snapshot.

## Prerequisites

Image-backed templates require `skopeo`, `umoci`, an ext4 formatter
(`mkfs.ext4` or `mke2fs`), a compatible Linux guest kernel, and a static
musl-built `vmon-agent`. Run `vmon doctor` on the server host to check the
local prerequisites. On macOS, the ext4 utilities may come from an e2fsprogs
installation; Vibemon searches the usual Homebrew locations as well as the
system paths.

The guest architecture must match the host hypervisor architecture. OCI
selection is explicitly for `linux/<arch>`; common manifest spellings are
normalized (`amd64`/`x64` to `x86_64`, and `arm64` to `aarch64`). macOS/HVF
runs only aarch64 Linux guests. See the [Support Matrix](support-matrix.md)
for the host constraints.

## Pipeline

| Stage | What Vibemon does | Operator implication |
| --- | --- | --- |
| Resolve | Uses `skopeo inspect` for the selected Linux architecture and resolves a registry image to its SHA-256 manifest digest. | A mutable tag is re-inspected when it is registered, so it can move. Keep the digest-pinned reference returned by the resolver when reproducibility matters. |
| Acquire | Uses `skopeo copy` to make a local OCI layout. Supported input transports include registry (`docker://`), OCI layout, directory, Docker archive, OCI archive, and containers storage. | The server host, not the guest, needs access to the image source. |
| Unpack | Uses `umoci unpack` to materialize the OCI root filesystem. | OCI image metadata supplies the image entrypoint, command, environment, working directory, and user defaults. |
| Prepare | Injects the static guest agent and creates an ext4 image from the unpacked tree. | A dynamically linked or missing agent is rejected; set `VMON_AGENT=/path/to/static-agent` only to a static ELF agent appropriate for the guest architecture. |
| Verify | Boots the ext4 rootfs with the chosen kernel and requested device slots, then snapshots that verified VM as a template. | A template is a boot-verified artifact, not merely an unpacked image. A boot failure prevents a usable template. |
| Cache | Keys the rootfs cache by image digest, disk size, and agent digest; includes memory, CPU, filesystem-slot, host-share, NIC, and TAP-slot choices in the template identity. | Changing any of those template-shaping options selects a different template. |

The ext4 disk-size request defaults to 1024 MiB in the image pipeline. It is a
capacity choice for the generated root filesystem; ensure it can hold the
unpacked image plus the injected agent.

## Cloud disk exports

Cloud disk exports are explicit image inputs, not OCI transports. Publish an
export once on a host with the provider workload identity and the e2fsprogs
tools, then use the original URI for sandbox creation:

```sh
vmon image publish-rootfs gs://bucket/exports/ubuntu.tar.gz
vmon image publish-rootfs s3://bucket/exports/ubuntu.vhd
vmon run gs://bucket/exports/ubuntu.tar.gz -- echo hello
vmon run s3://bucket/exports/ubuntu.vhd -- echo hello
```

Both `gs://` and `s3://` accept the same export formats:

- a gzip-compressed tar (`.tar.gz` or `.tgz`) whose first member is
  `disk.raw`;
- a raw GPT disk (`.raw` or `.img`); or
- a fixed VHD containing a GPT disk (`.vhd`).

QCOW2, dynamic VHD, MBR-only disks, LVM roots, and non-ext4 roots are not
supported. The publisher selects the largest partition that actually contains
an ext4 superblock, injects `/.vmon/agent`, minimizes the filesystem, and
uploads independent 1 MiB zstd frames plus a JSON sidecar beside the source.
Gzip-tar input is scanned as a forward-only stream, including when a larger
non-ext4 partition precedes or follows the root; direct raw and fixed-VHD input
uses range reads to probe partitions and copy only the selected root.

Publication reads one immutable source identity, uploads the derived rootfs
first, re-reads its stored identity, and writes the sidecar last. The sidecar
records the source and derived object identities, the extracted and compressed
rootfs sizes, frame index, compressed SHA-256, and guest-agent SHA-256. A later sandbox request starts from the
original cloud URI, validates that sidecar against the current source and
derived objects, and range-reads only the published zstd frames; it never
reconverts the export. A missing sidecar or any source, derived-object, size,
digest, version, or index mismatch is rejected with instructions to rerun
`vmon image publish-rootfs`.

GCS lazy reads require both a positive object generation and an ETag, and pin
every request to that identity. S3 lazy reads require both a non-null
`x-amz-version-id` and a strong quoted ETag; every range request specifies the
discovered `versionId` and uses `If-Match`. Enable S3 bucket versioning before
uploading the source export. An object whose version ID is absent or `null` is
rejected, even if it has an ETag.

GCS metadata OAuth tokens and dynamic AWS workload credentials (ECS task role,
EC2 instance role, or other provider workload credentials) are sent only to
provider-trusted HTTPS object endpoints. S3 requests use Signature Version 4.
`VMON_S3_ENDPOINT` may select an S3-compatible service, but it does not waive
the immutable-identity requirement: that service must return a non-null
version ID and a strong quoted ETag and honor version-pinned, conditional range
reads. Configure credentials and trust for a non-provider endpoint explicitly;
workload credentials are not forwarded to it.

Private OCI pulls use the same workload identity boundary: Google Artifact
Registry receives a short-lived metadata OAuth token, and Amazon ECR receives
a short-lived authorization token obtained with a signed ECR request. Set
`VMON_REGISTRY_AUTH_FILE` when an operator-managed Docker auth file is required
for another registry.

### Deployment identity and prefix policy

The Pulumi stacks do not create or configure the source bucket or registry
repository. Before an AWS deployment, enable versioning on the referenced
bucket and ensure each existing source export has a non-null version ID (copy
or upload it again after enabling versioning if necessary). GCS supplies each
object with a positive generation; ensure the source object's metadata also
includes an ETag. Configure `rootfsS3Prefix` in `deploy/aws` as
`s3://bucket/prefix/`, or
`rootfsGcsPrefix` in `deploy/gcp` as `gs://bucket/prefix/`; the trailing slash
is required. The source export must be below that prefix because the derived
rootfs and sidecar are written beside it.

The AWS worker role is limited to `s3:GetObject`, `s3:PutObject`, and
`s3:AbortMultipartUpload` on the configured prefix, plus
prefix-conditioned `s3:ListBucket` so missing-artifact probes return `404`.
The GCP worker service
account receives prefix-conditioned object-user access; the publish path
requires `storage.objects.get`, `storage.objects.create`, and
`storage.objects.delete`. An operator who publishes from another host must
grant its workload identity the same provider-specific operations. S3 buckets
should also abort abandoned multipart uploads with a bucket lifecycle rule.

Private cloud-registry IAM is opt-in. Set `ecrRepositoryArn` in `deploy/aws` to
a full ECR repository ARN, or `artifactRegistryRepository` in `deploy/gcp` to
the full
`projects/PROJECT/locations/LOCATION/repositories/REPOSITORY` resource name.
Omitting those keys grants no ECR or Artifact Registry access. Other private
registries continue to use the operator-managed `VMON_REGISTRY_AUTH_FILE`.

## Dockerfile builds

Dockerfile builds require `buildctl` and an isolated BuildKit endpoint in `VMON_BUILDKIT_ADDR`. Vibemon does not invoke Docker, Buildah, a shell, or an inherited host environment.

The daemon rejects contexts that escape through symlinks or exceed 1 GiB. It sends the context to BuildKit with a cleared environment, accepts at most 4 GiB of OCI output, validates the OCI layout, and only then publishes it under a content-addressed cache key. The build timeout is 30 minutes.

BuildKit executes Dockerfile instructions, so run the daemon behind `VMON_BUILDKIT_ADDR` as a disposable, least-privileged service. The bundled Compose and Helm deployments use a separate rootless BuildKit workload.

The build and pulled-image caches are not archives. Local `oci:<path>` references are accepted only when the resolved OCI layout is under the server's `builds/` or `images/` cache.

## Commands and image defaults

For an image-backed sandbox, the default process argument vector is:

1. image `Entrypoint` followed by image `Cmd`; or
2. if a non-empty command override is supplied, the entrypoint followed by that
   override; if no entrypoint exists, the override alone.

Environment entries are parsed as `KEY=value` pairs. This describes template
and sandbox defaults; it does not turn arbitrary container-runtime settings
into microVM devices or networking policy.

## Kernel and agent assets

Vibemon has pinned default guest-kernel downloads for `x86_64` and `aarch64`.
If no supported pinned kernel exists for the host architecture, provide one
with `VMON_KERNEL=/path/to/Image-or-bzImage`. A user-supplied image must still
be a Linux guest root filesystem: Vibemon directly boots a kernel rather than
booting a full container runtime or a non-Linux OS.

For lower-level, operator-supplied rootfs and kernel boot commands, see
[Low-Level VMM](low-level-vmm.md). Template state and its restore constraints
are covered in [Snapshots, Restore, and Fork](snapshots.md).
