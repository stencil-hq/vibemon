//! Wire types for the node API (`/node/v1`, ARCHITECTURE.md §8.5) and the
//! driver-owned KV records that reference nodes (§6).
//!
//! Drivers depend on this module through `vmon-node`; nodes serve it. All
//! types are JSON with camelCase field names.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use vmon_cluster::NodeId;

/// Path prefix of every node API route on the cluster listener.
pub const PREFIX: &str = "/node/v1";

/// `Upgrade` token used to open a raw byte tunnel to a guest port.
pub const TUNNEL_UPGRADE: &str = "vmon-tunnel";

/// Error body returned by every non-2xx node API response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
	/// Stable machine code, e.g. `not_found`, `busy`, `port_closed`, `conflict`,
	/// `invalid`, `unsupported`, `internal`.
	pub code:    String,
	pub message: String,
}

/// [`ErrorBody::code`] of a memory pause or snapshot of a sandbox that holds
/// GPUs (400): GPU state cannot be captured, so such sandboxes only pause
/// their filesystem.
pub const GPU_MEMORY_SNAPSHOT: &str = "gpu_memory_snapshot";

/// [`ErrorBody::code`] of a memory pause or snapshot of a sandbox whose guest
/// runs VMs (409).
///
/// KVM's nested state is not part of a snapshot. The sandbox keeps running;
/// it can be paused once its last VM exits. Also the VMM control code.
pub const NESTED_VIRTUALIZATION_ACTIVE: &str = "nested_virtualization_active";

/// `POST /sandboxes`: start a sandbox from a build's template snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandbox {
	pub sandbox_id:       String,
	pub team_id:          String,
	pub template_id:      String,
	pub build_id:         String,
	/// Nodes that hold the build's artifacts, best first; the receiving node
	/// resolves each to its own route (`Cluster::cluster_url`). Empty when the
	/// build is expected to be local.
	pub artifact_sources: Vec<NodeId>,
	pub vcpus:            u32,
	pub memory_mb:        u64,
	pub disk_mb:          u64,
	/// Authoritative expiry chosen by the driver (unix ms). The node arms the
	/// VMM hard deadline at `end_at_ms + 60 s` as a safety net.
	pub end_at_ms:        u64,
	pub init:             GuestInit,
	pub network:          NetworkPolicy,
	pub volume_mounts:    Vec<VolumeMount>,
	/// Bring-your-own-bucket mounts (vibemon extension `vmonS3Mounts`).
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub s3_mounts:        Vec<S3Mount>,
	/// Boot the rootfs fresh instead of forking the template snapshot.
	pub cold_boot:        bool,
	/// Whole GPUs (or vGPU VFs) to pass through, ≤ 8. The node reserves them
	/// all or answers `busy`; a GPU sandbox always cold-boots.
	#[serde(default)]
	pub gpus:             u32,
}

/// Everything the node passes to envd `POST /init` after a boot or resume.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GuestInit {
	pub env_vars:          BTreeMap<String, String>,
	pub envd_access_token: String,
	pub default_user:      Option<String>,
	pub default_workdir:   Option<String>,
	pub sandbox_id:        String,
	pub template_id:       String,
	/// PEM CA bundle to trust (egress HTTPS rules MITM CA), if any.
	pub ca_bundle:         Option<String>,
}

/// A team volume mounted into the guest over virtio-fs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMount {
	pub volume_id:    String,
	pub name:         String,
	/// Absolute guest path.
	pub path:         String,
	/// Stored under `volumes/<volumeID>/` in the operator object store and
	/// mounted as a write-back S3 mount on any node with feature
	/// `object-store` (otherwise a node-local directory).
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub object_store: bool,
}

/// How an S3 mount handles guest writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum S3MountMode {
	/// Lazy read-through; writes fail `EROFS`.
	#[default]
	Ro,
	/// Lazy read-through lower layer + guest overlay on the sandbox disk:
	/// writes stay in the sandbox and vanish with it.
	Overlay,
	/// Full read/write persisted to the bucket (`close`/`fsync` upload).
	Writeback,
}

impl S3MountMode {
	/// Wire spelling.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Ro => "ro",
			Self::Overlay => "overlay",
			Self::Writeback => "writeback",
		}
	}
}

/// Static credentials of a bring-your-own-bucket mount. Never logged
/// (`Debug` is redacted); persisted only encrypted by the driver.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3MountCredentials {
	pub access_key_id:     String,
	pub secret_access_key: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub session_token:     Option<String>,
}

impl std::fmt::Debug for S3MountCredentials {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("S3MountCredentials")
			.field("access_key_id", &self.access_key_id)
			.finish_non_exhaustive()
	}
}

/// A bring-your-own-bucket mount (`vmonS3Mounts[]`), already validated by
/// the driver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3Mount {
	/// Absolute, clean guest path.
	pub path:        String,
	/// `s3://bucket[/prefix]` or `http(s)://endpoint/bucket[/prefix]`.
	pub uri:         String,
	#[serde(default)]
	pub mode:        S3MountMode,
	/// Endpoint serving an `s3://` URI instead of AWS.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub endpoint:    Option<String>,
	/// Signing region (default: from the endpoint / environment).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region:      Option<String>,
	/// `None` = anonymous requests (public buckets); never the node's own
	/// credentials.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub credentials: Option<S3MountCredentials>,
}

/// Egress policy (E2B `SandboxNetworkConfig` egress subset), already
/// normalized by the driver: `allow_internet_access=false` is folded into
/// `deny_out = ["0.0.0.0/0"]`, bare IPs are `/32`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicy {
	/// CIDRs and domain patterns (`example.com`, `*.example.com`, `*`).
	pub allow_out:    Vec<String>,
	/// CIDRs only.
	pub deny_out:     Vec<String>,
	pub egress_proxy: Option<EgressProxy>,
	/// Domain pattern → HTTPS header transform rules.
	pub rules:        BTreeMap<String, Vec<NetworkRule>>,
}

impl NetworkPolicy {
	/// Whether the policy needs the transparent egress proxy (Linux nodes with
	/// feature `egress-proxy`) rather than a plain CIDR filter.
	pub fn needs_proxy(&self) -> bool {
		self.egress_proxy.is_some() || !self.rules.is_empty()
	}

	/// Whether any `allow_out` entry is a domain pattern.
	pub fn has_domains(&self) -> bool {
		self
			.allow_out
			.iter()
			.any(|entry| entry.parse::<std::net::IpAddr>().is_err() && !entry.contains('/'))
	}
}

/// SOCKS5 upstream for sandbox egress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressProxy {
	/// `host:port`.
	pub address:  String,
	pub username: Option<String>,
	pub password: Option<String>,
	pub tls:      Option<EgressProxyTls>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EgressProxyTls {
	pub enabled:     bool,
	pub server_name: Option<String>,
	/// PEM CA certificates (≤ 8 KiB) that replace the system roots for the proxy
	/// hop.
	#[serde(default)]
	pub ca_cert:     Option<String>,
}

/// One transform rule for matching outbound HTTPS requests.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkRule {
	/// Headers injected or overridden on matching requests.
	pub headers: BTreeMap<String, String>,
}

/// Node-local view of a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxInfo {
	pub sandbox_id:    String,
	pub team_id:       String,
	pub template_id:   String,
	pub build_id:      String,
	pub state:         SandboxState,
	pub vcpus:         u32,
	pub memory_mb:     u64,
	pub disk_mb:       u64,
	pub started_at_ms: u64,
	pub end_at_ms:     u64,
	pub envd_version:  String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxState {
	Running,
	Paused,
}

/// `POST /sandboxes/{id}/pause`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PauseSandbox {
	/// `false` = filesystem-only pause (fsfreeze + stop; resume cold-boots).
	pub memory:   bool,
	/// Source pause of a handover: the state stays node-local for the target
	/// to pull (`AdoptSandbox`), so no object-store upload starts.
	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub handover: bool,
}

/// `POST /sandboxes/{id}/adopt`: pull another node's paused sandbox
/// (`GET /sandboxes/{id}/handover` on `source`) and hold it paused here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AdoptSandbox {
	/// Authoritative sandbox metadata, state `paused` (as
	/// `ResumeSandbox::meta`).
	pub meta:             crate::sandbox::SandboxMeta,
	/// Node holding the paused state.
	pub source:           NodeId,
	/// Peers holding the template build, whose files the state may link
	/// instead of transferring them.
	#[serde(default)]
	pub artifact_sources: Vec<NodeId>,
}

/// `GET /sandboxes/{id}/handover`: a committed paused sandbox as a peer pulls
/// it (`AdoptSandbox`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandoverManifest {
	/// Template build that `HandoverFile::build_file` entries refer to.
	pub build_id: String,
	pub files:    Vec<HandoverFile>,
}

/// One file of a `HandoverManifest`; `GET /sandboxes/{id}/handover/{path}`
/// streams its data extents in order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandoverFile {
	/// `path` is relative to the sandbox: `paused/…` (the paused state:
	/// snapshot, RAM base, rootfs overlay and its base) or `console.log` /
	/// `metrics.jsonl` (history the logs and metrics routes serve).
	#[serde(flatten)]
	pub file:       ArtifactFile,
	/// Build-relative path of the same file in `HandoverManifest::build_id`
	/// (a hard link on the source, e.g. the template's RAM base or raw
	/// rootfs): a target whose copy of the build has an identical entry links
	/// it instead of fetching.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub build_file: Option<String>,
}

/// `POST /sandboxes/{id}/resume`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResumeSandbox {
	pub end_at_ms:        u64,
	pub init:             GuestInit,
	pub network:          NetworkPolicy,
	pub volume_mounts:    Vec<VolumeMount>,
	/// Bring-your-own-bucket mounts, re-attached on resume.
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub s3_mounts:        Vec<S3Mount>,
	/// Ignore the memory snapshot and cold-boot the paused rootfs.
	pub cold_boot:        bool,
	/// Published paused generation (`paused/<id>/remote`) to fetch when this
	/// node does not hold the paused state.
	#[serde(default)]
	pub remote:           Option<crate::paused_store::Remote>,
	/// Authoritative sandbox metadata for adopting a pause from another node.
	#[serde(default)]
	pub meta:             Option<crate::sandbox::SandboxMeta>,
	/// Nodes holding the template build (cold-boot fallback, template RAM
	/// base of delta snapshots).
	#[serde(default)]
	pub artifact_sources: Vec<NodeId>,
	/// GPUs to reserve again (as [`CreateSandbox::gpus`]); the resume then
	/// cold-boots the paused filesystem.
	#[serde(default)]
	pub gpus:             u32,
}

/// `POST /sandboxes/{id}/deadline`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deadline {
	pub end_at_ms: u64,
}

/// `POST /sandboxes/{id}/processes`: start a detached guest process through
/// envd (`process.Process/Start`). It keeps running after the request ends,
/// with no lifetime; stdin is `/dev/null`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartProcess {
	pub cmd:  String,
	#[serde(default)]
	pub args: Vec<String>,
	#[serde(default)]
	pub envs: BTreeMap<String, String>,
	/// Working directory (default: the user's default workdir or home).
	#[serde(default)]
	pub cwd:  Option<String>,
	/// User to run as (default: the sandbox's default user).
	#[serde(default)]
	pub user: Option<String>,
}

/// Reply of [`StartProcess`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStarted {
	pub pid: u32,
}

/// `POST /sandboxes/{id}/snapshot`: capture a running sandbox as a new build.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotSandbox {
	pub build_id:    String,
	pub template_id: String,
	/// Capture guest memory (`true`) or only the filesystem (`false`).
	pub memory:      bool,
}

/// One envd `/metrics` sample (5 s cadence).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricSample {
	pub timestamp_ms: u64,
	pub cpu_count:    u32,
	pub cpu_used_pct: f64,
	pub mem_used:     u64,
	pub mem_total:    u64,
	pub mem_cache:    u64,
	pub disk_used:    u64,
	pub disk_total:   u64,
}

/// One sandbox log line.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
	pub timestamp_ms: u64,
	/// `debug`, `info`, `warn`, or `error`.
	pub level:        String,
	pub message:      String,
	pub fields:       BTreeMap<String, String>,
}

/// Query for `GET /sandboxes/{id}/logs`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogQuery {
	/// Unix ms anchor; `None` = now (backward) or sandbox start (forward).
	pub cursor:    Option<u64>,
	pub limit:     Option<u32>,
	/// `forward` or `backward` (default).
	pub direction: Option<String>,
	/// Minimum level.
	pub level:     Option<String>,
	/// Case-sensitive substring filter.
	pub search:    Option<String>,
}

/// `POST /builds`: build a template on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartBuild {
	pub build_id:         String,
	pub template_id:      String,
	pub team_id:          String,
	pub from:             BuildBase,
	pub steps:            Vec<BuildStep>,
	pub start_cmd:        Option<String>,
	pub ready_cmd:        Option<String>,
	pub vcpus:            u32,
	pub memory_mb:        u64,
	pub disk_mb:          u64,
	/// Nodes holding uploaded file contexts (`/node/v1/files`).
	pub file_sources:     Vec<NodeId>,
	/// Ignore every cached step.
	pub force:            bool,
	/// Builder semantics derived from the client's SDK version.
	#[serde(default)]
	pub template_version: TemplateVersion,
}

/// Template builder semantics, selected by the driver from the client's
/// `User-Agent` SDK version (upstream `userAgentToTemplateVersion`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateVersion {
	/// SDK < 2.3.0: no default-user phase before steps; sandboxes default to
	/// user `user` with the home directory as workdir.
	#[serde(rename = "2.0.0")]
	V2_0,
	/// A `USER user` (with sudo) phase runs before the steps; sandbox defaults
	/// are the last `USER` and `WORKDIR` of the build.
	#[default]
	#[serde(rename = "2.1.0")]
	V2_1,
}

/// Base of a template build.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum BuildBase {
	/// OCI image reference (`ubuntu:22.04`, `ghcr.io/org/img@sha256:…`).
	#[serde(rename_all = "camelCase")]
	Image { image: String, registry: Option<RegistryAuth> },
	/// Another build's rootfs (`fromTemplate`), held by `sources`.
	#[serde(rename_all = "camelCase")]
	Build { build_id: String, sources: Vec<NodeId> },
}

/// Registry credentials (`fromImageRegistry`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RegistryAuth {
	#[serde(rename_all = "camelCase")]
	Registry { username: String, password: String },
	#[serde(rename_all = "camelCase")]
	Aws {
		aws_access_key_id:     String,
		aws_secret_access_key: String,
		aws_region:            String,
	},
	#[serde(rename_all = "camelCase")]
	Gcp { service_account_json: String },
}

/// One E2B v2 build step (`RUN`, `COPY`, `ENV`, `WORKDIR`, `USER`, …).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildStep {
	#[serde(rename = "type")]
	pub kind:       String,
	pub args:       Vec<String>,
	pub files_hash: Option<String>,
	pub force:      bool,
}

/// `GET /builds/{id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
	pub build_id:   String,
	pub status:     BuildState,
	/// Log entries starting at the requested `logsOffset`.
	pub logs:       Vec<BuildLogEntry>,
	/// Total log entries produced so far.
	pub logs_total: u64,
	pub reason:     Option<BuildReason>,
	/// Present once `status == Ready`.
	pub artifact:   Option<ArtifactInfo>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BuildState {
	Waiting,
	Building,
	Ready,
	Error,
	Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildLogEntry {
	pub timestamp_ms: u64,
	/// `debug`, `info`, `warn`, or `error`.
	pub level:        String,
	pub step:         Option<String>,
	pub message:      String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildReason {
	pub message: String,
	pub step:    Option<String>,
}

/// Facts about a build's artifacts on one node (also stored in KV
/// `art/<buildID>/<nodeID>`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactInfo {
	pub build_id:        String,
	pub node_id:         NodeId,
	pub arch:            String,
	pub backend:         String,
	pub cpu_baseline:    String,
	pub has_snapshot:    bool,
	pub bytes:           u64,
	pub envd_version:    String,
	pub vcpus:           u32,
	pub memory_mb:       u64,
	pub disk_mb:         u64,
	/// Template defaults delivered to envd `/init` (from `USER`, `WORKDIR`,
	/// `ENV` steps).
	pub default_user:    String,
	pub default_workdir: Option<String>,
	pub env:             BTreeMap<String, String>,
}

/// `GET /artifacts/{buildId}/manifest`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactManifest {
	pub info:  ArtifactInfo,
	pub files: Vec<ArtifactFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactFile {
	/// Path relative to the build directory, `/`-separated.
	pub path:    String,
	/// Logical (apparent) size in bytes.
	pub size:    u64,
	/// Extent digest (`artifacts::sparse::ExtentHasher`): SHA-256 over every
	/// data extent as `offset ‖ len ‖ bytes`, then the size.
	pub sha256:  String,
	/// Data extents `[offset, len]`, sorted; everything else reads as zeros.
	/// `GET /artifacts/{id}/files/{path}?sparse=true` streams exactly these
	/// bytes, in order.
	pub extents: Vec<[u64; 2]>,
}

/// Live catalog entry `sbx/<sandboxID>` in the raft KV.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxRoute {
	pub team_id:              String,
	pub node_id:              NodeId,
	pub template_id:          String,
	pub build_id:             String,
	pub state:                RouteState,
	pub started_at_ms:        u64,
	pub end_at_ms:            u64,
	pub auto_pause:           bool,
	pub auto_resume:          bool,
	pub allow_public_traffic: bool,
	pub traffic_token:        Option<String>,
	pub mask_request_host:    Option<String>,
	pub https_ports:          Vec<u16>,
	pub vcpus:                u32,
	pub memory_mb:            u64,
	pub envd_version:         String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RouteState {
	Running,
	Pausing,
	Resuming,
	Killing,
	Snapshotting,
	/// Moving to another node (driver `lifecycle::migrate`): `node_id` holds
	/// the sandbox, paused once the move is under way, until the target runs
	/// it and the route names the target.
	Migrating,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn domain_detection_ignores_ips_and_cidrs() {
		let policy = |allow: &[&str]| NetworkPolicy {
			allow_out: allow.iter().map(ToString::to_string).collect(),
			..NetworkPolicy::default()
		};
		assert!(!policy(&["8.8.8.8", "10.0.0.0/8", "::1"]).has_domains());
		assert!(policy(&["*.example.com"]).has_domains());
		assert!(policy(&["*"]).has_domains());
	}

	#[test]
	fn build_base_and_registry_auth_use_tagged_json() {
		let base = BuildBase::Image {
			image:    "ubuntu:22.04".into(),
			registry: Some(RegistryAuth::Aws {
				aws_access_key_id:     "k".into(),
				aws_secret_access_key: "s".into(),
				aws_region:            "us-east-1".into(),
			}),
		};
		let json = serde_json::to_value(&base).unwrap();
		assert_eq!(json["kind"], "image");
		assert_eq!(json["registry"]["type"], "aws");
		assert_eq!(json["registry"]["awsSecretAccessKey"], "s");
	}
}
