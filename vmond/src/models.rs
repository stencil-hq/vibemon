//! Typed v1 API request models — the OpenAPI-first contract.
//!
//! Field sets mirror `python/vmon/server/models.py` (§2.3/§2.4 of the
//! migration plan); sandbox VIEWS stay open JSON objects because
//! `record.view()` spreads a free-form detail map under canonical keys.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::registry::PersistencePolicy;

/// `POST /v1/sandboxes` body: today's `SandboxCreate` field set plus
/// `command` (foreground entrypoint override composed by the CLI).
/// External `remote_page_*` fields are rejected by validation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxCreate {
	pub image: Option<String>,
	pub template: Option<String>,
	pub dockerfile: Option<String>,
	#[serde(default = "default_context")]
	pub context: String,
	pub name: Option<String>,
	#[serde(default = "default_cpus")]
	pub cpus: u32,
	#[serde(default = "default_memory")]
	pub memory: u32,
	#[serde(default = "default_disk_mb")]
	pub disk_mb: u32,
	#[serde(default = "default_timeout")]
	pub timeout: Option<f64>,
	pub timeout_secs: Option<u64>,
	/// Per-sandbox network-idle timeout; zero disables.
	pub idle_timeout_secs: Option<f64>,
	/// Minimum raw NIC byte delta that qualifies an interval as active.
	pub activity_threshold_bytes: Option<u64>,
	/// Stored-state retention behavior.
	pub persistence: Option<PersistencePolicy>,
	pub workdir: Option<String>,
	pub env: Option<HashMap<String, String>>,
	/// Request-scoped secrets: `[{name, values}]`; never persisted.
	pub secrets: Option<Vec<Value>>,
	/// Host-brokered credential names; secret values never enter this document.
	pub credentials: Option<Vec<String>>,
	/// Mountpoint -> volume name or `{name, read_only}`.
	pub volumes: Option<HashMap<String, Value>>,
	/// Mountpoint -> lazy S3 mount specification.
	pub s3_mounts: Option<HashMap<String, S3MountSpec>>,
	pub tags: Option<HashMap<String, String>>,
	pub fs_dir: Option<String>,
	#[serde(default = "default_block_network")]
	pub block_network: bool,
	pub ports: Option<Vec<u16>>,
	pub egress_allow: Option<Vec<String>>,
	pub egress_allow_domains: Option<Vec<String>>,
	pub inbound_cidr_allowlist: Option<Vec<String>>,
	/// Single routed NIC attachment to a VPC.
	pub nics: Option<Vec<NicSpec>>,
	pub readiness_probe: Option<Value>,
	#[serde(default)]
	pub pool_size: u32,
	/// Rejected for external callers (mesh-internal restore plumbing).
	pub remote_page_url: Option<String>,
	pub remote_page_token: Option<String>,
	pub remote_page_digest: Option<String>,
	pub ha: Option<String>,
	pub arch: Option<String>,
	pub idempotency_key: Option<String>,
	/// Foreground entrypoint override (`command?: [str]`, new in v1).
	pub command: Option<Vec<String>>,
	/// Mesh-only tag preservation for restored remote virtio-fs snapshots.
	#[serde(skip)]
	pub(crate) s3_restore_tags: Option<HashMap<String, String>>,
	/// Owning tenant injected by the authenticated API boundary.
	#[serde(default = "default_tenant")]
	pub owner_tenant: String,
	/// Customer-managed encryption key injected by the API boundary.
	#[serde(default = "default_encryption_key")]
	pub encryption_key_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NicSpec {
	pub vpc:     String,
	pub ipv4:    Value,
	#[serde(default)]
	pub default: bool,
}

impl Default for SandboxCreate {
	fn default() -> Self {
		Self {
			image: None,
			template: None,
			dockerfile: None,
			context: default_context(),
			name: None,
			cpus: default_cpus(),
			memory: default_memory(),
			disk_mb: default_disk_mb(),
			timeout: default_timeout(),
			timeout_secs: None,
			idle_timeout_secs: None,
			activity_threshold_bytes: None,
			persistence: None,
			workdir: None,
			env: None,
			secrets: None,
			credentials: None,
			volumes: None,
			s3_mounts: None,
			tags: None,
			fs_dir: None,
			block_network: default_block_network(),
			ports: None,
			egress_allow: None,
			egress_allow_domains: None,
			inbound_cidr_allowlist: None,
			nics: None,
			readiness_probe: None,
			pool_size: 0,
			remote_page_url: None,
			remote_page_token: None,
			remote_page_digest: None,
			ha: None,
			arch: None,
			idempotency_key: None,
			command: None,
			s3_restore_tags: None,
			owner_tenant: default_tenant(),
			encryption_key_id: default_encryption_key(),
		}
	}
}

const fn default_block_network() -> bool {
	true
}

fn default_tenant() -> String {
	"default".to_owned()
}

fn default_encryption_key() -> String {
	"default".to_owned()
}

/// Create-time settings for one lazy S3 mount.
#[derive(Clone, Serialize)]
pub struct S3MountSpec {
	/// `s3://bucket[/prefix]` source URI.
	pub uri:           String,
	/// Optional S3-compatible path-style endpoint.
	pub endpoint:      Option<String>,
	/// Optional `SigV4` region override.
	pub region:        Option<String>,
	/// Mount without a guest-side volatile overlay.
	#[serde(default)]
	pub read_only:     bool,
	/// Inline access key; never serialized beyond the request boundary.
	#[serde(skip_serializing)]
	pub access_key:    Option<String>,
	/// Inline secret key; never serialized beyond the request boundary.
	#[serde(skip_serializing)]
	pub secret_key:    Option<String>,
	/// Inline session token; never serialized beyond the request boundary.
	#[serde(skip_serializing)]
	pub session_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct S3MountFields {
	uri:           String,
	endpoint:      Option<String>,
	region:        Option<String>,
	#[serde(default)]
	read_only:     bool,
	access_key:    Option<String>,
	secret_key:    Option<String>,
	session_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum S3MountWire {
	Uri(String),
	Fields(S3MountFields),
}

impl<'de> Deserialize<'de> for S3MountSpec {
	fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		match S3MountWire::deserialize(deserializer)? {
			S3MountWire::Uri(uri) => Ok(Self {
				uri,
				endpoint: None,
				region: None,
				read_only: false,
				access_key: None,
				secret_key: None,
				session_token: None,
			}),
			S3MountWire::Fields(fields) => Ok(Self {
				uri:           fields.uri,
				endpoint:      fields.endpoint,
				region:        fields.region,
				read_only:     fields.read_only,
				access_key:    fields.access_key,
				secret_key:    fields.secret_key,
				session_token: fields.session_token,
			}),
		}
	}
}

impl std::fmt::Debug for S3MountSpec {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("S3MountSpec")
			.field("uri", &self.uri)
			.field("endpoint", &self.endpoint)
			.field("region", &self.region)
			.field("read_only", &self.read_only)
			.field("access_key", &self.access_key.as_ref().map(|_| "<redacted>"))
			.field("secret_key", &self.secret_key.as_ref().map(|_| "<redacted>"))
			.field("session_token", &self.session_token.as_ref().map(|_| "<redacted>"))
			.finish()
	}
}

fn default_context() -> String {
	".".to_string()
}
const fn default_cpus() -> u32 {
	1
}
const fn default_memory() -> u32 {
	512
}
const fn default_disk_mb() -> u32 {
	1024
}
#[allow(clippy::unnecessary_wraps, reason = "serde default fns must return the field type")]
const fn default_timeout() -> Option<f64> {
	Some(300.0)
}

/// `POST /{id}/exec` body and the first WS exec frame (§2.4).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ExecBody {
	#[serde(default)]
	pub cmd:     Vec<String>,
	pub workdir: Option<String>,
	/// Legacy alias for `workdir` accepted on input.
	pub cwd:     Option<String>,
	pub env:     Option<HashMap<String, String>>,
	pub timeout: Option<f64>,
	#[serde(default)]
	pub tty:     bool,
}

/// `POST /{id}/snapshots` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SnapshotBody {
	pub name: Option<String>,
	#[serde(default)]
	pub stop: bool,
}

/// `POST /{id}/snapshots/fs` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SnapshotFsBody {
	pub name: Option<String>,
}

/// `POST /{id}/extend` body.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExtendBody {
	pub secs: u64,
}

/// Optional body for `POST /{id}/stop`, allowing foreground CLI runs to
/// preserve the process exit code they just observed.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StopBody {
	pub returncode: Option<i64>,
}

/// `GET|PUT /{id}/network` body (same field names as today).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NetworkBody {
	pub block_network: Option<bool>,
	pub cidr_allow:    Option<Vec<String>>,
	pub domain_allow:  Option<Vec<String>>,
}

/// `POST /v1/snapshots/{name}/restore` body.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RestoreBody {
	/// Optional stable ID for the restored sandbox.
	pub name:  Option<String>,
	/// Wait for the guest agent before returning.
	pub agent: Option<bool>,
	/// Runtime-only overrides such as environment, tags, timeout, secrets, and
	/// command.
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// Maximum number of clones accepted by one fork request.
pub const MAX_FORK_CLONES: u32 = 32;

/// `POST /v1/snapshots/{name}/fork` body.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ForkBody {
	/// Number of clones to create atomically.
	pub count: u32,
	/// Runtime-only overrides shared by every clone in the batch.
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// `PUT /v1/pools/{ref}` body (= today's `prewarm`).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolPutBody {
	pub size:  u32,
	/// Template kwargs forwarded to the image pipeline.
	#[serde(flatten)]
	pub extra: HashMap<String, Value>,
}

/// `POST /{id}/migrate` body (admin; mesh).
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MigrateBody {
	pub target: String,
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::SandboxCreate;

	#[test]
	fn s3_mounts_accept_uri_shorthand_and_never_serialize_credentials() {
		let request: SandboxCreate = serde_json::from_value(json!({
			"s3_mounts": {
				"/mnt/public": "s3://public/prefix",
				"/mnt/private": {
					"uri": "s3://private",
					"access_key": "access",
					"secret_key": "secret",
					"session_token": "session"
				}
			}
		}))
		.expect("S3 mount request parses");
		let mounts = request.s3_mounts.as_ref().expect("mounts");
		assert_eq!(mounts["/mnt/public"].uri, "s3://public/prefix");
		assert!(!mounts["/mnt/public"].read_only);
		assert_eq!(mounts["/mnt/private"].access_key.as_deref(), Some("access"));

		let wire = serde_json::to_value(request).expect("request serializes");
		let private = &wire["s3_mounts"]["/mnt/private"];
		assert!(private.get("access_key").is_none());
		assert!(private.get("secret_key").is_none());
		assert!(private.get("session_token").is_none());
	}
}
