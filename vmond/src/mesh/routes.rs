//! Peer-facing mesh HTTP routes.
//!
//! These handlers preserve the Python mesh wire shape for node-to-node routes:
//! FastAPI-style `{"detail": ...}` error bodies, bearer authentication, and
//! `X-Vmon-Mesh-Hop: 1` on peer-only calls.  The concrete state/lease/record
//! stores are provided by sibling modules through the traits below.

use std::{
	path::{Path as FsPath, PathBuf},
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
	Json, Router,
	body::{Body, to_bytes},
	extract::{Path, Query, Request, State},
	http::{HeaderMap, HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{get, post},
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::Instant;

use super::gossip::{self, JsonObject, MeshError, MeshResult, PeerHttpClient};
use crate::image::cas;

const BODY_LIMIT: usize = 16 * 1024 * 1024;
const ALLOWED_HA: &[&str] = &["async", "async+rerun", "off", "rerun"];
const MIGRATION_ID_FIELD: &str = "_mesh_migration_id";
const MIGRATION_BASE_DIGEST_FIELD: &str = "_mesh_migration_base_digest";
const MIGRATION_DELTA_DIGEST_FIELD: &str = "_mesh_migration_delta_digest";
const MIGRATION_SOURCE_URL_FIELD: &str = "_mesh_migration_source_url";
const MIGRATION_SOURCE_OWNER_FIELD: &str = "_mesh_migration_source_owner";
const MIGRATION_SOURCE_EPOCH_FIELD: &str = "_mesh_migration_source_epoch";
const MIGRATION_DELTA_DIR_FIELD: &str = "_mesh_migration_delta_dir";
const MIGRATION_COMMITTED_FIELD: &str = "_mesh_migration_committed";

pub type RouteResult<T> = std::result::Result<T, MeshRouteError>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeCapsWire {
	pub vcpus:   u32,
	pub mem_mib: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MeshSetupResult {
	pub blob:      String,
	pub node_id:   String,
	pub advertise: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PeerMember {
	pub node_id:   String,
	pub advertise: String,
	pub healthy:   bool,
}

#[derive(Clone, Debug)]
pub struct MetadataPull {
	pub template:        String,
	pub remote_page_url: String,
	pub digest:          String,
}

#[derive(Clone, Debug)]
pub struct MigratePrepareWire {
	pub digest:       String,
	pub snapshot_dir: String,
	pub params:       JsonObject,
	pub cleanup:      Option<MigrationCleanupWire>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MigrationCleanupWire {
	pub sid:            String,
	pub base_dir:       String,
	pub base_digest:    String,
	pub delta_dir:      String,
	pub delta_digest:   String,
	#[serde(default)]
	pub target_url:     String,
	#[serde(default)]
	pub migration_id:   String,
	#[serde(default)]
	pub epoch:          i64,
	#[serde(default)]
	pub schema_version: u8,
	#[serde(default)]
	pub source_owner:   String,
	#[serde(default)]
	pub source_epoch:   i64,
	#[serde(default)]
	pub target_owner:   String,
	#[serde(default)]
	pub target_epoch:   i64,
	#[serde(default)]
	pub token:          String,
	#[serde(default)]
	pub phase:          String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateRecordWire {
	pub sid:             String,
	pub params:          JsonObject,
	pub owner:           String,
	pub epoch:           i64,
	pub idempotency_key: String,
	pub ha:              String,
	pub restart_policy:  String,
	pub created_at:      f64,
}

impl CreateRecordWire {
	pub fn from_value(value: Value) -> MeshResult<Self> {
		let Value::Object(mut data) = value else {
			return Err(MeshError::invalid("record body must be an object"));
		};
		let sid = take_string(&mut data, "sid");
		if sid.is_empty() {
			return Err(MeshError::invalid("record sid is required"));
		}
		let Some(Value::Object(params)) = data.remove("params") else {
			return Err(MeshError::invalid("record params must be an object"));
		};
		let owner = take_string(&mut data, "owner");
		if owner.is_empty() {
			return Err(MeshError::invalid("record owner is required"));
		}
		let idempotency_key = take_string(&mut data, "idempotency_key");
		if idempotency_key.is_empty() {
			return Err(MeshError::invalid("record idempotency_key is required"));
		}
		let ha = nonempty_string(data.remove("ha")).unwrap_or_else(|| "off".to_owned());
		if !ALLOWED_HA.contains(&ha.as_str()) {
			return Err(MeshError::invalid(format!("invalid ha tier {ha:?}")));
		}
		let restart_policy = nonempty_string(data.remove("restart_policy"))
			.unwrap_or_else(|| restart_policy_for_ha(&ha).to_owned());
		Ok(Self {
			sid,
			params,
			owner,
			epoch: to_i64(data.remove("epoch")).unwrap_or(0),
			idempotency_key,
			ha,
			restart_policy,
			created_at: to_f64(data.remove("created_at")).unwrap_or_else(unix_now),
		})
	}

	pub fn to_value(&self) -> Value {
		json!({
			"sid": self.sid,
			"params": self.params,
			"owner": self.owner,
			"epoch": self.epoch,
			"idempotency_key": self.idempotency_key,
			"ha": self.ha,
			"restart_policy": self.restart_policy,
			"created_at": self.created_at,
		})
	}
}

#[derive(Clone, Debug)]
pub struct MeshRouteConfig {
	pub replicas:          usize,
	pub create_timeout:    Duration,
	pub default_ha_meshed: String,
	pub default_ha_local:  String,
}

impl Default for MeshRouteConfig {
	fn default() -> Self {
		Self {
			replicas:          1,
			create_timeout:    Duration::from_mins(2),
			default_ha_meshed: "async".to_owned(),
			default_ha_local:  "off".to_owned(),
		}
	}
}

#[derive(Clone)]
pub struct MeshRouteState {
	pub inbound_token:  Arc<str>,
	pub outbound_token: Arc<str>,
	pub transport:      PeerHttpClient,
	pub mesh:           Arc<dyn MeshControl>,
	pub engine:         Arc<dyn MeshEngine>,
	pub leases:         Arc<dyn MeshLeaseManager>,
	pub records:        Arc<dyn MeshRecordStore>,
	pub replicas:       Arc<dyn MeshReplicaStore>,
	pub transfer:       Arc<dyn MeshTemplateTransfer>,
	pub config:         MeshRouteConfig,
}
impl MeshRouteState {
	#[allow(clippy::too_many_arguments, reason = "explicit service wiring keeps mesh seams visible")]
	pub fn new(
		inbound_token: impl Into<String>,
		outbound_token: impl Into<String>,
		transport: PeerHttpClient,
		mesh: Arc<dyn MeshControl>,
		engine: Arc<dyn MeshEngine>,
		leases: Arc<dyn MeshLeaseManager>,
		records: Arc<dyn MeshRecordStore>,
		replicas: Arc<dyn MeshReplicaStore>,
		transfer: Arc<dyn MeshTemplateTransfer>,
		config: MeshRouteConfig,
	) -> Self {
		Self {
			inbound_token: Arc::from(inbound_token.into()),
			outbound_token: Arc::from(outbound_token.into()),
			transport,
			mesh,
			engine,
			leases,
			records,
			replicas,
			transfer,
			config,
		}
	}

	pub async fn idempotent_sandbox_create(
		&self,
		key: String,
		params: JsonObject,
	) -> MeshResult<Value> {
		idempotent_create(self, CreateKind::Sandbox, key, params).await
	}

	pub async fn idempotent_run_create(
		&self,
		key: String,
		mut params: JsonObject,
	) -> MeshResult<Value> {
		params.insert("detach".to_owned(), Value::Bool(true));
		idempotent_create(self, CreateKind::Run, key, params).await
	}

	pub async fn idempotent_restore_create(
		&self,
		key: String,
		mut params: JsonObject,
	) -> MeshResult<Value> {
		params.insert("detach".to_owned(), Value::Bool(true));
		idempotent_create(self, CreateKind::Restore, key, params).await
	}
}

/// Membership/state seam supplied by `mesh::state` + `mesh::place`.
pub trait MeshControl: Send + Sync {
	fn node_id(&self) -> String;
	fn advertise(&self) -> String;
	fn default_advertise(&self) -> String;
	fn caps(&self) -> NodeCapsWire;
	fn enabled(&self) -> bool;
	fn peers(&self) -> Vec<PeerMember>;
	fn expected_members(&self) -> usize;
	fn quorum_needed(&self) -> usize;

	fn setup(
		&self,
		advertise: String,
		region: String,
		caps: Option<NodeCapsWire>,
	) -> MeshResult<MeshSetupResult>;
	fn join(&self, blob: String, advertise: Option<String>, region: String) -> MeshResult<Value>;
	fn leave(&self) -> MeshResult<()>;
	fn status(&self) -> MeshResult<Value>;
	fn register(&self, state: JsonObject) -> MeshResult<Value>;
	fn heartbeat(&self, state: JsonObject, known: Vec<Value>) -> MeshResult<Value>;
	fn depart(&self, node_id: &str);
	fn ensure_heartbeat(&self) {}
	fn request_replication(&self) {}

	fn is_peer_healthy(&self, node_id: &str) -> bool;
	fn migration_target(&self, node_id: &str) -> MeshResult<PeerMember>;
	fn replica_targets(&self, sid: &str, k: usize) -> Vec<String>;
	fn peer_url(&self, node_id: &str) -> Option<String>;
	fn find_template_provider(&self, key: &str) -> Option<(String, String)>;
	fn request_template_key(&self, params: &JsonObject) -> Option<String>;
	fn has_local_template_key(&self, key: &str) -> bool;

	fn authoritative_owner(&self, sid: &str) -> MeshResult<Option<(String, i64)>>;
	fn local_epoch(&self, sid: &str) -> i64;
	fn owner_of(&self, sid: &str) -> MeshResult<Option<String>>;
	fn record_owner(&self, sid: &str, node_id: &str, epoch: i64);
	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: i64);
	fn forget_owner(&self, sid: &str);
	fn mark_unhealthy(&self, node_id: &str);

	fn pinned_local(&self, params: &JsonObject) -> bool;
	fn place(&self, params: &JsonObject) -> MeshResult<String>;
	fn coordinator_for(&self, key: &str) -> String;
	fn idem_owner(&self, key: &str) -> Option<String>;
	fn idem_pin(&self, key: &str, owner: &str) -> String;
	fn idem_unpin(&self, key: &str);
	fn worker_begin(&self, key: &str) -> bool;
	fn worker_end(&self, key: &str);
}

/// Keeps an actively exported replica checkpoint alive while its peer bundle
/// response is streamed.
pub trait ReplicaExport: Send {
	fn path(&self) -> &FsPath;
}

/// Held exclusively from migration pre-copy through the terminal source
/// outcome, preventing two targets from pausing or aborting the same VM.
pub trait MigrationLifecycleGuard: Send {}

struct NoopMigrationLifecycleGuard;

impl MigrationLifecycleGuard for NoopMigrationLifecycleGuard {}

/// Engine seam used by peer routes.  Implementations may block internally;
/// route code awaits the returned futures so callers can wrap sync work in
/// `spawn_blocking`.
pub trait MeshEngine: Send + Sync {
	fn owned_ids(&self) -> MeshResult<Vec<String>>;
	fn list_views(&self) -> MeshResult<Vec<Value>>;
	fn has_sandbox(&self, sid: &str) -> bool;
	/// True only when the VMM reports a live local candidate. Implementations
	/// must fail closed; registry presence is not liveness.
	fn candidate_is_running(&self, _sid: &str) -> bool {
		false
	}
	/// Return an actively exported replica checkpoint only after validating its
	/// exact durable sid, digest, and scoped object key.
	fn replica_export_path(
		&self,
		_sid: &str,
		_digest: &str,
		_object_key: &str,
	) -> MeshResult<Box<dyn ReplicaExport>> {
		Err(MeshError::invalid("replica export is unavailable"))
	}
	fn get_view(&self, sid: &str) -> MeshResult<Value>;
	fn checkpoint_age_sec(&self, sid: &str) -> Option<f64>;
	fn find_by_idempotency_key(&self, key: &str) -> MeshResult<Option<Value>>;
	fn record_idempotency(&self, sid: &str, key: &str) -> MeshResult<()>;
	fn record_create_epoch(&self, _sid: &str, _epoch: i64) -> MeshResult<()> {
		Ok(())
	}
	/// Non-blocking per-sandbox migration lifecycle guard. A concurrent target
	fn stage_migration_delta(
		&self,
		sid: String,
		verified_cache_path: String,
		base_dir: String,
	) -> BoxFuture<'_, MeshResult<String>>;
	/// must fail before it begins pre-copy or can affect the winner's journal.
	fn try_begin_migration(&self, _sid: &str) -> MeshResult<Box<dyn MigrationLifecycleGuard>> {
		Ok(Box::new(NoopMigrationLifecycleGuard))
	}
	fn record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> MeshResult<()>;
	fn teardown_candidate(&self, sid: String, expected_epoch: i64) -> BoxFuture<'_, MeshResult<()>>;
	fn delete_portable_history(
		&self,
		sid: String,
		owner: String,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>>;

	fn create_sandbox(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn run_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn restore_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>>;
	fn restore_from_template(
		&self,
		params: JsonObject,
		template_dir: String,
		quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>>;
	fn migrate_precopy(&self, sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>>;
	fn migrate_finalize(
		&self,
		sid: String,
		base_dir: String,
		cleanup: MigrationCleanupWire,
	) -> BoxFuture<'_, MeshResult<MigratePrepareWire>>;
	fn checkpoint_discard(
		&self,
		snapshot_dir: String,
		digest: String,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn migrate_abort(
		&self,
		sid: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
		params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn migrate_commit(
		&self,
		sid: String,
		base_dir: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn migrate_adopt_target(
		&self,
		sid: String,
		delta_dir: String,
		params: JsonObject,
	) -> BoxFuture<'_, MeshResult<Value>>;
	/// Start the exact paused migration candidate only after its target
	/// ownership record and writable-volume grants have committed.
	fn migrate_activate_target(
		&self,
		sid: String,
		expected_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn migration_cleanup_persist(&self, _cleanup: MigrationCleanupWire) -> MeshResult<()> {
		Ok(())
	}
	fn migration_cleanup_pending(&self) -> MeshResult<Vec<MigrationCleanupWire>> {
		Ok(Vec::new())
	}
	fn migration_cleanup_remove(&self, _sid: String) -> MeshResult<()> {
		Ok(())
	}
	fn migrate_cleanup_committed(
		&self,
		sid: String,
		base_dir: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		self.migrate_commit(sid, base_dir, base_digest, delta_dir, delta_digest)
	}
}

pub trait MeshLeaseManager: Send + Sync {
	fn vote_grant(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject>;
	fn vote_renew(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject>;
	fn vote_release(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
	) -> MeshResult<JsonObject>;

	fn acquire_writable_volume_leases(
		&self,
		params: JsonObject,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>>;
	fn release_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>>;
	fn release_record_volume_leases(&self, sid: String) -> BoxFuture<'_, MeshResult<()>>;
}

pub trait MeshRecordStore: Send + Sync {
	fn get(&self, sid: &str) -> MeshResult<Option<CreateRecordWire>>;
	fn reserve_create_epoch(
		&self,
		_sid: &str,
		_owner: &str,
		_idempotency_key: &str,
	) -> MeshResult<i64> {
		Ok(0)
	}
	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire>;
	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> MeshResult<()>;
	fn list(&self) -> MeshResult<Vec<CreateRecordWire>>;
	fn update_exact(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		self.put(record)
	}
	fn claim_expected_with_intent(
		&self,
		_expected_owner: String,
		_expected_epoch: i64,
		record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		self.put(record)
	}

	/// Pre-commit source-deletion authorization before a target can consume a
	/// live migration token. The default rejects the operation rather than
	/// accidentally turning an unfenced test/store implementation into a
	/// successful live migration.
	fn begin_migration_handoff(
		&self,
		_sid: &str,
		_source_owner: &str,
		_source_epoch: i64,
		_target_owner: &str,
		_token: &str,
	) -> MeshResult<()> {
		Err(MeshError::with_code(
			"live migration handoff is unsupported by this record store",
			"busy",
		))
	}

	/// Confirm that the target durably consumed one exact source authorization.
	#[allow(clippy::too_many_arguments, reason = "the full migration lineage is the fencing key")]
	fn confirm_migration_handoff(
		&self,
		_sid: &str,
		_source_owner: &str,
		_source_epoch: i64,
		_target_owner: &str,
		_target_epoch: i64,
		_token: &str,
	) -> MeshResult<()> {
		Err(MeshError::with_code(
			"migration handoff confirmation is unsupported by this record store",
			"busy",
		))
	}

	/// Revoke an unclaimed source authorization and install a fresh source
	/// epoch. `None` is fenced: a target may already have consumed the token.
	fn abort_migration_handoff(
		&self,
		_sid: &str,
		_source_owner: &str,
		_source_epoch: i64,
		_target_owner: &str,
		_token: &str,
	) -> MeshResult<Option<CreateRecordWire>> {
		Err(MeshError::with_code(
			"live migration handoff abort is unsupported by this record store",
			"busy",
		))
	}

	/// Clear a completed migration-abort disposition only for the exact source
	/// generation that durably recorded the token.
	fn complete_migration_abort(
		&self,
		_sid: &str,
		_token: &str,
		_owner: &str,
		_fresh_epoch: i64,
	) -> MeshResult<CreateRecordWire> {
		Err(MeshError::with_code(
			"migration abort completion is unsupported by this record store",
			"busy",
		))
	}

	/// Consume the exact source-authorized migration token.  A different token
	/// or a missing source marker is always fenced.
	fn claim_migration_handoff(
		&self,
		_source_owner: String,
		_source_epoch: i64,
		_token: String,
		_record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		Err(MeshError::with_code(
			"live migration handoff is unsupported by this record store",
			"busy",
		))
	}

	/// Notify the source that this durable target intent consumed its token.
	fn notify_source_migration_claim<'a>(
		&'a self,
		record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>>;

	fn try_begin_target_migration(
		&self,
		_sid: &str,
		_migration_id: &str,
	) -> MeshResult<Box<dyn MigrationLifecycleGuard>> {
		Ok(Box::new(NoopMigrationLifecycleGuard))
	}

	fn put_if_newer(&self, record: CreateRecordWire) -> MeshResult<bool> {
		let should_put = match self.get(&record.sid)? {
			Some(existing) => record.epoch >= existing.epoch,
			None => true,
		};
		if should_put {
			self.put(record)?;
		}
		Ok(should_put)
	}
}

#[derive(Clone, Debug)]
pub struct ReplicaRecordWire {
	pub sid:                   String,
	pub digest:                String,
	pub object_key:            String,
	pub source_node:           String,
	pub source_epoch:          u64,
	pub checkpoint_generation: u64,
	pub snapshot_dir:          String,
	pub params:                JsonObject,
	pub needs_secrets:         bool,
}

pub trait MeshReplicaStore: Send + Sync {
	fn get(&self, sid: &str) -> MeshResult<Option<ReplicaRecordWire>>;
	fn put(
		&self,
		sid: String,
		object_key: String,
		digest: String,
		source_node: String,
		source_epoch: u64,
		checkpoint_generation: u64,
		snapshot_dir: String,
		params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>>;
	fn list(&self) -> MeshResult<Vec<String>>;
	fn remove(&self, sid: &str) -> MeshResult<()>;
	/// Return the root for one active local replica only after validating the
	/// exact immutable object identity. The route streams this root as a
	/// bundle; callers must not substitute a digest-only template lookup.
	fn snapshot_export(&self, _sid: &str, _object_key: &str, _digest: &str) -> MeshResult<PathBuf> {
		Err(MeshError::invalid("replica snapshot export is unavailable"))
	}
}

pub trait MeshTemplateTransfer: Send + Sync {
	fn pull_template<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>>;

	/// Pull and CAS-index one migration checkpoint by its complete-tree digest.
	fn pull_checkpoint<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>>;

	/// Pull one exact replica checkpoint object. Digest is an integrity check,
	/// never an object-identity lookup.
	fn pull_snapshot<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		sid: String,
		object_key: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>>;

	fn pull_template_metadata<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>>;
}

#[derive(Debug)]
pub struct MeshRouteError {
	status:       StatusCode,
	detail:       Value,
	authenticate: bool,
}

impl MeshRouteError {
	pub fn detail(status: StatusCode, detail: impl Into<String>) -> Self {
		Self { status, detail: Value::String(detail.into()), authenticate: false }
	}

	pub fn coded(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
		Self {
			status,
			detail: json!({"code": code.into(), "message": message.into()}),
			authenticate: false,
		}
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::coded(StatusCode::BAD_REQUEST, "invalid", message)
	}

	pub fn unauthorized() -> Self {
		Self {
			status:       StatusCode::UNAUTHORIZED,
			detail:       Value::String("unauthorized".to_owned()),
			authenticate: true,
		}
	}
}

impl From<MeshError> for MeshRouteError {
	fn from(value: MeshError) -> Self {
		Self::coded(value.http_status(), value.code, value.message)
	}
}

impl IntoResponse for MeshRouteError {
	fn into_response(self) -> Response {
		let mut response = (self.status, Json(json!({"detail": self.detail}))).into_response();
		if self.authenticate {
			response
				.headers_mut()
				.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
		}
		response
	}
}

pub fn router(state: MeshRouteState) -> Router {
	Router::new()
		.route("/v1/mesh/setup", post(mesh_setup))
		.route("/v1/mesh/join", post(mesh_join))
		.route("/v1/mesh/leave", post(mesh_leave))
		.route("/v1/mesh/status", get(mesh_status))
		.route("/v1/mesh/members", post(mesh_members))
		.route("/v1/mesh/heartbeat", post(mesh_heartbeat))
		.route("/v1/mesh/depart", post(mesh_depart))
		.route("/v1/mesh/reachable/{node_id}", get(mesh_reachable))
		.route("/v1/mesh/locate/{sandbox_id}", get(mesh_locate))
		.route("/v1/mesh/owner", post(mesh_owner))
		.route("/v1/mesh/lease/grant", post(mesh_lease_grant))
		.route("/v1/mesh/lease/renew", post(mesh_lease_renew))
		.route("/v1/mesh/lease/release", post(mesh_lease_release))
		.route("/v1/mesh/migrate/precopy", post(mesh_migrate_precopy))
		.route("/v1/mesh/migrate/receive", post(mesh_migrate_receive))
		.route("/v1/mesh/migrate/claim", post(mesh_migrate_claim))
		.route("/v1/mesh/migrate/status", post(mesh_migrate_status))
		.route("/v1/mesh/replica/receive", post(mesh_replica_receive))
		.route("/v1/mesh/replica/list", get(mesh_replica_list))
		.route("/v1/replicas/{sid}/{digest}", get(mesh_replica_snapshot))
		.route("/v1/mesh/record/put", post(mesh_record_put))
		.route("/v1/mesh/candidate/teardown", post(mesh_candidate_teardown))
		.route("/v1/mesh/idem/sandboxes/coordinate", post(mesh_idem_sandbox_coordinate))
		.route("/v1/mesh/idem/sandboxes/work", post(mesh_idem_sandbox_work))
		.route("/v1/mesh/idem/run/coordinate", post(mesh_idem_run_coordinate))
		.route("/v1/mesh/idem/run/work", post(mesh_idem_run_work))
		.route("/v1/mesh/idem/restore/coordinate", post(mesh_idem_restore_coordinate))
		.route("/v1/mesh/idem/restore/work", post(mesh_idem_restore_work))
		.with_state(state)
}

async fn mesh_setup(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object(request).await?;
	let current_caps = state.mesh.caps();
	let caps = if body.contains_key("max_vcpus") || body.contains_key("max_mem_mib") {
		Some(NodeCapsWire {
			vcpus:   to_u32(body.get("max_vcpus").cloned()).unwrap_or(current_caps.vcpus),
			mem_mib: to_u64(body.get("max_mem_mib").cloned()).unwrap_or(current_caps.mem_mib),
		})
	} else {
		None
	};
	let advertise = string_or(body.get("advertise"), state.mesh.default_advertise());
	let region = string_or(body.get("region"), String::new());
	let setup = map_mesh(state.mesh.setup(advertise, region, caps))?;
	state.mesh.ensure_heartbeat();
	Ok(Json(json!({
		"blob": setup.blob,
		"node_id": setup.node_id,
		"advertise": setup.advertise,
	})))
}

async fn mesh_join(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object(request).await?;
	let blob = string_or(body.get("blob"), String::new());
	let advertise = body
		.get("advertise")
		.and_then(value_string)
		.filter(|value| !value.is_empty());
	let region = string_or(body.get("region"), String::new());
	let status = map_mesh(state.mesh.join(blob, advertise, region))?;
	state.mesh.ensure_heartbeat();
	Ok(Json(status))
}

async fn mesh_leave(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, false)?;
	let body = json_object_or_empty(request).await?;
	let mut drained = Vec::new();
	if py_truthy(body.get("drain")) && state.mesh.enabled() {
		for sid in map_mesh(state.engine.owned_ids())? {
			let target = drain_target(&state);
			let Some(target) = target else {
				drained
					.push(json!({"sandbox": sid, "status": "skipped", "reason": "no compatible peer"}));
				continue;
			};
			match migrate_sandbox_to(&state, sid.clone(), target.clone()).await {
				Ok(_) => drained.push(json!({"sandbox": sid, "status": "migrated", "target": target})),
				Err(err) => drained.push(json!({
					"sandbox": sid,
					"status": "failed",
					"reason": route_error_detail(&err),
				})),
			}
		}
	}
	map_mesh(state.mesh.leave())?;
	Ok(Json(json!({"ok": true, "drained": drained})))
}

async fn mesh_status(
	State(state): State<MeshRouteState>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, false)?;
	Ok(Json(map_mesh(mesh_status_view(&state))?))
}

/// Compose the mesh status document served by `GET /v1/mesh/status` and the
/// `SystemService.MeshStatus` RPC.
pub fn mesh_status_view(state: &MeshRouteState) -> MeshResult<Value> {
	let mut status = value_object(state.mesh.status()?)?;
	let mut self_status = status
		.remove("self")
		.and_then(|value| match value {
			Value::Object(object) => Some(object),
			_ => None,
		})
		.unwrap_or_default();
	self_status.insert("sandboxes".to_owned(), Value::Array(sandbox_status_rows(state)?));
	status.insert("self".to_owned(), Value::Object(self_status));
	status.insert("replicas_held".to_owned(), json!(state.replicas.list()?.len()));
	Ok(Value::Object(status))
}

async fn mesh_members(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	Ok(Json(map_mesh(state.mesh.register(body))?))
}

async fn mesh_heartbeat(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let state_body = match body.remove("state") {
		Some(Value::Object(object)) => object,
		_ => JsonObject::new(),
	};
	let known = match body.remove("known") {
		Some(Value::Array(items)) => items,
		_ => Vec::new(),
	};
	Ok(Json(map_mesh(state.mesh.heartbeat(state_body, known))?))
}

async fn mesh_depart(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	state
		.mesh
		.depart(&string_or(body.get("node_id"), String::new()));
	Ok(Json(json!({"ok": true})))
}

async fn mesh_reachable(
	State(state): State<MeshRouteState>,
	Path(node_id): Path<String>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, true)?;
	Ok(Json(json!({"reachable": state.mesh.is_peer_healthy(&node_id)})))
}

async fn mesh_locate(
	State(state): State<MeshRouteState>,
	Path(sandbox_id): Path<String>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, true)?;
	if state.engine.has_sandbox(&sandbox_id) {
		let (owner, epoch) = map_mesh(state.mesh.authoritative_owner(&sandbox_id))?
			.unwrap_or_else(|| (state.mesh.node_id(), state.mesh.local_epoch(&sandbox_id)));
		return Ok(Json(json!({"owner": owner, "epoch": epoch})));
	}
	if let Some((owner, epoch)) = map_mesh(state.mesh.authoritative_owner(&sandbox_id))? {
		return Ok(Json(json!({"owner": owner, "epoch": epoch})));
	}
	if let Some(record) = map_mesh(state.records.get(&sandbox_id))? {
		state
			.mesh
			.record_owner(&sandbox_id, &record.owner, record.epoch);
		return Ok(Json(json!({"owner": record.owner, "epoch": record.epoch})));
	}
	Err(MeshRouteError::detail(StatusCode::NOT_FOUND, "unknown sandbox"))
}

async fn mesh_owner(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let sid = string_or(body.get("sid"), String::new());
	let node_id = string_or(body.get("node_id"), String::new());
	let epoch = to_i64(body.get("epoch").cloned()).unwrap_or(0);
	if !sid.is_empty() && !node_id.is_empty() {
		state.mesh.record_owner(&sid, &node_id, epoch);
	}
	Ok(Json(json!({"ok": true})))
}

async fn mesh_lease_grant(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Grant).await
}

async fn mesh_lease_renew(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Renew).await
}

async fn mesh_lease_release(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	lease_vote(state, request, LeaseOp::Release).await
}

/// Live-migration phase 1 receiver: pull the bulk pre-copy checkpoint while
/// the source VM keeps running. Restore params and the final delta arrive
/// later via `/v1/mesh/migrate/receive`.
async fn mesh_migrate_precopy(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let digest = take_string(&mut body, "digest");
	let source_url = take_string(&mut body, "source_url");
	let name = take_string(&mut body, "name");
	if digest.is_empty() || source_url.is_empty() || name.is_empty() {
		return Err(MeshRouteError::detail(
			StatusCode::UNPROCESSABLE_ENTITY,
			"digest, source_url, and name are required",
		));
	}
	if state.engine.has_sandbox(&name) {
		return Err(MeshRouteError::coded(
			StatusCode::CONFLICT,
			"conflict",
			format!("sandbox {} already exists on the target node", py_repr(&name)),
		));
	}
	state
		.transfer
		.pull_checkpoint(
			state.transport.bulk_client(),
			source_url,
			digest,
			state.outbound_token.to_string(),
		)
		.await
		.map_err(|err| {
			MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"unreachable",
				format!("failed to pull migration pre-copy checkpoint: {err}"),
			)
		})?;
	Ok(Json(json!({"ok": true})))
}

pub(crate) async fn commit_target_migration(
	records: &dyn MeshRecordStore,
	engine: &dyn MeshEngine,
	leases: &dyn MeshLeaseManager,
	sid: String,
	epoch: i64,
	record: &mut CreateRecordWire,
	lease_records: Vec<Value>,
) -> MeshResult<()> {
	record
		.params
		.insert(MIGRATION_COMMITTED_FIELD.to_owned(), Value::Bool(true));
	if let Err(error) = records.update_exact(record.clone()) {
		// A target claim is not reversible by releasing writable volumes first:
		// the old candidate must be confirmed stopped before another owner can
		// use them.
		engine.teardown_candidate(sid, epoch).await?;
		leases.release_leases(lease_records).await?;
		return Err(error);
	}
	Ok(())
}

pub(crate) async fn persist_target_migration_leases(
	engine: &dyn MeshEngine,
	leases: &dyn MeshLeaseManager,
	sid: String,
	epoch: i64,
	lease_records: Vec<Value>,
) -> MeshResult<()> {
	if let Err(error) = engine.record_volume_leases(&sid, lease_records.clone()) {
		// A serving target without persisted leases cannot renew its ownership.
		// Tear down this uncommitted candidate before letting any retry proceed.
		let teardown = engine.teardown_candidate(sid, epoch).await;
		let release = leases.release_leases(lease_records).await;
		return Err(lease_persistence_error(error, teardown, release));
	}
	Ok(())
}

/// Live-migration phase 2 receiver: pull the final delta checkpoint, splice
/// it onto the locally installed pre-copy base, then restore and take
/// ownership of the sandbox.
async fn mesh_migrate_receive(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let digest = take_string(&mut body, "digest");
	let migration_id = take_string(&mut body, "migration_id");
	let base_digest = take_string(&mut body, "base_digest");
	let source_url = take_string(&mut body, "source_url");
	let source_owner = take_string(&mut body, "source_owner");
	let source_epoch = to_i64(body.remove("source_epoch")).unwrap_or(-1);
	let record = body
		.remove("record")
		.ok_or_else(|| MeshRouteError::invalid("migration record is required"))?;
	let mut record = CreateRecordWire::from_value(record).map_err(MeshRouteError::from)?;
	let params = match body.remove("params") {
		Some(Value::Object(params)) => params,
		_ => JsonObject::new(),
	};
	let name = string_or(params.get("name"), String::new());
	let epoch = to_i64(body.remove("epoch")).unwrap_or(0);
	let owner = state.mesh.node_id();
	if record.sid != name || record.owner != owner || record.epoch != epoch {
		return Err(MeshRouteError::invalid(
			"migration record does not match the target owner and epoch",
		));
	}
	if digest.is_empty()
		|| migration_id.is_empty()
		|| base_digest.is_empty()
		|| source_url.is_empty()
		|| name.is_empty()
		|| source_owner.is_empty()
		|| source_epoch < 0
	{
		return Err(MeshRouteError::detail(
			StatusCode::UNPROCESSABLE_ENTITY,
			"digest, migration_id, base_digest, source_url, source_owner, source_epoch, and \
			 params.name are required",
		));
	}
	record
		.params
		.insert(MIGRATION_SOURCE_OWNER_FIELD.to_owned(), Value::String(source_owner.clone()));
	record
		.params
		.insert(MIGRATION_SOURCE_EPOCH_FIELD.to_owned(), json!(source_epoch));
	if record
		.params
		.get("_mesh_migration_token")
		.and_then(Value::as_str)
		!= Some(migration_id.as_str())
	{
		return Err(MeshRouteError::invalid("migration token does not match the target record"));
	}
	// This guard is shared with background intent convergence.  A duplicate
	// receive returns a deterministic busy/pending response while the exact
	// token is being materialized; another token is fenced by the claim.
	let _receive_gate = map_mesh(
		state
			.records
			.try_begin_target_migration(&name, &migration_id),
	)?;
	map_mesh(state.records.begin_migration_handoff(
		&name,
		&source_owner,
		source_epoch,
		&record.owner,
		&migration_id,
	))?;
	record = map_mesh(state.records.claim_migration_handoff(
		source_owner.clone(),
		source_epoch,
		migration_id.clone(),
		record,
	))?;
	state
		.records
		.notify_source_migration_claim(&record)
		.await
		.map_err(|error| {
			MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"ambiguous",
				format!(
					"target intent persisted but source handoff confirmation failed: {}",
					error.message
				),
			)
		})?;
	if state.engine.has_sandbox(&name)
		&& record
			.params
			.get(MIGRATION_COMMITTED_FIELD)
			.and_then(Value::as_bool)
			== Some(true)
	{
		state
			.engine
			.migrate_activate_target(name.clone(), record.epoch)
			.await
			.map_err(MeshRouteError::from)?;
		return state
			.engine
			.get_view(&name)
			.map(Json)
			.map_err(MeshRouteError::from);
	}
	let base_dir = cas::lookup(&base_digest).ok().flatten().ok_or_else(|| {
		MeshRouteError::coded(
			StatusCode::CONFLICT,
			"missing_base",
			"pre-copy checkpoint is not installed on this node; restart the migration",
		)
	})?;
	let delta_dir = if let Some(path) = record
		.params
		.get(MIGRATION_DELTA_DIR_FIELD)
		.and_then(Value::as_str)
		.filter(|path| FsPath::new(path).is_dir())
	{
		PathBuf::from(path)
	} else {
		let installed = state
			.transfer
			.pull_checkpoint(
				state.transport.bulk_client(),
				source_url,
				digest.clone(),
				state.outbound_token.to_string(),
			)
			.await
			.map_err(|err| {
				MeshRouteError::coded(
					StatusCode::BAD_GATEWAY,
					"unreachable",
					format!("failed to pull migration delta checkpoint: {err}"),
				)
			})?;
		let delta_dir = PathBuf::from(
			state
				.engine
				.stage_migration_delta(
					record.sid.clone(),
					installed,
					base_dir.to_string_lossy().into_owned(),
				)
				.await
				.map_err(MeshRouteError::from)?,
		);
		record.params.insert(
			MIGRATION_DELTA_DIR_FIELD.to_owned(),
			Value::String(delta_dir.to_string_lossy().into_owned()),
		);
		// This is the durable target intent. It is written only after all
		// artifacts needed for a retry exist, and before lease/restore side effects.
		record = map_mesh(state.records.put(record))?;
		state
			.mesh
			.record_owner(&record.sid, &record.owner, record.epoch);
		delta_dir
	};

	let sid = record.sid.clone();
	let adopt_params = migration_restore_params(&record.params);
	let lease_records = map_mesh(
		state
			.leases
			.acquire_writable_volume_leases(adopt_params.clone(), record.epoch)
			.await,
	)?;
	if let Err(err) = state
		.engine
		.migrate_adopt_target(sid.clone(), delta_dir.to_string_lossy().into_owned(), adopt_params)
		.await
	{
		let _ = state.engine.teardown_candidate(sid, record.epoch).await;
		let _ = state.leases.release_leases(lease_records).await;
		return Err(MeshRouteError::from(err));
	}
	persist_target_migration_leases(
		&*state.engine,
		&*state.leases,
		sid.clone(),
		record.epoch,
		lease_records.clone(),
	)
	.await
	.map_err(MeshRouteError::from)?;
	map_mesh(
		commit_target_migration(
			&*state.records,
			&*state.engine,
			&*state.leases,
			sid.clone(),
			record.epoch,
			&mut record,
			lease_records,
		)
		.await,
	)?;
	state
		.engine
		.migrate_activate_target(sid.clone(), record.epoch)
		.await
		.map_err(MeshRouteError::from)?;
	let activated = map_mesh(state.engine.get_view(&sid))?;
	Ok(Json(apply_view_detail(activated, &record)))
}

async fn mesh_migrate_claim(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let sid = take_string(&mut body, "sid");
	let source_owner = take_string(&mut body, "source_owner");
	let source_epoch = to_i64(body.remove("source_epoch")).unwrap_or(-1);
	let target_owner = take_string(&mut body, "target_owner");
	let target_epoch = to_i64(body.remove("target_epoch")).unwrap_or(-1);
	let migration_id = take_string(&mut body, "migration_id");
	map_mesh(state.records.confirm_migration_handoff(
		&sid,
		&source_owner,
		source_epoch,
		&target_owner,
		target_epoch,
		&migration_id,
	))?;
	Ok(Json(json!({"ok": true})))
}

/// Return whether this exact migration token committed on the target.  The
/// source uses this after an ambiguous receive response and must not revive
/// its paused VM unless this endpoint proves the target did not commit.
async fn mesh_migrate_status(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let sid = take_string(&mut body, "sid");
	let migration_id = take_string(&mut body, "migration_id");
	let epoch = to_i64(body.remove("epoch")).unwrap_or(-1);
	if sid.is_empty() || migration_id.is_empty() || epoch < 0 {
		return Err(MeshRouteError::invalid("sid, migration_id, and epoch are required"));
	}
	let intent = state
		.records
		.get(&sid)
		.map_err(MeshRouteError::from)?
		.is_some_and(|record| {
			record.owner == state.mesh.node_id()
				&& record.epoch == epoch
				&& record
					.params
					.get(MIGRATION_ID_FIELD)
					.and_then(Value::as_str)
					== Some(migration_id.as_str())
		});
	let committed = intent
		&& state.engine.has_sandbox(&sid)
		&& state
			.records
			.get(&sid)
			.map_err(MeshRouteError::from)?
			.is_some_and(|record| {
				record
					.params
					.get(MIGRATION_COMMITTED_FIELD)
					.and_then(Value::as_bool)
					== Some(true)
			});
	let view = committed
		.then(|| state.engine.get_view(&sid))
		.transpose()
		.map_err(MeshRouteError::from)?;
	let state_name = if committed {
		"committed"
	} else if intent {
		"pending"
	} else {
		"absent"
	};
	Ok(Json(json!({"state": state_name, "committed": committed, "view": view})))
}

fn lease_persistence_error(
	primary: MeshError,
	teardown: MeshResult<()>,
	release: MeshResult<()>,
) -> MeshError {
	let mut diagnostics = Vec::new();
	if let Err(error) = teardown {
		diagnostics.push(format!("candidate teardown failed: {}", error.message));
	}
	if let Err(error) = release {
		diagnostics.push(format!("lease release failed: {}", error.message));
	}
	if diagnostics.is_empty() {
		primary
	} else {
		MeshError::with_code(
			format!("{}; cleanup: {}", primary.message, diagnostics.join("; ")),
			primary.code,
		)
	}
}

fn migration_restore_params(params: &JsonObject) -> JsonObject {
	params
		.iter()
		.filter(|(key, _)| !key.starts_with("_mesh_migration_"))
		.map(|(key, value)| (key.clone(), value.clone()))
		.collect()
}

async fn mesh_replica_receive(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let digest = take_string(&mut body, "digest");
	let object_key = take_string(&mut body, "object_key");
	let source_url = take_string(&mut body, "source_url");
	let source_node = take_string(&mut body, "source_node");
	let source_epoch = body.remove("source_epoch").and_then(|value| value.as_u64());
	let checkpoint_generation = body
		.remove("checkpoint_generation")
		.and_then(|value| value.as_u64());
	let params = match body.remove("params") {
		Some(Value::Object(params)) => params,
		_ => JsonObject::new(),
	};
	let name = string_or(params.get("name"), String::new());
	if digest.is_empty()
		|| source_url.is_empty()
		|| source_node.is_empty()
		|| source_epoch.is_none()
		|| checkpoint_generation.is_none()
		|| name.is_empty()
	{
		return Err(MeshRouteError::detail(
			StatusCode::UNPROCESSABLE_ENTITY,
			"digest, source_url, source_node, source_epoch, checkpoint_generation, and params.name \
			 are required",
		));
	}
	if state.engine.has_sandbox(&name) {
		return Ok(Json(json!({"ok": true, "skipped": "owner"})));
	}
	if let Some(current) = map_mesh(state.replicas.get(&name))?
		&& replica_metadata_matches(
			&current,
			&digest,
			&object_key,
			&source_node,
			source_epoch.expect("validated source epoch"),
			checkpoint_generation.expect("validated checkpoint generation"),
			&params,
		) {
		return Ok(Json(json!({"ok": true, "skipped": "current"})));
	}
	let installed = state
		.transfer
		.pull_snapshot(
			state.transport.bulk_client(),
			source_url,
			name.clone(),
			object_key.clone(),
			digest.clone(),
			state.outbound_token.to_string(),
		)
		.await
		.map_err(|err| {
			MeshRouteError::coded(
				StatusCode::BAD_GATEWAY,
				"unreachable",
				format!("failed to pull replica checkpoint: {err}"),
			)
		})?;
	map_mesh(
		state
			.replicas
			.put(
				name,
				object_key,
				digest,
				source_node,
				source_epoch.expect("validated source epoch"),
				checkpoint_generation.expect("validated checkpoint generation"),
				installed,
				params,
			)
			.await,
	)?;
	Ok(Json(json!({"ok": true})))
}

fn replica_metadata_matches(
	current: &ReplicaRecordWire,
	digest: &str,
	object_key: &str,
	source_node: &str,
	source_epoch: u64,
	checkpoint_generation: u64,
	params: &JsonObject,
) -> bool {
	current.digest == digest
		&& current.object_key == object_key
		&& current.source_node == source_node
		&& current.source_epoch == source_epoch
		&& current.checkpoint_generation == checkpoint_generation
		&& current.params == *params
}
#[derive(Deserialize)]
struct ReplicaSnapshotQuery {
	object_key: String,
}

async fn mesh_replica_snapshot(
	State(state): State<MeshRouteState>,
	Path((sid, digest)): Path<(String, String)>,
	Query(query): Query<ReplicaSnapshotQuery>,
	headers: HeaderMap,
) -> RouteResult<Response> {
	require_mesh_auth(&state, &headers, true)?;
	let export = map_mesh(
		state
			.engine
			.replica_export_path(&sid, &digest, &query.object_key),
	)?;
	let bundle = super::transfer::snapshot_bundle_for_path(export.path().to_path_buf(), &digest)
		.map_err(|error| MeshRouteError::coded(StatusCode::CONFLICT, "fenced", error.to_string()))?;
	let (tx, rx) = tokio::sync::mpsc::channel(16);
	tokio::task::spawn_blocking(move || {
		let _export = export;
		super::transfer::stream_bundle(&bundle, tx);
	});
	Response::builder()
		.header(header::CONTENT_TYPE, "application/octet-stream")
		.body(Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)))
		.map_err(|error| {
			MeshRouteError::coded(StatusCode::INTERNAL_SERVER_ERROR, "engine_error", error.to_string())
		})
}
async fn mesh_replica_list(
	State(state): State<MeshRouteState>,
	headers: HeaderMap,
) -> RouteResult<Json<Value>> {
	require_mesh_auth(&state, &headers, false)?;
	Ok(Json(json!({"sids": map_mesh(state.replicas.list())?})))
}

async fn mesh_record_put(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let record = CreateRecordWire::from_value(Value::Object(body)).map_err(MeshRouteError::from)?;
	let accepted = map_mesh(state.records.put_if_newer(record.clone()))?;
	if accepted {
		state
			.mesh
			.record_owner(&record.sid, &record.owner, record.epoch);
	}
	Ok(Json(json!({"ok": true})))
}

/// Remove only the candidate created by one exact owner/epoch/idempotency
/// reservation. This is peer-authenticated because it is a compensating
/// action for a coordinator that could not durably consume that reservation.
async fn mesh_candidate_teardown(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let mut body = json_object(request).await?;
	let sid = take_string(&mut body, "sid");
	let owner = take_string(&mut body, "owner");
	let idempotency_key = take_string(&mut body, "idempotency_key");
	let epoch = to_i64(body.remove("epoch")).unwrap_or(-1);
	if sid.is_empty() || owner != state.mesh.node_id() || idempotency_key.is_empty() || epoch < 0 {
		return Err(MeshRouteError::invalid(
			"sid, local owner, non-negative epoch, and idempotency_key are required",
		));
	}
	let candidate = state
		.engine
		.find_by_idempotency_key(&idempotency_key)
		.map_err(MeshRouteError::from)?;
	let exact_candidate = candidate.as_ref().is_some_and(|view| {
		sid_from_view(view).as_deref() == Some(sid.as_str())
			&& view
				.get("detail")
				.and_then(Value::as_object)
				.and_then(|detail| detail.get("_mesh_create_epoch"))
				.and_then(Value::as_i64)
				== Some(epoch)
	});
	if !exact_candidate {
		return Err(MeshRouteError::coded(
			StatusCode::CONFLICT,
			"conflict",
			"candidate does not match the requested idempotency reservation",
		));
	}
	state
		.engine
		.teardown_candidate(sid.clone(), epoch)
		.await
		.map_err(MeshRouteError::from)?;
	state
		.leases
		.release_record_volume_leases(sid)
		.await
		.map_err(MeshRouteError::from)?;
	Ok(Json(json!({"ok": true})))
}

async fn mesh_idem_sandbox_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Sandbox, IdemRole::Coordinate).await
}

async fn mesh_idem_sandbox_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Sandbox, IdemRole::Work).await
}

async fn mesh_idem_run_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Run, IdemRole::Coordinate).await
}

async fn mesh_idem_run_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Run, IdemRole::Work).await
}

async fn mesh_idem_restore_coordinate(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Restore, IdemRole::Coordinate).await
}

async fn mesh_idem_restore_work(
	State(state): State<MeshRouteState>,
	request: Request<Body>,
) -> RouteResult<Json<Value>> {
	idem_route(state, request, CreateKind::Restore, IdemRole::Work).await
}

#[derive(Clone, Copy)]
enum LeaseOp {
	Grant,
	Renew,
	Release,
}

async fn lease_vote(
	state: MeshRouteState,
	request: Request<Body>,
	op: LeaseOp,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let volume = string_or(body.get("volume"), String::new());
	let holder_node = string_or(body.get("holder_node"), String::new());
	let epoch = to_i64(body.get("epoch").cloned()).unwrap_or(0);
	let ttl = to_f64(body.get("ttl").cloned()).unwrap_or(0.0);
	let mut decision = match op {
		LeaseOp::Grant => state.leases.vote_grant(volume, holder_node, epoch, ttl),
		LeaseOp::Renew => state.leases.vote_renew(volume, holder_node, epoch, ttl),
		LeaseOp::Release => state.leases.vote_release(volume, holder_node, epoch),
	}
	.map_err(|err| {
		MeshRouteError::coded(StatusCode::UNPROCESSABLE_ENTITY, "invalid", err.message)
	})?;
	if matches!(op, LeaseOp::Release) {
		let released = decision
			.get("granted")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		decision.insert("released".to_owned(), Value::Bool(released));
	}
	Ok(Json(Value::Object(decision)))
}

#[derive(Clone, Copy)]
enum IdemRole {
	Coordinate,
	Work,
}

async fn idem_route(
	state: MeshRouteState,
	request: Request<Body>,
	kind: CreateKind,
	role: IdemRole,
) -> RouteResult<Json<Value>> {
	let headers = request.headers().clone();
	require_mesh_auth(&state, &headers, true)?;
	let body = json_object(request).await?;
	let (key, mut params) = idem_payload(body).map_err(MeshRouteError::from)?;
	if matches!(kind, CreateKind::Run | CreateKind::Restore) {
		params.insert("detach".to_owned(), Value::Bool(true));
	}
	let value = match role {
		IdemRole::Coordinate => coordinate_create(&state, kind, &key, params).await,
		IdemRole::Work => worker_create(&state, kind, &key, params).await,
	};
	Ok(Json(map_mesh(value)?))
}

#[derive(Clone, Copy)]
enum CreateKind {
	Sandbox,
	Run,
	Restore,
}

impl CreateKind {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Sandbox => "sandbox",
			Self::Run => "run",
			Self::Restore => "restore",
		}
	}

	const fn work_path(self) -> &'static str {
		match self {
			Self::Sandbox => "/v1/mesh/idem/sandboxes/work",
			Self::Run => "/v1/mesh/idem/run/work",
			Self::Restore => "/v1/mesh/idem/restore/work",
		}
	}

	const fn coordinate_path(self) -> &'static str {
		match self {
			Self::Sandbox => "/v1/mesh/idem/sandboxes/coordinate",
			Self::Run => "/v1/mesh/idem/run/coordinate",
			Self::Restore => "/v1/mesh/idem/restore/coordinate",
		}
	}

	const fn no_owner_message(self) -> &'static str {
		match self {
			Self::Sandbox => "no reachable owner for idempotent create",
			Self::Run => "no reachable owner for idempotent run",
			Self::Restore => "no reachable owner for idempotent restore",
		}
	}
}

async fn idempotent_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: String,
	params: JsonObject,
) -> MeshResult<Value> {
	if !state.mesh.enabled() {
		return worker_create(state, kind, &idem_key, params).await;
	}
	if state.mesh.pinned_local(&params) {
		let prepared = prepare_create_params(state, params, kind)?;
		let view = worker_create(state, kind, &idem_key, prepared.clone()).await?;
		let sid = sid_from_view(&view).unwrap_or_default();
		return commit_create_record(
			state,
			kind,
			view,
			state.mesh.node_id(),
			state.mesh.local_epoch(&sid),
			idem_key,
			prepared,
		)
		.await;
	}
	let coordinator = state.mesh.coordinator_for(&idem_key);
	if coordinator == state.mesh.node_id() {
		return coordinate_create(state, kind, &idem_key, params).await;
	}
	let Some(peer_url) = state.mesh.peer_url(&coordinator) else {
		state.mesh.mark_unhealthy(&coordinator);
		return coordinate_create(state, kind, &idem_key, params).await;
	};
	let payload = json!({"key": idem_key, "params": params});
	match state
		.transport
		.post(&peer_url, kind.coordinate_path(), &payload, Some(state.config.create_timeout))
		.await
	{
		Ok(view) => Ok(Value::Object(view)),
		Err(err) if err.code == "unreachable" => {
			state.mesh.mark_unhealthy(&coordinator);
			coordinate_create(state, kind, &idem_key, value_object(payload["params"].clone())?).await
		},
		Err(err) => Err(err),
	}
}

async fn coordinate_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: &str,
	params: JsonObject,
) -> MeshResult<Value> {
	let params = prepare_create_params(state, params, kind)?;
	if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
		let sid = sid_from_view(&existing).unwrap_or_default();
		return commit_create_record(
			state,
			kind,
			view_with_ha(existing, &params),
			state.mesh.node_id(),
			state.mesh.local_epoch(&sid),
			idem_key.to_owned(),
			params,
		)
		.await;
	}
	for _ in 0..std::cmp::max(1, state.mesh.peers().len() + 1) {
		let owner = state.mesh.idem_pin(idem_key, &state.mesh.place(&params)?);
		if owner == state.mesh.node_id() {
			let view = worker_create(state, kind, idem_key, params.clone()).await?;
			return commit_create_record(state, kind, view, owner, 0, idem_key.to_owned(), params)
				.await;
		}
		let Some(peer_url) = state.mesh.peer_url(&owner) else {
			state.mesh.mark_unhealthy(&owner);
			state.mesh.idem_unpin(idem_key);
			continue;
		};
		let payload = json!({"key": idem_key, "params": params});
		match state
			.transport
			.post(&peer_url, kind.work_path(), &payload, Some(state.config.create_timeout))
			.await
		{
			Ok(view) => {
				return commit_create_record(
					state,
					kind,
					Value::Object(view),
					owner,
					0,
					idem_key.to_owned(),
					params,
				)
				.await;
			},
			Err(err) if err.code == "unreachable" => {
				state.mesh.mark_unhealthy(&owner);
				state.mesh.idem_unpin(idem_key);
			},
			Err(err) => return Err(err),
		}
	}
	Err(MeshError::unreachable(kind.no_owner_message()))
}

async fn worker_create(
	state: &MeshRouteState,
	kind: CreateKind,
	idem_key: &str,
	params: JsonObject,
) -> MeshResult<Value> {
	let params = prepare_create_params(state, params, kind)?;
	if let Some(existing) = state.engine.find_by_idempotency_key(idem_key)? {
		return Ok(existing);
	}
	if !state.mesh.worker_begin(idem_key) {
		return Err(MeshError::conflict("idempotent create already in progress"));
	}
	let result = async {
		let name = string_or(params.get("name"), String::new());
		if !name.is_empty()
			&& (state.engine.has_sandbox(&name)
				|| state.mesh.owner_of(&name)?.is_some()
				|| scatter_locate(state, &name).await?.is_some())
		{
			return Err(MeshError::conflict(format!("sandbox {} already exists", py_repr(&name))));
		}
		let epoch = state
			.records
			.reserve_create_epoch(&name, &state.mesh.node_id(), idem_key)?;
		let view = local_create_view(state, kind, params.clone(), epoch).await?;
		if let Some(sid) = sid_from_view(&view) {
			if let Err(error) = state.engine.record_create_epoch(&sid, epoch) {
				cleanup_uncommitted_create(state, sid, epoch).await;
				return Err(error);
			}
			if let Err(error) = state.engine.record_idempotency(&sid, idem_key) {
				cleanup_uncommitted_create(state, sid, epoch).await;
				return Err(error);
			}
			let current = match state.engine.get_view(&sid) {
				Ok(current) => current,
				Err(error) => {
					cleanup_uncommitted_create(state, sid, epoch).await;
					return Err(error);
				},
			};
			let mut view = view_with_ha(current, &params);
			if let Some(object) = view.as_object_mut() {
				object.insert("_mesh_create_epoch".to_owned(), Value::from(epoch));
			}
			return Ok(view);
		}
		Ok(view)
	}
	.await;
	state.mesh.worker_end(idem_key);
	result
}

async fn local_create_view(
	state: &MeshRouteState,
	kind: CreateKind,
	mut params: JsonObject,
	epoch: i64,
) -> MeshResult<Value> {
	match kind {
		CreateKind::Sandbox => {
			if state.mesh.enabled()
				&& let Some(key) = state.mesh.request_template_key(&params)
				&& !state.mesh.has_local_template_key(&key)
				&& let Some((peer_url, digest)) = state.mesh.find_template_provider(&key)
			{
				match state
					.transfer
					.pull_template_metadata(
						state.transport.bulk_client(),
						peer_url.clone(),
						digest.clone(),
						state.outbound_token.to_string(),
					)
					.await
				{
					Ok(installed) => {
						params.insert("template".to_owned(), Value::String(installed.template));
						params.insert(
							"remote_page_url".to_owned(),
							Value::String(installed.remote_page_url),
						);
						params.insert(
							"remote_page_token".to_owned(),
							Value::String(state.outbound_token.to_string()),
						);
						params.insert("remote_page_digest".to_owned(), Value::String(installed.digest));
					},
					Err(_) => {
						if let Ok(installed) = state
							.transfer
							.pull_template(
								state.transport.bulk_client(),
								peer_url,
								digest,
								state.outbound_token.to_string(),
							)
							.await
						{
							params.insert("template".to_owned(), Value::String(installed));
						}
					},
				}
			}
			let lease_records = state
				.leases
				.acquire_writable_volume_leases(params.clone(), epoch)
				.await?;
			let record = match state.engine.create_sandbox(params).await {
				Ok(view) => view,
				Err(err) => {
					let _ = state.leases.release_leases(lease_records).await;
					return Err(err);
				},
			};
			if let Some(sid) = sid_from_view(&record) {
				if let Err(error) = state
					.engine
					.record_volume_leases(&sid, lease_records.clone())
				{
					let teardown = state.engine.teardown_candidate(sid, epoch).await;
					let release = state.leases.release_leases(lease_records).await;
					return Err(lease_persistence_error(error, teardown, release));
				}
				state.mesh.request_replication();
			}
			Ok(record)
		},
		CreateKind::Run => {
			params.insert("detach".to_owned(), Value::Bool(true));
			let view = state.engine.run_detached(params.clone()).await?;
			Ok(view_with_ha(view, &params))
		},
		CreateKind::Restore => {
			params.insert("detach".to_owned(), Value::Bool(true));
			let view = state.engine.restore_detached(params.clone()).await?;
			Ok(view_with_ha(view, &params))
		},
	}
}

/// A local candidate is never allowed to outlive a failed reservation consume
/// or local idempotency publication. Teardown precedes releasing its persisted
/// lease records so another owner cannot race a still-live guest.
async fn cleanup_uncommitted_create(state: &MeshRouteState, sid: String, epoch: i64) {
	let _ = state.engine.teardown_candidate(sid.clone(), epoch).await;
	let _ = state.leases.release_record_volume_leases(sid).await;
}

fn matches_create_record(
	record: Option<CreateRecordWire>,
	sid: &str,
	owner: &str,
	epoch: i64,
	idempotency_key: &str,
) -> bool {
	record.is_some_and(|record| {
		record.sid == sid
			&& record.owner == owner
			&& record.epoch == epoch
			&& record.idempotency_key == idempotency_key
	})
}

async fn cleanup_failed_create_commit(
	state: &MeshRouteState,
	sid: String,
	owner: String,
	epoch: i64,
	idempotency_key: String,
) -> MeshResult<()> {
	if owner != state.mesh.node_id() {
		let peer_url = state.mesh.peer_url(&owner).ok_or_else(|| {
			MeshError::unreachable("create owner is unreachable for candidate teardown")
		})?;
		state
			.transport
			.post(
				&peer_url,
				"/v1/mesh/candidate/teardown",
				&json!({
					"sid": sid,
					"owner": owner,
					"epoch": epoch,
					"idempotency_key": idempotency_key,
				}),
				Some(state.config.create_timeout),
			)
			.await?;
		return Ok(());
	}

	let candidate = state.engine.find_by_idempotency_key(&idempotency_key)?;
	let exact_candidate = candidate.as_ref().is_some_and(|view| {
		sid_from_view(view).as_deref() == Some(sid.as_str())
			&& view
				.get("detail")
				.and_then(Value::as_object)
				.and_then(|detail| detail.get("_mesh_create_epoch"))
				.and_then(Value::as_i64)
				== Some(epoch)
	});
	if !exact_candidate {
		return Err(MeshError::conflict("candidate does not match failed create reservation"));
	}
	state.engine.teardown_candidate(sid.clone(), epoch).await?;
	state.leases.release_record_volume_leases(sid).await
}

async fn commit_create_record(
	state: &MeshRouteState,
	kind: CreateKind,
	mut view: Value,
	owner: String,
	epoch: i64,
	idem_key: String,
	params: JsonObject,
) -> MeshResult<Value> {
	let reserved_epoch = view
		.as_object_mut()
		.and_then(|object| object.remove("_mesh_create_epoch"))
		.and_then(|value| value.as_i64())
		.unwrap_or(epoch);
	let Some(sid) = sid_from_view(&view) else {
		return Ok(view);
	};
	if !state.mesh.enabled() {
		return Ok(view_with_ha(view, &params));
	}
	let record = make_create_record(
		sid.clone(),
		owner.clone(),
		reserved_epoch,
		idem_key.clone(),
		params,
		kind,
	);
	let record = match replicate_record(state, record, true).await {
		Ok(record) => record,
		Err(error) => {
			let Ok(current) = state.records.get(&sid) else {
				return Err(error);
			};
			if matches_create_record(current, &sid, &owner, reserved_epoch, &idem_key) {
				return Err(error);
			}
			let _ = cleanup_failed_create_commit(state, sid, owner, reserved_epoch, idem_key).await;
			return Err(error);
		},
	};
	state.mesh.request_replication();
	Ok(apply_view_detail(view, &record))
}

async fn replicate_record(
	state: &MeshRouteState,
	record: CreateRecordWire,
	require_quorum: bool,
) -> MeshResult<CreateRecordWire> {
	let record = state.records.put(record)?;
	state
		.mesh
		.record_owner(&record.sid, &record.owner, record.epoch);
	let mut acks = 1usize;
	let mut live_peer_acks_needed = 0usize;
	for peer in state.mesh.peers() {
		if peer.healthy {
			live_peer_acks_needed += 1;
		}
		let posted = state
			.transport
			.post(
				&peer.advertise,
				"/v1/mesh/record/put",
				&record.to_value(),
				Some(state.config.create_timeout),
			)
			.await;
		if posted.is_ok() {
			acks += 1;
		}
	}
	let expected = state.mesh.expected_members();
	let needed = if expected <= 2 {
		1 + live_peer_acks_needed
	} else {
		state.mesh.quorum_needed()
	};
	if require_quorum && acks < needed {
		return Err(MeshError::with_code(
			format!(
				"create record for {} replicated to {acks}/{needed} required members",
				py_repr(&record.sid)
			),
			"record_unreplicated",
		));
	}
	Ok(record)
}

/// Live ("teleport") migration: stream the bulk checkpoint while the source
/// keeps running, then suspend, ship only the RAM/disk delta, and resume on
/// the target. Downtime is bounded by the delta size, not the full image.
///
/// The returned view carries a `migration` object with `precopy_ms` (source
/// kept running), `downtime_ms` (suspend until the target VM runs), and
/// `total_ms`.
pub(crate) async fn migrate_sandbox_to(
	state: &MeshRouteState,
	sandbox_id: String,
	target: String,
) -> RouteResult<Value> {
	// This permit spans both pre-copy and the blackout/journal terminal path.
	// A second target therefore fails before it can pause, journal, or abort
	// the first migration.
	let _migration_guard = map_mesh(state.engine.try_begin_migration(&sandbox_id))?;
	let started = Instant::now();
	let peer = map_mesh(state.mesh.migration_target(&target))?;
	let mut record = map_mesh(state.records.get(&sandbox_id))?
		.ok_or_else(|| MeshRouteError::detail(StatusCode::NOT_FOUND, "unknown sandbox"))?;
	// Phase 1 (pre-copy): checkpoint the running sandbox and let the target
	// pull the bulk RAM+disk image; the source resumes right after the dump.
	let precopy = map_mesh(state.engine.migrate_precopy(sandbox_id.clone()).await)?;
	let precopy_body = json!({
		"digest": precopy.digest,
		"source_url": state.mesh.advertise(),
		"name": sandbox_id,
	});
	if let Err(err) = state
		.transport
		.post(
			&peer.advertise,
			"/v1/mesh/migrate/precopy",
			&precopy_body,
			Some(state.config.create_timeout),
		)
		.await
	{
		let _ = state
			.engine
			.checkpoint_discard(precopy.snapshot_dir.clone(), precopy.digest.clone())
			.await;
		return Err(MeshRouteError::coded(
			StatusCode::BAD_GATEWAY,
			"unreachable",
			format!("migration pre-copy to {target} failed (source unaffected): {}", err.message),
		));
	}
	let precopy_ms = started.elapsed().as_millis() as u64;
	// Everything from here until the target confirms the restore is guest
	// blackout: finalize pauses the VM as its first effect.
	let blackout = Instant::now();
	// Phase 2 (suspend + delta): pause the source, capture what changed since
	// the pre-copy, and stop it exactly at the delta. A finalize failure
	// leaves the source running.
	let source_owner = record.owner.clone();
	let source_epoch = record.epoch;
	let epoch =
		map_mesh(state.mesh.authoritative_owner(&sandbox_id))?.map_or(0, |(_, epoch)| epoch) + 1;
	let migration_id = uuid::Uuid::new_v4().simple().to_string();
	let cleanup_context = MigrationCleanupWire {
		sid: sandbox_id.clone(),
		base_dir: precopy.snapshot_dir.clone(),
		base_digest: precopy.digest.clone(),
		delta_dir: String::new(),
		delta_digest: String::new(),
		target_url: peer.advertise.clone(),
		migration_id: migration_id.clone(),
		epoch,
		schema_version: 1,
		source_owner: source_owner.clone(),
		source_epoch,
		target_owner: target.clone(),
		target_epoch: epoch,
		token: migration_id.clone(),
		phase: "prepared".to_owned(),
	};
	let fin = match state
		.engine
		.migrate_finalize(sandbox_id.clone(), precopy.snapshot_dir.clone(), cleanup_context)
		.await
	{
		Ok(fin) => fin,
		Err(err) => {
			// Finalization may have durably journaled the paused delta before a
			// later teardown/status failure.  Retaining the pre-copy base is
			// therefore the only safe response; startup recovery will either
			// consume the journal or staging GC will reclaim both artifacts.
			return Err(MeshRouteError::from(err));
		},
	};
	let cleanup = fin.cleanup.clone().ok_or_else(|| {
		MeshRouteError::invalid("migration finalizer did not persist recovery journal")
	})?;
	record.params.clone_from(&fin.params);
	record
		.params
		.insert(MIGRATION_ID_FIELD.to_owned(), Value::String(migration_id.clone()));
	record
		.params
		.insert(MIGRATION_BASE_DIGEST_FIELD.to_owned(), Value::String(precopy.digest.clone()));
	record
		.params
		.insert(MIGRATION_DELTA_DIGEST_FIELD.to_owned(), Value::String(fin.digest.clone()));
	record
		.params
		.insert(MIGRATION_SOURCE_URL_FIELD.to_owned(), Value::String(state.mesh.advertise()));
	record
		.params
		.insert("_mesh_migration_token".to_owned(), Value::String(migration_id.clone()));
	record
		.params
		.insert(MIGRATION_SOURCE_OWNER_FIELD.to_owned(), Value::String(source_owner.clone()));
	record
		.params
		.insert(MIGRATION_SOURCE_EPOCH_FIELD.to_owned(), json!(source_epoch));
	record.owner.clone_from(&target);
	record.epoch = epoch;
	if let Err(error) = state.records.begin_migration_handoff(
		&sandbox_id,
		&source_owner,
		source_epoch,
		&target,
		&migration_id,
	) {
		// A begin error is ambiguous.  Only the exact abort CAS can prove this
		// token remains unclaimed and install the fresh source epoch.
		match state.records.abort_migration_handoff(
			&sandbox_id,
			&source_owner,
			source_epoch,
			&target,
			&migration_id,
		) {
			Ok(Some(fresh)) => {
				state
					.engine
					.migrate_abort(
						sandbox_id.clone(),
						precopy.digest.clone(),
						fin.snapshot_dir.clone(),
						fin.digest.clone(),
						fin.params.clone(),
					)
					.await
					.map_err(MeshRouteError::from)?;
				state
					.mesh
					.record_owner(&sandbox_id, &fresh.owner, fresh.epoch);
				map_mesh(state.engine.migration_cleanup_remove(cleanup.sid.clone()))?;
				return Err(MeshRouteError::from(error));
			},
			Ok(None) => {
				state.mesh.forget_owner(&sandbox_id);
				return Err(MeshRouteError::from(error));
			},
			Err(abort) => {
				state.mesh.forget_owner(&sandbox_id);
				return Err(MeshRouteError::from(abort));
			},
		}
	}
	// Only a durable authorization marker makes the paused source
	// non-serving; the target may now consume the exact token.
	state.mesh.forget_owner(&sandbox_id);
	let receive = json!({
		"digest": fin.digest,
		"base_digest": precopy.digest,
		"source_url": state.mesh.advertise(),
		"params": fin.params.clone(),
		"record": record.to_value(),
		"epoch": epoch,
		"migration_id": migration_id.clone(),
		"source_owner": source_owner,
		"source_epoch": source_epoch,
	});
	let mut view = match state
		.transport
		.post(
			&peer.advertise,
			"/v1/mesh/migrate/receive",
			&receive,
			Some(state.config.create_timeout),
		)
		.await
	{
		Ok(view) => Value::Object(view),
		Err(err) => {
			let status = state
				.transport
				.post(
					&peer.advertise,
					"/v1/mesh/migrate/status",
					&json!({
						"sid": sandbox_id.clone(),
						"epoch": epoch,
						"migration_id": migration_id.clone(),
					}),
					Some(state.config.create_timeout),
				)
				.await;
			match status {
				Ok(mut status) if status.get("committed").and_then(Value::as_bool) == Some(true) => {
					status
						.remove("view")
						.unwrap_or_else(|| Value::Object(JsonObject::new()))
				},
				Ok(status) if status.get("state").and_then(Value::as_str) == Some("absent") => {
					// The target explicitly denies this token.  Persist the
					// recovery phase before changing ownership, then let the
					// exact CAS prove the target never consumed it.
					let mut abort_cleanup = cleanup.clone();
					"abort_pending".clone_into(&mut abort_cleanup.phase);
					map_mesh(state.engine.migration_cleanup_persist(abort_cleanup))?;
					let fresh = match state.records.abort_migration_handoff(
						&sandbox_id,
						&cleanup.source_owner,
						cleanup.source_epoch,
						&cleanup.target_owner,
						&cleanup.migration_id,
					) {
						Ok(Some(fresh)) => fresh,
						Ok(None) => {
							return Err(MeshRouteError::coded(
								StatusCode::BAD_GATEWAY,
								"unreachable",
								"target migration status changed while recovering source",
							));
						},
						Err(error) => return Err(MeshRouteError::from(error)),
					};
					let mut restore_cleanup = cleanup.clone();
					"restore_pending".clone_into(&mut restore_cleanup.phase);
					map_mesh(state.engine.migration_cleanup_persist(restore_cleanup))?;
					state
						.engine
						.migrate_abort(
							sandbox_id.clone(),
							precopy.digest.clone(),
							fin.snapshot_dir.clone(),
							fin.digest.clone(),
							fin.params.clone(),
						)
						.await
						.map_err(MeshRouteError::from)?;
					state
						.mesh
						.record_owner(&sandbox_id, &fresh.owner, fresh.epoch);
					map_mesh(state.engine.migration_cleanup_remove(cleanup.sid.clone()))?;
					return Err(MeshRouteError::coded(
						StatusCode::BAD_GATEWAY,
						"unreachable",
						format!(
							"migration to {target} was rejected before target intent: {}",
							err.message
						),
					));
				},
				Ok(_) => {
					// The target durably accepted this exact token but had not
					// acknowledged its restore. Retry the same receive (never a
					// fresh migration) so a restarted target can adopt the intent.
					let mut adopted = None;
					for _ in 0..3 {
						tokio::time::sleep(Duration::from_millis(100)).await;
						if let Ok(view) = state
							.transport
							.post(
								&peer.advertise,
								"/v1/mesh/migrate/receive",
								&receive,
								Some(state.config.create_timeout),
							)
							.await
						{
							adopted = Some(view);
							break;
						}
					}
					let Some(view) = adopted else {
						return Err(MeshRouteError::coded(
							StatusCode::BAD_GATEWAY,
							"ambiguous",
							format!(
								"migration to {target} has durable target intent but no confirmation yet: \
								 {}",
								err.message
							),
						));
					};
					Value::Object(view)
				},
				Err(status_err) => {
					return Err(MeshRouteError::coded(
						StatusCode::BAD_GATEWAY,
						"ambiguous",
						format!(
							"migration to {target} receive response was ambiguous and target commit \
							 could not be resolved: {}; status: {}",
							err.message, status_err.message
						),
					));
				},
			}
		},
	};
	let downtime_ms = blackout.elapsed().as_millis() as u64;
	if let Err(err) = replicate_record(state, record, false).await {
		tracing::warn!(error = %err, "target migration record replication deferred after commit");
	}
	// The pre-receive record is now a committed-cleanup record. Rewriting it is
	// harmless and keeps retry metadata durable if a prior write was interrupted.
	let leases_clean = state
		.leases
		.release_record_volume_leases(sandbox_id)
		.await
		.map_err(MeshRouteError::from);
	let artifacts_clean = state
		.engine
		.migrate_cleanup_committed(
			cleanup.sid.clone(),
			cleanup.base_dir.clone(),
			cleanup.base_digest.clone(),
			cleanup.delta_dir.clone(),
			cleanup.delta_digest.clone(),
		)
		.await;
	if leases_clean.is_ok() && artifacts_clean.is_ok() {
		if let Err(err) = state.engine.migration_cleanup_remove(cleanup.sid.clone()) {
			tracing::warn!(sid = %cleanup.sid, error = %err, "retaining completed migration cleanup");
		} else {
			state.mesh.forget_owner(&cleanup.sid);
		}
	} else {
		tracing::warn!(
			sid = %cleanup.sid,
			lease_error = ?leases_clean.err(),
			artifact_error = ?artifacts_clean.err(),
			"deferring committed migration cleanup"
		);
	}
	if let Value::Object(map) = &mut view {
		map.insert(
			"migration".to_owned(),
			json!({
				"precopy_ms": precopy_ms,
				"downtime_ms": downtime_ms,
				"total_ms": started.elapsed().as_millis() as u64,
			}),
		);
	}
	Ok(view)
}

fn drain_target(state: &MeshRouteState) -> Option<String> {
	for peer in state.mesh.peers() {
		if state.mesh.migration_target(&peer.node_id).is_ok() {
			return Some(peer.node_id);
		}
	}
	None
}

async fn scatter_locate(state: &MeshRouteState, sandbox_id: &str) -> MeshResult<Option<String>> {
	for peer in state.mesh.peers() {
		let path = format!("/v1/mesh/locate/{sandbox_id}");
		let Ok(response) = state.transport.get(&peer.advertise, &path, None).await else {
			continue;
		};
		let owner = response
			.get("owner")
			.and_then(value_string)
			.filter(|owner| !owner.is_empty());
		if let Some(owner) = owner {
			let epoch = to_i64(response.get("epoch").cloned()).unwrap_or(0);
			state.mesh.record_owner(sandbox_id, &owner, epoch);
			return Ok(Some(owner));
		}
	}
	if let Some(record) = state.records.get(sandbox_id)? {
		state
			.mesh
			.record_owner(sandbox_id, &record.owner, record.epoch);
		return Ok(Some(record.owner));
	}
	Ok(None)
}

fn make_create_record(
	sid: String,
	owner: String,
	epoch: i64,
	idempotency_key: String,
	mut params: JsonObject,
	kind: CreateKind,
) -> CreateRecordWire {
	params.remove("idempotency_key");
	params.insert("_kind".to_owned(), Value::String(kind.as_str().to_owned()));
	let ha = string_or(params.get("ha"), "off".to_owned());
	let restart_policy =
		string_or(params.get("restart_policy"), restart_policy_for_ha(&ha).to_owned());
	params.insert("restart_policy".to_owned(), Value::String(restart_policy.clone()));
	CreateRecordWire {
		sid,
		params,
		owner,
		epoch,
		idempotency_key,
		ha,
		restart_policy,
		created_at: unix_now(),
	}
}

/// Adds authoritative mesh placement fields to an engine-owned sandbox view.
pub(crate) fn apply_view_detail(view: Value, record: &CreateRecordWire) -> Value {
	let mut object = match view {
		Value::Object(object) => object,
		other => return other,
	};
	object.insert("ha".to_owned(), Value::String(record.ha.clone()));
	object.insert("node".to_owned(), Value::String(record.owner.clone()));
	object.insert("restart_policy".to_owned(), Value::String(record.restart_policy.clone()));
	Value::Object(object)
}

fn view_with_ha(view: Value, params: &JsonObject) -> Value {
	let mut object = match view {
		Value::Object(object) => object,
		other => return other,
	};
	object.insert("ha".to_owned(), Value::String(string_or(params.get("ha"), "off".to_owned())));
	object.insert(
		"restart_policy".to_owned(),
		Value::String(string_or(params.get("restart_policy"), "none".to_owned())),
	);
	Value::Object(object)
}

fn prepare_create_params(
	state: &MeshRouteState,
	mut params: JsonObject,
	kind: CreateKind,
) -> MeshResult<JsonObject> {
	params.remove("idempotency_key");
	if params
		.get("name")
		.and_then(value_string)
		.unwrap_or_default()
		.is_empty()
	{
		params
			.insert("name".to_owned(), Value::String(format!("sb-{}", uuid::Uuid::new_v4().simple())));
	}
	if state.mesh.enabled() && py_truthy(params.get("fs_dir")) {
		return Err(MeshError::invalid(
			"host-local fs_dir cannot be placed or protected; use a volume",
		));
	}
	apply_ha_defaults(state, &mut params)?;
	params.insert("_kind".to_owned(), Value::String(kind.as_str().to_owned()));
	Ok(params)
}

fn apply_ha_defaults(state: &MeshRouteState, params: &mut JsonObject) -> MeshResult<()> {
	let raw = params.get("ha").and_then(value_string).unwrap_or_default();
	let ha = if raw.is_empty() {
		if state.mesh.enabled() {
			state.config.default_ha_meshed.clone()
		} else {
			state.config.default_ha_local.clone()
		}
	} else {
		raw
	};
	if !ALLOWED_HA.contains(&ha.as_str()) {
		return Err(MeshError::invalid("ha must be one of: async, async+rerun, off, rerun"));
	}
	params.insert("ha".to_owned(), Value::String(ha.clone()));
	params.insert("restart_policy".to_owned(), Value::String(restart_policy_for_ha(&ha).to_owned()));
	Ok(())
}

fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

fn idem_payload(mut body: JsonObject) -> MeshResult<(String, JsonObject)> {
	let key = take_string(&mut body, "key").trim().to_owned();
	if key.is_empty() {
		return Err(MeshError::new("missing idempotency key"));
	}
	let params = match body.remove("params") {
		Some(Value::Object(mut params)) => {
			params.remove("idempotency_key");
			params
		},
		_ => return Err(MeshError::new("missing idempotency params")),
	};
	Ok((key, params))
}

fn sandbox_status_rows(state: &MeshRouteState) -> MeshResult<Vec<Value>> {
	let mut rows = Vec::new();
	for view in state.engine.list_views()? {
		let Value::Object(object) = view else {
			continue;
		};
		if object.get("status").and_then(value_string).as_deref() == Some("terminated") {
			continue;
		}
		let sid = object
			.get("id")
			.or_else(|| object.get("name"))
			.and_then(value_string)
			.unwrap_or_default();
		let ha = object
			.get("ha")
			.and_then(value_string)
			.unwrap_or_else(|| "off".to_owned());
		let restart_policy = object
			.get("restart_policy")
			.and_then(value_string)
			.unwrap_or_else(|| "none".to_owned());
		let checkpoint_age = if ha == "off" {
			None
		} else {
			state.engine.checkpoint_age_sec(&sid)
		};
		let replicas = if ha == "off" {
			Vec::new()
		} else {
			state.mesh.replica_targets(&sid, state.config.replicas)
		};
		rows.push(json!({
			"id": sid,
			"ha": ha,
			"restart_policy": restart_policy,
			"checkpoint_age_sec": checkpoint_age,
			"replicas": replicas,
		}));
	}
	Ok(rows)
}

fn require_mesh_auth(
	state: &MeshRouteState,
	headers: &HeaderMap,
	require_hop: bool,
) -> RouteResult<()> {
	let expected = state.inbound_token.trim();
	if !expected.is_empty() {
		let supplied = gossip::bearer_token(headers).unwrap_or_default();
		if !token_matches(supplied, expected) {
			return Err(MeshRouteError::unauthorized());
		}
	}
	if require_hop && !gossip::has_mesh_hop(headers) {
		return Err(MeshRouteError::invalid("missing mesh hop header"));
	}
	Ok(())
}

fn token_matches(supplied: &str, expected: &str) -> bool {
	expected
		.split(',')
		.map(str::trim)
		.filter(|token| !token.is_empty())
		.any(|token| token == supplied)
}

async fn json_object(request: Request<Body>) -> RouteResult<JsonObject> {
	let bytes = to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|err| MeshRouteError::invalid(format!("failed to read request body: {err}")))?;
	if bytes.is_empty() {
		return Err(MeshRouteError::invalid("request body must be a JSON object"));
	}
	match serde_json::from_slice::<Value>(&bytes) {
		Ok(Value::Object(object)) => Ok(object),
		Ok(_) => Err(MeshRouteError::invalid("request body must be a JSON object")),
		Err(err) => Err(MeshRouteError::invalid(format!("invalid JSON body: {err}"))),
	}
}

async fn json_object_or_empty(request: Request<Body>) -> RouteResult<JsonObject> {
	let bytes = to_bytes(request.into_body(), BODY_LIMIT)
		.await
		.map_err(|err| MeshRouteError::invalid(format!("failed to read request body: {err}")))?;
	if bytes.is_empty() {
		return Ok(JsonObject::new());
	}
	match serde_json::from_slice::<Value>(&bytes) {
		Ok(Value::Object(object)) => Ok(object),
		Ok(_) => Err(MeshRouteError::invalid("request body must be a JSON object")),
		Err(err) => Err(MeshRouteError::invalid(format!("invalid JSON body: {err}"))),
	}
}

fn value_object(value: Value) -> MeshResult<JsonObject> {
	match value {
		Value::Object(object) => Ok(object),
		_ => Err(MeshError::invalid("expected JSON object")),
	}
}

fn map_mesh<T>(value: MeshResult<T>) -> RouteResult<T> {
	value.map_err(MeshRouteError::from)
}

fn route_error_detail(error: &MeshRouteError) -> String {
	match &error.detail {
		Value::String(text) => text.clone(),
		Value::Object(object) => object
			.get("message")
			.and_then(Value::as_str)
			.unwrap_or_else(|| error.status.canonical_reason().unwrap_or("mesh error"))
			.to_owned(),
		_ => error
			.status
			.canonical_reason()
			.unwrap_or("mesh error")
			.to_owned(),
	}
}

fn sid_from_view(value: &Value) -> Option<String> {
	let object = value.as_object()?;
	object
		.get("id")
		.or_else(|| object.get("name"))
		.and_then(value_string)
		.filter(|sid| !sid.is_empty())
}

fn take_string(object: &mut JsonObject, key: &str) -> String {
	nonempty_string(object.remove(key)).unwrap_or_default()
}

fn string_or(value: Option<&Value>, default: String) -> String {
	value.and_then(value_string).unwrap_or(default)
}

fn value_string(value: &Value) -> Option<String> {
	match value {
		Value::String(text) => Some(text.clone()),
		Value::Number(number) => Some(number.to_string()),
		Value::Bool(value) => Some(value.to_string()),
		_ => None,
	}
}

fn nonempty_string(value: Option<Value>) -> Option<String> {
	value
		.as_ref()
		.and_then(value_string)
		.filter(|text| !text.is_empty())
}

fn to_i64(value: Option<Value>) -> Option<i64> {
	match value? {
		Value::Number(number) => number
			.as_i64()
			.or_else(|| number.as_u64().and_then(|n| i64::try_from(n).ok())),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(i64::from(value)),
		_ => None,
	}
}

fn to_u32(value: Option<Value>) -> Option<u32> {
	to_u64(value).and_then(|value| u32::try_from(value).ok())
}

fn to_u64(value: Option<Value>) -> Option<u64> {
	match value? {
		Value::Number(number) => number
			.as_u64()
			.or_else(|| number.as_i64().and_then(|n| u64::try_from(n).ok())),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(u64::from(value)),
		_ => None,
	}
}

fn to_f64(value: Option<Value>) -> Option<f64> {
	match value? {
		Value::Number(number) => number.as_f64(),
		Value::String(text) => text.parse().ok(),
		Value::Bool(value) => Some(if value { 1.0 } else { 0.0 }),
		_ => None,
	}
}

fn py_truthy(value: Option<&Value>) -> bool {
	match value {
		None | Some(Value::Null) => false,
		Some(Value::Bool(value)) => *value,
		Some(Value::Number(number)) => number.as_f64().is_some_and(|value| value != 0.0),
		Some(Value::String(text)) => !text.is_empty(),
		Some(Value::Array(items)) => !items.is_empty(),
		Some(Value::Object(object)) => !object.is_empty(),
	}
}

fn py_repr(value: &str) -> String {
	format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn unix_now() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;

	fn current_replica() -> ReplicaRecordWire {
		ReplicaRecordWire {
			sid:                   "sandbox".to_owned(),
			digest:                "digest-a".to_owned(),
			object_key:            "object-a".to_owned(),
			source_node:           "source-a".to_owned(),
			source_epoch:          4,
			checkpoint_generation: 7,
			snapshot_dir:          "/snapshots/a".to_owned(),
			params:                serde_json::Map::from_iter([("revision".to_owned(), json!(7))]),
			needs_secrets:         false,
		}
	}

	#[test]
	fn replica_current_skip_requires_exact_metadata() {
		let current = current_replica();
		let params = current.params.clone();
		assert!(replica_metadata_matches(
			&current, "digest-a", "object-a", "source-a", 4, 7, &params,
		));
		assert!(!replica_metadata_matches(
			&current, "digest-b", "object-a", "source-a", 4, 7, &params,
		));
		assert!(!replica_metadata_matches(
			&current, "digest-a", "object-b", "source-a", 4, 7, &params,
		));
		assert!(!replica_metadata_matches(
			&current, "digest-a", "object-a", "source-b", 4, 7, &params,
		));
		assert!(!replica_metadata_matches(
			&current, "digest-a", "object-a", "source-a", 5, 7, &params,
		));
		assert!(!replica_metadata_matches(
			&current, "digest-a", "object-a", "source-a", 4, 8, &params,
		));
		let changed_params = serde_json::Map::from_iter([("revision".to_owned(), json!(8))]);
		assert!(!replica_metadata_matches(
			&current,
			"digest-a",
			"object-a",
			"source-a",
			4,
			7,
			&changed_params,
		));
	}
}
