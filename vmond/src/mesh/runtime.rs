//! Concrete mesh runtime wiring for the live server.
//!
//! The Phase 4 mesh modules are deliberately small and trait-oriented.  This
//! module owns the concrete stores and adapters that make those traits point at
//! the real server engine, on-disk mesh state, peer HTTP transport, and
//! background loops.

use std::{
	collections::{BTreeMap, BTreeSet, HashMap, HashSet},
	fs::{self, OpenOptions},
	future::Future,
	io::Write,
	os::unix::fs::OpenOptionsExt,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use futures_util::{FutureExt, future::BoxFuture};
use parking_lot::{Mutex, RwLock};
use serde_json::{Map, Value, json};
use tokio::{
	sync::{Notify, broadcast},
	task::JoinHandle,
};

use super::{
	gossip::{self, HeartbeatView, MeshError, MeshResult, PeerEndpoint, PeerHttpClient},
	lease::{self, DEFAULT_TTL, LeaseDecision, LeaseRecord},
	place::{self, PlacementContext, PlacementRequest, PlacementWeights},
	reconciler::{self, ReconcileConfig, Reconciler},
	record, replica,
	replica_cache::ReplicaCache,
	routes::{
		CreateRecordWire, MeshControl, MeshEngine, MeshLeaseManager, MeshRecordStore,
		MeshReplicaStore, MeshRouteConfig, MeshRouteState, MeshSetupResult, MeshTemplateTransfer,
		MetadataPull, MigratePrepareWire, MigrationCleanupWire, NodeCapsWire, PeerMember,
		ReplicaExport, ReplicaRecordWire,
	},
	state::{self, MembershipState, NodeCaps, NodeState},
	transfer,
};
use crate::{
	EngineError, ErrorCode, Result,
	config::ServeConfig,
	engine::{Engine, EngineApi, OwnershipHandoff},
	function::store::Store as FunctionStore,
	home::Home,
	image::cas,
	models::SandboxCreate,
	security::{EncryptedArchive, Keyring, crypto::KeySnapshot},
};

const OWNER_EPOCH_FLOOR: u64 = 0;
const REPLICA_ARCHIVE_ROOT: &str = "replica";

/// Live mesh runtime shared by API routes, peer routes, and background loops.
pub struct MeshRuntime {
	pub(crate) config: ServeConfig,
	home: Home,
	membership_path: PathBuf,
	node_id: String,
	membership: RwLock<MembershipState>,
	token: RwLock<String>,
	default_advertise: String,
	transport: PeerHttpClient,
	pub(crate) engine: MeshEngineAdapter,
	records: record::RecordStore,
	replicas: replica::ReplicaStore,
	replica_cache: ReplicaCache,
	pub(crate) function_store: FunctionStore,
	owner_lease_deadlines: Mutex<HashMap<String, tokio::time::Instant>>,
	owner_renew_inflight: Mutex<HashSet<(String, u64)>>,
	fence_retries: Mutex<HashMap<String, u64>>,
	leases: lease::LeaseManager,
	owners: RwLock<BTreeMap<String, (String, u64)>>,
	restoring: RwLock<BTreeMap<String, (String, u64)>>,
	suspended_restores: RwLock<BTreeSet<(String, String, u64)>>,
	target_migrations: Arc<Mutex<HashSet<String>>>,
	orphans: Mutex<Vec<(String, String)>>,
	idem_pins: RwLock<BTreeMap<String, String>>,
	workers: Mutex<HashSet<String>>,
	events: Mutex<Vec<String>>,
	compat_backend: String,
	compat_arch: String,
	cpu_baseline: String,
	weights: PlacementWeights,
	replicate_notify: Notify,
	background_started: AtomicBool,
	pub(crate) cluster_store: Option<Arc<super::cluster_store::ProductionStore>>,
	s3_client: Option<Arc<crate::s3::S3Client>>,
}

pub(crate) fn should_fence_epoch(current: Option<u64>, expected: u64) -> bool {
	current == Some(expected)
}

pub(crate) fn owner_watchdog_deadline(
	request_started: tokio::time::Instant,
	remaining_ttl: f64,
) -> Option<tokio::time::Instant> {
	(remaining_ttl.is_finite() && remaining_ttl > 0.0)
		.then(|| request_started + Duration::from_secs_f64((remaining_ttl - 5.0).max(0.0)))
}

pub(crate) fn renewal_failure_fences(
	now: tokio::time::Instant,
	deadline: Option<tokio::time::Instant>,
	authority_lost: bool,
) -> bool {
	authority_lost || deadline.is_none_or(|deadline| deadline <= now)
}

struct TargetMigrationGuard {
	active: Arc<Mutex<HashSet<String>>>,
	sid:    String,
}

impl Drop for TargetMigrationGuard {
	fn drop(&mut self) {
		self.active.lock().remove(&self.sid);
	}
}

impl super::routes::MigrationLifecycleGuard for TargetMigrationGuard {}

pub(crate) fn begin_target_migration(
	active: &Arc<Mutex<HashSet<String>>>,
	sid: &str,
) -> MeshResult<Box<dyn super::routes::MigrationLifecycleGuard>> {
	if !active.lock().insert(sid.to_owned()) {
		return Err(MeshError::with_code(
			format!("target migration for sandbox {sid} is pending"),
			"busy",
		));
	}
	Ok(Box::new(TargetMigrationGuard { active: active.clone(), sid: sid.to_owned() }))
}

pub(crate) fn expired_owner_deadlines(
	now: tokio::time::Instant,
	deadlines: &HashMap<String, tokio::time::Instant>,
) -> Vec<String> {
	deadlines
		.iter()
		.filter(|(_, deadline)| **deadline <= now)
		.map(|(sid, _)| sid.clone())
		.collect()
}

pub(crate) fn fence_blocks_successor(fences: &HashMap<String, u64>, sid: &str) -> bool {
	fences.contains_key(sid)
}

pub(crate) fn fence_identity(
	sid: &str,
	owners: &BTreeMap<String, (String, u64)>,
	restoring: &BTreeMap<String, (String, u64)>,
) -> Option<(String, u64)> {
	restoring
		.get(sid)
		.cloned()
		.or_else(|| owners.get(sid).cloned())
}

/// Durable restore and rollback intents must not become serving owners merely
/// because the process restarted between their staging and commit boundaries.
pub(crate) fn record_is_staging(params: &Map<String, Value>) -> bool {
	let migration_pending = params.contains_key("_mesh_migration_token")
		&& params
			.get("_mesh_migration_committed")
			.and_then(Value::as_bool)
			!= Some(true);
	let lifecycle_pending =
		["_mesh_restore_pending", "_mesh_rollback_pending", "restore_pending", "rollback_pending"]
			.iter()
			.any(|key| {
				params
					.get(*key)
					.is_some_and(|value| value.is_object() || value.as_bool() == Some(true))
			});
	let lifecycle_operation = params
		.get("lifecycle_operation")
		.and_then(Value::as_str)
		.is_some_and(|operation| matches!(operation, "restore" | "rollback"));
	migration_pending || lifecycle_pending || lifecycle_operation
}
/// Durable deletion-tombstone storage used by the replay convergence loop.
///
/// This deliberately exposes only the transition that follows a tombstone:
/// callers cannot accidentally remove a live record while replaying a crash.
pub(crate) trait DeleteTombstoneStore: Send + Sync {
	fn list_deleting(&self) -> MeshResult<Vec<CreateRecordWire>>;
	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> MeshResult<()>;
}

/// Engine operations whose order makes a deletion durable.
pub(crate) trait DeleteTombstoneEngine: Send + Sync {
	fn teardown_candidate(&self, sid: String, expected_epoch: i64) -> BoxFuture<'_, MeshResult<()>>;
	fn delete_portable_history(
		&self,
		sid: String,
		owner: String,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>>;
}

/// Replay every durable delete tombstone in the only safe order:
/// stop the fenced candidate, remove portable history, then commit removal.
///
/// A failed step intentionally leaves its tombstone untouched for the next
/// convergence pass.  `fence` runs before teardown so the record is never
/// advertised or renewed while its prior local candidate is being stopped.
pub(crate) async fn converge_delete_tombstones<S, E, F>(
	store: &S,
	engine: &E,
	mut fence: F,
) -> MeshResult<()>
where
	S: DeleteTombstoneStore + ?Sized,
	E: DeleteTombstoneEngine + ?Sized,
	F: FnMut(&CreateRecordWire),
{
	for record in store.list_deleting()? {
		fence(&record);
		if let Err(error) = engine
			.teardown_candidate(record.sid.clone(), record.epoch)
			.await
		{
			tracing::warn!(sid = %record.sid, %error, "deletion tombstone teardown remains pending");
			continue;
		}
		if let Err(error) = engine
			.delete_portable_history(record.sid.clone(), record.owner.clone(), record.epoch)
			.await
		{
			tracing::warn!(sid = %record.sid, %error, "deletion tombstone history cleanup remains pending");
			continue;
		}
		if let Err(error) = store.commit_delete(&record.sid, &record.owner, record.epoch) {
			tracing::warn!(sid = %record.sid, %error, "deletion tombstone remains pending");
		}
	}
	Ok(())
}

impl MeshRuntime {
	fn rearm_owner_watchdog(
		&self,
		sid: &str,
		request_started: tokio::time::Instant,
		remaining_ttl: f64,
	) -> Result<bool> {
		let Some(deadline) = owner_watchdog_deadline(request_started, remaining_ttl) else {
			return Err(EngineError::busy(format!("owner lease for sandbox {sid} expired")));
		};
		if !self.engine.candidate_is_running(sid) {
			return Ok(false);
		}
		let remaining = deadline
			.checked_duration_since(tokio::time::Instant::now())
			.ok_or_else(|| {
				EngineError::busy(format!("owner lease watchdog deadline elapsed for sandbox {sid}"))
			})?;
		let secs = remaining.as_secs();
		if secs == 0 {
			return Err(EngineError::busy(format!(
				"owner lease watchdog deadline elapsed for sandbox {sid}"
			)));
		}
		self
			.engine
			.engine
			.mesh_rearm_owner_lease(sid, secs)
			.map(|()| true)
	}

	fn renew_and_arm_serving_owner(
		&self,
		sid: &str,
		owner: &str,
		epoch: u64,
	) -> Result<tokio::time::Instant> {
		let request_started = tokio::time::Instant::now();
		let Some(store) = &self.cluster_store else {
			return Ok(request_started + Duration::from_secs_f64((DEFAULT_TTL - 5.0).max(0.0)));
		};
		let Some(ttl) = store.renew_owner_lease(sid, owner, u64_to_i64(epoch))? else {
			return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
		};
		if !matches!(self.rearm_owner_watchdog(sid, request_started, ttl), Ok(true)) {
			return Err(EngineError::busy(format!("owner watchdog unavailable for sandbox {sid}")));
		}
		owner_watchdog_deadline(request_started, ttl)
			.ok_or_else(|| EngineError::busy(format!("owner lease for sandbox {sid} expired")))
	}

	/// Build the concrete mesh runtime and load durable mesh/record/replica
	/// state.
	pub fn new(config: ServeConfig, home: Home, engine: Arc<Engine>) -> Result<Arc<Self>> {
		let caps = state::probe_caps();
		let membership_path = state::mesh_state_path(&home);
		let default_advertise = state::default_advertise(&config.host, config.port);
		let now = unix_now();
		let membership = MembershipState::load(&membership_path, caps, now)
			.map_err(|err| EngineError::invalid(err.to_string()))?
			.unwrap_or_else(|| disabled_membership(caps, &default_advertise));
		let token = config.token.clone().unwrap_or_default();
		let transport = PeerHttpClient::new(token.clone()).map_err(mesh_to_engine)?;
		let records = record::RecordStore::for_home(home.clone());
		records.load();
		let replicas = replica::ReplicaStore::for_home(home.clone());
		replicas.load();
		let replica_cache = ReplicaCache::new(home.replicas_dir().join("objects"));
		let function_store = FunctionStore::open(&home)?;
		let leases = lease::LeaseManager::for_home(home.clone());
		let cleanup_dir = home.records_dir().join("migration-cleanup");
		let (compat_backend, compat_arch) = state::probe_compat();
		let cpu_baseline = state::probe_cpu_baseline();
		let cluster_store = if config.cluster_mode == crate::config::ClusterMode::Production {
			let url = config.postgres_url.clone().unwrap_or_default();
			Some(Arc::new(super::cluster_store::ProductionStore::connect(&url)?))
		} else {
			None
		};
		let s3_client = if config.cluster_mode == crate::config::ClusterMode::Production {
			let s3_cfg = crate::s3::S3MountConfig {
				bucket:    config.s3_bucket.clone().unwrap_or_default(),
				prefix:    config.s3_prefix.clone().unwrap_or_default(),
				region:    config.s3_region.clone().unwrap_or_default(),
				endpoint:  config.s3_endpoint.clone(),
				read_only: false,
				creds:     Some(crate::s3::S3Credentials {
					access_key:    config.s3_access_key.clone().unwrap_or_default(),
					secret_key:    config.s3_secret_key.clone().unwrap_or_default(),
					session_token: None,
				}),
				auth:      crate::s3::S3Auth::Inline,
			};
			Some(Arc::new(
				crate::s3::S3Client::new(s3_cfg).map_err(|e| EngineError::engine(e.to_string()))?,
			))
		} else {
			None
		};
		let mut ownership_records = if let Some(store) = &cluster_store {
			store.list()?
		} else {
			records.list()
		};
		if let Some(store) = &cluster_store {
			for record in &mut ownership_records {
				if let Some(key_id) = legacy_portable_key(&record.params, &config) {
					record
						.params
						.insert("encryption_key_id".to_owned(), Value::String(key_id));
					*record = store.update_exact_record(record)?;
					records.put(record.clone())?;
				}
			}
		}
		let suspend_markers = if let Some(store) = &cluster_store {
			store
				.list_suspend_markers()?
				.into_iter()
				.map(|marker| (marker.sid.clone(), marker))
				.collect::<HashMap<_, _>>()
		} else {
			HashMap::new()
		};
		let rollback_markers = if let Some(store) = &cluster_store {
			store
				.list_rollback_markers()?
				.into_iter()
				.map(|marker| marker.sid)
				.collect::<HashSet<_>>()
		} else {
			HashSet::new()
		};
		let local_node_id = membership.node_id.clone();
		let mut owners = BTreeMap::new();
		let mut restoring = BTreeMap::new();
		let mut startup_orphans = Vec::new();
		let mut startup_deadlines = HashMap::new();
		for record in &ownership_records {
			let entry = (record.owner.clone(), i64_to_u64(record.epoch));
			if rollback_markers.contains(&record.sid) {
				// Rollback replay owns this exact generation. It cannot be
				// renewed or routed as a normal serving owner during startup.
				restoring.insert(record.sid.clone(), entry);
			} else if let Some(marker) = suspend_markers.get(&record.sid) {
				if !engine.mesh_has_sandbox(&record.sid) && marker.state == "suspended" {
					engine.mesh_install_suspended_placeholder(record, marker)?;
				}
				// A marker is never serving on startup, even when this node
				// holds its owner id. The placeholder enables explicit resume.
				restoring.insert(record.sid.clone(), entry);
			} else if record_is_staging(&record.params) {
				restoring.insert(record.sid.clone(), entry);
				if let Some(pending) = record
					.params
					.get("_mesh_restore_pending")
					.and_then(Value::as_object)
				{
					let dead = if record.owner == local_node_id {
						pending
							.get("source_owner")
							.and_then(Value::as_str)
							.unwrap_or(&record.owner)
							.to_owned()
					} else {
						record.owner.clone()
					};
					startup_orphans.push((record.sid.clone(), dead));
				}
			} else if record.owner == local_node_id && engine.candidate_is_running(&record.sid) {
				let started = tokio::time::Instant::now();
				match cluster_store.as_ref().expect("checked").renew_owner_lease(
					&record.sid,
					&record.owner,
					record.epoch,
				)? {
					Some(ttl) if ttl.is_finite() && ttl > 0.0 => {
						let deadline = owner_watchdog_deadline(started, ttl)
							.expect("positive finite owner TTL has a watchdog deadline");
						startup_deadlines.insert(record.sid.clone(), deadline);
						owners.insert(record.sid.clone(), entry);
					},
					_ => {
						// Keep the exact stale identity non-serving so proxy
						// cannot fall back to an epoch floor while the fencing
						// retry removes any surviving local candidate.
						restoring.insert(record.sid.clone(), entry);
						startup_deadlines.insert(record.sid.clone(), tokio::time::Instant::now());
						let _ = engine.remove_local_candidate(&record.sid);
					},
				}
			} else if record.owner == local_node_id && cluster_store.is_some() {
				// A rebooted node cannot renew a clean durable owner record
				// without the VM it purports to own. Keep it non-serving and
				// route it through HA recovery instead.
				restoring.insert(record.sid.clone(), entry);
				startup_deadlines.insert(record.sid.clone(), tokio::time::Instant::now());
				startup_orphans.push((record.sid.clone(), record.owner.clone()));
			} else {
				owners.insert(record.sid.clone(), entry);
			}
		}
		let migration_preserve_paths = ownership_records
			.iter()
			.filter(|record| record.owner == membership.node_id)
			.filter_map(|record| {
				record
					.params
					.get("_mesh_migration_delta_dir")
					.and_then(Value::as_str)
					.map(PathBuf::from)
			})
			.collect::<Vec<_>>();
		crate::engine::staging_gc::reclaim_orphaned_migration_staging(
			&home.security_dir().join("runtime"),
			&migration_preserve_paths,
		)
		.map_err(EngineError::from)?;
		let runtime = Arc::new(Self {
			node_id: local_node_id,
			config: config.clone(),
			home,
			membership_path,
			membership: RwLock::new(membership),
			token: RwLock::new(token),
			default_advertise,
			transport,
			engine: MeshEngineAdapter::new(engine.clone(), cleanup_dir),
			records,
			replicas,
			replica_cache,
			function_store,
			owner_lease_deadlines: Mutex::new(startup_deadlines),
			owner_renew_inflight: Mutex::new(HashSet::new()),
			fence_retries: Mutex::new(HashMap::new()),
			leases,
			owners: RwLock::new(owners),
			restoring: RwLock::new(restoring),
			suspended_restores: RwLock::new(BTreeSet::new()),
			target_migrations: Arc::new(Mutex::new(HashSet::new())),
			orphans: Mutex::new(startup_orphans),
			idem_pins: RwLock::new(BTreeMap::new()),
			workers: Mutex::new(HashSet::new()),
			events: Mutex::new(Vec::new()),
			compat_backend,
			compat_arch,
			cpu_baseline,
			weights: placement_weights(&config),
			replicate_notify: Notify::new(),
			background_started: AtomicBool::new(false),
			cluster_store,
			s3_client,
		});
		runtime.sweep_replica_cache();
		engine.set_restore_handoff(runtime.clone() as Arc<dyn OwnershipHandoff>);
		Ok(runtime)
	}

	/// Verify production object storage before accepting requests.
	pub async fn verify_storage(&self) -> Result<()> {
		if let Some(s3) = &self.s3_client {
			s3.probe()
				.await
				.map_err(|error| EngineError::engine(format!("S3 checkpoint store: {error}")))?;
		}
		Ok(())
	}

	/// Build the peer-route state object from the shared concrete runtime.
	pub fn route_state(self: &Arc<Self>) -> MeshRouteState {
		let token = self.token.read().clone();
		MeshRouteState::new(
			token.clone(),
			token,
			self.transport.clone(),
			self.clone() as Arc<dyn MeshControl>,
			Arc::new(self.engine.clone()) as Arc<dyn MeshEngine>,
			self.clone() as Arc<dyn MeshLeaseManager>,
			self.clone() as Arc<dyn MeshRecordStore>,
			self.clone() as Arc<dyn MeshReplicaStore>,
			Arc::new(TemplateTransfer) as Arc<dyn MeshTemplateTransfer>,
			self.route_config(),
		)
	}

	/// Start heartbeat, reconciliation, and replica loops once for the server.
	pub fn start_background(
		self: &Arc<Self>,
		shutdown: &broadcast::Sender<()>,
	) -> Vec<JoinHandle<()>> {
		if self.background_started.swap(true, Ordering::SeqCst) {
			return Vec::new();
		}
		let mut handles = Vec::new();
		let heartbeat_runtime = self.clone();
		let mut heartbeat_shutdown = shutdown.subscribe();
		handles.push(tokio::spawn(async move {
			heartbeat_loop(heartbeat_runtime, &mut heartbeat_shutdown).await;
		}));

		let reconcile_runtime = self.clone();
		let mut reconcile_shutdown = shutdown.subscribe();
		handles.push(tokio::spawn(async move {
			reconcile_loop(reconcile_runtime, &mut reconcile_shutdown).await;
		}));
		if self.cluster_store.is_some() {
			let lease_runtime = self.clone();
			let mut lease_shutdown = shutdown.subscribe();
			handles.push(tokio::spawn(async move {
				owner_lease_loop(lease_runtime, &mut lease_shutdown).await;
			}));
		}
		handles
	}

	pub async fn migrate_sandbox(self: &Arc<Self>, sid: String, target: String) -> Result<Value> {
		super::routes::migrate_sandbox_to(&self.route_state(), sid, target)
			.await
			.map_err(|err| EngineError::engine(format!("mesh migration failed: {err:?}")))
	}

	/// True when `PostgreSQL` has an exact durable suspended marker, independent
	/// of whether this node has already projected its non-serving placeholder.
	/// Routing uses this only to select the fenced adoption path.
	pub(crate) fn has_suspended_marker(&self, sid: &str) -> Result<bool> {
		let Some(store) = &self.cluster_store else {
			return Ok(false);
		};
		Ok(store
			.suspend_marker(sid)?
			.is_some_and(|marker| marker.state == "suspended"))
	}

	/// True only for a durable suspended marker projected as this node's
	/// non-serving placeholder. Resume alone may bypass generic proxy routing.
	pub(crate) fn passive_suspend_marker(&self, sid: &str) -> Result<bool> {
		let Some(store) = &self.cluster_store else {
			return Ok(false);
		};
		let Some(marker) = store.suspend_marker(sid)? else {
			return Ok(false);
		};
		if marker.state != "suspended" {
			return Ok(false);
		}
		let view = self.engine.engine.mesh_get_view(sid)?;
		Ok(view.get("status").and_then(Value::as_str) == Some("suspended")
			&& view
				.get("_mesh_suspended_projection")
				.and_then(Value::as_bool)
				== Some(true))
	}

	pub fn schedule_fence(self: &Arc<Self>, sid: String, expected_epoch: u64) {
		let current_epoch = self
			.owners
			.read()
			.get(&sid)
			.map(|(_, epoch)| *epoch)
			.or_else(|| self.restoring.read().get(&sid).map(|(_, epoch)| *epoch));
		if !should_fence_epoch(current_epoch, expected_epoch) {
			return;
		}
		let mut fences = self.fence_retries.lock();
		if fences.contains_key(&sid) {
			return;
		}
		fences.insert(sid.clone(), expected_epoch);
		drop(fences);
		self.owners.write().remove(&sid);
		self.restoring.write().remove(&sid);
		self.owner_lease_deadlines.lock().remove(&sid);
		let runtime = self.clone();
		tokio::spawn(async move {
			loop {
				let engine = runtime.engine.engine.clone();
				let remove_sid = sid.clone();
				let stopped = matches!(
					tokio::task::spawn_blocking(move || engine.remove_local_candidate(&remove_sid))
						.await,
					Ok(Ok(()))
				);
				if stopped {
					runtime.fence_retries.lock().remove(&sid);
					if let Err(error) =
						MeshLeaseManager::release_record_volume_leases(&*runtime, sid.clone()).await
					{
						tracing::warn!(sid = %sid, %error, "fenced candidate stopped but volume lease release failed");
					}
					return;
				}
				// Keep the epoch fenced and its volume leases held until a local
				// stop is positively confirmed; no retry may renew or serve it.
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		});
	}

	pub async fn create_sandbox(self: &Arc<Self>, params: Map<String, Value>) -> Result<Value> {
		let key = params
			.get("idempotency_key")
			.and_then(Value::as_str)
			.map(str::trim)
			.filter(|key| !key.is_empty())
			.map_or_else(mesh_request_idempotency_key, str::to_owned);
		let view =
			super::routes::MeshRouteState::idempotent_sandbox_create(&self.route_state(), key, params)
				.await
				.map_err(|err| EngineError::engine(format!("mesh create failed: {err:?}")))?;
		Ok(view)
	}

	pub const fn peer_client(&self) -> &PeerHttpClient {
		&self.transport
	}

	pub(crate) fn nonserving_epoch(
		sid: &str,
		restoring: &BTreeMap<String, (String, u64)>,
		fenced: &HashMap<String, u64>,
	) -> Option<u64> {
		restoring
			.get(sid)
			.map(|(_, epoch)| *epoch)
			.or_else(|| fenced.get(sid).copied())
	}

	pub fn restoring_epoch(&self, sid: &str) -> Option<u64> {
		let restoring = self.restoring.read();
		let fenced = self.fence_retries.lock();
		Self::nonserving_epoch(sid, &restoring, &fenced)
	}

	pub fn mesh_enabled(&self) -> bool {
		self.membership.read().enabled
	}

	pub fn local_node_id(&self) -> String {
		self.membership.read().node_id.clone()
	}

	/// Return the live topology snapshot used to admit function HA policies.
	///
	/// This is deliberately topology only: function workers are not sandbox
	/// owners and must not be inserted into the sandbox owner map.
	pub fn function_placement_nodes(&self) -> Vec<NodeState> {
		let now = unix_now();
		let interval = self.config.mesh_heartbeat_sec;
		self
			.all_nodes()
			.into_iter()
			.filter(|node| node.node_id == self.local_node_id() || node.healthy(now, interval))
			.collect()
	}

	#[cfg(test)]
	pub(crate) async fn persist_before_migration_receive<T, F, Fut>(
		engine: &dyn MeshEngine,
		intent: MigrationCleanupWire,
		receive: F,
	) -> MeshResult<T>
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = MeshResult<T>>,
	{
		engine.migration_cleanup_persist(intent)?;
		receive().await
	}

	fn route_config(&self) -> MeshRouteConfig {
		MeshRouteConfig {
			replicas:          self.config.replicas,
			create_timeout:    secs(self.config.mesh_create_timeout_sec),
			default_ha_meshed: self.config.default_ha(true).to_owned(),
			default_ha_local:  self.config.default_ha(false).to_owned(),
		}
	}

	fn reconcile_config(&self) -> ReconcileConfig {
		ReconcileConfig::from_serve_config(&self.config, self.enabled(), self.token.read().clone())
	}

	pub(crate) async fn converge_migration_intent(
		engine: &dyn MeshEngine,
		leases: &dyn MeshLeaseManager,
		records: &dyn MeshRecordStore,
		transfer: &dyn MeshTemplateTransfer,
		client: &reqwest::Client,
		token: &str,
		mut record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		let migration_id = record
			.params
			.get("_mesh_migration_id")
			.and_then(Value::as_str)
			.ok_or_else(|| MeshError::invalid("migration intent has no token"))?;
		let _target_guard = records.try_begin_target_migration(&record.sid, migration_id)?;
		records.notify_source_migration_claim(&record).await?;
		if record
			.params
			.get("_mesh_migration_committed")
			.and_then(Value::as_bool)
			== Some(true)
		{
			if engine.has_sandbox(&record.sid) {
				engine
					.migrate_activate_target(record.sid.clone(), record.epoch)
					.await?;
			}
			return Ok(record);
		}
		let delta_dir = if let Some(path) = record
			.params
			.get("_mesh_migration_delta_dir")
			.and_then(Value::as_str)
			.filter(|path| Path::new(path).is_dir())
		{
			path.to_owned()
		} else {
			let source_url = record
				.params
				.get("_mesh_migration_source_url")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no source URL"))?;
			let digest = record
				.params
				.get("_mesh_migration_delta_digest")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no delta digest"))?;
			let base_digest = record
				.params
				.get("_mesh_migration_base_digest")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no base digest"))?;
			let base_dir = match cas::lookup(base_digest).map_err(engine_to_mesh)? {
				Some(path) => path,
				None => PathBuf::from(
					transfer
						.pull_checkpoint(
							client,
							source_url.to_owned(),
							base_digest.to_owned(),
							token.to_owned(),
						)
						.await?,
				),
			};
			let installed = transfer
				.pull_checkpoint(client, source_url.to_owned(), digest.to_owned(), token.to_owned())
				.await?;
			let staged = engine
				.stage_migration_delta(
					record.sid.clone(),
					installed,
					base_dir.to_string_lossy().into_owned(),
				)
				.await?;
			record
				.params
				.insert("_mesh_migration_delta_dir".to_owned(), Value::String(staged.clone()));
			record = records.update_exact(record)?;
			staged
		};
		let params: Map<String, Value> = record
			.params
			.iter()
			.filter(|(key, _)| !key.starts_with("_mesh_migration_"))
			.map(|(key, value)| (key.clone(), value.clone()))
			.collect();
		let acquired = leases
			.acquire_writable_volume_leases(params.clone(), record.epoch)
			.await?;
		let adopted = if engine.has_sandbox(&record.sid) {
			Ok(())
		} else {
			engine
				.migrate_adopt_target(record.sid.clone(), delta_dir, params)
				.await
				.map(|_| ())
		};
		if let Err(err) = adopted {
			let _ = engine
				.teardown_candidate(record.sid.clone(), record.epoch)
				.await;
			let _ = leases.release_leases(acquired).await;
			return Err(err);
		}
		if let Err(error) = engine.record_volume_leases(&record.sid, acquired.clone()) {
			let _ = engine
				.teardown_candidate(record.sid.clone(), record.epoch)
				.await;
			let _ = leases.release_leases(acquired).await;
			return Err(error);
		}
		record
			.params
			.insert("_mesh_migration_committed".to_owned(), Value::Bool(true));
		match records.update_exact(record.clone()) {
			Ok(committed) => {
				engine
					.migrate_activate_target(committed.sid.clone(), committed.epoch)
					.await?;
				Ok(committed)
			},
			Err(error) => {
				let _ = engine
					.teardown_candidate(record.sid.clone(), record.epoch)
					.await;
				let _ = leases.release_leases(acquired).await;
				Err(error)
			},
		}
	}

	pub(crate) async fn replay_source_migration_abort(
		records: &dyn MeshRecordStore,
		engine: &dyn MeshEngine,
		cleanup: &MigrationCleanupWire,
	) -> MeshResult<CreateRecordWire> {
		let current = records
			.get(&cleanup.sid)?
			.ok_or_else(|| MeshError::invalid("source migration record disappeared during abort"))?;
		if cleanup.phase == "complete_pending" {
			if current.owner != cleanup.source_owner || current.epoch != cleanup.source_epoch {
				return Err(MeshError::with_code(
					"source migration completion is no longer authoritative",
					"busy",
				));
			}
			let completed = records.complete_migration_abort(
				&cleanup.sid,
				&cleanup.migration_id,
				&current.owner,
				current.epoch,
			)?;
			engine.migration_cleanup_remove(cleanup.sid.clone())?;
			return Ok(completed);
		}
		if cleanup.phase != "abort_pending" && cleanup.phase != "restore_pending" {
			let mut pending = cleanup.clone();
			"abort_pending".clone_into(&mut pending.phase);
			engine.migration_cleanup_persist(pending)?;
		}
		// The record store retains an aborted disposition keyed by this token.
		// Consequently, this returns the same fresh source generation both
		// before and after a crash that follows the initial fenced CAS.
		let fresh = records
			.abort_migration_handoff(
				&cleanup.sid,
				&cleanup.source_owner,
				cleanup.source_epoch,
				&cleanup.target_owner,
				&cleanup.migration_id,
			)?
			.ok_or_else(|| MeshError::with_code("target may have consumed migration token", "busy"))?;
		if cleanup.phase != "restore_pending" {
			let mut restore = cleanup.clone();
			"restore_pending".clone_into(&mut restore.phase);
			engine.migration_cleanup_persist(restore)?;
		}
		let params = current
			.params
			.iter()
			.filter(|(key, _)| !key.starts_with("_mesh_migration_"))
			.map(|(key, value)| (key.clone(), value.clone()))
			.collect();
		engine
			.migrate_abort(
				cleanup.sid.clone(),
				cleanup.base_digest.clone(),
				cleanup.delta_dir.clone(),
				cleanup.delta_digest.clone(),
				params,
			)
			.await?;
		let mut completion = cleanup.clone();
		"complete_pending".clone_into(&mut completion.phase);
		completion.source_epoch = fresh.epoch;
		engine.migration_cleanup_persist(completion)?;
		let completed = records.complete_migration_abort(
			&cleanup.sid,
			&cleanup.migration_id,
			&fresh.owner,
			fresh.epoch,
		)?;
		engine.migration_cleanup_remove(cleanup.sid.clone())?;
		Ok(completed)
	}

	pub(crate) fn source_migration_abort_allowed(
		status: Result<&Map<String, Value>, &MeshError>,
	) -> bool {
		matches!(
			status,
			Ok(status)
				if status.get("committed").and_then(Value::as_bool) != Some(true)
					&& status.get("state").and_then(Value::as_str) == Some("absent")
		)
	}

	async fn converge_migrations(&self) {
		let transfer = TemplateTransfer;
		let token = self.token.read().clone();
		for record in MeshRecordStore::list(self).unwrap_or_default() {
			if record.owner != self.node_id
				|| record
					.params
					.get("_mesh_migration_id")
					.and_then(Value::as_str)
					.is_none()
			{
				continue;
			}
			match Self::converge_migration_intent(
				&self.engine,
				self,
				self,
				&transfer,
				self.transport.bulk_client(),
				&token,
				record,
			)
			.await
			{
				Ok(committed) => {
					let entry = (committed.owner.clone(), i64_to_u64(committed.epoch));
					self.restoring.write().remove(&committed.sid);
					self.owners.write().insert(committed.sid, entry);
				},
				Err(err) => tracing::warn!(error = %err, "target migration intent remains pending"),
			}
		}
		for cleanup in self.engine.migration_cleanup_pending().unwrap_or_default() {
			if cleanup.target_url.is_empty() {
				continue;
			}
			let status = self
				.transport
				.post(
					&cleanup.target_url,
					"/v1/mesh/migrate/status",
					&json!({
						"sid": cleanup.sid,
						"epoch": cleanup.epoch,
						"migration_id": cleanup.migration_id,
					}),
					Some(secs(self.config.mesh_create_timeout_sec)),
				)
				.await;
			if Self::source_migration_abort_allowed(status.as_ref()) {
				match Self::replay_source_migration_abort(self, &self.engine, &cleanup).await {
					Ok(fresh) => {
						self
							.owners
							.write()
							.insert(cleanup.sid.clone(), (fresh.owner, i64_to_u64(fresh.epoch)));
					},
					Err(error) => tracing::warn!(
						%error,
						sid = %cleanup.sid,
						"source migration abort remains pending"
					),
				}
				continue;
			}
			let Ok(status) = status else {
				// Pending, unreachable, and malformed status are ambiguous:
				// source recovery must wait for an explicit target absence.
				continue;
			};
			if status.get("committed").and_then(Value::as_bool) != Some(true) {
				continue;
			}
			let leases =
				MeshLeaseManager::release_record_volume_leases(self, cleanup.sid.clone()).await;
			let artifacts = self
				.engine
				.migrate_cleanup_committed(
					cleanup.sid.clone(),
					cleanup.base_dir.clone(),
					cleanup.base_digest.clone(),
					cleanup.delta_dir.clone(),
					cleanup.delta_digest.clone(),
				)
				.await;
			if leases.is_ok() && artifacts.is_ok() {
				if let Err(err) = self.engine.migration_cleanup_remove(cleanup.sid.clone()) {
					tracing::warn!(error = %err, "removing completed migration cleanup record failed");
				} else {
					self.owners.write().remove(&cleanup.sid);
				}
			} else {
				tracing::warn!(
					lease_error = ?leases.err(),
					artifact_error = ?artifacts.err(),
					"committed migration cleanup remains pending"
				);
			}
		}
		if let Some(store) = &self.cluster_store
			&& let Err(error) = converge_delete_tombstones(store.as_ref(), &self.engine, |record| {
				self.owners.write().remove(&record.sid);
			})
			.await
		{
			tracing::warn!(%error, "listing deletion tombstones failed");
		}
	}

	fn save_membership(&self, membership: &MembershipState) -> MeshResult<()> {
		membership.save(&self.membership_path).map_err(Into::into)
	}

	fn local_state(&self) -> NodeState {
		let membership = self.membership.read().clone();
		let mut state = NodeState::new(membership.node_id.clone(), membership.advertise.clone());
		state.region.clone_from(&membership.region);
		state.caps = membership.caps;
		state.ts = unix_now();
		state.last_seen = state.ts;
		state.backend.clone_from(&self.compat_backend);
		state.arch.clone_from(&self.compat_arch);
		state.cpu_baseline.clone_from(&self.cpu_baseline);
		state.function_artifacts = self
			.function_store
			.placement_artifact_digests(4096)
			.unwrap_or_default();

		for view in self.engine.list_views().unwrap_or_default() {
			let Some(object) = view.as_object() else {
				continue;
			};
			let status = object
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or_default();
			if status == "terminated" {
				continue;
			}
			if let Some(id) = object.get("id").and_then(Value::as_str) {
				state.owned.push(id.to_owned());
				state
					.owned_epochs
					.insert(id.to_owned(), self.local_epoch_u64(id));
			}
			if status == "running" {
				state.committed_vcpus += object.get("cpus").and_then(value_u64).unwrap_or(0);
				state.committed_mem_mib += object.get("memory").and_then(value_u64).unwrap_or(0);
			}
		}
		state.owned.sort();

		for (key, ready) in self.local_pools() {
			state.pools.insert(key.clone(), ready);
			state.templates.push(key);
		}
		if let Ok(digests) = cas::list_digests() {
			for (digest, template) in digests {
				state.templates.push(digest.clone());
				state.template_index.insert(digest, template);
			}
		}
		state.templates.sort();
		state.templates.dedup();
		state.inflight = self.workers.lock().len() as u64;
		state
	}

	fn local_pools(&self) -> BTreeMap<String, u64> {
		let Ok(value) = self.engine.engine.pool_list() else {
			return BTreeMap::new();
		};
		let Some(object) = value.as_object() else {
			return BTreeMap::new();
		};
		object
			.iter()
			.map(|(key, value)| {
				let ready = value
					.get("ready")
					.and_then(Value::as_u64)
					.unwrap_or_default();
				(key.clone(), ready)
			})
			.collect()
	}

	fn all_nodes(&self) -> Vec<NodeState> {
		let membership = self.membership.read().clone();
		let mut nodes = vec![self.local_state()];
		nodes.extend(membership.peers.values().cloned());
		nodes
	}

	fn context(&self) -> PlacementContext {
		let membership = self.membership.read();
		PlacementContext {
			ingress_id:     membership.node_id.clone(),
			ingress_region: membership.region.clone(),
			backend:        self.compat_backend.clone(),
			arch:           self.compat_arch.clone(),
			cpu_baseline:   self.cpu_baseline.clone(),
			interval:       self.config.mesh_heartbeat_sec,
			weights:        self.weights,
		}
	}

	fn merge_node_state(&self, value: Value) -> MeshResult<()> {
		let mut node = NodeState::from_wire(&value)?;
		if node.node_id.is_empty() || node.node_id.starts_with("seed-") {
			return Ok(());
		}
		let mut membership = self.membership.write();
		if node.node_id == membership.node_id {
			return Ok(());
		}
		if node.last_seen <= 0.0 {
			node.last_seen = unix_now();
		}
		membership.peers.retain(|peer_id, peer| {
			!(peer_id.starts_with("seed-")
				&& peer.advertise == node.advertise
				&& peer_id != &node.node_id)
		});
		membership.peers.insert(node.node_id.clone(), node);
		membership.bump_expected();
		self.save_membership(&membership)
	}

	fn local_epoch_u64(&self, sid: &str) -> u64 {
		self
			.owners
			.read()
			.get(sid)
			.map_or(OWNER_EPOCH_FLOOR, |(_, epoch)| *epoch)
	}

	async fn acquire_writable_leases(
		&self,
		params: &Map<String, Value>,
		epoch: u64,
	) -> MeshResult<Vec<LeaseRecord>> {
		let volumes = writable_volumes(params)?;
		if volumes.is_empty() {
			return Ok(Vec::new());
		}
		if self.enabled() && self.expected_members() < 3 {
			return Err(MeshError::with_code(
				"writable volumes need >=3 nodes on mesh contexts",
				"unsupported",
			));
		}
		let holder = self.node_id();
		let mut acquired: Vec<LeaseRecord> = Vec::new();
		for volume in volumes {
			match self.acquire_one_lease(&volume, &holder, epoch).await {
				Ok(record) => acquired.push(record),
				Err(err) => {
					let _ = self.release_lease_records(&acquired).await;
					return Err(err);
				},
			}
		}
		Ok(acquired)
	}

	async fn acquire_one_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
	) -> MeshResult<LeaseRecord> {
		if let Some(store) = &self.cluster_store {
			return store
				.acquire_lease(volume, holder, epoch, DEFAULT_TTL)
				.map_err(engine_to_mesh);
		}
		let local = self
			.leases
			.vote_grant(volume, holder, epoch, DEFAULT_TTL)
			.map_err(engine_to_mesh)?;
		let needed = lease::majority_needed(self.expected_members()).map_err(engine_to_mesh)?;
		let mut votes = usize::from(local.granted);
		let mut record = local.record.clone().filter(|_| local.granted);
		let payload = json!({
			"volume": volume,
			"holder_node": holder,
			"epoch": epoch,
			"ttl": DEFAULT_TTL,
		});
		for peer in self.peers() {
			if !peer.healthy {
				continue;
			}
			let posted = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/grant", &payload, None)
				.await;
			let Ok(object) = posted else {
				continue;
			};
			let decision = lease_decision(Value::Object(object))?;
			if decision.granted {
				votes += 1;
				if record.is_none() {
					record = decision.record;
				}
			}
		}
		if votes >= needed && local.granted {
			return record.ok_or_else(|| MeshError::invalid("lease grant missing record"));
		}
		let _ = self
			.leases
			.vote_release(volume, holder, epoch)
			.map_err(engine_to_mesh);
		let release = json!({"volume": volume, "holder_node": holder, "epoch": epoch});
		for peer in self.peers() {
			let _ = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/release", &release, None)
				.await;
		}
		Err(MeshError::with_code(
			format!("lease grant for volume '{volume}' got {votes}/{needed} votes"),
			lease::LEASE_UNAVAILABLE_CODE,
		))
	}

	/// Renew one held lease by strict majority. Unlike a failed grant, a
	/// failed renew does NOT release votes: the holder keeps writing until its
	/// renew deadline passes, then stops itself.
	async fn renew_one_lease(
		&self,
		volume: &str,
		holder: &str,
		epoch: u64,
		ttl: f64,
	) -> MeshResult<LeaseRecord> {
		let local = self
			.leases
			.vote_renew(volume, holder, epoch, ttl)
			.map_err(engine_to_mesh)?;
		let needed = lease::majority_needed(self.expected_members()).map_err(engine_to_mesh)?;
		let mut votes = usize::from(local.granted);
		let mut record = local.record.clone().filter(|_| local.granted);
		let payload = json!({
			"volume": volume,
			"holder_node": holder,
			"epoch": epoch,
			"ttl": ttl,
		});
		for peer in self.peers() {
			if !peer.healthy {
				continue;
			}
			let posted = self
				.transport
				.post(&peer.advertise, "/v1/mesh/lease/renew", &payload, None)
				.await;
			let Ok(object) = posted else {
				continue;
			};
			let decision = lease_decision(Value::Object(object))?;
			if decision.granted {
				votes += 1;
				if record.is_none() {
					record = decision.record;
				}
			}
		}
		if votes >= needed && local.granted {
			return record.ok_or_else(|| MeshError::invalid("lease renew missing record"));
		}
		Err(MeshError::with_code(
			format!("lease renew for volume '{volume}' got {votes}/{needed} votes"),
			lease::LEASE_UNAVAILABLE_CODE,
		))
	}

	/// Renew local writable-volume leases, stopping writers that miss their
	/// renew deadline. Port of Python `renew_writable_volume_leases_once`.
	async fn renew_writable_volume_leases(&self) -> MeshResult<()> {
		if !self.enabled() {
			return Ok(());
		}
		let node_id = self.node_id();
		for (name, leases, writable) in self.engine.engine.mesh_volume_lease_records() {
			let mut refreshed: Vec<LeaseRecord> = Vec::new();
			let mut stop_reason: Option<String> = None;
			for volume in writable {
				let Some(data) = leases.get(&volume).and_then(Value::as_object) else {
					continue;
				};
				let holder = data
					.get("holder")
					.and_then(Value::as_str)
					.unwrap_or_default()
					.to_owned();
				if holder != node_id {
					continue;
				}
				let epoch = data.get("epoch").and_then(Value::as_u64).unwrap_or(0);
				let ttl = data
					.get("ttl")
					.and_then(Value::as_f64)
					.unwrap_or(DEFAULT_TTL);
				let granted_at = data
					.get("granted_at")
					.and_then(Value::as_f64)
					.unwrap_or(0.0);
				let renew_deadline = data
					.get("renew_deadline")
					.and_then(Value::as_f64)
					.unwrap_or(granted_at + ttl / 2.0);
				match self.renew_one_lease(&volume, &holder, epoch, ttl).await {
					Ok(lease) => refreshed.push(lease),
					Err(err) => {
						if unix_now() >= renew_deadline {
							stop_reason = Some(err.message.clone());
							break;
						}
						tracing::warn!("lease renew deferred for {name}/{volume}: {}", err.message);
					},
				}
			}
			if let Some(reason) = stop_reason {
				tracing::warn!("stopping {name} after writable-volume lease renewal failure: {reason}");
				let stopped = self.engine.engine.mesh_stop_sandbox(&name);
				let released = self.release_recorded_leases(&name, &leases).await;
				stopped.map_err(engine_to_mesh)?;
				released?;
			} else if !refreshed.is_empty() {
				let values = refreshed
					.iter()
					.map(lease_record_value)
					.collect::<MeshResult<Vec<_>>>()?;
				self
					.engine
					.engine
					.mesh_record_volume_leases(&name, values)
					.map_err(engine_to_mesh)?;
			}
		}
		Ok(())
	}

	/// Release every vote in a recorded `volume_leases` map, then clear the
	/// sandbox's lease metadata. The clear only happens after a successful
	/// release so an incomplete release stays visible on the record.
	async fn release_recorded_leases(
		&self,
		sid: &str,
		leases: &Map<String, Value>,
	) -> MeshResult<()> {
		let values = leases.values().cloned().collect::<Vec<_>>();
		tokio::task::yield_now().await;
		self.release_lease_values(values)?;
		self
			.engine
			.engine
			.mesh_clear_volume_leases(sid)
			.map_err(engine_to_mesh)
	}

	async fn release_lease_records(&self, leases: &[LeaseRecord]) -> MeshResult<()> {
		let values = leases
			.iter()
			.map(lease_record_value)
			.collect::<MeshResult<Vec<_>>>()?;
		tokio::task::yield_now().await;
		self.release_lease_values(values)
	}

	#[allow(clippy::unnecessary_wraps, reason = "trait signature compatibility")]
	fn release_lease_values(&self, leases: Vec<Value>) -> MeshResult<()> {
		for value in leases {
			let Some((volume, holder, epoch)) = lease_tuple(&value) else {
				continue;
			};
			if let Some(store) = &self.cluster_store
				&& let Err(e) = store.release_lease(&volume, &holder, epoch)
			{
				eprintln!("Error releasing lease in cluster_store: {e}");
			}
		}
		Ok(())
	}

	fn record_orphan_for_dead_owner(&self, dead: &str) {
		let owned = if let Some(store) = &self.cluster_store {
			match store.owned_by(dead) {
				Ok(owned) => owned,
				Err(error) => {
					tracing::error!(%error, owner = dead, "failed to query orphaned sandboxes");
					return;
				},
			}
		} else {
			self
				.owners
				.read()
				.iter()
				.filter(|(_, (owner, _))| owner.as_str() == dead)
				.map(|(sid, _)| sid.clone())
				.collect()
		};
		let mut orphans = self.orphans.lock();
		for sid in owned {
			if !orphans
				.iter()
				.any(|(seen_sid, seen_dead)| seen_sid == &sid && seen_dead == dead)
			{
				orphans.push((sid, dead.to_owned()));
			}
		}
	}

	async fn materialize_replica(
		&self,
		mut shared: replica::ReplicaRecord,
	) -> Result<replica::ReplicaRecord> {
		let _cache_use = self
			.replica_cache
			.acquire(&shared.sid, &shared.object_key, &shared.digest);
		let local = self
			.replicas
			.get(&shared.sid)
			.filter(|record| record.digest == shared.digest && record.object_key == shared.object_key);
		if let Some(local) = &local
			&& Path::new(&local.snapshot_dir).is_dir()
			&& verify_replica_digest(Path::new(&local.snapshot_dir), &shared.digest).is_ok()
		{
			shared.snapshot_dir.clone_from(&local.snapshot_dir);
			if shared.needs_secrets
				&& local.source_node == shared.source_node
				&& local.source_epoch == shared.source_epoch
				&& local.checkpoint_generation == shared.checkpoint_generation
				&& self.replicas.secrets_ready(&shared.sid)
			{
				shared.params = local.params.clone();
			}
			self.replicas.put_record(shared.clone())?;
			self.sweep_replica_cache();
			return Ok(shared);
		}
		if shared.needs_secrets
			&& let Some(local) = local
			&& local.source_node == shared.source_node
			&& local.source_epoch == shared.source_epoch
			&& local.checkpoint_generation == shared.checkpoint_generation
			&& self.replicas.secrets_ready(&shared.sid)
		{
			shared.params = local.params;
		}
		let snapshot_name = Path::new(&shared.snapshot_dir)
			.file_name()
			.and_then(|name| name.to_str())
			.filter(|name| !name.is_empty() && *name != "." && *name != "..")
			.ok_or_else(|| {
				EngineError::invalid("replica snapshot path has no valid final component")
			})?;
		let extract_root =
			self
				.replica_cache
				.root_for(&shared.sid, &shared.object_key, &shared.digest);
		let snapshot_dir = extract_root.join(snapshot_name);
		if snapshot_dir.is_dir() && verify_replica_digest(&snapshot_dir, &shared.digest).is_err() {
			let metadata = fs::symlink_metadata(&extract_root)?;
			if metadata.file_type().is_symlink() || !metadata.is_dir() {
				return Err(EngineError::invalid("replica cache root is not a controlled directory"));
			}
			tokio::fs::remove_dir_all(&extract_root).await?;
		}
		if !snapshot_dir.is_dir() {
			let s3 = self
				.s3_client
				.as_ref()
				.ok_or_else(|| EngineError::engine("shared replica requires S3"))?;
			let store = self.cluster_store.as_ref().ok_or_else(|| {
				EngineError::engine("shared replica read requires the fenced cluster store")
			})?;
			let _read = store.acquire_replica_read(&shared.object_key)?;
			let key = shared.object_key.clone();
			let stat = s3
				.head_object(&key)
				.await
				.map_err(|error| EngineError::engine(format!("reading shared replica: {error}")))?;
			let staging = extract_root.with_extension(format!("{}.partial", uuid::Uuid::new_v4()));
			let key_id = shared
				.params
				.get("encryption_key_id")
				.and_then(Value::as_str)
				.filter(|key_id| !key_id.is_empty())
				.ok_or_else(|| EngineError::invalid("shared replica requires encryption_key_id"))?;
			let key_snapshot = Arc::new(Keyring::open(&self.home)?.snapshot(key_id)?);
			let staged_snapshot = download_s3_bundle(
				s3,
				&key,
				&stat,
				&staging,
				Arc::clone(&key_snapshot),
				Some(REPLICA_ARCHIVE_ROOT),
			)
			.await?;
			if let Err(error) = verify_replica_digest(&staged_snapshot, &shared.digest) {
				let _ = tokio::fs::remove_dir_all(&staging).await;
				return Err(error);
			}
			let destination_snapshot = staging.join(snapshot_name);
			if staged_snapshot != destination_snapshot {
				tokio::fs::rename(&staged_snapshot, &destination_snapshot).await?;
			}
			match fs::symlink_metadata(&extract_root) {
				Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
					return Err(EngineError::invalid(
						"replica cache root is not a controlled directory",
					));
				},
				Ok(_) => tokio::fs::remove_dir_all(&extract_root).await?,
				Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
				Err(error) => return Err(error.into()),
			}
			tokio::fs::rename(&staging, &extract_root).await?;
		}
		shared.snapshot_dir = snapshot_dir.to_string_lossy().into_owned();
		self.replicas.put_record(shared.clone())?;
		self.sweep_replica_cache();
		Ok(shared)
	}

	fn sweep_replica_cache(&self) {
		let records = match self.authoritative_replica_records() {
			Ok(records) => records,
			Err(error) => {
				tracing::warn!(%error, "loading authoritative replica cache roots");
				return;
			},
		};
		if let Err(error) = self.replica_cache.sweep(&records) {
			tracing::warn!(%error, "sweeping materialized replica cache");
		}
	}

	fn authoritative_replica_records(&self) -> Result<Vec<replica::ReplicaRecord>> {
		let Some(store) = &self.cluster_store else {
			return Ok(self.replicas.records());
		};
		let records = store.replica_records()?;
		for local in self.replicas.records() {
			let current = records
				.iter()
				.any(|shared| shared.sid == local.sid && shared.object_key == local.object_key);
			if !current
				&& !self
					.replica_cache
					.is_in_use(&local.sid, &local.object_key, &local.digest)
				&& let Err(error) = self
					.replicas
					.remove_if_object_key(&local.sid, &local.object_key)
			{
				tracing::warn!(%error, sid = %local.sid, "dropping stale local replica metadata");
			}
		}
		Ok(records)
	}
}

pub(crate) fn verify_replica_digest(snapshot_dir: &Path, expected: &str) -> Result<()> {
	let actual = cas::snapshot_digest(snapshot_dir)?;
	if actual == expected {
		Ok(())
	} else {
		Err(EngineError::invalid(format!(
			"shared replica checkpoint digest mismatch: expected {expected}, got {actual}"
		)))
	}
}

#[cfg(test)]
pub(crate) fn install_replica_cache<F>(
	cache_root: &Path,
	snapshot_name: &str,
	expected_digest: &str,
	extract: F,
) -> Result<PathBuf>
where
	F: FnOnce(&Path) -> Result<()>,
{
	let final_snapshot = cache_root.join(snapshot_name);
	if final_snapshot.is_dir() && verify_replica_digest(&final_snapshot, expected_digest).is_ok() {
		return Ok(final_snapshot);
	}
	let _ = fs::remove_dir_all(cache_root);
	let staging = cache_root.with_extension(format!("{}.partial", uuid::Uuid::new_v4()));
	let _ = fs::remove_dir_all(&staging);
	extract(&staging)?;
	let staged_snapshot = staging.join(snapshot_name);
	if !staged_snapshot.is_dir() {
		let _ = fs::remove_dir_all(&staging);
		return Err(EngineError::invalid("replica extraction omitted checkpoint directory"));
	}
	if let Err(err) = verify_replica_digest(&staged_snapshot, expected_digest) {
		let _ = fs::remove_dir_all(&staging);
		return Err(err);
	}
	fs::rename(&staging, cache_root)?;
	Ok(final_snapshot)
}

#[cfg(test)]
pub(crate) fn refresh_replica_metadata(
	mut shared: replica::ReplicaRecord,
	local_snapshot_dir: Option<&str>,
	local_secrets: Option<&Map<String, Value>>,
) -> replica::ReplicaRecord {
	if let Some(path) = local_snapshot_dir {
		shared.snapshot_dir = path.to_owned();
	}
	if shared.needs_secrets
		&& let Some(params) = local_secrets
		&& let Some(secrets) = params.get("secrets")
	{
		shared.params.insert("secrets".to_owned(), secrets.clone());
	}
	shared
}

fn legacy_portable_key(params: &Map<String, Value>, config: &ServeConfig) -> Option<String> {
	if params.get("ha").and_then(Value::as_str).unwrap_or("off") == "off" {
		return None;
	}
	if params
		.get("encryption_key_id")
		.and_then(Value::as_str)
		.is_some_and(|key| !key.is_empty() && key != "default")
	{
		return None;
	}
	let tenant_key = params
		.get("owner_tenant")
		.and_then(Value::as_str)
		.and_then(|tenant| config.tenant_keys.get(tenant))
		.filter(|key| !key.is_empty() && key.as_str() != "default");
	tenant_key.cloned().or_else(|| {
		config
			.portable_history_key_id
			.as_deref()
			.filter(|key| !key.is_empty() && *key != "default")
			.map(str::to_owned)
	})
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap, HashMap},
		time::Duration,
	};

	use serde_json::{Map, Value, json};

	use super::{
		MeshRuntime, expired_owner_deadlines, extracted_bundle_root, fence_blocks_successor,
		fence_identity, owner_watchdog_deadline, record_is_staging, renewal_failure_fences,
		restored_metadata, should_fence_epoch, verify_replica_digest,
	};

	#[test]
	fn rejects_corrupted_replica_bundle_content() {
		let checkpoint = tempfile::tempdir().unwrap();

		std::fs::write(checkpoint.path().join("agent-ready.json"), "{}").unwrap();
		std::fs::write(checkpoint.path().join("memory.bin"), "original").unwrap();
		let digest = crate::image::cas::snapshot_digest(checkpoint.path()).unwrap();
		std::fs::write(checkpoint.path().join("memory.bin"), "corrupted").unwrap();

		assert!(verify_replica_digest(checkpoint.path(), &digest).is_err());
	}

	#[test]
	fn extracted_bundle_root_requires_one_expected_directory() {
		let staging = tempfile::tempdir().unwrap();
		let checkpoint = staging.path().join("checkpoint");
		std::fs::create_dir(&checkpoint).unwrap();
		assert_eq!(extracted_bundle_root(staging.path(), Some("checkpoint")).unwrap(), checkpoint);
		assert!(extracted_bundle_root(staging.path(), Some("other")).is_err());
		std::fs::create_dir(staging.path().join("extra")).unwrap();
		assert!(extracted_bundle_root(staging.path(), Some("checkpoint")).is_err());
	}

	#[test]
	fn successful_renewal_uses_request_start_and_safety_margin() {
		let now = tokio::time::Instant::now();
		let request_started = now - Duration::from_secs(2);
		assert_eq!(
			owner_watchdog_deadline(request_started, 30.0),
			Some(request_started + Duration::from_secs(25))
		);
		assert_eq!(owner_watchdog_deadline(now, f64::NAN), None);
		assert_eq!(owner_watchdog_deadline(now, 0.0), None);
	}

	#[test]
	fn transient_renewal_failure_fences_only_at_cached_deadline() {
		let now = tokio::time::Instant::now();
		assert!(!renewal_failure_fences(now, Some(now + Duration::from_secs(1)), false));
		assert!(renewal_failure_fences(now, Some(now), false));
	}

	#[test]
	fn authority_loss_and_missing_restart_deadline_fence_immediately() {
		let now = tokio::time::Instant::now();
		assert!(renewal_failure_fences(now, Some(now + Duration::from_mins(1)), true));
		assert!(renewal_failure_fences(now, None, false));
	}

	#[test]
	fn deadline_scan_selects_only_elapsed_owners() {
		let now = tokio::time::Instant::now();
		let deadlines = HashMap::from([
			("elapsed".to_owned(), now),
			("serving".to_owned(), now + Duration::from_secs(1)),
		]);
		assert_eq!(expired_owner_deadlines(now, &deadlines), vec!["elapsed".to_owned()]);
	}

	#[test]
	fn stale_epoch_fence_never_tears_down_its_successor() {
		assert!(should_fence_epoch(Some(7), 7));
		assert!(!should_fence_epoch(Some(8), 7));
		assert!(!should_fence_epoch(None, 7));
	}

	#[test]
	fn pending_rollback_is_startup_staging() {
		let mut params = Map::<String, Value>::new();
		params.insert("_mesh_rollback_pending".to_owned(), Value::Bool(true));
		assert!(record_is_staging(&params));
	}

	#[test]
	fn restored_metadata_keeps_create_fields_and_never_persists_secrets() {
		let record = crate::mesh::reconciler::SandboxRecord {
			name:   "sandbox".to_owned(),
			detail: Map::from_iter([
				("name".to_owned(), json!("sandbox")),
				("image".to_owned(), json!("debian")),
				("ha".to_owned(), json!("active")),
				("restart_policy".to_owned(), json!("always")),
				("credentials".to_owned(), json!(["tenant-token"])),
				("s3_mounts".to_owned(), json!({"/mnt/data": "s3://bucket/prefix"})),
				("owner_tenant".to_owned(), json!("tenant-a")),
				("encryption_key_id".to_owned(), json!("key-a")),
				("idle_timeout_secs".to_owned(), json!(0.0)),
				("activity_threshold_bytes".to_owned(), json!(512)),
				("persistence".to_owned(), json!({"type": "ephemeral"})),
				("secrets".to_owned(), json!({"token": "must-not-persist"})),
				("status".to_owned(), json!("running")),
			]),
		};
		let (params, ha, restart_policy) = restored_metadata(&record).unwrap();

		assert_eq!(params.get("image"), Some(&json!("debian")));
		assert_eq!(params.get("credentials"), Some(&json!(["tenant-token"])));
		assert_eq!(params.get("s3_mounts"), Some(&json!({"/mnt/data": "s3://bucket/prefix"})));
		assert_eq!(params.get("owner_tenant"), Some(&json!("tenant-a")));
		assert_eq!(params.get("encryption_key_id"), Some(&json!("key-a")));
		assert_eq!(params.get("idle_timeout_secs"), Some(&json!(0.0)));
		assert_eq!(params.get("activity_threshold_bytes"), Some(&json!(512)));
		assert_eq!(params.get("persistence"), Some(&json!({"type": "ephemeral"})));
		assert!(!params.contains_key("secrets"));
		assert!(!params.contains_key("status"));
		assert_eq!(ha, "active");
		assert_eq!(restart_policy, "always");
	}
	#[test]
	fn expired_restoring_identity_fences_newer_epoch_over_stale_owner() {
		let mut owners = BTreeMap::new();
		owners.insert("sandbox".to_owned(), ("node-a".to_owned(), 5));
		let mut restoring = BTreeMap::new();
		restoring.insert("sandbox".to_owned(), ("node-a".to_owned(), 6));

		assert_eq!(fence_identity("sandbox", &owners, &restoring), Some(("node-a".to_owned(), 6)));
	}

	#[test]
	fn fence_retry_epoch_is_nonserving_when_restore_map_is_empty() {
		let restoring = BTreeMap::new();
		let fenced = HashMap::from([("sandbox".to_owned(), 1)]);

		assert_eq!(MeshRuntime::nonserving_epoch("sandbox", &restoring, &fenced), Some(1));
	}

	#[test]
	fn active_fence_blocks_epoch_two_successor_until_teardown_completes() {
		let fences = HashMap::from([("sandbox".to_owned(), 1)]);

		assert!(fence_blocks_successor(&fences, "sandbox"));
		assert!(!fence_blocks_successor(&HashMap::new(), "sandbox"));
	}
}

impl DeleteTombstoneStore for super::cluster_store::ProductionStore {
	fn list_deleting(&self) -> MeshResult<Vec<CreateRecordWire>> {
		self
			.list_deleting()
			.map(|records| records.into_iter().map(record_wire).collect())
			.map_err(engine_to_mesh)
	}

	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> MeshResult<()> {
		self
			.commit_delete(sid, owner, epoch)
			.map_err(engine_to_mesh)
	}
}

impl DeleteTombstoneEngine for MeshEngineAdapter {
	fn teardown_candidate(&self, sid: String, expected_epoch: i64) -> BoxFuture<'_, MeshResult<()>> {
		MeshEngine::teardown_candidate(self, sid, expected_epoch)
	}

	fn delete_portable_history(
		&self,
		sid: String,
		owner: String,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		MeshEngine::delete_portable_history(self, sid, owner, epoch)
	}
}

impl OwnershipHandoff for MeshRuntime {
	fn current_owner(&self, sid: &str) -> Result<Option<(String, i64)>> {
		Ok(self
			.owners
			.read()
			.get(sid)
			.map(|(owner, epoch)| (owner.clone(), u64_to_i64(*epoch))))
	}

	fn begin_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative restore epoch"))?;
		if fence_blocks_successor(&self.fence_retries.lock(), sid) {
			return Err(EngineError::busy(format!(
				"restore handoff waits for fenced teardown of sandbox {sid}"
			)));
		}
		let expected = (owner.to_owned(), epoch);
		if let Some(store) = &self.cluster_store {
			let resuming = if store.begin_resume(sid, owner, epoch as i64)? {
				true
			} else {
				store.suspend_marker(sid)?.is_some_and(|marker| {
					marker.state == "resuming" && marker.owner == owner && marker.epoch == epoch as i64
				})
			};
			let rolling_back = matches!(
				store.rollback_disposition(sid, owner, epoch as i64)?,
				super::cluster_store::RollbackDisposition::RollingBack(_)
			);
			if !resuming && !rolling_back {
				return Err(EngineError::busy(format!("restore handoff fenced for sandbox {sid}")));
			}
			if resuming {
				self
					.suspended_restores
					.write()
					.insert((sid.to_owned(), owner.to_owned(), epoch));
			}
		}
		self.restoring.write().insert(sid.to_owned(), expected);
		self.owner_lease_deadlines.lock().remove(sid);
		self.owners.write().remove(sid);
		Ok(())
	}

	fn acquire_restore_volume_leases<'a>(
		&'a self,
		_sid: &'a str,
		_owner: &'a str,
		epoch: i64,
		params: &'a mut Map<String, Value>,
	) -> BoxFuture<'a, Result<Vec<Value>>> {
		async move {
			let epoch =
				u64::try_from(epoch).map_err(|_| EngineError::invalid("negative restore epoch"))?;
			// Archive metadata is evidence only; fresh grants bind the candidate
			// to its newly claimed epoch.
			params.remove("volume_leases");
			params.remove("_mesh_volume_leases");
			MeshLeaseManager::acquire_writable_volume_leases(self, params.clone(), u64_to_i64(epoch))
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn persist_restore_volume_leases<'a>(
		&'a self,
		sid: &'a str,
		leases: &'a [Value],
	) -> BoxFuture<'a, Result<()>> {
		async move {
			self
				.engine
				.engine
				.mesh_record_volume_leases(sid, leases.to_vec())
		}
		.boxed()
	}

	fn release_restore_volume_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, Result<()>> {
		async move { self.release_lease_values(leases).map_err(mesh_to_engine) }.boxed()
	}

	fn release_source_volume_leases<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<()>> {
		async move {
			MeshLeaseManager::release_record_volume_leases(self, sid.to_owned())
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn commit_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative restore epoch"))?;
		let expected = (owner.to_owned(), epoch);
		let mut restoring = self.restoring.write();
		if restoring.get(sid) != Some(&expected) {
			return Err(EngineError::busy(format!("restore handoff fenced for sandbox {sid}")));
		}

		// A portable rollback restores through Engine rather than the mesh
		// reconciler. Take its just-created local view as the source of truth,
		// but keep only durable, non-secret create metadata.
		let restored = sandbox_record(self.engine.engine.mesh_get_view(sid)?)?;
		let (params, ha, restart_policy) = restored_metadata(&restored)?;
		let deadline = self.renew_and_arm_serving_owner(sid, owner, epoch)?;
		if let Some(store) = &self.cluster_store {
			let Some(mut record) = store.resolve(sid)? else {
				return Err(EngineError::not_found(format!("no create record for sandbox {sid}")));
			};
			if record.owner != owner || record.epoch != u64_to_i64(epoch) {
				return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
			}
			record.params = params;
			record.ha = ha;
			record.restart_policy = restart_policy;
			// A suspended marker becomes Running only in this same authoritative
			// metadata write; generic rollback retains its existing path.
			if self
				.suspended_restores
				.read()
				.contains(&(sid.to_owned(), owner.to_owned(), epoch))
			{
				store.finish_resume(&record)?;
			} else {
				store.update_exact_record(&record)?;
			}
		} else {
			let Some(mut record) = self.records.get(sid) else {
				return Err(EngineError::not_found(format!("no create record for sandbox {sid}")));
			};
			if record.owner != owner || record.epoch != u64_to_i64(epoch) {
				return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
			}
			record.params = params;
			record.ha = ha;
			record.restart_policy = restart_policy;
			self.records.put(record)?;
		}

		// Do not advertise the candidate until its exact durable metadata
		// commit succeeded. Holding the restoring guard prevents a stale
		// handoff from removing a later claimant's staging entry.
		self
			.suspended_restores
			.write()
			.remove(&(sid.to_owned(), owner.to_owned(), epoch));
		restoring.remove(sid);
		self.owners.write().insert(sid.to_owned(), expected);
		self
			.owner_lease_deadlines
			.lock()
			.insert(sid.to_owned(), deadline);
		Ok(())
	}

	fn abort_restore(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative restore epoch"))?;
		let expected = (owner.to_owned(), epoch);
		let suspended =
			self
				.suspended_restores
				.read()
				.contains(&(sid.to_owned(), owner.to_owned(), epoch));
		if suspended && let Some(store) = &self.cluster_store {
			store.abort_resume(sid, owner, epoch as i64)?;
		}
		self
			.suspended_restores
			.write()
			.remove(&(sid.to_owned(), owner.to_owned(), epoch));
		let mut restoring = self.restoring.write();
		if restoring.get(sid) == Some(&expected) {
			restoring.remove(sid);
		}
		Ok(())
	}

	fn prepare_suspend(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		point: &str,
		generation: u64,
	) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative suspend epoch"))?;
		let expected = (owner.to_owned(), epoch);
		if self.owners.read().get(sid) != Some(&expected) {
			return Err(EngineError::busy(format!("suspend handoff fenced for sandbox {sid}")));
		}
		if let Some(store) = &self.cluster_store {
			store.prepare_suspend(sid, owner, epoch as i64, point, generation)?;
		}
		self.restoring.write().insert(sid.to_owned(), expected);
		self.owner_lease_deadlines.lock().remove(sid);
		self.owners.write().remove(sid);
		Ok(())
	}

	fn commit_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative suspend epoch"))?;
		let expected = (owner.to_owned(), epoch);
		if let Some(store) = &self.cluster_store {
			store.commit_suspend(sid, owner, epoch as i64)?;
		}
		if self.restoring.write().remove(sid) != Some(expected) {
			return Err(EngineError::busy(format!("suspend handoff fenced for sandbox {sid}")));
		}
		Ok(())
	}

	fn abort_suspend(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative suspend epoch"))?;
		let expected = (owner.to_owned(), epoch);
		if let Some(store) = &self.cluster_store {
			store.abort_suspend(sid, owner, epoch as i64)?;
		}
		if self.restoring.write().remove(sid) == Some(expected.clone()) {
			self.owners.write().insert(sid.to_owned(), expected);
		}
		Ok(())
	}

	fn prepare_rollback(
		&self,
		sid: &str,
		owner: &str,
		epoch: i64,
		target: &str,
		safety: &str,
		operation_generation: u64,
		checkpoint_generation: u64,
	) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative rollback epoch"))?;
		let expected = (owner.to_owned(), epoch);
		if self.owners.read().get(sid) != Some(&expected) {
			return Err(EngineError::busy(format!("rollback handoff fenced for sandbox {sid}")));
		}
		if let Some(store) = &self.cluster_store {
			store.prepare_rollback(
				sid,
				owner,
				epoch as i64,
				target,
				safety,
				operation_generation,
				checkpoint_generation,
			)?;
		}
		self.restoring.write().insert(sid.to_owned(), expected);
		self.owner_lease_deadlines.lock().remove(sid);
		self.owners.write().remove(sid);
		Ok(())
	}

	fn commit_rollback(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative rollback epoch"))?;
		let expected = (owner.to_owned(), epoch);
		let deadline = self.renew_and_arm_serving_owner(sid, owner, epoch)?;
		if let Some(store) = &self.cluster_store {
			let restored = sandbox_record(self.engine.engine.mesh_get_view(sid)?)?;
			let (params, ha, restart_policy) = restored_metadata(&restored)?;
			let Some(mut record) = store.resolve(sid)? else {
				return Err(EngineError::not_found(format!("no create record for sandbox {sid}")));
			};
			if record.owner != owner || record.epoch != epoch as i64 {
				return Err(EngineError::busy(format!("rollback ownership fenced for sandbox {sid}")));
			}
			record.params = params;
			record.ha = ha;
			record.restart_policy = restart_policy;
			store.finish_rollback(&record)?;
		}
		let mut owners = self.owners.write();
		let mut restoring = self.restoring.write();
		match restoring.get(sid) {
			Some(current) if current == &expected => {
				restoring.remove(sid);
			},
			None if owners.get(sid) == Some(&expected) => {},
			_ => {
				return Err(EngineError::busy(format!("rollback handoff fenced for sandbox {sid}")));
			},
		}
		owners.insert(sid.to_owned(), expected);
		self
			.owner_lease_deadlines
			.lock()
			.insert(sid.to_owned(), deadline);
		Ok(())
	}

	fn abort_rollback(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative rollback epoch"))?;
		let expected = (owner.to_owned(), epoch);
		if let Some(store) = &self.cluster_store {
			store.abort_rollback(sid, owner, epoch as i64)?;
		}
		if self.restoring.write().remove(sid) == Some(expected.clone()) {
			self.owners.write().insert(sid.to_owned(), expected);
		}
		Ok(())
	}

	fn begin_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		let epoch =
			u64::try_from(epoch).map_err(|_| EngineError::invalid("negative delete epoch"))?;
		if let Some(store) = &self.cluster_store {
			store.begin_delete(sid, owner, epoch as i64)?;
		} else if !self
			.records
			.get(sid)
			.is_some_and(|record| record.owner == owner && record.epoch == epoch as i64)
		{
			return Err(EngineError::busy(format!("delete handoff fenced for sandbox {sid}")));
		}
		self.owners.write().remove(sid);
		Ok(())
	}

	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		if let Some(store) = &self.cluster_store {
			store.commit_delete(sid, owner, epoch)?;
		} else if self
			.records
			.get(sid)
			.is_some_and(|record| record.owner == owner && record.epoch == epoch)
		{
			self.records.remove(sid)?;
		}
		Ok(())
	}
}

impl MeshControl for MeshRuntime {
	fn node_id(&self) -> String {
		self.membership.read().node_id.clone()
	}

	fn advertise(&self) -> String {
		self.membership.read().advertise.clone()
	}

	fn default_advertise(&self) -> String {
		self.default_advertise.clone()
	}

	fn caps(&self) -> NodeCapsWire {
		caps_wire(self.membership.read().caps)
	}

	fn enabled(&self) -> bool {
		self.membership.read().enabled
	}

	fn peers(&self) -> Vec<PeerMember> {
		let membership = self.membership.read().clone();
		let now = unix_now();
		membership
			.peers
			.values()
			.map(|peer| PeerMember {
				node_id:   peer.node_id.clone(),
				advertise: peer.advertise.clone(),
				healthy:   peer.healthy(now, self.config.mesh_heartbeat_sec),
			})
			.collect()
	}

	fn expected_members(&self) -> usize {
		self.membership.read().expected_members
	}

	fn quorum_needed(&self) -> usize {
		self.membership.read().quorum_needed()
	}

	fn setup(
		&self,
		advertise: String,
		region: String,
		caps: Option<NodeCapsWire>,
	) -> MeshResult<MeshSetupResult> {
		let mut membership = self.membership.write();
		if membership.enabled && membership.advertise == advertise {
			return Ok(MeshSetupResult {
				blob:      state::encode_blob(&membership.advertise, &self.token.read()),
				node_id:   membership.node_id.clone(),
				advertise: membership.advertise.clone(),
			});
		}
		let node_id = self.node_id.clone();
		let caps = caps.map_or(membership.caps, node_caps);
		*membership = MembershipState::enabled(node_id, advertise, region, caps);
		self.save_membership(&membership)?;
		Ok(MeshSetupResult {
			blob:      state::encode_blob(&membership.advertise, &self.token.read()),
			node_id:   membership.node_id.clone(),
			advertise: membership.advertise.clone(),
		})
	}

	fn join(&self, blob: String, advertise: Option<String>, region: String) -> MeshResult<Value> {
		let (peer_url, token) = state::decode_blob(&blob)?;
		if self.token.read().is_empty() && !token.is_empty() {
			*self.token.write() = token;
		}
		let caps = self.membership.read().caps;
		let mut membership = MembershipState::enabled(
			self.node_id.clone(),
			advertise.unwrap_or_else(|| self.default_advertise.clone()),
			region,
			caps,
		);
		let mut peer = NodeState::new("seed", peer_url.clone());
		peer.node_id = format!("seed-{}", state::new_node_id());
		peer.last_seen = unix_now();
		membership.peers.insert(peer.node_id.clone(), peer);
		membership.bump_expected();
		self.save_membership(&membership)?;
		*self.membership.write() = membership.clone();
		Ok(json!({
			"ok": true,
			"node_id": membership.node_id,
			"advertise": membership.advertise,
			"peers": [peer_url],
		}))
	}

	fn request_replication(&self) {
		self.replicate_notify.notify_one();
	}

	fn leave(&self) -> MeshResult<()> {
		let mut membership = self.membership.write();
		membership.enabled = false;
		membership.peers.clear();
		membership.expected_members = 1;
		self.save_membership(&membership)
	}

	fn status(&self) -> MeshResult<Value> {
		let membership = self.membership.read().clone();
		let self_state = self.local_state().to_wire();
		let peers = membership
			.peers
			.values()
			.map(NodeState::to_wire)
			.collect::<Vec<_>>();
		Ok(json!({
			"enabled": membership.enabled,
			"node_id": membership.node_id,
			"advertise": membership.advertise,
			"region": membership.region,
			"expected_members": membership.expected_members,
			"quorum": membership.quorum_needed(),
			"self": self_state,
			"peers": peers,
		}))
	}

	fn register(&self, state: Map<String, Value>) -> MeshResult<Value> {
		self.merge_node_state(Value::Object(state))?;
		Ok(json!({"members": self.known_members_wire()}))
	}

	fn heartbeat(&self, state: Map<String, Value>, known: Vec<Value>) -> MeshResult<Value> {
		self.merge_node_state(Value::Object(state))?;
		for item in known {
			let _ = self.merge_node_state(item);
		}
		Ok(json!({"state": self.self_state_wire(), "known": self.known_members_wire()}))
	}

	fn depart(&self, node_id: &str) {
		let mut membership = self.membership.write();
		if membership.depart(node_id) {
			let _ = self.save_membership(&membership);
			self.record_orphan_for_dead_owner(node_id);
		}
	}

	fn is_peer_healthy(&self, node_id: &str) -> bool {
		self
			.membership
			.read()
			.is_member_healthy(node_id, unix_now(), self.config.mesh_heartbeat_sec)
	}

	fn migration_target(&self, node_id: &str) -> MeshResult<PeerMember> {
		self
			.peers()
			.into_iter()
			.find(|peer| peer.node_id == node_id)
			.ok_or_else(|| MeshError::unreachable(format!("unknown mesh node {node_id}")))
	}

	fn replica_targets(&self, sid: &str, k: usize) -> Vec<String> {
		let members = self.peers();
		let mut peers = members
			.iter()
			.filter(|peer| peer.healthy)
			.map(|peer| peer.node_id.clone())
			.collect::<Vec<_>>();
		if peers.is_empty() {
			peers = members.into_iter().map(|peer| peer.node_id).collect();
		}
		place::sort_by_hrw_desc(sid, &mut peers);
		peers.truncate(k);
		peers
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		self
			.membership
			.read()
			.peers
			.get(node_id)
			.map(|peer| peer.advertise.clone())
	}

	fn find_template_provider(&self, key: &str) -> Option<(String, String)> {
		let now = unix_now();
		self
			.membership
			.read()
			.peers
			.values()
			.filter(|peer| peer.healthy(now, self.config.mesh_heartbeat_sec))
			.find_map(|peer| {
				peer
					.template_index
					.get(key)
					.cloned()
					.map(|digest| (peer.advertise.clone(), digest))
			})
	}

	fn request_template_key(&self, params: &Map<String, Value>) -> Option<String> {
		place::request_template_key(&PlacementRequest::from_map(params.clone()))
	}

	fn has_local_template_key(&self, key: &str) -> bool {
		let local = self.local_state();
		local.templates.iter().any(|template| template == key)
			|| local.template_index.contains_key(key)
	}

	fn authoritative_owner(&self, sid: &str) -> MeshResult<Option<(String, i64)>> {
		if let Some(store) = &self.cluster_store {
			return store
				.resolve(sid)
				.map(|record| record.map(|record| (record.owner, record.epoch)))
				.map_err(engine_to_mesh);
		}
		Ok(self
			.owners
			.read()
			.get(sid)
			.map(|(owner, epoch)| (owner.clone(), u64_to_i64(*epoch))))
	}

	fn local_epoch(&self, sid: &str) -> i64 {
		u64_to_i64(self.local_epoch_u64(sid))
	}

	fn owner_of(&self, sid: &str) -> MeshResult<Option<String>> {
		Ok(self.authoritative_owner(sid)?.map(|(owner, _)| owner))
	}

	fn record_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		let epoch_u64 = i64_to_u64(epoch);
		let request_started = tokio::time::Instant::now();
		let local_deadline = if node_id == self.node_id {
			match &self.cluster_store {
				Some(store) => match store.renew_owner_lease(sid, node_id, epoch) {
					Ok(Some(ttl)) if ttl.is_finite() && ttl > 0.0 => self
						.rearm_owner_watchdog(sid, request_started, ttl)
						.ok()
						.filter(|armed| *armed)
						.map(|_| request_started + Duration::from_secs_f64((ttl - 5.0).max(0.0))),
					_ => None,
				},
				None => Some(
					tokio::time::Instant::now() + Duration::from_secs_f64((DEFAULT_TTL - 5.0).max(0.0)),
				),
			}
		} else {
			Some(tokio::time::Instant::now())
		};
		let Some(deadline) = local_deadline else {
			self
				.restoring
				.write()
				.insert(sid.to_owned(), (node_id.to_owned(), epoch_u64));
			self
				.owner_lease_deadlines
				.lock()
				.insert(sid.to_owned(), tokio::time::Instant::now());
			self.owners.write().remove(sid);
			return;
		};
		let mut owners = self.owners.write();
		let accept = owners
			.get(sid)
			.is_none_or(|(_, existing_epoch)| epoch_u64 >= *existing_epoch);
		if accept {
			owners.insert(sid.to_owned(), (node_id.to_owned(), epoch_u64));
		}
		drop(owners);
		if accept && node_id == self.node_id {
			self
				.owner_lease_deadlines
				.lock()
				.insert(sid.to_owned(), deadline);
		}
	}

	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		self.record_owner(sid, node_id, epoch);
	}

	fn forget_owner(&self, sid: &str) {
		self.owners.write().remove(sid);
	}

	fn mark_unhealthy(&self, node_id: &str) {
		if let Some(peer) = self.membership.write().peers.get_mut(node_id) {
			peer.last_seen = 0.0;
		}
		self.record_orphan_for_dead_owner(node_id);
	}

	fn pinned_local(&self, params: &Map<String, Value>) -> bool {
		place::pinned_local(&PlacementRequest::from_map(params.clone()))
	}

	fn place(&self, params: &Map<String, Value>) -> MeshResult<String> {
		if !self.enabled() {
			return Ok(self.node_id());
		}
		Ok(place::place(
			&PlacementRequest::from_map(params.clone()),
			&self.context(),
			&self.all_nodes(),
			unix_now(),
		)?)
	}

	fn coordinator_for(&self, key: &str) -> String {
		let live = self
			.membership
			.read()
			.live_member_ids(unix_now(), self.config.mesh_heartbeat_sec);
		place::hrw_winner(key, live.iter().map(String::as_str))
			.map_or_else(|| MeshControl::node_id(self), str::to_owned)
	}

	fn idem_owner(&self, key: &str) -> Option<String> {
		self.idem_pins.read().get(key).cloned()
	}

	fn idem_pin(&self, key: &str, owner: &str) -> String {
		let mut pins = self.idem_pins.write();
		pins
			.entry(key.to_owned())
			.or_insert_with(|| owner.to_owned())
			.clone()
	}

	fn idem_unpin(&self, key: &str) {
		self.idem_pins.write().remove(key);
	}

	fn worker_begin(&self, key: &str) -> bool {
		self.workers.lock().insert(key.to_owned())
	}

	fn worker_end(&self, key: &str) {
		self.workers.lock().remove(key);
	}
}

impl HeartbeatView for MeshRuntime {
	fn heartbeat_peers(&self) -> Vec<PeerEndpoint> {
		self
			.membership
			.read()
			.peers
			.values()
			.map(|peer| PeerEndpoint {
				node_id:   peer.node_id.clone(),
				advertise: peer.advertise.clone(),
			})
			.collect()
	}

	fn self_state_wire(&self) -> Value {
		self.local_state().to_wire()
	}

	fn known_members_wire(&self) -> Vec<Value> {
		let mut members = vec![self.self_state_wire()];
		members.extend(
			self
				.membership
				.read()
				.peers
				.values()
				.map(NodeState::to_wire),
		);
		members
	}

	fn merge_heartbeat_response(&self, response: Map<String, Value>) -> MeshResult<()> {
		if let Some(state) = response.get("state") {
			self.merge_node_state(state.clone())?;
		}
		if let Some(Value::Array(known)) = response.get("known") {
			for item in known {
				let _ = self.merge_node_state(item.clone());
			}
		}
		Ok(())
	}

	fn reap_stale_peers(&self) {
		let now = unix_now();
		let reap_sec = self.config.mesh_reap_sec;
		let mut dead = Vec::new();
		{
			let mut membership = self.membership.write();
			membership.peers.retain(|node_id, peer| {
				let keep = peer.last_seen <= 0.0 || now - peer.last_seen <= reap_sec;
				if !keep {
					dead.push(node_id.clone());
				}
				keep
			});
			if !dead.is_empty() {
				let _ = self.save_membership(&membership);
			}
		}
		for node_id in dead {
			self.record_orphan_for_dead_owner(&node_id);
		}
	}
}

impl MeshRecordStore for MeshRuntime {
	fn get(&self, sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		if let Some(store) = &self.cluster_store {
			let mut record = store.resolve(sid).map_err(engine_to_mesh)?;
			if let Some(record) = &mut record {
				self.records.attach_secrets(record);
			}
			return Ok(record.map(record_wire));
		}
		Ok(self.records.get(sid).map(record_wire))
	}

	fn reserve_create_epoch(
		&self,
		sid: &str,
		owner: &str,
		idempotency_key: &str,
	) -> MeshResult<i64> {
		match &self.cluster_store {
			Some(store) => store
				.reserve_create_epoch(sid, owner, idempotency_key)
				.map_err(engine_to_mesh),
			None => Ok(0),
		}
	}

	fn try_begin_target_migration(
		&self,
		sid: &str,
		_migration_id: &str,
	) -> MeshResult<Box<dyn super::routes::MigrationLifecycleGuard>> {
		if fence_blocks_successor(&self.fence_retries.lock(), sid) {
			return Err(MeshError::unreachable(format!(
				"migration target waits for fenced teardown of sandbox {sid}"
			)));
		}
		begin_target_migration(&self.target_migrations, sid)
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		let mut record = create_record(record)?;
		if let Some(store) = &self.cluster_store {
			let (params, secrets) = record::split_secrets(&record.params);
			record.params = params;
			let mut stored = store.record(&record).map_err(engine_to_mesh)?;
			self.records.remember_secrets(&stored.sid, secrets);
			self.records.attach_secrets(&mut stored);
			return Ok(record_wire(stored));
		}
		let sid = record.sid.clone();
		self.records.put(record).map_err(engine_to_mesh)?;
		Ok(record_wire(self.records.get(&sid).expect("record was just inserted")))
	}

	fn update_exact(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		if let Some(store) = &self.cluster_store {
			let mut record = create_record(record)?;
			let (params, secrets) = record::split_secrets(&record.params);
			record.params = params;
			let updated = store.update_exact_record(&record).map_err(engine_to_mesh)?;
			self.records.remember_secrets(&updated.sid, secrets);
			return Ok(record_wire(updated));
		}
		self
			.records
			.put(create_record(record.clone())?)
			.map_err(engine_to_mesh)?;
		Ok(record)
	}

	fn claim_expected_with_intent(
		&self,
		expected_owner: String,
		expected_epoch: i64,
		record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		if let Some(store) = &self.cluster_store {
			let intent = create_record(record)?;
			return store
				.claim_expected_with_intent(
					&intent.sid,
					&expected_owner,
					expected_epoch,
					&intent.owner,
					&intent,
				)
				.map(record_wire)
				.map_err(engine_to_mesh);
		}
		self
			.records
			.put(create_record(record.clone())?)
			.map_err(engine_to_mesh)?;
		Ok(record)
	}

	fn begin_migration_handoff(
		&self,
		sid: &str,
		source_owner: &str,
		source_epoch: i64,
		target_owner: &str,
		token: &str,
	) -> MeshResult<()> {
		if let Some(store) = &self.cluster_store {
			store
				.begin_migration_handoff(sid, source_owner, source_epoch, target_owner, token)
				.map_err(engine_to_mesh)
		} else {
			self
				.records
				.begin_migration_handoff(sid, source_owner, source_epoch, target_owner, token)
				.map_err(engine_to_mesh)
		}
	}

	fn confirm_migration_handoff(
		&self,
		sid: &str,
		source_owner: &str,
		source_epoch: i64,
		target_owner: &str,
		target_epoch: i64,
		token: &str,
	) -> MeshResult<()> {
		if let Some(store) = &self.cluster_store {
			store
				.confirm_migration_handoff(
					sid,
					source_owner,
					source_epoch,
					target_owner,
					target_epoch,
					token,
				)
				.map_err(engine_to_mesh)
		} else {
			self
				.records
				.confirm_migration_handoff(
					sid,
					source_owner,
					source_epoch,
					target_owner,
					target_epoch,
					token,
				)
				.map_err(engine_to_mesh)
		}
	}

	fn abort_migration_handoff(
		&self,
		sid: &str,
		source_owner: &str,
		source_epoch: i64,
		target_owner: &str,
		token: &str,
	) -> MeshResult<Option<CreateRecordWire>> {
		if let Some(store) = &self.cluster_store {
			store
				.abort_migration_handoff(sid, source_owner, source_epoch, target_owner, token)
				.map(|record| record.map(record_wire))
				.map_err(engine_to_mesh)
		} else {
			self
				.records
				.abort_migration_handoff(sid, source_owner, source_epoch, target_owner, token)
				.map(|record| record.map(record_wire))
				.map_err(engine_to_mesh)
		}
	}

	fn complete_migration_abort(
		&self,
		sid: &str,
		token: &str,
		owner: &str,
		fresh_epoch: i64,
	) -> MeshResult<CreateRecordWire> {
		if let Some(store) = &self.cluster_store {
			store
				.complete_migration_abort(sid, token, owner, fresh_epoch)
				.map(record_wire)
				.map_err(engine_to_mesh)
		} else {
			self
				.records
				.complete_migration_abort(sid, token, owner, fresh_epoch)
				.map(record_wire)
				.map_err(engine_to_mesh)
		}
	}

	fn claim_migration_handoff(
		&self,
		source_owner: String,
		source_epoch: i64,
		token: String,
		record: CreateRecordWire,
	) -> MeshResult<CreateRecordWire> {
		let mut intent = create_record(record)?;
		let target_owner = intent.owner.clone();
		if intent
			.params
			.get("_mesh_migration_token")
			.and_then(Value::as_str)
			!= Some(token.as_str())
		{
			return Err(MeshError::invalid("migration token does not match target intent"));
		}
		if let Some(store) = &self.cluster_store {
			let (params, secrets) = record::split_secrets(&intent.params);
			intent.params = params;
			let claimed = store
				.claim_migration_handoff(
					&intent.sid,
					&source_owner,
					source_epoch,
					&target_owner,
					&token,
					&intent,
				)
				.map_err(engine_to_mesh)?;
			self.records.remember_secrets(&claimed.sid, secrets);
			Ok(record_wire(claimed))
		} else {
			self
				.records
				.claim_migration_handoff(&source_owner, source_epoch, &target_owner, &token, intent)
				.map(record_wire)
				.map_err(engine_to_mesh)
		}
	}

	fn notify_source_migration_claim<'a>(
		&'a self,
		record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>> {
		async move {
			let source_url = record
				.params
				.get("_mesh_migration_source_url")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no source URL"))?;
			let source_owner = record
				.params
				.get("_mesh_migration_source_owner")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no source owner"))?;
			let source_epoch = record
				.params
				.get("_mesh_migration_source_epoch")
				.and_then(Value::as_i64)
				.filter(|epoch| *epoch >= 0)
				.ok_or_else(|| MeshError::invalid("migration intent has no source epoch"))?;
			let migration_id = record
				.params
				.get("_mesh_migration_id")
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
				.ok_or_else(|| MeshError::invalid("migration intent has no token"))?;
			let claim = json!({
				"sid": record.sid,
				"source_owner": source_owner,
				"source_epoch": source_epoch,
				"target_owner": record.owner,
				"target_epoch": record.epoch,
				"migration_id": migration_id,
			});
			self
				.transport
				.post(
					source_url,
					"/v1/mesh/migrate/claim",
					&claim,
					Some(secs(self.config.mesh_create_timeout_sec)),
				)
				.await?;
			Ok(())
		}
		.boxed()
	}

	fn commit_delete(&self, sid: &str, owner: &str, epoch: i64) -> MeshResult<()> {
		if let Some(store) = &self.cluster_store {
			store
				.commit_delete(sid, owner, epoch)
				.map_err(engine_to_mesh)?;
			self.records.forget_secrets(sid);
			return Ok(());
		}
		let Some(record) = self.records.get(sid) else {
			return Ok(());
		};
		if record.owner != owner || record.epoch != epoch {
			return Err(MeshError::invalid(format!(
				"delete commit for {sid} at {owner}/{epoch} was fenced by {}/{}",
				record.owner, record.epoch
			)));
		}
		self.records.remove(sid).map_err(engine_to_mesh)
	}

	fn list(&self) -> MeshResult<Vec<CreateRecordWire>> {
		if let Some(store) = &self.cluster_store {
			let mut records = store.list().map_err(engine_to_mesh)?;
			for record in &mut records {
				self.records.attach_secrets(record);
			}
			Ok(records.into_iter().map(record_wire).collect())
		} else {
			Ok(self.records.list().into_iter().map(record_wire).collect())
		}
	}
}

impl MeshReplicaStore for MeshRuntime {
	fn get(&self, sid: &str) -> MeshResult<Option<ReplicaRecordWire>> {
		if let Some(store) = &self.cluster_store {
			return store
				.replica(sid)
				.map(|record| record.map(replica_wire))
				.map_err(engine_to_mesh);
		}
		Ok(self.replicas.get(sid).map(replica_wire))
	}

	fn put(
		&self,
		sid: String,
		object_key: String,
		digest: String,
		source_node: String,
		source_epoch: u64,
		checkpoint_generation: u64,
		snapshot_dir: String,
		params: Map<String, Value>,
	) -> BoxFuture<'_, MeshResult<()>> {
		async move {
			// The target may be publishing directly from a freshly materialized
			// cache root that is not authoritative until `publication.commit`.
			// Keep that exact root alive through upload, verification, commit,
			// local metadata persistence, and the immediately following sweep.
			let _cache_use = self.replica_cache.acquire(&sid, &object_key, &digest);
			if let Some(store) = &self.cluster_store {
				let (clean, secrets) = record::split_secrets(&params);
				let key_id = clean
					.get("encryption_key_id")
					.and_then(Value::as_str)
					.filter(|key_id| !key_id.is_empty())
					.map(str::to_owned)
					.ok_or_else(|| {
						MeshError::invalid("production replica storage requires encryption_key_id")
					})?;
				let keyring = Keyring::open(&self.home).map_err(engine_to_mesh)?;
				let key_snapshot = Arc::new(keyring.snapshot(&key_id).map_err(engine_to_mesh)?);
				let expected_object_key =
					replica_object_key(store.object_namespace(), &digest, &key_snapshot)
						.map_err(engine_to_mesh)?;
				if object_key != expected_object_key {
					return Err(MeshError::invalid("replica object key does not match its key scope"));
				}
				let shared = replica::ReplicaRecord::new_fenced(
					sid.clone(),
					digest.clone(),
					source_node.clone(),
					source_epoch,
					checkpoint_generation,
					snapshot_dir.clone(),
					clean,
					secrets.is_some(),
				)
				.map_err(engine_to_mesh)?
				.with_object_key(object_key.clone())
				.map_err(engine_to_mesh)?;
				let mut publication = store
					.acquire_replica_publication(
						&sid,
						&source_node,
						i64::try_from(source_epoch)
							.map_err(|_| MeshError::invalid("replica source epoch exceeds i64"))?,
						&object_key,
					)
					.map_err(engine_to_mesh)?;
				let s3 = self
					.s3_client
					.as_ref()
					.ok_or_else(|| MeshError::new("production replica storage requires S3"))?;
				publish_verified_s3_replica(
					s3,
					&object_key,
					&digest,
					Path::new(&snapshot_dir),
					key_snapshot,
				)
				.await
				.map_err(engine_to_mesh)?;
				publication.commit(&shared).map_err(engine_to_mesh)?;
			}
			let local = replica::ReplicaRecord::new_fenced(
				sid,
				digest,
				source_node,
				source_epoch,
				checkpoint_generation,
				snapshot_dir,
				params,
				false,
			)
			.map_err(engine_to_mesh)?;
			let local = if object_key.is_empty() {
				local
			} else {
				local.with_object_key(object_key).map_err(engine_to_mesh)?
			};
			self.replicas.put_record(local).map_err(engine_to_mesh)?;
			self.sweep_replica_cache();
			Ok(())
		}
		.boxed()
	}

	fn list(&self) -> MeshResult<Vec<String>> {
		if let Some(store) = &self.cluster_store {
			return store.list_replicas().map_err(engine_to_mesh);
		}
		Ok(self.replicas.list())
	}

	fn remove(&self, sid: &str) -> MeshResult<()> {
		if let Some(store) = &self.cluster_store {
			store.remove_replica(sid).map_err(engine_to_mesh)?;
		}
		self.replicas.remove(sid).map_err(engine_to_mesh)?;
		self.sweep_replica_cache();
		Ok(())
	}
}

impl MeshLeaseManager for MeshRuntime {
	fn vote_grant(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<Map<String, Value>> {
		if let Some(store) = &self.cluster_store {
			let result = store.acquire_lease(&volume, &holder_node, i64_to_u64(epoch), ttl);
			let decision = match result {
				Ok(record) => lease::LeaseDecision::granted(record),
				Err(err) => lease::LeaseDecision::denied(None, err.to_string()),
			};
			return decision_object(Ok(decision));
		}
		decision_object(
			self
				.leases
				.vote_grant(&volume, &holder_node, i64_to_u64(epoch), ttl),
		)
	}

	fn vote_renew(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<Map<String, Value>> {
		if let Some(store) = &self.cluster_store {
			let result = store.acquire_lease(&volume, &holder_node, i64_to_u64(epoch), ttl);
			let decision = match result {
				Ok(record) => lease::LeaseDecision::granted(record),
				Err(err) => lease::LeaseDecision::denied(None, err.to_string()),
			};
			return decision_object(Ok(decision));
		}
		decision_object(
			self
				.leases
				.vote_renew(&volume, &holder_node, i64_to_u64(epoch), ttl),
		)
	}

	fn vote_release(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
	) -> MeshResult<Map<String, Value>> {
		if let Some(store) = &self.cluster_store {
			let result = store.release_lease(&volume, &holder_node, i64_to_u64(epoch));
			let decision = match result {
				Ok(()) => lease::LeaseDecision::released(),
				Err(err) => lease::LeaseDecision::denied(None, err.to_string()),
			};
			return decision_object(Ok(decision));
		}
		decision_object(
			self
				.leases
				.vote_release(&volume, &holder_node, i64_to_u64(epoch)),
		)
	}

	fn acquire_writable_volume_leases(
		&self,
		params: Map<String, Value>,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>> {
		async move {
			let records = self
				.acquire_writable_leases(&params, i64_to_u64(epoch))
				.await?;
			records
				.iter()
				.map(lease_record_value)
				.collect::<MeshResult<Vec<_>>>()
		}
		.boxed()
	}

	fn release_leases(&self, leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>> {
		async move { self.release_lease_values(leases) }.boxed()
	}

	fn release_record_volume_leases(&self, sid: String) -> BoxFuture<'_, MeshResult<()>> {
		async move {
			let view = self.engine.get_view(&sid)?;
			let leases = view
				.get("volume_leases")
				.and_then(Value::as_object)
				.cloned()
				.unwrap_or_default();
			self.release_recorded_leases(&sid, &leases).await
		}
		.boxed()
	}
}

impl reconciler::MeshReconcileState for MeshRuntime {
	fn enabled(&self) -> bool {
		MeshControl::enabled(self)
	}

	fn interval(&self) -> f64 {
		self.config.mesh_heartbeat_sec.max(1.0)
	}

	fn node_id(&self) -> &str {
		&self.node_id
	}

	fn advertise(&self) -> String {
		self.membership.read().advertise.clone()
	}

	fn create_timeout(&self) -> Duration {
		secs(self.config.mesh_create_timeout_sec)
	}

	fn expected_members(&self) -> usize {
		MeshControl::expected_members(self)
	}

	fn live_member_ids(&self) -> Vec<String> {
		self
			.membership
			.read()
			.live_member_ids(unix_now(), self.config.mesh_heartbeat_sec)
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		MeshControl::peer_url(self, node_id)
	}

	fn is_peer_healthy(&self, node_id: &str) -> bool {
		MeshControl::is_peer_healthy(self, node_id)
	}

	fn owner_of(&self, sid: &str) -> Result<Option<String>> {
		MeshControl::owner_of(self, sid).map_err(|error| EngineError::engine(error.to_string()))
	}

	fn is_nonserving_lifecycle(&self, sid: &str) -> Result<bool> {
		if let Some(store) = &self.cluster_store {
			return Ok(store.suspend_marker(sid)?.is_some()
				|| store.rollback_marker(sid)?.is_some()
				|| store
					.resolve(sid)?
					.is_some_and(|record| record_is_staging(&record.params)));
		}
		Ok(self
			.records
			.get(sid)
			.is_some_and(|record| record_is_staging(&record.params)))
	}

	fn local_candidate_missing(&self, sid: &str) -> Result<bool> {
		if let Some(store) = &self.cluster_store {
			let Some(record) = store.resolve(sid)? else {
				return Ok(false);
			};
			return Ok(record.owner == self.node_id
				&& !record_is_staging(&record.params)
				&& store.suspend_marker(sid)?.is_none()
				&& store.rollback_marker(sid)?.is_none()
				&& !self.engine.candidate_is_running(sid));
		}
		let Some(record) = self.records.get(sid) else {
			return Ok(false);
		};
		Ok(record.owner == self.node_id
			&& !record_is_staging(&record.params)
			&& !self.engine.candidate_is_running(sid))
	}

	fn restore_owner(&self, sid: &str, exclude: &HashSet<String>) -> Option<String> {
		let mut candidates = self.live_member_ids();
		candidates.retain(|node_id| !exclude.contains(node_id));
		place::hrw_winner(sid, candidates.iter().map(String::as_str)).map(str::to_owned)
	}

	fn restore_quorum_met(&self, confirmations: usize) -> bool {
		self.membership.read().restore_quorum_met(confirmations)
	}

	fn quorum_needed(&self) -> usize {
		MeshControl::quorum_needed(self)
	}

	fn drain_orphans(&self) -> Vec<(String, String)> {
		std::mem::take(&mut *self.orphans.lock())
	}

	fn requeue_orphan(&self, sid: &str, dead: &str) {
		self.orphans.lock().push((sid.to_owned(), dead.to_owned()));
	}

	fn claim_restore_epoch(&self, sid: &str, owner: &str) -> Result<u64> {
		if let Some(store) = &self.cluster_store {
			let record = store.claim_next(sid, owner)?;
			return Ok(i64_to_u64(record.epoch));
		}
		let epoch = MeshControl::authoritative_owner(self, sid)
			.map_err(mesh_to_engine)?
			.map_or(1, |(_, epoch)| i64_to_u64(epoch).saturating_add(1));
		self
			.records
			.update_owner(sid, owner, u64_to_i64(epoch))?
			.ok_or_else(|| EngineError::not_found(format!("no create record for sandbox {sid}")))?;
		Ok(epoch)
	}

	fn claim_restore_epoch_observed(
		&self,
		sid: &str,
		owner: &str,
		previous: &reconciler::CreateRecord,
		pending: &Value,
	) -> Result<u64> {
		if fence_blocks_successor(&self.fence_retries.lock(), sid) {
			return Err(EngineError::busy(format!(
				"restore claim waits for fenced teardown of sandbox {sid}"
			)));
		}
		if let Some(store) = &self.cluster_store {
			let claimed = store.claim_expected_pending(
				sid,
				&previous.owner,
				u64_to_i64(previous.epoch),
				owner,
				pending,
			)?;
			let epoch = i64_to_u64(claimed.epoch);
			self.owners.write().remove(sid);
			self
				.restoring
				.write()
				.insert(sid.to_owned(), (owner.to_owned(), epoch));
			self.owner_lease_deadlines.lock().remove(sid);
			return Ok(epoch);
		}
		let current = self
			.records
			.get(sid)
			.ok_or_else(|| EngineError::not_found(format!("no create record for sandbox {sid}")))?;
		if current.owner != previous.owner || current.epoch != u64_to_i64(previous.epoch) {
			return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
		}
		self.claim_restore_epoch(sid, owner)
	}

	fn owns_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<bool> {
		if let Some(store) = &self.cluster_store {
			return store.owns_epoch(sid, owner, u64_to_i64(epoch));
		}
		Ok(MeshControl::authoritative_owner(self, sid)
			.map_err(mesh_to_engine)?
			.is_some_and(|(current, current_epoch)| {
				current == owner && i64_to_u64(current_epoch) == epoch
			}))
	}

	fn finalize_restore_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<()> {
		if !self.owns_epoch(sid, owner, epoch)? {
			return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
		}
		MeshControl::record_owner(self, sid, owner, u64_to_i64(epoch));
		Ok(())
	}

	fn renew_restore_epoch(&self, sid: &str, owner: &str, epoch: u64) -> Result<bool> {
		let epoch = u64_to_i64(epoch);
		let request_started = tokio::time::Instant::now();
		if let Some(store) = &self.cluster_store {
			return store
				.renew_owner_lease(sid, owner, epoch)
				.and_then(|remaining| {
					let Some(remaining) = remaining else {
						return Ok(false);
					};
					self.rearm_owner_watchdog(sid, request_started, remaining)?;
					Ok(true)
				});
		}
		self.owns_epoch(sid, owner, i64_to_u64(epoch))
	}

	fn abort_restore_epoch(
		&self,
		sid: &str,
		owner: &str,
		epoch: u64,
		previous_owner: &str,
		previous_epoch: u64,
	) -> Result<()> {
		if let Some(store) = &self.cluster_store {
			if store.resolve(sid)?.is_some_and(|record| {
				record.owner == owner
					&& record.epoch == u64_to_i64(epoch)
					&& record.params.contains_key("_mesh_restore_pending")
			}) {
				// The atomic pending marker is the retryable, non-serving
				// recovery journal; a failed candidate must not release it.
				return Ok(());
			}
			let Some(restored) = store.release_claim(
				sid,
				owner,
				u64_to_i64(epoch),
				previous_owner,
				u64_to_i64(previous_epoch),
			)?
			else {
				return Ok(());
			};
			MeshControl::record_owner(self, sid, &restored.owner, restored.epoch);
			return Ok(());
		}
		Ok(())
	}

	fn commit_restore_epoch(
		&self,
		sid: &str,
		owner: &str,
		epoch: u64,
		params: &Value,
		ha: &str,
		restart_policy: &str,
	) -> Result<()> {
		if let Some(store) = &self.cluster_store {
			let Some(mut record) = store.resolve(sid)? else {
				return Err(EngineError::not_found(format!("no create record for sandbox {sid}")));
			};
			if record.owner != owner || record.epoch != u64_to_i64(epoch) {
				return Err(EngineError::busy(format!("restore ownership fenced for sandbox {sid}")));
			}
			record.params = params_object(params)?;
			for field in [
				"_mesh_restore_pending",
				"_mesh_rollback_pending",
				"restore_pending",
				"rollback_pending",
				"_mesh_lifecycle_operation",
			] {
				record.params.remove(field);
			}
			ha.clone_into(&mut record.ha);
			restart_policy.clone_into(&mut record.restart_policy);
			let deadline = self.renew_and_arm_serving_owner(sid, owner, epoch)?;
			store.update_exact_record(&record)?;
			self
				.owners
				.write()
				.insert(sid.to_owned(), (owner.to_owned(), epoch));
			self
				.owner_lease_deadlines
				.lock()
				.insert(sid.to_owned(), deadline);
			let mut restoring = self.restoring.write();
			if restoring.get(sid) == Some(&(owner.to_owned(), epoch)) {
				restoring.remove(sid);
			}
			return Ok(());
		}
		self.finalize_restore_epoch(sid, owner, epoch)
	}

	fn restore_commit_applied(&self, sid: &str, owner: &str, epoch: u64) -> Result<bool> {
		let Some(store) = &self.cluster_store else {
			return Ok(false);
		};
		Ok(store.resolve(sid)?.is_some_and(|record| {
			record.owner == owner
				&& record.epoch == u64_to_i64(epoch)
				&& !record_is_staging(&record.params)
		}))
	}

	fn worker_begin(&self, key: &str) -> bool {
		MeshControl::worker_begin(self, key)
	}

	fn worker_end(&self, key: &str) {
		MeshControl::worker_end(self, key);
	}

	fn note_event(&self, event: &str) {
		self.events.lock().push(event.to_owned());
	}

	fn replica_targets(&self, sid: &str, replicas: usize) -> Vec<String> {
		MeshControl::replica_targets(self, sid, replicas)
	}

	fn fenced_local_ids(&self, owned_ids: &[String]) -> Result<Vec<String>> {
		let local = reconciler::MeshReconcileState::node_id(self);
		let mut fenced = Vec::new();
		for sid in owned_ids {
			if (!self.engine.candidate_is_running(sid) && !self.is_nonserving_lifecycle(sid)?)
				|| reconciler::MeshReconcileState::owner_of(self, sid)?
					.is_some_and(|owner| owner != local)
			{
				fenced.push(sid.clone());
			}
		}
		Ok(fenced)
	}

	fn local_owner_epochs(&self, owned_ids: &[String]) -> Result<Vec<reconciler::LocalOwnerEpoch>> {
		let mut local = Vec::new();
		for sid in owned_ids {
			let Some((owner, epoch)) = self.owners.read().get(sid).cloned() else {
				continue;
			};
			if !self.engine.candidate_is_running(sid) {
				// Registry status is not VMM liveness. Drop the serving route,
				// stop renewals, fence any stale candidate, and let HA recover
				// through the explicit local-loss path.
				self.owners.write().remove(sid);
				self.owner_lease_deadlines.lock().remove(sid);
				self.orphans.lock().push((sid.clone(), owner.clone()));
				continue;
			}
			local.push(reconciler::LocalOwnerEpoch { sid: sid.clone(), owner, epoch });
		}
		Ok(local)
	}

	fn renew_owner_leases(&self, local: &[reconciler::LocalOwnerEpoch]) -> Result<Vec<String>> {
		let mut fenced = Vec::new();
		let now = tokio::time::Instant::now();
		for local in local {
			if local.owner != self.node_id {
				fenced.push(local.sid.clone());
				continue;
			}
			let Ok(epoch) = i64::try_from(local.epoch) else {
				fenced.push(local.sid.clone());
				continue;
			};
			if let Some(store) = &self.cluster_store {
				let request_started = tokio::time::Instant::now();
				match store.renew_owner_lease(&local.sid, &local.owner, epoch) {
					Ok(Some(remaining_ttl)) if remaining_ttl.is_finite() && remaining_ttl > 0.0 => {
						if !matches!(
							self.rearm_owner_watchdog(&local.sid, request_started, remaining_ttl),
							Ok(true)
						) {
							fenced.push(local.sid.clone());
							continue;
						}
						self.owner_lease_deadlines.lock().insert(
							local.sid.clone(),
							request_started + Duration::from_secs_f64((remaining_ttl - 5.0).max(0.0)),
						);
					},
					Ok(_) => fenced.push(local.sid.clone()),
					Err(_)
						if self
							.owner_lease_deadlines
							.lock()
							.get(&local.sid)
							.is_none_or(|deadline| *deadline <= now) =>
					{
						fenced.push(local.sid.clone());
					},
					Err(_) => {},
				}
			}
		}
		Ok(fenced)
	}

	fn forget_local_owner(&self, sid: &str) {
		MeshControl::forget_owner(self, sid);
	}
}

impl reconciler::CreateRecordStore for MeshRuntime {
	fn get(&self, sid: &str) -> Result<Option<reconciler::CreateRecord>> {
		if let Some(store) = &self.cluster_store {
			return store
				.resolve(sid)
				.map(|record| record.map(reconcile_record));
		}
		Ok(self.records.get(sid).map(reconcile_record))
	}

	fn reconcile_records(&self) -> Result<()> {
		if self.cluster_store.is_none() {
			self.records.load();
		}
		Ok(())
	}
}

impl reconciler::ReplicaStore for MeshRuntime {
	fn get<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<Option<reconciler::ReplicaRecord>>> {
		async move {
			if let Some(store) = &self.cluster_store {
				let Some(shared) = store.replica(sid)? else {
					return Ok(None);
				};
				return self
					.materialize_replica(shared)
					.await
					.map(reconcile_replica)
					.map(Some);
			}
			Ok(self.replicas.get(sid).map(reconcile_replica))
		}
		.boxed()
	}

	fn holds(&self, sid: &str) -> Result<bool> {
		if let Some(store) = &self.cluster_store {
			return store.replica(sid).map(|record| record.is_some());
		}
		Ok(self.replicas.holds(sid))
	}

	fn secrets_ready(&self, sid: &str) -> Result<bool> {
		if let Some(store) = &self.cluster_store {
			let Some(shared) = store.replica(sid)? else {
				return Ok(false);
			};
			if !shared.needs_secrets {
				return Ok(true);
			}
			return Ok(self.replicas.get(sid).is_some_and(|local| {
				local.digest == shared.digest && local.object_key == shared.object_key
			}) && self.replicas.secrets_ready(sid));
		}
		Ok(self.replicas.secrets_ready(sid))
	}

	fn drop_replica(&self, record: &reconciler::ReplicaRecord) -> Result<()> {
		if let Some(store) = &self.cluster_store {
			let shared = replica::ReplicaRecord::new_fenced(
				record.sid.clone(),
				record.digest.clone(),
				record.source_node.clone(),
				record.source_epoch,
				record.checkpoint_generation,
				record.snapshot_dir.to_string_lossy().into_owned(),
				record
					.params
					.as_object()
					.cloned()
					.ok_or_else(|| EngineError::invalid("replica parameters are not an object"))?,
				record.needs_secrets,
			)?
			.with_object_key(record.object_key.clone())?;
			store.remove_replica_exact(&shared)?;
		}
		self.replicas.drop_replica(&record.sid)?;
		self.sweep_replica_cache();
		Ok(())
	}

	fn acquire_cache_root(&self, record: &reconciler::ReplicaRecord) -> Option<Box<dyn Send>> {
		Some(Box::new(
			self
				.replica_cache
				.acquire(&record.sid, &record.object_key, &record.digest),
		))
	}

	fn publication_key(&self, sid: &str, prep: &reconciler::ReplicatePreparation) -> Result<String> {
		let object_key = if let Some(store) = &self.cluster_store {
			let key_id = prep
				.params
				.get("encryption_key_id")
				.and_then(Value::as_str)
				.filter(|key_id| !key_id.is_empty() && *key_id != "default")
				.ok_or_else(|| {
					EngineError::invalid("portable replica requires a non-local encryption_key_id")
				})?;
			let snapshot = Keyring::open(&self.home)?.snapshot(key_id)?;
			replica_object_key(store.object_namespace(), &prep.digest, &snapshot)?
		} else {
			String::new()
		};
		self
			.engine
			.engine
			.mesh_bind_replica_export(sid, &prep.digest, &object_key)?;
		Ok(object_key)
	}
}

impl reconciler::LeaseManager for MeshRuntime {
	fn renew_writable_volume_leases_once(&self) -> BoxFuture<'_, Result<()>> {
		async move {
			self
				.renew_writable_volume_leases()
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn acquire_writable_volume_leases<'a>(
		&'a self,
		params: &'a Value,
		epoch: u64,
	) -> BoxFuture<'a, Result<Vec<LeaseRecord>>> {
		async move {
			let params = params.as_object().cloned().unwrap_or_default();
			self
				.acquire_writable_leases(&params, epoch)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn release_leases<'a>(&'a self, leases: &'a [LeaseRecord]) -> BoxFuture<'a, Result<()>> {
		async move {
			self
				.release_lease_records(leases)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn release_record_volume_leases<'a>(
		&'a self,
		record: &'a reconciler::SandboxRecord,
	) -> BoxFuture<'a, Result<()>> {
		async move {
			let leases = record
				.detail
				.get("volume_leases")
				.and_then(Value::as_object)
				.cloned()
				.unwrap_or_default();
			self
				.release_recorded_leases(&record.name, &leases)
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}
}

impl super::routes::MigrationLifecycleGuard for crate::engine::LifecycleCaptureGuard {}

#[derive(Clone)]
pub struct MeshEngineAdapter {
	pub(crate) engine: Arc<Engine>,
	cleanup_dir:       PathBuf,
}

struct ActiveReplicaExport(Arc<crate::engine::ReplicaExport>);

impl ReplicaExport for ActiveReplicaExport {
	fn path(&self) -> &Path {
		self.0.path()
	}
}

impl MeshEngineAdapter {
	const fn new(engine: Arc<Engine>, cleanup_dir: PathBuf) -> Self {
		Self { engine, cleanup_dir }
	}

	fn cleanup_path(&self, sid: &str) -> PathBuf {
		self.cleanup_dir.join(format!("{sid}.json"))
	}

	fn validate_cleanup(cleanup: &MigrationCleanupWire) -> MeshResult<()> {
		let safe_component = |value: &str| {
			!value.is_empty()
				&& value != "."
				&& value != ".."
				&& !value.contains(std::path::MAIN_SEPARATOR)
				&& !value.contains('/')
				&& !value.contains('\\')
		};
		if cleanup.schema_version != 1
			|| !safe_component(&cleanup.sid)
			|| cleanup.source_owner.is_empty()
			|| cleanup.target_owner.is_empty()
			|| cleanup.token.is_empty()
			|| cleanup.phase != "prepared"
			|| cleanup.source_epoch < 0
			|| cleanup.target_epoch < 0
		{
			return Err(MeshError::invalid("invalid migration cleanup journal"));
		}
		Ok(())
	}

	fn sync_cleanup_dir(&self) -> MeshResult<()> {
		fs::File::open(&self.cleanup_dir)
			.and_then(|directory| directory.sync_all())
			.map_err(|error| engine_to_mesh(error.into()))
	}
}

impl MeshEngine for MeshEngineAdapter {
	fn owned_ids(&self) -> MeshResult<Vec<String>> {
		self.engine.mesh_owned_ids().map_err(engine_to_mesh)
	}

	fn list_views(&self) -> MeshResult<Vec<Value>> {
		self.engine.mesh_list_views().map_err(engine_to_mesh)
	}

	fn has_sandbox(&self, sid: &str) -> bool {
		self.engine.mesh_has_sandbox(sid)
	}

	fn candidate_is_running(&self, sid: &str) -> bool {
		self.engine.candidate_is_running(sid)
	}

	fn replica_export_path(
		&self,
		sid: &str,
		digest: &str,
		object_key: &str,
	) -> MeshResult<Box<dyn ReplicaExport>> {
		let export = self
			.engine
			.mesh_replica_export(sid, digest, object_key)
			.map_err(engine_to_mesh)?;
		Ok(Box::new(ActiveReplicaExport(export)))
	}

	fn get_view(&self, sid: &str) -> MeshResult<Value> {
		self.engine.mesh_get_view(sid).map_err(engine_to_mesh)
	}

	fn checkpoint_age_sec(&self, sid: &str) -> Option<f64> {
		self.engine.mesh_checkpoint_age_sec(sid)
	}

	fn find_by_idempotency_key(&self, key: &str) -> MeshResult<Option<Value>> {
		self
			.engine
			.mesh_find_by_idempotency_key(key)
			.map_err(engine_to_mesh)
	}

	fn record_idempotency(&self, sid: &str, key: &str) -> MeshResult<()> {
		self
			.engine
			.mesh_record_idempotency(sid, key)
			.map_err(engine_to_mesh)
	}

	fn record_create_epoch(&self, sid: &str, epoch: i64) -> MeshResult<()> {
		self
			.engine
			.mesh_record_create_epoch(sid, epoch)
			.map_err(engine_to_mesh)
	}

	fn record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> MeshResult<()> {
		self
			.engine
			.mesh_record_volume_leases(sid, leases)
			.map_err(engine_to_mesh)
	}

	fn try_begin_migration(
		&self,
		sid: &str,
	) -> MeshResult<Box<dyn super::routes::MigrationLifecycleGuard>> {
		self
			.engine
			.try_acquire_running_capture(sid)
			.map_err(engine_to_mesh)?
			.map(|guard| Box::new(guard) as Box<dyn super::routes::MigrationLifecycleGuard>)
			.ok_or_else(|| {
				MeshError::with_code(format!("migration already active for sandbox {sid}"), "busy")
			})
	}

	fn delete_portable_history(
		&self,
		sid: String,
		owner: String,
		epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move { spawn_engine(move || engine.delete_portable_history(&sid, &owner, epoch)).await }
			.boxed()
	}

	fn teardown_candidate(
		&self,
		sid: String,
		_expected_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move { spawn_engine(move || engine.remove_local_candidate(&sid)).await }.boxed()
	}

	fn create_sandbox(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		let engine = self.engine.clone();
		async move { spawn_engine(move || engine.mesh_create_from_params(params)).await }.boxed()
	}

	fn run_detached(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_detached(&self, params: Map<String, Value>) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_from_template(
		&self,
		params: Map<String, Value>,
		template_dir: String,
		quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine.mesh_restore_from_template(params, Path::new(&template_dir), quorum_ok)
			})
			.await
		}
		.boxed()
	}

	fn migrate_precopy(&self, sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				let prep = engine.mesh_migrate_precopy(&sid)?;
				Ok(MigratePrepareWire {
					digest:       prep.digest,
					snapshot_dir: prep.snapshot_dir.to_string_lossy().into_owned(),
					params:       params_object(&prep.params)?,
					cleanup:      None,
				})
			})
			.await
		}
		.boxed()
	}

	fn migrate_finalize(
		&self,
		sid: String,
		base_dir: String,
		cleanup: MigrationCleanupWire,
	) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				let (prep, cleanup) =
					engine.mesh_migrate_finalize(&sid, Path::new(&base_dir), cleanup)?;
				Ok(MigratePrepareWire {
					digest:       prep.digest,
					snapshot_dir: prep.snapshot_dir.to_string_lossy().into_owned(),
					params:       params_object(&prep.params)?,
					cleanup:      Some(cleanup),
				})
			})
			.await
		}
		.boxed()
	}

	fn checkpoint_discard(
		&self,
		snapshot_dir: String,
		digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || engine.mesh_replicate_cleanup(&digest, Path::new(&snapshot_dir)))
				.await
		}
		.boxed()
	}

	fn migrate_abort(
		&self,
		sid: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
		params: Map<String, Value>,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine
					.mesh_migrate_abort(&sid, &base_digest, Path::new(&delta_dir), &delta_digest, params)
					.map(|_| ())
			})
			.await
		}
		.boxed()
	}

	fn migrate_commit(
		&self,
		sid: String,
		base_dir: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine.mesh_migrate_commit(
					&sid,
					Path::new(&base_dir),
					&base_digest,
					Path::new(&delta_dir),
					&delta_digest,
				)
			})
			.await
		}
		.boxed()
	}

	fn stage_migration_delta(
		&self,
		sid: String,
		verified_cache_path: String,
		base_dir: String,
	) -> BoxFuture<'_, MeshResult<String>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine
					.mesh_stage_migration_delta(
						&sid,
						Path::new(&verified_cache_path),
						Path::new(&base_dir),
					)
					.map(|path| path.to_string_lossy().into_owned())
			})
			.await
		}
		.boxed()
	}

	fn migrate_adopt_target(
		&self,
		sid: String,
		delta_dir: String,
		params: Map<String, Value>,
	) -> BoxFuture<'_, MeshResult<Value>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || engine.mesh_migrate_adopt_target(&sid, Path::new(&delta_dir), params))
				.await
		}
		.boxed()
	}

	fn migrate_activate_target(
		&self,
		sid: String,
		expected_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || engine.mesh_migrate_activate_target(&sid, expected_epoch)).await
		}
		.boxed()
	}

	fn migration_cleanup_persist(&self, cleanup: MigrationCleanupWire) -> MeshResult<()> {
		Self::validate_cleanup(&cleanup)?;
		fs::create_dir_all(&self.cleanup_dir).map_err(|error| engine_to_mesh(error.into()))?;
		let path = self.cleanup_path(&cleanup.sid);
		let tmp = self
			.cleanup_dir
			.join(format!(".{}.{}.tmp", cleanup.sid, uuid::Uuid::new_v4()));
		let bytes = serde_json::to_vec(&cleanup)
			.map_err(|error| MeshError::new(format!("serializing migration cleanup: {error}")))?;
		let result = (|| -> MeshResult<()> {
			let mut file = OpenOptions::new()
				.write(true)
				.create_new(true)
				.mode(0o600)
				.open(&tmp)
				.map_err(|error| engine_to_mesh(error.into()))?;
			file
				.write_all(&bytes)
				.and_then(|()| file.write_all(b"\n"))
				.and_then(|()| file.sync_all())
				.map_err(|error| engine_to_mesh(error.into()))?;
			fs::rename(&tmp, &path).map_err(|error| engine_to_mesh(error.into()))?;
			self.sync_cleanup_dir()
		})();
		if result.is_err() {
			let _ = fs::remove_file(&tmp);
		}
		result
	}

	fn migration_cleanup_pending(&self) -> MeshResult<Vec<MigrationCleanupWire>> {
		let Ok(entries) = fs::read_dir(&self.cleanup_dir) else {
			return Ok(Vec::new());
		};
		let mut pending = Vec::new();
		let mut removed_temp = false;
		for entry in entries.filter_map(std::result::Result::ok) {
			let path = entry.path();
			let temporary = path
				.file_name()
				.and_then(|name| name.to_str())
				.is_some_and(|name| {
					name.starts_with('.')
						&& Path::new(name)
							.extension()
							.is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
				});
			if temporary {
				if fs::remove_file(&path).is_ok() {
					removed_temp = true;
				}
				continue;
			}
			if path.extension().is_some_and(|ext| ext == "json") {
				pending.push(
					fs::read(&path)
						.map_err(|err| engine_to_mesh(err.into()))
						.and_then(|bytes| {
							serde_json::from_slice(&bytes).map_err(|err| {
								MeshError::new(format!("reading migration cleanup record: {err}"))
							})
						})?,
				);
			}
		}
		if removed_temp {
			self.sync_cleanup_dir()?;
		}
		Ok(pending)
	}

	fn migration_cleanup_remove(&self, sid: String) -> MeshResult<()> {
		match fs::remove_file(self.cleanup_path(&sid)) {
			Ok(()) => self.sync_cleanup_dir(),
			Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
			Err(err) => Err(engine_to_mesh(err.into())),
		}
	}

	fn migrate_cleanup_committed(
		&self,
		sid: String,
		base_dir: String,
		base_digest: String,
		delta_dir: String,
		delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		let engine = self.engine.clone();
		async move {
			spawn_engine(move || {
				engine.mesh_migrate_cleanup_committed(
					&sid,
					Path::new(&base_dir),
					&base_digest,
					Path::new(&delta_dir),
					&delta_digest,
				)
			})
			.await
		}
		.boxed()
	}
}
impl reconciler::ReconcileEngine for MeshEngineAdapter {
	fn owned_ids(&self) -> Result<Vec<String>> {
		self.engine.mesh_owned_ids()
	}

	fn get(&self, sid: &str) -> Result<reconciler::SandboxRecord> {
		sandbox_record(self.engine.mesh_get_view(sid)?)
	}

	fn remove(&self, sid: &str) -> Result<()> {
		self.engine.remove_local_candidate(sid)
	}

	fn candidate_is_running(&self, sid: &str) -> bool {
		self.engine.candidate_is_running(sid)
	}

	fn replicate_prepare<'a>(
		&'a self,
		sid: &'a str,
	) -> futures_util::future::BoxFuture<'a, Result<reconciler::ReplicatePreparation>> {
		let engine = self.engine.clone();
		let sid = sid.to_owned();
		async move {
			tokio::task::spawn_blocking(move || engine.mesh_replicate_prepare(&sid))
				.await
				.map_err(|err| {
					EngineError::engine(format!("mesh replicate prepare task failed: {err}"))
				})?
		}
		.boxed()
	}

	fn replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()> {
		self.engine.mesh_replicate_cleanup(digest, snapshot_dir)
	}

	fn restore_from_template<'a>(
		&'a self,
		params: &'a Value,
		snapshot_dir: &'a Path,
		quorum_ok: bool,
	) -> BoxFuture<'a, Result<reconciler::SandboxRecord>> {
		let engine = self.engine.clone();
		let params = params.as_object().cloned().unwrap_or_default();
		let snapshot_dir = snapshot_dir.to_owned();
		async move {
			let view = spawn_engine(move || {
				engine.mesh_restore_from_template_paused(params, &snapshot_dir, quorum_ok)
			})
			.await
			.map_err(mesh_to_engine)?;
			sandbox_record(view)
		}
		.boxed()
	}

	fn activate_candidate<'a>(&'a self, sid: &'a str) -> BoxFuture<'a, Result<()>> {
		let engine = self.engine.clone();
		let sid = sid.to_owned();
		async move {
			spawn_engine(move || engine.mesh_activate_candidate(&sid))
				.await
				.map_err(mesh_to_engine)
		}
		.boxed()
	}

	fn record_volume_leases(&self, sid: &str, leases: &[LeaseRecord]) -> Result<()> {
		let values = leases
			.iter()
			.map(lease_record_value)
			.collect::<MeshResult<Vec<_>>>()
			.map_err(mesh_to_engine)?;
		self.engine.mesh_record_volume_leases(sid, values)
	}

	fn record_idempotency(&self, sid: &str, key: &str) -> Result<()> {
		self.engine.mesh_record_idempotency(sid, key)
	}

	fn set_ha_metadata(&self, sid: &str, ha: &str, restart_policy: &str) -> Result<()> {
		self.engine.mesh_set_ha_metadata(sid, ha, restart_policy)
	}

	fn create_from_record(&self, params: &Value) -> Result<reconciler::SandboxRecord> {
		sandbox_record(
			self
				.engine
				.mesh_create_from_params(params_object(params)?)?,
		)
	}

	fn run_from_record(&self, params: &Value) -> Result<Value> {
		self.engine.mesh_create_from_params(params_object(params)?)
	}

	fn begin_restore_cancellation(&self, sid: &str) {
		self.engine.begin_restore_cancellation(sid);
	}

	fn cancel_restore(&self, sid: &str) {
		self
			.engine
			.begin_restore_cancellation(sid)
			.store(true, Ordering::Release);
	}

	fn end_restore_cancellation(&self, sid: &str) {
		self.engine.end_restore_cancellation(sid);
	}

	fn restore_from_record(&self, params: &Value) -> Result<Value> {
		self.engine.mesh_create_from_params(params_object(params)?)
	}
}

pub(crate) fn replica_object_key(
	namespace: &str,
	digest: &str,
	snapshot: &KeySnapshot,
) -> Result<String> {
	if digest.is_empty() || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
		return Err(EngineError::invalid("replica digest is invalid"));
	}
	Ok(format!("{namespace}/replicas/by-key/{}/{digest}.vbundle", snapshot.object_key_scope()))
}

fn replica_staging_key(final_key: &str, digest: &str) -> Result<String> {
	let (namespace, scoped) = final_key.split_once("/replicas/by-key/").ok_or_else(|| {
		EngineError::invalid("replica object key is outside the scoped replica domain")
	})?;
	let (scope, name) = scoped
		.split_once('/')
		.ok_or_else(|| EngineError::invalid("replica object key has no key scope"))?;
	if scope.len() != 64
		|| !scope.bytes().all(|byte| byte.is_ascii_hexdigit())
		|| name != format!("{digest}.vbundle")
	{
		return Err(EngineError::invalid("replica object key is malformed"));
	}
	Ok(format!(
		"{namespace}/replicas/.staging/by-key/{scope}/{digest}-{}.vbundle",
		uuid::Uuid::new_v4()
	))
}

fn extracted_bundle_root(staging: &Path, expected_name: Option<&str>) -> Result<PathBuf> {
	let expected_name = expected_name
		.map(|name| {
			Path::new(name)
				.file_name()
				.and_then(|component| component.to_str())
				.filter(|component| !component.is_empty() && *component != "." && *component != "..")
				.ok_or_else(|| EngineError::invalid("replica bundle root name is invalid"))
		})
		.transpose()?;
	let mut entries = fs::read_dir(staging)?.collect::<std::io::Result<Vec<_>>>()?;
	if entries.len() != 1 {
		return Err(EngineError::invalid("replica bundle must contain exactly one root directory"));
	}
	let entry = entries.pop().expect("checked length");
	let file_name = entry.file_name();
	let name = file_name
		.to_str()
		.ok_or_else(|| EngineError::invalid("replica bundle root name is not UTF-8"))?;
	if expected_name.is_some_and(|expected| expected != name) {
		return Err(EngineError::invalid("replica bundle root does not match checkpoint name"));
	}
	let path = entry.path();
	let metadata = fs::symlink_metadata(&path)?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(EngineError::invalid("replica bundle root must be a directory"));
	}
	Ok(path)
}

async fn publish_verified_s3_replica(
	s3: &crate::s3::S3Client,
	final_key: &str,
	digest: &str,
	snapshot_dir: &Path,
	snapshot: Arc<KeySnapshot>,
) -> Result<()> {
	let final_key = final_key.to_owned();
	match s3.head_object(&final_key).await {
		Ok(stat) if stat.size > 0 => {
			let verify_root = snapshot_dir
				.parent()
				.ok_or_else(|| EngineError::invalid("replica snapshot has no parent"))?
				.join(format!(".replica-final-verify-{}", uuid::Uuid::new_v4()));
			if verify_uploaded_s3_bundle(
				s3,
				&final_key,
				&stat,
				&verify_root,
				digest,
				REPLICA_ARCHIVE_ROOT,
				Arc::clone(&snapshot),
			)
			.await
			.is_ok()
			{
				return Ok(());
			}
		},
		Ok(_) | Err(crate::s3::S3Error::NotFound) => {},
		Err(error) => {
			return Err(EngineError::engine(format!("checking shared replica object: {error}")));
		},
	}

	let staging_key = replica_staging_key(&final_key, digest)?;
	let published = async {
		upload_s3_bundle(s3, &staging_key, snapshot_dir, Arc::clone(&snapshot), REPLICA_ARCHIVE_ROOT)
			.await?;
		let staging_stat = s3.head_object(&staging_key).await.map_err(|error| {
			EngineError::engine(format!("verifying staged replica object: {error}"))
		})?;
		if staging_stat.size == 0 {
			return Err(EngineError::engine("staged shared replica object is empty"));
		}
		if staging_stat.size > 5 * 1024 * 1024 * 1024 * 1024 {
			return Err(EngineError::invalid(
				"staged shared replica object exceeds S3's 5 TiB object limit",
			));
		}
		let staging_verify = snapshot_dir
			.parent()
			.ok_or_else(|| EngineError::invalid("replica snapshot has no parent"))?
			.join(format!(".replica-staging-verify-{}", uuid::Uuid::new_v4()));
		verify_uploaded_s3_bundle(
			s3,
			&staging_key,
			&staging_stat,
			&staging_verify,
			digest,
			REPLICA_ARCHIVE_ROOT,
			Arc::clone(&snapshot),
		)
		.await?;

		copy_verify_or_rollback(
			&final_key,
			async {
				s3.copy_object(&staging_key, &final_key, staging_stat.size)
					.await
					.map_err(|error| {
						EngineError::engine(format!("publishing shared replica object: {error}"))
					})
			},
			async {
				let final_stat = s3.head_object(&final_key).await.map_err(|error| {
					EngineError::engine(format!("verifying published replica object: {error}"))
				})?;
				if final_stat.size == 0 {
					return Err(EngineError::engine("published shared replica object is empty"));
				}
				let final_verify = snapshot_dir
					.parent()
					.ok_or_else(|| EngineError::invalid("replica snapshot has no parent"))?
					.join(format!(".replica-final-verify-{}", uuid::Uuid::new_v4()));
				verify_uploaded_s3_bundle(
					s3,
					&final_key,
					&final_stat,
					&final_verify,
					digest,
					REPLICA_ARCHIVE_ROOT,
					snapshot,
				)
				.await
			},
			async {
				match s3.remove(&final_key).await {
					Ok(()) | Err(crate::s3::S3Error::NotFound) => Ok(()),
					Err(error) => Err(EngineError::engine(format!(
						"removing unverified shared replica object: {error}"
					))),
				}
			},
		)
		.await
	}
	.await;
	let cleanup = match s3.remove(&staging_key).await {
		Ok(()) | Err(crate::s3::S3Error::NotFound) => Ok(()),
		Err(error) => {
			Err(EngineError::engine(format!("removing staged shared replica object: {error}")))
		},
	};
	published?;
	cleanup
}

/// Copy a staged object, verify the final key, and remove any ambiguous or
/// invalid final object before returning the publication error.
pub(crate) async fn copy_verify_or_rollback<C, V, D>(
	key: &str,
	copy: C,
	verify: V,
	rollback: D,
) -> Result<()>
where
	C: Future<Output = Result<()>>,
	V: Future<Output = Result<()>>,
	D: Future<Output = Result<()>>,
{
	let result = async {
		copy.await?;
		verify.await
	}
	.await;
	if let Err(primary) = result {
		if let Err(cleanup) = rollback.await {
			tracing::warn!(%cleanup, object_key = key, "failed to remove unverified replica object");
		}
		return Err(primary);
	}
	Ok(())
}

async fn upload_s3_bundle(
	s3: &crate::s3::S3Client,
	key: &str,
	root: &Path,
	snapshot: Arc<KeySnapshot>,
	archive_root: &str,
) -> Result<()> {
	let (tx, rx) = tokio::sync::mpsc::channel(4);
	let root = root.to_owned();
	let archive_root = archive_root.to_owned();
	let producer = tokio::task::spawn_blocking(move || {
		let writer = super::bundle::ChannelWriter::new(tx);
		let mut encrypted = EncryptedArchive::encrypt_with_snapshot(writer, &snapshot)?;
		super::bundle::write_bundle_named(&root, &archive_root, &mut encrypted, &|_| true)?;
		let writer = encrypted.finish()?;
		writer.finish(Ok(()));
		Ok(())
	});
	let uploaded = s3
		.put_multipart(key, rx)
		.await
		.map_err(|error| EngineError::engine(format!("uploading shared replica: {error}")));
	let produced = producer
		.await
		.map_err(|error| EngineError::engine(format!("replica bundle task failed: {error}")))?;
	uploaded?;
	produced
}

async fn download_s3_bundle(
	s3: &crate::s3::S3Client,
	key: &str,
	stat: &crate::s3::ObjStat,
	dest_root: &Path,
	snapshot: Arc<KeySnapshot>,
	expected_name: Option<&str>,
) -> Result<PathBuf> {
	if dest_root.exists() {
		tokio::fs::remove_dir_all(dest_root).await?;
	}
	if let Some(parent) = dest_root.parent() {
		tokio::fs::create_dir_all(parent).await?;
	}
	let (tx, rx) = tokio::sync::mpsc::channel(4);
	let extract_root = dest_root.to_owned();
	let extractor = tokio::task::spawn_blocking(move || {
		EncryptedArchive::decrypt_with_snapshot(
			super::bundle::ChannelReader::new(rx),
			&snapshot,
			|reader| super::bundle::read_bundle(reader, &extract_root),
		)
	});
	let transferred = async {
		let mut offset = 0u64;
		while offset < stat.size {
			let len = (stat.size - offset).min(1 << 20) as u32;
			let chunk = s3
				.read_range_if_match(key, stat, offset, len)
				.await
				.map_err(|error| EngineError::engine(format!("downloading shared replica: {error}")))?;
			if chunk.is_empty() {
				return Err(EngineError::engine("shared replica ended before its advertised size"));
			}
			offset = offset.saturating_add(chunk.len() as u64);
			tx.send(Ok(chunk))
				.await
				.map_err(|_| EngineError::engine("replica extractor stopped early"))?;
		}
		Ok(())
	}
	.await;
	drop(tx);
	let extracted = extractor
		.await
		.map_err(|error| EngineError::engine(format!("replica extraction task failed: {error}")))?;
	if let Err(error) = transferred {
		let _ = tokio::fs::remove_dir_all(dest_root).await;
		return Err(error);
	}
	if let Err(error) = extracted {
		let _ = tokio::fs::remove_dir_all(dest_root).await;
		return Err(error);
	}
	match extracted_bundle_root(dest_root, expected_name) {
		Ok(root) => Ok(root),
		Err(error) => {
			let _ = tokio::fs::remove_dir_all(dest_root).await;
			Err(error)
		},
	}
}

/// Read back a just-published replica while its publication lease is held.
/// Object metadata is not proof that a multipart upload is complete or
/// uncorrupted; only extracting and hashing the exact archive is.
async fn verify_uploaded_s3_bundle(
	s3: &crate::s3::S3Client,
	key: &str,
	stat: &crate::s3::ObjStat,
	staging: &Path,
	expected_digest: &str,
	expected_name: &str,
	snapshot: Arc<KeySnapshot>,
) -> Result<()> {
	let verified = async {
		let snapshot =
			download_s3_bundle(s3, key, stat, staging, snapshot, Some(expected_name)).await?;
		let actual = tokio::task::spawn_blocking(move || cas::snapshot_digest(&snapshot))
			.await
			.map_err(|error| {
				EngineError::engine(format!("replica verification task failed: {error}"))
			})??;
		if actual != expected_digest {
			return Err(EngineError::engine(format!(
				"published replica digest mismatch: expected {expected_digest}, got {actual}"
			)));
		}
		Ok(())
	}
	.await;
	let cleanup = match tokio::fs::remove_dir_all(staging).await {
		Ok(()) => Ok(()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(error) => {
			Err(EngineError::engine(format!("removing replica verification staging: {error}")))
		},
	};
	verified?;
	cleanup
}

struct TemplateTransfer;

impl MeshTemplateTransfer for TemplateTransfer {
	fn pull_template<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		async move {
			let path = transfer::pull_template(client, &peer_url, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			Ok(path.to_string_lossy().into_owned())
		}
		.boxed()
	}

	fn pull_checkpoint<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		async move {
			let path = transfer::pull_checkpoint(client, &peer_url, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			Ok(path.to_string_lossy().into_owned())
		}
		.boxed()
	}

	fn pull_snapshot<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		sid: String,
		object_key: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		async move {
			let path = transfer::pull_snapshot(client, &peer_url, &sid, &object_key, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			Ok(path.to_string_lossy().into_owned())
		}
		.boxed()
	}

	fn pull_template_metadata<'a>(
		&'a self,
		client: &'a reqwest::Client,
		peer_url: String,
		digest: String,
		token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>> {
		async move {
			let path = transfer::pull_template_metadata(client, &peer_url, &digest, &token)
				.await
				.map_err(engine_to_mesh)?;
			let metadata = transfer::remote_page_metadata(&path).map_err(engine_to_mesh)?;
			Ok(MetadataPull {
				template:        path.to_string_lossy().into_owned(),
				remote_page_url: metadata.page_url,
				digest:          metadata.digest,
			})
		}
		.boxed()
	}
}

async fn owner_lease_loop(runtime: Arc<MeshRuntime>, shutdown: &mut broadcast::Receiver<()>) {
	let mut renewal_tick = tokio::time::interval(Duration::from_secs(1));
	renewal_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	let mut safety_tick = tokio::time::interval(Duration::from_millis(100));
	safety_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = safety_tick.tick() => {
				let expired = {
					let mut deadlines = runtime.owner_lease_deadlines.lock();
					let expired = expired_owner_deadlines(tokio::time::Instant::now(), &deadlines);
					for sid in &expired {
						deadlines.remove(sid);
					}
					expired
				};
				for sid in expired {
					let owner = {
						let owners = runtime.owners.read();
						let restoring = runtime.restoring.read();
						fence_identity(&sid, &owners, &restoring)
					};
					if let Some((_, epoch)) = owner {
						runtime.schedule_fence(sid, epoch);
					}
				}
			},
			_ = renewal_tick.tick() => {
				let mut local = runtime
					.owners
					.read()
					.iter()
					.chain(runtime.restoring.read().iter())
					.filter(|(_, (owner, _))| owner == &runtime.node_id)
					.map(|(sid, (owner, epoch))| (sid.clone(), (owner.clone(), *epoch)))
					.collect::<HashMap<_, _>>();
				for (sid, (owner, epoch)) in local.drain() {
					if runtime
						.cluster_store
						.as_ref()
						.is_some_and(|store| store.rollback_marker(&sid).ok().flatten().is_some())
					{
						// Rollback replay owns this restoring generation. It
						// must claim/adopt it explicitly, never via generic
						// owner lease renewal.
						runtime.owner_lease_deadlines.lock().remove(&sid);
						continue;
					}
					if runtime
						.owner_lease_deadlines
						.lock()
						.get(&sid)
						.is_some_and(|deadline| *deadline <= tokio::time::Instant::now())
					{
						runtime.schedule_fence(sid, epoch);
						continue;
					}
					spawn_owner_renewal(runtime.clone(), sid, owner, epoch);
				}
			},
		}
	}
}

fn spawn_owner_renewal(runtime: Arc<MeshRuntime>, sid: String, owner: String, epoch: u64) {
	let inflight = (sid.clone(), epoch);
	if !runtime.owner_renew_inflight.lock().insert(inflight.clone()) {
		return;
	}
	let Some(store) = runtime.cluster_store.clone() else {
		runtime.owner_renew_inflight.lock().remove(&inflight);
		return;
	};
	let verify_store = store.clone();
	tokio::spawn(async move {
		let request_started = tokio::time::Instant::now();
		let renewal_sid = sid.clone();
		let renewal_owner = owner.clone();
		let renewal = tokio::task::spawn_blocking(move || {
			let epoch =
				i64::try_from(epoch).map_err(|_| EngineError::invalid("owner epoch exceeds i64"))?;
			store.renew_owner_lease(&renewal_sid, &renewal_owner, epoch)
		});
		let result = tokio::time::timeout(Duration::from_secs(3), renewal).await;
		runtime
			.owner_renew_inflight
			.lock()
			.remove(&(sid.clone(), epoch));
		let serving =
			runtime
				.owners
				.read()
				.get(&sid)
				.is_some_and(|(current_owner, current_epoch)| {
					current_owner == &owner && *current_epoch == epoch
				});
		let restoring =
			runtime
				.restoring
				.read()
				.get(&sid)
				.is_some_and(|(current_owner, current_epoch)| {
					current_owner == &owner && *current_epoch == epoch
				});
		let lifecycle_staged = verify_store.suspend_marker(&sid).ok().flatten().is_some()
			|| verify_store.rollback_marker(&sid).ok().flatten().is_some();
		if !serving && !restoring {
			return;
		}
		match result {
			Ok(Ok(Ok(Some(ttl)))) if ttl.is_finite() && ttl > 0.0 => {
				let deadline = owner_watchdog_deadline(request_started, ttl)
					.expect("positive finite owner TTL has a watchdog deadline");
				match runtime.rearm_owner_watchdog(&sid, request_started, ttl) {
					Ok(true) if deadline > tokio::time::Instant::now() => {
						runtime
							.owner_lease_deadlines
							.lock()
							.insert(sid.clone(), deadline);
						if restoring
							&& !lifecycle_staged
							&& runtime.engine.candidate_is_running(&sid)
							&& verify_store
								.resolve(&sid)
								.ok()
								.flatten()
								.is_some_and(|record| {
									record.owner == owner
										&& record.epoch == u64_to_i64(epoch)
										&& !record_is_staging(&record.params)
								}) {
							runtime
								.owners
								.write()
								.insert(sid.clone(), (owner.clone(), epoch));
							runtime.restoring.write().remove(&sid);
						}
					},
					Ok(false) if restoring => {
						let clean_without_candidate = !lifecycle_staged
							&& verify_store
								.resolve(&sid)
								.ok()
								.flatten()
								.is_some_and(|record| {
									record.owner == owner
										&& record.epoch == u64_to_i64(epoch)
										&& !record_is_staging(&record.params)
								});
						if clean_without_candidate {
							runtime.restoring.write().remove(&sid);
							runtime.orphans.lock().push((sid.clone(), owner.clone()));
						} else {
							// Prelaunch durable staging legitimately owns only
							// the control-plane lease until materialization.
							runtime
								.owner_lease_deadlines
								.lock()
								.insert(sid.clone(), deadline);
						}
					},
					_ => runtime.schedule_fence(sid, epoch),
				}
			},
			Ok(Ok(Ok(_))) => runtime.schedule_fence(sid, epoch),
			Ok(Ok(Err(_)) | Err(_)) | Err(_) => {
				let now = tokio::time::Instant::now();
				let deadline = runtime.owner_lease_deadlines.lock().get(&sid).copied();
				if renewal_failure_fences(now, deadline, false) {
					runtime.schedule_fence(sid, epoch);
				}
			},
		}
	});
}

async fn heartbeat_loop(runtime: Arc<MeshRuntime>, shutdown: &mut broadcast::Receiver<()>) {
	let mut ticker = tokio::time::interval(secs(runtime.config.mesh_heartbeat_sec));
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = ticker.tick() => {
				if runtime.enabled() {
					let _ = gossip::heartbeat_once(&*runtime, runtime.peer_client()).await;
				}
			}
		}
	}
}

async fn reconcile_loop(runtime: Arc<MeshRuntime>, shutdown: &mut broadcast::Receiver<()>) {
	let mut ticker = tokio::time::interval(secs(runtime.config.mesh_heartbeat_sec.max(1.0)));
	// A long pass (peer pulls) must not queue a burst of catch-up passes.
	ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	let mut last = HashMap::new();
	// One reconciler for the loop's lifetime: its per-sid checkpoint throttle
	// only works if it survives across passes and notify wakeups.
	let ctx = Reconciler::new(
		runtime.reconcile_config(),
		&*runtime,
		&runtime.engine,
		&*runtime,
		&*runtime,
		&*runtime,
		runtime.peer_client().client(),
	);
	loop {
		tokio::select! {
			_ = shutdown.recv() => break,
			_ = ticker.tick() => {
				run_reconcile_pass(&runtime, &ctx, &mut last).await;
			}
			() = runtime.replicate_notify.notified() => {
				run_reconcile_pass(&runtime, &ctx, &mut last).await;
			}
		}
	}
}

async fn run_reconcile_pass(
	runtime: &Arc<MeshRuntime>,
	ctx: &Reconciler<'_>,
	last: &mut HashMap<String, String>,
) {
	runtime.converge_migrations().await;
	runtime.sweep_replica_cache();
	if !runtime.enabled() {
		return;
	}
	if let Err(err) = ctx.ha_reconcile_once().await {
		tracing::warn!(error = %err, "mesh HA reconciliation failed");
	}
	if let Err(err) = ctx.replicate_once(last).await {
		tracing::warn!(error = %err, "mesh replica loop failed");
	}
}

async fn spawn_engine<F, T>(call: F) -> MeshResult<T>
where
	F: FnOnce() -> Result<T> + Send + 'static,
	T: Send + 'static,
{
	tokio::task::spawn_blocking(call)
		.await
		.map_err(|err| MeshError::with_code(format!("engine task failed: {err}"), "engine_error"))?
		.map_err(engine_to_mesh)
}

fn disabled_membership(caps: NodeCaps, advertise: &str) -> MembershipState {
	MembershipState {
		enabled: false,
		node_id: state::new_node_id(),
		advertise: advertise.to_owned(),
		region: String::new(),
		caps,
		peers: BTreeMap::new(),
		expected_members: 1,
	}
}

const fn placement_weights(config: &ServeConfig) -> PlacementWeights {
	PlacementWeights {
		warm:     config.mesh_w_warm,
		free:     config.mesh_w_free,
		local:    config.mesh_w_local,
		region:   config.mesh_w_region,
		inflight: config.mesh_w_inflight,
	}
}

fn caps_wire(caps: NodeCaps) -> NodeCapsWire {
	NodeCapsWire { vcpus: caps.vcpus.min(u64::from(u32::MAX)) as u32, mem_mib: caps.mem_mib }
}

fn node_caps(caps: NodeCapsWire) -> NodeCaps {
	NodeCaps { vcpus: u64::from(caps.vcpus), mem_mib: caps.mem_mib }
}

fn secs(value: f64) -> Duration {
	Duration::from_secs_f64(value.max(1.0))
}

fn value_u64(value: &Value) -> Option<u64> {
	value
		.as_u64()
		.or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
		.or_else(|| {
			value
				.as_f64()
				.filter(|value| *value >= 0.0)
				.map(|value| value as u64)
		})
}

#[allow(
	clippy::unnecessary_wraps,
	reason = "callers share MeshResult plumbing with validators even though parsing is infallible"
)]
pub(crate) fn writable_volumes(params: &Map<String, Value>) -> MeshResult<Vec<String>> {
	let mut volumes = BTreeSet::new();
	let Some(raw) = params.get("volumes") else {
		return Ok(Vec::new());
	};
	match raw {
		Value::Object(object) => {
			for value in object.values() {
				if let Some((name, read_only)) = volume_name_and_mode(value)
					&& !read_only
				{
					volumes.insert(name);
				}
			}
		},
		Value::Array(items) => {
			for value in items {
				if let Some((name, read_only)) = volume_name_and_mode(value)
					&& !read_only
				{
					volumes.insert(name);
				}
			}
		},
		_ => {},
	}
	Ok(volumes.into_iter().collect())
}

fn volume_name_and_mode(value: &Value) -> Option<(String, bool)> {
	match value {
		Value::String(name) => Some((name.clone(), false)),
		Value::Object(object) => object.get("name").and_then(Value::as_str).map(|name| {
			let read_only = object
				.get("read_only")
				.or_else(|| object.get("ro"))
				.and_then(Value::as_bool)
				.unwrap_or(false);
			(name.to_owned(), read_only)
		}),
		_ => None,
	}
}

fn decision_object(result: Result<LeaseDecision>) -> MeshResult<Map<String, Value>> {
	let value = serde_json::to_value(result.map_err(engine_to_mesh)?)
		.map_err(|err| MeshError::invalid(format!("lease decision serialization failed: {err}")))?;
	value_object(value)
}

fn lease_decision(value: Value) -> MeshResult<LeaseDecision> {
	serde_json::from_value(value)
		.map_err(|err| MeshError::invalid(format!("invalid lease decision: {err}")))
}

fn lease_record_value(record: &LeaseRecord) -> MeshResult<Value> {
	serde_json::to_value(record.to_wire())
		.map_err(|err| MeshError::invalid(format!("lease record serialization failed: {err}")))
}

fn lease_tuple(value: &Value) -> Option<(String, String, u64)> {
	let object = value.as_object()?;
	let volume = object.get("volume")?.as_str()?.to_owned();
	let holder = object
		.get("holder")
		.or_else(|| object.get("holder_node"))?
		.as_str()?
		.to_owned();
	let epoch = object.get("epoch").and_then(value_u64)?;
	Some((volume, holder, epoch))
}

fn record_wire(record: record::CreateRecord) -> CreateRecordWire {
	CreateRecordWire {
		sid:             record.sid,
		params:          record.params,
		owner:           record.owner,
		epoch:           record.epoch,
		idempotency_key: record.idempotency_key,
		ha:              record.ha,
		restart_policy:  record.restart_policy,
		created_at:      record.created_at,
	}
}

fn create_record(record: CreateRecordWire) -> MeshResult<record::CreateRecord> {
	record::CreateRecord::new(
		record.sid,
		record.params,
		record.owner,
		record.epoch,
		record.idempotency_key,
		record.ha,
		record.restart_policy,
		record.created_at,
	)
	.map_err(engine_to_mesh)
}

fn replica_wire(record: replica::ReplicaRecord) -> ReplicaRecordWire {
	ReplicaRecordWire {
		sid:                   record.sid,
		digest:                record.digest,
		object_key:            record.object_key,
		source_node:           record.source_node,
		source_epoch:          record.source_epoch,
		checkpoint_generation: record.checkpoint_generation,
		snapshot_dir:          record.snapshot_dir,
		params:                record.params,
		needs_secrets:         record.needs_secrets,
	}
}

fn reconcile_record(record: record::CreateRecord) -> reconciler::CreateRecord {
	reconciler::CreateRecord {
		sid:             record.sid,
		params:          Value::Object(record.params),
		owner:           record.owner,
		epoch:           i64_to_u64(record.epoch),
		idempotency_key: record.idempotency_key,
		ha:              record.ha,
		restart_policy:  record.restart_policy,
	}
}

fn reconcile_replica(record: replica::ReplicaRecord) -> reconciler::ReplicaRecord {
	reconciler::ReplicaRecord {
		sid:                   record.sid,
		digest:                record.digest,
		object_key:            record.object_key,
		source_node:           record.source_node,
		source_epoch:          record.source_epoch,
		checkpoint_generation: record.checkpoint_generation,
		snapshot_dir:          PathBuf::from(record.snapshot_dir),
		params:                Value::Object(record.params),
		needs_secrets:         record.needs_secrets,
	}
}

fn sandbox_record(value: Value) -> Result<reconciler::SandboxRecord> {
	let object = value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::engine("sandbox view must be an object"))?;
	let name = object
		.get("name")
		.or_else(|| object.get("id"))
		.and_then(Value::as_str)
		.unwrap_or_default()
		.to_owned();
	Ok(reconciler::SandboxRecord { name, detail: object })
}

/// Select the durable portion of a freshly restored Engine record. Engine
/// views also contain status and transient process fields, so they must not be
/// written back verbatim to ownership storage.
fn restored_metadata(
	record: &reconciler::SandboxRecord,
) -> Result<(Map<String, Value>, String, String)> {
	if record.name.is_empty() {
		return Err(EngineError::engine("restored sandbox view has no name"));
	}
	let ha = record
		.detail
		.get("ha")
		.and_then(Value::as_str)
		.unwrap_or("off")
		.to_owned();
	let restart_policy = record
		.detail
		.get("restart_policy")
		.and_then(Value::as_str)
		.unwrap_or("none")
		.to_owned();
	let mut params = sanitize_sandbox_params(record.detail.clone());
	// Secrets are process-local recovery inputs; never promote them into
	// durable ownership parameters during a rollback handoff.
	params.remove("secrets");
	for field in [
		"_mesh_restore_pending",
		"_mesh_rollback_pending",
		"restore_pending",
		"rollback_pending",
		"_mesh_lifecycle_operation",
	] {
		params.remove(field);
	}
	Ok((params, ha, restart_policy))
}

fn params_object(value: &Value) -> Result<Map<String, Value>> {
	value
		.as_object()
		.cloned()
		.ok_or_else(|| EngineError::invalid("record params must be an object"))
}

fn value_object(value: Value) -> MeshResult<Map<String, Value>> {
	match value {
		Value::Object(object) => Ok(object),
		_ => Err(MeshError::invalid("expected JSON object")),
	}
}

fn mesh_to_engine(error: MeshError) -> EngineError {
	let code = match error.code.as_str() {
		"not_found" => ErrorCode::NotFound,
		"not_running" => ErrorCode::NotRunning,
		"busy" | "conflict" => ErrorCode::Busy,
		"unsupported" => ErrorCode::Unsupported,
		"unauthorized" => ErrorCode::Unauthorized,
		"forbidden" => ErrorCode::Forbidden,
		"invalid" | "arch_required" => ErrorCode::Invalid,
		_ => ErrorCode::Engine,
	};
	EngineError::new(code, error.message)
}

fn engine_to_mesh(error: EngineError) -> MeshError {
	MeshError::with_code(error.message, error.code.as_str())
}

fn mesh_request_idempotency_key() -> String {
	hex::encode(rand::random::<[u8; 18]>())
}

fn i64_to_u64(value: i64) -> u64 {
	u64::try_from(value).unwrap_or_default()
}

fn u64_to_i64(value: u64) -> i64 {
	i64::try_from(value).unwrap_or(i64::MAX)
}

fn unix_now() -> f64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

fn sanitize_sandbox_params(mut params: Map<String, Value>) -> Map<String, Value> {
	const KEYS: &[&str] = &[
		"image",
		"template",
		"dockerfile",
		"context",
		"name",
		"cpus",
		"memory",
		"disk_mb",
		"timeout",
		"timeout_secs",
		"idle_timeout_secs",
		"activity_threshold_bytes",
		"persistence",
		"workdir",
		"env",
		"credentials",
		"s3_mounts",
		"owner_tenant",
		"encryption_key_id",
		"secrets",
		"volumes",
		"tags",
		"fs_dir",
		"block_network",
		"ports",
		"egress_allow",
		"egress_allow_domains",
		"inbound_cidr_allowlist",
		"readiness_probe",
		"pool_size",
		"remote_page_url",
		"remote_page_token",
		"remote_page_digest",
		"ha",
		"arch",
		"idempotency_key",
		"command",
	];
	params.retain(|key, _| KEYS.contains(&key.as_str()));
	params
}

pub(crate) fn sandbox_create_from_mesh_params(params: Map<String, Value>) -> Result<SandboxCreate> {
	serde_json::from_value(Value::Object(sanitize_sandbox_params(params))).map_err(EngineError::from)
}
