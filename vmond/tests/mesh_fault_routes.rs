mod mesh_support;

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use axum::{Json, Router, routing::post};
use futures_util::future::BoxFuture;
use mesh_support::{FaultProxy, InProcNode};
use parking_lot::Mutex;
use serde_json::{Value, json};
use vmond::mesh::{
	gossip::{JsonObject, MeshError, MeshResult, PeerHttpClient},
	lease::LeaseManager,
	routes::{
		CreateRecordWire, MeshControl, MeshEngine, MeshLeaseManager, MeshRecordStore,
		MeshReplicaStore, MeshRouteConfig, MeshRouteState, MeshSetupResult, MeshTemplateTransfer,
		MetadataPull, MigratePrepareWire, MigrationCleanupWire, NodeCapsWire, PeerMember,
		ReplicaRecordWire, router as mesh_router,
	},
};

#[derive(Clone)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
	fn new(now: f64) -> Self {
		Self(Arc::new(AtomicU64::new(now.to_bits())))
	}

	fn now(&self) -> f64 {
		f64::from_bits(self.0.load(Ordering::Relaxed))
	}

	fn set(&self, now: f64) {
		self.0.store(now.to_bits(), Ordering::Relaxed);
	}
}

#[derive(Default)]
struct FakeMesh {
	node_id:   String,
	advertise: String,
	healthy:   Mutex<BTreeSet<String>>,
	owners:    Mutex<BTreeMap<String, (String, i64)>>,
	placement: Mutex<Option<String>>,
	peer_urls: Mutex<BTreeMap<String, String>>,
}

impl FakeMesh {
	fn new(node_id: &str, advertise: &str) -> Self {
		Self {
			node_id:   node_id.to_owned(),
			advertise: advertise.to_owned(),
			healthy:   Mutex::new(BTreeSet::new()),
			owners:    Mutex::new(BTreeMap::new()),
			placement: Mutex::new(None),
			peer_urls: Mutex::new(BTreeMap::new()),
		}
	}

	fn mark_healthy(&self, node_id: &str) {
		self.healthy.lock().insert(node_id.to_owned());
	}

	fn route_to(&self, node_id: &str, url: String) {
		self.peer_urls.lock().insert(node_id.to_owned(), url);
	}

	fn place_on(&self, node_id: &str) {
		*self.placement.lock() = Some(node_id.to_owned());
	}
}

impl MeshControl for FakeMesh {
	fn node_id(&self) -> String {
		self.node_id.clone()
	}

	fn advertise(&self) -> String {
		self.advertise.clone()
	}

	fn default_advertise(&self) -> String {
		self.advertise.clone()
	}

	fn caps(&self) -> NodeCapsWire {
		NodeCapsWire { vcpus: 2, mem_mib: 1024 }
	}

	fn enabled(&self) -> bool {
		true
	}

	fn peers(&self) -> Vec<PeerMember> {
		Vec::new()
	}

	fn expected_members(&self) -> usize {
		1
	}

	fn quorum_needed(&self) -> usize {
		1
	}

	fn setup(
		&self,
		advertise: String,
		_region: String,
		_caps: Option<NodeCapsWire>,
	) -> MeshResult<MeshSetupResult> {
		Ok(MeshSetupResult {
			blob: format!("blob:{advertise}"),
			node_id: self.node_id.clone(),
			advertise,
		})
	}

	fn join(&self, _blob: String, _advertise: Option<String>, _region: String) -> MeshResult<Value> {
		Ok(json!({"ok": true, "node_id": self.node_id}))
	}

	fn leave(&self) -> MeshResult<()> {
		Ok(())
	}

	fn status(&self) -> MeshResult<Value> {
		Ok(json!({"enabled": true, "node_id": self.node_id, "peers": []}))
	}

	fn register(&self, _state: JsonObject) -> MeshResult<Value> {
		Ok(json!({"ok": true}))
	}

	fn heartbeat(&self, state: JsonObject, known: Vec<Value>) -> MeshResult<Value> {
		Ok(json!({"state": state, "known": known}))
	}

	fn depart(&self, node_id: &str) {
		self.healthy.lock().remove(node_id);
	}

	fn is_peer_healthy(&self, node_id: &str) -> bool {
		node_id == self.node_id || self.healthy.lock().contains(node_id)
	}

	fn migration_target(&self, node_id: &str) -> MeshResult<PeerMember> {
		Ok(PeerMember {
			node_id:   node_id.to_owned(),
			advertise: format!("http://{node_id}"),
			healthy:   true,
		})
	}

	fn replica_targets(&self, _sid: &str, _k: usize) -> Vec<String> {
		Vec::new()
	}

	fn peer_url(&self, node_id: &str) -> Option<String> {
		self
			.peer_urls
			.lock()
			.get(node_id)
			.cloned()
			.or_else(|| Some(format!("http://{node_id}")))
	}

	fn find_template_provider(&self, _key: &str) -> Option<(String, String)> {
		None
	}

	fn request_template_key(&self, _params: &JsonObject) -> Option<String> {
		None
	}

	fn has_local_template_key(&self, _key: &str) -> bool {
		false
	}

	fn authoritative_owner(&self, sid: &str) -> MeshResult<Option<(String, i64)>> {
		Ok(self.owners.lock().get(sid).cloned())
	}

	fn local_epoch(&self, _sid: &str) -> i64 {
		0
	}

	fn owner_of(&self, sid: &str) -> MeshResult<Option<String>> {
		Ok(self.authoritative_owner(sid)?.map(|(owner, _)| owner))
	}

	fn record_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		self
			.owners
			.lock()
			.insert(sid.to_owned(), (node_id.to_owned(), epoch));
	}

	fn broadcast_owner(&self, sid: &str, node_id: &str, epoch: i64) {
		self.record_owner(sid, node_id, epoch);
	}

	fn forget_owner(&self, sid: &str) {
		self.owners.lock().remove(sid);
	}

	fn mark_unhealthy(&self, node_id: &str) {
		self.healthy.lock().remove(node_id);
	}

	fn pinned_local(&self, _params: &JsonObject) -> bool {
		false
	}

	fn place(&self, _params: &JsonObject) -> MeshResult<String> {
		Ok(self
			.placement
			.lock()
			.clone()
			.unwrap_or_else(|| self.node_id.clone()))
	}

	fn coordinator_for(&self, _key: &str) -> String {
		self.node_id.clone()
	}

	fn idem_owner(&self, _key: &str) -> Option<String> {
		None
	}

	fn idem_pin(&self, _key: &str, owner: &str) -> String {
		owner.to_owned()
	}

	fn idem_unpin(&self, _key: &str) {}

	fn worker_begin(&self, _key: &str) -> bool {
		true
	}

	fn worker_end(&self, _key: &str) {}
}

#[derive(Default)]
struct FakeEngine {
	candidate: Mutex<Option<Value>>,
	teardowns: Mutex<Vec<(String, i64)>>,
}

impl MeshEngine for FakeEngine {
	fn owned_ids(&self) -> MeshResult<Vec<String>> {
		Ok(Vec::new())
	}

	fn list_views(&self) -> MeshResult<Vec<Value>> {
		Ok(Vec::new())
	}

	fn has_sandbox(&self, sid: &str) -> bool {
		self
			.candidate
			.lock()
			.as_ref()
			.and_then(|candidate| candidate.get("id"))
			.and_then(Value::as_str)
			== Some(sid)
	}

	fn get_view(&self, sid: &str) -> MeshResult<Value> {
		self
			.candidate
			.lock()
			.as_ref()
			.filter(|candidate| candidate.get("id").and_then(Value::as_str) == Some(sid))
			.cloned()
			.ok_or_else(|| MeshError::invalid(format!("unknown sandbox {sid}")))
	}

	fn checkpoint_age_sec(&self, _sid: &str) -> Option<f64> {
		None
	}

	fn find_by_idempotency_key(&self, key: &str) -> MeshResult<Option<Value>> {
		Ok(self
			.candidate
			.lock()
			.as_ref()
			.filter(|candidate| {
				candidate
					.get("detail")
					.and_then(Value::as_object)
					.and_then(|detail| detail.get("idempotency_key"))
					.and_then(Value::as_str)
					== Some(key)
			})
			.cloned())
	}

	fn record_idempotency(&self, _sid: &str, key: &str) -> MeshResult<()> {
		if let Some(candidate) = self.candidate.lock().as_mut() {
			candidate["detail"]["idempotency_key"] = json!(key);
		}
		Ok(())
	}

	fn record_create_epoch(&self, _sid: &str, epoch: i64) -> MeshResult<()> {
		if let Some(candidate) = self.candidate.lock().as_mut() {
			candidate["detail"]["_mesh_create_epoch"] = json!(epoch);
		}
		Ok(())
	}

	fn record_volume_leases(&self, _sid: &str, _leases: Vec<Value>) -> MeshResult<()> {
		Ok(())
	}

	fn teardown_candidate(&self, sid: String, expected_epoch: i64) -> BoxFuture<'_, MeshResult<()>> {
		self.teardowns.lock().push((sid, expected_epoch));
		self.candidate.lock().take();
		Box::pin(async { Ok(()) })
	}

	fn delete_portable_history(
		&self,
		_sid: String,
		_owner: String,
		_epoch: i64,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn stage_migration_delta(
		&self,
		_sid: String,
		verified_cache_path: String,
		_base_dir: String,
	) -> BoxFuture<'_, MeshResult<String>> {
		Box::pin(async move { Ok(verified_cache_path) })
	}

	fn create_sandbox(&self, mut params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		let id = params
			.remove("name")
			.and_then(|v| v.as_str().map(str::to_owned))
			.unwrap_or_else(|| "created".to_owned());
		*self.candidate.lock() = Some(json!({"id": id, "detail": {}}));
		Box::pin(async move { Ok(json!({"id": id})) })
	}

	fn run_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_detached(&self, params: JsonObject) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn restore_from_template(
		&self,
		params: JsonObject,
		_template_dir: String,
		_quorum_ok: bool,
	) -> BoxFuture<'_, MeshResult<Value>> {
		self.create_sandbox(params)
	}

	fn migrate_precopy(&self, sid: String) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		Box::pin(async move { Err(MeshError::invalid(format!("no checkpoint for {sid}"))) })
	}

	fn migrate_finalize(
		&self,
		sid: String,
		_base_dir: String,
		_cleanup: MigrationCleanupWire,
	) -> BoxFuture<'_, MeshResult<MigratePrepareWire>> {
		Box::pin(async move { Err(MeshError::invalid(format!("no checkpoint for {sid}"))) })
	}

	fn checkpoint_discard(
		&self,
		_snapshot_dir: String,
		_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn migrate_abort(
		&self,
		_sid: String,
		_base_digest: String,
		_delta_dir: String,
		_delta_digest: String,
		_params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn migrate_commit(
		&self,
		_sid: String,
		_base_dir: String,
		_base_digest: String,
		_delta_dir: String,
		_delta_digest: String,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn migrate_adopt_target(
		&self,
		_sid: String,
		_delta_dir: String,
		_params: JsonObject,
	) -> BoxFuture<'_, MeshResult<Value>> {
		Box::pin(async { Err(MeshError::invalid("no staged migration")) })
	}

	fn migrate_activate_target(&self, _sid: String, _epoch: i64) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}
}

struct FakeLease {
	manager:  LeaseManager,
	released: Mutex<Vec<String>>,
}

impl FakeLease {
	fn new(clock: ManualClock) -> Self {
		let root = tempfile::tempdir().unwrap().keep().join("leases");
		Self {
			manager:  LeaseManager::with_clock(root, move || clock.now()),
			released: Mutex::new(Vec::new()),
		}
	}

	fn wire(decision: vmond::mesh::lease::LeaseDecision) -> MeshResult<JsonObject> {
		decision
			.to_wire()
			.as_object()
			.cloned()
			.ok_or_else(|| MeshError::invalid("lease decision was not an object"))
	}
}

impl MeshLeaseManager for FakeLease {
	fn vote_grant(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject> {
		Self::wire(
			self
				.manager
				.vote_grant(&volume, &holder_node, epoch as u64, ttl)
				.map_err(MeshError::from)?,
		)
	}

	fn vote_renew(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
		ttl: f64,
	) -> MeshResult<JsonObject> {
		Self::wire(
			self
				.manager
				.vote_renew(&volume, &holder_node, epoch as u64, ttl)
				.map_err(MeshError::from)?,
		)
	}

	fn vote_release(
		&self,
		volume: String,
		holder_node: String,
		epoch: i64,
	) -> MeshResult<JsonObject> {
		Self::wire(
			self
				.manager
				.vote_release(&volume, &holder_node, epoch as u64)
				.map_err(MeshError::from)?,
		)
	}

	fn acquire_writable_volume_leases(
		&self,
		_params: JsonObject,
		_epoch: i64,
	) -> BoxFuture<'_, MeshResult<Vec<Value>>> {
		Box::pin(async { Ok(Vec::new()) })
	}

	fn release_leases(&self, _leases: Vec<Value>) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn release_record_volume_leases(&self, sid: String) -> BoxFuture<'_, MeshResult<()>> {
		self.released.lock().push(sid);
		Box::pin(async { Ok(()) })
	}
}

#[derive(Default)]
struct FakeRecords {
	records:               Mutex<BTreeMap<String, CreateRecordWire>>,
	reservations:          Mutex<BTreeMap<String, (String, String, i64)>>,
	put_error_after_store: AtomicBool,
	put_fails:             AtomicBool,
}

impl MeshRecordStore for FakeRecords {
	fn get(&self, sid: &str) -> MeshResult<Option<CreateRecordWire>> {
		Ok(self.records.lock().get(sid).cloned())
	}

	fn reserve_create_epoch(
		&self,
		sid: &str,
		owner: &str,
		idempotency_key: &str,
	) -> MeshResult<i64> {
		let mut reservations = self.reservations.lock();
		if let Some((reserved_owner, reserved_key, epoch)) = reservations.get(sid) {
			return if reserved_owner == owner && reserved_key == idempotency_key {
				Ok(*epoch)
			} else {
				Err(MeshError::conflict("sandbox creation is reserved by another request"))
			};
		}
		let epoch = 1;
		reservations.insert(sid.to_owned(), (owner.to_owned(), idempotency_key.to_owned(), epoch));
		Ok(epoch)
	}

	fn put(&self, record: CreateRecordWire) -> MeshResult<CreateRecordWire> {
		let mut reservations = self.reservations.lock();
		if let Some((owner, key, epoch)) = reservations.get(&record.sid)
			&& (owner != &record.owner || key != &record.idempotency_key || *epoch != record.epoch)
		{
			return Err(MeshError::conflict("sandbox creation reservation does not match record"));
		}
		if self.put_fails.load(Ordering::SeqCst) {
			return Err(MeshError::new("record put failed"));
		}
		self
			.records
			.lock()
			.insert(record.sid.clone(), record.clone());
		if self.put_error_after_store.swap(false, Ordering::SeqCst) {
			return Err(MeshError::new("record response lost after commit"));
		}
		reservations.remove(&record.sid);
		Ok(record)
	}

	fn list(&self) -> MeshResult<Vec<CreateRecordWire>> {
		Ok(self.records.lock().values().cloned().collect())
	}

	fn commit_delete(&self, sid: &str, _owner: &str, _epoch: i64) -> MeshResult<()> {
		self.records.lock().remove(sid);
		Ok(())
	}

	fn notify_source_migration_claim<'a>(
		&'a self,
		_record: &'a CreateRecordWire,
	) -> BoxFuture<'a, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}
}

#[derive(Default)]
struct FakeReplicas;

impl MeshReplicaStore for FakeReplicas {
	fn get(&self, _sid: &str) -> MeshResult<Option<ReplicaRecordWire>> {
		Ok(None)
	}

	fn put(
		&self,
		_sid: String,
		_object_key: String,
		_digest: String,
		_source_node: String,
		_source_epoch: u64,
		_checkpoint_generation: u64,
		_snapshot_dir: String,
		_params: JsonObject,
	) -> BoxFuture<'_, MeshResult<()>> {
		Box::pin(async { Ok(()) })
	}

	fn list(&self) -> MeshResult<Vec<String>> {
		Ok(Vec::new())
	}

	fn remove(&self, _sid: &str) -> MeshResult<()> {
		Ok(())
	}
}

#[derive(Default)]
struct FakeTransfer;

impl MeshTemplateTransfer for FakeTransfer {
	fn pull_template<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::unreachable("template pull not configured")) })
	}

	fn pull_checkpoint<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::unreachable("checkpoint pull not configured")) })
	}

	fn pull_snapshot<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_sid: String,
		_object_key: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<String>> {
		Box::pin(async { Err(MeshError::unreachable("snapshot pull not configured")) })
	}

	fn pull_template_metadata<'a>(
		&'a self,
		_client: &'a reqwest::Client,
		_peer_url: String,
		_digest: String,
		_token: String,
	) -> BoxFuture<'a, MeshResult<MetadataPull>> {
		Box::pin(async { Err(MeshError::unreachable("metadata pull not configured")) })
	}
}

fn route_state(mesh: Arc<FakeMesh>, lease: Arc<FakeLease>, token: &str) -> MeshRouteState {
	route_state_with(
		mesh,
		Arc::new(FakeEngine::default()),
		lease,
		Arc::new(FakeRecords::default()),
		token,
	)
}

fn route_state_with(
	mesh: Arc<FakeMesh>,
	engine: Arc<FakeEngine>,
	lease: Arc<FakeLease>,
	records: Arc<FakeRecords>,
	token: &str,
) -> MeshRouteState {
	MeshRouteState::new(
		token,
		token,
		PeerHttpClient::new(token).unwrap(),
		mesh,
		engine,
		lease,
		records,
		Arc::new(FakeReplicas),
		Arc::new(FakeTransfer),
		MeshRouteConfig::default(),
	)
}

async fn spawn_mesh_node(name: &str, token: &str, state: MeshRouteState) -> InProcNode {
	InProcNode::spawn(name, token, mesh_router(state)).await
}

async fn json_body(response: reqwest::Response) -> Value {
	let status = response.status();
	let bytes = response.bytes().await.expect("response bytes");
	serde_json::from_slice(&bytes).unwrap_or_else(|err| {
		panic!("HTTP {status} did not return JSON: {err}; body={}", String::from_utf8_lossy(&bytes))
	})
}

#[test]
fn create_reservation_rejoins_same_token_and_rejects_competing_token() {
	let records = FakeRecords::default();

	assert_eq!(
		records
			.reserve_create_epoch("sandbox", "node-a", "token-a")
			.unwrap(),
		1
	);
	assert_eq!(
		records
			.reserve_create_epoch("sandbox", "node-a", "token-a")
			.unwrap(),
		1
	);
	let error = records
		.reserve_create_epoch("sandbox", "node-a", "token-b")
		.unwrap_err();
	assert_eq!(error.code, "conflict");
}

#[tokio::test]
async fn candidate_teardown_route_rejects_stale_epoch_before_releasing_leases() {
	let token = "secret";
	let engine = Arc::new(FakeEngine::default());
	*engine.candidate.lock() = Some(json!({
		"id": "sandbox",
		"detail": {"_mesh_create_epoch": 7, "idempotency_key": "token-a"}
	}));
	let lease = Arc::new(FakeLease::new(ManualClock::new(100.0)));
	let state = route_state_with(
		Arc::new(FakeMesh::new("A", "http://a")),
		engine.clone(),
		lease.clone(),
		Arc::new(FakeRecords::default()),
		token,
	);
	let node = spawn_mesh_node("A", token, state).await;

	let stale = node
		.post_json(
			"/v1/mesh/candidate/teardown",
			&json!({"sid": "sandbox", "owner": "A", "epoch": 6, "idempotency_key": "token-a"}),
		)
		.await;
	assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
	assert!(engine.teardowns.lock().is_empty());
	assert!(lease.released.lock().is_empty());

	let exact = node
		.post_json(
			"/v1/mesh/candidate/teardown",
			&json!({"sid": "sandbox", "owner": "A", "epoch": 7, "idempotency_key": "token-a"}),
		)
		.await;
	assert_eq!(exact.status(), reqwest::StatusCode::OK);
	assert_eq!(*engine.teardowns.lock(), vec![("sandbox".to_owned(), 7)]);
	assert_eq!(*lease.released.lock(), vec!["sandbox".to_owned()]);
	node.stop().await;
}

#[tokio::test]
async fn durable_create_record_after_response_error_preserves_local_candidate() {
	let token = "secret";
	let engine = Arc::new(FakeEngine::default());
	let lease = Arc::new(FakeLease::new(ManualClock::new(100.0)));
	let records = Arc::new(FakeRecords::default());
	records.put_error_after_store.store(true, Ordering::SeqCst);
	let state = route_state_with(
		Arc::new(FakeMesh::new("A", "http://a")),
		engine.clone(),
		lease.clone(),
		records.clone(),
		token,
	);
	let node = spawn_mesh_node("A", token, state).await;

	let response = node
		.post_json(
			"/v1/mesh/idem/sandboxes/coordinate",
			&json!({"key": "token-a", "params": {"name": "sandbox"}}),
		)
		.await;
	assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
	assert!(engine.has_sandbox("sandbox"));
	assert!(engine.teardowns.lock().is_empty());
	assert!(lease.released.lock().is_empty());
	assert!(records.get("sandbox").unwrap().is_some());
	node.stop().await;
}

#[tokio::test]
async fn failed_remote_create_record_tears_down_exact_peer_candidate_after_put_failure() {
	let token = "secret";
	let peer_engine = Arc::new(FakeEngine::default());
	let peer_lease = Arc::new(FakeLease::new(ManualClock::new(100.0)));
	let peer_state = route_state_with(
		Arc::new(FakeMesh::new("B", "http://b")),
		peer_engine.clone(),
		peer_lease.clone(),
		Arc::new(FakeRecords::default()),
		token,
	);
	let peer = spawn_mesh_node("B", token, peer_state).await;

	let coordinator_mesh = Arc::new(FakeMesh::new("A", "http://a"));
	coordinator_mesh.place_on("B");
	coordinator_mesh.route_to("B", peer.url());
	let coordinator_records = Arc::new(FakeRecords::default());
	coordinator_records.put_fails.store(true, Ordering::SeqCst);
	let coordinator_state = route_state_with(
		coordinator_mesh,
		Arc::new(FakeEngine::default()),
		Arc::new(FakeLease::new(ManualClock::new(100.0))),
		coordinator_records.clone(),
		token,
	);
	let coordinator = spawn_mesh_node("A", token, coordinator_state).await;

	let response = coordinator
		.post_json(
			"/v1/mesh/idem/sandboxes/coordinate",
			&json!({"key": "token-a", "params": {"name": "sandbox"}}),
		)
		.await;
	assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
	assert!(!peer_engine.has_sandbox("sandbox"));
	assert_eq!(*peer_engine.teardowns.lock(), vec![("sandbox".to_owned(), 1)]);
	assert_eq!(*peer_lease.released.lock(), vec!["sandbox".to_owned()]);
	assert!(coordinator_records.get("sandbox").unwrap().is_none());

	coordinator.stop().await;
	peer.stop().await;
}

#[tokio::test]
async fn reachable_route_reports_self_healthy_peer_and_unknown_peer() {
	let token = "secret";
	let mesh = Arc::new(FakeMesh::new("A", "http://a"));
	mesh.mark_healthy("B");
	let state = route_state(mesh, Arc::new(FakeLease::new(ManualClock::new(100.0))), token);
	let node = spawn_mesh_node("A", token, state).await;

	let peer = node.get_json("/v1/mesh/reachable/B").await;
	assert_eq!(peer.status(), reqwest::StatusCode::OK);
	assert_eq!(json_body(peer).await, json!({"reachable": true}));

	let ghost = node.get_json("/v1/mesh/reachable/ghost").await;
	assert_eq!(ghost.status(), reqwest::StatusCode::OK);
	assert_eq!(json_body(ghost).await, json!({"reachable": false}));

	let self_check = node.get_json("/v1/mesh/reachable/A").await;
	assert_eq!(self_check.status(), reqwest::StatusCode::OK);
	assert_eq!(json_body(self_check).await, json!({"reachable": true}));

	node.stop().await;
}

#[tokio::test]
async fn lease_grant_route_preserves_non_overlap_on_early_successor_request() {
	let token = "secret";
	let clock = ManualClock::new(100.0);
	let lease = Arc::new(FakeLease::new(clock.clone()));
	let state = route_state(Arc::new(FakeMesh::new("A", "http://a")), lease, token);
	let node = spawn_mesh_node("A", token, state).await;

	let first = node
		.post_json(
			"/v1/mesh/lease/grant",
			&json!({"volume": "shared", "holder_node": "A", "epoch": 1, "ttl": 10.0}),
		)
		.await;
	assert_eq!(first.status(), reqwest::StatusCode::OK);
	let body = json_body(first).await;
	assert_eq!(body["granted"], true);
	assert_eq!(body["record"]["holder"], "A");

	clock.set(109.999);
	let early = node
		.post_json(
			"/v1/mesh/lease/grant",
			&json!({"volume": "shared", "holder_node": "B", "epoch": 2, "ttl": 10.0}),
		)
		.await;
	assert_eq!(early.status(), reqwest::StatusCode::OK);
	let early_body = json_body(early).await;
	assert_eq!(early_body["granted"], false);
	assert_eq!(early_body["reason"], "conflict");

	clock.set(110.0);
	let successor = node
		.post_json(
			"/v1/mesh/lease/grant",
			&json!({"volume": "shared", "holder_node": "B", "epoch": 2, "ttl": 10.0}),
		)
		.await;
	assert_eq!(successor.status(), reqwest::StatusCode::OK);
	let successor_body = json_body(successor).await;
	assert_eq!(successor_body["granted"], true);
	assert_eq!(successor_body["record"]["holder"], "B");
	assert_eq!(successor_body["record"]["granted_at"], 110.0);

	node.stop().await;
}

#[tokio::test]
async fn fault_proxy_drops_matching_request_once_then_forwards_after_reset() {
	let backend = InProcNode::spawn(
		"backend",
		"secret",
		Router::new().route("/v1/mesh/ping", post(|| async { Json(json!({"ok": true})) })),
	)
	.await;
	let proxy = FaultProxy::start(backend.addr()).await;
	proxy.drop_next(1, Some(vec!["/v1/mesh/ping"]));
	let client = reqwest::Client::builder()
		.timeout(std::time::Duration::from_millis(300))
		.build()
		.unwrap();

	let dropped = client
		.post(format!("{}/v1/mesh/ping", proxy.url()))
		.json(&json!({"state": {"node_id": "A"}}))
		.send()
		.await;
	assert!(dropped.is_err(), "dropped request should close before a response");

	let forwarded = client
		.post(format!("{}/v1/mesh/ping", proxy.url()))
		.json(&json!({"state": {"node_id": "A"}}))
		.send()
		.await
		.unwrap();
	assert_eq!(forwarded.status(), reqwest::StatusCode::OK);
	assert_eq!(forwarded.json::<Value>().await.unwrap(), json!({"ok": true}));

	proxy.close();
	backend.stop().await;
}

#[tokio::test]
async fn legacy_record_remove_route_is_not_registered() {
	let token = "secret";
	let state = route_state(
		Arc::new(FakeMesh::new("A", "http://a")),
		Arc::new(FakeLease::new(ManualClock::new(100.0))),
		token,
	);
	let node = spawn_mesh_node("A", token, state).await;

	let response = node
		.post_json("/v1/mesh/record/remove", &json!({"sid": "sandbox", "owner": "A", "epoch": 1}))
		.await;
	assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

	node.stop().await;
}
