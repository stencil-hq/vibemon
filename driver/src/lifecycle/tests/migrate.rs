//! Offline migration between fake nodes: the admin route, rollback onto the
//! source, the evacuator of draining nodes, node-local paused sandboxes, and
//! recovery of a migration whose driver died.

use std::sync::Weak;

use vmon_node::api::AdoptSandbox;

use super::*;
use crate::{lifecycle::routes::put_route, store::sandboxes::SandboxState};

const PEER: NodeId = NodeId(0x00d2);
const OTHER: NodeId = NodeId(0x00d3);

/// Handover calls a fake node received, injected adoption failures, and the
/// other fake nodes it pulls paused state from.
#[derive(Default)]
pub(super) struct FakeMigration {
	/// `POST /sandboxes/{id}/adopt` requests.
	pub(super) adopts:      Vec<(String, AdoptSandbox)>,
	/// `DELETE /sandboxes/{id}/paused` calls.
	pub(super) drops:       Vec<String>,
	/// Pauses that were the source side of a handover.
	pub(super) handovers:   Vec<String>,
	/// The next this many adoptions fail like a node out of disk.
	pub(super) fail_adopts: u32,
	/// Fake nodes by node id.
	pub(super) peers:       BTreeMap<NodeId, Weak<Mutex<FakeNode>>>,
}

/// Copies the source's committed pause into this node's table.
pub(super) async fn fake_adopt(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Json(req): Json<AdoptSandbox>,
) -> Response {
	let source = node
		.lock()
		.migration
		.peers
		.get(&req.source)
		.and_then(Weak::upgrade);
	let held = source.and_then(|source| {
		source
			.lock()
			.sandboxes
			.get(&id)
			.filter(|info| info.state == NodeState::Paused)
			.cloned()
	});
	let mut node = node.lock();
	node.migration.adopts.push((id.clone(), req.clone()));
	if node.migration.fail_adopts > 0 {
		node.migration.fail_adopts -= 1;
		return disk_full(&format!("adopting sandbox {id}"));
	}
	if node.sandboxes.contains_key(&id) {
		return StatusCode::CONFLICT.into_response();
	}
	let Some(mut info) = held else {
		return StatusCode::CONFLICT.into_response();
	};
	info.end_at_ms = req.meta.end_at_ms;
	node.sandboxes.insert(id, info.clone());
	Json(info).into_response()
}

/// Forgets a paused copy; running sandboxes are refused.
pub(super) async fn fake_drop_paused(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
) -> StatusCode {
	let mut node = node.lock();
	node.migration.drops.push(id.clone());
	match node.sandboxes.get(&id).map(|info| info.state) {
		None => StatusCode::NOT_FOUND,
		Some(NodeState::Running) => StatusCode::CONFLICT,
		Some(NodeState::Paused) => {
			node.sandboxes.remove(&id);
			StatusCode::NO_CONTENT
		},
	}
}

/// A node-only gossip member joined to the harness cluster, served by its own
/// fake node, which can pull paused state from the harness node's.
struct Peer {
	cluster: Cluster,
	fake:    Shared,
	_dir:    tempfile::TempDir,
}

async fn join_peer(h: &Harness, id: NodeId, change: impl FnOnce(&mut NodeCapacity)) -> Peer {
	let dir = tempfile::tempdir().unwrap();
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let mut capacity = NodeCapacity {
		arch: "x86_64".into(),
		backend: "kvm".into(),
		os: "linux".into(),
		cpu_baseline: "v3".into(),
		cpus: 8,
		memory_mb: 16_384,
		disk_free_mb: 100_000,
		..NodeCapacity::default()
	};
	change(&mut capacity);
	let cluster = Cluster::start(
		ClusterConfig {
			id,
			data_dir: dir.path().join("cluster"),
			roles: Roles { node: true, driver: false },
			gossip_bind: "127.0.0.1:0".parse().unwrap(),
			advertise: None,
			cluster_port: listener.local_addr().unwrap().port(),
			join: vec![h.cluster.gossip.local_addr().to_string()],
			secret: SECRET.as_bytes().to_vec(),
			bootstrap_expect: 1,
			raft_reap_after: Duration::from_mins(10),
		},
		MemberMeta { version: "test".into(), node: Some(capacity), ..MemberMeta::default() },
	)
	.await
	.unwrap();
	let fake: Shared = Arc::default();
	let served = fake_routes(Arc::clone(&fake));
	tokio::spawn(async move { axum::serve(listener, served).await.unwrap() });
	h.fake
		.lock()
		.migration
		.peers
		.insert(id, Arc::downgrade(&fake));
	fake
		.lock()
		.migration
		.peers
		.insert(NODE, Arc::downgrade(&h.fake));
	eventually("the peer node in gossip", || async {
		h.state
			.node_members()
			.iter()
			.any(|(member, _)| *member == id)
	})
	.await;
	Peer { cluster, fake, _dir: dir }
}

/// A running sandbox with ten minutes left.
async fn create(h: &Harness) -> String {
	let (status, created, _) = h
		.api
		.call("POST", "/v2/sandboxes", Some(json!({"templateID": "my-tpl", "timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	created["sandboxID"].as_str().unwrap().to_owned()
}

async fn admin_migrate(h: &Harness, id: &str, body: Option<Value>) -> (StatusCode, Value) {
	let (status, body, _) = h
		.api
		.call("POST", &format!("/admin/sandboxes/{id}/migrate"), body)
		.await;
	(status, body)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn migrate_moves_a_running_sandbox_route_and_record_to_the_target() {
	let h = harness().await;
	let id = create(&h).await;
	let peer = join_peer(&h, PEER, |_| {}).await;
	let before = get_route(&h.cluster.kv, &id).await.unwrap().unwrap();
	assert_eq!(before.node_id, NODE);

	let (status, body) = admin_migrate(&h, &id, Some(json!({"nodeID": PEER.to_string()}))).await;
	assert_eq!(status, StatusCode::OK, "{body}");
	assert_eq!(
		body,
		json!({"sandboxID": id.as_str(), "nodeID": PEER.to_string(), "state": "running"})
	);

	let route = get_route(&h.cluster.kv, &id).await.unwrap().unwrap();
	assert_eq!((route.node_id, route.state), (PEER, RouteState::Running));
	assert_eq!((route.started_at_ms, route.end_at_ms), (before.started_at_ms, before.end_at_ms));
	let row = h.store.sandbox(&id).await.unwrap().unwrap();
	assert_eq!(row.state(), SandboxState::Running);
	assert_eq!(row.node_id, PEER.to_string());
	{
		let source = h.fake.lock();
		assert_eq!(source.migration.handovers, vec![id.clone()], "a handover pause");
		assert_eq!(source.migration.drops, vec![id.clone()], "the source dropped its copy");
		assert!(!source.sandboxes.contains_key(&id));
		assert_eq!(source.kills, Vec::<String>::new(), "nothing is killed");
	}
	{
		let target = peer.fake.lock();
		let (adopted, adopt) = &target.migration.adopts[0];
		assert_eq!(adopted, &id);
		assert_eq!(adopt.source, NODE);
		assert!(adopt.artifact_sources.contains(&NODE), "the build's holders");
		assert_eq!(adopt.meta.state, NodeState::Paused);
		assert_eq!(adopt.meta.end_at_ms, before.end_at_ms);
		assert_eq!(adopt.meta.envd_access_token, h.state.keys.envd_token(&id));
		let (resumed, resume) = &target.resumes[0];
		assert_eq!(resumed, &id);
		assert_eq!(resume.end_at_ms, before.end_at_ms);
		assert_eq!(resume.init.envd_access_token, h.state.keys.envd_token(&id));
		assert_eq!(resume.init.env_vars["FROM_TEMPLATE"], "1");
		assert!(resume.remote.is_none() && resume.meta.is_none(), "a plain local resume");
		assert_eq!(target.sandboxes[&id].state, NodeState::Running);
	}
	let (status, detail, _) = h.api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(detail["state"], "running");

	// Migrating to the node it is on changes nothing.
	let (status, body) = admin_migrate(&h, &id, Some(json!({"nodeID": PEER.to_string()}))).await;
	assert_eq!(status, StatusCode::OK, "{body}");
	assert_eq!(peer.fake.lock().migration.adopts.len(), 1);

	peer.cluster.shutdown().await;
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_adoption_resumes_the_sandbox_on_its_source() {
	let h = harness().await;
	let id = create(&h).await;
	let peer = join_peer(&h, PEER, |_| {}).await;
	let before = get_route(&h.cluster.kv, &id).await.unwrap().unwrap();
	peer.fake.lock().migration.fail_adopts = 1;

	let (status, body) = admin_migrate(&h, &id, None).await;
	assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");

	let route = get_route(&h.cluster.kv, &id).await.unwrap().unwrap();
	assert_eq!((route.node_id, route.state), (NODE, RouteState::Running));
	assert_eq!(route.end_at_ms, before.end_at_ms);
	let row = h.store.sandbox(&id).await.unwrap().unwrap();
	assert_eq!(row.state(), SandboxState::Running);
	assert_eq!(row.node_id, NODE.to_string());
	{
		let source = h.fake.lock();
		assert_eq!(source.migration.handovers, vec![id.clone()]);
		assert!(source.migration.drops.is_empty(), "the source keeps its state");
		let (resumed, resume) = &source.resumes[0];
		assert_eq!(resumed, &id);
		assert_eq!(resume.end_at_ms, before.end_at_ms);
		assert_eq!(source.sandboxes[&id].state, NodeState::Running);
	}
	{
		let target = peer.fake.lock();
		assert_eq!(target.migration.drops, vec![id.clone()], "the target cleans up");
		assert!(target.sandboxes.is_empty() && target.resumes.is_empty());
	}

	peer.cluster.shutdown().await;
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_nodes_are_evacuated_to_compatible_nodes_only() {
	let h = harness().await;
	let id = create(&h).await;
	// A ready node on another CPU baseline that holds the build: placement
	// that ignored the baseline would prefer it.
	let incompatible = join_peer(&h, PEER, |cap| cap.cpu_baseline = "v4".into()).await;
	let held = ArtifactInfo { node_id: PEER, ..artifact(&h.build_id) };
	h.cluster
		.kv
		.put(&art_key(&h.build_id, PEER), serde_json::to_vec(&held).unwrap(), None)
		.await
		.unwrap();
	let compatible = join_peer(&h, OTHER, |_| {}).await;

	let (status, body, _) = h
		.api
		.call("POST", &format!("/nodes/{NODE}"), Some(json!({"status": "draining"})))
		.await;
	assert!(status.is_success(), "{status} {body}");
	eventually("the draining node evacuated", || async {
		get_route(&h.cluster.kv, &id)
			.await
			.unwrap()
			.is_some_and(|route| route.node_id == OTHER && route.state == RouteState::Running)
	})
	.await;
	eventually("the source dropped its copy", || async {
		h.fake.lock().migration.drops == vec![id.clone()]
	})
	.await;
	let row = h.store.sandbox(&id).await.unwrap().unwrap();
	assert_eq!(row.node_id, OTHER.to_string());
	assert!(
		incompatible.fake.lock().migration.adopts.is_empty(),
		"never placed on another CPU baseline"
	);
	assert_eq!(compatible.fake.lock().migration.adopts.len(), 1);
	assert_eq!(compatible.fake.lock().sandboxes[&id].state, NodeState::Running);

	incompatible.cluster.shutdown().await;
	compatible.cluster.shutdown().await;
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_local_paused_sandboxes_move_and_stay_paused() {
	let h = harness().await;
	let id = create(&h).await;
	let (status, ..) = h
		.api
		.call("POST", &format!("/sandboxes/{id}/pause"), Some(json!({})))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	let peer = join_peer(&h, PEER, |_| {}).await;

	let (status, body) = admin_migrate(&h, &id, None).await;
	assert_eq!(status, StatusCode::OK, "{body}");
	assert_eq!(
		body,
		json!({"sandboxID": id.as_str(), "nodeID": PEER.to_string(), "state": "paused"})
	);
	assert!(get_route(&h.cluster.kv, &id).await.unwrap().is_none());
	let row = h.store.sandbox(&id).await.unwrap().unwrap();
	assert_eq!(row.state(), SandboxState::Paused);
	assert_eq!(row.node_id, PEER.to_string());
	{
		let source = h.fake.lock();
		assert_eq!(source.migration.drops, vec![id.clone()]);
		assert!(source.migration.handovers.is_empty(), "already paused");
		assert!(!source.sandboxes.contains_key(&id));
	}
	{
		let target = peer.fake.lock();
		let (adopted, adopt) = &target.migration.adopts[0];
		assert_eq!((adopted, adopt.source), (&id, NODE));
		assert_eq!(adopt.meta.state, NodeState::Paused);
		assert!(target.resumes.is_empty(), "it stays paused");
		assert_eq!(target.sandboxes[&id].state, NodeState::Paused);
	}

	// Its new holder resumes it.
	let (status, resumed, _) = h
		.api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{resumed}");
	let route = get_route(&h.cluster.kv, &id).await.unwrap().unwrap();
	assert_eq!(route.node_id, PEER);
	assert_eq!(peer.fake.lock().resumes.len(), 1);

	peer.cluster.shutdown().await;
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_migration_is_recovered_on_its_node_instead_of_killed() {
	let h = harness().await;
	let running = create(&h).await;
	let paused = create(&h).await;
	// Drivers that died mid-migration: one before the source paused, one after.
	for id in [&running, &paused] {
		let mut route = get_route(&h.cluster.kv, id).await.unwrap().unwrap();
		route.state = RouteState::Migrating;
		put_route(&h.cluster.kv, id, &route).await.unwrap();
	}
	h.fake.lock().sandboxes.get_mut(&paused).unwrap().state = NodeState::Paused;

	eventually("both migrations recovered", || async {
		let mut recovered = true;
		for id in [&running, &paused] {
			recovered &= get_route(&h.cluster.kv, id)
				.await
				.unwrap()
				.is_some_and(|route| route.node_id == NODE && route.state == RouteState::Running);
		}
		recovered
	})
	.await;
	{
		let fake = h.fake.lock();
		assert!(fake.kills.is_empty(), "nothing was killed as abandoned");
		assert_eq!(fake.resumes.len(), 1);
		assert_eq!(fake.resumes[0].0, paused, "the handover pause resumed in place");
		assert_eq!(fake.sandboxes[&paused].state, NodeState::Running);
		assert_eq!(fake.sandboxes[&running].state, NodeState::Running);
	}
	for id in [&running, &paused] {
		let row = h.store.sandbox(id).await.unwrap().unwrap();
		assert_eq!(row.state(), SandboxState::Running);
	}

	h.finish().await;
}
