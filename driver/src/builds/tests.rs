//! Build supervision end to end: a real single-driver [`Cluster`], a
//! node-only gossip member whose cluster URL is an in-process fake node
//! serving `/node/v1/builds/*`, with the store on the driver's raft KV.

use std::{collections::BTreeMap, future::Future, path::Path, sync::Arc, time::Duration};

use axum::{
	Json, Router,
	extract::{Path as UrlPath, Query, State},
	http::StatusCode,
	routing::{get, post},
};
use parking_lot::Mutex;
use tokio::net::TcpListener;
use vmon_cluster::{
	Cluster, ClusterConfig, MemberMeta, NodeCapacity, NodeId, Roles, gossip::MemberState,
};
use vmon_node::api::{
	ArtifactInfo, BuildLogEntry, BuildReason, BuildState, BuildStatus as NodeBuildStatus, StartBuild,
};

use super::{
	BuildRequest, StartRequest, art_key, lock_key, register_build, start_build, supervisor,
};
use crate::{
	crypto::Keys,
	state::{AppState, DriverConfig, TeamLimits},
	store::{Store, Team, builds::BuildStatus},
};

const SECRET: &str = "test-secret";
const WAIT: Duration = Duration::from_secs(30);
const NODE_ID: NodeId = NodeId(0xb0b);

/// Scripted builder: per build, its log lines, state, and outcome.
#[derive(Default)]
struct FakeNode {
	started:      Vec<StartBuild>,
	builds:       BTreeMap<String, Script>,
	/// How long each status request takes (a stalled builder or host).
	status_delay: Duration,
}

struct Script {
	state:    BuildState,
	logs:     Vec<BuildLogEntry>,
	reason:   Option<BuildReason>,
	artifact: Option<ArtifactInfo>,
	/// Status requests answered so far.
	polls:    u64,
}

type Shared = Arc<Mutex<FakeNode>>;

async fn fake_start(State(node): State<Shared>, Json(req): Json<StartBuild>) -> StatusCode {
	let mut node = node.lock();
	node.builds.insert(req.build_id.clone(), Script {
		state:    BuildState::Building,
		logs:     Vec::new(),
		reason:   None,
		artifact: None,
		polls:    0,
	});
	node.started.push(req);
	StatusCode::ACCEPTED
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusQuery {
	logs_offset: u64,
}

async fn fake_status(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Query(query): Query<StatusQuery>,
) -> Result<Json<NodeBuildStatus>, StatusCode> {
	let delay = node.lock().status_delay;
	tokio::time::sleep(delay).await;
	let mut node = node.lock();
	let script = node.builds.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
	script.polls += 1;
	// Pages of at most 2 lines exercise the supervisor's drain loop.
	let from = usize::try_from(query.logs_offset)
		.unwrap()
		.min(script.logs.len());
	let to = (from + 2).min(script.logs.len());
	Ok(Json(NodeBuildStatus {
		build_id:   id,
		status:     script.state,
		logs:       script.logs[from..to].to_vec(),
		logs_total: script.logs.len() as u64,
		reason:     script.reason.clone(),
		artifact:   script.artifact.clone(),
	}))
}

fn line(step: &str, message: &str) -> BuildLogEntry {
	BuildLogEntry {
		timestamp_ms: 1_767_323_045_000,
		level:        "info".into(),
		step:         Some(step.into()),
		message:      message.into(),
	}
}

fn artifact(build_id: &str) -> ArtifactInfo {
	ArtifactInfo {
		build_id:        build_id.into(),
		node_id:         NODE_ID,
		arch:            "x86_64".into(),
		backend:         "kvm".into(),
		cpu_baseline:    "x86-64-v3".into(),
		has_snapshot:    true,
		bytes:           123,
		envd_version:    "0.6.4".into(),
		vcpus:           2,
		memory_mb:       1024,
		disk_mb:         4096,
		default_user:    "user".into(),
		default_workdir: Some("/home/user".into()),
		env:             BTreeMap::new(),
	}
}

fn capacity() -> NodeCapacity {
	NodeCapacity {
		arch:           "x86_64".into(),
		backend:        "kvm".into(),
		os:             "linux".into(),
		cpu_baseline:   "x86-64-v3".into(),
		cpus:           8,
		memory_mb:      16384,
		disk_free_mb:   100_000,
		vcpus_used:     0,
		memory_used_mb: 0,
		sandboxes:      0,
		builds:         0,
		draining:       false,
		features:       vec![],
		cached_builds:  vec![],
	}
}

async fn start_cluster(
	id: NodeId,
	dir: &Path,
	roles: Roles,
	listener: &TcpListener,
	join: Vec<String>,
) -> Cluster {
	let cfg = ClusterConfig {
		id,
		data_dir: dir.join(id.to_string()),
		roles,
		gossip_bind: "127.0.0.1:0".parse().unwrap(),
		gossip_advertise: "127.0.0.1:0".parse().unwrap(),
		cluster_url: format!("http://{}", listener.local_addr().unwrap()),
		join,
		secret: SECRET.as_bytes().to_vec(),
		bootstrap_expect: 1,
		raft_reap_after: Duration::from_mins(10),
	};
	let meta = MemberMeta {
		version: "test".into(),
		node: roles.node.then(capacity),
		..MemberMeta::default()
	};
	Cluster::start(cfg, meta).await.unwrap()
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
	F: FnMut() -> Fut,
	Fut: Future<Output = bool>,
{
	let deadline = tokio::time::Instant::now() + WAIT;
	while !check().await {
		assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
}

/// A driver (raft leader) and a node-only gossip member served by `fake`,
/// with one team.
struct Fixture {
	store:  Store,
	_dir:   tempfile::TempDir,
	driver: Cluster,
	node:   Cluster,
	fake:   Shared,
	state:  AppState,
	team:   Team,
}

async fn fixture() -> Fixture {
	let dir = tempfile::tempdir().unwrap();

	let driver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let driver = start_cluster(
		NodeId(0xd1),
		dir.path(),
		Roles { node: false, driver: true },
		&driver_listener,
		vec![],
	)
	.await;
	let store = Store::new(driver.kv.clone());
	let driver_router = driver.router();
	tokio::spawn(async move { axum::serve(driver_listener, driver_router).await.unwrap() });

	let fake: Shared = Arc::default();
	let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let node = start_cluster(
		NODE_ID,
		dir.path(),
		Roles { node: true, driver: false },
		&node_listener,
		vec![driver.gossip.local_addr().to_string()],
	)
	.await;
	let node_router = Router::new()
		.route("/node/v1/builds", post(fake_start))
		.route("/node/v1/builds/{id}", get(fake_status))
		.with_state(Arc::clone(&fake));
	tokio::spawn(async move { axum::serve(node_listener, node_router).await.unwrap() });

	let config = DriverConfig {
		domain:         "e2b.test".into(),
		public_url:     "http://127.0.0.1:9".into(),
		admin_token:    None,
		cluster_secret: SECRET.into(),
		data_dir:       dir.path().to_path_buf(),
		default_limits: TeamLimits::default(),
		shared_volumes: false,
		object_store:   None,
	};
	let state =
		AppState::new(config, store.clone(), driver.clone(), Keys::derive(SECRET.as_bytes()));
	eventually("raft leadership", || async { driver.is_leader() }).await;
	eventually("the builder node in gossip", || async {
		state
			.node_members()
			.iter()
			.any(|(id, meta)| *id == NODE_ID && meta.node.is_some())
	})
	.await;
	let team = store
		.create_team("Acme", TeamLimits::default())
		.await
		.unwrap();
	Fixture { store, _dir: dir, driver, node, fake, state, team }
}

impl Fixture {
	/// Registers `name` and starts it on the fake builder; returns the build ID.
	async fn start(&self, name: &str) -> String {
		let request = BuildRequest { name: Some(name.into()), ..BuildRequest::default() };
		let registered = register_build(&self.state, &self.team, &request)
			.await
			.unwrap();
		let start =
			StartRequest { from_image: Some("ubuntu:24.04".into()), ..StartRequest::default() };
		start_build(
			&self.state,
			&self.team,
			&registered.template_id,
			&registered.build_id,
			start,
			None,
		)
		.await
		.unwrap();
		registered.build_id
	}

	async fn status(&self, build_id: &str) -> BuildStatus {
		self.store.build(build_id).await.unwrap().unwrap().status()
	}

	/// The holder of `lock/build/<id>`, if any.
	async fn lease_holder(&self, build_id: &str) -> Option<String> {
		self
			.driver
			.kv
			.get(&lock_key(build_id))
			.await
			.unwrap()
			.map(|entry| String::from_utf8(entry.value).unwrap())
	}

	fn polls(&self, build_id: &str) -> u64 {
		self.fake.lock().builds[build_id].polls
	}

	fn finish_ready(&self, build_id: &str) {
		let mut fake = self.fake.lock();
		let script = fake.builds.get_mut(build_id).unwrap();
		script.state = BuildState::Ready;
		script.artifact = Some(artifact(build_id));
	}

	async fn shutdown(self) {
		self.driver.shutdown().await;
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn supervisor_drives_builds_to_ready_and_fails_them_when_the_builder_dies() {
	let Fixture { store, _dir, driver, node, fake, state, team } = fixture().await;
	tokio::spawn(supervisor::run(state.clone(), supervisor::Timing {
		scan:           Duration::from_millis(100),
		poll:           Duration::from_millis(50),
		lock_ttl:       Duration::from_secs(5),
		renew:          Duration::from_secs(1),
		housekeeping:   Duration::from_hours(1),
		snapshot_stale: Duration::from_hours(1),
	}));

	// waiting → building: register and start on the (only) builder.
	let request = BuildRequest { name: Some("my-tpl:v1".into()), ..BuildRequest::default() };
	let registered = register_build(&state, &team, &request).await.unwrap();
	let build_id = registered.build_id.clone();
	let waiting = store.build(&build_id).await.unwrap().unwrap();
	assert_eq!(waiting.status(), BuildStatus::Waiting);
	let start = StartRequest { from_image: Some("e2bdev/base".into()), ..StartRequest::default() };
	start_build(
		&state,
		&team,
		&registered.template_id,
		&build_id,
		start,
		Some("e2b-python-sdk/2.2.0"),
	)
	.await
	.unwrap();
	{
		let fake = fake.lock();
		assert_eq!(fake.started.len(), 1);
		assert_eq!(fake.started[0].template_version, vmon_node::api::TemplateVersion::V2_0);
	}
	let building = store.build(&build_id).await.unwrap().unwrap();
	assert_eq!(building.status(), BuildStatus::Building);
	assert_eq!(building.node_id.as_deref(), Some(NODE_ID.to_string().as_str()));

	fake.lock().builds.get_mut(&build_id).unwrap().logs = vec![
		line("base", "[base] FROM e2bdev/base"),
		line("base", "pulling"),
		line("1", "[builder 1/1] RUN true"),
	];
	eventually("three persisted log lines", || async {
		store.build_log_count(&build_id).await.unwrap() == 3
	})
	.await;
	assert_eq!(store.build(&build_id).await.unwrap().unwrap().status(), BuildStatus::Building);

	// building → ready: two more lines plus the artifact in the same report.
	{
		let mut fake = fake.lock();
		let script = fake.builds.get_mut(&build_id).unwrap();
		script
			.logs
			.push(line("finalize", "[finalize] Finalizing template build"));
		script
			.logs
			.push(line("finalize", "Build finished, took 1s"));
		script.state = BuildState::Ready;
		script.artifact = Some(artifact(&build_id));
	}
	eventually("ready build", || async {
		store.build(&build_id).await.unwrap().unwrap().status() == BuildStatus::Ready
	})
	.await;
	let ready = store.build(&build_id).await.unwrap().unwrap();
	assert_eq!(ready.envd_version.as_deref(), Some("0.6.4"));
	assert_eq!(ready.artifact.as_ref().unwrap(), &artifact(&build_id));
	let logs = store
		.build_logs_page(&build_id, crate::store::builds::LogLevel::Debug, 0, 100)
		.await
		.unwrap();
	let messages: Vec<_> = logs.iter().map(|l| l.message.as_str()).collect();
	assert_eq!(messages, [
		"[base] FROM e2bdev/base",
		"pulling",
		"[builder 1/1] RUN true",
		"[finalize] Finalizing template build",
		"Build finished, took 1s"
	]);
	assert_eq!(
		store
			.tag_build(&registered.template_id, "v1")
			.await
			.unwrap()
			.as_deref(),
		Some(build_id.as_str())
	);
	let record = driver
		.kv
		.get(&art_key(&build_id, NODE_ID))
		.await
		.unwrap()
		.expect("artifact record in KV");
	assert_eq!(serde_json::from_slice::<ArtifactInfo>(&record.value).unwrap(), artifact(&build_id));

	// A second build whose builder leaves the cluster fails.
	let request = BuildRequest { name: Some("other".into()), ..BuildRequest::default() };
	let second = register_build(&state, &team, &request).await.unwrap();
	let start = StartRequest { from_image: Some("ubuntu:24.04".into()), ..StartRequest::default() };
	start_build(&state, &team, &second.template_id, &second.build_id, start, None)
		.await
		.unwrap();
	node.shutdown().await;
	eventually("the builder leaving gossip", || async {
		driver
			.members()
			.iter()
			.any(|(m, _)| m.id == NODE_ID && m.state != MemberState::Alive)
	})
	.await;
	eventually("failed build", || async {
		store
			.build(&second.build_id)
			.await
			.unwrap()
			.unwrap()
			.status()
			== BuildStatus::Error
	})
	.await;
	let failed = store.build(&second.build_id).await.unwrap().unwrap();
	assert!(
		failed
			.reason
			.unwrap()
			.message
			.contains("no longer available")
	);

	driver.shutdown().await;
}

/// Regression (I/O stalls on a single driver): the lease was only renewed
/// between polls, so a poll stuck longer than the TTL let it expire, and the
/// expired lease was then taken for another driver's; supervision stopped
/// and the build sat in `building` until a rescan. Renewal must not depend
/// on poll latency, a lapsed lease must be re-taken, and only a lease held
/// by someone else stops supervision.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_build_lease_survives_slow_polls_and_lapses_and_yields_only_to_another_holder() {
	let f = fixture().await;
	let me = f.driver.local_id().to_string();
	let slow = f.start("slow").await;
	let taken = f.start("taken").await;
	// One scan only: supervision that stops is never restarted.
	tokio::spawn(supervisor::run(f.state.clone(), supervisor::Timing {
		scan:           Duration::from_hours(1),
		poll:           Duration::from_millis(50),
		lock_ttl:       Duration::from_secs(1),
		renew:          Duration::from_millis(200),
		housekeeping:   Duration::from_hours(1),
		snapshot_stale: Duration::from_hours(1),
	}));
	eventually("both builds polled", || async { f.polls(&slow) > 0 && f.polls(&taken) > 0 }).await;

	// Polls stall for 3 lease TTLs: the lease is renewed meanwhile.
	f.fake.lock().status_delay = Duration::from_secs(3);
	tokio::time::sleep(Duration::from_millis(2500)).await;
	assert_eq!(f.lease_holder(&slow).await.as_deref(), Some(me.as_str()), "kept during slow polls");
	f.fake.lock().status_delay = Duration::ZERO;

	// A lease that lapsed (e.g. its renewal applied after the TTL) is re-taken.
	f.driver.kv.delete(&lock_key(&slow)).await.unwrap();
	eventually("the lapsed lease re-taken", || async {
		f.lease_holder(&slow).await.as_deref() == Some(me.as_str())
	})
	.await;

	// Another holder's lease stops supervision of that build.
	f.driver
		.kv
		.put(&lock_key(&taken), b"another-driver".to_vec(), None)
		.await
		.unwrap();
	tokio::time::sleep(Duration::from_millis(500)).await;
	let polls = f.polls(&taken);
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert_eq!(f.polls(&taken), polls, "no polls after a takeover");
	assert_eq!(f.lease_holder(&taken).await.as_deref(), Some("another-driver"));

	// The build whose lease survived is still supervised to completion.
	f.finish_ready(&slow);
	eventually("ready build", || async { f.status(&slow).await == BuildStatus::Ready }).await;
	f.finish_ready(&taken);
	tokio::time::sleep(Duration::from_millis(500)).await;
	assert_eq!(f.status(&taken).await, BuildStatus::Building, "left to the other holder");
	f.shutdown().await;
}

/// Regression (`--max-builds` queue on a busy builder): the hour limit
/// counted from dispatch, so builds queued on the node for most of the hour
/// timed out with the upstream "polling timed out" error before (or soon
/// after) they ran. The clock starts when the builder reports `building`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_queued_on_the_builder_does_not_count_toward_the_build_timeout() {
	let f = fixture().await;
	let queued = f.start("queued").await;
	f.fake.lock().builds.get_mut(&queued).unwrap().state = BuildState::Waiting;
	tokio::spawn(supervisor::run(f.state.clone(), supervisor::Timing {
		scan:           Duration::from_millis(100),
		poll:           Duration::from_millis(50),
		lock_ttl:       Duration::from_secs(5),
		renew:          Duration::from_secs(1),
		housekeeping:   Duration::from_hours(1),
		snapshot_stale: Duration::from_hours(1),
	}));
	eventually("queued build polled", || async { f.polls(&queued) > 2 }).await;
	let row = f.store.build(&queued).await.unwrap().unwrap();
	assert_eq!(row.status(), BuildStatus::Building);
	assert_eq!(row.started_at, None, "still queued on the builder");

	f.fake.lock().builds.get_mut(&queued).unwrap().state = BuildState::Building;
	eventually("the start recorded", || async {
		f.store
			.build(&queued)
			.await
			.unwrap()
			.unwrap()
			.started_at
			.is_some()
	})
	.await;
	f.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_that_fetched_a_build_is_recorded_as_its_holder() {
	let dir = tempfile::tempdir().unwrap();
	let driver_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let driver = start_cluster(
		NodeId(0xd1),
		dir.path(),
		Roles { node: false, driver: true },
		&driver_listener,
		vec![],
	)
	.await;
	let store = Store::new(driver.kv.clone());
	let driver_router = driver.router();
	tokio::spawn(async move { axum::serve(driver_listener, driver_router).await.unwrap() });
	// The fake node serves the manifest of its peer-fetched copy.
	let node_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let node_url = format!("http://{}", node_listener.local_addr().unwrap());
	let node_router = Router::new().route(
		"/node/v1/artifacts/{id}/manifest",
		get(|UrlPath(id): UrlPath<String>| async move {
			Json(vmon_node::api::ArtifactManifest { info: artifact(&id), files: vec![] })
		}),
	);
	tokio::spawn(async move { axum::serve(node_listener, node_router).await.unwrap() });
	let config = DriverConfig {
		domain:         "e2b.test".into(),
		public_url:     "http://127.0.0.1:9".into(),
		admin_token:    None,
		cluster_secret: SECRET.into(),
		data_dir:       dir.path().to_path_buf(),
		default_limits: TeamLimits::default(),
		shared_volumes: false,
		object_store:   None,
	};
	let state =
		AppState::new(config, store.clone(), driver.clone(), Keys::derive(SECRET.as_bytes()));
	eventually("raft leadership", || async { driver.is_leader() }).await;

	let build_id = uuid::Uuid::new_v4().to_string();
	super::record_holder(&state, NODE_ID, &node_url, &build_id)
		.await
		.unwrap();
	let record = driver
		.kv
		.get(&art_key(&build_id, NODE_ID))
		.await
		.unwrap()
		.expect("holder recorded");
	assert_eq!(serde_json::from_slice::<ArtifactInfo>(&record.value).unwrap(), artifact(&build_id));
	// A manifest describing another node is not recorded under this one.
	assert!(
		super::record_holder(&state, NodeId(0xbad), &node_url, &build_id)
			.await
			.is_err()
	);
	assert!(
		driver
			.kv
			.get(&art_key(&build_id, NodeId(0xbad)))
			.await
			.unwrap()
			.is_none()
	);

	driver.shutdown().await;
}
