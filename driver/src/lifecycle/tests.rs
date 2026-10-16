//! Sandbox lifecycle end to end through the HTTP API: a real single-process
//! [`Cluster`] (driver + node roles) whose cluster URL also serves an
//! in-process fake node (`/node/v1/sandboxes*`), and `MySQL` (gated on
//! `VMON_MYSQL_TEST_URL`).

use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use axum::{
	Json, Router,
	body::Body,
	extract::{Path as UrlPath, State},
	http::{Request, StatusCode},
	response::{IntoResponse, Response},
	routing::post,
};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;
use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeCapacity, NodeId, Roles};
use vmon_node::api::{
	ArtifactInfo, CreateSandbox, Deadline, ErrorBody, PauseSandbox, ResumeSandbox, RouteState,
	SandboxInfo, SandboxState as NodeState, SnapshotSandbox,
};

mod portable;
mod s3;
mod spawns;

use super::routes::{Lease, get_route, hold_lease, list_routes, lock_key, team_prefix};
use crate::{
	api,
	builds::{self, art_key},
	crypto::Keys,
	state::{AppState, DriverConfig, TeamLimits},
	store::{
		EventQuery, Store, Team,
		templates::{RegisterBuild, RegisterOutcome},
		testdb::TestDb,
	},
};

const SECRET: &str = "lifecycle-secret";
const ADMIN: &str = "admin-token";
const NODE: NodeId = NodeId(0x00d1);
const WAIT: Duration = Duration::from_secs(30);

/// Calls the fake node received, plus its live sandbox table and injected
/// failures.
#[derive(Default)]
struct FakeNode {
	sandboxes:         BTreeMap<String, SandboxInfo>,
	creates:           Vec<CreateSandbox>,
	pauses:            Vec<(String, bool)>,
	resumes:           Vec<(String, ResumeSandbox)>,
	kills:             Vec<String>,
	deadlines:         Vec<(String, u64)>,
	/// Build IDs captured by `POST /sandboxes/{id}/snapshot`.
	snapshots:         Vec<String>,
	/// Build IDs whose artifacts were deleted (`DELETE /artifacts/{id}`).
	deleted_artifacts: Vec<String>,
	/// The next this many creates fail like a node out of disk.
	fail_creates:      u32,
	/// The next this many snapshots fail like a node out of disk.
	fail_snapshots:    u32,
	/// How long a create takes (a booting VM).
	create_delay:      Duration,
	/// The next resume completes but answers 500 (as if the reply was lost).
	resume_then_fail:  bool,
}

/// The node's 500 for a VM that could not start or be captured.
fn disk_full(what: &str) -> Response {
	let body = ErrorBody {
		code:    "internal".into(),
		message: format!("{what}: No space left on device (os error 28)"),
	};
	(StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response()
}

type Shared = Arc<Mutex<FakeNode>>;

fn info(req: &CreateSandbox) -> SandboxInfo {
	SandboxInfo {
		sandbox_id:    req.sandbox_id.clone(),
		team_id:       req.team_id.clone(),
		template_id:   req.template_id.clone(),
		build_id:      req.build_id.clone(),
		state:         NodeState::Running,
		vcpus:         req.vcpus,
		memory_mb:     req.memory_mb,
		disk_mb:       req.disk_mb,
		started_at_ms: 0,
		end_at_ms:     req.end_at_ms,
		envd_version:  "0.6.4".into(),
	}
}

async fn fake_create(State(node): State<Shared>, Json(req): Json<CreateSandbox>) -> Response {
	let delay = node.lock().create_delay;
	tokio::time::sleep(delay).await;
	let mut node = node.lock();
	if node.fail_creates > 0 {
		node.fail_creates -= 1;
		return disk_full(&format!("vm {} failed to start", req.sandbox_id));
	}
	let info = info(&req);
	node.sandboxes.insert(req.sandbox_id.clone(), info.clone());
	node.creates.push(req);
	(StatusCode::CREATED, Json(info)).into_response()
}

async fn fake_snapshot(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Json(req): Json<SnapshotSandbox>,
) -> Response {
	let mut node = node.lock();
	if !node.sandboxes.contains_key(&id) {
		return StatusCode::NOT_FOUND.into_response();
	}
	if node.fail_snapshots > 0 {
		node.fail_snapshots -= 1;
		return disk_full(&format!("capturing sandbox {id}"));
	}
	node.snapshots.push(req.build_id.clone());
	Json(artifact(&req.build_id)).into_response()
}

async fn fake_delete_artifact(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
) -> StatusCode {
	node.lock().deleted_artifacts.push(id);
	StatusCode::NO_CONTENT
}

async fn fake_list(State(node): State<Shared>) -> Json<Vec<SandboxInfo>> {
	Json(node.lock().sandboxes.values().cloned().collect())
}

async fn fake_kill(State(node): State<Shared>, UrlPath(id): UrlPath<String>) -> StatusCode {
	let mut node = node.lock();
	node.kills.push(id.clone());
	if node.sandboxes.remove(&id).is_some() {
		StatusCode::NO_CONTENT
	} else {
		StatusCode::NOT_FOUND
	}
}

async fn fake_pause(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Json(req): Json<PauseSandbox>,
) -> Result<Json<SandboxInfo>, StatusCode> {
	let mut node = node.lock();
	node.pauses.push((id.clone(), req.memory));
	let info = node.sandboxes.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
	info.state = NodeState::Paused;
	Ok(Json(info.clone()))
}

async fn fake_resume(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Json(req): Json<ResumeSandbox>,
) -> Result<Json<SandboxInfo>, StatusCode> {
	let mut node = node.lock();
	// A node adopting a pause from the object store learns it from the request.
	if !node.sandboxes.contains_key(&id) && req.remote.is_some() {
		let meta = req.meta.as_ref().ok_or(StatusCode::BAD_REQUEST)?;
		if meta.sandbox_id != id {
			return Err(StatusCode::BAD_REQUEST);
		}
		node.sandboxes.insert(id.clone(), meta.info());
	}
	let info = node.sandboxes.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
	if info.state != NodeState::Paused {
		return Err(StatusCode::CONFLICT);
	}
	info.state = NodeState::Running;
	info.end_at_ms = req.end_at_ms;
	let info = info.clone();
	node.resumes.push((id, req));
	if std::mem::take(&mut node.resume_then_fail) {
		return Err(StatusCode::INTERNAL_SERVER_ERROR);
	}
	Ok(Json(info))
}

async fn fake_get(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
) -> Result<Json<SandboxInfo>, StatusCode> {
	node
		.lock()
		.sandboxes
		.get(&id)
		.cloned()
		.map(Json)
		.ok_or(StatusCode::NOT_FOUND)
}

async fn fake_deadline(
	State(node): State<Shared>,
	UrlPath(id): UrlPath<String>,
	Json(req): Json<Deadline>,
) -> StatusCode {
	let mut node = node.lock();
	node.deadlines.push((id.clone(), req.end_at_ms));
	if let Some(info) = node.sandboxes.get_mut(&id) {
		info.end_at_ms = req.end_at_ms;
	}
	StatusCode::NO_CONTENT
}

fn fake_routes(node: Shared) -> Router {
	Router::new()
		.route("/node/v1/sandboxes", post(fake_create).get(fake_list))
		.route("/node/v1/sandboxes/{id}", axum::routing::delete(fake_kill).get(fake_get))
		.route("/node/v1/sandboxes/{id}/pause", post(fake_pause))
		.route("/node/v1/sandboxes/{id}/resume", post(fake_resume))
		.route("/node/v1/sandboxes/{id}/deadline", post(fake_deadline))
		.route("/node/v1/sandboxes/{id}/snapshot", post(fake_snapshot))
		.route("/node/v1/artifacts/{id}", axum::routing::delete(fake_delete_artifact))
		.with_state(node)
}

fn artifact(build_id: &str) -> ArtifactInfo {
	ArtifactInfo {
		build_id:        build_id.into(),
		node_id:         NODE,
		arch:            "x86_64".into(),
		backend:         "kvm".into(),
		cpu_baseline:    "v3".into(),
		has_snapshot:    true,
		bytes:           1,
		envd_version:    "0.6.4".into(),
		vcpus:           2,
		memory_mb:       512,
		disk_mb:         2048,
		default_user:    "user".into(),
		default_workdir: Some("/home/user".into()),
		env:             BTreeMap::from([
			("FROM_TEMPLATE".into(), "1".into()),
			("SHARED".into(), "template".into()),
		]),
	}
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
	F: FnMut() -> Fut,
	Fut: Future<Output = bool>,
{
	let deadline = tokio::time::Instant::now() + WAIT;
	while !check().await {
		assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

struct Api {
	router:  Router,
	team_id: String,
}

impl Api {
	async fn call(
		&self,
		method: &str,
		uri: &str,
		body: Option<Value>,
	) -> (StatusCode, Value, axum::http::HeaderMap) {
		let headers = [("x-admin-token", ADMIN), ("x-team-id", self.team_id.as_str())];
		self.call_with_headers(method, uri, &headers, body).await
	}

	/// [`Self::call`] with exactly `headers` as credentials.
	async fn call_with_headers(
		&self,
		method: &str,
		uri: &str,
		headers: &[(&str, &str)],
		body: Option<Value>,
	) -> (StatusCode, Value, axum::http::HeaderMap) {
		let mut request = Request::builder()
			.method(method)
			.uri(uri)
			.header("content-type", "application/json");
		for (name, value) in headers {
			request = request.header(*name, *value);
		}
		let request = request
			.body(body.map_or_else(Body::empty, |b| Body::from(b.to_string())))
			.unwrap();
		let response = self.router.clone().oneshot(request).await.unwrap();
		let status = response.status();
		let headers = response.headers().clone();
		let bytes = response.into_body().collect().await.unwrap().to_bytes();
		let json = if bytes.is_empty() {
			Value::Null
		} else {
			serde_json::from_slice(&bytes).unwrap()
		};
		(status, json, headers)
	}
}

/// A driver whose cluster URL also serves the fake node, a team limited to
/// two concurrent sandboxes, and its ready template `my-tpl`.
struct Harness {
	db:          TestDb,
	_dir:        tempfile::TempDir,
	cluster:     Cluster,
	fake:        Shared,
	state:       AppState,
	api:         Api,
	team:        Team,
	build_id:    String,
	template_id: String,
}

/// `None` (test skipped) without `VMON_MYSQL_TEST_URL`.
async fn harness() -> Option<Harness> {
	let db = TestDb::create().await?;
	let dir = tempfile::tempdir().unwrap();

	// One process serving both roles; its cluster URL also hosts the fake node
	// API.
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let cluster_url = format!("http://{}", listener.local_addr().unwrap());
	let cluster = Cluster::start(
		ClusterConfig {
			id: NODE,
			data_dir: dir.path().join("cluster"),
			roles: Roles { node: true, driver: true },
			gossip_bind: "127.0.0.1:0".parse().unwrap(),
			gossip_advertise: "127.0.0.1:0".parse().unwrap(),
			cluster_url,
			join: vec![],
			secret: SECRET.as_bytes().to_vec(),
			bootstrap_expect: 1,
			raft_reap_after: Duration::from_mins(10),
		},
		MemberMeta {
			version: "test".into(),
			node: Some(NodeCapacity {
				arch: "x86_64".into(),
				backend: "kvm".into(),
				os: "linux".into(),
				cpu_baseline: "v3".into(),
				cpus: 8,
				memory_mb: 16_384,
				disk_free_mb: 100_000,
				..NodeCapacity::default()
			}),
			..MemberMeta::default()
		},
	)
	.await
	.unwrap();
	let fake: Shared = Arc::default();
	let served = cluster.router().merge(fake_routes(Arc::clone(&fake)));
	tokio::spawn(async move { axum::serve(listener, served).await.unwrap() });

	let config = DriverConfig {
		domain:         "e2b.test".into(),
		public_url:     "http://127.0.0.1:9".into(),
		admin_token:    Some(ADMIN.into()),
		cluster_secret: SECRET.into(),
		data_dir:       dir.path().to_path_buf(),
		default_limits: TeamLimits::default(),
		shared_volumes: false,
		object_store:   None,
	};
	let state =
		AppState::new(config, db.store.clone(), cluster.clone(), Keys::derive(SECRET.as_bytes()));
	eventually("raft leadership", || async { cluster.is_leader() }).await;
	eventually("the node in gossip", || async {
		state.node_members().iter().any(|(id, _)| *id == NODE)
	})
	.await;
	super::spawn_background(state.clone());

	// A team limited to two concurrent sandboxes, with one ready template.
	let limits = TeamLimits { max_concurrent_sandboxes: 2, ..TeamLimits::default() };
	let team = db.store.create_team("Acme", limits).await.unwrap();
	let build_id = uuid::Uuid::new_v4().to_string();
	let registered = db
		.store
		.register_build(&RegisterBuild {
			team_id:               &team.id,
			namespace:             &team.slug,
			alias:                 "my-tpl",
			new_template_id:       "tpl0000000000000000a",
			build_id:              &build_id,
			tags:                  &["default".to_owned()],
			vcpus:                 2,
			memory_mb:             512,
			disk_mb:               2048,
			min_free_disk_mb:      None,
			max_concurrent_builds: 10,
		})
		.await
		.unwrap();
	let RegisterOutcome::Registered { template_id, .. } = registered else {
		panic!("{registered:?}")
	};
	assert!(
		db.store
			.finish_build_ready(&build_id, &artifact(&build_id))
			.await
			.unwrap()
	);
	cluster
		.kv
		.put(&art_key(&build_id, NODE), serde_json::to_vec(&artifact(&build_id)).unwrap(), None)
		.await
		.unwrap();

	let api = Api { router: api::router(state.clone()), team_id: team.id.clone() };
	Some(Harness { db, _dir: dir, cluster, fake, state, api, team, build_id, template_id })
}

impl Harness {
	async fn finish(self) {
		self.cluster.shutdown().await;
		self.db.drop_db().await;
	}

	/// Admission index entries (`team/<team>/sbx/*`) of the team.
	async fn admitted(&self) -> usize {
		self
			.cluster
			.kv
			.list(&team_prefix(&self.team.id))
			.await
			.unwrap()
			.len()
	}

	async fn create(&self) -> (StatusCode, Value) {
		let (status, body, _) = self
			.api
			.call("POST", "/v2/sandboxes", Some(json!({"templateID": "my-tpl"})))
			.await;
		(status, body)
	}

	async fn kill(&self, id: &str) {
		let (status, ..) = self
			.api
			.call("DELETE", &format!("/sandboxes/{id}"), None)
			.await;
		assert_eq!(status, StatusCode::NO_CONTENT, "kill {id}");
	}
}

/// `(id, retired)` of every hidden fork template, oldest first.
async fn fork_templates(store: &Store) -> Vec<(String, bool)> {
	let rows: Vec<(String, i64)> = sqlx::query_as(
		"SELECT id, CAST(deleted_at IS NOT NULL AS SIGNED) FROM templates WHERE kind = 'fork' ORDER \
		 BY created_at, id",
	)
	.fetch_all(store.pool())
	.await
	.unwrap();
	rows
		.into_iter()
		.map(|(id, retired)| (id, retired == 1))
		.collect()
}

/// Rows of snapshot builds (fork builds included) still in `MySQL`.
async fn snapshot_builds(store: &Store) -> i64 {
	sqlx::query_scalar("SELECT COUNT(*) FROM builds WHERE kind = 'snapshot'")
		.fetch_one(store.pool())
		.await
		.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_timeout_autopause_connect_kill_and_admission_limit() {
	let Some(h) = harness().await else {
		return;
	};
	let Harness { db, cluster, fake, state, api, team, template_id, .. } = &h;

	// create: 201 with tokens; the node got the merged env and a deadline.
	let (status, created, _) = api
		.call(
			"POST",
			"/v2/sandboxes",
			Some(json!({"templateID": "my-tpl", "timeout": 1, "autoPause": true, "envVars": {"SHARED": "request"},
				"metadata": {"owner": "test"}})),
		)
		.await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	let id = created["sandboxID"].as_str().unwrap().to_owned();
	assert!(id.starts_with('i') && id.len() == 21);
	assert_eq!(created["templateID"], template_id.as_str());
	assert_eq!(created["alias"], "my-tpl");
	assert_eq!(created["clientID"], "6532622b");
	assert_eq!(created["envdVersion"], "0.6.4");
	assert_eq!(created["envdAccessToken"], state.keys.envd_token(&id).as_str());
	assert!(created.get("trafficAccessToken").is_none());
	{
		let fake = fake.lock();
		let req = &fake.creates[0];
		assert_eq!(req.init.env_vars["SHARED"], "request");
		assert_eq!(req.init.env_vars["FROM_TEMPLATE"], "1");
		assert_eq!(req.init.default_user.as_deref(), Some("user"));
		assert_eq!(req.init.envd_access_token, state.keys.envd_token(&id));
		assert_eq!((req.vcpus, req.memory_mb), (2, 512));
	}
	let (status, detail, _) = api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(detail["lifecycle"], json!({"autoResume": false, "onTimeout": "pause"}));
	assert_eq!(detail["metadata"]["owner"], "test");

	// timeout → reaper auto-pauses (memory snapshot), route removed, row paused.
	eventually("auto-pause on timeout", || async { fake.lock().pauses == vec![(id.clone(), true)] })
		.await;
	eventually("route removed after the pause", || async {
		get_route(&cluster.kv, &id).await.unwrap().is_none()
	})
	.await;
	let (_, detail, _) = api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(detail["state"], "paused");
	assert!(detail["domain"].is_null(), "paused sandboxes report no domain");
	let (_, listed, headers) = api.call("GET", "/v2/sandboxes?state=paused", None).await;
	assert_eq!(listed.as_array().unwrap().len(), 1);
	assert!(headers.get("x-total-running").is_none());
	let (status, ..) = api
		.call("POST", &format!("/sandboxes/{id}/pause"), Some(json!({})))
		.await;
	assert_eq!(status, StatusCode::CONFLICT, "pausing a paused sandbox");

	// connect on paused → resume (201); connect again → extend only (200).
	let (status, resumed, _) = api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{resumed}");
	assert_eq!(resumed["sandboxID"], id.as_str());
	{
		let fake = fake.lock();
		assert_eq!(fake.resumes.len(), 1);
		let (_, req) = &fake.resumes[0];
		assert!(!req.cold_boot);
		assert_eq!(
			req.init.env_vars["SHARED"], "request",
			"env vars are re-sent from the encrypted row"
		);
	}
	let route = get_route(&cluster.kv, &id).await.unwrap().unwrap();
	let resumed_end = route.end_at_ms;
	let (status, ..) = api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), Some(json!({"timeout": 5})))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(
		get_route(&cluster.kv, &id)
			.await
			.unwrap()
			.unwrap()
			.end_at_ms,
		resumed_end,
		"connect never shortens"
	);
	let (status, ..) = api
		.call("POST", &format!("/sandboxes/{id}/timeout"), Some(json!({"timeout": 900})))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	assert!(
		get_route(&cluster.kv, &id)
			.await
			.unwrap()
			.unwrap()
			.end_at_ms
			> resumed_end
	);
	assert!(fake.lock().deadlines.iter().any(|(sid, _)| *sid == id));

	// Admission: second sandbox fits, third hits the limit of 2.
	let (status, second, _) = api
		.call("POST", "/v2/sandboxes", Some(json!({"templateID": "my-tpl"})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{second}");
	let (status, rejected, _) = api
		.call("POST", "/v2/sandboxes", Some(json!({"templateID": "my-tpl"})))
		.await;
	assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
	assert_eq!(rejected["code"], 429);
	assert!(
		rejected["message"]
			.as_str()
			.unwrap()
			.contains("maximum number of concurrent E2B sandboxes (2)")
	);
	assert_eq!(rejected["error_code"], "sandbox_concurrency_limit");
	assert_eq!(fake.lock().creates.len(), 2, "a rejected create never reaches a node");
	let (_, listed, headers) = api.call("GET", "/v2/sandboxes?limit=1", None).await;
	assert_eq!(listed.as_array().unwrap().len(), 1);
	assert_eq!(headers["x-total-running"], "2");
	let token = headers["x-next-token"].to_str().unwrap().to_owned();
	let (_, page2, headers) = api
		.call("GET", &format!("/v2/sandboxes?limit=1&nextToken={token}"), None)
		.await;
	assert_eq!(page2.as_array().unwrap().len(), 1);
	assert_ne!(page2[0]["sandboxID"], listed[0]["sandboxID"]);
	assert!(headers.get("x-next-token").is_none());

	// kill: 204, node state deleted, then 404 everywhere; the freed slot admits
	// again.
	let (status, ..) = api.call("DELETE", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	assert!(fake.lock().kills.contains(&id));
	let (status, body, _) = api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	assert_eq!(
		body["message"],
		format!("Sandbox \"{id}\" doesn't exist or you don't have access to it")
	);
	let (status, ..) = api.call("DELETE", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	let (status, ..) = api
		.call("POST", &format!("/v2/sandboxes/{id}/connect"), None)
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	let (status, third, _) = api
		.call("POST", "/v2/sandboxes", Some(json!({"templateID": "my-tpl"})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{third}");

	let events = db
		.store
		.list_events(&team.id, &EventQuery {
			sandbox_id: Some(id.clone()),
			limit: 10,
			order_asc: true,
			..EventQuery::default()
		})
		.await
		.unwrap();
	let types: Vec<&str> = events.iter().map(|e| e.event_type.as_str()).collect();
	assert_eq!(types, [
		"sandbox.lifecycle.created",
		"sandbox.lifecycle.paused",
		"sandbox.lifecycle.resumed",
		"sandbox.lifecycle.updated",
		"sandbox.lifecycle.killed"
	]);
	assert_eq!(events.last().unwrap().data.as_ref().unwrap().0["kill_reason"], "request");

	h.finish().await;
}

/// Regression: a resume whose reply was lost (timeout) while the node did
/// resume must keep the sandbox — dropping its route made the reconciler kill
/// the VM and delete the paused state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumes_the_node_completed_despite_an_error_keep_the_sandbox() {
	let Some(h) = harness().await else {
		return;
	};
	let (status, created) = h.create().await;
	assert_eq!(status, StatusCode::CREATED, "{created}");
	let id = created["sandboxID"].as_str().unwrap().to_owned();
	let pause_uri = format!("/sandboxes/{id}/pause");
	let (status, ..) = h.api.call("POST", &pause_uri, Some(json!({}))).await;
	assert_eq!(status, StatusCode::NO_CONTENT);

	// The node resumes but the reply fails: the driver asks the node and keeps
	// it.
	h.fake.lock().resume_then_fail = true;
	let connect_uri = format!("/v2/sandboxes/{id}/connect");
	let (status, body, _) = h
		.api
		.call("POST", &connect_uri, Some(json!({"timeout": 600})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{body}");
	assert_eq!(get_route(&h.cluster.kv, &id).await.unwrap().unwrap().state, RouteState::Running);
	let (_, detail, _) = h.api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(detail["state"], "running");

	// A resume the driver gave up on (route gone, row paused) that the node
	// completed anyway is paused again, never killed.
	let (status, ..) = h.api.call("POST", &pause_uri, Some(json!({}))).await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	h.fake.lock().sandboxes.get_mut(&id).unwrap().state = NodeState::Running;
	super::background::reconcile(&h.state, &mut super::background::Departures::default())
		.await
		.unwrap();
	{
		let fake = h.fake.lock();
		assert!(fake.kills.is_empty(), "{:?}", fake.kills);
		assert_eq!(fake.pauses.len(), 3, "two API pauses and the reconciler's");
		assert_eq!(fake.pauses.last(), Some(&(id.clone(), true)));
		assert_eq!(fake.sandboxes[&id].state, NodeState::Paused);
	}
	let (_, detail, _) = h.api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(detail["state"], "paused");
	h.kill(&id).await;

	h.finish().await;
}

/// Regression (raft commits stalled by disk I/O): a create's sandbox lock
/// lapsed while the node was still booting the VM, and the reconciler, seeing
/// an unlocked running route whose VM the node did not list yet, removed the
/// route and marked the sandbox killed; the VM that came up was then killed
/// as an orphan. A lapsed lock is re-taken by its holder, and a fresh route
/// is never declared lost just because its VM is not listed yet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_whose_lock_lapsed_is_not_killed_while_its_vm_boots() {
	let Some(h) = harness().await else {
		return;
	};
	let kv = &h.cluster.kv;

	// Lease semantics shared by sandbox locks and build supervision.
	let ttl = Duration::from_mins(1);
	assert_eq!(hold_lease(kv, "lease/x", b"a", ttl).await.unwrap(), Lease::Taken);
	assert_eq!(hold_lease(kv, "lease/x", b"a", ttl).await.unwrap(), Lease::Extended);
	assert_eq!(hold_lease(kv, "lease/x", b"b", ttl).await.unwrap(), Lease::Lost);
	kv.delete("lease/x").await.unwrap();
	assert_eq!(
		hold_lease(kv, "lease/x", b"a", ttl).await.unwrap(),
		Lease::Taken,
		"lapsed: re-taken"
	);

	h.fake.lock().create_delay = Duration::from_secs(2);
	let lapse_and_reconcile = async {
		eventually("the create admitted", || async { !list_routes(kv).await.unwrap().is_empty() })
			.await;
		let (id, _) = list_routes(kv).await.unwrap().remove(0);
		kv.delete(&lock_key(&id)).await.unwrap();
		super::background::reconcile(&h.state, &mut super::background::Departures::default())
			.await
			.unwrap();
		id
	};
	let ((status, created), id) = tokio::join!(h.create(), lapse_and_reconcile);
	assert_eq!(status, StatusCode::CREATED, "{created}");
	assert_eq!(created["sandboxID"], id.as_str());
	assert_eq!(get_route(kv, &id).await.unwrap().unwrap().state, RouteState::Running);
	super::background::reconcile(&h.state, &mut super::background::Departures::default())
		.await
		.unwrap();
	let kills = h.fake.lock().kills.clone();
	assert!(kills.is_empty(), "{kills:?}");
	assert!(get_route(kv, &id).await.unwrap().is_some());
	let (_, detail, _) = h.api.call("GET", &format!("/sandboxes/{id}"), None).await;
	assert_eq!(detail["state"], "running");
	h.kill(&id).await;

	h.finish().await;
}

/// Regression (node out of disk during fork benchmarks): failed creates and
/// forks must give back every admission slot and leave no hidden fork
/// template behind; a fork's build is reclaimed (node artifacts and `MySQL`
/// rows) once its last child is gone, not before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_creates_and_forks_release_capacity_and_fork_builds_are_reclaimed() {
	let Some(h) = harness().await else {
		return;
	};
	let kv = &h.cluster.kv;
	const STALE: Duration = Duration::from_mins(30);

	// The node cannot start the VM: 500, and the slot is free again.
	h.fake.lock().fail_creates = 1;
	let (status, failed) = h.create().await;
	assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{failed}");
	assert_eq!(failed["error_code"], "sandbox_create_failed");
	assert_eq!(h.admitted().await, 0);
	let routes = list_routes(kv).await.unwrap();
	assert!(routes.is_empty(), "unexpected routes: {routes:?}");

	let (status, source) = h.create().await;
	assert_eq!(status, StatusCode::CREATED, "{source}");
	let source = source["sandboxID"].as_str().unwrap().to_owned();
	let fork_uri = format!("/sandboxes/{source}/fork");

	// The child fails on the node: a per-child error, no slot kept, and the
	// hidden template is retired right away.
	h.fake.lock().fail_creates = 1;
	let (status, children, _) = h
		.api
		.call("POST", &fork_uri, Some(json!({"count": 1})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{children}");
	assert!(children[0]["sandbox"].is_null(), "{children}");
	assert_eq!(children[0]["error"]["code"], 500, "{children}");
	assert_eq!(h.fake.lock().fail_creates, 0);
	assert_eq!(h.admitted().await, 1, "only the source holds a slot");

	// The capture itself fails: an error, and its template is retired too.
	h.fake.lock().fail_snapshots = 1;
	let (status, error, _) = h
		.api
		.call("POST", &fork_uri, Some(json!({"count": 1})))
		.await;
	assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{error}");
	assert_eq!(h.admitted().await, 1);
	let forks = fork_templates(&h.db.store).await;
	assert_eq!(forks.len(), 2);
	assert!(forks.iter().all(|(_, retired)| *retired), "{forks:?}");
	assert_eq!(get_route(kv, &source).await.unwrap().unwrap().state, RouteState::Running);

	// Capacity is fully back: the limit of 2 admits exactly one more.
	let (status, second) = h.create().await;
	assert_eq!(status, StatusCode::CREATED, "{second}");
	let (status, _) = h.create().await;
	assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
	h.kill(second["sandboxID"].as_str().unwrap()).await;
	assert_eq!(h.admitted().await, 1);

	// A successful fork: its template is retired at once, but the build stays
	// while the child runs.
	let (status, children, _) = h
		.api
		.call("POST", &fork_uri, Some(json!({"count": 1})))
		.await;
	assert_eq!(status, StatusCode::CREATED, "{children}");
	let child = children[0]["sandbox"]["sandboxID"]
		.as_str()
		.unwrap()
		.to_owned();
	let fork_build = h.fake.lock().snapshots.last().unwrap().clone();
	assert_ne!(fork_build, h.build_id);
	assert!(
		fork_templates(&h.db.store)
			.await
			.iter()
			.all(|(_, retired)| *retired)
	);
	builds::housekeeping(&h.state, STALE).await;
	{
		let fake = h.fake.lock();
		assert!(!fake.deleted_artifacts.contains(&fork_build), "in use by the running child");
		assert_eq!(fake.deleted_artifacts.len(), 2, "both failed forks' builds are collected");
	}
	assert_eq!(fork_templates(&h.db.store).await.len(), 1, "collected forks are purged");

	// Last user gone → artifacts deleted on the node, rows purged; nothing
	// admitted after the source goes too.
	h.kill(&child).await;
	builds::housekeeping(&h.state, STALE).await;
	assert!(h.fake.lock().deleted_artifacts.contains(&fork_build));
	let templates = fork_templates(&h.db.store).await;
	assert!(templates.is_empty(), "unexpected fork templates: {templates:?}");
	assert_eq!(snapshot_builds(&h.db.store).await, 0);
	let artifacts = kv.list(&format!("art/{fork_build}/")).await.unwrap();
	assert!(artifacts.is_empty(), "unexpected artifacts: {artifacts:?}");
	h.kill(&source).await;
	assert_eq!(h.admitted().await, 0);
	let routes = list_routes(kv).await.unwrap();
	assert!(routes.is_empty(), "unexpected routes: {routes:?}");
	assert!(h.fake.lock().sandboxes.is_empty());

	// A client giving up mid-create (SDK timeout) must not strand its slot:
	// the create still completes and is recorded like any other.
	h.fake.lock().create_delay = Duration::from_millis(500);
	let abandoned = tokio::time::timeout(Duration::from_millis(100), h.create()).await;
	assert!(abandoned.is_err(), "the create outlives the client");
	eventually("the abandoned create to finish", || async { h.fake.lock().sandboxes.len() == 1 })
		.await;
	let id = h.fake.lock().sandboxes.keys().next().unwrap().clone();
	eventually("the abandoned create to be recorded", || async {
		h.db.store.sandbox(&id).await.unwrap().is_some()
	})
	.await;
	assert_eq!(h.admitted().await, 1);
	h.kill(&id).await;
	assert_eq!(h.admitted().await, 0);

	h.finish().await;
}
