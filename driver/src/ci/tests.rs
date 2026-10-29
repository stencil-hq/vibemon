//! The CI controller end to end: signed webhooks into the driver API, the
//! leader's controller against an in-process fake GitHub, and the lifecycle
//! harness's fake node (sandbox creates, the runner process, kills).

use std::{collections::BTreeMap, sync::Arc};

use axum::{
	Json, Router,
	extract::{Path, State},
	http::StatusCode,
	routing::{delete, get, post},
};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use vmon_node::ci_cache::{
	index::{self, Reserved},
	records::CacheScope,
};

use super::webhook::sign;
use crate::{
	lifecycle::tests::{Harness, eventually, harness_with},
	store::{
		self,
		ci::{CiAppRow, InstallationSeen, JobState, RunnerState},
	},
};

const WEBHOOK_SECRET: &str = "hook-secret";
const ADMIN: &str = "admin-token";
const TEMPLATE_LABEL: &str = "vmon-small/from=my-tpl";

/// Calls the fake GitHub received.
#[derive(Default)]
struct FakeGithub {
	/// `(repo, body)` of each JIT config request.
	jit:     Vec<(String, Value)>,
	/// Deleted runner ids.
	deleted: Vec<u64>,
	/// Login of every run's actor.
	actor:   String,
}

type Shared = Arc<Mutex<FakeGithub>>;

async fn token() -> Json<Value> {
	let expires = store::now() + chrono::Duration::hours(1);
	Json(json!({ "token": "ghs_test", "expires_at": expires }))
}

async fn jitconfig(
	State(github): State<Shared>,
	Path((owner, repo)): Path<(String, String)>,
	Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
	let mut github = github.lock();
	github.jit.push((format!("{owner}/{repo}"), body.clone()));
	let id = 1000 + github.jit.len();
	let name = body["name"].as_str().unwrap_or_default().to_owned();
	(
		StatusCode::CREATED,
		Json(json!({
			"runner": { "id": id, "name": name, "status": "offline", "busy": false },
			"encoded_jit_config": format!("JIT-{name}"),
		})),
	)
}

async fn delete_runner(
	State(github): State<Shared>,
	Path((_, _, id)): Path<(String, String, u64)>,
) -> StatusCode {
	github.lock().deleted.push(id);
	StatusCode::NO_CONTENT
}

async fn run(State(github): State<Shared>) -> Json<Value> {
	let actor = github.lock().actor.clone();
	Json(json!({
		"actor": { "login": actor },
		"triggering_actor": { "login": actor },
		"event": "push",
		"head_branch": "main",
		"head_sha": "abc123",
		"pull_requests": [],
	}))
}

async fn fake_github() -> (String, Shared) {
	let github: Shared = Arc::default();
	let router = Router::new()
		.route("/app/installations/{id}/access_tokens", post(token))
		.route("/app/hook/deliveries", get(|| async { Json(json!([])) }))
		.route("/repos/{owner}/{repo}/actions/runners/generate-jitconfig", post(jitconfig))
		.route(
			"/repos/{owner}/{repo}/actions/runners",
			get(|| async { Json(json!({ "total_count": 0, "runners": [] })) }),
		)
		.route("/repos/{owner}/{repo}/actions/runners/{id}", delete(delete_runner))
		.route("/repos/{owner}/{repo}/actions/runs/{id}", get(run))
		.with_state(Arc::clone(&github));
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("http://{}", listener.local_addr().unwrap());
	tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
	(url, github)
}

fn payload(
	action: &str,
	job_id: u64,
	labels: &[&str],
	runner: Option<&str>,
	private: bool,
) -> Value {
	let conclusion = match action {
		"completed" => Some("success"),
		_ => None,
	};
	json!({
		"action": action,
		"workflow_job": {
			"id": job_id,
			"run_id": 50,
			"run_attempt": 1,
			"workflow_name": "CI",
			"name": format!("test {job_id}"),
			"head_branch": "main",
			"head_sha": "abc123",
			"html_url": format!("https://github.com/acme/app/actions/runs/50/job/{job_id}"),
			"labels": labels,
			"status": action,
			"conclusion": conclusion,
			"runner_name": runner,
		},
		"repository": {
			"id": 77,
			"full_name": "acme/app",
			"private": private,
			"default_branch": "main",
		},
		"installation": { "id": 1 },
	})
}

async fn deliver(h: &Harness, event: &str, body: &Value) -> (StatusCode, Value) {
	deliver_as(h, event, body, "d-1").await
}

async fn deliver_as(h: &Harness, event: &str, body: &Value, guid: &str) -> (StatusCode, Value) {
	let signature = sign(WEBHOOK_SECRET.as_bytes(), body.to_string().as_bytes());
	let headers = [
		("x-github-event", event),
		("x-github-delivery", guid),
		("x-hub-signature-256", signature.as_str()),
	];
	let (status, reply, _) = h
		.api
		.call_with_headers("POST", "/ci/github/webhook", &headers, Some(body.clone()))
		.await;
	(status, reply)
}

async fn job_state(h: &Harness, job_id: u64) -> JobState {
	h.store.ci_job(job_id).await.unwrap().unwrap().state
}

async fn configure(h: &Harness) {
	let keys = &h.state.keys;
	let now = store::now();
	h.store
		.put_ci_app(&CiAppRow {
			app_id:             42,
			slug:               "vmon-ci".into(),
			name:               "vmon CI".into(),
			owner:              "acme".into(),
			html_url:           "https://github.com/apps/vmon-ci".into(),
			webhook_url:        None,
			dashboard_url:      None,
			client_id:          None,
			client_secret_enc:  None,
			private_key_enc:    keys.encrypt(include_bytes!("testdata/app-key.pem")),
			webhook_secret_enc: keys.encrypt(WEBHOOK_SECRET.as_bytes()),
			permissions:        None,
			created_at:         now,
			updated_at:         now,
		})
		.await
		.unwrap();
	h.store
		.upsert_ci_account(&InstallationSeen {
			installation_id: 1,
			login:           "acme".into(),
			kind:            "Organization".into(),
			suspended:       false,
			permissions:     Some(BTreeMap::from([("administration".to_owned(), "write".to_owned())])),
		})
		.await
		.unwrap();
	let team = h.team.id.clone();
	h.store
		.update_ci_account(1, |account| account.team_id = Some(team.clone()))
		.await
		.unwrap();
}

/// Requests of the fake deliveries API: `(query of each list call, ids of
/// each redelivery)`.
#[derive(Default)]
struct FakeDeliveries {
	base:      String,
	lists:     Vec<String>,
	redeliver: Vec<u64>,
}

type SharedDeliveries = Arc<Mutex<FakeDeliveries>>;

async fn list_deliveries(
	State(fake): State<SharedDeliveries>,
	axum::extract::RawQuery(query): axum::extract::RawQuery,
) -> axum::response::Response {
	use axum::response::IntoResponse;
	let query = query.unwrap_or_default();
	let base = {
		let mut fake = fake.lock();
		fake.lists.push(query.clone());
		fake.base.clone()
	};
	let at = |hours: i64| store::now() - chrono::Duration::hours(hours);
	let failed = |id: u64, guid: &str, event: &str, hours: i64| {
		json!({"id": id, "guid": guid, "delivered_at": at(hours), "redelivery": false,
			"status_code": 502, "event": event, "action": "queued"})
	};
	let (page, next) = if query.contains("cursor=v1_2") {
		// Page 2 reaches past the 24 h window: no page 3 is read.
		(
			vec![failed(704, "guid-d", "workflow_job", 23), failed(703, "guid-c", "workflow_job", 30)],
			Some("v1_3"),
		)
	} else if query.contains("cursor=v1_3") {
		(vec![failed(705, "guid-e", "workflow_job", 31)], None)
	} else {
		(
			vec![
				failed(710, "guid-a", "workflow_job", 1),
				failed(709, "guid-push", "push", 1),
				failed(702, "guid-b", "workflow_job", 2),
				failed(701, "guid-b", "workflow_job", 3),
			],
			Some("v1_2"),
		)
	};
	let mut response = Json(page).into_response();
	if let Some(cursor) = next {
		let link = format!(
			"<{base}/app/hook/deliveries?per_page=100&status=failure&cursor={cursor}>; rel=\"next\""
		);
		response.headers_mut().insert("link", link.parse().unwrap());
	}
	response
}

async fn redeliver_attempt(
	State(fake): State<SharedDeliveries>,
	Path(id): Path<u64>,
) -> StatusCode {
	fake.lock().redeliver.push(id);
	StatusCode::ACCEPTED
}

/// Redelivery on a busy shared app: failures only, followed by cursor until
/// the window ends; deliveries vmon accepted, other events, and old ones are
/// skipped; each delivery is requested at most three times.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_deliveries_are_redelivered_across_pages_with_a_cap() {
	let fake: SharedDeliveries = Arc::default();
	let router = Router::new()
		.route("/app/hook/deliveries", get(list_deliveries))
		.route("/app/hook/deliveries/{id}/attempts", post(redeliver_attempt))
		.with_state(Arc::clone(&fake));
	let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("http://{}", listener.local_addr().unwrap());
	fake.lock().base.clone_from(&url);
	tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
	let h = harness_with(|config| config.ci_github_api = url).await;
	configure(&h).await;
	// vmon answered guid-a itself (the relay failed afterwards).
	let (status, _) = deliver_as(&h, "ping", &json!({"zen": "hi"}), "guid-a").await;
	assert_eq!(status, StatusCode::OK);
	assert!(h.store.ci_delivery_accepted("guid-a").await.unwrap());

	let github = super::github::Github::load(&h.state)
		.await
		.unwrap()
		.unwrap();
	super::controller::redeliver(&h.state, &github)
		.await
		.unwrap();
	{
		let fake = fake.lock();
		assert_eq!(fake.lists.len(), 2, "{:?}", fake.lists);
		assert!(
			fake
				.lists
				.iter()
				.all(|query| query.contains("status=failure")),
			"{:?}",
			fake.lists
		);
		assert!(fake.lists[1].contains("cursor=v1_2"));
		let mut asked = fake.redeliver.clone();
		asked.sort_unstable();
		// guid-b's newest attempt, guid-d; not guid-a (accepted), the push,
		// guid-c (too old), or page 3.
		assert_eq!(asked, [702, 704]);
	}
	for _ in 0..3 {
		super::controller::redeliver(&h.state, &github)
			.await
			.unwrap();
	}
	let asked = fake.lock().redeliver.clone();
	assert_eq!(asked.iter().filter(|&&id| id == 702).count(), 3, "capped at three");
	assert_eq!(asked.len(), 6);
	h.finish().await;
}

/// Overview problems name missing permissions of the app and installations.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_permissions_are_setup_problems() {
	let h = harness_with(|_| {}).await;
	configure(&h).await;
	let mut app = h.store.ci_app().await.unwrap().unwrap();
	app.permissions = Some(BTreeMap::from([("actions".to_owned(), "read".to_owned())]));
	h.store.put_ci_app(&app).await.unwrap();
	h.store
		.upsert_ci_account(&InstallationSeen {
			installation_id: 1,
			login:           "acme".into(),
			kind:            "Organization".into(),
			suspended:       false,
			permissions:     Some(BTreeMap::from([
				("actions".to_owned(), "read".to_owned()),
				("administration".to_owned(), "read".to_owned()),
			])),
		})
		.await
		.unwrap();
	let headers = [("x-admin-token", ADMIN)];
	let (status, overview, _) = h
		.api
		.call_with_headers("GET", "/admin/ci", &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK);
	let problems = overview["problems"].to_string();
	assert!(
		problems
			.contains("installation acme lacks Administration: write; runners cannot be registered"),
		"{problems}"
	);
	assert!(problems.contains("does not request Administration: write"), "{problems}");
	h.finish().await;
}

#[test]
fn rotation_visits_every_item() {
	let cursor = Mutex::new(None);
	let items = [1, 2, 3, 4, 5];
	assert_eq!(super::controller::rotate(&items, &cursor, 2), [1, 2]);
	assert_eq!(super::controller::rotate(&items, &cursor, 2), [3, 4]);
	assert_eq!(super::controller::rotate(&items, &cursor, 2), [5, 1]);
	// The cursor item may be gone by the next pass.
	assert_eq!(super::controller::rotate(&[2, 4, 6], &cursor, 2), [2, 4]);
	assert_eq!(super::controller::rotate(&[2, 4, 6], &cursor, 9), [6, 2, 4]);
	assert_eq!(super::controller::rotate::<i32>(&[], &cursor, 2), Vec::<i32>::new());
}

/// A job vmon refused that starts on a runner provisioned for an allowed
/// job: the runner is stopped (sandbox killed, GitHub runner removed), and
/// the allowed job gets a fresh runner. An undecided job is adopted instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_job_on_an_allowed_jobs_runner_stops_it() {
	let (github_url, github) = fake_github().await;
	let h = harness_with(|config| config.ci_github_api = github_url).await;
	super::spawn_controller(h.state.clone());
	configure(&h).await;
	h.store
		.update_ci_account(1, |account| {
			account.public.enabled = true;
			account.public.users = vec!["alice".into()];
		})
		.await
		.unwrap();
	github.lock().actor = "alice".into();
	deliver(&h, "workflow_job", &payload("queued", 20, &[TEMPLATE_LABEL], None, false)).await;
	eventually("the allowed job's runner", || async { h.fake.lock().processes.len() == 1 }).await;
	let first = h
		.store
		.ci_job(20)
		.await
		.unwrap()
		.unwrap()
		.sandbox_id
		.unwrap();
	{
		let fake = h.fake.lock();
		let gate = &fake.processes[0].1.envs["VMON_CI_GATE"];
		assert!(gate.contains("repo='acme/app'"), "{gate}");
		assert!(gate.contains("mode='users'") && gate.contains("users='alice'"), "{gate}");
	}
	github.lock().actor = "mallory".into();
	let stranger = payload("queued", 21, &[TEMPLATE_LABEL], None, false);
	deliver(&h, "workflow_job", &stranger).await;
	eventually("the stranger refused", || async { job_state(&h, 21).await == JobState::Refused })
		.await;

	let stolen = payload("in_progress", 21, &[TEMPLATE_LABEL], Some("vmon-20-1"), false);
	deliver(&h, "workflow_job", &stolen).await;
	eventually("the stolen runner's sandbox killed", || async {
		h.fake.lock().kills.contains(&first)
	})
	.await;
	eventually("the allowed job's fresh runner", || async { h.fake.lock().processes.len() == 2 })
		.await;
	let job20 = h.store.ci_job(20).await.unwrap().unwrap();
	assert_eq!(job20.runner.as_deref(), Some("vmon-20-2"));
	assert_ne!(job20.sandbox_id.as_deref(), Some(first.as_str()));
	let job21 = h.store.ci_job(21).await.unwrap().unwrap();
	assert_eq!(job21.state, JobState::Refused);
	assert!(job21.reason.as_deref().unwrap().contains("vmon-20-1"), "{:?}", job21.reason);
	eventually("the stolen GitHub runner removed", || async {
		github.lock().deleted.contains(&1001)
	})
	.await;
	eventually("the stolen runner forgotten", || async {
		h.store.ci_runner("vmon-20-1").await.unwrap().is_none()
	})
	.await;

	// A job whose `queued` webhook was lost (never decided) that starts on
	// the allowed job's new runner is adopted, not stopped; the allowed job
	// gets yet another runner.
	let second = h
		.store
		.ci_job(20)
		.await
		.unwrap()
		.unwrap()
		.sandbox_id
		.unwrap();
	let lost = payload("in_progress", 22, &[TEMPLATE_LABEL], Some("vmon-20-2"), false);
	deliver(&h, "workflow_job", &lost).await;
	let job22 = h.store.ci_job(22).await.unwrap().unwrap();
	assert_eq!(job22.state, JobState::Running);
	assert_eq!(job22.runner.as_deref(), Some("vmon-20-2"));
	assert_eq!(job22.sandbox_id.as_deref(), Some(second.as_str()));
	eventually("the allowed job's third runner", || async { h.fake.lock().processes.len() == 3 })
		.await;
	assert_eq!(h.store.ci_job(20).await.unwrap().unwrap().runner.as_deref(), Some("vmon-20-3"));
	assert!(!h.fake.lock().kills.contains(&second), "the adopted runner keeps its sandbox");
	h.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn webhooks_drive_runners_from_queued_to_teardown() {
	let (github_url, github) = fake_github().await;
	let h = harness_with(|config| config.ci_github_api = github_url).await;
	super::spawn_controller(h.state.clone());

	// Intake answers: no app → 503, bad signature → 401, ping → 200.
	let queued = payload("queued", 1, &["self-hosted", TEMPLATE_LABEL], None, true);
	assert_eq!(deliver(&h, "workflow_job", &queued).await.0, StatusCode::SERVICE_UNAVAILABLE);
	configure(&h).await;
	let (status, ..) = h
		.api
		.call_with_headers(
			"POST",
			"/ci/github/webhook",
			&[("x-github-event", "workflow_job"), ("x-hub-signature-256", "sha256=00")],
			Some(queued.clone()),
		)
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	assert_eq!(deliver(&h, "ping", &json!({"zen": "hi"})).await.0, StatusCode::OK);
	let foreign = payload("queued", 9, &["ubuntu-latest"], None, true);
	assert_eq!(deliver(&h, "workflow_job", &foreign).await.0, StatusCode::OK);
	assert!(h.store.ci_job(9).await.unwrap().is_none());

	// queued → a sandbox at the label's size and the runner process.
	assert_eq!(deliver(&h, "workflow_job", &queued).await.0, StatusCode::ACCEPTED);
	assert_eq!(deliver(&h, "workflow_job", &queued).await.0, StatusCode::ACCEPTED, "redelivery");
	eventually("the runner started", || async { !h.fake.lock().processes.is_empty() }).await;
	let job = h.store.ci_job(1).await.unwrap().unwrap();
	assert_eq!(job.state, JobState::Starting);
	let sandbox = job.sandbox_id.clone().unwrap();
	{
		let fake = h.fake.lock();
		assert_eq!(fake.creates.len(), 1, "one sandbox per runner");
		let create = &fake.creates[0];
		assert_eq!((create.vcpus, create.memory_mb, create.cold_boot), (2, 4096, true));
		let env: Vec<&str> = create.init.env_vars.keys().map(String::as_str).collect();
		assert_eq!(env, ["FROM_TEMPLATE", "SHARED"], "only the template's env, no secrets");
		let (target, process) = &fake.processes[0];
		assert_eq!(target, &sandbox);
		assert_eq!(process.user.as_deref(), Some("user"));
		assert_eq!(process.cwd.as_deref(), Some("/home/user/actions-runner"));
		assert_eq!(process.envs["VMON_CI_JITCONFIG"], "JIT-vmon-1-1");
		assert!(process.args[1].contains("./run.sh --jitconfig"), "{:?}", process.args);
		assert!(process.args[1].contains("ACTIONS_RUNNER_HOOK_JOB_STARTED"), "{:?}", process.args);
		assert!(!process.args.concat().contains("JIT-"), "the JIT config stays out of argv");
		let gate = &process.envs["VMON_CI_GATE"];
		assert!(gate.contains("repo='acme/app'") && gate.contains("mode='private'"), "{gate}");
	}
	{
		let github = github.lock();
		let (repo, body) = &github.jit[0];
		assert_eq!(repo, "acme/app");
		assert_eq!(body["name"], "vmon-1-1");
		assert_eq!(body["labels"], json!([TEMPLATE_LABEL]));
	}
	let row = h.store.sandbox(&sandbox).await.unwrap().unwrap();
	assert_eq!(row.metadata["vmon.ci.job"], "1");
	assert_eq!(row.metadata["vmon.ci.runner"], "vmon-1-1");
	assert_eq!(row.metadata["vmon.ci.repo"], "acme/app");

	// in_progress → running, and the sandbox may use the job's cache scope.
	let running =
		payload("in_progress", 1, &["self-hosted", TEMPLATE_LABEL], Some("vmon-1-1"), true);
	deliver(&h, "workflow_job", &running).await;
	assert_eq!(job_state(&h, 1).await, JobState::Running);
	let kv = &h.state.cluster.kv;
	eventually("the sandbox's cache grant", || async {
		index::grant(kv, &sandbox)
			.await
			.unwrap()
			.is_some_and(|grant| grant.job_id == Some(1) && grant.scope.is_some())
	})
	.await;
	let grant = index::grant(kv, &sandbox).await.unwrap().unwrap();
	assert_eq!(grant.runner, "vmon-1-1");
	let scope = grant.scope.unwrap();
	assert_eq!((scope.repo_id, scope.repo.as_str()), (77, "acme/app"));
	assert_eq!(scope.write, "refs/heads/main");
	assert_eq!(scope.read, ["refs/heads/main"]);
	// completed → the sandbox is killed and its grant goes.
	let done = payload("completed", 1, &["self-hosted", TEMPLATE_LABEL], Some("vmon-1-1"), true);
	deliver(&h, "workflow_job", &done).await;
	eventually("the sandbox killed", || async { h.fake.lock().kills.contains(&sandbox) }).await;
	eventually("the runner forgotten", || async {
		h.store.ci_runner("vmon-1-1").await.unwrap().is_none()
	})
	.await;
	assert_eq!(job_state(&h, 1).await, JobState::Completed);
	assert!(github.lock().deleted.contains(&1001), "the GitHub runner is removed");
	eventually("the grant removed", || async {
		index::grant(kv, &sandbox).await.unwrap().is_none()
	})
	.await;

	// Refusals: unknown size, public repository without a policy.
	deliver(&h, "workflow_job", &payload("queued", 2, &["vmon-huge"], None, true)).await;
	let refused = h.store.ci_job(2).await.unwrap().unwrap();
	assert_eq!(refused.state, JobState::Refused);
	assert!(refused.reason.unwrap().contains("unknown size 'vmon-huge'"));
	deliver(&h, "workflow_job", &payload("queued", 3, &[TEMPLATE_LABEL], None, false)).await;
	eventually("the public job refused", || async { job_state(&h, 3).await == JobState::Refused })
		.await;
	let public = h.store.ci_job(3).await.unwrap().unwrap();
	assert!(
		public
			.reason
			.unwrap()
			.contains("public repositories are off")
	);
	// Allowed users: both actors must be listed.
	github.lock().actor = "mallory".into();
	h.store
		.update_ci_account(1, |account| {
			account.public.enabled = true;
			account.public.users = vec!["alice".into()];
		})
		.await
		.unwrap();
	deliver(&h, "workflow_job", &payload("queued", 5, &[TEMPLATE_LABEL], None, false)).await;
	eventually("the stranger's job refused", || async {
		job_state(&h, 5).await == JobState::Refused
	})
	.await;
	assert!(
		h.store
			.ci_job(5)
			.await
			.unwrap()
			.unwrap()
			.reason
			.unwrap()
			.contains("@mallory")
	);

	// debug + failure → held, sandbox kept.
	let debug_label = "vmon-small/from=my-tpl/debug";
	deliver(&h, "workflow_job", &payload("queued", 4, &[debug_label], None, true)).await;
	eventually("the debug runner started", || async { h.fake.lock().processes.len() == 2 }).await;
	let held_sandbox = h
		.store
		.ci_job(4)
		.await
		.unwrap()
		.unwrap()
		.sandbox_id
		.unwrap();
	deliver(&h, "workflow_job", &payload("in_progress", 4, &[debug_label], Some("vmon-4-1"), true))
		.await;
	let mut failed = payload("completed", 4, &[debug_label], Some("vmon-4-1"), true);
	failed["workflow_job"]["conclusion"] = json!("failure");
	deliver(&h, "workflow_job", &failed).await;
	let held = h.store.ci_job(4).await.unwrap().unwrap();
	assert_eq!(held.state, JobState::Held);
	assert!(held.held_until.is_some());
	assert_eq!(h.store.ci_runner("vmon-4-1").await.unwrap().unwrap().state, RunnerState::Held);
	tokio::time::sleep(std::time::Duration::from_secs(2)).await;
	assert!(!h.fake.lock().kills.contains(&held_sandbox), "held sandboxes stay");

	// The admin API reports it all.
	let headers = [("x-admin-token", ADMIN)];
	let (status, overview, _) = h
		.api
		.call_with_headers("GET", "/admin/ci", &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK, "{overview}");
	assert_eq!(overview["stats"]["jobs"], 5);
	assert_eq!(overview["stats"]["refused"], 3);
	assert_eq!(overview["accounts"][0]["team"]["id"], h.team.id.as_str());
	let problems = overview["problems"].to_string();
	assert!(problems.contains("cannot resolve the CI template 'ci'"), "{problems}");
	let (_, page, next) = h
		.api
		.call_with_headers("GET", "/admin/ci/jobs?state=refused&limit=2", &headers, None)
		.await;
	assert_eq!(page["jobs"].as_array().unwrap().len(), 2);
	assert_eq!(page["jobs"][0]["jobId"], 5, "newest first");
	assert_eq!(next["x-next-token"], "3");
	let (_, detail, _) = h
		.api
		.call_with_headers("GET", "/admin/ci/jobs/1", &headers, None)
		.await;
	assert_eq!(detail["state"], "completed");
	assert_eq!(detail["size"], "vmon-small");
	assert_eq!(detail["from"], "my-tpl");
	assert!(detail["events"].as_array().unwrap().len() >= 3, "{detail}");
	let (status, ..) = h
		.api
		.call_with_headers("GET", "/admin/ci/jobs/4/logs", &headers, None)
		.await;
	assert_ne!(status, StatusCode::CONFLICT, "a held job's log is available");
	let (status, ..) = h.api.call_with_headers("GET", "/admin/ci", &[], None).await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	h.finish().await;
}

/// The cache admin API: per-repository totals, one repository's entries
/// (case-insensitive name, newest used first, holders), prefix deletes, and
/// 404 for repositories without entries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_admin_api_lists_and_deletes_entries() {
	let h = harness_with(|_| {}).await;
	let kv = &h.state.cluster.kv;
	let node = vmon_cluster::NodeId(3);
	let scope = CacheScope {
		repo_id: 77,
		repo:    "acme/app".into(),
		write:   "refs/heads/main".into(),
		read:    vec!["refs/heads/main".into()],
	};
	for (n, key) in ["cargo-linux-a", "cargo-linux-b", "npm-a"]
		.into_iter()
		.enumerate()
	{
		let Reserved::New(row) = index::reserve(kv, &scope, key, "v1", 1, node, store::now())
			.await
			.unwrap()
		else {
			panic!("{key} exists");
		};
		let entry = vmon_node::ci_cache::records::entry_key(77, &scope.write, key, "v1");
		let digest = format!("{:064x}", n + 1);
		let size = 100 * (n as u64 + 1);
		let at = store::now() + chrono::Duration::seconds(n as i64);
		index::finalize(kv, &entry, &digest, size, node, at)
			.await
			.unwrap()
			.unwrap();
		assert_eq!(row.key, key);
	}
	let headers = [("x-admin-token", ADMIN)];
	let (status, list, _) = h
		.api
		.call_with_headers("GET", "/admin/ci/caches", &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK, "{list}");
	assert_eq!(list["repos"][0]["repoId"], 77);
	assert_eq!(list["repos"][0]["entries"], 3);
	assert_eq!(list["repos"][0]["sizeBytes"], 600);
	assert_eq!(list["repos"][0]["budgetBytes"], index::REPO_BUDGET);

	let (status, detail, _) = h
		.api
		.call_with_headers("GET", "/admin/ci/caches/ACME/App", &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK, "{detail}");
	assert_eq!(detail["repo"]["repo"], "acme/app");
	assert_eq!(detail["entries"][0]["key"], "npm-a", "newest used first");
	assert_eq!(detail["entries"][0]["state"], "ready");
	assert_eq!(detail["entries"][0]["holders"], json!([node.to_string()]));
	assert_eq!(detail["entries"][0]["scope"], "refs/heads/main");

	let (status, deleted, _) = h
		.api
		.call_with_headers("DELETE", "/admin/ci/caches/acme/app?keyPrefix=cargo-", &headers, None)
		.await;
	assert_eq!(status, StatusCode::OK, "{deleted}");
	assert_eq!(deleted, json!({ "deleted": 2, "freedBytes": 300 }));
	let left = index::entries(kv, Some(77)).await.unwrap();
	assert_eq!(left.iter().map(|row| row.key.as_str()).collect::<Vec<_>>(), ["npm-a"]);

	let (status, missing, _) = h
		.api
		.call_with_headers("GET", "/admin/ci/caches/acme/other", &headers, None)
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	assert_eq!(missing["error_code"], "not_found");
	h.finish().await;
}
