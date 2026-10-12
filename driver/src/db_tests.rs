//! MySQL-backed API tests for the core driver surface (auth, CLI login,
//! secrets, volumes, pagination, webhooks). Gated on `VMON_MYSQL_TEST_URL`;
//! each test uses its own throwaway database.

use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use axum::{
	Router,
	body::{Body, Bytes},
	http::{HeaderMap, Request, StatusCode, header},
	routing::post,
};
use http_body_util::BodyExt;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tower::ServiceExt;
use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeId, Roles};

use crate::{
	admin, api, crypto,
	state::{AppState, DriverConfig, TeamLimits},
	store::{NewSandboxEvent, event_types, testdb::TestDb},
};

const ADMIN_TOKEN: &str = "admin-secret";

struct TestApp {
	db:      TestDb,
	state:   AppState,
	router:  Router,
	cluster: Cluster,
}

impl TestApp {
	async fn start() -> Option<Self> {
		let db = TestDb::create().await?;
		let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
		let cluster = Cluster::start(
			ClusterConfig {
				id:               NodeId::random(),
				data_dir:         PathBuf::from("/nonexistent"),
				roles:            Roles { node: false, driver: false },
				gossip_bind:      any,
				gossip_advertise: any,
				cluster_url:      "http://127.0.0.1:1".into(),
				join:             Vec::new(),
				secret:           b"test-secret".to_vec(),
				bootstrap_expect: 1,
				raft_reap_after:  Duration::from_mins(10),
			},
			MemberMeta::default(),
		)
		.await
		.expect("start role-less cluster");
		let config = DriverConfig {
			domain:         "vmon.test".into(),
			public_url:     "http://api.vmon.test".into(),
			admin_token:    Some(ADMIN_TOKEN.into()),
			cluster_secret: "test-secret".into(),
			data_dir:       PathBuf::from("/nonexistent"),
			default_limits: TeamLimits::default(),
			shared_volumes: false,
			object_store:   None,
		};
		let keys = crypto::Keys::derive(config.cluster_secret.as_bytes());
		let state = AppState::new(config, db.store.clone(), cluster.clone(), keys);
		let router = Router::new()
			.merge(api::teams::routes())
			.merge(api::keys::routes())
			.merge(api::login::routes())
			.merge(api::secrets::routes())
			.merge(api::volumes::routes())
			.merge(api::events::routes())
			.merge(api::webhooks::routes())
			.merge(api::admin::routes())
			.with_state(state.clone());
		Some(Self { db, state, router, cluster })
	}

	async fn finish(self) {
		self.cluster.shutdown().await;
		self.db.drop_db().await;
	}

	async fn call(&self, request: Request<Body>) -> (StatusCode, HeaderMap, Bytes) {
		let response = self.router.clone().oneshot(request).await.unwrap();
		let status = response.status();
		let headers = response.headers().clone();
		let body = response.into_body().collect().await.unwrap().to_bytes();
		(status, headers, body)
	}

	async fn json(&self, request: Request<Body>) -> (StatusCode, HeaderMap, Value) {
		let (status, headers, body) = self.call(request).await;
		(status, headers, serde_json::from_slice(&body).unwrap_or(Value::Null))
	}

	async fn team(&self, name: &str) -> (crate::store::Team, String) {
		admin::create_team(&self.state.store, &self.state.keys, name, TeamLimits::default())
			.await
			.unwrap()
	}
}

fn req(method: &str, uri: &str, headers: &[(&str, &str)], body: Option<Value>) -> Request<Body> {
	let mut builder = Request::builder().method(method).uri(uri);
	for (k, v) in headers {
		builder = builder.header(*k, *v);
	}
	match body {
		Some(body) => builder
			.header(header::CONTENT_TYPE, "application/json")
			.body(Body::from(body.to_string()))
			.unwrap(),
		None => builder.body(Body::empty()).unwrap(),
	}
}

fn form(uri: &str, fields: &[(&str, &str)]) -> Request<Body> {
	let body = url::form_urlencoded::Serializer::new(String::new())
		.extend_pairs(fields)
		.finish();
	Request::post(uri)
		.header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
		.body(Body::from(body))
		.unwrap()
}

#[tokio::test]
async fn migrations_apply_cleanly_twice() {
	let Some(db) = TestDb::create().await else {
		return;
	};
	db.store.migrate().await.expect("second migrate is a no-op");
	let tables: Vec<String> = sqlx::query_scalar(
		"SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY \
		 table_name",
	)
	.fetch_all(db.store.pool())
	.await
	.unwrap();
	for table in [
		"api_keys",
		"refresh_tokens",
		"sandbox_events",
		"secret_versions",
		"teams",
		"webhook_deliveries",
	] {
		assert!(tables.iter().any(|t| t == table), "missing table {table}: {tables:?}");
	}
	db.drop_db().await;
}

#[tokio::test]
async fn api_key_auth_valid_invalid_blocked_and_deleted() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, key) = app.team("Acme").await;

	let (status, ..) = app
		.json(req("GET", "/secrets", &[("X-API-KEY", &key)], None))
		.await;
	assert_eq!(status, StatusCode::OK);

	let (status, _, body) = app.json(req("GET", "/secrets", &[], None)).await;
	assert_eq!(
		(status, body["message"].as_str()),
		(StatusCode::UNAUTHORIZED, Some("authorization header is missing"))
	);

	let (status, _, body) = app
		.json(req("GET", "/secrets", &[("x-api-key", "sk_nope")], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	assert!(
		body["message"]
			.as_str()
			.unwrap()
			.starts_with("API key is malformed")
	);

	let unknown = crypto::generate_api_key();
	let (status, _, body) = app
		.json(req("GET", "/secrets", &[("x-api-key", &unknown)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	assert!(
		body["message"]
			.as_str()
			.unwrap()
			.ends_with("Cannot get the team for the given API key")
	);
	assert_eq!(body["code"], 401);

	// /api-keys refuses API-key auth; admin + X-Team-ID works.
	let (status, ..) = app
		.json(req("GET", "/api-keys", &[("x-api-key", &key)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	let (status, _, keys) = app
		.json(req(
			"GET",
			"/api-keys",
			&[("x-admin-token", ADMIN_TOKEN), ("x-team-id", &team.id)],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(keys.as_array().unwrap().len(), 1);
	assert_eq!(keys[0]["mask"]["maskedValuePrefix"], &key[4..6]);
	let (status, ..) = app
		.json(req("GET", "/api-keys", &[("x-admin-token", "wrong"), ("x-team-id", &team.id)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// Blocked team: reads pass, writes are 403 (the block is visible once the
	// cache entry is invalidated).
	admin::set_team_blocked(&app.state.store, &team.id, true)
		.await
		.unwrap();
	app.state.auth_cache.invalidate_team(&team.id);
	let (status, ..) = app
		.json(req("GET", "/secrets", &[("x-api-key", &key)], None))
		.await;
	assert_eq!(status, StatusCode::OK);
	let new_secret = json!({"name": "a", "value": "v"});
	let (status, _, body) = app
		.json(req("POST", "/secrets", &[("x-api-key", &key)], Some(new_secret)))
		.await;
	assert_eq!((status, body["message"].as_str()), (StatusCode::FORBIDDEN, Some("team is blocked")));
	admin::set_team_blocked(&app.state.store, &team.id, false)
		.await
		.unwrap();

	// Deleting a key revokes it immediately on this driver.
	let key_id = keys[0]["id"].as_str().unwrap();
	let (status, ..) = app
		.call(req(
			"DELETE",
			&format!("/admin/teams/{}/api-keys/{key_id}", team.id),
			&[("x-admin-token", ADMIN_TOKEN)],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	let (status, ..) = app
		.json(req("GET", "/secrets", &[("x-api-key", &key)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// last_used_at is stamped asynchronously on first lookup.
	let (_, _, created) = app
		.json(req(
			"POST",
			&format!("/admin/teams/{}/api-keys", team.id),
			&[("x-admin-token", ADMIN_TOKEN)],
			Some(json!({"name": "k2"})),
		))
		.await;
	let fresh = created["key"].as_str().unwrap().to_owned();
	app.json(req("GET", "/secrets", &[("x-api-key", &fresh)], None))
		.await;
	let mut stamped = false;
	for _ in 0..50 {
		if app
			.state
			.store
			.api_key(&team.id, created["id"].as_str().unwrap())
			.await
			.unwrap()
			.unwrap()
			.last_used_at
			.is_some()
		{
			stamped = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	assert!(stamped, "last_used_at must be updated");
	app.finish().await;
}

/// Logs in through the CLI form and returns the callback query pairs.
async fn cli_login(
	app: &TestApp,
	email: &str,
	password: &str,
) -> (StatusCode, BTreeMap<String, String>) {
	let (status, headers, _) = app
		.call(form("/docs/api/cli", &[
			("email", email),
			("password", password),
			("next", "http://localhost:4321/"),
		]))
		.await;
	let pairs = headers
		.get(header::LOCATION)
		.map(|loc| {
			url::Url::parse(loc.to_str().unwrap())
				.unwrap()
				.query_pairs()
				.into_owned()
				.collect()
		})
		.unwrap_or_default();
	(status, pairs)
}

#[tokio::test]
async fn cli_login_jwt_expiry_refresh_rotation_and_revoke() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, _) = app.team("Login Team").await;
	let user =
		admin::create_user(&app.state.store, "Dev@Example.com", "correct horse", Some(&team.slug))
			.await
			.unwrap();

	let (status, _) = cli_login(&app, "dev@example.com", "wrong password").await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	let (status, ..) = app
		.call(form("/docs/api/cli", &[
			("email", "dev@example.com"),
			("password", "correct horse"),
			("next", "http://evil.test/"),
		]))
		.await;
	assert_eq!(status, StatusCode::BAD_REQUEST, "non-loopback callbacks never receive tokens");

	let (status, pairs) = cli_login(&app, "DEV@example.com", "correct horse").await;
	assert_eq!(status, StatusCode::FOUND);
	for field in
		["email", "accessToken", "refreshToken", "tokenEndpoint", "revokeEndpoint", "clientId"]
	{
		assert!(pairs.contains_key(field), "callback lacks {field}");
	}
	assert_eq!(pairs["tokenEndpoint"], "http://api.vmon.test/auth/token");
	let bearer = format!("Bearer {}", pairs["accessToken"]);

	// GET /teams with the access token returns a working API key; a second
	// call returns the same key instead of minting another.
	let (status, _, teams) = app
		.json(req("GET", "/teams", &[("authorization", &bearer)], None))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(teams[0]["teamID"], team.id.as_str());
	assert_eq!(teams[0]["isDefault"], true);
	let api_key = teams[0]["apiKey"].as_str().unwrap().to_owned();
	let (_, _, again) = app
		.json(req("GET", "/teams", &[("authorization", &bearer)], None))
		.await;
	assert_eq!(again[0]["apiKey"], api_key.as_str());
	let (status, ..) = app
		.json(req("GET", "/secrets", &[("x-api-key", &api_key)], None))
		.await;
	assert_eq!(status, StatusCode::OK);

	// Bearer + X-Team-ID: member team ok, foreign team rejected.
	let (other, _) = app.team("Other").await;
	let (status, ..) = app
		.json(req("GET", "/api-keys", &[("authorization", &bearer), ("x-team-id", &team.id)], None))
		.await;
	assert_eq!(status, StatusCode::OK);
	let (status, ..) = app
		.json(req("GET", "/api-keys", &[("authorization", &bearer), ("x-team-id", &other.id)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// Expired and foreign-key tokens are rejected.
	let expired = format!("Bearer {}", app.state.keys.access_token(&user.id, &user.email, -5));
	let (status, ..) = app
		.json(req("GET", "/teams", &[("authorization", &expired)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	let foreign =
		format!("Bearer {}", crypto::Keys::derive(b"other").access_token(&user.id, &user.email, 60));
	let (status, ..) = app
		.json(req("GET", "/teams", &[("authorization", &foreign)], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);

	// Refresh rotation.
	let first = pairs["refreshToken"].clone();
	let refresh = |token: &str| {
		form("/auth/token", &[
			("grant_type", "refresh_token"),
			("refresh_token", token),
			("client_id", "vmon-cli"),
		])
	};
	let (status, _, rotated) = app.json(refresh(&first)).await;
	assert_eq!(status, StatusCode::OK);
	let second = rotated["refresh_token"].as_str().unwrap().to_owned();
	assert_ne!(second, first);
	let (status, ..) = app
		.json(req(
			"GET",
			"/teams",
			&[("authorization", &format!("Bearer {}", rotated["access_token"].as_str().unwrap()))],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::OK);

	// Replaying the rotated token is invalid_grant and burns the family.
	let (status, _, body) = app.json(refresh(&first)).await;
	assert_eq!((status, body["error"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_grant")));
	let (_, _, body) = app.json(refresh(&second)).await;
	assert_eq!(body["error"], "invalid_grant", "reuse detection must revoke descendants");

	// Revoke: a fresh login's refresh token stops working after revoke.
	let (_, pairs) = cli_login(&app, "dev@example.com", "correct horse").await;
	let token = pairs["refreshToken"].clone();
	let (status, ..) = app
		.call(form("/auth/revoke", &[("token", &token), ("token_type_hint", "refresh_token")]))
		.await;
	assert_eq!(status, StatusCode::OK);
	let (_, _, body) = app.json(refresh(&token)).await;
	assert_eq!(body["error"], "invalid_grant");
	let (status, ..) = app
		.call(form("/auth/revoke", &[("token", "unknown")]))
		.await;
	assert_eq!(status, StatusCode::OK, "revoking unknown tokens is not an error (RFC 7009)");

	let (status, _, body) = app.json(req("POST", "/access-tokens", &[], None)).await;
	assert_eq!(status, StatusCode::GONE);
	assert!(
		body["message"]
			.as_str()
			.unwrap()
			.starts_with("E2B_ACCESS_TOKEN is deprecated")
	);

	// Rigs: admin-only, then the spec's 501 for deployments without rigs.
	let rigs = [
		("GET", "/clusters/c1/rigs"),
		("PUT", "/clusters/c1/rigs/r1/capacity"),
		("GET", "/clusters/c1/rigs/r1/instances"),
		("GET", "/clusters/c1/rigs/r1/errors"),
		("DELETE", "/clusters/c1/rigs/instances/i1?decrementDesired=true"),
	];
	for (method, uri) in rigs {
		let (status, ..) = app.json(req(method, uri, &[], None)).await;
		assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {uri}");
		let (status, _, body) = app
			.json(req(method, uri, &[("x-admin-token", ADMIN_TOKEN)], None))
			.await;
		assert_eq!(
			(status, body["message"].as_str()),
			(StatusCode::NOT_IMPLEMENTED, Some("Not implemented by this deployment")),
			"{method} {uri}"
		);
	}
	app.finish().await;
}

#[tokio::test]
async fn secret_versioning_and_pagination() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, key) = app.team("Secrets").await;
	let auth = [("x-api-key", key.as_str())];

	let (status, _, created) = app
		.json(req(
			"POST",
			"/secrets",
			&auth,
			Some(json!({"name": "OpenAI_Key", "value": "v1", "metadata": {"env": "prod"}})),
		))
		.await;
	assert_eq!(status, StatusCode::CREATED);
	assert_eq!(
		(created["name"].as_str(), created["currentVersion"].as_i64()),
		(Some("openai_key"), Some(1))
	);
	let id = created["secretID"].as_str().unwrap().to_owned();
	assert!(id.starts_with("sec_"));

	let (status, ..) = app
		.json(req("POST", "/secrets", &auth, Some(json!({"name": "OPENAI_KEY", "value": "x"}))))
		.await;
	assert_eq!(status, StatusCode::CONFLICT, "names are unique case-insensitively");

	let (_, _, v2) = app
		.json(req("POST", &format!("/secrets/{id}"), &auth, Some(json!({"value": "v2"}))))
		.await;
	assert_eq!(v2["currentVersion"], 2);
	assert_eq!(v2["metadata"]["env"], "prod", "omitted metadata is kept");
	let (_, _, v3) = app
		.json(req(
			"POST",
			&format!("/secrets/{id}"),
			&auth,
			Some(json!({"value": "v3", "metadata": {}})),
		))
		.await;
	assert_eq!(v3["currentVersion"], 3);
	assert_eq!(v3["metadata"], json!({}));
	assert!(v3.get("value").is_none(), "values are never returned");

	let decrypt = |enc: Option<Vec<u8>>| app.state.keys.decrypt_str(&enc.unwrap()).unwrap();
	assert_eq!(
		decrypt(
			app.state
				.store
				.secret_value(&team.id, &id, Some(1))
				.await
				.unwrap()
		),
		"v1"
	);
	assert_eq!(
		decrypt(
			app.state
				.store
				.secret_value(&team.id, "openai_key", None)
				.await
				.unwrap()
		),
		"v3"
	);
	assert!(
		app.state
			.store
			.secret_value(&team.id, &id, Some(4))
			.await
			.unwrap()
			.is_none()
	);

	let (status, _, by_name) = app
		.json(req("GET", "/secrets/OpenAI_Key", &auth, None))
		.await;
	assert_eq!((status, by_name["secretID"].as_str()), (StatusCode::OK, Some(id.as_str())));

	// Another team cannot see it.
	let (_, other_key) = app.team("Other").await;
	let (status, ..) = app
		.json(req("GET", &format!("/secrets/{id}"), &[("x-api-key", &other_key)], None))
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);

	// Pagination: 5 secrets, pages of 2 → 2, 2, 1 with X-Next-Token on the first
	// two.
	for i in 0..4 {
		app.json(req(
			"POST",
			"/secrets",
			&auth,
			Some(json!({"name": format!("s{i}"), "value": "x"})),
		))
		.await;
	}
	let mut seen = Vec::new();
	let mut token: Option<String> = None;
	let mut pages = Vec::new();
	loop {
		let uri = match &token {
			Some(t) => format!("/secrets?limit=2&nextToken={t}"),
			None => String::from("/secrets?limit=2"),
		};
		let (status, headers, page) = app.json(req("GET", &uri, &auth, None)).await;
		assert_eq!(status, StatusCode::OK);
		let page = page.as_array().unwrap().clone();
		pages.push(page.len());
		seen.extend(
			page
				.iter()
				.map(|s| s["secretID"].as_str().unwrap().to_owned()),
		);
		token = headers
			.get("x-next-token")
			.map(|v| v.to_str().unwrap().to_owned());
		if token.is_none() {
			break;
		}
	}
	assert_eq!(pages, vec![2, 2, 1]);
	let mut unique = seen.clone();
	unique.sort();
	unique.dedup();
	assert_eq!(unique.len(), 5, "pages must not overlap: {seen:?}");
	let (status, ..) = app.json(req("GET", "/secrets?limit=0", &auth, None)).await;
	assert_eq!(status, StatusCode::BAD_REQUEST);
	let (status, ..) = app
		.json(req("GET", "/secrets?nextToken=garbage!", &auth, None))
		.await;
	assert_eq!(status, StatusCode::BAD_REQUEST);

	let (status, ..) = app
		.call(req("DELETE", &format!("/secrets/{id}"), &auth, None))
		.await;
	assert_eq!(status, StatusCode::NO_CONTENT);
	let (status, ..) = app
		.call(req("DELETE", &format!("/secrets/{id}"), &auth, None))
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	app.finish().await;
}

#[tokio::test]
async fn volume_tokens_are_scoped_to_one_volume() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, key) = app.team("Vols").await;
	let missing_node = NodeId(0xdead_beef).to_string();
	let a = app
		.state
		.store
		.insert_volume(&team.id, "a", Some(&missing_node), false)
		.await
		.unwrap();
	let b = app
		.state
		.store
		.insert_volume(&team.id, "b", Some(&missing_node), false)
		.await
		.unwrap();

	let (status, _, va) = app
		.json(req("GET", &format!("/volumes/{}", a.id), &[("x-api-key", &key)], None))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(va["name"], "a");
	let token = format!("Bearer {}", va["token"].as_str().unwrap());

	let (status, _, body) = app
		.json(req(
			"GET",
			&format!("/volumecontent/{}/path?path=/", b.id),
			&[("authorization", &token)],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::FORBIDDEN);
	assert!(body["code"].is_string(), "volume-content errors use string codes");
	let (status, ..) = app
		.json(req("GET", &format!("/volumecontent/{}/path?path=/", a.id), &[], None))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED);
	let access = format!("Bearer {}", app.state.keys.access_token("u", "e", 60));
	let (status, ..) = app
		.json(req(
			"GET",
			&format!("/volumecontent/{}/path?path=/", a.id),
			&[("authorization", &access)],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::UNAUTHORIZED, "user access tokens are not volume tokens");
	// The right token passes auth and reaches placement (no node is alive).
	let (status, ..) = app
		.json(req(
			"GET",
			&format!("/volumecontent/{}/path?path=/", a.id),
			&[("authorization", &token)],
			None,
		))
		.await;
	assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

	// Other teams cannot read the volume record.
	let (_, other_key) = app.team("Other").await;
	let (status, ..) = app
		.json(req("GET", &format!("/volumes/{}", a.id), &[("x-api-key", &other_key)], None))
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	let (status, ..) = app
		.json(req("POST", "/volumes", &[("x-api-key", &key)], Some(json!({"name": "c"}))))
		.await;
	assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "no node can host a new volume");
	assert!(
		app.state
			.store
			.volume_by_name(&team.id, "c")
			.await
			.unwrap()
			.is_none()
	);
	app.finish().await;
}

#[tokio::test]
async fn webhook_delivery_is_signed_and_recorded() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, key) = app.team("Hooks").await;
	let auth = [("x-api-key", key.as_str())];

	let seen = Arc::new(Mutex::new(Vec::<(HeaderMap, Bytes)>::new()));
	let sink = seen.clone();
	let receiver = Router::new().route(
		"/hook",
		post(move |headers: HeaderMap, body: Bytes| {
			let sink = sink.clone();
			async move {
				sink.lock().push((headers, body));
				StatusCode::NO_CONTENT
			}
		}),
	);
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("http://{}/hook", listener.local_addr().unwrap());
	tokio::spawn(async move { axum::serve(listener, receiver).await.unwrap() });

	let event = |sandbox_id: &'static str, event_type: &'static str| NewSandboxEvent {
		team_id: &team.id,
		sandbox_id,
		execution_id: "exec",
		template_id: "tpl",
		build_id: "bld",
		event_type,
		data: json!({"kill_reason": "request"}),
	};
	// Events before registration are not delivered.
	app.state
		.store
		.record_event(&event("iold", event_types::KILLED))
		.await
		.unwrap();

	let (status, _, hook) = app
		.json(req(
			"POST",
			"/events/webhooks",
			&auth,
			Some(
				json!({"name": "h", "url": url, "events": [event_types::KILLED], "signatureSecret": "shh"}),
			),
		))
		.await;
	assert_eq!(status, StatusCode::CREATED);
	let hook_id = hook["id"].as_str().unwrap().to_owned();

	app.state
		.store
		.record_event(&event("inew", event_types::CREATED))
		.await
		.unwrap();
	app.state
		.store
		.record_event(&event("inew", event_types::KILLED))
		.await
		.unwrap();
	crate::webhooks::deliver_pending(&app.state).await.unwrap();
	crate::webhooks::deliver_pending(&app.state).await.unwrap();

	let received = seen.lock().clone();
	assert_eq!(received.len(), 1, "only the subscribed post-registration event, exactly once");
	let (headers, body) = &received[0];
	let signature = headers.get("e2b-signature").unwrap().to_str().unwrap();
	assert_eq!(signature, crypto::webhook_signature("shh", body));
	assert_ne!(signature, crypto::webhook_signature("other", body));
	let payload: Value = serde_json::from_slice(body).unwrap();
	assert_eq!(
		(payload["sandbox_id"].as_str(), payload["type"].as_str()),
		(Some("inew"), Some(event_types::KILLED))
	);

	let (status, _, deliveries) = app
		.json(req("GET", &format!("/events/webhooks/{hook_id}/deliveries"), &auth, None))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(deliveries["data"].as_array().unwrap().len(), 1);
	assert_eq!(deliveries["data"][0]["attempts"][0]["status"], "success");
	assert!(deliveries["nextCursor"].is_null());
	let (_, _, stats) = app
		.json(req("GET", &format!("/events/webhooks/{hook_id}/stats"), &auth, None))
		.await;
	assert_eq!((stats["total"].as_i64(), stats["failed"].as_i64()), (Some(1), Some(0)));

	// Events API sees all three, newest first, filterable by type.
	let (_, _, events) = app
		.json(req("GET", "/events/sandboxes?limit=100", &auth, None))
		.await;
	assert_eq!(events.as_array().unwrap().len(), 3);
	let (_, _, killed) = app
		.json(req(
			"GET",
			&format!("/events/sandboxes/inew?types={}", event_types::KILLED),
			&auth,
			None,
		))
		.await;
	assert_eq!(killed.as_array().unwrap().len(), 1);
	assert_eq!(killed[0]["sandboxTeamId"], team.id.as_str());

	// Disabled webhooks stop receiving; delete returns 200 then 404.
	app.json(req(
		"PATCH",
		&format!("/events/webhooks/{hook_id}"),
		&auth,
		Some(json!({"enabled": false})),
	))
	.await;
	app.state
		.store
		.record_event(&event("inew2", event_types::KILLED))
		.await
		.unwrap();
	crate::webhooks::deliver_pending(&app.state).await.unwrap();
	assert_eq!(seen.lock().len(), 1);
	let (status, ..) = app
		.call(req("DELETE", &format!("/events/webhooks/{hook_id}"), &auth, None))
		.await;
	assert_eq!(status, StatusCode::OK);
	let (status, ..) = app
		.call(req("DELETE", &format!("/events/webhooks/{hook_id}"), &auth, None))
		.await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	app.finish().await;
}

#[tokio::test]
async fn team_metrics_follow_lifecycle_events() {
	let Some(app) = TestApp::start().await else {
		return;
	};
	let (team, key) = app.team("Metrics").await;
	for (id, ty) in
		[("i1", event_types::CREATED), ("i2", event_types::CREATED), ("i1", event_types::KILLED)]
	{
		app.state
			.store
			.record_event(&NewSandboxEvent {
				team_id:      &team.id,
				sandbox_id:   id,
				execution_id: "e",
				template_id:  "t",
				build_id:     "b",
				event_type:   ty,
				data:         Value::Null,
			})
			.await
			.unwrap();
	}
	let auth = [("x-api-key", key.as_str())];
	let (status, _, max) = app
		.json(req(
			"GET",
			&format!("/teams/{}/metrics/max?metric=concurrent_sandboxes", team.id),
			&auth,
			None,
		))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(max["value"], 2.0);
	let (status, ..) = app
		.json(req("GET", &format!("/teams/{}/metrics/max?metric=bogus", team.id), &auth, None))
		.await;
	assert_eq!(status, StatusCode::BAD_REQUEST);
	let (other, _) = app.team("Other").await;
	let (status, ..) = app
		.json(req("GET", &format!("/teams/{}/metrics", other.id), &auth, None))
		.await;
	assert_eq!(status, StatusCode::FORBIDDEN);
	app.finish().await;
}
