//! Service tests on a private one-voter cluster KV: what `@actions/cache`
//! v4 sends (Twirp JSON with proto field names, Azure block uploads, plain
//! and ranged downloads), scoping, immutability, and the pass-through.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::body::Body;
use chrono::Utc;
use http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use parking_lot::Mutex;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use vmon_cluster::{Cluster, ClusterConfig, MemberMeta, NodeId, Roles};

use super::{
	CacheConfig, PING_PATH, Service,
	blobs::BlobStore,
	index,
	records::{CacheEntryRow, CacheGrant, CacheScope, EntryState},
};

const LOCAL: &str = "172.20.0.1:7948";
const CACHE: &str = "/twirp/github.actions.results.api.v1.CacheService/";
const REPO: u64 = 42;
const MAIN: &str = "refs/heads/main";
const FEAT: &str = "refs/heads/feat";
const PR: &str = "refs/pull/7/merge";
const OTHER: &str = "refs/heads/other";

struct Harness {
	service: Arc<Service>,
	cluster: Cluster,
	_dir:    tempfile::TempDir,
}

async fn start_cluster(dir: &tempfile::TempDir) -> Cluster {
	let cluster = Cluster::start(
		ClusterConfig {
			id:               NodeId::random(),
			data_dir:         dir.path().join("cluster"),
			roles:            Roles { node: true, driver: true },
			gossip_bind:      "127.0.0.1:0".parse().expect("socket addr"),
			advertise:        None,
			cluster_port:     1,
			join:             Vec::new(),
			secret:           b"test-secret".to_vec(),
			bootstrap_expect: 1,
			raft_reap_after:  Duration::from_mins(10),
		},
		MemberMeta { version: "test".into(), ..MemberMeta::default() },
	)
	.await
	.expect("start test cluster");
	let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
	while !cluster.is_leader() {
		assert!(tokio::time::Instant::now() < deadline, "test cluster never led");
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	cluster
}

fn scope(write: &str, read: &[&str]) -> CacheScope {
	CacheScope {
		repo_id: REPO,
		repo:    "acme/app".into(),
		write:   write.into(),
		read:    read.iter().map(|r| (*r).to_owned()).collect(),
	}
}

impl Harness {
	async fn start(upstream: &str, budget: u64) -> Self {
		let dir = tempfile::tempdir().expect("temp dir");
		let cluster = start_cluster(&dir).await;
		let store = BlobStore::open(&dir.path().join("ci-cache"), budget).expect("blob store");
		let service = Service::with(
			CacheConfig {
				dir: dir.path().join("ci-cache"),
				budget,
				upstream: upstream.parse().expect("upstream"),
				secret: "test-secret".into(),
				http: reqwest::Client::new(),
			},
			store,
			cluster.kv.clone(),
			cluster.local_id(),
			None,
		);
		let harness = Self { service, cluster, _dir: dir };
		// push to main, push to a feature branch, a pull request into the
		// feature branch, and a push to an unrelated branch.
		for (sandbox, scope) in [
			("main", scope(MAIN, &[MAIN])),
			("feat", scope(FEAT, &[FEAT, MAIN])),
			("pr", scope(PR, &[PR, FEAT, MAIN])),
			("other", scope(OTHER, &[OTHER, MAIN])),
		] {
			harness.grant(sandbox, Some(scope)).await;
		}
		harness
	}

	async fn grant(&self, sandbox: &str, scope: Option<CacheScope>) {
		let grant = CacheGrant { runner: format!("vmon-1-{sandbox}"), job_id: Some(1), scope };
		index::put_grant(&self.cluster.kv, sandbox, &grant, Duration::from_hours(1))
			.await
			.expect("grant");
	}

	async fn send(
		&self,
		sandbox: &str,
		req: Request<Body>,
	) -> (StatusCode, http::HeaderMap, Vec<u8>) {
		let local: SocketAddr = LOCAL.parse().expect("addr");
		let response = self.service.handle(req, sandbox, local).await;
		let (parts, body) = response.into_parts();
		let bytes = body.collect().await.expect("body").to_bytes().to_vec();
		(parts.status, parts.headers, bytes)
	}

	async fn twirp(&self, sandbox: &str, method: &str, body: &Value) -> (StatusCode, Value) {
		let req = Request::post(format!("{CACHE}{method}"))
			.header("content-type", "application/json")
			.header("authorization", "Bearer runtime-token")
			.body(Body::from(body.to_string()))
			.expect("request");
		let (status, _, bytes) = self.send(sandbox, req).await;
		(status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
	}

	/// Path and query of a signed URL (the service answers for `LOCAL`).
	fn target(url: &str) -> String {
		let rest = url
			.strip_prefix(&format!("http://{LOCAL}"))
			.unwrap_or_else(|| panic!("signed URL on another host: {url}"));
		rest.to_owned()
	}

	/// `saveCache`: reserve, upload (single shot, or blocks of `block`
	/// bytes), finalize. `Err(status)` when the reservation is refused.
	async fn save(
		&self,
		sandbox: &str,
		key: &str,
		version: &str,
		data: &[u8],
		block: Option<usize>,
	) -> Result<u64, StatusCode> {
		let (status, created) = self
			.twirp(sandbox, "CreateCacheEntry", &json!({"key": key, "version": version}))
			.await;
		if status != StatusCode::OK {
			return Err(status);
		}
		assert_eq!(created["ok"], true, "{created}");
		let target = Self::target(created["signed_upload_url"].as_str().expect("upload url"));
		match block {
			None => {
				let req = Request::put(&target)
					.header("x-ms-blob-type", "BlockBlob")
					.body(Body::from(data.to_vec()))
					.expect("request");
				let (status, headers, _) = self.send(sandbox, req).await;
				assert_eq!(status, StatusCode::CREATED);
				assert!(headers.contains_key("x-ms-request-id"));
			},
			Some(size) => {
				let mut ids = Vec::new();
				for (n, chunk) in data.chunks(size).enumerate() {
					let id = base64::Engine::encode(
						&base64::engine::general_purpose::STANDARD,
						format!("block-{n:06}"),
					);
					let query = url::form_urlencoded::Serializer::new(String::new())
						.append_pair("comp", "block")
						.append_pair("blockid", &id)
						.finish();
					let req = Request::put(format!("{target}&{query}"))
						.body(Body::from(chunk.to_vec()))
						.expect("request");
					assert_eq!(self.send(sandbox, req).await.0, StatusCode::CREATED);
					ids.push(id);
				}
				let latest: String = ids
					.iter()
					.flat_map(|id| ["<Latest>", id.as_str(), "</Latest>"])
					.collect();
				let xml = format!(
					"<?xml version=\"1.0\" encoding=\"UTF-8\" \
					 standalone=\"yes\"?><BlockList>{latest}</BlockList>"
				);
				let req = Request::put(format!("{target}&comp=blocklist"))
					.body(Body::from(xml))
					.expect("request");
				assert_eq!(self.send(sandbox, req).await.0, StatusCode::CREATED);
			},
		}
		let (status, finalized) = self
			.twirp(
				sandbox,
				"FinalizeCacheEntryUpload",
				&json!({
					"key": key, "version": version, "size_bytes": data.len().to_string()
				}),
			)
			.await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(finalized["ok"], true, "{finalized}");
		Ok(finalized["entry_id"]
			.as_str()
			.expect("entry id as a string")
			.parse()
			.expect("id"))
	}

	/// `restoreCache`: the matched key and the archive.
	async fn restore(
		&self,
		sandbox: &str,
		key: &str,
		restore_keys: &[&str],
		version: &str,
	) -> Option<(String, Vec<u8>)> {
		let (status, found) = self
			.twirp(
				sandbox,
				"GetCacheEntryDownloadURL",
				&json!({
					"key": key, "restore_keys": restore_keys, "version": version
				}),
			)
			.await;
		assert_eq!(status, StatusCode::OK, "{found}");
		if found["ok"] != true {
			return None;
		}
		let target = Self::target(found["signed_download_url"].as_str().expect("download url"));
		let req = Request::get(&target).body(Body::empty()).expect("request");
		let (status, headers, bytes) = self.send(sandbox, req).await;
		assert_eq!(status, StatusCode::OK);
		assert_eq!(headers["content-length"], bytes.len().to_string().as_str());
		Some((
			found["matched_key"]
				.as_str()
				.expect("matched key")
				.to_owned(),
			bytes,
		))
	}

	async fn entries(&self) -> Vec<CacheEntryRow> {
		index::entries(&self.cluster.kv, Some(REPO))
			.await
			.expect("entries")
	}
}

#[tokio::test(flavor = "multi_thread")]
async fn lookup_follows_scopes_keys_and_versions() {
	let h = Harness::start("http://127.0.0.1:9/", 1 << 30).await;
	h.save("main", "cargo-linux-aaa", "v1", b"main-a", None)
		.await
		.unwrap();
	tokio::time::sleep(Duration::from_millis(5)).await;
	h.save("main", "cargo-linux-bbb", "v1", b"main-b", Some(2))
		.await
		.unwrap();
	h.save("feat", "cargo-linux-aaa", "v1", b"feat-a", None)
		.await
		.unwrap();
	h.save("other", "other-key", "v1", b"other", None)
		.await
		.unwrap();

	// Exact key: the job's own scope first, then the default branch.
	assert_eq!(
		h.restore("feat", "cargo-linux-aaa", &[], "v1")
			.await
			.unwrap()
			.1,
		b"feat-a"
	);
	assert_eq!(
		h.restore("main", "cargo-linux-aaa", &[], "v1")
			.await
			.unwrap()
			.1,
		b"main-a"
	);
	// The block upload assembled in order.
	assert_eq!(
		h.restore("feat", "cargo-linux-bbb", &[], "v1")
			.await
			.unwrap()
			.1,
		b"main-b"
	);
	// A version mismatch never matches.
	assert!(
		h.restore("main", "cargo-linux-aaa", &["cargo-"], "v2")
			.await
			.is_none()
	);
	// Restore keys are prefixes, newest entry first.
	let (matched, data) = h
		.restore("main", "cargo-linux-ccc", &["nothing-", "cargo-linux-"], "v1")
		.await
		.unwrap();
	assert_eq!((matched.as_str(), data.as_slice()), ("cargo-linux-bbb", b"main-b".as_slice()));
	// Within a scope's search, the exact key wins over prefixes, and each
	// scope is searched completely before the next: `feat`'s own prefix
	// match beats main's newer entry.
	let (matched, data) = h
		.restore("feat", "cargo-linux-zzz", &["cargo-linux-"], "v1")
		.await
		.unwrap();
	assert_eq!((matched.as_str(), data.as_slice()), ("cargo-linux-aaa", b"feat-a".as_slice()));
	// A push never reads another branch.
	assert!(
		h.restore("feat", "other-key", &["other"], "v1")
			.await
			.is_none()
	);
	assert!(h.restore("main", "other-key", &[], "v1").await.is_none());
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pull_requests_read_base_and_default_but_write_only_their_ref() {
	let h = Harness::start("http://127.0.0.1:9/", 1 << 30).await;
	h.save("main", "deps", "v1", b"from-main", None)
		.await
		.unwrap();
	h.save("feat", "base-only", "v1", b"from-feat", None)
		.await
		.unwrap();
	assert_eq!(h.restore("pr", "deps", &[], "v1").await.unwrap().1, b"from-main");
	assert_eq!(h.restore("pr", "base-only", &[], "v1").await.unwrap().1, b"from-feat");
	// Saving `deps` from the PR creates a PR-scoped entry, never touching
	// main's (which exists) or the base's.
	h.save("pr", "deps", "v1", b"from-pr", None).await.unwrap();
	h.save("pr", "pr-only", "v1", b"pr", None).await.unwrap();
	let mut scopes: Vec<_> = h
		.entries()
		.await
		.into_iter()
		.filter(|row| row.key != "base-only")
		.map(|row| (row.key, row.scope))
		.collect();
	scopes.sort();
	assert_eq!(scopes, vec![
		("deps".to_owned(), MAIN.to_owned()),
		("deps".to_owned(), PR.to_owned()),
		("pr-only".to_owned(), PR.to_owned()),
	]);
	assert_eq!(h.restore("pr", "deps", &[], "v1").await.unwrap().1, b"from-pr");
	assert_eq!(h.restore("main", "deps", &[], "v1").await.unwrap().1, b"from-main");
	assert!(h.restore("main", "pr-only", &[], "v1").await.is_none());
	assert!(h.restore("feat", "pr-only", &["pr-"], "v1").await.is_none());
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn entries_are_immutable_and_uploads_are_checked() {
	let h = Harness::start("http://127.0.0.1:9/", 1 << 30).await;
	h.save("main", "k", "v1", b"first", None).await.unwrap();
	// A second reservation of the same (scope, key, version): 409 like
	// GitHub (`already_exists`); the toolkit reports it and moves on.
	let (status, body) = h
		.twirp("main", "CreateCacheEntry", &json!({"key": "k", "version": "v1"}))
		.await;
	assert_eq!((status, body["code"].as_str()), (StatusCode::CONFLICT, Some("already_exists")));
	// While an upload is pending the key is reserved too.
	let (status, created) = h
		.twirp("main", "CreateCacheEntry", &json!({"key": "k2", "version": "v1"}))
		.await;
	assert_eq!(status, StatusCode::OK);
	assert_eq!(
		h.twirp("main", "CreateCacheEntry", &json!({"key": "k2", "version": "v1"}))
			.await
			.0,
		StatusCode::CONFLICT
	);
	// Another sandbox cannot use the upload URL; a wrong size is refused and
	// releases the reservation.
	let target = Harness::target(created["signed_upload_url"].as_str().unwrap());
	let put = |body: &'static [u8]| Request::put(&target).body(Body::from(body)).unwrap();
	assert_eq!(h.send("feat", put(b"x")).await.0, StatusCode::FORBIDDEN);
	assert_eq!(h.send("main", put(b"12345")).await.0, StatusCode::CREATED);
	let (_, finalized) = h
		.twirp(
			"main",
			"FinalizeCacheEntryUpload",
			&json!({"key": "k2", "version": "v1", "sizeBytes": 4}),
		)
		.await;
	assert_eq!(finalized["ok"], false, "{finalized}");
	assert!(h.entries().await.iter().all(|row| row.key != "k2"));
	// The same key in another scope or version is a different entry.
	h.save("feat", "k", "v1", b"feat", None).await.unwrap();
	h.save("main", "k", "v2", b"v2", None).await.unwrap();
	// Re-uploading the same bytes shares one archive.
	h.save("other", "k", "v1", b"first", None).await.unwrap();
	let digest = hex::encode(Sha256::digest(b"first"));
	let blob = index::blob(&h.cluster.kv, &digest).await.unwrap().unwrap();
	assert_eq!(blob.entries.len(), 2);
	assert!(
		h.entries()
			.await
			.iter()
			.all(|row| row.state == EntryState::Ready)
	);
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_serve_ranges_and_only_to_their_sandbox() {
	let h = Harness::start("http://127.0.0.1:9/", 1 << 30).await;
	h.save("main", "k", "v1", b"0123456789", None)
		.await
		.unwrap();
	let (_, found) = h
		.twirp(
			"feat",
			"GetCacheEntryDownloadURL",
			&json!({"key": "k", "restoreKeys": [], "version": "v1"}),
		)
		.await;
	let target = Harness::target(found["signed_download_url"].as_str().unwrap());
	let get = |range: Option<(&str, &str)>| {
		let mut req = Request::get(&target);
		if let Some((name, value)) = range {
			req = req.header(name, value);
		}
		req.body(Body::empty()).unwrap()
	};
	let (status, headers, body) = h.send("feat", get(Some(("x-ms-range", "bytes=2-5")))).await;
	assert_eq!((status, body.as_slice()), (StatusCode::PARTIAL_CONTENT, b"2345".as_slice()));
	assert_eq!(headers["content-range"], "bytes 2-5/10");
	let (status, _, body) = h.send("feat", get(Some(("range", "bytes=7-")))).await;
	assert_eq!((status, body.as_slice()), (StatusCode::PARTIAL_CONTENT, b"789".as_slice()));
	let (status, _, body) = h.send("feat", get(Some(("range", "bytes=8-100")))).await;
	assert_eq!((status, body.as_slice()), (StatusCode::PARTIAL_CONTENT, b"89".as_slice()));
	assert_eq!(
		h.send("feat", get(Some(("range", "bytes=10-")))).await.0,
		StatusCode::RANGE_NOT_SATISFIABLE
	);
	let head = Request::head(&target).body(Body::empty()).unwrap();
	let (status, headers, body) = h.send("feat", head).await;
	assert_eq!((status, body.len()), (StatusCode::OK, 0));
	assert_eq!(headers["content-length"], "10");
	// The URL is bound to the sandbox it was issued to.
	assert_eq!(h.send("main", get(None)).await.0, StatusCode::FORBIDDEN);
	let forged = Request::get(format!("{}?sig=0000", target.split('?').next().unwrap()))
		.body(Body::empty())
		.unwrap();
	assert_eq!(h.send("feat", forged).await.0, StatusCode::FORBIDDEN);
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn non_ci_sandboxes_are_refused_and_other_calls_pass_through() {
	// A fake results service that records what it received.
	let seen: Arc<Mutex<Vec<(String, String, String)>>> = Arc::default();
	let app = {
		let seen = Arc::clone(&seen);
		axum::Router::new().fallback(move |req: Request<Body>| {
			let seen = Arc::clone(&seen);
			async move {
				let auth = req
					.headers()
					.get("authorization")
					.and_then(|v| v.to_str().ok())
					.unwrap_or_default()
					.to_owned();
				let path = req.uri().path().to_owned();
				let body = req.into_body().collect().await.unwrap().to_bytes();
				seen
					.lock()
					.push((path, auth, String::from_utf8_lossy(&body).into_owned()));
				(
					StatusCode::OK,
					[("content-type", "application/json")],
					r#"{"ok":true,"signed_upload_url":"https://example.blob.core.windows.net/x"}"#,
				)
			}
		})
	};
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let upstream = format!("http://{}/", listener.local_addr().unwrap());
	tokio::spawn(async move { axum::serve(listener, app).await });
	let h = Harness::start(&upstream, 1 << 30).await;

	let artifact = || {
		Request::post("/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact")
			.header("content-type", "application/json")
			.header("authorization", "Bearer job-token")
			.body(Body::from(r#"{"name":"dist"}"#))
			.unwrap()
	};
	let (status, _, body) = h.send("feat", artifact()).await;
	assert_eq!(status, StatusCode::OK);
	assert!(String::from_utf8_lossy(&body).contains("example.blob.core.windows.net"));
	assert_eq!(seen.lock().clone(), vec![(
		"/twirp/github.actions.results.api.v1.ArtifactService/CreateArtifact".to_owned(),
		"Bearer job-token".to_owned(),
		r#"{"name":"dist"}"#.to_owned(),
	)]);

	// No grant: not a CI runner sandbox.
	let (status, body) = h
		.twirp("web", "GetCacheEntryDownloadURL", &json!({"key": "k", "version": "v"}))
		.await;
	assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("permission_denied")));
	assert_eq!(h.send("web", artifact()).await.0, StatusCode::FORBIDDEN);
	assert_eq!(seen.lock().len(), 1, "nothing forwarded for a non-CI sandbox");
	// The start command's probe answers anyone.
	let ping = Request::get(PING_PATH).body(Body::empty()).unwrap();
	assert_eq!(h.send("web", ping).await.0, StatusCode::OK);
	// Unknown cache methods are not forwarded either.
	let (status, _) = h.twirp("feat", "DeleteCacheEntry", &json!({})).await;
	assert_eq!(status, StatusCode::NOT_FOUND);
	assert_eq!(seen.lock().len(), 1);
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sweep_expires_unused_entries_and_enforces_repository_budgets() {
	let h = Harness::start("http://127.0.0.1:9/", 1 << 30).await;
	h.save("main", "old", "v1", b"aaaa", None).await.unwrap();
	h.save("main", "lru", "v1", b"bbbb", None).await.unwrap();
	h.save("main", "fresh", "v1", b"cccc", None).await.unwrap();
	// `old` was last used 8 days ago, `lru` 2 days ago.
	let kv = &h.cluster.kv;
	for (key, days) in [("old", 8), ("lru", 2)] {
		let entry_key = super::records::entry_key(REPO, MAIN, key, "v1");
		let mut row: CacheEntryRow =
			serde_json::from_slice(&kv.get(&entry_key).await.unwrap().unwrap().value).unwrap();
		row.last_used_at = Utc::now() - chrono::Duration::days(days);
		kv.put(&entry_key, serde_json::to_vec(&row).unwrap(), None)
			.await
			.unwrap();
	}
	// Budget 5 bytes: `old` expires; of the remaining 8 bytes `lru` goes.
	let swept = index::sweep(kv, Utc::now(), 5).await.unwrap();
	assert_eq!(swept, index::Swept { expired: 1, evicted: 1, bytes: 8 });
	let keys: Vec<_> = h.entries().await.into_iter().map(|row| row.key).collect();
	assert_eq!(keys, vec!["fresh"]);
	// The archives of deleted entries lose their records; the node's sweep
	// then drops the local copies.
	assert!(
		index::blob(kv, &hex::encode(Sha256::digest(b"aaaa")))
			.await
			.unwrap()
			.is_none()
	);
	h.service.gc().await;
	assert_eq!(h.service.store_digests(), vec![hex::encode(Sha256::digest(b"cccc"))]);
	h.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn evicting_the_last_copy_drops_its_entries() {
	// Budget 6 bytes: the second archive pushes the first out.
	let h = Harness::start("http://127.0.0.1:9/", 6).await;
	h.save("main", "a", "v1", b"aaaa", None).await.unwrap();
	tokio::time::sleep(Duration::from_millis(5)).await;
	h.save("main", "b", "v1", b"bbbb", None).await.unwrap();
	let keys: Vec<_> = h.entries().await.into_iter().map(|row| row.key).collect();
	assert_eq!(keys, vec!["b"]);
	assert!(h.restore("main", "a", &[], "v1").await.is_none());
	h.cluster.shutdown().await;
}
