//! The guest-facing HTTP service: Twirp cache calls, the signed blob URLs,
//! and the pass-through to GitHub's results service.

use std::{
	collections::HashMap,
	io::SeekFrom,
	net::SocketAddr,
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};

use anyhow::Context as _;
use axum::{
	Router,
	body::{Body, Bytes},
	extract::{Path, State},
	response::{IntoResponse as _, Response},
	routing::get,
};
use chrono::Utc;
use futures::{StreamExt as _, TryStreamExt as _};
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use http_body_util::BodyExt as _;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::{
	io::{AsyncReadExt as _, AsyncSeekExt as _, AsyncWriteExt as _},
	sync::watch,
};
use tokio_util::io::ReaderStream;
use url::Url;
use vmon_cluster::{Cluster, NodeId, kv::Kv};

use super::{
	CacheConfig, LocalService, PING_PATH,
	blobs::{BlobStore, Pin},
	index::{self, REPO_BUDGET, Reserved},
	records::{CacheGrant, CacheScope, entry_key, valid_digest},
	upload::{BlobError, Uploads},
};
use crate::{client::NodeClient, server::ApiError};

/// Twirp prefix of every results-service call.
const TWIRP: &str = "/twirp/";
/// The cache service the toolkit talks to.
const CACHE_SERVICE: &str = "github.actions.results.api.v1.CacheService";
/// Path prefix of the signed blob URLs (`<account>/<container>/<blob>` for
/// the Azure SDKs' IP-style URL parsing).
const BLOB_PATH: &str = "/vmon/cache/";
/// How long a cache call waits for the driver to resolve the job's scope.
const SCOPE_WAIT: Duration = Duration::from_secs(20);
/// Lifetime of a signed download URL.
const DOWNLOAD_TTL: Duration = Duration::from_hours(1);
/// Longest key or version accepted (GitHub's key limit).
const MAX_KEY: usize = 512;
/// Largest Twirp or block-list body read into memory.
const MAX_SMALL_BODY: usize = 4 << 20;
/// Timeout of a pass-through call.
const UPSTREAM_TIMEOUT: Duration = Duration::from_mins(5);
/// Azure REST version reported on blob responses.
const MS_VERSION: &str = "2025-01-05";

/// A signed download URL.
#[derive(Clone, Debug)]
struct Download {
	sandbox: String,
	digest:  String,
	size:    u64,
	expires: Instant,
}

/// Progress of a peer fetch.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Fetch {
	/// Bytes written to `path` so far.
	Running(u64),
	/// Installed as a local copy.
	Done,
	Failed(String),
}

/// A peer fetch in progress: its temp file and progress.
#[derive(Clone)]
struct FetchHandle {
	path:     PathBuf,
	progress: watch::Receiver<Fetch>,
}

/// The node's cache service (one per node).
pub struct Service {
	kv:        Kv,
	cluster:   Option<Cluster>,
	node:      NodeId,
	store:     Arc<BlobStore>,
	uploads:   Uploads,
	downloads: Mutex<HashMap<String, Download>>,
	fetches:   Mutex<HashMap<String, FetchHandle>>,
	upstream:  Url,
	secret:    String,
	http:      reqwest::Client,
}

/// Twirp error response.
fn twirp_error(status: StatusCode, code: &str, msg: impl Into<String>) -> Response {
	#[derive(Serialize)]
	struct Error<'a> {
		code: &'a str,
		msg:  String,
	}
	(status, axum::Json(Error { code, msg: msg.into() })).into_response()
}

/// An error response of a Twirp handler (boxed: responses are large).
struct Fail(Box<Response>);

impl From<Response> for Fail {
	fn from(response: Response) -> Self {
		Self(Box::new(response))
	}
}

fn json<T: Serialize>(value: &T) -> Response {
	axum::Json(value).into_response()
}

/// Azure-style blob response headers.
fn blob_headers(headers: &mut HeaderMap) {
	headers.insert("x-ms-version", HeaderValue::from_static(MS_VERSION));
	if let Ok(id) = HeaderValue::from_str(&uuid::Uuid::new_v4().to_string()) {
		headers.insert("x-ms-request-id", id);
	}
}

impl BlobError {
	fn into_response(self) -> Response {
		let body = format!(
			"<?xml version=\"1.0\" \
			 encoding=\"utf-8\"?><Error><Code>{}</Code><Message>{}</Message></Error>",
			self.code,
			self.message.replace('&', "&amp;").replace('<', "&lt;")
		);
		let mut response = (self.status, body).into_response();
		let headers = response.headers_mut();
		headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/xml"));
		headers.insert("x-ms-error-code", HeaderValue::from_static(self.code));
		blob_headers(headers);
		response
	}
}

/// An int64 as protobuf JSON writes it (a string) or as Go clients do (a
/// number).
fn int64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
	#[derive(Deserialize)]
	#[serde(untagged)]
	enum Int {
		Number(u64),
		Text(String),
	}
	match Int::deserialize(deserializer)? {
		Int::Number(n) => Ok(n),
		Int::Text(text) => text.parse().map_err(serde::de::Error::custom),
	}
}

#[derive(Debug, Deserialize)]
struct CreateRequest {
	key:     String,
	version: String,
}

#[derive(Debug, Deserialize)]
struct FinalizeRequest {
	key:        String,
	version:    String,
	#[serde(alias = "sizeBytes", deserialize_with = "int64")]
	size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct DownloadRequest {
	key:          String,
	#[serde(default, alias = "restoreKeys")]
	restore_keys: Vec<String>,
	version:      String,
}

#[derive(Debug, Default, Serialize)]
struct CreateResponse {
	ok:                bool,
	#[serde(skip_serializing_if = "String::is_empty")]
	signed_upload_url: String,
	#[serde(skip_serializing_if = "String::is_empty")]
	message:           String,
}

#[derive(Debug, Default, Serialize)]
struct FinalizeResponse {
	ok:       bool,
	/// int64 → JSON string (protobuf JSON; Go clients decode a string).
	#[serde(skip_serializing_if = "String::is_empty")]
	entry_id: String,
	#[serde(skip_serializing_if = "String::is_empty")]
	message:  String,
}

#[derive(Debug, Default, Serialize)]
struct DownloadResponse {
	ok:                  bool,
	#[serde(skip_serializing_if = "String::is_empty")]
	signed_download_url: String,
	#[serde(skip_serializing_if = "String::is_empty")]
	matched_key:         String,
}

/// A requested byte range: `bytes=a-b` / `bytes=a-` from `x-ms-range`
/// (the Azure SDKs) or `Range`.
fn requested_range(headers: &HeaderMap) -> Option<(u64, Option<u64>)> {
	let value = headers
		.get("x-ms-range")
		.or_else(|| headers.get(header::RANGE))?
		.to_str()
		.ok()?;
	let spec = value.trim().strip_prefix("bytes=")?;
	let (start, end) = spec.split_once('-')?;
	let start = start.trim().parse().ok()?;
	let end = match end.trim() {
		"" => None,
		end => Some(end.parse().ok()?),
	};
	Some((start, end))
}

/// The satisfiable `[start, end]` of a request for a `size`-byte blob:
/// `Ok(None)` = the whole blob, `Err` = 416.
fn resolve_range(
	requested: Option<(u64, Option<u64>)>,
	size: u64,
) -> Result<Option<(u64, u64)>, ()> {
	let Some((start, end)) = requested else {
		return Ok(None);
	};
	if start >= size {
		return Err(());
	}
	let end = end.map_or(size - 1, |end| end.min(size - 1));
	if end < start {
		return Err(());
	}
	Ok(Some((start, end)))
}

/// Query parameters of a blob URL.
fn query_params(uri: &http::Uri) -> HashMap<String, String> {
	uri.query()
		.map(|query| {
			url::form_urlencoded::parse(query.as_bytes())
				.into_owned()
				.collect()
		})
		.unwrap_or_default()
}

async fn small_body(body: Body) -> Result<Bytes, Fail> {
	match http_body_util::Limited::new(body, MAX_SMALL_BODY)
		.collect()
		.await
	{
		Ok(collected) => Ok(collected.to_bytes()),
		Err(error) => {
			Err(twirp_error(StatusCode::BAD_REQUEST, "malformed", error.to_string()).into())
		},
	}
}

fn parse<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, Fail> {
	serde_json::from_slice(body)
		.map_err(|error| twirp_error(StatusCode::BAD_REQUEST, "malformed", error.to_string()).into())
}

fn check_key(key: &str, what: &str) -> Result<(), Fail> {
	if key.is_empty() || key.len() > MAX_KEY {
		return Err(
			twirp_error(
				StatusCode::BAD_REQUEST,
				"invalid_argument",
				format!("{what} must be 1 to {MAX_KEY} bytes"),
			)
			.into(),
		);
	}
	Ok(())
}

impl Service {
	/// The service over `config` (blobs under `config.dir`).
	pub fn new(config: CacheConfig, cluster: Cluster) -> anyhow::Result<Arc<Self>> {
		let store = BlobStore::open(&config.dir, config.budget)
			.with_context(|| format!("opening the CI cache at {}", config.dir.display()))?;
		Ok(Self::with(config, store, cluster.kv.clone(), cluster.local_id(), Some(cluster)))
	}

	pub(super) fn with(
		config: CacheConfig,
		store: Arc<BlobStore>,
		kv: Kv,
		node: NodeId,
		cluster: Option<Cluster>,
	) -> Arc<Self> {
		Arc::new(Self {
			kv,
			cluster,
			node,
			store,
			uploads: Uploads::default(),
			downloads: Mutex::default(),
			fetches: Mutex::default(),
			upstream: config.upstream,
			secret: config.secret,
			http: config.http,
		})
	}

	/// Handles one guest request from `sandbox` that dialed `local` (the
	/// TAP's host address and [`super::PORT`]).
	pub async fn handle(
		self: &Arc<Self>,
		req: Request<Body>,
		sandbox: &str,
		local: SocketAddr,
	) -> Response {
		let path = req.uri().path().to_owned();
		if path == PING_PATH {
			return "ok".into_response();
		}
		if let Some(rest) = path.strip_prefix(BLOB_PATH)
			&& !rest.contains('/')
		{
			return self.blob(req, sandbox).await;
		}
		let Some(call) = path.strip_prefix(TWIRP) else {
			return twirp_error(StatusCode::NOT_FOUND, "bad_route", format!("no route for {path}"));
		};
		let grant = match index::grant(&self.kv, sandbox).await {
			Ok(Some(grant)) => grant,
			Ok(None) => {
				return twirp_error(
					StatusCode::FORBIDDEN,
					"permission_denied",
					"the vmon CI cache serves CI runner sandboxes only",
				);
			},
			Err(error) => {
				tracing::warn!(%sandbox, "CI cache grant lookup failed: {error:#}");
				return twirp_error(
					StatusCode::SERVICE_UNAVAILABLE,
					"unavailable",
					"cluster store unavailable",
				);
			},
		};
		let Some(method) = call
			.strip_prefix(CACHE_SERVICE)
			.and_then(|rest| rest.strip_prefix('/'))
		else {
			return self.pass_through(req).await;
		};
		if req.method() != Method::POST {
			return twirp_error(StatusCode::NOT_FOUND, "bad_route", "Twirp calls are POSTs");
		}
		let method = method.to_owned();
		let body = match small_body(req.into_body()).await {
			Ok(body) => body,
			Err(fail) => return *fail.0,
		};
		let (grant, scope) = match self.scope(sandbox, grant).await {
			Ok(scoped) => scoped,
			Err(fail) => return *fail.0,
		};
		let result = match method.as_str() {
			"CreateCacheEntry" => self.create(&body, sandbox, &grant, &scope, local).await,
			"FinalizeCacheEntryUpload" => self.finalize(&body, sandbox, &scope).await,
			"GetCacheEntryDownloadURL" => self.download_url(&body, sandbox, &scope, local).await,
			_ => Err(
				twirp_error(
					StatusCode::NOT_FOUND,
					"bad_route",
					format!("{CACHE_SERVICE}/{method} is not served by the vmon CI cache"),
				)
				.into(),
			),
		};
		result.unwrap_or_else(|fail| *fail.0)
	}

	/// The job's scope, waiting [`SCOPE_WAIT`] for the driver to learn the
	/// job and resolve it.
	async fn scope(
		&self,
		sandbox: &str,
		mut grant: CacheGrant,
	) -> Result<(CacheGrant, CacheScope), Fail> {
		let deadline = Instant::now() + SCOPE_WAIT;
		loop {
			if let Some(scope) = grant.scope.clone()
				&& grant.job_id.is_some()
			{
				return Ok((grant, scope));
			}
			if Instant::now() >= deadline {
				return Err(
					twirp_error(
						StatusCode::SERVICE_UNAVAILABLE,
						"unavailable",
						"vmon has not resolved this job's cache scope yet",
					)
					.into(),
				);
			}
			tokio::time::sleep(Duration::from_secs(1)).await;
			grant = match index::grant(&self.kv, sandbox).await {
				Ok(Some(grant)) => grant,
				Ok(None) => {
					return Err(
						twirp_error(
							StatusCode::FORBIDDEN,
							"permission_denied",
							"the vmon CI cache serves CI runner sandboxes only",
						)
						.into(),
					);
				},
				Err(_) => continue,
			};
		}
	}

	fn store_error(error: &anyhow::Error) -> Fail {
		tracing::warn!("CI cache store call failed: {error:#}");
		twirp_error(StatusCode::SERVICE_UNAVAILABLE, "unavailable", "cluster store unavailable")
			.into()
	}

	async fn create(
		&self,
		body: &[u8],
		sandbox: &str,
		grant: &CacheGrant,
		scope: &CacheScope,
		local: SocketAddr,
	) -> Result<Response, Fail> {
		let request: CreateRequest = parse(body)?;
		check_key(&request.key, "key")?;
		check_key(&request.version, "version")?;
		let job_id = grant.job_id.unwrap_or_default();
		let reserved = index::reserve(
			&self.kv,
			scope,
			&request.key,
			&request.version,
			job_id,
			self.node,
			crate::ci_cache::now(),
		)
		.await
		.map_err(|error| Self::store_error(&error))?;
		let row = match reserved {
			Reserved::New(row) => row,
			Reserved::Exists => {
				tracing::info!(%sandbox, repo = %scope.repo, scope = %scope.write, key = %request.key, "CI cache entry exists");
				return Err(
					twirp_error(
						StatusCode::CONFLICT,
						"already_exists",
						"cache entry with the same key, version, and scope already exists",
					)
					.into(),
				);
			},
		};
		let key = entry_key(scope.repo_id, &scope.write, &request.key, &request.version);
		let limit = self.store.budget().min(REPO_BUDGET);
		let token = self.uploads.open(&self.store, sandbox, &key, limit);
		tracing::info!(%sandbox, repo = %scope.repo, scope = %scope.write, key = %request.key, "CI cache upload reserved");
		Ok(json(&CreateResponse {
			ok: true,
			signed_upload_url: format!("http://{local}{BLOB_PATH}{}?sig={token}", row.id),
			..CreateResponse::default()
		}))
	}

	async fn finalize(
		&self,
		body: &[u8],
		sandbox: &str,
		scope: &CacheScope,
	) -> Result<Response, Fail> {
		let request: FinalizeRequest = parse(body)?;
		check_key(&request.key, "key")?;
		check_key(&request.version, "version")?;
		let key = entry_key(scope.repo_id, &scope.write, &request.key, &request.version);
		let refuse = |message: &str| -> Result<Response, Fail> {
			Ok(json(&FinalizeResponse {
				ok: false,
				message: message.to_owned(),
				..FinalizeResponse::default()
			}))
		};
		let Some((token, session)) = self.uploads.by_entry(sandbox, &key).await else {
			return refuse("no upload of this cache entry is in progress");
		};
		let committed = session.lock().await.committed.clone();
		let Some((digest, size)) = committed else {
			return refuse("the cache archive was not uploaded");
		};
		if size != request.size_bytes {
			self.uploads.close(&token).await;
			let _ = index::release(&self.kv, &key).await;
			return refuse(&format!("uploaded {size} bytes, not {}", request.size_bytes));
		}
		let finalized =
			index::finalize(&self.kv, &key, &digest, size, self.node, crate::ci_cache::now())
				.await
				.map_err(|error| Self::store_error(&error))?;
		self.uploads.close(&token).await;
		let Some(row) = finalized else {
			return refuse("the cache reservation expired");
		};
		tracing::info!(%sandbox, repo = %scope.repo, scope = %scope.write, key = %request.key, size, "CI cache entry saved");
		self.evict(0).await;
		Ok(json(&FinalizeResponse {
			ok: true,
			entry_id: row.id.to_string(),
			..FinalizeResponse::default()
		}))
	}

	async fn download_url(
		&self,
		body: &[u8],
		sandbox: &str,
		scope: &CacheScope,
		local: SocketAddr,
	) -> Result<Response, Fail> {
		let request: DownloadRequest = parse(body)?;
		check_key(&request.key, "key")?;
		check_key(&request.version, "version")?;
		let found =
			index::lookup(&self.kv, scope, &request.key, &request.restore_keys, &request.version)
				.await
				.map_err(|error| Self::store_error(&error))?;
		let Some((row, digest)) =
			found.and_then(|row| row.digest.clone().map(|digest| (row, digest)))
		else {
			tracing::info!(%sandbox, repo = %scope.repo, scope = %scope.write, key = %request.key, "CI cache miss");
			return Ok(json(&DownloadResponse::default()));
		};
		tracing::info!(%sandbox, repo = %scope.repo, scope = %row.scope, key = %request.key, matched = %row.key, "CI cache hit");
		{
			let (kv, row) = (self.kv.clone(), row.clone());
			tokio::spawn(async move {
				let _ = index::touch(&kv, &row, crate::ci_cache::now()).await;
			});
		}
		let token = hex::encode(rand::random::<[u8; 24]>());
		{
			let mut downloads = self.downloads.lock();
			downloads.retain(|_, download| download.expires > Instant::now());
			downloads.insert(token.clone(), Download {
				sandbox: sandbox.to_owned(),
				digest,
				size: row.size,
				expires: Instant::now() + DOWNLOAD_TTL,
			});
		}
		Ok(json(&DownloadResponse {
			ok:                  true,
			signed_download_url: format!("http://{local}{BLOB_PATH}{}?sig={token}", row.id),
			matched_key:         row.key,
		}))
	}

	// ----------------------------------------------------------------- blobs

	async fn blob(self: &Arc<Self>, req: Request<Body>, sandbox: &str) -> Response {
		let params = query_params(req.uri());
		let token = params.get("sig").map(String::as_str).unwrap_or_default();
		let denied = || {
			BlobError::new(
				StatusCode::FORBIDDEN,
				"AuthenticationFailed",
				"unknown or expired signature",
			)
			.into_response()
		};
		let method = req.method().clone();
		match method {
			Method::PUT => {
				let Some(session) = self.uploads.get(token) else {
					return denied();
				};
				let mut upload = session.lock().await;
				if upload.sandbox != sandbox {
					return denied();
				}
				let result = match params.get("comp").map(String::as_str) {
					Some("block") => {
						let id = params.get("blockid").cloned().unwrap_or_default();
						upload.stage_block(&id, req.into_body()).await
					},
					Some("blocklist") => {
						match http_body_util::Limited::new(req.into_body(), MAX_SMALL_BODY)
							.collect()
							.await
						{
							Ok(xml) => upload
								.commit_blocks(&self.store, &xml.to_bytes())
								.await
								.map(|_| ()),
							Err(error) => Err(BlobError::new(
								StatusCode::BAD_REQUEST,
								"InvalidInput",
								error.to_string(),
							)),
						}
					},
					None => upload
						.put_blob(&self.store, req.into_body())
						.await
						.map(|_| ()),
					Some(_) => Ok(()),
				};
				match result {
					Ok(()) => {
						let mut response = StatusCode::CREATED.into_response();
						let headers = response.headers_mut();
						blob_headers(headers);
						if let Ok(value) =
							HeaderValue::from_str(&format!("\"{}\"", uuid::Uuid::new_v4().simple()))
						{
							headers.insert(header::ETAG, value);
						}
						if let Ok(value) = HeaderValue::from_str(
							&Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
						) {
							headers.insert(header::LAST_MODIFIED, value);
						}
						headers
							.insert("x-ms-request-server-encrypted", HeaderValue::from_static("false"));
						response
					},
					Err(error) => error.into_response(),
				}
			},
			Method::GET | Method::HEAD => {
				let download = {
					let downloads = self.downloads.lock();
					downloads
						.get(token)
						.filter(|download| {
							download.sandbox == sandbox && download.expires > Instant::now()
						})
						.cloned()
				};
				let Some(download) = download else {
					return denied();
				};
				let Ok(range) = resolve_range(requested_range(req.headers()), download.size) else {
					return BlobError::new(
						StatusCode::RANGE_NOT_SATISFIABLE,
						"InvalidRange",
						"the range specified is invalid for the current size of the resource",
					)
					.into_response();
				};
				let head = req.method() == Method::HEAD;
				match self.serve(&download, range, head).await {
					Ok(response) => response,
					Err(error) => {
						tracing::warn!(%sandbox, digest = %download.digest, "CI cache download failed: {error:#}");
						BlobError::new(
							StatusCode::SERVICE_UNAVAILABLE,
							"ServerBusy",
							format!("{error:#}"),
						)
						.into_response()
					},
				}
			},
			_ => BlobError::new(
				StatusCode::METHOD_NOT_ALLOWED,
				"UnsupportedHttpVerb",
				"unsupported method",
			)
			.into_response(),
		}
	}

	/// The response headers of a (ranged) blob body.
	fn blob_response(size: u64, range: Option<(u64, u64)>, digest: &str, body: Body) -> Response {
		let (status, length) = match range {
			Some((start, end)) => (StatusCode::PARTIAL_CONTENT, end - start + 1),
			None => (StatusCode::OK, size),
		};
		let mut response = (status, body).into_response();
		let headers = response.headers_mut();
		blob_headers(headers);
		headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
		headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
		headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
		headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
		if let Ok(etag) = HeaderValue::from_str(&format!("\"{}\"", &digest[..16])) {
			headers.insert(header::ETAG, etag);
		}
		if let Some((start, end)) = range
			&& let Ok(value) = HeaderValue::from_str(&format!("bytes {start}-{end}/{size}"))
		{
			headers.insert(header::CONTENT_RANGE, value);
		}
		response
	}

	/// Serves a download: the local copy, or a peer fetch in progress (whole
	/// downloads stream while the copy is written; ranges wait for it).
	async fn serve(
		self: &Arc<Self>,
		download: &Download,
		range: Option<(u64, u64)>,
		head: bool,
	) -> anyhow::Result<Response> {
		let digest = download.digest.as_str();
		let pin = self.store.pin(digest);
		if head {
			return Ok(Self::blob_response(download.size, range, digest, Body::empty()));
		}
		loop {
			if self.store.touch(digest).is_some() {
				let mut file = tokio::fs::File::open(self.store.path(digest)).await?;
				let (start, length) = match range {
					Some((start, end)) => (start, end - start + 1),
					None => (0, download.size),
				};
				file.seek(SeekFrom::Start(start)).await?;
				let stream = ReaderStream::new(file.take(length)).map(move |chunk| {
					let _ = &pin;
					chunk
				});
				return Ok(Self::blob_response(
					download.size,
					range,
					digest,
					Body::from_stream(stream),
				));
			}
			let handle = self.fetch(digest, download.size);
			let state = handle.progress.borrow().clone();
			match state {
				Fetch::Done => {},
				Fetch::Failed(error) => anyhow::bail!("fetching from the holders failed: {error}"),
				Fetch::Running(_) if range.is_some() => {
					let mut progress = handle.progress.clone();
					progress
						.wait_for(|state| !matches!(state, Fetch::Running(_)))
						.await
						.context("peer fetch ended")?;
				},
				Fetch::Running(_) => {
					let Ok(file) = tokio::fs::File::open(&handle.path).await else {
						// Installed meanwhile (renamed away).
						tokio::time::sleep(Duration::from_millis(20)).await;
						continue;
					};
					let body = tail(file, handle.progress, download.size, pin);
					return Ok(Self::blob_response(download.size, None, digest, body));
				},
			}
		}
	}

	/// The peer fetch of `digest`, started if none runs.
	fn fetch(self: &Arc<Self>, digest: &str, size: u64) -> FetchHandle {
		let mut fetches = self.fetches.lock();
		if let Some(handle) = fetches.get(digest) {
			return handle.clone();
		}
		let (tx, rx) = watch::channel(Fetch::Running(0));
		let handle = FetchHandle { path: self.store.temp_path(), progress: rx };
		fetches.insert(digest.to_owned(), handle.clone());
		let (service, digest, path) = (Arc::clone(self), digest.to_owned(), handle.path.clone());
		tokio::spawn(async move {
			let pin = service.store.pin(&digest);
			let result = service.fetch_from_holders(&digest, size, &path, &tx).await;
			let state = match result {
				Ok(()) => {
					tracing::info!(%digest, size, "CI cache archive fetched from a peer");
					Fetch::Done
				},
				Err(error) => {
					let _ = tokio::fs::remove_file(&path).await;
					tracing::warn!(%digest, "CI cache peer fetch failed: {error:#}");
					Fetch::Failed(format!("{error:#}"))
				},
			};
			service.fetches.lock().remove(&digest);
			let _ = tx.send(state);
			drop(pin);
			service.evict(0).await;
		});
		handle
	}

	async fn fetch_from_holders(
		&self,
		digest: &str,
		size: u64,
		path: &std::path::Path,
		progress: &watch::Sender<Fetch>,
	) -> anyhow::Result<()> {
		let cluster = self.cluster.as_ref().context("no cluster to fetch from")?;
		let blob = index::blob(&self.kv, digest)
			.await?
			.context("the archive is no longer indexed")?;
		self.evict(size).await;
		let mut last = anyhow::anyhow!("no other node holds the archive");
		for holder in blob.holders.iter().filter(|holder| **holder != self.node) {
			let Some(url) = cluster.cluster_url(*holder) else {
				last = anyhow::anyhow!("holder {holder} is not in the cluster");
				continue;
			};
			let response = match NodeClient::new(&url, &self.secret, self.http.clone()) {
				Ok(client) => client.ci_cache_blob(digest).await,
				Err(error) => Err(error),
			};
			let response = match response {
				Ok(response) => response,
				Err(error) => {
					last = anyhow::anyhow!("holder {holder}: {error}");
					continue;
				},
			};
			let mut file = tokio::fs::File::create(path).await?;
			let mut hasher = Sha256::new();
			let mut written = 0u64;
			let mut stream = response.bytes_stream();
			while let Some(chunk) = stream.try_next().await.context("reading from the holder")? {
				written += chunk.len() as u64;
				anyhow::ensure!(written <= size, "the holder sent more than {size} bytes");
				hasher.update(&chunk);
				file.write_all(&chunk).await?;
				file.flush().await?;
				let _ = progress.send(Fetch::Running(written));
			}
			anyhow::ensure!(written == size, "the holder sent {written} of {size} bytes");
			anyhow::ensure!(
				hex::encode(hasher.finalize()) == digest,
				"the archive's digest does not match"
			);
			file.sync_all().await?;
			drop(file);
			let (store, tmp, key) = (Arc::clone(&self.store), path.to_owned(), digest.to_owned());
			tokio::task::spawn_blocking(move || store.install(&tmp, &key, size)).await??;
			if !index::add_holder(&self.kv, digest, self.node).await? {
				tracing::info!(%digest, "CI cache archive was deleted while fetching");
			}
			return Ok(());
		}
		Err(last)
	}

	/// Evicts local copies beyond the budget (with `incoming` bytes on the
	/// way) and unregisters them as holders.
	async fn evict(&self, incoming: u64) {
		let store = Arc::clone(&self.store);
		let evicted = tokio::task::spawn_blocking(move || store.evict(incoming))
			.await
			.unwrap_or_default();
		for digest in evicted {
			match index::drop_holder(&self.kv, &digest, self.node).await {
				Ok(gone) if !gone.is_empty() => {
					tracing::info!(%digest, entries = gone.len(), "CI cache evicted the last copy; its entries are gone");
				},
				Ok(_) => tracing::debug!(%digest, "CI cache evicted a local copy"),
				Err(error) => {
					tracing::warn!(%digest, "unregistering an evicted CI cache copy failed: {error:#}");
				},
			}
		}
	}

	/// Digests of the local copies (tests).
	#[cfg(test)]
	pub(super) fn store_digests(&self) -> Vec<String> {
		self.store.digests()
	}

	/// Periodic cleanup: releases expired uploads, forgets expired download
	/// URLs, drops copies no entry uses (or that this node is not recorded
	/// for), and enforces the budget.
	pub async fn gc(&self) {
		for key in self.uploads.expire().await {
			if let Err(error) = index::release(&self.kv, &key).await {
				tracing::debug!(%key, "releasing an expired CI cache upload failed: {error:#}");
			}
		}
		self
			.downloads
			.lock()
			.retain(|_, download| download.expires > Instant::now());
		match index::blobs(&self.kv).await {
			Ok(blobs) => {
				let unused: Vec<String> = self
					.store
					.digests()
					.into_iter()
					.filter(|digest| {
						blobs
							.get(digest)
							.is_none_or(|blob| !blob.holders.contains(&self.node))
					})
					.collect();
				let store = Arc::clone(&self.store);
				let removed = tokio::task::spawn_blocking(move || {
					unused.iter().filter(|digest| store.remove(digest)).count()
				})
				.await
				.unwrap_or_default();
				if removed > 0 {
					tracing::info!(removed, "CI cache dropped unused archive copies");
				}
			},
			Err(error) => tracing::debug!("CI cache sweep skipped: {error:#}"),
		}
		self.evict(0).await;
	}

	// ----------------------------------------------------------- pass-through

	/// Forwards a non-cache Twirp call to GitHub's results service, headers
	/// (`Authorization` included) and body unchanged. Twirp bodies are small
	/// JSON (archives go to the signed blob URLs), so the body is buffered and
	/// sent with its length rather than chunked.
	async fn pass_through(&self, req: Request<Body>) -> Response {
		let path = req
			.uri()
			.path_and_query()
			.map_or("/", http::uri::PathAndQuery::as_str);
		let Ok(url) = self.upstream.join(path) else {
			return twirp_error(StatusCode::BAD_REQUEST, "malformed", "bad path");
		};
		let (parts, body) = req.into_parts();
		let body = match small_body(body).await {
			Ok(body) => body,
			Err(fail) => return *fail.0,
		};
		let mut request = self
			.http
			.request(parts.method.clone(), url)
			.timeout(UPSTREAM_TIMEOUT)
			.body(body);
		for (name, value) in &parts.headers {
			if !matches!(
				name.as_str(),
				"host"
					| "content-length"
					| "connection"
					| "transfer-encoding"
					| "keep-alive"
					| "upgrade"
			) {
				request = request.header(name, value);
			}
		}
		match request.send().await {
			Ok(upstream) => {
				let status = upstream.status();
				let mut headers = HeaderMap::new();
				for (name, value) in upstream.headers() {
					if !matches!(name.as_str(), "connection" | "transfer-encoding" | "keep-alive") {
						headers.append(name, value.clone());
					}
				}
				let mut response = (status, Body::from_stream(upstream.bytes_stream())).into_response();
				*response.headers_mut() = headers;
				response
			},
			Err(error) => {
				tracing::warn!("CI results pass-through failed: {error}");
				twirp_error(StatusCode::BAD_GATEWAY, "unavailable", format!("results service: {error}"))
			},
		}
	}
}

/// A body that follows a peer fetch writing `file`: it yields bytes as they
/// land and ends after `size` bytes, or fails with the fetch.
fn tail(file: tokio::fs::File, progress: watch::Receiver<Fetch>, size: u64, pin: Pin) -> Body {
	struct Tail {
		file:     tokio::fs::File,
		progress: watch::Receiver<Fetch>,
		read:     u64,
		buf:      Vec<u8>,
		_pin:     Pin,
	}
	let state = Tail { file, progress, read: 0, buf: vec![0u8; 256 << 10], _pin: pin };
	let stream = futures::stream::try_unfold(state, move |mut state| async move {
		loop {
			if state.read >= size {
				return Ok::<_, std::io::Error>(None);
			}
			let current = state.progress.borrow().clone();
			let available = match current {
				Fetch::Running(written) => written,
				Fetch::Done => size,
				Fetch::Failed(error) => return Err(std::io::Error::other(error)),
			};
			if available > state.read {
				let want =
					usize::try_from((available - state.read).min(state.buf.len() as u64)).unwrap_or(0);
				let n = state.file.read(&mut state.buf[..want]).await?;
				if n == 0 {
					tokio::time::sleep(Duration::from_millis(10)).await;
					continue;
				}
				state.read += n as u64;
				let chunk = Bytes::copy_from_slice(&state.buf[..n]);
				return Ok(Some((chunk, state)));
			}
			if state.progress.changed().await.is_err() {
				// The fetch task ended; its final state is in the channel.
				let finished = state.progress.borrow().clone();
				if !matches!(finished, Fetch::Done) {
					return Err(std::io::Error::other("peer fetch ended"));
				}
			}
		}
	});
	Body::from_stream(stream)
}

/// Serves guest connections handed over by the egress proxy.
#[derive(Clone)]
pub struct Listener(pub Arc<Service>);

impl LocalService for Listener {
	fn serve(&self, stream: tokio::net::TcpStream, sandbox_id: String, local: SocketAddr) {
		let service = Arc::clone(&self.0);
		tokio::spawn(async move {
			let sandbox: Arc<str> = sandbox_id.into();
			let handler = hyper::service::service_fn(move |req: Request<hyper::body::Incoming>| {
				let (service, sandbox) = (Arc::clone(&service), Arc::clone(&sandbox));
				async move {
					let req = req.map(Body::new);
					Ok::<_, std::convert::Infallible>(service.handle(req, &sandbox, local).await)
				}
			});
			let _ = hyper::server::conn::http1::Builder::new()
				.serve_connection(TokioIo::new(stream), handler)
				.await;
		});
	}
}

// ------------------------------------------------------------ peer route

/// `GET /ci-cache/blobs/{digest}` (relative to `/node/v1`): a local copy for
/// a peer's fetch.
pub fn routes(service: Arc<Service>) -> Router {
	Router::new()
		.route("/ci-cache/blobs/{digest}", get(peer_blob))
		.with_state(service)
}

async fn peer_blob(
	State(service): State<Arc<Service>>,
	Path(digest): Path<String>,
) -> Result<Response, ApiError> {
	if !valid_digest(&digest) {
		return Err(ApiError::invalid("invalid digest"));
	}
	let pin = service.store.pin(&digest);
	let Some(size) = service.store.touch(&digest) else {
		return Err(ApiError::not_found(format!("no local copy of {digest}")));
	};
	let file = tokio::fs::File::open(service.store.path(&digest))
		.await
		.map_err(|error| ApiError::not_found(format!("{digest}: {error}")))?;
	let stream = ReaderStream::new(file).map(move |chunk| {
		let _ = &pin;
		chunk
	});
	Ok((
		[
			(header::CONTENT_TYPE, "application/octet-stream".to_owned()),
			(header::CONTENT_LENGTH, size.to_string()),
		],
		Body::from_stream(stream),
	)
		.into_response())
}
