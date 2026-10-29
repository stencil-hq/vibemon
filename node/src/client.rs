//! HTTP client for the node API (`/node/v1`). Used by drivers (and by nodes
//! fetching artifacts/files from peers). Every call carries the cluster
//! secret as a bearer token.

use std::time::Duration;

use bytes::Bytes;
use http::{Method, StatusCode, header};
use http_body_util::Empty;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde::{Serialize, de::DeserializeOwned};
use url::Url;

use crate::api::{
	AdoptSandbox, ArtifactInfo, ArtifactManifest, BuildStatus, CreateSandbox, Deadline, ErrorBody,
	HandoverManifest, LogEntry, LogQuery, MetricSample, NetworkPolicy, PREFIX, PauseSandbox,
	ProcessStarted, ResumeSandbox, SandboxInfo, SnapshotSandbox, StartBuild, StartProcess,
	TUNNEL_UPGRADE,
};

/// Default timeout for control calls; long operations (create, pause,
/// snapshot) pass their own.
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound for lifecycle operations that snapshot or boot VMs.
const LIFECYCLE_TIMEOUT: Duration = Duration::from_mins(5);
/// Upper bound for an adoption, which transfers a sandbox's written disk and
/// guest RAM between nodes.
const HANDOVER_TIMEOUT: Duration = Duration::from_mins(30);

/// Failure talking to a node.
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
	/// Connection could not be established or broke mid-request.
	#[error("node unreachable: {0}")]
	Unreachable(String),
	/// The node answered with a non-2xx status.
	#[error("node error {status} {code}: {message}")]
	Api { status: u16, code: String, message: String },
}

impl NodeError {
	/// Stable error code (`unreachable` for transport failures).
	pub fn code(&self) -> &str {
		match self {
			Self::Unreachable(_) => "unreachable",
			Self::Api { code, .. } => code,
		}
	}

	pub fn is_not_found(&self) -> bool {
		self.code() == "not_found"
	}
}

/// Client bound to one node's cluster URL.
#[derive(Clone, Debug)]
pub struct NodeClient {
	base:   Url,
	secret: String,
	http:   reqwest::Client,
}

impl NodeClient {
	/// `cluster_url` is the member's route (`MemberMeta::cluster_url`,
	/// `Cluster::cluster_url`); `http` is a shared connection pool.
	pub fn new(cluster_url: &str, secret: &str, http: reqwest::Client) -> Result<Self, NodeError> {
		let base = Url::parse(cluster_url)
			.map_err(|e| NodeError::Unreachable(format!("bad node url {cluster_url}: {e}")))?;
		Ok(Self { base, secret: secret.to_owned(), http })
	}

	/// Cluster URL this client targets.
	pub const fn base(&self) -> &Url {
		&self.base
	}

	pub async fn create_sandbox(&self, req: &CreateSandbox) -> Result<SandboxInfo, NodeError> {
		self
			.json(Method::POST, "/sandboxes", Some(req), LIFECYCLE_TIMEOUT)
			.await
	}

	pub async fn list_sandboxes(&self) -> Result<Vec<SandboxInfo>, NodeError> {
		self
			.json::<(), _>(Method::GET, "/sandboxes", None, CONTROL_TIMEOUT)
			.await
	}

	pub async fn get_sandbox(&self, id: &str) -> Result<SandboxInfo, NodeError> {
		self
			.json::<(), _>(Method::GET, &format!("/sandboxes/{id}"), None, CONTROL_TIMEOUT)
			.await
	}

	/// Kills a running sandbox and deletes its paused state.
	pub async fn kill_sandbox(&self, id: &str) -> Result<(), NodeError> {
		self
			.empty::<()>(Method::DELETE, &format!("/sandboxes/{id}"), None, LIFECYCLE_TIMEOUT)
			.await
	}

	pub async fn pause_sandbox(
		&self,
		id: &str,
		req: &PauseSandbox,
	) -> Result<SandboxInfo, NodeError> {
		self
			.json(Method::POST, &format!("/sandboxes/{id}/pause"), Some(req), LIFECYCLE_TIMEOUT)
			.await
	}

	pub async fn resume_sandbox(
		&self,
		id: &str,
		req: &ResumeSandbox,
	) -> Result<SandboxInfo, NodeError> {
		self
			.json(Method::POST, &format!("/sandboxes/{id}/resume"), Some(req), LIFECYCLE_TIMEOUT)
			.await
	}

	/// Pulls the paused sandbox `req.source` holds and keeps it paused here.
	pub async fn adopt_sandbox(
		&self,
		id: &str,
		req: &AdoptSandbox,
	) -> Result<SandboxInfo, NodeError> {
		self
			.json(Method::POST, &format!("/sandboxes/{id}/adopt"), Some(req), HANDOVER_TIMEOUT)
			.await
	}

	/// Deletes this node's paused copy and forgets the sandbox, leaving the
	/// KV and the object store alone (a handover source after the target took
	/// over, or a target whose adoption is rolled back).
	pub async fn drop_paused(&self, id: &str) -> Result<(), NodeError> {
		self
			.empty::<()>(Method::DELETE, &format!("/sandboxes/{id}/paused"), None, LIFECYCLE_TIMEOUT)
			.await
	}

	/// Describes a committed paused sandbox for a peer's adoption.
	pub async fn handover_manifest(&self, id: &str) -> Result<HandoverManifest, NodeError> {
		self
			.json::<(), _>(Method::GET, &format!("/sandboxes/{id}/handover"), None, LIFECYCLE_TIMEOUT)
			.await
	}

	/// Streams the data extents of one handover file (in manifest order); the
	/// caller consumes `bytes_stream()`.
	pub async fn handover_extents(
		&self,
		id: &str,
		path: &str,
	) -> Result<reqwest::Response, NodeError> {
		let url = self.url(&format!("/sandboxes/{id}/handover/{path}"))?;
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.send()
			.await
			.map_err(transport)?;
		ensure_ok(response).await
	}

	pub async fn set_deadline(&self, id: &str, end_at_ms: u64) -> Result<(), NodeError> {
		let body = Deadline { end_at_ms };
		self
			.empty(Method::POST, &format!("/sandboxes/{id}/deadline"), Some(&body), CONTROL_TIMEOUT)
			.await
	}

	/// Starts a detached guest process (envd `process.Process/Start`) that
	/// keeps running after this call; returns its pid.
	pub async fn start_process(&self, id: &str, req: &StartProcess) -> Result<u32, NodeError> {
		let started: ProcessStarted = self
			.json(Method::POST, &format!("/sandboxes/{id}/processes"), Some(req), CONTROL_TIMEOUT)
			.await?;
		Ok(started.pid)
	}

	pub async fn update_network(&self, id: &str, policy: &NetworkPolicy) -> Result<(), NodeError> {
		self
			.empty(Method::PUT, &format!("/sandboxes/{id}/network"), Some(policy), CONTROL_TIMEOUT)
			.await
	}

	/// Captures a running sandbox as a new build (snapshot templates, fork).
	pub async fn snapshot_sandbox(
		&self,
		id: &str,
		req: &SnapshotSandbox,
	) -> Result<ArtifactInfo, NodeError> {
		self
			.json(Method::POST, &format!("/sandboxes/{id}/snapshot"), Some(req), LIFECYCLE_TIMEOUT)
			.await
	}

	/// Metric samples in `[start_ms, end_ms]`.
	pub async fn metrics(
		&self,
		id: &str,
		start_ms: Option<u64>,
		end_ms: Option<u64>,
	) -> Result<Vec<MetricSample>, NodeError> {
		let url = self.url(&format!("/sandboxes/{id}/metrics"))?;
		let mut query = Vec::with_capacity(2);
		if let Some(start) = start_ms {
			query.push(("start", start));
		}
		if let Some(end) = end_ms {
			query.push(("end", end));
		}
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.query(&query)
			.timeout(CONTROL_TIMEOUT)
			.send()
			.await
			.map_err(transport)?;
		decode(response).await
	}

	pub async fn logs(&self, id: &str, query: &LogQuery) -> Result<Vec<LogEntry>, NodeError> {
		let url = self.url(&format!("/sandboxes/{id}/logs"))?;
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.query(query)
			.timeout(CONTROL_TIMEOUT)
			.send()
			.await
			.map_err(transport)?;
		decode(response).await
	}

	pub async fn start_build(&self, req: &StartBuild) -> Result<(), NodeError> {
		self
			.empty(Method::POST, "/builds", Some(req), CONTROL_TIMEOUT)
			.await
	}

	/// Build status with log entries from `logs_offset` on.
	pub async fn build_status(
		&self,
		build_id: &str,
		logs_offset: u64,
	) -> Result<BuildStatus, NodeError> {
		self
			.json::<(), _>(
				Method::GET,
				&format!("/builds/{build_id}?logsOffset={logs_offset}"),
				None,
				CONTROL_TIMEOUT,
			)
			.await
	}

	pub async fn cancel_build(&self, build_id: &str) -> Result<(), NodeError> {
		self
			.empty::<()>(Method::DELETE, &format!("/builds/{build_id}"), None, CONTROL_TIMEOUT)
			.await
	}

	pub async fn artifact_manifest(&self, build_id: &str) -> Result<ArtifactManifest, NodeError> {
		self
			.json::<(), _>(
				Method::GET,
				&format!("/artifacts/{build_id}/manifest"),
				None,
				CONTROL_TIMEOUT,
			)
			.await
	}

	/// Streams the data extents of one artifact file (in manifest order); the
	/// caller consumes `bytes_stream()`.
	pub async fn artifact_extents(
		&self,
		build_id: &str,
		path: &str,
	) -> Result<reqwest::Response, NodeError> {
		let url = self.url(&format!("/artifacts/{build_id}/files/{path}?sparse=true"))?;
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.send()
			.await
			.map_err(transport)?;
		ensure_ok(response).await
	}

	pub async fn delete_artifact(&self, build_id: &str) -> Result<(), NodeError> {
		self
			.empty::<()>(Method::DELETE, &format!("/artifacts/{build_id}"), None, CONTROL_TIMEOUT)
			.await
	}

	/// Stores a build file-context archive on the node.
	pub async fn put_file(
		&self,
		template_id: &str,
		hash: &str,
		body: reqwest::Body,
	) -> Result<(), NodeError> {
		let url = self.url(&format!("/files/{template_id}/{hash}"))?;
		let response = self
			.http
			.put(url)
			.bearer_auth(&self.secret)
			.body(body)
			.timeout(LIFECYCLE_TIMEOUT)
			.send()
			.await
			.map_err(transport)?;
		ensure_ok(response).await.map(drop)
	}

	/// Streams a build file-context archive.
	pub async fn get_file(
		&self,
		template_id: &str,
		hash: &str,
	) -> Result<reqwest::Response, NodeError> {
		let url = self.url(&format!("/files/{template_id}/{hash}"))?;
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.send()
			.await
			.map_err(transport)?;
		ensure_ok(response).await
	}

	/// Streams a peer's local copy of a CI cache archive
	/// (`crate::ci_cache`); the caller verifies its sha256.
	pub async fn ci_cache_blob(&self, digest: &str) -> Result<reqwest::Response, NodeError> {
		let url = self.url(&format!("/ci-cache/blobs/{digest}"))?;
		let response = self
			.http
			.get(url)
			.bearer_auth(&self.secret)
			.send()
			.await
			.map_err(transport)?;
		ensure_ok(response).await
	}

	/// Forwards an arbitrary request below `/node/v1` (volume content API).
	/// `path_and_query` starts with `/`.
	pub async fn forward(
		&self,
		method: Method,
		path_and_query: &str,
		headers: http::HeaderMap,
		body: reqwest::Body,
	) -> Result<reqwest::Response, NodeError> {
		let url = self.url(path_and_query)?;
		self
			.http
			.request(method, url)
			.headers(headers)
			.bearer_auth(&self.secret)
			.body(body)
			.timeout(LIFECYCLE_TIMEOUT)
			.send()
			.await
			.map_err(transport)
	}

	/// Opens a raw byte tunnel to guest TCP `port` (`127.0.0.1` inside the
	/// guest). Returns `NodeError::Api { code: "port_closed" }` when nothing
	/// listens there and `not_found` for unknown sandboxes.
	pub async fn open_tunnel(
		&self,
		sandbox_id: &str,
		port: u16,
	) -> Result<TokioIo<Upgraded>, NodeError> {
		let url = self.url(&format!("/sandboxes/{sandbox_id}/tunnel/{port}"))?;
		let host = url
			.host_str()
			.ok_or_else(|| NodeError::Unreachable(format!("node url {url} has no host")))?;
		let addr_port = url.port_or_known_default().unwrap_or(80);
		let stream = tokio::net::TcpStream::connect((host, addr_port))
			.await
			.map_err(|e| NodeError::Unreachable(format!("connect {host}:{addr_port}: {e}")))?;
		stream.set_nodelay(true).ok();
		let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
			.await
			.map_err(|e| NodeError::Unreachable(format!("tunnel handshake: {e}")))?;
		tokio::spawn(async move {
			if let Err(error) = connection.with_upgrades().await {
				tracing::debug!(%error, "tunnel connection ended");
			}
		});
		let request = http::Request::builder()
			.method(Method::GET)
			.uri(url.path())
			.header(header::HOST, format!("{host}:{addr_port}"))
			.header(header::AUTHORIZATION, format!("Bearer {}", self.secret))
			.header(header::CONNECTION, "Upgrade")
			.header(header::UPGRADE, TUNNEL_UPGRADE)
			.body(Empty::<Bytes>::new())
			.map_err(|e| NodeError::Unreachable(format!("tunnel request: {e}")))?;
		let response = sender
			.send_request(request)
			.await
			.map_err(|e| NodeError::Unreachable(format!("tunnel request: {e}")))?;
		if response.status() != StatusCode::SWITCHING_PROTOCOLS {
			let status = response.status().as_u16();
			let body = http_body_util::BodyExt::collect(response.into_body())
				.await
				.map(|collected| collected.to_bytes())
				.unwrap_or_default();
			return Err(api_error(status, &body));
		}
		let upgraded = hyper::upgrade::on(response)
			.await
			.map_err(|e| NodeError::Unreachable(format!("tunnel upgrade: {e}")))?;
		Ok(TokioIo::new(upgraded))
	}

	fn url(&self, path: &str) -> Result<Url, NodeError> {
		self
			.base
			.join(&format!("{PREFIX}{path}"))
			.map_err(|e| NodeError::Unreachable(format!("bad node path {path}: {e}")))
	}

	async fn json<B: Serialize + Sync, T: DeserializeOwned>(
		&self,
		method: Method,
		path: &str,
		body: Option<&B>,
		timeout: Duration,
	) -> Result<T, NodeError> {
		let response = self.send(method, path, body, timeout).await?;
		decode(response).await
	}

	async fn empty<B: Serialize + Sync>(
		&self,
		method: Method,
		path: &str,
		body: Option<&B>,
		timeout: Duration,
	) -> Result<(), NodeError> {
		let response = self.send(method, path, body, timeout).await?;
		ensure_ok(response).await.map(drop)
	}

	async fn send<B: Serialize + Sync>(
		&self,
		method: Method,
		path: &str,
		body: Option<&B>,
		timeout: Duration,
	) -> Result<reqwest::Response, NodeError> {
		let mut request = self
			.http
			.request(method, self.url(path)?)
			.bearer_auth(&self.secret)
			.timeout(timeout);
		if let Some(body) = body {
			request = request.json(body);
		}
		request.send().await.map_err(transport)
	}
}

fn transport(error: reqwest::Error) -> NodeError {
	NodeError::Unreachable(error.to_string())
}

fn api_error(status: u16, body: &[u8]) -> NodeError {
	match serde_json::from_slice::<ErrorBody>(body) {
		Ok(ErrorBody { code, message }) => NodeError::Api { status, code, message },
		Err(_) => NodeError::Api {
			status,
			code: if status == 404 {
				"not_found".into()
			} else {
				"internal".into()
			},
			message: String::from_utf8_lossy(body).into_owned(),
		},
	}
}

async fn ensure_ok(response: reqwest::Response) -> Result<reqwest::Response, NodeError> {
	if response.status().is_success() {
		return Ok(response);
	}
	let status = response.status().as_u16();
	let body = response.bytes().await.unwrap_or_default();
	Err(api_error(status, &body))
}

async fn decode<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, NodeError> {
	let response = ensure_ok(response).await?;
	response
		.json::<T>()
		.await
		.map_err(|e| NodeError::Unreachable(format!("decoding node response: {e}")))
}
