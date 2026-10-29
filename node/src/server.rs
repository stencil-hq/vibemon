//! Node API server (`/node/v1`, ARCHITECTURE.md §8.5).
//!
//! Sandbox lifecycle, metrics, logs, and guest tunnels; `Node::router`
//! merges these with the build/artifact, file-context, and volume routes and
//! applies the cluster-secret check.

use std::sync::Arc;

use axum::{
	Json, Router,
	extract::{FromRef, Path, Query, Request, State},
	http::{StatusCode, header},
	response::{IntoResponse, Response},
	routing::{delete, get, post, put},
};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use vmon_cluster::{Cluster, NodeId};

use crate::{
	api::{
		AdoptSandbox, ArtifactInfo, CreateSandbox, Deadline, ErrorBody, HandoverManifest, LogEntry,
		LogQuery, MetricSample, NetworkPolicy, PauseSandbox, ResumeSandbox, SandboxInfo,
		SnapshotSandbox, TUNNEL_UPGRADE,
	},
	artifacts::sparse,
	gc4::{ConnectError, DATA_CHUNK},
	sandbox::SandboxManager,
};

/// Node API error: HTTP status plus [`ErrorBody`].
#[derive(Debug)]
pub struct ApiError {
	pub status:  StatusCode,
	pub code:    &'static str,
	pub message: String,
}

impl ApiError {
	/// Error with an explicit status and code.
	pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
		Self { status, code, message: message.into() }
	}

	/// 404 `not_found`.
	pub fn not_found(message: impl Into<String>) -> Self {
		Self::new(StatusCode::NOT_FOUND, "not_found", message)
	}

	/// 409 `conflict` (wrong state, duplicate id).
	pub fn conflict(message: impl Into<String>) -> Self {
		Self::new(StatusCode::CONFLICT, "conflict", message)
	}

	/// 400 `invalid`.
	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_REQUEST, "invalid", message)
	}

	/// 400 [`crate::api::GPU_MEMORY_SNAPSHOT`]: memory capture of a sandbox
	/// holding GPUs.
	pub fn gpu_memory_snapshot(id: &str) -> Self {
		Self::new(
			StatusCode::BAD_REQUEST,
			crate::api::GPU_MEMORY_SNAPSHOT,
			format!("sandbox {id} holds GPUs; its memory cannot be paused or snapshotted"),
		)
	}

	/// 409 [`crate::api::NESTED_VIRTUALIZATION_ACTIVE`]: memory capture of a
	/// sandbox whose guest runs VMs.
	pub fn nested_virtualization_active(id: &str) -> Self {
		Self::new(
			StatusCode::CONFLICT,
			crate::api::NESTED_VIRTUALIZATION_ACTIVE,
			format!(
				"sandbox {id} is running virtual machines (hardware virtualization is in use); its \
				 memory cannot be paused or snapshotted until they exit"
			),
		)
	}

	/// 503 `busy` (no capacity; the driver retries another node).
	pub fn busy(message: impl Into<String>) -> Self {
		Self::new(StatusCode::SERVICE_UNAVAILABLE, "busy", message)
	}

	/// 502 `unreachable` (a peer node or the guest agent failed).
	pub fn unreachable(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_GATEWAY, "unreachable", message)
	}

	/// 500 `internal` from any error (full context chain in the message).
	pub fn internal(error: impl std::fmt::Display) -> Self {
		Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", format!("{error:#}"))
	}
}

impl From<anyhow::Error> for ApiError {
	fn from(error: anyhow::Error) -> Self {
		match error.downcast::<Self>() {
			Ok(api) => api,
			Err(error) => Self::internal(format!("{error:#}")),
		}
	}
}

impl std::fmt::Display for ApiError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}: {}", self.code, self.message)
	}
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		(self.status, Json(ErrorBody { code: self.code.to_owned(), message: self.message }))
			.into_response()
	}
}

/// Resolves the peer node ids of driver requests to cluster URLs.
///
/// Ids (`artifact_sources`, `file_sources`, …) go through this node's own
/// view of the cluster ([`Cluster::cluster_url`]), so transfers take the route
/// this node reaches each peer at (LAN first). Without a cluster (tests)
/// nothing resolves.
#[derive(Clone, Default)]
pub struct Peers(Option<Cluster>);

impl Peers {
	/// Resolves through `cluster`.
	pub const fn new(cluster: Cluster) -> Self {
		Self(Some(cluster))
	}

	/// Cluster URLs of `ids`, in order; ids unknown to gossip are skipped.
	pub fn urls(&self, ids: &[NodeId]) -> Vec<String> {
		let Some(cluster) = &self.0 else {
			return Vec::new();
		};
		ids.iter()
			.filter_map(|&id| {
				let url = cluster.cluster_url(id);
				if url.is_none() {
					tracing::debug!(node = %id, "skipping a source unknown to gossip");
				}
				url
			})
			.collect()
	}
}

/// State of the sandbox routes.
#[derive(Clone)]
struct SandboxApi {
	manager: Arc<SandboxManager>,
	peers:   Peers,
}

impl FromRef<SandboxApi> for Arc<SandboxManager> {
	fn from_ref(api: &SandboxApi) -> Self {
		Self::clone(&api.manager)
	}
}

impl FromRef<SandboxApi> for Peers {
	fn from_ref(api: &SandboxApi) -> Self {
		api.peers.clone()
	}
}

/// Sandbox routes (relative to `/node/v1`); `peers` resolves the sources of
/// create, resume and adopt requests.
pub fn sandbox_routes(manager: Arc<SandboxManager>, peers: Peers) -> Router {
	Router::new()
		.route("/sandboxes", post(create).get(list))
		.route("/sandboxes/{id}", get(get_one).delete(kill))
		.route("/sandboxes/{id}/pause", post(pause))
		.route("/sandboxes/{id}/resume", post(resume))
		.route("/sandboxes/{id}/adopt", post(adopt))
		.route("/sandboxes/{id}/paused", delete(drop_paused))
		.route("/sandboxes/{id}/handover", get(handover_manifest))
		.route("/sandboxes/{id}/handover/{*path}", get(handover_file))
		.route("/sandboxes/{id}/deadline", post(deadline))
		.route("/sandboxes/{id}/network", put(network))
		.route("/sandboxes/{id}/snapshot", post(snapshot))
		.route("/sandboxes/{id}/metrics", get(metrics))
		.route("/sandboxes/{id}/logs", get(logs))
		.route("/sandboxes/{id}/tunnel/{port}", get(tunnel))
		.with_state(SandboxApi { manager, peers })
}

type Manager = State<Arc<SandboxManager>>;

async fn create(
	State(manager): Manager,
	State(peers): State<Peers>,
	Json(req): Json<CreateSandbox>,
) -> Result<(StatusCode, Json<SandboxInfo>), ApiError> {
	let sources = peers.urls(&req.artifact_sources);
	let info = detached(async move { manager.create(req, &sources).await }).await?;
	Ok((StatusCode::CREATED, Json(info)))
}

/// Runs a VM-starting operation to completion even when its request is
/// dropped (driver timeout or restart): cancelled mid-boot, it would leave a
/// VMM the manager does not track. A finished but unwanted sandbox is listed,
/// so the driver's reconciler kills it.
async fn detached<T: Send + 'static>(
	operation: impl Future<Output = Result<T, ApiError>> + Send + 'static,
) -> Result<T, ApiError> {
	tokio::spawn(operation).await.map_err(ApiError::internal)?
}

async fn list(State(manager): Manager) -> Json<Vec<SandboxInfo>> {
	Json(manager.list())
}

async fn get_one(
	State(manager): Manager,
	Path(id): Path<String>,
) -> Result<Json<SandboxInfo>, ApiError> {
	Ok(Json(manager.get(&id)?))
}

async fn kill(State(manager): Manager, Path(id): Path<String>) -> Result<StatusCode, ApiError> {
	manager.kill(&id).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn pause(
	State(manager): Manager,
	Path(id): Path<String>,
	Json(req): Json<PauseSandbox>,
) -> Result<Json<SandboxInfo>, ApiError> {
	Ok(Json(manager.pause(&id, &req).await?))
}

async fn resume(
	State(manager): Manager,
	State(peers): State<Peers>,
	Path(id): Path<String>,
	Json(req): Json<ResumeSandbox>,
) -> Result<Json<SandboxInfo>, ApiError> {
	let sources = peers.urls(&req.artifact_sources);
	Ok(Json(detached(async move { manager.resume(&id, req, &sources).await }).await?))
}

async fn adopt(
	State(manager): Manager,
	State(peers): State<Peers>,
	Path(id): Path<String>,
	Json(req): Json<AdoptSandbox>,
) -> Result<Json<SandboxInfo>, ApiError> {
	let source = peers.urls(&[req.source]).pop().ok_or_else(|| {
		ApiError::unreachable(format!("source node {} is unknown to gossip", req.source))
	})?;
	let sources = peers.urls(&req.artifact_sources);
	let adopted =
		detached(async move { manager.adopt_paused(&id, req.meta, &source, &sources).await });
	Ok(Json(adopted.await?))
}

async fn drop_paused(
	State(manager): Manager,
	Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
	manager.drop_paused(&id).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn handover_manifest(
	State(manager): Manager,
	Path(id): Path<String>,
) -> Result<Json<HandoverManifest>, ApiError> {
	Ok(Json(HandoverManifest::clone(&*manager.handover_manifest(&id).await?)))
}

/// Streams exactly the manifest entry's data extents, in order.
async fn handover_file(
	State(manager): Manager,
	Path((id, path)): Path<(String, String)>,
) -> Result<Response, ApiError> {
	let (file, extents) = manager.handover_file(&id, &path).await?;
	let len: u64 = extents.iter().map(|extent| extent[1]).sum();
	Response::builder()
		.header(header::CONTENT_TYPE, "application/octet-stream")
		.header(header::CONTENT_LENGTH, len)
		.body(axum::body::Body::from_stream(sparse::read_extents(file, extents)))
		.map_err(ApiError::internal)
}

async fn deadline(
	State(manager): Manager,
	Path(id): Path<String>,
	Json(req): Json<Deadline>,
) -> Result<StatusCode, ApiError> {
	manager.set_deadline(&id, req.end_at_ms).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn network(
	State(manager): Manager,
	Path(id): Path<String>,
	Json(policy): Json<NetworkPolicy>,
) -> Result<StatusCode, ApiError> {
	manager.update_network(&id, policy).await?;
	Ok(StatusCode::NO_CONTENT)
}

async fn snapshot(
	State(manager): Manager,
	Path(id): Path<String>,
	Json(req): Json<SnapshotSandbox>,
) -> Result<Json<ArtifactInfo>, ApiError> {
	Ok(Json(manager.snapshot(&id, req).await?))
}

/// `?start&end` of the metrics route (unix ms).
#[derive(Debug, Deserialize)]
struct Range {
	start: Option<u64>,
	end:   Option<u64>,
}

async fn metrics(
	State(manager): Manager,
	Path(id): Path<String>,
	Query(range): Query<Range>,
) -> Result<Json<Vec<MetricSample>>, ApiError> {
	Ok(Json(manager.metrics(&id, range.start, range.end).await?))
}

async fn logs(
	State(manager): Manager,
	Path(id): Path<String>,
	Query(query): Query<LogQuery>,
) -> Result<Json<Vec<LogEntry>>, ApiError> {
	Ok(Json(manager.logs(&id, query).await?))
}

/// `GET /sandboxes/{id}/tunnel/{port}` with `Upgrade: vmon-tunnel`: opens
/// the guest connection first (so a closed port is a clean 502), then
/// answers 101 and splices the upgraded connection to the GC4 stream.
async fn tunnel(
	State(manager): Manager,
	Path((id, port)): Path<(String, u16)>,
	mut request: Request,
) -> Result<Response, ApiError> {
	let wants_tunnel = request
		.headers()
		.get(header::UPGRADE)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value.eq_ignore_ascii_case(TUNNEL_UPGRADE));
	if !wants_tunnel {
		return Err(ApiError::new(
			StatusCode::UPGRADE_REQUIRED,
			"invalid",
			format!("expected Upgrade: {TUNNEL_UPGRADE}"),
		));
	}
	let vm = manager.live_vm(&id)?;
	let mut guest = vm.tcp_connect(port).await.map_err(|error| match error {
		ConnectError::Refused => ApiError::new(
			StatusCode::BAD_GATEWAY,
			"port_closed",
			format!("port {port} is not open in sandbox {id}"),
		),
		ConnectError::Failed(message) => ApiError::unreachable(message),
	})?;
	let upgrade = hyper::upgrade::on(&mut request);
	tokio::spawn(async move {
		match upgrade.await {
			Ok(upgraded) => {
				let mut client = TokioIo::new(upgraded);
				// Full-size DATA frames: tokio's default 8 KiB copy buffer cuts a
				// bulk upload into 8x the frames, each a separate agent-side
				// wakeup.
				let copied = tokio::io::copy_bidirectional_with_sizes(
					&mut client,
					&mut guest,
					DATA_CHUNK,
					DATA_CHUNK,
				)
				.await;
				if let Err(error) = copied {
					tracing::debug!(%error, port, "tunnel ended with error");
				}
			},
			Err(error) => tracing::debug!(%error, "tunnel upgrade failed"),
		}
	});
	Response::builder()
		.status(StatusCode::SWITCHING_PROTOCOLS)
		.header(header::CONNECTION, "Upgrade")
		.header(header::UPGRADE, TUNNEL_UPGRADE)
		.body(axum::body::Body::empty())
		.map_err(ApiError::internal)
}
