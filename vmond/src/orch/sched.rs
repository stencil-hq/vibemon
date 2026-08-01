//! Scheduler gRPC surface: placement, direct-to-worker forwarding, routing.
//!
//! One [`SchedGrpc`] serves three vmon.v1 services on every `vmon sched`
//! instance:
//!
//! - `SandboxService.Create` places the sandbox on a live worker from the
//!   in-memory [`super::table::WorkerTable`] (power-of-two-choices) and
//!   forwards the untouched spec straight to that worker's gRPC endpoint.
//!   `busy`/unreachable workers are penalized and the create retries on the
//!   next candidate — worker-side admission is the source of truth. The
//!   returned view is annotated with `node` (worker id) and `endpoint` (worker
//!   base URL) so clients may dial the owner directly.
//! - Every sandbox-scoped RPC (unary, server-stream, and bidi exec/shell)
//!   routes by sandbox id: local cache → Redis route → live-worker probe.
//! - `SnapshotService` locates the worker holding a named snapshot by fan-out
//!   and forwards restore/fork/delete there.
//! - `SystemService.Info`/`MeshStatus` expose the scheduler + worker roster.
//!
//! Redis is never on the create path: routes (`vmon:o:sb:<sid>`) enter a
//! bounded queue after the worker acknowledges create and one writer pipelines
//! batches. Retried creates carrying an `idempotency_key` converge on one
//! sandbox through a `SET NX` winner gate (`vmon:o:idem:<key>`); when Redis is
//! unavailable the gate degrades to at-least-once, exactly like the whiteboard
//! intends.

use std::{
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	time::Duration,
};

use dashmap::DashMap;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use serde_json::Value;
use tokio::{
	sync::{Semaphore, mpsc},
	time::timeout,
};
use tonic::{
	Code, Request, Response, Status, Streaming,
	metadata::{Ascii, MetadataValue},
	transport::{Channel, Endpoint},
};
use vmon_proto::v1 as pb;

use super::{
	SandboxRoute, keys, now_ms,
	redis::Redis,
	table::{PlacementNeeds, WorkerTable},
};

/// Mirror of the API layer's uniform message cap.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// TCP connect budget for worker channels.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Per-worker budget during List/locate/probe fan-outs.
const FANOUT_TIMEOUT: Duration = Duration::from_secs(5);
/// Idempotency winner-gate lifetime.
const IDEM_TTL_MS: u64 = 60 * 60 * 1000;
/// Poll cadence while waiting for an idempotent winner to finish.
const IDEM_POLL: Duration = Duration::from_millis(250);
/// How long a rejected/unreachable worker is skipped by placement.
const PENALTY: Duration = Duration::from_secs(2);
/// Bound on the local sid → (wid, url) cache.
const ROUTE_CACHE_MAX: usize = 65_536;
/// HTTP/2 connections per worker URL. Servers advertise ~200 concurrent
/// streams per connection, while one create burst rides hundreds of
/// concurrent watch relays; sharding requests across a small pool keeps
/// admissions and readiness relays from queueing behind each other.
const WORKER_CHANNELS: usize = 8;
/// Maximum pending route writes; excess writes are dropped rather than growing
/// memory.
const ROUTE_WRITE_QUEUE_MAX: usize = 4_096;
/// Maximum Redis `SET PX` commands written in one pipeline.
const ROUTE_WRITE_BATCH_MAX: usize = 128;
/// Maximum concurrently-executing `BatchCreate` items per stream.
const BATCH_CONCURRENCY: usize = 4096;
/// Buffered per-stream `BatchCreate` results awaiting the client.
const BATCH_RESULT_QUEUE: usize = 256;
/// Poll cadence while a `Watch(until_ready)` waits for an async route write.
const WATCH_ROUTE_POLL: Duration = Duration::from_millis(100);

type BoxStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

/// Tuning knobs owned by `server.rs`.
#[derive(Clone, Debug)]
pub struct SchedGrpcOptions {
	/// Bearer token attached to every worker-bound RPC.
	pub worker_token:   Option<String>,
	/// Deadline for one worker create attempt.
	pub create_timeout: Duration,
	/// Additional placement attempts after the first rejected create.
	pub create_retries: usize,
}

impl Default for SchedGrpcOptions {
	fn default() -> Self {
		Self { worker_token: None, create_timeout: Duration::from_mins(1), create_retries: 3 }
	}
}

struct RouteWrite {
	sid:     String,
	payload: Vec<u8>,
}
/// Shared scheduler state: worker table, Redis handle, channel + route caches.
pub struct SchedCore {
	table:        Arc<WorkerTable>,
	redis:        Redis,
	options:      SchedGrpcOptions,
	bearer:       Option<MetadataValue<Ascii>>,
	instance:     String,
	channels:     DashMap<String, Vec<Channel>>,
	channel_seq:  AtomicUsize,
	route_cache:  DashMap<String, (String, String)>,
	route_writes: mpsc::Sender<RouteWrite>,
	/// Creates successfully forwarded by this scheduler process; feeds the
	/// dashboard's cumulative counter in Redis.
	created:      AtomicU64,
}

impl SchedCore {
	/// Build the shared state; `instance` identifies this scheduler process
	/// (leader lease holder, idempotency pending markers).
	pub fn new(
		table: Arc<WorkerTable>,
		redis: Redis,
		options: SchedGrpcOptions,
		instance: String,
	) -> crate::Result<Arc<Self>> {
		let bearer = match &options.worker_token {
			Some(token) => Some(
				MetadataValue::try_from(format!("Bearer {token}"))
					.map_err(|_| crate::EngineError::invalid("worker token is not valid metadata"))?,
			),
			None => None,
		};
		let (route_writes, route_write_rx) = mpsc::channel(ROUTE_WRITE_QUEUE_MAX);
		tokio::spawn(route_writer(redis.clone(), route_write_rx));
		Ok(Arc::new(Self {
			table,
			redis,
			options,
			bearer,
			instance,
			channels: DashMap::new(),
			channel_seq: AtomicUsize::new(0),
			route_cache: DashMap::new(),
			route_writes,
			created: AtomicU64::new(0),
		}))
	}

	pub const fn table(&self) -> &Arc<WorkerTable> {
		&self.table
	}

	pub fn instance(&self) -> &str {
		&self.instance
	}

	/// Creates successfully forwarded since this process started.
	pub fn created_total(&self) -> u64 {
		self.created.load(Ordering::Relaxed)
	}

	fn request<T>(&self, message: T) -> Request<T> {
		let mut request = Request::new(message);
		if let Some(bearer) = &self.bearer {
			request
				.metadata_mut()
				.insert("authorization", bearer.clone());
		}
		request
	}

	/// Pick one channel from the worker's pool round-robin, building the
	/// pool of lazy connections on first contact.
	fn channel(&self, url: &str) -> Result<Channel, Status> {
		let seq = self.channel_seq.fetch_add(1, Ordering::Relaxed);
		if let Some(pool) = self.channels.get(url) {
			return Ok(pool[seq % pool.len()].clone());
		}
		let endpoint = Endpoint::from_shared(url.to_owned())
			.map_err(|error| {
				coded(
					Code::Unavailable,
					"engine_error",
					true,
					format!("invalid worker URL {url}: {error}"),
				)
			})?
			.connect_timeout(CONNECT_TIMEOUT);
		let pool: Vec<Channel> = (0..WORKER_CHANNELS)
			.map(|_connection| endpoint.connect_lazy())
			.collect();
		let channel = pool[seq % pool.len()].clone();
		if self.channels.len() >= ROUTE_CACHE_MAX {
			self.channels.clear();
		}
		self.channels.insert(url.to_owned(), pool);
		Ok(channel)
	}

	fn sandbox_client(
		&self,
		url: &str,
	) -> Result<pb::sandbox_service_client::SandboxServiceClient<Channel>, Status> {
		Ok(pb::sandbox_service_client::SandboxServiceClient::new(self.channel(url)?)
			.max_decoding_message_size(MAX_MESSAGE_SIZE)
			.max_encoding_message_size(MAX_MESSAGE_SIZE))
	}

	fn snapshot_client(
		&self,
		url: &str,
	) -> Result<pb::snapshot_service_client::SnapshotServiceClient<Channel>, Status> {
		Ok(pb::snapshot_service_client::SnapshotServiceClient::new(self.channel(url)?)
			.max_decoding_message_size(MAX_MESSAGE_SIZE)
			.max_encoding_message_size(MAX_MESSAGE_SIZE))
	}

	fn cache_route(&self, sid: &str, wid: &str, url: &str) {
		if self.route_cache.len() >= ROUTE_CACHE_MAX {
			self.route_cache.clear();
		}
		self
			.route_cache
			.insert(sid.to_owned(), (wid.to_owned(), url.to_owned()));
	}

	fn drop_route(&self, sid: &str) {
		self.route_cache.remove(sid);
	}

	/// Resolve the worker owning `sid`: local cache, then the Redis route,
	/// then a live-worker probe (covers Redis flakiness and foreign creates).
	async fn resolve_sandbox(&self, sid: &str) -> Result<(String, String), Status> {
		if sid.is_empty() {
			return Err(coded(Code::InvalidArgument, "invalid", false, "sandbox id is required"));
		}
		if let Some((wid, url)) = self.route_cache.get(sid).map(|entry| entry.clone()) {
			// Prefer the freshest advertised URL for a known worker.
			let url = self.table.url_of(&wid).unwrap_or(url);
			return Ok((wid, url));
		}
		match self.redis.get(&keys::sandbox(sid)).await {
			Ok(Some(raw)) => {
				if let Ok(route) = serde_json::from_slice::<SandboxRoute>(&raw) {
					if route.state == super::STATE_LOST {
						return Err(coded(
							Code::NotFound,
							"not_found",
							false,
							format!("sandbox {sid} was lost with worker {}", route.wid),
						));
					}
					let url = self.table.url_of(&route.wid).unwrap_or(route.url);
					self.cache_route(sid, &route.wid, &url);
					return Ok((route.wid, url));
				}
			},
			Ok(None) => {},
			Err(error) => {
				tracing::warn!(sid, %error, "orch: route lookup failed; probing workers");
			},
		}
		if let Some((wid, url)) = self.probe_workers(sid).await {
			self.cache_route(sid, &wid, &url);
			return Ok((wid, url));
		}
		Err(coded(Code::NotFound, "not_found", false, format!("sandbox {sid} not found")))
	}

	/// Ask every live worker whether it owns `sid`; first positive answer
	/// wins. O(workers), used only when both caches miss.
	async fn probe_workers(&self, sid: &str) -> Option<(String, String)> {
		let mut probes: FuturesUnordered<_> = self
			.table
			.live()
			.into_iter()
			.map(|worker| {
				let client = self.sandbox_client(&worker.url);
				let request = self.request(pb::SandboxRef { id: sid.to_owned() });
				async move {
					let mut client = client.ok()?;
					match timeout(FANOUT_TIMEOUT, client.get(request)).await {
						Ok(Ok(_view)) => Some((worker.wid, worker.url)),
						_ => None,
					}
				}
			})
			.collect();
		while let Some(hit) = probes.next().await {
			if hit.is_some() {
				return hit;
			}
		}
		None
	}

	/// Enqueue the route for a freshly-created sandbox off the critical path.
	fn write_route_detached(&self, sid: String, wid: String, url: String, state: String) {
		let route = SandboxRoute { sid: sid.clone(), wid, url, state, ts_ms: now_ms(), result: None };
		let Ok(payload) = serde_json::to_vec(&route) else {
			return;
		};
		if let Err(error) = self
			.route_writes
			.try_send(RouteWrite { sid: sid.clone(), payload })
		{
			tracing::warn!(sid, %error, "orch: route write queue unavailable; dropping route write");
		}
	}

	fn record_idempotent_winner(&self, key: &str, sid: &str) {
		let redis = self.redis.clone();
		let key = idem_key(key);
		let sid = sid.as_bytes().to_vec();
		tokio::spawn(async move {
			if let Err(error) = redis.set_px(&key, &sid, IDEM_TTL_MS).await {
				tracing::warn!(%error, "orch: idempotency record failed");
			}
		});
	}
}

async fn route_writer(redis: Redis, mut writes: mpsc::Receiver<RouteWrite>) {
	while let Some(first) = writes.recv().await {
		let mut batch = Vec::with_capacity(ROUTE_WRITE_BATCH_MAX);
		batch.push((keys::sandbox(&first.sid), first.payload));
		while batch.len() < ROUTE_WRITE_BATCH_MAX {
			match writes.try_recv() {
				Ok(write) => batch.push((keys::sandbox(&write.sid), write.payload)),
				Err(_) => break,
			}
		}
		if let Err(error) = redis.set_px_pipeline(&batch, super::ROUTE_TTL_MS).await {
			tracing::warn!(routes = batch.len(), %error, "orch: route write batch failed");
		}
	}
}

fn idem_key(key: &str) -> String {
	format!("vmon:o:idem:{key}")
}

/// Build a scheduler-origin gRPC status carrying the stable vmond code.
fn coded(code: Code, vmon_code: &str, retryable: bool, message: impl Into<String>) -> Status {
	let mut status = Status::new(code, message.into());
	if let Ok(value) = MetadataValue::try_from(vmon_code) {
		status.metadata_mut().insert("vmon-code", value);
	}
	status.metadata_mut().insert(
		"vmon-retryable",
		MetadataValue::from_static(if retryable { "true" } else { "false" }),
	);
	status
}

/// Map a failed create onto the stable per-item batch error, mirroring the
/// `vmon-code` metadata the unary path carries on its `Status`.
fn batch_error(status: &Status) -> pb::BatchCreateError {
	let code = status
		.metadata()
		.get("vmon-code")
		.and_then(|value| value.to_str().ok())
		.map_or_else(
			|| {
				match status.code() {
					Code::Aborted => "busy",
					Code::InvalidArgument => "invalid",
					Code::NotFound => "not_found",
					Code::DeadlineExceeded => "deadline",
					_ => "engine_error",
				}
				.to_owned()
			},
			str::to_owned,
		);
	pb::BatchCreateError { code, message: status.message().to_owned() }
}

/// Whether a failed create attempt may be retried on another worker.
/// `Aborted` is the worker admission gate saying `busy`; the transport codes
/// mean we never reached a healthy worker.
const fn create_retryable(code: Code) -> bool {
	matches!(code, Code::Aborted | Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled)
}

/// Annotate a sandbox view JSON document with its owner. `JsonView` is
/// schemaless by contract, so this is additive for every SDK.
fn annotate_view(json: &str, wid: &str, url: &str) -> String {
	let Ok(Value::Object(mut view)) = serde_json::from_str::<Value>(json) else {
		return json.to_owned();
	};
	view.insert("node".to_owned(), Value::String(wid.to_owned()));
	view.insert("endpoint".to_owned(), Value::String(url.to_owned()));
	serde_json::to_string(&Value::Object(view)).unwrap_or_else(|_| json.to_owned())
}

fn view_sid(json: &str) -> Option<String> {
	let view: Value = serde_json::from_str(json).ok()?;
	view
		.get("name")
		.or_else(|| view.get("id"))
		.and_then(Value::as_str)
		.map(str::to_owned)
}

fn view_state(json: &str) -> String {
	serde_json::from_str::<Value>(json)
		.ok()
		.and_then(|view| {
			view
				.get("status")
				.and_then(Value::as_str)
				.map(str::to_owned)
		})
		.unwrap_or_else(|| "running".to_owned())
}

fn relay<T: Send + 'static>(response: Response<Streaming<T>>) -> Response<BoxStream<T>> {
	let (metadata, stream, extensions) = response.into_parts();
	let stream: BoxStream<T> = Box::pin(stream);
	Response::from_parts(metadata, stream, extensions)
}

/// The scheduler's tonic service implementation.
#[derive(Clone)]
pub struct SchedGrpc {
	core: Arc<SchedCore>,
}

impl SchedGrpc {
	pub const fn new(core: Arc<SchedCore>) -> Self {
		Self { core }
	}

	/// Place and forward one create attempt loop.
	async fn place_and_create(
		&self,
		spec_json: String,
		no_wait: bool,
		needs: &PlacementNeeds,
	) -> Result<Response<pb::JsonView>, Status> {
		let mut tried: Vec<String> = Vec::new();
		let mut last: Option<Status> = None;
		for _attempt in 0..=self.core.options.create_retries {
			let Some(placement) = self.core.table.pick(needs, &tried) else {
				break;
			};
			let mut client = self.core.sandbox_client(&placement.url)?;
			let request = self
				.core
				.request(pb::CreateSandboxRequest { spec_json: spec_json.clone(), no_wait });
			let outcome = timeout(self.core.options.create_timeout, client.create(request)).await;
			match outcome {
				Ok(Ok(response)) => {
					self.core.created.fetch_add(1, Ordering::Relaxed);
					let (metadata, view, extensions) = response.into_parts();
					let annotated = annotate_view(&view.json, &placement.wid, &placement.url);
					if let Some(sid) = view_sid(&view.json) {
						self.core.cache_route(&sid, &placement.wid, &placement.url);
						self.core.write_route_detached(
							sid,
							placement.wid.clone(),
							placement.url.clone(),
							view_state(&view.json),
						);
					}
					return Ok(Response::from_parts(
						metadata,
						pb::JsonView { json: annotated },
						extensions,
					));
				},
				Ok(Err(status)) if create_retryable(status.code()) => {
					tracing::info!(
						worker = %placement.wid,
						code = ?status.code(),
						"orch: create attempt rejected; retrying elsewhere"
					);
					self.core.table.penalize(&placement.wid, PENALTY);
					tried.push(placement.wid.clone());
					last = Some(status);
				},
				Ok(Err(status)) => return Err(status), // deterministic (invalid spec, …)
				Err(_elapsed) => {
					self.core.table.penalize(&placement.wid, PENALTY);
					tried.push(placement.wid.clone());
					last = Some(coded(
						Code::DeadlineExceeded,
						"deadline",
						true,
						format!("create on worker {} timed out", placement.wid),
					));
				},
			}
		}
		Err(last.unwrap_or_else(|| {
			coded(
				Code::Aborted,
				"busy",
				true,
				format!(
					"no worker with capacity for {} MiB (arch {:?}); {} live workers",
					needs.mem_mib,
					needs.arch,
					self.core.table.live().len()
				),
			)
		}))
	}

	/// Wait for the idempotent winner's sandbox and return its view.
	async fn await_idempotent_winner(&self, key: &str) -> Result<Response<pb::JsonView>, Status> {
		let deadline = tokio::time::Instant::now() + self.core.options.create_timeout;
		loop {
			match self.core.redis.get(&idem_key(key)).await {
				Ok(Some(raw)) => {
					let value = String::from_utf8_lossy(&raw).into_owned();
					if let Some(sid) = value.strip_prefix("sid:") {
						let (wid, url) = self.core.resolve_sandbox(sid).await?;
						let mut client = self.core.sandbox_client(&url)?;
						let response = client
							.get(self.core.request(pb::SandboxRef { id: sid.to_owned() }))
							.await?;
						let (metadata, view, extensions) = response.into_parts();
						let annotated = annotate_view(&view.json, &wid, &url);
						return Ok(Response::from_parts(
							metadata,
							pb::JsonView { json: annotated },
							extensions,
						));
					}
					// Still pending on another scheduler.
				},
				Ok(None) => {
					// Winner failed and released the gate; caller retries.
					return Err(coded(
						Code::Aborted,
						"busy",
						true,
						"idempotent create released by another scheduler; retry",
					));
				},
				Err(error) => {
					tracing::warn!(%error, "orch: idempotency poll failed");
				},
			}
			if tokio::time::Instant::now() >= deadline {
				return Err(coded(
					Code::Aborted,
					"busy",
					true,
					"idempotent create still in progress elsewhere",
				));
			}
			tokio::time::sleep(IDEM_POLL).await;
		}
	}

	/// Admit, place, and forward one create: the shared body of unary
	/// `Create` and each `BatchCreate` item, including the idempotency
	/// winner gate (spec-level `idempotency_key`).
	async fn create_one(
		&self,
		message: pb::CreateSandboxRequest,
	) -> Result<Response<pb::JsonView>, Status> {
		let spec: Value = serde_json::from_str(&message.spec_json)
			.map_err(|_| coded(Code::InvalidArgument, "invalid", false, "invalid create spec JSON"))?;
		let needs = PlacementNeeds::from_create_json(&spec);
		let idempotency = spec
			.get("idempotency_key")
			.and_then(Value::as_str)
			.filter(|key| !key.is_empty())
			.map(str::to_owned);

		if let Some(key) = &idempotency {
			let marker = format!("pending:{}", self.core.instance);
			match self
				.core
				.redis
				.set_nx_px(&idem_key(key), marker.as_bytes(), IDEM_TTL_MS)
				.await
			{
				Ok(true) => {
					// Winner: create, then durably map key → sid.
					let response = self
						.place_and_create(message.spec_json, message.no_wait, &needs)
						.await;
					match &response {
						Ok(view) => {
							if let Some(sid) = view_sid(&view.get_ref().json) {
								self
									.core
									.record_idempotent_winner(key, &format!("sid:{sid}"));
							}
						},
						Err(_status) => {
							// Release the gate so a retry can win again.
							let redis = self.core.redis.clone();
							let key = idem_key(key);
							tokio::spawn(async move {
								let _ = redis.del(&key).await;
							});
						},
					}
					return response;
				},
				Ok(false) => return self.await_idempotent_winner(key).await,
				Err(error) => {
					// Redis down: degrade to at-least-once rather than fail.
					tracing::warn!(%error, "orch: idempotency gate unavailable");
				},
			}
		}
		self
			.place_and_create(message.spec_json, message.no_wait, &needs)
			.await
	}

	async fn run_batch_create<S>(
		self,
		mut inbound: S,
		results: mpsc::Sender<Result<pb::BatchCreateResponse, Status>>,
	) where
		S: futures_util::Stream<Item = Result<pb::BatchCreateRequest, Status>>
			+ Send
			+ Unpin
			+ 'static,
	{
		let permits = Arc::new(Semaphore::new(BATCH_CONCURRENCY));
		while let Some(item) = inbound.next().await {
			let item = match item {
				Ok(item) => item,
				Err(status) => {
					let _ = results.send(Err(status)).await;
					return;
				},
			};
			let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
				return; // The semaphore is never closed.
			};
			let this = self.clone();
			let results = results.clone();
			tokio::spawn(async move {
				let _permit = permit;
				let outcome = match item.create {
					Some(create) => match this.create_one(create).await {
						Ok(view) => pb::batch_create_response::Outcome::Json(view.into_inner().json),
						Err(status) => pb::batch_create_response::Outcome::Error(batch_error(&status)),
					},
					None => pb::batch_create_response::Outcome::Error(pb::BatchCreateError {
						code:    "invalid".to_owned(),
						message: "batch item carries no create payload".to_owned(),
					}),
				};
				let response = pb::BatchCreateResponse { seq: item.seq, outcome: Some(outcome) };
				// A client hang-up drops the receiver; nothing to clean up.
				let _ = results.send(Ok(response)).await;
			});
		}
	}
}

#[tonic::async_trait]
impl pb::sandbox_service_server::SandboxService for SchedGrpc {
	type AttachStream = BoxStream<pb::ExecOutput>;
	type BatchCreateStream = BoxStream<pb::BatchCreateResponse>;
	type ExecStream = BoxStream<pb::ExecOutput>;
	type LogsStream = BoxStream<pb::LogChunk>;
	type PtyAttachStream = BoxStream<pb::ExecOutput>;
	type PtyOpenStream = BoxStream<pb::ExecOutput>;
	type ShellStream = BoxStream<pb::ExecOutput>;
	type WatchStream = BoxStream<pb::JsonView>;

	async fn batch_create(
		&self,
		request: Request<Streaming<pb::BatchCreateRequest>>,
	) -> Result<Response<Self::BatchCreateStream>, Status> {
		let inbound = request.into_inner();
		let (results, outbound) = mpsc::channel(BATCH_RESULT_QUEUE);
		tokio::spawn(self.clone().run_batch_create(inbound, results));
		Ok(Response::new(
			Box::pin(tokio_stream::wrappers::ReceiverStream::new(outbound)) as Self::BatchCreateStream
		))
	}

	async fn watch(
		&self,
		request: Request<pb::WatchSandboxRequest>,
	) -> Result<Response<Self::WatchStream>, Status> {
		let message = request.into_inner();
		let mut route = self.core.resolve_sandbox(&message.id).await;
		if message.until_ready {
			// An admitted (`no_wait`) create writes its route detached, so a
			// prompt watcher may race the route write. Bound the wait by the
			// create timeout: past one full create budget, an unroutable sid
			// is a genuine miss, not a pending write.
			let deadline = tokio::time::Instant::now() + self.core.options.create_timeout;
			while matches!(&route, Err(status) if status.code() == Code::NotFound)
				&& tokio::time::Instant::now() < deadline
			{
				tokio::time::sleep(WATCH_ROUTE_POLL).await;
				route = self.core.resolve_sandbox(&message.id).await;
			}
		}
		let (_wid, url) = route?;
		let mut client = self.core.sandbox_client(&url)?;
		client.watch(self.core.request(message)).await.map(relay)
	}

	async fn create(
		&self,
		request: Request<pb::CreateSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		self.create_one(request.into_inner()).await
	}

	async fn list(
		&self,
		request: Request<pb::ListSandboxesRequest>,
	) -> Result<Response<pb::ListSandboxesResponse>, Status> {
		let message = request.into_inner();
		let workers = self.core.table.live();
		let mut calls: FuturesUnordered<_> = workers
			.into_iter()
			.map(|worker| {
				let core = Arc::clone(&self.core);
				let tags = message.tags.clone();
				async move {
					let mut client = core.sandbox_client(&worker.url).ok()?;
					let response = timeout(
						FANOUT_TIMEOUT,
						client.list(core.request(pb::ListSandboxesRequest { tags })),
					)
					.await;
					match response {
						Ok(Ok(list)) => Some((worker, list.into_inner().sandboxes_json)),
						Ok(Err(status)) => {
							tracing::warn!(worker = %worker.wid, %status, "orch: list fan-out failed");
							None
						},
						Err(_elapsed) => {
							tracing::warn!(worker = %worker.wid, "orch: list fan-out timed out");
							None
						},
					}
				}
			})
			.collect();
		let mut merged = Vec::new();
		while let Some(result) = calls.next().await {
			if let Some((worker, views)) = result {
				merged.extend(
					views
						.iter()
						.map(|view| annotate_view(view, &worker.wid, &worker.url)),
				);
			}
		}
		Ok(Response::new(pb::ListSandboxesResponse { sandboxes_json: merged }))
	}

	async fn get(&self, request: Request<pb::SandboxRef>) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		let response = client.get(self.core.request(message)).await?;
		let (metadata, view, extensions) = response.into_parts();
		let annotated = annotate_view(&view.json, &wid, &url);
		Ok(Response::from_parts(metadata, pb::JsonView { json: annotated }, extensions))
	}

	async fn stop(
		&self,
		request: Request<pb::StopSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.stop(self.core.request(message)).await
	}

	async fn remove(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let sid = message.id.clone();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		let response = client.remove(self.core.request(message)).await?;
		// Removal erases the identity; drop caches and the Redis route.
		self.core.drop_route(&sid);
		let redis = self.core.redis.clone();
		tokio::spawn(async move {
			let _ = redis.del(&keys::sandbox(&sid)).await;
		});
		Ok(response)
	}

	async fn terminate(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.terminate(self.core.request(message)).await
	}

	async fn pause(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.pause(self.core.request(message)).await
	}

	async fn resume(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.resume(self.core.request(message)).await
	}

	async fn suspend(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.suspend(self.core.request(message)).await
	}

	async fn extend(
		&self,
		request: Request<pb::ExtendSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.extend(self.core.request(message)).await
	}

	async fn set_idle_timeout(
		&self,
		request: Request<pb::SetIdleTimeoutRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.set_idle_timeout(self.core.request(message)).await
	}

	async fn resize(
		&self,
		request: Request<pb::ResizeSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.resize(self.core.request(message)).await
	}

	async fn pty_list(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::PtySessionList>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.pty_list(self.core.request(message)).await
	}

	async fn pty_close(
		&self,
		request: Request<pb::PtyCloseRequest>,
	) -> Result<Response<pb::PtySessionCloseResponse>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.pty_close(self.core.request(message)).await
	}

	async fn pty_exec(
		&self,
		request: Request<pb::PtyExecRequest>,
	) -> Result<Response<pb::PtyExecResponse>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.pty_exec(self.core.request(message)).await
	}

	async fn metrics(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.metrics(self.core.request(message)).await
	}

	async fn logs(
		&self,
		request: Request<pb::LogsRequest>,
	) -> Result<Response<Self::LogsStream>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.logs(self.core.request(message)).await.map(relay)
	}

	async fn exec_capture(
		&self,
		request: Request<pb::ExecCaptureRequest>,
	) -> Result<Response<pb::ExecCaptureResponse>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.exec_capture(self.core.request(message)).await
	}

	async fn exec(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ExecStream>, Status> {
		let mut inbound = request.into_inner();
		let first = inbound
			.message()
			.await?
			.ok_or_else(|| coded(Code::InvalidArgument, "invalid", false, "empty exec stream"))?;
		let sid = match &first.input {
			Some(pb::exec_input::Input::Start(start)) if !start.sandbox_id.is_empty() => {
				start.sandbox_id.clone()
			},
			_ => {
				return Err(coded(
					Code::InvalidArgument,
					"invalid",
					false,
					"first exec frame must be a start payload naming a sandbox",
				));
			},
		};
		let (_wid, url) = self.core.resolve_sandbox(&sid).await?;
		let mut client = self.core.sandbox_client(&url)?;
		let outbound = tokio_stream::once(first)
			.chain(tokio_stream::StreamExt::filter_map(inbound, |frame| frame.ok()));
		client.exec(self.core.request(outbound)).await.map(relay)
	}

	async fn pty_open(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyOpenStream>, Status> {
		let mut inbound = request.into_inner();
		let first = inbound
			.message()
			.await?
			.ok_or_else(|| coded(Code::InvalidArgument, "invalid", false, "empty pty-open stream"))?;
		let sid = match &first.input {
			Some(pb::exec_input::Input::PtyOpen(open)) if !open.sandbox_id.is_empty() => {
				open.sandbox_id.clone()
			},
			_ => {
				return Err(coded(
					Code::InvalidArgument,
					"invalid",
					false,
					"first pty-open frame must be a pty_open payload naming a sandbox",
				));
			},
		};
		let (_wid, url) = self.core.resolve_sandbox(&sid).await?;
		let mut client = self.core.sandbox_client(&url)?;
		let outbound = tokio_stream::once(first)
			.chain(tokio_stream::StreamExt::filter_map(inbound, |frame| frame.ok()));
		client
			.pty_open(self.core.request(outbound))
			.await
			.map(relay)
	}

	async fn pty_attach(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyAttachStream>, Status> {
		let mut inbound = request.into_inner();
		let first = inbound.message().await?.ok_or_else(|| {
			coded(Code::InvalidArgument, "invalid", false, "empty pty-attach stream")
		})?;
		let sid = match &first.input {
			Some(pb::exec_input::Input::PtyAttach(attach)) if !attach.sandbox_id.is_empty() => {
				attach.sandbox_id.clone()
			},
			_ => {
				return Err(coded(
					Code::InvalidArgument,
					"invalid",
					false,
					"first pty-attach frame must be a pty_attach payload naming a sandbox",
				));
			},
		};
		let (_wid, url) = self.core.resolve_sandbox(&sid).await?;
		let mut client = self.core.sandbox_client(&url)?;
		let outbound = tokio_stream::once(first)
			.chain(tokio_stream::StreamExt::filter_map(inbound, |frame| frame.ok()));
		client
			.pty_attach(self.core.request(outbound))
			.await
			.map(relay)
	}

	async fn shell(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ShellStream>, Status> {
		let mut inbound = request.into_inner();
		let first = inbound
			.message()
			.await?
			.ok_or_else(|| coded(Code::InvalidArgument, "invalid", false, "empty shell stream"))?;
		let Some(pb::exec_input::Input::ShellParamsJson(params_json)) = &first.input else {
			return Err(coded(
				Code::InvalidArgument,
				"invalid",
				false,
				"first shell frame must carry shell params JSON",
			));
		};
		let params: Value = if params_json.trim().is_empty() {
			Value::Object(serde_json::Map::new())
		} else {
			serde_json::from_str(params_json).map_err(|_| {
				coded(Code::InvalidArgument, "invalid", false, "invalid shell params JSON")
			})?
		};
		// A named shell attaches to an existing sandbox; an anonymous shell
		// creates an ephemeral one, so it is scheduled like a create.
		let named = params
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
			.map(str::to_owned);
		let url = if let Some(sid) = named {
			self.core.resolve_sandbox(&sid).await?.1
		} else {
			let needs = PlacementNeeds::from_create_json(&params);
			let Some(placement) = self.core.table.pick(&needs, &[]) else {
				return Err(coded(
					Code::Aborted,
					"busy",
					true,
					"no worker with capacity for an ephemeral shell",
				));
			};
			placement.url.clone()
		};
		let mut client = self.core.sandbox_client(&url)?;
		let outbound = tokio_stream::once(first)
			.chain(tokio_stream::StreamExt::filter_map(inbound, |frame| frame.ok()));
		client.shell(self.core.request(outbound)).await.map(relay)
	}

	async fn attach(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<Self::AttachStream>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.attach(self.core.request(message)).await.map(relay)
	}

	async fn file_read(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::FileContent>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.file_read(self.core.request(message)).await
	}

	async fn file_write(
		&self,
		request: Request<pb::FileWriteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.file_write(self.core.request(message)).await
	}

	async fn file_delete(
		&self,
		request: Request<pb::FileDeleteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.file_delete(self.core.request(message)).await
	}

	async fn file_list(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.file_list(self.core.request(message)).await
	}

	async fn file_stat(
		&self,
		request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.file_stat(self.core.request(message)).await
	}

	async fn network_get(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.network_get(self.core.request(message)).await
	}

	async fn network_set(
		&self,
		request: Request<pb::NetworkSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.network_set(self.core.request(message)).await
	}

	async fn tunnels(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.tunnels(self.core.request(message)).await
	}

	async fn migrate(
		&self,
		request: Request<pb::MigrateRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.migrate(self.core.request(message)).await
	}

	async fn snapshot(
		&self,
		request: Request<pb::SnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.snapshot(self.core.request(message)).await
	}

	async fn snapshot_fs(
		&self,
		request: Request<pb::SnapshotFsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.snapshot_fs(self.core.request(message)).await
	}

	async fn history(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::RecoveryPointList>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.history(self.core.request(message)).await
	}

	async fn rollback(
		&self,
		request: Request<pb::RollbackSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.core.resolve_sandbox(&message.id).await?;
		let mut client = self.core.sandbox_client(&url)?;
		client.rollback(self.core.request(message)).await
	}
}

impl SchedGrpc {
	/// Find the live worker that has snapshot `name` registered.
	async fn locate_snapshot(&self, name: &str) -> Result<(String, String), Status> {
		let workers = self.core.table.live();
		let mut calls: FuturesUnordered<_> = workers
			.into_iter()
			.map(|worker| {
				let core = Arc::clone(&self.core);
				let name = name.to_owned();
				async move {
					let mut client = core.snapshot_client(&worker.url).ok()?;
					let response =
						timeout(FANOUT_TIMEOUT, client.list(core.request(pb::ListSnapshotsRequest {})))
							.await;
					match response {
						Ok(Ok(list)) if list.get_ref().snapshots.iter().any(|snap| snap == &name) => {
							Some((worker.wid, worker.url))
						},
						_ => None,
					}
				}
			})
			.collect();
		while let Some(hit) = calls.next().await {
			if let Some(found) = hit {
				return Ok(found);
			}
		}
		Err(coded(
			Code::NotFound,
			"not_found",
			false,
			format!("snapshot {name} not found on any live worker"),
		))
	}
}

#[tonic::async_trait]
impl pb::snapshot_service_server::SnapshotService for SchedGrpc {
	async fn list(
		&self,
		_request: Request<pb::ListSnapshotsRequest>,
	) -> Result<Response<pb::SnapshotList>, Status> {
		let workers = self.core.table.live();
		let mut calls: FuturesUnordered<_> = workers
			.into_iter()
			.map(|worker| {
				let core = Arc::clone(&self.core);
				async move {
					let mut client = core.snapshot_client(&worker.url).ok()?;
					timeout(FANOUT_TIMEOUT, client.list(core.request(pb::ListSnapshotsRequest {})))
						.await
						.ok()?
						.ok()
						.map(|list| list.into_inner().snapshots)
				}
			})
			.collect();
		let mut merged = Vec::new();
		while let Some(result) = calls.next().await {
			if let Some(snapshots) = result {
				merged.extend(snapshots);
			}
		}
		merged.sort_unstable();
		merged.dedup();
		Ok(Response::new(pb::SnapshotList { snapshots: merged }))
	}

	async fn restore(
		&self,
		request: Request<pb::RestoreSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (wid, url) = self.locate_snapshot(&message.name).await?;
		let mut client = self.core.snapshot_client(&url)?;
		let response = client.restore(self.core.request(message)).await?;
		let (metadata, view, extensions) = response.into_parts();
		if let Some(sid) = view_sid(&view.json) {
			self.core.cache_route(&sid, &wid, &url);
			self
				.core
				.write_route_detached(sid, wid.clone(), url.clone(), view_state(&view.json));
		}
		let annotated = annotate_view(&view.json, &wid, &url);
		Ok(Response::from_parts(metadata, pb::JsonView { json: annotated }, extensions))
	}

	async fn fork(
		&self,
		request: Request<pb::ForkSnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let message = request.into_inner();
		let (wid, url) = self.locate_snapshot(&message.name).await?;
		let mut client = self.core.snapshot_client(&url)?;
		let response = client.fork(self.core.request(message)).await?;
		let (metadata, view, extensions) = response.into_parts();
		// Fork returns `{"clones": [<view>, ...]}`; annotate and route each.
		let json = match serde_json::from_str::<Value>(&view.json) {
			Ok(Value::Object(mut body)) => {
				if let Some(Value::Array(clones)) = body.get_mut("clones") {
					for clone in clones.iter_mut() {
						if let Value::Object(clone_view) = clone {
							clone_view.insert("node".to_owned(), Value::String(wid.clone()));
							clone_view.insert("endpoint".to_owned(), Value::String(url.clone()));
							let sid = clone_view
								.get("name")
								.or_else(|| clone_view.get("id"))
								.and_then(Value::as_str)
								.map(str::to_owned);
							let state = clone_view
								.get("status")
								.and_then(Value::as_str)
								.unwrap_or("running")
								.to_owned();
							if let Some(sid) = sid {
								self.core.cache_route(&sid, &wid, &url);
								self
									.core
									.write_route_detached(sid, wid.clone(), url.clone(), state);
							}
						}
					}
				}
				serde_json::to_string(&Value::Object(body)).unwrap_or(view.json)
			},
			_ => view.json,
		};
		Ok(Response::from_parts(metadata, pb::JsonView { json }, extensions))
	}

	async fn delete(&self, request: Request<pb::SnapshotRef>) -> Result<Response<pb::Ok>, Status> {
		let message = request.into_inner();
		let (_wid, url) = self.locate_snapshot(&message.name).await?;
		let mut client = self.core.snapshot_client(&url)?;
		client.delete(self.core.request(message)).await
	}
}

#[tonic::async_trait]
impl pb::system_service_server::SystemService for SchedGrpc {
	type EventsStream = BoxStream<pb::JsonView>;

	async fn info(
		&self,
		_request: Request<pb::InfoRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let live = self.core.table.live();
		let doc = serde_json::json!({
			"role": "scheduler",
			"version": env!("CARGO_PKG_VERSION"),
			"instance": self.core.instance,
			"workers_live": live.len(),
			"workers_known": self.core.table.len(),
			"capacity": {
				"vcpus": live.iter().map(|worker| worker.caps.vcpus).sum::<u64>(),
				"mem_mib": live.iter().map(|worker| worker.caps.mem_mib).sum::<u64>(),
			},
			"used": {
				"vcpus": live.iter().map(|worker| worker.used.vcpus).sum::<u64>(),
				"mem_mib": live.iter().map(|worker| worker.used.mem_mib).sum::<u64>(),
			},
			"running": live.iter().map(|worker| worker.running).sum::<u64>(),
		});
		Ok(Response::new(pb::JsonView { json: doc.to_string() }))
	}

	async fn events(
		&self,
		_request: Request<pb::EventsRequest>,
	) -> Result<Response<Self::EventsStream>, Status> {
		Err(coded(
			Code::Unimplemented,
			"unsupported",
			false,
			"the scheduler does not stream engine events; subscribe on a worker",
		))
	}

	async fn mesh_status(
		&self,
		_request: Request<pb::MeshStatusRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let workers: Vec<Value> = self
			.core
			.table
			.live()
			.into_iter()
			.map(|worker| {
				serde_json::json!({
					"id": worker.wid,
					"url": worker.url,
					"arch": worker.arch,
					"backend": worker.backend,
					"running": worker.running,
					"accepting": worker.accepting,
					"draining": worker.draining,
					"caps": {"vcpus": worker.caps.vcpus, "mem_mib": worker.caps.mem_mib},
					"used": {"vcpus": worker.used.vcpus, "mem_mib": worker.used.mem_mib},
				})
			})
			.collect();
		let doc = serde_json::json!({
			"mesh": false,
			"orch": {"scheduler": self.core.instance, "workers": workers},
		});
		Ok(Response::new(pb::JsonView { json: doc.to_string() }))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn route_enqueue_does_not_wait_for_redis() {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("bind");
		let addr = listener.local_addr().expect("address");
		let _server = tokio::spawn(async move {
			let (_stream, _) = listener.accept().await.expect("accept");
			std::future::pending::<()>().await;
		});
		let redis = Redis::new(&format!("redis://{addr}")).expect("redis");
		let table = Arc::new(WorkerTable::new(super::super::table::TableConfig::from_dead_after(
			Duration::from_secs(1),
		)));
		let core = SchedCore::new(table, redis, SchedGrpcOptions::default(), "test".to_owned())
			.expect("core");

		timeout(Duration::from_millis(50), async {
			core.cache_route("sb-fast", "worker", "http://worker");
			core.write_route_detached(
				"sb-fast".to_owned(),
				"worker".to_owned(),
				"http://worker".to_owned(),
				"running".to_owned(),
			);
		})
		.await
		.expect("enqueue must not wait for Redis");
		assert_eq!(
			core.route_cache.get("sb-fast").map(|route| route.clone()),
			Some(("worker".to_owned(), "http://worker".to_owned()))
		);
	}

	#[test]
	fn annotates_views_and_extracts_identity() {
		let raw = r#"{"name":"sb-1","status":"running","cpus":2}"#;
		let annotated = annotate_view(raw, "w1", "http://worker:8000");
		let view: Value = serde_json::from_str(&annotated).expect("annotated json");
		assert_eq!(view["node"], "w1");
		assert_eq!(view["endpoint"], "http://worker:8000");
		assert_eq!(view["cpus"], 2, "original fields must survive");
		assert_eq!(view_sid(&annotated).as_deref(), Some("sb-1"));
		assert_eq!(view_state(&annotated), "running");
		// Malformed payloads pass through untouched rather than erroring.
		assert_eq!(annotate_view("not-json", "w", "u"), "not-json");
	}

	#[test]
	fn retryable_create_codes_match_admission_contract() {
		assert!(create_retryable(Code::Aborted), "busy admission rejects must retry");
		assert!(create_retryable(Code::Unavailable));
		assert!(create_retryable(Code::DeadlineExceeded));
		assert!(!create_retryable(Code::InvalidArgument), "bad specs must fail fast");
		assert!(!create_retryable(Code::NotFound));
	}

	#[test]
	fn scheduler_statuses_carry_stable_codes() {
		let status = coded(Code::Aborted, "busy", true, "no capacity");
		assert_eq!(status.code(), Code::Aborted);
		assert_eq!(
			status
				.metadata()
				.get("vmon-code")
				.and_then(|value| value.to_str().ok()),
			Some("busy")
		);
		assert_eq!(
			status
				.metadata()
				.get("vmon-retryable")
				.and_then(|value| value.to_str().ok()),
			Some("true")
		);
	}

	#[test]
	fn batch_errors_carry_stable_codes() {
		let busy = batch_error(&coded(Code::Aborted, "busy", true, "no capacity"));
		assert_eq!(busy.code, "busy", "vmon-code metadata is the code of record");
		assert_eq!(busy.message, "no capacity");
		// Without vmon-code metadata the gRPC code maps to a stable default.
		let bare = batch_error(&Status::aborted("worker at capacity"));
		assert_eq!(bare.code, "busy", "ABORTED without metadata still maps to busy");
		let invalid = batch_error(&Status::invalid_argument("bad spec"));
		assert_eq!(invalid.code, "invalid");
	}

	#[tokio::test]
	async fn batch_stream_forwards_inbound_transport_errors() {
		let redis = Redis::new("redis://127.0.0.1:1").expect("redis");
		let table = Arc::new(WorkerTable::new(super::super::table::TableConfig::from_dead_after(
			Duration::from_secs(1),
		)));
		let core = SchedCore::new(table, redis, SchedGrpcOptions::default(), "test".to_owned())
			.expect("core");
		let scheduler = SchedGrpc::new(core);
		let inbound = tokio_stream::iter([Err::<pb::BatchCreateRequest, _>(Status::data_loss(
			"broken inbound stream",
		))]);
		let (sender, mut receiver) = mpsc::channel(1);

		scheduler.run_batch_create(inbound, sender).await;

		let status = receiver
			.recv()
			.await
			.expect("terminal stream item")
			.expect_err("transport failure must remain an error");
		assert_eq!(status.code(), Code::DataLoss);
		assert_eq!(status.message(), "broken inbound stream");
		assert!(receiver.recv().await.is_none(), "terminal error closes the stream");
	}

	#[test]
	fn admitted_views_report_starting_state() {
		// `no_wait` creates come back `starting`; the route write must record
		// that state verbatim, which rides on `view_state`.
		assert_eq!(view_state(r#"{"name":"sb-1","status":"starting"}"#), "starting");
	}
}
