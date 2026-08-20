//! `vmon sched` server assembly: listeners, auth, background loops.
//!
//! One scheduler process serves the vmon.v1 `SandboxService`,
//! `SnapshotService`, and `SystemService` over native h2c and runs three
//! background loops:
//!
//! 1. **Table feed** — bootstrap the worker table from the self-expiring
//!    `vmon:o:w:*` keys, then follow the workers stream for updates
//!    (re-bootstrapping after Redis hiccups).
//! 2. **Leadership** — renew the `vmon:o:leader` lease every tick; the holder
//!    runs the controller janitor and, when configured, the autoscaler.
//!    Non-leaders keep their tables and stay ready.
//! 3. **Serving** — an axum router carrying the tonic services plus `/healthz`,
//!    behind a bearer-token guard.
//!
//! `--redis` may be omitted for single-host development: an embedded
//! [`super::miniredis`] instance is spawned and workers can be pointed at
//! the printed URL. Production deployments always name a real Redis.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{
	Json, Router,
	body::Body,
	extract::ws::WebSocketUpgrade,
	http::{Method, Request, header},
	middleware::{self, Next},
	response::{IntoResponse, Response},
	routing::get,
};
use tokio::{net::TcpListener, sync::broadcast, time::Instant};
use tonic::service::Routes;
use vmon_proto::v1 as pb;

use super::{
	autoscaler::{Autoscaler, AutoscalerOptions},
	controller::{Controller, LeaderLease},
	dashboard, keys,
	miniredis::MiniRedis,
	redis::{Redis, StreamFollower},
	sched::{DEFAULT_CREATE_TIMEOUT, SchedCore, SchedGrpc, SchedGrpcOptions},
	table::{TableConfig, WorkerTable},
};
use crate::{EngineError, Result, security::Principal};

/// Mirror of the API layer's uniform message cap.
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
/// Leadership lease lifetime; renewed at half-life by the leader loop.
const LEADER_TTL: Duration = Duration::from_secs(10);
/// Cadence of the leadership/controller loop.
const LEADER_TICK: Duration = Duration::from_secs(2);
/// Stream-follow block window.
const FOLLOW_BLOCK_MS: u64 = 5_000;

/// `vmon sched` configuration, resolved by the CLI.
#[derive(Clone, Debug)]
pub struct SchedulerOptions {
	/// TCP bind address (`host:port`).
	pub listen:         String,
	/// Redis URL; `None` spawns an embedded dev [`MiniRedis`].
	pub redis_url:      Option<String>,
	/// Inbound bearer token; required for non-loopback binds.
	pub token:          Option<String>,
	/// Bearer token attached to worker-bound RPCs.
	pub worker_token:   Option<String>,
	/// Worker liveness window (matches the workers' `--orch-dead-after-sec`).
	pub dead_after:     Duration,
	/// Worker-create deadline; defaults to two minutes so measured cold template
	/// builds have margin without slowing ordinary operations.
	pub create_timeout: Duration,
	/// Additional placement attempts after a rejected create.
	pub create_retries: usize,
	/// HPA policy; `None` disables scaling decisions (janitor still runs).
	pub autoscaler:     Option<AutoscalerOptions>,
}

impl Default for SchedulerOptions {
	fn default() -> Self {
		Self {
			listen:         "127.0.0.1:8100".to_owned(),
			redis_url:      None,
			token:          None,
			worker_token:   None,
			dead_after:     Duration::from_secs(5),
			create_timeout: DEFAULT_CREATE_TIMEOUT,
			create_retries: 3,
			autoscaler:     None,
		}
	}
}

/// A running scheduler: bound address, shared core, and task lifecycle.
/// Dropping the handle (or calling [`SchedulerHandle::shutdown`]) stops the
/// listener and every background loop.
pub struct SchedulerHandle {
	/// Bound listen address (useful with an ephemeral port).
	pub addr: SocketAddr,
	/// Shared scheduler state (worker table access for tests/diagnostics).
	pub core: Arc<SchedCore>,
	/// Embedded dev Redis, when one was spawned.
	embedded: Option<MiniRedis>,
	shutdown: broadcast::Sender<()>,
	tasks:    Vec<tokio::task::JoinHandle<()>>,
}

impl SchedulerHandle {
	/// The URL of the embedded dev Redis, when one was spawned.
	pub fn embedded_redis_url(&self) -> Option<String> {
		self.embedded.as_ref().map(MiniRedis::url)
	}

	/// Stop the listener and background loops.
	pub fn shutdown(mut self) {
		let _ = self.shutdown.send(());
		for task in self.tasks.drain(..) {
			task.abort();
		}
	}
}

impl Drop for SchedulerHandle {
	fn drop(&mut self) {
		let _ = self.shutdown.send(());
		for task in &self.tasks {
			task.abort();
		}
	}
}

/// Blocking `vmon sched` entrypoint: serve until SIGINT/SIGTERM.
pub fn serve_scheduler(options: SchedulerOptions) -> Result<()> {
	init_logging();
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.map_err(|error| EngineError::engine(format!("tokio runtime: {error}")))?;
	runtime.block_on(async move {
		let handle = spawn_scheduler(options).await?;
		tracing::info!(addr = %handle.addr, "vmon sched listening");
		if let Some(url) = handle.embedded_redis_url() {
			tracing::info!(%url, "embedded dev redis (point workers' --orch-redis here)");
		}
		wait_for_shutdown_signal().await;
		handle.shutdown();
		Ok(())
	})
}

/// Bind and start a scheduler; returns once the listener is accepting.
pub async fn spawn_scheduler(options: SchedulerOptions) -> Result<SchedulerHandle> {
	validate_auth(&options)?;
	let embedded = match &options.redis_url {
		Some(_url) => None,
		None => Some(MiniRedis::spawn().await?),
	};
	let redis_url = options
		.redis_url
		.clone()
		.or_else(|| embedded.as_ref().map(MiniRedis::url))
		.expect("redis url or embedded instance");
	let redis = Redis::new(&redis_url)?;
	redis
		.ping()
		.await
		.map_err(|error| EngineError::engine(format!("redis {redis_url} is unreachable: {error}")))?;

	let table = Arc::new(WorkerTable::new(TableConfig::from_dead_after(options.dead_after)));
	let instance = format!("sched-{}", hex::encode(rand::random::<[u8; 6]>()));
	let core = SchedCore::new(
		Arc::clone(&table),
		redis.clone(),
		SchedGrpcOptions {
			worker_token:   options.worker_token.clone(),
			create_timeout: options.create_timeout,
			create_retries: options.create_retries,
		},
		instance.clone(),
	)?;

	let listener = TcpListener::bind(&options.listen)
		.await
		.map_err(|error| EngineError::engine(format!("bind {}: {error}", options.listen)))?;
	let addr = listener
		.local_addr()
		.map_err(|error| EngineError::engine(format!("listener addr: {error}")))?;

	let (shutdown, _) = broadcast::channel::<()>(4);
	let mut tasks = Vec::new();

	// 1. Table feed: bootstrap from keys, then follow the stream.
	bootstrap_table(&redis, &table).await;
	tasks.push(tokio::spawn(follow_workers(redis.clone(), Arc::clone(&table))));

	// 2. Leadership: controller janitor + optional autoscaler.
	let lease = LeaderLease::new(redis.clone(), instance.clone(), LEADER_TTL);
	let controller = Controller::new(redis.clone(), Arc::clone(&table));
	let autoscaler = options
		.autoscaler
		.clone()
		.map(|policy| Autoscaler::new(policy, redis.clone(), Arc::clone(&table)));
	tasks.push(tokio::spawn(lead(lease, controller, autoscaler)));
	tasks.push(tokio::spawn(record_dashboard(
		redis.clone(),
		Arc::clone(&core),
		shutdown.subscribe(),
	)));
	// 3. Serving.
	let router = router(SchedGrpc::new(Arc::clone(&core)), options.token.clone(), redis.clone());
	let mut serve_shutdown = shutdown.subscribe();
	tasks.push(tokio::spawn(async move {
		let outcome = axum::serve(listener, router)
			.with_graceful_shutdown(async move {
				let _ = serve_shutdown.recv().await;
			})
			.await;
		if let Err(error) = outcome {
			tracing::error!(%error, "scheduler listener failed");
		}
	}));

	Ok(SchedulerHandle { addr, core, embedded, shutdown, tasks })
}

/// Sample the fleet every 2s; each tick forwards the number of creates this
/// scheduler placed since the previous tick and the per-worker network
/// counter memory to the dashboard recorder.
async fn record_dashboard(
	redis: Redis,
	core: Arc<SchedCore>,
	mut shutdown: broadcast::Receiver<()>,
) {
	let mut ticker = tokio::time::interval(Duration::from_secs(2));
	let mut previous = 0_u64;
	let mut net = dashboard::NetRates::default();
	loop {
		tokio::select! {
			_ = ticker.tick() => {
				let total = core.created_total();
				dashboard::record(&redis, total.saturating_sub(previous), &mut net).await;
				previous = total;
			},
			_ = shutdown.recv() => return,
		}
	}
}

/// Reject token-less schedulers on non-loopback binds, mirroring
/// `vmon serve`'s TCP auth stance.
fn validate_auth(options: &SchedulerOptions) -> Result<()> {
	if options.token.is_some() {
		return Ok(());
	}
	let host = options
		.listen
		.rsplit_once(':')
		.map_or(options.listen.as_str(), |(host, _port)| host);
	let loopback = matches!(host, "127.0.0.1" | "::1" | "[::1]" | "localhost");
	if loopback {
		Ok(())
	} else {
		Err(EngineError::invalid(
			"refusing token-less scheduler on a non-loopback bind; pass --token",
		))
	}
}

/// Build the tonic service set. The same [`Routes`] shape backs both the
/// native h2c router and each `/grpc` WebSocket bridge upgrade.
fn grpc_routes(grpc: SchedGrpc) -> Routes {
	Routes::default()
		.add_service(
			pb::sandbox_service_server::SandboxServiceServer::new(grpc.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::snapshot_service_server::SnapshotServiceServer::new(grpc.clone())
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
		.add_service(
			pb::system_service_server::SystemServiceServer::new(grpc)
				.max_decoding_message_size(MAX_MESSAGE_SIZE)
				.max_encoding_message_size(MAX_MESSAGE_SIZE),
		)
}

/// gRPC-over-WebSocket bridge (`GET /grpc`), mirroring `vmon serve`.
///
/// Clients that cannot speak native h2c — browsers, and the JS SDK's mesh
/// driver, which only implements this bridge — reach the scheduler here.
/// [`require_bearer`] has already authenticated the upgrade, and the
/// scheduler has no tenant model, so the bridge runs as the local admin.
fn bridge_ws(ws: WebSocketUpgrade, grpc: SchedGrpc) -> Response {
	ws.max_message_size(MAX_MESSAGE_SIZE + 1024)
		.max_frame_size(MAX_MESSAGE_SIZE + 1024)
		.on_upgrade(move |socket| {
			crate::api::bridge::serve_bridge(grpc_routes(grpc), Principal::local_admin(), socket)
		})
		.into_response()
}

fn router(grpc: SchedGrpc, token: Option<String>, redis: Redis) -> Router {
	let grpc_router = grpc_routes(grpc.clone()).prepare().into_axum_router();
	let bridge_grpc = grpc;
	Router::new()
		.route("/healthz", get(healthz))
		.route(
			"/grpc",
			get(move |ws| {
				let grpc = bridge_grpc.clone();
				async move { bridge_ws(ws, grpc) }
			}),
		)
		.merge(dashboard::router(redis))
		.merge(grpc_router)
		.layer(middleware::from_fn_with_state(Arc::new(token), require_bearer))
}

async fn healthz() -> Json<serde_json::Value> {
	Json(serde_json::json!({"ok": true}))
}

/// Bearer guard for every non-public route. gRPC clients need a
/// trailers-only grpc-status response, not a bare HTTP 401.
///
/// Public GETs: `/healthz`, plus the browser-facing dashboard (`/` and
/// `/api/dashboard`) — read-only fleet telemetry; network exposure is the
/// deployment's concern (the AWS stack scopes the port to `allowedCidr`).
async fn require_bearer(
	axum::extract::State(token): axum::extract::State<Arc<Option<String>>>,
	request: Request<Body>,
	next: Next,
) -> Response {
	let public = request.method() == Method::GET
		&& matches!(request.uri().path(), "/healthz" | "/" | "/api/dashboard");
	let Some(expected) = token.as_ref() else {
		return next.run(request).await;
	};
	if public {
		return next.run(request).await;
	}
	let supplied = request
		.headers()
		.get(header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.strip_prefix("Bearer "))
		.map(ToOwned::to_owned)
		.or_else(|| bridge_query_token(&request));
	if supplied.as_deref() == Some(expected.as_str()) {
		return next.run(request).await;
	}
	let mut response = Response::new(Body::empty());
	let headers = response.headers_mut();
	headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/grpc"));
	headers.insert("grpc-status", header::HeaderValue::from_static("16"));
	headers.insert("grpc-message", header::HeaderValue::from_static("invalid bearer token"));
	headers.insert("vmon-code", header::HeaderValue::from_static("unauthorized"));
	headers.insert(header::WWW_AUTHENTICATE, header::HeaderValue::from_static("Bearer"));
	response
}

/// Token supplied as `?token=` on the `/grpc` upgrade.
///
/// Browsers cannot set an `Authorization` header on a WebSocket handshake, so
/// the bridge accepts the token in the query string — but only for a GET that
/// is actually an upgrade, so a bare URL can never authenticate an RPC.
fn bridge_query_token(request: &Request<Body>) -> Option<String> {
	if request.method() != Method::GET || request.uri().path() != "/grpc" {
		return None;
	}
	let upgrading = request
		.headers()
		.get(header::UPGRADE)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
	if !upgrading {
		return None;
	}
	crate::api::state::query_token(request.uri().query())
}

/// Seed the table from every live self-expiring worker key.
async fn bootstrap_table(redis: &Redis, table: &Arc<WorkerTable>) {
	let worker_keys = match redis.scan_prefix(keys::WORKER_PREFIX).await {
		Ok(worker_keys) => worker_keys,
		Err(error) => {
			tracing::warn!(%error, "orch: worker bootstrap scan failed");
			return;
		},
	};
	for key in worker_keys {
		match redis.get(&key).await {
			Ok(Some(raw)) => {
				if let Ok(heartbeat) = serde_json::from_slice(&raw) {
					table.upsert(heartbeat);
				}
			},
			Ok(None) => {}, // expired between SCAN and GET — self-healing
			Err(error) => tracing::warn!(%error, key, "orch: worker bootstrap read failed"),
		}
	}
	tracing::info!(workers = table.len(), "orch: worker table bootstrapped");
}

/// Follow the workers stream forever; a Redis error re-bootstraps from keys
/// so no state is missed across the gap.
async fn follow_workers(redis: Redis, table: Arc<WorkerTable>) {
	let mut follower = StreamFollower::new(&redis, keys::WORKERS_STREAM);
	loop {
		match follower.next_batch(FOLLOW_BLOCK_MS).await {
			Ok(payloads) => {
				for payload in payloads {
					match serde_json::from_slice(&payload) {
						Ok(heartbeat) => table.upsert(heartbeat),
						Err(error) => {
							tracing::debug!(%error, "orch: skipping undecodable heartbeat");
						},
					}
				}
			},
			Err(error) => {
				tracing::warn!(%error, "orch: stream follow failed; re-bootstrapping");
				tokio::time::sleep(Duration::from_secs(1)).await;
				bootstrap_table(&redis, &table).await;
			},
		}
	}
}

/// Leadership loop: the lease holder runs the controller janitor every tick
/// and the autoscaler at its own cadence.
async fn lead(lease: LeaderLease, mut controller: Controller, mut autoscaler: Option<Autoscaler>) {
	let mut last_scale = Instant::now();
	let mut was_leader = false;
	loop {
		tokio::time::sleep(LEADER_TICK).await;
		let is_leader = lease.tick().await;
		if is_leader != was_leader {
			tracing::info!(leader = is_leader, holder = lease.holder(), "orch: leadership changed");
			was_leader = is_leader;
		}
		if !is_leader {
			continue;
		}
		match controller.tick().await {
			Ok(0) => {},
			Ok(lost) => tracing::info!(lost, "orch: marked sandboxes lost"),
			Err(error) => tracing::warn!(%error, "orch: controller tick failed"),
		}
		if let Some(autoscaler) = autoscaler.as_mut()
			&& last_scale.elapsed() >= autoscaler.options().interval
		{
			last_scale = Instant::now();
			if let Err(error) = autoscaler.tick().await {
				tracing::warn!(%error, "orch: autoscaler tick failed");
			}
		}
	}
}

/// Install the tracing subscriber for `vmon sched` (same defaults as
/// `vmon serve`; `RUST_LOG` overrides; embedded/test callers keep theirs).
fn init_logging() {
	use tracing_subscriber::{EnvFilter, fmt};
	let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
	let _ = fmt().with_env_filter(filter).try_init();
}

async fn wait_for_shutdown_signal() {
	use tokio::signal::unix::{SignalKind, signal};
	let Ok(mut sigterm) = signal(SignalKind::terminate()) else {
		return std::future::pending().await;
	};
	tokio::select! {
		_ = tokio::signal::ctrl_c() => {},
		_ = sigterm.recv() => {},
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn tokenless_schedulers_are_loopback_only() {
		let loopback = SchedulerOptions::default();
		assert!(validate_auth(&loopback).is_ok());
		let exposed =
			SchedulerOptions { listen: "0.0.0.0:8100".to_owned(), ..SchedulerOptions::default() };
		assert!(validate_auth(&exposed).is_err(), "public bind without a token must be refused");
		let with_token = SchedulerOptions {
			listen: "0.0.0.0:8100".to_owned(),
			token: Some("secret".to_owned()),
			..SchedulerOptions::default()
		};
		assert!(validate_auth(&with_token).is_ok());
	}

	fn upgrade_request(method: Method, uri: &str, upgrading: bool) -> Request<Body> {
		let mut builder = Request::builder().method(method).uri(uri);
		if upgrading {
			builder = builder.header(header::UPGRADE, "websocket");
		}
		builder.body(Body::empty()).expect("request builds")
	}

	/// The `?token=` escape hatch exists only because browsers cannot set an
	/// `Authorization` header on a WebSocket handshake. It must therefore stay
	/// confined to a genuine `GET /grpc` upgrade, or a token pasted into a URL
	/// would authenticate ordinary RPCs (and leak through logs and referrers).
	#[test]
	fn bridge_query_tokens_are_limited_to_grpc_upgrades() {
		assert_eq!(
			bridge_query_token(&upgrade_request(Method::GET, "/grpc?token=a%20b", true)).as_deref(),
			Some("a b"),
			"percent-decoded token on a real upgrade"
		);
		assert_eq!(
			bridge_query_token(&upgrade_request(Method::GET, "/grpc?access_token=t", true)).as_deref(),
			Some("t")
		);
		assert_eq!(
			bridge_query_token(&upgrade_request(Method::GET, "/grpc?token=t", false)),
			None,
			"a non-upgrade GET must not authenticate from the query string"
		);
		assert_eq!(
			bridge_query_token(&upgrade_request(Method::POST, "/grpc?token=t", true)),
			None,
			"only GET upgrades carry a query token"
		);
		assert_eq!(
			bridge_query_token(&upgrade_request(
				Method::GET,
				"/vmon.v1.SystemService/Info?token=t",
				true
			)),
			None,
			"query tokens never reach the native h2c RPC paths"
		);
	}
}
