//! Hermetic end-to-end test of the orchestration layer: a real scheduler
//! (embedded `MiniRedis`, controller, autoscaler) driving two in-process stub
//! gRPC workers whose state is published by the real [`OrchWorker`].
//!
//! No hypervisor and no external Redis are involved; the stubs implement just
//! enough of `SandboxService` to prove placement, worker-token propagation,
//! busy-retry redistribution, idempotent creates, sid routing (unary, capture,
//! and bidi exec), the autoscaler scale-up hook, and dead-worker lost-marking.

use std::{
	net::SocketAddr,
	pin::Pin,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use dashmap::DashMap;
use serde_json::{Value, json};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{
	Code, Request, Response, Status, Streaming,
	metadata::MetadataValue,
	service::Routes,
	transport::{Channel, Endpoint},
};
use vmon_proto::v1 as pb;

use super::{
	Resources, STATE_LOST, SandboxRoute,
	autoscaler::AutoscalerOptions,
	keys,
	redis::Redis,
	server::{SchedulerOptions, spawn_scheduler},
	worker::{OrchWorker, OrchWorkerOptions},
};

const SCHED_TOKEN: &str = "sched-secret";
const WORKER_TOKEN: &str = "worker-secret";

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;
type SchedClient = pb::sandbox_service_client::SandboxServiceClient<Channel>;

// ---------------------------------------------------------------------------
// Stub worker: a minimal in-memory SandboxService.
// ---------------------------------------------------------------------------

struct StubState {
	wid:       String,
	capacity:  usize,
	sandboxes: DashMap<String, Value>,
	next_id:   AtomicU64,
}

#[derive(Clone)]
struct StubWorker {
	state: Arc<StubState>,
}

impl StubWorker {
	fn new(wid: &str, capacity: usize) -> Self {
		Self {
			state: Arc::new(StubState {
				wid: wid.to_owned(),
				capacity,
				sandboxes: DashMap::new(),
				next_id: AtomicU64::new(0),
			}),
		}
	}

	fn count(&self) -> usize {
		self.state.sandboxes.len()
	}

	/// Deterministic pick of one hosted sandbox id.
	fn first_sid(&self) -> String {
		let mut sids: Vec<String> = self
			.state
			.sandboxes
			.iter()
			.map(|entry| entry.key().clone())
			.collect();
		sids.sort_unstable();
		sids
			.first()
			.expect("stub hosts at least one sandbox")
			.clone()
	}

	fn views(&self) -> Vec<Value> {
		self
			.state
			.sandboxes
			.iter()
			.map(|entry| entry.value().clone())
			.collect()
	}

	fn view_of(&self, id: &str) -> Result<Value, Status> {
		self
			.state
			.sandboxes
			.get(id)
			.map(|entry| entry.value().clone())
			.ok_or_else(|| Status::not_found(format!("sandbox {id} not found")))
	}
}

fn stub_unimplemented<T>() -> Result<Response<T>, Status> {
	Err(Status::unimplemented("stub"))
}

fn json_response(view: &Value) -> Response<pb::JsonView> {
	Response::new(pb::JsonView { json: view.to_string() })
}

#[tonic::async_trait]
impl pb::sandbox_service_server::SandboxService for StubWorker {
	type AttachStream = BoxStream<pb::ExecOutput>;
	type BatchCreateStream = BoxStream<pb::BatchCreateResponse>;
	type ExecStream = BoxStream<pb::ExecOutput>;
	type LogsStream = BoxStream<pb::LogChunk>;
	type PtyAttachStream = BoxStream<pb::ExecOutput>;
	type PtyOpenStream = BoxStream<pb::ExecOutput>;
	type ShellStream = BoxStream<pb::ExecOutput>;
	type WatchStream = BoxStream<pb::JsonView>;

	async fn create(
		&self,
		request: Request<pb::CreateSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let (metadata, _, message) = request.into_parts();
		// The scheduler must attach its configured worker token.
		let bearer = metadata
			.get("authorization")
			.and_then(|value| value.to_str().ok());
		if bearer != Some(&format!("Bearer {WORKER_TOKEN}")) {
			return Err(Status::unauthenticated("missing or wrong worker bearer token"));
		}
		// Mirror of the real worker admission gate: full means ABORTED/busy.
		if self.state.sandboxes.len() >= self.state.capacity {
			let mut status = Status::aborted(format!("worker {} at capacity", self.state.wid));
			status
				.metadata_mut()
				.insert("vmon-code", MetadataValue::from_static("busy"));
			return Err(status);
		}
		let spec: Value = serde_json::from_str(&message.spec_json)
			.map_err(|_| Status::invalid_argument("invalid create spec JSON"))?;
		let cpus = spec.get("cpus").and_then(Value::as_u64).unwrap_or(1);
		let memory = spec.get("memory").and_then(Value::as_u64).unwrap_or(512);
		let n = self.state.next_id.fetch_add(1, Ordering::SeqCst) + 1;
		let sid = format!("sb-{}-{n}", self.state.wid);
		// `no_wait` admits: identity assigned, boot pending → `starting`.
		let status = if message.no_wait {
			"starting"
		} else {
			"running"
		};
		let view = json!({
			"id": sid,
			"name": sid,
			"status": status,
			"cpus": cpus,
			"memory": memory,
		});
		self.state.sandboxes.insert(sid, view.clone());
		Ok(json_response(&view))
	}

	async fn list(
		&self,
		_request: Request<pb::ListSandboxesRequest>,
	) -> Result<Response<pb::ListSandboxesResponse>, Status> {
		let sandboxes_json = self.views().iter().map(Value::to_string).collect();
		Ok(Response::new(pb::ListSandboxesResponse { sandboxes_json }))
	}

	async fn get(&self, request: Request<pb::SandboxRef>) -> Result<Response<pb::JsonView>, Status> {
		Ok(json_response(&self.view_of(&request.into_inner().id)?))
	}

	async fn stop(
		&self,
		request: Request<pb::StopSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		let id = request.into_inner().id;
		let mut entry = self
			.state
			.sandboxes
			.get_mut(&id)
			.ok_or_else(|| Status::not_found(format!("sandbox {id} not found")))?;
		entry.value_mut()["status"] = Value::String("stopped".to_owned());
		Ok(json_response(entry.value()))
	}

	async fn remove(
		&self,
		request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		let id = request.into_inner().id;
		let (_sid, view) = self
			.state
			.sandboxes
			.remove(&id)
			.ok_or_else(|| Status::not_found(format!("sandbox {id} not found")))?;
		Ok(json_response(&view))
	}

	async fn exec_capture(
		&self,
		request: Request<pb::ExecCaptureRequest>,
	) -> Result<Response<pb::ExecCaptureResponse>, Status> {
		self.view_of(&request.into_inner().id)?;
		Ok(Response::new(pb::ExecCaptureResponse {
			code:   0,
			signal: None,
			stdout: format!("echo-from-{}", self.state.wid).into_bytes(),
			stderr: Vec::new(),
		}))
	}

	async fn exec(
		&self,
		request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ExecStream>, Status> {
		let mut inbound = request.into_inner();
		let first = inbound
			.message()
			.await?
			.ok_or_else(|| Status::invalid_argument("empty exec stream"))?;
		let Some(pb::exec_input::Input::Start(start)) = first.input else {
			return Err(Status::invalid_argument("first exec frame must be start"));
		};
		let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::ExecOutput, Status>>(16);
		tokio::spawn(async move {
			let ready = pb::ExecOutput {
				output: Some(pb::exec_output::Output::Ready(pb::Ready {
					sandbox_id: start.sandbox_id,
				})),
			};
			if tx.send(Ok(ready)).await.is_err() {
				return;
			}
			while let Ok(Some(frame)) = inbound.message().await {
				match frame.input {
					Some(pb::exec_input::Input::Stdin(data)) => {
						let chunk = pb::ExecOutput {
							output: Some(pb::exec_output::Output::Chunk(pb::Output {
								stream: pb::Stream::Stdout as i32,
								data,
							})),
						};
						if tx.send(Ok(chunk)).await.is_err() {
							return;
						}
					},
					Some(pb::exec_input::Input::Eof(_)) => {
						let exit = pb::ExecOutput {
							output: Some(pb::exec_output::Output::Exit(pb::Exit {
								code:   0,
								signal: None,
							})),
						};
						let _ = tx.send(Ok(exit)).await;
						return;
					},
					_ => {},
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::ExecStream))
	}

	async fn terminate(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn pause(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn resume(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn suspend(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn extend(
		&self,
		_request: Request<pb::ExtendSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn set_idle_timeout(
		&self,
		_request: Request<pb::SetIdleTimeoutRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn metrics(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn logs(
		&self,
		_request: Request<pb::LogsRequest>,
	) -> Result<Response<Self::LogsStream>, Status> {
		stub_unimplemented()
	}

	async fn shell(
		&self,
		_request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::ShellStream>, Status> {
		stub_unimplemented()
	}

	async fn attach(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<Self::AttachStream>, Status> {
		stub_unimplemented()
	}

	async fn file_read(
		&self,
		_request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::FileContent>, Status> {
		stub_unimplemented()
	}

	async fn file_write(
		&self,
		_request: Request<pb::FileWriteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		stub_unimplemented()
	}

	async fn file_delete(
		&self,
		_request: Request<pb::FileDeleteRequest>,
	) -> Result<Response<pb::Ok>, Status> {
		stub_unimplemented()
	}

	async fn file_list(
		&self,
		_request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn file_stat(
		&self,
		_request: Request<pb::FilePathRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn network_get(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn network_set(
		&self,
		_request: Request<pb::NetworkSetRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn tunnels(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn migrate(
		&self,
		_request: Request<pb::MigrateRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn snapshot(
		&self,
		_request: Request<pb::SnapshotRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn snapshot_fs(
		&self,
		_request: Request<pb::SnapshotFsRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn history(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::RecoveryPointList>, Status> {
		stub_unimplemented()
	}

	async fn rollback(
		&self,
		_request: Request<pb::RollbackSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn batch_create(
		&self,
		_request: Request<Streaming<pb::BatchCreateRequest>>,
	) -> Result<Response<Self::BatchCreateStream>, Status> {
		// The scheduler fans batch items out as unary creates; the worker-side
		// batch surface is exercised by the real worker, not this stub.
		stub_unimplemented()
	}

	async fn resize(
		&self,
		_request: Request<pb::ResizeSandboxRequest>,
	) -> Result<Response<pb::JsonView>, Status> {
		stub_unimplemented()
	}

	async fn pty_open(
		&self,
		_request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyOpenStream>, Status> {
		stub_unimplemented()
	}

	async fn pty_attach(
		&self,
		_request: Request<Streaming<pb::ExecInput>>,
	) -> Result<Response<Self::PtyAttachStream>, Status> {
		stub_unimplemented()
	}

	async fn pty_list(
		&self,
		_request: Request<pb::SandboxRef>,
	) -> Result<Response<pb::PtySessionList>, Status> {
		stub_unimplemented()
	}

	async fn pty_close(
		&self,
		_request: Request<pb::PtyCloseRequest>,
	) -> Result<Response<pb::PtySessionCloseResponse>, Status> {
		stub_unimplemented()
	}

	async fn pty_exec(
		&self,
		_request: Request<pb::PtyExecRequest>,
	) -> Result<Response<pb::PtyExecResponse>, Status> {
		stub_unimplemented()
	}

	async fn watch(
		&self,
		request: Request<pb::WatchSandboxRequest>,
	) -> Result<Response<Self::WatchStream>, Status> {
		let message = request.into_inner();
		let view = self.view_of(&message.id)?;
		let (tx, rx) = tokio::sync::mpsc::channel::<Result<pb::JsonView, Status>>(4);
		let state = Arc::clone(&self.state);
		tokio::spawn(async move {
			// First frame: the current view, per the Watch contract.
			if tx
				.send(Ok(pb::JsonView { json: view.to_string() }))
				.await
				.is_err()
			{
				return;
			}
			if message.until_ready && view.get("status").and_then(Value::as_str) != Some("running") {
				// A stub sandbox "boots" instantly: flip to running, emit it, end.
				if let Some(mut entry) = state.sandboxes.get_mut(&message.id) {
					entry.value_mut()["status"] = Value::String("running".to_owned());
					let _ = tx
						.send(Ok(pb::JsonView { json: entry.value().to_string() }))
						.await;
				}
			}
		});
		Ok(Response::new(Box::pin(ReceiverStream::new(rx)) as Self::WatchStream))
	}
}

// ---------------------------------------------------------------------------
// Harness: serve a stub, publish its state through the real OrchWorker.
// ---------------------------------------------------------------------------

struct StubHandle {
	stub:      StubWorker,
	server:    tokio::task::JoinHandle<()>,
	publisher: tokio::task::JoinHandle<()>,
	routes:    tokio::task::JoinHandle<()>,
}

/// Serve one stub on an ephemeral loopback port and pair it with the real
/// worker-side publisher (heartbeat 200ms, key TTL 1s, snapshot every 150ms).
async fn start_stub_worker(redis_url: &str, wid: &str, capacity: usize) -> StubHandle {
	let stub = StubWorker::new(wid, capacity);
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
		.await
		.expect("bind stub listener");
	let addr = listener.local_addr().expect("stub listener addr");
	let router = Routes::default()
		.add_service(pb::sandbox_service_server::SandboxServiceServer::new(stub.clone()))
		.prepare()
		.into_axum_router();
	let server = tokio::spawn(async move {
		let _ = axum::serve(listener, router).await;
	});

	let worker = OrchWorker::new(OrchWorkerOptions {
		redis_url:          redis_url.to_owned(),
		wid:                wid.to_owned(),
		url:                format!("http://{addr}"),
		arch:               "testarch".to_owned(),
		backend:            "test".to_owned(),
		caps:               Resources { vcpus: 8, mem_mib: 4096 },
		heartbeat:          Duration::from_millis(200),
		dead_after:         Duration::from_secs(1),
		max_sandboxes:      u64::try_from(capacity).expect("capacity fits u64"),
		memory_reserve_mib: 0,
	})
	.expect("orch worker");
	let routes = worker.spawn_route_writer();
	let state = Arc::clone(&stub.state);
	let publisher = tokio::spawn(async move {
		loop {
			let views: Vec<Value> = state
				.sandboxes
				.iter()
				.map(|entry| entry.value().clone())
				.collect();
			if let Err(error) = worker.publish_once(&views).await {
				eprintln!("stub {} publish failed: {error}", worker.wid());
			}
			tokio::time::sleep(Duration::from_millis(150)).await;
		}
	});
	StubHandle { stub, server, publisher, routes }
}

// ---------------------------------------------------------------------------
// Test-side helpers.
// ---------------------------------------------------------------------------

/// Poll `ready` every 100ms until it holds; panic with `label` past `deadline`.
async fn wait_until(label: &str, deadline: Duration, mut ready: impl AsyncFnMut() -> bool) {
	let end = tokio::time::Instant::now() + deadline;
	loop {
		if ready().await {
			return;
		}
		assert!(tokio::time::Instant::now() < end, "timed out waiting for: {label}");
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
}

async fn sched_channel(addr: SocketAddr) -> Channel {
	Endpoint::from_shared(format!("http://{addr}"))
		.expect("scheduler endpoint")
		.connect()
		.await
		.expect("scheduler channel")
}

/// Wrap a message with the scheduler's inbound bearer token.
fn authed<T>(message: T) -> Request<T> {
	let mut request = Request::new(message);
	request
		.metadata_mut()
		.insert("authorization", MetadataValue::from_static("Bearer sched-secret"));
	request
}

async fn create_sandbox(client: &mut SchedClient, spec: &str) -> Result<Value, Status> {
	let response = client
		.create(authed(pb::CreateSandboxRequest { spec_json: spec.to_owned(), no_wait: false }))
		.await?;
	Ok(serde_json::from_str(&response.into_inner().json).expect("create response view JSON"))
}

/// Token-less HTTP/1.1 GET against the scheduler; returns the raw response
/// (status line, headers, body) so callers can assert on any part.
async fn http_get(addr: SocketAddr, path: &str) -> String {
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	let mut stream = tokio::net::TcpStream::connect(addr)
		.await
		.expect("scheduler TCP connect");
	let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
	stream
		.write_all(request.as_bytes())
		.await
		.expect("request write");
	let mut response = String::new();
	stream
		.read_to_string(&mut response)
		.await
		.expect("response read");
	response
}

fn scheduler_options(marker: &std::path::Path) -> SchedulerOptions {
	SchedulerOptions {
		listen:         "127.0.0.1:0".to_owned(),
		redis_url:      None,
		token:          Some(SCHED_TOKEN.to_owned()),
		worker_token:   Some(WORKER_TOKEN.to_owned()),
		dead_after:     Duration::from_secs(1),
		create_timeout: Duration::from_secs(5),
		create_retries: 3,
		autoscaler:     Some(AutoscalerOptions {
			// Keep the lower clamp at the live fleet size so an idle test host
			// cannot trigger scale-down and drain both stub workers. Worker
			// heartbeats report measured host memory, not configured guest RAM,
			// so the sandboxes below do not manufacture scale-up pressure.
			min: 2,
			max: 4,
			target_util: 0.1,
			interval: Duration::from_secs(1),
			cooldown_up: Duration::from_millis(10),
			// `printf`, not `echo -n`: macOS /bin/sh prints `-n` literally.
			scale_up_cmd: Some(format!("printf %s \"$VMON_SCALE_DESIRED\" > {}", marker.display())),
			..AutoscalerOptions::default()
		}),
	}
}

// ---------------------------------------------------------------------------
// The scenario.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orchestration_end_to_end() {
	let scratch = tempfile::tempdir().expect("tempdir");
	let marker = scratch.path().join("scale-up");

	// 1. Real scheduler: embedded MiniRedis, controller, autoscaler hook.
	let handle = spawn_scheduler(scheduler_options(&marker))
		.await
		.expect("spawn scheduler");
	let redis_url = handle.embedded_redis_url().expect("embedded redis url");
	let redis = Redis::new(&redis_url).expect("redis handle");

	// 2. Two stub workers (capacity 2 each) + real publishers.
	let wa = start_stub_worker(&redis_url, "wa", 2).await;
	let wb = start_stub_worker(&redis_url, "wb", 2).await;

	// 3. Heartbeats reach the scheduler's in-memory worker table.
	wait_until("two live workers in the table", Duration::from_secs(10), async || {
		handle.core.table().live().len() == 2
	})
	.await;

	// 4. Bearer guard: token-less calls are rejected, tokened ones pass.
	let channel = sched_channel(handle.addr).await;
	let mut bare = SchedClient::new(channel.clone());
	let unauthed = bare
		.get(Request::new(pb::SandboxRef { id: "whatever".to_owned() }))
		.await
		.expect_err("token-less scheduler call must fail");
	assert_eq!(
		unauthed.code(),
		Code::Unauthenticated,
		"scheduler rejects missing bearer: {unauthed:?}"
	);
	// Dashboard stays browser-reachable without a bearer, unlike the API.
	let index = http_get(handle.addr, "/").await;
	assert!(
		index.starts_with("HTTP/1.1 200") && index.contains("<title>Vibemon"),
		"token-less GET / serves the dashboard: {index}"
	);
	let data = http_get(handle.addr, "/api/dashboard").await;
	assert!(
		data.starts_with("HTTP/1.1 200") && data.contains("\"live\""),
		"token-less GET /api/dashboard serves fleet telemetry: {data}"
	);
	let mut client = SchedClient::new(channel.clone());

	// 5. Two creates land on live workers and come back annotated.
	let spec = r#"{"cpus":1,"memory":512}"#;
	let first = create_sandbox(&mut client, spec).await.expect("create #1");
	let second = create_sandbox(&mut client, spec).await.expect("create #2");
	for (label, view) in [("create #1", &first), ("create #2", &second)] {
		let node = view.get("node").and_then(Value::as_str).unwrap_or_default();
		assert!(node == "wa" || node == "wb", "{label}: view names its worker, got {view}");
		let endpoint = view
			.get("endpoint")
			.and_then(Value::as_str)
			.unwrap_or_default();
		assert!(!endpoint.is_empty(), "{label}: view carries the worker endpoint");
	}

	// 6. Idempotency: two concurrent creates with one key → one sandbox.
	let before = wa.stub.count() + wb.stub.count();
	let idem_spec = r#"{"cpus":1,"memory":512,"idempotency_key":"e2e-idem"}"#;
	let mut left = SchedClient::new(channel.clone());
	let mut right = SchedClient::new(channel.clone());
	let (one, two) =
		tokio::join!(create_sandbox(&mut left, idem_spec), create_sandbox(&mut right, idem_spec));
	let one = one.expect("idempotent create (left)");
	let two = two.expect("idempotent create (right)");
	let name = one.get("name").and_then(Value::as_str).unwrap_or_default();
	assert!(!name.is_empty(), "idempotent create returns a named sandbox: {one}");
	assert_eq!(
		one.get("name"),
		two.get("name"),
		"idempotent creates converge on one sandbox: {one} vs {two}"
	);
	assert_eq!(
		wa.stub.count() + wb.stub.count(),
		before + 1,
		"idempotent pair created exactly one sandbox"
	);

	// 7. Fill the cluster: 4 accepted creates can only mean 2 per stub
	// (worker-side capacity gates at 2, so acceptance proves busy-retry
	// redistribution); the 5th must fail busy.
	create_sandbox(&mut client, spec)
		.await
		.expect("create #4 (fills the cluster)");
	assert_eq!(wa.stub.count(), 2, "stub A holds exactly its capacity");
	assert_eq!(wb.stub.count(), 2, "stub B holds exactly its capacity");
	let overflow = create_sandbox(&mut client, spec)
		.await
		.expect_err("full cluster must reject the 5th create");
	assert_eq!(overflow.code(), Code::Aborted, "full cluster rejects with ABORTED: {overflow:?}");
	assert_eq!(
		overflow
			.metadata()
			.get("vmon-code")
			.and_then(|value| value.to_str().ok()),
		Some("busy"),
		"rejection carries vmon-code busy: {overflow:?}"
	);

	// 8. Routing: sandbox-scoped RPCs reach the owning worker.
	let b_sid = wb.stub.first_sid();
	let capture = client
		.exec_capture(authed(pb::ExecCaptureRequest {
			id:   b_sid.clone(),
			exec: Some(pb::ExecStart { cmd: vec!["echo".to_owned()], ..Default::default() }),
		}))
		.await
		.expect("exec_capture routes through the scheduler")
		.into_inner();
	assert_eq!(capture.code, 0, "exec_capture exit code");
	assert_eq!(capture.stdout, b"echo-from-wb", "exec_capture reached stub B");
	let viewed: Value = serde_json::from_str(
		&client
			.get(authed(pb::SandboxRef { id: b_sid.clone() }))
			.await
			.expect("get through the scheduler")
			.into_inner()
			.json,
	)
	.expect("get view JSON");
	assert_eq!(
		viewed.get("node").and_then(Value::as_str),
		Some("wb"),
		"get annotates the owning worker: {viewed}"
	);

	// 9. Bidi exec through the scheduler: Ready → echo → Exit.
	let a_sid = wa.stub.first_sid();
	let (tx, rx) = tokio::sync::mpsc::channel::<pb::ExecInput>(8);
	tx.send(pb::ExecInput {
		input: Some(pb::exec_input::Input::Start(pb::ExecStart {
			cmd: vec!["cat".to_owned()],
			sandbox_id: a_sid.clone(),
			..Default::default()
		})),
	})
	.await
	.expect("queue exec start");
	let mut outbound = client
		.exec(authed(ReceiverStream::new(rx)))
		.await
		.expect("exec stream opens through the scheduler")
		.into_inner();
	let ready = outbound
		.message()
		.await
		.expect("exec ready frame")
		.expect("exec stream stays open for ready");
	assert!(
		matches!(
			ready.output,
			Some(pb::exec_output::Output::Ready(ref r)) if r.sandbox_id == a_sid
		),
		"first exec frame is Ready for {a_sid}: {ready:?}"
	);
	tx.send(pb::ExecInput { input: Some(pb::exec_input::Input::Stdin(b"hi".to_vec())) })
		.await
		.expect("queue stdin");
	let echoed = outbound
		.message()
		.await
		.expect("exec chunk frame")
		.expect("exec stream stays open for the echo");
	match echoed.output {
		Some(pb::exec_output::Output::Chunk(chunk)) => {
			assert_eq!(chunk.data, b"hi", "exec echoes stdin");
			assert_eq!(chunk.stream, pb::Stream::Stdout as i32, "echo rides stdout");
		},
		other => panic!("expected a stdout chunk, got {other:?}"),
	}
	tx.send(pb::ExecInput { input: Some(pb::exec_input::Input::Eof(pb::Eof {})) })
		.await
		.expect("queue eof");
	let exit = outbound
		.message()
		.await
		.expect("exec exit frame")
		.expect("exec stream stays open for the exit");
	assert!(
		matches!(exit.output, Some(pb::exec_output::Output::Exit(pb::Exit { code: 0, .. }))),
		"exec ends with Exit code 0: {exit:?}"
	);
	drop(tx);

	// 10. Autoscaler: configured guest RAM is not treated as resident host
	// memory. The preceding multi-second scenario spans several autoscaler
	// ticks; low measured host use must not invoke the scale-up hook.
	assert!(!marker.exists(), "idle CoW guest allocations must not manufacture autoscaler pressure");

	// 11. Worker death: kill stub A and its publisher; the leader's
	// controller must reap it and mark its sandboxes lost.
	wa.server.abort();
	wa.publisher.abort();
	wa.routes.abort();
	let lost_key = keys::sandbox(&a_sid);
	wait_until("controller marks A's sandbox lost", Duration::from_secs(25), async || {
		matches!(
			redis.get(&lost_key).await,
			Ok(Some(raw)) if serde_json::from_slice::<SandboxRoute>(&raw)
				.is_ok_and(|route| route.state == STATE_LOST)
		)
	})
	.await;
	// A second, stateless scheduler on the same Redis resolves through the
	// route (sched #1 still holds a warm in-memory cache entry pinning the
	// sid to A's dead endpoint, which surfaces as UNAVAILABLE instead).
	let mut observer_options = scheduler_options(&marker);
	observer_options.redis_url = Some(redis_url.clone());
	observer_options.autoscaler = None;
	let observer = spawn_scheduler(observer_options)
		.await
		.expect("spawn observer scheduler");
	let mut observer_client = SchedClient::new(sched_channel(observer.addr).await);
	let lost = observer_client
		.get(authed(pb::SandboxRef { id: a_sid.clone() }))
		.await
		.expect_err("lost sandbox must not resolve");
	assert_eq!(lost.code(), Code::NotFound, "lost sandbox is NOT_FOUND: {lost:?}");
	assert!(lost.message().contains("lost"), "status message names the loss: {}", lost.message());
	let listed = client
		.list(authed(pb::ListSandboxesRequest { tags: Vec::new() }))
		.await
		.expect("list after worker death")
		.into_inner()
		.sandboxes_json;
	assert_eq!(listed.len(), 2, "list returns only B's sandboxes: {listed:?}");
	for json in &listed {
		let view: Value = serde_json::from_str(json).expect("listed view JSON");
		assert_eq!(
			view.get("node").and_then(Value::as_str),
			Some("wb"),
			"surviving sandbox is annotated with B: {view}"
		);
	}

	// 12. Shutdown.
	observer.shutdown();
	handle.shutdown();
	wb.server.abort();
	wb.publisher.abort();
	wb.routes.abort();
}

/// Admission/readiness split through the scheduler: a `no_wait` create comes
/// back `starting` yet fully annotated, `Watch` routes to the owner and relays
/// its stream until ready, and `BatchCreate` streams one seq-tagged result per
/// item. Hermetic: embedded `MiniRedis`, one stub worker, no hypervisor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_wait_watch_and_batch_create() {
	let handle = spawn_scheduler(SchedulerOptions {
		listen:         "127.0.0.1:0".to_owned(),
		redis_url:      None,
		token:          Some(SCHED_TOKEN.to_owned()),
		worker_token:   Some(WORKER_TOKEN.to_owned()),
		dead_after:     Duration::from_secs(1),
		create_timeout: Duration::from_secs(5),
		create_retries: 3,
		autoscaler:     None,
	})
	.await
	.expect("spawn scheduler");
	let redis_url = handle.embedded_redis_url().expect("embedded redis url");
	let worker = start_stub_worker(&redis_url, "wc", 16).await;
	wait_until("stub worker in the table", Duration::from_secs(10), async || {
		handle.core.table().live().len() == 1
	})
	.await;
	let mut client = SchedClient::new(sched_channel(handle.addr).await);
	let spec = r#"{"cpus":1,"memory":512}"#;

	// 1. `no_wait` passthrough: the stub admits with `starting` only when the
	// forwarded request carries the flag, and the scheduler annotates the
	// admitted view exactly like a ready one.
	let admitted: Value = serde_json::from_str(
		&client
			.create(authed(pb::CreateSandboxRequest { spec_json: spec.to_owned(), no_wait: true }))
			.await
			.expect("no_wait create")
			.into_inner()
			.json,
	)
	.expect("admitted view JSON");
	assert_eq!(
		admitted.get("status").and_then(Value::as_str),
		Some("starting"),
		"no_wait create returns the admitted view: {admitted}"
	);
	assert_eq!(
		admitted.get("node").and_then(Value::as_str),
		Some("wc"),
		"admitted view names its worker: {admitted}"
	);
	let endpoint = admitted
		.get("endpoint")
		.and_then(Value::as_str)
		.unwrap_or_default();
	assert!(!endpoint.is_empty(), "admitted view carries the worker endpoint: {admitted}");

	// 2. Watch routes by sid and relays the owner's stream: current
	// (starting) frame, then the ready frame, then end (until_ready).
	let sid = admitted
		.get("name")
		.and_then(Value::as_str)
		.expect("admitted view has a name")
		.to_owned();
	let mut frames = client
		.watch(authed(pb::WatchSandboxRequest { id: sid.clone(), until_ready: true }))
		.await
		.expect("watch opens through the scheduler")
		.into_inner();
	let first: Value = serde_json::from_str(
		&frames
			.message()
			.await
			.expect("watch first frame")
			.expect("watch stream opens with the current view")
			.json,
	)
	.expect("watch frame JSON");
	assert_eq!(first.get("name").and_then(Value::as_str), Some(sid.as_str()));
	assert_eq!(first["status"], "starting", "first frame is the admitted view: {first}");
	let ready: Value = serde_json::from_str(
		&frames
			.message()
			.await
			.expect("watch ready frame")
			.expect("watch stream stays open until ready")
			.json,
	)
	.expect("watch ready JSON");
	assert_eq!(ready["status"], "running", "readiness flows through the relay: {ready}");
	assert!(
		frames.message().await.expect("watch end").is_none(),
		"until_ready stream ends after running"
	);

	// 3. BatchCreate: N items in → N seq-tagged annotated results out.
	let (tx, rx) = tokio::sync::mpsc::channel::<pb::BatchCreateRequest>(8);
	for seq in 1..=5u64 {
		tx.send(pb::BatchCreateRequest {
			seq,
			create: Some(pb::CreateSandboxRequest { spec_json: spec.to_owned(), no_wait: false }),
		})
		.await
		.expect("queue batch item");
	}
	drop(tx);
	let mut responses = client
		.batch_create(authed(ReceiverStream::new(rx)))
		.await
		.expect("batch create opens through the scheduler")
		.into_inner();
	let mut seqs = Vec::new();
	while let Some(response) = responses.message().await.expect("batch response frame") {
		match response.outcome {
			Some(pb::batch_create_response::Outcome::Json(json)) => {
				let view: Value = serde_json::from_str(&json).expect("batch view JSON");
				assert_eq!(
					view.get("node").and_then(Value::as_str),
					Some("wc"),
					"batch view is annotated: {view}"
				);
			},
			other => panic!("batch item {} failed: {other:?}", response.seq),
		}
		seqs.push(response.seq);
	}
	seqs.sort_unstable();
	assert_eq!(seqs, vec![1, 2, 3, 4, 5], "every batch item reports exactly once");

	handle.shutdown();
	worker.server.abort();
	worker.publisher.abort();
}
