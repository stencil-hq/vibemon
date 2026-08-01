//! Sandbox lifecycle streaming for the v1 gRPC surface.
//!
//! Backs `SandboxService::Watch` (and the `no_wait` create identity) with the
//! engine lifecycle bus: a blocking flume receiver is bridged onto a tokio
//! mpsc channel from a dedicated thread, mirroring `SystemService::events`.

use std::thread;

use serde_json::Value;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use vmon_proto::v1 as pb;

use super::grpc::{BoxStream, json_view};

/// Generate a sandbox id with the engine's `prepare_create` shape
/// (`sb-` + 24 hex chars = 12 random bytes). Assigning the name in the API
/// layer lets a `no_wait` create return its identity before the boot runs;
/// the facade keeps the caller-supplied name verbatim.
pub(super) fn generate_sid() -> String {
	format!("sb-{:016x}{:08x}", rand::random::<u64>(), rand::random::<u32>())
}

/// Whether one lifecycle event document belongs to the watched sandbox.
fn event_matches(event: &Value, id: &str) -> bool {
	event.get("id").and_then(Value::as_str) == Some(id)
		|| event.get("name").and_then(Value::as_str) == Some(id)
}

/// Whether a frame ends the watch. Failure and hard-terminal states always
/// end it; with `until_ready` the first `running`/`ready` frame is terminal
/// success.
fn watch_ends_after(status: &str, kind: Option<&str>, until_ready: bool) -> bool {
	matches!(status, "stopped" | "terminated" | "failed")
		|| kind == Some("failed")
		|| (until_ready && (status == "running" || kind == Some("ready")))
}

/// Bridge one sandbox's lifecycle events onto a tonic stream.
///
/// `events` must be subscribed BEFORE the `initial` view was read so no
/// transition is lost between the snapshot and the first bus event. The
/// receiver is blocking (flume), so it is drained from a dedicated thread —
/// never polled on the async runtime.
pub(super) fn watch_stream(
	id: String,
	until_ready: bool,
	initial: Option<Value>,
	events: flume::Receiver<Value>,
) -> BoxStream<pb::JsonView> {
	let (tx, out) = mpsc::channel::<Result<pb::JsonView, Status>>(32);
	thread::spawn(move || {
		if let Some(view) = &initial {
			let status = view
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or_default();
			let done = watch_ends_after(status, None, until_ready);
			if tx.blocking_send(Ok(json_view(view))).is_err() || done {
				return;
			}
		}
		while let Ok(event) = events.recv() {
			if !event_matches(&event, &id) {
				continue;
			}
			let status = event
				.get("status")
				.and_then(Value::as_str)
				.unwrap_or_default();
			let kind = event.get("type").and_then(Value::as_str);
			let done = watch_ends_after(status, kind, until_ready);
			if tx.blocking_send(Ok(json_view(&event))).is_err() || done {
				return;
			}
		}
	});
	Box::pin(ReceiverStream::new(out))
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		sync::{Arc, Mutex},
		time::Duration,
	};

	use serde_json::{Value, json};
	use tokio_stream::StreamExt as _;
	use tonic::{Code, Request, metadata::MetadataValue, transport::Endpoint};
	use vmon_proto::v1 as pb;

	use super::{
		super::{
			grpc::{self, GrpcApi},
			state::{ApiState, Transport},
		},
		*,
	};
	use crate::{
		EngineError, Result,
		config::ServeConfig,
		engine::{EngineApi, ExecCapture, ExecRequest, ExecStream, RecoveryPoint, ShellSession},
		function::FunctionDomain,
		home::Home,
		models::{ForkBody, NetworkBody, PoolPutBody, RestoreBody, SandboxCreate},
		security::{
			Principal,
			credentials::{Credential, CredentialMetadata},
		},
	};

	/// Minimal scripted engine: create/get respond from slots, creates are
	/// reported on a channel, and the lifecycle bus is test-driven.
	struct StubEngine {
		create_response: Mutex<Result<Value>>,
		get_response:    Mutex<Result<Value>>,
		create_calls:    (flume::Sender<SandboxCreate>, flume::Receiver<SandboxCreate>),
		events_tx:       flume::Sender<Value>,
		events_rx:       flume::Receiver<Value>,
	}

	impl StubEngine {
		fn new() -> Arc<Self> {
			let (events_tx, events_rx) = flume::unbounded();
			Arc::new(Self {
				create_response: Mutex::new(Ok(json!({"id": "created"}))),
				get_response: Mutex::new(Ok(json!({"id": "sandbox"}))),
				create_calls: flume::unbounded(),
				events_tx,
				events_rx,
			})
		}

		fn set_create(&self, response: Result<Value>) {
			*self.create_response.lock().expect("create response") = response;
		}

		fn set_get(&self, response: Result<Value>) {
			*self.get_response.lock().expect("get response") = response;
		}
	}

	macro_rules! stub_unexpected {
		($($method:ident($($arg:ident: $ty:ty),*) -> $ret:ty;)+) => {
			$(fn $method(&self, $($arg: $ty),*) -> $ret {
				$(let _ = $arg;)*
				Err(EngineError::engine(concat!("unexpected EngineApi::", stringify!($method))))
			})+
		};
	}

	impl EngineApi for StubEngine {
		stub_unexpected! {
			list(tags: Option<HashMap<String, String>>) -> Result<Vec<Value>>;
			stop(id: &str) -> Result<Value>;
			remove(id: &str) -> Result<Value>;
			terminate(id: &str, reason: &str) -> Result<Value>;
			pause(id: &str) -> Result<Value>;
			resume(id: &str) -> Result<Value>;
			suspend(id: &str) -> Result<Value>;
			history(id: &str) -> Result<Vec<RecoveryPoint>>;
			rollback(id: &str, recovery_point: &str) -> Result<Value>;
			extend(id: &str, secs: u64) -> Result<Value>;
			metrics(id: &str) -> Result<Value>;
			logs(id: &str) -> Result<Vec<u8>>;
			logs_follow(id: &str) -> Result<flume::Receiver<Vec<u8>>>;
			exec_capture(id: &str, req: ExecRequest) -> Result<ExecCapture>;
			exec_stream(id: &str, req: ExecRequest) -> Result<ExecStream>;
			shell_start(params: Value) -> Result<ShellSession>;
			file_read(id: &str, path: &str) -> Result<Vec<u8>>;
			file_write(id: &str, path: &str, data: &[u8]) -> Result<()>;
			file_delete(id: &str, path: &str, recursive: bool) -> Result<()>;
			file_list(id: &str, path: &str) -> Result<Value>;
			file_stat(id: &str, path: &str) -> Result<Value>;
			network_get(id: &str) -> Result<Value>;
			network_set(id: &str, policy: NetworkBody) -> Result<Value>;
			tunnels(id: &str) -> Result<Value>;
			tunnel_target(id: &str, port: u16) -> Result<(String, u16)>;
			snapshot(id: &str, name: Option<String>, stop: bool) -> Result<Value>;
			snapshot_fs(id: &str, name: Option<String>) -> Result<Value>;
			snapshots() -> Result<Vec<String>>;
			restore(snapshot: &str, body: RestoreBody) -> Result<Value>;
			snapshot_delete(snapshot: &str) -> Result<()>;
			fork(snapshot: &str, body: ForkBody) -> Result<Value>;
			volume_list() -> Result<Vec<String>>;
			volume_create(name: &str) -> Result<()>;
			volume_delete(name: &str) -> Result<()>;
			pool_list() -> Result<Value>;
			pool_set(reference: &str, body: PoolPutBody) -> Result<Value>;
			pool_delete(reference: &str) -> Result<()>;
			credential_list(tenant: &str) -> Result<Vec<CredentialMetadata>>;
			credential_put(tenant: &str, key_id: &str, credential: Credential) -> Result<CredentialMetadata>;
			credential_delete(tenant: &str, name: &str) -> Result<()>;
			info() -> Result<Value>;
			migrate(id: &str, target: &str) -> Result<Value>;
		}

		fn create(&self, params: SandboxCreate) -> Result<Value> {
			let response = self
				.create_response
				.lock()
				.expect("create response")
				.clone();
			let _ = self.create_calls.0.send(params);
			response
		}

		fn get(&self, id: &str) -> Result<Value> {
			let _ = id;
			self.get_response.lock().expect("get response").clone()
		}

		fn subscribe_events(&self) -> flume::Receiver<Value> {
			self.events_rx.clone()
		}

		fn shell_cleanup(&self, _name: &str) {}

		fn prometheus_metrics(&self) -> String {
			String::new()
		}
	}

	fn test_state(engine: Arc<StubEngine>) -> (ApiState, tempfile::TempDir) {
		let config = ServeConfig::default();
		let temp = tempfile::tempdir().expect("api tempdir");
		let home = Home::new(temp.path());
		let engine_api: Arc<dyn EngineApi> = engine;
		let functions =
			FunctionDomain::open(home, engine_api.clone(), &config).expect("open function domain");
		(ApiState::new(engine_api, functions, config, Transport::Tcp), temp)
	}

	fn test_api(engine: Arc<StubEngine>) -> (GrpcApi, tempfile::TempDir) {
		let (state, temp) = test_state(engine);
		(GrpcApi::new(state), temp)
	}

	fn admin_request<T>(message: T) -> Request<T> {
		let mut request = Request::new(message);
		request.extensions_mut().insert(Principal::local_admin());
		request
	}

	async fn next_frame(stream: &mut BoxStream<pb::JsonView>) -> Option<Value> {
		tokio::time::timeout(Duration::from_secs(5), stream.next())
			.await
			.expect("watch frame before deadline")
			.map(|frame| serde_json::from_str(&frame.expect("watch frame").json).expect("frame json"))
	}

	#[test]
	fn generated_sids_use_the_engine_shape() {
		let sid = generate_sid();
		assert!(sid.starts_with("sb-"));
		assert_eq!(sid.len(), "sb-".len() + 24);
		assert!(sid[3..].chars().all(|ch| ch.is_ascii_hexdigit()));
		assert_ne!(sid, generate_sid());
	}

	#[tokio::test]
	async fn no_wait_create_returns_starting_and_boots_in_background() {
		use pb::sandbox_service_server::SandboxService as _;
		let engine = StubEngine::new();
		let (api, _temp) = test_api(engine.clone());
		let response = api
			.create(admin_request(pb::CreateSandboxRequest {
				spec_json: "{}".to_owned(),
				no_wait:   true,
			}))
			.await
			.expect("no_wait create");
		let view: Value =
			serde_json::from_str(&response.into_inner().json).expect("admitted view json");
		assert_eq!(view["status"], "starting");
		let name = view["name"].as_str().expect("generated name");
		assert!(name.starts_with("sb-"), "generated name {name:?}");
		assert_eq!(view["id"], view["name"]);
		assert_eq!(view["owner_tenant"], "default", "admin creates keep the serde default tenant");
		let boot_calls = engine.create_calls.1.clone();
		let booted =
			tokio::task::spawn_blocking(move || boot_calls.recv_timeout(Duration::from_secs(5)))
				.await
				.expect("background boot recv task")
				.expect("background boot reached the engine");
		assert_eq!(booted.name.as_deref(), Some(name));
	}

	#[tokio::test]
	async fn watch_replays_initial_state_then_ends_on_ready_when_until_ready() {
		use pb::sandbox_service_server::SandboxService as _;
		let engine = StubEngine::new();
		engine.set_get(Ok(json!({"id": "sb-w", "name": "sb-w", "status": "creating"})));
		let (api, _temp) = test_api(engine.clone());
		let mut stream = api
			.watch(admin_request(pb::WatchSandboxRequest {
				id:          "sb-w".to_owned(),
				until_ready: true,
			}))
			.await
			.expect("watch stream")
			.into_inner();
		let first = next_frame(&mut stream).await.expect("initial frame");
		assert_eq!(first["status"], "creating");
		// Another sandbox's transition must not leak into this watch.
		engine
			.events_tx
			.send(json!({"type": "ready", "id": "sb-other", "status": "running"}))
			.expect("send unrelated event");
		engine
			.events_tx
			.send(json!({"type": "ready", "id": "sb-w", "name": "sb-w", "status": "running"}))
			.expect("send ready event");
		let second = next_frame(&mut stream).await.expect("ready frame");
		assert_eq!(second["status"], "running");
		assert_eq!(second["type"], "ready");
		assert!(next_frame(&mut stream).await.is_none(), "stream ends after ready");
	}

	#[tokio::test]
	async fn watch_unknown_sid_without_until_ready_is_not_found() {
		use pb::sandbox_service_server::SandboxService as _;
		let engine = StubEngine::new();
		engine.set_get(Err(EngineError::not_found("unknown sandbox 'ghost'")));
		let (api, _temp) = test_api(engine);
		let Err(error) = api
			.watch(admin_request(pb::WatchSandboxRequest {
				id:          "ghost".to_owned(),
				until_ready: false,
			}))
			.await
		else {
			panic!("unknown sid must be rejected");
		};
		assert_eq!(error.code(), Code::NotFound);
	}

	#[tokio::test]
	async fn batch_create_reports_per_item_success_and_stable_invalid() {
		let engine = StubEngine::new();
		engine.set_create(Ok(json!({"id": "sb-batch", "name": "sb-batch", "status": "running"})));
		let (state, _temp) = test_state(engine);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("bind test grpc listener");
		let addr = listener.local_addr().expect("test listener address");
		let router = grpc::routes(state);
		let server = tokio::spawn(async move {
			axum::serve(listener, router)
				.await
				.expect("serve test grpc");
		});
		let channel = Endpoint::from_shared(format!("http://{addr}"))
			.expect("test endpoint")
			.connect_lazy();
		let mut client = pb::sandbox_service_client::SandboxServiceClient::new(channel);
		let items = vec![
			pb::BatchCreateRequest {
				seq:    1,
				create: Some(pb::CreateSandboxRequest { spec_json: "{}".to_owned(), no_wait: false }),
			},
			pb::BatchCreateRequest {
				seq:    2,
				create: Some(pb::CreateSandboxRequest {
					spec_json: r#"{"cpus": 0}"#.to_owned(),
					no_wait:   false,
				}),
			},
			pb::BatchCreateRequest { seq: 3, create: None },
		];
		let mut request = Request::new(tokio_stream::iter(items));
		request
			.metadata_mut()
			.insert("x-vmon-principal-role", MetadataValue::from_static("admin"));
		let mut stream = tokio::time::timeout(Duration::from_secs(5), client.batch_create(request))
			.await
			.expect("batch connect before deadline")
			.expect("batch stream accepted")
			.into_inner();
		let mut outcomes = HashMap::new();
		while let Some(item) = tokio::time::timeout(Duration::from_secs(5), stream.next())
			.await
			.expect("batch item before deadline")
		{
			let item = item.expect("batch response");
			outcomes.insert(item.seq, item.outcome.expect("batch outcome"));
		}
		assert_eq!(outcomes.len(), 3, "one response per request");
		match outcomes.remove(&1).expect("seq 1") {
			pb::batch_create_response::Outcome::Json(json) => {
				let view: Value = serde_json::from_str(&json).expect("created view json");
				assert_eq!(view["id"], "sb-batch");
			},
			pb::batch_create_response::Outcome::Error(error) => {
				panic!("valid item failed: {} {}", error.code, error.message)
			},
		}
		for seq in [2, 3] {
			match outcomes.remove(&seq).expect("invalid seq") {
				pb::batch_create_response::Outcome::Error(error) => {
					assert_eq!(error.code, "invalid");
					assert!(!error.message.is_empty());
				},
				pb::batch_create_response::Outcome::Json(json) => {
					panic!("invalid item {seq} succeeded: {json}")
				},
			}
		}
		server.abort();
	}
}
