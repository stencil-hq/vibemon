//! envd HTTP server: listener + tokio runtime lifecycle ([`spawn`],
//! [`run_standalone`]), request routing, access-token enforcement, CORS, and
//! the Connect RPC dispatcher for `process.Process` and
//! `filesystem.Filesystem` (unary, server-streaming, client-streaming).

use std::{
	future::Future,
	net::{IpAddr, Ipv4Addr, SocketAddr},
	sync::Arc,
	time::{Duration, Instant},
};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde::{Serialize, de::DeserializeOwned};

use super::{
	DEFAULT_PORT, auth,
	cgroups::{self, Cgroups, Mode},
	connect::{
		self, Code, Codec, EnvelopeReader, FLAG_COMPRESSED, FLAG_END_STREAM, Framing, RpcError,
		StreamSink,
	},
	files, filesystem,
	http::{self as h, Resp},
	init, metrics, pause,
	pb::{filesystem as fspb, process as prpb},
	process,
	state::{Defaults, EnvdState, write_run_file},
	users::User,
	watch,
};

/// Default `PATH` for spawned processes when the agent provides none.
const DEFAULT_PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
/// Body limit for unary RPCs and small JSON endpoints.
const MAX_UNARY_BODY: usize = connect::MAX_MESSAGE_BYTES;
/// `Accept-Post` (RFC 9110 §15.5.16 suggests it for 415 responses).
const ACCEPT_POST: header::HeaderName = header::HeaderName::from_static("accept-post");
/// Streaming response channel depth (envelopes).
const STREAM_DEPTH: usize = 16;

/// How the agent starts envd.
#[derive(Clone, Debug)]
pub struct EnvdConfig {
	/// TCP port (default 49983).
	pub port:        u16,
	/// Bind address (default `0.0.0.0`).
	pub bind:        IpAddr,
	/// Initial `E2B_SANDBOX_ID` (normally set later by `/init`).
	pub sandbox_id:  Option<String>,
	/// Initial `E2B_TEMPLATE_ID` (normally set later by `/init`).
	pub template_id: Option<String>,
	/// Template/agent environment (incl. `PATH`) every process starts with.
	pub env:         Vec<(String, String)>,
}

impl Default for EnvdConfig {
	fn default() -> Self {
		Self {
			port:        DEFAULT_PORT,
			bind:        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
			sandbox_id:  None,
			template_id: None,
			env:         Vec::new(),
		}
	}
}

/// A running envd server.
pub struct EnvdHandle {
	addr:     SocketAddr,
	shutdown: Option<tokio::sync::oneshot::Sender<()>>,
	thread:   Option<std::thread::JoinHandle<()>>,
}

impl EnvdHandle {
	/// The bound listen address.
	pub const fn local_addr(&self) -> SocketAddr {
		self.addr
	}

	/// Stop accepting, drop the runtime (spawned processes keep running).
	pub fn shutdown(mut self) {
		self.stop();
	}

	/// Block until the server thread exits (never, unless it fails).
	pub fn join(mut self) {
		if let Some(t) = self.thread.take() {
			let _ = t.join();
		}
	}

	fn stop(&mut self) {
		if let Some(tx) = self.shutdown.take() {
			let _ = tx.send(());
		}
		if let Some(t) = self.thread.take() {
			let _ = t.join();
		}
	}
}

impl Drop for EnvdHandle {
	fn drop(&mut self) {
		self.stop();
	}
}

fn build_state(config: &EnvdConfig) -> Arc<EnvdState> {
	let path_env = config
		.env
		.iter()
		.rev()
		.find(|(k, _)| k == "PATH")
		.map_or_else(|| DEFAULT_PATH.to_owned(), |(_, v)| v.clone());
	let template_env = config
		.env
		.iter()
		.filter(|(k, _)| k != "PATH")
		.cloned()
		.collect();
	write_run_file(".E2B_SANDBOX", "true");
	if let Some(id) = &config.sandbox_id {
		write_run_file(".E2B_SANDBOX_ID", id);
	}
	if let Some(id) = &config.template_id {
		write_run_file(".E2B_TEMPLATE_ID", id);
	}
	Arc::new(EnvdState {
		template_env,
		path_env,
		defaults: parking_lot::RwLock::new(Defaults {
			user: "root".into(),
			sandbox_id: config.sandbox_id.clone(),
			template_id: config.template_id.clone(),
			..Defaults::default()
		}),
		processes: process::Registry::default(),
		watchers: watch::Registry::default(),
		cgroups: Cgroups::init(std::path::Path::new(cgroups::CGROUP_MOUNT)),
		freeze_lock: tokio::sync::Mutex::new(()),
		init_lock: tokio::sync::Mutex::new(()),
		cpu: parking_lot::Mutex::new(metrics::CpuSampler::default()),
		cpu_target: parking_lot::Mutex::new((0, 0)),
	})
}

/// Bind `config.bind:config.port` and serve envd on a dedicated thread with
/// its own tokio runtime. Returns once the listener is bound.
pub fn spawn(config: EnvdConfig) -> std::io::Result<EnvdHandle> {
	let listener = std::net::TcpListener::bind(SocketAddr::new(config.bind, config.port))?;
	listener.set_nonblocking(true)?;
	let addr = listener.local_addr()?;
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.thread_name("envd")
		.build()?;
	let (tx, rx) = tokio::sync::oneshot::channel();
	let thread = std::thread::Builder::new()
		.name("envd".into())
		.spawn(move || {
			runtime.block_on(async move {
				let state = build_state(&config);
				let listener = match tokio::net::TcpListener::from_std(listener) {
					Ok(l) => l,
					Err(e) => {
						crate::logging::emit(
							crate::logging::Level::Error,
							"envd: listener failed",
							serde_json::json!({"error": e.to_string()}),
						);
						return;
					},
				};
				crate::logging::emit(
					crate::logging::Level::Info,
					"envd: listening",
					serde_json::json!({"addr": addr.to_string(), "version": super::ENVD_VERSION}),
				);
				tokio::select! {
					() = accept_loop(listener, state) => {},
					_ = rx => {},
				}
			});
			runtime.shutdown_background();
		})?;
	Ok(EnvdHandle { addr, shutdown: Some(tx), thread: Some(thread) })
}

/// `vmon-agent envd [--port N]`: serve in the foreground until killed.
pub fn run_standalone(args: &[String]) -> std::io::Result<()> {
	// Like upstream envd, only PATH is inherited: HOME/USER/... are per request
	// user.
	let env = std::env::var("PATH")
		.map(|p| vec![("PATH".to_owned(), p)])
		.unwrap_or_default();
	let mut config = EnvdConfig { env, ..EnvdConfig::default() };
	let mut it = args.iter();
	while let Some(arg) = it.next() {
		let value = if let Some(v) = arg.strip_prefix("--port=") {
			v.to_owned()
		} else if arg == "--port" {
			it.next().cloned().unwrap_or_default()
		} else {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidInput,
				format!("unknown envd argument {arg:?} (usage: vmon-agent envd [--port N])"),
			));
		};
		config.port = value.parse().map_err(|_| {
			std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("invalid port {value:?}"))
		})?;
	}
	// The standalone server must reap orphans of the processes it spawns.
	// SAFETY: prctl(PR_SET_CHILD_SUBREAPER) only changes this process's flag.
	unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
	let handle = spawn(config)?;
	std::thread::spawn(|| {
		loop {
			std::thread::sleep(Duration::from_millis(200));
			crate::reaper::reap_untracked();
		}
	});
	handle.join();
	Ok(())
}

async fn accept_loop(listener: tokio::net::TcpListener, state: Arc<EnvdState>) {
	loop {
		let Ok((stream, peer)) = listener.accept().await else {
			// EMFILE and friends: back off instead of spinning.
			tokio::time::sleep(Duration::from_millis(50)).await;
			continue;
		};
		let _ = stream.set_nodelay(true);
		let local = stream.local_addr().unwrap_or(peer);
		let state = state.clone();
		tokio::spawn(async move {
			let service = hyper::service::service_fn(move |req| {
				let state = state.clone();
				async move { Ok::<_, std::convert::Infallible>(route(state, req, peer, local).await) }
			});
			let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
				.serve_connection(TokioIo::new(stream), service)
				.await;
		});
	}
}

async fn route(
	state: Arc<EnvdState>,
	req: Request<Incoming>,
	peer: SocketAddr,
	local: SocketAddr,
) -> Resp {
	let method = req.method().clone();
	let path = req.uri().path().to_owned();
	if method == Method::OPTIONS
		&& req
			.headers()
			.contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
	{
		return h::preflight(req.headers().get(header::ACCESS_CONTROL_REQUEST_HEADERS));
	}
	let mut resp = if !auth::is_exempt(&method, &path)
		&& !auth::token_ok(req.headers(), state.access_token().as_deref())
	{
		h::error(StatusCode::UNAUTHORIZED, auth::UNAUTHORIZED_MESSAGE)
	} else {
		dispatch(&state, req, &method, &path, peer, local).await
	};
	h::add_cors(&mut resp);
	resp
}

async fn dispatch(
	state: &Arc<EnvdState>,
	req: Request<Incoming>,
	method: &Method,
	path: &str,
	peer: SocketAddr,
	local: SocketAddr,
) -> Resp {
	if let Some(rpc) = path.strip_prefix("/process.Process/") {
		return rpc_entry(state, req, Service::Process, rpc.to_owned()).await;
	}
	if let Some(rpc) = path.strip_prefix("/filesystem.Filesystem/") {
		return rpc_entry(state, req, Service::Filesystem, rpc.to_owned()).await;
	}
	let query = req.uri().query().map(str::to_owned);
	match (method, path) {
		(&Method::GET, "/health") => {
			let mut r = h::no_content();
			r.headers_mut()
				.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
			r
		},
		(&Method::GET, "/metrics") => {
			let target = *state.cpu_target.lock();
			let collected = metrics::collect(&mut state.cpu.lock(), target);
			match collected {
				Ok(m) => h::json(StatusCode::OK, &m),
				Err(e) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e),
			}
		},
		(&Method::GET, "/envs") => h::json(StatusCode::OK, &state.default_env()),
		(&Method::POST, "/init") => {
			let (parts, body) = req.into_parts();
			match h::read_body(body, MAX_UNARY_BODY).await {
				Ok(b) => init::handle(state, &parts.headers, &b, peer, local).await,
				Err(e) => h::text_error(StatusCode::BAD_REQUEST, e.to_string()),
			}
		},
		(&Method::GET, "/files") => files::download(state, req.headers(), query.as_deref()).await,
		(&Method::POST, "/files") => {
			let (parts, body) = req.into_parts();
			files::upload(state, &parts.headers, query.as_deref(), body).await
		},
		(&Method::POST, "/files/compose") => files::compose(state, req.into_body()).await,
		(&Method::POST, "/freeze") => freeze(state, query.as_deref()).await,
		(&Method::POST, "/unfreeze") => {
			let Ok(_guard) = state.freeze_lock.try_lock() else {
				return h::response(StatusCode::SERVICE_UNAVAILABLE, h::empty());
			};
			match state.cgroups.thaw() {
				Ok(()) => h::no_content(),
				Err(e) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e),
			}
		},
		(&Method::POST, "/fsfreeze" | "/fsthaw") => {
			let Ok(_guard) = state.freeze_lock.try_lock() else {
				return h::response(StatusCode::SERVICE_UNAVAILABLE, h::empty());
			};
			let freeze = path == "/fsfreeze";
			let result = tokio::task::spawn_blocking(move || {
				if freeze {
					pause::fs_freeze()
				} else {
					pause::fs_thaw()
				}
			})
			.await;
			match result {
				Ok(Ok(())) => h::no_content(),
				Ok(Err(e)) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
				Err(e) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
			}
		},
		(&Method::POST, "/collapse") => match tokio::task::spawn_blocking(pause::collapse).await {
			Ok(Ok(r)) => h::json(StatusCode::OK, &r),
			Ok(Err(e)) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
			Err(e) => h::error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
		},
		(
			_,
			"/health" | "/metrics" | "/envs" | "/init" | "/files" | "/files/compose" | "/freeze"
			| "/unfreeze" | "/fsfreeze" | "/fsthaw" | "/collapse",
		) => h::error(StatusCode::METHOD_NOT_ALLOWED, format!("method {method} not allowed")),
		_ => h::error(StatusCode::NOT_FOUND, format!("path {path} not found")),
	}
}

async fn freeze(state: &Arc<EnvdState>, query: Option<&str>) -> Resp {
	let q = files::parse_query(query);
	let mode = match q.get("mode").map(String::as_str) {
		None | Some("" | "legacy") => Mode::Legacy,
		Some("hierarchy") => Mode::Hierarchy,
		Some(other) => return h::error(StatusCode::BAD_REQUEST, format!("invalid mode: {other}")),
	};
	let max_cgroups = q
		.get("maxCgroups")
		.and_then(|v| v.parse::<i64>().ok())
		.and_then(|v| usize::try_from(v).ok());
	let max_wait = q
		.get("maxWaitMs")
		.and_then(|v| v.parse::<i64>().ok())
		.filter(|&v| v > 0)
		.map(|v| Duration::from_millis(v.unsigned_abs()));
	let Ok(_guard) = state.freeze_lock.try_lock() else {
		return h::response(StatusCode::SERVICE_UNAVAILABLE, h::empty());
	};
	let result = state.cgroups.freeze(mode, max_cgroups, max_wait).await;
	if max_wait.is_some() {
		h::json(StatusCode::OK, &result)
	} else {
		h::no_content()
	}
}

#[derive(Clone, Copy)]
enum Service {
	Process,
	Filesystem,
}

/// Per-call context shared by all RPC handlers.
struct Call {
	state:     Arc<EnvdState>,
	headers:   HeaderMap,
	codec:     Codec,
	deadline:  Option<Duration>,
	/// `Authorization: Basic` username (validated lazily per handler).
	user_name: Option<String>,
}

impl Call {
	fn user(&self) -> Result<User, RpcError> {
		self
			.state
			.resolve_user(self.user_name.as_deref())
			.map_err(|e| match e {
				super::state::UserError::Unknown(_) => {
					RpcError::new(Code::Unauthenticated, e.to_string())
				},
				super::state::UserError::Lookup(_) => RpcError::internal(e.to_string()),
			})
	}

	fn resolve(&self, user: &User, path: &str) -> Result<std::path::PathBuf, RpcError> {
		filesystem::resolve(path, user, self.state.workdir().as_deref())
	}
}

fn rpc_error_response(err: &RpcError) -> Resp {
	let mut resp = h::response(
		StatusCode::from_u16(err.code.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
		h::full(err.to_json().to_string()),
	);
	resp
		.headers_mut()
		.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
	resp
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
	Unary,
	ServerStream,
	ClientStream,
}

fn rpc_kind(service: Service, name: &str) -> Option<Kind> {
	Some(match (service, name) {
		(Service::Process, "List" | "Update" | "SendInput" | "SendSignal" | "CloseStdin")
		| (
			Service::Filesystem,
			"Stat" | "MakeDir" | "Move" | "ListDir" | "Remove" | "CreateWatcher" | "GetWatcherEvents"
			| "RemoveWatcher",
		) => Kind::Unary,
		(Service::Process, "Start" | "Connect") | (Service::Filesystem, "WatchDir") => {
			Kind::ServerStream
		},
		(Service::Process, "StreamInput") => Kind::ClientStream,
		_ => return None,
	})
}

async fn rpc_entry(
	state: &Arc<EnvdState>,
	req: Request<Incoming>,
	service: Service,
	name: String,
) -> Resp {
	let Some(kind) = rpc_kind(service, &name) else {
		return rpc_error_response(&RpcError::new(
			Code::NotFound,
			format!("unknown procedure {name}"),
		));
	};
	if req.method() != Method::POST {
		let mut r = h::error(StatusCode::METHOD_NOT_ALLOWED, "connect RPCs require POST");
		r.headers_mut()
			.insert(header::ALLOW, HeaderValue::from_static("POST"));
		return r;
	}
	let content_type = req
		.headers()
		.get(header::CONTENT_TYPE)
		.and_then(|v| v.to_str().ok())
		.unwrap_or("");
	let Some((codec, framing)) = Codec::from_content_type(content_type) else {
		let mut r = h::response(StatusCode::UNSUPPORTED_MEDIA_TYPE, h::empty());
		r.headers_mut().insert(
			ACCEPT_POST,
			HeaderValue::from_static(
				"application/json, application/proto, application/connect+json, \
				 application/connect+proto",
			),
		);
		return r;
	};
	let expected = if kind == Kind::Unary {
		Framing::Unary
	} else {
		Framing::Stream
	};
	if framing != expected {
		let mut r = h::response(StatusCode::UNSUPPORTED_MEDIA_TYPE, h::empty());
		r.headers_mut()
			.insert(ACCEPT_POST, HeaderValue::from_static(codec.content_type(expected)));
		return r;
	}
	let (parts, body) = req.into_parts();
	let headers = parts.headers;
	let fail = |e: RpcError| -> Resp {
		if kind == Kind::Unary {
			rpc_error_response(&e)
		} else {
			stream_error_response(codec, &e)
		}
	};
	let deadline = match connect::parse_timeout(&headers) {
		Ok(d) => d,
		Err(e) => return fail(e),
	};
	let user_name = match auth::basic_auth_user(&headers) {
		Ok(u) => u,
		Err(e) => return fail(RpcError::new(Code::Unauthenticated, e)),
	};
	let encoding = match connect::request_encoding(&headers, framing) {
		Ok(e) => e,
		Err(e) => return fail(e),
	};
	let call = Call { state: state.clone(), headers, codec, deadline, user_name };
	// envd's authn middleware rejects unknown Basic-auth users on every RPC.
	if call.user_name.is_some()
		&& let Err(e) = call.user()
	{
		return fail(e);
	}

	match kind {
		Kind::Unary => {
			let body = match h::read_body(body, MAX_UNARY_BODY).await {
				Ok(b) => b,
				Err(h::BodyError::TooLarge) => {
					return rpc_error_response(&RpcError::new(
						Code::ResourceExhausted,
						"request too large",
					));
				},
				Err(e) => return rpc_error_response(&RpcError::new(Code::Canceled, e.to_string())),
			};
			let body = match encoding.decode(body) {
				Ok(b) => b,
				Err(e) => return rpc_error_response(&e),
			};
			let fut = unary(&call, service, &name, body);
			let result = match deadline {
				Some(d) => tokio::time::timeout(d, fut).await.unwrap_or_else(|_| {
					Err(RpcError::new(Code::DeadlineExceeded, "the operation timed out"))
				}),
				None => fut.await,
			};
			match result {
				Ok(bytes) => {
					let mut r = h::response(StatusCode::OK, h::full(bytes));
					r.headers_mut().insert(
						header::CONTENT_TYPE,
						HeaderValue::from_static(codec.content_type(Framing::Unary)),
					);
					r
				},
				Err(e) => rpc_error_response(&e),
			}
		},
		Kind::ServerStream => {
			// The request is exactly one envelope.
			let raw = match h::read_body(body, MAX_UNARY_BODY + 5).await {
				Ok(b) => b,
				Err(e) => return stream_error_response(codec, &RpcError::invalid(e.to_string())),
			};
			let message = match single_envelope(&raw, encoding) {
				Ok(m) => m,
				Err(e) => return stream_error_response(codec, &e),
			};
			let (tx, resp_body) = h::channel(STREAM_DEPTH);
			let sink = StreamSink::new(tx, codec);
			tokio::spawn(async move {
				let result = server_stream(&call, &name, &message, &sink).await;
				sink.finish(result.err().as_ref()).await;
			});
			stream_response(codec, resp_body)
		},
		Kind::ClientStream => {
			let result = stream_input(&call, body, encoding).await;
			let (tx, resp_body) = h::channel(2);
			let sink = StreamSink::new(tx, codec);
			match result {
				Ok(()) => {
					let _ = sink.send(&prpb::StreamInputResponse {}).await;
					sink.finish(None).await;
				},
				Err(e) => sink.finish(Some(&e)).await,
			}
			stream_response(codec, resp_body)
		},
	}
}

fn stream_response(codec: Codec, body: h::RespBody) -> Resp {
	let mut r = h::response(StatusCode::OK, body);
	r.headers_mut()
		.insert(header::CONTENT_TYPE, HeaderValue::from_static(codec.content_type(Framing::Stream)));
	r
}

fn stream_error_response(codec: Codec, err: &RpcError) -> Resp {
	stream_response(codec, h::full(connect::end_stream(Some(err))))
}

fn single_envelope(raw: &Bytes, encoding: connect::Encoding) -> Result<Bytes, RpcError> {
	let mut reader = EnvelopeReader::default();
	reader.push(raw);
	let (flags, payload) = reader
		.next()?
		.ok_or_else(|| RpcError::invalid("missing request message"))?;
	if !reader.is_empty() {
		return Err(RpcError::invalid("server-streaming request must contain exactly one message"));
	}
	decode_envelope(flags, payload, encoding)
}

fn decode_envelope(
	flags: u8,
	payload: Bytes,
	encoding: connect::Encoding,
) -> Result<Bytes, RpcError> {
	if flags & FLAG_END_STREAM != 0 {
		return Err(RpcError::invalid("unexpected end-stream message in request"));
	}
	if flags & FLAG_COMPRESSED != 0 {
		if encoding == connect::Encoding::Identity {
			return Err(RpcError::new(
				Code::Internal,
				"compressed message without Connect-Content-Encoding",
			));
		}
		return encoding.decode(payload);
	}
	Ok(payload)
}

async fn run_unary<Req, Res, F, Fut>(codec: Codec, body: &[u8], f: F) -> Result<Bytes, RpcError>
where
	Req: prost::Message + Default + DeserializeOwned,
	Res: prost::Message + Serialize,
	F: FnOnce(Req) -> Fut,
	Fut: Future<Output = Result<Res, RpcError>>,
{
	let req = codec.decode::<Req>(body)?;
	let res = f(req).await?;
	Ok(codec.encode(&res))
}

async fn blocking<T: Send + 'static>(
	f: impl FnOnce() -> Result<T, RpcError> + Send + 'static,
) -> Result<T, RpcError> {
	tokio::task::spawn_blocking(f)
		.await
		.unwrap_or_else(|e| Err(RpcError::internal(format!("task failed: {e}"))))
}

async fn unary(call: &Call, service: Service, name: &str, body: Bytes) -> Result<Bytes, RpcError> {
	let codec = call.codec;
	let processes = &call.state.processes;
	match (service, name) {
		(Service::Process, "List") => {
			run_unary(codec, &body, |_: prpb::ListRequest| async {
				Ok(prpb::ListResponse { processes: processes.list() })
			})
			.await
		},
		(Service::Process, "Update") => {
			run_unary(codec, &body, |req: prpb::UpdateRequest| async move {
				let proc = processes.find_live(req.process.as_ref())?;
				if let Some(size) = req.pty.and_then(|p| p.size) {
					proc.resize(size.cols, size.rows)?;
				}
				Ok(prpb::UpdateResponse {})
			})
			.await
		},
		(Service::Process, "SendInput") => {
			run_unary(codec, &body, |req: prpb::SendInputRequest| async move {
				let proc = processes.find_live(req.process.as_ref())?;
				process::send_input(&proc, req.input).await?;
				Ok(prpb::SendInputResponse {})
			})
			.await
		},
		(Service::Process, "SendSignal") => {
			run_unary(codec, &body, |req: prpb::SendSignalRequest| async move {
				let proc = processes.find_live(req.process.as_ref())?;
				let sig = process::posix_signal(req.signal)?;
				proc.signal(sig).map_err(|e| match e.raw_os_error() {
					Some(libc::ESRCH) => {
						RpcError::not_found(format!("process with pid {} not found", proc.pid))
					},
					_ => RpcError::internal(format!("error sending signal: {e}")),
				})?;
				Ok(prpb::SendSignalResponse {})
			})
			.await
		},
		(Service::Process, "CloseStdin") => {
			run_unary(codec, &body, |req: prpb::CloseStdinRequest| async move {
				let proc = processes.find_live(req.process.as_ref())?;
				proc.close_stdin().await?;
				Ok(prpb::CloseStdinResponse {})
			})
			.await
		},
		(Service::Filesystem, "Stat") => {
			run_unary(codec, &body, |req: fspb::StatRequest| async move {
				let user = call.user()?;
				let path = call.resolve(&user, &req.path)?;
				blocking(move || filesystem::stat(&path)).await
			})
			.await
		},
		(Service::Filesystem, "MakeDir") => {
			run_unary(codec, &body, |req: fspb::MakeDirRequest| async move {
				let user = call.user()?;
				let path = call.resolve(&user, &req.path)?;
				blocking(move || filesystem::make_dir(&path, &user)).await
			})
			.await
		},
		(Service::Filesystem, "Move") => {
			run_unary(codec, &body, |req: fspb::MoveRequest| async move {
				let user = call.user()?;
				let source = call.resolve(&user, &req.source)?;
				let destination = call.resolve(&user, &req.destination)?;
				blocking(move || filesystem::rename(&source, &destination, &user)).await
			})
			.await
		},
		(Service::Filesystem, "ListDir") => {
			run_unary(codec, &body, |req: fspb::ListDirRequest| async move {
				let user = call.user()?;
				let path = call.resolve(&user, &req.path)?;
				blocking(move || filesystem::list_dir(&path, req.depth)).await
			})
			.await
		},
		(Service::Filesystem, "Remove") => {
			run_unary(codec, &body, |req: fspb::RemoveRequest| async move {
				let user = call.user()?;
				let path = call.resolve(&user, &req.path)?;
				blocking(move || filesystem::remove(&path)).await
			})
			.await
		},
		(Service::Filesystem, "CreateWatcher") => {
			run_unary(codec, &body, |req: fspb::CreateWatcherRequest| async move {
				let user = call.user()?;
				let path = call.resolve(&user, &req.path)?;
				let watcher = watch::Watcher::new(
					&path,
					req.recursive,
					req.include_entry,
					req.allow_network_mounts,
				)?;
				Ok(fspb::CreateWatcherResponse { watcher_id: call.state.watchers.create(watcher) })
			})
			.await
		},
		(Service::Filesystem, "GetWatcherEvents") => {
			run_unary(codec, &body, |req: fspb::GetWatcherEventsRequest| async move {
				Ok(fspb::GetWatcherEventsResponse {
					events: call.state.watchers.drain(&req.watcher_id)?,
				})
			})
			.await
		},
		(Service::Filesystem, "RemoveWatcher") => {
			run_unary(codec, &body, |req: fspb::RemoveWatcherRequest| async move {
				call.state.watchers.remove(&req.watcher_id)?;
				Ok(fspb::RemoveWatcherResponse {})
			})
			.await
		},
		_ => Err(RpcError::new(Code::Unimplemented, format!("{name} is not a unary procedure"))),
	}
}

fn deadline_error() -> RpcError {
	RpcError::new(Code::DeadlineExceeded, "the operation timed out")
}

/// Run `fut` within the stream deadline, if any.
async fn within(
	deadline: Option<Duration>,
	fut: impl Future<Output = Result<(), RpcError>>,
) -> Result<(), RpcError> {
	match deadline {
		Some(d) => tokio::time::timeout(d, fut)
			.await
			.unwrap_or_else(|_| Err(deadline_error())),
		None => fut.await,
	}
}

async fn server_stream(
	call: &Call,
	name: &str,
	message: &[u8],
	sink: &StreamSink,
) -> Result<(), RpcError> {
	let keepalive = connect::parse_keepalive(&call.headers);
	match name {
		"Start" => {
			let req: prpb::StartRequest = call.codec.decode(message)?;
			let user = call.user()?;
			let started = Instant::now();
			let ctx = process::SpawnContext { user: &user, lifetime: call.deadline };
			let (proc, rx) = process::spawn(&call.state, req, &ctx)?;
			process::forward(proc.pid, process::Subscription::Live(rx), sink, keepalive, |event| {
				prpb::StartResponse { event: Some(event) }
			})
			.await?;
			// The process was killed by its lifetime deadline.
			if call.deadline.is_some_and(|d| started.elapsed() >= d) {
				return Err(deadline_error());
			}
			Ok(())
		},
		"Connect" => {
			let req: prpb::ConnectRequest = call.codec.decode(message)?;
			let proc = call
				.state
				.processes
				.find_connectable(req.process.as_ref())?;
			let sub = proc.subscribe();
			within(
				call.deadline,
				process::forward(proc.pid, sub, sink, keepalive, |event| prpb::ConnectResponse {
					event: Some(event),
				}),
			)
			.await
		},
		"WatchDir" => {
			let req: fspb::WatchDirRequest = call.codec.decode(message)?;
			let user = call.user()?;
			let path = call.resolve(&user, &req.path)?;
			let watcher =
				watch::Watcher::new(&path, req.recursive, req.include_entry, req.allow_network_mounts)?;
			within(call.deadline, watch::stream(watcher, sink, keepalive)).await
		},
		_ => Err(RpcError::new(
			Code::Unimplemented,
			format!("{name} is not a server-streaming procedure"),
		)),
	}
}

/// `StreamInput`: first message selects the process, then `data` messages
/// are written in order; ends at the client's end-stream or body EOF.
async fn stream_input(
	call: &Call,
	mut body: Incoming,
	encoding: connect::Encoding,
) -> Result<(), RpcError> {
	use prpb::stream_input_request::Event;
	let work = async {
		let mut reader = EnvelopeReader::default();
		let mut target: Option<Arc<process::Proc>> = None;
		loop {
			while let Some((flags, payload)) = reader.next()? {
				if flags & FLAG_END_STREAM != 0 {
					return Ok(());
				}
				let payload = decode_envelope(flags, payload, encoding)?;
				let msg: prpb::StreamInputRequest = call.codec.decode(&payload)?;
				match msg.event {
					Some(Event::Start(start)) => {
						target = Some(call.state.processes.find_live(start.process.as_ref())?);
					},
					Some(Event::Data(data)) => {
						let proc = target.as_ref().ok_or_else(|| {
							RpcError::new(Code::FailedPrecondition, "first message must be start")
						})?;
						process::send_input(proc, data.input).await?;
					},
					Some(Event::Keepalive(_)) | None => {},
				}
			}
			match body.frame().await {
				Some(Ok(frame)) => {
					if let Ok(data) = frame.into_data() {
						reader.push(&data);
					}
				},
				Some(Err(e)) => {
					return Err(RpcError::new(Code::Canceled, format!("read request: {e}")));
				},
				None if reader.is_empty() => return Ok(()),
				None => return Err(RpcError::invalid("truncated request message")),
			}
		}
	};
	within(call.deadline, work).await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rpc_kinds() {
		assert!(matches!(rpc_kind(Service::Process, "Start"), Some(Kind::ServerStream)));
		assert!(matches!(rpc_kind(Service::Process, "StreamInput"), Some(Kind::ClientStream)));
		assert!(matches!(rpc_kind(Service::Filesystem, "GetWatcherEvents"), Some(Kind::Unary)));
		assert!(rpc_kind(Service::Filesystem, "Start").is_none());
		assert!(rpc_kind(Service::Process, "Stat").is_none());
	}

	#[test]
	fn stream_request_must_be_one_plain_envelope() {
		let one = connect::envelope(0, b"{}");
		assert_eq!(&single_envelope(&one, connect::Encoding::Identity).unwrap()[..], b"{}");
		let mut two = one.to_vec();
		two.extend_from_slice(&one);
		assert!(single_envelope(&Bytes::from(two), connect::Encoding::Identity).is_err());
		let compressed = connect::envelope(FLAG_COMPRESSED, b"x");
		assert_eq!(
			single_envelope(&compressed, connect::Encoding::Identity)
				.unwrap_err()
				.code,
			Code::Internal
		);
		assert!(single_envelope(&Bytes::new(), connect::Encoding::Identity).is_err());
	}
}
