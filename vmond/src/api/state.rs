use std::{
	io,
	os::fd::AsRawFd,
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use axum::{
	body::Body,
	extract::State,
	http::{HeaderMap, HeaderValue, Method, Request, header},
	middleware::Next,
	response::Response,
	serve::Listener,
};
use tokio::{
	net::{UnixListener, UnixStream},
	sync::Semaphore,
};

use super::error::ApiError;
use crate::{
	config::ServeConfig,
	engine::EngineApi,
	function::FunctionDomain,
	mesh::runtime::MeshRuntime,
	security::{Principal, TenantDirectory, tenant::Role},
};

pub(super) const PRINCIPAL_TENANT_HEADER: &str = "x-vmon-principal-tenant";
pub(super) const PRINCIPAL_ROLE_HEADER: &str = "x-vmon-principal-role";
pub(super) const PRINCIPAL_KEY_HEADER: &str = "x-vmon-principal-key";
const MESH_HOP_HEADER: &str = "x-vmon-mesh-hop";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transport {
	Tcp,
	Unix,
}

#[derive(Clone)]
pub struct ApiState {
	pub engine:        Arc<dyn EngineApi>,
	pub functions:     Arc<FunctionDomain>,
	pub config:        Arc<ServeConfig>,
	pub auth_failures: Arc<AtomicU64>,
	pub transport:     Transport,
	pub mesh:          Option<Arc<MeshRuntime>>,
	pub orch_worker:   Option<Arc<crate::orch::worker::OrchWorker>>,
	/// Bounds concurrently booting local sandboxes; see [`boot_gate`].
	pub boot_gate:     Arc<Semaphore>,
}

/// Size the local boot gate from `boot_concurrency` (zero derives 4× the
/// host's logical CPUs). Admission stays instant — the gate only queues the
/// CPU-heavy VM boots so a create burst cannot thrash the host or exhaust
/// the blocking thread pool.
pub fn boot_gate(config: &ServeConfig) -> Arc<Semaphore> {
	let permits = if config.boot_concurrency == 0 {
		std::thread::available_parallelism().map_or(16, std::num::NonZero::get) * 4
	} else {
		config.boot_concurrency
	};
	Arc::new(Semaphore::new(permits))
}

impl ApiState {
	pub fn new(
		engine: Arc<dyn EngineApi>,
		functions: Arc<FunctionDomain>,
		config: ServeConfig,
		transport: Transport,
	) -> Self {
		let boot_gate = boot_gate(&config);
		Self {
			engine,
			functions,
			config: Arc::new(config),
			auth_failures: Arc::new(AtomicU64::new(0)),
			transport,
			mesh: None,
			orch_worker: None,
			boot_gate,
		}
	}

	pub fn with_transport(&self, transport: Transport) -> Self {
		Self { transport, ..self.clone() }
	}

	pub fn with_mesh(&self, mesh: Arc<MeshRuntime>) -> Self {
		Self { mesh: Some(mesh), ..self.clone() }
	}

	pub fn with_orch_worker(&self, worker: Arc<crate::orch::worker::OrchWorker>) -> Self {
		Self { orch_worker: Some(worker), ..self.clone() }
	}

	pub fn count_auth_failure(&self) {
		self.auth_failures.fetch_add(1, Ordering::Relaxed);
	}

	pub fn auth_failure_count(&self) -> u64 {
		self.auth_failures.load(Ordering::Relaxed)
	}

	/// Resolve an API bearer into its tenant identity.
	pub fn principal_for_token(&self, supplied: Option<&str>) -> crate::Result<Option<Principal>> {
		Ok(TenantDirectory::new(
			self.config.token.clone(),
			self.config.client_token.clone(),
			self.config.tenant_tokens.clone(),
			self.config.tenant_keys.clone(),
		)?
		.authenticate(supplied))
	}

	/// Whether TCP requests require a configured bearer credential.
	pub fn tokens_configured(&self) -> crate::Result<bool> {
		Ok(TenantDirectory::new(
			self.config.token.clone(),
			self.config.client_token.clone(),
			self.config.tenant_tokens.clone(),
			self.config.tenant_keys.clone(),
		)?
		.tokens_configured())
	}
}

#[derive(Clone, Debug)]
pub struct UdsPeerInfo;

pub struct UdsPeerListener {
	inner:      UnixListener,
	server_uid: u32,
}

impl UdsPeerListener {
	pub const fn new(inner: UnixListener, server_uid: u32) -> Self {
		Self { inner, server_uid }
	}
}

impl Listener for UdsPeerListener {
	type Addr = UdsPeerInfo;
	type Io = UnixStream;

	async fn accept(&mut self) -> (Self::Io, Self::Addr) {
		loop {
			match self.inner.accept().await {
				Ok((stream, _addr)) => {
					let uid = peer_uid(&stream).ok();
					let allowed = uid.is_some_and(|uid| uid == 0 || uid == self.server_uid);
					if allowed {
						return (stream, UdsPeerInfo);
					}
					tracing::warn!(uid = ?uid, "rejecting unauthorized UDS peer");
				},
				Err(err) => {
					tracing::error!("UDS accept error: {err}");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	fn local_addr(&self) -> io::Result<Self::Addr> {
		Ok(UdsPeerInfo)
	}
}

pub async fn auth_middleware(
	State(state): State<ApiState>,
	mut request: Request<Body>,
	next: Next,
) -> Result<Response, ApiError> {
	if is_public_path(request.method(), request.uri().path()) {
		return Ok(next.run(request).await);
	}
	match state.transport {
		Transport::Unix => {
			let principal = Principal::local_admin();
			set_principal_headers(request.headers_mut(), &principal);
			request.extensions_mut().insert(principal);
			Ok(next.run(request).await)
		},
		Transport::Tcp => authorize_tcp(&state, request, next).await,
	}
}

async fn authorize_tcp(
	state: &ApiState,
	mut request: Request<Body>,
	next: Next,
) -> Result<Response, ApiError> {
	let supplied = request_bearer_token(&request);
	let configured = state.tokens_configured()?;
	let principal = state.principal_for_token(supplied.as_deref())?;
	if configured && principal.is_none() {
		state.count_auth_failure();
		if request.uri().path().starts_with("/vmon.v1.") {
			return Ok(grpc_auth_error("16", "unauthorized", true));
		}
		return Err(ApiError::unauthorized("unauthorized"));
	}
	let authenticated = principal.unwrap_or_else(Principal::local_admin);
	let principal = forwarded_principal(&request, &authenticated).unwrap_or(authenticated);
	if !principal.is_admin() && is_admin_path(request.uri().path()) {
		if request.uri().path().starts_with("/vmon.v1.") {
			return Ok(grpc_auth_error("7", "forbidden", false));
		}
		return Err(ApiError::forbidden("forbidden"));
	}
	set_principal_headers(request.headers_mut(), &principal);
	request.extensions_mut().insert(principal);
	Ok(next.run(request).await)
}

fn forwarded_principal(request: &Request<Body>, authenticated: &Principal) -> Option<Principal> {
	if !authenticated.is_admin()
		|| request
			.headers()
			.get(MESH_HOP_HEADER)
			.and_then(|value| value.to_str().ok())
			!= Some("1")
	{
		return None;
	}
	let role = request
		.headers()
		.get(PRINCIPAL_ROLE_HEADER)
		.and_then(|value| value.to_str().ok())?;
	if role == "admin" {
		return Some(Principal::local_admin());
	}
	if role != "client" {
		return None;
	}
	let tenant = request
		.headers()
		.get(PRINCIPAL_TENANT_HEADER)
		.and_then(|value| value.to_str().ok())?;
	let key_id = request
		.headers()
		.get(PRINCIPAL_KEY_HEADER)
		.and_then(|value| value.to_str().ok())?;
	Principal::client(tenant, key_id).ok()
}

pub(super) fn set_principal_headers(headers: &mut HeaderMap, principal: &Principal) {
	let role = match principal.role {
		Role::Admin => "admin",
		Role::Client => "client",
	};
	for (name, value) in [
		(PRINCIPAL_TENANT_HEADER, principal.tenant.as_str()),
		(PRINCIPAL_ROLE_HEADER, role),
		(PRINCIPAL_KEY_HEADER, principal.key_id.as_str()),
	] {
		if let Ok(value) = HeaderValue::try_from(value) {
			headers.insert(name, value);
		}
	}
}

fn grpc_auth_error(status: &'static str, message: &'static str, authenticate: bool) -> Response {
	let mut response = Response::new(Body::empty());
	let headers = response.headers_mut();
	headers.insert(header::CONTENT_TYPE, axum::http::HeaderValue::from_static("application/grpc"));
	headers.insert("grpc-status", axum::http::HeaderValue::from_static(status));
	headers.insert("grpc-message", axum::http::HeaderValue::from_static(message));
	headers.insert("vmon-code", axum::http::HeaderValue::from_static("unauthorized"));
	if authenticate {
		headers.insert(header::WWW_AUTHENTICATE, axum::http::HeaderValue::from_static("Bearer"));
	}
	response
}

pub fn is_public_path(method: &Method, path: &str) -> bool {
	*method == Method::GET && (path == "/healthz" || is_static_web_path(path))
}

fn is_static_web_path(path: &str) -> bool {
	!(path.starts_with("/v1")
		|| path.starts_with("/healthz")
		|| path.starts_with("/metrics")
		|| path.starts_with("/vmon.v1.")
		|| path == "/grpc")
}

/// Require administrator authority for every API without explicit tenant
/// scoping.
pub fn is_admin_path(path: &str) -> bool {
	if path == "/metrics"
		|| path.starts_with("/v1/mesh/")
		|| path.starts_with("/v1/templates/")
		|| path.starts_with("/v1/functions/")
		|| path.starts_with("/v1/apps/")
	{
		return true;
	}
	let Some(rpc) = path.strip_prefix("/vmon.v1.") else {
		return false;
	};
	let Some((service, method)) = rpc.split_once('/') else {
		return true;
	};
	match service {
		"SandboxService" => matches!(method, "Migrate" | "Snapshot" | "SnapshotFs"),
		"CredentialService" => false,
		_ => true,
	}
}

/// Whether a `/grpc` bridge connection is limited to non-admin RPCs: a TCP
/// connection authenticated with the client token only. UDS peer-cred
/// connections and admin bearers get the full surface.
pub(super) fn bridge_principal(
	state: &ApiState,
	headers: &axum::http::HeaderMap,
	query: Option<&str>,
) -> Principal {
	if state.transport == Transport::Unix {
		return Principal::local_admin();
	}
	let supplied = bearer_token(headers.get(header::AUTHORIZATION)).or_else(|| query_token(query));
	state
		.principal_for_token(supplied.as_deref())
		.ok()
		.flatten()
		.unwrap_or_else(Principal::local_admin)
}

pub fn bearer_token(header: Option<&axum::http::HeaderValue>) -> Option<String> {
	let header = header?.to_str().ok()?;
	let (scheme, value) = header.split_once(' ')?;
	if scheme.eq_ignore_ascii_case("bearer") && !value.trim().is_empty() {
		Some(value.trim().to_owned())
	} else {
		None
	}
}

fn request_bearer_token(request: &Request<Body>) -> Option<String> {
	bearer_token(request.headers().get(header::AUTHORIZATION))
		.or_else(|| websocket_query_token(request))
}

fn websocket_query_token(request: &Request<Body>) -> Option<String> {
	if request.method() != Method::GET {
		return None;
	}
	if !is_query_token_websocket_path(request.uri().path()) {
		return None;
	}
	if !request
		.headers()
		.get(header::UPGRADE)
		.and_then(|value| value.to_str().ok())
		.is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
	{
		return None;
	}
	query_token(request.uri().query())
}

fn is_query_token_websocket_path(path: &str) -> bool {
	if path == "/grpc" {
		return true;
	}
	let segments = path.trim_start_matches('/').split('/').collect::<Vec<_>>();
	matches!(
		segments.as_slice(),
		["v1", "sandboxes", id, "ports", port, "ws"]
		| ["v1", "sandboxes", id, "ports", port, "ws", ..]
			if !id.is_empty() && !port.is_empty()
	)
}

pub fn query_token(query: Option<&str>) -> Option<String> {
	for pair in query?.split('&') {
		let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
		if matches!(key, "token" | "access_token") && !value.is_empty() {
			return Some(percent_decode_query(value));
		}
	}
	None
}

fn percent_decode_query(value: &str) -> String {
	let bytes = value.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut index = 0;
	while index < bytes.len() {
		match bytes[index] {
			b'%' if index + 2 < bytes.len() => {
				if let Some(decoded) = hex_pair(bytes[index + 1], bytes[index + 2]) {
					out.push(decoded);
					index += 3;
				} else {
					out.push(bytes[index]);
					index += 1;
				}
			},
			b'+' => {
				out.push(b' ');
				index += 1;
			},
			byte => {
				out.push(byte);
				index += 1;
			},
		}
	}
	String::from_utf8_lossy(&out).into_owned()
}

fn hex_pair(high: u8, low: u8) -> Option<u8> {
	Some(hex_value(high)? << 4 | hex_value(low)?)
}

const fn hex_value(byte: u8) -> Option<u8> {
	match byte {
		b'0'..=b'9' => Some(byte - b'0'),
		b'a'..=b'f' => Some(byte - b'a' + 10),
		b'A'..=b'F' => Some(byte - b'A' + 10),
		_ => None,
	}
}

#[cfg(test)]
#[allow(
	clippy::items_after_test_module,
	reason = "cfg-specific peer credential helpers stay outside the tests module"
)]
mod tests {
	use super::*;

	fn request(method: Method, uri: &str, upgrade: bool) -> Request<Body> {
		let mut builder = Request::builder().method(method).uri(uri);
		if upgrade {
			builder = builder.header(header::UPGRADE, "websocket");
		}
		builder.body(Body::empty()).expect("test request builds")
	}

	#[test]
	fn query_token_auth_is_limited_to_get_websocket_routes() {
		let bridge = request(Method::GET, "/grpc?token=a%20b", true);
		assert_eq!(request_bearer_token(&bridge), Some("a b".to_owned()));

		let bridge_access = request(Method::GET, "/grpc?access_token=tok", true);
		assert_eq!(request_bearer_token(&bridge_access), Some("tok".to_owned()));

		let port_ws = request(Method::GET, "/v1/sandboxes/sb/ports/8080/ws/path?token=tok", true);
		assert_eq!(request_bearer_token(&port_ws), Some("tok".to_owned()));

		let post_bridge = request(Method::POST, "/grpc?token=tok", true);
		assert_eq!(request_bearer_token(&post_bridge), None);

		let upgraded_rpc = request(Method::GET, "/vmon.v1.SystemService/Info?token=tok", true);
		assert_eq!(request_bearer_token(&upgraded_rpc), None);

		let non_upgrade_bridge = request(Method::GET, "/grpc?token=tok", false);
		assert_eq!(request_bearer_token(&non_upgrade_bridge), None);
	}

	#[test]
	fn unscoped_apis_are_admin_only_by_default() {
		for path in [
			"/metrics",
			"/v1/functions/acme/run/invoke",
			"/v1/apps/acme/app/functions/run/invoke",
			"/vmon.v1.ArtifactService/Get",
			"/vmon.v1.FunctionService/Get",
			"/vmon.v1.CallService/Create",
			"/vmon.v1.ActorService/Create",
			"/vmon.v1.SnapshotService/List",
			"/vmon.v1.VolumeService/List",
			"/vmon.v1.PoolService/List",
			"/vmon.v1.SystemService/Info",
			"/vmon.v1.SandboxService/Migrate",
			"/vmon.v1.SandboxService/Snapshot",
			"/vmon.v1.SandboxService/SnapshotFs",
			"/vmon.v1.FutureService/NewMethod",
		] {
			assert!(is_admin_path(path), "{path}");
		}
		for path in [
			"/vmon.v1.SandboxService/Create",
			"/vmon.v1.SandboxService/Get",
			"/vmon.v1.SandboxService/History",
			"/vmon.v1.CredentialService/List",
			"/vmon.v1.CredentialService/Put",
			"/vmon.v1.CredentialService/Delete",
			"/v1/sandboxes/sb/ports/8080",
		] {
			assert!(!is_admin_path(path), "{path}");
		}
	}
}

#[cfg(target_os = "linux")]
fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
	let mut cred = libc::ucred { pid: 0, uid: 0, gid: 0 };
	let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
	// SAFETY: all pointers reference initialized stack storage valid for the call;
	// `stream.as_raw_fd()` is an open socket descriptor.
	let rc = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_PEERCRED,
			std::ptr::addr_of_mut!(cred).cast(),
			std::ptr::addr_of_mut!(len),
		)
	};
	if rc == 0 {
		Ok(cred.uid)
	} else {
		Err(io::Error::last_os_error())
	}
}

#[cfg(target_os = "macos")]
fn peer_uid(stream: &UnixStream) -> io::Result<u32> {
	let mut euid: libc::uid_t = 0;
	let mut egid: libc::gid_t = 0;
	// SAFETY: `euid` and `egid` are valid out-pointers and `stream.as_raw_fd()` is
	// an open socket descriptor.
	let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut euid, &mut egid) };
	if rc == 0 {
		Ok(euid)
	} else {
		Err(io::Error::last_os_error())
	}
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn peer_uid(_stream: &UnixStream) -> io::Result<u32> {
	Err(io::Error::new(
		io::ErrorKind::Unsupported,
		"peer credentials are unsupported on this platform",
	))
}
