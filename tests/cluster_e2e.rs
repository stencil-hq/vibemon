//! Real-VM mesh/cluster acceptance suite.
//!
//! Port of `python/tests/test_cluster_e2e.py` and
//! `python/tests/test_sdk_cluster_e2e.py` onto the Rust v1 API. These tests are
//! intentionally gated: they boot real VMs, start multiple `vmon serve` nodes,
//! and kill/partition real gateway processes.

mod common;

use std::{
	collections::HashSet,
	fs,
	io::{Read, Write},
	net::{SocketAddr, TcpListener, TcpStream},
	os::unix::{fs::DirBuilderExt, process::CommandExt},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	sync::{
		Arc, Mutex,
		atomic::{AtomicBool, Ordering},
	},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};
use tonic::{Code, Request};
use vmon_proto::v1 as pb;

const LOOPBACK: &str = "127.0.0.1";
const REPLICA_TIMEOUT: Duration = Duration::from_mins(2);
const RESTORE_TIMEOUT: Duration = Duration::from_mins(3);
const FAST_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_secs(2);
const EXTRA_PATH: &str =
	"/opt/homebrew/bin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/bin:/usr/sbin:/sbin";

fn require_cluster_e2e() -> bool {
	if std::env::var("VMON_CLUSTER_E2E").as_deref() != Ok("1") {
		eprintln!("SKIP cluster_e2e: set VMON_CLUSTER_E2E=1 to run real cluster e2e tests");
		return false;
	}
	if !common::require_hv() {
		eprintln!("SKIP cluster_e2e: set VMON_E2E=1 on a host with KVM/HVF");
		return false;
	}
	for tool in ["skopeo", "umoci", "mkfs.ext4"] {
		if !have_tool(tool) {
			eprintln!("SKIP cluster_e2e: {tool} not found on PATH");
			return false;
		}
	}
	true
}

fn tool_path() -> String {
	let inherited = std::env::var("PATH").unwrap_or_default();
	format!("{EXTRA_PATH}:{inherited}")
}

fn have_tool(name: &str) -> bool {
	tool_path()
		.split(':')
		.any(|dir| !dir.is_empty() && Path::new(dir).join(name).is_file())
}

fn e2e_image() -> String {
	std::env::var("VMON_E2E_IMAGE").unwrap_or_else(|_| "alpine:latest".to_owned())
}

fn cached_kernel() -> Option<PathBuf> {
	let name = if cfg!(target_arch = "aarch64") {
		"Image-aarch64"
	} else {
		"bzImage-x86_64"
	};
	let home = std::env::var("HOME").ok()?;
	let path = Path::new(&home).join(".vmon/assets").join(name);
	path.is_file().then_some(path)
}

fn unique(prefix: &str) -> String {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system clock before epoch")
		.as_nanos();
	format!("{prefix}-{}-{nanos}", std::process::id())
}

fn short_home(prefix: &str) -> PathBuf {
	let path = PathBuf::from(format!("/tmp/{}{pid}", prefix, pid = std::process::id()));
	let _ = fs::remove_dir_all(&path);
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(&path)
		.unwrap_or_else(|err| panic!("creating {}: {err}", path.display()));
	path
}

fn free_port() -> u16 {
	TcpListener::bind((LOOPBACK, 0))
		.expect("bind ephemeral port")
		.local_addr()
		.expect("ephemeral addr")
		.port()
}

#[derive(Clone)]
struct FaultProxy {
	listen_port: u16,
	target_port: u16,
	state:       Arc<ProxyState>,
}

struct ProxyState {
	running:         AtomicBool,
	blocked:         AtomicBool,
	stop:            AtomicBool,
	blocked_sources: Mutex<HashSet<String>>,
	active:          Mutex<Vec<TcpStream>>,
}

impl FaultProxy {
	fn new(listen_port: u16, target_port: u16) -> Self {
		Self {
			listen_port,
			target_port,
			state: Arc::new(ProxyState {
				running:         AtomicBool::new(false),
				blocked:         AtomicBool::new(false),
				stop:            AtomicBool::new(false),
				blocked_sources: Mutex::new(HashSet::new()),
				active:          Mutex::new(Vec::new()),
			}),
		}
	}

	/// Idempotent: a restart of the node process must not rebind a proxy whose
	/// accept loop is still alive (Python harness parity).
	fn start(&self) {
		if self.state.running.swap(true, Ordering::SeqCst) {
			return;
		}
		self.state.stop.store(false, Ordering::SeqCst);
		let listener = TcpListener::bind((LOOPBACK, self.listen_port))
			.unwrap_or_else(|err| panic!("binding fault proxy {}: {err}", self.listen_port));
		listener
			.set_nonblocking(true)
			.expect("set proxy listener nonblocking");
		let target_port = self.target_port;
		let state = self.state.clone();
		thread::spawn(move || {
			accept_proxy(listener, target_port, &state);
			state.running.store(false, Ordering::SeqCst);
		});
	}

	fn partition(&self, enabled: bool) {
		self.state.blocked.store(enabled, Ordering::SeqCst);
		if enabled {
			self.sever();
		}
	}

	fn block_source(&self, node_id: &str, enabled: bool) {
		{
			let mut blocked = self
				.state
				.blocked_sources
				.lock()
				.expect("blocked sources lock");
			if enabled {
				blocked.insert(node_id.to_owned());
			} else {
				blocked.remove(node_id);
			}
		}
		if enabled {
			self.sever();
		}
	}

	fn close(&self) {
		self.state.stop.store(true, Ordering::SeqCst);
		self.sever();
		let _ = TcpStream::connect((LOOPBACK, self.listen_port));
	}

	fn sever(&self) {
		let streams = std::mem::take(&mut *self.state.active.lock().expect("active streams lock"));
		for stream in streams {
			let _ = stream.shutdown(std::net::Shutdown::Both);
		}
	}
}

fn accept_proxy(listener: TcpListener, target_port: u16, state: &Arc<ProxyState>) {
	while !state.stop.load(Ordering::SeqCst) {
		match listener.accept() {
			Ok((client, _)) => {
				// macOS/BSD: accepted sockets inherit the listener's
				// non-blocking mode; the handler expects blocking reads with
				// timeouts.
				let _ = client.set_nonblocking(false);
				let state = state.clone();
				thread::spawn(move || handle_proxy_client(client, target_port, state));
			},
			Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
				thread::sleep(Duration::from_millis(50));
			},
			Err(_) => return,
		}
	}
}

fn handle_proxy_client(mut client: TcpStream, target_port: u16, state: Arc<ProxyState>) {
	let first = match read_initial_request(&mut client) {
		Ok(first) if !first.is_empty() => first,
		_ => return,
	};
	if state.blocked.load(Ordering::SeqCst) {
		return;
	}
	let path = request_path(&first);
	let body = request_body(&first);
	if let Some(source) = request_source(&path, body) {
		let blocked = state.blocked_sources.lock().expect("blocked sources lock");
		if blocked.contains("*") || blocked.contains(&source) {
			return;
		}
	}
	let Ok(mut backend) = TcpStream::connect((LOOPBACK, target_port)) else {
		return;
	};
	if backend.write_all(&first).is_err() {
		return;
	}
	if let Ok(clone) = client.try_clone() {
		state
			.active
			.lock()
			.expect("active streams lock")
			.push(clone);
	}
	if let Ok(clone) = backend.try_clone() {
		state
			.active
			.lock()
			.expect("active streams lock")
			.push(clone);
	}
	// Mirror the Python harness: long requests (cold image import + first
	// boot) can sit silent for minutes before the response, so replace the
	// 2s initial-request timeout before piping.
	let _ = client.set_read_timeout(Some(Duration::from_mins(5)));
	let mut client_to_backend = match client.try_clone() {
		Ok(client_clone) => match backend.try_clone() {
			Ok(backend_clone) => Some((client_clone, backend_clone)),
			Err(_) => None,
		},
		Err(_) => None,
	};
	let left = client_to_backend.take().map(|(mut src, mut dst)| {
		thread::spawn(move || {
			let _ = std::io::copy(&mut src, &mut dst);
			// Propagate half-close so a `Connection: close` client sees EOF
			// as soon as the peer finishes, not when a timeout fires.
			let _ = dst.shutdown(std::net::Shutdown::Write);
		})
	});
	let right = thread::spawn(move || {
		let _ = std::io::copy(&mut backend, &mut client);
		let _ = client.shutdown(std::net::Shutdown::Write);
	});
	if let Some(left) = left {
		let _ = left.join();
	}
	let _ = right.join();
}

fn read_initial_request(client: &mut TcpStream) -> std::io::Result<Vec<u8>> {
	client.set_read_timeout(Some(Duration::from_secs(2)))?;
	let mut chunks = Vec::new();
	let mut buf = vec![0_u8; 65_536];
	let mut header_end = None;
	let mut content_length = 0_usize;
	while chunks.len() < 1_048_576 {
		let n = client.read(&mut buf)?;
		if n == 0 {
			break;
		}
		chunks.extend_from_slice(&buf[..n]);
		if header_end.is_none()
			&& let Some(pos) = chunks.windows(4).position(|window| window == b"\r\n\r\n")
		{
			header_end = Some(pos + 4);
			content_length = content_length_from_head(&chunks[..pos]);
		}
		if let Some(end) = header_end
			&& chunks.len() >= end + content_length
		{
			break;
		}
	}
	Ok(chunks)
}

fn content_length_from_head(head: &[u8]) -> usize {
	String::from_utf8_lossy(head)
		.lines()
		.find_map(|line| {
			let (name, value) = line.split_once(':')?;
			name
				.eq_ignore_ascii_case("content-length")
				.then(|| value.trim().parse().ok())
				.flatten()
		})
		.unwrap_or(0)
}

fn request_path(data: &[u8]) -> String {
	let line = String::from_utf8_lossy(data)
		.lines()
		.next()
		.unwrap_or_default()
		.to_owned();
	line
		.split_whitespace()
		.nth(1)
		.unwrap_or_default()
		.to_owned()
}

fn request_body(data: &[u8]) -> &[u8] {
	data
		.windows(4)
		.position(|window| window == b"\r\n\r\n")
		.map_or(&[], |index| &data[index + 4..])
}

fn request_source(path: &str, body: &[u8]) -> Option<String> {
	if !path.starts_with("/v1/mesh/") {
		return None;
	}
	let payload = serde_json::from_slice::<Value>(body).ok()?;
	let object = payload.as_object()?;
	if let Some(node_id) = object
		.get("state")
		.and_then(Value::as_object)
		.and_then(|state| state.get("node_id"))
		.and_then(Value::as_str)
	{
		return Some(node_id.to_owned());
	}
	for field in ["holder_node", "source_node", "node_id", "owner"] {
		if let Some(value) = object.get(field).and_then(Value::as_str)
			&& !value.is_empty()
		{
			return Some(value.to_owned());
		}
	}
	None
}

struct NodeProc {
	name:         String,
	port:         u16,
	home:         PathBuf,
	token:        String,
	log_path:     PathBuf,
	backend_port: u16,
	proxy:        Option<FaultProxy>,
	proc:         Option<Child>,
	node_id:      Option<String>,
}

impl NodeProc {
	fn start(&mut self) {
		assert!(
			self
				.proc
				.as_mut()
				.and_then(|child| child.try_wait().ok())
				.flatten()
				.is_some()
				|| self.proc.is_none(),
			"{} already running",
			self.name
		);
		fs::create_dir_all(&self.home)
			.unwrap_or_else(|err| panic!("creating {}: {err}", self.home.display()));
		if let Some(proxy) = &self.proxy {
			proxy.start();
		}
		let log = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&self.log_path)
			.unwrap_or_else(|err| panic!("opening {}: {err}", self.log_path.display()));
		let mut command = Command::new(env!("CARGO_BIN_EXE_vmon"));
		command
			.arg("serve")
			.arg("--home")
			.arg(&self.home)
			.arg("--host")
			.arg(LOOPBACK)
			.arg("--port")
			.arg(self.backend_port.to_string())
			.arg("--token")
			.arg(&self.token)
			.arg("--idle-timeout")
			.arg("900")
			.env("PATH", tool_path())
			.env("VMON_API_TOKEN", &self.token)
			.env("VMON_HOME", &self.home)
			.env("VMON_REPLICAS", "1")
			.env("VMON_REPLICATE_SEC", "5")
			.env("VMON_MESH_HEARTBEAT_SEC", "0.5")
			.env("VMON_MESH_REAP_SEC", "2.0")
			.env("VMON_MESH_W_LOCAL", "1000000")
			.stdin(Stdio::null())
			.stdout(log.try_clone().expect("clone node log"))
			.stderr(log)
			.process_group(0);
		if let Some(kernel) = cached_kernel() {
			command.env("VMON_KERNEL", kernel);
		}
		self.apply_env_overrides(&mut command);
		let child = command
			.spawn()
			.unwrap_or_else(|err| panic!("spawning {}: {err}", self.name));
		self.proc = Some(child);
		self.wait_healthy(Duration::from_mins(1));
	}

	fn apply_env_overrides(&self, command: &mut Command) {
		let env_file = self.home.join("cluster-e2e-env.json");
		if let Ok(bytes) = fs::read(&env_file)
			&& let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&bytes)
		{
			for (key, value) in map {
				if let Some(text) = value.as_str() {
					command.env(key, text);
				}
			}
		}
	}

	fn wait_healthy(&mut self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		loop {
			// Python-harness parity: startup readiness probes the backend
			// directly so proxy faults never gate node startup.
			if self
				.try_backend_api_json("GET", "/healthz", None, &[], Duration::from_secs(2))
				.is_ok()
			{
				return;
			}
			let exited = self
				.proc
				.as_mut()
				.and_then(|child| child.try_wait().ok())
				.flatten();
			if let Some(status) = exited {
				panic!("{} exited with {status}; log tail:\n{}", self.name, self.log_tail());
			}
			assert!(
				Instant::now() < deadline,
				"{} never became healthy; log tail:\n{}",
				self.name,
				self.log_tail()
			);
			thread::sleep(Duration::from_millis(250));
		}
	}

	fn restart(&mut self) {
		self.stop_process(libc::SIGTERM, Duration::from_secs(20));
		self.proc = None;
		self.start();
	}

	fn kill_hard(&mut self) {
		self.stop_process(libc::SIGKILL, Duration::from_secs(20));
		if let Some(proxy) = &self.proxy {
			proxy.sever();
		}
	}

	fn stop(&mut self) {
		self.stop_process(libc::SIGTERM, Duration::from_secs(20));
		if let Some(proxy) = &self.proxy {
			proxy.close();
		}
	}

	fn stop_process(&mut self, signal: i32, timeout: Duration) {
		let Some(child) = &mut self.proc else {
			return;
		};
		if child.try_wait().ok().flatten().is_some() {
			return;
		}
		let pgid = child.id() as i32;
		// SAFETY: the child was spawned in its own process group; negative pgid targets
		// that group.
		unsafe {
			libc::kill(-pgid, signal);
		}
		let deadline = Instant::now() + timeout;
		while Instant::now() < deadline {
			if child.try_wait().ok().flatten().is_some() {
				return;
			}
			thread::sleep(Duration::from_millis(100));
		}
		// SAFETY: the child was spawned in its own process group; negative pgid targets
		// that group.
		unsafe {
			libc::kill(-pgid, libc::SIGKILL);
		}
		let _ = child.wait();
	}

	fn url(&self) -> String {
		format!("http://{LOOPBACK}:{}", self.port)
	}

	/// Fresh gRPC channel through the public (fault-proxied) port.
	fn grpc(&self) -> Result<common::api::Grpc, String> {
		common::api::Grpc::connect_tcp(self.port, Some(&self.token))
	}

	/// Fresh gRPC channel straight to the backend, bypassing the fault proxy.
	fn grpc_backend(&self) -> Result<common::api::Grpc, String> {
		common::api::Grpc::connect_tcp(self.backend_port, Some(&self.token))
	}

	fn api_json(
		&self,
		method: &str,
		path: &str,
		payload: Option<&Value>,
		headers: &[(&str, &str)],
		timeout: Duration,
	) -> Value {
		self
			.try_api_json(method, path, payload, headers, timeout)
			.unwrap_or_else(|err| {
				panic!(
					"{} {path} via {} failed: {err}\nlog tail:\n{}",
					method,
					self.name,
					self.log_tail()
				)
			})
	}

	fn try_backend_api_json(
		&self,
		method: &str,
		path: &str,
		payload: Option<&Value>,
		headers: &[(&str, &str)],
		timeout: Duration,
	) -> Result<Value, String> {
		self.try_api_json_on_port(self.backend_port, method, path, payload, headers, timeout)
	}

	fn try_api_json(
		&self,
		method: &str,
		path: &str,
		payload: Option<&Value>,
		headers: &[(&str, &str)],
		timeout: Duration,
	) -> Result<Value, String> {
		self.try_api_json_on_port(self.port, method, path, payload, headers, timeout)
	}

	fn try_api_json_on_port(
		&self,
		port: u16,
		method: &str,
		path: &str,
		payload: Option<&Value>,
		headers: &[(&str, &str)],
		timeout: Duration,
	) -> Result<Value, String> {
		let body = payload.map(Value::to_string);
		let (status, raw) = http_json(
			SocketAddr::from(([127, 0, 0, 1], port)),
			method,
			path,
			&self.token,
			body.as_deref(),
			headers,
			timeout,
		)?;
		if !(200..300).contains(&status) {
			return Err(format!("HTTP {status}: {}", String::from_utf8_lossy(&raw)));
		}
		parse_json_body(&raw).map_err(|err| format!("invalid JSON body {raw:?}: {err}"))
	}

	fn log_tail(&self) -> String {
		let text = fs::read_to_string(&self.log_path).unwrap_or_default();
		let lines = text.lines().collect::<Vec<_>>();
		let start = lines.len().saturating_sub(80);
		lines[start..].join("\n")
	}
}

impl Drop for NodeProc {
	fn drop(&mut self) {
		self.stop();
	}
}

fn start_node(
	home: PathBuf,
	port: u16,
	env_overrides: &[(&str, &str)],
	name: &str,
	token: &str,
	fault_proxy: bool,
) -> NodeProc {
	let backend_port = if fault_proxy {
		// The listener released by `free_port()` can be handed right back for
		// the next ephemeral bind; a proxy forwarding to itself never serves.
		loop {
			let candidate = free_port();
			if candidate != port {
				break candidate;
			}
		}
	} else {
		port
	};
	let proxy = fault_proxy.then(|| FaultProxy::new(port, backend_port));
	let env_map = env_overrides
		.iter()
		.map(|(key, value)| ((*key).to_owned(), Value::String((*value).to_owned())))
		.collect::<serde_json::Map<_, _>>();
	let env_file = home.join("cluster-e2e-env.json");
	let log_path = home.join("node.log");
	fs::write(&env_file, Value::Object(env_map).to_string())
		.unwrap_or_else(|err| panic!("writing {}: {err}", env_file.display()));
	let mut node = NodeProc {
		name: name.to_owned(),
		port,
		home,
		token: token.to_owned(),
		log_path,
		backend_port,
		proxy,
		proc: None,
		node_id: None,
	};
	node.start();
	node
}

fn http_json(
	addr: SocketAddr,
	method: &str,
	path: &str,
	token: &str,
	body: Option<&str>,
	extra_headers: &[(&str, &str)],
	timeout: Duration,
) -> Result<(u16, Vec<u8>), String> {
	let mut stream = TcpStream::connect_timeout(&addr, timeout).map_err(|err| err.to_string())?;
	stream
		.set_read_timeout(Some(timeout))
		.map_err(|err| err.to_string())?;
	stream
		.set_write_timeout(Some(timeout))
		.map_err(|err| err.to_string())?;
	let payload = body.unwrap_or_default();
	let mut request = format!(
		"{method} {path} HTTP/1.1\r\nHost: {LOOPBACK}\r\nAuthorization: Bearer \
		 {token}\r\nConnection: close\r\n"
	);
	for (key, value) in extra_headers {
		request.push_str(key);
		request.push_str(": ");
		request.push_str(value);
		request.push_str("\r\n");
	}
	if body.is_some() {
		request.push_str("Content-Type: application/json\r\n");
		request.push_str("Content-Length: ");
		request.push_str(&payload.len().to_string());
		request.push_str("\r\n");
	}
	request.push_str("\r\n");
	stream
		.write_all(request.as_bytes())
		.map_err(|err| err.to_string())?;
	if body.is_some() {
		stream
			.write_all(payload.as_bytes())
			.map_err(|err| err.to_string())?;
	}
	let mut response = Vec::new();
	stream
		.read_to_end(&mut response)
		.map_err(|err| err.to_string())?;
	let split = response
		.windows(4)
		.position(|window| window == b"\r\n\r\n")
		.ok_or_else(|| "response had no header terminator".to_owned())?;
	let head = String::from_utf8_lossy(&response[..split]).into_owned();
	let status = head
		.split_whitespace()
		.nth(1)
		.and_then(|value| value.parse::<u16>().ok())
		.ok_or_else(|| format!("bad status line: {head}"))?;
	let mut body = response[split + 4..].to_vec();
	if head.lines().any(|line| {
		line.to_ascii_lowercase().starts_with("transfer-encoding:")
			&& line.to_ascii_lowercase().contains("chunked")
	}) {
		body = decode_chunked(&body)?;
	}
	Ok((status, body))
}

fn decode_chunked(mut body: &[u8]) -> Result<Vec<u8>, String> {
	let mut out = Vec::new();
	loop {
		let line_end = body
			.windows(2)
			.position(|window| window == b"\r\n")
			.ok_or_else(|| "malformed chunked response".to_owned())?;
		let size_text = String::from_utf8_lossy(&body[..line_end]);
		let size = usize::from_str_radix(size_text.split(';').next().unwrap_or_default().trim(), 16)
			.map_err(|err| format!("bad chunk size {size_text:?}: {err}"))?;
		body = &body[line_end + 2..];
		if size == 0 {
			return Ok(out);
		}
		if body.len() < size + 2 {
			return Err("truncated chunked response".to_owned());
		}
		out.extend_from_slice(&body[..size]);
		body = &body[size + 2..];
	}
}

fn parse_json_body(raw: &[u8]) -> Result<Value, serde_json::Error> {
	if raw.is_empty() {
		Ok(Value::Object(Default::default()))
	} else {
		serde_json::from_slice(raw)
	}
}

fn eventually<T, F>(description: &str, timeout: Duration, interval: Duration, mut f: F) -> T
where
	F: FnMut() -> Option<T>,
{
	let deadline = Instant::now() + timeout;
	let mut attempts = 0_u64;
	loop {
		attempts += 1;
		if let Some(value) = f() {
			return value;
		}
		assert!(
			Instant::now() < deadline,
			"timed out waiting for {description} after {attempts} attempts"
		);
		thread::sleep(interval);
	}
}

fn mesh_setup(node: &mut NodeProc) -> Value {
	let response = node.api_json(
		"POST",
		"/v1/mesh/setup",
		Some(&json!({"advertise": node.url(), "max_vcpus": 64, "max_mem_mib": 65_536})),
		&[],
		FAST_TIMEOUT,
	);
	node.node_id = response
		.get("node_id")
		.and_then(Value::as_str)
		.map(ToOwned::to_owned);
	response
}

fn mesh_join(node: &mut NodeProc, blob: &str) -> Value {
	let response = node.api_json(
		"POST",
		"/v1/mesh/join",
		Some(&json!({"blob": blob, "advertise": node.url()})),
		&[],
		FAST_TIMEOUT,
	);
	node.node_id = response
		.get("node_id")
		.and_then(Value::as_str)
		.map(ToOwned::to_owned);
	response
}

fn mesh_leave(node: &NodeProc, drain: bool) -> Value {
	node.api_json("POST", "/v1/mesh/leave", Some(&json!({"drain": drain})), &[], FAST_TIMEOUT)
}

fn wait_healthy(nodes: &[&NodeProc], timeout: Duration) -> Vec<Value> {
	eventually("mesh nodes to see each other healthy", timeout, POLL_INTERVAL, || {
		let ids = nodes
			.iter()
			.map(|node| node.node_id.as_deref().expect("node has joined mesh"))
			.collect::<Vec<_>>();
		let statuses = nodes
			.iter()
			.map(|node| {
				node
					.try_api_json("GET", "/v1/mesh/status", None, &[], Duration::from_secs(5))
					.ok()
			})
			.collect::<Option<Vec<_>>>()?;
		for (status, my_id) in statuses.iter().zip(&ids) {
			if status.get("enabled").and_then(Value::as_bool) != Some(true) {
				return None;
			}
			for other in &ids {
				if other != my_id && !peer_seen(status, other) {
					return None;
				}
			}
		}
		Some(statuses)
	})
}

fn peer_seen(status: &Value, peer_id: &str) -> bool {
	status
		.get("peers")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
		.any(|peer| {
			peer.get("node_id").and_then(Value::as_str) == Some(peer_id)
				&& peer
					.get("last_seen")
					.and_then(Value::as_f64)
					.is_some_and(|seen| seen > 0.0)
		})
}

fn sandbox_row(listing: &Value, sid: &str) -> Option<Value> {
	listing
		.get("sandboxes")
		.and_then(Value::as_array)?
		.iter()
		.find(|item| {
			item.get("id").and_then(Value::as_str) == Some(sid)
				|| item.get("name").and_then(Value::as_str) == Some(sid)
		})
		.cloned()
}

/// `GET /v1/sandboxes` equivalent: list via gRPC and rebuild the JSON listing
/// shape (`{"sandboxes": [...]}`) the assertions consume.
fn try_list_sandboxes(node: &NodeProc) -> Result<Value, String> {
	list_sandboxes_via(node.grpc()?, false)
}

/// Listing against the backend port with the mesh-hop marker, so a
/// partitioned node answers locally instead of proxying.
fn try_list_sandboxes_backend(node: &NodeProc) -> Result<Value, String> {
	list_sandboxes_via(node.grpc_backend()?, true)
}

fn list_sandboxes_via(grpc: common::api::Grpc, mesh_hop: bool) -> Result<Value, String> {
	let mut sandboxes = grpc.sandboxes();
	let mut request = Request::new(pb::ListSandboxesRequest { tags: Vec::new() });
	if mesh_hop {
		request
			.metadata_mut()
			.insert("x-vmon-mesh-hop", "1".parse().expect("static metadata value"));
	}
	let response = grpc
		.block_on(sandboxes.list(request))
		.map_err(|status| common::api::status_detail(&status))?
		.into_inner();
	let rows = response
		.sandboxes_json
		.iter()
		.map(|row| serde_json::from_str::<Value>(row))
		.collect::<Result<Vec<_>, _>>()
		.map_err(|err| format!("invalid sandbox view JSON: {err}"))?;
	Ok(json!({ "sandboxes": rows }))
}

/// Check one node's local engine without following mesh ownership.
fn local_sandbox_exists(node: &NodeProc, sid: &str) -> Result<bool, String> {
	let grpc = node.grpc_backend()?;
	let mut sandboxes = grpc.sandboxes();
	let mut request = Request::new(pb::SandboxRef { id: sid.to_owned() });
	request
		.metadata_mut()
		.insert("x-vmon-mesh-hop", "1".parse().expect("static metadata value"));
	match grpc.block_on(sandboxes.get(request)) {
		Ok(_) => Ok(true),
		Err(status) if status.code() == Code::NotFound => Ok(false),
		Err(status) => Err(common::api::status_detail(&status)),
	}
}

fn remove_sandbox_best_effort(nodes: &[&NodeProc], sid: &str) {
	for node in nodes {
		let Ok(grpc) = node.grpc() else {
			continue;
		};
		let mut sandboxes = grpc.sandboxes();
		let _ = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: sid.to_owned() }));
	}
}

fn create_sandbox(node: &NodeProc, body: &Value) -> Result<Value, String> {
	let grpc = node.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(
			sandboxes
				.create(pb::CreateSandboxRequest { spec_json: body.to_string(), no_wait: false }),
		)
		.map_err(|status| common::api::status_detail(&status))?
		.into_inner();
	serde_json::from_str(&view.json).map_err(|err| format!("invalid create view JSON: {err}"))
}

fn replica_list(node: &NodeProc) -> Vec<String> {
	node
		.api_json("GET", "/v1/mesh/replica/list", None, &[], Duration::from_secs(5))
		.get("sids")
		.and_then(Value::as_array)
		.into_iter()
		.flatten()
		.filter_map(Value::as_str)
		.map(ToOwned::to_owned)
		.collect()
}

fn try_exec_capture(
	node: &NodeProc,
	sid: &str,
	cmd: &[&str],
	timeout: Duration,
) -> Result<(i64, String, String), String> {
	let grpc = node.grpc()?;
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   sid.to_owned(),
		exec: Some(pb::ExecStart {
			cmd: cmd.iter().map(|&part| part.to_owned()).collect(),
			timeout: Some(timeout.as_secs_f64()),
			..Default::default()
		}),
	};
	let response = grpc
		.block_on(sandboxes.exec_capture(request))
		.map_err(|status| {
			format!(
				"exec {cmd:?} via {} failed: {}\nlog tail:\n{}",
				node.name,
				common::api::status_detail(&status),
				node.log_tail()
			)
		})?
		.into_inner();
	Ok((
		response.code,
		String::from_utf8_lossy(&response.stdout).into_owned(),
		String::from_utf8_lossy(&response.stderr).into_owned(),
	))
}

fn exec_capture(
	node: &NodeProc,
	sid: &str,
	cmd: &[&str],
	timeout: Duration,
) -> (i64, String, String) {
	try_exec_capture(node, sid, cmd, timeout).unwrap_or_else(|error| panic!("{error}"))
}

fn eventually_guest_file(node: &NodeProc, sid: &str, path: &str, expected: &str) {
	let command = ["cat", path];
	let deadline = Instant::now() + Duration::from_secs(45);
	loop {
		match try_exec_capture(node, sid, &command, Duration::from_secs(15)) {
			Ok((0, stdout, _)) if stdout.contains(expected) => return,
			Ok((exit, stdout, stderr)) => {
				assert!(
					Instant::now() < deadline,
					"restored guest file did not contain replicated volume data: exit={exit} \
					 stdout={stdout:?} stderr={stderr:?}"
				);
			},
			Err(error) => {
				assert!(Instant::now() < deadline, "restored guest file remained unavailable: {error}");
			},
		}
		thread::sleep(POLL_INTERVAL);
	}
}

fn volume_lease(row: &Value, volume: &str) -> Value {
	row.get("volume_leases")
		.and_then(Value::as_object)
		.and_then(|leases| leases.get(volume))
		.cloned()
		.unwrap_or_else(|| panic!("missing lease for {volume} in row {row}"))
}

fn assert_volume_lease_non_overlap(leases: &[Value]) {
	let mut intervals = leases
		.iter()
		.map(|lease| {
			let start = lease
				.get("granted_at")
				.and_then(Value::as_f64)
				.expect("lease granted_at");
			let end = lease
				.get("renew_deadline")
				.or_else(|| lease.get("expires_at"))
				.and_then(Value::as_f64)
				.expect("lease end");
			(start, end, lease.clone())
		})
		.collect::<Vec<_>>();
	intervals.sort_by(|left, right| left.0.partial_cmp(&right.0).expect("lease timestamp order"));
	for window in intervals.windows(2) {
		let (left_start, left_end, left) = &window[0];
		let (right_start, _, right) = &window[1];
		assert!(left_start < left_end, "invalid lease interval: {left}");
		assert!(
			right_start >= left_end,
			"overlapping writable-volume leases: left={left}, right={right}"
		);
	}
}

fn replica_volume_payload(home: &Path, sid: &str, volume: &str, file: &str) -> Option<String> {
	let meta = fs::read_to_string(home.join("replicas").join(format!("{sid}.json"))).ok()?;
	let meta = serde_json::from_str::<Value>(&meta).ok()?;
	let snapshot_dir = meta.get("snapshot_dir").and_then(Value::as_str)?;
	fs::read_to_string(
		Path::new(snapshot_dir)
			.join("volumes")
			.join(volume)
			.join(file),
	)
	.ok()
	.map(|text| text.trim().to_owned())
}

fn skip_if_no_virtiofs(error: &str) -> bool {
	let lower = error.to_ascii_lowercase();
	if lower.contains("no such device") || lower.contains("enodev") || lower.contains("virtio-fs") {
		eprintln!("SKIP cluster_e2e: guest kernel lacks virtio-fs support ({error})");
		return true;
	}
	false
}

fn partition_node(node: &NodeProc, peers: &[&NodeProc], enabled: bool) {
	let proxy = node.proxy.as_ref().expect("partition requires fault proxy");
	let node_id = node.node_id.as_deref().expect("node must have joined mesh");
	proxy.partition(enabled);
	for peer in peers {
		if peer.name == node.name {
			continue;
		}
		if let Some(peer_proxy) = &peer.proxy {
			peer_proxy.block_source(node_id, enabled);
		}
	}
}

#[test]
fn two_node_failover_and_restore() {
	if !require_cluster_e2e() {
		return;
	}
	let token = unique("cluster-e2e");
	let home_a = short_home("vce-a-");
	let home_b = short_home("vce-b-");
	let sid = unique("ce");
	let mut gateway_a = start_node(home_a, free_port(), &[], "node-a", &token, false);
	let mut gateway_b = start_node(home_b, free_port(), &[], "node-b", &token, false);

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateway_a);
		let join = mesh_join(
			&mut gateway_b,
			setup
				.get("blob")
				.and_then(Value::as_str)
				.expect("join blob"),
		);
		let node_a = setup
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node a id")
			.to_owned();
		let node_b = join
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node b id")
			.to_owned();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));
		gateway_b.restart();
		gateway_a.restart();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));

		let row = create_sandbox(
			&gateway_a,
			&json!({
				"image": e2e_image(),
				"name": sid,
				"command": ["sleep", "600"],
				"block_network": true,
				"timeout_secs": 900,
				"idempotency_key": format!("{sid}-create"),
			}),
		)
		.unwrap_or_else(|err| panic!("creating failover sandbox failed: {err}"));
		assert_eq!(row.get("status").and_then(Value::as_str), Some("running"));
		let owner_node = row.get("node").and_then(Value::as_str).expect("owner node");
		let (owner, survivor, survivor_node) = if owner_node == node_a {
			(&mut gateway_a, &gateway_b, node_b.as_str())
		} else {
			(&mut gateway_b, &gateway_a, node_a.as_str())
		};
		eventually("survivor to hold a replica", REPLICA_TIMEOUT, POLL_INTERVAL, || {
			replica_list(survivor).contains(&sid).then_some(())
		});
		owner.kill_hard();
		assert!(
			owner
				.proc
				.as_mut()
				.and_then(|child| child.try_wait().ok())
				.flatten()
				.is_some(),
			"owner gateway did not stop"
		);
		let restored = eventually(
			"orphaned sandbox to restore on survivor",
			Duration::from_mins(2),
			POLL_INTERVAL,
			|| {
				let listing = try_list_sandboxes(survivor).ok()?;
				let row = sandbox_row(&listing, &sid)?;
				(row.get("node").and_then(Value::as_str) == Some(survivor_node)
					&& row.get("status").and_then(Value::as_str) == Some("running"))
				.then_some(row)
			},
		);
		assert_eq!(restored.get("node").and_then(Value::as_str), Some(survivor_node));

		let left = mesh_leave(survivor, false);
		assert_eq!(left.get("ok").and_then(Value::as_bool), Some(true), "mesh leave failed: {left}");
		let status = survivor.api_json("GET", "/v1/mesh/status", None, &[], FAST_TIMEOUT);
		assert_eq!(
			status.get("enabled").and_then(Value::as_bool),
			Some(false),
			"leave did not disable mesh: {status}"
		);
	}));
	remove_sandbox_best_effort(&[&gateway_a, &gateway_b], &sid);
	gateway_a.stop();
	gateway_b.stop();
	let _ = fs::remove_dir_all(&gateway_a.home);
	let _ = fs::remove_dir_all(&gateway_b.home);
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}

#[test]
fn two_node_readonly_volume_ha_materializes_on_survivor() {
	if !require_cluster_e2e() {
		return;
	}
	let token = unique("cluster-e2e");
	let home_a = short_home("vcv-a-");
	let home_b = short_home("vcv-b-");
	let sid = unique("vol-ha");
	let vol_name = unique("havol").replace('-', "_");
	let sentinel = "HA_VOLUME_OK";
	let seeded = home_a.join("volumes").join(&vol_name);
	fs::create_dir_all(&seeded).expect("create seeded volume");
	fs::write(seeded.join("sentinel.txt"), sentinel).expect("seed volume sentinel");
	let mut gateway_a = start_node(home_a, free_port(), &[], "vol-a", &token, false);
	let mut gateway_b = start_node(home_b, free_port(), &[], "vol-b", &token, false);

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateway_a);
		let join = mesh_join(
			&mut gateway_b,
			setup
				.get("blob")
				.and_then(Value::as_str)
				.expect("join blob"),
		);
		let node_a = setup
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node a id")
			.to_owned();
		let node_b = join
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node b id")
			.to_owned();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));
		gateway_b.restart();
		gateway_a.restart();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));

		let owner_row = match create_sandbox(
			&gateway_a,
			&json!({
				"image": e2e_image(),
				"name": sid,
				"block_network": true,
				"timeout_secs": 900,
				"volumes": {"/data": {"name": vol_name, "read_only": true}},
				"idempotency_key": format!("{sid}-create"),
			}),
		) {
			Ok(row) => row,
			Err(err) if skip_if_no_virtiofs(&err) => return,
			Err(err) => panic!("creating volume HA sandbox failed: {err}"),
		};
		assert_eq!(owner_row.get("status").and_then(Value::as_str), Some("running"));
		assert_eq!(owner_row.get("node").and_then(Value::as_str), Some(node_a.as_str()));
		eventually("node B to hold volume replica", REPLICA_TIMEOUT, POLL_INTERVAL, || {
			replica_list(&gateway_b).contains(&sid).then_some(())
		});
		gateway_a.kill_hard();
		let restored = eventually(
			"volume sandbox to restore on node B",
			Duration::from_mins(2),
			POLL_INTERVAL,
			|| {
				let listing = try_list_sandboxes(&gateway_b).ok()?;
				let row = sandbox_row(&listing, &sid)?;
				(row.get("node").and_then(Value::as_str) == Some(node_b.as_str())
					&& row.get("status").and_then(Value::as_str) == Some("running"))
				.then_some(row)
			},
		);
		assert_eq!(restored.get("node").and_then(Value::as_str), Some(node_b.as_str()));
		eventually_guest_file(&gateway_b, &sid, "/data/sentinel.txt", sentinel);
	}));
	remove_sandbox_best_effort(&[&gateway_b, &gateway_a], &sid);
	gateway_a.stop();
	gateway_b.stop();
	let _ = fs::remove_dir_all(&gateway_a.home);
	let _ = fs::remove_dir_all(&gateway_b.home);
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}

#[test]
fn two_node_migrate_moves_running_sandbox() {
	if !require_cluster_e2e() {
		return;
	}
	let token = unique("cluster-e2e");
	let home_a = short_home("vcm-a-");
	let home_b = short_home("vcm-b-");
	let sid = unique("mig");
	let ram_marker = format!("ram-{sid}");
	let disk_marker = format!("disk-{sid}");
	let secret_marker = format!("secret-{sid}");
	let mut gateway_a = start_node(home_a, free_port(), &[], "mig-a", &token, false);
	let mut gateway_b = start_node(home_b, free_port(), &[], "mig-b", &token, false);

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateway_a);
		let join = mesh_join(
			&mut gateway_b,
			setup
				.get("blob")
				.and_then(Value::as_str)
				.expect("join blob"),
		);
		let node_a = setup
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node a id")
			.to_owned();
		let node_b = join
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node b id")
			.to_owned();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));
		gateway_b.restart();
		gateway_a.restart();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));

		let row = create_sandbox(
			&gateway_a,
			&json!({
				"image": e2e_image(),
				"name": sid,
				"command": ["sleep", "600"],
				"block_network": true,
				"timeout_secs": 900,
				"ha": "off",
				"secrets": [{"name": "migration", "values": {"MIGRATION_SECRET": secret_marker}}],
				"idempotency_key": format!("{sid}-create"),
			}),
		)
		.unwrap_or_else(|err| panic!("creating migrate sandbox failed: {err}"));
		assert_eq!(row.get("status").and_then(Value::as_str), Some("running"));
		// Idempotent placement may pick either node; migrate from wherever the
		// sandbox landed to the other node.
		let owner_node = row
			.get("node")
			.and_then(Value::as_str)
			.expect("owner node")
			.to_owned();
		let (source, target, target_node) = if owner_node == node_a {
			(&gateway_a, &gateway_b, node_b.as_str())
		} else {
			(&gateway_b, &gateway_a, node_a.as_str())
		};

		let (exit, stdout, stderr) = exec_capture(
			source,
			&sid,
			&["/bin/sh", "-c", "printf %s \"$MIGRATION_SECRET\""],
			Duration::from_secs(30),
		);
		assert_eq!(exit, 0, "pre-migration secret read failed stdout={stdout:?} stderr={stderr:?}");
		assert_eq!(stdout, secret_marker, "secret binding was absent before migration");

		// One marker in guest RAM (tmpfs) and one on the writable rootfs: live
		// migration must ship both the final RAM delta and the disk block delta.
		let write_markers = format!(
			"mkdir -p /dev/shm && mount -t tmpfs tmpfs /dev/shm && printf %s '{ram_marker}' > \
			 /dev/shm/teleport && printf %s '{disk_marker}' > /root/teleport && sync"
		);
		let (exit, stdout, stderr) =
			exec_capture(source, &sid, &["/bin/sh", "-c", &write_markers], Duration::from_secs(30));
		assert_eq!(exit, 0, "writing markers failed stdout={stdout:?} stderr={stderr:?}");

		let migrated = {
			let grpc = target
				.grpc()
				.unwrap_or_else(|err| panic!("gRPC connect to {} failed: {err}", target.name));
			let mut sandboxes = grpc.sandboxes();
			let view =
				grpc
					.block_on(sandboxes.migrate(pb::MigrateRequest {
						id:     sid.clone(),
						target: target_node.to_owned(),
					}))
					.unwrap_or_else(|status| {
						panic!(
							"migrate via {} failed: {}\nlog tail:\n{}",
							target.name,
							common::api::status_detail(&status),
							target.log_tail()
						)
					})
					.into_inner();
			serde_json::from_str::<Value>(&view.json).expect("migrate view JSON")
		};
		assert_eq!(
			migrated.get("node").and_then(Value::as_str),
			Some(target_node),
			"migration response reported the wrong owner: {migrated}"
		);
		assert_eq!(
			migrated.get("status").and_then(Value::as_str),
			Some("running"),
			"migration response did not report a running sandbox: {migrated}"
		);
		let timing = migrated
			.get("migration")
			.unwrap_or_else(|| panic!("migrate response missing timing object: {migrated}"));
		let downtime_ms = timing
			.get("downtime_ms")
			.and_then(Value::as_u64)
			.expect("downtime_ms");
		let total_ms = timing
			.get("total_ms")
			.and_then(Value::as_u64)
			.expect("total_ms");
		assert!(downtime_ms > 0, "guest blackout must be measured: {timing}");
		assert!(downtime_ms <= total_ms, "downtime {downtime_ms}ms cannot exceed total {total_ms}ms");

		let moved =
			eventually("migrated sandbox to run on target", RESTORE_TIMEOUT, POLL_INTERVAL, || {
				let listing = try_list_sandboxes(target).ok()?;
				let row = sandbox_row(&listing, &sid)?;
				(row.get("node").and_then(Value::as_str) == Some(target_node)
					&& row.get("status").and_then(Value::as_str) == Some("running"))
				.then_some(row)
			});
		assert_eq!(moved.get("node").and_then(Value::as_str), Some(target_node));

		let (exit, stdout, stderr) =
			exec_capture(target, &sid, &["cat", "/dev/shm/teleport"], Duration::from_secs(30));
		assert_eq!(exit, 0, "RAM marker read failed stdout={stdout:?} stderr={stderr:?}");
		assert_eq!(stdout, ram_marker, "RAM marker changed across migration");
		let (exit, stdout, stderr) =
			exec_capture(target, &sid, &["cat", "/root/teleport"], Duration::from_secs(30));
		assert_eq!(exit, 0, "disk marker read failed stdout={stdout:?} stderr={stderr:?}");
		assert_eq!(stdout, disk_marker, "disk marker changed across migration");
		let (exit, stdout, stderr) = exec_capture(
			target,
			&sid,
			&["/bin/sh", "-c", "printf %s \"$MIGRATION_SECRET\""],
			Duration::from_secs(30),
		);
		assert_eq!(exit, 0, "secret read failed stdout={stdout:?} stderr={stderr:?}");
		assert_eq!(stdout, secret_marker, "secret binding changed across migration");

		eventually("source node to drop migrated sandbox", FAST_TIMEOUT, POLL_INTERVAL, || {
			matches!(local_sandbox_exists(source, &sid), Ok(false)).then_some(())
		});
	}));
	if result.is_err() {
		for gateway in [&gateway_a, &gateway_b] {
			eprintln!("{} log tail:\n{}", gateway.name, gateway.log_tail());
			eprintln!("{} listing: {:?}", gateway.name, try_list_sandboxes(gateway));
			let record = gateway.home.join("records").join(format!("{sid}.json"));
			eprintln!("{} record: {:?}", gateway.name, fs::read_to_string(record));
		}
	}
	remove_sandbox_best_effort(&[&gateway_a, &gateway_b], &sid);
	gateway_a.stop();
	gateway_b.stop();
	let _ = fs::remove_dir_all(&gateway_a.home);
	let _ = fs::remove_dir_all(&gateway_b.home);
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}

#[test]
fn three_node_writable_volume_quorum_ha() {
	if !require_cluster_e2e() {
		return;
	}
	let token = unique("cluster-e2e");
	let sid = unique("ceq");
	let vol_name = unique("qvol").replace('-', "_");
	let sentinel = "WRITABLE_QUORUM_OK";
	let env = [("VMON_RESTORE_QUORUM", "1"), ("VMON_REPLICAS", "2"), ("VMON_REPLICATE_SEC", "1")];
	let mut gateways = [
		start_node(short_home("vcq-a-"), free_port(), &env, "q-a", &token, false),
		start_node(short_home("vcq-b-"), free_port(), &env, "q-b", &token, false),
		start_node(short_home("vcq-c-"), free_port(), &env, "q-c", &token, false),
	];

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateways[0]);
		let mut node_ids = vec![
			setup
				.get("node_id")
				.and_then(Value::as_str)
				.expect("node a id")
				.to_owned(),
		];
		for gateway in gateways.iter_mut().skip(1) {
			let joined = mesh_join(
				gateway,
				setup
					.get("blob")
					.and_then(Value::as_str)
					.expect("join blob"),
			);
			node_ids.push(
				joined
					.get("node_id")
					.and_then(Value::as_str)
					.expect("joined id")
					.to_owned(),
			);
		}
		wait_healthy(&[&gateways[0], &gateways[1], &gateways[2]], Duration::from_mins(1));
		for gateway in &mut gateways {
			gateway.restart();
		}
		wait_healthy(&[&gateways[0], &gateways[1], &gateways[2]], Duration::from_mins(1));

		let created = match create_sandbox(
			&gateways[0],
			&json!({
				"image": e2e_image(),
				"name": sid,
				"block_network": true,
				"timeout_secs": 900,
				"volumes": {"/data": {"name": vol_name, "read_only": false}},
				"idempotency_key": format!("{sid}-create"),
			}),
		) {
			Ok(row) => row,
			Err(err) if skip_if_no_virtiofs(&err) => return,
			Err(err) => panic!("creating writable volume sandbox failed: {err}"),
		};
		assert_eq!(created.get("status").and_then(Value::as_str), Some("running"));
		assert_eq!(created.get("node").and_then(Value::as_str), Some(node_ids[0].as_str()));
		let (exit, stdout, stderr) = exec_capture(
			&gateways[0],
			&sid,
			&["sh", "-c", &format!("echo {sentinel} > /data/written.txt && sync")],
			Duration::from_secs(30),
		);
		assert_eq!(exit, 0, "guest write failed stdout={stdout:?} stderr={stderr:?}");
		let survivors = [
			(&gateways[1], &node_ids[1], &gateways[1].home),
			(&gateways[2], &node_ids[2], &gateways[2].home),
		];
		for (gw, _, home) in survivors {
			eventually("replica to carry post-write payload", REPLICA_TIMEOUT, POLL_INTERVAL, || {
				(replica_list(gw).contains(&sid)
					&& replica_volume_payload(home, &sid, &vol_name, "written.txt").as_deref()
						== Some(sentinel))
				.then_some(())
			});
		}
		gateways[0].kill_hard();
		let restored = eventually(
			"writable volume sandbox to restore on a survivor",
			Duration::from_secs(150),
			POLL_INTERVAL,
			|| {
				for index in 1..gateways.len() {
					let Ok(listing) = try_list_sandboxes(&gateways[index]) else {
						continue;
					};
					let Some(row) = sandbox_row(&listing, &sid) else {
						continue;
					};
					if row.get("node").and_then(Value::as_str) == Some(node_ids[index].as_str())
						&& row.get("status").and_then(Value::as_str) == Some("running")
					{
						return Some((index, row));
					}
				}
				None
			},
		);
		let restorer_index = restored.0;
		eventually_guest_file(&gateways[restorer_index], &sid, "/data/written.txt", sentinel);
	}));
	if result.is_err() {
		for (index, gateway) in gateways.iter().enumerate() {
			eprintln!("{} log tail:\n{}", gateway.name, gateway.log_tail());
			eprintln!("{} listing: {:?}", gateway.name, try_list_sandboxes(gateway));
			if index != 0 {
				eprintln!("{} replicas: {:?}", gateway.name, replica_list(gateway));
			}
			let record = gateway.home.join("records").join(format!("{sid}.json"));
			eprintln!("{} record: {:?}", gateway.name, fs::read_to_string(record));
		}
	}
	remove_sandbox_best_effort(&[&gateways[2], &gateways[1], &gateways[0]], &sid);
	for gateway in &mut gateways {
		gateway.stop();
	}
	for gateway in &gateways {
		let _ = fs::remove_dir_all(&gateway.home);
	}
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}

#[test]
fn macos_user_net_failover_restores_gateway_reachability() {
	if !require_cluster_e2e() {
		return;
	}
	if !cfg!(target_os = "macos") {
		eprintln!("SKIP cluster_e2e: macOS user-net failover only runs on Darwin");
		return;
	}
	let token = unique("cluster-e2e");
	let home_a = short_home("vcu-a-");
	let home_b = short_home("vcu-b-");
	let sid = unique("usernet-ha");
	let mut gateway_a = start_node(home_a, free_port(), &[], "node-usernet-a", &token, false);
	let mut gateway_b = start_node(home_b, free_port(), &[], "node-usernet-b", &token, false);

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateway_a);
		let join = mesh_join(
			&mut gateway_b,
			setup
				.get("blob")
				.and_then(Value::as_str)
				.expect("join blob"),
		);
		let node_a = setup
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node a id")
			.to_owned();
		let node_b = join
			.get("node_id")
			.and_then(Value::as_str)
			.expect("node b id")
			.to_owned();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));
		gateway_b.restart();
		gateway_a.restart();
		wait_healthy(&[&gateway_a, &gateway_b], Duration::from_mins(1));

		let row = create_sandbox(
			&gateway_a,
			&json!({
				"image": e2e_image(),
				"name": sid,
				"block_network": false,
				"timeout_secs": 900,
				"idempotency_key": format!("{sid}-create"),
			}),
		)
		.unwrap_or_else(|err| panic!("creating user-net sandbox failed: {err}"));
		assert_eq!(row.get("status").and_then(Value::as_str), Some("running"));
		let owner_node = row.get("node").and_then(Value::as_str).expect("owner node");
		let (owner, survivor, survivor_node) = if owner_node == node_a {
			(&mut gateway_a, &gateway_b, node_b.as_str())
		} else {
			(&mut gateway_b, &gateway_a, node_a.as_str())
		};
		eventually("survivor to hold user-net replica", REPLICA_TIMEOUT, POLL_INTERVAL, || {
			replica_list(survivor).contains(&sid).then_some(())
		});
		owner.kill_hard();
		let restored = eventually(
			"user-net sandbox to restore on survivor",
			Duration::from_mins(2),
			POLL_INTERVAL,
			|| {
				let listing = try_list_sandboxes(survivor).ok()?;
				let row = sandbox_row(&listing, &sid)?;
				(row.get("node").and_then(Value::as_str) == Some(survivor_node)
					&& row.get("status").and_then(Value::as_str) == Some("running"))
				.then_some(row)
			},
		);
		assert_eq!(restored.get("node").and_then(Value::as_str), Some(survivor_node));
		let (exit, stdout, stderr) = exec_capture(
			survivor,
			&sid,
			&["/bin/sh", "-c", "ping -c 1 -W 2 10.0.2.2"],
			Duration::from_secs(30),
		);
		assert_eq!(exit, 0, "gateway reachability probe failed stdout={stdout:?} stderr={stderr:?}");
	}));
	remove_sandbox_best_effort(&[&gateway_b, &gateway_a], &sid);
	gateway_a.stop();
	gateway_b.stop();
	let _ = fs::remove_dir_all(&gateway_a.home);
	let _ = fs::remove_dir_all(&gateway_b.home);
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}

#[test]
fn writable_volume_lease_handoff_has_no_active_overlap() {
	if !require_cluster_e2e() {
		return;
	}
	let token = unique("cluster-e2e");
	let sid = unique("lease-handoff");
	let vol_name = unique("leasevol").replace('-', "_");
	let env = [("VMON_RESTORE_QUORUM", "1"), ("VMON_REPLICAS", "2"), ("VMON_REPLICATE_SEC", "1")];
	let mut gateways = [
		start_node(short_home("vcl-a-"), free_port(), &env, "lease-a", &token, true),
		start_node(short_home("vcl-b-"), free_port(), &env, "lease-b", &token, true),
		start_node(short_home("vcl-c-"), free_port(), &env, "lease-c", &token, true),
	];

	let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
		let setup = mesh_setup(&mut gateways[0]);
		let mut node_ids = vec![
			setup
				.get("node_id")
				.and_then(Value::as_str)
				.expect("node a id")
				.to_owned(),
		];
		for gateway in gateways.iter_mut().skip(1) {
			let joined = mesh_join(
				gateway,
				setup
					.get("blob")
					.and_then(Value::as_str)
					.expect("join blob"),
			);
			node_ids.push(
				joined
					.get("node_id")
					.and_then(Value::as_str)
					.expect("joined id")
					.to_owned(),
			);
		}
		wait_healthy(&[&gateways[0], &gateways[1], &gateways[2]], Duration::from_mins(1));
		for gateway in &mut gateways {
			gateway.restart();
		}
		wait_healthy(&[&gateways[0], &gateways[1], &gateways[2]], Duration::from_mins(1));

		let created = match create_sandbox(
			&gateways[0],
			&json!({
				"image": e2e_image(),
				"name": sid,
				"block_network": true,
				"timeout_secs": 900,
				"volumes": {"/data": {"name": vol_name, "read_only": false}},
				"idempotency_key": format!("{sid}-create"),
			}),
		) {
			Ok(value) => value,
			Err(err) if skip_if_no_virtiofs(&err) => return,
			Err(err) => panic!("creating lease handoff sandbox failed: {err}"),
		};
		let first_lease = volume_lease(&created, &vol_name);
		eventually("both survivors to hold replicas", REPLICA_TIMEOUT, POLL_INTERVAL, || {
			(replica_list(&gateways[1]).contains(&sid) && replica_list(&gateways[2]).contains(&sid))
				.then_some(())
		});

		partition_node(&gateways[0], &[&gateways[0], &gateways[1], &gateways[2]], true);
		let owner_stopped = eventually(
			"partitioned writable-volume owner to stop",
			Duration::from_mins(2),
			POLL_INTERVAL,
			|| {
				let listing = try_list_sandboxes_backend(&gateways[0]).ok()?;
				let row = sandbox_row(&listing, &sid)?;
				(row.get("status").and_then(Value::as_str) != Some("running")).then_some(row)
			},
		);
		assert_eq!(
			owner_stopped.get("status").and_then(Value::as_str),
			Some("stopped"),
			"old owner did not self-fence: {owner_stopped}"
		);

		for gateway in &gateways[1..] {
			gateway
				.proxy
				.as_ref()
				.expect("fault proxy")
				.block_source(&node_ids[0], true);
		}
		let restored = eventually(
			"surviving majority to restore successor lease",
			RESTORE_TIMEOUT,
			POLL_INTERVAL,
			|| {
				for index in 1..gateways.len() {
					let Ok(listing) = try_list_sandboxes_backend(&gateways[index]) else {
						continue;
					};
					let Some(row) = sandbox_row(&listing, &sid) else {
						continue;
					};
					let has_lease = row
						.get("volume_leases")
						.and_then(Value::as_object)
						.is_some_and(|leases| leases.get(&vol_name).is_some());
					if row.get("node").and_then(Value::as_str) == Some(node_ids[index].as_str())
						&& row.get("status").and_then(Value::as_str) == Some("running")
						&& has_lease
					{
						return Some(row);
					}
				}
				None
			},
		);
		let successor_lease = volume_lease(&restored, &vol_name);
		let first_deadline = first_lease
			.get("renew_deadline")
			.and_then(Value::as_f64)
			.expect("first renew_deadline");
		let successor_start = successor_lease
			.get("granted_at")
			.and_then(Value::as_f64)
			.expect("successor granted_at");
		assert!(
			first_deadline <= successor_start,
			"successor lease activated before old owner reached stop deadline: old={first_lease}, \
			 successor={successor_lease}"
		);
		assert_volume_lease_non_overlap(&[first_lease, successor_lease]);
	}));
	partition_node(&gateways[0], &[&gateways[0], &gateways[1], &gateways[2]], false);
	remove_sandbox_best_effort(&[&gateways[2], &gateways[1], &gateways[0]], &sid);
	for gateway in &mut gateways {
		gateway.stop();
	}
	for gateway in &gateways {
		let _ = fs::remove_dir_all(&gateway.home);
	}
	if let Err(payload) = result {
		std::panic::resume_unwind(payload);
	}
}
