//! GC4 guest-agent host client. Port of `python/vmon/agent_client.py`.

use std::{
	collections::{BTreeMap, HashMap},
	io::{Read, Write},
	os::unix::net::UnixStream,
	path::Path,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
		mpsc::{self, Receiver, RecvTimeoutError, Sender},
	},
	thread,
	time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde_json::{Map, Value, json};
use vmon_agent::proto::{self, Frame};

use crate::error::{EngineError, Result};

const PING_ATTEMPT: Duration = Duration::from_millis(500);
const SIGKILL: i32 = 9;

/// Thread-safe connection to one guest-agent Unix socket.
#[allow(
	clippy::module_name_repetitions,
	reason = "Public API mirrors the Python agent client naming."
)]
#[derive(Clone)]
pub struct AgentConn {
	inner: Arc<AgentInner>,
}

/// A live guest exec session.
pub struct ExecSession {
	inner:           Arc<AgentInner>,
	id:              u32,
	tty:             bool,
	default_timeout: Option<Duration>,
	exit_rx:         Receiver<Result<ExitStatus>>,
	exit_cache:      Mutex<Option<Result<ExitStatus>>>,
	/// Stdout chunks for this exec; closes on EOF or process exit.
	pub stdout:      Receiver<Vec<u8>>,
	/// Stderr chunks for this exec; closes on EOF or process exit.
	pub stderr:      Receiver<Vec<u8>>,
}

/// Control handle for a split guest exec session.
#[derive(Clone)]
pub struct ExecHandle {
	inner: Arc<AgentInner>,
	id:    u32,
	tty:   bool,
}

/// Owned pieces of a guest exec session, used by the Engine streaming facade.
pub struct ExecSessionParts {
	pub control: ExecHandle,
	pub stdout:  Receiver<Vec<u8>>,
	pub stderr:  Receiver<Vec<u8>>,
	pub exit:    Receiver<Result<ExitStatus>>,
}

/// Live attachment to a persistent guest PTY.
pub struct PtyAgentStream {
	pub session: Value,
	pub control: PtyAgentHandle,
	pub stdout:  Receiver<Vec<u8>>,
	pub exit:    Receiver<Result<ExitStatus>>,
}

/// Input/control channel for one persistent PTY attachment.
#[derive(Clone)]
pub struct PtyAgentHandle {
	inner:      Arc<AgentInner>,
	frame_id:   u32,
	session_id: String,
}

/// Exit status reported by the guest agent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExitStatus {
	/// Process exit code, or -1 when the agent omitted it.
	pub code:   i64,
	/// Terminating signal, when the guest reported one.
	pub signal: Option<i64>,
}

/// Monotonic guest workload counters used by idle reclamation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuestActivity {
	/// Non-idle aggregate CPU ticks.
	pub cpu_ticks:     u64,
	/// Sectors read or written across block devices.
	pub disk_sectors:  u64,
	/// Bytes received or sent across network interfaces.
	pub network_bytes: u64,
}

struct AgentInner {
	writer:  Mutex<UnixStream>,
	state:   Mutex<State>,
	next_id: AtomicU32,
	epoch:   u64,
}

#[derive(Default)]
struct State {
	closed:    bool,
	pending:   HashMap<u32, Sender<Result<Value>>>,
	sessions:  HashMap<u32, SessionSink>,
	downloads: HashMap<u32, Sender<Vec<u8>>>,
}

struct SessionSink {
	stdout: Sender<Vec<u8>>,
	stderr: Sender<Vec<u8>>,
	exit:   Sender<Result<ExitStatus>>,
}

impl AgentConn {
	/// Connect to the agent socket and start the reader thread.
	pub fn connect(sock: &Path, connect_timeout: Duration) -> Result<Self> {
		let deadline = Instant::now() + connect_timeout;
		let stream = loop {
			match UnixStream::connect(sock) {
				Ok(stream) => break stream,
				Err(err) => {
					if Instant::now() >= deadline {
						return Err(EngineError::engine(format!(
							"connecting to agent socket {}: {err}",
							sock.display()
						)));
					}
					thread::sleep(Duration::from_millis(100));
				},
			}
		};

		let mut writer = stream.try_clone()?;
		writer.write_all(&proto::SYNC)?;
		let inner = Arc::new(AgentInner {
			writer:  Mutex::new(writer),
			state:   Mutex::new(State::default()),
			next_id: AtomicU32::new(1),
			epoch:   random_epoch()?,
		});
		let reader_inner = Arc::clone(&inner);
		thread::Builder::new()
			.name("vmon-agent-reader".to_string())
			.spawn(move || reader_loop(reader_inner, stream))?;
		let conn = Self { inner };
		// The host-side virtio-console socket accepts connections as soon as the
		// VMM starts, before the guest has paged in and launched its agent. The
		// mandatory epoch exchange is therefore part of connection readiness and
		// must share the caller's remaining boot budget rather than an unrelated
		// fixed cap. This keeps cold remote-image boots configurable through their
		// template timeout while still fencing stale PTY sink bindings.
		let handshake_timeout = deadline.saturating_duration_since(Instant::now());
		let hello = conn
			.request(proto::OP_HELLO, json!({ "epoch": conn.inner.epoch }), handshake_timeout)
			.inspect_err(|_| conn.close())?;
		if hello.get("epoch").and_then(Value::as_u64) != Some(conn.inner.epoch) {
			conn.close();
			return Err(EngineError::engine(
				"guest agent handshake failed: epoch not echoed; rebuild vmon-agent",
			));
		}
		Ok(conn)
	}

	/// Return true once the reader has marked the connection closed.
	pub fn is_closed(&self) -> bool {
		self.inner.state.lock().closed
	}

	/// Close the connection and wake all waiters.
	pub fn close(&self) {
		self
			.inner
			.abort(EngineError::engine("agent connection closed"));
	}

	/// Send one JSON request and wait for its response.
	pub fn request(&self, op: &str, params: Value, timeout: Duration) -> Result<Value> {
		let payload = request_payload(op, params)?;
		let (id, rx) = self.register_pending()?;
		if let Err(err) = self.inner.send_json(proto::FRAME_REQ, id, &payload) {
			self.remove_pending(id);
			return Err(err);
		}
		let resp = recv_pending(rx, timeout, "agent request timed out")?;
		ensure_response_object(&resp)?;
		response_ok(&resp, "agent request failed")?;
		Ok(resp)
	}

	/// Connection epoch used to prevent stream-id reuse across reconnects.
	pub fn epoch(&self) -> u64 {
		self.inner.epoch
	}

	/// Ping the guest agent, retrying dropped early requests until timeout.
	pub fn ping(&self, timeout: Duration) -> Result<Value> {
		let deadline = Instant::now() + timeout;
		loop {
			if Instant::now() >= deadline {
				return Err(EngineError::engine("agent request timed out"));
			}
			let (id, rx) = self.register_pending()?;
			let payload = request_payload("ping", Value::Null)?;
			if let Err(err) = self
				.inner
				.send_sync_and_json(proto::FRAME_REQ, id, &payload)
			{
				self.remove_pending(id);
				return Err(err);
			}
			let remaining = deadline.saturating_duration_since(Instant::now());
			let attempt = remaining.min(PING_ATTEMPT);
			match recv_pending(rx, attempt, "agent request timed out") {
				Ok(resp) => {
					ensure_response_object(&resp)?;
					response_ok(&resp, "agent request failed")?;
					return Ok(resp);
				},
				Err(err) if err.message == "agent request timed out" => {
					self.remove_pending(id);
					if Instant::now() >= deadline {
						return Err(err);
					}
				},
				Err(err) => {
					self.remove_pending(id);
					return Err(err);
				},
			}
		}
	}

	/// Sample autonomous guest CPU, disk, and network activity.
	pub fn activity(&self, timeout: Duration) -> Result<GuestActivity> {
		let response = self.request("activity", Value::Null, timeout)?;
		let field = |name| {
			response
				.get(name)
				.and_then(Value::as_u64)
				.ok_or_else(|| EngineError::engine(format!("agent activity omitted {name}")))
		};
		Ok(GuestActivity {
			cpu_ticks:     field("cpu_ticks")?,
			disk_sectors:  field("disk_sectors")?,
			network_bytes: field("network_bytes")?,
		})
	}

	/// Configure the guest network stack.
	pub fn net_config(
		&self,
		ip: &str,
		prefix: u8,
		gw: &str,
		dns: Option<&[String]>,
		timeout: Duration,
	) -> Result<Value> {
		self.request(
			"net_config",
			json!({ "ip": ip, "prefix": prefix, "gw": gw, "dns": dns.unwrap_or(&[]) }),
			timeout,
		)
	}

	/// Mount a guest filesystem path.
	pub fn mount(
		&self,
		tag: &str,
		path: &Path,
		ro: bool,
		fstype: &str,
		timeout: Duration,
	) -> Result<Value> {
		self.request(
			"mount",
			json!({ "tag": tag, "path": path_to_str(path)?, "ro": ro, "fstype": fstype }),
			timeout,
		)
	}

	/// Mount a read-only virtio-fs lower layer beneath a volatile guest overlay.
	pub fn mount_overlay(&self, tag: &str, path: &Path, timeout: Duration) -> Result<Value> {
		self.request("mount_overlay", json!({ "tag": tag, "path": path_to_str(path)? }), timeout)
	}

	/// Probe guest TCP connectivity.
	pub fn tcp_probe(&self, port: u16, host: &str, timeout: Duration) -> Result<bool> {
		let resp = self.request("tcp_probe", json!({ "port": port, "host": host }), timeout)?;
		Ok(resp
			.get("connected")
			.and_then(Value::as_bool)
			.unwrap_or(false))
	}

	/// Execute a command in the guest.
	pub fn exec<S: AsRef<str>>(
		&self,
		cmd: &[S],
		cwd: Option<&Path>,
		env: Option<&BTreeMap<String, String>>,
		tty: bool,
		timeout: Option<Duration>,
	) -> Result<ExecSession> {
		let id = self.new_id();
		let (stdout_tx, stdout_rx) = mpsc::channel();
		let (stderr_tx, stderr_rx) = mpsc::channel();
		let (exit_tx, exit_rx) = mpsc::channel();
		{
			let mut state = self.inner.state.lock();
			ensure_open(&state)?;
			state.sessions.insert(id, SessionSink {
				stdout: stdout_tx,
				stderr: stderr_tx,
				exit:   exit_tx,
			});
		}

		let mut payload = Map::new();
		payload.insert("op".to_string(), Value::String("exec".to_string()));
		payload.insert(
			"cmd".to_string(),
			Value::Array(
				cmd.iter()
					.map(|part| Value::String(part.as_ref().to_string()))
					.collect(),
			),
		);
		payload.insert("tty".to_string(), Value::Bool(tty));
		if let Some(cwd) = cwd {
			payload.insert("cwd".to_string(), Value::String(path_to_str(cwd)?.to_string()));
		}
		if let Some(env) = env {
			let mut obj = Map::new();
			for (key, value) in env {
				obj.insert(key.clone(), Value::String(value.clone()));
			}
			payload.insert("env".to_string(), Value::Object(obj));
		}

		if let Err(err) = self
			.inner
			.send_json(proto::FRAME_REQ, id, &Value::Object(payload))
		{
			self.remove_session(id);
			return Err(err);
		}
		Ok(ExecSession {
			inner: Arc::clone(&self.inner),
			id,
			tty,
			default_timeout: timeout,
			exit_rx,
			exit_cache: Mutex::new(None),
			stdout: stdout_rx,
			stderr: stderr_rx,
		})
	}

	/// Read a guest file into memory.
	pub fn fs_read(&self, path: &Path, timeout: Duration) -> Result<Vec<u8>> {
		let id = self.new_id();
		let (pending_tx, pending_rx) = mpsc::channel();
		let (download_tx, download_rx) = mpsc::channel();
		{
			let mut state = self.inner.state.lock();
			ensure_open(&state)?;
			state.pending.insert(id, pending_tx);
			state.downloads.insert(id, download_tx);
		}
		let payload = json!({ "op": "fs_read", "path": path_to_str(path)? });
		if let Err(err) = self.inner.send_json(proto::FRAME_REQ, id, &payload) {
			self.remove_pending_and_download(id);
			return Err(err);
		}
		let resp = match recv_pending(pending_rx, timeout, "agent request timed out") {
			Ok(resp) => resp,
			Err(err) => {
				self.remove_pending_and_download(id);
				return Err(err);
			},
		};
		ensure_response_object(&resp)?;
		response_ok(&resp, "fs_read failed")?;
		let mut data = Vec::new();
		while let Ok(chunk) = download_rx.recv() {
			data.extend_from_slice(&chunk);
		}
		Ok(data)
	}

	/// Flush every mounted guest filesystem before discarding VM memory.
	pub fn fs_sync(&self, timeout: Duration) -> Result<Value> {
		self.request("fs_sync", Value::Null, timeout)
	}

	/// Write a guest file with the default mode.
	pub fn fs_write(&self, path: &Path, data: &[u8], timeout: Duration) -> Result<Value> {
		self.fs_write_with_mode(path, data, 0o644, timeout)
	}

	/// Write a guest file with an explicit mode.
	pub fn fs_write_with_mode(
		&self,
		path: &Path,
		data: &[u8],
		mode: u32,
		timeout: Duration,
	) -> Result<Value> {
		let (id, rx) = self.register_pending()?;
		let payload = json!({ "op": "fs_write", "path": path_to_str(path)?, "mode": mode });
		let send_result = (|| -> Result<()> {
			self.inner.send_json(proto::FRAME_REQ, id, &payload)?;
			for chunk in data.chunks(proto::MAX_PAYLOAD_LEN) {
				self.inner.send_frame(proto::FRAME_STDIN, id, chunk)?;
			}
			self.inner.send_frame(proto::FRAME_STDIN, id, &[])?;
			Ok(())
		})();
		if let Err(err) = send_result {
			self.remove_pending(id);
			return Err(err);
		}
		let resp = recv_pending(rx, timeout, "agent request timed out")?;
		ensure_response_object(&resp)?;
		response_ok(&resp, "fs_write failed")?;
		Ok(resp)
	}

	/// List a guest directory.
	pub fn fs_list(&self, path: &Path, timeout: Duration) -> Result<Vec<Value>> {
		let resp = self.request("fs_list", json!({ "path": path_to_str(path)? }), timeout)?;
		Ok(resp
			.get("entries")
			.and_then(Value::as_array)
			.cloned()
			.unwrap_or_default())
	}

	/// Stat a guest filesystem path.
	pub fn fs_stat(&self, path: &Path, timeout: Duration) -> Result<Value> {
		self.request("fs_stat", json!({ "path": path_to_str(path)? }), timeout)
	}

	/// Create a guest directory, including parents.
	pub fn fs_mkdir(&self, path: &Path, timeout: Duration) -> Result<Value> {
		self.fs_mkdir_with_parents(path, true, timeout)
	}

	/// Create a guest directory with explicit parent behavior.
	pub fn fs_mkdir_with_parents(
		&self,
		path: &Path,
		parents: bool,
		timeout: Duration,
	) -> Result<Value> {
		self.request("fs_mkdir", json!({ "path": path_to_str(path)?, "parents": parents }), timeout)
	}

	/// Remove a guest filesystem path.
	pub fn fs_remove(&self, path: &Path, recursive: bool, timeout: Duration) -> Result<Value> {
		self.request(
			"fs_remove",
			json!({ "path": path_to_str(path)?, "recursive": recursive }),
			timeout,
		)
	}

	/// Resize a tty exec session.
	pub fn resize(&self, session_id: u32, rows: u16, cols: u16) -> Result<()> {
		self.inner.send_json(
			proto::FRAME_REQ,
			session_id,
			&json!({ "op": "resize", "rows": rows, "cols": cols }),
		)
	}

	fn register_pending(&self) -> Result<(u32, Receiver<Result<Value>>)> {
		let id = self.new_id();
		let (tx, rx) = mpsc::channel();
		{
			let mut state = self.inner.state.lock();
			ensure_open(&state)?;
			state.pending.insert(id, tx);
		}
		Ok((id, rx))
	}

	/// Open a persistent PTY and bind its output to this connection.
	pub fn pty_open(&self, params: Value, timeout: Duration) -> Result<PtyAgentStream> {
		self.open_pty_stream("pty_open", params, timeout)
	}

	/// Rebind an existing persistent PTY to this connection.
	pub fn pty_attach(&self, params: Value, timeout: Duration) -> Result<PtyAgentStream> {
		self.open_pty_stream("pty_attach", params, timeout)
	}

	fn open_pty_stream(&self, op: &str, params: Value, timeout: Duration) -> Result<PtyAgentStream> {
		let session_id = params
			.get("session_id")
			.and_then(Value::as_str)
			.unwrap_or_default()
			.to_owned();
		let payload = request_payload(op, params)?;
		let id = self.new_id();
		let (pending_tx, pending_rx) = mpsc::channel();
		let (stdout_tx, stdout_rx) = mpsc::channel();
		let (stderr_tx, _stderr_rx) = mpsc::channel();
		let (exit_tx, exit_rx) = mpsc::channel();
		{
			let mut state = self.inner.state.lock();
			ensure_open(&state)?;
			state.pending.insert(id, pending_tx);
			state.sessions.insert(id, SessionSink {
				stdout: stdout_tx,
				stderr: stderr_tx,
				exit:   exit_tx,
			});
		}
		if let Err(error) = self.inner.send_json(proto::FRAME_REQ, id, &payload) {
			self.remove_pending(id);
			self.remove_session(id);
			return Err(error);
		}
		let response = match recv_pending(pending_rx, timeout, "PTY request timed out") {
			Ok(response) => response,
			Err(error) => {
				self.remove_pending(id);
				self.remove_session(id);
				return Err(error);
			},
		};
		if response_is_false(&response) {
			self.remove_session(id);
			let message = response_error_message(&response, "PTY request failed");
			return Err(if message == "session cap reached" {
				EngineError::busy(message)
			} else {
				response_error(&response, "PTY request failed")
			});
		}
		let actual_id = response
			.get("session_id")
			.and_then(Value::as_str)
			.unwrap_or(&session_id)
			.to_owned();
		Ok(PtyAgentStream {
			session: response,
			control: PtyAgentHandle {
				inner:      Arc::clone(&self.inner),
				frame_id:   id,
				session_id: actual_id,
			},
			stdout:  stdout_rx,
			exit:    exit_rx,
		})
	}

	/// List sessions from the guest source of truth.
	pub fn pty_list(&self, timeout: Duration) -> Result<Vec<Value>> {
		let response = self.request("pty_list", Value::Null, timeout)?;
		Ok(response
			.get("sessions")
			.and_then(Value::as_array)
			.cloned()
			.unwrap_or_default())
	}

	/// Terminate a persistent guest PTY.
	pub fn pty_close(&self, session_id: &str, timeout: Duration) -> Result<Value> {
		self.request("pty_close", json!({ "session_id": session_id }), timeout)
	}

	/// Execute a sibling command in a persistent PTY's context.
	pub fn pty_exec(&self, session_id: &str, command: &str, timeout: Duration) -> Result<Value> {
		self.request(
			"pty_exec",
			json!({
				"session_id": session_id,
				"command": command,
				"request_id": self.inner.next_id.load(Ordering::Relaxed),
				"timeout_ms": timeout.as_millis() as u64,
			}),
			timeout + Duration::from_secs(1),
		)
	}

	fn new_id(&self) -> u32 {
		loop {
			let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
			if id != 0 && id != u32::MAX {
				return id;
			}
		}
	}

	fn remove_pending(&self, id: u32) {
		self.inner.state.lock().pending.remove(&id);
	}

	fn remove_session(&self, id: u32) {
		self.inner.state.lock().sessions.remove(&id);
	}

	fn remove_pending_and_download(&self, id: u32) {
		let mut state = self.inner.state.lock();
		state.pending.remove(&id);
		state.downloads.remove(&id);
	}
}

impl PtyAgentHandle {
	/// Write input bytes to the attached guest PTY.
	pub fn write(&self, data: &[u8]) -> Result<()> {
		for chunk in data.chunks(proto::MAX_PAYLOAD_LEN) {
			self
				.inner
				.send_frame(proto::FRAME_STDIN, self.frame_id, chunk)?;
		}
		Ok(())
	}

	/// Resize the persistent PTY.
	pub fn resize(&self, rows: u16, cols: u16) -> Result<()> {
		self.inner.send_json(
			proto::FRAME_REQ,
			self.frame_id,
			&json!({
				"op": "pty_resize",
				"session_id": self.session_id,
				"rows": rows,
				"cols": cols,
			}),
		)
	}

	/// Unbind this connection without terminating the guest process.
	pub fn detach(&self) -> Result<()> {
		self
			.inner
			.send_frame(proto::FRAME_STDIN, self.frame_id, &[])
	}
}

impl ExecSession {
	/// Return the request/session id used on the wire.
	pub const fn id(&self) -> u32 {
		self.id
	}

	/// Split this session into a control handle and raw stream receivers.
	pub fn split(self) -> ExecSessionParts {
		ExecSessionParts {
			control: ExecHandle { inner: Arc::clone(&self.inner), id: self.id, tty: self.tty },
			stdout:  self.stdout,
			stderr:  self.stderr,
			exit:    self.exit_rx,
		}
	}

	/// Write stdin bytes to the guest process.
	pub fn write_stdin(&self, data: &[u8]) -> Result<()> {
		for chunk in data.chunks(proto::MAX_PAYLOAD_LEN) {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, chunk)?;
		}
		Ok(())
	}

	/// Close stdin, or send EOT for tty sessions.
	pub fn close_stdin(&self) -> Result<()> {
		if self.tty {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, b"\x04")
		} else {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, &[])
		}
	}

	/// Send a signal to the guest process.
	pub fn kill(&self, signal: i32) -> Result<()> {
		self
			.inner
			.send_json(proto::FRAME_KILL, self.id, &json!({ "signal": signal }))
	}

	/// Resize the tty backing this session.
	pub fn resize(&self, rows: u16, cols: u16) -> Result<()> {
		self.inner.send_json(
			proto::FRAME_REQ,
			self.id,
			&json!({ "op": "resize", "rows": rows, "cols": cols }),
		)
	}

	/// Wait for process exit and return the guest exit code.
	pub fn wait(&self, timeout: Option<Duration>) -> Result<i64> {
		let cached = self.exit_cache.lock().clone();
		if let Some(cached) = cached {
			return cached.map(|status| status.code);
		}
		let limit = timeout.or(self.default_timeout);
		let received = match limit {
			Some(limit) => match self.exit_rx.recv_timeout(limit) {
				Ok(status) => status,
				Err(RecvTimeoutError::Timeout) => {
					let _ = self.kill(SIGKILL);
					return Err(EngineError::engine("agent exec timed out"));
				},
				Err(RecvTimeoutError::Disconnected) => {
					Err(EngineError::engine("agent connection closed"))
				},
			},
			None => match self.exit_rx.recv() {
				Ok(status) => status,
				Err(_) => Err(EngineError::engine("agent connection closed")),
			},
		};
		*self.exit_cache.lock() = Some(received.clone());
		received.map(|status| status.code)
	}

	/// Drain stdout until EOF.
	pub fn read_stdout_to_end(&self) -> Vec<u8> {
		let mut out = Vec::new();
		while let Ok(chunk) = self.stdout.recv() {
			out.extend_from_slice(&chunk);
		}
		out
	}
}

impl ExecHandle {
	/// Return the request/session id used on the wire.
	pub const fn id(&self) -> u32 {
		self.id
	}

	/// Write stdin bytes to the guest process.
	pub fn write_stdin(&self, data: &[u8]) -> Result<()> {
		for chunk in data.chunks(proto::MAX_PAYLOAD_LEN) {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, chunk)?;
		}
		Ok(())
	}

	/// Close stdin, or send EOT for tty sessions.
	pub fn close_stdin(&self) -> Result<()> {
		if self.tty {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, b"\x04")
		} else {
			self.inner.send_frame(proto::FRAME_STDIN, self.id, &[])
		}
	}

	/// Send a signal to the guest process.
	pub fn kill(&self, signal: i32) -> Result<()> {
		self
			.inner
			.send_json(proto::FRAME_KILL, self.id, &json!({ "signal": signal }))
	}

	/// Resize the tty backing this session.
	pub fn resize(&self, rows: u16, cols: u16) -> Result<()> {
		self.inner.send_json(
			proto::FRAME_REQ,
			self.id,
			&json!({ "op": "resize", "rows": rows, "cols": cols }),
		)
	}
}

impl AgentInner {
	fn send_frame(&self, ty: u8, id: u32, payload: &[u8]) -> Result<()> {
		{
			let state = self.state.lock();
			ensure_open(&state)?;
		}
		let send_result = {
			let mut writer = self.writer.lock();
			proto::write_frame(&mut *writer, ty, id, payload)
		};
		if let Err(err) = send_result {
			let engine_err = EngineError::engine(err.to_string());
			self.abort(engine_err.clone());
			return Err(engine_err);
		}
		Ok(())
	}

	fn send_json(&self, ty: u8, id: u32, payload: &Value) -> Result<()> {
		let raw = serde_json::to_vec(payload)?;
		self.send_frame(ty, id, &raw)
	}

	fn send_sync_and_json(&self, ty: u8, id: u32, payload: &Value) -> Result<()> {
		{
			let state = self.state.lock();
			ensure_open(&state)?;
		}
		let raw = serde_json::to_vec(payload)?;
		let send_result = {
			let mut writer = self.writer.lock();
			writer
				.write_all(&proto::SYNC)
				.and_then(|()| proto::write_frame(&mut *writer, ty, id, &raw))
		};
		if let Err(err) = send_result {
			let engine_err = EngineError::engine(err.to_string());
			self.abort(engine_err.clone());
			return Err(engine_err);
		}
		Ok(())
	}

	fn abort(&self, error: EngineError) {
		let (pending, sessions, downloads) = {
			let mut state = self.state.lock();
			if state.closed {
				return;
			}
			state.closed = true;
			let pending = state.pending.drain().map(|(_, tx)| tx).collect::<Vec<_>>();
			let sessions = state
				.sessions
				.drain()
				.map(|(_, session)| session)
				.collect::<Vec<_>>();
			let downloads = state
				.downloads
				.drain()
				.map(|(_, tx)| tx)
				.collect::<Vec<_>>();
			(pending, sessions, downloads)
		};

		for tx in pending {
			let _ = tx.send(Err(error.clone()));
		}
		for tx in downloads {
			drop(tx);
		}
		for session in sessions {
			drop(session.stdout);
			drop(session.stderr);
			let _ = session.exit.send(Err(error.clone()));
		}
		let _ = self.writer.lock().shutdown(std::net::Shutdown::Both);
	}
}

fn reader_loop(inner: Arc<AgentInner>, mut reader: UnixStream) {
	loop {
		match proto::read_frame(&mut reader) {
			Ok(Some(frame)) => {
				if let Err(err) = dispatch_frame(&inner, frame) {
					inner.abort(err);
					return;
				}
			},
			Ok(None) => {
				inner.abort(EngineError::engine("agent connection closed"));
				return;
			},
			Err(err) => {
				inner.abort(EngineError::engine(format!("agent connection failed: {err}")));
				return;
			},
		}
	}
}

fn dispatch_frame(inner: &AgentInner, frame: Frame) -> Result<()> {
	match frame.ty {
		proto::FRAME_RESP => handle_resp(inner, frame.id, &frame.payload),
		proto::FRAME_STDOUT | proto::FRAME_STDERR => {
			handle_stream(inner, frame.ty, frame.id, frame.payload);
			Ok(())
		},
		proto::FRAME_EXIT => handle_exit(inner, frame.id, &frame.payload),
		_ => Err(EngineError::engine(format!("unexpected agent frame type {}", frame.ty))),
	}
}

fn handle_resp(inner: &AgentInner, id: u32, payload: &[u8]) -> Result<()> {
	let result = if payload.is_empty() {
		Value::Object(Map::new())
	} else {
		serde_json::from_slice(payload)?
	};
	let (pending, session_error) = {
		let mut state = inner.state.lock();
		let pending = state.pending.remove(&id);
		state.downloads.remove(&id);
		let session_error = if pending.is_none() && response_is_false(&result) {
			state.sessions.remove(&id)
		} else {
			None
		};
		(pending, session_error)
	};
	if let Some(tx) = pending {
		let _ = tx.send(Ok(result.clone()));
	}
	if let Some(session) = session_error {
		let error = response_error(&result, "agent request failed");
		drop(session.stdout);
		drop(session.stderr);
		let _ = session.exit.send(Err(error));
	}
	Ok(())
}

fn handle_stream(inner: &AgentInner, ty: u8, id: u32, payload: Vec<u8>) {
	let mut state = inner.state.lock();
	if let Some(session) = state.sessions.get_mut(&id) {
		let target = if ty == proto::FRAME_STDOUT {
			&mut session.stdout
		} else {
			&mut session.stderr
		};
		if payload.is_empty() {
			let replacement = mpsc::channel().0;
			drop(std::mem::replace(target, replacement));
		} else if target.send(payload).is_err() {
			state.sessions.remove(&id);
		}
		return;
	}
	if ty == proto::FRAME_STDOUT {
		if payload.is_empty() {
			state.downloads.remove(&id);
		} else if let Some(download) = state.downloads.get(&id)
			&& download.send(payload).is_err()
		{
			state.downloads.remove(&id);
		}
	}
}

fn handle_exit(inner: &AgentInner, id: u32, payload: &[u8]) -> Result<()> {
	let result = if payload.is_empty() {
		Value::Object(Map::new())
	} else {
		serde_json::from_slice(payload)?
	};
	let code = result.get("code").and_then(Value::as_i64).unwrap_or(-1);
	let signal = result.get("signal").and_then(Value::as_i64);
	let session = inner.state.lock().sessions.remove(&id);
	if let Some(session) = session {
		drop(session.stdout);
		drop(session.stderr);
		let _ = session.exit.send(Ok(ExitStatus { code, signal }));
	}
	Ok(())
}

fn recv_pending(
	rx: Receiver<Result<Value>>,
	timeout: Duration,
	timeout_message: &str,
) -> Result<Value> {
	match rx.recv_timeout(timeout) {
		Ok(result) => result,
		Err(RecvTimeoutError::Timeout) => Err(EngineError::engine(timeout_message)),
		Err(RecvTimeoutError::Disconnected) => Err(EngineError::engine("agent connection closed")),
	}
}

fn request_payload(op: &str, params: Value) -> Result<Value> {
	let mut obj = match params {
		Value::Null => Map::new(),
		Value::Object(obj) => obj,
		_ => return Err(EngineError::invalid("agent request params must be a JSON object")),
	};
	obj.insert("op".to_string(), Value::String(op.to_string()));
	Ok(Value::Object(obj))
}

fn random_epoch() -> Result<u64> {
	let mut bytes = [0u8; 8];
	std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
	let epoch = u64::from_ne_bytes(bytes);
	Ok(if epoch == 0 { 1 } else { epoch })
}

fn ensure_open(state: &State) -> Result<()> {
	if state.closed {
		Err(EngineError::engine("agent connection closed"))
	} else {
		Ok(())
	}
}

fn ensure_response_object(resp: &Value) -> Result<()> {
	if resp.is_object() {
		Ok(())
	} else {
		Err(EngineError::engine("agent response payload is not a JSON object"))
	}
}

fn response_ok(resp: &Value, default: &str) -> Result<()> {
	if response_is_false(resp) {
		Err(response_error(resp, default))
	} else {
		Ok(())
	}
}

fn response_is_false(resp: &Value) -> bool {
	resp.get("ok").and_then(Value::as_bool) == Some(false)
}

fn response_error_message(resp: &Value, default: &str) -> String {
	resp
		.get("error")
		.and_then(Value::as_str)
		.filter(|message| !message.is_empty())
		.unwrap_or(default)
		.to_string()
}

fn response_error(resp: &Value, default: &str) -> EngineError {
	let message = response_error_message(resp, default);
	match resp.get("errno").and_then(Value::as_i64) {
		Some(2) => EngineError::not_found(message),
		Some(1 | 13) => EngineError::forbidden(message),
		Some(20 | 21 | 39) => EngineError::invalid(message),
		_ => EngineError::engine(message),
	}
}

fn path_to_str(path: &Path) -> Result<&str> {
	path
		.to_str()
		.ok_or_else(|| EngineError::invalid("path is not valid UTF-8"))
}

#[cfg(test)]
mod tests {
	use std::{
		os::unix::net::{UnixListener, UnixStream},
		thread::JoinHandle,
	};

	use tempfile::TempDir;

	use super::*;
	use crate::error::ErrorCode;

	fn spawn_agent<F>(handler: F) -> (TempDir, std::path::PathBuf, JoinHandle<()>)
	where
		F: FnOnce(UnixStream) + Send + 'static,
	{
		let dir = tempfile::tempdir().unwrap();
		let sock = dir.path().join("agent.sock");
		let listener = UnixListener::bind(&sock).unwrap();
		let handle = thread::spawn(move || {
			let (stream, _) = listener.accept().unwrap();
			handler(stream);
		});
		(dir, sock, handle)
	}

	fn next_frame(stream: &mut UnixStream) -> Frame {
		proto::read_frame(stream).unwrap().unwrap()
	}

	/// Next non-handshake request; answers the connect HELLO like a real agent.
	fn next_req(stream: &mut UnixStream) -> (u32, Value) {
		loop {
			let frame = proto::read_frame(stream).unwrap().unwrap();
			if frame.ty == proto::FRAME_REQ {
				let payload: Value = serde_json::from_slice(&frame.payload).unwrap();
				if payload.get("op").and_then(Value::as_str) == Some(proto::OP_HELLO) {
					let epoch = payload.get("epoch").cloned().unwrap_or(Value::Null);
					write_json(
						stream,
						proto::FRAME_RESP,
						frame.id,
						json!({ "ok": true, "epoch": epoch }),
					);
					continue;
				}
				return (frame.id, payload);
			}
		}
	}

	fn write_json(stream: &mut UnixStream, ty: u8, id: u32, payload: Value) {
		let raw = serde_json::to_vec(&payload).unwrap();
		proto::write_frame(stream, ty, id, &raw).unwrap();
	}

	fn short_timeout() -> Duration {
		Duration::from_secs(2)
	}

	#[test]
	fn agent_ping_round_trips() {
		let (_dir, sock, handle) = spawn_agent(|mut stream| {
			let (id, req) = next_req(&mut stream);
			assert_eq!(req.get("op").and_then(Value::as_str), Some("ping"));
			write_json(&mut stream, proto::FRAME_RESP, id, json!({ "ok": true, "pong": true }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let resp = conn.ping(short_timeout()).unwrap();
		assert_eq!(resp.get("pong").and_then(Value::as_bool), Some(true));
		drop(conn);
		handle.join().unwrap();
	}

	#[test]
	fn agent_exec_streams_stdout_and_exit_without_resp() {
		let (_dir, sock, handle) = spawn_agent(|mut stream| {
			let (id, req) = next_req(&mut stream);
			assert_eq!(req.get("op").and_then(Value::as_str), Some("exec"));
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, id, b"hel").unwrap();
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, id, b"lo").unwrap();
			write_json(&mut stream, proto::FRAME_EXIT, id, json!({ "code": 7, "signal": null }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let session = conn
			.exec(&["sh", "-c", "echo hello"], None, None, false, Some(short_timeout()))
			.unwrap();
		assert_eq!(session.read_stdout_to_end(), b"hello");
		assert_eq!(session.wait(None).unwrap(), 7);
		drop(conn);
		handle.join().unwrap();
	}

	#[test]
	fn agent_fs_read_waits_for_chunks_eof_and_resp() {
		let (_dir, sock, handle) = spawn_agent(|mut stream| {
			let (id, req) = next_req(&mut stream);
			assert_eq!(req.get("op").and_then(Value::as_str), Some("fs_read"));
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, id, b"abc").unwrap();
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, id, b"def").unwrap();
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, id, &[]).unwrap();
			write_json(&mut stream, proto::FRAME_RESP, id, json!({ "ok": true, "size": 6 }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let data = conn
			.fs_read(Path::new("/tmp/file"), short_timeout())
			.unwrap();
		assert_eq!(data, b"abcdef");
		drop(conn);
		handle.join().unwrap();
	}

	#[test]
	fn agent_fs_write_sends_chunks_and_eof_before_waiting() {
		let data = vec![b'x'; proto::MAX_PAYLOAD_LEN + 5];
		let expected = data.clone();
		let (_dir, sock, handle) = spawn_agent(move |mut stream| {
			let (id, req) = next_req(&mut stream);
			assert_eq!(req.get("op").and_then(Value::as_str), Some("fs_write"));
			let first = next_frame(&mut stream);
			let second = next_frame(&mut stream);
			let eof = next_frame(&mut stream);
			assert_eq!(first.ty, proto::FRAME_STDIN);
			assert_eq!(second.ty, proto::FRAME_STDIN);
			assert_eq!(eof.ty, proto::FRAME_STDIN);
			assert_eq!(first.id, id);
			assert_eq!(second.id, id);
			assert_eq!(eof.id, id);
			assert_eq!(first.payload.len(), proto::MAX_PAYLOAD_LEN);
			assert_eq!(second.payload.len(), 5);
			assert!(eof.payload.is_empty());
			let mut got = first.payload;
			got.extend_from_slice(&second.payload);
			assert_eq!(got, expected);
			write_json(&mut stream, proto::FRAME_RESP, id, json!({ "ok": true, "size": got.len() }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let resp = conn
			.fs_write(Path::new("/tmp/file"), &data, short_timeout())
			.unwrap();
		assert_eq!(resp.get("size").and_then(Value::as_u64), Some(data.len() as u64));
		drop(conn);
		handle.join().unwrap();
	}

	#[test]
	fn agent_error_resp_maps_to_engine_error() {
		let (_dir, sock, handle) = spawn_agent(|mut stream| {
			let (id, _req) = next_req(&mut stream);
			write_json(&mut stream, proto::FRAME_RESP, id, json!({ "ok": false, "error": "boom" }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let err = conn
			.request("ping", Value::Null, short_timeout())
			.unwrap_err();
		assert_eq!(err.code, ErrorCode::Engine);
		assert_eq!(err.message, "boom");
		drop(conn);
		handle.join().unwrap();
	}

	#[test]
	fn agent_error_resp_maps_filesystem_errno() {
		let cases = [
			(2, ErrorCode::NotFound),
			(13, ErrorCode::Forbidden),
			(1, ErrorCode::Forbidden),
			(21, ErrorCode::Invalid),
			(20, ErrorCode::Invalid),
			(39, ErrorCode::Invalid),
		];
		for (errno, expected) in cases {
			let response = json!({ "ok": false, "error": "filesystem failure", "errno": errno });
			let error = response_error(&response, "fallback");
			assert_eq!(error.code, expected, "engine code for errno {errno}");
			assert_eq!(error.message, "filesystem failure");
		}
	}

	#[test]
	fn agent_ignores_unknown_id_frames() {
		let (_dir, sock, handle) = spawn_agent(|mut stream| {
			let (id, _req) = next_req(&mut stream);
			proto::write_frame(&mut stream, proto::FRAME_STDOUT, u32::MAX, b"boot exec noise")
				.unwrap();
			write_json(&mut stream, proto::FRAME_EXIT, u32::MAX, json!({ "code": 0 }));
			write_json(&mut stream, proto::FRAME_RESP, id, json!({ "ok": true, "pong": true }));
		});
		let conn = AgentConn::connect(&sock, short_timeout()).unwrap();
		let resp = conn.ping(short_timeout()).unwrap();
		assert_eq!(resp.get("pong").and_then(Value::as_bool), Some(true));
		drop(conn);
		handle.join().unwrap();
	}
}
