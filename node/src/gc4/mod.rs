//! GC4 host client: the node's side of the guest-agent control channel
//! (ARCHITECTURE.md §8.3), spoken over the VMM's `agent.sock`.
//!
//! One [`AgentClient`] owns one socket connection (one agent epoch). A reader
//! task demultiplexes frames by id to pending requests, exec sessions, and
//! tunnel streams ([`AgentStream`]); a writer task serializes outgoing frames
//! so header and payload never interleave. Sandboxes and template builds use
//! it for `hello`, `net_config`, `mount`, `exec`, `fs_*`, and `tcp_connect`.

mod stream;

use std::{
	collections::{BTreeMap, HashMap},
	io,
	path::Path,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
	time::Duration,
};

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
	sync::{mpsc, oneshot},
	task::AbortHandle,
	time::Instant,
};
use vmon_agent::proto::{self, Op};

pub use self::stream::{AgentStream, ConnectError, DATA_CHUNK};

/// Default budget for one request/response round trip.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One decoded GC4 frame.
#[derive(Debug)]
struct Frame {
	ty:      u8,
	id:      u32,
	payload: Bytes,
}

/// Encodes one frame (header + payload) into a single buffer so the writer
/// task emits it with one `write_all`.
fn encode(ty: u8, id: u32, payload: &[u8]) -> Vec<u8> {
	debug_assert!(payload.len() <= proto::MAX_PAYLOAD_LEN);
	let mut out = Vec::with_capacity(proto::HEADER_LEN + payload.len());
	out.extend_from_slice(
		&u32::try_from(payload.len())
			.unwrap_or(u32::MAX)
			.to_le_bytes(),
	);
	out.push(ty);
	out.extend_from_slice(&id.to_le_bytes());
	out.extend_from_slice(payload);
	out
}

/// Reads the next frame, skipping in-band SYNC markers. `Ok(None)` = clean EOF.
async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Option<Frame>> {
	loop {
		let mut header = [0u8; proto::HEADER_LEN];
		let first = reader.read(&mut header[..1]).await?;
		if first == 0 {
			return Ok(None);
		}
		reader.read_exact(&mut header[1..]).await?;
		if header == proto::SYNC {
			continue;
		}
		let len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
		if len > proto::MAX_PAYLOAD_LEN {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("GC4 frame of {len} bytes exceeds the limit"),
			));
		}
		let ty = header[4];
		let id = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);
		let mut payload = vec![0u8; len];
		reader.read_exact(&mut payload).await?;
		return Ok(Some(Frame { ty, id, payload: Bytes::from(payload) }));
	}
}

/// Output of an exec session, in arrival order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExecEvent {
	Stdout(Bytes),
	Stderr(Bytes),
	/// The process exited; the session is over.
	Exit {
		code:   i32,
		signal: Option<i32>,
	},
	/// The agent rejected the session (spawn failure, lost connection); the
	/// session is over.
	Failed(String),
}

/// `exec` request: run `cmd` in the guest.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ExecRequest {
	pub cmd:  Vec<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cwd:  Option<String>,
	pub env:  BTreeMap<String, String>,
	pub tty:  bool,
	/// User name or uid to run as (agent switches uid/gid and sets HOME).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

/// `mount` request: mount `source` (virtio-fs tag or block device) at `path`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountRequest {
	pub source:    String,
	pub path:      String,
	/// `virtiofs` or a block filesystem such as `ext4`.
	pub fstype:    String,
	pub read_only: bool,
}

/// `net_config` request: static IPv4 config for the guest's `eth0`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NetConfig {
	pub ip:     String,
	pub prefix: u8,
	pub gw:     String,
	pub dns:    Vec<String>,
}

/// Where the reader task delivers frames for one id.
enum Route {
	/// Waiting for a single RESP.
	Request(oneshot::Sender<Result<Value, String>>),
	/// `fs_read`: STDOUT chunks, then RESP.
	Read { data: Vec<u8>, done: oneshot::Sender<Result<Vec<u8>, String>> },
	/// Exec session output.
	Exec(mpsc::UnboundedSender<ExecEvent>),
	/// Tunnel stream.
	Stream(Arc<stream::Shared>),
}

/// Routing table shared by the reader task and client handles.
struct Table {
	routes: Mutex<Routes>,
	tx:     mpsc::UnboundedSender<Vec<u8>>,
}

#[derive(Default)]
struct Routes {
	map:    HashMap<u32, Route>,
	/// Set once the connection is gone; new routes fail immediately.
	closed: Option<String>,
}

impl Table {
	fn send(&self, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
		self
			.tx
			.send(encode(ty, id, payload))
			.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "agent connection closed"))
	}

	fn register(&self, id: u32, route: Route) -> anyhow::Result<()> {
		let mut routes = self.routes.lock();
		if let Some(reason) = &routes.closed {
			bail!("agent connection closed: {reason}");
		}
		routes.map.insert(id, route);
		Ok(())
	}

	fn remove(&self, id: u32) {
		self.routes.lock().map.remove(&id);
	}

	fn dispatch(&self, frame: Frame) {
		let mut routes = self.routes.lock();
		let Some(route) = routes.map.get_mut(&frame.id) else {
			if matches!(frame.ty, proto::FRAME_DATA | proto::FRAME_WINDOW) {
				let _ = self.send(proto::FRAME_CLOSE, frame.id, b"unknown stream");
			}
			return;
		};
		match route {
			Route::Request(_) => {
				if frame.ty == proto::FRAME_RESP
					&& let Some(Route::Request(done)) = routes.map.remove(&frame.id)
				{
					let _ = done.send(parse_response(&frame.payload));
				}
			},
			Route::Read { data, .. } => match frame.ty {
				proto::FRAME_STDOUT => data.extend_from_slice(&frame.payload),
				proto::FRAME_RESP => {
					if let Some(Route::Read { data, done }) = routes.map.remove(&frame.id) {
						let _ = done.send(parse_response(&frame.payload).map(|_| data));
					}
				},
				_ => {},
			},
			Route::Exec(events) => {
				let event = match frame.ty {
					proto::FRAME_STDOUT if !frame.payload.is_empty() => {
						Some(ExecEvent::Stdout(frame.payload))
					},
					proto::FRAME_STDERR if !frame.payload.is_empty() => {
						Some(ExecEvent::Stderr(frame.payload))
					},
					proto::FRAME_EXIT => Some(parse_exit(&frame.payload)),
					proto::FRAME_RESP => parse_response(&frame.payload).err().map(ExecEvent::Failed),
					_ => None,
				};
				if let Some(event) = event {
					let over = matches!(event, ExecEvent::Exit { .. } | ExecEvent::Failed(_));
					let _ = events.send(event);
					if over {
						routes.map.remove(&frame.id);
					}
				}
			},
			Route::Stream(shared) => {
				let shared = Arc::clone(shared);
				if shared.on_frame(frame.ty, &frame.payload, |ty, payload| {
					let _ = self.send(ty, frame.id, payload);
				}) {
					routes.map.remove(&frame.id);
				}
			},
		}
	}

	fn fail_all(&self, reason: &str) {
		let routes = {
			let mut routes = self.routes.lock();
			routes.closed = Some(reason.to_owned());
			std::mem::take(&mut routes.map)
		};
		for (_, route) in routes {
			match route {
				Route::Request(done) => drop(done.send(Err(reason.to_owned()))),
				Route::Read { done, .. } => drop(done.send(Err(reason.to_owned()))),
				Route::Exec(events) => drop(events.send(ExecEvent::Failed(reason.to_owned()))),
				Route::Stream(shared) => shared.connection_lost(reason),
			}
		}
	}
}

fn parse_response(payload: &[u8]) -> Result<Value, String> {
	let value: Value =
		serde_json::from_slice(payload).map_err(|e| format!("malformed agent response: {e}"))?;
	if value.get("ok").and_then(Value::as_bool) == Some(true) {
		return Ok(value);
	}
	let error = value
		.get("error")
		.and_then(Value::as_str)
		.unwrap_or("agent request failed");
	Err(match value.get("errno").and_then(Value::as_i64) {
		Some(errno) => format!("{error} (errno {errno})"),
		None => error.to_owned(),
	})
}

fn parse_exit(payload: &[u8]) -> ExecEvent {
	let value: Value = serde_json::from_slice(payload).unwrap_or(Value::Null);
	let code = value
		.get("code")
		.and_then(Value::as_i64)
		.and_then(|code| i32::try_from(code).ok())
		.unwrap_or(-1);
	let signal = value
		.get("signal")
		.and_then(Value::as_i64)
		.and_then(|signal| i32::try_from(signal).ok());
	ExecEvent::Exit { code, signal }
}

/// State behind every clone of one [`AgentClient`]; dropping the last clone
/// (and every stream) closes the connection.
struct Inner {
	table:   Arc<Table>,
	next_id: AtomicU32,
	epoch:   u64,
	reader:  AbortHandle,
}

impl Drop for Inner {
	fn drop(&mut self) {
		self.reader.abort();
	}
}

/// Request ids start at a random point per connection. Requests written by an
/// earlier connection that timed out (e.g. a `hello` the guest had not read
/// yet) stay queued in the console and are answered later; with ids that
/// restart at 1 their replies would be routed to this connection's requests
/// (a stale `hello` reply carrying the wrong epoch). Random starts make such
/// replies unroutable, so they are dropped.
fn first_request_id() -> u32 {
	rand::random_range(0x0001_0000..0x8000_0000)
}

impl Inner {
	fn new_id(&self) -> u32 {
		loop {
			let id = self.next_id.fetch_add(1, Ordering::Relaxed);
			// 0 is never used and u32::MAX is the VMM's boot-exec id.
			if id != 0 && id != u32::MAX {
				return id;
			}
		}
	}
}

/// Connection to one guest agent (one epoch). Cheap to clone.
#[derive(Clone)]
pub struct AgentClient {
	inner: Arc<Inner>,
}

impl std::fmt::Debug for AgentClient {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AgentClient")
			.field("epoch", &self.inner.epoch)
			.finish_non_exhaustive()
	}
}

impl AgentClient {
	/// Connects to `agent.sock` (retrying while the VMM binds it) and performs
	/// the `hello` handshake within `timeout`. On a cold boot the VMM accepts
	/// clients only after the guest agent announced itself, so this also waits
	/// for the agent.
	pub async fn connect(path: &Path, timeout: Duration) -> anyhow::Result<Self> {
		let deadline = Instant::now() + timeout;
		let stream = loop {
			match tokio::net::UnixStream::connect(path).await {
				Ok(stream) => break stream,
				Err(error) if Instant::now() < deadline => {
					tracing::trace!(%error, path = %path.display(), "agent socket not ready");
					tokio::time::sleep(Duration::from_millis(10)).await;
				},
				Err(error) => {
					return Err(error).with_context(|| format!("connecting {}", path.display()));
				},
			}
		};
		tokio::time::timeout_at(deadline, Self::handshake(stream))
			.await
			.map_err(|_| anyhow!("guest agent did not answer hello within {timeout:?}"))?
	}

	/// Runs the GC4 protocol over an already-connected byte stream: SYNC,
	/// reader/writer tasks, then `hello` with a fresh random epoch.
	pub async fn handshake<S>(stream: S) -> anyhow::Result<Self>
	where
		S: AsyncRead + AsyncWrite + Send + 'static,
	{
		let (mut read_half, mut write_half) = tokio::io::split(stream);
		let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
		let table = Arc::new(Table { routes: Mutex::new(Routes::default()), tx });

		let writer_table = Arc::downgrade(&table);
		tokio::spawn(async move {
			let result = async {
				write_half.write_all(&proto::SYNC).await?;
				while let Some(frame) = rx.recv().await {
					write_half.write_all(&frame).await?;
				}
				write_half.shutdown().await
			}
			.await;
			if let Err(error) = result
				&& let Some(table) = writer_table.upgrade()
			{
				table.fail_all(&format!("agent socket write failed: {error}"));
			}
		});

		let reader_table = Arc::clone(&table);
		let reader = tokio::spawn(async move {
			let reason = loop {
				match read_frame(&mut read_half).await {
					Ok(Some(frame)) => reader_table.dispatch(frame),
					Ok(None) => break "agent connection closed".to_owned(),
					Err(error) => break format!("agent connection failed: {error}"),
				}
			};
			reader_table.fail_all(&reason);
		})
		.abort_handle();

		let epoch = loop {
			let epoch = rand::random::<u64>();
			if epoch != 0 {
				break epoch;
			}
		};
		let client = Self {
			inner: Arc::new(Inner {
				table,
				next_id: AtomicU32::new(first_request_id()),
				epoch,
				reader,
			}),
		};
		let reply = client.request(Op::Hello, json!({ "epoch": epoch })).await?;
		if reply.get("epoch").and_then(Value::as_u64) != Some(epoch) {
			bail!("guest agent echoed the wrong hello epoch: {reply}");
		}
		Ok(client)
	}

	/// Random epoch announced in `hello`.
	pub fn epoch(&self) -> u64 {
		self.inner.epoch
	}

	/// Whether the connection is gone (agent exit, VMM exit, socket error).
	pub fn is_closed(&self) -> bool {
		self.inner.table.routes.lock().closed.is_some()
	}

	/// Sends `op` with the fields of `args` (a JSON object, or `null` for none)
	/// and waits up to [`REQUEST_TIMEOUT`] for the RESP. `ok:false` replies
	/// become errors carrying the agent's message.
	pub async fn request(&self, op: Op, args: Value) -> anyhow::Result<Value> {
		self.request_timeout(op, args, REQUEST_TIMEOUT).await
	}

	/// [`Self::request`] with an explicit timeout.
	pub async fn request_timeout(
		&self,
		op: Op,
		args: Value,
		timeout: Duration,
	) -> anyhow::Result<Value> {
		let payload = request_payload(op, args)?;
		let id = self.inner.new_id();
		let (done, reply) = oneshot::channel();
		self.inner.table.register(id, Route::Request(done))?;
		self.send(proto::FRAME_REQ, id, &payload)?;
		self.await_reply(id, op, reply, timeout).await
	}

	async fn await_reply<T>(
		&self,
		id: u32,
		op: Op,
		reply: oneshot::Receiver<Result<T, String>>,
		timeout: Duration,
	) -> anyhow::Result<T> {
		let op = op.name();
		match tokio::time::timeout(timeout, reply).await {
			Ok(Ok(Ok(value))) => Ok(value),
			Ok(Ok(Err(error))) => Err(anyhow!("agent {op}: {error}")),
			Ok(Err(_)) => Err(anyhow!("agent {op}: connection closed")),
			Err(_) => {
				self.inner.table.remove(id);
				Err(anyhow!("agent {op}: no reply within {timeout:?}"))
			},
		}
	}

	fn send(&self, ty: u8, id: u32, payload: &[u8]) -> anyhow::Result<()> {
		self.inner.table.send(ty, id, payload).map_err(Into::into)
	}

	/// Sends `data` as consecutive frames of type `ty`, each within the payload
	/// limit.
	fn send_chunked(&self, ty: u8, id: u32, data: &[u8]) -> anyhow::Result<()> {
		for chunk in data.chunks(proto::MAX_PAYLOAD_LEN) {
			self.send(ty, id, chunk)?;
		}
		Ok(())
	}

	/// `ping`: liveness plus agent version.
	pub async fn ping(&self) -> anyhow::Result<Value> {
		self.request(Op::Ping, Value::Null).await
	}

	/// Starts a process; output arrives through [`ExecSession::next`].
	#[expect(
		clippy::unused_async,
		clippy::unused_async_trait_impl,
		reason = "async API shared with request-based ops"
	)]
	pub async fn exec(&self, req: &ExecRequest) -> anyhow::Result<ExecSession> {
		if req.cmd.is_empty() {
			bail!("exec needs a command");
		}
		let id = self.inner.new_id();
		let payload = request_payload(Op::Exec, serde_json::to_value(req)?)?;
		let (events_tx, events) = mpsc::unbounded_channel();
		self.inner.table.register(id, Route::Exec(events_tx))?;
		self.send(proto::FRAME_REQ, id, &payload)?;
		Ok(ExecSession { id, client: self.clone(), events })
	}

	/// Writes `data` to `path` (created with `mode` if missing, truncated
	/// otherwise).
	pub async fn fs_write(&self, path: &str, mode: u32, data: &[u8]) -> anyhow::Result<()> {
		let id = self.inner.new_id();
		let payload = request_payload(Op::FsWrite, json!({ "path": path, "mode": mode }))?;
		let (done, reply) = oneshot::channel();
		self.inner.table.register(id, Route::Request(done))?;
		self.send(proto::FRAME_REQ, id, &payload)?;
		self.send_chunked(proto::FRAME_STDIN, id, data)?;
		self.send(proto::FRAME_STDIN, id, &[])?;
		self
			.await_reply(id, Op::FsWrite, reply, REQUEST_TIMEOUT.max(Duration::from_mins(5)))
			.await
			.map(drop)
	}

	/// Reads a whole guest file into memory.
	pub async fn fs_read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
		let id = self.inner.new_id();
		let payload = request_payload(Op::FsRead, json!({ "path": path }))?;
		let (done, reply) = oneshot::channel();
		self
			.inner
			.table
			.register(id, Route::Read { data: Vec::new(), done })?;
		self.send(proto::FRAME_REQ, id, &payload)?;
		self
			.await_reply(id, Op::FsRead, reply, REQUEST_TIMEOUT.max(Duration::from_mins(5)))
			.await
	}

	/// Flushes guest filesystems (`sync(2)`).
	pub async fn fs_sync(&self) -> anyhow::Result<()> {
		self
			.request_timeout(Op::FsSync, Value::Null, Duration::from_mins(2))
			.await
			.map(drop)
	}

	/// Mounts a virtio-fs tag or block device in the guest.
	pub async fn mount(&self, req: &MountRequest) -> anyhow::Result<()> {
		self
			.request(
				Op::Mount,
				json!({
					"tag": req.source,
					"source": req.source,
					"path": req.path,
					"fstype": req.fstype,
					"ro": req.read_only,
				}),
			)
			.await
			.map(drop)
	}

	/// Injects 64 bytes of fresh host CSPRNG entropy and reseeds the guest
	/// kernel CRNG (after a memory restore, whose CRNG state every fork of
	/// the snapshot shares).
	pub async fn reseed(&self) -> anyhow::Result<()> {
		use base64::Engine as _;
		use rand::TryRng as _;
		let mut bytes = [0u8; 64];
		rand::rngs::SysRng.try_fill_bytes(&mut bytes)?;
		self
			.request(
				Op::Reseed,
				json!({
					"bytes": base64::engine::general_purpose::STANDARD.encode(bytes),
				}),
			)
			.await
			.map(drop)
	}

	/// Applies a static IPv4 config to the guest's `eth0`.
	pub async fn net_config(&self, config: &NetConfig) -> anyhow::Result<()> {
		self
			.request(Op::NetConfig, serde_json::to_value(config)?)
			.await
			.map(drop)
	}

	/// Opens a byte stream to guest TCP `127.0.0.1:port` (§8.3).
	pub async fn tcp_connect(&self, port: u16) -> Result<AgentStream, ConnectError> {
		stream::open(&self.inner, port).await
	}
}

/// Serializes a `REQ` payload: the fields of `args` (an object, or `null` for
/// none) plus `op`.
///
/// # Errors
/// `args` is neither an object nor `null`, or it already carries an `op` field.
fn request_payload(op: Op, args: Value) -> anyhow::Result<Vec<u8>> {
	let mut fields = match args {
		Value::Object(fields) => fields,
		Value::Null => serde_json::Map::new(),
		other => bail!("agent {}: arguments must be a JSON object, got {other}", op.name()),
	};
	if fields.insert("op".to_owned(), json!(op)).is_some() {
		bail!("agent {}: arguments must not set `op`", op.name());
	}
	Ok(serde_json::to_vec(&fields)?)
}

/// A running guest process started by [`AgentClient::exec`].
pub struct ExecSession {
	id:     u32,
	client: AgentClient,
	events: mpsc::UnboundedReceiver<ExecEvent>,
}

impl ExecSession {
	/// Next output chunk or the terminal exit/failure; `None` afterwards.
	pub async fn next(&mut self) -> Option<ExecEvent> {
		self.events.recv().await
	}

	/// Writes to the process's stdin.
	#[expect(
		clippy::unused_async,
		clippy::unused_async_trait_impl,
		reason = "async like every other session call; callers must not depend on frames being \
		          queued synchronously"
	)]
	pub async fn write_stdin(&self, data: &[u8]) -> anyhow::Result<()> {
		if data.is_empty() {
			return Ok(());
		}
		self.client.send_chunked(proto::FRAME_STDIN, self.id, data)
	}

	/// Closes stdin (EOF; `^D` for tty sessions).
	#[expect(
		clippy::unused_async,
		clippy::unused_async_trait_impl,
		reason = "async like every other session call"
	)]
	pub async fn close_stdin(&self) -> anyhow::Result<()> {
		self.client.send(proto::FRAME_STDIN, self.id, &[])
	}

	/// Signals the process group.
	#[expect(
		clippy::unused_async,
		clippy::unused_async_trait_impl,
		reason = "async like every other session call"
	)]
	pub async fn kill(&self, signal: i32) -> anyhow::Result<()> {
		self.client.send(
			proto::FRAME_KILL,
			self.id,
			&serde_json::to_vec(&json!({ "signal": signal }))?,
		)
	}

	/// Collects all output until exit: `(stdout, stderr, exit code)`.
	/// A signal-terminated process reports `128 + signal`.
	pub async fn output(mut self) -> anyhow::Result<(Vec<u8>, Vec<u8>, i32)> {
		let mut stdout = Vec::new();
		let mut stderr = Vec::new();
		while let Some(event) = self.next().await {
			match event {
				ExecEvent::Stdout(chunk) => stdout.extend_from_slice(&chunk),
				ExecEvent::Stderr(chunk) => stderr.extend_from_slice(&chunk),
				ExecEvent::Exit { code, signal } => {
					return Ok((stdout, stderr, signal.map_or(code, |signal| 128 + signal)));
				},
				ExecEvent::Failed(error) => bail!("exec failed: {error}"),
			}
		}
		bail!("exec session ended without an exit status")
	}
}

impl Drop for ExecSession {
	fn drop(&mut self) {
		self.client.inner.table.remove(self.id);
	}
}

#[cfg(test)]
mod tests;
