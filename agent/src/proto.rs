//! GC4: the framed agent ↔ host protocol on the guest's virtio-console port
//! [`AGENT_PORT_NAME`].
//!
//! Frame = 9-byte header (`u32 LE payload_len`, `u8 type`, `u32 LE id`) +
//! payload (≤ [`MAX_PAYLOAD_LEN`]). The host writes [`SYNC`] on connect; the
//! agent writes one [`SYNC`] when ready (after envd listens on 49983).
//!
//! Frame types: `REQ=1` (JSON request, `id` chosen by the host), `STDIN=2`,
//! `STDOUT=3`, `STDERR=4` (empty payload = EOF of that stream), `EXIT=5`
//! (`{code, signal}`), `RESP=6` (JSON), `KILL=7` (`{signal?}`), and for TCP
//! tunnels `DATA=8` (raw bytes), `WINDOW=9` (`u32 LE` extra credit),
//! `CLOSE=10` (empty = half-close, non-empty = UTF-8 abort reason).
//!
//! A `REQ` payload is a JSON object whose `op` field names an [`Op`]; each
//! variant documents its fields and reply. Every failed request answers
//! `RESP {ok:false, error, errno?}`.
//!
//! # Tunnels
//! Each side may have at most the peer's advertised window of un-credited
//! bytes in flight; receivers return credit with `WINDOW` after writing data
//! to its destination. A stream ends when both directions sent `CLOSE` (empty)
//! or either side aborted. DATA/WINDOW for an unknown stream id is answered
//! with `CLOSE "unknown stream"`.

use std::{
	collections::VecDeque,
	io::{self, Read, Write},
};

/// Name of the virtio-console port carrying GC4: the VMM announces it with
/// `VIRTIO_CONSOLE_PORT_NAME`, the guest exposes it as
/// `/sys/class/virtio-ports/<dev>/name` for the `/dev/<dev>` character device.
pub const AGENT_PORT_NAME: &str = "vmon.gc4";

/// Name of the virtio-console port for structured guest log lines (`vmon-log:{json}`).
///
/// The VMM appends its output to the console log, so it replaces `/dev/console`
/// (a byte-per-exit UART) for high-rate lines.
pub const LOG_PORT_NAME: &str = "vmon.log";

/// Largest frame payload either side may send.
pub const MAX_PAYLOAD_LEN: usize = 1 << 20;
/// Frame header size in bytes.
pub const HEADER_LEN: usize = 9;

/// Channel resync marker the host writes on connect.
///
/// Its first four bytes are `0xFFFF_FFFF` — an impossible payload length
/// (> `MAX_PAYLOAD_LEN`) — so it can never be confused with a real frame
/// header, letting the reader recover frame alignment after pre-protocol
/// console noise or a reconnect.
pub const SYNC: [u8; HEADER_LEN] = [0xff, 0xff, 0xff, 0xff, 0xff, b'v', b'm', b'o', b'n'];

pub const FRAME_REQ: u8 = 1;
pub const FRAME_STDIN: u8 = 2;
pub const FRAME_STDOUT: u8 = 3;
pub const FRAME_STDERR: u8 = 4;
pub const FRAME_EXIT: u8 = 5;
pub const FRAME_RESP: u8 = 6;
pub const FRAME_KILL: u8 = 7;
/// Tunnel payload bytes for stream `id` (both directions).
pub const FRAME_DATA: u8 = 8;
/// `u32 LE` additional receive credit granted by the sender of this frame.
pub const FRAME_WINDOW: u8 = 9;
/// Empty = half-close (no more DATA from the sender); non-empty = abort.
pub const FRAME_CLOSE: u8 = 10;

/// Initial per-direction tunnel window (1 MiB).
///
/// Credit returns only after a full host ↔ guest round trip through the VMM,
/// so a smaller window starves bulk transfers (256 KiB held 64 MiB uploads
/// near 300 MB/s).
pub const TUNNEL_WINDOW: u32 = 1024 * 1024;

pub const PTY_SCROLLBACK_LIMIT: usize = 1 << 20;

/// Declares [`Op`] from one table of `Variant = "wire_name"` entries so the
/// enum, its wire names, and the name lookup cannot drift apart.
macro_rules! ops {
	($($(#[$doc:meta])* $variant:ident = $name:literal,)*) => {
		/// GC4 request operation: the `op` field of a `REQ` payload.
		///
		/// The single source of op names for the guest dispatcher and every
		/// host caller. Serializes as its wire name, so host code writes
		/// `json!({"op": Op::FsStat, "path": p})`.
		#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
		pub enum Op {
			$($(#[$doc])* $variant,)*
		}

		impl Op {
			/// Every operation, in declaration order.
			pub const ALL: &'static [Self] = &[$(Self::$variant,)*];

			/// Wire name carried in the `op` field.
			pub const fn name(self) -> &'static str {
				match self {
					$(Self::$variant => $name,)*
				}
			}

			/// Parses a wire name; `None` for ops this agent does not know.
			pub fn from_name(name: &str) -> Option<Self> {
				match name {
					$($name => Some(Self::$variant),)*
					_ => None,
				}
			}
		}
	};
}

ops! {
	/// `{epoch≠0}` → `{ok, epoch}`. Starts a new host connection: unbinds
	/// PTYs and aborts every tunnel of the previous one.
	Hello = "hello",
	/// `{}` → `{ok, version, arch}`.
	Ping = "ping",
	/// `{}` → `{ok, cpu_ticks, disk_sectors, network_bytes}` (cumulative).
	Activity = "activity",
	/// `{cmd:[..], cwd?, env?:{K:V}, tty?, user?:name|uid}` → no RESP on
	/// success; `STDOUT`/`STDERR` frames then `EXIT {code, signal}`. The host
	/// sends `STDIN` (empty = EOF) and `KILL`. `user` switches uid/gid/groups
	/// and defaults `HOME`/`USER`/`LOGNAME`.
	Exec = "exec",
	/// `{rows, cols}` on the id of an `exec` with `tty`.
	Resize = "resize",
	/// `{session_id?, cols?, rows?, exec?, workdir?, env?}` → session meta,
	/// then `STDOUT` on the same id.
	PtyOpen = "pty_open",
	/// `{session_id, cols?, rows?}` → session meta, scrollback, retained `EXIT`.
	PtyAttach = "pty_attach",
	/// `{session_id?}` → `{ok}`; unbinds sessions bound to this id.
	PtyDetach = "pty_detach",
	/// `{session_id, cols?, rows?}` → `{ok}`.
	PtyResize = "pty_resize",
	/// `{}` → `{ok, sessions:[meta]}` (exited sessions kept 60 s).
	PtyList = "pty_list",
	/// `{session_id}` → `{ok, session_id, exit_code}`.
	PtyClose = "pty_close",
	/// `{session_id, command, timeout_ms?}` → `{ok, stdout, stderr, code}`.
	PtyExec = "pty_exec",
	/// `{path}` → `STDOUT` chunks, empty `STDOUT`, then `{ok, size}`.
	FsRead = "fs_read",
	/// `{path, mode?}` → host streams `STDIN`; an empty `STDIN` ends it →
	/// `{ok, size}`.
	FsWrite = "fs_write",
	/// `{path}` → `{ok, entries:[{name, type, size, mode, mtime}]}`.
	FsList = "fs_list",
	/// `{path}` → `{ok, type, size, mode, mtime}`.
	FsStat = "fs_stat",
	/// `{path, parents?=true}` → `{ok}`.
	FsMkdir = "fs_mkdir",
	/// `{path, recursive?}` → `{ok}`.
	FsRemove = "fs_remove",
	/// `{}` → `{ok}` after `sync(2)`.
	FsSync = "fs_sync",
	/// `{ip, prefix, gw, dns?:[..]}` → `{ok}`; replaces every IPv4 address of
	/// `eth0` (fork/restore safe) and the default route.
	NetConfig = "net_config",
	/// `{port, host?}` → `{ok, connected}`.
	TcpProbe = "tcp_probe",
	/// `{port, host?="127.0.0.1", window}` → `{ok, window}` or `{ok:false,
	/// error, errno}` (111 = ECONNREFUSED = port not open). Opens a byte
	/// stream on the request id; see [`StreamFlow`].
	TcpConnect = "tcp_connect",
	/// `{source|tag, path, fstype?="virtiofs", ro?, options?}` → `{ok}`.
	Mount = "mount",
	/// `{tag, path}` → `{ok}`: virtiofs lower plus a tmpfs-backed upper under
	/// `/var/lib/vmon/s3/<tag>`.
	MountOverlay = "mount_overlay",
	/// `{layers:[path], dest}` → `{ok, bytes, files, whiteouts}`; applies
	/// gzip/zstd/plain layer tars in order with OCI whiteouts, ownership,
	/// modes, mtimes, xattrs, hardlinks, and devices.
	OciUnpack = "oci_unpack",
	/// `{root, user?="user", uid?=1000, sudo?=true, hostname?="vibemon",
	/// agent_src?="/proc/self/exe", metadata?:{K:V}}` → `{ok, user, uid, gid,
	/// home, shell}`.
	Provision = "provision",
	/// `{unix_ns}` → `{ok}` (`CLOCK_REALTIME`).
	SetTime = "set_time",
	/// `{bytes:base64}` → `{ok}`: credits exactly 64 bytes of host entropy
	/// (`RNDADDENTROPY`) and reseeds the kernel CRNG (`RNDRESEEDCRNG`). Sent
	/// after every memory restore.
	Reseed = "reseed",
	/// `{}` → `{ok}`, then sync, unmount, and power off.
	Poweroff = "poweroff",
}

impl serde::Serialize for Op {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(self.name())
	}
}

impl<'de> serde::Deserialize<'de> for Op {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		let name = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
		Self::from_name(&name).ok_or_else(|| serde::de::Error::custom(format!("unknown op {name:?}")))
	}
}

/// Encode a `WINDOW` payload; used by both GC4 endpoints.
pub const fn encode_window(credit: u32) -> [u8; 4] {
	credit.to_le_bytes()
}

/// Decode a `WINDOW` payload; `None` unless it is exactly 4 bytes.
pub fn decode_window(payload: &[u8]) -> Option<u32> {
	Some(u32::from_le_bytes(payload.try_into().ok()?))
}

/// Meaning of a received `CLOSE` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseKind {
	/// Peer will send no more DATA.
	HalfClose,
	/// Peer reset the stream with this reason.
	Abort(String),
}

impl CloseKind {
	/// Classify a `CLOSE` payload (empty = half-close).
	pub fn parse(payload: &[u8]) -> Self {
		if payload.is_empty() {
			Self::HalfClose
		} else {
			Self::Abort(String::from_utf8_lossy(payload).into_owned())
		}
	}
}

/// Tunnel protocol violations detected by [`StreamFlow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowError {
	/// Sender attempted more bytes than its credit.
	CreditExceeded,
	/// Peer sent more DATA than our advertised window allows.
	WindowExceeded,
	/// DATA after the peer's half-close.
	DataAfterClose,
	/// DATA queued after our own half-close or after abort.
	SendAfterClose,
	/// Credit overflowed `u64` (malicious WINDOW flood).
	CreditOverflow,
}

impl std::fmt::Display for FlowError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(match self {
			Self::CreditExceeded => "send credit exceeded",
			Self::WindowExceeded => "window exceeded",
			Self::DataAfterClose => "data after close",
			Self::SendAfterClose => "send after close",
			Self::CreditOverflow => "credit overflow",
		})
	}
}

/// Credit-based flow control and close state of one tunnel stream; shared by
/// the agent's tunnel runtime and the host GC4 client so both enforce the
/// same rules.
#[derive(Debug, Clone)]
pub struct StreamFlow {
	send_credit:   u64,
	recv_window:   u32,
	recv_inflight: u64,
	pending_grant: u64,
	local_closed:  bool,
	remote_closed: bool,
	aborted:       bool,
}

impl StreamFlow {
	/// `send_credit` = peer's advertised window; `recv_window` = ours.
	pub const fn new(send_credit: u32, recv_window: u32) -> Self {
		Self {
			send_credit: send_credit as u64,
			recv_window,
			recv_inflight: 0,
			pending_grant: 0,
			local_closed: false,
			remote_closed: false,
			aborted: false,
		}
	}

	/// How many of `want` bytes may be sent now in one DATA frame.
	pub fn sendable(&self, want: usize) -> usize {
		if self.local_closed || self.aborted {
			return 0;
		}
		let credit = usize::try_from(self.send_credit).unwrap_or(usize::MAX);
		want.min(credit).min(MAX_PAYLOAD_LEN)
	}

	/// Remaining send credit in bytes.
	pub const fn send_credit(&self) -> u64 {
		self.send_credit
	}

	/// Account `n` bytes sent in a DATA frame.
	pub const fn on_sent(&mut self, n: usize) -> Result<(), FlowError> {
		if self.local_closed || self.aborted {
			return Err(FlowError::SendAfterClose);
		}
		let n = n as u64;
		if n > self.send_credit {
			return Err(FlowError::CreditExceeded);
		}
		self.send_credit -= n;
		Ok(())
	}

	/// Add credit from a received WINDOW frame.
	pub fn on_window(&mut self, credit: u32) -> Result<(), FlowError> {
		self.send_credit = self
			.send_credit
			.checked_add(u64::from(credit))
			.ok_or(FlowError::CreditOverflow)?;
		Ok(())
	}

	/// Account a received DATA frame of `n` bytes against our window.
	pub fn on_data(&mut self, n: usize) -> Result<(), FlowError> {
		if self.remote_closed {
			return Err(FlowError::DataAfterClose);
		}
		let next = self.recv_inflight + n as u64;
		if next > u64::from(self.recv_window) {
			return Err(FlowError::WindowExceeded);
		}
		self.recv_inflight = next;
		Ok(())
	}

	/// Record `n` received bytes delivered to their destination; returns the
	/// credit to grant with a WINDOW frame once at least a quarter of the
	/// window is pending (batched, never more than was received). No credit is
	/// granted once the peer half-closed or the stream aborted.
	pub fn on_consumed(&mut self, n: usize) -> Option<u32> {
		let n = (n as u64).min(self.recv_inflight - self.pending_grant);
		self.pending_grant += n;
		let threshold = u64::from((self.recv_window / 4).max(1));
		if self.pending_grant >= threshold && !self.remote_closed {
			let grant = self.pending_grant;
			self.pending_grant = 0;
			self.recv_inflight -= grant;
			return Some(u32::try_from(grant).unwrap_or(u32::MAX));
		}
		None
	}

	/// Mark our direction half-closed; returns `false` if already closed.
	pub const fn close_local(&mut self) -> bool {
		!std::mem::replace(&mut self.local_closed, true)
	}

	/// Apply a received CLOSE frame.
	pub const fn on_close(&mut self, kind: &CloseKind) {
		match kind {
			CloseKind::HalfClose => self.remote_closed = true,
			CloseKind::Abort(_) => self.abort(),
		}
	}

	/// Reset the stream (either side).
	pub const fn abort(&mut self) {
		self.aborted = true;
		self.local_closed = true;
		self.remote_closed = true;
	}

	/// Whether the peer half-closed or the stream was aborted.
	pub const fn remote_closed(&self) -> bool {
		self.remote_closed
	}

	/// Whether the stream was aborted.
	pub const fn aborted(&self) -> bool {
		self.aborted
	}

	/// Both directions closed (or aborted): the stream can be forgotten.
	pub const fn is_finished(&self) -> bool {
		self.local_closed && self.remote_closed
	}
}

/// Identifies a stream within one host connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PtyBinding {
	pub epoch:    u64,
	pub frame_id: u32,
}

/// Byte-bounded PTY history. Oldest bytes are discarded first.
#[derive(Debug)]
pub struct Scrollback {
	bytes: VecDeque<u8>,
	limit: usize,
}

impl Scrollback {
	pub const fn new(limit: usize) -> Self {
		Self { bytes: VecDeque::new(), limit }
	}

	pub fn push(&mut self, chunk: &[u8]) {
		if self.limit == 0 {
			self.bytes.clear();
			return;
		}
		if chunk.len() >= self.limit {
			self.bytes.clear();
			self
				.bytes
				.extend(chunk[chunk.len() - self.limit..].iter().copied());
			return;
		}
		let overflow = self
			.bytes
			.len()
			.saturating_add(chunk.len())
			.saturating_sub(self.limit);
		self.bytes.drain(..overflow);
		self.bytes.extend(chunk.iter().copied());
	}

	pub fn snapshot(&self) -> Vec<u8> {
		self.bytes.iter().copied().collect()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
	pub ty:      u8,
	pub id:      u32,
	pub payload: Vec<u8>,
}

#[cfg(test)]
impl Frame {
	pub fn new(ty: u8, id: u32, payload: Vec<u8>) -> Self {
		Self { ty, id, payload }
	}
}

pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<Option<Frame>> {
	loop {
		let mut header = [0u8; HEADER_LEN];
		header[0] = match read_first_byte(reader)? {
			Some(byte) => byte,
			None => return Ok(None),
		};
		reader.read_exact(&mut header[1..])?;

		// A SYNC marker is not a frame: skip it and keep reading. The host emits
		// one on connect, so an in-stream marker (e.g. after a reconnect) simply
		// realigns the stream.
		if header == SYNC {
			continue;
		}

		let payload_len = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
		if payload_len > MAX_PAYLOAD_LEN {
			// A length this large means the stream is desynced — pre-protocol
			// console noise, or a partial/duplicate SYNC marker. Recover to the
			// next SYNC instead of erroring, which would tear down the channel and
			// (since the agent is guest init) panic the kernel.
			if !resync_to_marker(reader)? {
				return Ok(None);
			}
			continue;
		}

		let ty = header[4];
		let id = u32::from_le_bytes([header[5], header[6], header[7], header[8]]);
		let mut payload = vec![0u8; payload_len];
		if payload_len != 0 {
			reader.read_exact(&mut payload)?;
		}

		return Ok(Some(Frame { ty, id, payload }));
	}
}

/// Read and discard bytes until the [`SYNC`] marker is seen.
///
/// Recovers frame alignment past arbitrary pre-protocol noise (e.g.
/// guest-kernel console output some kernels emit on the virtio-console before
/// the agent owns it). Returns `Ok(false)` on EOF before a marker is found.
pub fn resync_to_marker<R: Read>(reader: &mut R) -> io::Result<bool> {
	let mut window = [0u8; HEADER_LEN];
	let mut filled = 0usize;
	let mut byte = [0u8; 1];
	loop {
		match reader.read(&mut byte) {
			Ok(0) => return Ok(false),
			Ok(_) => {},
			Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
			Err(err) => return Err(err),
		}
		if filled < HEADER_LEN {
			window[filled] = byte[0];
			filled += 1;
		} else {
			window.rotate_left(1);
			window[HEADER_LEN - 1] = byte[0];
		}
		if filled == HEADER_LEN && window == SYNC {
			return Ok(true);
		}
	}
}

pub fn write_frame<W: Write>(writer: &mut W, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
	if payload.len() > MAX_PAYLOAD_LEN {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("frame payload length {} exceeds {}", payload.len(), MAX_PAYLOAD_LEN),
		));
	}

	// Header and payload go out in one write: on the agent's virtio port every
	// write becomes its own virtqueue buffer and doorbell (a VM exit).
	let mut frame = Vec::with_capacity(HEADER_LEN + payload.len());
	frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
	frame.push(ty);
	frame.extend_from_slice(&id.to_le_bytes());
	frame.extend_from_slice(payload);
	writer.write_all(&frame)
}

fn read_first_byte<R: Read>(reader: &mut R) -> io::Result<Option<u8>> {
	let mut byte = [0u8; 1];
	loop {
		match reader.read(&mut byte) {
			Ok(0) => return Ok(None),
			Ok(1) => return Ok(Some(byte[0])),
			Ok(_) => unreachable!("one-byte buffer cannot read more than one byte"),
			Err(err) if err.kind() == io::ErrorKind::Interrupted => {},
			Err(err) => return Err(err),
		}
	}
}

#[cfg(test)]
mod tests {
	use std::io::Cursor;

	use super::*;

	#[test]
	fn every_op_round_trips_through_its_wire_name() {
		for op in Op::ALL {
			assert_eq!(Op::from_name(op.name()), Some(*op), "{op:?}");
			let json = serde_json::to_value(op).unwrap();
			assert_eq!(serde_json::from_value::<Op>(json).unwrap(), *op);
		}
		assert_eq!(Op::from_name("bogus"), None);
		assert!(serde_json::from_value::<Op>(serde_json::json!("bogus")).is_err());
	}

	#[test]
	fn frame_type_constants_match_contract() {
		assert_eq!(FRAME_REQ, 1);
		assert_eq!(FRAME_STDIN, 2);
		assert_eq!(FRAME_STDOUT, 3);
		assert_eq!(FRAME_STDERR, 4);
		assert_eq!(FRAME_EXIT, 5);
		assert_eq!(FRAME_RESP, 6);
		assert_eq!(FRAME_KILL, 7);
	}

	#[test]
	fn tunnel_frames_round_trip() {
		let mut bytes = Vec::new();
		write_frame(&mut bytes, FRAME_DATA, 5, b"abc").unwrap();
		write_frame(&mut bytes, FRAME_WINDOW, 5, &encode_window(70_000)).unwrap();
		write_frame(&mut bytes, FRAME_CLOSE, 5, b"").unwrap();
		write_frame(&mut bytes, FRAME_CLOSE, 6, b"reset").unwrap();
		let mut cur = Cursor::new(bytes);
		assert_eq!(read_frame(&mut cur).unwrap().unwrap(), Frame::new(8, 5, b"abc".to_vec()));
		let win = read_frame(&mut cur).unwrap().unwrap();
		assert_eq!((win.ty, decode_window(&win.payload)), (9, Some(70_000)));
		let half = read_frame(&mut cur).unwrap().unwrap();
		assert_eq!((half.ty, CloseKind::parse(&half.payload)), (10, CloseKind::HalfClose));
		let abort = read_frame(&mut cur).unwrap().unwrap();
		assert_eq!(CloseKind::parse(&abort.payload), CloseKind::Abort("reset".into()));
		assert!(read_frame(&mut cur).unwrap().is_none());
	}

	#[test]
	fn decode_window_rejects_wrong_length() {
		assert_eq!(decode_window(&[1, 0, 0]), None);
		assert_eq!(decode_window(&[1, 0, 0, 0, 0]), None);
	}

	#[test]
	fn sender_never_exceeds_credit() {
		let mut flow = StreamFlow::new(10, TUNNEL_WINDOW);
		assert_eq!(flow.sendable(100), 10);
		flow.on_sent(6).unwrap();
		assert_eq!(flow.sendable(100), 4);
		assert_eq!(flow.on_sent(5), Err(FlowError::CreditExceeded));
		flow.on_sent(4).unwrap();
		assert_eq!(flow.sendable(1), 0);
		flow.on_window(3).unwrap();
		assert_eq!(flow.sendable(100), 3);
	}

	#[test]
	fn sendable_caps_at_max_payload() {
		let mut flow = StreamFlow::new(0, 1);
		flow.on_window(u32::MAX).unwrap();
		flow.on_window(u32::MAX).unwrap();
		assert_eq!(flow.sendable(usize::MAX), MAX_PAYLOAD_LEN);
	}

	#[test]
	fn receiver_rejects_data_beyond_window() {
		let mut flow = StreamFlow::new(0, 100);
		flow.on_data(60).unwrap();
		flow.on_data(40).unwrap();
		assert_eq!(flow.on_data(1), Err(FlowError::WindowExceeded));
	}

	#[test]
	fn credit_grant_is_batched_and_restores_window() {
		let mut flow = StreamFlow::new(0, 100);
		flow.on_data(100).unwrap();
		assert_eq!(flow.on_consumed(10), None);
		assert_eq!(flow.on_consumed(15), Some(25));
		// 25 bytes of window restored: exactly 25 more may arrive.
		flow.on_data(25).unwrap();
		assert_eq!(flow.on_data(1), Err(FlowError::WindowExceeded));
		// Consuming more than was received never over-grants.
		assert_eq!(flow.on_consumed(1000), Some(100));
		flow.on_data(100).unwrap();
		assert_eq!(flow.on_data(1), Err(FlowError::WindowExceeded));
	}

	#[test]
	fn full_window_consumed_always_grants() {
		// Tiny window: threshold max(1) still guarantees progress.
		let mut flow = StreamFlow::new(0, 3);
		flow.on_data(3).unwrap();
		assert_eq!(flow.on_consumed(3), Some(3));
	}

	#[test]
	fn half_close_transitions() {
		let mut flow = StreamFlow::new(10, 10);
		assert!(flow.close_local());
		assert!(!flow.close_local());
		assert_eq!(flow.sendable(5), 0);
		assert_eq!(flow.on_sent(1), Err(FlowError::SendAfterClose));
		assert!(!flow.is_finished());
		// Peer may still send until it half-closes.
		flow.on_data(4).unwrap();
		flow.on_close(&CloseKind::HalfClose);
		// Credit is pointless after the peer stopped sending.
		assert_eq!(flow.on_consumed(4), None);
		assert_eq!(flow.on_data(1), Err(FlowError::DataAfterClose));
		assert!(flow.is_finished());
		assert!(!flow.aborted());
	}

	#[test]
	fn abort_finishes_both_directions() {
		let mut flow = StreamFlow::new(10, 10);
		flow.on_data(10).unwrap();
		flow.on_close(&CloseKind::Abort("boom".into()));
		assert!(flow.aborted() && flow.is_finished());
		assert_eq!(flow.sendable(5), 0);
		assert_eq!(flow.on_consumed(10), None);
	}

	#[test]
	fn window_flood_overflow_is_an_error() {
		let mut flow = StreamFlow::new(0, 1);
		flow.send_credit = u64::MAX - 1;
		assert_eq!(flow.on_window(2), Err(FlowError::CreditOverflow));
	}

	#[test]
	fn write_frame_uses_little_endian_layout() {
		let mut out = Vec::new();
		write_frame(&mut out, FRAME_REQ, 0x0a0b0c0d, b"hi").unwrap();

		assert_eq!(out, vec![
			2, 0, 0, 0, // payload length
			FRAME_REQ, 0x0d, 0x0c, 0x0b, 0x0a, // id
			b'h', b'i'
		]);
	}

	#[test]
	fn read_frame_round_trips() {
		let mut bytes = Vec::new();
		write_frame(&mut bytes, FRAME_STDOUT, 42, b"payload").unwrap();

		let frame = read_frame(&mut Cursor::new(bytes)).unwrap().unwrap();
		assert_eq!(frame, Frame::new(FRAME_STDOUT, 42, b"payload".to_vec()));
	}

	#[test]
	fn read_frame_returns_none_on_clean_eof() {
		assert!(
			read_frame(&mut Cursor::new(Vec::<u8>::new()))
				.unwrap()
				.is_none()
		);
	}

	#[test]
	fn write_frame_rejects_oversized_payload() {
		let payload = vec![0u8; MAX_PAYLOAD_LEN + 1];
		let err = write_frame(&mut Vec::new(), FRAME_REQ, 1, &payload).unwrap_err();
		assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
	}

	#[test]
	fn read_frame_resyncs_past_oversized_garbage_to_next_marker() {
		// An oversized length (pre-protocol console noise / desync) is not fatal:
		// the reader scans to the next SYNC marker and returns the following
		// frame.
		let mut bytes = Vec::new();
		bytes.extend_from_slice(&((MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes());

		bytes.push(FRAME_REQ);
		bytes.extend_from_slice(&7u32.to_le_bytes());
		bytes.extend_from_slice(&SYNC);
		write_frame(&mut bytes, FRAME_STDOUT, 9, b"ok").unwrap();

		let frame = read_frame(&mut Cursor::new(bytes)).unwrap().unwrap();
		assert_eq!(frame, Frame::new(FRAME_STDOUT, 9, b"ok".to_vec()));
	}
	#[test]
	fn scrollback_discards_oldest_bytes_at_bound() {
		let mut ring = Scrollback::new(5);
		ring.push(b"abc");
		ring.push(b"defg");
		assert_eq!(ring.snapshot(), b"cdefg");
		ring.push(b"012345");
		assert_eq!(ring.snapshot(), b"12345");
	}

	#[test]
	fn pty_binding_includes_connection_epoch() {
		let stale = PtyBinding { epoch: 7, frame_id: 1 };
		let current = PtyBinding { epoch: 8, frame_id: 1 };
		assert_ne!(stale, current);
		assert_eq!(current, PtyBinding { epoch: 8, frame_id: 1 });
	}

	#[test]
	fn read_frame_skips_inline_sync_marker() {
		let mut bytes = Vec::new();
		bytes.extend_from_slice(&SYNC);
		write_frame(&mut bytes, FRAME_REQ, 1, b"x").unwrap();
		let frame = read_frame(&mut Cursor::new(bytes)).unwrap().unwrap();
		assert_eq!(frame, Frame::new(FRAME_REQ, 1, b"x".to_vec()));
	}

	#[test]
	fn read_frame_returns_none_when_oversized_then_eof() {
		let mut bytes = Vec::new();
		bytes.extend_from_slice(&((MAX_PAYLOAD_LEN as u32) + 1).to_le_bytes());
		bytes.push(FRAME_REQ);
		bytes.extend_from_slice(&7u32.to_le_bytes());
		assert!(read_frame(&mut Cursor::new(bytes)).unwrap().is_none());
	}

	#[test]
	fn resync_to_marker_skips_noise_and_aligns() {
		let mut bytes = Vec::new();
		bytes.extend_from_slice(b"kernel boot noise \x00\xfe garbage");
		bytes.extend_from_slice(&SYNC);
		write_frame(&mut bytes, FRAME_REQ, 3, b"hi").unwrap();
		let mut cur = Cursor::new(bytes);
		assert!(resync_to_marker(&mut cur).unwrap());
		let frame = read_frame(&mut cur).unwrap().unwrap();
		assert_eq!(frame, Frame::new(FRAME_REQ, 3, b"hi".to_vec()));
	}

	#[test]
	fn resync_to_marker_returns_false_without_marker() {
		let mut cur = Cursor::new(b"no marker present here".to_vec());
		assert!(!resync_to_marker(&mut cur).unwrap());
	}
}
