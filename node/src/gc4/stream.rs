//! GC4 tunnel streams (`tcp_connect`, ARCHITECTURE.md §8.3) exposed as
//! [`AgentStream`]: an `AsyncRead + AsyncWrite` byte stream to a guest TCP
//! port, multiplexed over the shared agent channel with credit-based flow
//! control ([`proto::StreamFlow`]) so one slow consumer never stalls others.

use std::{
	collections::VecDeque,
	io,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::{
	io::{AsyncRead, AsyncWrite, ReadBuf},
	sync::oneshot,
};
use vmon_agent::proto::{self, CloseKind, Op, StreamFlow};

use super::{Inner, REQUEST_TIMEOUT, Route};

/// Largest DATA frame the host sends; keeps the shared channel interleaved.
pub const DATA_CHUNK: usize = 64 * 1024;
/// `ECONNREFUSED` on Linux guests.
const ECONNREFUSED: i64 = 111;

/// Why [`super::AgentClient::tcp_connect`] failed.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
	/// Nothing listens on the guest port.
	#[error("port is not open in the guest")]
	Refused,
	/// Any other failure (agent error, timeout, lost connection).
	#[error("guest tcp_connect failed: {0}")]
	Failed(String),
}

/// Per-stream state shared between the reader task and the stream handle.
pub(super) struct Shared {
	state: Mutex<State>,
}

struct State {
	flow:         StreamFlow,
	/// Pending `tcp_connect` reply.
	open:         Option<oneshot::Sender<Result<(), ConnectError>>>,
	recv:         VecDeque<Bytes>,
	abort_reason: Option<String>,
	/// Our write side was shut down.
	shut:         bool,
	read_waker:   Option<Waker>,
	write_waker:  Option<Waker>,
}

impl State {
	fn wake_all(&mut self) {
		if let Some(waker) = self.read_waker.take() {
			waker.wake();
		}
		if let Some(waker) = self.write_waker.take() {
			waker.wake();
		}
	}

	fn abort(&mut self, reason: &str) {
		self.flow.abort();
		self.abort_reason.get_or_insert_with(|| reason.to_owned());
		if let Some(open) = self.open.take() {
			let _ = open.send(Err(ConnectError::Failed(reason.to_owned())));
		}
		self.wake_all();
	}
}

impl Shared {
	/// Applies one frame for this stream; `reply` sends a frame back on the
	/// same id. Returns `true` when the stream is finished and its route can
	/// be dropped.
	pub(super) fn on_frame(&self, ty: u8, payload: &[u8], reply: impl Fn(u8, &[u8])) -> bool {
		let mut state = self.state.lock();
		match ty {
			proto::FRAME_RESP => {
				let Some(open) = state.open.take() else {
					return false;
				};
				let value: Value = serde_json::from_slice(payload).unwrap_or(Value::Null);
				if value.get("ok").and_then(Value::as_bool) == Some(true) {
					let window = value
						.get("window")
						.and_then(Value::as_u64)
						.and_then(|window| u32::try_from(window).ok())
						.unwrap_or(proto::TUNNEL_WINDOW);
					if state.flow.on_window(window).is_err() {
						state.abort("invalid tunnel window");
						return true;
					}
					let _ = open.send(Ok(()));
					return false;
				}
				let error = if value.get("errno").and_then(Value::as_i64) == Some(ECONNREFUSED) {
					ConnectError::Refused
				} else {
					ConnectError::Failed(
						value
							.get("error")
							.and_then(Value::as_str)
							.unwrap_or("tcp_connect failed")
							.to_owned(),
					)
				};
				state.flow.abort();
				let _ = open.send(Err(error));
				true
			},
			proto::FRAME_DATA => {
				if let Err(error) = state.flow.on_data(payload.len()) {
					let reason = error.to_string();
					reply(proto::FRAME_CLOSE, reason.as_bytes());
					state.abort(&reason);
					return true;
				}
				if !payload.is_empty() {
					state.recv.push_back(Bytes::copy_from_slice(payload));
				}
				if let Some(waker) = state.read_waker.take() {
					waker.wake();
				}
				false
			},
			proto::FRAME_WINDOW => {
				let Some(credit) = proto::decode_window(payload) else {
					reply(proto::FRAME_CLOSE, b"malformed window");
					state.abort("malformed window");
					return true;
				};
				if let Err(error) = state.flow.on_window(credit) {
					let reason = error.to_string();
					reply(proto::FRAME_CLOSE, reason.as_bytes());
					state.abort(&reason);
					return true;
				}
				if let Some(waker) = state.write_waker.take() {
					waker.wake();
				}
				false
			},
			proto::FRAME_CLOSE => {
				let kind = CloseKind::parse(payload);
				if let CloseKind::Abort(reason) = &kind {
					state.abort(reason);
				} else {
					state.flow.on_close(&kind);
					state.wake_all();
				}
				state.flow.is_finished()
			},
			_ => false,
		}
	}

	/// The agent connection died: fail the stream.
	pub(super) fn connection_lost(&self, reason: &str) {
		self.state.lock().abort(reason);
	}
}

/// Sends `tcp_connect` and waits for the agent's verdict.
pub(super) async fn open(inner: &Arc<Inner>, port: u16) -> Result<AgentStream, ConnectError> {
	let id = inner.new_id();
	let (open_tx, open_rx) = oneshot::channel();
	let shared = Arc::new(Shared {
		state: Mutex::new(State {
			flow:         StreamFlow::new(0, proto::TUNNEL_WINDOW),
			open:         Some(open_tx),
			recv:         VecDeque::new(),
			abort_reason: None,
			shut:         false,
			read_waker:   None,
			write_waker:  None,
		}),
	});
	inner
		.table
		.register(id, Route::Stream(Arc::clone(&shared)))
		.map_err(|e| ConnectError::Failed(e.to_string()))?;
	let args = json!({ "port": port, "host": "127.0.0.1", "window": proto::TUNNEL_WINDOW });
	let payload = match super::request_payload(Op::TcpConnect, args) {
		Ok(payload) => payload,
		Err(error) => {
			inner.table.remove(id);
			return Err(ConnectError::Failed(error.to_string()));
		},
	};
	if let Err(error) = inner.table.send(proto::FRAME_REQ, id, &payload) {
		inner.table.remove(id);
		return Err(ConnectError::Failed(error.to_string()));
	}
	let verdict = match tokio::time::timeout(REQUEST_TIMEOUT, open_rx).await {
		Ok(Ok(verdict)) => verdict,
		Ok(Err(_)) => Err(ConnectError::Failed("agent connection closed".into())),
		Err(_) => {
			Err(ConnectError::Failed(format!("no tcp_connect reply within {REQUEST_TIMEOUT:?}")))
		},
	};
	match verdict {
		Ok(()) => Ok(AgentStream { id, shared, inner: Arc::clone(inner) }),
		Err(error) => {
			inner.table.remove(id);
			Err(error)
		},
	}
}

/// Byte stream to a guest TCP port over GC4. Dropping it without
/// `shutdown` resets the guest connection.
pub struct AgentStream {
	id:     u32,
	shared: Arc<Shared>,
	inner:  Arc<Inner>,
}

impl std::fmt::Debug for AgentStream {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AgentStream")
			.field("id", &self.id)
			.finish_non_exhaustive()
	}
}

impl AgentStream {
	fn send(&self, ty: u8, payload: &[u8]) -> io::Result<()> {
		self.inner.table.send(ty, self.id, payload)
	}
}

impl AsyncRead for AgentStream {
	fn poll_read(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		let this = self.get_mut();
		let mut state = this.shared.state.lock();
		if let Some(front) = state.recv.front_mut() {
			let n = front.len().min(buf.remaining());
			buf.put_slice(&front[..n]);
			front.advance(n);
			if front.is_empty() {
				state.recv.pop_front();
			}
			if let Some(grant) = state.flow.on_consumed(n) {
				this.send(proto::FRAME_WINDOW, &proto::encode_window(grant))?;
			}
			return Poll::Ready(Ok(()));
		}
		if let Some(reason) = &state.abort_reason {
			return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, reason.clone())));
		}
		if state.flow.remote_closed() {
			return Poll::Ready(Ok(()));
		}
		state.read_waker = Some(cx.waker().clone());
		Poll::Pending
	}
}

impl AsyncWrite for AgentStream {
	fn poll_write(
		self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		let this = self.get_mut();
		let mut state = this.shared.state.lock();
		if let Some(reason) = &state.abort_reason {
			return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, reason.clone())));
		}
		if state.shut {
			return Poll::Ready(Err(io::Error::new(
				io::ErrorKind::BrokenPipe,
				"tunnel write side closed",
			)));
		}
		if buf.is_empty() {
			return Poll::Ready(Ok(0));
		}
		let n = state.flow.sendable(buf.len().min(DATA_CHUNK));
		if n == 0 {
			state.write_waker = Some(cx.waker().clone());
			return Poll::Pending;
		}
		state
			.flow
			.on_sent(n)
			.map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e.to_string()))?;
		this.send(proto::FRAME_DATA, &buf[..n])?;
		Poll::Ready(Ok(n))
	}

	fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Poll::Ready(Ok(()))
	}

	fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		let this = self.get_mut();
		let mut state = this.shared.state.lock();
		state.shut = true;
		if state.abort_reason.is_none() && state.flow.close_local() {
			this.send(proto::FRAME_CLOSE, &[])?;
		}
		if state.flow.is_finished() {
			drop(state);
			this.inner.table.remove(this.id);
		}
		Poll::Ready(Ok(()))
	}
}

impl Drop for AgentStream {
	fn drop(&mut self) {
		let mut state = self.shared.state.lock();
		if !state.flow.is_finished() {
			let reason = "closed by host";
			let _ = self
				.inner
				.table
				.send(proto::FRAME_CLOSE, self.id, reason.as_bytes());
			state.flow.abort();
		}
		drop(state);
		self.inner.table.remove(self.id);
	}
}
