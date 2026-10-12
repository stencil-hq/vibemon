//! Agent side of GC4 TCP tunnels (`tcp_connect` + `DATA`/`WINDOW`/`CLOSE`).
//!
//! Each stream owns two threads: a socket→host pump that only reads as many
//! bytes as it has credit for, and a host→socket writer fed by a queue that
//! flow control bounds to the advertised window. The GC4 reader thread never
//! blocks on a socket, so a slow guest peer cannot stall other streams or the
//! control channel.

use std::{
	collections::HashMap,
	io::{self, Read, Write},
	net::{Shutdown, TcpStream, ToSocketAddrs},
	sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, mpsc},
	thread,
	time::Duration,
};

use serde_json::{Value, json};

use crate::proto::{self, CloseKind, Frame, StreamFlow};

/// Read chunk for the socket→host pump.
const CHUNK: usize = 64 * 1024;
/// Connect timeout for `tcp_connect`.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Destination for outgoing frames (the shared agent-port writer).
pub trait FrameSink: Send + Sync {
	/// Write one complete frame.
	fn send(&self, ty: u8, id: u32, payload: &[u8]) -> io::Result<()>;
}

impl<W: Write + Send> FrameSink for Mutex<W> {
	fn send(&self, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
		let mut writer = lock(self);
		proto::write_frame(&mut *writer, ty, id, payload)?;
		writer.flush()
	}
}

/// Parsed `tcp_connect` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectRequest {
	/// Guest-side host to dial (default `127.0.0.1`).
	pub host:   String,
	/// Guest-side TCP port.
	pub port:   u16,
	/// Host's receive window = our initial send credit.
	pub window: u32,
}

impl ConnectRequest {
	/// Parse `{port, host?, window?}` (window defaults to
	/// [`proto::TUNNEL_WINDOW`]).
	pub fn from_json(request: &Value) -> Result<Self, String> {
		let port = request
			.get("port")
			.and_then(Value::as_u64)
			.and_then(|port| u16::try_from(port).ok())
			.filter(|port| *port != 0)
			.ok_or("port must be 1..=65535")?;
		let window = match request.get("window") {
			None | Some(Value::Null) => proto::TUNNEL_WINDOW,
			Some(value) => value
				.as_u64()
				.and_then(|window| u32::try_from(window).ok())
				.filter(|window| *window != 0)
				.ok_or("window must be 1..=u32::MAX")?,
		};
		let host = request
			.get("host")
			.and_then(Value::as_str)
			.unwrap_or("127.0.0.1")
			.to_owned();
		Ok(Self { host, port, window })
	}
}

enum Inbound {
	Data(Vec<u8>),
	HalfClose,
}

struct Stream {
	flow:   Mutex<StreamFlow>,
	credit: Condvar,
	socket: TcpStream,
	queue:  Mutex<Option<mpsc::Sender<Inbound>>>,
}

/// All tunnel streams of the agent's GC4 connection.
pub struct Tunnels {
	sink:    Arc<dyn FrameSink>,
	streams: Mutex<HashMap<u32, Arc<Stream>>>,
}

impl Tunnels {
	/// Create the table; frames are written to `sink`.
	pub fn new(sink: Arc<dyn FrameSink>) -> Arc<Self> {
		Arc::new(Self { sink, streams: Mutex::new(HashMap::new()) })
	}

	/// Number of live streams.
	pub fn len(&self) -> usize {
		lock(&self.streams).len()
	}

	/// Whether no stream is live.
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// Handle `REQ tcp_connect` on stream `id`: connects on a worker thread,
	/// answers `RESP`, and starts the pumps.
	pub fn open(self: &Arc<Self>, id: u32, request: ConnectRequest) {
		let tunnels = Arc::clone(self);
		thread::spawn(move || tunnels.connect(id, &request));
	}

	fn connect(self: &Arc<Self>, id: u32, request: &ConnectRequest) {
		if lock(&self.streams).contains_key(&id) {
			self.resp(id, &json!({"ok": false, "error": "stream id in use", "errno": libc::EBUSY}));
			return;
		}
		let socket = match dial(&request.host, request.port) {
			Ok(socket) => socket,
			Err(err) => {
				let error = if err.raw_os_error() == Some(libc::ECONNREFUSED) {
					"port is not open".to_owned()
				} else {
					format!("connect {}:{}: {err}", request.host, request.port)
				};
				let errno = err.raw_os_error().unwrap_or(libc::EIO);
				self.resp(id, &json!({"ok": false, "error": error, "errno": errno}));
				return;
			},
		};
		let _ = socket.set_nodelay(true);
		let clones = socket
			.try_clone()
			.and_then(|a| Ok((a, socket.try_clone()?)));
		let (reader, writer) = match clones {
			Ok(pair) => pair,
			Err(err) => {
				let errno = err.raw_os_error().unwrap_or(libc::EIO);
				self.resp(
					id,
					&json!({"ok": false, "error": format!("clone socket: {err}"), "errno": errno}),
				);
				return;
			},
		};
		let (tx, rx) = mpsc::channel();
		let stream = Arc::new(Stream {
			flow: Mutex::new(StreamFlow::new(request.window, proto::TUNNEL_WINDOW)),
			credit: Condvar::new(),
			socket,
			queue: Mutex::new(Some(tx)),
		});
		lock(&self.streams).insert(id, Arc::clone(&stream));
		self.resp(id, &json!({"ok": true, "window": proto::TUNNEL_WINDOW}));

		let tunnels = Arc::clone(self);
		let pump_stream = Arc::clone(&stream);
		thread::spawn(move || tunnels.pump_out(id, &pump_stream, reader));
		let tunnels = Arc::clone(self);
		thread::spawn(move || tunnels.pump_in(id, &stream, writer, &rx));
	}

	/// Route a `DATA`/`WINDOW`/`CLOSE` frame from the host. Never blocks on a
	/// socket.
	pub fn handle_frame(&self, frame: Frame) {
		let Some(stream) = lock(&self.streams).get(&frame.id).cloned() else {
			if frame.ty != proto::FRAME_CLOSE {
				let _ = self
					.sink
					.send(proto::FRAME_CLOSE, frame.id, b"unknown stream");
			}
			return;
		};
		let id = frame.id;
		match frame.ty {
			proto::FRAME_DATA => {
				if frame.payload.is_empty() {
					return;
				}
				let accepted = lock(&stream.flow).on_data(frame.payload.len());
				if let Err(err) = accepted {
					self.abort(id, &stream, &err.to_string());
					return;
				}
				if let Some(queue) = lock(&stream.queue).as_ref() {
					let _ = queue.send(Inbound::Data(frame.payload));
				}
			},
			proto::FRAME_WINDOW => {
				let result = match proto::decode_window(&frame.payload) {
					Some(credit) => lock(&stream.flow)
						.on_window(credit)
						.map_err(|e| e.to_string()),
					None => Err("bad window payload".to_owned()),
				};
				match result {
					Ok(()) => stream.credit.notify_all(),
					Err(reason) => self.abort(id, &stream, &reason),
				}
			},
			proto::FRAME_CLOSE => match CloseKind::parse(&frame.payload) {
				CloseKind::HalfClose => {
					lock(&stream.flow).on_close(&CloseKind::HalfClose);
					let queue = lock(&stream.queue).take();
					if let Some(queue) = queue {
						let _ = queue.send(Inbound::HalfClose);
					}
					self.forget_if_finished(id, &stream);
				},
				CloseKind::Abort(_) => self.teardown(id, &stream),
			},
			_ => {},
		}
	}

	/// Abort every stream without notifying the host (new host connection).
	pub fn abort_all(&self) {
		let streams: Vec<_> = lock(&self.streams).drain().collect();
		for (_, stream) in streams {
			reset(&stream);
		}
	}

	fn pump_out(&self, id: u32, stream: &Stream, mut socket: TcpStream) {
		let mut buf = vec![0u8; CHUNK];
		loop {
			let want = {
				let mut flow = lock(&stream.flow);
				loop {
					if flow.aborted() {
						return;
					}
					let n = flow.sendable(CHUNK);
					if n > 0 {
						break n;
					}
					flow = stream
						.credit
						.wait(flow)
						.unwrap_or_else(PoisonError::into_inner);
				}
			};
			match socket.read(&mut buf[..want]) {
				Ok(0) => {
					let first = {
						let mut flow = lock(&stream.flow);
						!flow.aborted() && flow.close_local()
					};
					if first {
						let _ = self.sink.send(proto::FRAME_CLOSE, id, &[]);
						self.forget_if_finished(id, stream);
					}
					return;
				},
				Ok(n) => {
					if lock(&stream.flow).on_sent(n).is_err() {
						return;
					}
					if self.sink.send(proto::FRAME_DATA, id, &buf[..n]).is_err() {
						return;
					}
				},
				Err(err) if err.kind() == io::ErrorKind::Interrupted => {},
				Err(err) => {
					self.abort(id, stream, &format!("read: {err}"));
					return;
				},
			}
		}
	}

	fn pump_in(
		&self,
		id: u32,
		stream: &Stream,
		mut socket: TcpStream,
		rx: &mpsc::Receiver<Inbound>,
	) {
		while let Ok(message) = rx.recv() {
			match message {
				Inbound::Data(bytes) => {
					if let Err(err) = socket.write_all(&bytes) {
						self.abort(id, stream, &format!("write: {err}"));
						return;
					}
					let grant = lock(&stream.flow).on_consumed(bytes.len());
					if let Some(credit) = grant {
						let _ = self
							.sink
							.send(proto::FRAME_WINDOW, id, &proto::encode_window(credit));
					}
				},
				Inbound::HalfClose => {
					let _ = socket.shutdown(Shutdown::Write);
					return;
				},
			}
		}
	}

	/// Local abort: tell the host, then tear down.
	fn abort(&self, id: u32, stream: &Stream, reason: &str) {
		if lock(&stream.flow).aborted() {
			return;
		}
		let _ = self.sink.send(proto::FRAME_CLOSE, id, reason.as_bytes());
		self.teardown(id, stream);
	}

	fn teardown(&self, id: u32, stream: &Stream) {
		reset(stream);
		let mut streams = lock(&self.streams);
		if streams
			.get(&id)
			.is_some_and(|s| std::ptr::eq(Arc::as_ptr(s), stream))
		{
			streams.remove(&id);
		}
	}

	fn forget_if_finished(&self, id: u32, stream: &Stream) {
		if lock(&stream.flow).is_finished() {
			let mut streams = lock(&self.streams);
			if streams
				.get(&id)
				.is_some_and(|s| std::ptr::eq(Arc::as_ptr(s), stream))
			{
				streams.remove(&id);
			}
		}
	}

	fn resp(&self, id: u32, value: &Value) {
		if let Ok(payload) = serde_json::to_vec(value) {
			let _ = self.sink.send(proto::FRAME_RESP, id, &payload);
		}
	}
}

fn reset(stream: &Stream) {
	lock(&stream.flow).abort();
	stream.credit.notify_all();
	lock(&stream.queue).take();
	let _ = stream.socket.shutdown(Shutdown::Both);
}

fn dial(host: &str, port: u16) -> io::Result<TcpStream> {
	let mut last = io::Error::new(io::ErrorKind::NotFound, "host did not resolve");
	for addr in (host, port).to_socket_addrs()? {
		match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
			Ok(socket) => return Ok(socket),
			Err(err) => last = err,
		}
	}
	Err(last)
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
	mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
	use std::{net::TcpListener, os::unix::net::UnixStream, time::Instant};

	use super::*;

	/// Sink that records frames for inspection.
	#[derive(Default)]
	struct Recorder {
		frames: Mutex<Vec<Frame>>,
		cv:     Condvar,
	}

	impl FrameSink for Recorder {
		fn send(&self, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
			lock(&self.frames).push(Frame { ty, id, payload: payload.to_vec() });
			self.cv.notify_all();
			Ok(())
		}
	}

	impl Recorder {
		fn wait_for(&self, what: impl Fn(&[Frame]) -> bool) -> Vec<Frame> {
			let deadline = Instant::now() + Duration::from_secs(5);
			let mut frames = lock(&self.frames);
			while !what(&frames) {
				let left = deadline.saturating_duration_since(Instant::now());
				assert!(
					!left.is_zero(),
					"timed out; frames: {:?}",
					frames.iter().map(|f| f.ty).collect::<Vec<_>>()
				);
				frames = self.cv.wait_timeout(frames, left).unwrap().0;
			}
			frames.clone()
		}
	}

	fn data_bytes(frames: &[Frame], id: u32) -> usize {
		frames
			.iter()
			.filter(|f| f.ty == proto::FRAME_DATA && f.id == id)
			.map(|f| f.payload.len())
			.sum()
	}

	fn resp(frames: &[Frame], id: u32) -> Option<Value> {
		frames
			.iter()
			.find(|f| f.ty == proto::FRAME_RESP && f.id == id)
			.map(|f| serde_json::from_slice(&f.payload).unwrap())
	}

	fn open(window: u32) -> (Arc<Recorder>, Arc<Tunnels>, TcpStream) {
		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let port = listener.local_addr().unwrap().port();
		let recorder = Arc::new(Recorder::default());
		let tunnels = Tunnels::new(recorder.clone());
		tunnels.open(1, ConnectRequest { host: "127.0.0.1".into(), port, window });
		let (peer, _) = listener.accept().unwrap();
		let frames = recorder.wait_for(|f| resp(f, 1).is_some());
		assert_eq!(resp(&frames, 1).unwrap(), json!({"ok": true, "window": proto::TUNNEL_WINDOW}));
		(recorder, tunnels, peer)
	}

	#[test]
	fn closed_port_reports_econnrefused() {
		let port = TcpListener::bind("127.0.0.1:0")
			.unwrap()
			.local_addr()
			.unwrap()
			.port();
		let recorder = Arc::new(Recorder::default());
		let tunnels = Tunnels::new(recorder.clone());
		tunnels.open(9, ConnectRequest { host: "127.0.0.1".into(), port, window: 10 });
		let frames = recorder.wait_for(|f| resp(f, 9).is_some());
		let resp = resp(&frames, 9).unwrap();
		assert_eq!(resp["ok"], false);
		assert_eq!(resp["errno"], libc::ECONNREFUSED);
		assert_eq!(resp["error"], "port is not open");
		assert!(tunnels.is_empty());
	}

	#[test]
	fn outbound_respects_credit_and_resumes_on_window() {
		let (recorder, tunnels, mut peer) = open(1000);
		peer.write_all(&vec![7u8; 5000]).unwrap();
		recorder.wait_for(|f| data_bytes(f, 1) == 1000);
		thread::sleep(Duration::from_millis(100));
		assert_eq!(data_bytes(&lock(&recorder.frames), 1), 1000, "sent beyond credit");
		tunnels.handle_frame(Frame {
			ty:      proto::FRAME_WINDOW,
			id:      1,
			payload: proto::encode_window(4000).to_vec(),
		});
		recorder.wait_for(|f| data_bytes(f, 1) == 5000);
	}

	#[test]
	fn inbound_data_is_written_and_credited_then_half_close_finishes() {
		let (recorder, tunnels, mut peer) = open(proto::TUNNEL_WINDOW);
		let quarter = (proto::TUNNEL_WINDOW / 4) as usize;
		tunnels.handle_frame(Frame {
			ty:      proto::FRAME_DATA,
			id:      1,
			payload: vec![1u8; quarter],
		});
		let mut got = vec![0u8; quarter];
		peer.read_exact(&mut got).unwrap();
		let frames = recorder.wait_for(|f| f.iter().any(|f| f.ty == proto::FRAME_WINDOW));
		let win = frames.iter().find(|f| f.ty == proto::FRAME_WINDOW).unwrap();
		assert_eq!(proto::decode_window(&win.payload), Some(quarter as u32));

		// Host half-close → socket sees EOF; guest closes → agent half-closes.
		tunnels.handle_frame(Frame { ty: proto::FRAME_CLOSE, id: 1, payload: vec![] });
		assert_eq!(peer.read(&mut got).unwrap(), 0);
		assert_eq!(tunnels.len(), 1);
		drop(peer);
		recorder.wait_for(|f| {
			f.iter()
				.any(|f| f.ty == proto::FRAME_CLOSE && f.payload.is_empty())
		});
		let deadline = Instant::now() + Duration::from_secs(2);
		while !tunnels.is_empty() && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(10));
		}
		assert!(tunnels.is_empty());
	}

	#[test]
	fn data_beyond_window_aborts_stream() {
		let (recorder, tunnels, mut peer) = open(10);
		let too_much = proto::TUNNEL_WINDOW as usize + 1;
		tunnels.handle_frame(Frame {
			ty:      proto::FRAME_DATA,
			id:      1,
			payload: vec![0; too_much],
		});
		let frames = recorder.wait_for(|f| f.iter().any(|f| f.ty == proto::FRAME_CLOSE));
		let close = frames.iter().find(|f| f.ty == proto::FRAME_CLOSE).unwrap();
		assert_eq!(close.payload, b"window exceeded");
		assert!(tunnels.is_empty());
		let mut buf = [0u8; 1];
		assert!(matches!(peer.read(&mut buf), Ok(0) | Err(_)));
	}

	#[test]
	fn host_abort_resets_socket_without_echo() {
		let (recorder, tunnels, mut peer) = open(10);
		tunnels.handle_frame(Frame {
			ty:      proto::FRAME_CLOSE,
			id:      1,
			payload: b"bye".to_vec(),
		});
		assert!(tunnels.is_empty());
		let mut buf = [0u8; 1];
		assert!(matches!(peer.read(&mut buf), Ok(0) | Err(_)));
		thread::sleep(Duration::from_millis(50));
		assert!(
			!lock(&recorder.frames)
				.iter()
				.any(|f| f.ty == proto::FRAME_CLOSE)
		);
	}

	#[test]
	fn unknown_stream_gets_close_but_close_is_ignored() {
		let recorder = Arc::new(Recorder::default());
		let tunnels = Tunnels::new(recorder.clone());
		tunnels.handle_frame(Frame {
			ty:      proto::FRAME_DATA,
			id:      4,
			payload: b"x".to_vec(),
		});
		tunnels.handle_frame(Frame { ty: proto::FRAME_CLOSE, id: 5, payload: vec![] });
		let frames = lock(&recorder.frames).clone();
		assert_eq!(frames, vec![Frame {
			ty:      proto::FRAME_CLOSE,
			id:      4,
			payload: b"unknown stream".to_vec(),
		}]);
	}

	#[test]
	fn slow_socket_does_not_block_frame_handling() {
		// Guest peer never reads: inbound DATA up to the window must still be
		// accepted instantly, and another stream keeps flowing.
		let (_recorder, tunnels, _stalled) = open(10);
		let start = Instant::now();
		for _ in 0..4 {
			tunnels.handle_frame(Frame {
				ty:      proto::FRAME_DATA,
				id:      1,
				payload: vec![0; (proto::TUNNEL_WINDOW / 4) as usize],
			});
		}
		assert!(start.elapsed() < Duration::from_secs(1));
		assert_eq!(tunnels.len(), 1);
	}

	#[test]
	fn mutex_sink_writes_parseable_frames() {
		let (a, mut b) = UnixStream::pair().unwrap();
		let sink = Mutex::new(a);
		sink
			.send(proto::FRAME_WINDOW, 3, &proto::encode_window(9))
			.unwrap();
		let frame = proto::read_frame(&mut b).unwrap().unwrap();
		assert_eq!((frame.ty, frame.id, proto::decode_window(&frame.payload)), (9, 3, Some(9)));
	}

	#[test]
	fn connect_request_validation() {
		assert!(ConnectRequest::from_json(&json!({"port": 0})).is_err());
		assert!(ConnectRequest::from_json(&json!({"port": 70000})).is_err());
		assert!(ConnectRequest::from_json(&json!({"port": 1, "window": 0})).is_err());
		assert_eq!(ConnectRequest::from_json(&json!({"port": 49983})).unwrap(), ConnectRequest {
			host:   "127.0.0.1".into(),
			port:   49983,
			window: proto::TUNNEL_WINDOW,
		});
	}
}
