//! Minimal synchronous host-side client for the GC4 guest-agent frame
//! protocol. Mirrors the sequencing of `python/vmon/agent_client.py`
//! (connect → SYNC preamble → framed requests) with a reader thread decoding
//! frames off the socket; `agent/src/proto.rs` is the wire source of truth.

use std::{
	io::Write,
	os::unix::net::UnixStream,
	path::Path,
	sync::mpsc::{self, Receiver, RecvTimeoutError},
	thread,
	time::{Duration, Instant},
};

use serde_json::{Value, json};
use vmon_agent::proto::{self, Frame, Op};

pub struct AgentClient {
	stream:  UnixStream,
	frames:  Receiver<Frame>,
	next_id: u32,
}

impl AgentClient {
	/// Connect to the vmm agent socket, retrying until `timeout`, then write
	/// the SYNC preamble exactly like the Python host client does on connect.
	pub fn connect(sock: &Path, timeout: Duration) -> Self {
		let deadline = Instant::now() + timeout;
		let mut stream = loop {
			match UnixStream::connect(sock) {
				Ok(stream) => break stream,
				Err(e) => {
					assert!(
						Instant::now() < deadline,
						"connecting to agent socket {}: {e}",
						sock.display()
					);
					thread::sleep(Duration::from_millis(100));
				},
			}
		};
		stream.write_all(&proto::SYNC).expect("write SYNC preamble");
		let mut reader = stream.try_clone().expect("clone agent stream");
		let (tx, rx) = mpsc::channel();
		thread::spawn(move || {
			loop {
				match proto::read_frame(&mut reader) {
					Ok(Some(frame)) => {
						if tx.send(frame).is_err() {
							return;
						}
					},
					Ok(None) | Err(_) => return,
				}
			}
		});
		Self { stream, frames: rx, next_id: 1 }
	}

	fn send_request(&mut self, body: &Value) -> u32 {
		let id = self.next_id;
		self.next_id += 1;
		let payload = serde_json::to_vec(body).expect("encode agent request");
		proto::write_frame(&mut self.stream, proto::FRAME_REQ, id, &payload)
			.expect("write agent request");
		id
	}

	/// Ping until the agent answers or `timeout` elapses. Anything queued
	/// before the guest agent opens its virtio-console port — including the
	/// connect-time SYNC preamble — is dropped by the guest, so every retry
	/// re-sends SYNC (harmless mid-stream; readers skip it) plus a fresh
	/// request id.
	pub fn ping(&mut self, timeout: Duration) -> Value {
		let deadline = Instant::now() + timeout;
		loop {
			self
				.stream
				.write_all(&proto::SYNC)
				.expect("re-send SYNC preamble");
			let id = self.send_request(&json!({"op": Op::Ping}));
			let attempt = (Instant::now() + Duration::from_millis(500)).min(deadline);
			if let Some(resp) = self.wait_resp(id, attempt) {
				return resp;
			}
			assert!(Instant::now() < deadline, "agent did not answer ping within {timeout:?}");
		}
	}

	/// Run `argv` in the guest; returns the exit code and combined
	/// stdout/stderr. Frames for other request IDs (for example, boot-exec
	/// hand-off) are skipped.
	pub fn exec(&mut self, argv: &[&str], timeout: Duration) -> (i64, Vec<u8>) {
		let deadline = Instant::now() + timeout;
		let id = self.send_request(&json!({"op": Op::Exec, "cmd": argv, "tty": false}));
		let mut stdout = Vec::new();
		loop {
			let frame = self.recv_until(deadline, "agent exec");
			if frame.id != id {
				continue;
			}
			match frame.ty {
				proto::FRAME_STDOUT | proto::FRAME_STDERR => stdout.extend_from_slice(&frame.payload),
				proto::FRAME_EXIT => {
					let exit: Value = serde_json::from_slice(&frame.payload).expect("exit payload json");
					let code = exit.get("code").and_then(Value::as_i64).unwrap_or(-1);
					return (code, stdout);
				},
				proto::FRAME_RESP => {
					let resp: Value = serde_json::from_slice(&frame.payload).expect("resp payload json");
					assert_ne!(
						resp.get("ok").and_then(Value::as_bool),
						Some(false),
						"agent exec failed: {resp}"
					);
				},
				_ => {},
			}
		}
	}

	/// Write `content` to `path` in the guest with the specified file `mode`.
	pub fn write_file(&mut self, path: &str, content: &[u8], mode: u32, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		let id = self.send_request(&json!({
			"op": Op::FsWrite,
			"path": path,
			"mode": mode,
		}));

		proto::write_frame(&mut self.stream, proto::FRAME_STDIN, id, content)
			.expect("write file data frame");

		proto::write_frame(&mut self.stream, proto::FRAME_STDIN, id, &[])
			.expect("write file EOF frame");

		if let Some(resp) = self.wait_resp(id, deadline) {
			assert_eq!(resp.get("ok").and_then(Value::as_bool), Some(true), "fs_write failed: {resp}");
		} else {
			panic!("fs_write timed out");
		}
	}

	fn wait_resp(&self, id: u32, deadline: Instant) -> Option<Value> {
		loop {
			let now = Instant::now();
			if now >= deadline {
				return None;
			}
			match self.frames.recv_timeout(deadline - now) {
				Ok(frame) if frame.ty == proto::FRAME_RESP && frame.id == id => {
					return Some(serde_json::from_slice(&frame.payload).expect("resp payload json"));
				},
				Ok(_) => {},
				Err(RecvTimeoutError::Timeout) => return None,
				Err(RecvTimeoutError::Disconnected) => panic!("agent connection closed"),
			}
		}
	}

	fn recv_until(&self, deadline: Instant, what: &str) -> Frame {
		let now = Instant::now();
		assert!(now < deadline, "{what} timed out");
		match self.frames.recv_timeout(deadline - now) {
			Ok(frame) => frame,
			Err(e) => panic!("{what}: {e}"),
		}
	}
}
