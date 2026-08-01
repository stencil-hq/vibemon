use std::{
	collections::VecDeque,
	io::{self, Read, Write},
};

pub const MAX_PAYLOAD_LEN: usize = 1 << 20;
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

pub const OP_HELLO: &str = "hello";
pub const PTY_SCROLLBACK_LIMIT: usize = 1 << 20;

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

	let mut header = [0u8; HEADER_LEN];
	header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
	header[4] = ty;
	header[5..].copy_from_slice(&id.to_le_bytes());
	writer.write_all(&header)?;
	writer.write_all(payload)
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
		// the reader scans to the next SYNC marker and returns the following frame.
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
