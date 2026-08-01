//! Read-only remote virtio-fs device and its proxy protocol.
//!
//! The proxy protocol stays deliberately small: the device only asks the host
//! proxy to stat, list, or read paths relative to a mounted object prefix.

pub mod proto {
	use std::io::{self, Read, Write};

	use serde::{Deserialize, Serialize};

	/// Request frame type.
	pub const REQ: u8 = 1;
	/// JSON response frame type.
	pub const OK_JSON: u8 = 2;
	/// Raw object-data response frame type.
	pub const OK_DATA: u8 = 3;
	/// Error response frame type.
	pub const ERR: u8 = 4;
	/// Number of bytes in a protocol frame header.
	pub const HEADER_LEN: usize = 9;
	/// Largest payload accepted from the proxy.
	pub const MAX_FRAME: usize = 2 * 1024 * 1024;

	/// A request sent from the remote filesystem device to its object proxy.
	#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
	#[serde(tag = "op", rename_all = "snake_case")]
	pub enum Request {
		/// Look up metadata for one path.
		Stat { tag: String, path: String },
		/// List direct children of one directory path.
		List { tag: String, path: String },
		/// Read a byte range from one file path.
		Read { tag: String, path: String, offset: u64, len: u32 },
	}

	/// Object kind returned by the proxy.
	#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
	#[serde(rename_all = "lowercase")]
	pub enum Kind {
		/// A regular object.
		File,
		/// A synthetic or explicit directory.
		Dir,
	}

	/// Metadata returned for a requested path.
	#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
	pub struct StatReply {
		/// Object kind.
		pub kind:  Kind,
		/// Object length in bytes, or zero for directories.
		pub size:  u64,
		/// Unix modification time in seconds.
		pub mtime: u64,
		/// Object `ETag`, omitted for directories.
		pub etag:  Option<String>,
	}

	/// A direct child returned by a directory listing.
	#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
	pub struct Entry {
		/// One path segment, never a slash-separated path.
		pub name:  String,
		/// Child kind.
		pub kind:  Kind,
		/// Child byte length, or zero for a directory.
		pub size:  u64,
		/// Unix modification time in seconds.
		pub mtime: u64,
	}

	/// Directory contents returned by the proxy.
	#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
	pub struct ListReply {
		/// Direct children, already merged and sorted by name.
		pub entries: Vec<Entry>,
	}

	/// A proxy failure returned to the device.
	#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
	pub struct ErrReply {
		/// Stable wire error code.
		pub code: String,
		/// Human-readable diagnostic.
		pub msg:  String,
	}

	/// Reads one frame from a dedicated proxy connection.
	///
	/// # Errors
	///
	/// Returns an error for truncated input, an oversized payload, or an I/O
	/// failure while reading the header or payload.
	pub fn read_frame<R: Read>(reader: &mut R) -> io::Result<(u8, u32, Vec<u8>)> {
		let mut header = [0u8; HEADER_LEN];
		reader.read_exact(&mut header)?;
		let payload_len =
			u32::from_le_bytes(header[..4].try_into().expect("fixed header length")) as usize;
		if payload_len > MAX_FRAME {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				format!("frame payload length {payload_len} exceeds {MAX_FRAME}"),
			));
		}
		let ty = header[4];
		let id = u32::from_le_bytes(header[5..].try_into().expect("fixed header length"));
		let mut payload = vec![0; payload_len];
		reader.read_exact(&mut payload)?;
		Ok((ty, id, payload))
	}

	/// Writes one frame to a dedicated proxy connection.
	///
	/// # Errors
	///
	/// Returns an error when `payload` exceeds [`MAX_FRAME`] or the output
	/// fails.
	pub fn write_frame<W: Write>(writer: &mut W, ty: u8, id: u32, payload: &[u8]) -> io::Result<()> {
		if payload.len() > MAX_FRAME {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				format!("frame payload length {} exceeds {MAX_FRAME}", payload.len()),
			));
		}

		let mut header = [0u8; HEADER_LEN];
		header[..4].copy_from_slice(&(payload.len() as u32).to_le_bytes());
		header[4] = ty;
		header[5..].copy_from_slice(&id.to_le_bytes());
		writer.write_all(&header)?;
		writer.write_all(payload)
	}

	#[cfg(test)]
	mod tests {
		use std::io::Cursor;

		use super::*;

		#[test]
		fn frame_round_trip() {
			let mut bytes = Vec::new();
			write_frame(&mut bytes, OK_JSON, 0x0a0b_0c0d, b"reply").unwrap();

			let (ty, id, payload) = read_frame(&mut Cursor::new(bytes)).unwrap();
			assert_eq!(ty, OK_JSON);
			assert_eq!(id, 0x0a0b_0c0d);
			assert_eq!(payload, b"reply");
		}

		#[test]
		fn oversized_frame_is_rejected() {
			let mut bytes = [0u8; HEADER_LEN];
			bytes[..4].copy_from_slice(&((MAX_FRAME + 1) as u32).to_le_bytes());
			let err = read_frame(&mut Cursor::new(bytes)).unwrap_err();
			assert_eq!(err.kind(), io::ErrorKind::InvalidData);
		}
	}
}

#[cfg(not(target_os = "windows"))]
use std::os::unix::net::UnixStream as ProxyStream;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};
#[cfg(target_os = "windows")]
use std::{fs::OpenOptions, thread};
#[cfg(target_os = "windows")]
type ProxyStream = std::fs::File;

#[cfg(target_os = "windows")]
use remote_fuse as fs;
use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_FS};
use virtio_queue::{Queue, QueueT};
use vm_memory::GuestAddress;

#[cfg(not(target_os = "windows"))]
use super::fs;
use super::{Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, fuse_errno as errno};

#[cfg(target_os = "windows")]
mod remote_fuse {
	use virtio_queue::DescriptorChain;
	use vm_memory::{Bytes, GuestAddress};

	use crate::{memory::GuestMemoryMmap, virtio::descriptor_range_valid};

	pub(super) const QUEUE_SIZE: u16 = 64;
	pub(super) const NUM_QUEUES: usize = 2;
	pub(super) const HIPRIO_QUEUE: usize = 0;
	pub(super) const REQUEST_QUEUE: usize = 1;
	pub(super) const TAG_LEN: usize = 36;
	pub(super) const CONFIG_SPACE_SIZE: usize = TAG_LEN + 4;
	pub(super) const NUM_REQUEST_QUEUES: u32 = 1;
	pub(super) const FUSE_ROOT_ID: u64 = 1;
	pub(super) const FUSE_PROTO_MINOR: u32 = 44;
	pub(super) const MAX_WRITE: u32 = 1 << 20;
	pub(super) const FUSE_LOOKUP: u32 = 1;
	pub(super) const FUSE_FORGET: u32 = 2;
	pub(super) const FUSE_GETATTR: u32 = 3;
	pub(super) const FUSE_SETATTR: u32 = 4;
	pub(super) const FUSE_READLINK: u32 = 5;
	pub(super) const FUSE_SYMLINK: u32 = 6;
	pub(super) const FUSE_MKNOD: u32 = 8;
	pub(super) const FUSE_MKDIR: u32 = 9;
	pub(super) const FUSE_UNLINK: u32 = 10;
	pub(super) const FUSE_RMDIR: u32 = 11;
	pub(super) const FUSE_RENAME: u32 = 12;
	pub(super) const FUSE_LINK: u32 = 13;
	pub(super) const FUSE_OPEN: u32 = 14;
	pub(super) const FUSE_READ: u32 = 15;
	pub(super) const FUSE_WRITE: u32 = 16;
	pub(super) const FUSE_STATFS: u32 = 17;
	pub(super) const FUSE_RELEASE: u32 = 18;
	pub(super) const FUSE_FSYNC: u32 = 20;
	pub(super) const FUSE_FLUSH: u32 = 25;
	pub(super) const FUSE_INIT: u32 = 26;
	pub(super) const FUSE_OPENDIR: u32 = 27;
	pub(super) const FUSE_READDIR: u32 = 28;
	pub(super) const FUSE_RELEASEDIR: u32 = 29;
	pub(super) const FUSE_FSYNCDIR: u32 = 30;
	pub(super) const FUSE_ACCESS: u32 = 34;
	pub(super) const FUSE_CREATE: u32 = 35;
	pub(super) const FUSE_INTERRUPT: u32 = 36;
	pub(super) const FUSE_BATCH_FORGET: u32 = 42;
	pub(super) const FUSE_FALLOCATE: u32 = 43;
	pub(super) const FUSE_READDIRPLUS: u32 = 44;
	pub(super) const FUSE_RENAME2: u32 = 45;

	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseInHeader {
		pub(super) len:          u32,
		pub(super) opcode:       u32,
		pub(super) unique:       u64,
		pub(super) nodeid:       u64,
		pub(super) uid:          u32,
		pub(super) gid:          u32,
		pub(super) pid:          u32,
		pub(super) total_extlen: u16,
		pub(super) padding:      u16,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseOutHeader {
		pub(super) len:    u32,
		pub(super) error:  i32,
		pub(super) unique: u64,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseAttr {
		pub(super) ino:       u64,
		pub(super) size:      u64,
		pub(super) blocks:    u64,
		pub(super) atime:     u64,
		pub(super) mtime:     u64,
		pub(super) ctime:     u64,
		pub(super) atimensec: u32,
		pub(super) mtimensec: u32,
		pub(super) ctimensec: u32,
		pub(super) mode:      u32,
		pub(super) nlink:     u32,
		pub(super) uid:       u32,
		pub(super) gid:       u32,
		pub(super) rdev:      u32,
		pub(super) blksize:   u32,
		pub(super) flags:     u32,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseAttrOut {
		pub(super) attr_valid:      u64,
		pub(super) attr_valid_nsec: u32,
		pub(super) dummy:           u32,
		pub(super) attr:            FuseAttr,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseEntryOut {
		pub(super) nodeid:           u64,
		pub(super) generation:       u64,
		pub(super) entry_valid:      u64,
		pub(super) attr_valid:       u64,
		pub(super) entry_valid_nsec: u32,
		pub(super) attr_valid_nsec:  u32,
		pub(super) attr:             FuseAttr,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseInitOut {
		pub(super) major:                u32,
		pub(super) minor:                u32,
		pub(super) max_readahead:        u32,
		pub(super) flags:                u32,
		pub(super) max_background:       u16,
		pub(super) congestion_threshold: u16,
		pub(super) max_write:            u32,
		pub(super) time_gran:            u32,
		pub(super) max_pages:            u16,
		pub(super) map_alignment:        u16,
		pub(super) flags2:               u32,
		pub(super) unused:               [u32; 7],
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseOpenOut {
		pub(super) fh:         u64,
		pub(super) open_flags: u32,
		pub(super) backing_id: i32,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseReadIn {
		pub(super) fh:         u64,
		pub(super) offset:     u64,
		pub(super) size:       u32,
		pub(super) read_flags: u32,
		pub(super) lock_owner: u64,
		pub(super) flags:      u32,
		pub(super) padding:    u32,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseDirent {
		pub(super) ino:     u64,
		pub(super) off:     u64,
		pub(super) namelen: u32,
		pub(super) type_:   u32,
	}
	#[repr(C)]
	#[derive(Clone, Copy, Default)]
	pub(super) struct FuseKstatfs {
		pub(super) blocks:  u64,
		pub(super) bfree:   u64,
		pub(super) bavail:  u64,
		pub(super) files:   u64,
		pub(super) ffree:   u64,
		pub(super) bsize:   u32,
		pub(super) namelen: u32,
		pub(super) frsize:  u32,
		pub(super) padding: u32,
		pub(super) spare:   [u32; 6],
	}

	pub(super) const IN_HEADER_SIZE: usize = std::mem::size_of::<FuseInHeader>();
	pub(super) const OUT_HEADER_SIZE: usize = std::mem::size_of::<FuseOutHeader>();
	pub(super) const DIRENT_HEADER: usize = std::mem::size_of::<FuseDirent>();
	const MAX_REQUEST_SIZE: usize = IN_HEADER_SIZE + 40 + MAX_WRITE as usize;
	pub(super) type SplitChain = (Vec<u8>, Vec<(GuestAddress, u32)>);

	pub(super) fn read_struct<T: Copy>(buf: &[u8], off: usize) -> Option<T> {
		let end = off.checked_add(std::mem::size_of::<T>())?;
		if end > buf.len() {
			return None;
		}
		// SAFETY: the bounds check proves the full value lies in `buf`; unaligned
		// access is required because FUSE structures are byte-packed on the wire.
		Some(unsafe { std::ptr::read_unaligned(buf.as_ptr().add(off).cast()) })
	}
	pub(super) const fn struct_bytes<T: Copy>(value: &T) -> &[u8] {
		// SAFETY: callers pass fully initialized, plain-data FUSE wire structures.
		unsafe { std::slice::from_raw_parts((value as *const T).cast(), std::mem::size_of::<T>()) }
	}
	pub(super) const fn align8(value: usize) -> usize {
		(value + 7) & !7
	}
	pub(super) fn write_reply(
		mem: &GuestMemoryMmap,
		writable: &[(GuestAddress, u32)],
		unique: u64,
		error: i32,
		body: &[u8],
	) -> u32 {
		let Some(capacity) = writable
			.iter()
			.try_fold(0usize, |total, &(_, len)| total.checked_add(len as usize))
		else {
			return 0;
		};
		if capacity < OUT_HEADER_SIZE {
			return 0;
		}
		let body_len = body.len().min(capacity - OUT_HEADER_SIZE);
		let total = OUT_HEADER_SIZE + body_len;
		let header = FuseOutHeader { len: total as u32, error, unique };
		let mut reply = Vec::with_capacity(total);
		reply.extend_from_slice(struct_bytes(&header));
		reply.extend_from_slice(&body[..body_len]);
		let mut offset = 0;
		for &(address, len) in writable {
			if offset == reply.len() {
				break;
			}
			let count = (len as usize).min(reply.len() - offset);
			if mem
				.write_slice(&reply[offset..offset + count], address)
				.is_err()
			{
				return 0;
			}
			offset += count;
		}
		if offset == reply.len() {
			total as u32
		} else {
			0
		}
	}
	pub(super) fn split_chain(
		mem: &GuestMemoryMmap,
		chain: DescriptorChain<&GuestMemoryMmap>,
	) -> Option<SplitChain> {
		let mut request = Vec::new();
		let mut writable = Vec::new();
		let mut seen_writable = false;
		for descriptor in chain {
			if !descriptor_range_valid(mem, descriptor.addr(), descriptor.len()) {
				return None;
			}
			if descriptor.is_write_only() {
				seen_writable = true;
				writable.push((descriptor.addr(), descriptor.len()));
			} else {
				if seen_writable {
					return None;
				}
				let end = request.len().checked_add(descriptor.len() as usize)?;
				if end > MAX_REQUEST_SIZE {
					return None;
				}
				let start = request.len();
				request.resize(end, 0);
				mem.read_slice(&mut request[start..], descriptor.addr())
					.ok()?;
			}
		}
		Some((request, writable))
	}
}

#[cfg(target_os = "windows")]
mod libc {
	pub const W_OK: i32 = 2;
	pub const DT_DIR: u8 = 4;
	pub const DT_REG: u8 = 8;
	pub const O_ACCMODE: i32 = 3;
	pub const O_WRONLY: i32 = 1;
	pub const O_RDWR: i32 = 2;
	pub const O_CREAT: i32 = 0o100;
	pub const O_TRUNC: i32 = 0o1000;
}
use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	snapshot::FsStateSer,
};

const REMOTE_TIMEOUT_SEC: u64 = 5;
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);
const PROXY_REQUEST_ID: u32 = 1;

enum ProxyReply {
	Json(Vec<u8>),
	Data(Vec<u8>),
}

/// In-memory state for a remote read-only virtio-fs mount.
///
/// Object paths are relative to the mount root. The lazy platform transport
/// connection is intentionally excluded from snapshots; it reconnects when
/// the guest next asks the device to serve a request.
struct RemoteFsState {
	sock:    PathBuf,
	conn:    Option<ProxyStream>,
	inodes:  HashMap<u64, String>,
	by_path: HashMap<String, u64>,
	next:    u64,
	tag:     String,
}

impl RemoteFsState {
	fn new(tag: String, sock: PathBuf) -> Self {
		let mut inodes = HashMap::new();
		inodes.insert(fs::FUSE_ROOT_ID, String::new());
		let mut by_path = HashMap::new();
		by_path.insert(String::new(), fs::FUSE_ROOT_ID);
		Self { sock, conn: None, inodes, by_path, next: fs::FUSE_ROOT_ID + 1, tag }
	}

	fn restore(tag: String, sock: PathBuf, saved: &FsStateSer) -> Result<Self> {
		let mut state = Self::new(tag, sock);
		for (id, path) in &saved.inodes {
			if *id == fs::FUSE_ROOT_ID {
				if !path.is_empty() {
					return Err(err("remote virtio-fs root path is not empty"));
				}
				continue;
			}
			if *id < fs::FUSE_ROOT_ID || !valid_path(path) || state.by_path.contains_key(path) {
				return Err(err(format!("invalid remote virtio-fs snapshot path {path:?}")));
			}
			state.inodes.insert(*id, path.clone());
			state.by_path.insert(path.clone(), *id);
			state.next = state.next.max(id.saturating_add(1));
		}
		state.next = state.next.max(saved.next).max(fs::FUSE_ROOT_ID + 1);
		Ok(state)
	}

	fn save(&self) -> FsStateSer {
		let mut inodes = self
			.inodes
			.iter()
			.map(|(&id, path)| (id, path.clone()))
			.collect::<Vec<_>>();
		inodes.sort_unstable_by_key(|(id, _)| *id);
		FsStateSer { inodes, next: self.next }
	}

	fn intern(&mut self, path: String) -> u64 {
		if let Some(&id) = self.by_path.get(&path) {
			return id;
		}
		let id = self.next;
		self.next = self.next.saturating_add(1);
		self.inodes.insert(id, path.clone());
		self.by_path.insert(path, id);
		id
	}

	fn path(&self, nodeid: u64) -> Option<String> {
		self.inodes.get(&nodeid).cloned()
	}

	fn call(&mut self, request: &proto::Request) -> std::result::Result<ProxyReply, i32> {
		let payload = serde_json::to_vec(request).map_err(|_| -errno::EIO)?;

		for _ in 0..2 {
			if self.conn.is_none() {
				let conn = Self::connect_proxy(&self.sock);
				match conn {
					Ok(conn) => self.conn = Some(conn),
					Err(_) => continue,
				}
			}

			let response = {
				let conn = self.conn.as_mut().expect("connection was populated");
				proto::write_frame(conn, proto::REQ, PROXY_REQUEST_ID, &payload)
					.and_then(|()| proto::read_frame(conn))
			};
			let (ty, _id, payload) = match response {
				Ok(frame) if frame.1 == PROXY_REQUEST_ID => frame,
				Ok(_) | Err(_) => {
					self.conn = None;
					continue;
				},
			};

			match ty {
				proto::OK_JSON => return Ok(ProxyReply::Json(payload)),
				proto::OK_DATA => return Ok(ProxyReply::Data(payload)),
				proto::ERR => {
					let reply =
						serde_json::from_slice::<proto::ErrReply>(&payload).map_err(|_| -errno::EIO)?;
					return Err(proxy_errno(&reply.code));
				},
				_ => {
					self.conn = None;
				},
			}
		}

		Err(-errno::EIO)
	}

	#[cfg(not(target_os = "windows"))]
	fn connect_proxy(path: &std::path::Path) -> std::io::Result<ProxyStream> {
		let conn = ProxyStream::connect(path)?;
		conn.set_read_timeout(Some(PROXY_TIMEOUT))?;
		conn.set_write_timeout(Some(PROXY_TIMEOUT))?;
		Ok(conn)
	}

	#[cfg(target_os = "windows")]
	fn connect_proxy(path: &std::path::Path) -> std::io::Result<ProxyStream> {
		let deadline = std::time::Instant::now() + PROXY_TIMEOUT;
		loop {
			match OpenOptions::new().read(true).write(true).open(path) {
				Ok(pipe) => return Ok(pipe),
				Err(error)
					if matches!(
						error.raw_os_error(),
						Some(2 | 231) // ERROR_FILE_NOT_FOUND | ERROR_PIPE_BUSY
					) && std::time::Instant::now() < deadline =>
				{
					thread::sleep(Duration::from_millis(10));
				},
				Err(error) => return Err(error),
			}
		}
	}

	fn stat(&mut self, path: &str) -> std::result::Result<proto::StatReply, i32> {
		let request = proto::Request::Stat { tag: self.tag.clone(), path: path.to_owned() };
		match self.call(&request)? {
			ProxyReply::Json(payload) => serde_json::from_slice(&payload).map_err(|_| -errno::EIO),
			ProxyReply::Data(_) => Err(-errno::EIO),
		}
	}

	fn list(&mut self, path: &str) -> std::result::Result<proto::ListReply, i32> {
		let request = proto::Request::List { tag: self.tag.clone(), path: path.to_owned() };
		match self.call(&request)? {
			ProxyReply::Json(payload) => serde_json::from_slice(&payload).map_err(|_| -errno::EIO),
			ProxyReply::Data(_) => Err(-errno::EIO),
		}
	}

	fn read(&mut self, path: &str, offset: u64, len: u32) -> std::result::Result<Vec<u8>, i32> {
		let request =
			proto::Request::Read { tag: self.tag.clone(), path: path.to_owned(), offset, len };
		match self.call(&request)? {
			ProxyReply::Data(payload) => Ok(payload),
			ProxyReply::Json(_) => Err(-errno::EIO),
		}
	}

	fn directory_entries(&mut self, path: &str) -> std::result::Result<Vec<DirentInfo>, i32> {
		let current = self.intern(path.to_owned());
		let parent = self.intern(parent_path(path).to_owned());
		let mut listed = self.list(path)?.entries;
		listed.sort_unstable_by(|left, right| left.name.cmp(&right.name));

		let mut entries = vec![
			DirentInfo {
				name:  b".".to_vec(),
				ino:   current,
				kind:  proto::Kind::Dir,
				size:  0,
				mtime: 0,
			},
			DirentInfo {
				name:  b"..".to_vec(),
				ino:   parent,
				kind:  proto::Kind::Dir,
				size:  0,
				mtime: 0,
			},
		];
		for entry in listed {
			if !valid_segment(&entry.name) {
				continue;
			}
			let path = child_path(path, &entry.name).expect("validated path segment");
			let ino = self.intern(path);
			entries.push(DirentInfo {
				name: entry.name.into_bytes(),
				ino,
				kind: entry.kind,
				size: entry.size,
				mtime: entry.mtime,
			});
		}
		Ok(entries)
	}

	fn build_readdir(
		&mut self,
		path: &str,
		offset: u64,
		max: usize,
	) -> std::result::Result<Vec<u8>, i32> {
		let entries = self.directory_entries(path)?;
		let mut out = Vec::new();
		for (idx, entry) in entries.iter().enumerate() {
			if (idx as u64) < offset {
				continue;
			}
			let Some(reclen) = fs::DIRENT_HEADER
				.checked_add(entry.name.len())
				.map(fs::align8)
			else {
				continue;
			};
			if reclen > max.saturating_sub(out.len()) {
				break;
			}
			let Ok(namelen) = u32::try_from(entry.name.len()) else {
				continue;
			};
			let dirent = fs::FuseDirent {
				ino: entry.ino,
				off: idx as u64 + 1,
				namelen,
				type_: dtype(entry.kind),
			};
			out.extend_from_slice(fs::struct_bytes(&dirent));
			out.extend_from_slice(&entry.name);
			out.resize(out.len() + reclen - fs::DIRENT_HEADER - entry.name.len(), 0);
		}
		Ok(out)
	}

	fn build_readdirplus(
		&mut self,
		path: &str,
		offset: u64,
		max: usize,
	) -> std::result::Result<Vec<u8>, i32> {
		let entries = self.directory_entries(path)?;
		let mut out = Vec::new();
		for (idx, entry) in entries.iter().enumerate() {
			if (idx as u64) < offset {
				continue;
			}
			let Some(reclen) = std::mem::size_of::<fs::FuseEntryOut>()
				.checked_add(fs::DIRENT_HEADER)
				.and_then(|len| len.checked_add(entry.name.len()))
				.map(fs::align8)
			else {
				continue;
			};
			if reclen > max.saturating_sub(out.len()) {
				break;
			}
			let Ok(namelen) = u32::try_from(entry.name.len()) else {
				continue;
			};
			let entry_out = fs::FuseEntryOut {
				nodeid:           entry.ino,
				generation:       0,
				entry_valid:      REMOTE_TIMEOUT_SEC,
				attr_valid:       REMOTE_TIMEOUT_SEC,
				entry_valid_nsec: 0,
				attr_valid_nsec:  0,
				attr:             attr(entry.ino, entry.kind, entry.size, entry.mtime),
			};
			let dirent = fs::FuseDirent {
				ino: entry.ino,
				off: idx as u64 + 1,
				namelen,
				type_: dtype(entry.kind),
			};
			out.extend_from_slice(fs::struct_bytes(&entry_out));
			out.extend_from_slice(fs::struct_bytes(&dirent));
			out.extend_from_slice(&entry.name);
			out.resize(
				out.len() + reclen
					- std::mem::size_of::<fs::FuseEntryOut>()
					- fs::DIRENT_HEADER
					- entry.name.len(),
				0,
			);
		}
		Ok(out)
	}

	fn dispatch(
		&mut self,
		mem: &GuestMemoryMmap,
		req: &[u8],
		writable: &[(GuestAddress, u32)],
	) -> u32 {
		let Some(header) = fs::read_struct::<fs::FuseInHeader>(req, 0) else {
			return 0;
		};
		let req_len = header.len as usize;
		if req_len < fs::IN_HEADER_SIZE || req_len > req.len() {
			return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
		}
		let req = &req[..req_len];
		let body_cap = writable
			.iter()
			.try_fold(0usize, |acc, &(_, len)| acc.checked_add(len as usize))
			.unwrap_or(0)
			.saturating_sub(fs::OUT_HEADER_SIZE)
			.min(fs::MAX_WRITE as usize);

		match header.opcode {
			fs::FUSE_INIT => {
				let client_minor =
					fs::read_struct::<u32>(req, fs::IN_HEADER_SIZE + std::mem::size_of::<u32>())
						.unwrap_or(0);
				let out = fs::FuseInitOut {
					major: 7,
					minor: client_minor.min(fs::FUSE_PROTO_MINOR),
					max_readahead: fs::read_struct::<u32>(req, fs::IN_HEADER_SIZE + 8).unwrap_or(0),
					max_write: fs::MAX_WRITE,
					time_gran: 1,
					..Default::default()
				};
				fs::write_reply(mem, writable, header.unique, 0, fs::struct_bytes(&out))
			},
			fs::FUSE_LOOKUP => {
				let Some(name) = request_name(req) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				let Some(parent) = self.path(header.nodeid) else {
					return fs::write_reply(mem, writable, header.unique, -errno::ENOENT, &[]);
				};
				let Some(path) = child_path(&parent, name) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				match self.stat(&path) {
					Ok(stat) => {
						let nodeid = self.intern(path);
						let out = fs::FuseEntryOut {
							nodeid,
							generation: 0,
							entry_valid: REMOTE_TIMEOUT_SEC,
							attr_valid: REMOTE_TIMEOUT_SEC,
							entry_valid_nsec: 0,
							attr_valid_nsec: 0,
							attr: attr(nodeid, stat.kind, stat.size, stat.mtime),
						};
						fs::write_reply(mem, writable, header.unique, 0, fs::struct_bytes(&out))
					},
					Err(errno) => fs::write_reply(mem, writable, header.unique, errno, &[]),
				}
			},
			fs::FUSE_GETATTR => {
				let stat = if header.nodeid == fs::FUSE_ROOT_ID {
					Ok(proto::StatReply { kind: proto::Kind::Dir, size: 0, mtime: 0, etag: None })
				} else {
					self
						.path(header.nodeid)
						.ok_or(-errno::ENOENT)
						.and_then(|path| self.stat(&path))
				};
				match stat {
					Ok(stat) => {
						let out = fs::FuseAttrOut {
							attr_valid:      REMOTE_TIMEOUT_SEC,
							attr_valid_nsec: 0,
							dummy:           0,
							attr:            attr(header.nodeid, stat.kind, stat.size, stat.mtime),
						};
						fs::write_reply(mem, writable, header.unique, 0, fs::struct_bytes(&out))
					},
					Err(errno) => fs::write_reply(mem, writable, header.unique, errno, &[]),
				}
			},
			fs::FUSE_OPEN | fs::FUSE_OPENDIR => {
				let Some(flags) = fs::read_struct::<u32>(req, fs::IN_HEADER_SIZE) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				if write_intent(flags) {
					return fs::write_reply(mem, writable, header.unique, -errno::EROFS, &[]);
				}
				if self.path(header.nodeid).is_none() {
					return fs::write_reply(mem, writable, header.unique, -errno::ENOENT, &[]);
				}
				let out = fs::FuseOpenOut { fh: header.nodeid, ..Default::default() };
				fs::write_reply(mem, writable, header.unique, 0, fs::struct_bytes(&out))
			},
			fs::FUSE_READ => {
				let Some(read) = fs::read_struct::<fs::FuseReadIn>(req, fs::IN_HEADER_SIZE) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				let Some(path) = self.path(header.nodeid) else {
					return fs::write_reply(mem, writable, header.unique, -errno::ENOENT, &[]);
				};
				let len = (read.size as usize).min(body_cap) as u32;
				if len == 0 {
					return fs::write_reply(mem, writable, header.unique, 0, &[]);
				}
				match self.read(&path, read.offset, len) {
					Ok(mut bytes) => {
						bytes.truncate(len as usize);
						fs::write_reply(mem, writable, header.unique, 0, &bytes)
					},
					Err(errno) => fs::write_reply(mem, writable, header.unique, errno, &[]),
				}
			},
			fs::FUSE_READDIR | fs::FUSE_READDIRPLUS => {
				let Some(read) = fs::read_struct::<fs::FuseReadIn>(req, fs::IN_HEADER_SIZE) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				let Some(path) = self.path(header.nodeid) else {
					return fs::write_reply(mem, writable, header.unique, -errno::ENOENT, &[]);
				};
				let max = (read.size as usize).min(body_cap);
				let entries = if header.opcode == fs::FUSE_READDIRPLUS {
					self.build_readdirplus(&path, read.offset, max)
				} else {
					self.build_readdir(&path, read.offset, max)
				};
				match entries {
					Ok(entries) => fs::write_reply(mem, writable, header.unique, 0, &entries),
					Err(errno) => fs::write_reply(mem, writable, header.unique, errno, &[]),
				}
			},
			fs::FUSE_STATFS => {
				let out =
					fs::FuseKstatfs { bsize: 4096, namelen: 255, frsize: 4096, ..Default::default() };
				fs::write_reply(mem, writable, header.unique, 0, fs::struct_bytes(&out))
			},
			fs::FUSE_ACCESS => {
				let Some(mask) = fs::read_struct::<u32>(req, fs::IN_HEADER_SIZE) else {
					return fs::write_reply(mem, writable, header.unique, -errno::EINVAL, &[]);
				};
				let error = if mask & libc::W_OK as u32 != 0 {
					-errno::EROFS
				} else {
					0
				};
				fs::write_reply(mem, writable, header.unique, error, &[])
			},
			fs::FUSE_RELEASE
			| fs::FUSE_RELEASEDIR
			| fs::FUSE_FLUSH
			| fs::FUSE_FSYNC
			| fs::FUSE_FSYNCDIR => fs::write_reply(mem, writable, header.unique, 0, &[]),
			fs::FUSE_FORGET | fs::FUSE_BATCH_FORGET | fs::FUSE_INTERRUPT => 0,
			fs::FUSE_READLINK => fs::write_reply(mem, writable, header.unique, -errno::ENOSYS, &[]),
			fs::FUSE_SETATTR
			| fs::FUSE_SYMLINK
			| fs::FUSE_MKNOD
			| fs::FUSE_MKDIR
			| fs::FUSE_UNLINK
			| fs::FUSE_RMDIR
			| fs::FUSE_RENAME
			| fs::FUSE_LINK
			| fs::FUSE_WRITE
			| fs::FUSE_CREATE
			| fs::FUSE_FALLOCATE
			| fs::FUSE_RENAME2 => fs::write_reply(mem, writable, header.unique, -errno::EROFS, &[]),
			_ => fs::write_reply(mem, writable, header.unique, -errno::ENOSYS, &[]),
		}
	}
}

struct DirentInfo {
	name:  Vec<u8>,
	ino:   u64,
	kind:  proto::Kind,
	size:  u64,
	mtime: u64,
}

fn proxy_errno(code: &str) -> i32 {
	match code {
		"not_found" => -errno::ENOENT,
		"access" => -errno::EACCES,
		"bad_request" => -errno::EINVAL,
		"stale" | "io" => -errno::EIO,
		_ => -errno::EIO,
	}
}

fn valid_path(path: &str) -> bool {
	path.is_empty() || path.split('/').all(valid_segment)
}

fn valid_segment(segment: &str) -> bool {
	!segment.is_empty()
		&& segment != "."
		&& segment != ".."
		&& !segment.contains('/')
		&& !segment.as_bytes().contains(&0)
}

fn child_path(parent: &str, name: &str) -> Option<String> {
	if !valid_segment(name) {
		return None;
	}
	Some(if parent.is_empty() {
		name.to_owned()
	} else {
		format!("{parent}/{name}")
	})
}

fn parent_path(path: &str) -> &str {
	path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

fn request_name(req: &[u8]) -> Option<&str> {
	let tail = req.get(fs::IN_HEADER_SIZE..)?;
	let end = tail.iter().position(|&byte| byte == 0)?;
	let name = std::str::from_utf8(&tail[..end]).ok()?;
	valid_segment(name).then_some(name)
}

fn attr(nodeid: u64, kind: proto::Kind, size: u64, mtime: u64) -> fs::FuseAttr {
	let is_dir = kind == proto::Kind::Dir;
	let size = if is_dir { 0 } else { size };
	fs::FuseAttr {
		ino: nodeid,
		size,
		blocks: size.div_ceil(512),
		atime: mtime,
		mtime,
		ctime: mtime,
		mode: if is_dir { 0o040555 } else { 0o100444 },
		nlink: 1,
		uid: 0,
		gid: 0,
		blksize: 4096,
		..Default::default()
	}
}

#[allow(
	clippy::unnecessary_cast,
	reason = "libc DT constants have different integer types on supported hosts"
)]
const fn dtype(kind: proto::Kind) -> u32 {
	match kind {
		proto::Kind::File => libc::DT_REG as u32,
		proto::Kind::Dir => libc::DT_DIR as u32,
	}
}

#[allow(
	clippy::unnecessary_cast,
	reason = "libc O constants have different integer types on supported hosts"
)]
const fn write_intent(flags: u32) -> bool {
	let access = flags & libc::O_ACCMODE as u32;
	access == libc::O_WRONLY as u32
		|| access == libc::O_RDWR as u32
		|| flags & (libc::O_CREAT as u32 | libc::O_TRUNC as u32) != 0
}

/// Read-only virtio-fs device backed by a per-VM object-proxy Unix socket.
pub struct RemoteFs {
	config:         Vec<u8>,
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,
	state:          RemoteFsState,
	mem:            Option<GuestMemoryMmap>,
	interrupt:      Option<Arc<Interrupt>>,
	hiprio_queue:   Option<Queue>,
	req_queue:      Option<Queue>,
}

impl RemoteFs {
	/// Creates a device that connects lazily to the proxy at `sock`.
	pub fn new(tag: String, sock: PathBuf) -> Self {
		Self::with_state(tag.clone(), RemoteFsState::new(tag, sock))
	}

	/// Restores inode mappings while leaving the proxy connection disconnected.
	///
	/// # Errors
	///
	/// Returns an error when the saved inode table contains an invalid relative
	/// object path.
	pub fn restore(tag: String, sock: PathBuf, state: &FsStateSer) -> Result<Self> {
		Ok(Self::with_state(tag.clone(), RemoteFsState::restore(tag, sock, state)?))
	}

	/// Serializes the inode-to-mount-relative-object-path mapping.
	pub fn save(&self) -> FsStateSer {
		self.state.save()
	}

	fn with_state(tag: String, state: RemoteFsState) -> Self {
		let mut config = vec![0; fs::CONFIG_SPACE_SIZE];
		let tag_bytes = tag.as_bytes();
		let len = tag_bytes.len().min(fs::TAG_LEN);
		config[..len].copy_from_slice(&tag_bytes[..len]);
		config[fs::TAG_LEN..fs::TAG_LEN + 4].copy_from_slice(&fs::NUM_REQUEST_QUEUES.to_le_bytes());

		Self {
			config,
			features: 1u64 << VIRTIO_F_VERSION_1,
			acked_features: 0,
			queue_sizes: vec![fs::QUEUE_SIZE; fs::NUM_QUEUES],
			state,
			mem: None,
			interrupt: None,
			hiprio_queue: None,
			req_queue: None,
		}
	}
}

impl VirtioDevice for RemoteFs {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_FS
	}

	fn queue_max_sizes(&self) -> &[u16] {
		&self.queue_sizes
	}

	fn features(&self) -> u64 {
		self.features
	}

	fn ack_features(&mut self, value: u64) {
		self.acked_features = value & self.features;
	}

	fn read_config(&self, offset: u64, data: &mut [u8]) {
		let Ok(offset) = usize::try_from(offset) else {
			data.fill(0);
			return;
		};
		for (index, byte) in data.iter_mut().enumerate() {
			*byte = offset
				.checked_add(index)
				.and_then(|index| self.config.get(index))
				.copied()
				.unwrap_or(0);
		}
	}

	fn write_config(&mut self, _offset: u64, _data: &[u8]) {
		// virtio-fs config space is read-only for the driver.
	}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		let _ = (fs::HIPRIO_QUEUE, fs::REQUEST_QUEUE);
		let mut queues = queues.into_iter();
		self.hiprio_queue = queues.next();
		self.req_queue = queues.next();
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		self.acked_features = 0;
		self.mem = None;
		self.interrupt = None;
		self.hiprio_queue = None;
		self.req_queue = None;
		self.state.conn = None;
		Ok(())
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			return Ok(QueuePass::Drained);
		};
		let mut used = false;
		let state = &mut self.state;
		// Bound one pass: each request is a blocking proxy round-trip and
		// completes inline, so a spinning guest (or slow proxy) could
		// otherwise hold the shared worker indefinitely.
		let mut budget = QUEUE_PASS_BUDGET;

		if let Some(queue) = self.hiprio_queue.as_mut() {
			while budget != 0 {
				let Some(chain) = queue.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				let head = chain.head_index();
				if let Some((req, _)) = fs::split_chain(&mem, chain) {
					state.dispatch(&mem, &req, &[]);
				}
				queue.add_used(&mem, head, 0).map_err(|error| {
					err(format!("remote virtio-fs hiprio used-ring update failed: {error}"))
				})?;
				used = true;
			}
		}

		if let Some(queue) = self.req_queue.as_mut() {
			while budget != 0 {
				let Some(chain) = queue.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				let head = chain.head_index();
				let written = match fs::split_chain(&mem, chain) {
					Some((req, writable)) => state.dispatch(&mem, &req, &writable),
					None => 0,
				};
				queue.add_used(&mem, head, written).map_err(|error| {
					err(format!("remote virtio-fs request used-ring update failed: {error}"))
				})?;
				used = true;
			}
		}

		if used {
			interrupt.signal_used_queue()?;
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		let mut states = Vec::new();
		if let Some(queue) = &self.hiprio_queue {
			states.push(queue.state());
		}
		if let Some(queue) = &self.req_queue {
			states.push(queue.state());
		}
		states
	}
}

#[cfg(all(test, unix))]
mod tests {
	use std::{
		fs as stdfs,
		os::unix::net::UnixListener,
		path::{Path, PathBuf},
		thread::{self, JoinHandle},
		time::{SystemTime, UNIX_EPOCH},
	};

	use vm_memory::{Bytes, GuestAddress};

	use super::*;
	use crate::memory::GuestMemoryMmap;

	fn temp_dir(prefix: &str) -> PathBuf {
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock after epoch")
			.as_nanos();
		let path = PathBuf::from("/tmp").join(format!("{prefix}-{}-{nanos}", std::process::id()));
		stdfs::create_dir(&path).expect("temporary directory");
		path
	}

	fn json<T: serde::Serialize>(value: &T) -> Vec<u8> {
		serde_json::to_vec(value).expect("serializable protocol response")
	}

	fn err(code: &str) -> (u8, Vec<u8>) {
		(proto::ERR, json(&proto::ErrReply { code: code.to_owned(), msg: code.to_owned() }))
	}

	fn tree_reply(request: proto::Request) -> (u8, Vec<u8>) {
		match request {
			proto::Request::Stat { tag, path } => {
				assert_eq!(tag, "data");
				let stat = match path.as_str() {
					"" | "dir" => {
						proto::StatReply { kind: proto::Kind::Dir, size: 0, mtime: 100, etag: None }
					},
					"a.txt" => proto::StatReply {
						kind:  proto::Kind::File,
						size:  5,
						mtime: 101,
						etag:  Some("\"a\"".to_owned()),
					},
					"dir/b.txt" => proto::StatReply {
						kind:  proto::Kind::File,
						size:  6,
						mtime: 102,
						etag:  Some("\"b\"".to_owned()),
					},
					_ => return err("not_found"),
				};
				(proto::OK_JSON, json(&stat))
			},
			proto::Request::List { tag, path } => {
				assert_eq!(tag, "data");
				let entries = match path.as_str() {
					"" => vec![
						proto::Entry {
							name:  "a.txt".to_owned(),
							kind:  proto::Kind::File,
							size:  5,
							mtime: 101,
						},
						proto::Entry {
							name:  "dir".to_owned(),
							kind:  proto::Kind::Dir,
							size:  0,
							mtime: 100,
						},
					],
					"dir" => vec![proto::Entry {
						name:  "b.txt".to_owned(),
						kind:  proto::Kind::File,
						size:  6,
						mtime: 102,
					}],
					_ => return err("not_found"),
				};
				(proto::OK_JSON, json(&proto::ListReply { entries }))
			},
			proto::Request::Read { tag, path, offset, len } => {
				assert_eq!(tag, "data");
				let data = match path.as_str() {
					"a.txt" => b"hello".as_slice(),
					"dir/b.txt" => b"nested".as_slice(),
					_ => return err("not_found"),
				};
				let start = usize::try_from(offset).unwrap_or(usize::MAX);
				let data = data.get(start..).unwrap_or_default();
				(proto::OK_DATA, data[..data.len().min(len as usize)].to_vec())
			},
		}
	}

	fn serve_tree(sock: &Path, request_limit: Option<usize>) -> JoinHandle<()> {
		let listener = UnixListener::bind(sock).expect("proxy listener");
		thread::spawn(move || {
			let (mut stream, _) = listener.accept().expect("proxy accepts device");
			let mut served = 0usize;
			loop {
				let Ok((ty, id, payload)) = proto::read_frame(&mut stream) else {
					return;
				};
				assert_eq!(ty, proto::REQ);
				let request = serde_json::from_slice(&payload).expect("valid proxy request");
				let (ty, payload) = tree_reply(request);
				proto::write_frame(&mut stream, ty, id, &payload).expect("proxy response");
				served += 1;
				if request_limit.is_some_and(|limit| served == limit) {
					return;
				}
			}
		})
	}

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x2_0000)]).expect("guest memory")
	}

	fn fuse_request(opcode: u32, nodeid: u64, payload: &[u8]) -> Vec<u8> {
		let len = fs::IN_HEADER_SIZE + payload.len();
		let header =
			fs::FuseInHeader { len: len as u32, opcode, unique: 1, nodeid, ..Default::default() };
		let mut request = Vec::with_capacity(len);
		request.extend_from_slice(fs::struct_bytes(&header));
		request.extend_from_slice(payload);
		request
	}

	fn run_op(device: &mut RemoteFs, request: &[u8]) -> (i32, Vec<u8>) {
		let mem = guest_mem();
		let reply_at = GuestAddress(0x10_000);
		let writable = vec![(reply_at, 0x10_000u32)];
		let written = device.state.dispatch(&mem, request, &writable);

		let mut header = [0u8; fs::OUT_HEADER_SIZE];
		mem.read_slice(&mut header, reply_at).expect("reply header");
		let out: fs::FuseOutHeader = fs::read_struct(&header, 0).expect("valid reply header");
		assert_eq!(written, out.len, "reply len {} != written {written}", out.len);
		let body_len = (out.len as usize).saturating_sub(fs::OUT_HEADER_SIZE);
		let mut body = vec![0; body_len];
		if !body.is_empty() {
			mem.read_slice(&mut body, GuestAddress(reply_at.0 + fs::OUT_HEADER_SIZE as u64))
				.expect("reply body");
		}
		(out.error, body)
	}

	fn read_request(offset: u64, size: u32) -> Vec<u8> {
		fs::struct_bytes(&fs::FuseReadIn { offset, size, ..Default::default() }).to_vec()
	}

	#[test]
	fn remote_fs_serves_reads_listings_and_attrs() {
		let dir = temp_dir("vmon-remotefs");
		let sock = dir.join("proxy.sock");
		let proxy = serve_tree(&sock, None);
		let mut device = RemoteFs::new("data".to_owned(), sock.clone());

		let mut init = Vec::new();
		init.extend_from_slice(&7u32.to_le_bytes());
		init.extend_from_slice(&44u32.to_le_bytes());
		init.extend_from_slice(&0u32.to_le_bytes());
		assert_eq!(run_op(&mut device, &fuse_request(fs::FUSE_INIT, fs::FUSE_ROOT_ID, &init)).0, 0);

		let (error, body) =
			run_op(&mut device, &fuse_request(fs::FUSE_LOOKUP, fs::FUSE_ROOT_ID, b"a.txt\0"));
		assert_eq!(error, 0);
		let entry: fs::FuseEntryOut = fs::read_struct(&body, 0).expect("lookup entry");
		assert_eq!(entry.attr.mode, 0o100444);

		let open = (libc::O_RDONLY as u32).to_le_bytes();
		assert_eq!(run_op(&mut device, &fuse_request(fs::FUSE_OPEN, entry.nodeid, &open)).0, 0);
		let (error, body) =
			run_op(&mut device, &fuse_request(fs::FUSE_READ, entry.nodeid, &read_request(0, 5)));
		assert_eq!(error, 0);
		assert_eq!(body, b"hello");

		let (error, body) = run_op(
			&mut device,
			&fuse_request(fs::FUSE_READDIR, fs::FUSE_ROOT_ID, &read_request(0, 4096)),
		);
		assert_eq!(error, 0);
		assert!(
			body
				.windows(b"a.txt".len())
				.any(|window| window == b"a.txt")
		);
		assert!(body.windows(b"dir".len()).any(|window| window == b"dir"));

		let (error, body) = run_op(
			&mut device,
			&fuse_request(fs::FUSE_READDIRPLUS, fs::FUSE_ROOT_ID, &read_request(0, 4096)),
		);
		assert_eq!(error, 0);
		assert!(body.len() >= std::mem::size_of::<fs::FuseEntryOut>());
		assert!(
			body
				.windows(b"a.txt".len())
				.any(|window| window == b"a.txt")
		);

		let (error, body) =
			run_op(&mut device, &fuse_request(fs::FUSE_GETATTR, fs::FUSE_ROOT_ID, &[]));
		assert_eq!(error, 0);
		let root: fs::FuseAttrOut = fs::read_struct(&body, 0).expect("root attr");
		assert_eq!(root.attr.mode, 0o040555);

		let saved = device.save();
		let restored = RemoteFs::restore("data".to_owned(), sock, &saved).expect("restore state");
		assert_eq!(restored.state.path(entry.nodeid).as_deref(), Some("a.txt"));
		drop(restored);
		drop(device);
		proxy.join().expect("proxy exits");
		stdfs::remove_dir_all(dir).expect("remove temp directory");
	}

	#[test]
	fn remote_fs_rejects_mutating_requests() {
		let mut device = RemoteFs::new("data".to_owned(), PathBuf::from("/missing/proxy.sock"));
		assert_eq!(
			run_op(&mut device, &fuse_request(fs::FUSE_WRITE, fs::FUSE_ROOT_ID, &[])).0,
			-errno::EROFS
		);
		assert_eq!(
			run_op(&mut device, &fuse_request(fs::FUSE_MKDIR, fs::FUSE_ROOT_ID, b"new\0")).0,
			-errno::EROFS
		);
		let write_open = (libc::O_WRONLY as u32).to_le_bytes();
		assert_eq!(
			run_op(&mut device, &fuse_request(fs::FUSE_OPEN, fs::FUSE_ROOT_ID, &write_open)).0,
			-errno::EROFS
		);
		assert_eq!(
			run_op(&mut device, &fuse_request(u32::MAX, fs::FUSE_ROOT_ID, &[])).0,
			-errno::ENOSYS
		);
	}

	#[test]
	fn proxy_disconnect_reconnects_once_then_returns_eio() {
		let dir = temp_dir("vmon-remotefs-disconnect");
		let sock = dir.join("proxy.sock");
		let proxy = serve_tree(&sock, Some(1));
		let mut device = RemoteFs::new("data".to_owned(), sock);

		assert_eq!(
			run_op(&mut device, &fuse_request(fs::FUSE_LOOKUP, fs::FUSE_ROOT_ID, b"a.txt\0")).0,
			0
		);
		proxy.join().expect("proxy stopped after first request");
		assert_eq!(
			run_op(&mut device, &fuse_request(fs::FUSE_LOOKUP, fs::FUSE_ROOT_ID, b"dir\0")).0,
			-errno::EIO
		);
		drop(device);
		stdfs::remove_dir_all(dir).expect("remove temp directory");
	}

	#[test]
	fn restore_preserves_mount_relative_inode_paths() {
		let mut state =
			RemoteFsState::new("data".to_owned(), PathBuf::from("/tmp/vmon-remotefs-test.sock"));
		let inode = state.intern("dir/b.txt".to_owned());
		let saved = state.save();
		let restored = RemoteFs::restore(
			"data".to_owned(),
			PathBuf::from("/tmp/vmon-remotefs-test.sock"),
			&saved,
		)
		.expect("restore state");
		assert_eq!(restored.state.path(inode).as_deref(), Some("dir/b.txt"));
	}
}
