//! Windows virtio-fs backend.
//!
//! Uses Windows filesystem APIs while preserving the Linux FUSE wire protocol
//! expected by the guest. The current surface implements the read path used by
//! image and volume mounts; mutating requests return `EROFS` for read-only
//! shares and `ENOSYS` otherwise.

use std::{
	collections::HashMap,
	ffi::OsString,
	fs::{self, File, OpenOptions},
	io,
	os::windows::{
		ffi::OsStringExt,
		fs::{FileExt, OpenOptionsExt},
		io::AsRawHandle,
	},
	path::{Component, Path, PathBuf},
	sync::Arc,
};

use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_FS};
use virtio_queue::{Queue, QueueT};
use vm_memory::Bytes;
use windows_sys::Win32::{
	Foundation::HANDLE,
	Storage::FileSystem::{
		FILE_FLAG_BACKUP_SEMANTICS, FILE_NAME_NORMALIZED, GetFinalPathNameByHandleW, VOLUME_NAME_DOS,
	},
};

use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	snapshot::FsStateSer,
	virtio::{
		Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, descriptor_range_valid,
		fuse_errno as errno,
	},
};

const QUEUE_SIZE: u16 = 64;
const TAG_LEN: usize = 36;
const CONFIG_SPACE_SIZE: usize = TAG_LEN + 4;
const MAX_REQUEST_SIZE: usize = 1 << 20;
const MAX_READ_SIZE: usize = 1 << 20;
const FUSE_ROOT_ID: u64 = 1;
const FUSE_LOOKUP: u32 = 1;
const FUSE_GETATTR: u32 = 3;
const FUSE_OPEN: u32 = 14;
const FUSE_READ: u32 = 15;
const FUSE_RELEASE: u32 = 18;
const FUSE_INIT: u32 = 26;
const FUSE_OPENDIR: u32 = 27;
const FUSE_RELEASEDIR: u32 = 29;
const FUSE_ACCESS: u32 = 34;

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct InHeader {
	len:          u32,
	opcode:       u32,
	unique:       u64,
	nodeid:       u64,
	uid:          u32,
	gid:          u32,
	pid:          u32,
	total_extlen: u16,
	padding:      u16,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct OutHeader {
	len:    u32,
	error:  i32,
	unique: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Attr {
	ino:       u64,
	size:      u64,
	blocks:    u64,
	atime:     u64,
	mtime:     u64,
	ctime:     u64,
	atimensec: u32,
	mtimensec: u32,
	ctimensec: u32,
	mode:      u32,
	nlink:     u32,
	uid:       u32,
	gid:       u32,
	rdev:      u32,
	blksize:   u32,
	flags:     u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct AttrOut {
	attr_valid:      u64,
	attr_valid_nsec: u32,
	dummy:           u32,
	attr:            Attr,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct EntryOut {
	nodeid:           u64,
	generation:       u64,
	entry_valid:      u64,
	attr_valid:       u64,
	entry_valid_nsec: u32,
	attr_valid_nsec:  u32,
	attr:             Attr,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct OpenOut {
	fh:         u64,
	open_flags: u32,
	padding:    u32,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ReadIn {
	fh:         u64,
	offset:     u64,
	size:       u32,
	read_flags: u32,
	lock_owner: u64,
	flags:      u32,
	padding:    u32,
}

const fn pod<T: Copy>(value: &T) -> &[u8] {
	// SAFETY: all callers pass repr(C), plain-data FUSE wire structures.
	unsafe { std::slice::from_raw_parts(std::ptr::from_ref(value).cast(), std::mem::size_of::<T>()) }
}

fn read_pod<T: Copy + Default>(bytes: &[u8], offset: usize) -> Option<T> {
	let end = offset.checked_add(std::mem::size_of::<T>())?;
	let source = bytes.get(offset..end)?;
	let mut value = T::default();
	// SAFETY: source and destination are valid non-overlapping byte ranges.
	unsafe {
		std::ptr::copy_nonoverlapping(
			source.as_ptr(),
			std::ptr::from_mut(&mut value).cast(),
			source.len(),
		);
	};
	Some(value)
}

fn metadata_attr(id: u64, metadata: &fs::Metadata, read_only: bool) -> Attr {
	let directory = metadata.is_dir();
	let permissions = match (directory, read_only) {
		(true, true) => 0o040555,
		(true, false) => 0o040755,
		(false, true) => 0o100444,
		(false, false) => 0o100644,
	};
	Attr {
		ino: id,
		size: metadata.len(),
		blocks: metadata.len().div_ceil(512),
		mode: permissions,
		nlink: 1,
		blksize: 4096,
		..Default::default()
	}
}

fn open_host_path(path: &Path) -> io::Result<File> {
	OpenOptions::new()
		.read(true)
		.custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
		.open(path)
}

fn final_path(file: &File) -> io::Result<PathBuf> {
	let mut buffer = vec![0u16; 512];
	loop {
		// SAFETY: `file` remains open and `buffer` is writable for its advertised
		// length. The API does not retain either argument.
		let written = unsafe {
			GetFinalPathNameByHandleW(
				file.as_raw_handle() as HANDLE,
				buffer.as_mut_ptr(),
				buffer.len() as u32,
				FILE_NAME_NORMALIZED | VOLUME_NAME_DOS,
			)
		};
		if written == 0 {
			return Err(io::Error::last_os_error());
		}
		let written = written as usize;
		if written < buffer.len() {
			buffer.truncate(written);
			return Ok(PathBuf::from(OsString::from_wide(&buffer)));
		}
		buffer.resize(
			written
				.checked_add(1)
				.ok_or_else(|| io::Error::from(io::ErrorKind::OutOfMemory))?,
			0,
		);
	}
}

fn open_confined(root: &Path, path: &Path) -> io::Result<(File, PathBuf)> {
	let file = open_host_path(path)?;
	let resolved = final_path(&file)?;
	if !resolved.starts_with(root) {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			"virtio-fs path resolves outside the shared directory",
		));
	}
	Ok((file, resolved))
}

fn safe_relative(path: &Path) -> bool {
	!path.as_os_str().is_empty()
		&& path
			.components()
			.all(|part| matches!(part, Component::Normal(_)))
}

fn safe_lookup_name(name: &str) -> bool {
	if name.contains(':') {
		return false;
	}
	let mut parts = Path::new(name).components();
	matches!(parts.next(), Some(Component::Normal(_))) && parts.next().is_none()
}

fn fuse_errno(error: &io::Error) -> i32 {
	errno::from_io(error)
}

const fn mutating_opcode(opcode: u32) -> bool {
	matches!(
		opcode,
		4 | 6 | 8 | 9 | 10 | 11 | 12 | 13 | 16 | 20 | 21 | 23 | 24 | 35 | 38 | 39 | 40 | 43 | 45 | 47
	)
}

/// Windows host-directory virtio-fs device.
pub struct Fs {
	config:         Vec<u8>,
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,
	root:           PathBuf,
	read_only:      bool,
	inodes:         HashMap<u64, PathBuf>,
	by_path:        HashMap<PathBuf, u64>,
	next:           u64,
	mem:            Option<GuestMemoryMmap>,
	interrupt:      Option<Arc<Interrupt>>,
	queues:         Vec<Queue>,
}

impl Fs {
	/// Export a Windows host directory under a virtio-fs tag.
	pub fn new(tag: String, shared_dir: PathBuf, read_only: bool) -> Result<Self> {
		Self::with_state(tag, shared_dir, read_only, &FsStateSer { inodes: Vec::new(), next: 2 })
	}

	/// Restore the inode table for a Windows host-directory share.
	pub fn restore(
		tag: String,
		shared_dir: PathBuf,
		state: &FsStateSer,
		read_only: bool,
	) -> Result<Self> {
		Self::with_state(tag, shared_dir, read_only, state)
	}

	fn with_state(
		tag: String,
		shared_dir: PathBuf,
		read_only: bool,
		state: &FsStateSer,
	) -> Result<Self> {
		if tag.len() > TAG_LEN || tag.as_bytes().contains(&0) {
			return Err(err(format!("virtio-fs tag must be at most {TAG_LEN} non-NUL bytes")));
		}
		let root_file = open_host_path(&shared_dir)
			.map_err(|e| err(format!("virtio-fs shared dir {}: {e}", shared_dir.display())))?;
		let metadata = root_file
			.metadata()
			.map_err(|e| err(format!("virtio-fs shared dir {}: {e}", shared_dir.display())))?;
		if !metadata.is_dir() {
			return Err(err(format!(
				"virtio-fs shared dir {} is not a directory",
				shared_dir.display()
			)));
		}
		let root = final_path(&root_file)
			.map_err(|e| err(format!("resolving virtio-fs root {}: {e}", shared_dir.display())))?;

		let mut config = vec![0; CONFIG_SPACE_SIZE];
		config[..tag.len()].copy_from_slice(tag.as_bytes());
		config[TAG_LEN..].copy_from_slice(&1u32.to_le_bytes());
		let mut inodes = HashMap::from([(FUSE_ROOT_ID, root.clone())]);
		let mut by_path = HashMap::from([(root.clone(), FUSE_ROOT_ID)]);
		let mut greatest_id = FUSE_ROOT_ID;
		for (id, relative) in &state.inodes {
			if *id == FUSE_ROOT_ID && relative == "." {
				continue;
			}
			let relative = Path::new(relative);
			if *id <= FUSE_ROOT_ID || !safe_relative(relative) {
				return Err(err(format!(
					"invalid virtio-fs snapshot inode {id} path {}",
					relative.display()
				)));
			}
			let (_, path) = open_confined(&root, &root.join(relative)).map_err(|e| {
				err(format!("restoring virtio-fs inode {id} path {}: {e}", relative.display()))
			})?;
			if inodes.contains_key(id) || by_path.contains_key(&path) {
				return Err(err(format!(
					"duplicate virtio-fs snapshot inode {id} path {}",
					relative.display()
				)));
			}
			inodes.insert(*id, path.clone());
			by_path.insert(path, *id);
			greatest_id = greatest_id.max(*id);
		}
		let first_free = greatest_id
			.checked_add(1)
			.ok_or_else(|| err("virtio-fs snapshot exhausted inode IDs"))?;
		Ok(Self {
			config,
			features: 1 << VIRTIO_F_VERSION_1,
			acked_features: 0,
			queue_sizes: vec![QUEUE_SIZE; 2],
			root,
			read_only,
			inodes,
			by_path,
			next: state.next.max(first_free).max(2),
			mem: None,
			interrupt: None,
			queues: Vec::new(),
		})
	}

	/// Serialize the guest-visible inode table.
	pub fn save(&self) -> FsStateSer {
		let inodes = self
			.inodes
			.iter()
			.filter_map(|(id, path)| {
				(*id != FUSE_ROOT_ID)
					.then(|| {
						path
							.strip_prefix(&self.root)
							.ok()
							.map(|p| (*id, p.to_string_lossy().into_owned()))
					})
					.flatten()
			})
			.collect();
		FsStateSer { inodes, next: self.next }
	}

	fn intern(&mut self, path: PathBuf) -> Option<u64> {
		if let Some(id) = self.by_path.get(&path) {
			return Some(*id);
		}
		let id = self.next;
		self.next = self.next.checked_add(1)?;
		self.by_path.insert(path.clone(), id);
		self.inodes.insert(id, path);
		Some(id)
	}

	fn dispatch(&mut self, request: &[u8], max_body: usize) -> (i32, Vec<u8>) {
		let Some(header) = read_pod::<InHeader>(request, 0) else {
			return (-errno::EINVAL, Vec::new());
		};
		let header_len = header.len as usize;
		if header_len < std::mem::size_of::<InHeader>() || header_len > request.len() {
			return (-errno::EINVAL, Vec::new());
		}
		let request = &request[..header_len];
		let path = self.inodes.get(&header.nodeid).cloned();
		let result = match header.opcode {
			FUSE_INIT => {
				let mut out = vec![0u8; 64];
				out[0..4].copy_from_slice(&7u32.to_le_bytes());
				out[4..8].copy_from_slice(&44u32.to_le_bytes());
				out[20..24].copy_from_slice(&(MAX_READ_SIZE as u32).to_le_bytes());
				(0, out)
			},
			FUSE_LOOKUP => {
				let Some(parent) = path else {
					return (-errno::ENOENT, Vec::new());
				};
				let name = request
					.get(std::mem::size_of::<InHeader>()..)
					.and_then(|bytes| bytes.split(|byte| *byte == 0).next())
					.unwrap_or_default();
				let Ok(name) = std::str::from_utf8(name) else {
					return (-errno::EINVAL, Vec::new());
				};
				if !safe_lookup_name(name) {
					return (-errno::EINVAL, Vec::new());
				}
				let (file, candidate) = match open_confined(&self.root, &parent.join(name)) {
					Ok(value) => value,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				let metadata = match file.metadata() {
					Ok(metadata) => metadata,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				let Some(id) = self.intern(candidate) else {
					return (-errno::EOVERFLOW, Vec::new());
				};
				(
					0,
					pod(&EntryOut {
						nodeid: id,
						generation: 1,
						entry_valid: 1,
						attr_valid: 1,
						attr: metadata_attr(id, &metadata, self.read_only),
						..Default::default()
					})
					.to_vec(),
				)
			},
			FUSE_GETATTR => {
				let Some(path) = path else {
					return (-errno::ENOENT, Vec::new());
				};
				let (file, _) = match open_confined(&self.root, &path) {
					Ok(value) => value,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				let metadata = match file.metadata() {
					Ok(metadata) => metadata,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				(
					0,
					pod(&AttrOut {
						attr_valid: 1,
						attr: metadata_attr(header.nodeid, &metadata, self.read_only),
						..Default::default()
					})
					.to_vec(),
				)
			},
			FUSE_OPEN | FUSE_OPENDIR => {
				let Some(path) = path else {
					return (-errno::ENOENT, Vec::new());
				};
				let (file, _) = match open_confined(&self.root, &path) {
					Ok(value) => value,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				let is_dir = match file.metadata() {
					Ok(metadata) => metadata.is_dir(),
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				if header.opcode == FUSE_OPEN && is_dir {
					return (-errno::EISDIR, Vec::new());
				}
				if header.opcode == FUSE_OPENDIR && !is_dir {
					return (-errno::ENOTDIR, Vec::new());
				}
				(0, pod(&OpenOut { fh: header.nodeid, ..Default::default() }).to_vec())
			},
			FUSE_READ => {
				let Some(path) = path else {
					return (-errno::ENOENT, Vec::new());
				};
				let Some(input) = read_pod::<ReadIn>(request, std::mem::size_of::<InHeader>()) else {
					return (-errno::EINVAL, Vec::new());
				};
				if input.fh != header.nodeid {
					return (-errno::EINVAL, Vec::new());
				}
				let (file, _) = match open_confined(&self.root, &path) {
					Ok(value) => value,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				let mut bytes = vec![0; (input.size as usize).min(MAX_READ_SIZE).min(max_body)];
				let read = match file.seek_read(&mut bytes, input.offset) {
					Ok(read) => read,
					Err(error) => return (-fuse_errno(&error), Vec::new()),
				};
				bytes.truncate(read);
				(0, bytes)
			},
			FUSE_RELEASE | FUSE_RELEASEDIR => (0, Vec::new()),
			FUSE_ACCESS => {
				let Some(path) = path else {
					return (-errno::ENOENT, Vec::new());
				};
				match open_confined(&self.root, &path) {
					Ok(_) => (0, Vec::new()),
					Err(error) => (-fuse_errno(&error), Vec::new()),
				}
			},
			opcode if self.read_only && mutating_opcode(opcode) => (-errno::EROFS, Vec::new()),
			_ => (-errno::ENOSYS, Vec::new()),
		};
		if result.1.len() > max_body {
			(-errno::EOVERFLOW, Vec::new())
		} else {
			result
		}
	}
}

impl VirtioDevice for Fs {
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
		for (i, out) in data.iter_mut().enumerate() {
			*out = self.config.get(offset as usize + i).copied().unwrap_or(0);
		}
	}

	fn write_config(&mut self, _: u64, _: &[u8]) {}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		self.queues = queues;
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		self.mem = None;
		self.interrupt = None;
		self.queues.clear();
		Ok(())
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		let Some(mem) = self.mem.clone() else {
			return Ok(QueuePass::Drained);
		};
		let Some(interrupt) = self.interrupt.clone() else {
			return Ok(QueuePass::Drained);
		};
		if self.queues.len() <= 1 {
			return Ok(QueuePass::Drained);
		}
		let mut queue = self.queues.remove(1);
		// Bound one pass: FUSE requests complete inline, so a spinning guest
		// could otherwise keep the ring non-empty forever; `run_worker` re-arms
		// the queue event on `Budgeted`.
		let mut budget = QUEUE_PASS_BUDGET;
		let processed = (|| -> Result<bool> {
			let mut signal = false;
			while budget != 0 {
				let Some(chain) = queue.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				let head = chain.head_index();
				let descriptors: Vec<_> = chain.collect();
				let mut request = Vec::new();
				let mut writable = Vec::new();
				let mut writable_len = 0usize;
				let mut saw_writable = false;
				let mut valid = true;
				for descriptor in descriptors {
					if !descriptor_range_valid(&mem, descriptor.addr(), descriptor.len()) {
						valid = false;
						break;
					}
					let len = descriptor.len() as usize;
					if descriptor.is_write_only() {
						saw_writable = true;
						let Some(total) = writable_len.checked_add(len) else {
							valid = false;
							break;
						};
						writable_len = total;
						writable.push((descriptor.addr(), descriptor.len()));
					} else {
						if saw_writable {
							valid = false;
							break;
						}
						let Some(total) = request.len().checked_add(len) else {
							valid = false;
							break;
						};
						if total > MAX_REQUEST_SIZE {
							valid = false;
							break;
						}
						let start = request.len();
						request.resize(total, 0);
						mem.read_slice(&mut request[start..], descriptor.addr())?;
					}
				}
				if !valid || writable_len < std::mem::size_of::<OutHeader>() {
					queue.add_used(&mem, head, 0)?;
					signal = true;
					continue;
				}
				let unique = read_pod::<InHeader>(&request, 0).map_or(0, |header| header.unique);
				let max_body = writable_len - std::mem::size_of::<OutHeader>();
				let (error, body) = self.dispatch(&request, max_body);
				let header = OutHeader {
					len: (std::mem::size_of::<OutHeader>() + body.len()) as u32,
					error,
					unique,
				};
				let mut reply = pod(&header).to_vec();
				reply.extend_from_slice(&body);
				let mut copied = 0usize;
				for (addr, len) in writable {
					if copied == reply.len() {
						break;
					}
					let count = (len as usize).min(reply.len() - copied);
					mem.write_slice(&reply[copied..copied + count], addr)?;
					copied += count;
				}
				queue.add_used(&mem, head, copied as u32)?;
				signal = true;
			}
			Ok(signal)
		})();
		self.queues.insert(1, queue);
		if processed? {
			interrupt.signal_used_queue()?;
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		self.queues.iter().map(|queue| queue.state()).collect()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU64, Ordering};

	use super::*;

	static NEXT_SHARE: AtomicU64 = AtomicU64::new(0);

	struct TempShare {
		base:    PathBuf,
		root:    PathBuf,
		outside: PathBuf,
	}

	impl TempShare {
		fn new() -> Self {
			let base = std::env::temp_dir().join(format!(
				"vmon-fs-windows-{}-{}",
				std::process::id(),
				NEXT_SHARE.fetch_add(1, Ordering::Relaxed)
			));
			let root = base.join("share");
			let outside = base.join("outside");
			fs::create_dir_all(&root).unwrap();
			fs::create_dir(&outside).unwrap();
			Self { base, root, outside }
		}
	}

	impl Drop for TempShare {
		fn drop(&mut self) {
			fs::remove_dir_all(&self.base).unwrap();
		}
	}

	fn request(opcode: u32, nodeid: u64, body: &[u8]) -> Vec<u8> {
		let header = InHeader {
			len: (std::mem::size_of::<InHeader>() + body.len()) as u32,
			opcode,
			unique: 7,
			nodeid,
			..Default::default()
		};
		let mut request = pod(&header).to_vec();
		request.extend_from_slice(body);
		request
	}

	#[test]
	fn path_validation_rejects_escape_components() {
		assert!(safe_relative(Path::new("directory\\file")));
		assert!(!safe_relative(Path::new("..\\file")));
		assert!(!safe_relative(Path::new("C:\\file")));
		assert!(!safe_relative(Path::new("\\file")));

		assert!(safe_lookup_name("ordinary.txt"));
		assert!(!safe_lookup_name(""));
		assert!(!safe_lookup_name("."));
		assert!(!safe_lookup_name(".."));
		assert!(!safe_lookup_name("directory\\file"));
		assert!(!safe_lookup_name("file:stream"));
	}

	#[test]
	fn opened_handle_must_resolve_beneath_share() {
		let share = TempShare::new();
		let inside = share.root.join("inside.txt");
		let outside = share.outside.join("outside.txt");
		fs::write(&inside, b"inside").unwrap();
		fs::write(&outside, b"outside").unwrap();

		let root = final_path(&open_host_path(&share.root).unwrap()).unwrap();
		assert!(open_confined(&root, &inside).is_ok());
		let error = open_confined(&root, &outside).unwrap_err();
		assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

		let link = share.root.join("outside-link");
		if std::os::windows::fs::symlink_file(&outside, &link).is_ok() {
			let error = open_confined(&root, &link).unwrap_err();
			assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
		}
	}

	#[test]
	fn restore_rejects_parent_traversal() {
		let share = TempShare::new();
		let state = FsStateSer { inodes: vec![(2, "..\\outside".to_owned())], next: 3 };
		assert!(Fs::restore("test".to_owned(), share.root.clone(), &state, true).is_err());
	}

	#[test]
	fn read_is_bounded_by_reply_capacity() {
		let share = TempShare::new();
		fs::write(share.root.join("data"), b"abcdef").unwrap();
		let mut device = Fs::new("test".to_owned(), share.root.clone(), true).unwrap();

		let lookup = request(FUSE_LOOKUP, FUSE_ROOT_ID, b"data\0");
		let (error, body) = device.dispatch(&lookup, std::mem::size_of::<EntryOut>());
		assert_eq!(error, 0);
		let entry = read_pod::<EntryOut>(&body, 0).unwrap();

		let input = ReadIn { fh: entry.nodeid, size: u32::MAX, ..Default::default() };
		let read = request(FUSE_READ, entry.nodeid, pod(&input));
		let (error, body) = device.dispatch(&read, 3);
		assert_eq!(error, 0);
		assert_eq!(body, b"abc");
	}

	#[test]
	fn malformed_and_unsupported_requests_return_stable_errors() {
		let share = TempShare::new();
		let mut device = Fs::new("test".to_owned(), share.root.clone(), true).unwrap();

		let mut malformed = request(FUSE_GETATTR, FUSE_ROOT_ID, &[]);
		malformed[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
		assert_eq!(device.dispatch(&malformed, 0).0, -errno::EINVAL);
		assert_eq!(device.dispatch(&request(16, FUSE_ROOT_ID, &[]), 0).0, -errno::EROFS);
		assert_eq!(device.dispatch(&request(u32::MAX, FUSE_ROOT_ID, &[]), 0).0, -errno::ENOSYS);
	}
}
