//! In-VMM virtio-fs device: a read-only FUSE server over virtqueues.
//!
//! This is the self-contained alternative to vhost-user-fs — no external
//! `virtiofsd`. The device exports one host directory under a mount `tag`; the
//! guest does `mount -t virtiofs <tag> /mnt` and the common FUSE client in the
//! guest kernel drives it over the virtio request queue.
//!
//! Two virtqueues are configured (`num_request_queues = 1`): queue 0 is the
//! hiprio queue (FORGET / INTERRUPT — drained, never answered) and queue 1 is
//! the request queue carrying the FUSE traffic. A FUSE request chain is a run
//! of device-readable descriptors (the `fuse_in_header` + opcode args) followed
//! by device-writable descriptors (the `fuse_out_header` + reply); the two are
//! distinguished by [`Descriptor::is_write_only`](virtio_queue). We concatenate
//! the readable side into one request buffer and treat the writable side as one
//! contiguous reply buffer.
//!
//! The read path (INIT, GETATTR, LOOKUP, READLINK, OPEN/OPENDIR, READ, READDIR,
//! plus the trivial RELEASE/FLUSH/STATFS/ACCESS replies) is always served. When
//! the device is writable (`read_only == false`) the mutating opcodes are
//! served too: CREATE, WRITE, SETATTR, MKDIR, UNLINK, RMDIR, RENAME/RENAME2,
//! SYMLINK, LINK and FALLOCATE. On a read-only device every mutating opcode
//! (and a write-intent OPEN) is answered `-EROFS`; unknown opcodes are
//! `-ENOSYS`. Inode numbers (`nodeid`s) are interned lazily on LOOKUP and map
//! back to host paths confined to the shared directory (parent directories are
//! canonicalized and checked to stay under the root; final symlink components
//! are exposed as symlink inodes and never followed for host I/O).
//!
//! Endianness: the FUSE wire format is the guest's native byte order; both
//! supported targets (`x86_64`/`aarch64`-linux) are little-endian and match the
//! VMM host, so the `#[repr(C)]` POD structs are read/written by raw byte copy.

use std::{
	collections::HashMap,
	ffi::OsStr,
	fs::{self, File, Metadata},
	os::unix::{
		ffi::OsStrExt,
		fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt, PermissionsExt},
		io::AsRawFd,
	},
	path::{Path, PathBuf},
	sync::Arc,
};

use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_FS};
use virtio_queue::{DescriptorChain, Queue, QueueT};
use vm_memory::{Bytes, GuestAddress};

use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	snapshot::FsStateSer,
	virtio::{
		Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, descriptor_range_valid,
		fuse_errno as errno,
	},
};

pub(super) const QUEUE_SIZE: u16 = 64;
/// hiprio (0) + a single request queue (1).
pub(super) const NUM_QUEUES: usize = 2;
pub(super) const HIPRIO_QUEUE: usize = 0;
pub(super) const REQUEST_QUEUE: usize = 1;

/// `virtio_fs_config`: a 36-byte NUL-padded tag followed by
/// `num_request_queues`.
pub(super) const TAG_LEN: usize = 36;
pub(super) const CONFIG_SPACE_SIZE: usize = TAG_LEN + 4;
pub(super) const NUM_REQUEST_QUEUES: u32 = 1;

/// FUSE node id of the shared root directory.
pub(super) const FUSE_ROOT_ID: u64 = 1;
/// Highest FUSE minor version whose wire layout this server advertises.
/// Optional capabilities remain opt-in through INIT flags, which we leave unset
/// unless the operation is implemented.
pub(super) const FUSE_PROTO_MINOR: u32 = 44;
/// Cache validity (seconds) handed to the guest for attrs and dir entries.
/// Small so host-side changes to the read-only tree are noticed reasonably
/// promptly.
const TIMEOUT_SEC: u64 = 1;
/// Max bytes a single `FUSE_READ` reply body may carry, mirrored into
/// `max_write`.
pub(super) const MAX_WRITE: u32 = 1 << 20;

// FUSE opcodes (linux/fuse.h) handled here.
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

pub(super) const IN_HEADER_SIZE: usize = std::mem::size_of::<FuseInHeader>();
pub(super) const OUT_HEADER_SIZE: usize = std::mem::size_of::<FuseOutHeader>();
const MAX_REQUEST_SIZE: usize =
	IN_HEADER_SIZE + std::mem::size_of::<FuseWriteIn>() + MAX_WRITE as usize;
pub(super) const DIRENT_HEADER: usize = std::mem::size_of::<FuseDirent>();

// ---------------------------------------------------------------------------
// FUSE wire structures (linux/fuse.h, protocol 7.x). All `#[repr(C)]` POD with
// no internal padding (asserted below), so a raw byte copy yields the exact
// little-endian wire layout the guest FUSE client expects.
// ---------------------------------------------------------------------------

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct FuseOutHeader {
	pub(super) len:    u32,
	pub(super) error:  i32,
	pub(super) unique: u64,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct FuseAttrOut {
	pub(super) attr_valid:      u64,
	pub(super) attr_valid_nsec: u32,
	pub(super) dummy:           u32,
	pub(super) attr:            FuseAttr,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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
	pub(super) max_stack_depth:      u32,
	pub(super) request_timeout:      u16,
	pub(super) unused:               [u16; 11],
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct FuseOpenOut {
	pub(super) fh:         u64,
	pub(super) open_flags: u32,
	pub(super) backing_id: i32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub(super) struct FuseDirent {
	pub(super) ino:     u64,
	pub(super) off:     u64,
	pub(super) namelen: u32,
	pub(super) type_:   u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
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

// Wire sizes must match the kernel structs exactly; a mismatch means a padding
// surprise that would corrupt every reply, so pin them at compile time.
const _: () = assert!(IN_HEADER_SIZE == 40);
const _: () = assert!(OUT_HEADER_SIZE == 16);
const _: () = assert!(std::mem::size_of::<FuseAttr>() == 88);
const _: () = assert!(std::mem::size_of::<FuseAttrOut>() == 104);
const _: () = assert!(std::mem::size_of::<FuseEntryOut>() == 128);
const _: () = assert!(std::mem::size_of::<FuseInitOut>() == 64);
const _: () = assert!(std::mem::size_of::<FuseOpenOut>() == 16);
const _: () = assert!(std::mem::size_of::<FuseReadIn>() == 40);
const _: () = assert!(DIRENT_HEADER == 24);
const _: () = assert!(std::mem::size_of::<FuseKstatfs>() == 80);

// Mutating-path request/reply structs (writable FUSE). Same POD/raw-copy rules
// as the read-path structs above; sizes pinned below.
#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseCreateIn {
	flags:      u32,
	mode:       u32,
	umask:      u32,
	open_flags: u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseWriteIn {
	fh:          u64,
	offset:      u64,
	size:        u32,
	write_flags: u32,
	lock_owner:  u64,
	flags:       u32,
	padding:     u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseWriteOut {
	size:    u32,
	padding: u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseSetattrIn {
	valid:      u32,
	padding:    u32,
	fh:         u64,
	size:       u64,
	lock_owner: u64,
	atime:      u64,
	mtime:      u64,
	ctime:      u64,
	atimensec:  u32,
	mtimensec:  u32,
	ctimensec:  u32,
	mode:       u32,
	unused4:    u32,
	uid:        u32,
	gid:        u32,
	unused5:    u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseMkdirIn {
	mode:  u32,
	umask: u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseMknodIn {
	mode:    u32,
	rdev:    u32,
	umask:   u32,
	padding: u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseRenameIn {
	newdir: u64,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseRename2In {
	newdir:  u64,
	flags:   u32,
	padding: u32,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseLinkIn {
	oldnodeid: u64,
}

#[allow(
	dead_code,
	reason = "POD wire struct: fields are serialized as raw bytes, not all read by name."
)]
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct FuseFallocateIn {
	fh:      u64,
	offset:  u64,
	length:  u64,
	mode:    u32,
	padding: u32,
}

const _: () = assert!(std::mem::size_of::<FuseCreateIn>() == 16);
const _: () = assert!(std::mem::size_of::<FuseWriteIn>() == 40);
const _: () = assert!(std::mem::size_of::<FuseWriteOut>() == 8);
const _: () = assert!(std::mem::size_of::<FuseSetattrIn>() == 88);
const _: () = assert!(std::mem::size_of::<FuseMkdirIn>() == 8);
const _: () = assert!(std::mem::size_of::<FuseMknodIn>() == 16);
const _: () = assert!(std::mem::size_of::<FuseRenameIn>() == 8);
const _: () = assert!(std::mem::size_of::<FuseRename2In>() == 16);
const _: () = assert!(std::mem::size_of::<FuseLinkIn>() == 8);
const _: () = assert!(std::mem::size_of::<FuseFallocateIn>() == 32);

// ---------------------------------------------------------------------------
// POD <-> bytes helpers
// ---------------------------------------------------------------------------

/// Decode a POD struct from `buf` at byte offset `off`, or `None` if the buffer
/// is too short.
pub(super) fn read_struct<T: Copy>(buf: &[u8], off: usize) -> Option<T> {
	let size = std::mem::size_of::<T>();
	if off.checked_add(size)? > buf.len() {
		return None;
	}
	// SAFETY: the bound above guarantees `size` readable bytes at `off`;
	// `read_unaligned` tolerates any alignment, and `T` is a `#[repr(C)]` POD,
	// so every byte pattern of the right length is a valid value.
	Some(unsafe { std::ptr::read_unaligned(buf.as_ptr().add(off).cast::<T>()) })
}

/// View a POD struct as its raw little-endian bytes.
pub(super) const fn struct_bytes<T: Copy>(v: &T) -> &[u8] {
	// SAFETY: reads exactly `size_of::<T>()` bytes of `v`'s representation; `T`
	// is a `#[repr(C)]` POD with no padding (asserted), so all bytes are init.
	unsafe { std::slice::from_raw_parts((v as *const T).cast::<u8>(), std::mem::size_of::<T>()) }
}

pub(super) const fn align8(x: usize) -> usize {
	(x + 7) & !7
}

// ---------------------------------------------------------------------------
// Reply / request marshalling
// ---------------------------------------------------------------------------

/// Write a FUSE reply (`fuse_out_header` + `body`) across the writable
/// descriptors in order and return the number of bytes written.
///
/// The body is clamped to the writable capacity so a malformed short chain can
/// never overflow, and `out_header.len` is set to exactly what we wrote — for
/// variable-length replies (READ/READDIR/INIT) the guest derives the payload
/// size from this field.
pub(super) fn write_reply(
	mem: &GuestMemoryMmap,
	writable: &[(GuestAddress, u32)],
	unique: u64,
	error: i32,
	body: &[u8],
) -> u32 {
	let Some(cap) = writable
		.iter()
		.try_fold(0usize, |acc, &(_, l)| acc.checked_add(l as usize))
	else {
		return 0;
	};
	if cap < OUT_HEADER_SIZE {
		return 0;
	}
	let body_fit = body.len().min(cap - OUT_HEADER_SIZE);
	let total = OUT_HEADER_SIZE + body_fit;

	let header = FuseOutHeader { len: total as u32, error, unique };
	let mut reply = Vec::with_capacity(total);
	reply.extend_from_slice(struct_bytes(&header));
	reply.extend_from_slice(&body[..body_fit]);

	let mut off = 0usize;
	for &(addr, dlen) in writable {
		if off >= reply.len() {
			break;
		}
		let n = (dlen as usize).min(reply.len() - off);
		if mem.write_slice(&reply[off..off + n], addr).is_err() {
			return 0;
		}
		off += n;
	}
	if off == reply.len() { total as u32 } else { 0 }
}

/// Concatenated readable request bytes plus the ordered writable `(addr, len)`
/// reply targets carved out of one FUSE descriptor chain.
pub(super) type SplitChain = (Vec<u8>, Vec<(GuestAddress, u32)>);

/// Split one FUSE request chain into the concatenated readable bytes (the
/// request) and the ordered writable `(addr, len)` targets (the reply space).
pub(super) fn split_chain(
	mem: &GuestMemoryMmap,
	chain: DescriptorChain<&GuestMemoryMmap>,
) -> Option<SplitChain> {
	let mut req = Vec::new();
	let mut writable = Vec::new();
	let mut seen_writable = false;
	let mut req_len = 0usize;
	for d in chain {
		if !descriptor_range_valid(mem, d.addr(), d.len()) {
			return None;
		}
		if d.is_write_only() {
			seen_writable = true;
			writable.push((d.addr(), d.len()));
		} else {
			if seen_writable {
				return None;
			}
			let len = d.len() as usize;
			req_len = req_len.checked_add(len)?;
			if req_len > MAX_REQUEST_SIZE {
				return None;
			}
			let start = req.len();
			req.resize(start + len, 0);
			if mem
				.read_slice(&mut req[start..start + len], d.addr())
				.is_err()
			{
				return None;
			}
		}
	}
	Some((req, writable))
}

/// Fill a `fuse_attr` from host metadata; `ino` is the FUSE nodeid (kept equal
/// to the reported inode number for consistency).
fn attr_from(ino: u64, md: &Metadata) -> FuseAttr {
	FuseAttr {
		ino,
		size: md.size(),
		blocks: md.blocks(),
		atime: md.atime() as u64,
		mtime: md.mtime() as u64,
		ctime: md.ctime() as u64,
		atimensec: md.atime_nsec() as u32,
		mtimensec: md.mtime_nsec() as u32,
		ctimensec: md.ctime_nsec() as u32,
		mode: md.mode(),
		nlink: md.nlink() as u32,
		uid: md.uid(),
		gid: md.gid(),
		rdev: md.rdev() as u32,
		blksize: md.blksize() as u32,
		flags: 0,
	}
}

/// Map a raw `st_mode` to a `readdir` `d_type` value.
#[allow(
	clippy::unnecessary_cast,
	reason = "libc S_*/DT_* constants are u16/u8 on macOS but u32 on Linux"
)]
const fn dtype_from_mode(mode: u32) -> u32 {
	match mode & libc::S_IFMT as u32 {
		m if m == libc::S_IFDIR as u32 => libc::DT_DIR as u32,
		m if m == libc::S_IFREG as u32 => libc::DT_REG as u32,
		m if m == libc::S_IFLNK as u32 => libc::DT_LNK as u32,
		m if m == libc::S_IFCHR as u32 => libc::DT_CHR as u32,
		m if m == libc::S_IFBLK as u32 => libc::DT_BLK as u32,
		m if m == libc::S_IFIFO as u32 => libc::DT_FIFO as u32,
		m if m == libc::S_IFSOCK as u32 => libc::DT_SOCK as u32,
		_ => libc::DT_UNKNOWN as u32,
	}
}

// ---------------------------------------------------------------------------
// Mutating-path helpers (writable FUSE)
// ---------------------------------------------------------------------------

// `fuse_setattr_in.valid` bits we honor (linux/fuse.h).
const FATTR_MODE: u32 = 1 << 0;
const FATTR_UID: u32 = 1 << 1;
const FATTR_GID: u32 = 1 << 2;
const FATTR_SIZE: u32 = 1 << 3;
const FATTR_ATIME: u32 = 1 << 4;
const FATTR_MTIME: u32 = 1 << 5;
const FATTR_ATIME_NOW: u32 = 1 << 7;
const FATTR_MTIME_NOW: u32 = 1 << 8;
/// `fuse_setattr_in.fh` is valid: prefer the open handle for the size
/// truncation.
const FATTR_FH: u32 = 1 << 6;

/// `RENAME_NOREPLACE` (linux/fs.h): fail rather than clobber an existing dest.
const RENAME_NOREPLACE: u32 = 1;
/// `FALLOC_FL_KEEP_SIZE` (linux/falloc.h): allocation must not extend EOF.
const FALLOC_FL_KEEP_SIZE: u32 = 1;

const fn rename2_flags_supported(flags: u32) -> bool {
	flags & !RENAME_NOREPLACE == 0
}

/// Map a host I/O error to the negative FUSE errno the guest expects
/// (defaulting to `-EIO` when the OS gave no errno, e.g. a Rust-side failure).
fn neg_errno(error: &std::io::Error) -> i32 {
	-errno::from_io(error)
}

/// First NUL-terminated name in `buf[off..]` (the single-name request tail).
fn first_name(buf: &[u8], off: usize) -> Option<&[u8]> {
	let rest = buf.get(off..)?;
	let end = rest.iter().position(|&b| b == 0)?;
	Some(&rest[..end])
}

/// The two consecutive NUL-terminated names in `buf[off..]` (rename/symlink).
fn two_names(buf: &[u8], off: usize) -> Option<(&[u8], &[u8])> {
	let rest = buf.get(off..)?;
	let first_end = rest.iter().position(|&b| b == 0)?;
	let second = &rest[first_end + 1..];
	let second_end = second.iter().position(|&b| b == 0)?;
	Some((&rest[..first_end], &second[..second_end]))
}

/// `fchown(2)`; a `u32::MAX` (`-1`) uid/gid leaves that owner unchanged.
fn chown_fd(file: &File, uid: u32, gid: u32) -> std::io::Result<()> {
	// SAFETY: `file.as_raw_fd()` is a valid open file descriptor for the
	// duration of the call; uid/gid are passed exactly as kernel uid_t/gid_t.
	let r = unsafe { libc::fchown(file.as_raw_fd(), uid as libc::uid_t, gid as libc::gid_t) };
	if r != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

/// Apply the atime/mtime a SETATTR requested via `futimens(2)`, honoring the
/// `*_NOW` and omit semantics for whichever time is not selected in `valid`.
fn set_times_fd(file: &File, sin: &FuseSetattrIn) -> std::io::Result<()> {
	const UTIME_NOW: i64 = (1 << 30) - 1;
	const UTIME_OMIT: i64 = (1 << 30) - 2;
	let spec = |sel: u32, now: u32, sec: u64, nsec: u32| libc::timespec {
		tv_sec:  sec as libc::time_t,
		tv_nsec: if sin.valid & now != 0 {
			UTIME_NOW
		} else if sin.valid & sel != 0 {
			nsec as i64
		} else {
			UTIME_OMIT
		},
	};
	let times = [
		spec(FATTR_ATIME, FATTR_ATIME_NOW, sin.atime, sin.atimensec),
		spec(FATTR_MTIME, FATTR_MTIME_NOW, sin.mtime, sin.mtimensec),
	];
	// SAFETY: `file.as_raw_fd()` is a valid open file descriptor, and `times`
	// points to the required two-element timespec array for the duration of the
	// call.
	let r = unsafe { libc::futimens(file.as_raw_fd(), times.as_ptr()) };
	if r != 0 {
		return Err(std::io::Error::last_os_error());
	}
	Ok(())
}

/// Apply the subset of attributes selected by `sin.valid`. Every mutating
/// operation is fd-based, so a post-check symlink swap cannot redirect chmod /
/// chown / utimens or truncation outside the shared root.
fn apply_setattr(path: &Path, fh: Option<&File>, sin: &FuseSetattrIn) -> std::io::Result<()> {
	let needs_attr_fd = sin.valid
		& (FATTR_MODE
			| FATTR_UID
			| FATTR_GID
			| FATTR_ATIME
			| FATTR_MTIME
			| FATTR_ATIME_NOW
			| FATTR_MTIME_NOW)
		!= 0;
	let mut reopened = None;

	if sin.valid & FATTR_SIZE != 0 {
		if let Some(f) = fh {
			f.set_len(sin.size)?;
		} else {
			let f = open_nofollow(path, true)?;
			f.set_len(sin.size)?;
			if needs_attr_fd {
				reopened = Some(f);
			}
		}
	}

	if needs_attr_fd {
		let file = if let Some(f) = fh {
			f
		} else {
			if reopened.is_none() {
				reopened = Some(open_nofollow(path, false)?);
			}
			reopened.as_ref().expect("reopened fd is populated")
		};
		if sin.valid & FATTR_MODE != 0 {
			file.set_permissions(fs::Permissions::from_mode(sin.mode & 0o7777))?;
		}
		if sin.valid & (FATTR_UID | FATTR_GID) != 0 {
			let uid = if sin.valid & FATTR_UID != 0 {
				sin.uid
			} else {
				u32::MAX
			};
			let gid = if sin.valid & FATTR_GID != 0 {
				sin.gid
			} else {
				u32::MAX
			};
			chown_fd(file, uid, gid)?;
		}
		if sin.valid & (FATTR_ATIME | FATTR_MTIME | FATTR_ATIME_NOW | FATTR_MTIME_NOW) != 0 {
			set_times_fd(file, sin)?;
		}
	}

	Ok(())
}

/// Open `path` for reading (and writing when `write`) without following a
/// final-component symlink (`O_NOFOLLOW`). A symlink as the final element makes
/// the open fail with `ELOOP`, so a post-LOOKUP swap cannot redirect the I/O
/// outside the shared root. Used by OPEN/CREATE and the unknown-fh fallbacks.
fn open_file_nofollow(path: &Path, write: bool, truncate: bool) -> std::io::Result<File> {
	fs::OpenOptions::new()
		.read(true)
		.write(write)
		.truncate(truncate)
		.custom_flags(libc::O_NOFOLLOW)
		.open(path)
}

fn open_nofollow(path: &Path, write: bool) -> std::io::Result<File> {
	open_file_nofollow(path, write, false)
}

/// Open the directory at `path` for OPENDIR without following a final-component
/// symlink (`O_NOFOLLOW | O_DIRECTORY`).
fn open_dir_nofollow(path: &Path) -> std::io::Result<File> {
	fs::OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY)
		.open(path)
}

/// Best-effort grow of `f` to `want` bytes (FALLOCATE mode 0). `KEEP_SIZE` is a
/// visible no-op; other modes are rejected before this helper is called.
fn fallocate_grow(f: &File, want: u64) -> std::io::Result<()> {
	let cur = f.metadata().map_or(0, |m| m.len());
	if want > cur {
		f.set_len(want)?;
	}
	Ok(())
}

/// Build a `FUSE_READDIR` reply body: a packed stream of `fuse_dirent` records
/// for ".", "..", and the directory's entries, resuming at `offset` and not
/// exceeding `max` bytes.
///
/// Entries are sorted by name so the per-entry `off` cookies stay stable across
/// the several READDIR calls the kernel issues to page a large directory.
fn build_readdir(dir: &Path, offset: u64, max: usize) -> Vec<u8> {
	let dir_ino = fs::symlink_metadata(dir).map_or(FUSE_ROOT_ID, |m| m.ino());

	// (name bytes, inode, d_type)
	let mut entries: Vec<(Vec<u8>, u64, u32)> = vec![
		(b".".to_vec(), dir_ino, libc::DT_DIR as u32),
		(b"..".to_vec(), dir_ino, libc::DT_DIR as u32),
	];
	if let Ok(rd) = fs::read_dir(dir) {
		let mut listing: Vec<(Vec<u8>, u64, u32)> = Vec::new();
		for ent in rd.flatten() {
			let name = ent.file_name();
			// `DirEntry::metadata` does not traverse symlinks, so a symlink keeps
			// its own inode/type here (the guest LOOKUP resolves it later).
			let (ino, dtype) = match ent.metadata() {
				Ok(m) => (m.ino(), dtype_from_mode(m.mode())),
				Err(_) => (0, libc::DT_UNKNOWN as u32),
			};
			listing.push((name.as_os_str().as_bytes().to_vec(), ino, dtype));
		}
		listing.sort_by(|a, b| a.0.cmp(&b.0));
		entries.extend(listing);
	}

	let mut out = Vec::new();
	for (idx, (name, ino, dtype)) in entries.iter().enumerate() {
		// Cookie of entry `idx` is `idx + 1`; the kernel resumes by passing the
		// last consumed cookie as `offset`, so emit entries with `idx >= offset`.
		if (idx as u64) < offset {
			continue;
		}
		let reclen = align8(DIRENT_HEADER + name.len());
		if out.len() + reclen > max {
			break;
		}
		let dirent = FuseDirent {
			ino:     *ino,
			off:     idx as u64 + 1,
			namelen: name.len() as u32,
			type_:   *dtype,
		};
		out.extend_from_slice(struct_bytes(&dirent));
		out.extend_from_slice(name);
		// Pad the record to the 8-byte boundary the FUSE dirent stream requires.
		out.resize(out.len() + (reclen - DIRENT_HEADER - name.len()), 0);
	}
	out
}

// ---------------------------------------------------------------------------
// FUSE server state (inode table + request dispatch)
// ---------------------------------------------------------------------------

/// The host-facing FUSE state, kept distinct from the virtqueue fields so the
/// worker can borrow the inode table and the request queue disjointly.
struct FsState {
	/// Canonicalized shared directory; every served path must stay under it.
	root:    PathBuf,
	/// nodeid -> host path.
	inodes:  HashMap<u64, PathBuf>,
	/// host path -> nodeid (dedupe so a path keeps one stable nodeid).
	by_path: HashMap<PathBuf, u64>,
	/// Next nodeid to hand out.
	next:    u64,
	/// Open file/dir handles keyed by the fh handed back from OPEN/OPENDIR/
	/// CREATE. Session-local: this map is intentionally NOT serialized into
	/// `FsStateSer`, so after a memory snapshot+restore it starts empty. A guest
	/// may still issue READ/WRITE/FALLOCATE/SETATTR-size with a pre-snapshot fh;
	/// those fall back to reopening the confined nodeid path with `O_NOFOLLOW`
	/// (see `open_confined`), so a stale fh never errors `EBADF` and never
	/// follows a final-component symlink.
	handles: HashMap<u64, File>,
	/// Next fh to hand out; fh 0 is reserved/invalid.
	next_fh: u64,
}

impl FsState {
	/// Return the existing nodeid for `path`, or assign and record a new one.
	fn intern(&mut self, path: PathBuf) -> u64 {
		if let Some(&id) = self.by_path.get(&path) {
			return id;
		}
		let id = self.next;
		self.next += 1;
		self.inodes.insert(id, path.clone());
		self.by_path.insert(path, id);
		id
	}

	/// Resolve `<parent>/<name>` to an existing path whose parent is confined to
	/// the shared root. The final component is deliberately not canonicalized:
	/// LOOKUP must report symlink inodes themselves, and READLINK returns their
	/// stored target later without ever touching host paths outside the share.
	fn resolve_child(&self, parent: &Path, name: &[u8]) -> Option<PathBuf> {
		if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
			return None;
		}
		let parent_canon = fs::canonicalize(parent).ok()?;
		if !parent_canon.starts_with(&self.root) {
			return None;
		}
		let child = parent_canon.join(OsStr::from_bytes(name));
		fs::symlink_metadata(&child).ok()?;
		Some(child)
	}

	/// Resolve `<parent>/<name>` for a child that need NOT exist yet (CREATE /
	/// MKDIR / SYMLINK / LINK / RENAME destinations). The parent is
	/// canonicalized and confined under root; the single slash-free name is
	/// then appended, so the result stays under root by construction without
	/// following a final symlink. Rejects empty / `.` / `..` / slash-bearing
	/// names.
	fn resolve_new_child(&self, parent: &Path, name: &[u8]) -> Option<PathBuf> {
		if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') {
			return None;
		}
		let parent_canon = fs::canonicalize(parent).ok()?;
		if !parent_canon.starts_with(&self.root) {
			return None;
		}
		Some(parent_canon.join(OsStr::from_bytes(name)))
	}

	/// Store `file` under a fresh handle id and return it (fh 0 is reserved).
	fn insert_handle(&mut self, file: File) -> u64 {
		loop {
			let fh = self.next_fh.max(1);
			self.next_fh = fh.checked_add(1).unwrap_or(1);
			if let std::collections::hash_map::Entry::Vacant(e) = self.handles.entry(fh) {
				e.insert(file);
				return fh;
			}
		}
	}

	fn forget_node(&mut self, nodeid: u64) {
		if nodeid == FUSE_ROOT_ID {
			return;
		}
		if let Some(path) = self.inodes.remove(&nodeid) {
			self.by_path.remove(&path);
		}
	}

	fn remove_inode_subtree(&mut self, root: &Path) {
		let ids: Vec<u64> = self
			.inodes
			.iter()
			.filter_map(|(&id, path)| {
				(id != FUSE_ROOT_ID && (path == root || path.starts_with(root))).then_some(id)
			})
			.collect();
		for id in ids {
			self.forget_node(id);
		}
	}

	fn rename_inode_subtree(&mut self, src: &Path, dst: &Path) {
		let moved: Vec<(u64, PathBuf)> = self
			.inodes
			.iter()
			.filter_map(|(&id, path)| {
				if id == FUSE_ROOT_ID || !(path == src || path.starts_with(src)) {
					return None;
				}
				let rel = path.strip_prefix(src).ok()?;
				let new_path = if rel.as_os_str().is_empty() {
					dst.to_path_buf()
				} else {
					dst.join(rel)
				};
				Some((id, new_path))
			})
			.collect();

		self.remove_inode_subtree(dst);
		for (id, new_path) in moved {
			if let Some(path) = self.inodes.get_mut(&id) {
				let old_path = std::mem::replace(path, new_path.clone());
				self.by_path.remove(&old_path);
				self.by_path.insert(new_path, id);
			}
		}
	}

	/// Return the current host path for `nodeid` after proving its parent still
	/// resolves under the shared root. The final component is not followed, so
	/// symlink inodes can still be observed with GETATTR.
	fn confined_lstat_path(&self, nodeid: u64) -> std::io::Result<PathBuf> {
		let path = self
			.inodes
			.get(&nodeid)
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::ENOENT))?;
		if path == &self.root {
			return Ok(path.clone());
		}
		let parent = path
			.parent()
			.ok_or_else(|| std::io::Error::from_raw_os_error(libc::EACCES))?;
		let parent_canon = fs::canonicalize(parent)?;
		if !parent_canon.starts_with(&self.root) {
			return Err(std::io::Error::from_raw_os_error(libc::EACCES));
		}
		Ok(path.clone())
	}

	/// Return the current host path for `nodeid`, refusing stale inode-table
	/// entries that now escape the shared root through parent symlink swaps or a
	/// final-component symlink.
	fn confined_existing_path(&self, nodeid: u64) -> std::io::Result<PathBuf> {
		let path = self.confined_lstat_path(nodeid)?;
		if fs::symlink_metadata(&path)?.file_type().is_symlink() {
			return Err(std::io::Error::from_raw_os_error(libc::ELOOP));
		}
		let canon = fs::canonicalize(path)?;
		if !canon.starts_with(&self.root) {
			return Err(std::io::Error::from_raw_os_error(libc::EACCES));
		}
		Ok(canon)
	}

	/// Reopen the host path for `nodeid` (confined under root) without following
	/// a final-component symlink. This is the unknown-fh fallback for
	/// READ/WRITE/FALLOCATE after a snapshot restore, where the session-local
	/// handle table is empty but the guest still references pre-snapshot fhs.
	fn open_confined(&self, nodeid: u64, write: bool) -> std::io::Result<File> {
		let path = self.confined_existing_path(nodeid)?;
		open_nofollow(&path, write)
	}

	/// LOOKUP-style reply for a freshly created `child`: lstat it, intern a
	/// nodeid, and send a `fuse_entry_out` (or the host errno on failure).
	fn reply_entry(
		&mut self,
		mem: &GuestMemoryMmap,
		writable: &[(GuestAddress, u32)],
		unique: u64,
		child: PathBuf,
	) -> u32 {
		match fs::symlink_metadata(&child) {
			Ok(md) => {
				let id = self.intern(child);
				let out = FuseEntryOut {
					nodeid:           id,
					generation:       0,
					entry_valid:      TIMEOUT_SEC,
					attr_valid:       TIMEOUT_SEC,
					entry_valid_nsec: 0,
					attr_valid_nsec:  0,
					attr:             attr_from(id, &md),
				};
				write_reply(mem, writable, unique, 0, struct_bytes(&out))
			},
			Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
		}
	}

	fn save(&self) -> FsStateSer {
		let mut inodes: Vec<(u64, String)> = self
			.inodes
			.iter()
			.filter_map(|(&id, path)| {
				let rel = path.strip_prefix(&self.root).ok()?;
				let rel = if rel.as_os_str().is_empty() {
					".".to_string()
				} else {
					rel.to_string_lossy().into_owned()
				};
				Some((id, rel))
			})
			.collect();
		inodes.sort_unstable_by_key(|&(id, _)| id);
		FsStateSer { inodes, next: self.next }
	}

	fn restore(shared_dir: &Path, state: &FsStateSer) -> Result<Self> {
		let shared_dir_display = shared_dir.display();
		let root = fs::canonicalize(shared_dir)
			.map_err(|e| err(format!("virtio-fs shared dir {shared_dir_display}: {e}")))?;
		if !root.is_dir() {
			crate::bail!("virtio-fs shared dir {} is not a directory", root.display());
		}

		let mut inodes = HashMap::new();
		inodes.insert(FUSE_ROOT_ID, root.clone());
		let mut by_path = HashMap::new();
		by_path.insert(root.clone(), FUSE_ROOT_ID);
		let mut next = state.next.max(FUSE_ROOT_ID + 1);

		for (id, saved) in &state.inodes {
			if *id == FUSE_ROOT_ID {
				continue;
			}
			let rel = PathBuf::from(saved);
			if rel.is_absolute() {
				continue;
			}
			let candidate = if saved == "." {
				root.clone()
			} else {
				root.join(rel)
			};
			let Some(parent) = candidate.parent() else {
				continue;
			};
			let Ok(parent_canon) = fs::canonicalize(parent) else {
				continue;
			};
			if !parent_canon.starts_with(&root) {
				continue;
			}
			let Ok(md) = fs::symlink_metadata(&candidate) else {
				continue;
			};
			let restored = if md.file_type().is_symlink() {
				candidate
			} else {
				let Ok(canon) = fs::canonicalize(&candidate) else {
					continue;
				};
				if !canon.starts_with(&root) {
					continue;
				}
				canon
			};
			if by_path.contains_key(&restored) {
				continue;
			}
			inodes.insert(*id, restored.clone());
			by_path.insert(restored, *id);
			next = next.max(id.saturating_add(1));
		}

		Ok(Self { root, inodes, by_path, next, handles: HashMap::new(), next_fh: 1 })
	}

	fn build_readdirplus(&mut self, dir: &Path, offset: u64, max: usize) -> Vec<u8> {
		let dir_id = self
			.by_path
			.get(dir)
			.copied()
			.unwrap_or_else(|| self.intern(dir.to_path_buf()));
		let parent_path = if dir == self.root {
			self.root.clone()
		} else {
			dir.parent().unwrap_or(&self.root).to_path_buf()
		};
		let parent_id = self
			.by_path
			.get(&parent_path)
			.copied()
			.unwrap_or_else(|| self.intern(parent_path.clone()));
		let mut entries: Vec<(Vec<u8>, u64, u32, FuseEntryOut)> = Vec::new();
		if let Ok(md) = fs::symlink_metadata(dir) {
			entries.push((b".".to_vec(), dir_id, libc::DT_DIR as u32, FuseEntryOut {
				nodeid:           dir_id,
				generation:       0,
				entry_valid:      TIMEOUT_SEC,
				attr_valid:       TIMEOUT_SEC,
				entry_valid_nsec: 0,
				attr_valid_nsec:  0,
				attr:             attr_from(dir_id, &md),
			}));
		}
		if let Ok(md) = fs::symlink_metadata(&parent_path) {
			entries.push((b"..".to_vec(), parent_id, libc::DT_DIR as u32, FuseEntryOut {
				nodeid:           parent_id,
				generation:       0,
				entry_valid:      TIMEOUT_SEC,
				attr_valid:       TIMEOUT_SEC,
				entry_valid_nsec: 0,
				attr_valid_nsec:  0,
				attr:             attr_from(parent_id, &md),
			}));
		}
		if let Ok(rd) = fs::read_dir(dir) {
			let mut listing: Vec<(Vec<u8>, u64, u32, FuseEntryOut)> = Vec::new();
			for ent in rd.flatten() {
				let name = ent.file_name().as_os_str().as_bytes().to_vec();
				let child = ent.path();
				let Ok(md) = fs::symlink_metadata(&child) else {
					continue;
				};
				let stored = if md.file_type().is_symlink() {
					child.clone()
				} else {
					fs::canonicalize(&child).unwrap_or_else(|_| child.clone())
				};
				let id = self.intern(stored);
				listing.push((name, id, dtype_from_mode(md.mode()), FuseEntryOut {
					nodeid:           id,
					generation:       0,
					entry_valid:      TIMEOUT_SEC,
					attr_valid:       TIMEOUT_SEC,
					entry_valid_nsec: 0,
					attr_valid_nsec:  0,
					attr:             attr_from(id, &md),
				}));
			}
			listing.sort_by(|a, b| a.0.cmp(&b.0));
			entries.extend(listing);
		}

		let mut out = Vec::new();
		let plus_header = std::mem::size_of::<FuseEntryOut>() + DIRENT_HEADER;
		for (idx, (name, ino, dtype, entry)) in entries.iter().enumerate() {
			if (idx as u64) < offset {
				continue;
			}
			let reclen = align8(plus_header + name.len());
			if out.len() + reclen > max {
				break;
			}
			let dirent = FuseDirent {
				ino:     *ino,
				off:     idx as u64 + 1,
				namelen: name.len() as u32,
				type_:   *dtype,
			};
			out.extend_from_slice(struct_bytes(entry));
			out.extend_from_slice(struct_bytes(&dirent));
			out.extend_from_slice(name);
			out.resize(out.len() + (reclen - plus_header - name.len()), 0);
		}
		out
	}

	/// Service one FUSE request and return the number of reply bytes written
	/// (0 for the no-reply opcodes).
	fn dispatch(
		&mut self,
		mem: &GuestMemoryMmap,
		req: &[u8],
		writable: &[(GuestAddress, u32)],
		read_only: bool,
	) -> u32 {
		let Some(h) = read_struct::<FuseInHeader>(req, 0) else {
			// Can't even read the header (hence no `unique` to answer): just
			// release the buffer with a zero-length used entry.
			return 0;
		};
		let unique = h.unique;
		let nodeid = h.nodeid;
		let req_len = h.len as usize;
		if req_len < IN_HEADER_SIZE || req_len > req.len() {
			return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
		}
		let req = &req[..req_len];
		// Capacity available for the reply body (after the out-header).
		let cap = writable
			.iter()
			.try_fold(0usize, |acc, &(_, l)| acc.checked_add(l as usize))
			.unwrap_or(0);
		let body_cap = cap.saturating_sub(OUT_HEADER_SIZE).min(MAX_WRITE as usize);
		match h.opcode {
			FUSE_INIT => {
				// fuse_init_in: major@40, minor@44, max_readahead@48, flags@52.
				let client_minor = read_struct::<u32>(req, IN_HEADER_SIZE + 4).unwrap_or(0);
				let max_readahead = read_struct::<u32>(req, IN_HEADER_SIZE + 8).unwrap_or(0);
				let out = FuseInitOut {
					major: 7,
					minor: client_minor.min(FUSE_PROTO_MINOR),
					max_readahead,
					max_write: MAX_WRITE,
					time_gran: 1,
					..Default::default()
				};
				write_reply(mem, writable, unique, 0, struct_bytes(&out))
			},
			FUSE_GETATTR => match self.confined_lstat_path(nodeid) {
				Ok(p) => match fs::symlink_metadata(&p) {
					Ok(md) => {
						let out = FuseAttrOut {
							attr_valid:      TIMEOUT_SEC,
							attr_valid_nsec: 0,
							dummy:           0,
							attr:            attr_from(nodeid, &md),
						};
						write_reply(mem, writable, unique, 0, struct_bytes(&out))
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				},
				Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
			},
			FUSE_LOOKUP => {
				let Some(name) = first_name(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let resolved = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_child(&parent, name));
				match resolved {
					Some(child) => match fs::symlink_metadata(&child) {
						Ok(md) => {
							let id = self.intern(child);
							let out = FuseEntryOut {
								nodeid:           id,
								generation:       0,
								entry_valid:      TIMEOUT_SEC,
								attr_valid:       TIMEOUT_SEC,
								entry_valid_nsec: 0,
								attr_valid_nsec:  0,
								attr:             attr_from(id, &md),
							};
							write_reply(mem, writable, unique, 0, struct_bytes(&out))
						},
						Err(_) => write_reply(mem, writable, unique, -errno::ENOENT, &[]),
					},
					None => write_reply(mem, writable, unique, -errno::ENOENT, &[]),
				}
			},
			FUSE_READLINK => match self.confined_lstat_path(nodeid) {
				Ok(path) => match fs::read_link(&path) {
					Ok(target) => write_reply(mem, writable, unique, 0, target.as_os_str().as_bytes()),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				},
				Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
			},
			FUSE_OPEN | FUSE_OPENDIR => {
				let path = match self.confined_existing_path(nodeid) {
					Ok(path) => path,
					Err(e) => return write_reply(mem, writable, unique, neg_errno(&e), &[]),
				};
				let is_dir = h.opcode == FUSE_OPENDIR;
				// `fuse_open_in.flags` are the guest's open(2) flags. Any
				// write/create/truncate intent on a read-only device is -EROFS.
				let flags = read_struct::<u32>(req, IN_HEADER_SIZE).unwrap_or(0);
				let accmode = flags & libc::O_ACCMODE as u32;
				let wants_write = !is_dir
					&& (accmode == libc::O_WRONLY as u32
						|| accmode == libc::O_RDWR as u32
						|| flags & (libc::O_CREAT as u32 | libc::O_TRUNC as u32) != 0);
				if read_only && wants_write {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				// Open the resolved, root-confined path without following a
				// final-component symlink, then track the real fd so later
				// READ/WRITE on this handle can never be redirected by a swap.
				let opened = if is_dir {
					open_dir_nofollow(&path)
				} else {
					open_file_nofollow(&path, wants_write, flags & libc::O_TRUNC as u32 != 0)
				};
				match opened {
					Ok(file) => {
						let fh = self.insert_handle(file);
						let out = FuseOpenOut { fh, open_flags: 0, backing_id: 0 };
						write_reply(mem, writable, unique, 0, struct_bytes(&out))
					},
					// ELOOP: the final component is a symlink — refuse it.
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_READ => {
				let Some(rin) = read_struct::<FuseReadIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let want = (rin.size as usize).min(body_cap);
				let mut buf = vec![0u8; want];
				// Prefer the tracked handle; fall back to a confined O_NOFOLLOW
				// reopen by nodeid (covers a stale, post-snapshot fh). pread
				// clamps to EOF, yielding a short read at the file's end.
				let res = if let Some(f) = self.handles.get(&rin.fh) {
					f.read_at(&mut buf, rin.offset)
				} else {
					match self.open_confined(nodeid, false) {
						Ok(f) => f.read_at(&mut buf, rin.offset),
						Err(e) => Err(e),
					}
				};
				match res {
					Ok(n) => {
						buf.truncate(n);
						write_reply(mem, writable, unique, 0, &buf)
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_READDIR | FUSE_READDIRPLUS => match read_struct::<FuseReadIn>(req, IN_HEADER_SIZE) {
				Some(rin) => match self.confined_existing_path(nodeid) {
					Ok(path) => {
						let max = (rin.size as usize).min(body_cap);
						let body = if h.opcode == FUSE_READDIRPLUS {
							self.build_readdirplus(&path, rin.offset, max)
						} else {
							build_readdir(&path, rin.offset, max)
						};
						write_reply(mem, writable, unique, 0, &body)
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				},
				None => write_reply(mem, writable, unique, -errno::EINVAL, &[]),
			},
			FUSE_STATFS => {
				let st = FuseKstatfs { bsize: 4096, namelen: 255, frsize: 4096, ..Default::default() };
				write_reply(mem, writable, unique, 0, struct_bytes(&st))
			},
			// Drop the tracked handle (POSIX: an unlinked file's fd stays valid
			// until release). `fuse_release_in.fh` is the leading u64.
			FUSE_RELEASE | FUSE_RELEASEDIR => {
				let fh = read_struct::<u64>(req, IN_HEADER_SIZE).unwrap_or(0);
				self.handles.remove(&fh);
				write_reply(mem, writable, unique, 0, &[])
			},
			// Flush the tracked handle to disk; a stale/unknown fh (post-restore)
			// has nothing host-side to sync, so just acknowledge.
			FUSE_FSYNC | FUSE_FSYNCDIR => {
				let fh = read_struct::<u64>(req, IN_HEADER_SIZE).unwrap_or(0);
				if let Some(f) = self.handles.get(&fh)
					&& let Err(e) = f.sync_all()
				{
					return write_reply(mem, writable, unique, neg_errno(&e), &[]);
				}
				write_reply(mem, writable, unique, 0, &[])
			},
			// No host-side state to tear down: acknowledge.
			FUSE_FLUSH | FUSE_ACCESS => write_reply(mem, writable, unique, 0, &[]),
			// No-reply opcodes: the buffer is device-readable only.
			FUSE_FORGET => {
				self.forget_node(nodeid);
				0
			},
			FUSE_BATCH_FORGET => {
				let count = read_struct::<u32>(req, IN_HEADER_SIZE).unwrap_or(0) as usize;
				let entries_off = IN_HEADER_SIZE + 8;
				for i in 0..count {
					let Some(off) = entries_off.checked_add(i.saturating_mul(16)) else {
						break;
					};
					let Some(id) = read_struct::<u64>(req, off) else {
						break;
					};
					self.forget_node(id);
				}
				0
			},
			FUSE_INTERRUPT => 0,
			FUSE_CREATE => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(cin) = read_struct::<FuseCreateIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(name) = first_name(req, IN_HEADER_SIZE + std::mem::size_of::<FuseCreateIn>())
				else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(child) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				// Create + open the child without following a final-component
				// symlink: with O_CREAT|O_NOFOLLOW an already-present symlink at
				// `child` fails ELOOP, so a pre-planted symlink cannot escape.
				let mut create = fs::OpenOptions::new();
				create
					.read(true)
					.write(true)
					.truncate(cin.flags & libc::O_TRUNC as u32 != 0)
					.custom_flags(libc::O_NOFOLLOW)
					.mode(cin.mode & 0o7777);
				if cin.flags & libc::O_EXCL as u32 != 0 {
					create.create_new(true);
				} else {
					create.create(true);
				}
				match create.open(&child) {
					Ok(file) => match fs::symlink_metadata(&child) {
						Ok(md) => {
							let id = self.intern(child);
							let fh = self.insert_handle(file);
							let entry = FuseEntryOut {
								nodeid:           id,
								generation:       0,
								entry_valid:      TIMEOUT_SEC,
								attr_valid:       TIMEOUT_SEC,
								entry_valid_nsec: 0,
								attr_valid_nsec:  0,
								attr:             attr_from(id, &md),
							};
							let open = FuseOpenOut { fh, open_flags: 0, backing_id: 0 };
							let mut body = Vec::with_capacity(
								std::mem::size_of::<FuseEntryOut>() + std::mem::size_of::<FuseOpenOut>(),
							);
							body.extend_from_slice(struct_bytes(&entry));
							body.extend_from_slice(struct_bytes(&open));
							write_reply(mem, writable, unique, 0, &body)
						},
						Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_WRITE => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(win) = read_struct::<FuseWriteIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let data_off = IN_HEADER_SIZE + std::mem::size_of::<FuseWriteIn>();
				let Some(end) = data_off.checked_add(win.size as usize) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(data) = req.get(data_off..end) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				// Write through the tracked handle; fall back to a confined
				// O_NOFOLLOW reopen by nodeid for a stale/unknown fh (post-
				// restore) so the fallback can't follow a final symlink either.
				let res = if let Some(f) = self.handles.get(&win.fh) {
					f.write_at(data, win.offset)
				} else {
					match self.open_confined(nodeid, true) {
						Ok(f) => f.write_at(data, win.offset),
						Err(e) => Err(e),
					}
				};
				match res {
					Ok(n) => {
						let out = FuseWriteOut { size: n as u32, padding: 0 };
						write_reply(mem, writable, unique, 0, struct_bytes(&out))
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_SETATTR => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(sin) = read_struct::<FuseSetattrIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let path = match self.confined_existing_path(nodeid) {
					Ok(path) => path,
					Err(e) => return write_reply(mem, writable, unique, neg_errno(&e), &[]),
				};
				// Truncation prefers the tracked handle (FATTR_FH); without one
				// apply_setattr reopens the confined path with O_NOFOLLOW.
				let fh = if sin.valid & FATTR_FH != 0 {
					self.handles.get(&sin.fh)
				} else {
					None
				};
				if let Err(e) = apply_setattr(&path, fh, &sin) {
					return write_reply(mem, writable, unique, neg_errno(&e), &[]);
				}
				match fs::symlink_metadata(&path) {
					Ok(md) => {
						let out = FuseAttrOut {
							attr_valid:      TIMEOUT_SEC,
							attr_valid_nsec: 0,
							dummy:           0,
							attr:            attr_from(nodeid, &md),
						};
						write_reply(mem, writable, unique, 0, struct_bytes(&out))
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_MKNOD => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(min) = read_struct::<FuseMknodIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(name) = first_name(req, IN_HEADER_SIZE + std::mem::size_of::<FuseMknodIn>())
				else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(child) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				#[allow(clippy::unnecessary_cast, reason = "S_IF* are c_int on macOS, c_uint on Linux")]
				let (ifmt, ifreg) = (libc::S_IFMT as u32, libc::S_IFREG as u32);
				let kind = min.mode & ifmt;
				if kind != 0 && kind != ifreg {
					return write_reply(mem, writable, unique, -errno::EOPNOTSUPP, &[]);
				}
				let mut create = fs::OpenOptions::new();
				create
					.read(true)
					.write(true)
					.create_new(true)
					.mode(min.mode & !min.umask & 0o7777);
				match create.open(&child) {
					Ok(_) => self.reply_entry(mem, writable, unique, child),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_MKDIR => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(min) = read_struct::<FuseMkdirIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(name) = first_name(req, IN_HEADER_SIZE + std::mem::size_of::<FuseMkdirIn>())
				else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(child) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				match fs::DirBuilder::new()
					.mode(min.mode & !min.umask & 0o7777)
					.create(&child)
				{
					Ok(()) => self.reply_entry(mem, writable, unique, child),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_UNLINK => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(name) = first_name(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(child) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				match fs::remove_file(&child) {
					Ok(()) => {
						self.remove_inode_subtree(&child);
						write_reply(mem, writable, unique, 0, &[])
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_RMDIR => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(name) = first_name(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(child) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				match fs::remove_dir(&child) {
					Ok(()) => {
						self.remove_inode_subtree(&child);
						write_reply(mem, writable, unique, 0, &[])
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_RENAME | FUSE_RENAME2 => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				// v1 carries `fuse_rename_in { newdir }`; v2 `fuse_rename2_in {
				// newdir, flags, padding }`, both followed by the two names.
				let (newdir, flags, names_off) = if h.opcode == FUSE_RENAME2 {
					match read_struct::<FuseRename2In>(req, IN_HEADER_SIZE) {
						Some(r) => {
							(r.newdir, r.flags, IN_HEADER_SIZE + std::mem::size_of::<FuseRename2In>())
						},
						None => return write_reply(mem, writable, unique, -errno::EINVAL, &[]),
					}
				} else {
					match read_struct::<FuseRenameIn>(req, IN_HEADER_SIZE) {
						Some(r) => (r.newdir, 0, IN_HEADER_SIZE + std::mem::size_of::<FuseRenameIn>()),
						None => return write_reply(mem, writable, unique, -errno::EINVAL, &[]),
					}
				};
				let Some((oldname, newname)) = two_names(req, names_off) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let (Some(op), Some(np)) =
					(self.inodes.get(&nodeid).cloned(), self.inodes.get(&newdir).cloned())
				else {
					return write_reply(mem, writable, unique, -errno::ENOENT, &[]);
				};
				let (Some(src), Some(dst)) =
					(self.resolve_new_child(&op, oldname), self.resolve_new_child(&np, newname))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				if !rename2_flags_supported(flags) {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				}
				if flags & RENAME_NOREPLACE != 0 && dst.exists() {
					return write_reply(mem, writable, unique, -errno::EEXIST, &[]);
				}
				match fs::rename(&src, &dst) {
					Ok(()) => {
						self.rename_inode_subtree(&src, &dst);
						write_reply(mem, writable, unique, 0, &[])
					},
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_SYMLINK => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some((name, target)) = two_names(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(link) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				let target = PathBuf::from(OsStr::from_bytes(target));
				match std::os::unix::fs::symlink(&target, &link) {
					Ok(()) => self.reply_entry(mem, writable, unique, link),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_LINK => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(lin) = read_struct::<FuseLinkIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let Some(name) = first_name(req, IN_HEADER_SIZE + std::mem::size_of::<FuseLinkIn>())
				else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				let src = match self.confined_existing_path(lin.oldnodeid) {
					Ok(src) => src,
					Err(e) => return write_reply(mem, writable, unique, neg_errno(&e), &[]),
				};
				let Some(dst) = self
					.inodes
					.get(&nodeid)
					.cloned()
					.and_then(|parent| self.resolve_new_child(&parent, name))
				else {
					return write_reply(mem, writable, unique, -errno::EACCES, &[]);
				};
				match fs::hard_link(&src, &dst) {
					Ok(()) => self.reply_entry(mem, writable, unique, dst),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			FUSE_FALLOCATE => {
				if read_only {
					return write_reply(mem, writable, unique, -errno::EROFS, &[]);
				}
				let Some(fin) = read_struct::<FuseFallocateIn>(req, IN_HEADER_SIZE) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				if fin.mode & !FALLOC_FL_KEEP_SIZE != 0 {
					return write_reply(mem, writable, unique, -errno::EOPNOTSUPP, &[]);
				}
				let Some(want) = fin.offset.checked_add(fin.length) else {
					return write_reply(mem, writable, unique, -errno::EINVAL, &[]);
				};
				if fin.mode & FALLOC_FL_KEEP_SIZE != 0 {
					return write_reply(mem, writable, unique, 0, &[]);
				}
				// Operate on the tracked handle; fall back to a confined
				// O_NOFOLLOW reopen by nodeid for a stale/unknown fh.
				let res = if let Some(f) = self.handles.get(&fin.fh) {
					fallocate_grow(f, want)
				} else {
					match self.open_confined(nodeid, true) {
						Ok(f) => fallocate_grow(&f, want),
						Err(e) => Err(e),
					}
				};
				match res {
					Ok(()) => write_reply(mem, writable, unique, 0, &[]),
					Err(e) => write_reply(mem, writable, unique, neg_errno(&e), &[]),
				}
			},
			// Unknown / still-unsupported opcodes (xattrs, readdirplus, ioctl,
			// ...): -ENOSYS makes the guest stop asking and fall back where it can.
			_ => write_reply(mem, writable, unique, -errno::ENOSYS, &[]),
		}
	}
}

// ---------------------------------------------------------------------------
// virtio device
// ---------------------------------------------------------------------------

pub struct Fs {
	/// `virtio_fs_config` bytes (tag + `num_request_queues`).
	config:         Vec<u8>,
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,

	/// When set, every mutating FUSE opcode is rejected with `-EROFS`.
	read_only: bool,
	state:     FsState,

	mem:          Option<GuestMemoryMmap>,
	interrupt:    Option<Arc<Interrupt>>,
	hiprio_queue: Option<Queue>,
	req_queue:    Option<Queue>,
}

impl Fs {
	/// Build a virtio-fs device exporting `shared_dir` under `tag`. When
	/// `read_only`, every mutating FUSE opcode is rejected with `-EROFS`.
	pub fn new(tag: String, shared_dir: PathBuf, read_only: bool) -> Result<Self> {
		let shared_dir_display = shared_dir.display();
		let root = fs::canonicalize(&shared_dir)
			.map_err(|e| err(format!("virtio-fs shared dir {shared_dir_display}: {e}")))?;
		if !root.is_dir() {
			crate::bail!("virtio-fs shared dir {} is not a directory", root.display());
		}

		let mut inodes = HashMap::new();
		inodes.insert(FUSE_ROOT_ID, root.clone());
		let mut by_path = HashMap::new();
		by_path.insert(root.clone(), FUSE_ROOT_ID);

		Ok(Self::with_state(
			tag,
			FsState {
				root,
				inodes,
				by_path,
				next: FUSE_ROOT_ID + 1,
				handles: HashMap::new(),
				next_fh: 1,
			},
			read_only,
		))
	}

	pub fn restore(
		tag: String,
		shared_dir: PathBuf,
		state: &FsStateSer,
		read_only: bool,
	) -> Result<Self> {
		Ok(Self::with_state(tag, FsState::restore(&shared_dir, state)?, read_only))
	}

	pub fn save(&self) -> FsStateSer {
		self.state.save()
	}

	fn with_state(tag: String, state: FsState, read_only: bool) -> Self {
		let mut config = vec![0u8; CONFIG_SPACE_SIZE];
		let tag_bytes = tag.as_bytes();
		let n = tag_bytes.len().min(TAG_LEN);
		config[..n].copy_from_slice(&tag_bytes[..n]);
		config[TAG_LEN..TAG_LEN + 4].copy_from_slice(&NUM_REQUEST_QUEUES.to_le_bytes());

		Self {
			config,
			features: 1u64 << VIRTIO_F_VERSION_1,
			acked_features: 0,
			queue_sizes: vec![QUEUE_SIZE; NUM_QUEUES],
			read_only,
			state,
			mem: None,
			interrupt: None,
			hiprio_queue: None,
			req_queue: None,
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
		let Ok(offset) = usize::try_from(offset) else {
			data.fill(0);
			return;
		};
		for (i, b) in data.iter_mut().enumerate() {
			*b = offset
				.checked_add(i)
				.and_then(|idx| self.config.get(idx))
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
		// Queue order matches `queue_max_sizes`: 0 = hiprio, 1 = request.
		let _ = (HIPRIO_QUEUE, REQUEST_QUEUE);
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
		Ok(())
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			return Ok(QueuePass::Drained);
		};
		let mut used = false;
		let read_only = self.read_only;
		let state = &mut self.state;
		// Bound one pass: FUSE requests complete inline, so a spinning guest
		// could otherwise keep the rings non-empty forever and starve the
		// shared worker's other devices.
		let mut budget = QUEUE_PASS_BUDGET;

		// hiprio (queue 0): FORGET/BATCH_FORGET/INTERRUPT. They never produce a
		// reply, but FORGET still needs to age inode-table entries out.
		if let Some(hq) = self.hiprio_queue.as_mut() {
			while budget != 0 {
				let Some(chain) = hq.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				let head = chain.head_index();
				if let Some((req, _)) = split_chain(&mem, chain) {
					state.dispatch(&mem, &req, &[], read_only);
				}
				hq.add_used(&mem, head, 0)
					.map_err(|e| err(format!("virtio-fs hiprio used-ring update failed: {e}")))?;
				used = true;
			}
		}

		// request queue (queue 1): service FUSE. Disjoint borrows — the inode
		// table (`state`) and the queue are separate fields of `self`.
		if let Some(rq) = self.req_queue.as_mut() {
			while budget != 0 {
				let Some(chain) = rq.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				let head = chain.head_index();
				let written = match split_chain(&mem, chain) {
					Some((req, writable)) => state.dispatch(&mem, &req, &writable, read_only),
					None => 0,
				};
				rq.add_used(&mem, head, written)
					.map_err(|e| err(format!("virtio-fs request used-ring update failed: {e}")))?;
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
		// Order must mirror the transport's queue vec: hiprio (0) then request (1).
		let mut v = Vec::new();
		if let Some(q) = &self.hiprio_queue {
			v.push(q.state());
		}
		if let Some(q) = &self.req_queue {
			v.push(q.state());
		}
		v
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_dir(prefix: &str) -> PathBuf {
		let path = std::env::temp_dir().join(format!(
			"{prefix}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		fs::create_dir(&path).unwrap();
		path
	}

	#[test]
	fn fs_state_restore_drops_stale_paths() {
		let root = temp_dir("vmon-fs-state");
		fs::write(root.join("kept"), b"ok").unwrap();
		let state = FsStateSer {
			inodes: vec![
				(FUSE_ROOT_ID, ".".to_string()),
				(2, "kept".to_string()),
				(3, "missing".to_string()),
			],
			next:   4,
		};

		let fs = Fs::restore("host".to_string(), root.clone(), &state, true).unwrap();
		let saved = fs.save();

		assert!(
			saved
				.inodes
				.iter()
				.any(|(id, path)| *id == 2 && path == "kept")
		);
		assert!(!saved.inodes.iter().any(|(id, _)| *id == 3));
		assert_eq!(saved.next, 4);
		fs::remove_dir_all(root).unwrap();
	}

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x1_0000)]).expect("guest memory")
	}

	/// Build a FUSE request chain: `fuse_in_header` followed by `payload`.
	fn fuse_request(opcode: u32, nodeid: u64, payload: &[u8]) -> Vec<u8> {
		let len = IN_HEADER_SIZE + payload.len();
		let header =
			FuseInHeader { len: len as u32, opcode, unique: 1, nodeid, ..Default::default() };
		let mut buf = Vec::with_capacity(len);
		buf.extend_from_slice(struct_bytes(&header));
		buf.extend_from_slice(payload);
		buf
	}

	/// Dispatch one request against `fs` into a fresh guest memory and return
	/// the reply `(error, body_bytes)`.
	fn run_op(fs: &mut Fs, req: &[u8]) -> (i32, Vec<u8>) {
		let mem = guest_mem();
		let reply_at = GuestAddress(0x4000);
		let writable = vec![(reply_at, 0x1000u32)];
		let read_only = fs.read_only;
		let n = fs.state.dispatch(&mem, req, &writable, read_only);

		let mut hdr = [0u8; OUT_HEADER_SIZE];
		mem.read_slice(&mut hdr, reply_at).unwrap();
		let out: FuseOutHeader = read_struct(&hdr, 0).unwrap();
		let total = out.len as usize;
		assert_eq!(n, total as u32, "reply len {total} != written {n}");
		let body_len = total.saturating_sub(OUT_HEADER_SIZE);
		let mut body = vec![0u8; body_len];
		if body_len > 0 {
			mem.read_slice(&mut body, GuestAddress(reply_at.0 + OUT_HEADER_SIZE as u64))
				.unwrap();
		}
		(out.error, body)
	}

	#[test]
	fn init_negotiates_protocol_7_44_cap() {
		let root = temp_dir("vmon-fs-init");
		let mut fs = Fs::new("host".to_string(), root.clone(), true).unwrap();
		let init_payload = |minor: u32| {
			let mut payload = 7u32.to_le_bytes().to_vec();
			payload.extend_from_slice(&minor.to_le_bytes());
			payload.extend_from_slice(&0x20000u32.to_le_bytes());
			payload.extend_from_slice(&0u32.to_le_bytes());
			payload
		};

		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_INIT, FUSE_ROOT_ID, &init_payload(45)));
		assert_eq!(err, 0, "INIT succeeds for a newer guest minor");
		assert_eq!(body.len(), std::mem::size_of::<FuseInitOut>());
		let out: FuseInitOut = read_struct(&body, 0).unwrap();
		assert_eq!(out.major, 7);
		assert_eq!(out.minor, FUSE_PROTO_MINOR, "newer guest minor is capped");
		assert_eq!(out.minor, 44);
		assert_eq!(out.max_readahead, 0x20000);

		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_INIT, FUSE_ROOT_ID, &init_payload(12)));
		assert_eq!(err, 0, "INIT succeeds for an older guest minor");
		let out: FuseInitOut = read_struct(&body, 0).unwrap();
		assert_eq!(out.minor, 12, "older guest minor passes through");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn writable_create_and_write_persist_to_host() {
		let root = temp_dir("vmon-fs-write");
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		let cin = FuseCreateIn {
			flags:      libc::O_RDWR as u32,
			mode:       0o644,
			umask:      0,
			open_flags: 0,
		};
		let mut payload = struct_bytes(&cin).to_vec();
		payload.extend_from_slice(b"f\0");
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_CREATE, FUSE_ROOT_ID, &payload));
		assert_eq!(err, 0, "create succeeds on a writable share");
		assert_eq!(
			body.len(),
			std::mem::size_of::<FuseEntryOut>() + std::mem::size_of::<FuseOpenOut>(),
			"CREATE reply is entry_out ++ open_out"
		);
		assert!(root.join("f").exists(), "file materialized on host");
		let entry: FuseEntryOut = read_struct(&body, 0).unwrap();
		let open: FuseOpenOut = read_struct(&body, std::mem::size_of::<FuseEntryOut>()).unwrap();
		assert_ne!(open.fh, 0, "CREATE hands back a real (non-zero) file handle");

		// WRITE through the returned fh (the handle table), not by reopening.
		let win = FuseWriteIn { fh: open.fh, offset: 0, size: 5, ..Default::default() };
		let mut payload = struct_bytes(&win).to_vec();
		payload.extend_from_slice(b"hello");
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_WRITE, entry.nodeid, &payload));
		assert_eq!(err, 0, "write through the returned fh succeeds");
		let wout: FuseWriteOut = read_struct(&body, 0).unwrap();
		assert_eq!(wout.size, 5, "all 5 bytes accounted for");
		assert_eq!(fs::read(root.join("f")).unwrap(), b"hello");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn writable_mknod_regular_file_persists_to_host() {
		let root = temp_dir("vmon-fs-mknod");
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		#[allow(clippy::unnecessary_cast, reason = "S_IFREG is c_int on macOS, c_uint on Linux")]
		let min =
			FuseMknodIn { mode: libc::S_IFREG as u32 | 0o644, rdev: 0, umask: 0, padding: 0 };
		let mut payload = struct_bytes(&min).to_vec();
		payload.extend_from_slice(b"g\0");
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_MKNOD, FUSE_ROOT_ID, &payload));
		assert_eq!(err, 0, "mknod regular file succeeds on a writable share");
		let entry: FuseEntryOut = read_struct(&body, 0).unwrap();
		assert_eq!(entry.nodeid, 2, "mknod interns the new file");
		assert!(root.join("g").is_file(), "mknod materialized a regular file on host");
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn readdirplus_lists_entries_without_following_symlinks() {
		let root = temp_dir("vmon-fs-readdirplus");
		fs::write(root.join("f"), b"data").unwrap();
		std::os::unix::fs::symlink("f", root.join("l")).unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), true).unwrap();

		let rin = FuseReadIn { size: 4096, ..Default::default() };
		let payload = struct_bytes(&rin).to_vec();
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_READDIRPLUS, FUSE_ROOT_ID, &payload));
		assert_eq!(err, 0, "readdirplus succeeds");
		assert!(body.windows(1).any(|w| w == b"f"), "readdirplus body includes file entry");
		assert!(body.windows(1).any(|w| w == b"l"), "readdirplus body includes symlink entry");
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn writable_mkdir_and_unlink() {
		let root = temp_dir("vmon-fs-mkdir");
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		let min = FuseMkdirIn { mode: 0o755, umask: 0 };
		let mut payload = struct_bytes(&min).to_vec();
		payload.extend_from_slice(b"d\0");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_MKDIR, FUSE_ROOT_ID, &payload));
		assert_eq!(err, 0, "mkdir succeeds");
		assert!(root.join("d").is_dir(), "directory created on host");

		fs::write(root.join("victim"), b"x").unwrap();
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_UNLINK, FUSE_ROOT_ID, b"victim\0"));
		assert_eq!(err, 0, "unlink succeeds");
		assert!(!root.join("victim").exists(), "file removed from host");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn rename2_rejects_unsupported_flags_without_plain_rename() {
		const RENAME_EXCHANGE: u32 = 1 << 1;
		const RENAME_WHITEOUT: u32 = 1 << 2;

		let root = temp_dir("vmon-fs-rename2-flags");
		fs::write(root.join("old"), b"old").unwrap();
		fs::write(root.join("dst"), b"dst").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		let rename2_payload = |flags: u32, old: &[u8], new: &[u8]| {
			let rin = FuseRename2In { newdir: FUSE_ROOT_ID, flags, padding: 0 };
			let mut payload = struct_bytes(&rin).to_vec();
			payload.extend_from_slice(old);
			payload.push(0);
			payload.extend_from_slice(new);
			payload.push(0);
			payload
		};

		for flags in [RENAME_EXCHANGE, RENAME_WHITEOUT] {
			let (err, _) = run_op(
				&mut fs,
				&fuse_request(FUSE_RENAME2, FUSE_ROOT_ID, &rename2_payload(flags, b"old", b"dst")),
			);
			assert_eq!(err, -errno::EINVAL, "unsupported rename2 flag rejected");
			assert_eq!(
				fs::read(root.join("old")).unwrap(),
				b"old",
				"source must not be moved by an unsupported rename2 flag"
			);
			assert_eq!(
				fs::read(root.join("dst")).unwrap(),
				b"dst",
				"destination must not be clobbered by an unsupported rename2 flag"
			);
		}

		let (err, _) = run_op(
			&mut fs,
			&fuse_request(FUSE_RENAME2, FUSE_ROOT_ID, &rename2_payload(0, b"old", b"plain")),
		);
		assert_eq!(err, 0, "plain rename2 still succeeds");
		assert!(!root.join("old").exists());
		assert_eq!(fs::read(root.join("plain")).unwrap(), b"old");

		fs::write(root.join("noreplace-src"), b"src").unwrap();
		fs::write(root.join("noreplace-dst"), b"dst").unwrap();
		let (err, _) = run_op(
			&mut fs,
			&fuse_request(
				FUSE_RENAME2,
				FUSE_ROOT_ID,
				&rename2_payload(RENAME_NOREPLACE, b"noreplace-src", b"noreplace-dst"),
			),
		);
		assert_eq!(err, -errno::EEXIST, "RENAME_NOREPLACE still rejects existing dst");
		assert_eq!(fs::read(root.join("noreplace-src")).unwrap(), b"src");
		assert_eq!(fs::read(root.join("noreplace-dst")).unwrap(), b"dst");

		let (err, _) = run_op(
			&mut fs,
			&fuse_request(
				FUSE_RENAME2,
				FUSE_ROOT_ID,
				&rename2_payload(RENAME_NOREPLACE, b"noreplace-src", b"noreplace-new"),
			),
		);
		assert_eq!(err, 0, "RENAME_NOREPLACE still permits a missing dst");
		assert_eq!(fs::read(root.join("noreplace-new")).unwrap(), b"src");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn read_only_rejects_mutations_with_erofs() {
		let root = temp_dir("vmon-fs-rofs");
		fs::write(root.join("existing"), b"x").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), true).unwrap();

		let cin = FuseCreateIn {
			flags:      libc::O_RDWR as u32,
			mode:       0o644,
			umask:      0,
			open_flags: 0,
		};
		let mut payload = struct_bytes(&cin).to_vec();
		payload.extend_from_slice(b"new\0");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_CREATE, FUSE_ROOT_ID, &payload));
		assert_eq!(err, -errno::EROFS, "CREATE rejected read-only");

		let win = FuseWriteIn {
			fh:          FUSE_ROOT_ID,
			offset:      0,
			size:        1,
			write_flags: 0,
			lock_owner:  0,
			flags:       0,
			padding:     0,
		};
		let mut payload = struct_bytes(&win).to_vec();
		payload.extend_from_slice(b"y");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_WRITE, FUSE_ROOT_ID, &payload));
		assert_eq!(err, -errno::EROFS, "WRITE rejected read-only");

		let min = FuseMkdirIn { mode: 0o755, umask: 0 };
		let mut payload = struct_bytes(&min).to_vec();
		payload.extend_from_slice(b"d\0");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_MKDIR, FUSE_ROOT_ID, &payload));
		assert_eq!(err, -errno::EROFS, "MKDIR rejected read-only");

		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_UNLINK, FUSE_ROOT_ID, b"existing\0"));
		assert_eq!(err, -errno::EROFS, "UNLINK rejected read-only");
		assert!(root.join("existing").exists(), "read-only UNLINK must not delete");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn read_only_open_rejects_write_flags_but_allows_read() {
		let root = temp_dir("vmon-fs-open");
		fs::write(root.join("f"), b"data").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), true).unwrap();

		// fuse_open_in is { flags: u32, unused: u32 }.
		let mut ro = (libc::O_RDONLY as u32).to_le_bytes().to_vec();
		ro.extend_from_slice(&0u32.to_le_bytes());
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_OPEN, FUSE_ROOT_ID, &ro));
		assert_eq!(err, 0, "read-only open of a read-only share is allowed");

		let mut wo = (libc::O_WRONLY as u32).to_le_bytes().to_vec();
		wo.extend_from_slice(&0u32.to_le_bytes());
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_OPEN, FUSE_ROOT_ID, &wo));
		assert_eq!(err, -errno::EROFS, "write-open of a read-only share is rejected");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn writable_open_fh_write_then_read_roundtrip() {
		let root = temp_dir("vmon-fs-openfh");
		fs::write(root.join("f"), b"0000000000").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		// LOOKUP "f" to intern its nodeid.
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_LOOKUP, FUSE_ROOT_ID, b"f\0"));
		assert_eq!(err, 0, "lookup of an existing file");
		let ent: FuseEntryOut = read_struct(&body, 0).unwrap();

		// OPEN RDWR -> a real, non-zero fh.
		let mut flags = (libc::O_RDWR as u32).to_le_bytes().to_vec();
		flags.extend_from_slice(&0u32.to_le_bytes());
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_OPEN, ent.nodeid, &flags));
		assert_eq!(err, 0, "open of an existing file on a writable share");
		let open: FuseOpenOut = read_struct(&body, 0).unwrap();
		assert_ne!(open.fh, 0, "OPEN returns a real fh");

		// WRITE "abc" at offset 2 through the fh.
		let win = FuseWriteIn { fh: open.fh, offset: 2, size: 3, ..Default::default() };
		let mut payload = struct_bytes(&win).to_vec();
		payload.extend_from_slice(b"abc");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_WRITE, ent.nodeid, &payload));
		assert_eq!(err, 0, "write via the fh");
		assert_eq!(fs::read(root.join("f")).unwrap(), b"00abc00000");

		// READ 3 bytes at offset 2 through the fh.
		let rin = FuseReadIn { fh: open.fh, offset: 2, size: 3, ..Default::default() };
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_READ, ent.nodeid, struct_bytes(&rin)));
		assert_eq!(err, 0, "read via the fh");
		assert_eq!(body, b"abc", "read returns exactly what the fh wrote");

		// RELEASE drops the handle; a now-stale fh falls back to the confined
		// path reopen (still O_NOFOLLOW), so the write still lands.
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_RELEASE, ent.nodeid, struct_bytes(&open)));
		assert_eq!(err, 0, "release the handle");
		let win = FuseWriteIn { fh: open.fh, offset: 0, size: 2, ..Default::default() };
		let mut payload = struct_bytes(&win).to_vec();
		payload.extend_from_slice(b"ZZ");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_WRITE, ent.nodeid, &payload));
		assert_eq!(err, 0, "write after release falls back to a confined reopen");
		assert_eq!(fs::read(root.join("f")).unwrap(), b"ZZabc00000");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn open_rejects_final_component_symlink() {
		let root = temp_dir("vmon-fs-symlink");
		fs::write(root.join("f"), b"inside").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		// Intern "f" via LOOKUP while it is still a real file.
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_LOOKUP, FUSE_ROOT_ID, b"f\0"));
		assert_eq!(err, 0, "lookup of the real file");
		let ent: FuseEntryOut = read_struct(&body, 0).unwrap();

		// TOCTOU: swap the final component for a symlink pointing outside root.
		fs::remove_file(root.join("f")).unwrap();
		std::os::unix::fs::symlink("/etc/passwd", root.join("f")).unwrap();

		// OPEN must refuse to traverse the final-component symlink (O_NOFOLLOW).
		let mut flags = (libc::O_RDONLY as u32).to_le_bytes().to_vec();
		flags.extend_from_slice(&0u32.to_le_bytes());
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_OPEN, ent.nodeid, &flags));
		assert_eq!(err, -errno::ELOOP, "OPEN refuses a final-component symlink");

		// A fallback WRITE by nodeid (unknown fh) is likewise refused.
		let win = FuseWriteIn { fh: 0, offset: 0, size: 1, ..Default::default() };
		let mut payload = struct_bytes(&win).to_vec();
		payload.extend_from_slice(b"x");
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_WRITE, ent.nodeid, &payload));
		assert_eq!(err, -errno::ELOOP, "fallback WRITE refuses a final-component symlink");

		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn stale_parent_symlink_escape_is_rejected() {
		let root = temp_dir("vmon-fs-parent-symlink");
		let outside = temp_dir("vmon-fs-parent-outside");
		fs::create_dir(root.join("d")).unwrap();
		fs::write(root.join("d").join("f"), b"inside").unwrap();
		fs::write(outside.join("f"), b"outside").unwrap();
		let mut fs = Fs::new("host".to_string(), root.clone(), false).unwrap();

		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_LOOKUP, FUSE_ROOT_ID, b"d\0"));
		assert_eq!(err, 0, "lookup of original directory");
		let dir: FuseEntryOut = read_struct(&body, 0).unwrap();
		let (err, body) = run_op(&mut fs, &fuse_request(FUSE_LOOKUP, dir.nodeid, b"f\0"));
		assert_eq!(err, 0, "lookup of original child");
		let file: FuseEntryOut = read_struct(&body, 0).unwrap();

		fs::remove_dir_all(root.join("d")).unwrap();
		std::os::unix::fs::symlink(&outside, root.join("d")).unwrap();

		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_GETATTR, file.nodeid, &[]));
		assert_eq!(err, -errno::EACCES, "GETATTR refuses parent symlink escape");
		let rin = FuseReadIn { fh: 0, size: 7, ..Default::default() };
		let (err, _) = run_op(&mut fs, &fuse_request(FUSE_READ, file.nodeid, struct_bytes(&rin)));
		assert_eq!(err, -errno::EACCES, "fallback READ refuses parent symlink escape");

		fs::remove_dir_all(root).unwrap();
		fs::remove_dir_all(outside).unwrap();
	}
}
