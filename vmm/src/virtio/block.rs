//! virtio-blk backend over a host file (the guest root/data disk).
//!
//! Linux requests are serviced with `io_uring`: each IN/OUT chain's data
//! descriptors become `iovec`s pointing directly at guest-memory host
//! addresses. macOS services the same chains synchronously with positioned file
//! I/O on the device worker thread.

#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
#[cfg(not(target_os = "windows"))]
use std::{
	fs,
	os::unix::fs::{MetadataExt, OpenOptionsExt},
};
use std::{
	fs::{File, OpenOptions},
	io::{self, Seek, SeekFrom},
	path::Path,
	sync::Arc,
};

#[cfg(target_os = "linux")]
use io_uring::IoUring;
use virtio_bindings::{
	bindings::{
		virtio_blk::{
			VIRTIO_BLK_F_FLUSH, VIRTIO_BLK_F_RO, VIRTIO_BLK_S_IOERR, VIRTIO_BLK_S_OK,
			VIRTIO_BLK_S_UNSUPP, VIRTIO_BLK_T_FLUSH, VIRTIO_BLK_T_GET_ID, VIRTIO_BLK_T_IN,
			VIRTIO_BLK_T_OUT,
		},
		virtio_config::VIRTIO_F_VERSION_1,
	},
	virtio_ids::VIRTIO_ID_BLOCK,
};
use virtio_queue::{DescriptorChain, Queue, QueueT};
use vm_memory::{Bytes, GuestAddress, GuestMemory};

#[cfg(target_os = "linux")]
use crate::os::EventFd;
use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	virtio::{
		DiskCapture, DiskCaptureMethod, Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice,
		WorkerWaitSource, descriptor_range_valid,
	},
};

/// Creates `dest` as an exclusive copy-on-write overlay of `base`.
///
/// Reflink-capable filesystems clone instantly; other filesystems copy the
/// bytes. The returned descriptor remains bound to the created file even if
/// an attacker later swaps its path.
#[cfg(target_os = "windows")]
pub fn create_cow_overlay(base: &Path, dest: &Path) -> Result<File> {
	use std::io::{Read, Write};
	let mut source =
		File::open(base).map_err(|e| err(format!("opening base disk {}: {e}", base.display())))?;
	if !source.metadata()?.is_file() {
		return Err(err(format!("base disk {} must be a regular file", base.display())));
	}
	let mut output = OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.open(dest)
		.map_err(|e| {
			if e.kind() == std::io::ErrorKind::AlreadyExists {
				err(format!("overlay destination {} already exists", dest.display()))
			} else {
				err(format!("creating overlay {}: {e}", dest.display()))
			}
		})?;
	let mut buffer = vec![0u8; 64 * 1024];
	loop {
		let count = source.read(&mut buffer)?;
		if count == 0 {
			break;
		}
		output.write_all(&buffer[..count])?;
	}
	output.flush()?;
	Ok(output)
}

/// Creates `dest` as an exclusive copy-on-write overlay of `base`.
///
/// Reflink-capable filesystems clone instantly; other filesystems copy the
/// bytes. The returned descriptor remains bound to the created file even if
/// an attacker later swaps its path.
#[cfg(not(target_os = "windows"))]
pub fn create_cow_overlay(base: &Path, dest: &Path) -> Result<File> {
	let base_path_meta = fs::symlink_metadata(base)
		.map_err(|e| err(format!("checking base disk {}: {e}", base.display())))?;
	if base_path_meta.file_type().is_symlink() {
		return Err(err(format!(
			"base disk {} must be a regular file, not a symlink",
			base.display()
		)));
	}
	if !base_path_meta.is_file() {
		return Err(err(format!("base disk {} must be a regular file", base.display())));
	}

	match fs::symlink_metadata(dest) {
		Ok(_) => {
			return Err(err(format!(
				"overlay destination {} already exists; refusing to overwrite",
				dest.display()
			)));
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {},
		Err(e) => return Err(err(format!("checking overlay destination {}: {e}", dest.display()))),
	}

	let mut src = OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(base)
		.map_err(|e| {
			err(format!("opening base disk {} without following symlinks: {e}", base.display()))
		})?;
	let opened_base_meta = src
		.metadata()
		.map_err(|e| err(format!("checking opened base disk {}: {e}", base.display())))?;
	if !opened_base_meta.is_file() {
		return Err(err(format!("base disk {} must be a regular file", base.display())));
	}
	if opened_base_meta.dev() != base_path_meta.dev()
		|| opened_base_meta.ino() != base_path_meta.ino()
	{
		return Err(err(format!("base disk {} changed while creating overlay", base.display())));
	}

	let dst = OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(dest)
		.map_err(|e| {
			err(format!(
				"creating overlay {} exclusively without following symlinks: {e}",
				dest.display()
			))
		})?;
	let dst_meta = dst
		.metadata()
		.map_err(|e| err(format!("checking overlay {}: {e}", dest.display())))?;
	if !dst_meta.is_file() {
		return Err(err(format!("overlay destination {} must be a regular file", dest.display())));
	}

	#[cfg(target_os = "linux")]
	let clone_err = {
		let clone_err = linux::try_reflink(&src, &dst);
		if clone_err.is_none() {
			return Ok(dst);
		}
		clone_err.expect("reflink error recorded")
	};
	#[cfg(target_os = "macos")]
	let clone_err = io::Error::new(
		io::ErrorKind::Unsupported,
		"macOS clonefile fast path disabled because it cannot return the created fd",
	);

	// Reflink unsupported on this filesystem: fall back to a sparse-aware copy
	// through the already-open no-follow source and exclusive destination fds
	// (path-based copy helpers are off-limits; the path may be
	// attacker-controlled). Zero blocks become holes so a mostly-empty base
	// image costs almost nothing per overlay.
	src.seek(SeekFrom::Start(0))
		.map_err(|e| err(format!("seeking base disk {}: {e}", base.display())))?;
	dst.set_len(0)
		.map_err(|e| err(format!("resetting overlay {}: {e}", dest.display())))?;
	sparse_copy(&mut src, &dst).map_err(|e| {
		err(format!(
			"copying {} -> {} after reflink failed ({clone_err}): {e}",
			base.display(),
			dest.display()
		))
	})?;
	Ok(dst)
}

/// Copy `src` into `dst` skipping all-zero 64 KiB blocks (they become holes),
/// then size `dst` to the full logical length.
#[cfg(not(target_os = "windows"))]
fn sparse_copy(src: &mut File, dst: &File) -> io::Result<()> {
	use std::io::Read;
	const BLOCK: usize = 64 * 1024;
	let mut buf = vec![0u8; BLOCK];
	let mut offset = 0u64;
	loop {
		let mut filled = 0usize;
		while filled < BLOCK {
			let n = src.read(&mut buf[filled..])?;
			if n == 0 {
				break;
			}
			filled += n;
		}
		if filled == 0 {
			break;
		}
		if !crate::memory::is_zero(&buf[..filled]) {
			use std::os::unix::fs::FileExt;
			dst.write_all_at(&buf[..filled], offset)?;
		}
		offset += filled as u64;
		if filled < BLOCK {
			break;
		}
	}
	dst.set_len(offset)?;
	Ok(())
}

/// Clone the already-open active backing file into a new recovery artifact.
///
/// The source descriptor is deliberately supplied by [`Block`], rather than
/// reopening the configured path: a path replacement after boot must not make
/// a recovery point capture a different disk. The VMM calls this only after
/// the block worker has drained all submissions.
#[cfg(not(target_os = "windows"))]
fn clone_open_disk(source: &File, destination: &Path) -> Result<DiskCapture> {
	match fs::symlink_metadata(destination) {
		Ok(_) => {
			return Err(err(format!(
				"disk recovery destination {} already exists",
				destination.display()
			)));
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {},
		Err(e) => {
			return Err(err(format!(
				"checking disk recovery destination {}: {e}",
				destination.display()
			)));
		},
	}
	let destination = OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(destination)
		.map_err(|e| err(format!("creating disk recovery image: {e}")))?;
	let bytes = source
		.metadata()
		.map_err(|e| err(format!("reading active disk length: {e}")))?
		.len();

	#[cfg(target_os = "linux")]
	if linux::try_reflink(source, &destination).is_none() {
		destination
			.sync_all()
			.map_err(|e| err(format!("syncing reflinked disk recovery image: {e}")))?;
		return Ok(DiskCapture { bytes, method: DiskCaptureMethod::Reflink });
	}

	// A copy fallback is intentionally made while the block worker remains
	// stopped. It is therefore coherent, but not non-blocking: VCPUs may stall
	// on block IO until this copy completes.
	let mut source = source
		.try_clone()
		.map_err(|e| err(format!("cloning active disk descriptor: {e}")))?;
	source
		.seek(SeekFrom::Start(0))
		.map_err(|e| err(format!("seeking active disk for sparse recovery copy: {e}")))?;
	sparse_copy(&mut source, &destination)
		.map_err(|e| err(format!("sparse-copying disk recovery image: {e}")))?;
	destination
		.sync_all()
		.map_err(|e| err(format!("syncing sparse disk recovery image: {e}")))?;
	Ok(DiskCapture { bytes, method: DiskCaptureMethod::SparseCopy })
}

#[cfg(target_os = "windows")]
fn clone_open_disk(source: &File, destination: &Path) -> Result<DiskCapture> {
	use std::io::{Read, Write};

	let mut source = source
		.try_clone()
		.map_err(|e| err(format!("cloning active disk descriptor: {e}")))?;
	let bytes = source.metadata()?.len();
	let mut destination = OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.open(destination)
		.map_err(|e| err(format!("creating disk recovery image: {e}")))?;
	source.seek(SeekFrom::Start(0))?;
	let mut buffer = vec![0u8; 64 * 1024];
	loop {
		let read = source.read(&mut buffer)?;
		if read == 0 {
			break;
		}
		destination.write_all(&buffer[..read])?;
	}
	destination.sync_all()?;
	Ok(DiskCapture { bytes, method: DiskCaptureMethod::SparseCopy })
}

const SECTOR_SIZE: u64 = 512;
const QUEUE_SIZE: u16 = 256;
/// `io_uring` ring depth. The single virtqueue holds at most `QUEUE_SIZE`
/// descriptors and every request consumes at least three (header + data +
/// status), so this comfortably covers a full queue-notify batch.
#[cfg(target_os = "linux")]
const RING_ENTRIES: u32 = 256;
const CONFIG_SPACE_SIZE: usize = 0x20;
const VIRTIO_BLK_REQ_HEADER_SIZE: usize = 16;
const DEVICE_ID: &[u8] = b"vmon-blk";
/// Max bytes of the device-id string virtio-blk `GET_ID` may return.
const DEVICE_ID_LEN: usize = 20;

/// One in-flight `io_uring` request, parked until its completion is reaped.
///
/// `iovecs` hold raw host pointers into guest memory and back the `readv`/
/// `writev` submitted for this request; the kernel dereferences them after we
/// return from `process_queue_notify`, so the `Vec` must live here (its heap
/// buffer address is stable across the moves into the table) until the matching
/// completion arrives. `guest_ranges` mirrors the IN-request iovecs as GPA
/// ranges (empty for OUT) so the reaper can mark the kernel's raw-pointer
/// writes in the dirty bitmap (migration-delta contract).
#[cfg(target_os = "linux")]
struct PendingReq {
	head:         u16,
	status_addr:  GuestAddress,
	data_len:     u32,
	req_type:     u32,
	iovecs:       Vec<libc::iovec>,
	guest_ranges: Vec<(GuestAddress, u32)>,
}

// SAFETY: each `iov_base` points into the device's guest-memory mapping, which
// is `Arc`-held for the entire VM run, and a `Block` is owned by a single
// device worker thread (the only cross-thread movement is the one-time hand-off
// of the `Arc<Mutex<dyn VirtioDevice>>` into that thread). The raw pointers are
// thus never dereferenced from two threads at once.
#[cfg(target_os = "linux")]
#[allow(
	clippy::non_send_fields_in_send_ty,
	reason = "PendingReq iovecs point into VM-owned guest memory moved only with the device worker"
)]
// SAFETY: PendingReq's raw iovec pointers reference VM-owned guest memory that
// outlives the in-flight request; only the single device worker thread accesses
// them, so moving ownership to that worker is sound.
unsafe impl Send for PendingReq {}

/// Mark the first `written` bytes of an IN request's data descriptors dirty.
///
/// `io_uring` read completions land in guest RAM through the raw iovec
/// pointers, bypassing vm-memory's tracked writers; without this a
/// live-migration memory delta would miss the DMA and ship stale RAM.
#[cfg(target_os = "linux")]
fn mark_guest_ranges_dirty(mem: &GuestMemoryMmap, ranges: &[(GuestAddress, u32)], written: usize) {
	let mut remaining = written;
	for &(addr, len) in ranges {
		if remaining == 0 {
			break;
		}
		let n = remaining.min(len as usize);
		crate::memory::mark_dirty(mem, addr, n);
		remaining -= n;
	}
}

/// What happened to one processed descriptor chain.
enum Outcome {
	/// An SQE was pushed; the used ring is updated when the completion is
	/// reaped.
	#[cfg(target_os = "linux")]
	Submitted,
	/// The request was finished inline and its used-ring entry already added.
	Synchronous,
}

pub struct Block {
	disk:             File,
	capacity_sectors: u64,
	read_only:        bool,
	features:         u64,
	acked_features:   u64,
	config:           Vec<u8>,
	queue_sizes:      Vec<u16>,

	/// Submission/completion ring for the data path. Built up front — before the
	/// worker thread queries `worker_fds` — so its completion eventfd is always
	/// available to epoll; it carries traffic only once the queue is activated.
	#[cfg(target_os = "linux")]
	ring:         IoUring,
	/// Eventfd the ring signals on completion; epolled by the device worker.
	#[cfg(target_os = "linux")]
	io_uring_evt: EventFd,
	/// In-flight requests keyed by their SQE `user_data` token.
	#[cfg(target_os = "linux")]
	pending:      HashMap<u64, PendingReq>,
	/// Monotonic across activations so a late CQE cannot alias a new request.
	#[cfg(target_os = "linux")]
	next_token:   u64,

	mem:       Option<GuestMemoryMmap>,
	interrupt: Option<Arc<Interrupt>>,
	queue:     Option<Queue>,
}

impl Block {
	pub fn new(path: &Path, read_only: bool) -> Result<Self> {
		let disk = OpenOptions::new()
			.read(true)
			.write(!read_only)
			.open(path)
			.map_err(|e| err(format!("opening disk {}: {e}", path.display())))?;
		Self::from_file(disk, read_only)
	}

	pub fn from_file(disk: File, read_only: bool) -> Result<Self> {
		let len = disk.metadata()?.len();
		let capacity_sectors = len / SECTOR_SIZE;

		let mut features = (1u64 << VIRTIO_F_VERSION_1) | (1u64 << VIRTIO_BLK_F_FLUSH);
		if read_only {
			features |= 1u64 << VIRTIO_BLK_F_RO;
		}

		let mut config = vec![0u8; CONFIG_SPACE_SIZE];
		config[..8].copy_from_slice(&capacity_sectors.to_le_bytes());

		#[cfg(target_os = "linux")]
		let (ring, io_uring_evt) = {
			// Build the ring eagerly and register its completion eventfd now: the
			// worker thread reads `worker_fds()` before the guest driver ever
			// activates the device, so the fd has to exist already or completions
			// would never be polled.
			let ring = IoUring::new(RING_ENTRIES)?;
			let io_uring_evt = EventFd::new(libc::EFD_NONBLOCK)?;
			ring
				.submitter()
				.register_eventfd(io_uring_evt.as_raw_fd())?;
			(ring, io_uring_evt)
		};

		Ok(Self {
			disk,
			capacity_sectors,
			read_only,
			features,
			acked_features: 0,
			config,
			queue_sizes: vec![QUEUE_SIZE],
			#[cfg(target_os = "linux")]
			ring,
			#[cfg(target_os = "linux")]
			io_uring_evt,
			#[cfg(target_os = "linux")]
			pending: HashMap::new(),
			#[cfg(target_os = "linux")]
			next_token: 0,
			mem: None,
			interrupt: None,
			queue: None,
		})
	}
}

#[cfg(target_os = "linux")]
impl Block {
	/// Reap all currently-available `io_uring` completions, posting used-ring
	/// entries. Shared by the worker-fd path and `drain`.
	fn reap(&mut self, context: &str) -> Result<()> {
		let pending_len = self.pending.len();
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			if pending_len == 0 {
				return Ok(());
			}
			return Err(err(format!(
				"vmm: virtio-blk {context} missing active state while reaping {pending_len} in-flight \
				 io_uring request(s)"
			)));
		};
		let Some(queue) = self.queue.as_mut() else {
			if pending_len == 0 {
				return Ok(());
			}
			return Err(err(format!(
				"vmm: virtio-blk {context} missing queue while reaping {pending_len} in-flight \
				 io_uring request(s)"
			)));
		};
		let ring = &mut self.ring;
		let pending = &mut self.pending;

		let mut completed = false;
		for cqe in ring.completion() {
			let token = cqe.user_data();
			let Some(req) = pending.get(&token) else {
				continue;
			};
			let head = req.head;
			let status_addr = req.status_addr;
			let data_len = req.data_len;
			let req_type = req.req_type;
			let res = cqe.result();
			// A read completion means the kernel wrote guest RAM through the raw
			// iovec pointers, invisibly to vm-memory's bitmap; mark exactly the
			// bytes it reported written (migration-delta contract).
			if req_type == VIRTIO_BLK_T_IN && res > 0 {
				mark_guest_ranges_dirty(&mem, &req.guest_ranges, res as usize);
			}
			// io_uring data ops must transfer the full request length; a short
			// count means partial/stale guest data -> IOERR (the old synchronous
			// path used read_exact_at / write_all_at).
			let ok = res >= 0 && (res as u32) >= data_len;
			let status = if ok {
				VIRTIO_BLK_S_OK
			} else {
				VIRTIO_BLK_S_IOERR
			};
			mem.write_slice(&[status as u8], status_addr).map_err(|e| {
				err(format!("vmm: virtio-blk {context} failed writing completion status: {e}"))
			})?;
			let data_written = if ok && req_type == VIRTIO_BLK_T_IN {
				data_len
			} else {
				0
			};
			queue.add_used(&mem, head, data_written + 1).map_err(|e| {
				err(format!("vmm: virtio-blk {context} failed adding used descriptor: {e}"))
			})?;
			pending.remove(&token);
			completed = true;
		}
		if completed {
			interrupt.signal_used_queue().map_err(|e| {
				err(format!("vmm: virtio-blk {context} failed signalling used queue: {e}"))
			})?;
		}
		Ok(())
	}

	/// Consume completions during a device reset without completing them (no
	/// status or used-ring writes). Read DMA is still recorded in the dirty
	/// bitmap: the kernel already wrote guest RAM through the raw iovecs, and a
	/// live-migration delta must not miss it (migration-delta contract).
	fn discard_completed(&mut self) {
		let mem = self.mem.clone();
		for cqe in self.ring.completion() {
			let Some(req) = self.pending.remove(&cqe.user_data()) else {
				continue;
			};
			let res = cqe.result();
			if req.req_type == VIRTIO_BLK_T_IN
				&& res > 0
				&& let Some(mem) = &mem
			{
				mark_guest_ranges_dirty(mem, &req.guest_ranges, res as usize);
			}
		}
	}

	/// Drain submitted I/O before dropping queue and guest-memory handles.
	fn drain_for_reset(&mut self) -> Result<()> {
		self.discard_completed();
		while !self.pending.is_empty() {
			self.wait_for_pending_completion("reset")?;
			self.discard_completed();
		}
		self.clear_completion_eventfd();
		debug_assert!(self.pending.is_empty());
		Ok(())
	}

	#[allow(
		clippy::needless_pass_by_ref_mut,
		reason = "io_uring submission waits are cfg-gated behind the mutable ring API"
	)]
	fn wait_for_pending_completion(&mut self, context: &str) -> Result<()> {
		loop {
			match self.ring.submit_and_wait(1) {
				Ok(_) => return Ok(()),
				Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {},
				Err(e) => {
					return Err(err(format!(
						"vmm: virtio-blk {context} failed while waiting for {} in-flight io_uring \
						 request(s): {e}",
						self.pending.len()
					)));
				},
			}
		}
	}

	fn clear_completion_eventfd(&self) {
		while self.io_uring_evt.read().is_ok() {}
	}
}
impl VirtioDevice for Block {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_BLOCK
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
		let offset = offset as usize;
		for (i, b) in data.iter_mut().enumerate() {
			*b = self.config.get(offset + i).copied().unwrap_or(0);
		}
	}

	fn write_config(&mut self, _offset: u64, _data: &[u8]) {
		// virtio-blk config space is read-only for the driver.
	}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		self.queue = queues.into_iter().next();
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		#[cfg(target_os = "linux")]
		self.drain_for_reset()?;
		self.acked_features = 0;
		self.mem = None;
		self.interrupt = None;
		self.queue = None;
		Ok(())
	}

	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		#[cfg(target_os = "linux")]
		{
			vec![self.io_uring_evt.as_raw_fd()]
		}
		#[cfg(any(target_os = "macos", target_os = "windows"))]
		{
			Vec::new()
		}
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			return Ok(QueuePass::Drained);
		};
		let Some(queue) = self.queue.as_mut() else {
			return Ok(QueuePass::Drained);
		};
		let disk = &self.disk;
		let capacity = self.capacity_sectors;
		let read_only = self.read_only;
		// Bound one pass: inline completions recycle descriptors, so a spinning
		// guest could otherwise keep the available ring non-empty forever and
		// starve the shared worker's other devices.
		let mut budget = QUEUE_PASS_BUDGET;

		#[cfg(target_os = "linux")]
		{
			// Disjoint field borrows: backend/config fields vs. queue/ring/pending.
			let ring = &mut self.ring;
			let pending = &mut self.pending;
			let next_token = &mut self.next_token;

			let mut any_submitted = false;
			let mut sync_used = false;
			while budget != 0 {
				let Some(chain) = queue.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				match linux::process_chain(
					disk, capacity, read_only, &mem, queue, ring, pending, next_token, chain,
				)? {
					Outcome::Submitted => any_submitted = true,
					Outcome::Synchronous => sync_used = true,
				}
			}
			// Kick the kernel once for everything pushed this batch.
			if any_submitted {
				ring
					.submit()
					.map_err(|e| err(format!("virtio-blk submit failed: {e}")))?;
			}
			// Inline completions (errors, flush, get-id, unsupported) need their own
			// IRQ; asynchronous ones are signalled from `process_worker_source`.
			if sync_used {
				interrupt.signal_used_queue()?;
			}
		}

		#[cfg(any(target_os = "macos", target_os = "windows"))]
		{
			let mut sync_used = false;
			while budget != 0 {
				let Some(chain) = queue.pop_descriptor_chain(&mem) else {
					break;
				};
				budget -= 1;
				match sync_io::process_chain(disk, capacity, read_only, &mem, queue, chain)? {
					Outcome::Synchronous => sync_used = true,
				}
			}
			if sync_used {
				interrupt.signal_used_queue()?;
			}
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn process_worker_source(&mut self, _index: usize) -> Result<()> {
		#[cfg(target_os = "linux")]
		{
			let _ = self.io_uring_evt.read();
			self.reap("completion reap")
		}
		#[cfg(any(target_os = "macos", target_os = "windows"))]
		{
			Ok(())
		}
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		self.queue.iter().map(|q| q.state()).collect()
	}

	fn drain(&mut self) -> Result<()> {
		#[cfg(target_os = "linux")]
		{
			// Complete every in-flight request before a snapshot: a descriptor
			// already popped from the available ring but not yet added to used
			// would otherwise be lost across restore (the guest would hang).
			while !self.pending.is_empty() {
				self.wait_for_pending_completion("snapshot drain")?;
				self.reap("snapshot drain")?;
			}
		}
		Ok(())
	}

	fn capture_disk(&mut self, destination: &Path) -> Result<DiskCapture> {
		// `run_worker` already drains before acknowledging its pause request.
		// Repeat the drain here so this primitive remains safe if it is used by
		// a future control path that has acquired the device lock directly.
		self.drain()?;
		self.disk.sync_all().map_err(|e| {
			err(format!("syncing active virtio-block disk before recovery capture: {e}"))
		})?;
		clone_open_disk(&self.disk, destination)
	}
}

fn validate_data_descriptors(
	mem: &GuestMemoryMmap,
	data_descs: &[virtio_queue::desc::split::Descriptor],
	data_writable: bool,
) -> Option<u32> {
	let mut total_len = 0u32;
	for d in data_descs {
		if d.is_write_only() != data_writable || !descriptor_range_valid(mem, d.addr(), d.len()) {
			return None;
		}
		total_len = total_len.checked_add(d.len())?;
	}
	Some(total_len)
}

fn request_range_in_bounds(capacity_sectors: u64, sector: u64, total_len: u32) -> bool {
	let total_len = u64::from(total_len);
	capacity_sectors
		.checked_mul(SECTOR_SIZE)
		.and_then(|capacity_bytes| {
			sector
				.checked_mul(SECTOR_SIZE)
				.and_then(|start| start.checked_add(total_len))
				.map(|end| end <= capacity_bytes)
		})
		.unwrap_or(false)
}

fn complete_malformed(queue: &mut Queue, mem: &GuestMemoryMmap, head: u16) -> Result<Outcome> {
	queue
		.add_used(mem, head, 0)
		.map_err(|e| err(format!("virtio-blk malformed used-ring update failed: {e}")))?;
	Ok(Outcome::Synchronous)
}

/// Write `status` to the status descriptor and post a used-ring entry of
/// `data_written + 1` bytes (the data plus the status byte). Always returns
/// [`Outcome::Synchronous`].
fn complete_sync(
	queue: &mut Queue,
	mem: &GuestMemoryMmap,
	head: u16,
	status_addr: GuestAddress,
	status: u32,
	data_written: u32,
) -> Result<Outcome> {
	mem.write_slice(&[status as u8], status_addr)
		.map_err(|e| err(format!("virtio-blk status write failed: {e}")))?;
	queue
		.add_used(mem, head, data_written + 1)
		.map_err(|e| err(format!("virtio-blk used-ring update failed: {e}")))?;
	Ok(Outcome::Synchronous)
}

const fn io_status(ok: bool) -> u32 {
	if ok {
		VIRTIO_BLK_S_OK
	} else {
		VIRTIO_BLK_S_IOERR
	}
}
#[cfg(target_os = "linux")]
pub(super) mod linux {
	use io_uring::{opcode, types};

	use super::*;

	/// `FICLONE` ioctl: `_IOW(0x94, 9, int)`. Reflinks one file's extents into
	/// another (instant copy-on-write on btrfs/XFS).
	const FICLONE: libc::c_ulong = 0x4004_9409;

	pub(super) fn try_reflink(src: &File, dst: &File) -> Option<io::Error> {
		// SAFETY: both fds are valid open files; FICLONE clones src's extents into dst.
		let ret = unsafe { libc::ioctl(dst.as_raw_fd(), FICLONE, src.as_raw_fd()) };
		if ret == 0 {
			None
		} else {
			Some(io::Error::last_os_error())
		}
	}

	fn next_unused_token(pending: &HashMap<u64, PendingReq>, next_token: &mut u64) -> Result<u64> {
		let start = *next_token;
		loop {
			let token = *next_token;
			*next_token = next_token.wrapping_add(1);
			if !pending.contains_key(&token) {
				return Ok(token);
			}
			if *next_token == start {
				return Err(err("virtio-blk io_uring token space exhausted"));
			}
		}
	}

	/// Process one virtio-blk request chain.
	///
	/// IN/OUT requests are submitted to `ring` zero-copy (iovecs reference guest
	/// memory) and reaped later; every other result (errors, flush, get-id,
	/// unsupported, malformed) is finished inline with its used-ring entry added
	/// here. The returned [`Outcome`] tells the caller which path was taken.
	#[allow(
		clippy::too_many_arguments,
		reason = "virtio-blk request submission touches split device state"
	)]
	pub(super) fn process_chain(
		disk: &File,
		capacity_sectors: u64,
		read_only: bool,
		mem: &GuestMemoryMmap,
		queue: &mut Queue,
		ring: &mut IoUring,
		pending: &mut HashMap<u64, PendingReq>,
		next_token: &mut u64,
		chain: DescriptorChain<&GuestMemoryMmap>,
	) -> Result<Outcome> {
		let head = chain.head_index();
		let descs: Vec<_> = chain.collect();
		// Need at least a header descriptor and a status descriptor.
		if descs.len() < 2 {
			return complete_malformed(queue, mem, head);
		}
		let header = &descs[0];
		let status_desc = &descs[descs.len() - 1];
		let data_descs = &descs[1..descs.len() - 1];

		if !status_desc.is_write_only()
			|| status_desc.len() < 1
			|| !descriptor_range_valid(mem, status_desc.addr(), 1)
		{
			return complete_malformed(queue, mem, head);
		}
		let status_addr = status_desc.addr();

		if header.is_write_only()
			|| header.len() < VIRTIO_BLK_REQ_HEADER_SIZE as u32
			|| !descriptor_range_valid(mem, header.addr(), VIRTIO_BLK_REQ_HEADER_SIZE as u32)
		{
			return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
		}

		let mut hdr = [0u8; VIRTIO_BLK_REQ_HEADER_SIZE];
		if mem.read_slice(&mut hdr, header.addr()).is_err() {
			return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
		}
		let req_type = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
		let sector = u64::from_le_bytes(hdr[8..16].try_into().unwrap());

		match req_type {
			VIRTIO_BLK_T_IN | VIRTIO_BLK_T_OUT => {
				let is_read = req_type == VIRTIO_BLK_T_IN;
				let data_writable = is_read;
				let Some(total_len) = validate_data_descriptors(mem, data_descs, data_writable) else {
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				};

				if !request_range_in_bounds(capacity_sectors, sector, total_len)
					|| (!is_read && read_only)
				{
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				}

				// Build iovecs pointing straight at guest-memory host addresses — no
				// bounce buffer, no copy. For reads, keep the matching GPA ranges:
				// the kernel's completion writes bypass vm-memory's dirty tracking,
				// so the reaper must mark them itself (migration-delta contract).
				let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(data_descs.len());
				let mut guest_ranges: Vec<(GuestAddress, u32)> =
					Vec::with_capacity(if is_read { data_descs.len() } else { 0 });
				for d in data_descs {
					if d.len() == 0 {
						continue;
					}
					if mem.get_slice(d.addr(), d.len() as usize).is_err() {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
					match mem.get_host_address(d.addr()) {
						Ok(ptr) => {
							iovecs.push(libc::iovec {
								iov_base: ptr as *mut libc::c_void,
								iov_len:  d.len() as usize,
							});
							if is_read {
								guest_ranges.push((d.addr(), d.len()));
							}
						},
						Err(_) => {
							return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
						},
					}
				}
				if iovecs.is_empty() {
					// No data segments: nothing to transfer.
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_OK, 0);
				}

				let file_offset = sector * SECTOR_SIZE;
				let token = next_unused_token(pending, next_token)?;
				pending.insert(token, PendingReq {
					head,
					status_addr,
					data_len: total_len,
					req_type,
					iovecs,
					guest_ranges,
				});

				// Read the now-stable iovec backing back out of the table to build
				// the SQE (moving the `Vec` into the map did not move its heap data).
				let (iov_ptr, iov_len, single) = {
					let req = pending.get(&token).expect("pending entry just inserted");
					let single = if req.iovecs.len() == 1 {
						Some((req.iovecs[0].iov_base, req.iovecs[0].iov_len))
					} else {
						None
					};
					(req.iovecs.as_ptr(), req.iovecs.len() as u32, single)
				};
				let fd = types::Fd(disk.as_raw_fd());
				let entry = match (is_read, single) {
					(true, Some((base, len))) => opcode::Read::new(fd, base as *mut u8, len as u32)
						.offset(file_offset)
						.build()
						.user_data(token),
					(false, Some((base, len))) => opcode::Write::new(fd, base as *const u8, len as u32)
						.offset(file_offset)
						.build()
						.user_data(token),
					(true, None) => opcode::Readv::new(fd, iov_ptr, iov_len)
						.offset(file_offset)
						.build()
						.user_data(token),
					(false, None) => opcode::Writev::new(fd, iov_ptr, iov_len)
						.offset(file_offset)
						.build()
						.user_data(token),
				};

				// SAFETY: the entry's buffers (iovecs / single host pointer) live in
				// `pending[token]` and reference guest memory held for the VM's life,
				// so they stay valid until the completion removes the entry.
				let mut pushed = unsafe { ring.submission().push(&entry) }.is_ok();
				if !pushed {
					// Ring full: flush what we have to free slots, then retry once.
					ring
						.submit()
						.map_err(|e| err(format!("virtio-blk retry submit failed: {e}")))?;
					// SAFETY: identical to the first push: `entry` references request
					// buffers kept alive in `pending[token]`, and `submit()` above only
					// makes room in the SQ without invalidating them.
					pushed = unsafe { ring.submission().push(&entry) }.is_ok();
				}
				if !pushed {
					if let Some(req) = pending.remove(&token) {
						mem.write_slice(&[VIRTIO_BLK_S_IOERR as u8], req.status_addr)
							.map_err(|e| err(format!("virtio-blk status write failed: {e}")))?;
						queue
							.add_used(mem, req.head, 1)
							.map_err(|e| err(format!("virtio-blk used-ring update failed: {e}")))?;
					}
					return Ok(Outcome::Synchronous);
				}
				Ok(Outcome::Submitted)
			},
			VIRTIO_BLK_T_FLUSH => {
				if !data_descs.is_empty() {
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				}
				let status = io_status(disk.sync_all().is_ok());
				complete_sync(queue, mem, head, status_addr, status, 0)
			},
			VIRTIO_BLK_T_GET_ID => {
				for d in data_descs {
					if !d.is_write_only() || !descriptor_range_valid(mem, d.addr(), d.len()) {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
				}
				let mut id = [0u8; DEVICE_ID_LEN];
				let copy_len = DEVICE_ID.len().min(DEVICE_ID_LEN);
				id[..copy_len].copy_from_slice(&DEVICE_ID[..copy_len]);

				let mut written = 0usize;
				for d in data_descs {
					if written >= DEVICE_ID_LEN {
						break;
					}
					let n = (d.len() as usize).min(DEVICE_ID_LEN - written);
					if mem
						.write_slice(&id[written..written + n], d.addr())
						.is_err()
					{
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
					written += n;
				}
				complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_OK, written as u32)
			},
			_ => complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_UNSUPP, 0),
		}
	}
}

#[cfg(any(target_os = "macos", target_os = "windows"))]
pub(super) mod sync_io {
	#[cfg(target_os = "macos")]
	use std::os::unix::fs::FileExt;
	#[cfg(target_os = "windows")]
	use std::os::windows::fs::FileExt;

	use super::*;

	/// Process one virtio-blk request chain.
	pub(super) fn process_chain(
		disk: &File,
		capacity_sectors: u64,
		read_only: bool,
		mem: &GuestMemoryMmap,
		queue: &mut Queue,
		chain: DescriptorChain<&GuestMemoryMmap>,
	) -> Result<Outcome> {
		let head = chain.head_index();
		let descs: Vec<_> = chain.collect();
		if descs.len() < 2 {
			return complete_malformed(queue, mem, head);
		}
		let header = &descs[0];
		let status_desc = &descs[descs.len() - 1];
		let data_descs = &descs[1..descs.len() - 1];

		if !status_desc.is_write_only()
			|| status_desc.len() < 1
			|| !descriptor_range_valid(mem, status_desc.addr(), 1)
		{
			return complete_malformed(queue, mem, head);
		}
		let status_addr = status_desc.addr();

		if header.is_write_only()
			|| header.len() < VIRTIO_BLK_REQ_HEADER_SIZE as u32
			|| !descriptor_range_valid(mem, header.addr(), VIRTIO_BLK_REQ_HEADER_SIZE as u32)
		{
			return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
		}

		let mut hdr = [0u8; VIRTIO_BLK_REQ_HEADER_SIZE];
		if mem.read_slice(&mut hdr, header.addr()).is_err() {
			return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
		}
		let req_type = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
		let sector = u64::from_le_bytes(hdr[8..16].try_into().unwrap());

		match req_type {
			VIRTIO_BLK_T_IN | VIRTIO_BLK_T_OUT => {
				let is_read = req_type == VIRTIO_BLK_T_IN;
				let data_writable = is_read;
				let Some(total_len) = validate_data_descriptors(mem, data_descs, data_writable) else {
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				};
				if !request_range_in_bounds(capacity_sectors, sector, total_len)
					|| (!is_read && read_only)
				{
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				}

				let mut file_offset = sector * SECTOR_SIZE;
				for d in data_descs {
					if d.len() == 0 {
						continue;
					}
					let len = d.len() as usize;
					if mem.get_slice(d.addr(), len).is_err() {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
					let Ok(ptr) = mem.get_host_address(d.addr()) else {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					};
					let ok = if is_read {
						// SAFETY: descriptor validation above proved this guest-memory range exists
						// and is writable by the device for a read request.
						let dst = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
						read_exact_at(disk, dst, file_offset).is_ok()
					} else {
						// SAFETY: descriptor validation above proved this guest-memory range exists
						// and is readable by the device for a write request.
						let src = unsafe { std::slice::from_raw_parts(ptr, len) };
						write_all_at(disk, src, file_offset).is_ok()
					};
					if !ok {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
					file_offset += d.len() as u64;
				}
				complete_sync(
					queue,
					mem,
					head,
					status_addr,
					VIRTIO_BLK_S_OK,
					if is_read { total_len } else { 0 },
				)
			},
			VIRTIO_BLK_T_FLUSH => {
				if !data_descs.is_empty() {
					return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
				}
				let status = io_status(disk.sync_all().is_ok());
				complete_sync(queue, mem, head, status_addr, status, 0)
			},
			VIRTIO_BLK_T_GET_ID => {
				for d in data_descs {
					if !d.is_write_only() || !descriptor_range_valid(mem, d.addr(), d.len()) {
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
				}
				let mut id = [0u8; DEVICE_ID_LEN];
				let copy_len = DEVICE_ID.len().min(DEVICE_ID_LEN);
				id[..copy_len].copy_from_slice(&DEVICE_ID[..copy_len]);

				let mut written = 0usize;
				for d in data_descs {
					if written >= DEVICE_ID_LEN {
						break;
					}
					let n = (d.len() as usize).min(DEVICE_ID_LEN - written);
					if mem
						.write_slice(&id[written..written + n], d.addr())
						.is_err()
					{
						return complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_IOERR, 0);
					}
					written += n;
				}
				complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_OK, written as u32)
			},
			_ => complete_sync(queue, mem, head, status_addr, VIRTIO_BLK_S_UNSUPP, 0),
		}
	}

	fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
		while !buf.is_empty() {
			match positioned_read(file, buf, offset) {
				Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
				Ok(n) => {
					offset += n as u64;
					let tmp = buf;
					buf = &mut tmp[n..];
				},
				Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
				Err(e) => return Err(e),
			}
		}
		Ok(())
	}

	fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
		while !buf.is_empty() {
			match positioned_write(file, buf, offset) {
				Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
				Ok(n) => {
					offset += n as u64;
					buf = &buf[n..];
				},
				Err(e) if e.kind() == io::ErrorKind::Interrupted => {},
				Err(e) => return Err(e),
			}
		}
		Ok(())
	}

	#[cfg(target_os = "macos")]
	fn positioned_read(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
		file.read_at(buf, offset)
	}

	#[cfg(target_os = "windows")]
	fn positioned_read(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
		file.seek_read(buf, offset)
	}

	#[cfg(target_os = "macos")]
	fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
		file.write_at(buf, offset)
	}

	#[cfg(target_os = "windows")]
	fn positioned_write(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
		file.seek_write(buf, offset)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		io::Write,
		path::{Path, PathBuf},
		sync::atomic::{AtomicU64, Ordering},
	};

	use virtio_bindings::bindings::virtio_ring::VRING_DESC_F_WRITE;
	use virtio_queue::desc::split::Descriptor as SplitDescriptor;
	use vm_memory::GuestAddress;

	use super::*;

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	struct TestDir {
		path: PathBuf,
	}

	impl TestDir {
		fn new() -> Self {
			let unique = format!(
				"vmon-block-test-{}-{}",
				std::process::id(),
				NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
			);
			let path = std::env::temp_dir().join(unique);
			std::fs::create_dir(&path).expect("create temp block test dir");
			Self { path }
		}

		fn path(&self) -> &Path {
			&self.path
		}
	}

	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.path);
		}
	}

	fn overlay_err(base: &Path, dest: &Path) -> String {
		match create_cow_overlay(base, dest) {
			Ok(_) => panic!("expected overlay creation to fail"),
			Err(e) => e.to_string(),
		}
	}

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x1_0000)]).expect("guest memory")
	}

	#[test]
	fn data_descriptor_validation_enforces_expected_direction() {
		let mem = guest_mem();
		let readable = [SplitDescriptor::new(0x1000, 512, 0, 0)];
		let writable = [SplitDescriptor::new(0x1000, 512, VRING_DESC_F_WRITE as u16, 0)];

		assert_eq!(validate_data_descriptors(&mem, &readable, false), Some(512));
		assert!(validate_data_descriptors(&mem, &readable, true).is_none());
		assert_eq!(validate_data_descriptors(&mem, &writable, true), Some(512));
		assert!(validate_data_descriptors(&mem, &writable, false).is_none());
	}

	#[test]
	fn data_descriptor_validation_rejects_out_of_range_buffer() {
		let mem = guest_mem();
		let out_of_range = [SplitDescriptor::new(0xff00, 512, 0, 0)];

		assert!(validate_data_descriptors(&mem, &out_of_range, false).is_none());
	}

	#[test]
	fn request_range_validation_rejects_capacity_overrun_and_overflow() {
		assert!(request_range_in_bounds(2, 1, 512));
		assert!(!request_range_in_bounds(2, 1, 513));
		assert!(!request_range_in_bounds(2, u64::MAX, 1));
		assert!(!request_range_in_bounds(u64::MAX, 0, 1));
	}

	#[cfg(unix)]
	#[test]
	fn create_cow_overlay_rejects_symlink_base() {
		let tmp = TestDir::new();
		let real_base = tmp.path().join("base.img");
		let link_base = tmp.path().join("base-link.img");
		let dest = tmp.path().join("overlay.img");
		std::fs::write(&real_base, b"base").expect("write base disk");
		std::os::unix::fs::symlink(&real_base, &link_base).expect("create base symlink");

		let err = overlay_err(&link_base, &dest);
		assert!(err.contains("symlink"), "unexpected error: {err}");
	}

	#[test]
	fn create_cow_overlay_rejects_existing_destination_file() {
		let tmp = TestDir::new();
		let base = tmp.path().join("base.img");
		let dest = tmp.path().join("overlay.img");
		std::fs::write(&base, b"base").expect("write base disk");
		std::fs::write(&dest, b"existing").expect("write existing destination");

		let err = overlay_err(&base, &dest);
		assert!(err.contains("already exists"), "unexpected error: {err}");
	}

	#[test]
	fn create_cow_overlay_copies_into_new_destination() {
		let tmp = TestDir::new();
		let base = tmp.path().join("base.img");
		let dest = tmp.path().join("overlay.img");
		std::fs::write(&base, b"base").expect("write base disk");

		let mut overlay = create_cow_overlay(&base, &dest).expect("create overlay");
		use std::io::{Read, Seek, SeekFrom};
		overlay
			.seek(SeekFrom::Start(0))
			.expect("seek overlay after create");
		let mut contents = Vec::new();
		overlay
			.read_to_end(&mut contents)
			.expect("read overlay after create");

		assert_eq!(contents, b"base");
		assert_eq!(std::fs::read(&base).expect("read base"), b"base");
	}

	#[cfg(unix)]
	#[test]
	fn create_cow_overlay_rejects_existing_destination_symlink_without_clobbering_target() {
		let tmp = TestDir::new();
		let base = tmp.path().join("base.img");
		let victim = tmp.path().join("victim.img");
		let link_dest = tmp.path().join("overlay-link.img");
		std::fs::write(&base, b"base").expect("write base disk");
		std::fs::write(&victim, b"victim").expect("write victim");
		std::os::unix::fs::symlink(&victim, &link_dest).expect("create destination symlink");

		let err = overlay_err(&base, &link_dest);
		assert!(err.contains("already exists"), "unexpected error: {err}");
		assert_eq!(std::fs::read(&victim).expect("read victim after rejected overlay"), b"victim");
	}

	/// A reaped IN completion must mark exactly the written prefix of its data
	/// descriptors (migration-delta contract); ranges are marked in order and
	/// the spill into a later range is honored.
	#[test]
	#[cfg(target_os = "linux")]
	fn mark_guest_ranges_dirty_marks_only_the_written_prefix() {
		use vm_memory::bitmap::Bitmap;

		// 256 KiB ranges / margins dwarf any real host page size (4-64 KiB), so
		// the assertions hold regardless of the bitmap's page granularity.
		const RANGE_LEN: usize = 0x4_0000;
		let mem =
			GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10_0000)]).expect("guest memory");
		let ranges =
			[(GuestAddress(0), RANGE_LEN as u32), (GuestAddress(0x8_0000), RANGE_LEN as u32)];

		// One byte spills past the first range into the second.
		mark_guest_ranges_dirty(&mem, &ranges, RANGE_LEN + 1);

		let region = mem.iter().next().expect("region");
		let bitmap: &vm_memory::bitmap::AtomicBitmap = vm_memory::mmap::MmapRegion::bitmap(region);
		assert!(bitmap.dirty_at(0), "first range start must be dirty");
		assert!(bitmap.dirty_at(RANGE_LEN - 1), "first range end must be dirty");
		assert!(bitmap.dirty_at(0x8_0000), "spill byte in second range must be dirty");
		assert!(!bitmap.dirty_at(0x6_0000), "gap between the ranges must stay clean");
		assert!(!bitmap.dirty_at(0x9_0000), "unwritten tail of the second range must stay clean");
	}

	/// A short read marks only the bytes the kernel reported written; untouched
	/// ranges stay clean so migration deltas remain tight.
	#[test]
	#[cfg(target_os = "linux")]
	fn mark_guest_ranges_dirty_stops_at_short_read() {
		use vm_memory::bitmap::Bitmap;

		let mem =
			GuestMemoryMmap::from_ranges(&[(GuestAddress(0), 0x10_0000)]).expect("guest memory");
		let ranges = [(GuestAddress(0), 0x4_0000), (GuestAddress(0x8_0000), 0x4_0000)];

		mark_guest_ranges_dirty(&mem, &ranges, 1);

		let region = mem.iter().next().expect("region");
		let bitmap: &vm_memory::bitmap::AtomicBitmap = vm_memory::mmap::MmapRegion::bitmap(region);
		assert!(bitmap.dirty_at(0), "the single written byte must be dirty");
		assert!(!bitmap.dirty_at(0x6_0000), "bytes past the short read must stay clean");
		assert!(!bitmap.dirty_at(0x8_0000), "the second range must stay clean");
	}

	#[test]
	fn active_disk_clone_includes_boundary_bytes_not_later_writes() {
		let tmp = TestDir::new();
		let source_path = tmp.path().join("active.img");
		let image = tmp.path().join("capture.img");
		let mut source = OpenOptions::new()
			.read(true)
			.write(true)
			.create_new(true)
			.open(&source_path)
			.expect("create source disk");
		source.write_all(b"before").expect("write before boundary");
		source.sync_all().expect("sync boundary write");

		let capture = clone_open_disk(&source, &image).expect("capture active disk");
		assert_eq!(capture.bytes, 6);
		source.seek(SeekFrom::Start(0)).expect("rewind source");
		source.write_all(b"after!").expect("write after boundary");
		source.sync_all().expect("sync later write");

		assert_eq!(std::fs::read(image).expect("read captured image"), b"before");
	}
}
