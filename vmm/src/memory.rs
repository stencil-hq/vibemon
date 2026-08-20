//! Guest RAM allocation.

#[cfg(not(target_os = "windows"))]
use std::{fs::File, io, os::unix::io::AsRawFd};

#[cfg(not(target_os = "windows"))]
use vm_memory::FileOffset;
use vm_memory::{
	Address, GuestAddress, GuestMemory, GuestMemoryMmap as VmGuestMemoryMmap,
	bitmap::{AtomicBitmap, Bitmap},
};

use crate::result::{Result, err};

const PAGE_SIZE: usize = 4096;

/// Concrete guest-memory type used throughout the VMM.
///
/// Every region carries an [`vm_memory::bitmap::AtomicBitmap`] that records
/// HOST-side writes (virtio queues and any tracked writer). Together with the
/// hypervisor's guest-write dirty log it bounds the pages a live-migration
/// delta must inspect; outside of an armed migration the bitmap is ignored.
pub type GuestMemoryMmap = VmGuestMemoryMmap<AtomicBitmap>;

/// Mark `[gpa, gpa + len)` dirty in the host-write bitmap.
///
/// Raw-pointer DMA paths (`io_uring` iovecs) write guest RAM without
/// going through vm-memory's tracked writers and MUST call this on completion,
/// or a live-migration delta may miss their writes.
#[cfg_attr(
	target_os = "macos",
	allow(dead_code, reason = "only raw Linux DMA paths mark dirty; HVF never reads the bitmap")
)]
pub fn mark_dirty(mem: &GuestMemoryMmap, gpa: GuestAddress, len: usize) {
	use vm_memory::{GuestMemoryRegion, guest_memory::GuestMemory as _};
	let mut addr = gpa;
	let mut remaining = len;
	while remaining > 0 {
		let Some(region) = mem.find_region(addr) else {
			return;
		};
		let offset = (addr.raw_value() - region.start_addr().raw_value()) as usize;
		let region_len = usize::try_from(region.len()).unwrap_or(usize::MAX);
		let in_region = region_len.saturating_sub(offset).min(remaining);
		if in_region == 0 {
			return;
		}
		region.bitmap().mark_dirty(offset, in_region);
		remaining -= in_region;
		let Some(next) = addr.checked_add(in_region as u64) else {
			return;
		};
		addr = next;
	}
}

/// True when `bytes` is all zero.
///
/// After checking the first byte, compares each byte to its predecessor. Byte
/// slice equality lowers to `memcmp`, and overlapping reads keep both inputs in
/// the same cache lines.
pub fn is_zero(bytes: &[u8]) -> bool {
	match bytes {
		[] => true,
		[0, rest @ ..] => rest == &bytes[..bytes.len() - 1],
		_ => false,
	}
}

/// Clear every region's host-write bitmap (arming a fresh tracking window).
pub fn reset_dirty(mem: &GuestMemoryMmap) {
	for region in mem.iter() {
		let mmap: &vm_memory::mmap::MmapRegion<AtomicBitmap> = region;
		mmap.bitmap().reset();
	}
}

/// Split a requested RAM size into guest-physical regions (`x86_64)`: all RAM
/// below the 32-bit MMIO gap at GPA 0, the remainder relocated above 4 GiB.
#[cfg(target_arch = "x86_64")]
pub fn arrange_memory(size: usize) -> Vec<(GuestAddress, usize)> {
	use crate::layout::{FIRST_ADDR_PAST_32BITS, MMIO_MEM_START};
	let size = size as u64;
	if size == 0 {
		Vec::new()
	} else if size <= MMIO_MEM_START {
		vec![(GuestAddress(0), size as usize)]
	} else {
		vec![
			(GuestAddress(0), MMIO_MEM_START as usize),
			(GuestAddress(FIRST_ADDR_PAST_32BITS), (size - MMIO_MEM_START) as usize),
		]
	}
}

/// On aarch64, guest RAM is a single region at the DRAM base (2 GiB); the MMIO
/// devices and GIC live below it.
#[cfg(target_arch = "aarch64")]
pub fn arrange_memory(size: usize) -> Vec<(GuestAddress, usize)> {
	if size == 0 {
		Vec::new()
	} else {
		vec![(GuestAddress(crate::layout::DRAM_BASE), size)]
	}
}

/// Allocate non-zero, page-aligned guest RAM of `size` bytes as file-backed,
/// `MAP_SHARED` regions so snapshots can copy RAM and `CoW` forks can remap
/// the snapshot privately.
pub fn create_guest_memory(size: usize) -> Result<GuestMemoryMmap> {
	if size == 0 {
		return Err(err("guest memory size is zero"));
	}
	if !size.is_multiple_of(PAGE_SIZE) {
		return Err(err(format!("guest memory size {size} is not page-aligned")));
	}
	let regions = arrange_memory(size);
	#[cfg(target_os = "windows")]
	{
		Ok(GuestMemoryMmap::from_ranges(&regions)?)
	}
	#[cfg(not(target_os = "windows"))]
	{
		let mut ranges: Vec<(GuestAddress, usize, Option<FileOffset>)> =
			Vec::with_capacity(regions.len());
		for (addr, len) in regions {
			let file = create_shared_memory_file(len)?;
			ranges.push((addr, len, Some(FileOffset::new(file, 0))));
		}
		Ok(GuestMemoryMmap::from_ranges_with_files(ranges)?)
	}
}

/// Create the platform RAM backing file used by the shared guest mappings.
#[cfg(target_os = "linux")]
mod linux {
	use std::os::unix::io::FromRawFd;

	use super::*;

	pub(super) fn create_shared_memory_file(len: usize) -> Result<File> {
		create_memfd(len)
	}

	/// Create an anonymous memfd of exactly `len` bytes, returned as an owned
	/// `File`.
	///
	/// The fd is `MFD_CLOEXEC` and sized with `ftruncate`. Mapping it with a
	/// `FileOffset` (via `from_ranges_with_files`) yields a shared, file-backed
	/// region, so guest and VMM writes land in the same backing object.
	fn create_memfd(len: usize) -> Result<File> {
		// SAFETY: name is a valid NUL-terminated C string; flags are valid.
		let fd = unsafe { libc::memfd_create(c"vmon-ram".as_ptr(), libc::MFD_CLOEXEC) };
		if fd < 0 {
			return Err(io::Error::last_os_error().into());
		}
		// SAFETY: fd was just created and is exclusively owned here.
		let file = unsafe { File::from_raw_fd(fd) };
		size_file(&file, len)?;
		Ok(file)
	}

	pub(super) const fn private_mmap_flags() -> libc::c_int {
		libc::MAP_NORESERVE | libc::MAP_PRIVATE
	}

	/// Hint the host KSM daemon to dedup identical anonymous/COW pages in guest
	/// RAM across co-resident guests. No-op unless the operator enabled KSM
	/// (/sys/kernel/mm/ksm/run). Only anonymous pages are scanned, so this is
	/// effective for `MAP_PRIVATE` fork clones' `COWed` pages; harmless on the
	/// shared memfd.
	pub fn advise_mergeable(mem: &GuestMemoryMmap) {
		use vm_memory::{GuestMemory, GuestMemoryRegion};

		for region in mem.iter() {
			let ptr = region.as_ptr() as *mut libc::c_void;
			let len = region.len() as usize;
			// SAFETY: ptr/len name a live mapping owned by `mem`.
			if unsafe { libc::madvise(ptr, len, libc::MADV_MERGEABLE) } == 0 {
				crate::metrics::record_ksm_region();
			}
		}
	}
}

#[cfg(target_os = "macos")]
mod macos {
	use std::{
		os::unix::fs::OpenOptionsExt,
		sync::atomic::{AtomicU64, Ordering},
	};

	use super::*;

	/// Create an unlinked temporary file for macOS shared guest RAM mappings.
	pub(super) fn create_shared_memory_file(len: usize) -> Result<File> {
		static NEXT_TMP: AtomicU64 = AtomicU64::new(0);

		let tmpdir =
			std::env::var_os("TMPDIR").map_or_else(std::env::temp_dir, std::path::PathBuf::from);
		for _ in 0..128 {
			let id = NEXT_TMP.fetch_add(1, Ordering::Relaxed);
			let path = tmpdir.join(format!("vmon-ram-{}-{id}", std::process::id()));
			match std::fs::OpenOptions::new()
				.read(true)
				.write(true)
				.create_new(true)
				.mode(0o600)
				.open(&path)
			{
				Ok(file) => {
					let _ = std::fs::remove_file(&path);
					size_file(&file, len)?;
					return Ok(file);
				},
				Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {},
				Err(e) => {
					return Err(crate::result::err(format!(
						"creating RAM backing {}: {e}",
						path.display()
					)));
				},
			}
		}
		Err(crate::result::err("creating unique RAM backing file in TMPDIR"))
	}

	pub(super) const fn private_mmap_flags() -> libc::c_int {
		libc::MAP_PRIVATE
	}

	pub const fn advise_mergeable(_mem: &GuestMemoryMmap) {}
}

#[cfg(target_os = "linux")]
pub use linux::advise_mergeable;
#[cfg(target_os = "linux")]
use linux::{create_shared_memory_file, private_mmap_flags};
#[cfg(target_os = "macos")]
pub use macos::advise_mergeable;
#[cfg(target_os = "macos")]
use macos::{create_shared_memory_file, private_mmap_flags};
#[cfg(target_os = "windows")]
pub const fn advise_mergeable(_mem: &GuestMemoryMmap) {}

/// Resize a backing file to exactly `len` bytes.
#[cfg(not(target_os = "windows"))]
fn size_file(file: &File, len: usize) -> Result<()> {
	let len =
		libc::off_t::try_from(len).map_err(|_| err("RAM backing file size overflows off_t"))?;
	// SAFETY: fd is valid; sizing the file to `len` bytes.
	let ret = unsafe { libc::ftruncate(file.as_raw_fd(), len) };
	if ret < 0 {
		return Err(io::Error::last_os_error().into());
	}
	Ok(())
}

/// Map a validated full snapshot memory file `MAP_PRIVATE` per region for a
/// `CoW` fork: clean pages are shared via the host page cache across every
/// child process, while a write faults a private copy. `regions` is `(gpa, len,
/// file_offset)` matching the snapshot's contiguous region table.
#[cfg(target_os = "windows")]
pub fn create_guest_memory_private(
	mem_file: &std::path::Path,
	regions: &[(u64, u64, u64)],
) -> Result<GuestMemoryMmap> {
	use std::os::windows::fs::FileExt;

	use vm_memory::{Bytes, mmap::GuestRegionMmap};
	let file = std::fs::File::open(mem_file)
		.map_err(|e| err(format!("opening {}: {e}", mem_file.display())))?;
	let file_len = file.metadata()?.len();
	if regions.is_empty() {
		return Err(err("snapshot has no memory regions"));
	}
	let mut next_file_offset = 0u64;
	let mut mapped = Vec::with_capacity(regions.len());
	for (index, &(gpa, len, file_offset)) in regions.iter().enumerate() {
		if gpa % PAGE_SIZE as u64 != 0 || len == 0 || len % PAGE_SIZE as u64 != 0 {
			return Err(err(format!("snapshot memory region {index} is not page-aligned")));
		}
		let end = file_offset
			.checked_add(len)
			.ok_or_else(|| err(format!("snapshot memory region {index} file extent overflows")))?;
		if file_offset != next_file_offset || end > file_len {
			return Err(err(format!("snapshot memory region {index} has invalid file extent")));
		}
		next_file_offset = end;
		let size = usize::try_from(len).map_err(|_| err("snapshot memory region exceeds usize"))?;
		let region = vm_memory::mmap::MmapRegion::new(size)?;
		let guest = GuestRegionMmap::new(region, GuestAddress(gpa))
			.ok_or_else(|| err(format!("snapshot memory region {index} is invalid")))?;
		let mut offset = 0usize;
		let mut buffer = vec![0u8; 1 << 20];
		while offset < size {
			let count = (size - offset).min(buffer.len());
			let read = file.seek_read(&mut buffer[..count], file_offset + offset as u64)?;
			if read == 0 {
				return Err(err("snapshot memory file ended early"));
			}
			guest.write_slice(&buffer[..read], vm_memory::MemoryRegionAddress(offset as u64))?;
			offset += read;
		}
		mapped.push(guest);
	}
	GuestMemoryMmap::from_regions(mapped).map_err(Into::into)
}

#[cfg(not(target_os = "windows"))]
pub fn create_guest_memory_private(
	mem_file: &std::path::Path,
	regions: &[(u64, u64, u64)],
) -> Result<GuestMemoryMmap> {
	use vm_memory::mmap::{GuestRegionMmap, MmapRegion};

	let mem = File::open(mem_file)
		.map_err(|e| crate::result::err(format!("opening {}: {e}", mem_file.display())))?;
	let metadata = mem
		.metadata()
		.map_err(|e| crate::result::err(format!("stat {}: {e}", mem_file.display())))?;
	if !metadata.is_file() {
		return Err(crate::result::err(format!(
			"snapshot memory file {} is not a regular file",
			mem_file.display()
		)));
	}
	let file_len = metadata.len();
	if regions.is_empty() {
		return Err(crate::result::err("snapshot has no memory regions"));
	}
	let mut next_file_offset = 0u64;
	let mut gregions = Vec::with_capacity(regions.len());
	for (idx, &(gpa, len, file_offset)) in regions.iter().enumerate() {
		if gpa % PAGE_SIZE as u64 != 0 {
			return Err(crate::result::err(format!(
				"snapshot memory region {idx} GPA {gpa:#x} is not page-aligned"
			)));
		}
		if len == 0 {
			return Err(crate::result::err(format!("snapshot memory region @ {gpa:#x} is empty")));
		}
		if len % PAGE_SIZE as u64 != 0 {
			return Err(crate::result::err(format!(
				"snapshot memory region @ {gpa:#x} length {len} is not page-aligned"
			)));
		}
		if file_offset % PAGE_SIZE as u64 != 0 {
			return Err(crate::result::err(format!(
				"snapshot memory region @ {gpa:#x} file offset {file_offset} is not page-aligned"
			)));
		}
		if file_offset != next_file_offset {
			return Err(crate::result::err(format!(
				"snapshot memory region @ {gpa:#x} file offset {file_offset} != expected \
				 {next_file_offset}"
			)));
		}
		let end = file_offset.checked_add(len).ok_or_else(|| {
			crate::result::err(format!("snapshot memory region @ {gpa:#x} file offset overflows"))
		})?;
		if end > file_len {
			return Err(crate::result::err(format!(
				"snapshot memory region @ {gpa:#x} exceeds snapshot memory file length {file_len}"
			)));
		}
		next_file_offset = end;
		let len = usize::try_from(len).map_err(|_| {
			crate::result::err(format!("snapshot memory region @ {gpa:#x} is too large"))
		})?;
		let file = mem
			.try_clone()
			.map_err(|e| crate::result::err(format!("cloning {}: {e}", mem_file.display())))?;
		let fo = FileOffset::new(file, file_offset);
		let region = MmapRegion::<AtomicBitmap>::build(
			Some(fo),
			len,
			libc::PROT_READ | libc::PROT_WRITE,
			private_mmap_flags(),
		)
		.map_err(|e| crate::result::err(format!("mmap MAP_PRIVATE region @ {gpa:#x}: {e}")))?;
		let greg =
			GuestRegionMmap::new(region, GuestAddress(gpa)).ok_or("guest region address overflow")?;
		gregions.push(greg);
	}
	if next_file_offset != file_len {
		return Err(crate::result::err(format!(
			"snapshot memory regions cover {next_file_offset} bytes but memory file is {file_len} \
			 bytes"
		)));
	}
	GuestMemoryMmap::from_regions(gregions)
		.map_err(|e| crate::result::err(format!("assembling private guest memory: {e}")))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn result_err<T>(result: Result<T>) -> String {
		match result {
			Ok(_) => panic!("operation unexpectedly succeeded"),
			Err(e) => e.to_string(),
		}
	}

	#[cfg(not(target_os = "windows"))]
	fn private_memory_fixture() -> (std::path::PathBuf, Vec<u8>) {
		let path = std::env::temp_dir().join(format!(
			"vmon-private-memory-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		let mut contents = vec![0u8; PAGE_SIZE * 2];
		contents[..PAGE_SIZE].fill(0x35);
		contents[PAGE_SIZE..].fill(0xca);
		std::fs::write(&path, &contents).unwrap();
		(path, contents)
	}

	#[test]
	fn is_zero_handles_empty_and_nonzero_positions() {
		assert!(is_zero(&[]));
		assert!(is_zero(&[0]));
		assert!(!is_zero(&[1]));

		let mut bytes = vec![0; 64 * 1024 + 1];
		assert!(is_zero(&bytes));
		for index in [0, 1, bytes.len() / 2, bytes.len() - 1] {
			bytes[index] = 1;
			assert!(!is_zero(&bytes), "missed nonzero byte at {index}");
			bytes[index] = 0;
		}
	}

	#[test]
	fn create_guest_memory_rejects_zero_size() {
		let err = result_err(create_guest_memory(0));
		assert!(err.contains("zero"), "unexpected error: {err}");
	}

	#[test]
	fn create_guest_memory_rejects_unaligned_size() {
		let err = result_err(create_guest_memory(PAGE_SIZE + 1));
		assert!(err.contains("page-aligned"), "unexpected error: {err}");
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn private_snapshot_mapping_faults_original_contents_on_first_touch() {
		use vm_memory::Bytes;

		let (path, expected) = private_memory_fixture();
		let memory = create_guest_memory_private(&path, &[(0, expected.len() as u64, 0)])
			.expect("private snapshot mapping");
		let mut actual = vec![0u8; expected.len()];
		memory
			.read_slice(&mut actual, GuestAddress(0))
			.expect("read lazily faulted snapshot contents");
		let _ = std::fs::remove_file(path);
		assert_eq!(actual, expected);
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn private_snapshot_mappings_isolate_clone_writes() {
		use vm_memory::Bytes;

		let (path, expected) = private_memory_fixture();
		let regions = [(0, expected.len() as u64, 0)];
		let first =
			create_guest_memory_private(&path, &regions).expect("first private snapshot mapping");
		let second =
			create_guest_memory_private(&path, &regions).expect("second private snapshot mapping");
		let replacement = [0x7eu8; 32];
		first
			.write_slice(&replacement, GuestAddress(PAGE_SIZE as u64))
			.expect("write first clone");

		let mut sibling = [0u8; 32];
		second
			.read_slice(&mut sibling, GuestAddress(PAGE_SIZE as u64))
			.expect("read sibling clone");
		let base = std::fs::read(&path).expect("read snapshot base");
		let _ = std::fs::remove_file(path);
		assert_eq!(sibling, expected[PAGE_SIZE..PAGE_SIZE + sibling.len()]);
		assert_eq!(base, expected);
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn create_guest_memory_private_rejects_trailing_file_bytes() {
		let path = std::env::temp_dir().join(format!(
			"vmon-private-memory-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::write(&path, vec![0; PAGE_SIZE * 2]).unwrap();
		let err = result_err(create_guest_memory_private(&path, &[(0, PAGE_SIZE as u64, 0)]));
		let _ = std::fs::remove_file(&path);
		assert!(err.contains("memory file is"), "unexpected error: {err}");
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn advise_mergeable_smoke() {
		let mem = create_guest_memory(2 << 20).expect("guest memory");
		let before = crate::metrics::snapshot_json()["ksm"]["regions_advised"]
			.as_u64()
			.expect("ksm counter before");
		advise_mergeable(&mem);
		let after = crate::metrics::snapshot_json()["ksm"]["regions_advised"]
			.as_u64()
			.expect("ksm counter after");
		assert!(after >= before);
	}
}
