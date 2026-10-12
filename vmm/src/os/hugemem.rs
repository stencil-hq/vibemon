//! Lazily populated 2 MiB hugetlb guest RAM for snapshot restore and fork.
//!
//! Mapping a snapshot memory file `MAP_PRIVATE` costs one EPT/NPT violation
//! plus one host fault for every 4 KiB page the guest touches after restore:
//! about 1600 for the first command in a fresh sandbox, ~40 ms under nested
//! KVM. Backing RAM with 2 MiB hugetlb pages lets KVM install 2 MiB stage-2
//! entries, so first touches cost one fault per 2 MiB.
//!
//! Pages are filled on demand: a `userfaultfd` thread copies each missing
//! 2 MiB chunk from the memory file. Every chunk is installed write-protected
//! with `UFFD_FEATURE_WP_ASYNC`, so the kernel clears the bit on the first
//! write (guest or host) without a round trip, and `PAGEMAP_SCAN` later reports
//! the chunks written since the restore: what a delta snapshot against the same
//! file must inspect ([`written_log`]).
//!
//! The `userfaultfd` must be created before the VMM drops privileges, so
//! [`prepare`] runs early and [`map`] consumes it. Any unsupported piece
//! (kernel features, hugetlb reservation, alignment) makes [`map`] return
//! `None` and the caller keeps the 4 KiB `MAP_PRIVATE` file mapping.

use std::{
	fs::File,
	io,
	os::{
		fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
		unix::fs::{FileExt, MetadataExt},
	},
	path::Path,
};

use parking_lot::Mutex;
use vm_memory::{
	GuestAddress, GuestMemory, GuestMemoryRegion,
	bitmap::AtomicBitmap,
	mmap::{GuestRegionMmap, MmapRegion},
};

use crate::{memory::GuestMemoryMmap, os::libc_abi::IoctlRequest};

/// Hugetlb page size backing restored guest RAM.
pub const HUGE_PAGE: usize = 2 << 20;
const PAGE_SIZE: usize = 4096;

const UFFD_API: u64 = 0xaa;
const UFFD_FEATURE_MISSING_HUGETLBFS: u64 = 1 << 4;
const UFFD_FEATURE_EVENT_UNMAP: u64 = 1 << 6;
const UFFD_FEATURE_WP_HUGETLBFS_SHMEM: u64 = 1 << 12;
const UFFD_FEATURE_WP_ASYNC: u64 = 1 << 15;
const UFFD_FEATURES: u64 = UFFD_FEATURE_MISSING_HUGETLBFS
	| UFFD_FEATURE_EVENT_UNMAP
	| UFFD_FEATURE_WP_HUGETLBFS_SHMEM
	| UFFD_FEATURE_WP_ASYNC;
const UFFDIO_REGISTER_MODE_MISSING: u64 = 1;
const UFFDIO_REGISTER_MODE_WP: u64 = 2;
const UFFDIO_COPY_MODE_WP: u64 = 2;
/// `_UFFDIO_COPY` bit in the `ioctls` mask `UFFDIO_REGISTER` returns.
const UFFD_REGISTER_COPY_BIT: u64 = 1 << 3;
const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
const UFFD_EVENT_UNMAP: u8 = 0x16;
const UFFDIO_API: u64 = 0xc018_aa3f;
const UFFDIO_REGISTER: u64 = 0xc020_aa00;
const UFFDIO_COPY: u64 = 0xc028_aa03;
const UFFDIO_WAKE: u64 = 0x8010_aa02;

/// `PAGEMAP_SCAN` (`_IOWR('f', 16, struct pm_scan_arg)`), Linux 6.7+.
const PAGEMAP_SCAN: u64 = 0xc060_6610;
const PAGE_IS_WRITTEN: u64 = 1 << 1;
/// `page_region` entries fetched per `PAGEMAP_SCAN` call.
const SCAN_BATCH: usize = 256;

#[repr(C)]
struct UffdioApi {
	api:      u64,
	features: u64,
	ioctls:   u64,
}

#[repr(C)]
struct UffdioRange {
	start: u64,
	len:   u64,
}

#[repr(C)]
struct UffdioRegister {
	range:  UffdioRange,
	mode:   u64,
	ioctls: u64,
}

#[repr(C)]
struct UffdioCopy {
	dst:  u64,
	src:  u64,
	len:  u64,
	mode: u64,
	copy: i64,
}

/// `struct uffd_msg`: event byte, padding, then a 24-byte union. Page faults
/// carry `flags, address`; unmap events carry `start, end`.
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct UffdMsg {
	event: u8,
	_pad:  [u8; 7],
	arg:   [u64; 3],
}

#[repr(C)]
#[derive(Default)]
struct PmScanArg {
	size:                u64,
	flags:               u64,
	start:               u64,
	end:                 u64,
	walk_end:            u64,
	vec:                 u64,
	vec_len:             u64,
	max_pages:           u64,
	category_inverted:   u64,
	category_mask:       u64,
	category_anyof_mask: u64,
	return_mask:         u64,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct PageRegion {
	start:      u64,
	end:        u64,
	categories: u64,
}

/// One mapped guest RAM region: host address range and its offset in the
/// memory file.
#[derive(Clone, Copy)]
struct Span {
	host:        usize,
	len:         usize,
	file_offset: u64,
}

impl Span {
	const fn contains(&self, addr: usize) -> bool {
		addr >= self.host && addr - self.host < self.len
	}
}

/// A live lazily populated mapping, for [`written_log`].
struct Lazy {
	spans: Vec<Span>,
	dev:   u64,
	ino:   u64,
}

/// `userfaultfd` created by [`prepare`], consumed by [`map`].
static PREPARED: Mutex<Option<OwnedFd>> = Mutex::new(None);
/// Mappings populated from a memory file, keyed by their spans.
static LAZY: Mutex<Vec<Lazy>> = Mutex::new(Vec::new());

/// Creates the `userfaultfd` [`map`] needs. Call before dropping privileges
/// (unprivileged processes usually cannot create one). Best-effort: on failure
/// restored RAM stays on 4 KiB pages.
pub fn prepare() {
	match open_uffd() {
		Ok(fd) => *PREPARED.lock() = Some(fd),
		Err(e) => tracing::info!(error = %e, "2 MiB guest RAM unavailable, using 4 KiB pages"),
	}
}

fn open_uffd() -> io::Result<OwnedFd> {
	// SAFETY: plain integer flags; on success the syscall returns a new fd.
	let fd = unsafe { libc::syscall(libc::SYS_userfaultfd, libc::O_CLOEXEC | libc::O_NONBLOCK) };
	if fd < 0 {
		return Err(io::Error::last_os_error());
	}
	let fd = RawFd::try_from(fd).map_err(|_| io::Error::other("userfaultfd fd out of range"))?;
	// SAFETY: `fd` was just returned by userfaultfd and is owned by nobody else.
	let fd = unsafe { OwnedFd::from_raw_fd(fd) };
	let mut api = UffdioApi { api: UFFD_API, features: UFFD_FEATURES, ioctls: 0 };
	// SAFETY: `api` is writable storage matching the UFFDIO_API ABI for the
	// duration of the call. EINVAL means the kernel lacks a requested feature.
	if unsafe { libc::ioctl(fd.as_raw_fd(), UFFDIO_API as IoctlRequest, &raw mut api) } != 0 {
		return Err(io::Error::last_os_error());
	}
	Ok(fd)
}

/// Maps `regions` (`(gpa, len, file_offset)`, validated against `file` by the
/// caller) as private 2 MiB hugetlb memory filled on first touch from `file`.
/// `None` when [`prepare`] did not succeed, the layout is not 2 MiB aligned,
/// the pagemap is unavailable (no [`written_log`] for delta snapshots), or the
/// kernel cannot reserve the huge pages.
pub fn map(file: &File, regions: &[(u64, u64, u64)]) -> Option<Vec<GuestRegionMmap<AtomicBitmap>>> {
	let huge = HUGE_PAGE as u64;
	if regions
		.iter()
		.any(|&(gpa, len, off)| gpa % huge != 0 || len % huge != 0 || off % huge != 0)
	{
		return None;
	}
	crate::memory::pagemap()?;
	let uffd = PREPARED.lock().take()?;
	let meta = file.metadata().ok()?;
	let source = file.try_clone().ok()?;

	let mut mapped = Vec::with_capacity(regions.len());
	let mut spans = Vec::with_capacity(regions.len());
	for &(gpa, len, file_offset) in regions {
		let len = usize::try_from(len).ok()?;
		// Without MAP_NORESERVE the kernel reserves every huge page now, so a
		// short pool fails here instead of as a fault the VM cannot survive.
		let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB | libc::MAP_HUGE_2MB;
		let region = match MmapRegion::<AtomicBitmap>::build(
			None,
			len,
			libc::PROT_READ | libc::PROT_WRITE,
			flags,
		) {
			Ok(region) => region,
			Err(e) => {
				tracing::info!(error = %e, "cannot reserve 2 MiB pages for guest RAM, using 4 KiB pages");
				return None;
			},
		};
		let host = region.as_ptr().addr();
		// SAFETY: `host..host + len` is the mapping just created. Children the
		// VMM spawns must not share (and copy-on-write) guest RAM.
		unsafe { libc::madvise(region.as_ptr().cast(), len, libc::MADV_DONTFORK) };
		let mut register = UffdioRegister {
			range:  UffdioRange { start: host as u64, len: len as u64 },
			mode:   UFFDIO_REGISTER_MODE_MISSING | UFFDIO_REGISTER_MODE_WP,
			ioctls: 0,
		};
		// SAFETY: `register` matches the UFFDIO_REGISTER ABI and names the
		// mapping owned by `region`.
		let rc = unsafe {
			libc::ioctl(uffd.as_raw_fd(), UFFDIO_REGISTER as IoctlRequest, &raw mut register)
		};
		if rc != 0 || register.ioctls & UFFD_REGISTER_COPY_BIT == 0 {
			tracing::info!(
				error = %io::Error::last_os_error(),
				"cannot register 2 MiB guest RAM with userfaultfd, using 4 KiB pages"
			);
			return None;
		}
		spans.push(Span { host, len, file_offset });
		mapped.push(GuestRegionMmap::new(region, GuestAddress(gpa))?);
	}

	let lazy = Lazy { spans: spans.clone(), dev: meta.dev(), ino: meta.ino() };
	let filler = Filler { uffd, file: source, spans };
	if let Err(e) = std::thread::Builder::new()
		.name("vmon-hugemem".into())
		.spawn(move || filler.run())
	{
		// Unpopulated RAM without a filler would hang the first access; the
		// mappings are dropped (unmapped) on return.
		tracing::info!(error = %e, "cannot start the guest RAM filler, using 4 KiB pages");
		return None;
	}
	LAZY.lock().push(lazy);
	Some(mapped)
}

/// Serves missing-page faults for one lazily populated mapping.
struct Filler {
	uffd:  OwnedFd,
	file:  File,
	spans: Vec<Span>,
}

impl Filler {
	/// Runs until every span is unmapped (the guest memory was dropped).
	fn run(mut self) {
		let mut chunk = vec![0u8; HUGE_PAGE];
		let mut msgs = [UffdMsg::default(); 16];
		loop {
			let mut pfd =
				libc::pollfd { fd: self.uffd.as_raw_fd(), events: libc::POLLIN, revents: 0 };
			// SAFETY: one valid pollfd; no timeout or signal mask.
			let rc = unsafe { libc::ppoll(&raw mut pfd, 1, std::ptr::null(), std::ptr::null()) };
			if rc < 0 {
				if io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
					continue;
				}
				Self::fatal("polling userfaultfd", &io::Error::last_os_error());
			}
			// SAFETY: `msgs` is writable storage of the given size; the kernel
			// writes whole `uffd_msg` records.
			let n = unsafe {
				libc::read(self.uffd.as_raw_fd(), msgs.as_mut_ptr().cast(), size_of_val(&msgs))
			};
			let Ok(n) = usize::try_from(n) else {
				let e = io::Error::last_os_error();
				if matches!(e.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) {
					continue;
				}
				Self::fatal("reading userfaultfd", &e);
			};
			for msg in &msgs[..n / size_of::<UffdMsg>()] {
				match msg.event {
					UFFD_EVENT_PAGEFAULT => self.fill(msg.arg[1], &mut chunk),
					UFFD_EVENT_UNMAP => {
						self.unmapped(msg.arg[0], msg.arg[1]);
						if self.spans.is_empty() {
							return;
						}
					},
					_ => {},
				}
			}
		}
	}

	/// Installs the 2 MiB chunk containing `addr`, write-protected.
	fn fill(&self, addr: u64, chunk: &mut [u8]) {
		let Ok(addr) = usize::try_from(addr) else {
			return;
		};
		// A fault queued just before its span was unmapped needs no answer.
		let Some(span) = self.spans.iter().find(|s| s.contains(addr)) else {
			return;
		};
		let offset = (addr - span.host) & !(HUGE_PAGE - 1);
		if let Err(e) = self
			.file
			.read_exact_at(chunk, span.file_offset + offset as u64)
		{
			Self::fatal("reading the snapshot memory file", &e);
		}
		let dst = (span.host + offset) as u64;
		loop {
			let mut copy = UffdioCopy {
				dst,
				src: chunk.as_ptr().addr() as u64,
				len: HUGE_PAGE as u64,
				mode: UFFDIO_COPY_MODE_WP,
				copy: 0,
			};
			// SAFETY: `copy` matches the UFFDIO_COPY ABI; `src` is `chunk`
			// (HUGE_PAGE bytes) and `dst` a huge-page-aligned registered address.
			let rc = unsafe {
				libc::ioctl(self.uffd.as_raw_fd(), UFFDIO_COPY as IoctlRequest, &raw mut copy)
			};
			if rc == 0 {
				return;
			}
			let e = io::Error::last_os_error();
			match e.raw_os_error() {
				// Mapping layout changing under us: retry.
				Some(libc::EAGAIN) => {},
				// Another fault on this chunk was served first: just wake.
				Some(libc::EEXIST) => {
					self.wake(dst);
					return;
				},
				// The mapping or the process is going away.
				Some(libc::ENOENT | libc::ESRCH) => return,
				_ => Self::fatal("installing guest RAM", &e),
			}
		}
	}

	fn wake(&self, dst: u64) {
		let mut range = UffdioRange { start: dst, len: HUGE_PAGE as u64 };
		// SAFETY: `range` matches the UFFDIO_WAKE ABI. Failure leaves nothing to
		// wake (the waiter was already woken by the winning copy).
		unsafe { libc::ioctl(self.uffd.as_raw_fd(), UFFDIO_WAKE as IoctlRequest, &raw mut range) };
	}

	fn unmapped(&mut self, start: u64, end: u64) {
		let (Ok(start), Ok(end)) = (usize::try_from(start), usize::try_from(end)) else {
			return;
		};
		let first = self.spans.first().map(|s| s.host);
		self
			.spans
			.retain(|s| s.host + s.len <= start || s.host >= end);
		let mut lazy = LAZY.lock();
		for entry in lazy
			.iter_mut()
			.filter(|l| l.spans.first().map(|s| s.host) == first)
		{
			entry
				.spans
				.retain(|s| s.host + s.len <= start || s.host >= end);
		}
		lazy.retain(|l| !l.spans.is_empty());
	}

	/// Guest RAM can no longer be supplied: a faulting vCPU or device thread
	/// would block forever, so end the VMM.
	fn fatal(what: &str, e: &io::Error) -> ! {
		tracing::error!(error = %e, "{what} for lazily populated guest RAM failed");
		std::process::abort();
	}
}

/// Pages of `live` written since it was restored from `base_memory`, when
/// `live` is a lazily populated mapping of exactly that file (see [`map`]):
/// unwritten pages still equal the file. One word bitmap of 4 KiB pages per
/// region, at 2 MiB granularity. `None` for any other RAM or when the pagemap
/// scan fails.
pub fn written_log(live: &GuestMemoryMmap, base_memory: &Path) -> Option<Vec<Vec<u64>>> {
	let base = std::fs::metadata(base_memory).ok()?;
	{
		let lazy = LAZY.lock();
		let entry = lazy
			.iter()
			.find(|l| l.dev == base.dev() && l.ino == base.ino())?;
		let matches = entry.spans.len() == live.num_regions()
			&& entry.spans.iter().zip(live.iter()).all(|(span, region)| {
				span.host == region.as_ptr().addr() && span.len as u64 == region.len()
			});
		if !matches {
			return None;
		}
	}
	let pagemap = crate::memory::pagemap()?;
	let mut log = Vec::with_capacity(live.num_regions());
	let mut found = vec![PageRegion::default(); SCAN_BATCH];
	for region in live.iter() {
		let start = region.as_ptr().addr() as u64;
		let end = start + region.len();
		let pages = usize::try_from(region.len()).ok()? / PAGE_SIZE;
		let mut words = vec![0u64; pages.div_ceil(64)];
		let mut cursor = start;
		while cursor < end {
			let mut arg = PmScanArg {
				size: size_of::<PmScanArg>() as u64,
				start: cursor,
				end,
				vec: found.as_mut_ptr().addr() as u64,
				vec_len: SCAN_BATCH as u64,
				category_mask: PAGE_IS_WRITTEN,
				return_mask: PAGE_IS_WRITTEN,
				..PmScanArg::default()
			};
			// SAFETY: `arg` matches `struct pm_scan_arg`; `vec` points at
			// `SCAN_BATCH` writable `page_region` entries.
			let rc =
				unsafe { libc::ioctl(pagemap.as_raw_fd(), PAGEMAP_SCAN as IoctlRequest, &raw mut arg) };
			let filled = usize::try_from(rc).ok()?;
			for found in &found[..filled] {
				let first = usize::try_from((found.start - start) / PAGE_SIZE as u64).ok()?;
				let last = usize::try_from((found.end - start) / PAGE_SIZE as u64).ok()?;
				for page in first..last.min(pages) {
					words[page / 64] |= 1 << (page % 64);
				}
			}
			if arg.walk_end <= cursor {
				return None;
			}
			cursor = arg.walk_end;
		}
		log.push(words);
	}
	Some(log)
}

#[cfg(test)]
mod tests {
	use std::path::PathBuf;

	use vm_memory::Bytes;

	use super::*;

	/// Snapshot memory file with one 2 MiB chunk per `chunks` fill byte;
	/// removed on drop.
	struct Snapshot(PathBuf);

	impl Drop for Snapshot {
		fn drop(&mut self) {
			let _ = std::fs::remove_file(&self.0);
		}
	}

	fn snapshot_file(name: &str, chunks: &[u8]) -> (Snapshot, File) {
		let path = std::env::temp_dir().join(format!("vmon-hugemem-{name}-{}", std::process::id()));
		let bytes: Vec<u8> = chunks
			.iter()
			.flat_map(|&fill| std::iter::repeat_n(fill, HUGE_PAGE))
			.collect();
		std::fs::write(&path, bytes).unwrap();
		let handle = File::open(&path).unwrap();
		(Snapshot(path), handle)
	}

	#[test]
	fn misaligned_layout_falls_back() {
		let (_tmp, file) = snapshot_file("misaligned", &[1]);
		assert!(map(&file, &[(4096, HUGE_PAGE as u64, 0)]).is_none());
	}

	/// Needs a userfaultfd (root or `vm.unprivileged_userfaultfd=1`) and
	/// reservable 2 MiB pages; skipped otherwise.
	#[test]
	fn lazy_fill_reads_file_and_logs_only_written_huge_pages() {
		crate::memory::open_pagemap();
		prepare();
		let (tmp, file) = snapshot_file("lazy", &[0x11, 0x22]);
		let len = 2 * HUGE_PAGE as u64;
		let Some(regions) = map(&file, &[(0, len, 0)]) else {
			eprintln!("skipping: 2 MiB lazy RAM unavailable on this host");
			return;
		};
		let mem = GuestMemoryMmap::from_regions(regions).unwrap();
		let probe = |gpa: u64| mem.read_obj::<u8>(GuestAddress(gpa)).unwrap();
		assert_eq!((probe(0), probe(HUGE_PAGE as u64 + 5)), (0x11, 0x22));
		let log = written_log(&mem, &tmp.0).expect("written log");
		assert!(log[0].iter().all(|&word| word == 0), "reads must not count as writes");

		mem.write_obj(0x7fu8, GuestAddress(HUGE_PAGE as u64 + 4096))
			.unwrap();
		assert_eq!(probe(HUGE_PAGE as u64 + 4096), 0x7f);
		let log = written_log(&mem, &tmp.0).expect("written log");
		let pages_per_huge = HUGE_PAGE / PAGE_SIZE;
		let written: Vec<usize> = (0..2 * pages_per_huge)
			.filter(|page| log[0][page / 64] & (1 << (page % 64)) != 0)
			.collect();
		assert_eq!(written, (pages_per_huge..2 * pages_per_huge).collect::<Vec<_>>());

		drop(mem);
		for _ in 0..100 {
			if LAZY.lock().is_empty() {
				return;
			}
			std::thread::sleep(std::time::Duration::from_millis(10));
		}
		panic!("filler did not retire the unmapped mapping");
	}
}
