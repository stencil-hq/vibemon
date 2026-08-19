//! Transparent guest-RAM zram/paging support.

#[cfg(target_os = "linux")]
mod linux {
	use std::{
		collections::HashMap,
		fs::{File, OpenOptions},
		io::{self, Read, Write},
		mem,
		net::TcpStream,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
			unix::fs::OpenOptionsExt,
		},
		path::{Path, PathBuf},
		ptr,
		sync::{
			Arc,
			atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
		},
		time::Duration,
	};

	use parking_lot::Mutex;
	use tracing::warn;
	use vm_memory::{Address, GuestMemory, GuestMemoryRegion};

	use crate::{
		control::{PauseGate, RunState},
		memory::GuestMemoryMmap,
		os::libc_abi::IoctlRequest,
		result::{Result, err},
	};

	const UFFD_API: u64 = 0xaa;
	const UFFDIO_REGISTER_MODE_MISSING: u64 = 1;
	const UFFD_FEATURE_MISSING_SHMEM: u64 = 1 << 5;
	const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
	#[allow(dead_code, reason = "kernel constant is kept for future userfaultfd mode support")]
	const UFFD_USER_MODE_ONLY: i32 = 1;
	const UFFDIO_API_IOCTL: u64 = 0xc018_aa3f;
	const UFFDIO_REGISTER_IOCTL: u64 = 0xc020_aa00;
	const UFFDIO_COPY_IOCTL: u64 = 0xc028_aa03;
	const UFFD_REGISTER_COPY_BIT: u64 = 1 << 3;

	pub const PAGE_SIZE: usize = 4096;
	const SWAP_SLOT_SIZE: usize = PAGE_SIZE + 1;
	const MAX_EVICT_PER_SWEEP: usize = 8192;
	const SHARDS: usize = 256;

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

	#[repr(C)]
	struct UffdMsg {
		event:      u8,
		_pad:       [u8; 7],
		pf_flags:   u64,
		pf_address: u64,
		pf_feat:    u64,
	}

	struct PagerRegion {
		base:       *mut u8,
		gpa:        u64,
		len:        usize,
		memfd:      File,
		foff:       u64,
		page_start: usize,
	}

	enum Loc {
		Zero,
		Ram(Box<[u8]>),
		Swap { slot: u32, len: u32 },
		Remote { page: u32 },
	}

	struct SwapAlloc {
		fd:   OwnedFd,
		next: u32,
		free: Vec<u32>,
		cap:  u32,
	}

	impl SwapAlloc {
		fn alloc(&mut self) -> Option<u32> {
			if let Some(slot) = self.free.pop() {
				return Some(slot);
			}
			if self.next < self.cap {
				let slot = self.next;
				self.next += 1;
				Some(slot)
			} else {
				None
			}
		}

		fn free(&mut self, slot: u32) {
			self.free.push(slot);
		}

		const fn used_pages(&self) -> usize {
			self.next as usize - self.free.len()
		}
	}

	#[derive(Clone)]
	struct HttpEndpoint {
		host:        String,
		host_header: String,
		port:        u16,
		path:        String,
	}

	/// HTTP page source used by lazy restore to fetch snapshot RAM on first
	/// touch.
	#[derive(Clone)]
	pub struct RemotePageSource {
		endpoint: HttpEndpoint,
		token:    String,
	}

	impl RemotePageSource {
		pub fn new(base_url: &str, token: String) -> Result<Self> {
			Ok(Self { endpoint: parse_http_endpoint(base_url)?, token })
		}

		fn fetch_page(&self, page: u32, out: &mut [u8; PAGE_SIZE]) -> Result<()> {
			let mut stream = TcpStream::connect((self.endpoint.host.as_str(), self.endpoint.port))
				.map_err(|e| err(format!("connecting remote page source: {e}")))?;
			let timeout = Some(Duration::from_secs(10));
			stream
				.set_read_timeout(timeout)
				.map_err(|e| err(format!("setting remote page read timeout: {e}")))?;
			stream
				.set_write_timeout(timeout)
				.map_err(|e| err(format!("setting remote page write timeout: {e}")))?;
			let path = if self.endpoint.path.is_empty() {
				format!("/{page}")
			} else {
				format!("{}/{page}", self.endpoint.path)
			};
			let request = format!(
				"GET {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nX-Vmon-Mesh-Hop: \
				 1\r\nConnection: close\r\n\r\n",
				self.endpoint.host_header, self.token
			);
			stream
				.write_all(request.as_bytes())
				.map_err(|e| err(format!("requesting remote page {page}: {e}")))?;
			let mut response = Vec::with_capacity(PAGE_SIZE + 512);
			stream
				.read_to_end(&mut response)
				.map_err(|e| err(format!("reading remote page {page}: {e}")))?;
			let split = response
				.windows(4)
				.position(|w| w == b"\r\n\r\n")
				.ok_or_else(|| err("remote page response has no HTTP header terminator"))?;
			let head = std::str::from_utf8(&response[..split])
				.map_err(|e| err(format!("remote page response header is not UTF-8: {e}")))?;
			let status = head.lines().next().unwrap_or_default();
			if !status.contains(" 200 ") {
				return Err(err(format!("remote page source returned {status}")));
			}
			let body = &response[split + 4..];
			if body.len() != PAGE_SIZE {
				return Err(err(format!(
					"remote page source returned {} bytes, expected {PAGE_SIZE}",
					body.len()
				)));
			}
			out.copy_from_slice(body);
			Ok(())
		}
	}

	/// Shutdown hook fired when lazy remote memory cannot supply a faulting
	/// page.
	#[derive(Clone)]
	pub struct PagerFatal {
		gate:        Arc<PauseGate>,
		exit_reason: Arc<AtomicU8>,
		exit_code:   u8,
	}

	impl PagerFatal {
		pub const fn new(gate: Arc<PauseGate>, exit_reason: Arc<AtomicU8>, exit_code: u8) -> Self {
			Self { gate, exit_reason, exit_code }
		}
	}

	fn parse_http_endpoint(url: &str) -> Result<HttpEndpoint> {
		let rest = url
			.strip_prefix("http://")
			.ok_or_else(|| err("remote page-in supports only http:// mesh URLs"))?;
		let (authority, raw_path) = rest.split_once('/').unwrap_or((rest, ""));
		if authority.is_empty() {
			return Err(err("remote page URL is missing a host"));
		}
		let (host, port, host_header) = parse_authority(authority)?;
		let trimmed_path = raw_path.trim_matches('/');
		let path = if trimmed_path.is_empty() {
			String::new()
		} else {
			format!("/{trimmed_path}")
		};
		Ok(HttpEndpoint { host, host_header, port, path })
	}

	fn parse_authority(authority: &str) -> Result<(String, u16, String)> {
		if let Some(rest) = authority.strip_prefix('[') {
			let Some((host, tail)) = rest.split_once(']') else {
				return Err(err("remote page URL has an unterminated IPv6 host"));
			};
			let port = if let Some(port) = tail.strip_prefix(':') {
				parse_port(port)?
			} else if tail.is_empty() {
				80
			} else {
				return Err(err("remote page URL has invalid IPv6 authority"));
			};
			return Ok((host.to_string(), port, authority.to_string()));
		}
		if let Some((host, port)) = authority.rsplit_once(':')
			&& !host.is_empty()
			&& port.chars().all(|ch| ch.is_ascii_digit())
		{
			return Ok((host.to_string(), parse_port(port)?, authority.to_string()));
		}
		Ok((authority.to_string(), 80, authority.to_string()))
	}

	fn parse_port(port: &str) -> Result<u16> {
		port
			.parse::<u16>()
			.map_err(|e| err(format!("remote page URL has invalid port {port:?}: {e}")))
	}

	pub struct Pager {
		uffd:            OwnedFd,
		stop_evt:        OwnedFd,
		regions:         Vec<PagerRegion>,
		total_pages:     usize,
		target_pages:    usize,
		store_max_bytes: usize,
		swap:            Mutex<SwapAlloc>,
		shards:          Vec<Mutex<HashMap<u32, Loc>>>,
		evicted:         Vec<AtomicU64>,
		referenced:      Vec<AtomicU64>,
		resident_pages:  AtomicUsize,
		store_bytes:     AtomicUsize,
		clock_hand:      AtomicUsize,
		registered:      AtomicBool,
		disabled:        AtomicBool,
		remote_source:   Option<RemotePageSource>,
		fatal:           Option<PagerFatal>,
	}

	#[allow(
		clippy::non_send_fields_in_send_ty,
		reason = "raw region pointers reference guest memory mappings owned for the Pager lifetime"
	)]
	// SAFETY: PagerRegion raw pointers point into GuestMemoryMmap mappings owned by
	// Vmm. Vmm holds that memory for at least as long as the Pager and stops the
	// handler before drop.
	unsafe impl Send for Pager {}
	// SAFETY: same as `Send`; shared access is synchronized through atomics and
	// mutexes, and raw mapping pointers remain valid for the pager lifetime.
	unsafe impl Sync for Pager {}

	static ZERO_PAGE: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

	pub fn create_uffd() -> Result<OwnedFd> {
		let mut last_error: Option<io::Error> = None;
		for features in [UFFD_FEATURE_MISSING_SHMEM, 0] {
			let fd = match raw_uffd() {
				Ok(fd) => fd,
				Err(e) if e.raw_os_error() == Some(libc::EPERM) => {
					return Err(err(
						"userfaultfd denied: run vmon as root or set sysctl \
						 vm.unprivileged_userfaultfd=1",
					));
				},
				Err(e) => return Err(err(format!("userfaultfd: {e}"))),
			};
			let mut api = UffdioApi { api: UFFD_API, features, ioctls: 0 };
			// SAFETY: `fd` is a live userfaultfd, and `api` points to writable
			// storage matching the UFFDIO_API ABI for the duration of the ioctl.
			let rc = unsafe {
				libc::ioctl(
					fd.as_raw_fd(),
					UFFDIO_API_IOCTL as IoctlRequest,
					&mut api as *mut UffdioApi,
				)
			};
			if rc == 0 {
				return Ok(fd);
			}
			let e = io::Error::last_os_error();
			if e.raw_os_error() == Some(libc::EINVAL) {
				last_error = Some(e);
				continue;
			}
			return Err(err(format!("UFFDIO_API: {e}")));
		}
		Err(err(format!("UFFDIO_API: {}", last_error.unwrap_or_else(io::Error::last_os_error))))
	}

	fn raw_uffd() -> io::Result<OwnedFd> {
		let flags = libc::O_CLOEXEC | libc::O_NONBLOCK;
		// SAFETY: `syscall` is invoked with the userfaultfd number and plain
		// integer flags; on success it returns a new owned file descriptor.
		let fd = unsafe { libc::syscall(libc::SYS_userfaultfd, flags as libc::c_int) };
		if fd < 0 {
			Err(io::Error::last_os_error())
		} else {
			// SAFETY: `fd` was just returned by `userfaultfd` and is not owned
			// by any Rust value yet.
			Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
		}
	}

	pub fn open_swap_file(path: Option<&Path>) -> Result<OwnedFd> {
		if let Some(path) = path {
			let file = OpenOptions::new()
				.read(true)
				.write(true)
				.create(true)
				.truncate(true)
				.mode(0o600)
				.open(path)
				.map_err(|e| err(format!("opening zram swap file {}: {e}", path.display())))?;
			return Ok(file.into());
		}

		let dir = std::env::var_os("TMPDIR").map_or_else(|| PathBuf::from("/tmp"), PathBuf::from);
		match OpenOptions::new()
			.read(true)
			.write(true)
			.custom_flags(libc::O_TMPFILE | libc::O_CLOEXEC)
			.mode(0o600)
			.open(&dir)
		{
			Ok(file) => Ok(file.into()),
			Err(e)
				if matches!(e.raw_os_error(), Some(libc::EOPNOTSUPP | libc::EISDIR | libc::EINVAL)) =>
			{
				open_unlinked_swap_file(&dir)
			},
			Err(e) => Err(err(format!("creating anonymous zram swap file in {}: {e}", dir.display()))),
		}
	}

	fn open_unlinked_swap_file(dir: &Path) -> Result<OwnedFd> {
		let path = dir.join(format!("vmon-swap.{}", std::process::id()));
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.mode(0o600)
			.open(&path)
			.map_err(|e| err(format!("creating zram swap file {}: {e}", path.display())))?;
		if let Err(e) = std::fs::remove_file(&path) {
			return Err(err(format!("unlinking zram swap file {}: {e}", path.display())));
		}
		Ok(file.into())
	}

	pub fn register_missing(uffd: RawFd, start: u64, len: u64) -> Result<()> {
		let mut reg = UffdioRegister {
			range:  UffdioRange { start, len },
			mode:   UFFDIO_REGISTER_MODE_MISSING,
			ioctls: 0,
		};
		// SAFETY: `uffd` is a userfaultfd, and `reg` points to initialized,
		// writable storage matching the UFFDIO_REGISTER ABI.
		let rc = unsafe {
			libc::ioctl(uffd, UFFDIO_REGISTER_IOCTL as IoctlRequest, &mut reg as *mut UffdioRegister)
		};
		if rc < 0 {
			return Err(err(format!("UFFDIO_REGISTER: {}", io::Error::last_os_error())));
		}
		if reg.ioctls & UFFD_REGISTER_COPY_BIT == 0 {
			return Err(err("kernel uffd lacks copy support for registered range"));
		}
		Ok(())
	}

	fn uffd_copy(uffd: RawFd, dst_page_va: u64, src: *const u8, len: usize) -> io::Result<()> {
		let mut copy =
			UffdioCopy { dst: dst_page_va, src: src as u64, len: len as u64, mode: 0, copy: 0 };
		// SAFETY: `uffd` is a userfaultfd; `copy` points to writable storage
		// matching the UFFDIO_COPY ABI, and `src` names at least `len` bytes.
		let rc = unsafe {
			libc::ioctl(uffd, UFFDIO_COPY_IOCTL as IoctlRequest, &mut copy as *mut UffdioCopy)
		};
		if rc < 0 {
			return Err(io::Error::last_os_error());
		}
		if copy.copy < 0 {
			return Err(io::Error::from_raw_os_error((-copy.copy) as i32));
		}
		if copy.copy as u64 != len as u64 {
			return Err(io::Error::new(
				io::ErrorKind::WriteZero,
				format!("UFFDIO_COPY copied {} bytes, expected {len}", copy.copy),
			));
		}
		Ok(())
	}

	fn encode(page: &[u8; PAGE_SIZE]) -> Option<Vec<u8>> {
		if page.iter().all(|b| *b == 0) {
			return None;
		}
		let compressed = miniz_oxide::deflate::compress_to_vec(page, 6);
		if compressed.len() < PAGE_SIZE {
			let mut out = Vec::with_capacity(compressed.len() + 1);
			out.push(2);
			out.extend_from_slice(&compressed);
			Some(out)
		} else {
			let mut out = Vec::with_capacity(PAGE_SIZE + 1);
			out.push(1);
			out.extend_from_slice(page);
			Some(out)
		}
	}

	fn decode(buf: &[u8], out: &mut [u8; PAGE_SIZE]) -> Result<()> {
		let Some((&tag, payload)) = buf.split_first() else {
			return Err(err("empty pager page blob"));
		};
		match tag {
			1 => {
				if payload.len() != PAGE_SIZE {
					return Err(err(format!(
						"raw pager page has {} bytes, expected {PAGE_SIZE}",
						payload.len()
					)));
				}
				out.copy_from_slice(payload);
			},
			2 => {
				let decoded = miniz_oxide::inflate::decompress_to_vec_with_limit(payload, PAGE_SIZE)
					.map_err(|e| err(format!("inflating pager page: {e:?}")))?;
				if decoded.len() != PAGE_SIZE {
					return Err(err(format!(
						"inflated pager page has {} bytes, expected {PAGE_SIZE}",
						decoded.len()
					)));
				}
				out.copy_from_slice(&decoded);
			},
			other => return Err(err(format!("invalid pager page tag {other}"))),
		}
		Ok(())
	}

	fn collect_regions(mem: &GuestMemoryMmap) -> Result<(Vec<PagerRegion>, usize)> {
		let mut regions = Vec::new();
		let mut total_pages = 0usize;
		for region in mem.iter() {
			let len =
				usize::try_from(region.len()).map_err(|_| err("pager region length exceeds usize"))?;
			if len == 0 {
				return Err(err("pager memory region is empty"));
			}
			if !len.is_multiple_of(PAGE_SIZE) {
				return Err(err(format!("pager region length {len} is not page-aligned")));
			}
			let fo = region
				.file_offset()
				.ok_or_else(|| err("pager requires file-backed guest memory"))?;
			let memfd = fo
				.file()
				.try_clone()
				.map_err(|e| err(format!("cloning guest memory fd for pager: {e}")))?;
			regions.push(PagerRegion {
				base: region.as_ptr(),
				gpa: region.start_addr().raw_value(),
				len,
				memfd,
				foff: fo.start(),
				page_start: total_pages,
			});
			total_pages = total_pages
				.checked_add(len / PAGE_SIZE)
				.ok_or_else(|| err("pager page count overflow"))?;
		}
		if total_pages == 0 {
			return Err(err("pager guest memory has no pages"));
		}
		Ok((regions, total_pages))
	}

	fn new_pager(
		uffd: OwnedFd,
		swap_fd: OwnedFd,
		regions: Vec<PagerRegion>,
		total_pages: usize,
		target_pages: usize,
		store_max_bytes: usize,
		resident_pages: usize,
		remote_source: Option<RemotePageSource>,
		fatal: Option<PagerFatal>,
	) -> Result<Arc<Pager>> {
		let cap = u32::try_from(total_pages)
			.map_err(|_| err("pager supports at most u32::MAX guest pages"))?;
		// SAFETY: `eventfd` has no Rust-side preconditions; on success it
		// returns a new owned file descriptor.
		let stop_fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
		if stop_fd < 0 {
			return Err(io::Error::last_os_error().into());
		}
		let words = total_pages.div_ceil(64);
		let mut shards = Vec::with_capacity(SHARDS);
		for _ in 0..SHARDS {
			shards.push(Mutex::new(HashMap::new()));
		}
		// SAFETY: `stop_fd` was just returned by `eventfd` and is not owned
		// by any Rust value yet.
		let stop_evt = unsafe { OwnedFd::from_raw_fd(stop_fd) };
		Ok(Arc::new(Pager {
			uffd,
			stop_evt,
			regions,
			total_pages,
			target_pages,
			store_max_bytes,
			swap: Mutex::new(SwapAlloc { fd: swap_fd, next: 0, free: Vec::new(), cap }),
			shards,
			evicted: (0..words).map(|_| AtomicU64::new(0)).collect(),
			referenced: (0..words).map(|_| AtomicU64::new(0)).collect(),
			resident_pages: AtomicUsize::new(resident_pages),
			store_bytes: AtomicUsize::new(0),
			clock_hand: AtomicUsize::new(0),
			registered: AtomicBool::new(false),
			disabled: AtomicBool::new(false),
			remote_source,
			fatal,
		}))
	}
	impl Pager {
		pub fn new(
			uffd: OwnedFd,
			swap_fd: OwnedFd,
			mem: &GuestMemoryMmap,
			target_pages: usize,
			store_max_bytes: usize,
		) -> Result<Arc<Self>> {
			let (regions, total_pages) = collect_regions(mem)?;
			new_pager(
				uffd,
				swap_fd,
				regions,
				total_pages,
				target_pages,
				store_max_bytes,
				total_pages,
				None,
				None,
			)
		}

		pub fn new_remote(
			uffd: OwnedFd,
			swap_fd: OwnedFd,
			mem: &GuestMemoryMmap,
			source: RemotePageSource,
			fatal: PagerFatal,
		) -> Result<Arc<Self>> {
			let (regions, total_pages) = collect_regions(mem)?;
			let pager = new_pager(
				uffd,
				swap_fd,
				regions,
				total_pages,
				total_pages,
				0,
				0,
				Some(source),
				Some(fatal),
			)?;
			pager.seed_remote_pages()?;
			pager.register_all_missing()?;
			Ok(pager)
		}

		pub fn handler_loop(self: Arc<Self>) {
			let uffd = self.uffd.as_raw_fd();
			let stop = self.stop_evt.as_raw_fd();
			loop {
				let mut fds =
					[libc::pollfd { fd: uffd, events: libc::POLLIN, revents: 0 }, libc::pollfd {
						fd:      stop,
						events:  libc::POLLIN,
						revents: 0,
					}];
				// SAFETY: `fds` points to `pollfd` entries valid for the call;
				// null timeout and signal mask request an indefinite wait.
				let rc = unsafe {
					libc::ppoll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, ptr::null(), ptr::null())
				};
				if rc < 0 {
					let e = io::Error::last_os_error();
					if e.raw_os_error() == Some(libc::EINTR) {
						continue;
					}
					warn!("pager ppoll failed: {e}");
					return;
				}
				if fds[1].revents & libc::POLLIN != 0 {
					drain_eventfd(stop);
					return;
				}
				if fds[0].revents & libc::POLLIN == 0 {
					continue;
				}
				loop {
					let mut msg = mem::MaybeUninit::<UffdMsg>::zeroed();
					// SAFETY: `msg` points to enough uninitialized storage for one
					// `UffdMsg`, and `uffd` is opened O_NONBLOCK.
					let n = unsafe {
						libc::read(uffd, msg.as_mut_ptr() as *mut libc::c_void, mem::size_of::<UffdMsg>())
					};
					if n < 0 {
						let e = io::Error::last_os_error();
						if e.raw_os_error() == Some(libc::EAGAIN) {
							break;
						}
						if e.raw_os_error() == Some(libc::EINTR) {
							continue;
						}
						warn!("pager uffd read failed: {e}");
						break;
					}
					if n != mem::size_of::<UffdMsg>() as isize {
						warn!("pager uffd short read: {n}");
						break;
					}
					// SAFETY: the preceding read filled exactly one `UffdMsg`.
					let msg = unsafe { msg.assume_init() };
					if msg.event == UFFD_EVENT_PAGEFAULT {
						self.serve(msg.pf_address);
					}
				}
			}
		}

		fn serve(&self, addr: u64) {
			let page_va = addr & !((PAGE_SIZE as u64) - 1);
			let Some(idx) = self.page_index_for_va(page_va as usize) else {
				self.disable_once(format!("pager fault outside guest RAM at {page_va:#x}"));
				return;
			};
			let shard_idx = idx % SHARDS;
			let mut guard = self.shards[shard_idx].lock();
			if !self.bit_is_set(&self.evicted, idx) {
				drop(guard);
				if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, ZERO_PAGE.as_ptr(), PAGE_SIZE)
				{
					self.disable_once(format!("zero-fill UFFDIO_COPY failed: {e}"));
					return;
				}
				self.set_bit(&self.referenced, idx);
				crate::metrics::record_pager_fault_in();
				return;
			}

			let Some(loc) = guard.remove(&(idx as u32)) else {
				drop(guard);
				self.disable_once(format!("pager missing backing blob for page {idx}"));
				if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, ZERO_PAGE.as_ptr(), PAGE_SIZE)
				{
					self.disable_once(format!("zero-fill missing pager blob failed: {e}"));
					return;
				}
				self.clear_bit(&self.evicted, idx);
				self.set_bit(&self.referenced, idx);
				self.resident_pages.fetch_add(1, Ordering::SeqCst);
				crate::metrics::record_pager_fault_in();
				self.publish_gauges();
				return;
			};
			drop(guard);

			let mut tmp = [0u8; PAGE_SIZE];
			let src: *const u8 = match &loc {
				Loc::Zero => ZERO_PAGE.as_ptr(),
				Loc::Ram(buf) => {
					if let Err(e) = decode(buf, &mut tmp) {
						self.disable_once(format!("decoding pager RAM page {idx}: {e}"));
						ZERO_PAGE.as_ptr()
					} else {
						tmp.as_ptr()
					}
				},
				Loc::Swap { slot, len } => {
					let mut sbuf = vec![0u8; *len as usize];
					let fd = self.swap.lock().fd.as_raw_fd();
					if let Err(e) = pread_exact(fd, &mut sbuf, swap_offset(*slot)) {
						self.disable_once(format!("reading pager swap slot {slot}: {e}"));
						ZERO_PAGE.as_ptr()
					} else if let Err(e) = decode(&sbuf, &mut tmp) {
						self.disable_once(format!("decoding pager swap slot {slot}: {e}"));
						ZERO_PAGE.as_ptr()
					} else {
						tmp.as_ptr()
					}
				},
				Loc::Remote { page } => {
					let Some(source) = &self.remote_source else {
						self.fatal_remote_fault(
							page_va,
							format!("remote page fault for page {page} has no remote source"),
						);
						return;
					};
					if let Err(e) = source.fetch_page(*page, &mut tmp) {
						self.fatal_remote_fault(page_va, format!("fetching remote page {page}: {e}"));
						return;
					}
					tmp.as_ptr()
				},
			};

			if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, src, PAGE_SIZE) {
				self.shards[shard_idx].lock().insert(idx as u32, loc);
				self.disable_once(format!("fault-in UFFDIO_COPY failed: {e}"));
				return;
			}
			self.release_loc(loc);
			self.clear_bit(&self.evicted, idx);
			self.set_bit(&self.referenced, idx);
			self.resident_pages.fetch_add(1, Ordering::SeqCst);
			crate::metrics::record_pager_fault_in();
			self.publish_gauges();
		}

		pub fn evict_to_target(&self) {
			if self.disabled.load(Ordering::SeqCst) {
				return;
			}
			if let Err(e) = self.register_all_missing() {
				self.disable_once(e.to_string());
				return;
			}

			let mut budget = MAX_EVICT_PER_SWEEP;
			while self.resident_pages.load(Ordering::SeqCst) > self.target_pages && budget > 0 {
				let Some(idx) = self.next_victim() else {
					break;
				};
				self.evict_page(idx);
				budget -= 1;
				if self.disabled.load(Ordering::SeqCst) {
					break;
				}
			}
			self.publish_gauges();
		}

		fn fatal_remote_fault(&self, page_va: u64, message: String) {
			self.disable_once(message);
			if let Some(fatal) = &self.fatal {
				let _ = fatal.exit_reason.compare_exchange(
					0,
					fatal.exit_code,
					Ordering::SeqCst,
					Ordering::SeqCst,
				);
				fatal.gate.set_state(RunState::Stopping);
				fatal.gate.signal_all_vcpus();
			}
			if let Err(e) = uffd_copy(self.uffd.as_raw_fd(), page_va, ZERO_PAGE.as_ptr(), PAGE_SIZE) {
				self.disable_once(format!("zero-fill after remote page fault failed: {e}"));
			}
		}

		pub fn over_target(&self) -> bool {
			!self.disabled.load(Ordering::SeqCst)
				&& self.resident_pages.load(Ordering::SeqCst) > self.target_pages
		}

		pub fn request_stop(&self) {
			let one = 1u64.to_ne_bytes();
			while eventfd_write(self.stop_evt.as_raw_fd(), &one)
				.is_err_and(|e| e.raw_os_error() == Some(libc::EINTR))
			{}
		}

		fn next_victim(&self) -> Option<usize> {
			if self.total_pages == 0 {
				return None;
			}
			let start = self.clock_hand.load(Ordering::SeqCst) % self.total_pages;
			let mut first_resident = None;
			for step in 0..self.total_pages {
				let idx = (start + step) % self.total_pages;
				if self.bit_is_set(&self.evicted, idx) {
					continue;
				}
				if first_resident.is_none() {
					first_resident = Some(idx);
				}
				if self.bit_is_set(&self.referenced, idx) {
					self.clear_bit(&self.referenced, idx);
					continue;
				}
				self
					.clock_hand
					.store((idx + 1) % self.total_pages, Ordering::SeqCst);
				return Some(idx);
			}
			if let Some(idx) = first_resident {
				self
					.clock_hand
					.store((idx + 1) % self.total_pages, Ordering::SeqCst);
			}
			first_resident
		}

		fn evict_page(&self, idx: usize) {
			if self.bit_is_set(&self.evicted, idx) {
				return;
			}
			let Some((region_i, page_in_region)) = self.region_for_page(idx) else {
				self.disable_once(format!("pager page index {idx} outside regions"));
				return;
			};
			let region = &self.regions[region_i];
			let off = page_in_region * PAGE_SIZE;
			let mut page = [0u8; PAGE_SIZE];
			// SAFETY: `off` was derived from `idx` inside `region`; `page` is
			// exactly one page of writable stack storage.
			unsafe {
				ptr::copy_nonoverlapping(region.base.add(off), page.as_mut_ptr(), PAGE_SIZE);
			}
			let encoded = encode(&page);

			let shard_idx = idx % SHARDS;
			let mut guard = self.shards[shard_idx].lock();
			if self.bit_is_set(&self.evicted, idx) {
				return;
			}

			let loc = match encoded {
				None => Loc::Zero,
				Some(buf) => self.place_encoded(buf),
			};

			guard.insert(idx as u32, loc);
			self.set_bit(&self.evicted, idx);
			self.clear_bit(&self.referenced, idx);

			// SAFETY: `region.memfd` backs this mapping; offset and length name
			// the page selected above and fit in off_t on supported targets.
			let rc = unsafe {
				libc::fallocate(
					region.memfd.as_raw_fd(),
					libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
					(region.foff + off as u64) as libc::off_t,
					PAGE_SIZE as libc::off_t,
				)
			};
			if rc < 0 {
				let loc = guard.remove(&(idx as u32));
				self.clear_bit(&self.evicted, idx);
				drop(guard);
				if let Some(loc) = loc {
					self.release_loc(loc);
				}
				self.disable_once(format!(
					"punching guest RAM page {idx} (gpa {:#x}): {}",
					region.gpa + off as u64,
					io::Error::last_os_error()
				));
				return;
			}
			// SAFETY: the address range is the live page selected above from the
			// guest mapping; MADV_DONTNEED drops the present PTE after the backing
			// file hole is punched so the next access faults through userfaultfd.
			let rc = unsafe {
				libc::madvise(region.base.add(off) as *mut libc::c_void, PAGE_SIZE, libc::MADV_DONTNEED)
			};
			if rc < 0 {
				self.disable_once(format!(
					"dropping guest RAM PTE for page {idx} (gpa {:#x}): {}",
					region.gpa + off as u64,
					io::Error::last_os_error()
				));
			}

			self.resident_pages.fetch_sub(1, Ordering::SeqCst);
			crate::metrics::record_pager_eviction();
		}

		fn try_reserve_store(&self, len: usize) -> bool {
			self
				.store_bytes
				.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
					current
						.checked_add(len)
						.filter(|next| *next <= self.store_max_bytes)
				})
				.is_ok()
		}

		fn place_encoded(&self, buf: Vec<u8>) -> Loc {
			if self.try_reserve_store(buf.len()) {
				return Loc::Ram(buf.into_boxed_slice());
			}

			let mut swap = self.swap.lock();
			if let Some(slot) = swap.alloc() {
				match pwrite_all(swap.fd.as_raw_fd(), &buf, swap_offset(slot)) {
					Ok(()) => Loc::Swap { slot, len: buf.len() as u32 },
					Err(e) => {
						warn!("writing pager swap slot {slot} failed; keeping page in RAM: {e}");
						swap.free(slot);
						drop(swap);
						self.store_bytes.fetch_add(buf.len(), Ordering::SeqCst);
						Loc::Ram(buf.into_boxed_slice())
					},
				}
			} else {
				drop(swap);
				self.store_bytes.fetch_add(buf.len(), Ordering::SeqCst);
				Loc::Ram(buf.into_boxed_slice())
			}
		}

		fn release_loc(&self, loc: Loc) {
			match loc {
				Loc::Zero | Loc::Remote { .. } => {},
				Loc::Ram(buf) => {
					self.store_bytes.fetch_sub(buf.len(), Ordering::SeqCst);
				},
				Loc::Swap { slot, .. } => self.swap.lock().free(slot),
			}
		}

		fn page_index_for_va(&self, page_va: usize) -> Option<usize> {
			for region in &self.regions {
				let start = region.base as usize;
				let end = start.checked_add(region.len)?;
				if (start..end).contains(&page_va) {
					return Some(region.page_start + (page_va - start) / PAGE_SIZE);
				}
			}
			None
		}

		fn region_for_page(&self, idx: usize) -> Option<(usize, usize)> {
			for (i, region) in self.regions.iter().enumerate() {
				let pages = region.len / PAGE_SIZE;
				if idx >= region.page_start && idx < region.page_start + pages {
					return Some((i, idx - region.page_start));
				}
			}
			None
		}

		#[allow(clippy::unused_self, reason = "keeps bit helpers as Pager methods for API symmetry")]
		fn bit_is_set(&self, bits: &[AtomicU64], idx: usize) -> bool {
			let word = idx / 64;
			let bit = 1u64 << (idx % 64);
			bits[word].load(Ordering::SeqCst) & bit != 0
		}

		#[allow(clippy::unused_self, reason = "keeps bit helpers as Pager methods for API symmetry")]
		fn set_bit(&self, bits: &[AtomicU64], idx: usize) {
			let word = idx / 64;
			let bit = 1u64 << (idx % 64);
			bits[word].fetch_or(bit, Ordering::SeqCst);
		}

		#[allow(clippy::unused_self, reason = "keeps bit helpers as Pager methods for API symmetry")]
		fn clear_bit(&self, bits: &[AtomicU64], idx: usize) {
			let word = idx / 64;
			let bit = !(1u64 << (idx % 64));
			bits[word].fetch_and(bit, Ordering::SeqCst);
		}

		fn register_all_missing(&self) -> Result<()> {
			if self.registered.load(Ordering::SeqCst) {
				return Ok(());
			}
			for region in &self.regions {
				register_missing(self.uffd.as_raw_fd(), region.base as u64, region.len as u64)?;
			}
			self.registered.store(true, Ordering::SeqCst);
			Ok(())
		}

		fn seed_remote_pages(&self) -> Result<()> {
			for idx in 0..self.total_pages {
				let page = u32::try_from(idx).map_err(|_| err("remote page index exceeds u32"))?;
				self.shards[idx % SHARDS]
					.lock()
					.insert(page, Loc::Remote { page });
				self.set_bit(&self.evicted, idx);
			}
			self.publish_gauges();
			Ok(())
		}

		fn disable_once(&self, message: String) {
			if !self.disabled.swap(true, Ordering::SeqCst) {
				warn!("disabling pager: {message}");
			}
		}

		fn publish_gauges(&self) {
			crate::metrics::set_pager_gauges(
				self.resident_pages.load(Ordering::SeqCst),
				self.store_bytes.load(Ordering::SeqCst),
				self.swap.lock().used_pages(),
			);
		}

		#[cfg(test)]
		fn register_for_test(&self) -> Result<()> {
			self.register_all_missing()
		}
	}

	fn swap_offset(slot: u32) -> i64 {
		i64::from(slot) * SWAP_SLOT_SIZE as i64
	}

	fn drain_eventfd(fd: RawFd) {
		let mut buf = [0u8; 8];
		while eventfd_read(fd, &mut buf).is_err_and(|e| e.raw_os_error() == Some(libc::EINTR)) {}
	}

	fn eventfd_read(fd: RawFd, buf: &mut [u8; 8]) -> io::Result<()> {
		// SAFETY: `buf` is exactly the 8-byte eventfd counter size and is valid
		// for writes; `fd` is expected to be an eventfd owned by the pager.
		let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
		if n < 0 {
			return Err(io::Error::last_os_error());
		}
		Ok(())
	}

	fn eventfd_write(fd: RawFd, buf: &[u8; 8]) -> io::Result<()> {
		// SAFETY: `buf` is exactly the 8-byte eventfd counter size and is valid
		// for reads; `fd` is expected to be an eventfd owned by the pager.
		let n = unsafe { libc::write(fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
		if n < 0 {
			return Err(io::Error::last_os_error());
		}
		Ok(())
	}

	fn pread_exact(fd: RawFd, mut buf: &mut [u8], mut offset: i64) -> io::Result<()> {
		while !buf.is_empty() {
			// SAFETY: `buf` is valid writable memory, `fd` is an open file
			// descriptor, and `offset` is maintained within the requested range.
			let n = unsafe {
				libc::pread(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), offset as libc::off_t)
			};
			if n < 0 {
				let e = io::Error::last_os_error();
				if e.raw_os_error() == Some(libc::EINTR) {
					continue;
				}
				return Err(e);
			}
			if n == 0 {
				return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read from pager swap"));
			}
			let n = n as usize;
			let tmp = buf;
			buf = &mut tmp[n..];
			offset += n as i64;
		}
		Ok(())
	}

	fn pwrite_all(fd: RawFd, mut buf: &[u8], mut offset: i64) -> io::Result<()> {
		while !buf.is_empty() {
			// SAFETY: `buf` is valid readable memory, `fd` is an open file
			// descriptor, and `offset` is maintained within the requested range.
			let n = unsafe {
				libc::pwrite(fd, buf.as_ptr() as *const libc::c_void, buf.len(), offset as libc::off_t)
			};
			if n < 0 {
				let e = io::Error::last_os_error();
				if e.raw_os_error() == Some(libc::EINTR) {
					continue;
				}
				return Err(e);
			}
			if n == 0 {
				return Err(io::Error::new(io::ErrorKind::WriteZero, "short write to pager swap"));
			}
			let n = n as usize;
			buf = &buf[n..];
			offset += n as i64;
		}
		Ok(())
	}

	#[cfg(test)]
	mod tests {
		use std::{
			io::{Read, Write},
			net::TcpListener,
			sync::atomic::AtomicU8,
			thread::{self, JoinHandle},
			time::{Duration, Instant},
		};

		use super::*;

		struct Fixture {
			mem:     GuestMemoryMmap,
			pager:   Arc<Pager>,
			handler: Option<JoinHandle<()>>,
		}

		impl Fixture {
			fn new(target_pages: usize, store_max_bytes: usize) -> Option<Self> {
				let uffd = match create_uffd() {
					Ok(uffd) => uffd,
					Err(e) if e.to_string().contains("userfaultfd denied") => {
						eprintln!("skipping pager userfaultfd test: {e}");
						return None;
					},
					Err(e) => panic!("create userfaultfd for pager test: {e}"),
				};
				let swap = open_swap_file(None).expect("open swap file");
				let mem = crate::memory::create_guest_memory(2 << 20).expect("guest memory");
				let pager =
					Pager::new(uffd, swap, &mem, target_pages, store_max_bytes).expect("create pager");
				pager.register_for_test().expect("register missing faults");
				let p = pager.clone();
				let handler = thread::Builder::new()
					.name("pager-test".into())
					.spawn(move || p.handler_loop())
					.expect("spawn pager handler");
				Some(Self { mem, pager, handler: Some(handler) })
			}

			fn new_remote(url: &str) -> Option<Self> {
				let uffd = match create_uffd() {
					Ok(uffd) => uffd,
					Err(e) if e.to_string().contains("userfaultfd denied") => {
						eprintln!("skipping pager userfaultfd test: {e}");
						return None;
					},
					Err(e) => panic!("create userfaultfd for remote pager test: {e}"),
				};
				let swap = open_swap_file(None).expect("open swap file");
				let mem = crate::memory::create_guest_memory(2 << 20).expect("guest memory");
				let source = RemotePageSource::new(url, "secret".to_string()).expect("remote source");
				let gate = crate::control::PauseGate::new(1);
				let fatal = PagerFatal::new(gate, Arc::new(AtomicU8::new(0)), 4);
				let pager = Pager::new_remote(uffd, swap, &mem, source, fatal).expect("remote pager");
				let p = pager.clone();
				let handler = thread::Builder::new()
					.name("pager-remote-test".into())
					.spawn(move || p.handler_loop())
					.expect("spawn pager handler");
				Some(Self { mem, pager, handler: Some(handler) })
			}

			fn page_ptr(&self, page: usize) -> *mut u8 {
				self
					.mem
					.iter()
					.next()
					.expect("memory region")
					.as_ptr()
					.wrapping_add(page * PAGE_SIZE)
			}

			fn write_page(&self, page: usize, bytes: &[u8; PAGE_SIZE]) {
				// SAFETY: `page_ptr` returns a page inside the fixture's live guest
				// memory mapping, and `bytes` is exactly one page.
				unsafe {
					ptr::copy_nonoverlapping(bytes.as_ptr(), self.page_ptr(page), PAGE_SIZE);
				}
			}

			fn read_page(&self, page: usize) -> [u8; PAGE_SIZE] {
				let mut out = [0u8; PAGE_SIZE];
				// SAFETY: `page_ptr` returns a page inside the fixture's live guest
				// memory mapping, and `out` is exactly one page.
				unsafe {
					ptr::copy_nonoverlapping(self.page_ptr(page), out.as_mut_ptr(), PAGE_SIZE);
				}
				out
			}

			fn wait_resident(&self, page: usize) {
				let deadline = Instant::now() + Duration::from_secs(1);
				while self.pager.bit_is_set(&self.pager.evicted, page) && Instant::now() < deadline {
					thread::sleep(Duration::from_millis(1));
				}
				assert!(!self.pager.bit_is_set(&self.pager.evicted, page));
			}
		}

		impl Drop for Fixture {
			fn drop(&mut self) {
				self.pager.request_stop();
				if let Some(handler) = self.handler.take() {
					let _ = handler.join();
				}
			}
		}

		fn noisy_page() -> [u8; PAGE_SIZE] {
			let mut noisy = [0u8; PAGE_SIZE];
			let mut x = 0x1234_5678u64;
			for chunk in noisy.chunks_mut(8) {
				x ^= x << 13;
				x ^= x >> 7;
				x ^= x << 17;
				chunk.copy_from_slice(&x.to_ne_bytes());
			}
			noisy
		}

		fn one_page_server(page: &[u8; PAGE_SIZE], expected_page: u32) -> (String, JoinHandle<()>) {
			let page = *page;
			let listener = TcpListener::bind("127.0.0.1:0").expect("bind page server");
			let addr = listener.local_addr().expect("page server address");
			let handle = thread::spawn(move || {
				let (mut stream, _) = listener.accept().expect("accept page request");
				let mut req = [0u8; 1024];
				let n = stream.read(&mut req).expect("read page request");
				let req = std::str::from_utf8(&req[..n]).expect("request utf8");
				assert!(req.starts_with(&format!("GET /pages/{expected_page} HTTP/1.1")));
				assert!(req.contains("Authorization: Bearer secret"));
				stream
					.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\n\r\n")
					.expect("write response header");
				stream.write_all(&page).expect("write page");
			});
			(format!("http://{addr}/pages"), handle)
		}

		#[test]
		fn encode_decode_roundtrip_variants() {
			let zero = [0u8; PAGE_SIZE];
			assert!(encode(&zero).is_none());

			let repeated = [0xabu8; PAGE_SIZE];
			let compressed = encode(&repeated).expect("compressed blob");
			assert_eq!(compressed[0], 2);
			let mut out = [0u8; PAGE_SIZE];
			decode(&compressed, &mut out).expect("decode compressed");
			assert_eq!(out, repeated);

			let noisy = noisy_page();
			let raw = encode(&noisy).expect("raw blob");
			assert_eq!(raw[0], 1);
			decode(&raw, &mut out).expect("decode raw");
			assert_eq!(out, noisy);
		}

		#[test]
		fn remote_page_source_fetches_page_over_http() {
			let page = noisy_page();
			let (url, handle) = one_page_server(&page, 7);
			let source = RemotePageSource::new(&url, "secret".to_string()).expect("remote source");
			let mut out = [0u8; PAGE_SIZE];
			source.fetch_page(7, &mut out).expect("fetch page");
			assert_eq!(out, page);
			handle.join().expect("page server");
		}

		#[test]
		fn remote_pager_faults_in_source_page() {
			let page = noisy_page();
			let (url, handle) = one_page_server(&page, 0);
			let Some(f) = Fixture::new_remote(&url) else {
				return;
			};
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
			handle.join().expect("page server");
		}

		#[test]
		fn evict_faults_in_compressible_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page = [0xabu8; PAGE_SIZE];
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_zero_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), [0u8; PAGE_SIZE]);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_incompressible_page() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page = noisy_page();
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.bit_is_set(&f.pager.evicted, 0));
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_faults_in_swap_spill_page() {
			let Some(f) = Fixture::new(511, 0) else {
				return;
			};
			let page = noisy_page();
			f.write_page(0, &page);
			f.pager.evict_page(0);
			assert!(f.pager.swap.lock().used_pages() > 0);
			assert_eq!(f.read_page(0), page);
			f.wait_resident(0);
		}

		#[test]
		fn evict_to_target_prefers_unreferenced_pages() {
			let Some(f) = Fixture::new(511, PAGE_SIZE * 8) else {
				return;
			};
			let page0 = [0x11u8; PAGE_SIZE];
			let page1 = [0x22u8; PAGE_SIZE];
			f.write_page(0, &page0);
			f.write_page(1, &page1);
			f.pager.set_bit(&f.pager.referenced, 0);
			f.pager.clear_bit(&f.pager.referenced, 1);
			f.pager.evict_to_target();
			assert!(!f.pager.bit_is_set(&f.pager.evicted, 0));
			assert!(f.pager.bit_is_set(&f.pager.evicted, 1));
			assert_eq!(f.pager.resident_pages.load(Ordering::SeqCst), 511);
		}
	}
}

#[cfg(target_os = "linux")]
pub use linux::{Pager, PagerFatal, RemotePageSource, create_uffd, open_swap_file};

#[cfg(not(target_os = "linux"))]
mod non_linux {
	use std::{
		path::Path,
		sync::{Arc, atomic::AtomicU8},
	};

	use crate::{
		control::PauseGate,
		memory::GuestMemoryMmap,
		result::{Result, err},
	};

	pub struct Pager;

	pub fn create_uffd() -> Result<std::os::fd::OwnedFd> {
		Err(err("pager requires Linux"))
	}

	pub fn open_swap_file(_path: Option<&Path>) -> Result<std::os::fd::OwnedFd> {
		Err(err("pager requires Linux"))
	}

	pub struct RemotePageSource;

	impl RemotePageSource {
		pub fn new(_base_url: &str, _token: String) -> Result<Self> {
			Err(err("pager requires Linux"))
		}
	}

	pub struct PagerFatal;

	impl PagerFatal {
		pub fn new(_gate: Arc<PauseGate>, _exit_reason: Arc<AtomicU8>, _exit_code: u8) -> Self {
			Self
		}
	}

	impl Pager {
		pub fn new(
			_uffd: std::os::fd::OwnedFd,
			_swap_fd: std::os::fd::OwnedFd,
			_mem: &GuestMemoryMmap,
			_target_pages: usize,
			_store_max_bytes: usize,
		) -> Result<Arc<Self>> {
			Err(err("pager requires Linux"))
		}

		pub fn new_remote(
			_uffd: std::os::fd::OwnedFd,
			_swap_fd: std::os::fd::OwnedFd,
			_mem: &GuestMemoryMmap,
			_source: RemotePageSource,
			_fatal: PagerFatal,
		) -> Result<Arc<Self>> {
			Err(err("pager requires Linux"))
		}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub fn handler_loop(self: Arc<Self>) {}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn evict_to_target(&self) {}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn over_target(&self) -> bool {
			false
		}

		#[expect(
			clippy::unused_self,
			reason = "non-Linux Pager preserves the Linux instance-shaped platform API"
		)]
		pub const fn request_stop(&self) {}
	}
}

#[cfg(not(target_os = "linux"))]
pub use non_linux::{Pager, PagerFatal, RemotePageSource, create_uffd, open_swap_file};
