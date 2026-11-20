//! ovnfs call device: the host side of the ovnfs guest kernel client.
//!
//! Implements the call ABI of ovnfs's `kmod/ovnfs_wire.h`. One 4 KiB MMIO page:
//! `INFO` identifies the device and each port is one doorbell register. A
//! 64-bit doorbell write carries the guest-physical address of a
//! `struct ovnfs_call`; the trapping vCPU thread sends the request segments to
//! the ovnfs daemon as one length-prefixed frame (vectored, straight from guest
//! RAM), reads the reply frame straight into the reply segments and writes
//! `status`/`reply_len` back before the guest resumes. Requests and replies are
//! opaque bytes here.
//!
//! Every port is its own bus device, so it has its own lock and its own daemon
//! connection: a call on one port never waits for a call on another. The device
//! keeps no snapshot state; it is rebuilt from `--ovnfs` on boot, restore and
//! fork.

use std::{
	io,
	os::{
		fd::{AsRawFd, RawFd},
		unix::net::UnixStream,
	},
	path::Path,
	sync::Arc,
	time::{Duration, Instant},
};

use parking_lot::Mutex;
use tracing::warn;
use vm_memory::{Address, Bytes, GuestAddress, GuestMemory};

use crate::{
	devices::{Bus, BusDevice},
	memory::GuestMemoryMmap,
};

/// Size of the call page (`OVNFS_MMIO_SIZE`).
const MMIO_SIZE: u64 = 0x1000;
/// `OVNFS_CALL_MAGIC`: "VNFS".
const CALL_MAGIC: u64 = 0x5346_4e56;
/// `OVNFS_CALL_VERSION`.
const CALL_VERSION: u64 = 1;
/// `OVNFS_REG_INFO`: magic, version and port count, read as 64 bits.
const REG_INFO: u64 = 0x000;
/// `OVNFS_REG_DOORBELL`: port `n` is the 64-bit register at `+ 8 * n`.
const REG_DOORBELL: u64 = 0x100;
/// Width of one doorbell register.
const DOORBELL_SIZE: u64 = 8;
/// `OVNFS_MAX_PORTS`: as many doorbells as fit in the page.
const MAX_PORTS: usize = ((MMIO_SIZE - REG_DOORBELL) / DOORBELL_SIZE) as usize;
/// `OVNFS_MAX_FRAME`: largest request or reply (`OVNFS_MAX_IO` + headers).
const MAX_FRAME: usize = (1 << 20) + 4096;
/// `OVNFS_MAX_SEGS`: most segments of one call.
const MAX_SEGS: usize = 260;
/// `sizeof(struct ovnfs_call)` without its segments.
const CALL_HEADER_SIZE: usize = 16;
/// `sizeof(struct ovnfs_seg)`.
const SEG_SIZE: usize = 16;
/// The `__le32` byte count in front of every frame.
const FRAME_PREFIX_SIZE: usize = 4;
/// Longest a single send or receive on a daemon connection may block, so a
/// hung daemon cannot hold a vCPU (and with it a pause) forever.
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// Scratch buffer for discarding a reply that does not fit the call.
const DRAIN_CHUNK: usize = 16 * 1024;
/// Socket buffer size first asked for per direction: a whole `OVNFS_MAX_FRAME`
/// with room to spare, so a 1 MiB payload moves in one transfer instead of the
/// macOS `AF_UNIX` default of 8 KiB pieces.
const SOCKET_BUFFER_MAX: usize = 2 << 20;
/// Smallest socket buffer size tried before keeping the system default.
const SOCKET_BUFFER_MIN: usize = 256 << 10;
/// How long a call polls for the first reply byte before the vCPU thread
/// sleeps in a blocking receive. Small replies usually arrive within a few
/// microseconds, and polling saves the kernel sleep and wake-up a blocking
/// receive pays for each. The vCPU is stalled until the reply either way, so
/// spinning costs host CPU only while a call is in flight, at most this long
/// per call; replies slower than this pay the sleep as before.
const SPIN_WINDOW: Duration = Duration::from_micros(50);

// Linux errno values for `status`: the guest's numbering, not the host's
// (macOS numbers `EMSGSIZE` and `ENOTCONN` differently).
const LINUX_EIO: u32 = 5;
const LINUX_EINVAL: u32 = 22;
const LINUX_EMSGSIZE: u32 = 90;
const LINUX_ENOTCONN: u32 = 107;

/// Maps the call page at `base`: the `INFO` window, one doorbell device per
/// port (`cpus` ports, capped at `OVNFS_MAX_PORTS`) and the unused rest of the
/// page, each behind its own lock. Calls connect to the daemon at `sock`.
pub fn register(bus: &mut Bus, base: u64, mem: &GuestMemoryMmap, sock: &Path, cpus: u8) {
	register_with_timeout(bus, base, mem, sock, cpus, IO_TIMEOUT);
}

/// [`register`] with an explicit connection timeout, so tests need not wait
/// the production 30 s for a failure.
fn register_with_timeout(
	bus: &mut Bus,
	base: u64,
	mem: &GuestMemoryMmap,
	sock: &Path,
	cpus: u8,
	timeout: Duration,
) {
	let ports = usize::from(cpus).clamp(1, MAX_PORTS);
	let info = CALL_MAGIC | (CALL_VERSION << 32) | ((ports as u64) << 48);
	bus.register(base, REG_DOORBELL, Arc::new(Mutex::new(Registers { window: 0, info })));
	let sock: Arc<Path> = Arc::from(sock);
	for index in 0..ports {
		let port = Port::new(index, mem.clone(), sock.clone(), timeout);
		let offset = REG_DOORBELL + DOORBELL_SIZE * index as u64;
		bus.register(base + offset, DOORBELL_SIZE, Arc::new(Mutex::new(port)));
	}
	let tail = REG_DOORBELL + DOORBELL_SIZE * ports as u64;
	if tail < MMIO_SIZE {
		let rest = Registers { window: tail, info };
		bus.register(base + tail, MMIO_SIZE - tail, Arc::new(Mutex::new(rest)));
	}
}

/// The non-doorbell parts of the page: `INFO` reads the device identity,
/// everything else reads zero and ignores writes.
struct Registers {
	/// Offset of this device's window within the page.
	window: u64,
	/// The 64-bit `INFO` value.
	info:   u64,
}

impl BusDevice for Registers {
	fn read(&mut self, offset: u64, data: &mut [u8]) {
		data.fill(0);
		if self.window + offset == REG_INFO && data.len() == 8 {
			data.copy_from_slice(&self.info.to_le_bytes());
		}
	}

	fn write(&mut self, _offset: u64, _data: &[u8]) {}
}

/// One port: performs calls on the trapping vCPU thread over its own daemon
/// connection, dialed lazily and replaced after any failure.
struct Port {
	/// Port number, for logs.
	index:         usize,
	mem:           GuestMemoryMmap,
	/// The daemon's kernel-share socket.
	sock:          Arc<Path>,
	/// Send/receive timeout of the daemon connection.
	timeout:       Duration,
	/// The daemon connection; `None` until first use and after a failure.
	conn:          Option<UnixStream>,
	/// Whether the current daemon outage was logged, so it is logged once.
	outage_logged: bool,
	scratch:       Scratch,
}

/// Per-port buffers of the call in progress, kept across calls so that a call
/// neither allocates nor clears large stack arrays.
struct Scratch {
	/// The call's validated non-empty segments, request segments first.
	segs: Vec<Seg>,
	/// The frame being transferred: its length prefix, then its segments.
	iov:  Vec<libc::iovec>,
}

#[allow(
	clippy::non_send_fields_in_send_ty,
	reason = "Scratch pointers are dereferenced only during the call that stored them"
)]
// SAFETY: the raw pointers in `segs` and `iov` point into the guest RAM mapping
// that the owning port's `GuestMemoryMmap` keeps alive, or at a stack buffer of
// the call in progress. They are dereferenced (by `readv`/`writev`) only during
// that call, on the vCPU thread holding the port's lock, and every call
// overwrites them before use, so moving the port between threads is sound.
unsafe impl Send for Scratch {}

impl BusDevice for Port {
	fn read(&mut self, _offset: u64, data: &mut [u8]) {
		data.fill(0);
	}

	fn write(&mut self, offset: u64, data: &[u8]) {
		if offset != 0 {
			return;
		}
		if let Ok(gpa) = <[u8; 8]>::try_from(data) {
			self.call(GuestAddress(u64::from_le_bytes(gpa)));
		}
	}
}

/// One validated, non-empty call segment.
struct Seg {
	gpa:  GuestAddress,
	/// Host address of `gpa`; the segment lies inside one guest RAM region.
	host: *mut u8,
	len:  usize,
}

/// A validated call whose segments are in [`Scratch::segs`]: the first
/// `request_segs` carry the request, the rest receive the reply.
#[derive(Clone, Copy)]
struct Call {
	request_segs: usize,
	request_len:  usize,
	reply_cap:    usize,
}

/// A reply frame that was read off a healthy connection.
enum Reply {
	/// The reply was written to the reply segments; its length.
	Delivered(usize),
	/// The reply exceeded the reply segments and was discarded.
	TooLarge,
}

/// Why a request/reply exchange failed; decides the guest status and whether
/// the request may be resent on a new connection.
enum Failure {
	/// Dialing the daemon failed.
	Unreachable(io::Error),
	/// The connection broke before any reply byte arrived (for example the
	/// daemon closed an idle connection): resent once on a new connection.
	Broken(io::Error),
	/// A timeout, a broken frame or a protocol violation: never resent, the
	/// daemon may still be executing the request.
	Fatal(io::Error),
}

impl Failure {
	/// Classifies an I/O error that happened before any reply byte arrived: a
	/// timeout is fatal (the daemon may be executing the request), anything
	/// else means the connection broke. Errors after reply bytes arrived are
	/// always [`Failure::Fatal`].
	fn before_reply(error: io::Error) -> Self {
		if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) {
			Self::Fatal(error)
		} else {
			Self::Broken(error)
		}
	}

	/// The Linux errno the guest sees.
	const fn status(&self) -> u32 {
		match self {
			Self::Unreachable(_) => LINUX_ENOTCONN,
			Self::Broken(_) | Self::Fatal(_) => LINUX_EIO,
		}
	}
}

impl Scratch {
	/// Buffers sized for the largest call, so calls never grow them.
	fn new() -> Self {
		Self { segs: Vec::with_capacity(MAX_SEGS), iov: Vec::with_capacity(MAX_SEGS + 1) }
	}
}

/// How the first bytes of a reply were received.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Arrival {
	/// While polling, within the spin window.
	Spun,
	/// By the blocking receive after the spin window.
	Blocked,
}

/// Direction of a vectored socket transfer.
#[derive(Clone, Copy)]
enum Direction {
	Send,
	Receive,
}

impl Port {
	/// A port with no daemon connection yet.
	fn new(index: usize, mem: GuestMemoryMmap, sock: Arc<Path>, timeout: Duration) -> Self {
		Self { index, mem, sock, timeout, conn: None, outage_logged: false, scratch: Scratch::new() }
	}

	/// Performs the call at `call_gpa` and writes `status` and `reply_len`
	/// back. A call header outside guest RAM has nowhere to report to and is
	/// dropped.
	fn call(&mut self, call_gpa: GuestAddress) {
		let started = Instant::now();
		let (status, reply_len) = self.perform(call_gpa);
		let mut done = [0u8; 8];
		done[..4].copy_from_slice(&status.to_le_bytes());
		done[4..].copy_from_slice(&(reply_len as u32).to_le_bytes());
		let _ = self.mem.write_slice(&done, call_gpa);
		crate::metrics::record_ovnfs_call(started.elapsed());
	}

	/// Validates the call and relays it; returns `(status, reply_len)`.
	fn perform(&mut self, call_gpa: GuestAddress) -> (u32, usize) {
		let Some(call) = parse_call(&self.mem, call_gpa, &mut self.scratch.segs) else {
			return (LINUX_EINVAL, 0);
		};
		match self.exchange(call) {
			Ok(Reply::Delivered(len)) => (0, len),
			Ok(Reply::TooLarge) => (LINUX_EMSGSIZE, 0),
			Err(failure) => {
				self.log_failure(&failure);
				(failure.status(), 0)
			},
		}
	}

	/// Sends the request and receives the reply, re-dialing once when the
	/// first attempt could not dial or found its connection broken. A failed
	/// connection is closed, never reused.
	fn exchange(&mut self, call: Call) -> Result<Reply, Failure> {
		let mut retry_left = true;
		loop {
			match self.attempt(call) {
				Ok(reply) => return Ok(reply),
				Err(failure) => {
					self.conn = None;
					let retryable = matches!(failure, Failure::Unreachable(_) | Failure::Broken(_));
					if !(retry_left && retryable) {
						return Err(failure);
					}
					retry_left = false;
				},
			}
		}
	}

	/// One attempt: dial if there is no connection, then exchange on it.
	fn attempt(&mut self, call: Call) -> Result<Reply, Failure> {
		let fd = self.connection()?;
		transact(&self.mem, &mut self.scratch, fd, call)
	}

	/// The port's daemon connection, dialing one if there is none.
	fn connection(&mut self) -> Result<RawFd, Failure> {
		if let Some(conn) = &self.conn {
			return Ok(conn.as_raw_fd());
		}
		let conn = dial(&self.sock, self.timeout).map_err(Failure::Unreachable)?;
		self.outage_logged = false;
		Ok(self.conn.insert(conn).as_raw_fd())
	}

	/// Logs a failed call; an unreachable daemon is logged once per outage.
	fn log_failure(&mut self, failure: &Failure) {
		match failure {
			Failure::Unreachable(error) => {
				if !self.outage_logged {
					self.outage_logged = true;
					warn!(
						port = self.index,
						"ovnfs daemon {} unreachable: {error}",
						self.sock.display()
					);
				}
			},
			Failure::Broken(error) | Failure::Fatal(error) => {
				warn!(port = self.index, "ovnfs call failed: {error}");
			},
		}
	}
}

/// Connects to the daemon socket with send and receive timeouts.
fn dial(sock: &Path, timeout: Duration) -> io::Result<UnixStream> {
	let conn = UnixStream::connect(sock)?;
	conn.set_read_timeout(Some(timeout))?;
	conn.set_write_timeout(Some(timeout))?;
	grow_buffers(&conn);
	Ok(conn)
}

/// Raises the connection's send and receive buffers so a whole frame fits:
/// tries `SOCKET_BUFFER_MAX`, halving down to `SOCKET_BUFFER_MIN`. macOS
/// rejects sizes above `kern.ipc.maxsockbuf` (Linux clamps instead); when no
/// size is accepted the default stays, which is slower but correct.
fn grow_buffers(conn: &UnixStream) {
	for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
		let mut size = SOCKET_BUFFER_MAX;
		while size >= SOCKET_BUFFER_MIN && !set_buffer(conn, option, size) {
			size /= 2;
		}
	}
}

/// Sets the socket buffer `option` to `size` bytes; whether the kernel took it.
fn set_buffer(conn: &UnixStream, option: libc::c_int, size: usize) -> bool {
	let value = size as libc::c_int;
	// SAFETY: `value` is a live `c_int` and the length passed is its size; the
	// kernel only reads it during the call.
	let rc = unsafe {
		libc::setsockopt(
			conn.as_raw_fd(),
			libc::SOL_SOCKET,
			option,
			(&raw const value).cast(),
			size_of::<libc::c_int>() as libc::socklen_t,
		)
	};
	rc == 0
}

/// Reads and validates the call at `gpa`, leaving its non-empty segments in
/// `segs`: segment counts within `OVNFS_MAX_SEGS`, every segment inside one
/// guest RAM region, and request and reply each within `OVNFS_MAX_FRAME`.
/// `None` means `EINVAL`.
fn parse_call(mem: &GuestMemoryMmap, gpa: GuestAddress, segs: &mut Vec<Seg>) -> Option<Call> {
	segs.clear();
	let mut header = [0u8; CALL_HEADER_SIZE];
	mem.read_slice(&mut header, gpa).ok()?;
	let n_out = usize::from(u16::from_le_bytes([header[8], header[9]]));
	let n_in = usize::from(u16::from_le_bytes([header[10], header[11]]));
	if n_out + n_in > MAX_SEGS {
		return None;
	}
	let mut call = Call { request_segs: 0, request_len: 0, reply_cap: 0 };
	let mut entry_gpa = gpa.checked_add(CALL_HEADER_SIZE as u64)?;
	for index in 0..n_out + n_in {
		let mut entry = [0u8; SEG_SIZE];
		mem.read_slice(&mut entry, entry_gpa).ok()?;
		entry_gpa = entry_gpa.checked_add(SEG_SIZE as u64)?;
		let (seg_gpa, rest) = entry.split_first_chunk::<8>()?;
		let (len, _) = rest.split_first_chunk::<4>()?;
		let seg_gpa = GuestAddress(u64::from_le_bytes(*seg_gpa));
		let len = u32::from_le_bytes(*len) as usize;
		if len == 0 {
			continue;
		}
		let is_request = index < n_out;
		let total = if is_request {
			&mut call.request_len
		} else {
			&mut call.reply_cap
		};
		*total += len;
		if *total > MAX_FRAME {
			return None;
		}
		let host = mem.get_slice(seg_gpa, len).ok()?.ptr_guard_mut().as_ptr();
		segs.push(Seg { gpa: seg_gpa, host, len });
		if is_request {
			call.request_segs = segs.len();
		}
	}
	Some(call)
}

/// One request/reply exchange on the connection `fd`.
fn transact(
	mem: &GuestMemoryMmap,
	scratch: &mut Scratch,
	fd: RawFd,
	call: Call,
) -> Result<Reply, Failure> {
	let (request, reply) = scratch.segs.split_at(call.request_segs);
	let mut prefix = (call.request_len as u32).to_le_bytes();
	let frame = fill_iov(&mut scratch.iov, &mut prefix, request);
	let started = Instant::now();
	transfer_all(fd, frame, Direction::Send).map_err(Failure::before_reply)?;
	let sent = Instant::now();
	crate::metrics::record_ovnfs_send(sent - started);
	receive(mem, fd, reply, call.reply_cap, &mut scratch.iov, sent)
}

/// Receives one reply frame into the call's reply segments; the request went
/// out at `sent`. The length prefix and whatever body is already queued arrive
/// with a single receive across `[prefix, reply segments]`; the daemon sends
/// nothing past its reply, so that read cannot overrun the frame.
fn receive(
	mem: &GuestMemoryMmap,
	fd: RawFd,
	reply: &[Seg],
	reply_cap: usize,
	iov: &mut Vec<libc::iovec>,
	sent: Instant,
) -> Result<Reply, Failure> {
	let mut prefix = [0u8; FRAME_PREFIX_SIZE];
	let used = fill_iov(iov, &mut prefix, reply);
	let (got, arrival) = first_reply(fd, used, sent + SPIN_WINDOW).map_err(Failure::before_reply)?;
	crate::metrics::record_ovnfs_reply_wait(sent.elapsed());
	if arrival == Arrival::Spun {
		crate::metrics::record_ovnfs_spin_hit();
	}
	if got == 0 {
		return Err(Failure::Broken(io::ErrorKind::UnexpectedEof.into()));
	}
	let body = got.saturating_sub(FRAME_PREFIX_SIZE);
	if got < FRAME_PREFIX_SIZE {
		let rest = advance(&mut used[..1], got);
		transfer_all(fd, rest, Direction::Receive).map_err(Failure::Fatal)?;
	}
	let len = u32::from_le_bytes(prefix) as usize;
	if len > MAX_FRAME || body > len {
		mark_written(mem, reply, body);
		return Err(Failure::Fatal(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("daemon sent a reply frame of {len} bytes"),
		)));
	}
	if len > reply_cap {
		mark_written(mem, reply, body);
		drain(fd, len - body).map_err(Failure::Fatal)?;
		return Ok(Reply::TooLarge);
	}
	mark_written(mem, reply, len);
	let segments = truncate(&mut used[1..], len);
	let rest = advance(segments, body);
	transfer_all(fd, rest, Direction::Receive).map_err(Failure::Fatal)?;
	Ok(Reply::Delivered(len))
}

/// Refills `iov` with `prefix` followed by `segs`, the `iovec`s of one frame.
fn fill_iov<'a>(
	iov: &'a mut Vec<libc::iovec>,
	prefix: &mut [u8; FRAME_PREFIX_SIZE],
	segs: &[Seg],
) -> &'a mut [libc::iovec] {
	iov.clear();
	iov.push(libc::iovec { iov_base: prefix.as_mut_ptr().cast(), iov_len: FRAME_PREFIX_SIZE });
	for seg in segs {
		iov.push(libc::iovec { iov_base: seg.host.cast(), iov_len: seg.len });
	}
	iov
}

/// Records the first `len` bytes of the reply segments in the host-write dirty
/// bitmap: they are written through raw pointers, which vm-memory cannot see.
fn mark_written(mem: &GuestMemoryMmap, reply: &[Seg], mut len: usize) {
	for seg in reply {
		if len == 0 {
			break;
		}
		let n = seg.len.min(len);
		crate::memory::mark_dirty(mem, seg.gpa, n);
		len -= n;
	}
}

/// Shortens `iov` to its first `len` bytes (which it must hold).
fn truncate(iov: &mut [libc::iovec], mut len: usize) -> &mut [libc::iovec] {
	let mut keep = 0;
	while len > 0 {
		let entry = &mut iov[keep];
		entry.iov_len = entry.iov_len.min(len);
		len -= entry.iov_len;
		keep += 1;
	}
	&mut iov[..keep]
}

/// Drops the first `n` bytes from `iov` (which must hold at least `n`) and
/// returns the untransferred rest, without emptied entries.
fn advance(iov: &mut [libc::iovec], mut n: usize) -> &mut [libc::iovec] {
	let mut skip = 0;
	while skip < iov.len() && n >= iov[skip].iov_len {
		n -= iov[skip].iov_len;
		skip += 1;
	}
	let rest = &mut iov[skip..];
	if let Some(first) = rest.first_mut() {
		// SAFETY: `n < first.iov_len` here, so the advanced pointer stays inside
		// the buffer `first` describes.
		first.iov_base = unsafe { first.iov_base.cast::<u8>().add(n) }.cast();
		first.iov_len -= n;
	}
	rest
}

/// Transfers all of `iov`; a receive that reaches end of stream first fails
/// with `UnexpectedEof`.
fn transfer_all(fd: RawFd, mut iov: &mut [libc::iovec], direction: Direction) -> io::Result<()> {
	while !iov.is_empty() {
		let n = transfer(fd, iov, direction)?;
		if n == 0 {
			return Err(io::ErrorKind::UnexpectedEof.into());
		}
		iov = advance(std::mem::take(&mut iov), n);
	}
	Ok(())
}

/// One `writev`/`readv` on `fd`, restarted when a signal (such as a pause
/// kick) interrupts it; returns the bytes moved, 0 at end of stream.
fn transfer(fd: RawFd, iov: &[libc::iovec], direction: Direction) -> io::Result<usize> {
	// At most `MAX_SEGS + 1` entries, far below `IOV_MAX` (1024).
	let count = iov.len() as libc::c_int;
	loop {
		// SAFETY: every entry of `iov` describes memory valid for its length:
		// a frame prefix or scratch buffer of the caller, or a guest RAM range
		// validated by `parse_call` and kept mapped by the port's
		// `GuestMemoryMmap`. The kernel does not keep the pointers.
		let n = unsafe {
			match direction {
				Direction::Send => libc::writev(fd, iov.as_ptr(), count),
				Direction::Receive => libc::readv(fd, iov.as_ptr(), count),
			}
		};
		if let Ok(n) = usize::try_from(n) {
			return Ok(n);
		}
		let error = io::Error::last_os_error();
		if error.kind() != io::ErrorKind::Interrupted {
			return Err(error);
		}
	}
}

/// The first receive of a reply into `iov`: polls without sleeping until
/// `spin_until`, then blocks with the connection's timeout. Returns the bytes
/// received (0 at end of stream) and how they arrived; a partial reply
/// continues through the caller's blocking path.
fn first_reply(
	fd: RawFd,
	iov: &[libc::iovec],
	spin_until: Instant,
) -> io::Result<(usize, Arrival)> {
	loop {
		match receive_nonblocking(fd, iov) {
			Ok(n) => return Ok((n, Arrival::Spun)),
			Err(error)
				if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted) => {},
			Err(error) => return Err(error),
		}
		if Instant::now() >= spin_until {
			break;
		}
		std::hint::spin_loop();
	}
	let n = transfer(fd, iov, Direction::Receive)?;
	Ok((n, Arrival::Blocked))
}

/// One `recvmsg` into `iov` that fails with `WouldBlock` instead of sleeping
/// when nothing is queued; the socket itself stays blocking.
fn receive_nonblocking(fd: RawFd, iov: &[libc::iovec]) -> io::Result<usize> {
	// SAFETY: an all-zero `msghdr` is valid (no address, no control data); it
	// is plain data whose private padding fields differ between C libraries.
	let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
	msg.msg_iov = iov.as_ptr().cast_mut();
	// At most `MAX_SEGS + 1` entries; the field type differs between libcs.
	msg.msg_iovlen = iov.len() as _;
	// SAFETY: `msg` names only `iov`, whose entries describe memory valid for
	// their lengths (see `transfer`); `recvmsg` writes through them and does
	// not keep any pointer.
	let n = unsafe { libc::recvmsg(fd, &raw mut msg, libc::MSG_DONTWAIT) };
	usize::try_from(n).map_err(|_| io::Error::last_os_error())
}

/// Reads and discards `len` bytes, keeping the connection framed after a reply
/// that did not fit.
fn drain(fd: RawFd, mut len: usize) -> io::Result<()> {
	let mut scratch = [0u8; DRAIN_CHUNK];
	while len > 0 {
		let chunk = len.min(DRAIN_CHUNK);
		let mut iov = [libc::iovec { iov_base: scratch.as_mut_ptr().cast(), iov_len: chunk }];
		transfer_all(fd, &mut iov, Direction::Receive)?;
		len -= chunk;
	}
	Ok(())
}

#[cfg(test)]
impl Port {
	/// The daemon connection, for tests that inspect its socket options.
	const fn daemon_conn(&self) -> Option<&UnixStream> {
		self.conn.as_ref()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		io::{Read, Write},
		os::unix::net::UnixListener,
		path::PathBuf,
		sync::{
			atomic::{AtomicUsize, Ordering},
			mpsc,
		},
		thread,
	};

	use vm_memory::bitmap::{AtomicBitmap, Bitmap};

	use super::*;

	const RAM_BASE: u64 = 0x10_0000;
	const RAM_SIZE: usize = 0x4_0000;
	const PAGE_BASE: u64 = 0x1000;
	const TEST_TIMEOUT: Duration = Duration::from_secs(5);
	/// Request buffer of [`simple_call`]. The call header, request and reply
	/// sit 64 KiB apart: the dirty bitmap tracks host pages, which are 16 KiB
	/// on macOS arm64.
	const REQUEST: u64 = RAM_BASE + 0x1_0000;
	/// Reply buffer of [`simple_call`].
	const REPLY: u64 = RAM_BASE + 0x2_0000;

	/// A fresh daemon socket path under the temp dir, removed on drop.
	struct SockPath(PathBuf);

	impl SockPath {
		fn new() -> Self {
			static NEXT: AtomicUsize = AtomicUsize::new(0);
			let n = NEXT.fetch_add(1, Ordering::Relaxed);
			let name = format!("ovnfs-dev-{}-{n}.sock", std::process::id());
			let path = std::env::temp_dir().join(name);
			let _ = std::fs::remove_file(&path);
			Self(path)
		}
	}

	impl Drop for SockPath {
		fn drop(&mut self) {
			let _ = std::fs::remove_file(&self.0);
		}
	}

	fn memory() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(RAM_BASE), RAM_SIZE)]).unwrap()
	}

	/// A bus with the call page of `cpus` ports bound to `sock`.
	fn bus(mem: &GuestMemoryMmap, sock: &Path, cpus: u8) -> Bus {
		let mut bus = Bus::new();
		register_with_timeout(&mut bus, PAGE_BASE, mem, sock, cpus, TEST_TIMEOUT);
		bus
	}

	fn put(mem: &GuestMemoryMmap, gpa: u64, bytes: &[u8]) {
		mem.write_slice(bytes, GuestAddress(gpa)).unwrap();
	}

	fn get(mem: &GuestMemoryMmap, gpa: u64, len: usize) -> Vec<u8> {
		let mut bytes = vec![0u8; len];
		mem.read_slice(&mut bytes, GuestAddress(gpa)).unwrap();
		bytes
	}

	/// Writes a call at `gpa` with the given `(gpa, len)` segments; status and
	/// `reply_len` start as all ones so the test sees that they were written.
	fn write_call(mem: &GuestMemoryMmap, gpa: u64, out: &[(u64, u32)], ins: &[(u64, u32)]) {
		let mut call = Vec::new();
		call.extend_from_slice(&u32::MAX.to_le_bytes());
		call.extend_from_slice(&u32::MAX.to_le_bytes());
		call.extend_from_slice(&(out.len() as u16).to_le_bytes());
		call.extend_from_slice(&(ins.len() as u16).to_le_bytes());
		call.extend_from_slice(&0u32.to_le_bytes());
		for (seg_gpa, len) in out.iter().chain(ins) {
			call.extend_from_slice(&seg_gpa.to_le_bytes());
			call.extend_from_slice(&len.to_le_bytes());
			call.extend_from_slice(&0u32.to_le_bytes());
		}
		put(mem, gpa, &call);
	}

	/// Request "ping" in one segment, 64 reply bytes, call header at
	/// `RAM_BASE`.
	fn simple_call(mem: &GuestMemoryMmap) {
		put(mem, REQUEST, b"ping");
		write_call(mem, RAM_BASE, &[(REQUEST, 4)], &[(REPLY, 64)]);
	}

	/// Rings doorbell `port` with the call at `gpa`; returns
	/// `(status, reply_len)`.
	fn ring(bus: &Bus, mem: &GuestMemoryMmap, port: u64, gpa: u64) -> (u32, u32) {
		let doorbell = PAGE_BASE + REG_DOORBELL + DOORBELL_SIZE * port;
		assert!(bus.write(doorbell, &gpa.to_le_bytes()));
		let done = get(mem, gpa, 8);
		let status = u32::from_le_bytes(done[..4].try_into().unwrap());
		let reply_len = u32::from_le_bytes(done[4..].try_into().unwrap());
		(status, reply_len)
	}

	fn read_frame(conn: &mut UnixStream) -> Vec<u8> {
		let mut prefix = [0u8; 4];
		conn.read_exact(&mut prefix).unwrap();
		let mut body = vec![0u8; u32::from_le_bytes(prefix) as usize];
		conn.read_exact(&mut body).unwrap();
		body
	}

	fn write_frame(conn: &mut UnixStream, body: &[u8]) {
		let prefix = (body.len() as u32).to_le_bytes();
		conn.write_all(&prefix).unwrap();
		conn.write_all(body).unwrap();
	}

	#[test]
	fn ovnfs_info_reports_magic_version_and_ports() {
		let mem = memory();
		let sock = SockPath::new();
		let bus = bus(&mem, &sock.0, 3);
		let mut info = [0u8; 8];
		assert!(bus.read(PAGE_BASE, &mut info));
		assert_eq!(u64::from_le_bytes(info), 0x5346_4e56 | (1 << 32) | (3 << 48));
		let mut other = [0xffu8; 8];
		assert!(bus.read(PAGE_BASE + 0x800, &mut other));
		assert_eq!(other, [0; 8]);
	}

	#[test]
	fn ovnfs_round_trip_multi_segment() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let daemon = thread::spawn(move || {
			let (mut conn, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut conn), b"hello, daemon");
			write_frame(&mut conn, b"0123456789abcdef");
		});
		put(&mem, RAM_BASE + 0x1000, b"hello, ");
		put(&mem, RAM_BASE + 0x3000, b"daemon");
		write_call(&mem, RAM_BASE, &[(RAM_BASE + 0x1000, 7), (RAM_BASE + 0x3000, 6)], &[
			(RAM_BASE + 0x5000, 10),
			(RAM_BASE + 0x6000, 0),
			(RAM_BASE + 0x7000, 100),
		]);
		let bus = bus(&mem, &sock.0, 1);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 16));
		assert_eq!(get(&mem, RAM_BASE + 0x5000, 10), b"0123456789");
		assert_eq!(get(&mem, RAM_BASE + 0x7000, 6), b"abcdef");
		daemon.join().unwrap();
	}

	#[test]
	fn ovnfs_segment_outside_ram_is_einval() {
		let mem = memory();
		let sock = SockPath::new();
		let bus = bus(&mem, &sock.0, 1);
		let end = RAM_BASE + RAM_SIZE as u64;
		write_call(&mem, RAM_BASE, &[(end - 2, 4)], &[(REPLY, 64)]);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (LINUX_EINVAL, 0));
		write_call(&mem, RAM_BASE, &[(REQUEST, 4)], &[(0x10, 64)]);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (LINUX_EINVAL, 0));
	}

	#[test]
	fn ovnfs_too_many_segments_is_einval() {
		let mem = memory();
		let sock = SockPath::new();
		let bus = bus(&mem, &sock.0, 1);
		let out = vec![(REQUEST, 1); 200];
		let ins = vec![(REPLY, 1); MAX_SEGS + 1 - 200];
		write_call(&mem, RAM_BASE, &out, &ins);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (LINUX_EINVAL, 0));
	}

	#[test]
	fn ovnfs_unreachable_daemon_is_enotconn() {
		let mem = memory();
		let sock = SockPath::new();
		let bus = bus(&mem, &sock.0, 1);
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (LINUX_ENOTCONN, 0));
	}

	#[test]
	fn ovnfs_oversized_reply_is_emsgsize_and_connection_survives() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let daemon = thread::spawn(move || {
			let (mut conn, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut conn), b"ping");
			write_frame(&mut conn, &vec![7u8; 100_000]);
			assert_eq!(read_frame(&mut conn), b"ping");
			write_frame(&mut conn, b"pong");
		});
		let bus = bus(&mem, &sock.0, 1);
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (LINUX_EMSGSIZE, 0));
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 4));
		assert_eq!(get(&mem, REPLY, 4), b"pong");
		daemon.join().unwrap();
	}

	#[test]
	fn ovnfs_reconnects_after_daemon_closes() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let (closed_tx, closed_rx) = mpsc::channel();
		let daemon = thread::spawn(move || {
			let (mut first, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut first), b"ping");
			write_frame(&mut first, b"one");
			drop(first);
			closed_tx.send(()).unwrap();
			let (mut second, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut second), b"ping");
			write_frame(&mut second, b"two");
		});
		let bus = bus(&mem, &sock.0, 1);
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 3));
		closed_rx.recv_timeout(TEST_TIMEOUT).unwrap();
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 3));
		assert_eq!(get(&mem, REPLY, 3), b"two");
		daemon.join().unwrap();
	}

	#[test]
	fn ovnfs_ports_do_not_wait_for_each_other() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let (stalled_tx, stalled_rx) = mpsc::channel();
		let (release_tx, release_rx) = mpsc::channel::<()>();
		// The first connection carries the stalled call: its reply waits for
		// `release`; the second is answered at once.
		let daemon = thread::spawn(move || {
			let (mut slow, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut slow), b"slow");
			stalled_tx.send(()).unwrap();
			let (mut fast, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut fast), b"fast");
			write_frame(&mut fast, b"fast");
			release_rx.recv().unwrap();
			write_frame(&mut slow, b"late");
		});
		let bus = Arc::new(bus(&mem, &sock.0, 2));

		put(&mem, REQUEST, b"slow");
		write_call(&mem, RAM_BASE, &[(REQUEST, 4)], &[(REPLY, 64)]);
		let slow = {
			let (bus, mem) = (bus.clone(), mem.clone());
			thread::spawn(move || ring(&bus, &mem, 0, RAM_BASE))
		};
		stalled_rx.recv_timeout(TEST_TIMEOUT).unwrap();

		let fast_call = RAM_BASE + 0x8000;
		put(&mem, RAM_BASE + 0x9000, b"fast");
		write_call(&mem, fast_call, &[(RAM_BASE + 0x9000, 4)], &[(RAM_BASE + 0xa000, 64)]);
		let (done_tx, done_rx) = mpsc::channel();
		let fast = {
			let (bus, mem) = (bus, mem.clone());
			thread::spawn(move || done_tx.send(ring(&bus, &mem, 1, fast_call)).unwrap())
		};
		assert_eq!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap(), (0, 4));
		release_tx.send(()).unwrap();
		assert_eq!(slow.join().unwrap(), (0, 4));
		assert_eq!(get(&mem, REPLY, 4), b"late");
		fast.join().unwrap();
		daemon.join().unwrap();
	}

	/// Reads the socket buffer `option` of `conn` in bytes.
	fn buffer_size(conn: &UnixStream, option: libc::c_int) -> usize {
		let mut value: libc::c_int = 0;
		let mut len = size_of::<libc::c_int>() as libc::socklen_t;
		// SAFETY: `value` and `len` are live locals of the sizes the kernel
		// writes; it does not keep the pointers.
		let rc = unsafe {
			libc::getsockopt(
				conn.as_raw_fd(),
				libc::SOL_SOCKET,
				option,
				(&raw mut value).cast(),
				&raw mut len,
			)
		};
		assert_eq!(rc, 0, "getsockopt: {}", io::Error::last_os_error());
		value as usize
	}

	#[test]
	fn ovnfs_connection_buffers_hold_large_frames() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let daemon = thread::spawn(move || {
			let (mut conn, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut conn), b"ping");
			write_frame(&mut conn, b"pong");
		});
		simple_call(&mem);
		let mut port = Port::new(0, mem.clone(), Arc::from(sock.0.as_path()), TEST_TIMEOUT);
		port.write(0, &RAM_BASE.to_le_bytes());
		assert_eq!(get(&mem, RAM_BASE, 8), [0, 0, 0, 0, 4, 0, 0, 0]);
		let conn = port.daemon_conn().expect("connected port");
		for option in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
			let size = buffer_size(conn, option);
			assert!(size >= SOCKET_BUFFER_MIN, "socket buffer {option} is {size} bytes");
		}
		daemon.join().unwrap();
	}

	/// An `iovec` over `buf`.
	fn iovec(buf: &mut [u8]) -> libc::iovec {
		libc::iovec { iov_base: buf.as_mut_ptr().cast(), iov_len: buf.len() }
	}

	/// The process-wide count of replies that arrived while polling.
	fn spin_hits() -> u64 {
		crate::metrics::snapshot_json()["ovnfs"]["spin_hits"]
			.as_u64()
			.unwrap()
	}

	#[test]
	fn ovnfs_immediate_reply_is_a_spin_hit() {
		let mem = memory();
		simple_call(&mem);
		let (vmm, mut daemon) = UnixStream::pair().unwrap();
		// The reply is queued before the request goes out, so the first poll
		// finds it.
		write_frame(&mut daemon, b"pong");
		let mut scratch = Scratch::new();
		let call = parse_call(&mem, GuestAddress(RAM_BASE), &mut scratch.segs).unwrap();
		let hits = spin_hits();
		let reply = transact(&mem, &mut scratch, vmm.as_raw_fd(), call);
		assert!(matches!(reply, Ok(Reply::Delivered(4))));
		assert!(spin_hits() > hits);
		assert_eq!(get(&mem, REPLY, 4), b"pong");
		assert_eq!(read_frame(&mut daemon), b"ping");

		write_frame(&mut daemon, b"pong");
		let (mut prefix, mut body) = ([0u8; 4], [0u8; 16]);
		let iov = [iovec(&mut prefix), iovec(&mut body)];
		let spin_until = Instant::now() + SPIN_WINDOW;
		let first = first_reply(vmm.as_raw_fd(), &iov, spin_until).unwrap();
		assert_eq!(first, (8, Arrival::Spun));
	}

	#[test]
	fn ovnfs_slow_reply_falls_back_to_blocking() {
		let (vmm, mut daemon) = UnixStream::pair().unwrap();
		let replier = thread::spawn(move || {
			let mut request = [0u8; 4];
			daemon.read_exact(&mut request).unwrap();
			thread::sleep(Duration::from_micros(200));
			write_frame(&mut daemon, b"late");
		});
		// The window closes 50 µs after this point; the reply comes 200 µs after
		// the request, which is sent later still.
		let spin_until = Instant::now() + SPIN_WINDOW;
		(&vmm).write_all(b"ping").unwrap();
		let (mut prefix, mut body) = ([0u8; 4], [0u8; 16]);
		let iov = [iovec(&mut prefix), iovec(&mut body)];
		let (got, arrival) = first_reply(vmm.as_raw_fd(), &iov, spin_until).unwrap();
		assert_eq!(arrival, Arrival::Blocked);
		assert!(got >= FRAME_PREFIX_SIZE, "received {got} bytes");
		assert_eq!(u32::from_le_bytes(prefix), 4);
		replier.join().unwrap();

		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let daemon = thread::spawn(move || {
			let (mut conn, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut conn), b"ping");
			thread::sleep(Duration::from_micros(200));
			write_frame(&mut conn, b"pong");
		});
		let bus = bus(&mem, &sock.0, 1);
		simple_call(&mem);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 4));
		assert_eq!(get(&mem, REPLY, 4), b"pong");
		daemon.join().unwrap();
	}

	#[test]
	fn ovnfs_marks_reply_bytes_dirty() {
		let mem = memory();
		let sock = SockPath::new();
		let listener = UnixListener::bind(&sock.0).unwrap();
		let daemon = thread::spawn(move || {
			let (mut conn, _) = listener.accept().unwrap();
			assert_eq!(read_frame(&mut conn), b"ping");
			write_frame(&mut conn, b"pong");
		});
		simple_call(&mem);
		crate::memory::reset_dirty(&mem);
		let bus = bus(&mem, &sock.0, 1);
		assert_eq!(ring(&bus, &mem, 0, RAM_BASE), (0, 4));
		let region: &vm_memory::mmap::MmapRegion<AtomicBitmap> =
			mem.find_region(GuestAddress(RAM_BASE)).unwrap();
		assert!(region.bitmap().dirty_at(0), "status and reply_len were written");
		assert!(region.bitmap().dirty_at((REPLY - RAM_BASE) as usize));
		assert!(!region.bitmap().dirty_at((REQUEST - RAM_BASE) as usize));
		daemon.join().unwrap();
	}
}
