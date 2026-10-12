//! In-process libslirp user-mode networking backend for Windows.
//!
//! libslirp owns the IPv4 NAT, DHCP, DNS, and transport sockets. A private
//! thread drives its Winsock descriptors and exchanges Ethernet frames with
//! virtio-net. Snapshot state contains libslirp's guest-visible state; live
//! host sockets are deliberately not preserved.

use std::{
	cell::RefCell,
	collections::VecDeque,
	io::{self, Cursor},
	net::{Ipv4Addr, Ipv6Addr},
	os::{
		raw::c_int,
		windows::io::{AsRawHandle, RawHandle},
	},
	rc::Rc,
	sync::Arc,
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use flume::{Receiver, Sender, TryRecvError, TrySendError};
use libslirp::{Context, Handler, PollEvents, state_version};
use parking_lot::Mutex;
use windows_sys::Win32::Networking::WinSock::{
	POLLERR, POLLHUP, POLLIN, POLLOUT, SOCKET, SOCKET_ERROR, WSAPOLLFD, WSAPoll,
};

use crate::{
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
	tap::VNET_HDR_SIZE,
};

const USER_NET: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 0);
const USER_MASK: Ipv4Addr = Ipv4Addr::new(255, 255, 255, 0);
const USER_HOST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 2);
const USER_DHCP_START: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 15);
const USER_DNS: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 3);
const MAX_PENDING_RX_FRAMES: usize = 1024;
const MAX_PENDING_TX_FRAMES: usize = 1024;
const SLIRP_STATE_VERSION_BYTES: usize = 4;
const COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(10);

type TimerFn = Rc<RefCell<Box<dyn FnMut()>>>;

pub struct UserNet {
	tx:         Sender<Command>,
	packet_evt: EventFd,
	packets:    Arc<Mutex<VecDeque<Vec<u8>>>>,
	thread:     Option<JoinHandle<()>>,
}

enum Command {
	Packet(Vec<u8>),
	Save(Sender<Result<Vec<u8>>>),
	Stop,
}

struct SlirpTimer {
	func:       TimerFn,
	expires_at: Option<Instant>,
}

struct SlirpHandler {
	start:      Instant,
	packets:    Arc<Mutex<VecDeque<Vec<u8>>>>,
	packet_evt: EventFd,
	timers:     Vec<Option<SlirpTimer>>,
}

impl UserNet {
	pub fn new_with_state(_mac: [u8; 6], state: Option<Vec<u8>>) -> Result<Self> {
		let (tx, rx) = flume::bounded(MAX_PENDING_TX_FRAMES);
		let packet_evt = EventFd::new(EFD_NONBLOCK)
			.map_err(|e| err(format!("creating user-net packet event: {e}")))?;
		let handler_packet_evt = packet_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net packet event: {e}")))?;
		let packets = Arc::new(Mutex::new(VecDeque::new()));
		let thread_packets = packets.clone();
		let (ready_tx, ready_rx) = flume::bounded(1);
		let thread = thread::Builder::new()
			.name("vmon-slirp".to_string())
			.spawn(move || run_slirp(rx, handler_packet_evt, thread_packets, state, ready_tx))
			.map_err(|e| err(format!("spawning user-net slirp thread: {e}")))?;
		match ready_rx
			.recv()
			.map_err(|_| err("user-net slirp thread exited during startup"))?
		{
			Ok(()) => Ok(Self { tx, packet_evt, packets, thread: Some(thread) }),
			Err(e) => {
				let _ = thread.join();
				Err(e)
			},
		}
	}

	pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
		let _ = self.packet_evt.read();
		let mut packets = self.packets.lock();
		let Some(packet) = packets.front() else {
			return Err(io::ErrorKind::WouldBlock.into());
		};
		let len = VNET_HDR_SIZE + packet.len();
		if buf.len() < len {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"virtio-net RX buffer is smaller than the user-net packet",
			));
		}
		buf[..VNET_HDR_SIZE].fill(0);
		buf[VNET_HDR_SIZE..len].copy_from_slice(packet);
		packets.pop_front();
		if !packets.is_empty() {
			self.packet_evt.write(1)?;
		}
		Ok(len)
	}

	pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
		if buf.len() < VNET_HDR_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"virtio-net TX buffer is smaller than the vnet header",
			));
		}
		match self
			.tx
			.try_send(Command::Packet(buf[VNET_HDR_SIZE..].to_vec()))
		{
			Ok(()) => Ok(buf.len()),
			Err(TrySendError::Full(_)) => Err(io::ErrorKind::WouldBlock.into()),
			Err(TrySendError::Disconnected(_)) => Err(io::ErrorKind::BrokenPipe.into()),
		}
	}

	pub fn save_state(&self) -> Result<Vec<u8>> {
		let (tx, rx) = flume::bounded(1);
		self
			.tx
			.send(Command::Save(tx))
			.map_err(|_| err("saving user-net state failed: slirp thread gone"))?;
		rx.recv()
			.map_err(|_| err("saving user-net state failed: slirp thread exited"))?
	}

	#[allow(
		clippy::unused_self,
		reason = "network backends expose one instance-based offload interface"
	)]
	pub fn set_offloads(&self, offloads: u32) -> Result<()> {
		if offloads == 0 {
			Ok(())
		} else {
			Err(err(format!(
				"user-mode networking does not support negotiated TAP offloads {offloads:#x}"
			)))
		}
	}

	#[allow(
		clippy::unused_self,
		reason = "network backends expose one instance-based offload interface"
	)]
	pub const fn supported_offloads(&self) -> u32 {
		0
	}

	pub fn as_raw_handle(&self) -> RawHandle {
		self.packet_evt.as_raw_handle()
	}
}

impl Drop for UserNet {
	fn drop(&mut self) {
		let mut stop = Command::Stop;
		loop {
			match self.tx.try_send(stop) {
				Ok(()) => break,
				Err(TrySendError::Full(cmd)) => {
					stop = cmd;
					thread::yield_now();
				},
				Err(TrySendError::Disconnected(_)) => break,
			}
		}
		if let Some(thread) = self.thread.take() {
			let _ = thread.join();
		}
	}
}

impl Handler for SlirpHandler {
	type Timer = usize;

	fn clock_get_ns(&mut self) -> i64 {
		let elapsed = self.start.elapsed();
		elapsed
			.as_secs()
			.saturating_mul(1_000_000_000)
			.saturating_add(u64::from(elapsed.subsec_nanos())) as i64
	}

	fn send_packet(&mut self, buf: &[u8]) -> io::Result<usize> {
		let mut packets = self.packets.lock();
		if packets.len() >= MAX_PENDING_RX_FRAMES {
			packets.pop_front();
		}
		packets.push_back(buf.to_vec());
		drop(packets);
		self.packet_evt.write(1)?;
		Ok(buf.len())
	}

	fn register_poll_fd(&mut self, _fd: c_int) {}

	fn unregister_poll_fd(&mut self, _fd: c_int) {}

	fn guest_error(&mut self, msg: &str) {
		tracing::warn!("libslirp guest error: {msg}");
	}

	fn notify(&mut self) {}

	fn timer_new(&mut self, func: Box<dyn FnMut()>) -> Box<Self::Timer> {
		let id = self
			.timers
			.iter()
			.position(Option::is_none)
			.unwrap_or_else(|| {
				self.timers.push(None);
				self.timers.len() - 1
			});
		self.timers[id] =
			Some(SlirpTimer { func: Rc::new(RefCell::new(func)), expires_at: None });
		Box::new(id)
	}

	fn timer_mod(&mut self, timer: &mut Box<Self::Timer>, expire_time: i64) {
		let Some(Some(timer)) = self.timers.get_mut(**timer) else {
			return;
		};
		timer.expires_at = if expire_time < 0 {
			None
		} else {
			Some(
				self
					.start
					.checked_add(Duration::from_millis(expire_time as u64))
					.unwrap_or_else(Instant::now),
			)
		};
	}

	fn timer_free(&mut self, timer: Box<Self::Timer>) {
		if let Some(slot) = self.timers.get_mut(*timer) {
			*slot = None;
		}
	}
}

impl SlirpHandler {
	fn next_timer_timeout(&self) -> Option<Duration> {
		let now = Instant::now();
		self
			.timers
			.iter()
			.filter_map(|t| t.as_ref().and_then(|t| t.expires_at))
			.map(|t| t.saturating_duration_since(now))
			.min()
	}

	fn due_timers(&mut self) -> Vec<TimerFn> {
		let now = Instant::now();
		let mut callbacks = Vec::new();
		for timer in self.timers.iter_mut().flatten() {
			if timer.expires_at.is_some_and(|expires| expires <= now) {
				timer.expires_at = None;
				callbacks.push(timer.func.clone());
			}
		}
		callbacks
	}
}

fn run_slirp(
	rx: Receiver<Command>,
	packet_evt: EventFd,
	packets: Arc<Mutex<VecDeque<Vec<u8>>>>,
	state: Option<Vec<u8>>,
	ready: Sender<Result<()>>,
) {
	let handler = Rc::new(RefCell::new(SlirpHandler {
		start: Instant::now(),
		packets,
		packet_evt,
		timers: Vec::new(),
	}));
	let context = Context::new(
		false,
		true,
		USER_NET,
		USER_MASK,
		USER_HOST,
		false,
		Ipv6Addr::UNSPECIFIED,
		0,
		Ipv6Addr::UNSPECIFIED,
		Some("vmon".to_string()),
		None,
		None,
		None,
		USER_DHCP_START,
		USER_DNS,
		Ipv6Addr::UNSPECIFIED,
		Vec::new(),
		None,
		handler.clone(),
	);
	let startup = state
		.as_deref()
		.map_or_else(|| Ok(()), |state| load_slirp_state(&context, state));
	let startup_ok = startup.is_ok();
	let _ = ready.send(startup);
	if !startup_ok {
		return;
	}
	loop {
		if !drain_commands(&context, &rx) {
			return;
		}
		fire_due_timers(&handler);
		let mut timeout = u32::MAX;
		let mut slirp_fds = Vec::new();
		context.pollfds_fill(&mut timeout, |fd, events| {
			slirp_fds.push((fd, events));
			(slirp_fds.len() - 1) as i32
		});
		let timeout = merge_timeout(timeout, handler.borrow().next_timer_timeout())
			.min(COMMAND_POLL_INTERVAL.as_millis() as i32);
		let mut pollfds = slirp_fds
			.iter()
			.map(|&(fd, events)| WSAPOLLFD {
				fd:      fd as SOCKET,
				events:  to_poll_events(events),
				revents: 0,
			})
			.collect::<Vec<_>>();
		if pollfds.is_empty() {
			thread::sleep(Duration::from_millis(timeout.max(0) as u64));
		} else {
			// SAFETY: `pollfds` is writable for its supplied length and Winsock
			// was initialized before entering the worker loop.
			if unsafe { WSAPoll(pollfds.as_mut_ptr(), pollfds.len() as u32, timeout) } == SOCKET_ERROR
			{
				tracing::warn!("libslirp WSAPoll failed: {}", io::Error::last_os_error());
				return;
			}
		}
		let revents = pollfds
			.iter()
			.map(|fd| from_poll_events(fd.revents))
			.collect::<Vec<_>>();
		if !revents.is_empty() {
			context.pollfds_poll(false, |idx| {
				revents
					.get(idx as usize)
					.copied()
					.unwrap_or_else(PollEvents::empty)
			});
		}
		fire_due_timers(&handler);
	}
}

fn drain_commands(context: &Context<Rc<RefCell<SlirpHandler>>>, rx: &Receiver<Command>) -> bool {
	loop {
		match rx.try_recv() {
			Ok(Command::Packet(frame)) => context.input(&frame),
			Ok(Command::Save(reply)) => {
				let _ = reply.send(save_slirp_state(context));
			},
			Ok(Command::Stop) => return false,
			Err(TryRecvError::Empty) => return true,
			Err(TryRecvError::Disconnected) => return false,
		}
	}
}
fn save_slirp_state(context: &Context<Rc<RefCell<SlirpHandler>>>) -> Result<Vec<u8>> {
	let mut data = state_version().to_be_bytes().to_vec();
	data.append(
		&mut context
			.state_get()
			.map_err(|e| err(format!("saving libslirp state: {e}")))?,
	);
	Ok(data)
}
fn load_slirp_state(context: &Context<Rc<RefCell<SlirpHandler>>>, data: &[u8]) -> Result<()> {
	if data.len() < SLIRP_STATE_VERSION_BYTES {
		return Err(err("libslirp state is missing its version header"));
	}
	let version = i32::from_be_bytes(
		data[..SLIRP_STATE_VERSION_BYTES]
			.try_into()
			.expect("checked length"),
	);
	let current = state_version();
	if version > current {
		return Err(err(format!(
			"libslirp state version {version} is newer than supported {current}"
		)));
	}
	context
		.state_read(version, &mut Cursor::new(&data[SLIRP_STATE_VERSION_BYTES..]))
		.map(|_| ())
		.map_err(|e| err(format!("loading libslirp state: {e}")))
}
fn fire_due_timers(handler: &Rc<RefCell<SlirpHandler>>) {
	for callback in handler.borrow_mut().due_timers() {
		(callback.borrow_mut())();
	}
}
fn merge_timeout(slirp: u32, timer: Option<Duration>) -> i32 {
	let timer = timer.map(|t| t.as_millis().min(i32::MAX as u128) as i32);
	match (slirp, timer) {
		(u32::MAX, None) => i32::MAX,
		(u32::MAX, Some(ms)) => ms,
		(ms, None) => ms.min(i32::MAX as u32) as i32,
		(ms, Some(t)) => (ms.min(i32::MAX as u32) as i32).min(t),
	}
}
fn to_poll_events(events: PollEvents) -> i16 {
	let mut out = 0;
	if events.has_in() {
		out |= POLLIN;
	}
	if events.has_out() {
		out |= POLLOUT;
	}
	if events.has_err() {
		out |= POLLERR;
	}
	if events.has_hup() {
		out |= POLLHUP;
	}
	out
}
fn from_poll_events(events: i16) -> PollEvents {
	let mut out = PollEvents::empty();
	if events & POLLIN != 0 {
		out |= PollEvents::poll_in();
	}
	if events & POLLOUT != 0 {
		out |= PollEvents::poll_out();
	}
	if events & POLLERR != 0 {
		out |= PollEvents::poll_err();
	}
	if events & POLLHUP != 0 {
		out |= PollEvents::poll_hup();
	}
	out
}
