//! Entitlement-free user-mode networking backend for virtio-net on macOS.
//!
//! The guest still sees a virtio-net NIC, but packets are handled by libslirp
//! in userspace. Default mode provides outbound NAT, DHCP, and DNS without
//! `com.apple.vm.networking`; restricted mode accepts only guest DHCP and one
//! configured virtual-gateway service. Inbound connectivity requires explicit
//! forwarding support. Snapshots serialize libslirp's guest-visible state
//! (DHCP lease, ARP/NAT tables) on the slirp thread. Host-side sockets are not
//! carried across restore, so in-flight TCP connections reset; new permitted
//! connections work after restore.
//!
//! Policy mode (`--user-net-policy`, see [`filter`]) instead runs every
//! guest frame through a live-replaceable egress filter and lets it learn
//! addresses from slirp-DNS answers. Without it the backend behaves exactly
//! as above.

pub mod filter;

use std::{
	cell::RefCell,
	collections::VecDeque,
	io::{self, Cursor},
	net::{Ipv4Addr, Ipv6Addr},
	os::fd::{AsRawFd, RawFd},
	rc::Rc,
	sync::Arc,
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use flume::{Receiver, Sender, TryRecvError, TrySendError};
use libslirp::{Context, Handler, PollEvents, state_version};
use parking_lot::Mutex;

use self::filter::Filter;
use crate::{
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
	tap::VNET_HDR_SIZE,
};

/// Egress filter shared by the TX path, the slirp thread, and the VMM
/// control loop (`set_net_policy`).
pub type SharedFilter = Arc<Mutex<Filter>>;

const USER_NET: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 0);
const USER_MASK: Ipv4Addr = Ipv4Addr::new(255, 255, 255, 0);
const USER_HOST: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 2);
const USER_DHCP_START: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 15);
const USER_DNS: Ipv4Addr = Ipv4Addr::new(10, 0, 2, 3);
const MAX_PENDING_RX_FRAMES: usize = 1024;
const MAX_PENDING_TX_FRAMES: usize = 1024;
const SLIRP_STATE_VERSION_BYTES: usize = 4;
const USER_NET_STATE_MAGIC: [u8; 4] = *b"VMUN";
const USER_NET_STATE_FORMAT_VERSION: u8 = 2;
const USER_NET_STATE_HEADER_BYTES: usize = 8;
const ETHERTYPE_ARP: u16 = 0x0806;
const ETHERTYPE_IPV4: u16 = 0x0800;
const IPPROTO_TCP: u8 = 6;
const IPPROTO_UDP: u8 = 17;
const DHCP_CLIENT_PORT: u16 = 68;
const DHCP_SERVER_PORT: u16 = 67;

/// User-mode NAT backend driven by a private libslirp thread.
pub struct UserNet {
	tx:                Sender<Command>,
	command_evt:       EventFd,
	packet_evt:        EventFd,
	packets:           Arc<Mutex<VecDeque<Vec<u8>>>>,
	thread:            Option<JoinHandle<()>>,
	allowed_host_port: Option<u16>,
	restricted_egress: bool,
	filter:            Option<SharedFilter>,
}

enum Command {
	Packet(Vec<u8>),
	Save(bool, Option<u16>, Sender<Result<Vec<u8>>>),
	Stop,
}

type TimerFn = Rc<RefCell<Box<dyn FnMut()>>>;

struct SlirpTimer {
	func:       TimerFn,
	expires_at: Option<Instant>,
}

struct SlirpHandler {
	start:      Instant,
	packets:    Arc<Mutex<VecDeque<Vec<u8>>>>,
	packet_evt: EventFd,
	loop_evt:   EventFd,
	timers:     Vec<Option<SlirpTimer>>,
	filter:     Option<SharedFilter>,
}

impl UserNet {
	/// Start an entitlement-free user-mode NAT backend and optionally restore
	/// its serialized guest-visible libslirp state before the backend is exposed
	/// to vCPUs.
	pub fn new_with_state(mac: [u8; 6], state: Option<Vec<u8>>) -> Result<Self> {
		Self::new_with_restricted_egress(mac, false, None, None, state)
	}

	/// Start a user-mode NAT backend whose guest egress is governed by
	/// `filter` (see [`filter`]).
	pub fn new_filtered_with_state(
		mac: [u8; 6],
		filter: SharedFilter,
		state: Option<Vec<u8>>,
	) -> Result<Self> {
		Self::new_with_restricted_egress(mac, false, None, Some(filter), state)
	}

	/// Start a user-mode backend that permits only DHCP and one TCP service on
	/// the virtual host gateway.
	pub fn new_restricted_with_state(
		mac: [u8; 6],
		allowed_host_port: u16,
		state: Option<Vec<u8>>,
	) -> Result<Self> {
		if allowed_host_port == 0 {
			return Err(err("restricted user-net host port must be nonzero"));
		}
		Self::new_with_restricted_egress(mac, true, Some(allowed_host_port), None, state)
	}

	fn new_with_restricted_egress(
		_mac: [u8; 6],
		restricted_egress: bool,
		allowed_host_port: Option<u16>,
		filter: Option<SharedFilter>,
		state: Option<Vec<u8>>,
	) -> Result<Self> {
		let (restricted_egress, allowed_host_port) =
			restriction_for_state(restricted_egress, allowed_host_port, state.as_deref())?;
		if restricted_egress && filter.is_some() {
			return Err(err("a user-net policy cannot be combined with restricted user-net egress"));
		}
		let state = state
			.map(|state| split_user_net_state(&state).map(|(_, _, slirp_state)| slirp_state.to_vec()))
			.transpose()?;
		let (tx, rx) = flume::bounded(MAX_PENDING_TX_FRAMES);
		let command_evt = EventFd::new(EFD_NONBLOCK)
			.map_err(|e| err(format!("creating user-net command eventfd: {e}")))?;
		let packet_evt = EventFd::new(EFD_NONBLOCK)
			.map_err(|e| err(format!("creating user-net packet eventfd: {e}")))?;
		let packets = Arc::new(Mutex::new(VecDeque::new()));

		let thread_command_evt = command_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net command eventfd: {e}")))?;
		let handler_command_evt = command_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net loop eventfd: {e}")))?;
		let handler_packet_evt = packet_evt
			.try_clone()
			.map_err(|e| err(format!("cloning user-net packet eventfd: {e}")))?;
		let thread_packets = packets.clone();
		let thread_filter = filter.clone();
		let (ready_tx, ready_rx) = flume::bounded(1);
		let thread = thread::Builder::new()
			.name("vmon-slirp".to_string())
			.spawn(move || {
				run_slirp(
					rx,
					thread_command_evt,
					handler_command_evt,
					handler_packet_evt,
					thread_packets,
					restricted_egress,
					thread_filter,
					state,
					ready_tx,
				);
			})
			.map_err(|e| err(format!("spawning user-net slirp thread: {e}")))?;

		match ready_rx
			.recv()
			.map_err(|_| err("user-net slirp thread exited during startup"))?
		{
			Ok(()) => Ok(Self {
				tx,
				command_evt,
				packet_evt,
				packets,
				allowed_host_port,
				restricted_egress,
				filter,
				thread: Some(thread),
			}),
			Err(e) => {
				let _ = thread.join();
				Err(e)
			},
		}
	}

	/// Read one vnet-header-prefixed Ethernet frame produced by libslirp.
	///
	/// The wake token stays armed while frames remain queued, so the worker's
	/// poll is level-triggered like a TAP fd: a bounded RX pass that stops
	/// early is re-run instead of stranding the rest of the queue until
	/// libslirp happens to emit another frame. The token is cleared only under
	/// the queue lock after observing an empty queue; libslirp re-arms it
	/// after every push, so a queued frame always has a pending wake.
	pub fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
		let mut packets = self.packets.lock();
		let Some(packet) = packets.front() else {
			let _ = self.packet_evt.read();
			return Err(io::Error::from(io::ErrorKind::WouldBlock));
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
		if packets.is_empty() {
			let _ = self.packet_evt.read();
		}
		Ok(len)
	}

	/// Park RX while the guest has no receive buffers: queued frames are
	/// kept (libslirp's TCP is not built for a lossy link; dropped segments
	/// stall flows on its retransmission backoff) and the wake token is
	/// cleared so the worker does not spin. The device resumes RX from the
	/// guest's queue notification when it posts buffers again.
	pub fn defer_rx(&self) {
		let _ = self.packet_evt.read();
	}

	/// Queue a frame exactly as libslirp's `send_packet` does (push, then
	/// wake), for device-level tests.
	#[cfg(test)]
	pub(in crate::virtio) fn push_test_frame(&self, frame: &[u8]) {
		self.packets.lock().push_back(frame.to_vec());
		self.packet_evt.write(1).expect("wake");
	}

	/// Whether the worker's wait source would currently fire.
	#[cfg(test)]
	pub(in crate::virtio) fn rx_wake_armed(&self) -> bool {
		let mut fd = libc::pollfd { fd: self.as_raw_fd(), events: libc::POLLIN, revents: 0 };
		// SAFETY: `fd` is one valid pollfd and the timeout is zero.
		unsafe { libc::poll(&raw mut fd, 1, 0) == 1 }
	}

	/// Send one vnet-header-prefixed Ethernet frame from the guest into
	/// libslirp.
	pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
		if buf.len() < VNET_HDR_SIZE {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"virtio-net TX buffer is smaller than the vnet header",
			));
		}
		if self.restricted_egress
			&& !allows_restricted_egress(&buf[VNET_HDR_SIZE..], self.allowed_host_port)
		{
			return Ok(buf.len());
		}
		if let Some(filter) = &self.filter
			&& !filter.lock().admit(&buf[VNET_HDR_SIZE..], Instant::now())
		{
			return Ok(buf.len());
		}
		let frame = buf[VNET_HDR_SIZE..].to_vec();
		match self.tx.try_send(Command::Packet(frame)) {
			Ok(()) => {
				self.command_evt.write(1)?;
				Ok(buf.len())
			},
			Err(TrySendError::Full(_)) => Err(io::Error::from(io::ErrorKind::WouldBlock)),
			Err(TrySendError::Disconnected(_)) => Err(io::Error::from(io::ErrorKind::BrokenPipe)),
		}
	}

	/// Serialize libslirp's guest-visible state on the owning slirp thread.
	pub fn save_state(&self) -> Result<Vec<u8>> {
		let (reply_tx, reply_rx) = flume::bounded(1);
		self
			.tx
			.send(Command::Save(self.restricted_egress, self.allowed_host_port, reply_tx))
			.map_err(|_| err("saving user-net state failed: slirp thread gone"))?;
		self
			.command_evt
			.write(1)
			.map_err(|e| err(format!("waking user-net slirp thread for state save: {e}")))?;
		reply_rx
			.recv()
			.map_err(|_| err("saving user-net state failed: slirp thread exited"))?
	}

	/// Reject TAP offloads; libslirp consumes plain Ethernet frames.
	#[expect(
		clippy::unused_self,
		reason = "all net backends expose the same instance-shaped offload API"
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

	/// Return offload flags this backend can safely advertise.
	#[expect(
		clippy::unused_self,
		reason = "all net backends expose the same instance-shaped offload API"
	)]
	pub const fn supported_offloads(&self) -> u32 {
		0
	}

	/// Return the readiness fd polled by the virtio worker for RX packets.
	pub fn as_raw_fd(&self) -> RawFd {
		self.packet_evt.as_raw_fd()
	}
}

impl Drop for UserNet {
	fn drop(&mut self) {
		let mut stop = Command::Stop;
		loop {
			match self.tx.try_send(stop) {
				Ok(()) => {
					let _ = self.command_evt.write(1);
					break;
				},
				Err(TrySendError::Full(cmd)) => {
					stop = cmd;
					let _ = self.command_evt.write(1);
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
		if let Some(filter) = &self.filter {
			filter.lock().observe(buf, Instant::now());
		}
		let mut packets = self.packets.lock();
		if packets.len() >= MAX_PENDING_RX_FRAMES {
			packets.pop_front();
		}
		packets.push_back(buf.to_vec());
		drop(packets);
		self.packet_evt.write(1)?;
		Ok(buf.len())
	}

	fn register_poll_fd(&mut self, _fd: RawFd) {}

	fn unregister_poll_fd(&mut self, _fd: RawFd) {}

	fn guest_error(&mut self, msg: &str) {
		tracing::warn!("libslirp guest error: {msg}");
	}

	fn notify(&mut self) {
		let _ = self.loop_evt.write(1);
	}

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
			// libslirp passes an absolute deadline in milliseconds on the same
			// timebase as clock_get_ns(), not a relative delay.
			Some(
				self
					.start
					.checked_add(Duration::from_millis(expire_time as u64))
					.unwrap_or_else(Instant::now),
			)
		};
		let _ = self.loop_evt.write(1);
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
			.filter_map(|timer| timer.as_ref().and_then(|timer| timer.expires_at))
			.map(|expires| expires.saturating_duration_since(now))
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
	command_evt: EventFd,
	handler_loop_evt: EventFd,
	packet_evt: EventFd,
	packets: Arc<Mutex<VecDeque<Vec<u8>>>>,
	restricted_egress: bool,
	filter: Option<SharedFilter>,
	state: Option<Vec<u8>>,
	ready: Sender<Result<()>>,
) {
	let handler = Rc::new(RefCell::new(SlirpHandler {
		start: Instant::now(),
		packets,
		packet_evt,
		loop_evt: handler_loop_evt,
		timers: Vec::new(),
		filter,
	}));
	let context = Context::new(
		restricted_egress,
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

		let timeout = merge_timeout(timeout, handler.borrow().next_timer_timeout());
		let mut pollfds = Vec::with_capacity(1 + slirp_fds.len());
		pollfds.push(libc::pollfd {
			fd:      command_evt.as_raw_fd(),
			events:  libc::POLLIN,
			revents: 0,
		});
		for &(fd, events) in &slirp_fds {
			pollfds.push(libc::pollfd { fd, events: to_poll_events(events), revents: 0 });
		}

		// SAFETY: pollfds points to a mutable contiguous array of pollfd entries,
		// len matches that array, and timeout is the clamped poll timeout.
		let ret = unsafe { libc::poll(pollfds.as_mut_ptr(), pollfds.len() as libc::nfds_t, timeout) };
		if ret < 0 {
			let error = io::Error::last_os_error();
			if error.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			tracing::warn!("libslirp poll failed: {error}");
			return;
		}

		if pollfds[0].revents != 0 {
			let _ = command_evt.read();
			if !drain_commands(&context, &rx) {
				return;
			}
		}

		let revents = pollfds
			.iter()
			.skip(1)
			.map(|pollfd| from_poll_events(pollfd.revents))
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
			Ok(Command::Save(restricted_egress, allowed_host_port, reply)) => {
				let _ = reply.send(save_slirp_state(context, restricted_egress, allowed_host_port));
			},
			Ok(Command::Stop) => return false,
			Err(TryRecvError::Empty) => return true,
			Err(TryRecvError::Disconnected) => return false,
		}
	}
}

fn save_slirp_state(
	context: &Context<Rc<RefCell<SlirpHandler>>>,
	restricted_egress: bool,
	allowed_host_port: Option<u16>,
) -> Result<Vec<u8>> {
	let mut state = state_version().to_be_bytes().to_vec();
	let mut slirp_state = context
		.state_get()
		.map_err(|e| err(format!("saving libslirp state: {e}")))?;
	state.append(&mut slirp_state);
	Ok(encode_user_net_state(restricted_egress, allowed_host_port, state))
}

fn encode_user_net_state(
	restricted_egress: bool,
	allowed_host_port: Option<u16>,
	slirp_state: Vec<u8>,
) -> Vec<u8> {
	let mut state = Vec::with_capacity(USER_NET_STATE_HEADER_BYTES + slirp_state.len());
	state.extend_from_slice(&USER_NET_STATE_MAGIC);
	state.push(USER_NET_STATE_FORMAT_VERSION);
	state.push(u8::from(restricted_egress));
	state.extend_from_slice(&allowed_host_port.unwrap_or(0).to_be_bytes());
	state.extend_from_slice(&slirp_state);
	state
}

/// Merge the requested policy with snapshot state without weakening a newly
/// requested restriction. A fresh credential gateway port supersedes a stale
/// snapshot port; otherwise persisted policy remains authoritative.
fn restriction_for_state(
	requested: bool,
	requested_host_port: Option<u16>,
	state: Option<&[u8]>,
) -> Result<(bool, Option<u16>)> {
	if requested {
		return Ok((true, requested_host_port));
	}
	state
		.map(|state| {
			split_user_net_state(state)
				.map(|(restricted_egress, allowed_host_port, _)| (restricted_egress, allowed_host_port))
		})
		.transpose()
		.map(|persisted| persisted.unwrap_or((false, None)))
}

fn split_user_net_state(data: &[u8]) -> Result<(bool, Option<u16>, &[u8])> {
	if data.len() < USER_NET_STATE_HEADER_BYTES {
		return Err(err("user-net state is missing its format header"));
	}
	if data[..USER_NET_STATE_MAGIC.len()] != USER_NET_STATE_MAGIC {
		return Err(err("user-net state has an unrecognized format"));
	}
	if data[4] != USER_NET_STATE_FORMAT_VERSION {
		return Err(err(format!("unsupported user-net state format {}", data[4])));
	}
	let restricted_egress = match data[5] {
		0 => false,
		1 => true,
		value => return Err(err(format!("invalid user-net restricted-egress state {value}"))),
	};
	let allowed_host_port = u16::from_be_bytes([data[6], data[7]]);
	let allowed_host_port = if restricted_egress {
		if allowed_host_port == 0 {
			return Err(err("restricted user-net state is missing its allowed host port"));
		}
		Some(allowed_host_port)
	} else {
		if allowed_host_port != 0 {
			return Err(err("unrestricted user-net state has an allowed host port"));
		}
		None
	};
	Ok((restricted_egress, allowed_host_port, &data[USER_NET_STATE_HEADER_BYTES..]))
}

/// Permit only DHCP and one configured TCP service on the virtual host
/// gateway.
fn allows_restricted_egress(frame: &[u8], allowed_host_port: Option<u16>) -> bool {
	if frame.len() < 14 {
		return false;
	}
	match u16::from_be_bytes([frame[12], frame[13]]) {
		ETHERTYPE_ARP => allows_restricted_arp(frame),
		ETHERTYPE_IPV4 => allows_restricted_ipv4(frame, allowed_host_port),
		_ => false,
	}
}

fn allows_restricted_arp(frame: &[u8]) -> bool {
	const ARP_HEADER_BYTES: usize = 28;
	const ARP_OFFSET: usize = 14;
	if frame.len() < ARP_OFFSET + ARP_HEADER_BYTES
		|| u16::from_be_bytes([frame[ARP_OFFSET + 6], frame[ARP_OFFSET + 7]]) != 1
	{
		return false;
	}
	let target = &frame[ARP_OFFSET + 24..ARP_OFFSET + 28];
	target == USER_HOST.octets().as_slice()
}

fn allows_restricted_ipv4(frame: &[u8], allowed_host_port: Option<u16>) -> bool {
	const IPV4_OFFSET: usize = 14;
	if frame.len() < IPV4_OFFSET + 20 || frame[IPV4_OFFSET] >> 4 != 4 {
		return false;
	}
	let header_bytes = usize::from(frame[IPV4_OFFSET] & 0x0f) * 4;
	if header_bytes < 20 || frame.len() < IPV4_OFFSET + header_bytes {
		return false;
	}
	let destination = &frame[IPV4_OFFSET + 16..IPV4_OFFSET + 20];
	if frame[IPV4_OFFSET + 9] == IPPROTO_UDP && frame.len() >= IPV4_OFFSET + header_bytes + 4 {
		let transport = &frame[IPV4_OFFSET + header_bytes..];
		let source_port = u16::from_be_bytes([transport[0], transport[1]]);
		let destination_port = u16::from_be_bytes([transport[2], transport[3]]);
		if source_port == DHCP_CLIENT_PORT
			&& destination_port == DHCP_SERVER_PORT
			&& (destination == [255, 255, 255, 255] || destination == [10, 0, 2, 255])
		{
			return true;
		}
	}
	if frame[IPV4_OFFSET + 9] != IPPROTO_TCP || frame.len() < IPV4_OFFSET + header_bytes + 4 {
		return false;
	}
	let destination_port = u16::from_be_bytes([
		frame[IPV4_OFFSET + header_bytes + 2],
		frame[IPV4_OFFSET + header_bytes + 3],
	]);
	destination == USER_HOST.octets().as_slice() && Some(destination_port) == allowed_host_port
}

fn load_slirp_state(context: &Context<Rc<RefCell<SlirpHandler>>>, data: &[u8]) -> Result<()> {
	if data.len() < SLIRP_STATE_VERSION_BYTES {
		return Err(err("libslirp state is missing its version header"));
	}
	let mut version = [0u8; SLIRP_STATE_VERSION_BYTES];
	version.copy_from_slice(&data[..SLIRP_STATE_VERSION_BYTES]);
	let version = i32::from_be_bytes(version);
	let current = state_version();
	if version > current {
		return Err(err(format!(
			"libslirp state version {version} is newer than supported {current}"
		)));
	}
	let mut reader = Cursor::new(&data[SLIRP_STATE_VERSION_BYTES..]);
	context
		.state_read(version, &mut reader)
		.map_err(|e| err(format!("loading libslirp state: {e}")))?;
	Ok(())
}

fn fire_due_timers(handler: &Rc<RefCell<SlirpHandler>>) {
	let callbacks = handler.borrow_mut().due_timers();
	for callback in callbacks {
		(callback.borrow_mut())();
	}
}

fn merge_timeout(slirp_timeout: u32, timer_timeout: Option<Duration>) -> libc::c_int {
	let timer_ms = timer_timeout.map(|timeout| timeout.as_millis().min(i32::MAX as u128) as i32);
	match (slirp_timeout, timer_ms) {
		(u32::MAX, None) => -1,
		(u32::MAX, Some(ms)) => ms,
		(ms, None) => ms.min(i32::MAX as u32) as i32,
		(ms, Some(timer_ms)) => (ms.min(i32::MAX as u32) as i32).min(timer_ms),
	}
}

fn to_poll_events(events: PollEvents) -> libc::c_short {
	let mut out = 0;
	if events.has_in() {
		out |= libc::POLLIN;
	}
	if events.has_out() {
		out |= libc::POLLOUT;
	}
	if events.has_err() {
		out |= libc::POLLERR;
	}
	if events.has_hup() {
		out |= libc::POLLHUP;
	}
	out
}

fn from_poll_events(revents: libc::c_short) -> PollEvents {
	let mut out = PollEvents::empty();
	if revents & libc::POLLIN != 0 {
		out |= PollEvents::poll_in();
	}
	if revents & libc::POLLOUT != 0 {
		out |= PollEvents::poll_out();
	}
	if revents & libc::POLLERR != 0 {
		out |= PollEvents::poll_err();
	}
	if revents & libc::POLLHUP != 0 {
		out |= PollEvents::poll_hup();
	}
	out
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ipv4_frame(
		protocol: u8,
		destination: [u8; 4],
		source_port: u16,
		destination_port: u16,
	) -> Vec<u8> {
		let mut frame = vec![0; 14 + 20 + 8];
		frame[12..14].copy_from_slice(&ETHERTYPE_IPV4.to_be_bytes());
		frame[14] = 0x45;
		frame[23] = protocol;
		frame[30..34].copy_from_slice(&destination);
		frame[34..36].copy_from_slice(&source_port.to_be_bytes());
		frame[36..38].copy_from_slice(&destination_port.to_be_bytes());
		frame
	}

	#[test]
	fn restricted_egress_only_admits_dhcp_and_configured_gateway_port() {
		let allowed_host_port = Some(8443);
		assert!(allows_restricted_egress(
			&ipv4_frame(IPPROTO_UDP, [255; 4], 68, 67),
			allowed_host_port
		));
		assert!(!allows_restricted_egress(
			&ipv4_frame(IPPROTO_UDP, USER_DNS.octets(), 1, 53),
			allowed_host_port
		));
		assert!(!allows_restricted_egress(
			&ipv4_frame(IPPROTO_TCP, USER_DNS.octets(), 1, 53),
			allowed_host_port
		));
		assert!(allows_restricted_egress(
			&ipv4_frame(IPPROTO_TCP, USER_HOST.octets(), 1, 8443),
			allowed_host_port
		));
		assert!(!allows_restricted_egress(
			&ipv4_frame(IPPROTO_TCP, USER_HOST.octets(), 1, 443),
			allowed_host_port
		));
		assert!(!allows_restricted_egress(
			&ipv4_frame(IPPROTO_TCP, [8, 8, 8, 8], 1, 443),
			allowed_host_port
		));
	}

	/// Regression: reading one frame used to clear the wake token even with
	/// frames left, so a budget-limited RX pass stranded the rest of the
	/// queue until libslirp happened to emit again (its retransmit timer:
	/// multi-second TCP stalls under load).
	#[test]
	fn rx_wake_stays_armed_until_queue_is_drained() {
		let net = UserNet::new_with_state([0x02, 0, 0, 0, 0, 1], None).expect("user net");
		let mut buf = vec![0u8; 2048];
		net.push_test_frame(b"one");
		net.push_test_frame(b"two");
		assert!(net.rx_wake_armed());
		assert_eq!(net.read(&mut buf).expect("first"), VNET_HDR_SIZE + 3);
		assert!(net.rx_wake_armed(), "a frame is still queued");
		assert_eq!(&buf[VNET_HDR_SIZE..VNET_HDR_SIZE + 3], b"one");
		assert_eq!(net.read(&mut buf).expect("second"), VNET_HDR_SIZE + 3);
		assert!(!net.rx_wake_armed(), "drained queue must not keep waking the worker");
		assert_eq!(net.read(&mut buf).expect_err("empty").kind(), io::ErrorKind::WouldBlock);

		// Parking RX keeps the frames but stops the wakeups.
		net.push_test_frame(b"three");
		net.defer_rx();
		assert!(!net.rx_wake_armed());
		assert_eq!(net.read(&mut buf).expect("kept frame"), VNET_HDR_SIZE + 5);
	}

	#[test]
	fn requested_restriction_overrides_snapshot_policy_and_gateway_port() {
		let restricted = encode_user_net_state(true, Some(8443), vec![0, 0, 0, 7]);
		assert_eq!(
			restriction_for_state(false, None, Some(&restricted)).expect("valid state"),
			(true, Some(8443))
		);
		assert_eq!(
			restriction_for_state(true, Some(9443), Some(&restricted)).expect("valid state"),
			(true, Some(9443))
		);

		let unrestricted = encode_user_net_state(false, None, vec![0, 0, 0, 7]);
		assert_eq!(
			restriction_for_state(true, Some(8443), Some(&unrestricted)).expect("valid state"),
			(true, Some(8443))
		);
	}
}
