//! virtio-net backends over host networking.
//!
//! TAP/vmnet RX/TX move a 12-byte `virtio_net_hdr_v1` (flags, `gso_type`,
//! `hdr_len`, `gso_size`, `csum_start`, `csum_offset`, `num_buffers`) between
//! the guest virtqueue and the host backend. Linux TAP forwards that header
//! verbatim to `/dev/net/tun`; macOS vmnet and user-mode networking strip or
//! synthesize a zero header because they consume plain Ethernet frames.

use std::{io::ErrorKind, sync::Arc};

use virtio_bindings::{
	bindings::{virtio_config::VIRTIO_F_VERSION_1, virtio_net::VIRTIO_NET_F_MAC},
	virtio_ids::VIRTIO_ID_NET,
};
use virtio_queue::{DescriptorChain, Queue, QueueT, desc::split::Descriptor as QueueDescriptor};
use vm_memory::{Address, Bytes};

use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	tap::{TUN_F_CSUM, TUN_F_TSO4, TUN_F_TSO6, Tap, VNET_HDR_SIZE},
	virtio::{
		Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, WorkerWaitSource,
		descriptor_range_valid,
	},
};
#[cfg(target_os = "macos")]
pub mod user;
#[cfg(target_os = "windows")]
mod user_windows;
#[cfg(target_os = "windows")]
use user_windows as user;

const QUEUE_SIZE: u16 = 256;
const NUM_QUEUES: usize = 2;
const RX_QUEUE: usize = 0;
const TX_QUEUE: usize = 1;
const VIRTIO_NET_F_CSUM_BIT: u64 = 0;
const VIRTIO_NET_F_GUEST_CSUM_BIT: u64 = 1;
const VIRTIO_NET_F_GUEST_TSO4_BIT: u64 = 7;
const VIRTIO_NET_F_GUEST_TSO6_BIT: u64 = 8;
const VIRTIO_NET_F_HOST_TSO4_BIT: u64 = 11;
const VIRTIO_NET_F_HOST_TSO6_BIT: u64 = 12;
const VIRTIO_NET_F_MRG_RXBUF_BIT: u64 = 15;
const NUM_BUFFERS_OFFSET: usize = 10;
/// Max packet we read from/write to the tap, including the virtio-net header.
const FRAME_BUF_SIZE: usize = VNET_HDR_SIZE + 65_536;
const MAX_TX_CHAIN_SIZE: usize = FRAME_BUF_SIZE;
const CONFIG_SPACE_SIZE: usize = 12;

pub struct Net {
	backend:        Backend,
	features:       u64,
	acked_features: u64,
	config:         Vec<u8>,
	queue_sizes:    Vec<u16>,
	rx_buf:         Box<[u8]>,

	mem:       Option<GuestMemoryMmap>,
	interrupt: Option<Arc<Interrupt>>,
	rx_queue:  Option<Queue>,
	tx_queue:  Option<Queue>,
}

enum Backend {
	Tap(Tap),
	#[cfg(any(target_os = "macos", target_os = "windows"))]
	User(user::UserNet),
}

impl Backend {
	fn read(&self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			Self::Tap(tap) => tap.read(buf),
			#[cfg(any(target_os = "macos", target_os = "windows"))]
			Self::User(user) => user.read(buf),
		}
	}

	fn write(&self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			Self::Tap(tap) => tap.write(buf),
			#[cfg(any(target_os = "macos", target_os = "windows"))]
			Self::User(user) => user.write(buf),
		}
	}

	fn set_offloads(&self, offloads: u32) -> Result<()> {
		match self {
			Self::Tap(tap) => tap.set_offloads(offloads),
			#[cfg(any(target_os = "macos", target_os = "windows"))]
			Self::User(user) => user.set_offloads(offloads),
		}
	}

	const fn supported_offloads(&self) -> u32 {
		match self {
			Self::Tap(tap) => tap.supported_offloads(),
			#[cfg(any(target_os = "macos", target_os = "windows"))]
			Self::User(user) => user.supported_offloads(),
		}
	}

	#[cfg(unix)]
	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		match self {
			Self::Tap(tap) => vec![tap.as_raw_fd()],
			#[cfg(target_os = "macos")]
			Self::User(user) => vec![user.as_raw_fd()],
		}
	}

	#[cfg(target_os = "windows")]
	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		match self {
			Self::Tap(tap) => vec![tap.as_raw_handle()],
			Self::User(user) => vec![user.as_raw_handle()],
		}
	}

	/// Whether undeliverable RX frames must stay queued in the backend until
	/// the guest posts receive buffers (macOS libslirp) rather than being
	/// dropped (TAP: the host kernel's queue and TCP stacks absorb loss).
	const fn holds_rx(&self) -> bool {
		match self {
			Self::Tap(_) => false,
			#[cfg(target_os = "macos")]
			Self::User(_) => true,
			#[cfg(target_os = "windows")]
			Self::User(_) => false,
		}
	}

	/// Stop waking the worker for queued RX frames until the next guest queue
	/// notification (only called when [`Self::holds_rx`]).
	#[cfg_attr(
		not(target_os = "macos"),
		expect(clippy::missing_const_for_fn, reason = "only the macOS arm calls into libslirp")
	)]
	fn defer_rx(&self) {
		match self {
			Self::Tap(_) => {},
			#[cfg(target_os = "macos")]
			Self::User(user) => user.defer_rx(),
			#[cfg(target_os = "windows")]
			Self::User(_) => {},
		}
	}

	#[allow(
		clippy::unnecessary_wraps,
		reason = "user networking state serialization can fail on macOS and Windows"
	)]
	fn user_net_state(&self) -> Result<Option<Vec<u8>>> {
		match self {
			Self::Tap(_) => Ok(None),
			#[cfg(any(target_os = "macos", target_os = "windows"))]
			Self::User(user) => Ok(Some(user.save_state()?)),
		}
	}
}

impl Net {
	pub fn new(tap: Tap, mac: [u8; 6]) -> Self {
		Self::with_backend(Backend::Tap(tap), mac)
	}

	#[cfg(any(target_os = "macos", target_os = "windows"))]
	pub fn new_user_with_state(mac: [u8; 6], state: Option<Vec<u8>>) -> Result<Self> {
		Ok(Self::with_backend(Backend::User(user::UserNet::new_with_state(mac, state)?), mac))
	}

	/// Construct a user-net backend whose guest egress is governed by the
	/// live policy filter (`--user-net-policy`).
	#[cfg(target_os = "macos")]
	pub fn new_filtered_user_with_state(
		mac: [u8; 6],
		filter: user::SharedFilter,
		state: Option<Vec<u8>>,
	) -> Result<Self> {
		Ok(Self::with_backend(
			Backend::User(user::UserNet::new_filtered_with_state(mac, filter, state)?),
			mac,
		))
	}

	/// Construct a user-net backend that blocks guest egress except slirp
	/// services.
	#[cfg(target_os = "macos")]
	pub fn new_restricted_user_with_state(
		mac: [u8; 6],
		allowed_host_port: u16,
		state: Option<Vec<u8>>,
	) -> Result<Self> {
		Ok(Self::with_backend(
			Backend::User(user::UserNet::new_restricted_with_state(mac, allowed_host_port, state)?),
			mac,
		))
	}

	fn with_backend(backend: Backend, mac: [u8; 6]) -> Self {
		let features = net_features_for_offloads(backend.supported_offloads());
		let mut config = vec![0u8; CONFIG_SPACE_SIZE];
		config[..6].copy_from_slice(&mac);
		Self {
			backend,
			features,
			acked_features: 0,
			config,
			queue_sizes: vec![QUEUE_SIZE; NUM_QUEUES],
			rx_buf: vec![0u8; FRAME_BUF_SIZE].into_boxed_slice(),
			mem: None,
			interrupt: None,
			rx_queue: None,
			tx_queue: None,
		}
	}

	fn process_tx(&mut self) -> Result<QueuePass> {
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			return Ok(QueuePass::Drained);
		};
		let Some(tx_queue) = self.tx_queue.as_mut() else {
			return Ok(QueuePass::Drained);
		};
		let backend = &mut self.backend;

		// Bound one pass: TX chains complete inline, so a spinning guest could
		// otherwise keep the available ring non-empty forever and starve the
		// shared worker's other devices.
		let mut budget = QUEUE_PASS_BUDGET;
		let mut signalled = false;
		while budget != 0 {
			let Some(chain) = tx_queue.pop_descriptor_chain(&mem) else {
				break;
			};
			budget -= 1;
			let head = chain.head_index();
			if let Some(packet) = gather_tx_frame(&mem, chain)
				&& packet.len() > VNET_HDR_SIZE
			{
				match backend.write(&packet) {
					Ok(n) if n == packet.len() => {},
					Ok(n) => {
						return Err(err(format!(
							"virtio-net backend short write: {n}/{} bytes",
							packet.len()
						)));
					},
					Err(e) if e.kind() == ErrorKind::WouldBlock => {},
					Err(e) => return Err(err(format!("virtio-net backend write failed: {e}"))),
				}
			}
			tx_queue
				.add_used(&mem, head, 0)
				.map_err(|e| err(format!("virtio-net TX used-ring update failed: {e}")))?;
			signalled = true;
		}
		if signalled {
			interrupt.signal_used_queue()?;
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn process_rx(&mut self) -> Result<()> {
		let (Some(mem), Some(interrupt)) = (self.mem.clone(), self.interrupt.clone()) else {
			drain_backend(&self.backend);
			return Ok(());
		};
		let mergeable = self.mergeable_rx_buffers();
		let Some(rx_queue) = self.rx_queue.as_mut() else {
			drain_backend(&self.backend);
			return Ok(());
		};
		if rx_pass(&self.backend, &mem, rx_queue, &mut self.rx_buf, mergeable)? {
			interrupt.signal_used_queue()?;
		}
		Ok(())
	}

	const fn mergeable_rx_buffers(&self) -> bool {
		feature_acked(self.acked_features, VIRTIO_NET_F_MRG_RXBUF_BIT)
	}
}

impl VirtioDevice for Net {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_NET
	}

	fn queue_max_sizes(&self) -> &[u16] {
		&self.queue_sizes
	}

	fn features(&self) -> u64 {
		self.features
	}

	fn ack_features(&mut self, value: u64) {
		self.acked_features = value & self.features;
		let _ = self
			.backend
			.set_offloads(tap_offload_flags(self.acked_features));
	}

	fn read_config(&self, offset: u64, data: &mut [u8]) {
		let offset = offset as usize;
		for (i, b) in data.iter_mut().enumerate() {
			*b = self.config.get(offset + i).copied().unwrap_or(0);
		}
	}

	fn write_config(&mut self, _offset: u64, _data: &[u8]) {}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		self
			.backend
			.set_offloads(tap_offload_flags(self.acked_features))?;
		let mut queues = queues.into_iter();
		self.rx_queue = queues.next();
		self.tx_queue = queues.next();
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		self.acked_features = 0;
		let _ = self.backend.set_offloads(0);
		self.mem = None;
		self.interrupt = None;
		self.rx_queue = None;
		self.tx_queue = None;
		Ok(())
	}

	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		self.backend.worker_wait_sources()
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		// A single ioeventfd backs both queues; the driver notifies after
		// queueing TX frames (and RX buffers). RX is driven by backend
		// readability, except that a backend holding frames for an empty RX
		// ring resumes when the guest posts buffers (this notification).
		let _ = (RX_QUEUE, TX_QUEUE);
		let pass = self.process_tx()?;
		if self.backend.holds_rx() {
			self.process_rx()?;
		}
		Ok(pass)
	}

	fn process_worker_source(&mut self, _index: usize) -> Result<()> {
		self.process_rx()
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		// Order must mirror the transport's queue vec: rx (0) then tx (1).
		let mut v = Vec::new();
		if let Some(q) = &self.rx_queue {
			v.push(q.state());
		}
		if let Some(q) = &self.tx_queue {
			v.push(q.state());
		}
		v
	}

	fn user_net_state(&self) -> Result<Option<Vec<u8>>> {
		self.backend.user_net_state()
	}
}

/// Concatenate a TX chain's readable descriptors into one buffer: the 12-byte
/// `virtio_net_hdr_v1` the guest wrote (GSO/checksum fields) followed by the
/// Ethernet frame. The header is forwarded to the TAP verbatim — its layout
/// matches the kernel vnet header exactly and `num_buffers` is
/// device-write-only (ignored by the TAP on writes), so the TX path needs no
/// per-field parsing.
fn gather_tx_frame(
	mem: &GuestMemoryMmap,
	chain: DescriptorChain<&GuestMemoryMmap>,
) -> Option<Vec<u8>> {
	let descs: Vec<_> = chain.collect();
	let mut total = 0usize;
	for d in &descs {
		if d.is_write_only() || !descriptor_range_valid(mem, d.addr(), d.len()) {
			return None;
		}
		total = total.checked_add(d.len() as usize)?;
		if total > MAX_TX_CHAIN_SIZE {
			return None;
		}
	}
	if total < VNET_HDR_SIZE {
		return None;
	}

	let mut out = Vec::new();
	out.try_reserve_exact(total).ok()?;
	for d in descs {
		let len = d.len() as usize;
		let start = out.len();
		out.resize(start + len, 0);
		if mem
			.read_slice(&mut out[start..start + len], d.addr())
			.is_err()
		{
			return None;
		}
	}
	Some(out)
}

struct RxBuffer {
	head:  u16,
	descs: Vec<QueueDescriptor>,
	cap:   usize,
	valid: bool,
}

/// Write `packet` (including the tap-provided virtio-net header) into RX
/// chains. Returns false if no RX buffer was available.
fn add_rx_used(mem: &GuestMemoryMmap, rx_queue: &mut Queue, head: u16, len: u32) -> Result<()> {
	rx_queue
		.add_used(mem, head, len)
		.map_err(|e| err(format!("virtio-net RX used-ring update failed: {e}")))
}

fn deliver_rx(
	mem: &GuestMemoryMmap,
	rx_queue: &mut Queue,
	packet: &[u8],
	mergeable: bool,
) -> Result<bool> {
	if packet.len() < VNET_HDR_SIZE {
		return Ok(false);
	}
	let Some(first) = pop_rx_buffer(mem, rx_queue) else {
		return Ok(false);
	};

	if !mergeable {
		let ok = first.valid
			&& first.cap >= packet.len()
			&& write_packet(mem, std::slice::from_ref(&first), packet, 1).is_some();
		if !ok {
			add_rx_used(mem, rx_queue, first.head, 0)?;
			return Ok(true);
		}
		add_rx_used(mem, rx_queue, first.head, packet.len() as u32)?;
		return Ok(true);
	}

	let mut buffers = vec![first];
	if !buffers[0].valid {
		add_rx_used(mem, rx_queue, buffers[0].head, 0)?;
		return Ok(true);
	}
	let mut cap = buffers[0].cap;
	while cap < packet.len() {
		let Some(buf) = pop_rx_buffer(mem, rx_queue) else {
			for b in buffers {
				add_rx_used(mem, rx_queue, b.head, 0)?;
			}
			return Ok(true);
		};
		let Some(next_cap) = cap.checked_add(buf.cap) else {
			for b in buffers {
				add_rx_used(mem, rx_queue, b.head, 0)?;
			}
			add_rx_used(mem, rx_queue, buf.head, 0)?;
			return Ok(true);
		};
		cap = next_cap;
		if !buf.valid {
			for b in buffers {
				add_rx_used(mem, rx_queue, b.head, 0)?;
			}
			add_rx_used(mem, rx_queue, buf.head, 0)?;
			return Ok(true);
		}
		buffers.push(buf);
	}

	let num_buffers = buffers.len().min(u16::MAX as usize) as u16;
	let Some(used_lens) = write_packet(mem, &buffers, packet, num_buffers) else {
		for b in buffers {
			add_rx_used(mem, rx_queue, b.head, 0)?;
		}
		return Ok(true);
	};

	for (b, used_len) in buffers.into_iter().zip(used_lens) {
		add_rx_used(mem, rx_queue, b.head, used_len)?;
	}
	Ok(true)
}

/// Move backend frames into guest RX buffers; returns whether any used
/// entry was added (the caller then signals the guest).
fn rx_pass(
	backend: &Backend,
	mem: &GuestMemoryMmap,
	rx_queue: &mut Queue,
	buf: &mut [u8],
	mergeable: bool,
) -> Result<bool> {
	let mut signalled = false;
	// One bounded pass on unix: the tap/user backend wait source is a
	// level-triggered fd, so returning early re-fires poll and lets the
	// shared device loop service other devices in between. Windows keeps
	// one worker thread per device with an auto-reset event source and
	// drains fully, exactly as before.
	#[cfg(unix)]
	let mut budget = QUEUE_PASS_BUDGET;
	loop {
		#[cfg(unix)]
		{
			if budget == 0 {
				break;
			}
			budget -= 1;
		}
		if backend.holds_rx() && !rx_buffer_available(mem, rx_queue) {
			// Keep the frames; process_queue_notify resumes RX once the
			// guest refills the ring.
			backend.defer_rx();
			break;
		}
		match backend.read(buf) {
			Ok(0) => break,
			Ok(n) => {
				// A TAP frame without an RX buffer is dropped (and the backend
				// is still drained, so we never busy-loop on epoll).
				if n >= VNET_HDR_SIZE && deliver_rx(mem, rx_queue, &buf[..n], mergeable)? {
					signalled = true;
				}
			},
			Err(e) if e.kind() == ErrorKind::WouldBlock => break,
			Err(e) => return Err(err(format!("virtio-net backend read failed: {e}"))),
		}
	}
	Ok(signalled)
}

/// Whether the guest has posted at least one RX buffer not yet consumed.
/// An unreadable ring counts as available so the read path reports it.
fn rx_buffer_available(mem: &GuestMemoryMmap, rx_queue: &Queue) -> bool {
	rx_queue
		.avail_idx(mem, std::sync::atomic::Ordering::Acquire)
		.map_or(true, |avail| avail.0 != rx_queue.next_avail())
}
fn pop_rx_buffer(mem: &GuestMemoryMmap, rx_queue: &mut Queue) -> Option<RxBuffer> {
	let chain = rx_queue.pop_descriptor_chain(mem)?;
	let head = chain.head_index();
	let descs: Vec<_> = chain.collect();
	let mut cap = 0usize;
	for d in &descs {
		if !d.is_write_only() || !descriptor_range_valid(mem, d.addr(), d.len()) {
			return Some(RxBuffer { head, descs, cap: 0, valid: false });
		}
		let Some(next_cap) = cap.checked_add(d.len() as usize) else {
			return Some(RxBuffer { head, descs, cap: 0, valid: false });
		};
		cap = next_cap;
	}
	Some(RxBuffer { head, descs, cap, valid: true })
}

/// Copy `packet` (tap-provided header + frame) into one or more RX descriptor
/// chains, returning the bytes written per chain. `num_buffers` is stamped into
/// the delivered virtio-net header (merge count, or 1 when not mergeable).
fn write_packet(
	mem: &GuestMemoryMmap,
	buffers: &[RxBuffer],
	packet: &[u8],
	num_buffers: u16,
) -> Option<Vec<u32>> {
	let mut used_lens = Vec::with_capacity(buffers.len());
	let mut packet_offset = 0usize;
	let mut header = [0u8; VNET_HDR_SIZE];
	header.copy_from_slice(&packet[..VNET_HDR_SIZE]);
	// The TAP fills flags/gso_type/hdr_len/gso_size/csum_start/csum_offset; the
	// device owns num_buffers (device-write-only on RX). Always stamp it: the
	// merge count under MRG_RXBUF, or 1 per the VERSION_1 header rule — never
	// the stale value left in the read buffer.
	header[NUM_BUFFERS_OFFSET..VNET_HDR_SIZE].copy_from_slice(&num_buffers.to_le_bytes());

	for b in buffers {
		if !b.valid {
			return None;
		}
		let mut used = 0usize;
		for d in &b.descs {
			if packet_offset >= packet.len() {
				break;
			}
			let desc_len = d.len() as usize;
			let n = desc_len.min(packet.len() - packet_offset);
			let src = if packet_offset < VNET_HDR_SIZE {
				let hdr_n = n.min(VNET_HDR_SIZE - packet_offset);
				mem.write_slice(&header[packet_offset..packet_offset + hdr_n], d.addr())
					.ok()?;
				if hdr_n == n {
					packet_offset += n;
					used += n;
					continue;
				}
				mem.write_slice(
					&packet[VNET_HDR_SIZE..VNET_HDR_SIZE + (n - hdr_n)],
					d.addr().unchecked_add(hdr_n as u64),
				)
				.ok()?;
				packet_offset += n;
				used += n;
				continue;
			} else {
				&packet[packet_offset..packet_offset + n]
			};
			mem.write_slice(src, d.addr()).ok()?;
			packet_offset += n;
			used += n;
		}
		used_lens.push(used as u32);
	}
	if packet_offset == packet.len() {
		Some(used_lens)
	} else {
		None
	}
}

const fn feature_bit(bit: u64) -> u64 {
	1u64 << bit
}

const fn feature_acked(features: u64, bit: u64) -> bool {
	features & feature_bit(bit) != 0
}

const fn net_features_for_offloads(offloads: u32) -> u64 {
	let mut features = (1u64 << VIRTIO_F_VERSION_1)
		| (1u64 << VIRTIO_NET_F_MAC)
		| feature_bit(VIRTIO_NET_F_MRG_RXBUF_BIT);
	if offloads & TUN_F_CSUM != 0 {
		features |= feature_bit(VIRTIO_NET_F_CSUM_BIT) | feature_bit(VIRTIO_NET_F_GUEST_CSUM_BIT);
	}
	if offloads & TUN_F_TSO4 != 0 {
		features |=
			feature_bit(VIRTIO_NET_F_GUEST_TSO4_BIT) | feature_bit(VIRTIO_NET_F_HOST_TSO4_BIT);
	}
	if offloads & TUN_F_TSO6 != 0 {
		features |=
			feature_bit(VIRTIO_NET_F_GUEST_TSO6_BIT) | feature_bit(VIRTIO_NET_F_HOST_TSO6_BIT);
	}
	features
}

const fn tap_offload_flags(features: u64) -> u32 {
	let mut flags = 0;
	if feature_acked(features, VIRTIO_NET_F_CSUM_BIT)
		|| feature_acked(features, VIRTIO_NET_F_GUEST_CSUM_BIT)
		|| feature_acked(features, VIRTIO_NET_F_HOST_TSO4_BIT)
		|| feature_acked(features, VIRTIO_NET_F_HOST_TSO6_BIT)
		|| feature_acked(features, VIRTIO_NET_F_GUEST_TSO4_BIT)
		|| feature_acked(features, VIRTIO_NET_F_GUEST_TSO6_BIT)
	{
		flags |= TUN_F_CSUM;
	}
	if feature_acked(features, VIRTIO_NET_F_HOST_TSO4_BIT)
		|| feature_acked(features, VIRTIO_NET_F_GUEST_TSO4_BIT)
	{
		flags |= TUN_F_TSO4;
	}
	if feature_acked(features, VIRTIO_NET_F_HOST_TSO6_BIT)
		|| feature_acked(features, VIRTIO_NET_F_GUEST_TSO6_BIT)
	{
		flags |= TUN_F_TSO6;
	}
	flags
}

/// Read and discard pending frames so epoll does not keep firing.
fn drain_backend(backend: &Backend) {
	let mut scratch = vec![0u8; FRAME_BUF_SIZE].into_boxed_slice();
	loop {
		match backend.read(&mut scratch) {
			Ok(n) if n > 0 => {},
			_ => break,
		}
	}
}

#[cfg(test)]
mod tests {
	use virtio_bindings::bindings::virtio_ring::{VRING_DESC_F_NEXT, VRING_DESC_F_WRITE};
	use virtio_queue::{
		QueueT,
		desc::{RawDescriptor, split::Descriptor as SplitDescriptor},
	};
	use vm_memory::{Bytes, GuestAddress};

	use super::*;

	const MEM_SIZE: usize = 0x1_0000;
	const DESC_TABLE: u64 = 0x1000;
	const AVAIL_RING: u64 = 0x2000;
	const USED_RING: u64 = 0x3000;

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), MEM_SIZE)]).expect("guest memory")
	}

	fn queue_with_descs(mem: &GuestMemoryMmap, descs: &[SplitDescriptor]) -> Queue {
		queue_with_heads(mem, descs, &[0])
	}

	fn queue_with_heads(mem: &GuestMemoryMmap, descs: &[SplitDescriptor], heads: &[u16]) -> Queue {
		for (idx, desc) in descs.iter().enumerate() {
			mem.write_obj(RawDescriptor::from(*desc), GuestAddress(DESC_TABLE + (idx as u64 * 16)))
				.expect("write descriptor");
		}
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING))
			.expect("write avail flags");
		mem.write_obj(u16::to_le(heads.len() as u16), GuestAddress(AVAIL_RING + 2))
			.expect("write avail idx");
		for (idx, head) in heads.iter().enumerate() {
			mem.write_obj(u16::to_le(*head), GuestAddress(AVAIL_RING + 4 + (idx as u64 * 2)))
				.expect("write avail head");
		}

		let mut queue = Queue::new(8).expect("queue");
		queue.set_size(8);
		queue.set_desc_table_address(Some(DESC_TABLE as u32), Some((DESC_TABLE >> 32) as u32));
		queue.set_avail_ring_address(Some(AVAIL_RING as u32), Some((AVAIL_RING >> 32) as u32));
		queue.set_used_ring_address(Some(USED_RING as u32), Some((USED_RING >> 32) as u32));
		queue.set_ready(true);
		queue
	}

	fn used_idx(mem: &GuestMemoryMmap) -> u16 {
		u16::from_le(
			mem.read_obj::<u16>(GuestAddress(USED_RING + 2))
				.expect("read used idx"),
		)
	}

	fn used_len(mem: &GuestMemoryMmap) -> u32 {
		u32::from_le(
			mem.read_obj::<u32>(GuestAddress(USED_RING + 8))
				.expect("read used len"),
		)
	}

	fn used_len_at(mem: &GuestMemoryMmap, index: u64) -> u32 {
		u32::from_le(
			mem.read_obj::<u32>(GuestAddress(USED_RING + 8 + (index * 8)))
				.expect("read used len"),
		)
	}

	fn rx_packet(payload: &[u8]) -> Vec<u8> {
		let mut packet = vec![0u8; VNET_HDR_SIZE];
		packet.extend_from_slice(payload);
		packet
	}

	#[test]
	fn tap_offload_flags_track_negotiated_features() {
		assert_eq!(tap_offload_flags(0), 0);
		assert_eq!(tap_offload_flags(feature_bit(VIRTIO_NET_F_CSUM_BIT)), TUN_F_CSUM);
		assert_eq!(
			tap_offload_flags(feature_bit(VIRTIO_NET_F_HOST_TSO4_BIT)),
			TUN_F_CSUM | TUN_F_TSO4
		);
		assert_eq!(
			tap_offload_flags(feature_bit(VIRTIO_NET_F_GUEST_TSO6_BIT)),
			TUN_F_CSUM | TUN_F_TSO6
		);
	}

	#[test]
	fn net_features_drop_checksum_and_tso_without_tap_offloads() {
		let features = net_features_for_offloads(0);

		assert!(feature_acked(features, u64::from(VIRTIO_F_VERSION_1)));
		assert!(feature_acked(features, u64::from(VIRTIO_NET_F_MAC)));
		assert!(feature_acked(features, VIRTIO_NET_F_MRG_RXBUF_BIT));
		assert!(!feature_acked(features, VIRTIO_NET_F_CSUM_BIT));
		assert!(!feature_acked(features, VIRTIO_NET_F_GUEST_CSUM_BIT));
		assert!(!feature_acked(features, VIRTIO_NET_F_HOST_TSO4_BIT));
		assert!(!feature_acked(features, VIRTIO_NET_F_GUEST_TSO6_BIT));
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn restricted_user_net_state_restores_its_egress_policy() {
		let mac = [0x02, 0, 0, 0, 0, 1];
		let net = Net::new_restricted_user_with_state(mac, 8443, None).expect("restricted user net");
		let state = net
			.user_net_state()
			.expect("save restricted user net")
			.expect("user net state");
		assert_eq!(&state[..8], b"VMUN\x02\x01\x20\xfb");
		drop(net);

		let restored = Net::new_user_with_state(mac, Some(state)).expect("restore user net");
		let restored_state = restored
			.user_net_state()
			.expect("save restored user net")
			.expect("user net state");
		assert_eq!(&restored_state[..8], b"VMUN\x02\x01\x20\xfb");
	}

	#[test]
	fn net_features_advertise_linux_tap_offloads() {
		let features = net_features_for_offloads(TUN_F_CSUM | TUN_F_TSO4 | TUN_F_TSO6);

		assert!(feature_acked(features, VIRTIO_NET_F_CSUM_BIT));
		assert!(feature_acked(features, VIRTIO_NET_F_GUEST_CSUM_BIT));
		assert!(feature_acked(features, VIRTIO_NET_F_HOST_TSO4_BIT));
		assert!(feature_acked(features, VIRTIO_NET_F_GUEST_TSO4_BIT));
		assert!(feature_acked(features, VIRTIO_NET_F_HOST_TSO6_BIT));
		assert!(feature_acked(features, VIRTIO_NET_F_GUEST_TSO6_BIT));
	}

	#[test]
	fn gather_tx_frame_accepts_readable_descriptors() {
		let mem = guest_mem();
		let header = [0xa5u8; VNET_HDR_SIZE];
		mem.write_slice(&header, GuestAddress(0x4000))
			.expect("write virtio-net header");
		mem.write_slice(b"eth", GuestAddress(0x5000))
			.expect("write frame");
		let mut queue = queue_with_descs(&mem, &[
			SplitDescriptor::new(0x4000, VNET_HDR_SIZE as u32, VRING_DESC_F_NEXT as u16, 1),
			SplitDescriptor::new(0x5000, 3, 0, 0),
		]);

		let chain = queue.pop_descriptor_chain(&mem).expect("descriptor chain");
		let frame = gather_tx_frame(&mem, chain).expect("valid TX frame");

		assert_eq!(&frame[..VNET_HDR_SIZE], &header);
		assert_eq!(&frame[VNET_HDR_SIZE..], b"eth");
	}

	#[test]
	fn gather_tx_frame_rejects_write_only_descriptor() {
		let mem = guest_mem();
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			0x4000,
			VNET_HDR_SIZE as u32,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);

		let chain = queue.pop_descriptor_chain(&mem).expect("descriptor chain");
		assert!(gather_tx_frame(&mem, chain).is_none());
	}

	#[test]
	fn gather_tx_frame_rejects_out_of_range_descriptor() {
		let mem = guest_mem();
		let bad_addr = MEM_SIZE as u64 - VNET_HDR_SIZE as u64 + 1;
		let mut queue =
			queue_with_descs(&mem, &[SplitDescriptor::new(bad_addr, VNET_HDR_SIZE as u32, 0, 0)]);

		let chain = queue.pop_descriptor_chain(&mem).expect("descriptor chain");
		assert!(gather_tx_frame(&mem, chain).is_none());
	}

	#[test]
	fn deliver_rx_ignores_malformed_short_backend_frame_without_consuming_buffer() {
		let mem = guest_mem();
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			0x4000,
			(VNET_HDR_SIZE + 4) as u32,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);

		assert!(!deliver_rx(&mem, &mut queue, &[0u8; VNET_HDR_SIZE - 1], false).expect("deliver_rx"));
		assert_eq!(used_idx(&mem), 0);
	}

	#[test]
	fn deliver_rx_rejects_readable_descriptor_without_writing_payload() {
		let mem = guest_mem();
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(0x4000, 64, 0, 0)]);

		let packet = rx_packet(b"frame");
		assert!(deliver_rx(&mem, &mut queue, &packet, false).expect("deliver_rx"));

		let mut written = [0u8; 16];
		mem.read_slice(&mut written, GuestAddress(0x4000))
			.expect("read rx buffer");
		assert_eq!(written, [0u8; 16]);
		assert_eq!(used_idx(&mem), 1);
		assert_eq!(used_len(&mem), 0);
	}

	#[test]
	fn deliver_rx_rejects_out_of_range_descriptor() {
		let mem = guest_mem();
		let bad_addr = MEM_SIZE as u64 - 8;
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			bad_addr,
			64,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);

		let packet = rx_packet(b"frame");
		assert!(deliver_rx(&mem, &mut queue, &packet, false).expect("deliver_rx"));
		assert_eq!(used_idx(&mem), 1);
		assert_eq!(used_len(&mem), 0);
	}

	/// Regression: with the guest RX ring empty, user-net frames used to be
	/// read and dropped; libslirp's TCP then stalled for its retransmission
	/// backoff (pip/curl body truncations, 20–150 s stalls under load). The
	/// frame must stay queued without waking the worker, and be delivered
	/// once the guest posts a buffer.
	#[cfg(target_os = "macos")]
	#[test]
	fn user_rx_keeps_frames_while_guest_ring_is_empty() {
		let mem = guest_mem();
		let desc = SplitDescriptor::new(0x4000, 2048, VRING_DESC_F_WRITE as u16, 0);
		let mut queue = queue_with_heads(&mem, &[desc], &[]);
		let net = Net::new_user_with_state([0x02, 0, 0, 0, 0, 1], None).expect("user net");
		let Backend::User(user) = &net.backend else {
			unreachable!("user backend")
		};
		user.push_test_frame(b"frame");
		let mut buf = vec![0u8; FRAME_BUF_SIZE];

		assert!(!rx_pass(&net.backend, &mem, &mut queue, &mut buf, true).expect("rx pass"));
		assert_eq!(used_idx(&mem), 0);
		assert!(!user.rx_wake_armed(), "an empty ring must not make the worker spin");

		// The guest posts a buffer (and notifies): the kept frame is delivered.
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING + 4))
			.expect("avail head");
		mem.write_obj(u16::to_le(1), GuestAddress(AVAIL_RING + 2))
			.expect("avail idx");
		assert!(rx_pass(&net.backend, &mem, &mut queue, &mut buf, true).expect("rx pass"));
		assert_eq!(used_idx(&mem), 1);
		assert_eq!(used_len(&mem), (VNET_HDR_SIZE + 5) as u32);
		let mut payload = [0u8; 5];
		mem.read_slice(&mut payload, GuestAddress(0x4000 + VNET_HDR_SIZE as u64))
			.expect("read");
		assert_eq!(&payload, b"frame");
	}

	#[test]
	fn rx_buffer_available_tracks_avail_ring() {
		let mem = guest_mem();
		let desc = SplitDescriptor::new(0x4000, 64, VRING_DESC_F_WRITE as u16, 0);
		assert!(!rx_buffer_available(&mem, &queue_with_heads(&mem, &[desc], &[])));
		let mut queue = queue_with_heads(&mem, &[desc], &[0]);
		assert!(rx_buffer_available(&mem, &queue));
		assert!(deliver_rx(&mem, &mut queue, &rx_packet(b"x"), false).expect("deliver"));
		assert!(!rx_buffer_available(&mem, &queue), "consumed buffer is no longer available");
	}

	#[test]
	fn deliver_rx_writes_tap_header_and_payload() {
		let mem = guest_mem();
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			0x4000,
			(VNET_HDR_SIZE + 5) as u32,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let packet = rx_packet(b"hello");

		assert!(deliver_rx(&mem, &mut queue, &packet, false).expect("deliver_rx"));

		let mut written = vec![0u8; packet.len()];
		mem.read_slice(&mut written, GuestAddress(0x4000))
			.expect("read rx buffer");
		let mut expected = packet.clone();
		expected[NUM_BUFFERS_OFFSET..VNET_HDR_SIZE].copy_from_slice(&1u16.to_le_bytes());
		assert_eq!(written, expected);
		assert_eq!(used_idx(&mem), 1);
		assert_eq!(used_len(&mem), packet.len() as u32);
	}

	#[test]
	fn deliver_rx_sets_num_buffers_to_one_when_not_mergeable() {
		// VIRTIO_F_VERSION_1 headers are always 12 bytes; with MRG_RXBUF off the
		// device MUST set num_buffers to 1 (virtio 1.x 5.1.6.2.2), never forward
		// the stale value sitting in the tap read buffer.
		let mem = guest_mem();
		let mut queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			0x4000,
			(VNET_HDR_SIZE + 4) as u32,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let mut packet = rx_packet(b"data");
		// Poison num_buffers to prove the device overwrites it.
		packet[NUM_BUFFERS_OFFSET..VNET_HDR_SIZE].copy_from_slice(&0xbeef_u16.to_le_bytes());

		assert!(deliver_rx(&mem, &mut queue, &packet, false).expect("deliver_rx"));

		let mut header = [0u8; VNET_HDR_SIZE];
		mem.read_slice(&mut header, GuestAddress(0x4000))
			.expect("read rx header");
		assert_eq!(
			&header[NUM_BUFFERS_OFFSET..VNET_HDR_SIZE],
			&1u16.to_le_bytes(),
			"non-mergeable RX must report exactly one buffer"
		);
		assert_eq!(used_len(&mem), packet.len() as u32);
	}

	#[test]
	fn deliver_rx_uses_mergeable_buffers() {
		let mem = guest_mem();
		let mut queue = queue_with_heads(
			&mem,
			&[
				SplitDescriptor::new(0x4000, 8, VRING_DESC_F_WRITE as u16, 0),
				SplitDescriptor::new(0x5000, 32, VRING_DESC_F_WRITE as u16, 0),
			],
			&[0, 1],
		);
		let packet = rx_packet(b"abcdef");

		assert!(deliver_rx(&mem, &mut queue, &packet, true).expect("deliver_rx"));

		let mut first = [0u8; 8];
		let mut second = [0u8; 10];
		mem.read_slice(&mut first, GuestAddress(0x4000))
			.expect("read first rx buffer");
		mem.read_slice(&mut second, GuestAddress(0x5000))
			.expect("read second rx buffer");
		assert_eq!(&first, &packet[..8]);
		assert_eq!(&second[..2], &packet[8..10]);
		assert_eq!(&second[2..4], &2u16.to_le_bytes());
		assert_eq!(&second[4..], b"abcdef");
		assert_eq!(used_idx(&mem), 2);
		assert_eq!(used_len_at(&mem, 0), 8);
		assert_eq!(used_len_at(&mem, 1), 10);
	}
}
