//! virtio-console device carrying the guest-agent channel (GC4).
//!
//! The device offers `VIRTIO_CONSOLE_F_MULTIPORT` with two generic ports: id 0
//! named [`proto::AGENT_PORT_NAME`], and id 1 named [`proto::LOG_PORT_NAME`],
//! whose guest output (structured agent/envd log lines) is appended to the
//! VMM's stdout, the same sink as the serial console, at one exit per buffer
//! instead of one port-IO exit per byte. Linux exposes such a port as a plain
//! character device (`/dev/vportNp0`) rather than the `hvc0` tty, so the
//! agent's reads and writes bypass the tty layer: hvc pulls host input 16 bytes
//! at a time through flip buffers and line-discipline work, which capped
//! host -> guest throughput near 50 MB/s.
//!
//! Queues with multiport: 0 = port receiveq (host -> guest), 1 = port
//! transmitq (guest -> host), 2 = control receiveq, 3 = control transmitq,
//! 4/5 = log port receiveq (unused) / transmitq. A
//! driver that does not negotiate multiport (snapshots captured before the
//! device offered it restore with two queues) keeps the legacy console port,
//! i.e. `hvc0`, on queues 0/1.
//!
//! Control handshake (virtio 1.2 §5.3.6.2): driver `DEVICE_READY` -> device
//! `DEVICE_ADD(0)`, `DEVICE_ADD(1)`; driver `PORT_READY(n)` -> device
//! `PORT_OPEN(n, 1)`, then `PORT_NAME(n)`, so a port whose name is visible in
//! sysfs is already writable. The host end stays open for the device's
//! lifetime: the agent bridge forwards host bytes only after the guest agent
//! announced itself on the port, and a generic port drops data that arrives
//! while no guest process holds it open.
//!
//! The host injects bytes by pushing them into the shared `input` deque and
//! kicking `input_evt`; the worker copies them into the guest's RX descriptors.
//! Guest output is drained off the TX queue into a capped `output` buffer and
//! signalled through `output_evt` for the host bridge to forward or discard.
//! RX doorbells are suppressed (`VRING_USED_F_NO_NOTIFY`) unless the device is
//! starved for buffers with input pending: the guest recycles one page-sized
//! RX buffer per page it reads, and a doorbell (VM exit) per page dominated
//! bulk host -> guest transfers.

#[cfg(unix)]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "windows")]
use std::os::windows::io::AsRawHandle;
use std::{collections::VecDeque, io::Write, sync::Arc};

use parking_lot::Mutex;
use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_CONSOLE};
use virtio_queue::{Queue, QueueT};
use vm_memory::{Address, Bytes, GuestAddress};
use vmon_agent::proto;

use crate::{
	memory::GuestMemoryMmap,
	os::EventFd,
	result::{Result, err},
	virtio::{
		Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, WorkerWaitSource,
		descriptor_range_valid,
	},
};

/// Port queue depth: the guest posts page-sized RX buffers, so 256 entries keep
/// 1 MiB of host -> guest data in flight.
const DATA_QUEUE_SIZE: u16 = 256;
const CONTROL_QUEUE_SIZE: u16 = 32;
/// Log port queue depth: one guest `write` (a log line) per chain.
const LOG_QUEUE_SIZE: u16 = 64;
const RX_QUEUE: usize = 0;
const TX_QUEUE: usize = 1;
const CONTROL_RX_QUEUE: usize = 2;
const CONTROL_TX_QUEUE: usize = 3;
const LOG_RX_QUEUE: usize = 4;
const LOG_TX_QUEUE: usize = 5;
const QUEUE_SIZES: [u16; 6] = [
	DATA_QUEUE_SIZE,
	DATA_QUEUE_SIZE,
	CONTROL_QUEUE_SIZE,
	CONTROL_QUEUE_SIZE,
	LOG_QUEUE_SIZE,
	LOG_QUEUE_SIZE,
];
/// Feature bit `VIRTIO_CONSOLE_F_MULTIPORT`.
const VIRTIO_CONSOLE_F_MULTIPORT: u32 = 1;
/// `max_nr_ports`: the agent port and the log port.
const MAX_NR_PORTS: u32 = 2;
const AGENT_PORT_ID: u32 = 0;
const LOG_PORT_ID: u32 = 1;
/// `virtio_console_control` header: `le32 id`, `le16 event`, `le16 value`.
const CONTROL_HEADER_LEN: usize = 8;
const VIRTIO_CONSOLE_DEVICE_READY: u16 = 0;
const VIRTIO_CONSOLE_DEVICE_ADD: u16 = 1;
const VIRTIO_CONSOLE_PORT_READY: u16 = 3;
const VIRTIO_CONSOLE_PORT_OPEN: u16 = 6;
const VIRTIO_CONSOLE_PORT_NAME: u16 = 7;
/// `virtio_console_config`: `le16 cols`, `le16 rows`, `le32 max_nr_ports`,
/// `le32 emerg_wr`.
const CONFIG_SPACE_SIZE: usize = 12;
const CONFIG_MAX_NR_PORTS: usize = 4;
pub const OUTPUT_CAP: usize = 8 << 20;

/// Queues a saved console carries: an activated driver that did not negotiate
/// multiport uses only the legacy port pair (snapshots from before the device
/// offered multiport); otherwise the transport holds the full set.
pub const fn queue_count(acked_features: u64, activated: bool) -> usize {
	if activated && acked_features & (1u64 << VIRTIO_CONSOLE_F_MULTIPORT) == 0 {
		2
	} else {
		QUEUE_SIZES.len()
	}
}

struct TxDesc {
	addr: GuestAddress,
	len:  usize,
}

struct PendingTxChain {
	head:        u16,
	descs:       Vec<TxDesc>,
	desc_index:  usize,
	desc_offset: usize,
}

pub struct Console {
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,
	config:         Vec<u8>,

	/// Host -> guest bytes awaiting injection into the RX queue.
	input:            Arc<Mutex<VecDeque<u8>>>,
	/// Kicked by the host through `input_handle` to wake the worker for RX.
	input_evt:        EventFd,
	/// Guest -> host bytes drained from the TX queue.
	output:           Arc<Mutex<Vec<u8>>>,
	/// Kicked by the worker when fresh guest output is appended.
	output_evt:       EventFd,
	/// Kicked by the host bridge after it frees output-buffer capacity.
	drain_resume_evt: EventFd,
	/// TX chain popped while the host output buffer was full; completed later.
	pending_tx:       Option<PendingTxChain>,
	/// Device -> driver control messages awaiting a control receiveq buffer.
	control_out:      VecDeque<Vec<u8>>,
	mem:              Option<GuestMemoryMmap>,
	interrupt:        Option<Arc<Interrupt>>,
	rx_queue:         Option<Queue>,
	tx_queue:         Option<Queue>,
	control_rx:       Option<Queue>,
	control_tx:       Option<Queue>,
	log_rx:           Option<Queue>,
	log_tx:           Option<Queue>,
	/// Where log port output goes: the serial console's sink (VMM stdout).
	log_out:          Box<dyn Write + Send>,
}

impl Console {
	pub fn new() -> Result<Self> {
		let mut config = vec![0u8; CONFIG_SPACE_SIZE];
		config[CONFIG_MAX_NR_PORTS..CONFIG_MAX_NR_PORTS + 4]
			.copy_from_slice(&MAX_NR_PORTS.to_le_bytes());
		Ok(Self {
			features: (1u64 << VIRTIO_F_VERSION_1) | (1u64 << VIRTIO_CONSOLE_F_MULTIPORT),
			acked_features: 0,
			queue_sizes: QUEUE_SIZES.to_vec(),
			config,
			input: Arc::new(Mutex::new(VecDeque::new())),
			input_evt: EventFd::new(crate::os::EFD_NONBLOCK)?,
			output: Arc::new(Mutex::new(Vec::new())),
			output_evt: EventFd::new(crate::os::EFD_NONBLOCK)?,
			drain_resume_evt: EventFd::new(crate::os::EFD_NONBLOCK)?,
			pending_tx: None,
			control_out: VecDeque::new(),
			mem: None,
			interrupt: None,
			rx_queue: None,
			tx_queue: None,
			control_rx: None,
			control_tx: None,
			log_rx: None,
			log_tx: None,
			log_out: Box::new(crate::devices::serial::ConsoleOut),
		})
	}

	/// Whether the driver negotiated the multiport (agent port) layout.
	const fn multiport(&self) -> bool {
		self.acked_features & (1u64 << VIRTIO_CONSOLE_F_MULTIPORT) != 0
	}

	/// Shared host -> guest buffer plus a wake handle, for a VMM that drives the
	/// channel after the device has been moved into its worker: push bytes into
	/// the deque, then write `1` to the returned eventfd to kick RX servicing.
	pub fn input_handle(&self) -> (Arc<Mutex<VecDeque<u8>>>, EventFd) {
		(
			Arc::clone(&self.input),
			self
				.input_evt
				.try_clone()
				.expect("dup virtio-console input eventfd"),
		)
	}

	/// Shared guest -> host output buffer plus wake/resume eventfds for the
	/// host bridge. The bridge drains `output`, then kicks `drain_resume_evt`
	/// so the worker can resume TX once capacity is available.
	pub fn output_handle(&self) -> (Arc<Mutex<Vec<u8>>>, EventFd, EventFd) {
		(
			Arc::clone(&self.output),
			self
				.output_evt
				.try_clone()
				.expect("dup virtio-console output eventfd"),
			self
				.drain_resume_evt
				.try_clone()
				.expect("dup virtio-console drain-resume eventfd"),
		)
	}

	/// Raise the used-buffer interrupt if the device is activated.
	fn signal_used(&self) -> Result<()> {
		if let Some(interrupt) = &self.interrupt {
			interrupt.signal_used_queue()?;
		}
		Ok(())
	}

	/// Drain the transmit queue (guest -> host): append each chain's readable
	/// descriptor bytes to `output`. Returns whether any buffer was used.
	fn drain_pending_tx(
		pending_tx: &mut Option<PendingTxChain>,
		mem: &GuestMemoryMmap,
		tx_queue: &mut Queue,
		out: &mut Vec<u8>,
	) -> Result<(bool, bool)> {
		let mut produced = false;
		let (complete, head) = {
			let Some(pending) = pending_tx.as_mut() else {
				return Ok((false, false));
			};

			while pending.desc_index < pending.descs.len() {
				if out.len() >= OUTPUT_CAP {
					break;
				}
				let desc = &pending.descs[pending.desc_index];
				if pending.desc_offset >= desc.len {
					pending.desc_index += 1;
					pending.desc_offset = 0;
					continue;
				}

				let take = (desc.len - pending.desc_offset).min(OUTPUT_CAP - out.len());
				if take == 0 || out.try_reserve(take).is_err() {
					break;
				}

				let start = out.len();
				let Some(addr) = desc.addr.checked_add(pending.desc_offset as u64) else {
					pending.desc_index = pending.descs.len();
					break;
				};
				out.resize(start + take, 0);
				if mem.read_slice(&mut out[start..start + take], addr).is_err() {
					out.truncate(start);
					pending.desc_index = pending.descs.len();
					break;
				}

				pending.desc_offset += take;
				produced = true;
				if pending.desc_offset == desc.len {
					pending.desc_index += 1;
					pending.desc_offset = 0;
				}
			}

			(pending.desc_index >= pending.descs.len(), pending.head)
		};

		if !complete {
			return Ok((false, produced));
		}

		*pending_tx = None;
		tx_queue
			.add_used(mem, head, 0)
			.map_err(|e| err(format!("virtio-console TX used-ring update failed: {e}")))?;
		Ok((true, produced))
	}

	/// Drain the transmit queue (guest -> host): append each chain's readable
	/// descriptor bytes to `output`. Consumes at most `*budget` chains (a
	/// spinning guest could otherwise keep the pass alive forever). Returns
	/// whether any buffer was used.
	fn drain_tx(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(tx_queue) = self.tx_queue.as_mut() else {
			return Ok(false);
		};

		let mut used = false;
		let mut produced = false;
		{
			let mut out = self.output.lock();
			let (pending_used, pending_produced) =
				Self::drain_pending_tx(&mut self.pending_tx, &mem, tx_queue, &mut out)?;
			used |= pending_used;
			produced |= pending_produced;

			while self.pending_tx.is_none() && out.len() < OUTPUT_CAP && *budget != 0 {
				let Some(chain) = tx_queue.pop_descriptor_chain(&mem) else {
					break;
				};
				*budget -= 1;
				let head = chain.head_index();
				let mut descs = Vec::new();
				let mut valid = true;
				for d in chain {
					if d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
						valid = false;
						break;
					}
					descs.push(TxDesc { addr: d.addr(), len: d.len() as usize });
				}

				if valid {
					self.pending_tx =
						Some(PendingTxChain { head, descs, desc_index: 0, desc_offset: 0 });
					let (chain_used, chain_produced) =
						Self::drain_pending_tx(&mut self.pending_tx, &mem, tx_queue, &mut out)?;
					used |= chain_used;
					produced |= chain_produced;
				} else {
					// TX buffers are guest-readable only, so nothing is written
					// back.
					tx_queue
						.add_used(&mem, head, 0)
						.map_err(|e| err(format!("virtio-console TX used-ring update failed: {e}")))?;
					used = true;
				}
			}
		}

		if produced {
			self.output_evt.write(1)?;
		}
		Ok(used)
	}

	/// Drain the log port's transmit queue into `log_out`, one chain (guest
	/// `write`) at a time so lines from the same writer stay whole. Consumes at
	/// most `*budget` chains. Returns whether any buffer was used.
	fn drain_log_tx(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(log_tx) = self.log_tx.as_mut() else {
			return Ok(false);
		};
		let mut used = false;
		let mut line = Vec::new();
		while *budget != 0 {
			let Some(chain) = log_tx.pop_descriptor_chain(&mem) else {
				break;
			};
			*budget -= 1;
			let head = chain.head_index();
			line.clear();
			for d in chain {
				if d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
					line.clear();
					break;
				}
				let start = line.len();
				line.resize(start + d.len() as usize, 0);
				if mem.read_slice(&mut line[start..], d.addr()).is_err() {
					line.clear();
					break;
				}
			}
			// A lost log line must not stall the guest writer.
			let _ = self.log_out.write_all(&line);
			log_tx
				.add_used(&mem, head, 0)
				.map_err(|e| err(format!("virtio-console log used-ring update failed: {e}")))?;
			used = true;
		}
		Ok(used)
	}

	/// Fill the receive queue (host -> guest) from `input`: while bytes remain
	/// and an RX chain is available, copy as many as fit into the chain's
	/// writable descriptors. Consumes at most `*budget` chains. Returns whether
	/// any buffer was used.
	///
	/// Leaves RX doorbells disabled unless input is pending with no buffer to
	/// put it in: fresh host input re-runs this pass through `input_evt`, which
	/// polls the avail ring itself, so only a starved device needs the guest to
	/// announce new buffers.
	fn fill_rx(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(rx_queue) = self.rx_queue.as_mut() else {
			return Ok(false);
		};
		let mut inp = self.input.lock();

		let mut used = false;
		loop {
			while !inp.is_empty() && *budget != 0 {
				let Some(chain) = rx_queue.pop_descriptor_chain(&mem) else {
					break;
				};
				*budget -= 1;
				let head = chain.head_index();
				let mut descs = Vec::new();
				let mut valid = true;
				let mut capacity = 0u32;
				for d in chain {
					let Some(next_capacity) = capacity.checked_add(d.len()) else {
						valid = false;
						break;
					};
					if !d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
						valid = false;
						break;
					}
					capacity = next_capacity;
					descs.push((d.addr(), d.len() as usize));
				}

				// `capacity` fits in u32, so the running total never overflows.
				let mut written = 0usize;
				if valid {
					'descs: for (addr, len) in descs {
						let mut offset = 0usize;
						while offset < len {
							// Copy from the deque's front slice (no rotation) and
							// drain only what was written, so a guest-memory
							// fault never eats input.
							let front = inp.as_slices().0;
							if front.is_empty() {
								break 'descs;
							}
							let take = (len - offset).min(front.len());
							let Some(at) = addr.checked_add(offset as u64) else {
								break 'descs;
							};
							if mem.write_slice(&front[..take], at).is_err() {
								break 'descs;
							}
							inp.drain(..take);
							offset += take;
							written += take;
						}
					}
				}
				rx_queue
					.add_used(&mem, head, u32::try_from(written).unwrap_or(capacity))
					.map_err(|e| err(format!("virtio-console RX used-ring update failed: {e}")))?;
				used = true;
			}
			if inp.is_empty() || *budget == 0 {
				rx_queue
					.disable_notification(&mem)
					.map_err(|e| err(format!("virtio-console RX notification update failed: {e}")))?;
				break;
			}
			// Starved: ask for a doorbell when the guest posts a buffer. The
			// re-check catches buffers posted before the flag flipped.
			let posted = rx_queue
				.enable_notification(&mem)
				.map_err(|e| err(format!("virtio-console RX notification update failed: {e}")))?;
			if !posted {
				break;
			}
		}
		Ok(used)
	}

	/// Consume driver control messages (multiport only) and queue the device's
	/// replies. Consumes at most `*budget` chains. Returns whether any buffer
	/// was used.
	fn process_control_tx(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(queue) = self.control_tx.as_mut() else {
			return Ok(false);
		};
		let mut used = false;
		while *budget != 0 {
			let Some(chain) = queue.pop_descriptor_chain(&mem) else {
				break;
			};
			*budget -= 1;
			let head = chain.head_index();
			let mut header = [0u8; CONTROL_HEADER_LEN];
			let mut filled = 0usize;
			for d in chain {
				if d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
					break;
				}
				let take = (d.len() as usize).min(CONTROL_HEADER_LEN - filled);
				if mem
					.read_slice(&mut header[filled..filled + take], d.addr())
					.is_err()
				{
					break;
				}
				filled += take;
				if filled == CONTROL_HEADER_LEN {
					break;
				}
			}
			// Control transmit buffers are driver-readable only; nothing is
			// written back.
			queue
				.add_used(&mem, head, 0)
				.map_err(|e| err(format!("virtio-console control used-ring update failed: {e}")))?;
			used = true;
			if filled == CONTROL_HEADER_LEN {
				let id = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
				let event = u16::from_le_bytes([header[4], header[5]]);
				let value = u16::from_le_bytes([header[6], header[7]]);
				self.control_out.extend(control_replies(id, event, value));
			}
		}
		Ok(used)
	}

	/// Deliver queued control messages into the control receiveq, one message
	/// per chain. Consumes at most `*budget` chains. Returns whether any buffer
	/// was used.
	fn fill_control_rx(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(queue) = self.control_rx.as_mut() else {
			return Ok(false);
		};
		let mut used = false;
		while *budget != 0 {
			let Some(message) = self.control_out.front() else {
				break;
			};
			let Some(chain) = queue.pop_descriptor_chain(&mem) else {
				break;
			};
			*budget -= 1;
			let head = chain.head_index();
			let mut written = 0usize;
			let mut valid = true;
			for d in chain {
				if written == message.len() {
					break;
				}
				if !d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
					valid = false;
					break;
				}
				let take = (d.len() as usize).min(message.len() - written);
				if mem
					.write_slice(&message[written..written + take], d.addr())
					.is_err()
				{
					valid = false;
					break;
				}
				written += take;
			}
			// A message must arrive whole; otherwise the chain is returned empty
			// and the message waits for the next buffer.
			let delivered = valid && written == message.len();
			let used_len = if delivered {
				u32::try_from(written).unwrap_or(0)
			} else {
				0
			};
			queue
				.add_used(&mem, head, used_len)
				.map_err(|e| err(format!("virtio-console control used-ring update failed: {e}")))?;
			used = true;
			if delivered {
				self.control_out.pop_front();
			}
		}
		Ok(used)
	}

	/// One bounded pass over every queue: control first so port state changes
	/// precede data, then guest output, then pending host input.
	fn service_queues(&mut self, budget: &mut usize) -> Result<bool> {
		let mut used = self.process_control_tx(budget)?;
		used |= self.fill_control_rx(budget)?;
		used |= self.drain_tx(budget)?;
		used |= self.drain_log_tx(budget)?;
		used |= self.fill_rx(budget)?;
		Ok(used)
	}
}

/// Encode one device -> driver `virtio_console_control` message.
fn control_message(id: u32, event: u16, value: u16, payload: &[u8]) -> Vec<u8> {
	let mut message = Vec::with_capacity(CONTROL_HEADER_LEN + payload.len());
	message.extend_from_slice(&id.to_le_bytes());
	message.extend_from_slice(&event.to_le_bytes());
	message.extend_from_slice(&value.to_le_bytes());
	message.extend_from_slice(payload);
	message
}

/// The device's replies to one driver control message (virtio 1.2 §5.3.6.2).
/// Driver failures (`value` 0), guest-side `PORT_OPEN` (a process opened or
/// closed the port) and unknown events need no reply.
fn control_replies(id: u32, event: u16, value: u16) -> Vec<Vec<u8>> {
	match (event, value) {
		(VIRTIO_CONSOLE_DEVICE_READY, 1) => vec![
			control_message(AGENT_PORT_ID, VIRTIO_CONSOLE_DEVICE_ADD, 1, &[]),
			control_message(LOG_PORT_ID, VIRTIO_CONSOLE_DEVICE_ADD, 1, &[]),
		],
		(VIRTIO_CONSOLE_PORT_READY, 1) if id == AGENT_PORT_ID || id == LOG_PORT_ID => {
			let name = if id == AGENT_PORT_ID {
				proto::AGENT_PORT_NAME
			} else {
				proto::LOG_PORT_NAME
			};
			vec![
				control_message(id, VIRTIO_CONSOLE_PORT_OPEN, 1, &[]),
				control_message(id, VIRTIO_CONSOLE_PORT_NAME, 1, name.as_bytes()),
			]
		},
		_ => Vec::new(),
	}
}

impl VirtioDevice for Console {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_CONSOLE
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
		// virtio-console config space is read-only for the driver.
	}

	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()> {
		let mut queues: Vec<Option<Queue>> = queues.into_iter().map(Some).collect();
		let mut take = |index: usize| queues.get_mut(index).and_then(Option::take);
		self.rx_queue = take(RX_QUEUE);
		self.tx_queue = take(TX_QUEUE);
		if self.multiport() {
			self.control_rx = take(CONTROL_RX_QUEUE);
			self.control_tx = take(CONTROL_TX_QUEUE);
			if self.control_rx.is_none() || self.control_tx.is_none() {
				return Err(err("virtio-console multiport activation without control queues"));
			}
			self.log_rx = take(LOG_RX_QUEUE);
			self.log_tx = take(LOG_TX_QUEUE);
		}
		self.mem = Some(mem);
		self.interrupt = Some(interrupt);
		Ok(())
	}

	fn reset(&mut self) -> Result<()> {
		self.acked_features = 0;
		self.pending_tx = None;
		self.control_out.clear();
		self.mem = None;
		self.interrupt = None;
		self.rx_queue = None;
		self.tx_queue = None;
		self.control_rx = None;
		self.control_tx = None;
		self.log_rx = None;
		self.log_tx = None;
		Ok(())
	}

	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		vec![event_wait_source(&self.input_evt), event_wait_source(&self.drain_resume_evt)]
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		// The guest rang the doorbell after queueing TX data or control messages,
		// or posted RX buffers while the device was starved.
		let mut budget = QUEUE_PASS_BUDGET;
		if self.service_queues(&mut budget)? {
			self.signal_used()?;
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn process_worker_source(&mut self, index: usize) -> Result<()> {
		if index == 0 {
			let _ = self.input_evt.read();
			let mut budget = QUEUE_PASS_BUDGET;
			if self.fill_rx(&mut budget)? {
				self.signal_used()?;
			}
			if budget == 0 {
				// Bounded pass exhausted: re-arm our own wake source so the
				// remaining input is delivered on a later wake without
				// monopolizing the shared device loop.
				let _ = self.input_evt.write(1);
			}
		} else if index == 1 {
			let _ = self.drain_resume_evt.read();
			let mut budget = QUEUE_PASS_BUDGET;
			if self.drain_tx(&mut budget)? {
				self.signal_used()?;
			}
			if budget == 0 {
				let _ = self.drain_resume_evt.write(1);
			}
		}
		Ok(())
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		// Order must mirror the transport's queue vec: rx (0), tx (1), then the
		// control receiveq (2) and transmitq (3) and the log port's rx (4) and
		// tx (5) with multiport.
		[
			&self.rx_queue,
			&self.tx_queue,
			&self.control_rx,
			&self.control_tx,
			&self.log_rx,
			&self.log_tx,
		]
		.into_iter()
		.flatten()
		.map(Queue::state)
		.collect()
	}

	fn drain(&mut self) -> Result<()> {
		// A snapshot quiesce must drain to completion, not one bounded pass. Port
		// control is serviced too: the guest spins until its control message is
		// consumed, and undelivered device replies would be lost with the device.
		let mut used = false;
		loop {
			let mut budget = QUEUE_PASS_BUDGET;
			used |= self.process_control_tx(&mut budget)?;
			used |= self.fill_control_rx(&mut budget)?;
			used |= self.drain_tx(&mut budget)?;
			used |= self.drain_log_tx(&mut budget)?;
			if budget != 0 {
				break;
			}
		}
		if used {
			self.signal_used()?;
		}
		if self.pending_tx.is_some() {
			return Err(err("virtio-console snapshot blocked by backpressured TX output"));
		}
		if !self.control_out.is_empty() {
			return Err(err("virtio-console snapshot blocked by undelivered port control messages"));
		}
		Ok(())
	}
}

#[cfg(unix)]
fn event_wait_source(evt: &EventFd) -> WorkerWaitSource {
	evt.as_raw_fd()
}

#[cfg(target_os = "windows")]
fn event_wait_source(evt: &EventFd) -> WorkerWaitSource {
	evt.as_raw_handle()
}

#[cfg(test)]
mod tests {
	use virtio_bindings::bindings::virtio_ring::{
		VRING_DESC_F_NEXT, VRING_DESC_F_WRITE, VRING_USED_F_NO_NOTIFY,
	};
	use virtio_queue::{
		QueueT,
		desc::{RawDescriptor, split::Descriptor as SplitDescriptor},
	};
	use vm_memory::{Bytes, GuestAddress};

	use super::*;

	const MEM_SIZE: usize = 0x1_0000;
	const RING_SIZE: u16 = 8;
	/// Guest buffers live above the rings.
	const BUF: u64 = 0x8000;

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), MEM_SIZE)]).expect("guest memory")
	}

	/// Ring `slot` (queue index) occupies one page: descriptor table, then the
	/// avail ring at +0x400 and the used ring at +0x800.
	const fn ring_base(slot: u64) -> u64 {
		0x1000 * (slot + 1)
	}

	/// Queue `slot` whose descriptor `i` is a single-descriptor chain
	/// `descs[i]`; the first `published` of them are in the avail ring.
	fn ring(mem: &GuestMemoryMmap, slot: u64, descs: &[SplitDescriptor], published: u16) -> Queue {
		let base = ring_base(slot);
		for (idx, desc) in (0u64..).zip(descs) {
			mem.write_obj(RawDescriptor::from(*desc), GuestAddress(base + idx * 16))
				.expect("write descriptor");
		}
		let mut queue = Queue::new(RING_SIZE).expect("queue");
		queue.set_size(RING_SIZE);
		queue.set_desc_table_address(Some(base as u32), Some(0));
		queue.set_avail_ring_address(Some((base + 0x400) as u32), Some(0));
		queue.set_used_ring_address(Some((base + 0x800) as u32), Some(0));
		queue.set_ready(true);
		for head in 0..published {
			publish(mem, slot, head);
		}
		queue
	}

	/// Driver side: post descriptor `head` to ring `slot`'s avail ring.
	fn publish(mem: &GuestMemoryMmap, slot: u64, head: u16) {
		let avail = ring_base(slot) + 0x400;
		let idx = u16::from_le(
			mem.read_obj::<u16>(GuestAddress(avail + 2))
				.expect("avail idx"),
		);
		mem.write_obj(u16::to_le(head), GuestAddress(avail + 4 + u64::from(idx % RING_SIZE) * 2))
			.expect("write avail entry");
		mem.write_obj(u16::to_le(idx.wrapping_add(1)), GuestAddress(avail + 2))
			.expect("write avail idx");
	}

	fn queue_with_descs(mem: &GuestMemoryMmap, descs: &[SplitDescriptor]) -> Queue {
		ring(mem, 0, descs, 1)
	}

	/// `(head, len)` of used element `n` in ring `slot`.
	fn used_elem(mem: &GuestMemoryMmap, slot: u64, n: u64) -> (u32, u32) {
		let elem = ring_base(slot) + 0x804 + n * 8;
		let read = |at| u32::from_le(mem.read_obj::<u32>(GuestAddress(at)).expect("used elem"));
		(read(elem), read(elem + 4))
	}

	fn used_flags(mem: &GuestMemoryMmap, slot: u64) -> u16 {
		u16::from_le(
			mem.read_obj::<u16>(GuestAddress(ring_base(slot) + 0x800))
				.expect("read used flags"),
		)
	}

	fn used_len(mem: &GuestMemoryMmap) -> u32 {
		used_elem(mem, 0, 0).1
	}

	fn used_idx(mem: &GuestMemoryMmap) -> u16 {
		u16::from_le(
			mem.read_obj::<u16>(GuestAddress(ring_base(0) + 0x802))
				.expect("read used idx"),
		)
	}

	fn console_with_rx(mem: GuestMemoryMmap, queue: Queue, input: &[u8]) -> Console {
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.rx_queue = Some(queue);
		console.input.lock().extend(input.iter().copied());
		console
	}

	#[test]
	fn fill_rx_rejects_readable_descriptor_without_consuming_input() {
		let mem = guest_mem();
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(BUF, 64, 0, 0)]);
		let mut console = console_with_rx(mem, queue, b"ping");
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(console.fill_rx(&mut budget).expect("fill_rx"));

		let remaining: Vec<u8> = console.input.lock().iter().copied().collect();
		assert_eq!(remaining, b"ping");
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn fill_rx_rejects_out_of_range_descriptor_without_consuming_input() {
		let mem = guest_mem();
		let bad_addr = MEM_SIZE as u64 - 8;
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			bad_addr,
			64,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let mut console = console_with_rx(mem, queue, b"pong");
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(console.fill_rx(&mut budget).expect("fill_rx"));

		let remaining: Vec<u8> = console.input.lock().iter().copied().collect();
		assert_eq!(remaining, b"pong");
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn drain_tx_discards_write_only_descriptor_output() {
		let mem = guest_mem();
		mem.write_slice(b"guest", GuestAddress(BUF))
			.expect("write guest output");
		let queue =
			queue_with_descs(&mem, &[SplitDescriptor::new(BUF, 5, VRING_DESC_F_WRITE as u16, 0)]);
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.tx_queue = Some(queue);
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(console.drain_tx(&mut budget).expect("drain_tx"));

		assert!(console.output.lock().is_empty());
		assert_eq!(used_len(console.mem.as_ref().expect("mem")), 0);
	}

	#[test]
	fn drain_tx_stops_at_cap_and_resumes_on_drain_event() {
		let mem = guest_mem();
		mem.write_slice(b"guest", GuestAddress(BUF))
			.expect("write guest output");
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(BUF, 5, 0, 0)]);
		let mut console = Console::new().expect("console");
		console.mem = Some(mem);
		console.tx_queue = Some(queue);
		console.output.lock().resize(OUTPUT_CAP, b'x');
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(!console.drain_tx(&mut budget).expect("drain_tx"));
		assert_eq!(console.output.lock().len(), OUTPUT_CAP);
		assert_eq!(used_idx(console.mem.as_ref().expect("mem")), 0);

		console.output.lock().clear();
		console.drain_resume_evt.write(1).expect("signal resume");
		console
			.process_worker_source(1)
			.expect("process worker source");

		assert_eq!(&console.output.lock()[..], b"guest");
		assert_eq!(used_idx(console.mem.as_ref().expect("mem")), 1);
		assert_eq!(console.output_evt.read().expect("read output evt"), 1);
	}

	#[test]
	fn multiport_handshake_adds_open_named_agent_port() {
		let mem = guest_mem();
		let write = VRING_DESC_F_WRITE as u16;
		// Driver -> device control messages, posted one at a time.
		let ready = control_message(u32::MAX, VIRTIO_CONSOLE_DEVICE_READY, 1, &[]);
		let port_ready = control_message(AGENT_PORT_ID, VIRTIO_CONSOLE_PORT_READY, 1, &[]);
		mem.write_slice(&ready, GuestAddress(BUF)).expect("write");
		mem.write_slice(&port_ready, GuestAddress(BUF + 0x10))
			.expect("write");
		let control_tx = ring(
			&mem,
			CONTROL_TX_QUEUE as u64,
			&[SplitDescriptor::new(BUF, 8, 0, 0), SplitDescriptor::new(BUF + 0x10, 8, 0, 0)],
			1,
		);
		// Device -> driver: page-sized buffers in the driver, 0x100 here.
		let rx_buf = |i: u64| BUF + 0x1000 + i * 0x100;
		let control_rx = ring(
			&mem,
			CONTROL_RX_QUEUE as u64,
			&(0..4)
				.map(|i| SplitDescriptor::new(rx_buf(i), 0x100, write, 0))
				.collect::<Vec<_>>(),
			4,
		);
		let delivered = |n: u64| {
			let (head, len) = used_elem(&mem, CONTROL_RX_QUEUE as u64, n);
			let mut message = vec![0u8; len as usize];
			mem.read_slice(&mut message, GuestAddress(rx_buf(u64::from(head))))
				.expect("read control message");
			message
		};

		let mut console = Console::new().expect("console");
		let mut max_nr_ports = [0u8; 4];
		console.read_config(CONFIG_MAX_NR_PORTS as u64, &mut max_nr_ports);
		assert_eq!(u32::from_le_bytes(max_nr_ports), 2);
		console.ack_features(console.features());
		assert!(console.multiport());
		console.mem = Some(mem.clone());
		console.control_rx = Some(control_rx);
		console.control_tx = Some(control_tx);

		assert_eq!(console.process_queue_notify().expect("pass"), QueuePass::Drained);
		assert_eq!(delivered(0), control_message(0, VIRTIO_CONSOLE_DEVICE_ADD, 1, &[]));
		assert_eq!(delivered(1), control_message(1, VIRTIO_CONSOLE_DEVICE_ADD, 1, &[]));

		publish(&mem, CONTROL_TX_QUEUE as u64, 1);
		assert_eq!(console.process_queue_notify().expect("pass"), QueuePass::Drained);
		// Opened before named, so a port visible by name is already writable.
		assert_eq!(delivered(2), control_message(0, VIRTIO_CONSOLE_PORT_OPEN, 1, &[]));
		assert_eq!(
			delivered(3),
			control_message(0, VIRTIO_CONSOLE_PORT_NAME, 1, proto::AGENT_PORT_NAME.as_bytes())
		);
		assert_eq!(control_replies(LOG_PORT_ID, VIRTIO_CONSOLE_PORT_READY, 1), vec![
			control_message(1, VIRTIO_CONSOLE_PORT_OPEN, 1, &[]),
			control_message(1, VIRTIO_CONSOLE_PORT_NAME, 1, proto::LOG_PORT_NAME.as_bytes()),
		]);
		assert!(console.control_out.is_empty());
		console
			.drain()
			.expect("quiesce with no undelivered control messages");
	}

	/// Shared byte sink standing in for the VMM's stdout.
	#[derive(Clone, Default)]
	struct Captured(Arc<Mutex<Vec<u8>>>);

	impl Write for Captured {
		fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
			self.0.lock().extend_from_slice(buf);
			Ok(buf.len())
		}

		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}

	#[test]
	fn log_port_output_goes_whole_to_the_console_sink() {
		let mem = guest_mem();
		mem.write_slice(b"vmon-log:{\"msg\":\"a\"}\n", GuestAddress(BUF))
			.expect("write first line");
		mem.write_slice(b"vmon-log:", GuestAddress(BUF + 0x100))
			.expect("write split line head");
		mem.write_slice(b"{\"msg\":\"b\"}\n", GuestAddress(BUF + 0x200))
			.expect("write split line tail");
		let log_tx = ring(
			&mem,
			LOG_TX_QUEUE as u64,
			&[
				SplitDescriptor::new(BUF, 21, 0, 0),
				SplitDescriptor::new(BUF + 0x100, 9, VRING_DESC_F_NEXT as u16, 2),
				SplitDescriptor::new(BUF + 0x200, 12, 0, 0),
			],
			2,
		);
		let sink = Captured::default();
		let mut console = Console::new().expect("console");
		console.mem = Some(mem.clone());
		console.log_tx = Some(log_tx);
		console.log_out = Box::new(sink.clone());

		assert_eq!(console.process_queue_notify().expect("pass"), QueuePass::Drained);
		assert_eq!(&sink.0.lock()[..], b"vmon-log:{\"msg\":\"a\"}\nvmon-log:{\"msg\":\"b\"}\n");
		assert_eq!(used_elem(&mem, LOG_TX_QUEUE as u64, 1), (1, 0));
	}

	#[test]
	fn rx_doorbell_requested_only_while_starved() {
		let mem = guest_mem();
		let write = VRING_DESC_F_WRITE as u16;
		let queue = ring(
			&mem,
			0,
			&[SplitDescriptor::new(BUF, 4, write, 0), SplitDescriptor::new(BUF + 0x10, 64, write, 0)],
			1,
		);
		let mut console = console_with_rx(mem.clone(), queue, b"");
		let no_notify = VRING_USED_F_NO_NOTIFY as u16;

		let mut budget = QUEUE_PASS_BUDGET;
		assert!(!console.fill_rx(&mut budget).expect("fill_rx"));
		assert_eq!(used_flags(&mem, 0), no_notify, "idle device needs no doorbell");

		console.input.lock().extend(b"abcdefgh");
		assert!(console.fill_rx(&mut budget).expect("fill_rx"));
		assert_eq!(used_flags(&mem, 0), 0, "starved device must hear about new buffers");

		publish(&mem, 0, 1);
		assert!(console.fill_rx(&mut budget).expect("fill_rx"));
		assert_eq!(used_flags(&mem, 0), no_notify);
		assert!(console.input.lock().is_empty());
		// A buffer larger than the remaining input reports exactly what it holds
		// (a zero length reads as EOF on the guest port).
		assert_eq!(used_elem(&mem, 0, 0), (0, 4));
		assert_eq!(used_elem(&mem, 0, 1), (1, 4));
		let mut got = [0u8; 4];
		mem.read_slice(&mut got, GuestAddress(BUF)).expect("read");
		assert_eq!(&got, b"abcd");
		mem.read_slice(&mut got, GuestAddress(BUF + 0x10))
			.expect("read");
		assert_eq!(&got, b"efgh");
	}
}
