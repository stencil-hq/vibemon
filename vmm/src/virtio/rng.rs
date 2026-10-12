//! virtio-rng entropy device backed by the host CSPRNG.
//!
//! A single requestq (index 0) carries guest-posted write-only buffers; on each
//! queue notification the worker fills as many as it can with fresh host
//! entropy and returns them on the used ring. The device offers no features
//! beyond `VIRTIO_F_VERSION_1` and has no config space (an entropy source has
//! none).
//!
//! The guest driver exposes this as `/dev/hwrng`, which feeds the kernel CRNG —
//! seeding it quickly so early userspace `getrandom(2)` (TLS handshakes,
//! SSH/key generation, `systemd`, language runtimes) does not block waiting for
//! the pool to initialize on a freshly booted microVM.
//!
//! The host entropy source is opened once at construction. Unix holds
//! `/dev/urandom`; Windows uses the process CSPRNG through a temporary source
//! file so the queue path remains ordinary `Read` I/O.

#[cfg(not(target_os = "windows"))]
use std::io::Read;
use std::{fs::File, sync::Arc};

use virtio_bindings::{bindings::virtio_config::VIRTIO_F_VERSION_1, virtio_ids::VIRTIO_ID_RNG};
use virtio_queue::{Queue, QueueT};
use vm_memory::{Address, Bytes};

use crate::{
	memory::GuestMemoryMmap,
	result::{Result, err},
	virtio::{Interrupt, QUEUE_PASS_BUDGET, QueuePass, VirtioDevice, descriptor_range_valid},
};

const QUEUE_SIZE: u16 = 64;
const NUM_QUEUES: usize = 1;
/// Per-`read` chunk pulled from the entropy source, bounding the host buffer
/// while one (possibly large) guest descriptor is filled across reads.
const ENTROPY_CHUNK: u32 = 4096;
#[cfg(not(target_os = "windows"))]
const ENTROPY_SOURCE: &str = "/dev/urandom";

/// virtio-rng entropy device. See the module docs.
pub struct Rng {
	features:       u64,
	acked_features: u64,
	queue_sizes:    Vec<u16>,
	/// Open Unix entropy handle; Windows uses `ProcessPrng` directly.
	source:         Option<File>,
	mem:            Option<GuestMemoryMmap>,
	interrupt:      Option<Arc<Interrupt>>,
	queue:          Option<Queue>,
}

impl Rng {
	/// Open the host entropy source and build an inactive device.
	///
	/// # Errors
	/// Fails if [`ENTROPY_SOURCE`] cannot be opened. It is opened here, before
	/// the sandbox filters are installed; the worker only reads the held fd
	/// afterwards, so a sandboxed worker never needs to open it.
	#[allow(clippy::unnecessary_wraps, reason = "opening /dev/urandom can fail on Unix")]
	pub fn new() -> Result<Self> {
		#[cfg(not(target_os = "windows"))]
		let source = File::open(ENTROPY_SOURCE)
			.map_err(|e| err(format!("opening entropy source {ENTROPY_SOURCE}: {e}")))?;
		#[cfg(target_os = "windows")]
		let source = None;
		#[cfg(not(target_os = "windows"))]
		let source = Some(source);
		Ok(Self {
			features: 1u64 << VIRTIO_F_VERSION_1,
			acked_features: 0,
			queue_sizes: vec![QUEUE_SIZE; NUM_QUEUES],
			source,
			mem: None,
			interrupt: None,
			queue: None,
		})
	}

	#[allow(clippy::needless_pass_by_ref_mut, reason = "Unix reads mutate the entropy file cursor")]
	fn fill_entropy(source: &mut Option<File>, buffer: &mut [u8]) -> Result<usize> {
		#[cfg(target_os = "windows")]
		{
			let _ = source;
			// SAFETY: `buffer` is valid for `buffer.len()` writable bytes for the
			// duration of the call.
			let ok = unsafe {
				windows_sys::Win32::Security::Cryptography::ProcessPrng(
					buffer.as_mut_ptr(),
					buffer.len(),
				)
			};
			if ok == 0 {
				return Err(std::io::Error::last_os_error().into());
			}
			Ok(buffer.len())
		}
		#[cfg(not(target_os = "windows"))]
		{
			source
				.as_mut()
				.expect("Unix entropy source")
				.read(buffer)
				.map_err(Into::into)
		}
	}

	/// Raise the used-buffer interrupt if the device is activated.
	fn signal_used(&self) -> Result<()> {
		if let Some(interrupt) = &self.interrupt {
			interrupt.signal_used_queue()?;
		}
		Ok(())
	}

	/// Fill each posted request buffer with host entropy and return it on the
	/// used ring. Consumes at most `*budget` chains per call (a spinning guest
	/// could otherwise keep the pass alive forever). Returns whether any buffer
	/// was used.
	fn process_requestq(&mut self, budget: &mut usize) -> Result<bool> {
		let Some(mem) = self.mem.clone() else {
			return Ok(false);
		};
		let Some(queue) = self.queue.as_mut() else {
			return Ok(false);
		};
		let mut used = false;
		let mut buf = [0u8; ENTROPY_CHUNK as usize];
		while *budget != 0 {
			let Some(chain) = queue.pop_descriptor_chain(&mem) else {
				break;
			};
			*budget -= 1;
			let head = chain.head_index();
			let mut written = 0u32;
			for d in chain {
				// virtio-rng entropy buffers are guest write-only; on anything
				// else stop and complete the chain with whatever was written.
				if !d.is_write_only() || !descriptor_range_valid(&mem, d.addr(), d.len()) {
					break;
				}
				let mut addr = d.addr();
				let mut remaining = d.len();
				while remaining > 0 {
					let take = remaining.min(ENTROPY_CHUNK);
					let take_usize = take as usize;
					// A source/guest fault leaves the bytes already written valid;
					// stop this descriptor and complete the chain with that count.
					if Self::fill_entropy(&mut self.source, &mut buf[..take_usize]).is_err()
						|| mem.write_slice(&buf[..take_usize], addr).is_err()
					{
						break;
					}
					let (Some(next_addr), Some(next_written)) =
						(addr.checked_add(u64::from(take)), written.checked_add(take))
					else {
						break;
					};
					addr = next_addr;
					written = next_written;
					remaining -= take;
				}
			}
			queue
				.add_used(&mem, head, written)
				.map_err(|e| err(format!("virtio-rng used-ring update failed: {e}")))?;
			used = true;
		}
		Ok(used)
	}
}

impl VirtioDevice for Rng {
	fn device_type(&self) -> u32 {
		VIRTIO_ID_RNG
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

	fn read_config(&self, _offset: u64, data: &mut [u8]) {
		// virtio-rng has no device-specific config space.
		data.fill(0);
	}

	fn write_config(&mut self, _offset: u64, _data: &[u8]) {
		// virtio-rng has no device-specific config space.
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
		self.acked_features = 0;
		self.mem = None;
		self.interrupt = None;
		self.queue = None;
		Ok(())
	}

	fn process_queue_notify(&mut self) -> Result<QueuePass> {
		let mut budget = QUEUE_PASS_BUDGET;
		if self.process_requestq(&mut budget)? {
			self.signal_used()?;
		}
		Ok(if budget == 0 {
			QueuePass::Budgeted
		} else {
			QueuePass::Drained
		})
	}

	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		// One requestq; mirrors the transport's single-queue vec for snapshots.
		self.queue.as_ref().map(|q| q.state()).into_iter().collect()
	}
}

#[cfg(test)]
mod tests {
	use virtio_bindings::bindings::virtio_ring::VRING_DESC_F_WRITE;
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
	const BUF_ADDR: u64 = 0x4000;

	fn guest_mem() -> GuestMemoryMmap {
		GuestMemoryMmap::from_ranges(&[(GuestAddress(0), MEM_SIZE)]).expect("guest memory")
	}

	fn queue_with_descs(mem: &GuestMemoryMmap, descs: &[SplitDescriptor]) -> Queue {
		for (idx, desc) in descs.iter().enumerate() {
			mem.write_obj(RawDescriptor::from(*desc), GuestAddress(DESC_TABLE + (idx as u64 * 16)))
				.expect("write descriptor");
		}
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING))
			.expect("write avail flags");
		mem.write_obj(u16::to_le(1), GuestAddress(AVAIL_RING + 2))
			.expect("write avail idx");
		mem.write_obj(u16::to_le(0), GuestAddress(AVAIL_RING + 4))
			.expect("write avail head");

		let mut queue = Queue::new(8).expect("queue");
		queue.set_size(8);
		queue.set_desc_table_address(Some(DESC_TABLE as u32), Some((DESC_TABLE >> 32) as u32));
		queue.set_avail_ring_address(Some(AVAIL_RING as u32), Some((AVAIL_RING >> 32) as u32));
		queue.set_used_ring_address(Some(USED_RING as u32), Some((USED_RING >> 32) as u32));
		queue.set_ready(true);
		queue
	}

	fn used_len(mem: &GuestMemoryMmap) -> u32 {
		u32::from_le(
			mem.read_obj::<u32>(GuestAddress(USED_RING + 8))
				.expect("read used len"),
		)
	}

	fn used_idx(mem: &GuestMemoryMmap) -> u16 {
		u16::from_le(
			mem.read_obj::<u16>(GuestAddress(USED_RING + 2))
				.expect("read used idx"),
		)
	}

	fn rng_with_queue(mem: GuestMemoryMmap, queue: Queue) -> Rng {
		let mut rng = Rng::new().expect("rng");
		rng.mem = Some(mem);
		rng.queue = Some(queue);
		rng
	}

	#[test]
	fn fills_write_only_buffer_with_entropy() {
		let mem = guest_mem();
		let len = 64u32;
		// Sentinel so the test can prove entropy overwrote the guest buffer.
		mem.write_slice(&[0xaa; 64], GuestAddress(BUF_ADDR))
			.expect("seed sentinel");
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			BUF_ADDR,
			len,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let mut rng = rng_with_queue(mem, queue);
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(rng.process_requestq(&mut budget).expect("process"));

		let mem = rng.mem.as_ref().expect("mem");
		assert_eq!(used_len(mem), len);
		assert_eq!(used_idx(mem), 1);
		let mut out = [0u8; 64];
		mem.read_slice(&mut out, GuestAddress(BUF_ADDR))
			.expect("read filled buffer");
		assert!(out.iter().any(|&b| b != 0xaa), "entropy should overwrite the sentinel buffer");
	}

	#[test]
	fn rejects_read_only_descriptor_without_writing() {
		let mem = guest_mem();
		// A guest-readable (non-write) descriptor is invalid for virtio-rng; the
		// chain is still completed, with zero bytes written.
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(BUF_ADDR, 64, 0, 0)]);
		let mut rng = rng_with_queue(mem, queue);
		let mut budget = QUEUE_PASS_BUDGET;
		assert!(rng.process_requestq(&mut budget).expect("process"));

		let mem = rng.mem.as_ref().expect("mem");
		assert_eq!(used_len(mem), 0);
		assert_eq!(used_idx(mem), 1);
	}

	#[test]
	fn queue_states_reflect_activation() {
		let rng = Rng::new().expect("rng");
		let states = rng.queue_states();
		assert!(states.is_empty(), "unexpected queue states: {states:?}");

		let mem = guest_mem();
		let queue = queue_with_descs(&mem, &[SplitDescriptor::new(
			BUF_ADDR,
			64,
			VRING_DESC_F_WRITE as u16,
			0,
		)]);
		let rng = rng_with_queue(mem, queue);
		assert_eq!(rng.queue_states().len(), 1);
	}
}
