//! virtio core: device trait, shared interrupt, and the device worker loops.
//!
//! On unix hosts every virtio device of a VM is serviced by ONE shared
//! event-loop thread ([`run_shared_worker`]); Windows keeps one thread per
//! device ([`run_worker`]) because `WaitForMultipleObjects` caps a wait set at
//! 64 handles.

pub mod block;
#[cfg(any(target_os = "linux", test))]
mod block_remote;
pub mod console;
#[cfg(not(target_os = "windows"))]
pub mod fs;
#[cfg(target_os = "windows")]
#[path = "fs_windows.rs"]
pub mod fs;
mod fuse_errno;
pub mod mmio;
pub mod net;
#[cfg(all(target_arch = "x86_64", not(target_os = "windows")))]
pub mod pci;
pub mod remotefs;
pub mod rng;

#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(target_os = "windows")]
use std::os::windows::io::{AsRawHandle, RawHandle};
use std::{
	io,
	path::Path,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
	},
};

use parking_lot::Mutex;
use virtio_queue::Queue;
use vm_memory::{GuestAddress, GuestMemory};
#[cfg(target_os = "windows")]
use windows_sys::Win32::Foundation::{HANDLE, WAIT_FAILED, WAIT_OBJECT_0};
#[cfg(target_os = "windows")]
use windows_sys::Win32::System::Threading::{INFINITE, WaitForMultipleObjects};

use crate::{
	hv::IrqLine,
	memory::GuestMemoryMmap,
	os::EventFd,
	result::{Result, err},
};

/// virtio-mmio `InterruptStatus` bits.
pub const VIRTIO_MMIO_INT_VRING: u32 = 0x1;

pub fn descriptor_range_valid(mem: &GuestMemoryMmap, addr: GuestAddress, len: u32) -> bool {
	len == 0 || mem.check_range(addr, len as usize)
}

#[cfg(target_arch = "x86_64")]
type InterruptNotifier = Arc<dyn Fn() -> Result<bool> + Send + Sync>;

/// Host wait primitive consumed by a virtio worker.
#[cfg(unix)]
pub type WorkerWaitSource = RawFd;
/// Host wait primitive consumed by a virtio worker.
#[cfg(target_os = "windows")]
pub type WorkerWaitSource = RawHandle;

/// Shared interrupt state for one virtio device.
///
/// `status` backs the transport ISR/InterruptStatus register. `irq_line` is the
/// hypervisor-coupled guest interrupt line; PCI transports may install a
/// notifier that attempts MSI-X injection first and falls back to this line.
pub struct Interrupt {
	status:   AtomicU32,
	irq_line: IrqLine,
	#[cfg(target_arch = "x86_64")]
	notifier: Mutex<Option<InterruptNotifier>>,
}

impl Interrupt {
	/// Create interrupt state backed by the hypervisor-coupled guest IRQ line.
	pub const fn new(irq_line: IrqLine) -> Self {
		Self {
			status: AtomicU32::new(0),
			irq_line,
			#[cfg(target_arch = "x86_64")]
			notifier: Mutex::new(None),
		}
	}

	/// Read the current virtio transport ISR/InterruptStatus bits.
	pub fn status(&self) -> u32 {
		self.status.load(Ordering::SeqCst)
	}

	/// Clear the `InterruptStatus` bits the driver acked.
	pub fn ack(&self, value: u32) {
		self.status.fetch_and(!value, Ordering::SeqCst);
	}

	/// Restore the `InterruptStatus` register from a snapshot.
	pub fn restore_status(&self, value: u32) {
		self.status.store(value, Ordering::SeqCst);
	}

	/// Clear pending transport interrupt state during a device reset.
	pub fn clear(&self) {
		self.status.store(0, Ordering::SeqCst);
	}

	/// Signal "used buffer notification" and raise the transport interrupt.
	pub fn signal_used_queue(&self) -> Result<()> {
		self
			.status
			.fetch_or(VIRTIO_MMIO_INT_VRING, Ordering::SeqCst);
		#[cfg(target_arch = "x86_64")]
		if self.notify_msi() {
			return Ok(());
		}
		self.irq_line.trigger()?;
		Ok(())
	}
}

#[cfg(target_arch = "x86_64")]
impl Interrupt {
	/// Install the virtio-pci MSI-X notifier, returning true when it delivers.
	pub fn set_notifier(&self, notifier: impl Fn() -> Result<bool> + Send + Sync + 'static) {
		*self.notifier.lock() = Some(Arc::new(notifier));
	}

	/// Read and clear all pending interrupt status bits (virtio-pci ISR read).
	pub fn take_status(&self) -> u32 {
		self.status.swap(0, Ordering::SeqCst)
	}

	fn notify_msi(&self) -> bool {
		let notifier = self.notifier.lock().clone();
		notifier
			.as_ref()
			.is_some_and(|notify| notify().unwrap_or(false))
	}
}

/// Result of cloning an active virtio-block backing file for a disk-only
/// recovery point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskCapture {
	pub bytes:  u64,
	pub method: DiskCaptureMethod,
}

/// Copy mechanism used for a disk recovery artifact.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskCaptureMethod {
	/// The filesystem created a copy-on-write reflink.
	#[cfg_attr(
		not(target_os = "linux"),
		allow(dead_code, reason = "constructed by Linux FICLONE disk recovery capture")
	)]
	Reflink,
	/// Reflink was unavailable; the source was sparse-copied while its block
	/// worker remained quiesced.
	SparseCopy,
}

impl DiskCaptureMethod {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Reflink => "reflink",
			Self::SparseCopy => "sparse-copy",
		}
	}
}

/// A virtio device backend (block, net, ...).
///
/// The transport handles the control-plane registers and, on `DRIVER_OK`, calls
/// [`activate`](VirtioDevice::activate), handing over guest memory, the shared
/// interrupt, and the configured queues. Data-plane work then runs on the VM's
/// shared device worker thread driven by [`run_shared_worker`] (one thread per
/// device on Windows, via [`run_worker`]).
pub trait VirtioDevice: Send {
	/// virtio device type id (e.g. NET = 1, BLOCK = 2).
	fn device_type(&self) -> u32;
	/// Maximum size of each virtqueue, one entry per queue.
	fn queue_max_sizes(&self) -> &[u16];
	/// Feature bits offered to the driver.
	fn features(&self) -> u64;
	/// Record the subset of features the driver accepted.
	fn ack_features(&mut self, value: u64);
	/// Read device-specific config space.
	fn read_config(&self, offset: u64, data: &mut [u8]);
	/// Write device-specific config space.
	fn write_config(&mut self, offset: u64, data: &[u8]);
	/// Take ownership of the data-plane resources once the driver is ready.
	fn activate(
		&mut self,
		mem: GuestMemoryMmap,
		interrupt: Arc<Interrupt>,
		queues: Vec<Queue>,
	) -> Result<()>;
	/// Drop active data-plane state after the driver resets the device.
	///
	/// Backends must stop using queues, guest memory, interrupt handles, and any
	/// in-flight state from the previous activation. Configuration that belongs
	/// to the device itself may be retained.
	fn reset(&mut self) -> Result<()> {
		Ok(())
	}
	/// Extra host wait sources besides the queue notification object.
	fn worker_wait_sources(&self) -> Vec<WorkerWaitSource> {
		Vec::new()
	}
	/// Service one bounded pass of queue work after a guest notification.
	///
	/// A pass services at most [`QUEUE_PASS_BUDGET`] descriptor chains so a
	/// saturated queue cannot monopolize the shared device loop. When
	/// [`QueuePass::Budgeted`] is returned the worker re-arms the queue event
	/// and calls again on a later wake, interleaving other devices' sources.
	fn process_queue_notify(&mut self) -> Result<QueuePass>;
	/// One of [`worker_wait_sources`](VirtioDevice::worker_wait_sources) became
	/// ready.
	fn process_worker_source(&mut self, _index: usize) -> Result<()> {
		Ok(())
	}
	/// Live virtqueue state for snapshotting (read AFTER activation, when the
	/// device owns the queues). Default: no queues. Block/Net override this.
	fn queue_states(&self) -> Vec<virtio_queue::QueueState> {
		Vec::new()
	}
	/// Optional serialized backend state for user-mode networking. Non-net
	/// devices and TAP/vmnet net backends return `None`.
	fn user_net_state(&self) -> Result<Option<Vec<u8>>> {
		Ok(None)
	}
	/// Quiesce in-flight backend IO before a snapshot (e.g. drain async
	/// completions). Default: nothing to do. Block (`io_uring`) overrides this.
	fn drain(&mut self) -> Result<()> {
		Ok(())
	}
	/// Clone the currently attached backing image into `destination` for a
	/// disk-only recovery point. The caller has stopped this device's worker,
	/// so implementations may establish a precise write boundary without
	/// parking vCPUs. Non-block devices reject the operation.
	fn capture_disk(&mut self, _destination: &Path) -> Result<DiskCapture> {
		Err(err("disk-only recovery requires a virtio-block device"))
	}
}

/// Maximum descriptor chains a device services in one queue-notify pass.
///
/// Inline-completion devices post used entries while draining, so a spinning
/// guest can keep the available ring non-empty indefinitely; the budget turns
/// that into bounded passes the shared worker interleaves across devices.
pub const QUEUE_PASS_BUDGET: usize = 256;

/// Outcome of one bounded [`VirtioDevice::process_queue_notify`] pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePass {
	/// Every currently queued request was serviced.
	Drained,
	/// The pass budget was exhausted with requests possibly still queued; the
	/// worker re-arms the queue event and services the rest on a later wake.
	Budgeted,
}

#[cfg(target_os = "windows")]
const QUEUE_TOKEN: u64 = 0;
#[cfg(target_os = "windows")]
const KILL_TOKEN: u64 = 1;
#[cfg(target_os = "windows")]
const PAUSE_TOKEN: u64 = 2;
#[cfg(target_os = "windows")]
const RESUME_TOKEN: u64 = 3;
#[cfg(target_os = "windows")]
const FD_TOKEN_BASE: u64 = 4;

/// Pause-coordination eventfds shared between the VMM and a device worker.
///
/// During VM pause/snapshot the VMM writes `pause_evt`; the worker quiesces and
/// writes `ack_evt` to confirm it has stopped servicing queues, or
/// `failure_evt` if quiescing fails. The VMM later writes `resume_evt` to
/// release the worker back into its normal loop.
pub struct WorkerControl {
	/// VMM writes 1 -> worker should quiesce.
	pub pause_evt:        EventFd,
	/// VMM writes 1 -> worker resumes.
	pub resume_evt:       EventFd,
	/// Worker writes 1 once quiesced (VMM waits on this).
	pub ack_evt:          EventFd,
	/// Worker writes 1 if quiescing fails; the VMM rejects the pause/snapshot.
	pub failure_evt:      EventFd,
	/// Human-readable failure recorded before `failure_evt` is signalled.
	pub failure_msg:      Arc<Mutex<Option<String>>>,
	/// The VMM's current request. Clearing this before `resume_evt` makes a
	/// timed-out pause cancellable even when a worker has not yet consumed its
	/// pause event, so a failed disk capture cannot strand the block path.
	pub pause_requested:  Arc<AtomicBool>,
	/// Monotonically increasing pause request generation. Each pause requester
	/// publishes a new generation before waking the worker.
	pub pause_generation: Arc<AtomicU64>,
	/// Generation the worker has fully quiesced. It is stored before `ack_evt`
	/// is signalled so a later request cannot accept a stale wakeup.
	pub ack_generation:   Arc<AtomicU64>,
}

/// True when the pause request `generation` was cancelled or superseded.
fn pause_cancelled(control: &WorkerControl, generation: u64) -> bool {
	!control.pause_requested.load(Ordering::Acquire)
		|| control.pause_generation.load(Ordering::Acquire) != generation
}

/// Record a fatal device failure on its control block and return the error.
fn fail_device(control: &WorkerControl, message: String) -> crate::result::Error {
	*control.failure_msg.lock() = Some(message.clone());
	let _ = control.failure_evt.write(1);
	err(message)
}

/// Fully drain a device's queues and backend for a pause request, recording a
/// failure on `control` when quiescing fails. Unlike the normal bounded
/// passes, a pause drains to completion: the ack must guarantee quiescence.
fn quiesce_for_pause(device: &Arc<Mutex<dyn VirtioDevice>>, control: &WorkerControl) -> Result<()> {
	let mut dev = device.lock();
	loop {
		match dev.process_queue_notify() {
			Ok(QueuePass::Drained) => break,
			Ok(QueuePass::Budgeted) => {},
			Err(e) => return Err(fail_device(control, format!("pause queue drain failed: {e}"))),
		}
	}
	dev.drain()
		.map_err(|e| fail_device(control, format!("pause drain failed: {e}")))
}

/// Per-device worker loop, Windows only: `WaitForMultipleObjects` caps a wait
/// set at [`WINDOWS_MAXIMUM_WAIT_OBJECTS`] handles, so consolidating every
/// device of a VM into one thread would not scale there anyway. Unix hosts
/// use [`run_shared_worker`] instead.
#[cfg(target_os = "windows")]
pub fn run_worker(
	device: Arc<Mutex<dyn VirtioDevice>>,
	queue_evt: EventFd,
	kill_evt: EventFd,
	control: WorkerControl,
) -> Result<()> {
	let extra_sources = device.lock().worker_wait_sources();
	let mut wait_set = WorkerWaitSet::with_capacity(4 + extra_sources.len());
	wait_set.push(event_wait_source(&queue_evt), QUEUE_TOKEN);
	wait_set.push(event_wait_source(&kill_evt), KILL_TOKEN);
	wait_set.push(event_wait_source(&control.pause_evt), PAUSE_TOKEN);
	wait_set.push(event_wait_source(&control.resume_evt), RESUME_TOKEN);
	for (index, source) in extra_sources.iter().enumerate() {
		wait_set.push(*source, FD_TOKEN_BASE + index as u64);
	}

	// While paused, do not consume queue/backend notifications: they must remain
	// pending until resume because the frozen guest cannot ring them again.
	let mut pause_wait_set = WorkerWaitSet::with_capacity(2);
	pause_wait_set.push(event_wait_source(&control.resume_evt), RESUME_TOKEN);
	pause_wait_set.push(event_wait_source(&kill_evt), KILL_TOKEN);

	loop {
		let (token, _) = wait_set.wait()?;
		match token {
			QUEUE_TOKEN => {
				let _ = queue_evt.read();
				if device.lock().process_queue_notify()? == QueuePass::Budgeted {
					// Re-arm and fall back to the wait set so kill/pause stay
					// responsive between bounded passes.
					let _ = queue_evt.write(1);
				}
			},
			KILL_TOKEN => {
				let _ = kill_evt.read();
				return Ok(());
			},
			PAUSE_TOKEN => {
				let _ = control.pause_evt.read();
				if !control.pause_requested.load(Ordering::Acquire) {
					continue;
				}
				let requested_generation = control.pause_generation.load(Ordering::Acquire);
				quiesce_for_pause(&device, &control)?;
				if pause_cancelled(&control, requested_generation) {
					continue;
				}
				control
					.ack_generation
					.store(requested_generation, Ordering::Release);
				let _ = control.ack_evt.write(1);
				if pause_cancelled(&control, requested_generation) {
					continue;
				}
				'paused: loop {
					let (pause_token, _) = pause_wait_set.wait()?;
					match pause_token {
						RESUME_TOKEN => {
							let _ = control.resume_evt.read();
							if pause_cancelled(&control, requested_generation) {
								break 'paused;
							}
						},
						KILL_TOKEN => {
							let _ = kill_evt.read();
							return Ok(());
						},
						_ => {},
					}
				}
			},
			RESUME_TOKEN => {
				let _ = control.resume_evt.read();
			},
			token => {
				let index = (token - FD_TOKEN_BASE) as usize;
				device.lock().process_worker_source(index)?;
			},
		}
	}
}

/// One virtio device's worker wiring handed to [`run_shared_worker`].
#[cfg(unix)]
pub struct DeviceWorker {
	pub device:    Arc<Mutex<dyn VirtioDevice>>,
	pub queue_evt: EventFd,
	pub control:   WorkerControl,
}

#[cfg(unix)]
struct DeviceSlot {
	device:    Arc<Mutex<dyn VirtioDevice>>,
	queue_evt: EventFd,
	control:   WorkerControl,
	extra:     Vec<WorkerWaitSource>,
	/// `Some(acked pause generation)` while the device is quiesced. A paused
	/// device's queue/backend sources leave the poll set so their
	/// notifications stay pending until resume.
	paused:    Option<u64>,
}

/// Poll-set entry of the shared device loop.
#[cfg(unix)]
#[derive(Clone, Copy)]
enum SharedSource {
	/// The VM-global kill event: terminate the loop.
	Kill,
	/// A device's queue-notify event.
	Queue(usize),
	/// A device's pause request event.
	Pause(usize),
	/// A device's resume event.
	Resume(usize),
	/// One of a device's extra backend wait sources.
	Extra { slot: usize, source: usize },
}

/// Drive every virtio device of one VM from a single worker thread.
///
/// Replaces the historical one-thread-per-device model on unix hosts, where
/// per-device worker threads dominate the host thread count at thousands of
/// sandboxes. Per device the loop waits on the queue notification, the extra
/// backend sources, and the pause/resume control eventfds; `kill_evt` is
/// VM-global and terminates the loop.
///
/// The per-device pause protocol is bit-identical to the historical
/// per-device worker: on `pause_evt` the device quiesces and acks
/// independently (per-device ack/failure eventfds and generations), and while
/// paused its queue/backend notifications are left pending so the frozen
/// guest never loses a doorbell — meanwhile every other device keeps being
/// serviced.
///
/// Fairness: each ready source gets one bounded servicing pass per poll wake
/// ([`QUEUE_PASS_BUDGET`]); an exhausted pass re-arms its queue event instead
/// of looping in place, so one saturated device cannot starve its siblings.
/// Backend fds (tap, `io_uring`, ...) are level-triggered, so a bounded pass
/// there re-fires poll on its own.
#[cfg(unix)]
pub fn run_shared_worker(devices: Vec<DeviceWorker>, kill_evt: EventFd) -> Result<()> {
	let mut slots: Vec<DeviceSlot> = devices
		.into_iter()
		.map(|worker| {
			let extra = worker.device.lock().worker_wait_sources();
			DeviceSlot {
				device: worker.device,
				queue_evt: worker.queue_evt,
				control: worker.control,
				extra,
				paused: None,
			}
		})
		.collect();

	let mut poll_fds: Vec<libc::pollfd> = Vec::new();
	let mut sources: Vec<SharedSource> = Vec::new();
	let mut ready: Vec<SharedSource> = Vec::new();
	let mut rebuild = true;
	loop {
		if rebuild {
			rebuild_wait_set(&kill_evt, &slots, &mut poll_fds, &mut sources);
			rebuild = false;
		}
		wait_poll(&mut poll_fds)?;
		// Collect the whole ready batch first: servicing every ready source
		// once per wake (instead of always restarting at the first) keeps one
		// chatty device from starving the rest.
		ready.clear();
		for index in 0..poll_fds.len() {
			if take_revents(&mut poll_fds[index])? {
				ready.push(sources[index]);
			}
		}
		for entry in &ready {
			match *entry {
				SharedSource::Kill => {
					let _ = kill_evt.read();
					return Ok(());
				},
				SharedSource::Queue(index) => {
					let slot = &mut slots[index];
					if slot.paused.is_some() {
						// Paused earlier in this same batch: leave the
						// notification pending until resume.
						continue;
					}
					let _ = slot.queue_evt.read();
					let pass =
						slot.device.lock().process_queue_notify().map_err(|e| {
							fail_device(&slot.control, format!("device worker exited: {e}"))
						})?;
					if pass == QueuePass::Budgeted {
						let _ = slot.queue_evt.write(1);
					}
				},
				SharedSource::Pause(index) => {
					let slot = &mut slots[index];
					let _ = slot.control.pause_evt.read();
					if !slot.control.pause_requested.load(Ordering::Acquire) {
						continue;
					}
					let requested_generation = slot.control.pause_generation.load(Ordering::Acquire);
					quiesce_for_pause(&slot.device, &slot.control)?;
					if pause_cancelled(&slot.control, requested_generation) {
						continue;
					}
					slot
						.control
						.ack_generation
						.store(requested_generation, Ordering::Release);
					let _ = slot.control.ack_evt.write(1);
					if pause_cancelled(&slot.control, requested_generation) {
						continue;
					}
					slot.paused = Some(requested_generation);
					rebuild = true;
				},
				SharedSource::Resume(index) => {
					let slot = &mut slots[index];
					let _ = slot.control.resume_evt.read();
					if let Some(generation) = slot.paused
						&& pause_cancelled(&slot.control, generation)
					{
						slot.paused = None;
						rebuild = true;
					}
				},
				SharedSource::Extra { slot: index, source } => {
					let slot = &mut slots[index];
					if slot.paused.is_some() {
						continue;
					}
					slot
						.device
						.lock()
						.process_worker_source(source)
						.map_err(|e| fail_device(&slot.control, format!("device worker exited: {e}")))?;
				},
			}
		}
	}
}

/// Rebuild the shared loop's poll set from the current pause states.
#[cfg(unix)]
fn rebuild_wait_set(
	kill_evt: &EventFd,
	slots: &[DeviceSlot],
	poll_fds: &mut Vec<libc::pollfd>,
	sources: &mut Vec<SharedSource>,
) {
	poll_fds.clear();
	sources.clear();
	let mut push = |fd: RawFd, source: SharedSource| {
		poll_fds.push(libc::pollfd { fd, events: libc::POLLIN, revents: 0 });
		sources.push(source);
	};
	push(event_wait_source(kill_evt), SharedSource::Kill);
	for (index, slot) in slots.iter().enumerate() {
		if slot.paused.is_some() {
			// While paused only resume (and the global kill) may wake the
			// device: queue/backend notifications must stay pending because
			// the frozen guest cannot ring them again.
			push(event_wait_source(&slot.control.resume_evt), SharedSource::Resume(index));
			continue;
		}
		push(event_wait_source(&slot.queue_evt), SharedSource::Queue(index));
		push(event_wait_source(&slot.control.pause_evt), SharedSource::Pause(index));
		push(event_wait_source(&slot.control.resume_evt), SharedSource::Resume(index));
		for (source, fd) in slot.extra.iter().enumerate() {
			push(*fd, SharedSource::Extra { slot: index, source });
		}
	}
}

#[cfg(target_os = "windows")]
struct WorkerWaitSet {
	handles: Vec<RawHandle>,
	tokens:  Vec<u64>,
}

#[cfg(target_os = "windows")]
impl WorkerWaitSet {
	fn with_capacity(capacity: usize) -> Self {
		Self { handles: Vec::with_capacity(capacity), tokens: Vec::with_capacity(capacity) }
	}

	fn push(&mut self, source: WorkerWaitSource, token: u64) {
		self.handles.push(source);
		self.tokens.push(token);
	}

	fn wait(&self) -> io::Result<(u64, usize)> {
		wait_handles(&self.handles).map(|index| (self.tokens[index], index))
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

#[cfg(unix)]
fn wait_poll(poll_fds: &mut [libc::pollfd]) -> io::Result<()> {
	loop {
		// SAFETY: `poll_fds` remains live and its length is passed unchanged.
		let n = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as libc::nfds_t, -1) };
		if n >= 0 {
			return Ok(());
		}
		let e = io::Error::last_os_error();
		if e.kind() != io::ErrorKind::Interrupted {
			return Err(e);
		}
	}
}

#[cfg(unix)]
fn take_revents(poll_fd: &mut libc::pollfd) -> io::Result<bool> {
	let revents = poll_fd.revents;
	poll_fd.revents = 0;
	if revents & libc::POLLNVAL != 0 {
		return Err(io::Error::from_raw_os_error(libc::EBADF));
	}
	Ok(revents != 0)
}

#[cfg(target_os = "windows")]
const WINDOWS_MAXIMUM_WAIT_OBJECTS: usize = 64;

#[cfg(target_os = "windows")]
fn wait_handles(handles: &[RawHandle]) -> io::Result<usize> {
	if handles.is_empty() || handles.len() > WINDOWS_MAXIMUM_WAIT_OBJECTS {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("invalid Windows worker wait set size {}", handles.len()),
		));
	}
	// SAFETY: every entry is a live waitable handle retained by the worker.
	let result = unsafe {
		WaitForMultipleObjects(handles.len() as u32, handles.as_ptr().cast::<HANDLE>(), 0, INFINITE)
	};
	if result < WAIT_OBJECT_0 + handles.len() as u32 {
		return Ok((result - WAIT_OBJECT_0) as usize);
	}
	if result == WAIT_FAILED {
		return Err(io::Error::last_os_error());
	}
	Err(io::Error::other(format!("unexpected WaitForMultipleObjects result {result:#x}")))
}

#[cfg(all(test, unix))]
mod tests {
	use std::{
		sync::{
			Arc,
			atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
			mpsc::{Receiver, Sender, channel},
		},
		thread,
		time::{Duration, Instant},
	};

	use super::*;

	/// VMM-side clones of one device's control eventfds, mirroring what
	/// `DeviceHandle` keeps in vmm.rs.
	struct VmmSide {
		pause_evt:        EventFd,
		resume_evt:       EventFd,
		ack_evt:          EventFd,
		#[allow(dead_code, reason = "kept alive so the worker-side clone stays valid")]
		failure_evt:      EventFd,
		pause_requested:  Arc<AtomicBool>,
		pause_generation: Arc<AtomicU64>,
		ack_generation:   Arc<AtomicU64>,
	}

	impl VmmSide {
		/// Publish a new pause request and wake the worker (vmm.rs
		/// `finish_pause` sequencing).
		fn request_pause(&self) -> u64 {
			let generation = self.pause_generation.fetch_add(1, Ordering::AcqRel) + 1;
			self.pause_requested.store(true, Ordering::Release);
			self.pause_evt.write(1).unwrap();
			generation
		}

		/// Cancel the pause request and release the worker.
		fn release(&self) {
			self.pause_requested.store(false, Ordering::Release);
			self.resume_evt.write(1).unwrap();
		}
	}

	fn worker_control() -> (WorkerControl, VmmSide) {
		let pause_evt = EventFd::new(0).unwrap();
		let resume_evt = EventFd::new(0).unwrap();
		let ack_evt = EventFd::new(0).unwrap();
		let failure_evt = EventFd::new(0).unwrap();
		let pause_requested = Arc::new(AtomicBool::new(false));
		let pause_generation = Arc::new(AtomicU64::new(0));
		let ack_generation = Arc::new(AtomicU64::new(0));
		let control = WorkerControl {
			pause_evt:        pause_evt.try_clone().unwrap(),
			resume_evt:       resume_evt.try_clone().unwrap(),
			ack_evt:          ack_evt.try_clone().unwrap(),
			failure_evt:      failure_evt.try_clone().unwrap(),
			failure_msg:      Arc::new(Mutex::new(None)),
			pause_requested:  pause_requested.clone(),
			pause_generation: pause_generation.clone(),
			ack_generation:   ack_generation.clone(),
		};
		let vmm = VmmSide {
			pause_evt,
			resume_evt,
			ack_evt,
			failure_evt,
			pause_requested,
			pause_generation,
			ack_generation,
		};
		(control, vmm)
	}

	fn wait_for(counter: &AtomicUsize, at_least: usize) {
		let deadline = Instant::now() + Duration::from_secs(5);
		while counter.load(Ordering::Acquire) < at_least {
			assert!(Instant::now() < deadline, "timed out waiting for device servicing");
			thread::sleep(Duration::from_millis(1));
		}
	}

	macro_rules! virtio_device_boilerplate {
		() => {
			fn device_type(&self) -> u32 {
				0
			}

			fn queue_max_sizes(&self) -> &[u16] {
				&[]
			}

			fn features(&self) -> u64 {
				0
			}

			fn ack_features(&mut self, _value: u64) {}

			fn read_config(&self, _offset: u64, _data: &mut [u8]) {}

			fn write_config(&mut self, _offset: u64, _data: &[u8]) {}

			fn activate(
				&mut self,
				_mem: GuestMemoryMmap,
				_interrupt: Arc<Interrupt>,
				_queues: Vec<Queue>,
			) -> Result<()> {
				Ok(())
			}
		};
	}

	struct IdleDevice;

	impl VirtioDevice for IdleDevice {
		virtio_device_boilerplate!();

		fn process_queue_notify(&mut self) -> Result<QueuePass> {
			Ok(QueuePass::Drained)
		}
	}

	/// Counts queue-notify passes so tests can observe dispatch.
	struct CountingDevice {
		notifies: Arc<AtomicUsize>,
	}

	impl VirtioDevice for CountingDevice {
		virtio_device_boilerplate!();

		fn process_queue_notify(&mut self) -> Result<QueuePass> {
			self.notifies.fetch_add(1, Ordering::AcqRel);
			Ok(QueuePass::Drained)
		}
	}

	/// Reports an exhausted budget for the first `budgeted` passes, forcing the
	/// worker to re-arm the queue event between passes.
	struct BudgetedDevice {
		passes:   Arc<AtomicUsize>,
		budgeted: usize,
	}

	impl VirtioDevice for BudgetedDevice {
		virtio_device_boilerplate!();

		fn process_queue_notify(&mut self) -> Result<QueuePass> {
			let pass = self.passes.fetch_add(1, Ordering::AcqRel);
			if pass < self.budgeted {
				Ok(QueuePass::Budgeted)
			} else {
				Ok(QueuePass::Drained)
			}
		}
	}

	/// Blocks inside `process_queue_notify` until the test releases it, so a
	/// pause request can be cancelled and superseded mid-quiesce.
	struct DelayedPauseDevice {
		entered: Sender<()>,
		release: Receiver<()>,
	}

	impl VirtioDevice for DelayedPauseDevice {
		virtio_device_boilerplate!();

		fn process_queue_notify(&mut self) -> Result<QueuePass> {
			self.entered.send(()).unwrap();
			self.release.recv().unwrap();
			Ok(QueuePass::Drained)
		}
	}

	struct TestWorker {
		queue_evt: EventFd,
		worker:    DeviceWorker,
	}

	fn test_worker(device: impl VirtioDevice + 'static, control: WorkerControl) -> TestWorker {
		let queue_evt = EventFd::new(0).unwrap();
		TestWorker {
			worker: DeviceWorker {
				device: Arc::new(Mutex::new(device)),
				queue_evt: queue_evt.try_clone().unwrap(),
				control,
			},
			queue_evt,
		}
	}

	fn spawn_loop(workers: Vec<DeviceWorker>) -> (EventFd, thread::JoinHandle<Result<()>>) {
		let kill_evt = EventFd::new(0).unwrap();
		let loop_kill = kill_evt.try_clone().unwrap();
		let handle = thread::spawn(move || run_shared_worker(workers, loop_kill));
		(kill_evt, handle)
	}

	#[test]
	fn kill_terminates_shared_loop() {
		let (control_a, _vmm_a) = worker_control();
		let (control_b, _vmm_b) = worker_control();
		let a = test_worker(IdleDevice, control_a);
		let b = test_worker(IdleDevice, control_b);
		let (kill_evt, handle) = spawn_loop(vec![a.worker, b.worker]);

		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn shared_loop_dispatches_queue_events_to_each_device() {
		let notifies_a = Arc::new(AtomicUsize::new(0));
		let notifies_b = Arc::new(AtomicUsize::new(0));
		let (control_a, _vmm_a) = worker_control();
		let (control_b, _vmm_b) = worker_control();
		let a = test_worker(CountingDevice { notifies: notifies_a.clone() }, control_a);
		let b = test_worker(CountingDevice { notifies: notifies_b.clone() }, control_b);
		let (kill_evt, handle) = spawn_loop(vec![a.worker, b.worker]);

		a.queue_evt.write(1).unwrap();
		b.queue_evt.write(1).unwrap();
		wait_for(&notifies_a, 1);
		wait_for(&notifies_b, 1);

		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn paused_device_defers_its_queue_and_does_not_block_sibling() {
		let notifies_a = Arc::new(AtomicUsize::new(0));
		let notifies_b = Arc::new(AtomicUsize::new(0));
		let (control_a, vmm_a) = worker_control();
		let (control_b, _vmm_b) = worker_control();
		let a = test_worker(CountingDevice { notifies: notifies_a.clone() }, control_a);
		let b = test_worker(CountingDevice { notifies: notifies_b.clone() }, control_b);
		let (kill_evt, handle) = spawn_loop(vec![a.worker, b.worker]);

		// Pause device A; it acks independently while B stays live. The pause
		// quiesce itself runs one queue-notify pass, so record that baseline.
		let generation = vmm_a.request_pause();
		vmm_a.ack_evt.read().unwrap();
		assert_eq!(vmm_a.ack_generation.load(Ordering::Acquire), generation);
		let baseline = notifies_a.load(Ordering::Acquire);

		// A's doorbell stays pending while paused; B's is serviced normally.
		a.queue_evt.write(1).unwrap();
		b.queue_evt.write(1).unwrap();
		wait_for(&notifies_b, 1);
		thread::sleep(Duration::from_millis(20));
		assert_eq!(
			notifies_a.load(Ordering::Acquire),
			baseline,
			"paused device must not be serviced"
		);

		// Releasing A delivers the deferred doorbell.
		vmm_a.release();
		wait_for(&notifies_a, baseline + 1);

		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn pause_ack_resume_across_two_devices_is_independent() {
		let (control_a, vmm_a) = worker_control();
		let (control_b, vmm_b) = worker_control();
		let a = test_worker(IdleDevice, control_a);
		let b = test_worker(IdleDevice, control_b);
		let (kill_evt, handle) = spawn_loop(vec![a.worker, b.worker]);

		// Sequential per-device pause/ack, exactly like vmm.rs finish_pause.
		let generation_a = vmm_a.request_pause();
		vmm_a.ack_evt.read().unwrap();
		assert_eq!(vmm_a.ack_generation.load(Ordering::Acquire), generation_a);
		let generation_b = vmm_b.request_pause();
		vmm_b.ack_evt.read().unwrap();
		assert_eq!(vmm_b.ack_generation.load(Ordering::Acquire), generation_b);

		// Resume both, then pause again to prove the loop returned to normal
		// servicing for each device.
		vmm_a.release();
		vmm_b.release();
		let generation_a = vmm_a.request_pause();
		vmm_a.ack_evt.read().unwrap();
		assert_eq!(vmm_a.ack_generation.load(Ordering::Acquire), generation_a);
		vmm_a.release();

		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn budgeted_pass_rearms_queue_event_and_sibling_stays_live() {
		let passes = Arc::new(AtomicUsize::new(0));
		let notifies = Arc::new(AtomicUsize::new(0));
		let (control_a, _vmm_a) = worker_control();
		let (control_b, _vmm_b) = worker_control();
		let a = test_worker(BudgetedDevice { passes: passes.clone(), budgeted: 3 }, control_a);
		let b = test_worker(CountingDevice { notifies: notifies.clone() }, control_b);
		let (kill_evt, handle) = spawn_loop(vec![a.worker, b.worker]);

		// One doorbell; the worker must re-arm through three budgeted passes.
		a.queue_evt.write(1).unwrap();
		wait_for(&passes, 4);

		// The saturated device did not wedge the loop for its sibling.
		b.queue_evt.write(1).unwrap();
		wait_for(&notifies, 1);

		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn cancelled_quiesce_releases_worker_after_capture_error_path() {
		let (control, vmm) = worker_control();
		let worker = test_worker(IdleDevice, control);
		let (kill_evt, handle) = spawn_loop(vec![worker.worker]);

		let generation = vmm.request_pause();
		vmm.ack_evt.read().unwrap();
		assert_eq!(vmm.ack_generation.load(Ordering::Acquire), generation);
		vmm.release();
		thread::sleep(Duration::from_millis(10));
		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}

	#[test]
	fn cancelled_pause_ack_does_not_satisfy_next_generation() {
		let (entered_tx, entered_rx) = channel();
		let (release_tx, release_rx) = channel();
		let (control, vmm) = worker_control();
		let worker =
			test_worker(DelayedPauseDevice { entered: entered_tx, release: release_rx }, control);
		let (kill_evt, handle) = spawn_loop(vec![worker.worker]);

		let first_generation = vmm.request_pause();
		entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

		// Cancel the first request and immediately publish a second while the
		// worker is still blocked draining for the first.
		vmm.release();
		let second_generation = vmm.request_pause();
		assert_eq!(first_generation, 1);
		assert_eq!(second_generation, 2);
		release_tx.send(()).unwrap();
		entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
		assert_eq!(vmm.ack_generation.load(Ordering::Acquire), 0);

		release_tx.send(()).unwrap();
		vmm.ack_evt.read().unwrap();
		assert_eq!(vmm.ack_generation.load(Ordering::Acquire), second_generation);

		vmm.release();
		kill_evt.write(1).unwrap();
		assert!(handle.join().unwrap().is_ok());
	}
}
