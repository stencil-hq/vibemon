//! Process-global metrics registry.
//!
//! A single lazily-initialized [`Metrics`] holds plain atomics, so any thread —
//! vCPU, device worker, control-plane, or agent bridge — can record an event
//! without a lock or threading a handle through every call site. The `metrics`
//! lifecycle method (`ControlKind::Metrics`) serializes the whole registry via
//! [`snapshot_json`].
//!
//! Durations are stored as whole milliseconds. Boot/restore/fork timers are
//! gauges (last value wins; a process reconstructs exactly once), while the
//! snapshot timer keeps a running count plus last and cumulative duration.

use std::{
	sync::{
		LazyLock,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use serde_json::{Value, json};

/// KVM vCPU exit reasons we count, mirroring the arms of `run_vcpu`'s exit
/// match.
#[derive(Clone, Copy)]
pub enum VmExit {
	IoIn,
	IoOut,
	MmioRead,
	MmioWrite,
	Hlt,
	Shutdown,
	SystemEvent,
	FailEntry,
	InternalError,
	Other,
}

#[derive(Default)]
struct VmExitCounters {
	io_in:          AtomicU64,
	io_out:         AtomicU64,
	mmio_read:      AtomicU64,
	mmio_write:     AtomicU64,
	hlt:            AtomicU64,
	shutdown:       AtomicU64,
	system_event:   AtomicU64,
	fail_entry:     AtomicU64,
	internal_error: AtomicU64,
	other:          AtomicU64,
}

/// All process-wide counters and timers. Timers store milliseconds.
#[derive(Default)]
struct Metrics {
	boot_duration_ms:           AtomicU64,
	restore_duration_ms:        AtomicU64,
	fork_duration_ms:           AtomicU64,
	vm_exits:                   VmExitCounters,
	device_worker_errors:       AtomicU64,
	agent_bridge_disconnects:   AtomicU64,
	control_requests:           AtomicU64,
	snapshot_count:             AtomicU64,
	snapshot_last_duration_ms:  AtomicU64,
	snapshot_total_duration_ms: AtomicU64,
	pager_fault_ins:            AtomicU64,
	pager_evictions:            AtomicU64,
	pager_resident_pages:       AtomicU64,
	pager_compressed_bytes:     AtomicU64,
	pager_swapped_pages:        AtomicU64,
	ksm_regions_advised:        AtomicU64,
}

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

#[inline]
fn ms(duration: Duration) -> u64 {
	// Saturate rather than panic: a >584-million-year duration is not a real
	// concern, but `as u64` truncation of `u128` millis would be a silent bug.
	duration.as_millis().min(u128::from(u64::MAX)) as u64
}

#[inline]
fn saturating_add(counter: &AtomicU64, value: u64) {
	let _ = counter.try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
		Some(current.saturating_add(value))
	});
}

/// Record the wall-clock time a fresh boot (`Vmm::build`) took.
pub fn record_boot_duration(duration: Duration) {
	METRICS
		.boot_duration_ms
		.store(ms(duration), Ordering::Relaxed);
}

/// Record the wall-clock time a snapshot restore took.
pub fn record_restore_duration(duration: Duration) {
	METRICS
		.restore_duration_ms
		.store(ms(duration), Ordering::Relaxed);
}

/// Record the wall-clock time a `CoW` fork-from-template took.
pub fn record_fork_duration(duration: Duration) {
	METRICS
		.fork_duration_ms
		.store(ms(duration), Ordering::Relaxed);
}

/// Count one vCPU exit of the given reason. Called on the hot run-loop path, so
/// the increment is a single relaxed atomic.
pub fn record_vm_exit(exit: VmExit) {
	let counters = &METRICS.vm_exits;
	let counter = match exit {
		VmExit::IoIn => &counters.io_in,
		VmExit::IoOut => &counters.io_out,
		VmExit::MmioRead => &counters.mmio_read,
		VmExit::MmioWrite => &counters.mmio_write,
		VmExit::Hlt => &counters.hlt,
		VmExit::Shutdown => &counters.shutdown,
		VmExit::SystemEvent => &counters.system_event,
		VmExit::FailEntry => &counters.fail_entry,
		VmExit::InternalError => &counters.internal_error,
		VmExit::Other => &counters.other,
	};
	counter.fetch_add(1, Ordering::Relaxed);
}

/// Count one device-worker thread that exited with an error.
pub fn record_device_worker_error() {
	METRICS.device_worker_errors.fetch_add(1, Ordering::Relaxed);
}

/// Count one agent-bridge client disconnect.
pub fn record_agent_bridge_disconnect() {
	METRICS
		.agent_bridge_disconnects
		.fetch_add(1, Ordering::Relaxed);
}

/// Count one control-API request dispatched to the VMM.
pub fn record_control_request() {
	METRICS.control_requests.fetch_add(1, Ordering::Relaxed);
}

/// Record one completed snapshot and its duration.
pub fn record_snapshot(duration: Duration) {
	let d = ms(duration);
	METRICS.snapshot_count.fetch_add(1, Ordering::Relaxed);
	METRICS
		.snapshot_last_duration_ms
		.store(d, Ordering::Relaxed);
	saturating_add(&METRICS.snapshot_total_duration_ms, d);
}

#[cfg(target_os = "linux")]
mod linux {
	use super::*;

	/// Count one transparent pager fault-in.
	pub fn record_pager_fault_in() {
		METRICS.pager_fault_ins.fetch_add(1, Ordering::Relaxed);
	}

	/// Count one transparent pager eviction.
	pub fn record_pager_eviction() {
		METRICS.pager_evictions.fetch_add(1, Ordering::Relaxed);
	}

	/// Publish current transparent pager gauges.
	pub fn set_pager_gauges(resident_pages: usize, compressed_bytes: usize, swapped_pages: usize) {
		METRICS
			.pager_resident_pages
			.store(resident_pages as u64, Ordering::Relaxed);
		METRICS
			.pager_compressed_bytes
			.store(compressed_bytes as u64, Ordering::Relaxed);
		METRICS
			.pager_swapped_pages
			.store(swapped_pages as u64, Ordering::Relaxed);
	}

	/// Count one guest-memory region successfully marked `MADV_MERGEABLE` for
	/// KSM.
	pub fn record_ksm_region() {
		METRICS.ksm_regions_advised.fetch_add(1, Ordering::Relaxed);
	}
}

#[cfg(target_os = "linux")]
pub use linux::{
	record_ksm_region, record_pager_eviction, record_pager_fault_in, set_pager_gauges,
};

/// Snapshot every counter/timer into a JSON object for the `metrics` method.
pub fn snapshot_json() -> Value {
	let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
	let e = &METRICS.vm_exits;
	let io_in = load(&e.io_in);
	let io_out = load(&e.io_out);
	let mmio_read = load(&e.mmio_read);
	let mmio_write = load(&e.mmio_write);
	let hlt = load(&e.hlt);
	let shutdown = load(&e.shutdown);
	let system_event = load(&e.system_event);
	let fail_entry = load(&e.fail_entry);
	let internal_error = load(&e.internal_error);
	let other = load(&e.other);
	let vm_exit_total = io_in
		.saturating_add(io_out)
		.saturating_add(mmio_read)
		.saturating_add(mmio_write)
		.saturating_add(hlt)
		.saturating_add(shutdown)
		.saturating_add(system_event)
		.saturating_add(fail_entry)
		.saturating_add(internal_error)
		.saturating_add(other);

	json!({
		 "boot_duration_ms": load(&METRICS.boot_duration_ms),
		 "restore_duration_ms": load(&METRICS.restore_duration_ms),
		 "fork_duration_ms": load(&METRICS.fork_duration_ms),
		 "vm_exits": {
			  "io_in": io_in,
			  "io_out": io_out,
			  "mmio_read": mmio_read,
			  "mmio_write": mmio_write,
			  "hlt": hlt,
			  "shutdown": shutdown,
			  "system_event": system_event,
			  "fail_entry": fail_entry,
			  "internal_error": internal_error,
			  "other": other,
			  "total": vm_exit_total,
		 },
		 "device_worker_errors": load(&METRICS.device_worker_errors),
		 "agent_bridge_disconnects": load(&METRICS.agent_bridge_disconnects),
		 "control_requests": load(&METRICS.control_requests),
		 "snapshot": {
			  "count": load(&METRICS.snapshot_count),
			  "last_duration_ms": load(&METRICS.snapshot_last_duration_ms),
			  "total_duration_ms": load(&METRICS.snapshot_total_duration_ms),
		 },
		 "pager": {
			  "fault_ins": load(&METRICS.pager_fault_ins),
			  "evictions": load(&METRICS.pager_evictions),
			  "resident_pages": load(&METRICS.pager_resident_pages),
			  "compressed_bytes": load(&METRICS.pager_compressed_bytes),
			  "swapped_pages": load(&METRICS.pager_swapped_pages),
		 },
		 "ksm": {
			  "regions_advised": load(&METRICS.ksm_regions_advised),
		 },
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snapshot_json_reports_recorded_events() {
		// Note: the registry is process-global, so assert on relative deltas
		// rather than absolute values (other tests may share the process).
		let before = snapshot_json();
		let before_io_out = before["vm_exits"]["io_out"].as_u64().unwrap();
		let before_total = before["vm_exits"]["total"].as_u64().unwrap();
		let before_ctrl = before["control_requests"].as_u64().unwrap();
		let before_snap = before["snapshot"]["count"].as_u64().unwrap();

		record_vm_exit(VmExit::IoOut);
		record_vm_exit(VmExit::IoOut);
		record_control_request();
		record_snapshot(Duration::from_millis(7));
		record_boot_duration(Duration::from_millis(42));

		let after = snapshot_json();
		assert_eq!(after["vm_exits"]["io_out"].as_u64().unwrap(), before_io_out + 2);
		assert_eq!(after["vm_exits"]["total"].as_u64().unwrap(), before_total + 2);
		assert_eq!(after["control_requests"].as_u64().unwrap(), before_ctrl + 1);
		assert_eq!(after["snapshot"]["count"].as_u64().unwrap(), before_snap + 1);
		assert_eq!(after["snapshot"]["last_duration_ms"].as_u64().unwrap(), 7);
		assert_eq!(after["boot_duration_ms"].as_u64().unwrap(), 42);
	}
}
