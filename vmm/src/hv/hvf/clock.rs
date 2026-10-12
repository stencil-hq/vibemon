//! The guest virtual counter (`CNTVCT_EL0`) shared by every HVF vCPU of a VM.
//!
//! HVF derives each vCPU's `CNTVCT_EL0` as the host counter minus that vCPU's
//! vtimer offset (`CNTVOFF_EL2`), so the guest counter keeps running while the
//! VM is paused, and across the gap between a snapshot and its restore. The
//! guest instead needs one counter, identical on every vCPU, that stops while
//! the VM is paused and continues from the captured value after a restore:
//! paused or snapshotted time is not guest time (the guest's wall clock is
//! re-synced separately, e.g. by envd `/init`). Without this a fork of an old
//! snapshot jumps `CLOCK_MONOTONIC` forward by the whole downtime, firing every
//! in-flight guest timeout and watchdog at once, and per-vCPU pause bookkeeping
//! drifts the vCPUs' counters apart.
//!
//! [`GuestClock`] holds the one offset every vCPU runs with. It freezes as the
//! vCPUs park and thaws, once, into a fresh offset that every vCPU then
//! applies.

use parking_lot::Mutex;

/// VM-wide guest counter: one vtimer offset for every vCPU, frozen while the
/// VM is paused.
///
/// Counter values and offsets are in host counter ticks (`CNTVCT_EL0` units,
/// the timebase of HVF's vtimer offset) and use wrapping arithmetic, matching
/// the architectural modulo-2^64 `CNTVCT = CNTPCT - CNTVOFF`.
pub struct GuestClock {
	state: Mutex<ClockState>,
}

struct ClockState {
	/// Offset every running vCPU applies: guest counter = host counter − offset.
	offset: u64,
	/// While paused: the guest counter value the clock is frozen at, the latest
	/// value any vCPU could have observed before it parked.
	frozen: Option<u64>,
}

impl GuestClock {
	/// A running clock whose vCPUs use `offset` (a cold boot uses 0, HVF's
	/// default).
	pub const fn running(offset: u64) -> Self {
		Self { state: Mutex::new(ClockState { offset, frozen: None }) }
	}

	/// The offset running vCPUs use.
	pub fn offset(&self) -> u64 {
		self.state.lock().offset
	}

	/// Freeze the clock as a vCPU stops running guest code at host counter
	/// `host_now`. Several vCPUs freeze one pause; the clock keeps the latest
	/// counter value, so a vCPU that parked late never sees its counter step
	/// back on resume.
	pub fn freeze(&self, host_now: u64) {
		let mut state = self.state.lock();
		let counter = host_now.wrapping_sub(state.offset);
		state.frozen = Some(
			state
				.frozen
				.map_or(counter, |frozen| later(frozen, counter)),
		);
	}

	/// Freeze the clock at a captured guest counter value (snapshot restore),
	/// before any vCPU runs.
	pub fn restore(&self, counter: u64) {
		self.state.lock().frozen = Some(counter);
	}

	/// The guest counter value the clock is frozen at, `None` while running.
	pub fn frozen(&self) -> Option<u64> {
		self.state.lock().frozen
	}

	/// Thaw the clock as a vCPU resumes at host counter `host_now` and return
	/// the offset that vCPU must run with. The first thaw after a freeze
	/// continues the counter from the frozen value; every later thaw returns
	/// that same offset, so all vCPUs share one counter.
	pub fn thaw(&self, host_now: u64) -> u64 {
		let mut state = self.state.lock();
		if let Some(frozen) = state.frozen.take() {
			state.offset = host_now.wrapping_sub(frozen);
		}
		state.offset
	}
}

/// The later of two counter values, comparing modulo 2^64 like the hardware
/// counter (valid while they are less than 2^63 ticks apart).
const fn later(a: u64, b: u64) -> u64 {
	if b.wrapping_sub(a) as i64 > 0 { b } else { a }
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The guest counter a vCPU running with `offset` reads at `host`.
	const fn guest(host: u64, offset: u64) -> u64 {
		host.wrapping_sub(offset)
	}

	#[test]
	fn pause_freezes_the_counter_at_the_latest_parked_vcpu() {
		let clock = GuestClock::running(100);
		clock.freeze(1_000); // vCPU 0 parks
		clock.freeze(1_500); // vCPU 1 parks later (was mid-interrupt)
		clock.freeze(1_200);
		assert_eq!(clock.frozen(), Some(1_400));

		// Resumed 10_000 ticks later: no downtime, one offset for all vCPUs.
		let first = clock.thaw(11_500);
		let second = clock.thaw(11_900);
		assert_eq!(first, second, "every vCPU runs with the same offset");
		assert_eq!(guest(11_500, first), 1_400, "the counter continues where it stopped");
		assert_eq!(clock.frozen(), None);
	}

	#[test]
	fn restored_counter_continues_from_the_capture_without_downtime() {
		// Source VM: paused, captured at guest counter 7_000.
		let source = GuestClock::running(3_000);
		source.freeze(10_000);
		let captured = source.frozen().expect("paused clock is frozen");
		assert_eq!(captured, 7_000);

		// Fork restored long after: the host counter moved by ~10^12 ticks.
		let host_at_restore = 10_000 + 1_000_000_000_000;
		let fork = GuestClock::running(0);
		fork.restore(captured);
		let offsets: Vec<u64> = (0..8).map(|cpu| fork.thaw(host_at_restore + cpu)).collect();
		assert!(offsets.iter().all(|&o| o == offsets[0]), "{offsets:?}");
		assert_eq!(guest(host_at_restore, offsets[0]), captured);
	}

	#[test]
	fn counter_survives_a_host_whose_counter_is_behind_the_capture() {
		// Restoring on a host that rebooted since the capture (its counter is
		// smaller than the guest's): the offset wraps, the counter does not.
		let fork = GuestClock::running(0);
		fork.restore(u64::MAX - 5);
		let offset = fork.thaw(10);
		assert_eq!(guest(10, offset), u64::MAX - 5);
		assert_eq!(guest(20, offset), 4, "the guest counter wraps like the hardware counter");
	}

	#[test]
	fn freeze_keeps_the_latest_value_across_counter_wrap() {
		let clock = GuestClock::running(0);
		clock.freeze(u64::MAX - 1);
		clock.freeze(3); // parked 5 ticks later, after the counter wrapped
		clock.freeze(u64::MAX - 10);
		assert_eq!(clock.frozen(), Some(3));
	}
}
