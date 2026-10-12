//! Windows implementation of the Linux `eventfd(2)` surface used by workers.
//!
//! A manual-reset event provides the waitable host handle while a shared
//! userspace counter preserves eventfd-style coalescing: writes add to the
//! counter and signal the event, and reads drain the counter back to zero and
//! reset the event.

use std::{
	io,
	os::windows::io::{AsHandle, AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
	sync::{Arc, Condvar, Mutex, MutexGuard},
};

use windows_sys::Win32::{
	Foundation::{ERROR_INVALID_PARAMETER, HANDLE, WAIT_FAILED, WAIT_OBJECT_0},
	System::Threading::{CreateEventW, INFINITE, ResetEvent, SetEvent, WaitForSingleObject},
};

const LINUX_EFD_NONBLOCK: i32 = 0x800;
const LINUX_EFD_CLOEXEC: i32 = 0x80000;
const SUPPORTED_FLAGS: i32 = LINUX_EFD_NONBLOCK | LINUX_EFD_CLOEXEC;

#[derive(Debug, Default)]
struct CounterState {
	value: u64,
}

/// A waitable, counting wakeup object backed by a Windows event handle.
#[derive(Debug)]
pub struct EventFd {
	handle:      OwnedHandle,
	nonblocking: bool,
	state:       Arc<(Mutex<CounterState>, Condvar)>,
}

impl EventFd {
	/// Creates an eventfd-compatible wakeup object.
	pub fn new(flags: i32) -> io::Result<Self> {
		if flags & !SUPPORTED_FLAGS != 0 {
			return Err(io::Error::from_raw_os_error(ERROR_INVALID_PARAMETER as i32));
		}

		// SAFETY: null security attributes/name request a private,
		// non-inheritable manual-reset event. The returned handle is checked
		// before ownership is transferred to `OwnedHandle`.
		let handle = unsafe { CreateEventW(std::ptr::null(), 1, 0, std::ptr::null()) };
		if handle.is_null() {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: `CreateEventW` returned a fresh live kernel handle uniquely
		// owned by this value.
		let handle = unsafe { OwnedHandle::from_raw_handle(handle as RawHandle) };

		Ok(Self {
			handle,
			nonblocking: flags & LINUX_EFD_NONBLOCK != 0,
			state: Arc::new((Mutex::new(CounterState::default()), Condvar::new())),
		})
	}

	/// Adds a value to the counter and signals the waitable event.
	pub fn write(&self, v: u64) -> io::Result<()> {
		if v == u64::MAX {
			return Err(io::Error::new(
				io::ErrorKind::InvalidInput,
				"eventfd write value must be less than u64::MAX",
			));
		}
		if v == 0 {
			return Ok(());
		}

		let (lock, cv) = &*self.state;
		let mut state = lock_state(lock)?;
		while state.value > (u64::MAX - 1).saturating_sub(v) {
			if self.nonblocking {
				return Err(io::Error::new(io::ErrorKind::WouldBlock, "eventfd counter overflow"));
			}
			state = cv
				.wait(state)
				.map_err(|_| io::Error::other("eventfd counter state poisoned"))?;
		}
		state.value += v;
		// SAFETY: `handle` is a live event handle for the duration of the call.
		if unsafe { SetEvent(self.handle.as_raw_handle() as HANDLE) } == 0 {
			return Err(io::Error::last_os_error());
		}
		Ok(())
	}

	/// Drains and returns the current counter value.
	pub fn read(&self) -> io::Result<u64> {
		loop {
			{
				let (lock, cv) = &*self.state;
				let mut state = lock_state(lock)?;
				if state.value != 0 {
					let total = state.value;
					state.value = 0;
					// SAFETY: `handle` is a live event handle for the duration of
					// the call.
					if unsafe { ResetEvent(self.handle.as_raw_handle() as HANDLE) } == 0 {
						return Err(io::Error::last_os_error());
					}
					cv.notify_all();
					return Ok(total);
				}
			}

			if self.nonblocking {
				return Err(io::Error::from(io::ErrorKind::WouldBlock));
			}
			wait_event(self.handle.as_raw_handle())?;
		}
	}

	/// Duplicates the wait handle while sharing the same counter.
	pub fn try_clone(&self) -> io::Result<Self> {
		Ok(Self {
			handle:      self.handle.as_handle().try_clone_to_owned()?,
			nonblocking: self.nonblocking,
			state:       Arc::clone(&self.state),
		})
	}
}

impl AsRawHandle for EventFd {
	fn as_raw_handle(&self) -> RawHandle {
		self.handle.as_raw_handle()
	}
}

fn lock_state(state: &Mutex<CounterState>) -> io::Result<MutexGuard<'_, CounterState>> {
	state
		.lock()
		.map_err(|_| io::Error::other("eventfd counter state poisoned"))
}

fn wait_event(handle: RawHandle) -> io::Result<()> {
	// SAFETY: `handle` is a live event handle and the call does not retain it.
	match unsafe { WaitForSingleObject(handle as HANDLE, INFINITE) } {
		WAIT_OBJECT_0 => Ok(()),
		WAIT_FAILED => Err(io::Error::last_os_error()),
		result => Err(io::Error::other(format!("unexpected WaitForSingleObject result {result:#x}"))),
	}
}
