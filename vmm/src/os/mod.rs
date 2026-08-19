//! Host OS primitives that differ across supported hosts.
//!
//! [`EventFd`] is a counting worker wakeup object with the `vmm_sys_util`
//! eventfd-shaped API (`new(flags)`, `write(u64)`, `read() -> io::Result<u64>`,
//! `try_clone()`). Linux uses `eventfd(2)`, macOS uses a pipe, and Windows uses
//! a counter paired with a waitable event handle.
//!
//! [`libc_abi`] covers the narrower split *within* Linux, where glibc and musl
//! disagree on the spelling of several syscall arguments.

/// `EFD_NONBLOCK` flag for [`EventFd::new`]. Non-Linux shims recognize the
/// Linux numeric value so callers can use one platform-neutral constant.
pub const EFD_NONBLOCK: i32 = 0x800;

#[cfg(target_os = "linux")]
pub use vmm_sys_util::eventfd::EventFd;

#[cfg(target_os = "macos")]
mod eventfd;
#[cfg(target_os = "macos")]
pub use eventfd::EventFd;

#[cfg(target_os = "windows")]
mod windows_eventfd;
#[cfg(target_os = "windows")]
pub use windows_eventfd::EventFd;

/// Linux C-library ABI differences between glibc and musl.
///
/// The two libraries type several syscall wrapper arguments differently, so
/// code that must build against both spells those arguments through these
/// aliases instead of hard-coding one library's choice.
#[cfg(target_os = "linux")]
pub mod libc_abi {
	/// `ioctl`'s request argument: `c_ulong` under glibc, `c_int` under musl.
	///
	/// Request codes are bit patterns, so casting a `_IOC`-derived constant with
	/// `as` is correct for either width — the high direction bits wrap into the
	/// sign bit rather than being lost.
	#[cfg(target_env = "musl")]
	pub type IoctlRequest = libc::c_int;
	#[cfg(not(target_env = "musl"))]
	pub type IoctlRequest = libc::c_ulong;

	/// `getrlimit`/`setrlimit`'s resource argument: a dedicated alias under
	/// glibc, plain `c_int` under musl.
	#[cfg(target_env = "gnu")]
	pub type RlimitResource = libc::__rlimit_resource_t;
	#[cfg(not(target_env = "gnu"))]
	pub type RlimitResource = libc::c_int;

	/// A kernel thread id (`gettid`) used as a signal target.
	///
	/// Deliberately *not* a `pthread_t`. musl allocates the pthread descriptor
	/// inside the thread's own stack mapping and unmaps it on join, so a
	/// retained `pthread_t` dangles as soon as the thread exits — and
	/// `pthread_kill` dereferences it to read the tid, faulting. glibc only
	/// hides this by recycling thread stacks instead of unmapping them.
	///
	/// A tid is a plain integer, so a stale one degrades to `ESRCH` (or, in the
	/// worst case, a no-op signal to a recycled tid in this same process)
	/// instead of a use-after-free. Being `Copy + Send` also keeps
	/// [`crate::control::PauseGate`] thread-shareable with no `unsafe impl`.
	#[derive(Clone, Copy)]
	pub struct ThreadId(libc::pid_t);

	impl ThreadId {
		/// The calling thread's own kernel id.
		pub fn current() -> Self {
			// SAFETY: `gettid` takes no arguments and cannot fail.
			Self(unsafe { libc::gettid() })
		}

		/// Deliver `signal` to this thread.
		///
		/// Errors are dropped: the only expected one is `ESRCH` from a thread
		/// that already exited, which is indistinguishable from a successful
		/// no-op for the callers here. `tgkill` scopes the tid to this process,
		/// so a tid recycled elsewhere on the host can never be signalled.
		pub fn kill(self, signal: libc::c_int) {
			// SAFETY: neither C library exports a `tgkill` wrapper, so go through
			// `syscall(2)`; the arguments are plain scalars and a stale tid only
			// yields ESRCH.
			unsafe { libc::syscall(libc::SYS_tgkill, libc::getpid(), self.0, signal) };
		}
	}
}
