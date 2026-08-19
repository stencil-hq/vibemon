//! Control plane: vCPU pause/resume signalling primitives and a local command
//! server.
//!
//! [`PauseGate`] coordinates parking every vCPU thread at a safe point so a
//! snapshot sees a frozen machine. It uses a real-time signal to kick threads
//! out of an in-flight `KVM_RUN`: the handler is a no-op installed *without*
//! `SA_RESTART`, so the interrupted ioctl returns `EINTR` and the vCPU loop can
//! re-check its run state.
//!
//! [`serve`] exposes a newline-delimited JSON lifecycle protocol over a Unix
//! socket or Windows named pipe. The server thread never touches the `Vmm`
//! directly; it forwards parsed requests to the owning thread over a channel
//! and serializes that thread's JSON response back to the connection.

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::io::AsRawFd;
#[cfg(target_os = "windows")]
use std::os::windows::io::{FromRawHandle, RawHandle};
#[cfg(unix)]
use std::{
	fs,
	os::unix::{
		ffi::OsStrExt,
		fs::{DirBuilderExt, FileTypeExt, MetadataExt, PermissionsExt},
		net::{UnixListener, UnixStream},
	},
	path::Path,
};
use std::{
	io::{self, BufRead, BufReader, Read, Write},
	path::PathBuf,
	sync::Arc,
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use flume::{RecvTimeoutError, Sender};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
#[cfg(target_os = "windows")]
use windows_sys::Win32::{
	Foundation::{ERROR_PIPE_CONNECTED, HANDLE, INVALID_HANDLE_VALUE, LocalFree},
	Security::{
		Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW, PSECURITY_DESCRIPTOR,
		SECURITY_ATTRIBUTES,
	},
	Storage::FileSystem::{FILE_FLAG_FIRST_PIPE_INSTANCE, PIPE_ACCESS_DUPLEX},
	System::Pipes::{
		ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
		PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
	},
};

#[cfg(target_os = "linux")]
use crate::os::libc_abi::ThreadId;
use crate::result::Result;
#[cfg(unix)]
use crate::result::err;

const CONTROL_MAX_REQUEST_LINE: usize = 65_536;
const CONTROL_READ_TIMEOUT: Duration = Duration::from_secs(5);
#[cfg(unix)]
const CONTROL_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_REPLY_TIMEOUT: Duration = Duration::from_mins(1);
/// Real-time signal used on Linux to kick a vCPU out of `KVM_RUN`.
///
/// HVF uses the backend kicker installed on [`PauseGate`] instead of POSIX
/// signals because Hypervisor.framework exposes an explicit vCPU-exit API.
#[cfg(target_os = "linux")]
pub mod pause_signal {
	use parking_lot::Mutex;

	use crate::{os::libc_abi::ThreadId, result::Result};

	/// Real-time signal used to kick a vCPU out of `KVM_RUN`.
	pub fn signal() -> i32 {
		libc::SIGRTMIN()
	}

	const extern "C" fn handler(_: libc::c_int, _: *mut libc::siginfo_t, _: *mut libc::c_void) {}

	/// Install the no-op handler without `SA_RESTART` so a delivered signal
	/// makes an in-flight `KVM_RUN` ioctl return `EINTR`.
	pub fn install() -> Result<()> {
		vmm_sys_util::signal::register_signal_handler(signal(), handler)?;
		Ok(())
	}

	/// Signal every registered vCPU thread to interrupt `KVM_RUN`.
	pub fn signal_threads(tids: &Mutex<Vec<Option<ThreadId>>>) {
		for slot in tids.lock().iter() {
			if let Some(tid) = *slot {
				tid.kill(signal());
			}
		}
	}
}

#[cfg(not(target_os = "linux"))]
pub mod pause_signal {
	use crate::result::Result;

	#[allow(clippy::unnecessary_wraps, reason = "uniform API across target operating systems")]
	pub fn install() -> Result<()> {
		Ok(())
	}
}
/// Lifecycle state shared by all vCPU threads.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RunState {
	Running,
	Paused,
	Stopping,
}

/// Per-vCPU command channel payload (the `Vmm` holds the `Sender`, the vCPU
/// thread the `Receiver`).
pub enum VcpuCmd {
	Resume,
	Stop,
	/// Snapshot this vCPU's backend state and send it back, then stay parked.
	/// Only the owning vCPU thread may touch backend-local vCPU handles.
	SaveState(Sender<Result<crate::arch::state::VcpuState>>),
}

/// Synchronisation gate used to park/unpark vCPU threads at a safe point.
pub struct PauseGate {
	state:  Mutex<RunState>,
	cv:     Condvar,
	parked: Mutex<Vec<bool>>,
	cpus:   u32,
	#[cfg(target_os = "linux")]
	tids:   Mutex<Vec<Option<ThreadId>>>,
	kicker: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl PauseGate {
	/// Create a gate for `cpus` vCPU threads, initially [`RunState::Running`].
	pub fn new(cpus: u32) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RunState::Running),
			cv: Condvar::new(),
			parked: Mutex::new(vec![false; cpus as usize]),
			cpus,
			#[cfg(target_os = "linux")]
			tids: Mutex::new(vec![None; cpus as usize]),
			kicker: Mutex::new(None),
		})
	}

	/// Create a gate for `cpus` vCPU threads, initially [`RunState::Paused`].
	///
	/// The caller must construct this before spawning vCPU threads so every
	/// thread observes the pause before its first hypervisor run call.
	pub fn new_paused(cpus: u32) -> Arc<Self> {
		Arc::new(Self {
			state: Mutex::new(RunState::Paused),
			cv: Condvar::new(),
			parked: Mutex::new(vec![false; cpus as usize]),
			cpus,
			#[cfg(target_os = "linux")]
			tids: Mutex::new(vec![None; cpus as usize]),
			kicker: Mutex::new(None),
		})
	}

	/// Current run state.
	pub fn state(&self) -> RunState {
		*self.state.lock()
	}

	/// Set the run state. (Callers drive the park/unpark handshake separately.)
	pub fn set_state(&self, s: RunState) {
		*self.state.lock() = s;
	}

	/// Transition the run state only if the current state is `expected`.
	pub fn set_state_if(&self, expected: RunState, next: RunState) -> bool {
		let mut state = self.state.lock();
		if *state == expected {
			*state = next;
			true
		} else {
			false
		}
	}

	/// Called by vCPU thread `vcpu_id` on entry: record its pthread id so the
	/// gate can later signal exactly the registered threads.
	#[allow(
		clippy::missing_const_for_fn,
		clippy::unused_self,
		reason = "Linux records pthread IDs; other backends keep the same gate API"
	)]
	pub fn register_tid(&self, vcpu_id: usize) {
		#[cfg(target_os = "linux")]
		{
			self.tids.lock()[vcpu_id] = Some(ThreadId::current());
		}
		#[cfg(not(target_os = "linux"))]
		let _ = vcpu_id;
	}

	/// Mark vCPU `vcpu_id` parked and wake any waiter (e.g. `pause()`).
	pub fn mark_parked(&self, vcpu_id: usize) {
		let mut g = self.parked.lock();
		if let Some(parked) = g.get_mut(vcpu_id)
			&& !*parked
		{
			*parked = true;
			self.cv.notify_all();
		}
	}

	/// Reset parked state (done once per `Running`->`Paused` transition).
	pub fn reset_parked(&self) {
		self.parked.lock().fill(false);
	}

	/// Number of vCPUs currently parked.
	pub fn parked_count(&self) -> u32 {
		self.parked.lock().iter().filter(|parked| **parked).count() as u32
	}

	/// Parked vCPU IDs for diagnostics.
	pub fn parked_vcpus(&self) -> Vec<usize> {
		self
			.parked
			.lock()
			.iter()
			.enumerate()
			.filter_map(|(id, parked)| (*parked).then_some(id))
			.collect()
	}

	/// Block until every vCPU is parked or `timeout` elapses; returns whether
	/// all vCPUs are parked.
	pub fn wait_for_all_parked(&self, timeout: Duration) -> bool {
		let mut g = self.parked.lock();
		self.cv.wait_while_for(
			&mut g,
			|parked| parked.iter().filter(|is_parked| **is_parked).count() < self.cpus as usize,
			timeout,
		);
		g.iter().filter(|parked| **parked).count() >= self.cpus as usize
	}

	/// Install a backend-specific wakeup for vCPU APIs that are not
	/// signal-driven.
	#[cfg(any(target_os = "macos", target_os = "windows"))]
	pub fn set_kicker(&self, kicker: Arc<dyn Fn() + Send + Sync>) {
		*self.kicker.lock() = Some(kicker);
	}

	/// Kick every registered vCPU out of the hypervisor run call.
	pub fn signal_all_vcpus(&self) {
		#[cfg(target_os = "linux")]
		pause_signal::signal_threads(&self.tids);
		let kicker = self.kicker.lock().clone();
		if let Some(kicker) = kicker {
			kicker();
		}
	}
}

/// Lifecycle control request parsed off the socket.
#[derive(Debug)]
pub enum ControlKind {
	Ping,
	Info,
	Pause,
	Resume,
	Snapshot {
		name:  String,
		base:  Option<String>,
		track: bool,
	},
	/// Capture only the active writable root disk. Unlike `Snapshot`, this
	/// leaves vCPUs running and never serializes VM/RAM state.
	DiskSnapshot {
		name: String,
	},
	Quit,
	Metrics,
	Extend {
		secs: u64,
	},
	/// Re-arm only the production ownership watchdog.
	RearmOwnerLease {
		secs: u64,
	},
}

/// JSON error object returned by the lifecycle API.
#[derive(Clone, Debug, Serialize)]
pub struct ApiError {
	pub code:    String,
	pub message: String,
}

impl ApiError {
	pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self { code: code.into(), message: message.into() }
	}

	pub fn bad_request(message: impl Into<String>) -> Self {
		Self::new("bad_request", message)
	}

	pub fn unknown_method(message: impl Into<String>) -> Self {
		Self::new("unknown_method", message)
	}

	pub fn snapshots_disabled(message: impl Into<String>) -> Self {
		Self::new("snapshots_disabled", message)
	}

	pub fn path_denied(message: impl Into<String>) -> Self {
		Self::new("path_denied", message)
	}

	pub fn not_paused(message: impl Into<String>) -> Self {
		Self::new("not_paused", message)
	}

	pub fn internal(message: impl Into<String>) -> Self {
		Self::new("internal", message)
	}
}

pub type ControlReply = std::result::Result<serde_json::Value, ApiError>;

/// A parsed command plus the oneshot channel its JSON reply is sent back on.
pub struct ControlCmd {
	pub kind:  ControlKind,
	pub reply: Sender<ControlReply>,
}
/// Shared ownership watchdog state, updated directly by the control socket so
/// a lease renewal never waits behind a long-running lifecycle command.
#[derive(Clone)]
pub struct OwnerWatchdog {
	inner:  Arc<(Mutex<OwnerWatchdogState>, Condvar)>,
	handle: Arc<Mutex<Option<JoinHandle<()>>>>,
}

struct OwnerWatchdogState {
	deadline: Option<Instant>,
	stopped:  bool,
}

impl OwnerWatchdog {
	/// Create a watchdog with its initial deadline already armed.
	pub fn new(deadline: Option<Instant>) -> Self {
		Self {
			inner:  Arc::new((
				Mutex::new(OwnerWatchdogState { deadline, stopped: false }),
				Condvar::new(),
			)),
			handle: Arc::new(Mutex::new(None)),
		}
	}

	/// Start expiry handling. The callback must stop the VMM without waiting on
	/// the lifecycle command receiver.
	pub fn start(&self, on_expire: impl FnOnce() + Send + 'static) {
		let inner = self.inner.clone();
		let handle = thread::spawn(move || {
			let (state, wake) = &*inner;
			let mut state = state.lock();
			while !state.stopped {
				let Some(deadline) = state.deadline else {
					wake.wait(&mut state);
					continue;
				};
				let remaining = deadline.saturating_duration_since(Instant::now());
				if remaining.is_zero() {
					state.stopped = true;
					drop(state);
					on_expire();
					return;
				}
				wake.wait_for(&mut state, remaining);
			}
		});
		*self.handle.lock() = Some(handle);
	}

	/// Reset only the ownership deadline to now plus `secs`.
	pub fn rearm(&self, secs: u64) -> std::result::Result<i64, ApiError> {
		if !(1..=86_400).contains(&secs) {
			return Err(ApiError::bad_request(
				"rearm_owner_lease params.secs must be between 1 and 86400",
			));
		}
		let deadline = Instant::now()
			.checked_add(Duration::from_secs(secs))
			.ok_or_else(|| {
				ApiError::bad_request("rearm_owner_lease params.secs is too far in the future")
			})?;
		let (state, wake) = &*self.inner;
		let mut state = state.lock();
		if state.stopped {
			return Err(ApiError::internal("owner watchdog is no longer active"));
		}
		state.deadline = Some(deadline);
		drop(state);
		wake.notify_all();
		Ok(deadline_to_unix(deadline))
	}

	/// Stop and join the timer thread.
	pub fn shutdown(&self) {
		let (state, wake) = &*self.inner;
		state.lock().stopped = true;
		wake.notify_all();
		let handle = self.handle.lock().take();
		if let Some(handle) = handle {
			let _ = handle.join();
		}
	}
}

fn deadline_to_unix(deadline: Instant) -> i64 {
	let now = Instant::now();
	let now_unix = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |duration| duration.as_secs() as i64);
	now_unix.saturating_add(
		i64::try_from(deadline.saturating_duration_since(now).as_secs()).unwrap_or(i64::MAX),
	)
}

/// Filesystem owner to prepare for a pre-bound control socket.
#[derive(Clone, Copy)]
pub struct SocketOwner {
	pub uid: u32,
	pub gid: u32,
}

#[cfg(unix)]
#[derive(Clone, Copy)]
struct SocketIdentity {
	dev: u64,
	ino: u64,
}

#[cfg(unix)]
impl SocketIdentity {
	fn from_metadata(meta: &fs::Metadata) -> Self {
		Self { dev: meta.dev(), ino: meta.ino() }
	}

	fn matches(self, meta: &fs::Metadata) -> bool {
		self.dev == meta.dev() && self.ino == meta.ino()
	}
}

/// Bound control socket listener. Binding can happen before sandbox uid/gid
/// drops; serving starts later, after the process sandbox is in place.
#[cfg(unix)]
pub struct ControlServer {
	sock_path:  PathBuf,
	owner_uid:  u32,
	launch_uid: u32,
	listener:   UnixListener,
	identity:   SocketIdentity,
}

#[cfg(unix)]
pub struct ControlCleanup {
	sock_path: PathBuf,
	owner_uid: u32,
	identity:  SocketIdentity,
}

#[cfg(unix)]
impl ControlServer {
	pub fn cleanup(&self) {
		cleanup_socket_for_uid(&self.sock_path, self.owner_uid, Some(self.identity));
	}

	pub fn cleanup_token(&self) -> ControlCleanup {
		ControlCleanup {
			sock_path: self.sock_path.clone(),
			owner_uid: self.owner_uid,
			identity:  self.identity,
		}
	}

	pub fn start(self, tx: Sender<ControlCmd>, watchdog: Option<OwnerWatchdog>) {
		let launch_uid = self.launch_uid;
		std::thread::spawn(move || {
			for stream in self.listener.incoming() {
				if let Ok(stream) = stream
					&& configure_stream(&stream)
						.and_then(|()| verify_peer_credentials(&stream, launch_uid))
						.is_ok()
				{
					let tx = tx.clone();
					let watchdog = watchdog.clone();
					std::thread::spawn(move || handle_connection(stream, tx, watchdog));
				}
			}
		});
	}
}

#[cfg(unix)]
impl ControlCleanup {
	pub fn cleanup(&self) {
		cleanup_socket_for_uid(&self.sock_path, self.owner_uid, Some(self.identity));
	}
}
#[cfg(target_os = "windows")]
/// Windows named-pipe control server owned by the VMM lifecycle.
pub struct ControlServer {
	pipe_name: Vec<u16>,
	listener:  Mutex<Option<std::fs::File>>,
}

#[cfg(target_os = "windows")]
/// No-op cleanup token matching the Unix control-server interface.
pub struct ControlCleanup;

#[cfg(target_os = "windows")]
impl ControlServer {
	/// Close the outstanding named-pipe listener.
	pub fn cleanup(&self) {
		self.listener.lock().take();
	}

	/// Return the cleanup token used by shared VMM teardown code.
	#[allow(
		clippy::unused_self,
		reason = "the Unix implementation derives cleanup state from the server"
	)]
	pub const fn cleanup_token(&self) -> ControlCleanup {
		ControlCleanup
	}

	/// Serve control requests on named-pipe instances.
	pub fn start(self, tx: Sender<ControlCmd>, watchdog: Option<OwnerWatchdog>) {
		let pipe_name = self.pipe_name;
		let first = self.listener.into_inner();
		std::thread::spawn(move || {
			let mut listener = first;
			while let Some(pipe) = listener {
				if connect_named_pipe(&pipe).is_err() {
					return;
				}
				listener = create_control_pipe(&pipe_name, false).ok();
				let tx = tx.clone();
				let watchdog = watchdog.clone();
				std::thread::spawn(move || handle_connection(pipe, tx, watchdog));
			}
		});
	}
}

#[cfg(target_os = "windows")]
impl ControlCleanup {
	/// Complete platform-neutral teardown; Windows handles close through RAII.
	#[allow(
		clippy::unused_self,
		reason = "shared teardown invokes cleanup through the platform token"
	)]
	pub const fn cleanup(&self) {}
}

#[cfg(target_os = "windows")]
/// Bind the Windows control plane to a deterministic named-pipe name.
pub fn bind(
	sock_path: PathBuf,
	_owner: Option<SocketOwner>,
	_launch_uid: u32,
) -> Result<ControlServer> {
	let pipe_name = crate::windows_pipe::pipe_name(&sock_path);
	let listener = create_control_pipe(&pipe_name, true)?;
	Ok(ControlServer { pipe_name, listener: Mutex::new(Some(listener)) })
}

#[cfg(target_os = "windows")]
fn create_control_pipe(pipe_name: &[u16], first: bool) -> Result<std::fs::File> {
	const SDDL_REVISION_1: u32 = 1;
	let sddl: Vec<u16> = "D:P(A;;GA;;;SY)(A;;GA;;;OW)"
		.encode_utf16()
		.chain(Some(0))
		.collect();
	let mut descriptor: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
	// SAFETY: SDDL is NUL-terminated and descriptor points to writable storage.
	if unsafe {
		ConvertStringSecurityDescriptorToSecurityDescriptorW(
			sddl.as_ptr(),
			SDDL_REVISION_1,
			&mut descriptor,
			std::ptr::null_mut(),
		)
	} == 0
	{
		return Err(io::Error::last_os_error().into());
	}
	let attributes = SECURITY_ATTRIBUTES {
		nLength:              std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
		lpSecurityDescriptor: descriptor,
		bInheritHandle:       0,
	};
	let open_mode = PIPE_ACCESS_DUPLEX
		| if first {
			FILE_FLAG_FIRST_PIPE_INSTANCE
		} else {
			0
		};
	// SAFETY: pipe_name and the security descriptor remain valid for the call.
	let handle = unsafe {
		CreateNamedPipeW(
			pipe_name.as_ptr(),
			open_mode,
			PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
			PIPE_UNLIMITED_INSTANCES,
			64 * 1024,
			64 * 1024,
			CONTROL_READ_TIMEOUT.as_millis() as u32,
			&attributes,
		)
	};
	// SAFETY: ConvertStringSecurityDescriptor allocated descriptor with LocalAlloc.
	unsafe { LocalFree(descriptor.cast()) };
	if handle == INVALID_HANDLE_VALUE {
		return Err(io::Error::last_os_error().into());
	}
	// SAFETY: CreateNamedPipeW returned a fresh owned handle.
	Ok(unsafe { std::fs::File::from_raw_handle(handle as RawHandle) })
}

#[cfg(target_os = "windows")]
fn connect_named_pipe(pipe: &std::fs::File) -> io::Result<()> {
	use std::os::windows::io::AsRawHandle;
	let handle = pipe.as_raw_handle() as HANDLE;
	// SAFETY: handle is a live named-pipe server handle and synchronous operation
	// requires no OVERLAPPED storage.
	if unsafe { ConnectNamedPipe(handle, std::ptr::null_mut()) } != 0 {
		return Ok(());
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error() == Some(ERROR_PIPE_CONNECTED as i32) {
		Ok(())
	} else {
		Err(error)
	}
}

/// Bind a `UnixListener` at `sock_path` without spawning the server thread.
///
/// The socket is only created inside a private (0700-or-stricter) directory
/// owned by the requested `owner` uid, by this process uid when `owner` is
/// `None`, or by the current root euid while pre-binding a sandbox-owned
/// socket before the uid/gid drop. If that immediate parent is missing, it is
/// created with 0700 permissions and chowned to `owner` when requested;
/// otherwise unsafe parents are rejected. Stale sockets are unlinked only after
/// verifying they are inactive sockets owned by the expected uid or allowed
/// pre-bind uid.
#[cfg(unix)]
pub fn bind(
	sock_path: PathBuf,
	owner: Option<SocketOwner>,
	launch_uid: u32,
) -> Result<ControlServer> {
	prepare_socket_path(&sock_path, owner)?;
	let owner_uid = owner.map_or_else(current_euid, |owner| owner.uid);
	let listener = UnixListener::bind(&sock_path)?;
	if let Some(owner) = owner
		&& let Err(e) = chown_path(&sock_path, owner)
	{
		cleanup_socket_for_uid(&sock_path, current_euid(), None);
		return Err(e);
	}
	if let Err(e) = chmod_path(&sock_path, 0o600) {
		cleanup_socket_for_uid(&sock_path, owner_uid, None);
		return Err(e);
	}
	let identity = match bound_socket_identity(&sock_path, owner_uid) {
		Ok(identity) => identity,
		Err(e) => {
			cleanup_socket_for_uid(&sock_path, owner_uid, None);
			return Err(e);
		},
	};
	Ok(ControlServer { sock_path, owner_uid, launch_uid, listener, identity })
}

#[cfg(unix)]
fn bound_socket_identity(sock_path: &Path, expected_uid: u32) -> Result<SocketIdentity> {
	let meta = fs::symlink_metadata(sock_path)?;
	if !meta.file_type().is_socket() {
		return Err(err(format!("bound control path {} is not a socket", sock_path.display())));
	}
	if meta.uid() != expected_uid {
		return Err(err(format!(
			"bound control socket {} is owned by uid {}, expected uid {expected_uid}",
			sock_path.display(),
			meta.uid()
		)));
	}
	Ok(SocketIdentity::from_metadata(&meta))
}

#[cfg(unix)]
fn cleanup_socket_for_uid(sock_path: &Path, expected_uid: u32, identity: Option<SocketIdentity>) {
	let Ok(meta) = fs::symlink_metadata(sock_path) else {
		return;
	};
	if !meta.file_type().is_socket() || meta.uid() != expected_uid {
		return;
	}
	if let Some(identity) = identity
		&& !identity.matches(&meta)
	{
		return;
	}
	let _ = fs::remove_file(sock_path);
}

#[cfg(unix)]
fn prepare_socket_path(sock_path: &Path, owner: Option<SocketOwner>) -> Result<()> {
	if sock_path.as_os_str().is_empty() {
		return Err(err("control socket path must not be empty"));
	}
	let bind_uid = current_euid();
	let expected_uid = owner.map_or(bind_uid, |owner| owner.uid);
	let prebind_uid = allowed_prebind_parent_uid(owner, expected_uid, bind_uid);
	ensure_private_parent(socket_parent(sock_path), sock_path, owner, expected_uid, prebind_uid)?;
	remove_stale_socket(sock_path, expected_uid, prebind_uid)?;
	Ok(())
}

#[cfg(unix)]
fn socket_parent(sock_path: &Path) -> &Path {
	sock_path
		.parent()
		.filter(|parent| !parent.as_os_str().is_empty())
		.unwrap_or_else(|| Path::new("."))
}

#[cfg(unix)]
fn ensure_private_parent(
	parent: &Path,
	sock_path: &Path,
	owner: Option<SocketOwner>,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	match fs::symlink_metadata(parent) {
		Ok(meta) => {
			verify_private_parent(parent, &meta, expected_uid, prebind_uid)?;
			reown_prebind_parent(parent, sock_path, owner, &meta, expected_uid, prebind_uid)
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			fs::DirBuilder::new()
				.mode(0o700)
				.create(parent)
				.map_err(|e| {
					err(format!("creating private control socket directory {}: {e}", parent.display()))
				})?;
			if let Some(owner) = owner {
				chown_path(parent, owner)?;
			}
			chmod_path(parent, 0o700)?;
			let meta = fs::symlink_metadata(parent)?;
			verify_private_parent(parent, &meta, expected_uid, prebind_uid)
		},
		Err(e) => Err(e.into()),
	}
}

#[cfg(unix)]
fn reown_prebind_parent(
	parent: &Path,
	sock_path: &Path,
	owner: Option<SocketOwner>,
	meta: &fs::Metadata,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let Some(owner) = owner else {
		return Ok(());
	};
	if meta.uid() == expected_uid || prebind_uid != Some(meta.uid()) {
		return Ok(());
	}

	ensure_parent_can_be_reowned(parent, sock_path, expected_uid, prebind_uid)?;
	chown_path(parent, owner)?;
	chmod_path(parent, 0o700)?;
	let meta = fs::symlink_metadata(parent)?;
	verify_private_parent(parent, &meta, expected_uid, None)
}

#[cfg(unix)]
fn ensure_parent_can_be_reowned(
	parent: &Path,
	sock_path: &Path,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let sock_name = sock_path.file_name().ok_or_else(|| {
		err(format!("control socket path {} must include a file name", sock_path.display()))
	})?;
	for entry in fs::read_dir(parent)? {
		let entry = entry?;
		if entry.file_name().as_os_str() != sock_name {
			return Err(err(format!(
				"refusing to chown non-empty control socket parent {}",
				parent.display()
			)));
		}
		let meta = fs::symlink_metadata(entry.path())?;
		if !meta.file_type().is_socket() {
			return Err(err(format!(
				"refusing to chown control socket parent {} with non-socket entry {}",
				parent.display(),
				entry.path().display()
			)));
		}
		if !socket_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
			let expected = prebind_uid.map_or_else(
				|| format!("uid {expected_uid}"),
				|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
			);
			return Err(err(format!(
				"refusing to chown control socket parent {} with socket entry owned by uid {}, \
				 expected {expected}",
				parent.display(),
				meta.uid()
			)));
		}
	}
	Ok(())
}

#[cfg(unix)]
const fn allowed_prebind_parent_uid(
	owner: Option<SocketOwner>,
	expected_uid: u32,
	bind_uid: u32,
) -> Option<u32> {
	if owner.is_some() && bind_uid == 0 && bind_uid != expected_uid {
		Some(bind_uid)
	} else {
		None
	}
}

#[cfg(unix)]
fn parent_owner_is_allowed(parent_uid: u32, expected_uid: u32, prebind_uid: Option<u32>) -> bool {
	parent_uid == expected_uid || prebind_uid == Some(parent_uid)
}

#[cfg(unix)]
fn socket_owner_is_allowed(socket_uid: u32, expected_uid: u32, prebind_uid: Option<u32>) -> bool {
	socket_uid == expected_uid || prebind_uid == Some(socket_uid)
}

#[cfg(unix)]
const fn private_parent_mode_is_safe(mode: u32) -> bool {
	mode & 0o077 == 0
}

#[cfg(unix)]
fn verify_private_parent(
	parent: &Path,
	meta: &fs::Metadata,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	if meta.file_type().is_symlink() {
		return Err(err(format!("control socket parent {} must not be a symlink", parent.display())));
	}
	if !meta.is_dir() {
		return Err(err(format!("control socket parent {} is not a directory", parent.display())));
	}

	if !parent_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
		let expected = prebind_uid.map_or_else(
			|| format!("uid {expected_uid}"),
			|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
		);
		return Err(err(format!(
			"control socket parent {} is owned by uid {}, expected {expected}",
			parent.display(),
			meta.uid()
		)));
	}

	let mode = meta.permissions().mode() & 0o777;
	if !private_parent_mode_is_safe(mode) {
		return Err(err(format!(
			"control socket parent {} must be private (mode 0700 or stricter), found {mode:03o}",
			parent.display()
		)));
	}
	Ok(())
}

#[cfg(unix)]
fn remove_stale_socket(
	sock_path: &Path,
	expected_uid: u32,
	prebind_uid: Option<u32>,
) -> Result<()> {
	let meta = match fs::symlink_metadata(sock_path) {
		Ok(meta) => meta,
		Err(e) => {
			return if e.kind() == io::ErrorKind::NotFound {
				Ok(())
			} else {
				Err(e.into())
			};
		},
	};

	if !meta.file_type().is_socket() {
		return Err(err(format!(
			"refusing to remove non-socket control path {}",
			sock_path.display()
		)));
	}
	if !socket_owner_is_allowed(meta.uid(), expected_uid, prebind_uid) {
		let expected = prebind_uid.map_or_else(
			|| format!("uid {expected_uid}"),
			|uid| format!("uid {expected_uid} or pre-bind uid {uid}"),
		);
		return Err(err(format!(
			"refusing to remove control socket {} owned by uid {}, expected {expected}",
			sock_path.display(),
			meta.uid()
		)));
	}
	match UnixStream::connect(sock_path) {
		Ok(_) => {
			Err(err(format!("refusing to remove active control socket {}", sock_path.display())))
		},
		Err(e) if e.kind() == io::ErrorKind::ConnectionRefused => Ok(()),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
		Err(e) => {
			Err(err(format!("refusing to probe stale control socket {}: {e}", sock_path.display())))
		},
	}?;
	fs::remove_file(sock_path)?;
	Ok(())
}

#[cfg(unix)]
fn chown_path(path: &Path, owner: SocketOwner) -> Result<()> {
	let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
		err(format!("control socket path {} contains an interior NUL byte", path.display()))
	})?;
	// SAFETY: `c_path` is a NUL-terminated path derived from `path`; `lchown`
	// reads that pointer only for the duration of the call, and uid/gid are
	// plain value arguments from `SocketOwner`.
	let rc =
		unsafe { libc::lchown(c_path.as_ptr(), owner.uid as libc::uid_t, owner.gid as libc::gid_t) };
	if rc != 0 {
		return Err(err(format!(
			"chowning control socket path {} to {}:{}: {}",
			path.display(),
			owner.uid,
			owner.gid,
			io::Error::last_os_error()
		)));
	}
	Ok(())
}

#[cfg(unix)]
#[allow(clippy::useless_conversion, reason = "mode_t is u16 on macOS but u32 on Linux")]
fn chmod_path(path: &Path, mode: libc::mode_t) -> Result<()> {
	let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
		err(format!("control socket path {} contains an interior NUL byte", path.display()))
	})?;
	// SAFETY: `c_path` is a NUL-terminated path derived from `path`; `fchmodat`
	// reads that pointer only for the duration of the call, with `AT_FDCWD`
	// and `AT_SYMLINK_NOFOLLOW` requiring no additional Rust-side invariants.
	let rc =
		unsafe { libc::fchmodat(libc::AT_FDCWD, c_path.as_ptr(), mode, libc::AT_SYMLINK_NOFOLLOW) };
	if rc == 0 {
		return Ok(());
	}

	let e = io::Error::last_os_error();
	if !chmod_fallback_allowed(&e) {
		return Err(err(format!("chmoding control socket path {} to {mode:o}: {e}", path.display())));
	}

	let meta = fs::symlink_metadata(path)?;
	if meta.file_type().is_symlink() {
		return Err(err(format!("refusing to chmod symlink control socket path {}", path.display())));
	}
	fs::set_permissions(path, fs::Permissions::from_mode(u32::from(mode)))?;
	Ok(())
}

#[cfg(unix)]
fn chmod_fallback_allowed(e: &io::Error) -> bool {
	matches!(
		 e.raw_os_error(),
		 Some(code)
			  if code == libc::EINVAL || code == libc::ENOTSUP || code == libc::EOPNOTSUPP
	)
}

#[cfg(unix)]
fn current_euid() -> u32 {
	// SAFETY: `geteuid` is thread-safe and has no preconditions.
	unsafe { libc::geteuid() as u32 }
}

#[cfg(unix)]
fn configure_stream(stream: &UnixStream) -> io::Result<()> {
	stream.set_read_timeout(Some(CONTROL_READ_TIMEOUT))?;
	stream.set_write_timeout(Some(CONTROL_WRITE_TIMEOUT))?;
	Ok(())
}

#[cfg(target_os = "linux")]
fn verify_peer_credentials(stream: &UnixStream, launch_uid: u32) -> io::Result<()> {
	// SAFETY: `libc::ucred` is a C POD credential struct; an all-zero bit
	// pattern is valid storage before `getsockopt` fills it.
	let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
	let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
	// SAFETY: `cred` and `len` point to writable storage of the advertised
	// `libc::ucred` size, and the fd comes from a live UnixStream.
	let rc = unsafe {
		libc::getsockopt(
			stream.as_raw_fd(),
			libc::SOL_SOCKET,
			libc::SO_PEERCRED,
			&mut cred as *mut _ as *mut libc::c_void,
			&mut len,
		)
	};
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	if cred.uid != 0 && cred.uid != launch_uid {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("control socket peer uid {} is not root or launch uid {launch_uid}", cred.uid),
		));
	}
	Ok(())
}

#[cfg(target_os = "macos")]
fn verify_peer_credentials(stream: &UnixStream, launch_uid: u32) -> io::Result<()> {
	let mut euid: libc::uid_t = 0;
	let mut egid: libc::gid_t = 0;
	// SAFETY: pointers are valid for writes and fd comes from a live UnixStream.
	let rc = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut euid, &mut egid) };
	if rc != 0 {
		return Err(io::Error::last_os_error());
	}
	if euid != 0 && euid != launch_uid {
		return Err(io::Error::new(
			io::ErrorKind::PermissionDenied,
			format!("control socket peer uid {euid} is not root or launch uid {launch_uid}"),
		));
	}
	Ok(())
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
fn verify_peer_credentials(_: &UnixStream, _: u32) -> io::Result<()> {
	Ok(())
}

#[derive(Serialize)]
struct Banner<'a> {
	vmm: &'a str,
	api: u8,
}

#[derive(Deserialize)]
struct Request {
	id:     u64,
	method: String,
	#[serde(default)]
	params: serde_json::Value,
}

#[derive(Serialize)]
struct Response {
	#[serde(skip_serializing_if = "Option::is_none")]
	id:     Option<u64>,
	ok:     bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	result: Option<serde_json::Value>,
	#[serde(skip_serializing_if = "Option::is_none")]
	error:  Option<ApiError>,
}

enum RequestLine {
	Text(String),
	BadRequest { message: &'static str, close: bool },
}

trait ControlStream: Read + Write + Send + Sized + 'static {
	fn duplicate(&self) -> io::Result<Self>;
}

#[cfg(unix)]
impl ControlStream for UnixStream {
	fn duplicate(&self) -> io::Result<Self> {
		self.try_clone()
	}
}

#[cfg(target_os = "windows")]
impl ControlStream for std::fs::File {
	fn duplicate(&self) -> io::Result<Self> {
		self.try_clone()
	}
}

/// Serve one client connection until EOF or an IO error.
fn handle_connection<S: ControlStream>(
	stream: S,
	tx: Sender<ControlCmd>,
	watchdog: Option<OwnerWatchdog>,
) {
	let Ok(mut writer) = stream.duplicate() else {
		return;
	};
	if write_json_line(&mut writer, &Banner { vmm: env!("CARGO_PKG_VERSION"), api: 1 }).is_err() {
		return;
	}

	let mut reader = BufReader::new(stream);
	let mut line = Vec::new();

	loop {
		let request = match read_request_line(&mut reader, &mut line) {
			Ok(Some(RequestLine::Text(request))) => request,
			Ok(Some(RequestLine::BadRequest { message, close })) => {
				let write_failed =
					write_error(&mut writer, None, ApiError::bad_request(message)).is_err();
				if write_failed || close {
					return;
				}
				continue;
			},
			Ok(None) | Err(_) => return,
		};

		let error_id = request_id_for_error(&request);
		let (id, kind) = match parse_request(&request) {
			Ok(parsed) => parsed,
			Err(error) => {
				if write_error(&mut writer, error_id, error).is_err() {
					return;
				}
				continue;
			},
		};

		crate::metrics::record_control_request();
		if let ControlKind::RearmOwnerLease { secs } = kind {
			let reply = watchdog
				.as_ref()
				.ok_or_else(|| ApiError::bad_request("owner watchdog is not configured"))
				.and_then(|watchdog| watchdog.rearm(secs))
				.map(|deadline_unix| serde_json::json!({ "deadline_unix": deadline_unix }));
			if match reply {
				Ok(result) => write_success(&mut writer, id, result),
				Err(error) => write_error(&mut writer, Some(id), error),
			}
			.is_err()
			{
				return;
			}
			continue;
		}

		let (rtx, rrx) = flume::bounded::<ControlReply>(1);
		if tx.send(ControlCmd { kind, reply: rtx }).is_err() {
			return;
		}
		let reply = match rrx.recv_timeout(CONTROL_REPLY_TIMEOUT) {
			Ok(reply) => reply,
			Err(RecvTimeoutError::Timeout) => Err(ApiError::internal(format!(
				"control reply timed out after {CONTROL_REPLY_TIMEOUT:?}"
			))),
			Err(RecvTimeoutError::Disconnected) => return,
		};
		let write_result = match reply {
			Ok(result) => write_success(&mut writer, id, result),
			Err(error) => write_error(&mut writer, Some(id), error),
		};
		if write_result.is_err() {
			return;
		}
	}
}

fn write_success<W: Write>(writer: &mut W, id: u64, result: serde_json::Value) -> io::Result<()> {
	write_json_line(writer, &Response {
		id:     Some(id),
		ok:     true,
		result: Some(result),
		error:  None,
	})
}

fn write_error<W: Write>(writer: &mut W, id: Option<u64>, error: ApiError) -> io::Result<()> {
	write_json_line(writer, &Response { id, ok: false, result: None, error: Some(error) })
}

fn write_json_line<W: Write, T: Serialize>(writer: &mut W, value: &T) -> io::Result<()> {
	serde_json::to_writer(&mut *writer, value)
		.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
	writer.write_all(b"\n")
}

fn read_request_line<R: BufRead>(
	reader: &mut R,
	line: &mut Vec<u8>,
) -> io::Result<Option<RequestLine>> {
	line.clear();
	loop {
		let available = reader.fill_buf()?;
		if available.is_empty() {
			if line.is_empty() {
				return Ok(None);
			}
			break;
		}

		let take = available
			.iter()
			.position(|byte| *byte == b'\n')
			.map_or(available.len(), |pos| pos + 1);
		let newline = available[..take].last() == Some(&b'\n');
		let payload_len = take - usize::from(newline);
		if line.len() + payload_len > CONTROL_MAX_REQUEST_LINE {
			reader.consume(take);
			return Ok(Some(RequestLine::BadRequest {
				message: "control request line exceeds 65536 bytes",
				close:   true,
			}));
		}
		line.extend_from_slice(&available[..take]);
		reader.consume(take);
		if newline {
			break;
		}
	}

	let Ok(request) = std::str::from_utf8(line) else {
		return Ok(Some(RequestLine::BadRequest {
			message: "control request line is not valid UTF-8",
			close:   false,
		}));
	};
	Ok(Some(RequestLine::Text(request.trim().to_owned())))
}

fn request_id_for_error(request: &str) -> Option<u64> {
	serde_json::from_str::<serde_json::Value>(request)
		.ok()?
		.get("id")?
		.as_u64()
}

fn parse_request(request: &str) -> std::result::Result<(u64, ControlKind), ApiError> {
	let request: Request = serde_json::from_str(request)
		.map_err(|e| ApiError::bad_request(format!("malformed JSON request: {e}")))?;
	let kind = match request.method.as_str() {
		"ping" => ControlKind::Ping,
		"info" => ControlKind::Info,
		"pause" => ControlKind::Pause,
		"resume" => ControlKind::Resume,
		"snapshot" => {
			let name = request
				.params
				.get("name")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| ApiError::bad_request("snapshot params.name must be a string"))?;
			let base = match request.params.get("base") {
				None | Some(serde_json::Value::Null) => None,
				Some(serde_json::Value::String(s)) => Some(s.clone()),
				Some(_) => {
					return Err(ApiError::bad_request("snapshot params.base must be a string"));
				},
			};
			let track = match request.params.get("track") {
				None | Some(serde_json::Value::Null) => false,
				Some(serde_json::Value::Bool(track)) => *track,
				Some(_) => {
					return Err(ApiError::bad_request("snapshot params.track must be a boolean"));
				},
			};
			ControlKind::Snapshot { name: name.to_owned(), base, track }
		},
		"disk_snapshot" => {
			let name = request
				.params
				.get("name")
				.and_then(serde_json::Value::as_str)
				.ok_or_else(|| ApiError::bad_request("disk_snapshot params.name must be a string"))?;
			ControlKind::DiskSnapshot { name: name.to_owned() }
		},
		"quit" => ControlKind::Quit,
		"metrics" => ControlKind::Metrics,
		"extend" => {
			let secs = request
				.params
				.get("secs")
				.and_then(serde_json::Value::as_u64)
				.ok_or_else(|| ApiError::bad_request("extend params.secs must be a number"))?;
			ControlKind::Extend { secs }
		},
		"rearm_owner_lease" => {
			let secs = request
				.params
				.get("secs")
				.and_then(serde_json::Value::as_u64)
				.ok_or_else(|| {
					ApiError::bad_request("rearm_owner_lease params.secs must be a number")
				})?;
			ControlKind::RearmOwnerLease { secs }
		},
		method => return Err(ApiError::unknown_method(format!("unknown lifecycle method {method}"))),
	};
	Ok((request.id, kind))
}

#[cfg(all(test, unix))]
mod tests {
	use super::*;

	#[test]
	fn initially_paused_gate_reports_paused_before_vcpu_registration() {
		let gate = PauseGate::new_paused(2);

		assert_eq!(gate.state(), RunState::Paused);
		assert_eq!(gate.parked_count(), 0);
		assert!(!gate.wait_for_all_parked(Duration::from_millis(1)));
	}

	#[test]
	fn json_parse_maps_methods_and_snapshot_name() {
		let (id, kind) =
			parse_request(r#"{"id":7,"method":"snapshot","params":{"name":"safe"}}"#).unwrap();
		assert_eq!(id, 7);
		assert!(matches!(
			kind,
			ControlKind::Snapshot {
				name,
				base: None,
				track: false,
			} if name == "safe"
		));

		let (_, kind) = parse_request(
			r#"{"id":8,"method":"snapshot","params":{"name":"d","base":"base0","track":true}}"#,
		)
		.unwrap();
		assert!(matches!(
			kind,
			ControlKind::Snapshot {
				name,
				base,
				track: true,
			} if name == "d" && base.as_deref() == Some("base0")
		));

		let (_, kind) = parse_request(r#"{"id":9,"method":"metrics","params":null}"#).unwrap();
		assert!(matches!(kind, ControlKind::Metrics));
	}

	#[test]
	fn json_parse_maps_owner_lease_rearm() {
		let (id, kind) =
			parse_request(r#"{"id":11,"method":"rearm_owner_lease","params":{"secs":15}}"#).unwrap();
		assert_eq!(id, 11);
		assert!(matches!(kind, ControlKind::RearmOwnerLease { secs: 15 }));
	}

	#[test]
	fn owner_watchdog_rearm_extends_while_command_work_is_blocked() {
		let watchdog = OwnerWatchdog::new(Some(Instant::now() + Duration::from_millis(20)));
		let (tx, rx) = flume::bounded(1);
		watchdog.start(move || {
			let _ = tx.send(());
		});
		watchdog.rearm(1).unwrap();
		assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
		watchdog.shutdown();
	}

	#[test]
	fn owner_rearm_bypasses_unserved_lifecycle_receiver() {
		use std::io::{BufRead, BufReader, Write};

		let (server, mut client) = UnixStream::pair().expect("create connected control streams");
		client
			.set_read_timeout(Some(Duration::from_millis(100)))
			.expect("set client read timeout");
		let (tx, rx) = flume::unbounded();
		let watchdog = OwnerWatchdog::new(Some(Instant::now() + Duration::from_secs(1)));
		let watcher = watchdog.clone();
		watchdog.start(|| {});
		std::thread::spawn(move || handle_connection(server, tx, Some(watcher)));

		let mut reader = BufReader::new(client.try_clone().expect("clone client stream"));
		let mut line = String::new();
		reader.read_line(&mut line).expect("read banner");
		client
			.write_all(br#"{"id":1,"method":"rearm_owner_lease","params":{"secs":1}}"#)
			.and_then(|()| client.write_all(b"\n"))
			.expect("send rearm request");
		line.clear();
		reader
			.read_line(&mut line)
			.expect("read immediate rearm reply");
		let reply: serde_json::Value = serde_json::from_str(&line).expect("parse rearm reply");
		assert_eq!(reply["ok"], true);
		assert!(rx.try_recv().is_err(), "rearm must not wait behind lifecycle work");
		drop(client);
		watchdog.shutdown();
	}

	#[test]
	fn owner_watchdog_expires_without_command_receiver() {
		let watchdog = OwnerWatchdog::new(Some(Instant::now() + Duration::from_millis(20)));
		let (tx, rx) = flume::bounded(1);
		watchdog.start(move || {
			let _ = tx.send(());
		});
		rx.recv_timeout(Duration::from_secs(1))
			.expect("watchdog expires");
		assert!(watchdog.rearm(1).is_err(), "expired watchdog cannot be resurrected");
		watchdog.shutdown();
	}

	#[test]
	fn json_parse_maps_disk_snapshot_name() {
		let (id, kind) =
			parse_request(r#"{"id":10,"method":"disk_snapshot","params":{"name":"disk-0"}}"#).unwrap();
		assert_eq!(id, 10);
		assert!(matches!(kind, ControlKind::DiskSnapshot { name } if name == "disk-0"));
	}

	#[test]
	fn json_parse_rejects_unknown_method() {
		let error = parse_request(r#"{"id":1,"method":"bogus","params":null}"#).unwrap_err();
		assert_eq!(error.code, "unknown_method");
	}

	#[test]
	fn root_prebind_allows_current_root_parent_for_sandbox_socket() {
		let owner = Some(SocketOwner { uid: 1000, gid: 1000 });

		let prebind_uid = allowed_prebind_parent_uid(owner, 1000, 0);

		assert_eq!(prebind_uid, Some(0));
		assert!(parent_owner_is_allowed(0, 1000, prebind_uid));
		assert!(parent_owner_is_allowed(1000, 1000, prebind_uid));
	}

	#[test]
	fn prebind_parent_allowance_is_limited_to_root_uid_drop() {
		let owner = Some(SocketOwner { uid: 1000, gid: 1000 });

		assert_eq!(allowed_prebind_parent_uid(owner, 1000, 1000), None);
		assert_eq!(allowed_prebind_parent_uid(owner, 1000, 501), None);
		assert_eq!(allowed_prebind_parent_uid(None, 0, 0), None);
		assert!(!parent_owner_is_allowed(0, 1000, None));
		assert!(!parent_owner_is_allowed(1001, 1000, Some(0)));
	}

	#[test]
	fn stale_socket_allowance_matches_expected_or_prebind_owner() {
		assert!(socket_owner_is_allowed(1000, 1000, None));
		assert!(socket_owner_is_allowed(0, 1000, Some(0)));
		assert!(!socket_owner_is_allowed(1001, 1000, Some(0)));
	}

	#[test]
	fn oversized_request_line_is_rejected_and_connection_is_closed() {
		use std::{io::Write, os::unix::net::UnixStream, thread};

		let (mut writer, reader) = UnixStream::pair().unwrap();
		let handle = thread::spawn(move || {
			let payload = vec![b'a'; CONTROL_MAX_REQUEST_LINE + 1];
			let _ = writer.write_all(&payload);
		});
		let mut reader = BufReader::new(reader);
		let mut line = Vec::new();

		let request = read_request_line(&mut reader, &mut line).unwrap();

		assert!(matches!(
			request,
			Some(RequestLine::BadRequest {
				message: "control request line exceeds 65536 bytes",
				close:   true,
			})
		));
		drop(reader);
		handle.join().unwrap();
	}

	#[test]
	fn cleanup_skips_reused_socket_path_with_different_inode() {
		use std::{fs, os::unix::net::UnixListener};

		let dir = std::env::temp_dir().join(format!(
			"vmon-control-cleanup-{}-{}",
			std::process::id(),
			"reuse"
		));
		let _ = fs::remove_dir_all(&dir);
		fs::create_dir(&dir).unwrap();
		let sock = dir.join("control.sock");
		let listener = UnixListener::bind(&sock).unwrap();
		let live = fs::symlink_metadata(&sock).unwrap();
		// A recorded identity from a *previous* socket at this path. Craft it to
		// differ from the live inode rather than relying on the filesystem to hand
		// a replacement a fresh inode number — Linux readily recycles it, which
		// would make a stale identity spuriously match the live socket.
		let stale = SocketIdentity { dev: live.dev(), ino: live.ino().wrapping_add(1) };
		cleanup_socket_for_uid(&sock, live.uid(), Some(stale));

		assert!(sock.exists(), "cleanup removed a socket whose identity did not match");
		drop(listener);
		fs::remove_file(&sock).unwrap();
		fs::remove_dir(&dir).unwrap();
	}

	#[test]
	fn private_parent_mode_rejects_group_or_world_access() {
		assert!(private_parent_mode_is_safe(0o700));
		assert!(private_parent_mode_is_safe(0o500));
		assert!(!private_parent_mode_is_safe(0o710));
		assert!(!private_parent_mode_is_safe(0o770));
		assert!(!private_parent_mode_is_safe(0o707));
	}
}
