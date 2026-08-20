//! Top-level VM lifecycle: build / run / pause / resume / snapshot / restore.
//!
//! [`Vmm`] owns the KVM VM, the device model, and the vCPU + worker threads.
//! A fresh boot goes through [`Vmm::build`]; `--restore <dir>` reconstructs a
//! paused-then-snapshotted VM through [`Vmm::restore`] without booting a
//! kernel. Once running, an optional Unix control socket drives
//! pause/resume/snapshot.
//!
//! ## Per-VM thread census
//!
//! A running VM owns: one thread per vCPU, ONE shared virtio device event-loop
//! thread (`vmon-virtio`, every device multiplexed over a single poll set —
//! see [`crate::virtio::run_shared_worker`]), and the main thread which runs
//! the run/control loop. Optional helpers (pager handler, agent bridge,
//! deadline timer) add at most one thread each. Windows builds
//! (`vmm_windows.rs`) keep one worker thread per device instead, because
//! `WaitForMultipleObjects` caps a wait set at 64 handles.

use std::{
	collections::VecDeque,
	fs,
	io::{self, Read, Write},
	os::unix::{
		fs::{FileTypeExt, MetadataExt, PermissionsExt},
		io::AsRawFd,
		net::{UnixListener, UnixStream},
	},
	path::{Path, PathBuf},
	sync::{
		Arc, OnceLock,
		atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant},
};

use flume::{Receiver, RecvTimeoutError, Sender};
use parking_lot::Mutex;
use serde_json::json;
use tracing::{error, info, warn};
use vmon_agent::proto;

#[cfg(target_arch = "x86_64")]
use crate::devices::pci::{PciEcam, PciFunction, PciMmioDispatcher, PciRoot};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use crate::devices::vfio::{
	PCI_HIGH_MMIO_SIZE, PCI_HIGH_MMIO_START, PreopenedVfio, VfioPciDevice, VfioPciResources,
	attach_gpu_devices, normalize_gpu_paths, preopen_gpu_devices,
};
#[cfg(target_arch = "aarch64")]
use crate::hv::Gic;
#[cfg(target_arch = "x86_64")]
use crate::layout::{
	PCI_CONFIG_IO_BASE, PCI_CONFIG_IO_SIZE, PCI_ECAM_BASE, PCI_ECAM_SIZE, PCI_VIRTIO_BAR_SIZE,
	PCI_VIRTIO_MMIO_SIZE, PCI_VIRTIO_MMIO_START,
};
#[cfg(target_arch = "x86_64")]
use crate::virtio::pci::{PciCommonState, PciTransport};
use crate::{
	arch,
	config::{BootMode, Config, MAX_COUNT, Transport},
	control::{self, ControlCmd, ControlKind, PauseGate, RunState, VcpuCmd},
	devices::{Bus, serial::SerialDevice},
	disk_snapshot,
	hv::{Exit, Vcpu, Vm},
	layout::{IRQ_BASE, MMIO_DEVICE_SIZE, MMIO_MEM_START, SERIAL_IRQ},
	metrics::VmExit,
	os::{EFD_NONBLOCK, EventFd},
	result::{Result, err},
	snapshot::{
		self, BackendHint, DeviceKind, DeviceState, DeviceTransportKind, Snapshot, build_arch,
	},
	tap::Tap,
	virtio::{
		DeviceWorker, Interrupt, VirtioDevice, WorkerControl,
		block::Block,
		console::OUTPUT_CAP,
		fs::Fs,
		mmio::{MmioState, MmioTransport},
		net::Net,
		remotefs::RemoteFs,
		run_shared_worker,
	},
};

const MAX_FORK_CHILD_CONCURRENCY: usize = 8;

const QUEUE_NOTIFY_OFFSET: u64 = 0x50;
const PAUSE_WAIT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const PAUSE_VCPU_PARK_TIMEOUT: Duration = Duration::from_secs(5);
const PAUSE_WORKER_ACK_TIMEOUT: Duration = Duration::from_secs(5);
const SNAPSHOT_VCPU_STATE_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
type PreopenedGpuResources = Option<PreopenedVfio>;
#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
type PreopenedGpuResources = ();

#[cfg(target_arch = "x86_64")]
const DEFAULT_CMDLINE: &str = "console=ttyS0 reboot=t panic=-1";
#[cfg(target_arch = "aarch64")]
const DEFAULT_CMDLINE: &str = "console=ttyS0 reboot=t panic=-1 earlycon=uart8250,mmio,0x9000000";

/// Saved host terminal settings, restored on shutdown. Captured at runtime
/// (not known at declaration), so `OnceLock` — not `LazyLock` — is correct.
static ORIG_TERMIOS: OnceLock<libc::termios> = OnceLock::new();

type ConsoleInput = (Arc<Mutex<VecDeque<u8>>>, EventFd);
type ConsoleOutput = (Arc<Mutex<Vec<u8>>>, EventFd, EventFd);

enum DeviceTransport {
	Mmio(Arc<Mutex<MmioTransport>>),
	#[cfg(target_arch = "x86_64")]
	Pci(Arc<Mutex<PciTransport>>),
}

enum FsSave {
	Host(Arc<Mutex<Fs>>),
	Remote(Arc<Mutex<RemoteFs>>),
}

impl FsSave {
	fn save(&self) -> snapshot::FsStateSer {
		match self {
			Self::Host(device) => device.lock().save(),
			Self::Remote(device) => device.lock().save(),
		}
	}
}

/// VMM-side handle to a wired virtio device: lets the lifecycle pause/resume
/// the worker and read transport/queue/interrupt state for a snapshot.
struct DeviceHandle {
	kind:             DeviceKind,
	mmio_base:        u64,
	gsi:              u32,
	transport:        DeviceTransport,
	device:           Arc<Mutex<dyn VirtioDevice>>,
	interrupt:        Arc<Interrupt>,
	backend:          BackendHint,
	fs:               Option<FsSave>,
	/// Clones of the worker's control eventfds (the worker holds the originals).
	pause_evt:        EventFd,
	resume_evt:       EventFd,
	ack_evt:          EventFd,
	failure_evt:      EventFd,
	failure_msg:      Arc<Mutex<Option<String>>>,
	pause_requested:  Arc<AtomicBool>,
	pause_generation: Arc<AtomicU64>,
	ack_generation:   Arc<AtomicU64>,
}

#[cfg(target_os = "macos")]
enum MacosStartup {
	Boot {
		entry:        vm_memory::GuestAddress,
		initrd:       Option<(vm_memory::GuestAddress, usize)>,
		virtio_infos: Vec<(u64, u32)>,
	},
	Restore {
		vcpus:   Vec<arch::state::VcpuState>,
		machine: arch::state::MachineState,
	},
}

#[cfg(target_os = "macos")]
enum MacosPhaseB {
	Boot { entry: vm_memory::GuestAddress, fdt_addr: u64 },
	Restore(arch::state::VcpuState),
}

/// A built-but-not-yet-started VM and its lifecycle state.
pub struct Vmm {
	vm:              Arc<Vm>,
	cpus:            u8,
	mem_mib:         usize,
	cmdline:         String,
	boot_mode:       BootMode,
	firmware:        Option<PathBuf>,
	#[cfg(target_arch = "x86_64")]
	#[allow(dead_code, reason = "UEFI firmware mmap must stay alive but is not read afterwards")]
	firmware_memory: Option<crate::memory::GuestMemoryMmap>,
	gate:            Arc<PauseGate>,

	/// Prepared Linux vCPUs (configured/restored), drained into threads by
	/// `start`.
	#[cfg(target_os = "linux")]
	vcpus:         Vec<Vcpu>,
	/// Plain startup payload used to create/configure HVF vCPUs on their owning
	/// threads.
	#[cfg(target_os = "macos")]
	macos_startup: MacosStartup,
	vcpu_cmd_txs:  Vec<Sender<VcpuCmd>>,
	vcpu_cmd_rxs:  Vec<Receiver<VcpuCmd>>,
	vcpu_handles:  Vec<JoinHandle<()>>,

	pager:         Option<Arc<crate::pager::Pager>>,
	pager_handler: Option<JoinHandle<()>>,

	pio_bus:          Arc<Bus>,
	mmio_bus:         Arc<Bus>,
	serial:           Arc<Mutex<SerialDevice>>,
	devices:          Vec<DeviceHandle>,
	device_workers:   Vec<DeviceWorker>,
	device_loop:      Option<JoinHandle<()>>,
	/// VM-global kill event terminating the shared device loop.
	worker_kill_evt:  EventFd,
	#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
	vfio_devices:     Vec<Arc<VfioPciDevice>>,
	/// Host end of the virtio-console agent channel (input buffer + wake
	/// eventfd), present when `--console-agent` is set. Used to push bytes into
	/// the guest.
	console_input:    Option<ConsoleInput>,
	/// Guest-output side of the virtio-console agent channel, used by
	/// `--agent-sock` to bridge raw bytes back to the host client.
	console_output:   Option<ConsoleOutput>,
	/// User lifetime timeout seconds (`--timeout-secs`).
	timeout_secs:     Option<u64>,
	/// Production ownership watchdog seconds (`--owner-lease-secs`).
	owner_lease_secs: Option<u64>,
	/// Monotonic origin used for uptime and deadline math.
	started_at:       Instant,
	/// Snapshot name a dirty-tracking window is armed against: a delta against
	/// exactly this base may use the hypervisor log + host-write bitmap
	/// instead of a full RAM scan. Consumed (cleared) by that delta.
	dirty_base:       Mutex<Option<String>>,
	/// User lifetime deadline, reset only by `extend`.
	deadline:         Option<Instant>,
	/// Ownership deadline, reset only by `rearm_owner_lease`.
	owner_deadline:   Option<Instant>,
	/// Control-plane socket path; `status.json` is written beside it on exit.
	api_sock:         Option<PathBuf>,
	/// VMM-level exit reason; the first thread to stop the VM records it.
	exit_reason:      Arc<AtomicU8>,
	#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
	gic:              Gic,
	#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
	gic:              Option<Gic>,
}

struct AgentSocket {
	path:       PathBuf,
	listener:   UnixListener,
	launch_uid: u32,
}

impl AgentSocket {
	fn cleanup(&self) {
		cleanup_agent_socket_path(&self.path);
	}
}

impl Drop for AgentSocket {
	fn drop(&mut self) {
		self.cleanup();
	}
}

#[derive(Clone, Copy)]
struct ControlPlaneOwner {
	socket_owner: Option<control::SocketOwner>,
	launch_uid:   u32,
}

pub struct PreboundSockets {
	control_server: Option<control::ControlServer>,
	agent_socket:   Option<AgentSocket>,
	launch_uid:     u32,
}

impl PreboundSockets {
	const fn none(launch_uid: u32) -> Self {
		Self { control_server: None, agent_socket: None, launch_uid }
	}
}

#[allow(
	dead_code,
	reason = "MachineInfo will be used more extensively in forthcoming VMM features"
)]
struct MachineInfo {
	cpus:          u8,
	mem_mib:       usize,
	state:         RunState,
	devices:       Vec<&'static str>,
	deadline_unix: Option<i64>,
	uptime_ms:     u64,
}

/// VMM-level lifecycle exit reason, distinct from any in-guest process code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VmmExit {
	Timeout,
	Quit,
	Shutdown,
	Killed,
}

impl VmmExit {
	const fn as_u8(self) -> u8 {
		match self {
			Self::Timeout => 1,
			Self::Quit => 2,
			Self::Shutdown => 3,
			Self::Killed => 4,
		}
	}

	/// Decode a value stored via [`VmmExit::as_u8`]; `0` (unset) and any unknown
	/// value fall back to `Shutdown` (a clean VMM stop).
	const fn from_u8(value: u8) -> Self {
		match value {
			1 => Self::Timeout,
			2 => Self::Quit,
			4 => Self::Killed,
			_ => Self::Shutdown,
		}
	}

	const fn reason(self) -> &'static str {
		match self {
			Self::Timeout => "timeout",
			Self::Quit => "quit",
			Self::Shutdown => "shutdown",
			Self::Killed => "killed",
		}
	}

	/// VMM process-level return code surfaced in `status.json`.
	const fn returncode(self) -> i64 {
		match self {
			Self::Timeout => 124,
			Self::Quit | Self::Killed => 137,
			Self::Shutdown => 0,
		}
	}
}

/// Build (or restore) a guest and run it, optionally under a control socket.
pub fn run(
	#[allow(unused_mut, reason = "NVIDIA VFIO paths are normalized only on x86_64 Linux")]
	mut config: Config,
) -> Result<()> {
	#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
	normalize_gpu_paths(&mut config.vfio_gpus)?;
	// --fork-from with --count > 1: this process is the fanout parent. It must
	// spawn children before any per-child jail, control, or agent sockets exist.
	if let Some(dir) = &config.fork_from
		&& config.count > 1
	{
		return spawn_fork_children(dir, &config);
	}
	if config.no_sandbox {
		tracing::warn!("host process sandbox explicitly disabled");
	}
	#[cfg(target_os = "linux")]
	if config.jail {
		return crate::jail::fork_into_jail(&config);
	}
	#[cfg(not(target_os = "linux"))]
	if config.jail {
		return Err(err("--jail requires a Linux host"));
	}
	run_inner(config)
}

pub fn run_inner(config: Config) -> Result<()> {
	let launch_uid = launch_owner_uid()?;
	run_inner_with_prebound(config, PreboundSockets::none(launch_uid))
}

fn launch_owner_uid() -> Result<u32> {
	// SAFETY: `getuid` has no preconditions and only reads process credentials.
	let real_uid = unsafe { libc::getuid() as u32 };
	if real_uid != 0 {
		return Ok(real_uid);
	}
	Ok(parse_env_u32("SUDO_UID")?
		.filter(|uid| *uid != 0)
		.unwrap_or(0))
}

fn parse_env_u32(name: &str) -> Result<Option<u32>> {
	match std::env::var(name) {
		Ok(value) => value
			.parse()
			.map(Some)
			.map_err(|_| err(format!("invalid value {value:?} for {name}"))),
		Err(std::env::VarError::NotPresent) => Ok(None),
		Err(std::env::VarError::NotUnicode(_)) => Err(err(format!("{name} must be valid UTF-8"))),
	}
}

/// Main VMM lifecycle path after any entry-point-only launch wrapping.
pub fn run_inner_with_prebound(config: Config, mut prebound: PreboundSockets) -> Result<()> {
	let api_sock = config.api_sock.clone();
	// Cloned for the pre-opened status file; `api_sock` itself is moved into
	// bind_control_socket below.
	let status_sock = config.api_sock.clone();
	let snapshot_root = config.snapshot_root.clone();
	let agent_sock = config.agent_sock.clone();
	let agent_exec = config.agent_exec.clone();
	#[cfg(target_os = "linux")]
	let filters_enabled = config.jail || !config.no_sandbox;
	#[cfg(not(target_os = "linux"))]
	let filters_enabled = false;
	#[cfg(not(target_os = "linux"))]
	if !config.no_sandbox {
		warn!("host sandbox is unavailable on this OS; continuing without it");
	}
	#[cfg(not(target_os = "linux"))]
	let _ = filters_enabled;
	let jail = config.jail;
	let restoring = config.restore.is_some();
	let forking = config.fork_from.is_some();
	#[cfg(target_os = "linux")]
	let sandbox_config = {
		let sandbox_paths = build_sandbox_paths(&config)?;
		crate::sandbox::SandboxConfig::new(
			config.sandbox_uid,
			config.sandbox_gid,
			sandbox_paths,
			config.seccomp_action,
		)
	};
	let control_owner = control_plane_owner(prebound.launch_uid);
	let warm = restoring || forking;

	// Kept for direct unit callers of `run_inner_with_prebound`; the public
	// entry point handles this before jail/socket setup.
	if let Some(dir) = &config.fork_from
		&& config.count > 1
	{
		return spawn_fork_children(dir, &config);
	}

	// Bind the control-plane and agent sockets FIRST, while still privileged.
	// They are created before any privilege drop; SO_PEERCRED later accepts root
	// plus the original launch owner (SUDO_UID when launched through sudo).
	// Under --jail the jailer has already pivoted and pre-bound them, so reuse
	// those.
	let control_server = match prebound.control_server.take() {
		Some(server) => Some(server),
		None => bind_control_socket(api_sock, control_owner)?,
	};
	let agent_socket = match prebound.agent_socket.take() {
		Some(socket) => Some(socket),
		None => match bind_agent_socket(agent_sock, jail, prebound.launch_uid) {
			Ok(socket) => socket,
			Err(e) => {
				cleanup_control_server(control_server.as_ref());
				return Err(e);
			},
		},
	};

	// Pre-open status.json while still privileged (before the drop + Landlock),
	// so the exit-time write lands through this held fd even after dropping to
	// the sandbox uid and installing path-based Landlock rules. The socket
	// parent dir already exists here (bound above, or pre-created by the jailer).
	let mut status_file = open_status_file(status_sock.as_deref());

	enum PagerSetup {
		Zram(std::os::fd::OwnedFd, std::os::fd::OwnedFd, usize),
		Remote(std::os::fd::OwnedFd, std::os::fd::OwnedFd, String),
	}
	let pager_setup = if let Some(remote_url) = config.remote_page_url.clone() {
		let setup = crate::pager::create_uffd()
			.and_then(|uffd| crate::pager::open_swap_file(None).map(|swap| (uffd, swap)));
		match setup {
			Ok((uffd, swap)) => Some(PagerSetup::Remote(uffd, swap, remote_url)),
			Err(e) => {
				write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
				cleanup_agent_socket(agent_socket.as_ref());
				cleanup_control_server(control_server.as_ref());
				return Err(e);
			},
		}
	} else {
		match config.mem_target_mib {
			Some(target_mib) => {
				let setup = crate::pager::create_uffd().and_then(|uffd| {
					crate::pager::open_swap_file(config.zram_swap_file.as_deref())
						.map(|swap| (uffd, swap, target_mib))
				});
				match setup {
					Ok((uffd, swap, target_mib)) => Some(PagerSetup::Zram(uffd, swap, target_mib)),
					Err(e) => {
						write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
						cleanup_agent_socket(agent_socket.as_ref());
						cleanup_control_server(control_server.as_ref());
						return Err(e);
					},
				}
			},
			None => None,
		}
	};

	#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
	let preopened_gpu = match preopen_gpu_devices(&config.vfio_gpus) {
		Ok(resources) => resources,
		Err(e) => {
			write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
			cleanup_agent_socket(agent_socket.as_ref());
			cleanup_control_server(control_server.as_ref());
			return Err(e);
		},
	};
	#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
	let preopened_gpu = ();

	// Privilege phase: no_new_privs + uid/gid drop. Run BEFORE the VMM opens
	// /dev/kvm and backing files so every fd is owned by the unprivileged
	// sandbox uid rather than root. Under --jail the operator must grant that
	// uid access to the bind-mounted /dev/kvm. The drop is a no-op when the
	// process is not root. (rlimit tightening is deferred to the filter phase
	// so the NOFILE clamp can account for the fds Vmm::build opens.)
	#[cfg(target_os = "linux")]
	if filters_enabled && let Err(e) = crate::sandbox::apply_privilege_phase(&sandbox_config) {
		write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
		cleanup_agent_socket(agent_socket.as_ref());
		cleanup_control_server(control_server.as_ref());
		return Err(e);
	}

	// Build / restore / fork the VMM as the (now dropped) uid.
	let t0 = std::time::Instant::now();
	let build_result = if let Some(dir) = config.restore.clone() {
		Vmm::restore(&dir, &config)
	} else if let Some(dir) = config.fork_from.clone() {
		Vmm::fork_from(&dir, &config)
	} else {
		Vmm::build(config.clone(), preopened_gpu)
	};
	let mut vmm = match build_result {
		Ok(vmm) => vmm,
		Err(e) => {
			write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
			cleanup_agent_socket(agent_socket.as_ref());
			cleanup_control_server(control_server.as_ref());
			return Err(e);
		},
	};
	let reconstruct_elapsed = t0.elapsed();
	if restoring {
		crate::metrics::record_restore_duration(reconstruct_elapsed);
	} else if forking {
		crate::metrics::record_fork_duration(reconstruct_elapsed);
	} else {
		crate::metrics::record_boot_duration(reconstruct_elapsed);
	}
	if let Some(pager_setup) = pager_setup {
		let pager = match pager_setup {
			PagerSetup::Zram(uffd, swap, target_mib) => {
				let pager_limits = (|| -> Result<(usize, usize)> {
					let target_bytes = target_mib
						.checked_mul(1 << 20)
						.ok_or_else(|| err("--mem-target-mib overflows usize"))?;
					let target_pages = target_bytes / 4096;
					let store_max_mib = config
						.zram_store_max_mib
						.unwrap_or_else(|| (config.mem_mib / 4).max(16));
					let store_max = store_max_mib
						.checked_mul(1 << 20)
						.ok_or_else(|| err("--zram-store-max-mib overflows usize"))?;
					Ok((target_pages, store_max))
				})();
				let (target_pages, store_max) = match pager_limits {
					Ok(limits) => limits,
					Err(e) => {
						write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
						cleanup_agent_socket(agent_socket.as_ref());
						cleanup_control_server(control_server.as_ref());
						return Err(e);
					},
				};
				crate::pager::Pager::new(uffd, swap, vmm.vm.memory(), target_pages, store_max)
			},
			PagerSetup::Remote(uffd, swap, url) => {
				let token = std::env::var("VMON_REMOTE_PAGE_TOKEN")
					.map_err(|_| err("remote page-in requires VMON_REMOTE_PAGE_TOKEN"))?;
				let source = crate::pager::RemotePageSource::new(&url, token)?;
				let fatal = crate::pager::PagerFatal::new(
					vmm.gate.clone(),
					vmm.exit_reason.clone(),
					VmmExit::Killed.as_u8(),
				);
				crate::pager::Pager::new_remote(uffd, swap, vmm.vm.memory(), source, fatal)
			},
		};
		match pager {
			Ok(pager) => vmm.pager = Some(pager),
			Err(e) => {
				write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
				cleanup_agent_socket(agent_socket.as_ref());
				cleanup_control_server(control_server.as_ref());
				return Err(e);
			},
		}
	}

	// Installed before any vCPU can be asked to pause; harmless without a socket.
	if let Err(e) = control::pause_signal::install() {
		write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
		cleanup_agent_socket(agent_socket.as_ref());
		cleanup_control_server(control_server.as_ref());
		return Err(e);
	}

	// Filter phase: tighten rlimits, then Landlock then seccomp. MUST be applied
	// before vmm.start() spawns the vCPU/device-worker threads so they inherit it.
	#[cfg(target_os = "linux")]
	if filters_enabled && let Err(e) = crate::sandbox::apply_filter_phase(&sandbox_config) {
		write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
		cleanup_agent_socket(agent_socket.as_ref());
		cleanup_control_server(control_server.as_ref());
		return Err(e);
	}

	if let Err(e) = vmm.start() {
		let shutdown = vmm.shutdown_threads();
		write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
		cleanup_agent_socket(agent_socket.as_ref());
		cleanup_control_server(control_server.as_ref());
		return match shutdown {
			Ok(()) => Err(e),
			Err(shutdown_err) => {
				Err(err(format!("starting VM failed: {e}; shutdown failed: {shutdown_err}")))
			},
		};
	}
	let boot_exec = agent_exec.as_deref().map(boot_exec_frames);
	let (deferred_exec, immediate_exec) = if warm {
		(None, boot_exec)
	} else {
		(boot_exec, None)
	};
	let agent_bridge = match spawn_agent_bridge(agent_socket, &vmm, deferred_exec, warm) {
		Ok(handle) => handle,
		Err(e) => {
			let shutdown = vmm.shutdown_threads();
			write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
			cleanup_control_server(control_server.as_ref());
			return match shutdown {
				Ok(()) => Err(e),
				Err(shutdown_err) => Err(err(format!(
					"spawning agent bridge failed: {e}; shutdown failed: {shutdown_err}"
				))),
			};
		},
	};
	if warm {
		info!("warm-boot reconstruct took {:?}", t0.elapsed());
	}
	// Frame the --agent-exec entrypoint hand-off. Warm boots deliver it
	// immediately (the restored agent already owns the channel); cold boots
	// hand it to the agent bridge, which defers delivery until the agent's
	// readiness marker appears (input queued before the agent opens /dev/hvc0
	// is dropped by the guest tty layer).
	if let Some(frames) = &immediate_exec {
		if let Err(e) = vmm.agent_send(frames) {
			let shutdown = vmm.shutdown_threads();
			let agent_shutdown = join_agent_bridge(agent_bridge);
			write_final_status(status_file.as_mut(), status_sock.as_deref(), VmmExit::Killed);
			cleanup_control_server(control_server.as_ref());
			return match (shutdown, agent_shutdown) {
				(Ok(()), Ok(())) => Err(e),
				(Err(shutdown_err), _) => {
					Err(err(format!("sending agent exec failed: {e}; shutdown failed: {shutdown_err}")))
				},
				(Ok(()), Err(agent_err)) => Err(err(format!(
					"sending agent exec failed: {e}; agent bridge shutdown failed: {agent_err}"
				))),
			};
		}
		info!("agent exec dispatched at {:?} from launch", t0.elapsed());
	}
	vmm.serve_and_wait(control_server, snapshot_root.as_deref(), status_file, agent_bridge)
}

#[cfg(target_os = "linux")]
pub fn prebind_jail_sockets(config: &Config, launch_uid: u32) -> Result<PreboundSockets> {
	Ok(PreboundSockets {
		control_server: bind_control_socket(
			config.api_sock.clone(),
			control_plane_owner(launch_uid),
		)?,
		agent_socket: bind_agent_socket(config.agent_sock.clone(), true, launch_uid)?,
		launch_uid,
	})
}

fn bind_control_socket(
	api_sock: Option<PathBuf>,
	owner: ControlPlaneOwner,
) -> Result<Option<control::ControlServer>> {
	match api_sock {
		Some(sock) => control::bind(sock.clone(), owner.socket_owner, owner.launch_uid)
			.map(Some)
			.map_err(|error| {
				err(format!("binding control socket {} failed: {error}", sock.display()))
			}),
		None => Ok(None),
	}
}

fn bind_agent_socket(
	agent_sock: Option<PathBuf>,
	jail: bool,
	launch_uid: u32,
) -> Result<Option<AgentSocket>> {
	let Some(sock_path) = agent_sock else {
		return Ok(None);
	};
	if jail {
		validate_jail_agent_socket_path(&sock_path)?;
	}
	prepare_agent_socket_path(&sock_path, launch_uid)?;
	let listener = UnixListener::bind(&sock_path)
		.map_err(|e| err(format!("binding agent socket {} failed: {e}", sock_path.display())))?;
	if let Err(e) = fs::set_permissions(&sock_path, fs::Permissions::from_mode(0o600)) {
		cleanup_agent_socket_path(&sock_path);
		return Err(err(format!("chmoding agent socket {} to 600 failed: {e}", sock_path.display())));
	}
	listener.set_nonblocking(true).map_err(|e| {
		cleanup_agent_socket_path(&sock_path);
		err(format!("setting agent socket {} nonblocking failed: {e}", sock_path.display()))
	})?;
	Ok(Some(AgentSocket { path: sock_path, listener, launch_uid }))
}

/// Request id for the boot-time `--agent-exec` hand-off. Client request ids
/// count upward from 1, so the VMM's own request can never collide with one.
const BOOT_EXEC_ID: u32 = u32::MAX;

/// Frame the `--agent-exec` entrypoint hand-off: a SYNC marker realigning the
/// channel, then an exec request the guest agent runs as `/bin/sh -c` with
/// output on the guest console (the agent channel may have no host client
/// attached when the command runs).
fn boot_exec_frames(line: &str) -> Vec<u8> {
	let script = format!("exec >/dev/console 2>&1\n{line}");
	let payload = serde_json::to_vec(&json!({
		"op": "exec",
		"cmd": ["/bin/sh", "-c", script],
	}))
	.expect("boot exec request serializes");
	let mut frames = Vec::with_capacity(proto::SYNC.len() + proto::HEADER_LEN + payload.len());
	frames.extend_from_slice(&proto::SYNC);
	proto::write_frame(&mut frames, proto::FRAME_REQ, BOOT_EXEC_ID, &payload)
		.expect("boot exec payload is under the frame cap");
	frames
}

/// Streaming matcher that latches once the guest agent's readiness marker is
/// consumed.
struct SyncScan {
	window: [u8; proto::HEADER_LEN],
	filled: usize,
	seen:   bool,
}

impl SyncScan {
	const fn new(seen: bool) -> Self {
		Self { window: [0; proto::HEADER_LEN], filled: 0, seen }
	}

	fn feed(&mut self, bytes: &[u8]) {
		if self.seen {
			return;
		}
		for &byte in bytes {
			if self.filled < self.window.len() {
				self.window[self.filled] = byte;
				self.filled += 1;
			} else {
				self.window.rotate_left(1);
				self.window[self.window.len() - 1] = byte;
			}
			if self.filled == self.window.len() && self.window == proto::SYNC {
				self.seen = true;
				return;
			}
		}
	}
}

fn spawn_agent_bridge(
	agent_socket: Option<AgentSocket>,
	vmm: &Vmm,
	boot_exec: Option<Vec<u8>>,
	agent_ready: bool,
) -> Result<Option<JoinHandle<()>>> {
	let Some(socket) = agent_socket else {
		return Ok(None);
	};
	let (input, input_evt) = vmm
		.console_input
		.as_ref()
		.ok_or_else(|| err("--agent-sock requires a virtio-console agent device"))?;
	let (output, output_evt, drain_resume_evt) = vmm
		.console_output
		.as_ref()
		.ok_or_else(|| err("--agent-sock requires guest-output console handles"))?;
	let input = Arc::clone(input);
	let input_evt = input_evt.try_clone()?;
	let output = Arc::clone(output);
	let output_evt = output_evt.try_clone()?;
	let drain_resume_evt = drain_resume_evt.try_clone()?;
	let gate = Arc::clone(&vmm.gate);

	thread::Builder::new()
		.name("vmon-agent-bridge".to_string())
		.spawn(move || {
			if let Err(e) = agent_bridge_loop(
				socket,
				input,
				input_evt,
				output,
				output_evt,
				drain_resume_evt,
				gate,
				boot_exec,
				agent_ready,
			) {
				warn!("agent bridge exited: {e}");
			}
		})
		.map(Some)
		.map_err(|e| err(format!("spawning agent bridge thread failed: {e}")))
}

fn join_agent_bridge(handle: Option<JoinHandle<()>>) -> Result<()> {
	if let Some(handle) = handle
		&& handle.join().is_err()
	{
		return Err(err("agent bridge thread panicked during shutdown"));
	}
	Ok(())
}

fn cleanup_agent_socket(agent_socket: Option<&AgentSocket>) {
	if let Some(agent_socket) = agent_socket {
		agent_socket.cleanup();
	}
}

fn validate_jail_agent_socket_path(sock_path: &Path) -> Result<()> {
	let root = Path::new("/run/vmon");
	let escapes = sock_path
		.components()
		.any(|component| matches!(component, std::path::Component::ParentDir));
	if !sock_path.is_absolute() || escapes || sock_path == root || !sock_path.starts_with(root) {
		return Err(err(format!(
			"--agent-sock under --jail must be inside /run/vmon (got {})",
			sock_path.display()
		)));
	}
	Ok(())
}

fn prepare_agent_socket_path(sock_path: &Path, launch_uid: u32) -> Result<()> {
	if sock_path.as_os_str().is_empty() {
		return Err(err("agent socket path must not be empty"));
	}
	let parent = agent_socket_parent(sock_path);
	match fs::symlink_metadata(parent) {
		Ok(meta) => {
			if meta.file_type().is_symlink() || !meta.is_dir() {
				return Err(err(format!(
					"agent socket parent {} is not a directory",
					parent.display()
				)));
			}
			verify_private_agent_socket_parent(parent, &meta, launch_uid)?;
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			fs::create_dir_all(parent).map_err(|e| {
				err(format!("creating agent socket parent {} failed: {e}", parent.display()))
			})?;
			fs::set_permissions(parent, fs::Permissions::from_mode(0o700)).map_err(|e| {
				err(format!("chmoding agent socket parent {} to 700 failed: {e}", parent.display()))
			})?;
		},
		Err(e) => {
			return Err(err(format!("checking agent socket parent {} failed: {e}", parent.display())));
		},
	}
	remove_stale_agent_socket(sock_path)
}

fn agent_socket_parent(sock_path: &Path) -> &Path {
	sock_path.parent().unwrap_or_else(|| Path::new("."))
}
fn verify_private_agent_socket_parent(
	parent: &Path,
	meta: &fs::Metadata,
	launch_uid: u32,
) -> Result<()> {
	let mode = meta.permissions().mode() & 0o777;
	if mode & 0o077 != 0 {
		return Err(err(format!(
			"agent socket parent {} must not be group/world accessible (mode {mode:o})",
			parent.display()
		)));
	}
	let uid = meta.uid();
	if uid != 0 && uid != launch_uid {
		return Err(err(format!(
			"agent socket parent {} is owned by uid {uid}, expected root or launch uid {launch_uid}",
			parent.display()
		)));
	}
	Ok(())
}

fn remove_stale_agent_socket(sock_path: &Path) -> Result<()> {
	let meta = match fs::symlink_metadata(sock_path) {
		Ok(meta) => meta,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(e) => {
			return Err(err(format!("checking agent socket {} failed: {e}", sock_path.display())));
		},
	};
	if !meta.file_type().is_socket() {
		return Err(err(format!(
			"refusing to replace non-socket agent path {}",
			sock_path.display()
		)));
	}
	match UnixStream::connect(sock_path) {
		Ok(_) => Err(err(format!("agent socket {} is already active", sock_path.display()))),
		Err(e) if matches!(e.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound) => {
			fs::remove_file(sock_path).map_err(|e| {
				err(format!("removing stale agent socket {} failed: {e}", sock_path.display()))
			})
		},
		Err(e) => {
			Err(err(format!("probing existing agent socket {} failed: {e}", sock_path.display())))
		},
	}
}

fn cleanup_agent_socket_path(sock_path: &Path) {
	let Ok(meta) = fs::symlink_metadata(sock_path) else {
		return;
	};
	if meta.file_type().is_socket() {
		let _ = fs::remove_file(sock_path);
	}
}

fn agent_bridge_loop(
	socket: AgentSocket,
	console_input: Arc<Mutex<VecDeque<u8>>>,
	input_evt: EventFd,
	output: Arc<Mutex<Vec<u8>>>,
	output_evt: EventFd,
	drain_resume_evt: EventFd,
	gate: Arc<PauseGate>,
	boot_exec: Option<Vec<u8>>,
	agent_ready: bool,
) -> io::Result<()> {
	socket.listener.set_nonblocking(true)?;
	let mut client: Option<UnixStream> = None;
	let mut input_buf = vec![0u8; 64 * 1024];
	let mut boot_exec = boot_exec;
	let mut scan = SyncScan::new(agent_ready);

	loop {
		if gate.state() == RunState::Stopping {
			return Ok(());
		}
		// Deliver the deferred --agent-exec hand-off once the guest agent has
		// announced readiness on the channel (its startup SYNC marker). Checked
		// at the loop top so the `continue` paths cannot starve it.
		if scan.seen
			&& let Some(frames) = boot_exec.take()
		{
			console_input.lock().extend(frames.iter().copied());
			input_evt.write(1)?;
			info!("guest agent ready; boot exec dispatched");
		}
		if client.is_none() {
			discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
		}
		let had_client = client.is_some();

		let mut pollfds = Vec::with_capacity(3);
		let listener_index = if client.is_none() && scan.seen {
			let index = pollfds.len();
			pollfds.push(libc::pollfd {
				fd:      socket.listener.as_raw_fd(),
				events:  libc::POLLIN,
				revents: 0,
			});
			Some(index)
		} else {
			None
		};
		let output_index = pollfds.len();
		pollfds.push(libc::pollfd {
			fd:      output_evt.as_raw_fd(),
			events:  libc::POLLIN,
			revents: 0,
		});
		let client_index = if let Some(stream) = client.as_ref() {
			let mut events = libc::POLLIN;
			let output_pending = !output.lock().is_empty();
			if output_pending {
				events |= libc::POLLOUT;
			}
			let index = pollfds.len();
			pollfds.push(libc::pollfd { fd: stream.as_raw_fd(), events, revents: 0 });
			Some(index)
		} else {
			None
		};

		// SAFETY: `pollfds` points to `pollfds.len()` initialized entries for
		// the duration of the call, and `poll` does not retain the pointer.
		let rc = unsafe {
			libc::poll(
				pollfds.as_mut_ptr(),
				pollfds.len() as libc::nfds_t,
				poll_timeout_ms(Duration::from_millis(200)),
			)
		};
		if rc == 0 {
			if client.is_none() {
				discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
			}
			continue;
		}
		if rc < 0 {
			let e = io::Error::last_os_error();
			if e.kind() == io::ErrorKind::Interrupted {
				continue;
			}
			return Err(e);
		}

		let listener_revents = listener_index.map_or(0, |index| pollfds[index].revents);
		let output_revents = pollfds[output_index].revents;
		let client_revents = client_index.map_or(0, |index| pollfds[index].revents);
		let mut output_ready = false;
		if output_revents & libc::POLLIN != 0 {
			output_evt.read()?;
			if had_client {
				output_ready = true;
			} else {
				discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
			}
		}
		if listener_revents & libc::POLLIN != 0 && client.is_none() {
			discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
			if let Some(stream) = accept_agent_client(&socket.listener, socket.launch_uid)? {
				client = Some(stream);
			}
		}

		let mut close_client = false;
		if let Some(stream) = client.as_mut() {
			if client_revents & libc::POLLIN != 0 {
				match read_agent_socket_to_console(stream, &console_input, &input_evt, &mut input_buf) {
					Ok(true) => {},
					Ok(false) | Err(_) => close_client = true,
				}
			}
			if client_revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
				close_client = true;
			}
		}

		if close_client {
			client = None;
			crate::metrics::record_agent_bridge_disconnect();
		} else if let Some(stream) = client.as_mut()
			&& (output_ready || client_revents & libc::POLLOUT != 0)
			&& drain_guest_output_to_socket(stream, &output, &drain_resume_evt, &mut scan).is_err()
		{
			client = None;
			crate::metrics::record_agent_bridge_disconnect();
			discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
		}

		if client.is_none() {
			discard_guest_output(&output, &drain_resume_evt, &mut scan)?;
		}
	}
}

fn accept_agent_client(listener: &UnixListener, launch_uid: u32) -> io::Result<Option<UnixStream>> {
	match listener.accept() {
		Ok((stream, _)) => {
			if verify_agent_peer_credentials(&stream, launch_uid).is_err() {
				return Ok(None);
			}
			stream.set_nonblocking(true)?;
			Ok(Some(stream))
		},
		Err(e) if e.kind() == io::ErrorKind::Interrupted => Ok(None),
		Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
		Err(e) => Err(e),
	}
}

#[cfg(target_os = "linux")]
fn verify_agent_peer_credentials(stream: &UnixStream, launch_uid: u32) -> io::Result<()> {
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
	if cred.uid == 0 || cred.uid == launch_uid {
		return Ok(());
	}
	Err(io::Error::new(
		io::ErrorKind::PermissionDenied,
		format!("agent socket peer uid {} is not root or launch uid {launch_uid}", cred.uid),
	))
}

#[cfg(not(target_os = "linux"))]
#[allow(clippy::unnecessary_wraps, reason = "uniform interface across platforms")]
const fn verify_agent_peer_credentials(_: &UnixStream, _: u32) -> io::Result<()> {
	Ok(())
}

fn read_agent_socket_to_console(
	stream: &mut UnixStream,
	console_input: &Arc<Mutex<VecDeque<u8>>>,
	input_evt: &EventFd,
	buf: &mut [u8],
) -> io::Result<bool> {
	let mut appended = false;
	loop {
		match stream.read(buf) {
			Ok(0) => return Ok(false),
			Ok(n) => {
				console_input.lock().extend(buf[..n].iter().copied());
				appended = true;
				if n < buf.len() {
					break;
				}
			},
			Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
			Err(e) => return Err(e),
		}
	}
	if appended {
		input_evt.write(1)?;
	}
	Ok(true)
}

fn drain_guest_output_to_socket(
	stream: &mut UnixStream,
	output: &Arc<Mutex<Vec<u8>>>,
	drain_resume_evt: &EventFd,
	scan: &mut SyncScan,
) -> io::Result<()> {
	loop {
		let mut out = output.lock();
		if out.is_empty() {
			return Ok(());
		}
		match stream.write(&out[..]) {
			Ok(0) => {
				return Err(io::Error::new(
					io::ErrorKind::WriteZero,
					"agent socket accepted zero-byte write",
				));
			},
			Ok(n) => {
				scan.feed(&out[..n]);
				let before = out.len();
				out.drain(..n);
				signal_drain_resume_if_needed(before, out.len(), drain_resume_evt)?;
			},
			Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
			Err(e) => return Err(e),
		}
	}
}

fn discard_guest_output(
	output: &Arc<Mutex<Vec<u8>>>,
	drain_resume_evt: &EventFd,
	scan: &mut SyncScan,
) -> io::Result<()> {
	let mut out = output.lock();
	let before = out.len();
	if before == 0 {
		return Ok(());
	}
	scan.feed(&out);
	out.clear();
	signal_drain_resume_if_needed(before, 0, drain_resume_evt)
}

fn signal_drain_resume_if_needed(
	before: usize,
	after: usize,
	drain_resume_evt: &EventFd,
) -> io::Result<()> {
	if before >= OUTPUT_CAP && after < OUTPUT_CAP {
		drain_resume_evt.write(1)?;
	}
	Ok(())
}

#[cfg(target_os = "linux")]
#[allow(clippy::unnecessary_wraps, reason = "returns Result for API symmetry")]
fn build_sandbox_paths(config: &Config) -> Result<crate::sandbox::SandboxPaths> {
	let mut paths = crate::sandbox::SandboxPaths::default();
	if let Some(path) = &config.kernel {
		paths.ro.push(path.clone());
	}
	if let Some(path) = &config.initrd {
		paths.ro.push(path.clone());
	}
	if let Some(path) = &config.firmware {
		paths.ro.push(path.clone());
	}
	if let Some(path) = &config.disk_overlay_of {
		paths.ro.push(path.clone());
	}
	if let Some(path) = &config.rootfs {
		if config.rootfs_read_only {
			paths.ro.push(path.clone());
		} else {
			paths.rw.push(path.clone());
		}
	}
	if let Some(path) = &config.rootfs_remote_cache {
		paths.rw.push(path.clone());
	}
	if let Some(path) = &config.rootfs_remote_index {
		paths.ro.push(path.clone());
	}
	if let Some(path) = &config.restore {
		paths.ro_dirs.push(path.clone());
	}
	if let Some(path) = &config.fork_from {
		paths.ro_dirs.push(path.clone());
	}
	if let Some(path) = &config.fs_dir {
		paths.ro_dirs.push(path.clone());
	}
	for m in &config.volumes {
		if m.read_only {
			paths.ro_dirs.push(m.dir.clone());
		} else {
			paths.rw_dirs.push(m.dir.clone());
		}
	}
	if let Some(path) = &config.snapshot_root {
		paths.rw_dirs.push(path.clone());
	}
	// The control/agent socket parent directories need read/write access:
	// `status.json` is written beside the control socket on exit and both
	// sockets are unlinked during cleanup -- all after Landlock is installed.
	// The sockets are bound before the filter phase, so these dirs already
	// exist when the rules are added.
	for sock in [config.api_sock.as_ref(), config.agent_sock.as_ref()]
		.into_iter()
		.flatten()
	{
		if let Some(parent) = sock.parent()
			&& !parent.as_os_str().is_empty()
		{
			paths.rw_dirs.push(parent.to_path_buf());
		}
	}
	for m in &config.remote_fs {
		if let Some(parent) = m.sock.parent()
			&& !parent.as_os_str().is_empty()
		{
			paths.rw_dirs.push(parent.to_path_buf());
		}
	}
	Ok(paths)
}
/// Opens one block backend from the current launch configuration.
///
/// Both cold build and snapshot restore must use this function: a remote
/// overlay is sparse by design and opening it as a standalone local file would
/// silently serve holes as zeroes instead of consulting the remote base.
fn open_configured_block(config: &Config, path: &Path, read_only: bool) -> Result<Block> {
	if let Some(url) = &config.rootfs_remote_url {
		let cache = config
			.rootfs_remote_cache
			.as_deref()
			.ok_or_else(|| err("--rootfs-remote-url requires --rootfs-remote-cache"))?;
		let index = config
			.rootfs_remote_index
			.as_deref()
			.ok_or_else(|| err("--rootfs-remote-url requires --rootfs-remote-index"))?;
		let size = config
			.rootfs_remote_size
			.ok_or_else(|| err("--rootfs-remote-url requires --rootfs-remote-size"))?;
		let overlay = config
			.disk_overlay_of
			.as_ref()
			.map(|base| crate::virtio::block::create_cow_overlay(base, path))
			.transpose()?;
		return Block::new_remote(
			url.clone(),
			cache,
			index,
			path,
			size,
			config.rootfs_remote_bearer.clone(),
			overlay,
		);
	}
	if let Some(base) = &config.disk_overlay_of {
		let overlay = crate::virtio::block::create_cow_overlay(base, path)?;
		Block::from_file(overlay, read_only)
	} else {
		Block::new(path, read_only)
	}
}

fn machine_info(vmm: &Vmm) -> MachineInfo {
	let devices = {
		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		{
			let mut devices = vmm
				.devices
				.iter()
				.map(|device| device_kind_name(device.kind))
				.collect::<Vec<_>>();
			devices.extend(std::iter::repeat_n("nvidia-vgpu", vmm.vfio_devices.len()));
			devices
		}
		#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
		{
			vmm.devices
				.iter()
				.map(|device| device_kind_name(device.kind))
				.collect::<Vec<_>>()
		}
	};
	MachineInfo {
		cpus: vmm.cpus,
		mem_mib: vmm.mem_mib,
		state: vmm.gate.state(),
		devices,
		deadline_unix: earliest_deadline(vmm.deadline, vmm.owner_deadline).map(deadline_to_unix),
		uptime_ms: vmm.started_at.elapsed().as_millis() as u64,
	}
}

fn snapshot_name_to_path(
	root: Option<&Path>,
	name: &str,
) -> std::result::Result<PathBuf, control::ApiError> {
	if !snapshot::is_safe_snapshot_name(name) {
		return Err(control::ApiError::path_denied(format!(
			"snapshot name {name} is not a safe basename"
		)));
	}
	let root = root
		.ok_or_else(|| control::ApiError::snapshots_disabled("snapshot_root is not configured"))?;
	Ok(root.join(name))
}

const fn run_state_name(state: RunState) -> &'static str {
	match state {
		RunState::Running => "running",
		RunState::Paused | RunState::Stopping => "paused",
	}
}

/// Current wall-clock time as unix seconds (saturating to 0 before the epoch).
fn unix_now_secs() -> i64 {
	std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |d| d.as_secs() as i64)
}

/// Convert a monotonic deadline `Instant` to wall-clock unix seconds.
fn deadline_to_unix(deadline: Instant) -> i64 {
	let now = Instant::now();
	let now_unix = unix_now_secs();
	if deadline >= now {
		let delta = i64::try_from(deadline.duration_since(now).as_secs()).unwrap_or(i64::MAX);
		now_unix.saturating_add(delta)
	} else {
		let delta = i64::try_from(now.duration_since(deadline).as_secs()).unwrap_or(i64::MAX);
		now_unix.saturating_sub(delta)
	}
}

fn checked_deadline(base: Instant, secs: u64, label: &str) -> Result<Instant> {
	base
		.checked_add(Duration::from_secs(secs))
		.ok_or_else(|| err(format!("{label} is too far in the future")))
}

fn deadline_or_now(base: Instant, secs: u64, label: &str) -> Instant {
	match checked_deadline(base, secs, label) {
		Ok(deadline) => deadline,
		Err(e) => {
			warn!("{e}; expiring immediately");
			base
		},
	}
}

/// Return the deadline that stops the VM first.
fn earliest_deadline(user: Option<Instant>, owner: Option<Instant>) -> Option<Instant> {
	match (user, owner) {
		(Some(user), Some(owner)) => Some(user.min(owner)),
		(Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
		(None, None) => None,
	}
}

#[cfg(test)]
mod deadline_tests {
	use super::*;

	#[test]
	fn earliest_deadline_preserves_absent_owner_and_prefers_owner_when_earlier() {
		let now = Instant::now();
		let user = now + Duration::from_secs(10);
		let owner = now + Duration::from_secs(5);
		assert_eq!(earliest_deadline(Some(user), None), Some(user));
		assert_eq!(earliest_deadline(Some(user), Some(owner)), Some(owner));
		assert_eq!(earliest_deadline(None, Some(owner)), Some(owner));
	}
}

/// Record the VMM exit reason; the first writer wins so the root cause is not
/// clobbered when sibling threads observe `Stopping` and halt afterwards.
fn set_exit_reason(cell: &AtomicU8, exit: VmmExit) {
	let _ = cell.compare_exchange(0, exit.as_u8(), Ordering::SeqCst, Ordering::SeqCst);
}

/// Persist the VMM-level final status next to the control socket so the
/// supervisor can read the lifecycle reason and return code after exit.
fn status_body(exit: VmmExit) -> String {
	json!({
		 "state": "finished",
		 "reason": exit.reason(),
		 "vmm_returncode": exit.returncode(),
		 "finished_unix": unix_now_secs(),
	})
	.to_string()
}

fn write_status_json(api_sock: &Path, exit: VmmExit) {
	let Some(dir) = api_sock.parent() else {
		return;
	};
	let path = dir.join("status.json");
	if let Err(e) = fs::write(&path, status_body(exit)) {
		warn!("failed to write {}: {e}", path.display());
	}
}

/// Open (create + truncate) `status.json` beside the control socket so it can
/// be written at exit through a held fd. Opened *before* the privilege drop and
/// Landlock: DAC is checked at open (so the dropped sandbox uid can still
/// write) and Landlock path rules do not restrict writes to already-open fds.
/// Best-effort -- a failure falls back to the path-based writer.
fn open_status_file(api_sock: Option<&Path>) -> Option<std::fs::File> {
	let dir = api_sock?.parent()?;
	let path = dir.join("status.json");
	match std::fs::OpenOptions::new()
		.write(true)
		.create(true)
		.truncate(true)
		.open(&path)
	{
		Ok(file) => Some(file),
		Err(e) => {
			warn!("failed to pre-open {}: {e}", path.display());
			None
		},
	}
}

fn write_final_status(file: Option<&mut std::fs::File>, api_sock: Option<&Path>, exit: VmmExit) {
	if let Some(file) = file {
		write_status_to(file, exit);
	} else if let Some(api_sock) = api_sock {
		write_status_json(api_sock, exit);
	}
}

fn write_status_to(file: &mut std::fs::File, exit: VmmExit) {
	use std::io::{Seek, SeekFrom, Write};
	let body = status_body(exit);
	if let Err(e) = file
		.set_len(0)
		.and_then(|()| file.seek(SeekFrom::Start(0)).map(|_| ()))
	{
		warn!("failed to reset status.json: {e}");
		return;
	}
	if let Err(e) = file.write_all(body.as_bytes()) {
		warn!("failed to write status.json: {e}");
		return;
	}
	if let Err(e) = file.flush() {
		warn!("failed to flush status.json: {e}");
	}
}

fn parse_snapshot_boot_mode(value: &str) -> Result<BootMode> {
	match value {
		"direct" => Ok(BootMode::Direct),
		"uefi" => Ok(BootMode::Uefi),
		other => Err(err(format!("snapshot boot_mode {other} is not supported"))),
	}
}

const fn device_kind_name(kind: DeviceKind) -> &'static str {
	match kind {
		DeviceKind::Block => "block",
		DeviceKind::Net => "net",
		DeviceKind::Console => "console",
		DeviceKind::Fs => "fs",
		DeviceKind::Rng => "rng",
		DeviceKind::RemoteFs => "remote_fs",
	}
}

const fn control_plane_owner(launch_uid: u32) -> ControlPlaneOwner {
	ControlPlaneOwner {
		// None means: bind as the current effective uid. In a jail child this is
		// root before the drop, so sockets remain control-plane owned instead
		// of sandbox-uid owned.
		socket_owner: None,
		launch_uid,
	}
}

fn cleanup_control_server(control_server: Option<&control::ControlServer>) {
	if let Some(control_server) = control_server {
		control_server.cleanup();
	}
}

/// Parent side of `--fork-from --count N`: spawn N child `vmon` processes,
/// each forking one clone with a unique tap, MAC, and `CoW` disk overlay.
/// `--count 1` is forced on the children so they take the single-fork path (no
/// recursion).
fn spawn_fork_children(dir: &Path, config: &Config) -> Result<()> {
	use std::process::{Child, Command};

	fn terminate_fork_children(children: &mut Vec<(u32, Child)>) {
		for (i, child) in children.iter_mut() {
			if let Err(e) = child.kill() {
				warn!("failed to stop fork child {i} after sibling failure: {e}");
			}
		}
		while let Some((i, mut child)) = children.pop() {
			if let Err(e) = child.wait() {
				warn!("failed to reap fork child {i} after sibling failure: {e}");
			}
		}
	}

	fn wait_for_child(children: &mut Vec<(u32, Child)>) -> Result<()> {
		let (i, mut child) = children.remove(0);
		let status = match child.wait() {
			Ok(status) => status,
			Err(e) => {
				terminate_fork_children(children);
				return Err(err(format!("waiting for fork child {i}: {e}")));
			},
		};
		if !status.success() {
			terminate_fork_children(children);
			return Err(err(format!("fork child {i} exited with {status}")));
		}
		Ok(())
	}

	let count = config.count;
	if count == 0 || count > MAX_COUNT {
		return Err(err(format!("--count must be between 1 and {MAX_COUNT} (got {count})")));
	}

	let exe = std::env::current_exe()?;
	// Never trust a disk path embedded in snapshot state. Fork children receive
	// only an operator-supplied base from this launch.
	let snap = snapshot::read_state(dir)?;
	if snap.delta.is_some() {
		return Err(err("cannot fork from a delta snapshot; restore it instead"));
	}
	let base_disk: Option<String> = config
		.disk_overlay_of
		.as_ref()
		.or(config.rootfs.as_ref())
		.map(|path| path.to_string_lossy().into_owned());
	if base_disk.is_none()
		&& snap
			.devices
			.iter()
			.any(|device| matches!(device.backend, BackendHint::Block { .. }))
	{
		return Err(err(
			"forking a snapshot with a block device requires --disk-overlay-of or --rootfs",
		));
	}
	let snap_has_net = snap
		.devices
		.iter()
		.any(|d| matches!(d.backend, BackendHint::Net { .. } | BackendHint::UserNet { .. }));
	let fork_user_net = config.user_net
		|| (config.tap.is_none()
			&& snap
				.devices
				.iter()
				.any(|d| matches!(d.backend, BackendHint::UserNet { .. })));

	let max_in_flight = MAX_FORK_CHILD_CONCURRENCY.min(count as usize);
	let mut children = Vec::with_capacity(max_in_flight);
	for i in 0..count {
		if children.len() == max_in_flight {
			wait_for_child(&mut children)?;
		}

		let mut cmd = Command::new(&exe);
		// The binary is the multiplexed `vmon`; fork children re-enter the
		// per-VM monitor through its `vmm` subcommand.
		cmd.arg("vmm");
		cmd.arg("--fork-from").arg(dir).arg("--count").arg("1");
		// Only attach networking to a clone when the snapshot actually has a net
		// device (or the operator forced user-net); a no-net snapshot's clone must
		// not be handed a TAP it never had — a non-root host cannot create one.
		if snap_has_net || config.user_net {
			if fork_user_net {
				cmd.arg("--net").arg("user");
			} else {
				cmd.arg("--tap").arg(format!("tap{i}"));
			}
			cmd.arg("--mac")
				.arg(format!("02:00:00:00:00:{:02x}", i + 1));
		}
		cmd.arg("--transport").arg(config.transport.as_str());
		if let Some(base) = &base_disk {
			cmd.arg("--disk-overlay-of")
				.arg(base)
				.arg("--rootfs")
				.arg(format!("{base}.fork{i}"));
		}
		if let (Some(tag), Some(fs_dir)) = (&config.fs_tag, &config.fs_dir) {
			cmd.arg("--fs-tag").arg(tag).arg("--fs-dir").arg(fs_dir);
		}
		for m in &config.volumes {
			cmd.arg("--volume").arg(format!(
				"{}:{}{}",
				m.tag,
				m.dir.display(),
				if m.read_only { ":ro" } else { "" }
			));
		}
		if let Some(root) = &config.snapshot_root {
			cmd.arg("--snapshot-root").arg(root);
		}
		if config.jail {
			let base_id = config
				.id
				.as_deref()
				.ok_or_else(|| err("--jail with --fork-from --count requires --id"))?;
			let child_id = format!("{base_id}-{i}");
			cmd.arg("--jail").arg("--id").arg(&child_id);
			if let Some(root) = &config.jail_root {
				cmd.arg("--jail-root").arg(unique_child_jail_root(root, i));
			}
			if let Some(cpu_max) = &config.cgroup_cpu_max {
				cmd.arg("--cgroup-cpu-max").arg(cpu_max);
			}
			if let Some(mem_max) = &config.cgroup_mem_max {
				cmd.arg("--cgroup-mem-max").arg(mem_max);
			}
			if let Some(pids_max) = config.cgroup_pids_max {
				cmd.arg("--cgroup-pids-max").arg(pids_max.to_string());
			}
			cmd.arg("--cgroup-mode")
				.arg(cgroup_mode_arg(config.cgroup_mode));
			cmd.arg("--seccomp-action")
				.arg(seccomp_action_arg(config.seccomp_action));
			if let Some(netns) = &config.netns {
				cmd.arg("--netns").arg(netns);
			}
		}
		if config.no_sandbox {
			cmd.arg("--no-sandbox");
		}
		if let Some(uid) = config.sandbox_uid {
			cmd.arg("--sandbox-uid").arg(uid.to_string());
		}
		if let Some(gid) = config.sandbox_gid {
			cmd.arg("--sandbox-gid").arg(gid.to_string());
		}
		if config.ksm {
			cmd.arg("--ksm");
		}
		if let Some(sock) = &config.api_sock {
			if config.jail {
				cmd.arg("--api-sock").arg(sock);
			} else {
				cmd.arg("--api-sock").arg(format!("{}.{i}", sock.display()));
			}
		}
		if let Some(sock) = &config.agent_sock {
			if config.jail {
				cmd.arg("--agent-sock").arg(sock);
			} else {
				cmd.arg("--agent-sock")
					.arg(format!("{}.{i}", sock.display()));
			}
		}
		info!("fork child {i}");
		let child = match cmd.spawn() {
			Ok(child) => child,
			Err(e) => {
				terminate_fork_children(&mut children);
				return Err(err(format!("spawning fork child {i}: {e}")));
			},
		};
		children.push((i, child));
	}
	while !children.is_empty() {
		wait_for_child(&mut children)?;
	}
	Ok(())
}

fn unique_child_jail_root(root: &Path, i: u32) -> PathBuf {
	match (root.parent(), root.file_name()) {
		(Some(parent), Some(name)) => parent.join(format!("{}-{i}", name.to_string_lossy())),
		_ => PathBuf::from(format!("{}-{i}", root.display())),
	}
}

const fn cgroup_mode_arg(mode: crate::config::CgroupMode) -> &'static str {
	match mode {
		crate::config::CgroupMode::V2 => "v2",
		crate::config::CgroupMode::Off => "off",
	}
}

const fn seccomp_action_arg(action: crate::config::SeccompAction) -> &'static str {
	match action {
		crate::config::SeccompAction::Kill => "kill",
		crate::config::SeccompAction::Errno => "errno",
		crate::config::SeccompAction::Log => "log",
	}
}

impl Vmm {
	// -- construction --------------------------------------------------------

	/// Fresh boot: create the VM, load the kernel, build the device model, and
	/// configure vCPU registers — everything up to (not including) thread spawn.
	fn build(config: Config, preopened_gpu: PreopenedGpuResources) -> Result<Self> {
		#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
		let () = preopened_gpu;
		let mem_bytes = config
			.mem_mib
			.checked_mul(1 << 20)
			.ok_or("guest memory size overflows usize")?;
		let vm = Arc::new(Vm::new(mem_bytes)?);
		if config.ksm {
			crate::memory::advise_mergeable(vm.memory());
		}

		let boot_mode = config.boot_mode;
		let firmware = config.firmware.clone();
		let loaded = match boot_mode {
			BootMode::Direct => {
				let kernel = config
					.kernel
					.as_ref()
					.ok_or_else(|| err("--kernel <path> is required for --boot-mode direct"))?;
				arch::boot::load_kernel(kernel, vm.memory())?
			},
			BootMode::Uefi => {
				let firmware_path = firmware
					.as_ref()
					.ok_or_else(|| err("--boot-mode uefi requires --firmware"))?;
				#[cfg(target_arch = "x86_64")]
				{
					arch::boot::load_uefi_firmware(firmware_path, vm.memory(), &vm.fd)?
				}
				#[cfg(target_arch = "aarch64")]
				{
					arch::boot::load_uefi_firmware(firmware_path, vm.memory())?
				}
			},
		};
		let initrd = if boot_mode == BootMode::Direct {
			load_initrd(&config, &vm, &loaded)?
		} else {
			None
		};

		#[cfg(target_os = "linux")]
		let mut vcpus = Vec::with_capacity(config.cpus as usize);
		#[cfg(target_os = "linux")]
		for id in 0..config.cpus {
			vcpus.push(vm.create_vcpu(id)?);
		}

		#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
		let gic = {
			for (id, vcpu) in vcpus.iter().enumerate() {
				if id == 0 {
					vcpu.init_boot()?;
				} else {
					vcpu.init_secondary()?;
				}
			}
			vm.create_gic(u64::from(config.cpus))?
		};
		#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
		let gic = Some(vm.create_gic(u64::from(config.cpus))?);

		#[allow(unused_mut, reason = "pio_bus is only mutated (serial) on x86_64.")]
		let mut pio_bus = Bus::new();
		let mut mmio_bus = Bus::new();
		#[cfg(target_arch = "x86_64")]
		let serial = setup_serial(&vm, &mut pio_bus, None)?;
		#[cfg(target_arch = "aarch64")]
		let serial = setup_serial(&vm, &mut mmio_bus, None)?;

		#[cfg(target_arch = "x86_64")]
		let (pci_root, mut pci_mmio) =
			if config.transport == Transport::Pci || !config.vfio_gpus.is_empty() {
				let mmio = (config.transport == Transport::Pci)
					.then(|| Arc::new(Mutex::new(PciMmioDispatcher::new(PCI_VIRTIO_MMIO_START))));
				(Some(Arc::new(Mutex::new(PciRoot::new()))), mmio)
			} else {
				(None, None)
			};
		#[cfg(target_arch = "x86_64")]
		let mut pci_mmio_base = PCI_VIRTIO_MMIO_START;

		let mut cmdline = config
			.cmdline
			.clone()
			.unwrap_or_else(|| DEFAULT_CMDLINE.to_string());
		let mut devices: Vec<DeviceHandle> = Vec::new();
		let mut workers: Vec<DeviceWorker> = Vec::new();
		let mut virtio_infos: Vec<(u64, u32)> = Vec::new();
		let mut gsi = IRQ_BASE;
		let mut mmio_base = MMIO_MEM_START;
		#[cfg(target_arch = "x86_64")]
		let mut pci_slot = 1u8;
		#[cfg(target_arch = "x86_64")]
		let mut pci_bar_base = PCI_VIRTIO_MMIO_START;
		let mut console_input = None;
		let mut console_output = None;

		if let Some(path) = &config.rootfs {
			let block = open_configured_block(&config, path, config.rootfs_read_only)?;
			let device: Arc<Mutex<dyn VirtioDevice>> = Arc::new(Mutex::new(block));
			let backend = BackendHint::Block {
				path:      path.to_string_lossy().into_owned(),
				read_only: config.rootfs_read_only,
			};
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Block,
						backend,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Block,
							backend,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			if !cmdline_has_key(&cmdline, "root") {
				cmdline.push_str(if config.rootfs_read_only {
					" root=/dev/vda ro"
				} else {
					" root=/dev/vda rw"
				});
			}
			gsi += 1;
		}

		if config.console_agent {
			let console = crate::virtio::console::Console::new()?;
			console_input = Some(console.input_handle());
			console_output = Some(console.output_handle());
			let device: Arc<Mutex<dyn VirtioDevice>> = Arc::new(Mutex::new(console));
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Console,
						BackendHint::Console,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Console,
							BackendHint::Console,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			gsi += 1;
		}

		if config.rng {
			let rng = crate::virtio::rng::Rng::new()?;
			let device: Arc<Mutex<dyn VirtioDevice>> = Arc::new(Mutex::new(rng));
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Rng,
						BackendHint::Rng,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Rng,
							BackendHint::Rng,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			gsi += 1;
		}

		if let (Some(tag), Some(dir)) = (&config.fs_tag, &config.fs_dir) {
			let shared_dir = std::fs::canonicalize(dir)
				.map_err(|e| err(format!("virtio-fs shared dir {}: {e}", dir.display())))?;
			let fs_dev = Arc::new(Mutex::new(Fs::new(tag.clone(), shared_dir.clone(), true)?));
			let fs_handle = FsSave::Host(fs_dev.clone());
			let device: Arc<Mutex<dyn VirtioDevice>> = fs_dev;
			let backend = BackendHint::Fs {
				tag:        tag.clone(),
				shared_dir: shared_dir.to_string_lossy().into_owned(),
				read_only:  true,
			};
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Fs,
						backend,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Fs,
							backend,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			if let Some(device) = devices.last_mut() {
				device.fs = Some(fs_handle);
			}
			gsi += 1;
		}

		for m in &config.volumes {
			let shared_dir = std::fs::canonicalize(&m.dir)
				.map_err(|e| err(format!("virtio-fs volume dir {}: {e}", m.dir.display())))?;
			let fs_dev =
				Arc::new(Mutex::new(Fs::new(m.tag.clone(), shared_dir.clone(), m.read_only)?));
			let fs_handle = FsSave::Host(fs_dev.clone());
			let device: Arc<Mutex<dyn VirtioDevice>> = fs_dev;
			let backend = BackendHint::Fs {
				tag:        m.tag.clone(),
				shared_dir: shared_dir.to_string_lossy().into_owned(),
				read_only:  m.read_only,
			};
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Fs,
						backend,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Fs,
							backend,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			if let Some(device) = devices.last_mut() {
				device.fs = Some(fs_handle);
			}
			gsi += 1;
		}

		for m in &config.remote_fs {
			let remote_fs = Arc::new(Mutex::new(RemoteFs::new(m.tag.clone(), m.sock.clone())));
			let fs_handle = FsSave::Remote(remote_fs.clone());
			let device: Arc<Mutex<dyn VirtioDevice>> = remote_fs;
			let backend = BackendHint::RemoteFs {
				tag:  m.tag.clone(),
				sock: m.sock.to_string_lossy().into_owned(),
			};
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::RemoteFs,
						backend,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					mmio_base += MMIO_DEVICE_SIZE;
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::RemoteFs,
							backend,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
			if let Some(device) = devices.last_mut() {
				device.fs = Some(fs_handle);
			}
			gsi += 1;
		}

		if let Some(backend) = fresh_net_backend(&config) {
			let device = open_fresh_net_device(
				&backend,
				config.user_net_restricted,
				config.user_net_allow_host_port,
			)?;
			match config.transport {
				Transport::Mmio => {
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						DeviceKind::Net,
						backend,
						gsi,
						mmio_base,
						None,
						0,
						&mut workers,
						&mut devices,
					)?;
					push_virtio_cmdline(&mut cmdline, mmio_base, gsi);
					virtio_infos.push((mmio_base, gsi));
					#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
					{
						mmio_base += MMIO_DEVICE_SIZE;
					}
				},
				Transport::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							DeviceKind::Net,
							backend,
							gsi,
							pci_slot,
							pci_bar_base,
							None,
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
						pci_bar_base += PCI_VIRTIO_BAR_SIZE;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("--transport pci is rejected during config parsing");
				},
			}
		}

		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		let (vfio_devices, pci_high_mmio) = if let Some(preopened) = preopened_gpu {
			if pci_mmio.is_none() {
				pci_bar_base = mmio_base;
				pci_mmio_base = mmio_base;
				pci_mmio = Some(Arc::new(Mutex::new(PciMmioDispatcher::new(pci_mmio_base))));
			}
			let mut resources = VfioPciResources::new(pci_bar_base, PCI_ECAM_BASE)?;
			let high_base = PCI_HIGH_MMIO_START;
			let high_size = PCI_HIGH_MMIO_SIZE;
			let high_mmio = Arc::new(Mutex::new(PciMmioDispatcher::new(high_base)));
			let assigned = attach_gpu_devices(preopened, vm.clone(), &mut resources)?;
			for gpu in &assigned {
				if pci_slot >= 32 {
					return Err(err("NVIDIA vGPU exhausted guest PCI slots"));
				}
				let function: Arc<dyn PciFunction> = gpu.clone();
				pci_root
					.as_ref()
					.expect("VFIO requires PCI root")
					.lock()
					.register(pci_slot, 0, function.clone())?;
				pci_mmio
					.as_ref()
					.expect("VFIO requires PCI MMIO")
					.lock()
					.register(function.clone());
				high_mmio.lock().register(function);
				pci_slot += 1;
			}
			(assigned, Some((high_mmio, high_base, high_size)))
		} else {
			(Vec::new(), None)
		};

		#[cfg(target_arch = "x86_64")]
		if let (Some(root), Some(mmio)) = (&pci_root, &pci_mmio) {
			pio_bus.register(PCI_CONFIG_IO_BASE, PCI_CONFIG_IO_SIZE, root.clone());
			mmio_bus.register(
				PCI_ECAM_BASE,
				PCI_ECAM_SIZE,
				Arc::new(Mutex::new(PciEcam::new(root.clone()))),
			);
			let pci_mmio_size = PCI_ECAM_BASE
				.checked_sub(pci_mmio_base)
				.ok_or_else(|| err("PCI MMIO aperture overlaps ECAM"))?;
			mmio_bus.register(pci_mmio_base, pci_mmio_size, mmio.clone());
			#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
			if let Some((high_mmio, base, size)) = pci_high_mmio {
				mmio_bus.register(base, size, high_mmio);
			}
		}

		#[cfg(target_arch = "x86_64")]
		if let Some(uuid) = config.vm_uuid {
			arch::smbios::setup_system_uuid(vm.memory(), uuid)?;
		}

		#[cfg(target_arch = "x86_64")]
		configure_and_boot(&vm, &vcpus, &loaded, boot_mode, &cmdline, initrd, &virtio_infos)?;
		#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
		configure_and_boot(&vm, &vcpus, &loaded, &cmdline, initrd, &virtio_infos, &gic)?;
		#[cfg(target_os = "macos")]
		let macos_startup = MacosStartup::Boot { entry: loaded.entry, initrd, virtio_infos };

		let (txs, rxs) = vcpu_channels(config.cpus);
		let gate = if config.start_paused {
			PauseGate::new_paused(u32::from(config.cpus))
		} else {
			PauseGate::new(u32::from(config.cpus))
		};
		Ok(Self {
			gate,
			cpus: config.cpus,
			mem_mib: config.mem_mib,
			cmdline,
			boot_mode,
			firmware,
			#[cfg(target_arch = "x86_64")]
			firmware_memory: loaded.firmware_memory,
			vm,
			#[cfg(target_os = "linux")]
			vcpus,
			#[cfg(target_os = "macos")]
			macos_startup,
			vcpu_cmd_txs: txs,
			vcpu_cmd_rxs: rxs,
			vcpu_handles: Vec::new(),
			pager: None,
			pager_handler: None,
			pio_bus: Arc::new(pio_bus),
			mmio_bus: Arc::new(mmio_bus),
			serial,
			devices,
			device_workers: workers,
			device_loop: None,
			worker_kill_evt: EventFd::new(EFD_NONBLOCK)?,
			#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
			vfio_devices,
			console_input,
			console_output,
			timeout_secs: config.timeout_secs,
			owner_lease_secs: config.owner_lease_secs,
			started_at: Instant::now(),
			dirty_base: Mutex::new(None),
			deadline: None,
			owner_deadline: None,
			api_sock: config.api_sock,
			exit_reason: Arc::new(AtomicU8::new(0)),
			#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
			gic,
			#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
			gic,
		})
	}

	/// Restore a previously snapshotted VM (no kernel boot).
	///
	/// A delta snapshot (only produced by live migration) restores lazily: the
	/// base memory file is mapped `MAP_PRIVATE` and the delta pages fault
	/// private copies, so restore cost scales with the delta instead of guest
	/// RAM. Lazy remote paging and pager eviction manage RAM themselves and
	/// keep the eager path.
	fn restore(dir: &Path, config: &Config) -> Result<Self> {
		let image = if config.remote_page_url.is_some() {
			snapshot::read_snapshot_metadata(dir)?
		} else {
			snapshot::read_snapshot(dir)?
		};
		let lazy = image.snapshot().delta.is_some()
			&& config.remote_page_url.is_none()
			&& config.mem_target_mib.is_none();
		let vm = if lazy {
			Arc::new(Vm::with_memory(snapshot::map_memory_chain_private(dir, &image)?)?)
		} else {
			let mem_bytes = image
				.snapshot()
				.mem_mib
				.checked_mul(1 << 20)
				.ok_or("snapshot memory size overflows usize")?;
			let vm = Arc::new(Vm::new(mem_bytes)?);
			if config.remote_page_url.is_none() {
				snapshot::load_memory_chain(dir, &image, vm.memory())?;
			}
			vm
		};
		if config.ksm {
			crate::memory::advise_mergeable(vm.memory());
		}
		Self::reconstruct(vm, image.into_snapshot(), config)
	}

	/// Fork (`CoW`) from a template snapshot: child RAM is a `MAP_PRIVATE`
	/// mapping of the manifest-selected memory file, so clean pages are shared
	/// via the host page cache across children while writes fault to private
	/// copies.
	fn fork_from(dir: &Path, config: &Config) -> Result<Self> {
		let image = snapshot::read_snapshot(dir)?;
		snapshot::validate_snapshot(&image)?;
		if image.snapshot().delta.is_some() {
			return Err(err("cannot fork from a delta snapshot; restore it instead"));
		}
		let regions: Vec<(u64, u64, u64)> = image
			.snapshot()
			.mem_regions
			.iter()
			.map(|r| (r.gpa, r.len, r.file_offset))
			.collect();
		let memory = crate::memory::create_guest_memory_private(image.memory_file(), &regions)?;
		let vm = Arc::new(Vm::with_memory(memory)?);
		if config.ksm {
			crate::memory::advise_mergeable(vm.memory());
		}
		Self::reconstruct(vm, image.into_snapshot(), config)
	}

	/// Reconstruct VM state (machine + devices + vCPU registers) from a decoded
	/// snapshot onto an already-built `vm`. Shared by restore and fork.
	fn reconstruct(vm: Arc<Vm>, snap: Snapshot, config: &Config) -> Result<Self> {
		let boot_mode = parse_snapshot_boot_mode(&snap.boot_mode)?;
		let firmware = snap.firmware.as_deref().map(PathBuf::from);
		#[cfg(target_arch = "x86_64")]
		let firmware_memory = match boot_mode {
			BootMode::Direct => None,
			BootMode::Uefi => {
				let path = snap
					.firmware
					.as_deref()
					.ok_or_else(|| err("UEFI snapshot is missing firmware path"))?;
				Some(arch::boot::map_uefi_firmware(Path::new(path), vm.memory(), &vm.fd)?)
			},
		};
		// Machine + vCPU shells (register state applied last, pre-spawn).
		#[cfg(target_arch = "x86_64")]
		arch::state::restore_machine(&vm.fd, &snap.machine)?;

		#[cfg(target_os = "linux")]
		let mut vcpus = Vec::with_capacity(snap.cpus as usize);
		#[cfg(target_os = "linux")]
		for id in 0..snap.cpus {
			vcpus.push(vm.create_vcpu(id)?);
		}

		#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
		let gic = {
			for (id, vcpu) in vcpus.iter().enumerate() {
				if id == 0 {
					vcpu.init_boot()?;
				} else {
					vcpu.init_secondary()?;
				}
			}
			let gic = vm.create_gic(u64::from(snap.cpus))?;
			gic.restore_state(&snap.machine.gic)?;
			gic
		};
		#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
		let gic = {
			let gic = vm.create_gic(u64::from(snap.cpus))?;
			Some(gic)
		};
		#[cfg(target_os = "macos")]
		let macos_startup =
			MacosStartup::Restore { vcpus: snap.vcpus.clone(), machine: snap.machine.clone() };

		#[allow(unused_mut, reason = "pio_bus is only mutated (serial) on x86_64.")]
		let mut pio_bus = Bus::new();
		let mut mmio_bus = Bus::new();
		#[cfg(target_arch = "x86_64")]
		let serial = setup_serial(&vm, &mut pio_bus, Some(&snap.serial))?;
		#[cfg(target_arch = "aarch64")]
		let serial = setup_serial(&vm, &mut mmio_bus, Some(&snap.serial))?;

		#[cfg(target_arch = "x86_64")]
		let needs_pci = snap
			.devices
			.iter()
			.any(|d| d.transport == DeviceTransportKind::Pci);
		#[cfg(target_arch = "x86_64")]
		let (pci_root, pci_mmio) = if needs_pci {
			let root = Arc::new(Mutex::new(PciRoot::new()));
			let mmio = Arc::new(Mutex::new(PciMmioDispatcher::new(PCI_VIRTIO_MMIO_START)));
			pio_bus.register(PCI_CONFIG_IO_BASE, PCI_CONFIG_IO_SIZE, root.clone());
			mmio_bus.register(
				PCI_ECAM_BASE,
				PCI_ECAM_SIZE,
				Arc::new(Mutex::new(PciEcam::new(root.clone()))),
			);
			mmio_bus.register(PCI_VIRTIO_MMIO_START, PCI_VIRTIO_MMIO_SIZE, mmio.clone());
			(Some(root), Some(mmio))
		} else {
			(None, None)
		};
		#[cfg(target_arch = "x86_64")]
		let mut pci_slot = 1u8;
		#[cfg(not(target_arch = "x86_64"))]
		if snap
			.devices
			.iter()
			.any(|d| d.transport == DeviceTransportKind::Pci)
		{
			return Err(err("snapshot contains PCI transport on a non-x86_64 build"));
		}

		let mut devices: Vec<DeviceHandle> = Vec::new();
		let mut workers: Vec<DeviceWorker> = Vec::new();
		let mut console_input = None;
		let mut console_output = None;
		for ds in &snap.devices {
			let backend = resolve_backend(ds, config)?;
			let mut fs_handle = None;
			let device: Arc<Mutex<dyn VirtioDevice>> = match &backend {
				BackendHint::Block { path, read_only } => {
					let block = open_configured_block(config, Path::new(path), *read_only)?;
					Arc::new(Mutex::new(block))
				},
				BackendHint::Net { .. } | BackendHint::UserNet { .. } => open_net_device(
					&backend,
					ds.user_net_state.as_deref(),
					config.user_net_restricted,
					config.user_net_allow_host_port,
				)?,
				BackendHint::Fs { tag, shared_dir, read_only } => {
					let fs_state = ds.fs.as_ref().ok_or_else(|| {
						err("snapshot virtio-fs backend is missing serialized fs state")
					})?;
					let fs_dev = Arc::new(Mutex::new(Fs::restore(
						tag.clone(),
						PathBuf::from(shared_dir.as_str()),
						fs_state,
						*read_only,
					)?));
					fs_handle = Some(FsSave::Host(fs_dev.clone()));
					fs_dev
				},
				BackendHint::RemoteFs { tag, sock } => {
					let fs_state = ds.fs.as_ref().ok_or_else(|| {
						err("snapshot remote virtio-fs backend is missing serialized fs state")
					})?;
					let remote_fs = Arc::new(Mutex::new(RemoteFs::restore(
						tag.clone(),
						PathBuf::from(sock.as_str()),
						fs_state,
					)?));
					fs_handle = Some(FsSave::Remote(remote_fs.clone()));
					remote_fs
				},
				BackendHint::Console => {
					let console = crate::virtio::console::Console::new()?;
					console_input = Some(console.input_handle());
					console_output = Some(console.output_handle());
					Arc::new(Mutex::new(console))
				},
				BackendHint::Rng => Arc::new(Mutex::new(crate::virtio::rng::Rng::new()?)),
			};
			match ds.transport {
				DeviceTransportKind::Mmio => {
					let mstate = MmioState {
						device_features_select: ds.device_features_select,
						driver_features_select: ds.driver_features_select,
						acked_features:         ds.acked_features,
						status:                 ds.status,
						activated:              ds.activated,
						queues:                 ds.queues.iter().copied().map(Into::into).collect(),
					};
					wire_device(
						&vm,
						&mut mmio_bus,
						device,
						ds.kind,
						backend,
						ds.gsi,
						ds.mmio_base,
						Some(&mstate),
						ds.interrupt_status,
						&mut workers,
						&mut devices,
					)?;
				},
				DeviceTransportKind::Pci => {
					#[cfg(target_arch = "x86_64")]
					{
						wire_pci_device(
							&vm,
							pci_root.as_ref().expect("pci root"),
							pci_mmio.as_ref().expect("pci mmio"),
							device,
							ds.kind,
							backend,
							ds.gsi,
							pci_slot,
							ds.mmio_base,
							Some(ds),
							&mut workers,
							&mut devices,
						)?;
						pci_slot += 1;
					}
					#[cfg(not(target_arch = "x86_64"))]
					unreachable!("PCI snapshots are rejected above on non-x86_64");
				},
			}
			if let Some(fs_handle) = fs_handle
				&& let Some(device) = devices.last_mut()
			{
				device.fs = Some(fs_handle);
			}
		}

		// The Linux restore pass runs while the fds are still locally owned.
		#[cfg(target_os = "linux")]
		for (id, vcpu) in vcpus.iter().enumerate() {
			vcpu.restore_state(&snap.vcpus[id])?;
		}

		let (txs, rxs) = vcpu_channels(snap.cpus);
		let gate = if config.start_paused {
			PauseGate::new_paused(u32::from(snap.cpus))
		} else {
			PauseGate::new(u32::from(snap.cpus))
		};
		Ok(Self {
			gate,
			cpus: snap.cpus,
			mem_mib: snap.mem_mib,
			cmdline: snap.cmdline,
			boot_mode,
			firmware,
			#[cfg(target_arch = "x86_64")]
			firmware_memory,
			vm,
			#[cfg(target_os = "linux")]
			vcpus,
			#[cfg(target_os = "macos")]
			macos_startup,
			vcpu_cmd_txs: txs,
			vcpu_cmd_rxs: rxs,
			vcpu_handles: Vec::new(),
			pager: None,
			pager_handler: None,
			pio_bus: Arc::new(pio_bus),
			mmio_bus: Arc::new(mmio_bus),
			serial,
			devices,
			device_workers: workers,
			device_loop: None,
			worker_kill_evt: EventFd::new(EFD_NONBLOCK)?,
			#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
			vfio_devices: Vec::new(),
			console_input,
			console_output,
			timeout_secs: config.timeout_secs,
			owner_lease_secs: config.owner_lease_secs,
			started_at: Instant::now(),
			dirty_base: Mutex::new(None),
			deadline: None,
			owner_deadline: None,
			api_sock: config.api_sock.clone(),
			exit_reason: Arc::new(AtomicU8::new(0)),
			#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
			gic,
			#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
			gic,
		})
	}

	// -- run loop ------------------------------------------------------------

	/// Spawn the device-worker and vCPU threads and put the host console in raw
	/// mode.
	fn start(&mut self) -> Result<()> {
		setup_console(self.serial.clone());
		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		for gpu in &self.vfio_devices {
			gpu.start_interrupts()?;
		}
		let device_workers = std::mem::take(&mut self.device_workers);
		if !device_workers.is_empty() {
			let gate = self.gate.clone();
			let exit_reason = self.exit_reason.clone();
			let kill_evt = self.worker_kill_evt.try_clone()?;
			let handle = thread::Builder::new()
				.name("vmon-virtio".into())
				.spawn(move || {
					// A failing device records its failure_msg/failure_evt inside
					// the loop before the error propagates here.
					if let Err(e) = run_shared_worker(device_workers, kill_evt) {
						crate::metrics::record_device_worker_error();
						set_exit_reason(&exit_reason, VmmExit::Killed);
						gate.set_state(RunState::Stopping);
						gate.signal_all_vcpus();
						restore_terminal();
						error!("shared device worker exited: {e}");
					}
				})
				.map_err(|e| err(format!("spawning shared device worker failed: {e}")))?;
			self.device_loop = Some(handle);
		}

		// XSAVE2 buffer size for full extended-state capture. `Cap::Xsave2` is an
		// x86-only kvm-ioctls variant; on aarch64/HVF there is no XSAVE so it is 0
		// (arm's save_state ignores it).
		#[cfg(target_arch = "x86_64")]
		let xsave_size = self
			.vm
			.kvm
			.check_extension_int(kvm_ioctls::Cap::Xsave2)
			.max(0) as usize;
		#[cfg(target_arch = "aarch64")]
		let xsave_size = 0usize;

		if let Some(pager) = &self.pager {
			let p = pager.clone();
			self.pager_handler = Some(
				thread::Builder::new()
					.name("vmon-pager".into())
					.spawn(move || p.handler_loop())
					.map_err(|e| err(format!("spawning pager handler failed: {e}")))?,
			);
		}

		#[cfg(target_os = "linux")]
		{
			let vcpus = std::mem::take(&mut self.vcpus);
			let rxs = std::mem::take(&mut self.vcpu_cmd_rxs);
			for (id, (vcpu, rx)) in vcpus.into_iter().zip(rxs).enumerate() {
				let pio = self.pio_bus.clone();
				let mmio = self.mmio_bus.clone();
				let gate = self.gate.clone();
				let exit_reason = self.exit_reason.clone();
				self.vcpu_handles.push(thread::spawn(move || {
					run_vcpu(vcpu, pio, mmio, id as u8, gate, exit_reason, rx, xsave_size);
				}));
			}
		}
		#[cfg(target_os = "macos")]
		self.start_macos_vcpus(xsave_size)?;
		Ok(())
	}

	#[cfg(target_os = "macos")]
	fn start_macos_vcpus(&mut self, xsave_size: usize) -> Result<()> {
		let cpus = self.cpus as usize;
		let rxs = std::mem::take(&mut self.vcpu_cmd_rxs);
		let (mpidr_tx, mpidr_rx) = flume::unbounded::<(u8, Result<u64>)>();
		let (ack_tx, ack_rx) = flume::unbounded::<(u8, Result<()>)>();
		let hv_vcpu_ids = Arc::new(Mutex::new(vec![None; cpus]));
		let hv_run_states = Arc::new(
			(0..cpus)
				.map(|_| Arc::new(AtomicBool::new(false)))
				.collect::<Vec<_>>(),
		);
		let kicker_ids = hv_vcpu_ids.clone();
		let kicker_run_states = hv_run_states.clone();
		self.gate.set_kicker(Arc::new(move || {
			let ids = kicker_ids.lock();
			let mut running = Vec::with_capacity(ids.len());
			for (id, hv_id) in ids.iter().enumerate() {
				if kicker_run_states[id].load(Ordering::SeqCst)
					&& let Some(hv_id) = hv_id
				{
					running.push(*hv_id);
				}
			}
			if !running.is_empty() {
				// SAFETY: IDs are copied from live applevisor vCPUs and filtered
				// to vCPU threads reporting that they are inside `hv_vcpu_run`;
				// broadcasting exits to parked or host-side vCPUs can be consumed
				// by a later re-entry.
				unsafe {
					let _ = applevisor_sys::hv_vcpus_exit(running.as_ptr(), running.len() as u32);
				}
			}
		}));
		let mut phase_txs = Vec::with_capacity(cpus);
		let mut go_txs = Vec::with_capacity(cpus);

		for (id, rx) in rxs.into_iter().enumerate() {
			let (phase_tx, phase_rx) = flume::unbounded::<MacosPhaseB>();
			let (go_tx, go_rx) = flume::unbounded::<()>();
			phase_txs.push(phase_tx);
			go_txs.push(go_tx);

			let vm = self.vm.clone();
			let pio = self.pio_bus.clone();
			let mmio = self.mmio_bus.clone();
			let gate = self.gate.clone();
			let exit_reason = self.exit_reason.clone();
			let mpidr_tx = mpidr_tx.clone();
			let ack_tx = ack_tx.clone();
			let hv_vcpu_ids = hv_vcpu_ids.clone();
			let hv_run_state = hv_run_states[id].clone();
			self.vcpu_handles.push(thread::spawn(move || {
				let vcpu = match vm.create_vcpu(id as u8, hv_run_state).and_then(|vcpu| {
					if id == 0 {
						vcpu.init_boot()?;
					} else {
						vcpu.init_secondary()?;
					}
					hv_vcpu_ids.lock()[id] = Some(vcpu.hv_vcpu_id());
					let mpidr = arch::regs::read_mpidr(&vcpu)?;
					mpidr_tx
						.send((id as u8, Ok(mpidr)))
						.map_err(|e| err(format!("reporting vCPU {id} MPIDR: {e}")))?;
					Ok(vcpu)
				}) {
					Ok(vcpu) => vcpu,
					Err(e) => {
						let _ = mpidr_tx.send((
							id as u8,
							Err(err(format!("creating/configuring HVF vCPU {id}: {e}"))),
						));
						set_exit_reason(&exit_reason, VmmExit::Killed);
						gate.set_state(RunState::Stopping);
						gate.signal_all_vcpus();
						return;
					},
				};

				let Ok(phase) = phase_rx.recv() else { return };
				let configured = match phase {
					MacosPhaseB::Boot { entry, fdt_addr } => {
						arch::configure_vcpu(&vcpu, id as u8, entry, fdt_addr)
					},
					MacosPhaseB::Restore(state) => vcpu.restore_state(&state),
				};
				let run = configured.is_ok();
				let _ = ack_tx.send((id as u8, configured));
				if !run || go_rx.recv().is_err() {
					return;
				}
				run_vcpu(vcpu, pio, mmio, id as u8, gate, exit_reason, rx, xsave_size);
			}));
		}
		drop(mpidr_tx);
		drop(ack_tx);

		let mut mpidrs = vec![0u64; cpus];
		for _ in 0..cpus {
			let (id, mpidr) = mpidr_rx
				.recv()
				.map_err(|e| err(format!("waiting for HVF vCPU init: {e}")))?;
			mpidrs[id as usize] = mpidr?;
		}

		match &self.macos_startup {
			MacosStartup::Boot { entry, initrd, virtio_infos } => {
				let gic = self
					.gic
					.as_ref()
					.ok_or_else(|| err("boot configuration failed: GIC is not created"))?;
				let fdt_addr =
					configure_aarch64_fdt(&self.vm, &self.cmdline, *initrd, virtio_infos, gic, &mpidrs)?;
				for tx in &phase_txs {
					tx.send(MacosPhaseB::Boot { entry: *entry, fdt_addr })
						.map_err(|e| err(format!("sending HVF boot plan: {e}")))?;
				}
			},
			MacosStartup::Restore { vcpus, machine } => {
				let gic = self
					.gic
					.as_ref()
					.ok_or_else(|| err("restore failed: GIC is not created"))?;
				gic.restore_state(&machine.gic)?;
				for (tx, state) in phase_txs.iter().zip(vcpus.iter()) {
					tx.send(MacosPhaseB::Restore(state.clone()))
						.map_err(|e| err(format!("sending HVF restore state: {e}")))?;
				}
			},
		}

		for _ in 0..cpus {
			let (id, result) = ack_rx
				.recv()
				.map_err(|e| err(format!("waiting for HVF vCPU phase B: {e}")))?;
			result.map_err(|e| err(format!("configuring HVF vCPU {id}: {e}")))?;
		}
		for tx in go_txs {
			tx.send(())
				.map_err(|e| err(format!("starting HVF vCPU run loop: {e}")))?;
		}
		Ok(())
	}

	/// Block until the guest exits (no control socket) or process control
	/// commands.
	fn serve_and_wait(
		mut self,
		control_server: Option<control::ControlServer>,
		snapshot_root: Option<&Path>,
		mut status_file: Option<std::fs::File>,
		agent_bridge: Option<JoinHandle<()>>,
	) -> Result<()> {
		self.deadline = self
			.timeout_secs
			.map(|secs| deadline_or_now(self.started_at, secs, "--timeout-secs"));
		let owner_watchdog = self.owner_lease_secs.map(|secs| {
			let watchdog = control::OwnerWatchdog::new(Some(deadline_or_now(
				self.started_at,
				secs,
				"--owner-lease-secs",
			)));
			let gate = self.gate.clone();
			let exit_reason = self.exit_reason.clone();
			watchdog.start(move || {
				set_exit_reason(&exit_reason, VmmExit::Timeout);
				gate.set_state(RunState::Stopping);
				gate.signal_all_vcpus();
			});
			watchdog
		});
		self.owner_deadline = None;
		let exit;
		if let Some(server) = control_server {
			let cleanup = server.cleanup_token();
			let (tx, rx) = flume::unbounded::<ControlCmd>();
			server.start(tx, owner_watchdog.clone());
			exit = loop {
				self.sweep_pager_if_needed();
				let wait = self
					.deadline
					.map_or(Duration::from_millis(200), |deadline| {
						Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now()))
					});
				match rx.recv_timeout(wait) {
					Ok(cmd) => {
						let quit = matches!(cmd.kind, ControlKind::Quit);
						let reply = self.handle_control(cmd.kind, snapshot_root);
						let _ = cmd.reply.send(reply);
						if quit {
							break VmmExit::Quit;
						}
					},
					Err(RecvTimeoutError::Timeout) => {
						if self.gate.state() == RunState::Stopping {
							break VmmExit::from_u8(self.exit_reason.load(Ordering::SeqCst));
						}
					},
					Err(RecvTimeoutError::Disconnected) => {
						break VmmExit::from_u8(self.exit_reason.load(Ordering::SeqCst));
					},
				}
				if let Some(deadline) = self.deadline
					&& Instant::now() >= deadline
				{
					self.gate.set_state(RunState::Stopping);
					self.gate.signal_all_vcpus();
					break VmmExit::Timeout;
				}
			};
			cleanup.cleanup();
			if let Some(watchdog) = &owner_watchdog {
				watchdog.shutdown();
			}
		} else {
			let deadline = self.deadline;
			let deadline_timer = if self.pager.is_some() {
				loop {
					if self.gate.state() == RunState::Stopping {
						break None;
					}
					if let Some(deadline) = deadline
						&& Instant::now() >= deadline
					{
						set_exit_reason(&self.exit_reason, VmmExit::Timeout);
						self.gate.set_state(RunState::Stopping);
						self.gate.signal_all_vcpus();
						break None;
					}
					self.sweep_pager_if_needed();
					thread::sleep(Duration::from_millis(200));
				}
			} else {
				deadline.map(|deadline| {
					let gate = self.gate.clone();
					let exit_reason = self.exit_reason.clone();
					thread::spawn(move || {
						loop {
							let now = Instant::now();
							if now >= deadline {
								set_exit_reason(&exit_reason, VmmExit::Timeout);
								gate.set_state(RunState::Stopping);
								gate.signal_all_vcpus();
								return;
							}
							if gate.state() == RunState::Stopping {
								return;
							}
							thread::sleep(
								Duration::from_millis(50).min(deadline.saturating_duration_since(now)),
							);
						}
					})
				})
			};
			let handles = std::mem::take(&mut self.vcpu_handles);
			let mut join_error = None;
			for h in handles {
				if h.join().is_err() && join_error.is_none() {
					join_error = Some("vCPU thread panicked before shutdown");
				}
			}
			if let Some(timer) = deadline_timer {
				let _ = timer.join();
			}
			let shutdown = self.shutdown_threads();
			let agent_shutdown = join_agent_bridge(agent_bridge);
			if let Some(watchdog) = &owner_watchdog {
				watchdog.shutdown();
			}
			let exit_now = VmmExit::from_u8(self.exit_reason.load(Ordering::SeqCst));
			write_final_status(status_file.as_mut(), self.api_sock.as_deref(), exit_now);
			if let Some(message) = join_error {
				return Err(err(message));
			}
			return match shutdown {
				Ok(()) => agent_shutdown,
				Err(e) => Err(e),
			};
		}
		let shutdown = self.shutdown_threads();
		let agent_shutdown = join_agent_bridge(agent_bridge);
		write_final_status(status_file.as_mut(), self.api_sock.as_deref(), exit);
		match shutdown {
			Ok(()) => agent_shutdown,
			Err(e) => Err(e),
		}
	}

	fn sweep_pager_if_needed(&self) {
		let Some(pager) = self.pager.clone() else {
			return;
		};
		if self.gate.state() != RunState::Running || !pager.over_target() {
			return;
		}
		if let Err(e) = self.pause() {
			warn!("pager sweep skipped because VM pause failed: {e}");
			return;
		}
		pager.evict_to_target();
		if let Err(e) = self.resume() {
			warn!("pager sweep completed but VM resume failed: {e}");
		}
	}

	fn handle_control(
		&mut self,
		kind: ControlKind,
		snapshot_root: Option<&Path>,
	) -> control::ControlReply {
		match kind {
			ControlKind::Ping => Ok(json!({})),
			ControlKind::Info => {
				let info = machine_info(self);
				Ok(json!({
					 "cpus": info.cpus,
					 "mem_mib": info.mem_mib,
					 "state": run_state_name(info.state),
					 "devices": info.devices,
					 "deadline_unix": info.deadline_unix,
					 "uptime_ms": info.uptime_ms,
				}))
			},
			ControlKind::Pause => self
				.pause()
				.map(|()| json!({}))
				.map_err(|e| control::ApiError::internal(e.to_string())),
			ControlKind::Resume => self
				.resume()
				.map(|()| json!({}))
				.map_err(|e| control::ApiError::internal(e.to_string())),
			ControlKind::Snapshot { name, base, track } => {
				if let Some(b) = &base {
					if !snapshot::is_safe_snapshot_name(b) {
						return Err(control::ApiError::path_denied(format!(
							"snapshot base {b} is not a safe basename"
						)));
					}
					if b == &name {
						return Err(control::ApiError::bad_request(
							"snapshot base cannot be the same as its own name",
						));
					}
				}
				let dir = snapshot_name_to_path(snapshot_root, &name)?;
				if self.gate.state() != RunState::Paused {
					return Err(control::ApiError::not_paused("snapshot requires a paused VM"));
				}
				self
					.snapshot(&dir, &name, base.as_deref(), track)
					.map(|()| json!({}))
					.map_err(|e| control::ApiError::internal(e.to_string()))
			},
			ControlKind::DiskSnapshot { name } => {
				let dir = snapshot_name_to_path(snapshot_root, &name)?;
				self
					.disk_snapshot(&dir)
					.map(|manifest| {
						json!({
							"artifact": "disk",
							"path": format!("{name}/{}", disk_snapshot::DISK_DIR),
							"rootfs": manifest.rootfs,
							"capture": manifest.capture,
							"bytes": manifest.bytes,
						})
					})
					.map_err(|e| control::ApiError::internal(e.to_string()))
			},
			ControlKind::Quit => {
				self.gate.set_state(RunState::Stopping);
				self.gate.signal_all_vcpus();
				Ok(json!({}))
			},
			ControlKind::Metrics => {
				// Live machine fields (the lifecycle test asserts cpus/mem_mib/
				// state/device_count) merged onto the process-global counters.
				let info = machine_info(self);
				let device_count = info.devices.len();
				let mut metrics = crate::metrics::snapshot_json();
				if let Some(obj) = metrics.as_object_mut() {
					obj.insert("cpus".into(), json!(info.cpus));
					obj.insert("mem_mib".into(), json!(info.mem_mib));
					obj.insert("state".into(), json!(run_state_name(info.state)));
					obj.insert("deadline_unix".into(), json!(info.deadline_unix));
					obj.insert("uptime_ms".into(), json!(info.uptime_ms));
					obj.insert("devices".into(), json!(info.devices));
					obj.insert("device_count".into(), json!(device_count));
					obj.insert("vcpu_threads".into(), json!(self.vcpu_handles.len()));
					obj.insert("worker_threads".into(), json!(usize::from(self.device_loop.is_some())));
					obj.insert("parked_vcpus".into(), json!(self.gate.parked_count()));
				}
				Ok(metrics)
			},
			ControlKind::Extend { secs } => {
				// Re-arm the hard deadline to now + secs (works even when no
				// --timeout-secs was set at launch); the run loop reads
				// self.deadline each iteration.
				let deadline = checked_deadline(Instant::now(), secs, "extend params.secs")
					.map_err(|e| control::ApiError::bad_request(e.to_string()))?;
				self.deadline = Some(deadline);
				let deadline_unix = deadline_to_unix(deadline);
				Ok(json!({ "deadline_unix": deadline_unix }))
			},
			ControlKind::RearmOwnerLease { secs } => {
				if !(1..=86_400).contains(&secs) {
					return Err(control::ApiError::bad_request(
						"rearm_owner_lease params.secs must be between 1 and 86400",
					));
				}
				let deadline = checked_deadline(Instant::now(), secs, "rearm_owner_lease params.secs")
					.map_err(|e| control::ApiError::bad_request(e.to_string()))?;
				self.owner_deadline = Some(deadline);
				Ok(json!({ "deadline_unix": deadline_to_unix(deadline) }))
			},
		}
	}

	// -- lifecycle transitions ----------------------------------------------

	/// Park every vCPU at a safe point and quiesce every device worker.
	/// Idempotent: pausing an already-paused VM is a no-op `Ok`.
	fn pause(&self) -> Result<()> {
		match self.gate.state() {
			RunState::Paused => return Ok(()),
			RunState::Stopping => return Err(err("pause failed: VM is stopping")),
			RunState::Running => {},
		}
		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		if !self.vfio_devices.is_empty() {
			return Err(err("pause is not supported while an NVIDIA vGPU is attached"));
		}

		// Reset the parked counter BEFORE publishing Paused: a vCPU only bumps
		// `parked` after it observes Paused, so resetting first guarantees the
		// count starts at 0 and each vCPU is counted exactly once this round.
		self.gate.reset_parked();
		if !self.gate.set_state_if(RunState::Running, RunState::Paused) {
			return Err(err("pause failed: VM is stopping"));
		}

		if let Err(e) = self.finish_pause() {
			return self.fail_pause(e);
		}
		Ok(())
	}

	fn finish_pause(&self) -> Result<()> {
		self.wait_for_vcpu_parking()?;

		for (device_index, d) in self.devices.iter().enumerate() {
			self.ensure_not_stopping("pause worker signal")?;
			let generation = d.pause_generation.fetch_add(1, Ordering::AcqRel) + 1;
			d.pause_requested.store(true, Ordering::Release);
			d.pause_evt.write(1).map_err(|e| {
				err(format!(
					"pause worker signal failed for device {device_index} {}: {e}",
					device_kind_name(d.kind)
				))
			})?;
			self.wait_for_worker_ack(device_index, d, generation)?;
		}
		self.ensure_not_stopping("pause complete")?;
		Ok(())
	}

	fn fail_pause(&self, cause: crate::result::Error) -> Result<()> {
		self.gate.set_state(RunState::Stopping);
		self.gate.signal_all_vcpus();
		for tx in &self.vcpu_cmd_txs {
			let _ = tx.send(VcpuCmd::Stop);
		}
		let _ = self.worker_kill_evt.write(1);
		Err(err(format!("pause failed; VM is stopping: {cause}")))
	}

	fn fail_resume(&self, cause: crate::result::Error) -> Result<()> {
		self.gate.set_state(RunState::Stopping);
		self.gate.signal_all_vcpus();
		for tx in &self.vcpu_cmd_txs {
			let _ = tx.send(VcpuCmd::Stop);
		}
		let _ = self.worker_kill_evt.write(1);
		Err(err(format!("resume failed; VM is stopping: {cause}")))
	}

	fn wait_for_vcpu_parking(&self) -> Result<()> {
		let deadline = Instant::now() + PAUSE_VCPU_PARK_TIMEOUT;
		// HVF: let idle vCPUs observe Paused and self-park at the top of their
		// run loop before any force-exit. `hv_vcpus_exit` followed by re-entry
		// corrupts the vCPU, and an idle guest self-parks within a WFI poll
		// (≪ one poll interval), so this avoids the kick entirely for the common
		// case. Busy vCPUs that miss this window fall through to the kick loop.
		#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
		if self.gate.parked_count() < u32::from(self.cpus) {
			let wait = min_duration(
				PAUSE_WAIT_POLL_INTERVAL,
				deadline.saturating_duration_since(Instant::now()),
			);
			if !wait.is_zero() {
				self.gate.wait_for_all_parked(wait);
			}
		}
		loop {
			self.ensure_not_stopping("pause vcpu park")?;
			let parked = self.gate.parked_count();
			if parked >= u32::from(self.cpus) {
				return Ok(());
			}
			if Instant::now() >= deadline {
				return Err(err(format!(
					"pause vcpu park timed out after {:?}; parked {parked}/{}; missing vCPU(s): {}",
					PAUSE_VCPU_PARK_TIMEOUT,
					self.cpus,
					self.missing_parked_vcpus()
				)));
			}

			// Re-signal stragglers each round: a vCPU still in the hypervisor run
			// call after we set Paused is kicked out by the next signal and parks.
			self.gate.signal_all_vcpus();
			let wait = min_duration(
				PAUSE_WAIT_POLL_INTERVAL,
				deadline.saturating_duration_since(Instant::now()),
			);
			if !wait.is_zero() {
				self.gate.wait_for_all_parked(wait);
			}
		}
	}

	fn worker_pause_failure(device_index: usize, device: &DeviceHandle) -> Result<Option<String>> {
		if !wait_eventfd_read(&device.failure_evt, Duration::ZERO).map_err(|e| {
			err(format!(
				"pause worker failure poll failed for device {device_index} {}: {e}",
				device_kind_name(device.kind)
			))
		})? {
			return Ok(None);
		}

		let message = device
			.failure_msg
			.lock()
			.take()
			.unwrap_or_else(|| "worker reported failure without details".to_string());
		Ok(Some(format!(
			"pause worker failed for device {device_index} {}: {message}",
			device_kind_name(device.kind)
		)))
	}

	fn worker_failure_message(&self) -> Option<String> {
		self
			.devices
			.iter()
			.enumerate()
			.find_map(|(device_index, device)| {
				device.failure_msg.lock().as_ref().map(|message| {
					format!(
						"device worker {device_index} {} failed: {message}",
						device_kind_name(device.kind)
					)
				})
			})
	}

	fn wait_for_worker_ack(
		&self,
		device_index: usize,
		device: &DeviceHandle,
		expected_generation: u64,
	) -> Result<()> {
		let deadline = Instant::now() + PAUSE_WORKER_ACK_TIMEOUT;
		loop {
			self.ensure_not_stopping("pause worker ack")?;
			if let Some(message) = Self::worker_pause_failure(device_index, device)? {
				return Err(err(message));
			}
			if Instant::now() >= deadline {
				return Err(err(format!(
					"pause worker ack timed out after {:?} for device {device_index} {}",
					PAUSE_WORKER_ACK_TIMEOUT,
					device_kind_name(device.kind)
				)));
			}
			let wait = min_duration(
				PAUSE_WAIT_POLL_INTERVAL,
				deadline.saturating_duration_since(Instant::now()),
			);
			if wait_eventfd_read(&device.ack_evt, wait).map_err(|e| {
				err(format!(
					"pause worker ack failed for device {device_index} {}: {e}",
					device_kind_name(device.kind)
				))
			})? && device.ack_generation.load(Ordering::Acquire) >= expected_generation
			{
				return Ok(());
			}
		}
	}

	fn recv_vcpu_state(
		&self,
		vcpu_id: usize,
		rx: Receiver<Result<arch::state::VcpuState>>,
	) -> Result<arch::state::VcpuState> {
		let deadline = Instant::now() + SNAPSHOT_VCPU_STATE_TIMEOUT;
		loop {
			self.ensure_not_stopping("snapshot vcpu save")?;
			if Instant::now() >= deadline {
				return Err(err(format!(
					"snapshot vcpu save timed out after {SNAPSHOT_VCPU_STATE_TIMEOUT:?} for vCPU \
					 {vcpu_id}"
				)));
			}
			let wait = min_duration(
				PAUSE_WAIT_POLL_INTERVAL,
				deadline.saturating_duration_since(Instant::now()),
			);
			match rx.recv_timeout(wait) {
				Ok(Ok(state)) => return Ok(state),
				Ok(Err(e)) => {
					return Err(err(format!("snapshot vcpu save failed for vCPU {vcpu_id}: {e}")));
				},
				Err(RecvTimeoutError::Timeout) => {},
				Err(RecvTimeoutError::Disconnected) => {
					return Err(err(format!(
						"snapshot vcpu save reply channel closed for vCPU {vcpu_id}"
					)));
				},
			}
		}
	}

	fn ensure_not_stopping(&self, phase: &str) -> Result<()> {
		if self.gate.state() == RunState::Stopping {
			Err(err(format!("{phase} failed: VM is stopping")))
		} else {
			Ok(())
		}
	}

	fn missing_parked_vcpus(&self) -> String {
		let mut parked = vec![false; self.cpus as usize];
		for id in self.gate.parked_vcpus() {
			if let Some(slot) = parked.get_mut(id) {
				*slot = true;
			}
		}
		let missing = parked
			.iter()
			.enumerate()
			.filter_map(|(id, parked)| (!*parked).then_some(id.to_string()))
			.collect::<Vec<_>>();
		if missing.is_empty() {
			"none".to_string()
		} else {
			missing.join(", ")
		}
	}

	fn resume(&self) -> Result<()> {
		match self.gate.state() {
			RunState::Running => return Ok(()),
			RunState::Stopping => return Err(err("resume failed: VM is stopping")),
			RunState::Paused => {},
		}
		for (device_index, d) in self.devices.iter().enumerate() {
			d.pause_requested.store(false, Ordering::Release);
			if let Err(e) = d.resume_evt.write(1) {
				return self.fail_resume(err(format!(
					"resume worker signal failed for device {device_index} {}: {e}",
					device_kind_name(d.kind)
				)));
			}
		}
		// Flip to Running BEFORE releasing vCPUs, else a woken vCPU re-checks the
		// gate, still sees Paused, and immediately re-parks (resume would hang).
		self.gate.set_state(RunState::Running);
		for (vcpu_id, tx) in self.vcpu_cmd_txs.iter().enumerate() {
			if tx.send(VcpuCmd::Resume).is_err() {
				return self.fail_resume(err(format!(
					"resume command failed for vCPU {vcpu_id}: vcpu thread gone"
				)));
			}
		}
		Ok(())
	}

	/// Push the framed `--agent-exec` hand-off to the in-guest agent over the
	/// virtio-console channel (warm boots only: the restored agent already owns
	/// the channel). Fails fast if no `--console-agent` device is attached, so
	/// a misconfigured warm boot never silently hangs.
	fn agent_send(&self, frames: &[u8]) -> Result<()> {
		let (buf, evt) = self
			.console_input
			.as_ref()
			.ok_or("--agent-exec requires --console-agent (no console channel attached)")?;
		buf.lock().extend(frames.iter().copied());
		evt.write(1)?;
		Ok(())
	}

	/// Capture a coherent root disk without serializing VM state. A running VM
	/// establishes its boundary by quiescing the block worker; a fully paused VM
	/// already has that worker stopped and may capture directly.
	fn disk_snapshot(&self, dir: &Path) -> Result<disk_snapshot::DiskManifest> {
		let workers_already_paused = match self.gate.state() {
			RunState::Running => false,
			RunState::Paused => true,
			RunState::Stopping => return Err(err("disk snapshot requires a live VM")),
		};
		let blocks = self
			.devices
			.iter()
			.enumerate()
			.filter_map(|(index, device)| {
				matches!(
					(&device.kind, &device.backend),
					(DeviceKind::Block, BackendHint::Block { read_only: false, .. })
				)
				.then_some(index)
			})
			.collect::<Vec<_>>();
		if blocks.len() != 1 {
			return Err(err("disk snapshot requires exactly one writable virtio-block root device"));
		}

		let mut requested = Vec::with_capacity(blocks.len());
		let capture = if workers_already_paused {
			let index = blocks[0];
			disk_snapshot::write_disk_point(dir, |destination| {
				self.devices[index].device.lock().capture_disk(destination)
			})
		} else {
			(|| {
				for &index in &blocks {
					let device = &self.devices[index];
					self.ensure_not_stopping("disk snapshot worker signal")?;
					// Publish the intent before the event, allowing a timeout cleanup
					// to cancel a worker which has not consumed the event yet.
					let generation = device.pause_generation.fetch_add(1, Ordering::AcqRel) + 1;
					device.pause_requested.store(true, Ordering::Release);
					requested.push(index);
					device.pause_evt.write(1).map_err(|e| {
						err(format!(
							"disk snapshot worker signal failed for device {index} {}: {e}",
							device_kind_name(device.kind)
						))
					})?;
					self.wait_for_worker_ack(index, device, generation)?;
				}
				let index = blocks[0];
				disk_snapshot::write_disk_point(dir, |destination| {
					self.devices[index].device.lock().capture_disk(destination)
				})
			})()
		};

		// This is deliberately independent of `Vmm::resume`: vCPUs never
		// stopped, and every requested worker must be released even after clone,
		// manifest, or acknowledgement failure.
		let thaw = self.release_disk_workers(&requested);
		match (capture, thaw) {
			(Ok(manifest), Ok(())) => Ok(manifest),
			(Ok(_), Err(thaw)) => {
				disk_snapshot::remove_disk_point(dir);
				Err(thaw)
			},
			(Err(capture), Ok(())) => Err(capture),
			(Err(capture), Err(thaw)) => Err(err(format!(
				"disk snapshot failed: {capture}; additionally failed to release block worker: {thaw}"
			))),
		}
	}

	/// Cancel/release every block worker previously requested by
	/// [`Self::disk_snapshot`]. It attempts all releases before returning the
	/// first failure so an error cannot strand another worker.
	fn release_disk_workers(&self, requested: &[usize]) -> Result<()> {
		let mut release_error = None;
		for &index in requested {
			let device = &self.devices[index];
			device.pause_requested.store(false, Ordering::Release);
			if let Err(e) = device.resume_evt.write(1)
				&& release_error.is_none()
			{
				release_error = Some(err(format!(
					"disk snapshot worker release failed for device {index} {}: {e}",
					device_kind_name(device.kind)
				)));
			}
		}
		release_error.map_or(Ok(()), Err)
	}

	/// Write a complete snapshot of the (paused) VM to `dir`.
	/// If `base` is `Some`, store only the pages that differ from that sibling
	/// snapshot. User-mode NAT devices serialize libslirp's guest-visible state;
	/// in-flight host-side sockets are intentionally not carried across restore.
	fn snapshot(&self, dir: &Path, name: &str, base: Option<&str>, track: bool) -> Result<()> {
		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		if !self.vfio_devices.is_empty() {
			return Err(err("full VM snapshots are not supported while an NVIDIA vGPU is attached"));
		}
		match self.gate.state() {
			RunState::Stopping => return Err(err("snapshot failed: VM is stopping")),
			RunState::Paused => {},
			RunState::Running => return Err(err("not paused")),
		}
		let t0 = std::time::Instant::now();

		// Per-vCPU state is read by the owning thread (only it may touch its fd).
		let mut vcpus = Vec::with_capacity(self.cpus as usize);
		for (vcpu_id, tx) in self.vcpu_cmd_txs.iter().enumerate() {
			self.ensure_not_stopping("snapshot vcpu save")?;
			let (rtx, rrx) = flume::bounded(1);
			tx.send(VcpuCmd::SaveState(rtx)).map_err(|_| {
				err(format!("snapshot vcpu save command failed for vCPU {vcpu_id}: vcpu thread gone"))
			})?;
			vcpus.push(self.recv_vcpu_state(vcpu_id, rrx)?);
		}
		self.ensure_not_stopping("snapshot machine state")?;

		#[cfg(target_arch = "x86_64")]
		let machine = arch::state::save_machine(&self.vm.fd)?;
		#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
		let machine = arch::state::MachineState { gic: self.gic.save_state(u64::from(self.cpus))? };
		#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
		let machine = arch::state::MachineState {
			gic: self
				.gic
				.as_ref()
				.ok_or_else(|| err("snapshot machine state failed: GIC is not created"))?
				.save_state(u64::from(self.cpus))?,
		};

		self.ensure_not_stopping("snapshot serial state")?;
		// setup_console enqueues host stdin under this same mutex; keep the
		// guard for the full snapshot write so the 16550 FIFO cannot mutate.
		let serial_guard = self.serial.lock();
		let serial = serial_guard.state().into();

		self.ensure_not_stopping("snapshot device state")?;
		let mut devices = Vec::with_capacity(self.devices.len());
		for d in &self.devices {
			self.ensure_not_stopping("snapshot device state")?;
			let (
				transport,
				device_features_select,
				driver_features_select,
				acked_features,
				status,
				activated,
				transport_queues,
				transport_pci,
			) = match &d.transport {
				DeviceTransport::Mmio(transport) => {
					let ms = transport.lock().save();
					(
						DeviceTransportKind::Mmio,
						ms.device_features_select,
						ms.driver_features_select,
						ms.acked_features,
						ms.status,
						ms.activated,
						ms.queues,
						None,
					)
				},
				#[cfg(target_arch = "x86_64")]
				DeviceTransport::Pci(transport) => {
					let transport = transport.lock();
					let common = transport.common_state();
					(
						DeviceTransportKind::Pci,
						common.device_features_select,
						common.driver_features_select,
						common.acked_features,
						common.status,
						common.activated,
						common.queues,
						Some(transport.save()),
					)
				},
			};
			// After activation the device owns the queues, so read live state
			// from it; fall back to the transport for a not-yet-activated device.
			let (live, user_net_state) = {
				let device = d.device.lock();
				(device.queue_states(), device.user_net_state()?)
			};
			let queues = if live.is_empty() {
				transport_queues
			} else {
				live
			};
			let fs = if matches!(d.kind, DeviceKind::Fs | DeviceKind::RemoteFs) {
				Some(
					d.fs
						.as_ref()
						.ok_or_else(|| err("virtio-fs handle missing from device state"))?
						.save(),
				)
			} else {
				None
			};
			let transport_pci: Option<snapshot::PciTransportStateSer> = transport_pci;
			let mmio_base = transport_pci
				.as_ref()
				.map_or(d.mmio_base, |pci| pci.bar_base);
			devices.push(DeviceState {
				kind: d.kind,
				transport,
				mmio_base,
				gsi: d.gsi,
				interrupt_status: d.interrupt.status(),
				device_features_select,
				driver_features_select,
				acked_features,
				status,
				activated,
				queues: queues.into_iter().map(Into::into).collect(),
				backend: d.backend.clone(),
				user_net_state,
				transport_pci,
				fs,
			});
		}

		let mem_mib = self.mem_mib;
		let cpus = self.cpus;
		let cmdline = self.cmdline.clone();
		let boot_mode = match self.boot_mode {
			BootMode::Direct => "direct",
			BootMode::Uefi => "uefi",
		}
		.to_string();
		let firmware = self
			.firmware
			.as_ref()
			.map(|path| path.to_string_lossy().into_owned());
		self.ensure_not_stopping("snapshot memory/state write")?;
		// The pager handler remains live while paused, so reading evicted RAM
		// through the mmap faults pages back in and snapshots the correct bytes.
		// Consume the armed dirty-tracking window when this delta chains to
		// exactly the armed base; the fetch clears the hypervisor log, so the
		// window is single-use either way.
		let guest_dirty = match base {
			Some(base_name) if self.dirty_base.lock().as_deref() == Some(base_name) => {
				*self.dirty_base.lock() = None;
				self.vm.take_dirty_log()?
			},
			_ => None,
		};
		snapshot::write_snapshot(
			dir,
			base,
			self.vm.memory(),
			guest_dirty,
			move |mem_regions, delta| Snapshot {
				version: snapshot::SNAPSHOT_VERSION,
				arch: build_arch().to_string(),
				backend: crate::hv::current_backend(),
				mem_mib,
				cpus,
				cmdline,
				boot_mode,
				firmware,
				mem_regions,
				vcpus,
				machine,
				serial,
				devices,
				delta,
			},
		)?;
		if track {
			// Arm a fresh window against the snapshot just written, while the
			// VM is still paused so no write can slip between dump and arm.
			if self.vm.arm_dirty_tracking()? {
				crate::memory::reset_dirty(self.vm.memory());
				*self.dirty_base.lock() = Some(name.to_owned());
			}
		}
		self.ensure_not_stopping("snapshot commit")?;
		drop(serial_guard);
		crate::metrics::record_snapshot(t0.elapsed());
		Ok(())
	}

	fn shutdown_threads(&mut self) -> Result<()> {
		let mut join_error = None;
		self.gate.set_state(RunState::Stopping);
		restore_terminal();
		self.gate.signal_all_vcpus();
		for tx in &self.vcpu_cmd_txs {
			let _ = tx.send(VcpuCmd::Stop);
		}
		for h in self.vcpu_handles.drain(..) {
			if h.join().is_err() && join_error.is_none() {
				join_error = Some("vCPU thread panicked during shutdown");
			}
		}
		#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
		for gpu in &self.vfio_devices {
			if gpu.stop_interrupts().is_err() && join_error.is_none() {
				join_error = Some("NVIDIA vGPU interrupt shutdown failed");
			}
		}
		let _ = self.worker_kill_evt.write(1);
		if let Some(h) = self.device_loop.take()
			&& h.join().is_err()
			&& join_error.is_none()
		{
			join_error = Some("device worker thread panicked during shutdown");
		}
		if let Some(pager) = &self.pager {
			pager.request_stop();
		}
		if let Some(handle) = self.pager_handler.take()
			&& handle.join().is_err()
			&& join_error.is_none()
		{
			join_error = Some("pager handler thread panicked during shutdown");
		}
		if let Some(message) = join_error {
			Err(err(message))
		} else if let Some(message) = self.worker_failure_message() {
			Err(err(message))
		} else {
			Ok(())
		}
	}
}

fn wait_eventfd_read(evt: &EventFd, timeout: Duration) -> Result<bool> {
	let mut pfd = libc::pollfd { fd: evt.as_raw_fd(), events: libc::POLLIN, revents: 0 };
	// SAFETY: `pfd` points to one initialized pollfd for the duration of the call.
	let rc = unsafe { libc::poll(&mut pfd, 1, poll_timeout_ms(timeout)) };
	if rc == 0 {
		return Ok(false);
	}
	if rc < 0 {
		let e = io::Error::last_os_error();
		if e.kind() == io::ErrorKind::Interrupted {
			return Ok(false);
		}
		return Err(e.into());
	}

	let error_events = pfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL);
	if error_events != 0 {
		return Err(err(format!("eventfd poll returned error revents=0x{:x}", pfd.revents)));
	}
	if pfd.revents & libc::POLLIN == 0 {
		return Ok(false);
	}

	evt.read()
		.map_err(|e| err(format!("eventfd read failed: {e}")))?;
	Ok(true)
}

fn poll_timeout_ms(timeout: Duration) -> libc::c_int {
	if timeout.is_zero() {
		return 0;
	}
	let millis = timeout.as_millis();
	if millis == 0 {
		1
	} else {
		millis.min(i32::MAX as u128) as libc::c_int
	}
}

fn min_duration(a: Duration, b: Duration) -> Duration {
	if a <= b { a } else { b }
}

/// Build `cpus` per-vCPU command channels, returning paired senders/receivers.
fn vcpu_channels(cpus: u8) -> (Vec<Sender<VcpuCmd>>, Vec<Receiver<VcpuCmd>>) {
	let mut txs = Vec::with_capacity(cpus as usize);
	let mut rxs = Vec::with_capacity(cpus as usize);
	for _ in 0..cpus {
		let (tx, rx) = flume::unbounded();
		txs.push(tx);
		rxs.push(rx);
	}
	(txs, rxs)
}

fn fresh_net_backend(config: &Config) -> Option<BackendHint> {
	if config.user_net {
		Some(BackendHint::UserNet { mac: config.mac })
	} else {
		config
			.tap
			.as_ref()
			.map(|tap| BackendHint::Net { tap: tap.clone(), mac: config.mac })
	}
}

fn open_net_device(
	backend: &BackendHint,
	user_net_state: Option<&[u8]>,
	user_net_restricted: bool,
	user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	match backend {
		BackendHint::Net { tap, mac } => {
			let tap = Tap::open(tap, *mac)?;
			let mac = tap.mac();
			Ok(Arc::new(Mutex::new(Net::new(tap, mac))))
		},
		BackendHint::UserNet { mac } => {
			open_user_net_device(*mac, user_net_state, user_net_restricted, user_net_allow_host_port)
		},
		_ => Err(err("backend is not a virtio-net device")),
	}
}

fn open_fresh_net_device(
	backend: &BackendHint,
	user_net_restricted: bool,
	user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	match backend {
		BackendHint::Net { tap, mac } => {
			let tap = Tap::open(tap, *mac)?;
			let mac = tap.mac();
			Ok(Arc::new(Mutex::new(Net::new(tap, mac))))
		},
		BackendHint::UserNet { mac } => {
			open_fresh_user_net_device(*mac, user_net_restricted, user_net_allow_host_port)
		},
		_ => Err(err("backend is not a virtio-net device")),
	}
}

#[cfg(target_os = "macos")]
fn open_fresh_user_net_device(
	mac: [u8; 6],
	user_net_restricted: bool,
	user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	let net = if user_net_restricted {
		let allowed_host_port = user_net_allow_host_port
			.filter(|port| *port != 0)
			.ok_or_else(|| {
				err("--user-net-restricted requires a nonzero --user-net-allow-host-port")
			})?;
		Net::new_restricted_user_with_state(mac, allowed_host_port, None)?
	} else {
		Net::new_user_with_state(mac, None)?
	};
	Ok(Arc::new(Mutex::new(net)))
}

#[cfg(not(target_os = "macos"))]
fn open_fresh_user_net_device(
	_mac: [u8; 6],
	_user_net_restricted: bool,
	_user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	Err(err("--net user is currently supported only on macOS"))
}

#[cfg(target_os = "macos")]
fn open_user_net_device(
	mac: [u8; 6],
	user_net_state: Option<&[u8]>,
	user_net_restricted: bool,
	user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	let net = if user_net_restricted {
		let allowed_host_port = user_net_allow_host_port
			.filter(|port| *port != 0)
			.ok_or_else(|| {
				err("--user-net-restricted requires a nonzero --user-net-allow-host-port")
			})?;
		Net::new_restricted_user_with_state(
			mac,
			allowed_host_port,
			user_net_state.map(<[u8]>::to_vec),
		)?
	} else {
		Net::new_user_with_state(mac, user_net_state.map(<[u8]>::to_vec))?
	};
	Ok(Arc::new(Mutex::new(net)))
}

#[cfg(not(target_os = "macos"))]
fn open_user_net_device(
	_mac: [u8; 6],
	_user_net_state: Option<&[u8]>,
	_user_net_restricted: bool,
	_user_net_allow_host_port: Option<u16>,
) -> Result<Arc<Mutex<dyn VirtioDevice>>> {
	Err(err("--net user is currently supported only on macOS"))
}

/// Rebind a restored device only to host resources supplied for this launch.
///
/// Snapshot backend paths are untrusted metadata. Reusing them would let a
/// crafted snapshot open a host disk, TAP, directory, or Unix socket.
fn resolve_backend(ds: &DeviceState, config: &Config) -> Result<BackendHint> {
	match &ds.backend {
		BackendHint::Block { read_only, .. } => {
			let path = config
				.rootfs
				.as_ref()
				.ok_or_else(|| err("restoring a block device requires an explicit --rootfs binding"))?;
			Ok(BackendHint::Block {
				path:      path.to_string_lossy().into_owned(),
				read_only: *read_only,
			})
		},
		BackendHint::Net { mac, .. } => {
			let mac = if config.mac_specified {
				config.mac
			} else {
				*mac
			};
			if config.user_net {
				Ok(BackendHint::UserNet { mac })
			} else {
				let tap = config.tap.clone().ok_or_else(|| {
					err("restoring a TAP-backed network device requires --tap or --net user")
				})?;
				Ok(BackendHint::Net { tap, mac })
			}
		},
		BackendHint::UserNet { mac } => {
			let mac = if config.mac_specified {
				config.mac
			} else {
				*mac
			};
			if let Some(tap) = &config.tap {
				Ok(BackendHint::Net { tap: tap.clone(), mac })
			} else {
				Ok(BackendHint::UserNet { mac })
			}
		},
		BackendHint::Fs { tag, read_only, .. } => {
			let (shared_dir, read_only) = if let Some(mount) =
				config.volumes.iter().find(|mount| &mount.tag == tag)
			{
				(mount.dir.to_string_lossy().into_owned(), mount.read_only)
			} else if config.fs_tag.as_deref() == Some(tag.as_str()) {
				let dir = config.fs_dir.as_ref().ok_or_else(|| {
					err(format!("restoring virtio-fs tag {tag:?} requires an explicit --fs-dir binding"))
				})?;
				(dir.to_string_lossy().into_owned(), *read_only)
			} else {
				return Err(err(format!(
					"restoring virtio-fs tag {tag:?} requires a matching --volume or --fs-tag/--fs-dir \
					 binding"
				)));
			};
			Ok(BackendHint::Fs { tag: tag.clone(), shared_dir, read_only })
		},
		BackendHint::RemoteFs { tag, .. } => {
			let mount = config
				.remote_fs
				.iter()
				.find(|mount| mount.tag == *tag)
				.ok_or_else(|| {
					err(format!(
						"restoring remote virtio-fs tag {tag:?} requires a matching --remote-fs binding"
					))
				})?;
			Ok(BackendHint::RemoteFs {
				tag:  tag.clone(),
				sock: mount.sock.to_string_lossy().into_owned(),
			})
		},
		BackendHint::Console => Ok(BackendHint::Console),
		BackendHint::Rng => Ok(BackendHint::Rng),
	}
}

/// Create the serial console device and map it on the right bus for the arch.
#[cfg(target_arch = "x86_64")]
fn setup_serial(
	vm: &Vm,
	pio_bus: &mut Bus,
	state: Option<&snapshot::SerialState>,
) -> Result<Arc<Mutex<SerialDevice>>> {
	let serial = build_serial_device(vm, state)?;
	pio_bus.register(0x3f8, 8, serial.clone());
	Ok(serial)
}

/// Create the serial console device and map it on the right bus for the arch.
#[cfg(target_arch = "aarch64")]
fn setup_serial(
	vm: &Vm,
	mmio_bus: &mut Bus,
	state: Option<&snapshot::SerialState>,
) -> Result<Arc<Mutex<SerialDevice>>> {
	let serial = build_serial_device(vm, state)?;
	mmio_bus.register(
		crate::layout::SERIAL_MMIO_BASE,
		crate::layout::SERIAL_MMIO_SIZE,
		serial.clone(),
	);
	Ok(serial)
}

fn build_serial_device(
	vm: &Vm,
	state: Option<&snapshot::SerialState>,
) -> Result<Arc<Mutex<SerialDevice>>> {
	let irq_line = vm.irq_line(SERIAL_IRQ)?;
	let device = match state {
		Some(state) => {
			let state: vm_superio::serial::SerialState = state.clone().into();
			SerialDevice::from_state(irq_line, &state)?
		},
		None => SerialDevice::new(irq_line),
	};
	Ok(Arc::new(Mutex::new(device)))
}

/// Wire one virtio-mmio device: irqfd, queue-notify ioeventfd, transport on the
/// MMIO bus, worker control eventfds, and the
/// [`DeviceHandle`]/[`DeviceWorker`].
///
/// `saved` selects the transport constructor: `None` -> fresh `new` (boot);
/// `Some(state)` -> `from_state` + `reactivate` (restore), with `int_status`
/// applied to the device interrupt.
#[allow(
	clippy::too_many_arguments,
	reason = "Complexity is required due to the number of parameters needed to wire a device"
)]
fn wire_device(
	vm: &Vm,
	mmio_bus: &mut Bus,
	device: Arc<Mutex<dyn VirtioDevice>>,
	kind: DeviceKind,
	backend: BackendHint,
	gsi: u32,
	mmio_base: u64,
	saved: Option<&MmioState>,
	int_status: u32,
	workers: &mut Vec<DeviceWorker>,
	devices: &mut Vec<DeviceHandle>,
) -> Result<()> {
	let irq_line = vm.irq_line(gsi)?;
	let interrupt = Arc::new(Interrupt::new(irq_line));
	if saved.is_some() {
		interrupt.restore_status(int_status);
	}

	let queue_evt = EventFd::new(EFD_NONBLOCK)?;
	vm.register_ioevent(&queue_evt, mmio_base + QUEUE_NOTIFY_OFFSET, None)?;

	let transport = match saved {
		None => MmioTransport::new(device.clone(), vm.memory().clone(), interrupt.clone())?,
		Some(state) => {
			let mut t = MmioTransport::from_state(
				device.clone(),
				vm.memory().clone(),
				interrupt.clone(),
				state,
			)?;
			if state.activated {
				t.reactivate()?;
			}
			t
		},
	};
	let transport = Arc::new(Mutex::new(transport));
	mmio_bus.register(mmio_base, MMIO_DEVICE_SIZE, transport.clone());

	// Pause/ack eventfds are level-triggered; the VMM polls them before reading.
	let pause_evt = EventFd::new(0)?;
	let resume_evt = EventFd::new(0)?;
	let ack_evt = EventFd::new(0)?;
	let failure_evt = EventFd::new(0)?;
	let failure_msg = Arc::new(Mutex::new(None));
	let pause_requested = Arc::new(AtomicBool::new(false));
	let pause_generation = Arc::new(AtomicU64::new(0));
	let ack_generation = Arc::new(AtomicU64::new(0));

	devices.push(DeviceHandle {
		kind,
		mmio_base,
		gsi,
		transport: DeviceTransport::Mmio(transport),
		device: device.clone(),
		interrupt,
		backend,
		fs: None,
		pause_evt: pause_evt.try_clone()?,
		resume_evt: resume_evt.try_clone()?,
		ack_evt: ack_evt.try_clone()?,
		failure_evt: failure_evt.try_clone()?,
		failure_msg: failure_msg.clone(),
		pause_requested: pause_requested.clone(),
		pause_generation: pause_generation.clone(),
		ack_generation: ack_generation.clone(),
	});
	workers.push(DeviceWorker {
		device,
		queue_evt,
		control: WorkerControl {
			pause_evt,
			resume_evt,
			ack_evt,
			failure_evt,
			failure_msg,
			pause_requested,
			pause_generation,
			ack_generation,
		},
	});
	Ok(())
}

/// Wire one x86 virtio-pci function: legacy irqfd fallback, PCI config-space
/// function, BAR dispatcher entry, worker control eventfds, and worker spec.
#[cfg(target_arch = "x86_64")]
#[allow(clippy::too_many_arguments, reason = "Complexity is required to wire a PCI virtio device")]
fn wire_pci_device(
	vm: &Vm,
	pci_root: &Arc<Mutex<PciRoot>>,
	pci_mmio: &Arc<Mutex<PciMmioDispatcher>>,
	device: Arc<Mutex<dyn VirtioDevice>>,
	kind: DeviceKind,
	backend: BackendHint,
	gsi: u32,
	pci_slot: u8,
	bar_base: u64,
	saved: Option<&DeviceState>,
	workers: &mut Vec<DeviceWorker>,
	devices: &mut Vec<DeviceHandle>,
) -> Result<()> {
	let irq_line = vm.irq_line(gsi)?;
	let interrupt = Arc::new(Interrupt::new(irq_line));
	if let Some(saved) = saved {
		interrupt.restore_status(saved.interrupt_status);
	}

	let queue_evt = EventFd::new(EFD_NONBLOCK)?;
	let transport = match saved {
		None => PciTransport::new(
			device.clone(),
			vm.memory().clone(),
			interrupt.clone(),
			vm.fd.clone(),
			queue_evt.try_clone()?,
			bar_base,
			gsi,
		)?,
		Some(state) => {
			let pci = state
				.transport_pci
				.as_ref()
				.ok_or_else(|| err("snapshot PCI device is missing serialized PCI transport state"))?;
			let common = PciCommonState {
				device_features_select: state.device_features_select,
				driver_features_select: state.driver_features_select,
				acked_features:         state.acked_features,
				status:                 state.status,
				activated:              state.activated,
				queues:                 state.queues.iter().copied().map(Into::into).collect(),
			};
			let mut t = PciTransport::from_state(
				device.clone(),
				vm.memory().clone(),
				interrupt.clone(),
				vm.fd.clone(),
				queue_evt.try_clone()?,
				pci,
				common,
			)?;
			if state.activated {
				t.reactivate()?;
			}
			t
		},
	};
	let transport = Arc::new(Mutex::new(transport));
	PciTransport::attach_interrupt_notifier(&transport, &interrupt);
	let function: Arc<dyn PciFunction> = transport.clone();
	pci_root.lock().register(pci_slot, 0, function.clone())?;
	pci_mmio.lock().register(function);

	// Pause/ack eventfds are level-triggered; the VMM polls them before reading.
	let pause_evt = EventFd::new(0)?;
	let resume_evt = EventFd::new(0)?;
	let ack_evt = EventFd::new(0)?;
	let failure_evt = EventFd::new(0)?;
	let failure_msg = Arc::new(Mutex::new(None));
	let pause_requested = Arc::new(AtomicBool::new(false));
	let pause_generation = Arc::new(AtomicU64::new(0));
	let ack_generation = Arc::new(AtomicU64::new(0));

	devices.push(DeviceHandle {
		kind,
		mmio_base: bar_base,
		gsi,
		transport: DeviceTransport::Pci(transport),
		device: device.clone(),
		interrupt,
		backend,
		fs: None,
		pause_evt: pause_evt.try_clone()?,
		resume_evt: resume_evt.try_clone()?,
		ack_evt: ack_evt.try_clone()?,
		failure_evt: failure_evt.try_clone()?,
		failure_msg: failure_msg.clone(),
		pause_requested: pause_requested.clone(),
		pause_generation: pause_generation.clone(),
		ack_generation: ack_generation.clone(),
	});
	workers.push(DeviceWorker {
		device,
		queue_evt,
		control: WorkerControl {
			pause_evt,
			resume_evt,
			ack_evt,
			failure_evt,
			failure_msg,
			pause_requested,
			pause_generation,
			ack_generation,
		},
	});
	Ok(())
}

/// On x86 there is no FDT, so virtio-mmio devices are described on the kernel
/// command line. On aarch64 they are declared in the device tree instead.
#[allow(
	unused_variables,
	clippy::ptr_arg,
	clippy::needless_pass_by_ref_mut,
	clippy::missing_const_for_fn,
	reason = "fixed signature for the x86 formatting path; non-const there, empty elsewhere"
)]
fn push_virtio_cmdline(cmdline: &mut String, mmio_base: u64, gsi: u32) {
	#[cfg(target_arch = "x86_64")]
	{
		use std::fmt::Write as _;
		let _ = write!(cmdline, " virtio_mmio.device=4K@0x{mmio_base:x}:{gsi}");
	}
}

fn cmdline_has_key(cmdline: &str, key: &str) -> bool {
	cmdline.split_whitespace().any(|arg| {
		arg.strip_prefix(key)
			.is_some_and(|rest| rest.starts_with('='))
	})
}

#[cfg(target_arch = "x86_64")]
fn load_initrd(
	config: &Config,
	vm: &Vm,
	loaded: &arch::boot::LoadedKernel,
) -> Result<Option<(vm_memory::GuestAddress, usize)>> {
	let Some(path) = &config.initrd else {
		return Ok(None);
	};
	let initrd_addr_max = loaded
		.setup_header
		.map(|h| h.initrd_addr_max)
		.filter(|&m| m != 0)
		.map_or(0x37ff_ffff, u64::from);
	Ok(Some(arch::boot::load_initrd(path, vm.memory(), initrd_addr_max)?))
}

#[cfg(target_arch = "aarch64")]
fn load_initrd(
	config: &Config,
	vm: &Vm,
	loaded: &arch::boot::LoadedKernel,
) -> Result<Option<(vm_memory::GuestAddress, usize)>> {
	let Some(path) = &config.initrd else {
		return Ok(None);
	};
	Ok(Some(arch::boot::load_initrd(path, vm.memory(), loaded)?))
}

#[cfg(target_arch = "x86_64")]
fn configure_and_boot(
	vm: &Vm,
	vcpus: &[Vcpu],
	loaded: &arch::boot::LoadedKernel,
	boot_mode: BootMode,
	cmdline: &str,
	initrd: Option<(vm_memory::GuestAddress, usize)>,
	_virtio_infos: &[(u64, u32)],
) -> Result<()> {
	match boot_mode {
		BootMode::Direct => {
			let cmdline_obj = arch::boot::build_cmdline(cmdline)?;
			arch::boot::configure_system(
				vm.memory(),
				u8::try_from(vcpus.len()).unwrap(),
				&cmdline_obj,
				initrd,
				loaded.setup_header,
			)?;
			for (id, vcpu) in vcpus.iter().enumerate() {
				arch::configure_vcpu(vcpu, vm.memory(), vm.supported_cpuid(), id as u8, loaded.entry)?;
			}
		},
		BootMode::Uefi => {
			arch::boot::configure_uefi_system(
				vm.memory(),
				u8::try_from(vcpus.len()).unwrap(),
				PCI_ECAM_BASE,
				PCI_ECAM_SIZE,
			)?;
			for (id, vcpu) in vcpus.iter().enumerate() {
				arch::boot::configure_uefi_vcpu(vcpu, vm.supported_cpuid(), id as u8, loaded.entry)?;
			}
		},
	}
	Ok(())
}

#[cfg(target_arch = "aarch64")]
fn configure_aarch64_fdt(
	vm: &Vm,
	cmdline: &str,
	initrd: Option<(vm_memory::GuestAddress, usize)>,
	virtio_infos: &[(u64, u32)],
	gic: &Gic,
	mpidrs: &[u64],
) -> Result<u64> {
	use crate::{
		arch::fdt::FdtDevice,
		layout::{MMIO_DEVICE_SIZE, SERIAL_IRQ, SERIAL_MMIO_BASE, SERIAL_MMIO_SIZE},
	};

	let serial = FdtDevice { addr: SERIAL_MMIO_BASE, len: SERIAL_MMIO_SIZE, gsi: SERIAL_IRQ };
	let virtio: Vec<FdtDevice> = virtio_infos
		.iter()
		.map(|&(addr, gsi)| FdtDevice { addr, len: MMIO_DEVICE_SIZE, gsi })
		.collect();

	arch::boot::configure_system(vm.memory(), cmdline, initrd, gic, mpidrs, &serial, &virtio)
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn configure_and_boot(
	vm: &Vm,
	vcpus: &[Vcpu],
	loaded: &arch::boot::LoadedKernel,
	cmdline: &str,
	initrd: Option<(vm_memory::GuestAddress, usize)>,
	virtio_infos: &[(u64, u32)],
	gic: &Gic,
) -> Result<()> {
	let mpidrs: Vec<u64> = vcpus
		.iter()
		.map(arch::regs::read_mpidr)
		.collect::<Result<_>>()?;
	let fdt_addr = configure_aarch64_fdt(vm, cmdline, initrd, virtio_infos, gic, &mpidrs)?;

	for (id, vcpu) in vcpus.iter().enumerate() {
		arch::configure_vcpu(vcpu, id as u8, loaded.entry, fdt_addr)?;
	}
	Ok(())
}

/// Park a vCPU before it can enter the hypervisor and wait for its resume.
///
/// Returning `true` grants one pass through the vCPU run loop; `false` means
/// the thread must exit without entering the hypervisor.
fn park_vcpu_until_resumed(
	gate: &PauseGate,
	id: u8,
	cmd_rx: &Receiver<VcpuCmd>,
	mut save_state: impl FnMut(Sender<Result<crate::arch::state::VcpuState>>),
) -> bool {
	gate.mark_parked(id as usize);
	loop {
		match cmd_rx.recv() {
			Ok(VcpuCmd::Resume) => return true,
			Ok(VcpuCmd::Stop) | Err(_) => return false,
			Ok(VcpuCmd::SaveState(tx)) => save_state(tx),
		}
	}
}

/// The KVM run loop for one vCPU. Parks on pause, exits on stop/fatal.
#[allow(clippy::too_many_arguments, reason = "Vcpu run loop has many inputs")]
fn run_vcpu(
	mut vcpu: Vcpu,
	pio_bus: Arc<Bus>,
	mmio_bus: Arc<Bus>,
	id: u8,
	gate: Arc<PauseGate>,
	exit_reason: Arc<AtomicU8>,
	cmd_rx: Receiver<VcpuCmd>,
	xsave_size: usize,
) {
	gate.register_tid(id as usize);
	loop {
		match gate.state() {
			RunState::Stopping => return,
			RunState::Paused => {
				// HVF keeps the virtual timer running while a vCPU is parked, so
				// freeze guest virtual time across the pause and restore it on
				// resume. Otherwise the guest sees the wall-clock pause as a
				// forward CNTVCT jump and stalls/panics seconds later. KVM freezes
				// the vtimer itself, so these are HVF-only no-ops elsewhere.
				#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
				vcpu.freeze_vtimer();
				if !park_vcpu_until_resumed(&gate, id, &cmd_rx, |tx| {
					let _ = tx.send(vcpu.save_state(xsave_size));
				}) {
					return;
				}
				#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
				if let Err(e) = vcpu.thaw_vtimer() {
					fatal(&gate, &exit_reason, VmmExit::Killed, id, &format!("vtimer thaw failed: {e}"));
					return;
				}
				continue;
			},
			RunState::Running => {},
		}

		match vcpu.run() {
			Ok(Exit::PioIn { port, data }) => {
				crate::metrics::record_vm_exit(VmExit::IoIn);
				if !pio_bus.read(u64::from(port), data) {
					data.fill(0xff);
				}
			},
			Ok(Exit::PioOut { port, data }) => {
				crate::metrics::record_vm_exit(VmExit::IoOut);
				pio_bus.write(u64::from(port), data);
			},
			Ok(Exit::MmioRead { addr, data }) => {
				crate::metrics::record_vm_exit(VmExit::MmioRead);
				if !mmio_bus.read(addr, data) {
					data.fill(0xff);
				}
				#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
				if !complete_hvf_mmio(&mut vcpu, &gate, &exit_reason, id) {
					return;
				}
			},
			Ok(Exit::MmioWrite { addr, data }) => {
				crate::metrics::record_vm_exit(VmExit::MmioWrite);
				mmio_bus.write(addr, data);
				#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
				if !complete_hvf_mmio(&mut vcpu, &gate, &exit_reason, id) {
					return;
				}
			},
			Ok(Exit::Hlt) => {
				crate::metrics::record_vm_exit(VmExit::Hlt);
			},
			Ok(Exit::Shutdown) => {
				crate::metrics::record_vm_exit(VmExit::Shutdown);
				fatal(&gate, &exit_reason, VmmExit::Shutdown, id, "guest shutdown / triple fault");
				return;
			},
			Ok(Exit::SystemEvent(kind)) => {
				crate::metrics::record_vm_exit(VmExit::SystemEvent);
				fatal(&gate, &exit_reason, VmmExit::Shutdown, id, &format!("system event {kind}"));
				return;
			},
			Ok(Exit::FailEntry { reason, cpu }) => {
				crate::metrics::record_vm_exit(VmExit::FailEntry);
				fatal(
					&gate,
					&exit_reason,
					VmmExit::Killed,
					id,
					&format!("hypervisor entry failed ({reason:#x}) on cpu {cpu}"),
				);
				return;
			},
			Ok(Exit::InternalError) => {
				crate::metrics::record_vm_exit(VmExit::InternalError);
				fatal(&gate, &exit_reason, VmmExit::Killed, id, "hypervisor internal error");
				return;
			},
			Ok(Exit::Continue) => {},
			Ok(Exit::Other) => {
				crate::metrics::record_vm_exit(VmExit::Other);
			},
			Err(e) => {
				fatal(&gate, &exit_reason, VmmExit::Killed, id, &format!("run failed: {e}"));
				return;
			},
		}
	}
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn complete_hvf_mmio(
	vcpu: &mut Vcpu,
	gate: &Arc<PauseGate>,
	exit_reason: &Arc<AtomicU8>,
	id: u8,
) -> bool {
	if let Err(e) = vcpu.complete_mmio() {
		fatal(gate, exit_reason, VmmExit::Killed, id, &format!("completing HVF MMIO failed: {e}"));
		return false;
	}
	true
}

/// Signal a fatal vCPU condition: stop the VM and restore the terminal.
fn fatal(gate: &PauseGate, exit_reason: &AtomicU8, exit: VmmExit, id: u8, reason: &str) {
	set_exit_reason(exit_reason, exit);
	gate.set_state(RunState::Stopping);
	// Kick sibling vCPUs out of KVM_RUN so they observe Stopping and exit
	// (otherwise a no-socket join would block on a halted AP).
	gate.signal_all_vcpus();
	restore_terminal();
	error!("vcpu {id}: {reason}");
}

/// Put the controlling terminal into raw mode (if any) and forward host stdin
/// into the serial receive FIFO.
fn setup_console(serial: Arc<Mutex<SerialDevice>>) {
	// SAFETY: isatty on a fixed fd is always safe.
	if unsafe { libc::isatty(libc::STDIN_FILENO) } == 1 {
		enable_raw_mode();
	}
	thread::spawn(move || {
		let mut input = io::stdin().lock();
		let mut buf = [0u8; 64];
		loop {
			match input.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => serial.lock().enqueue(&buf[..n]),
				Err(_) => break,
			}
		}
	});
}

fn enable_raw_mode() {
	// SAFETY: termios calls on STDIN with valid pointers.
	unsafe {
		let mut term: libc::termios = std::mem::zeroed();
		if libc::tcgetattr(libc::STDIN_FILENO, &mut term) != 0 {
			return;
		}
		let _ = ORIG_TERMIOS.set(term);
		let mut raw = term;
		libc::cfmakeraw(&mut raw);
		raw.c_oflag |= libc::OPOST | libc::ONLCR;
		libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &raw);
	}
}

fn restore_terminal() {
	if let Some(term) = ORIG_TERMIOS.get() {
		// SAFETY: restoring previously saved, valid termios on STDIN.
		unsafe {
			libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, term);
		}
	}
}

#[cfg(test)]
mod backend_restore_tests {
	use std::sync::atomic::AtomicUsize;

	use super::*;

	#[test]
	fn initially_paused_vcpu_waits_before_first_hypervisor_run() {
		let gate = PauseGate::new_paused(1);
		let (tx, rx) = flume::unbounded();
		let run_calls = Arc::new(AtomicUsize::new(0));
		let run_calls_in_vcpu = run_calls.clone();
		let gate_in_vcpu = gate.clone();
		let handle = thread::spawn(move || {
			if park_vcpu_until_resumed(&gate_in_vcpu, 0, &rx, |_| {}) {
				run_calls_in_vcpu.fetch_add(1, Ordering::SeqCst);
			}
		});

		assert!(gate.wait_for_all_parked(Duration::from_secs(1)));
		assert_eq!(run_calls.load(Ordering::SeqCst), 0, "no vcpu.run before resume");

		tx.send(VcpuCmd::Resume).expect("resume parked vCPU");
		handle.join().expect("vCPU gate thread exits");
		assert_eq!(run_calls.load(Ordering::SeqCst), 1);
	}

	fn config(extra: &[&str]) -> Config {
		Config::from_args(
			["--restore", "/snapshot"]
				.into_iter()
				.chain(extra.iter().copied())
				.map(str::to_owned),
		)
		.expect("restore config")
	}
	#[cfg(target_os = "linux")]
	#[test]
	fn restored_remote_block_keeps_remote_base_instead_of_serving_sparse_holes() {
		let dir =
			std::env::temp_dir().join(format!("vmon-restore-remote-block-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir(&dir).expect("create remote restore test dir");
		let cache = dir.join("base.cache");
		let index = dir.join("base.index.json");
		let template_overlay = dir.join("template.img");
		let child_overlay = dir.join("child.img");
		std::fs::write(
			&index,
			br#"{"version":3,"compressed_size":1,"uncompressed_size":512,"original_size":512,"block_size":1048576,"blocks":[[0,1]]}"#,
		)
		.expect("write remote index");
		let common = [
			"--rootfs-remote-url",
			"http://127.0.0.1/object",
			"--rootfs-remote-cache",
			cache.to_str().expect("cache path"),
			"--rootfs-remote-index",
			index.to_str().expect("index path"),
			"--rootfs-remote-size",
			"512",
			"--no-sandbox",
		];
		let template_config = Config::from_args(
			["--restore", "/snapshot", "--rootfs", template_overlay.to_str().expect("template path")]
				.into_iter()
				.chain(common)
				.map(str::to_owned),
		)
		.expect("template config");
		let template =
			open_configured_block(&template_config, &template_overlay, false).expect("template block");
		assert!(template.is_remote_backed_for_test());
		drop(template);

		let child_config = Config::from_args(
			[
				"--restore",
				"/snapshot",
				"--rootfs",
				child_overlay.to_str().expect("child path"),
				"--disk-overlay-of",
				template_overlay.to_str().expect("template path"),
			]
			.into_iter()
			.chain(common)
			.map(str::to_owned),
		)
		.expect("child restore config");
		let child =
			open_configured_block(&child_config, &child_overlay, false).expect("restored child block");
		assert!(
			child.is_remote_backed_for_test(),
			"snapshot restore must not open the sparse overlay as a standalone local disk"
		);
		drop(child);
		std::fs::remove_dir_all(dir).expect("remove remote restore test dir");
	}

	fn device(kind: DeviceKind, backend: BackendHint) -> DeviceState {
		DeviceState {
			kind,
			transport: DeviceTransportKind::Mmio,
			mmio_base: MMIO_MEM_START,
			gsi: IRQ_BASE,
			interrupt_status: 0,
			device_features_select: 0,
			driver_features_select: 0,
			acked_features: 0,
			status: 0,
			activated: false,
			queues: Vec::new(),
			backend,
			user_net_state: None,
			transport_pci: None,
			fs: None,
		}
	}

	#[test]
	fn snapshot_host_paths_require_explicit_rebinding() {
		let cases = [
			device(DeviceKind::Block, BackendHint::Block {
				path:      "/etc/passwd".to_owned(),
				read_only: false,
			}),
			device(DeviceKind::Net, BackendHint::Net {
				tap: "host-tap".to_owned(),
				mac: [2, 0, 0, 0, 0, 1],
			}),
			device(DeviceKind::Fs, BackendHint::Fs {
				tag:        "host".to_owned(),
				shared_dir: "/".to_owned(),
				read_only:  false,
			}),
			device(DeviceKind::RemoteFs, BackendHint::RemoteFs {
				tag:  "objects".to_owned(),
				sock: "/private/host.sock".to_owned(),
			}),
		];
		for state in cases {
			assert!(resolve_backend(&state, &config(&[])).is_err());
		}
	}

	#[test]
	fn explicit_restore_bindings_replace_snapshot_paths() {
		let state = device(DeviceKind::Block, BackendHint::Block {
			path:      "/etc/passwd".to_owned(),
			read_only: false,
		});

		let backend =
			resolve_backend(&state, &config(&["--rootfs", "/safe/rootfs.img"])).expect("binding");
		assert!(matches!(
			backend,
			BackendHint::Block { path, .. } if path == "/safe/rootfs.img"
		));

		let state = device(DeviceKind::Net, BackendHint::Net {
			tap: "host-tap".to_owned(),
			mac: [2, 0, 0, 0, 0, 1],
		});
		let backend = resolve_backend(&state, &config(&["--tap", "safe-tap"])).expect("binding");
		assert!(matches!(backend, BackendHint::Net { tap, .. } if tap == "safe-tap"));
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn restricted_fresh_user_net_persists_its_policy_for_restore() {
		let mac = [0x02, 0, 0, 0, 0, 1];
		let fresh =
			open_fresh_user_net_device(mac, true, Some(8443)).expect("fresh restricted user network");
		let state = fresh
			.lock()
			.user_net_state()
			.expect("save restricted user network")
			.expect("user network state");
		assert_eq!(&state[..8], b"VMUN\x02\x01\x20\xfb");
		drop(fresh);

		let restored =
			open_user_net_device(mac, Some(&state), false, None).expect("restore user network");
		let restored_state = restored
			.lock()
			.user_net_state()
			.expect("save restored user network")
			.expect("user network state");
		assert_eq!(&restored_state[..8], b"VMUN\x02\x01\x20\xfb");
	}
}
