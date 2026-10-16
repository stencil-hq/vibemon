//! VM runtime (ARCHITECTURE.md §8.2).
//!
//! Launches `vmon vmm` children by self-exec, waits for readiness (control
//! `ping`, then GC4 `hello`), and hands out [`Vm`] handles with the control
//! client, the GC4 agent client, and tunnels. Used by the sandbox manager and
//! by template builds.

mod argv;
mod control;

use std::{
	fs,
	io::{self, Read, Seek, SeekFrom},
	os::unix::fs::{DirBuilderExt, PermissionsExt},
	path::{Path, PathBuf},
	process::Stdio,
	sync::Arc,
	time::Duration,
};

use anyhow::{Context, anyhow, bail};
use parking_lot::Mutex;
use serde_json::Value;
use tokio::{
	sync::{Semaphore, watch},
	time::Instant,
};

use self::argv::{VmSockets, vmm_argv};
pub use self::control::ControlClient;
use crate::{
	gc4::{AgentClient, AgentStream, ConnectError},
	net::{NetLease, NetManager},
};

/// How the VM boots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Boot {
	/// Direct kernel boot.
	Cold { kernel: PathBuf, initrd: Option<PathBuf>, cmdline: String },
	/// Copy-on-write fork of a snapshot directory (`--fork-from`): a full
	/// snapshot, or a delta whose base is a sibling full snapshot.
	Fork { snapshot: PathBuf },
	/// Restore of a snapshot whose guest RAM is paged in on first touch from
	/// an HTTP page source (`--restore` + `--remote-page-url`, Linux only):
	/// only the snapshot's state file must be local.
	RemotePages { snapshot: PathBuf, pages: RemotePages },
}

/// HTTP page source of a [`Boot::RemotePages`] restore: `GET <url>/<page>`
/// returns the 4 KiB full-image page with that index.
#[derive(Clone, PartialEq, Eq)]
pub struct RemotePages {
	pub url:   String,
	/// Bearer token (passed to the VMM as `VMON_REMOTE_PAGE_TOKEN`).
	pub token: String,
}

impl std::fmt::Debug for RemotePages {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("RemotePages")
			.field("url", &self.url)
			.finish_non_exhaustive()
	}
}

/// Root disk (`/dev/vda`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Disk {
	/// Use the file in place.
	Direct { path: PathBuf, read_only: bool },
	/// Private copy-on-write overlay `path` (must not exist) of the immutable
	/// `base`.
	Overlay { base: PathBuf, path: PathBuf },
	/// Lazily hydrated remote block image (`--rootfs-remote-*`): `overlay`
	/// takes the guest's writes, `cache` the fetched blocks; `index` is the
	/// image's local seek index and `url` its object, pinned to `etag`.
	Remote(RemoteDisk),
}

/// A [`Disk::Remote`] (see `vmm::block_image`).
#[derive(Clone, PartialEq, Eq)]
pub struct RemoteDisk {
	pub overlay: PathBuf,
	pub cache:   PathBuf,
	pub index:   PathBuf,
	/// Object URL; may carry a pre-signed query (not logged).
	pub url:     String,
	/// Quoted strong `ETag` every range response must carry.
	pub etag:    String,
	/// Guest-visible capacity (512-byte multiple).
	pub size:    u64,
}

impl std::fmt::Debug for RemoteDisk {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("RemoteDisk")
			.field("overlay", &self.overlay)
			.field("cache", &self.cache)
			.field("index", &self.index)
			.field("etag", &self.etag)
			.field("size", &self.size)
			.finish_non_exhaustive()
	}
}

/// Host directory shared over virtio-fs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsShare {
	/// `--fs-tag/--fs-dir` read-only share (at most one per VM).
	ReadOnly { tag: String, dir: PathBuf },
	/// `--volume tag:dir[:ro]` (at most 8 per VM).
	Volume { tag: String, dir: PathBuf, read_only: bool },
}

/// Guest NIC.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VmNet {
	None,
	/// libslirp user-mode NAT (macOS).
	User,
	/// Host TAP (Linux).
	Tap {
		name: String,
		mac:  String,
	},
}

/// Privilege handling of Linux VMMs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VmmIsolation {
	/// VMM defaults (sandbox on; fine for non-root launches).
	Default,
	/// Root launch dropping to this uid/gid (`--sandbox-uid/--sandbox-gid`).
	DropTo { uid: u32, gid: u32 },
	/// `--no-sandbox` (root launch without a configured sandbox user).
	Disabled,
}

/// Everything needed to start one VMM process.
#[derive(Clone, Debug)]
pub struct VmSpec {
	/// Unique instance name: `[a-z0-9-]`, ≤ 40 bytes; names the run dir.
	pub name:          String,
	pub boot:          Boot,
	/// Ignored on fork (the snapshot fixes it).
	pub vcpus:         u32,
	/// Ignored on fork (the snapshot fixes it).
	pub memory_mb:     u64,
	pub disk:          Option<Disk>,
	pub shares:        Vec<FsShare>,
	pub net:           VmNet,
	/// Enables `snapshot` into `<snapshot_root>/<name>`.
	pub snapshot_root: Option<PathBuf>,
	/// Serial console (VMM stdout), appended. VMM logs go to
	/// `<stem>.vmm.log` beside it.
	pub console_log:   PathBuf,
	/// VMM hard deadline (`--timeout-secs`).
	pub timeout:       Option<Duration>,
	/// Budget for control ping + GC4 hello.
	pub ready_timeout: Duration,
	/// Raw extra `vmon vmm` flags (egress user-net policy).
	pub extra_args:    Vec<String>,
}

/// How a VMM process ended.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VmExit {
	/// Process exit code (`None` when killed by a signal or unknown).
	pub code:   Option<i32>,
	/// `status.json` reason: `timeout`, `quit`, `shutdown`, `killed`.
	pub reason: Option<String>,
}

/// Construction parameters of [`VmRuntime`].
#[derive(Debug)]
pub struct VmRuntimeConfig {
	/// The `vmon` binary (`current_exe()`), run as `vmon vmm …`.
	pub vmon_bin:         PathBuf,
	/// Parent of the per-VM socket dirs (short path; 0700).
	pub run_dir:          PathBuf,
	/// Concurrent boots allowed.
	pub boot_concurrency: usize,
	pub isolation:        VmmIsolation,
	pub net:              Arc<NetManager>,
}

struct RuntimeInner {
	config: VmRuntimeConfig,
	boot:   Semaphore,
}

/// Launches and adopts VMs. Cheap to clone.
#[derive(Clone)]
pub struct VmRuntime {
	inner: Arc<RuntimeInner>,
}

impl std::fmt::Debug for VmRuntime {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("VmRuntime")
			.field("config", &self.inner.config)
			.finish_non_exhaustive()
	}
}

/// Network attachment for a template-build VM.
#[derive(Debug)]
pub struct BuildNet {
	/// Pass as [`VmSpec::net`].
	pub net: VmNet,
	lease:   NetLease,
}

impl BuildNet {
	/// Pushes the guest's static IPv4 config after boot.
	pub async fn configure(&self, vm: &Vm) -> anyhow::Result<()> {
		vm.agent().net_config(&self.lease.guest_config()).await
	}
}

fn validate_name(name: &str) -> anyhow::Result<()> {
	if name.is_empty()
		|| name.len() > 40
		|| !name
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
	{
		bail!("invalid vm name {name:?}");
	}
	Ok(())
}

/// Creates `dir` (and parents) with mode 0700, tightening an existing dir.
pub(crate) fn private_dir(dir: &Path) -> io::Result<()> {
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(dir)?;
	fs::set_permissions(dir, fs::Permissions::from_mode(0o700))
}

/// Last `lines` lines of a log file (for launch errors).
pub(crate) fn log_tail(path: &Path, lines: usize) -> String {
	let Ok(mut file) = fs::File::open(path) else {
		return String::new();
	};
	let len = file.metadata().map_or(0, |m| m.len());
	let _ = file.seek(SeekFrom::Start(len.saturating_sub(16 * 1024)));
	let mut text = String::new();
	let _ = file.read_to_string(&mut text);
	let all: Vec<&str> = text.lines().collect();
	all[all.len().saturating_sub(lines)..].join("\n")
}

fn vmm_log_path(console_log: &Path) -> PathBuf {
	let stem = console_log
		.file_stem()
		.and_then(|s| s.to_str())
		.unwrap_or("console");
	console_log.with_file_name(format!("{stem}.vmm.log"))
}

fn read_status(run_dir: &Path) -> Option<String> {
	let text = fs::read_to_string(run_dir.join("status.json")).ok()?;
	let value: Value = serde_json::from_str(&text).ok()?;
	value
		.get("reason")
		.and_then(Value::as_str)
		.map(str::to_owned)
}

fn pid_alive(pid: u32) -> bool {
	let Ok(pid) = libc::pid_t::try_from(pid) else {
		return false;
	};
	// SAFETY: signal 0 only checks for existence/permission.
	let rc = unsafe { libc::kill(pid, 0) };
	rc == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn signal(pid: u32, sig: libc::c_int) {
	if let Ok(pid) = libc::pid_t::try_from(pid) {
		// SAFETY: plain kill(2) on a pid we spawned or adopted.
		unsafe { libc::kill(pid, sig) };
	}
}

/// SIGKILLs a spawned VMM on drop unless disarmed: a launch abandoned by its
/// caller (a cancelled build drops the future) leaves no [`Vm`] to stop it.
struct KillOnDrop(Option<u32>);

impl Drop for KillOnDrop {
	fn drop(&mut self) {
		if let Some(pid) = self.0 {
			signal(pid, libc::SIGKILL);
		}
	}
}

#[cfg(target_os = "linux")]
fn chown_path(path: &Path, uid: u32, gid: u32) -> io::Result<()> {
	std::os::unix::fs::chown(path, Some(uid), Some(gid))
}

impl VmRuntime {
	/// Creates the runtime; the run dir is created 0700.
	pub fn new(config: VmRuntimeConfig) -> anyhow::Result<Self> {
		private_dir(&config.run_dir)
			.with_context(|| format!("creating run dir {}", config.run_dir.display()))?;
		let boot = Semaphore::new(config.boot_concurrency.max(1));
		Ok(Self { inner: Arc::new(RuntimeInner { config, boot }) })
	}

	/// Network manager shared with the sandbox manager.
	pub fn net(&self) -> &Arc<NetManager> {
		&self.inner.config.net
	}

	/// Socket dir of the VM instance `name`.
	pub fn instance_dir(&self, name: &str) -> PathBuf {
		self.inner.config.run_dir.join(name)
	}

	/// Allocates networking for a template-build VM (unrestricted egress).
	pub async fn build_network(&self, name: &str) -> anyhow::Result<BuildNet> {
		validate_name(name)?;
		let lease = self.inner.config.net.acquire_unfiltered().await?;
		Ok(BuildNet { net: lease.vm_net(), lease })
	}

	/// Spawns `vmon vmm` for `spec` and waits until both the control socket
	/// and the guest agent answer. On failure the child is killed and the
	/// error carries the console and VMM log tails.
	pub async fn launch(&self, spec: VmSpec) -> anyhow::Result<Vm> {
		validate_name(&spec.name)?;
		let _permit = self
			.inner
			.boot
			.acquire()
			.await
			.map_err(|_| anyhow!("vm runtime is shut down"))?;
		let run_dir = self.instance_dir(&spec.name);
		if run_dir.exists() {
			fs::remove_dir_all(&run_dir)
				.with_context(|| format!("clearing stale run dir {}", run_dir.display()))?;
		}
		private_dir(&run_dir)?;
		let api_sock = run_dir.join("api.sock");
		let agent_sock = run_dir.join("agent.sock");
		#[cfg(target_os = "linux")]
		if let VmmIsolation::DropTo { uid, gid } = self.inner.config.isolation {
			chown_path(&run_dir, uid, gid)?;
			let private_disk = match &spec.disk {
				Some(Disk::Overlay { path, .. }) => Some(path),
				Some(Disk::Remote(remote)) => Some(&remote.overlay),
				_ => None,
			};
			if let Some(parent) = private_disk.and_then(|path| path.parent()) {
				chown_path(parent, uid, gid)?;
			}
			if let Some(root) = &spec.snapshot_root {
				chown_path(root, uid, gid)?;
			}
		}
		let args = vmm_argv(
			&spec,
			&VmSockets { api: &api_sock, agent: &agent_sock },
			self.inner.config.isolation,
		);
		if let Some(parent) = spec.console_log.parent() {
			fs::create_dir_all(parent)?;
		}
		let console = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&spec.console_log)?;
		let vmm_log_path = vmm_log_path(&spec.console_log);
		let vmm_log = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&vmm_log_path)?;
		let started = Instant::now();
		let mut command = tokio::process::Command::new(&self.inner.config.vmon_bin);
		if let Boot::RemotePages { pages, .. } = &spec.boot {
			command.env("VMON_REMOTE_PAGE_TOKEN", &pages.token);
		}
		let mut child = command
			.arg("vmm")
			.args(&args)
			.stdin(Stdio::null())
			.stdout(console)
			.stderr(vmm_log)
			// VMMs outlive a node restart (restart recovery adopts them).
			.process_group(0)
			.kill_on_drop(false)
			.spawn()
			.with_context(|| format!("spawning {} vmm", self.inner.config.vmon_bin.display()))?;
		let pid = child
			.id()
			.ok_or_else(|| anyhow!("vmm exited immediately"))?;
		let (exit_tx, exit_rx) = watch::channel(None);
		let status_dir = run_dir.clone();
		tokio::spawn(async move {
			let status = child.wait().await;
			let exit =
				VmExit { code: status.ok().and_then(|s| s.code()), reason: read_status(&status_dir) };
			let _ = exit_tx.send(Some(exit));
		});
		let mut abandoned = KillOnDrop(Some(pid));
		let control = ControlClient::new(api_sock);
		let ready = wait_ready(&control, &agent_sock, &exit_rx, spec.ready_timeout).await;
		abandoned.0 = None;
		match ready {
			Ok(agent) => {
				tracing::debug!(vm = %spec.name, pid, elapsed_ms = started.elapsed().as_millis() as u64, "vm ready");
				Ok(Vm::new(
					spec.name,
					pid,
					run_dir,
					control,
					agent,
					agent_sock,
					exit_rx,
					spec.console_log,
				))
			},
			Err(error) => {
				signal(pid, libc::SIGKILL);
				let mut exit_rx = exit_rx;
				let _ = tokio::time::timeout(Duration::from_secs(5), exit_rx.wait_for(Option::is_some))
					.await;
				let console = log_tail(&spec.console_log, 20);
				let vmm = log_tail(&vmm_log_path, 10);
				Err(error.context(format!(
					"vm {} failed to start\n--- console ---\n{console}\n--- vmm ---\n{vmm}",
					spec.name
				)))
			},
		}
	}

	/// Re-attaches to a live VMM left by a previous node process (restart
	/// recovery): verifies the pid, pings the control socket, and reconnects
	/// the agent with a fresh epoch.
	pub async fn adopt(&self, name: &str, pid: u32, console_log: PathBuf) -> anyhow::Result<Vm> {
		validate_name(name)?;
		if !pid_alive(pid) {
			bail!("vmm pid {pid} is gone");
		}
		let run_dir = self.instance_dir(name);
		let control = ControlClient::new(run_dir.join("api.sock"));
		control.ping().await?;
		let agent_sock = run_dir.join("agent.sock");
		let agent = AgentClient::connect(&agent_sock, Duration::from_secs(10)).await?;
		let (exit_tx, exit_rx) = watch::channel(None);
		let status_dir = run_dir.clone();
		tokio::spawn(async move {
			while pid_alive(pid) {
				tokio::time::sleep(Duration::from_millis(500)).await;
			}
			let _ = exit_tx.send(Some(VmExit { code: None, reason: read_status(&status_dir) }));
		});
		Ok(Vm::new(name.to_owned(), pid, run_dir, control, agent, agent_sock, exit_rx, console_log))
	}
}

async fn wait_ready(
	control: &ControlClient,
	agent_sock: &Path,
	exit: &watch::Receiver<Option<VmExit>>,
	timeout: Duration,
) -> anyhow::Result<AgentClient> {
	let deadline = Instant::now() + timeout;
	loop {
		let exited = exit.borrow().clone();
		if let Some(exit) = exited {
			bail!("vmm exited during startup (code {:?}, reason {:?})", exit.code, exit.reason);
		}
		if control.path().exists() && control.ping().await.is_ok() {
			break;
		}
		if Instant::now() >= deadline {
			bail!("vmm control socket did not come up within {timeout:?}");
		}
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
	let remaining = deadline
		.saturating_duration_since(Instant::now())
		.max(Duration::from_millis(100));
	let mut exit = exit.clone();
	tokio::select! {
		agent = AgentClient::connect(agent_sock, remaining) => agent,
		_ = exit.wait_for(Option::is_some) => bail!("vmm exited before the guest agent answered"),
	}
}

struct VmInner {
	name:        String,
	pid:         u32,
	run_dir:     PathBuf,
	control:     ControlClient,
	agent:       Mutex<AgentClient>,
	agent_sock:  PathBuf,
	exit:        watch::Receiver<Option<VmExit>>,
	console_log: PathBuf,
}

/// Handle to one running VMM. Cheap to clone; dropping it does not stop the VM.
#[derive(Clone)]
pub struct Vm {
	inner: Arc<VmInner>,
}

impl std::fmt::Debug for Vm {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Vm")
			.field("name", &self.inner.name)
			.field("pid", &self.inner.pid)
			.finish_non_exhaustive()
	}
}

impl Vm {
	fn new(
		name: String,
		pid: u32,
		run_dir: PathBuf,
		control: ControlClient,
		agent: AgentClient,
		agent_sock: PathBuf,
		exit: watch::Receiver<Option<VmExit>>,
		console_log: PathBuf,
	) -> Self {
		Self {
			inner: Arc::new(VmInner {
				name,
				pid,
				run_dir,
				control,
				agent: Mutex::new(agent),
				agent_sock,
				exit,
				console_log,
			}),
		}
	}

	/// Instance name.
	pub fn name(&self) -> &str {
		&self.inner.name
	}

	/// VMM process id.
	pub fn pid(&self) -> u32 {
		self.inner.pid
	}

	/// Per-VM socket directory.
	pub fn run_dir(&self) -> &Path {
		&self.inner.run_dir
	}

	/// Serial console log path.
	pub fn console_log(&self) -> &Path {
		&self.inner.console_log
	}

	/// Control-socket client.
	pub fn control(&self) -> &ControlClient {
		&self.inner.control
	}

	/// Current GC4 connection.
	pub fn agent(&self) -> AgentClient {
		self.inner.agent.lock().clone()
	}

	/// The current GC4 connection, reconnecting (new epoch) if it dropped.
	pub async fn agent_connected(&self) -> anyhow::Result<AgentClient> {
		let current = self.agent();
		if !current.is_closed() {
			return Ok(current);
		}
		self.reconnect_agent().await
	}

	/// Replaces the GC4 connection with a fresh one (new epoch).
	pub async fn reconnect_agent(&self) -> anyhow::Result<AgentClient> {
		let agent = AgentClient::connect(&self.inner.agent_sock, Duration::from_secs(10)).await?;
		*self.inner.agent.lock() = agent.clone();
		Ok(agent)
	}

	/// Whether the VMM process has exited.
	pub fn exited(&self) -> Option<VmExit> {
		self.inner.exit.borrow().clone()
	}

	/// Opens a byte stream to guest `127.0.0.1:port`.
	pub async fn tcp_connect(&self, port: u16) -> Result<AgentStream, ConnectError> {
		let agent = self
			.agent_connected()
			.await
			.map_err(|e| ConnectError::Failed(e.to_string()))?;
		agent.tcp_connect(port).await
	}

	/// Pauses vCPUs and devices.
	pub async fn pause(&self) -> anyhow::Result<()> {
		self.inner.control.pause().await
	}

	/// Resumes a paused VM.
	pub async fn resume(&self) -> anyhow::Result<()> {
		self.inner.control.resume().await
	}

	/// Writes a full snapshot to `<snapshot_root>/<name>` (VM must be paused).
	pub async fn snapshot(&self, name: &str) -> anyhow::Result<()> {
		self.inner.control.snapshot(name).await
	}

	/// Writes `<snapshot_root>/<name>` as a delta against the sibling full
	/// snapshot `<snapshot_root>/<base>` (VM must be paused).
	pub async fn snapshot_delta(&self, name: &str, base: &str) -> anyhow::Result<()> {
		self.inner.control.snapshot_delta(name, base).await
	}

	/// Asks the VMM to exit (does not wait).
	pub async fn quit(&self) -> anyhow::Result<()> {
		self.inner.control.quit().await
	}

	/// Waits for the VMM process to exit.
	pub async fn wait(&self) -> VmExit {
		let mut exit = self.inner.exit.clone();
		match exit.wait_for(Option::is_some).await {
			Ok(exit) => exit.clone().unwrap_or_default(),
			Err(_) => VmExit::default(),
		}
	}

	/// `quit`, wait up to `grace`, then SIGKILL.
	pub async fn shutdown(&self, grace: Duration) -> VmExit {
		if self.exited().is_none() {
			if let Err(error) = self.quit().await {
				tracing::debug!(vm = %self.name(), %error, "vmm quit failed; killing");
			}
			if let Ok(exit) = tokio::time::timeout(grace, self.wait()).await {
				return exit;
			}
		}
		self.kill().await
	}

	/// SIGKILLs the VMM without waiting for it to exit (see [`Self::wait`]).
	pub fn terminate(&self) {
		if self.exited().is_none() {
			signal(self.inner.pid, libc::SIGKILL);
		}
	}

	/// SIGKILLs the VMM and waits for it.
	pub async fn kill(&self) -> VmExit {
		self.terminate();
		self.wait().await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn names_are_restricted_to_short_path_safe_tokens() {
		assert!(validate_name("isbx0123456789abcdefgh").is_ok());
		assert!(validate_name("build-1-steps").is_ok());
		for bad in ["", "Up", "a/b", "..", "a b", &"x".repeat(41)] {
			assert!(validate_name(bad).is_err(), "{bad:?} must be rejected");
		}
	}

	#[test]
	fn log_tail_returns_last_lines_only() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("console.log");
		fs::write(
			&path,
			(0..100)
				.map(|i| format!("line {i}"))
				.collect::<Vec<_>>()
				.join("\n"),
		)
		.unwrap();
		assert_eq!(log_tail(&path, 2), "line 98\nline 99");
		assert_eq!(log_tail(&dir.path().join("missing"), 2), "");
		assert_eq!(vmm_log_path(&path), dir.path().join("console.vmm.log"));
	}

	#[tokio::test]
	async fn abandoned_launch_kills_the_vmm() {
		let dir = tempfile::tempdir().unwrap();
		// A stand-in VMM that records its pid and never becomes ready.
		let pid_file = dir.path().join("pid");
		let fake = dir.path().join("vmon");
		fs::write(&fake, format!("#!/bin/sh\necho $$ > '{}'\nexec sleep 60\n", pid_file.display()))
			.unwrap();
		fs::set_permissions(&fake, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
		let runtime = VmRuntime::new(VmRuntimeConfig {
			vmon_bin:         fake,
			run_dir:          dir.path().join("run"),
			boot_concurrency: 1,
			isolation:        VmmIsolation::Default,
			net:              NetManager::new(crate::net::NetMode::Auto, "172.20.0.0/16", None)
				.unwrap(),
		})
		.unwrap();
		let spec = VmSpec {
			name:          "abandoned".into(),
			boot:          Boot::Cold {
				kernel:  dir.path().join("kernel"),
				initrd:  None,
				cmdline: String::new(),
			},
			vcpus:         1,
			memory_mb:     128,
			disk:          None,
			shares:        Vec::new(),
			net:           VmNet::None,
			snapshot_root: None,
			console_log:   dir.path().join("console.log"),
			timeout:       None,
			ready_timeout: Duration::from_mins(1),
			extra_args:    Vec::new(),
		};
		// The caller gives up while the VMM boots (a cancelled build), which
		// drops the launch future.
		let pid = tokio::select! {
			result = runtime.launch(spec) => panic!("launch returned: {:?}", result.err()),
			pid = async {
				loop {
					if let Some(pid) = fs::read_to_string(&pid_file)
						.ok()
						.and_then(|text| text.trim().parse::<u32>().ok())
					{
						break pid;
					}
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
			} => pid,
		};
		let deadline = Instant::now() + Duration::from_secs(5);
		while pid_alive(pid) && Instant::now() < deadline {
			tokio::time::sleep(Duration::from_millis(20)).await;
		}
		let alive = pid_alive(pid);
		if alive {
			signal(pid, libc::SIGKILL);
		}
		assert!(!alive, "the abandoned VMM is killed");
	}
}
