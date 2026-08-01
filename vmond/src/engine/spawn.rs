//! vmm process management: argv construction, spawn, readiness, stop
//! escalation. Port of python/vmon/vmm.py.

use std::{
	collections::{BTreeMap, HashMap},
	fs::{self, OpenOptions},
	io::{self, Write},
	os::unix::{fs::PermissionsExt, process::CommandExt},
	path::{Path, PathBuf},
	process::{Child, Command, Stdio},
	sync::{Arc, LazyLock, Mutex},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Value, json};

use crate::{
	doctor,
	engine::control::ControlClient,
	error::{EngineError, Result},
	home::state_dir,
};

const DEFAULT_MEM_MIB: u64 = 512;
const DEFAULT_CPUS: u64 = 1;
pub(crate) const MAX_CPUS: u64 = 64;
pub(crate) const MAX_MEM_MIB: u64 = 64 * 1024;
const MAX_TIMEOUT_SECS: u64 = 86_400;
const DEFAULT_LAUNCH_TIMEOUT: Duration = Duration::from_secs(15);
const STOP_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const READY_PROBE_TIMEOUT: Duration = Duration::from_millis(50);
const READY_SLEEP: Duration = Duration::from_millis(2);
const WAIT_POLL: Duration = Duration::from_millis(20);

/// Process-wide locks for every shared sandbox metadata path.
static META_PATH_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> =
	LazyLock::new(|| Mutex::new(HashMap::new()));

/// Atomically replace one metadata object while serializing all in-process
/// writers for its path.
pub(crate) fn replace_meta_map(path: &Path, metadata: &Map<String, Value>) -> Result<()> {
	let lock = meta_path_lock(path)?;
	let _guard = lock
		.lock()
		.map_err(|_| EngineError::engine(format!("metadata lock poisoned: {}", path.display())))?;
	write_meta_map_atomically(path, metadata)
}

/// Merge metadata updates without allowing legacy VMM writers to overwrite an
/// already durable lifecycle/status generation.
pub(crate) fn merge_meta_map(path: &Path, updates: Map<String, Value>) -> Result<()> {
	let lock = meta_path_lock(path)?;
	let _guard = lock
		.lock()
		.map_err(|_| EngineError::engine(format!("metadata lock poisoned: {}", path.display())))?;
	let mut metadata = read_meta_map(path)?;
	let lifecycle_managed = metadata.contains_key("lifecycle")
		|| metadata.contains_key("desired_state")
		|| metadata.contains_key("observed_state")
		|| metadata.contains_key("state_generation");
	for (key, value) in updates {
		if lifecycle_managed
			&& matches!(
				key.as_str(),
				"lifecycle"
					| "desired_state"
					| "observed_state"
					| "state_generation"
					| "lifecycle_failure"
					| "status"
			) {
			continue;
		}
		metadata.insert(key, value);
	}
	write_meta_map_atomically(path, &metadata)
}

fn meta_path_lock(path: &Path) -> Result<Arc<Mutex<()>>> {
	let canonical = path.to_path_buf();
	let mut locks = META_PATH_LOCKS.lock().map_err(|_| {
		EngineError::engine(format!("metadata lock registry poisoned: {}", path.display()))
	})?;
	Ok(locks.entry(canonical).or_default().clone())
}

fn read_meta_map(path: &Path) -> Result<Map<String, Value>> {
	match fs::read_to_string(path) {
		Ok(text) => match serde_json::from_str::<Value>(&text)? {
			Value::Object(map) => Ok(map),
			_ => {
				Err(EngineError::invalid(format!("metadata {} is not a JSON object", path.display())))
			},
		},
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Map::new()),
		Err(error) => Err(error.into()),
	}
}

fn write_meta_map_atomically(path: &Path, metadata: &Map<String, Value>) -> Result<()> {
	let parent = path.parent().ok_or_else(|| {
		EngineError::invalid(format!("metadata path {} has no parent", path.display()))
	})?;
	fs::create_dir_all(parent)?;
	let temporary = parent.join(format!(
		".{}.{}.{}.tmp",
		path
			.file_name()
			.and_then(|name| name.to_str())
			.unwrap_or("meta"),
		std::process::id(),
		SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap_or_default()
			.as_nanos(),
	));
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&temporary)?;
	let result = (|| -> Result<()> {
		file.write_all(&serde_json::to_vec_pretty(&Value::Object(metadata.clone()))?)?;
		file.sync_all()?;
		replace_file_atomically(&temporary, path)?;
		sync_parent_directory(parent)?;
		Ok(())
	})();
	if result.is_err() {
		let _ = fs::remove_file(&temporary);
	}
	result
}

#[cfg(not(windows))]
fn replace_file_atomically(temporary: &Path, destination: &Path) -> io::Result<()> {
	fs::rename(temporary, destination)
}

#[cfg(windows)]
fn replace_file_atomically(temporary: &Path, destination: &Path) -> io::Result<()> {
	use std::os::windows::ffi::OsStrExt;

	unsafe extern "system" {
		fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
	}

	const MOVEFILE_REPLACE_EXISTING: u32 = 0x1;
	const MOVEFILE_WRITE_THROUGH: u32 = 0x8;
	let temporary = temporary
		.as_os_str()
		.encode_wide()
		.chain(Some(0))
		.collect::<Vec<_>>();
	let destination = destination
		.as_os_str()
		.encode_wide()
		.chain(Some(0))
		.collect::<Vec<_>>();
	if unsafe {
		MoveFileExW(
			temporary.as_ptr(),
			destination.as_ptr(),
			MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
		)
	} == 0
	{
		return Err(io::Error::last_os_error());
	}
	Ok(())
}

#[cfg(not(windows))]
fn sync_parent_directory(parent: &Path) -> io::Result<()> {
	fs::File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent_directory(_parent: &Path) -> io::Result<()> {
	Ok(())
}
/// Kind of VMM launch to construct.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LaunchMode {
	/// Fresh direct-kernel boot.
	Boot,
	/// Warm-boot from a snapshot directory.
	Restore { snapshot: PathBuf },
	/// `CoW` fork from a template snapshot directory.
	Fork { snapshot: PathBuf },
}

/// Network device requested for launch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NetworkMode {
	/// VMM user-mode NAT networking.
	User,
	/// Host TAP/vmnet device by name.
	Tap(String),
}

/// A `--disk-overlay-of BASE --rootfs ROOTFS` pair.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootfsOverlay {
	/// Read-only backing disk image.
	pub base:   PathBuf,
	/// Overlay image path created by the VMM.
	pub rootfs: PathBuf,
}

/// A read-only host directory exposed by virtio-fs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FsShare {
	/// Guest-visible virtio-fs mount tag.
	pub tag: String,
	/// Host directory backing the share.
	pub dir: PathBuf,
}

/// One repeatable `--volume tag:dir[:ro]` mount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumeMount {
	/// Guest-visible virtio-fs tag.
	pub tag:       String,
	/// Host directory backing the mount.
	pub host_dir:  PathBuf,
	/// Whether the guest sees the mount read-only.
	pub read_only: bool,
}

impl VolumeMount {
	/// Validate and create a repeatable VMM volume mount.
	pub fn new(
		tag: impl Into<String>,
		host_dir: impl Into<PathBuf>,
		read_only: bool,
	) -> Result<Self> {
		let mount = Self { tag: tag.into(), host_dir: host_dir.into(), read_only };
		validate_volume_mount(&mount)?;
		Ok(mount)
	}
}

/// One read-only `--remote-fs tag:socket` object-proxy mount.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RemoteFsShare {
	/// Guest-visible virtio-fs tag.
	pub tag:  String,
	/// Absolute Unix socket path for the per-VM proxy.
	pub sock: PathBuf,
}

/// Builder-style launch specification consumed by [`build_launch_args`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LaunchSpec {
	/// Launch path: boot, restore, or fork.
	pub mode: LaunchMode,
	/// Control socket path passed as `--api-sock`.
	pub api_sock: PathBuf,
	/// Keep the VMM paused until a later internal control-plane resume.
	///
	/// This is a daemon lifecycle primitive, not a public sandbox-create field.
	pub start_paused: bool,
	/// Kernel image for fresh boots.
	pub kernel: Option<PathBuf>,
	/// Guest kernel command line.
	pub cmdline: Option<String>,
	/// Rootfs path passed as `--rootfs`.
	pub rootfs: Option<PathBuf>,
	/// Optional overlay backing disk passed before `--rootfs`.
	pub disk_overlay_of: Option<PathBuf>,
	/// Whether direct rootfs boots add `--rootfs-ro`.
	pub rootfs_read_only: bool,
	/// Memory size in MiB.
	pub mem_mib: Option<u64>,
	/// vCPU count.
	pub cpus: Option<u64>,
	/// Guest-agent Unix socket path.
	pub agent_sock: Option<PathBuf>,
	/// Optional network mode.
	pub network: Option<NetworkMode>,
	/// Restrict user-mode networking to slirp-provided services.
	///
	/// This is valid only when [`Self::network`] is [`Some(NetworkMode::User)`].
	pub user_net_restricted: bool,
	/// TCP port on the virtual host gateway allowed by restricted user
	/// networking.
	pub user_net_allow_host_port: Option<u16>,
	/// Optional guest MAC override.
	pub mac: Option<String>,
	/// Attach virtio-rng.
	pub rng: bool,
	/// Optional read-only host directory share.
	pub fs_share: Option<FsShare>,
	/// Repeatable virtio-fs volume mounts.
	pub volumes: Vec<VolumeMount>,
	/// Repeatable object-proxy virtio-fs mounts.
	pub remote_fs: Vec<RemoteFsShare>,
	/// Optional wall-clock timeout in seconds.
	pub timeout_secs: Option<u64>,
	/// Optional production ownership watchdog initial grace in seconds.
	///
	/// This is intentionally absent for single-node launches.
	pub owner_lease_secs: Option<u64>,
	/// Snapshot root passed to the VMM.
	pub snapshot_root: Option<PathBuf>,
	/// Mesh lazy-page HTTP source.
	pub remote_page_url: Option<String>,
	/// Bearer token exported as `VMON_REMOTE_PAGE_TOKEN` when remote paging.
	pub remote_page_token: Option<String>,
	/// Digest recorded in metadata for remote paging.
	pub remote_page_digest: Option<String>,
	/// Explicit `--console-agent` flag.
	pub console_agent: bool,
	/// Image reference recorded in metadata for fresh boots.
	pub image: Option<String>,
}

impl LaunchSpec {
	/// Start a fresh rootfs boot spec.
	pub fn boot_rootfs(
		api_sock: impl Into<PathBuf>,
		kernel: impl Into<PathBuf>,
		rootfs: impl Into<PathBuf>,
	) -> Self {
		Self {
			mode: LaunchMode::Boot,
			api_sock: api_sock.into(),
			start_paused: false,
			kernel: Some(kernel.into()),
			cmdline: Some(default_cmdline(false)),
			rootfs: Some(rootfs.into()),
			disk_overlay_of: None,
			rootfs_read_only: false,
			mem_mib: Some(DEFAULT_MEM_MIB),
			cpus: Some(DEFAULT_CPUS),
			agent_sock: None,
			network: None,
			user_net_restricted: false,
			user_net_allow_host_port: None,
			mac: None,
			rng: false,
			fs_share: None,
			volumes: Vec::new(),
			remote_fs: Vec::new(),
			timeout_secs: None,
			owner_lease_secs: None,
			snapshot_root: None,
			remote_page_url: None,
			remote_page_token: None,
			remote_page_digest: None,
			console_agent: false,
			image: None,
		}
	}

	/// Start a warm-restore spec.
	pub fn restore(api_sock: impl Into<PathBuf>, snapshot: impl Into<PathBuf>) -> Self {
		Self::base(api_sock, LaunchMode::Restore { snapshot: snapshot.into() })
	}

	/// Start a `CoW` fork spec.
	pub fn fork_from(api_sock: impl Into<PathBuf>, snapshot: impl Into<PathBuf>) -> Self {
		Self::base(api_sock, LaunchMode::Fork { snapshot: snapshot.into() })
	}

	fn base(api_sock: impl Into<PathBuf>, mode: LaunchMode) -> Self {
		Self {
			mode,
			api_sock: api_sock.into(),
			start_paused: false,
			kernel: None,
			cmdline: None,
			rootfs: None,
			disk_overlay_of: None,
			rootfs_read_only: false,
			mem_mib: None,
			cpus: None,
			agent_sock: None,
			network: None,
			user_net_restricted: false,
			user_net_allow_host_port: None,
			mac: None,
			rng: false,
			fs_share: None,
			volumes: Vec::new(),
			remote_fs: Vec::new(),
			timeout_secs: None,
			owner_lease_secs: None,
			snapshot_root: None,
			remote_page_url: None,
			remote_page_token: None,
			remote_page_digest: None,
			console_agent: false,
			image: None,
		}
	}

	/// Use the guest-agent init command line and socket.
	pub fn with_agent_sock(mut self, agent_sock: impl Into<PathBuf>) -> Self {
		self.agent_sock = Some(agent_sock.into());
		if matches!(self.mode, LaunchMode::Boot) {
			self.cmdline = Some(default_cmdline(true));
		}
		self
	}

	/// Set memory in MiB.
	pub const fn with_mem_mib(mut self, mem_mib: u64) -> Self {
		self.mem_mib = Some(mem_mib);
		self
	}

	/// Set vCPU count.
	pub const fn with_cpus(mut self, cpus: u64) -> Self {
		self.cpus = Some(cpus);
		self
	}

	/// Keep the launched VMM paused until an internal resume command.
	pub const fn with_start_paused(mut self) -> Self {
		self.start_paused = true;
		self
	}

	/// Use a `CoW` disk overlay path.
	pub fn with_disk_overlay(
		mut self,
		base: impl Into<PathBuf>,
		rootfs: impl Into<PathBuf>,
	) -> Self {
		self.disk_overlay_of = Some(base.into());
		self.rootfs = Some(rootfs.into());
		self
	}

	/// Mark direct rootfs boots read-only.
	pub const fn with_rootfs_read_only(mut self) -> Self {
		self.rootfs_read_only = true;
		self
	}

	/// Attach a host TAP/vmnet interface.
	pub fn with_tap(mut self, tap: impl Into<String>) -> Self {
		self.network = Some(NetworkMode::Tap(tap.into()));
		self
	}

	/// Attach user-mode NAT networking.
	pub fn with_user_net(mut self) -> Self {
		self.network = Some(NetworkMode::User);
		self
	}

	/// Restrict user-mode networking to slirp-provided services and one TCP
	/// port on its virtual host gateway.
	///
	/// The resulting launch emits `--user-net-restricted` and
	/// `--user-net-allow-host-port` alongside `--net user`.
	pub const fn with_restricted_user_net(mut self, allowed_host_port: u16) -> Self {
		self.user_net_restricted = true;
		self.user_net_allow_host_port = Some(allowed_host_port);
		self
	}

	/// Override the guest MAC address.
	pub fn with_mac(mut self, mac: impl Into<String>) -> Self {
		self.mac = Some(mac.into());
		self
	}

	/// Attach virtio-rng.
	pub const fn with_rng(mut self) -> Self {
		self.rng = true;
		self
	}

	/// Attach a read-only host virtio-fs share.
	pub fn with_fs_share(mut self, tag: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
		self.fs_share = Some(FsShare { tag: tag.into(), dir: dir.into() });
		self
	}

	/// Add one repeatable virtio-fs volume mount.
	pub fn with_volume(mut self, mount: VolumeMount) -> Self {
		self.volumes.push(mount);
		self
	}

	/// Add one object-proxy virtio-fs mount.
	pub fn with_remote_fs(mut self, mount: RemoteFsShare) -> Self {
		self.remote_fs.push(mount);
		self
	}

	/// Set wall-clock timeout seconds.
	pub const fn with_timeout_secs(mut self, timeout_secs: u64) -> Self {
		self.timeout_secs = Some(timeout_secs);
		self
	}

	/// Set the production ownership watchdog initial grace seconds.
	pub const fn with_owner_lease_secs(mut self, owner_lease_secs: u64) -> Self {
		self.owner_lease_secs = Some(owner_lease_secs);
		self
	}

	/// Set the snapshot root directory.
	pub fn with_snapshot_root(mut self, snapshot_root: impl Into<PathBuf>) -> Self {
		self.snapshot_root = Some(snapshot_root.into());
		self
	}

	/// Configure lazy remote page restore.
	pub fn with_remote_page(
		mut self,
		url: impl Into<String>,
		token: Option<String>,
		digest: Option<String>,
	) -> Self {
		self.remote_page_url = Some(url.into());
		self.remote_page_token = token;
		self.remote_page_digest = digest;
		self
	}

	/// Add explicit `--console-agent`.
	pub const fn with_console_agent(mut self) -> Self {
		self.console_agent = true;
		self
	}

	/// Validate resource bounds and cross-field launch invariants.
	pub fn validate(&self) -> Result<()> {
		if matches!(self.mode, LaunchMode::Boot) && self.kernel.is_none() {
			return Err(EngineError::invalid("boot launch requires a kernel"));
		}
		if matches!(self.mode, LaunchMode::Boot) && self.rootfs.is_none() {
			return Err(EngineError::invalid("boot launch requires a rootfs"));
		}
		if self.disk_overlay_of.is_some() && self.rootfs.is_none() {
			return Err(EngineError::invalid("disk overlay requires a rootfs path"));
		}
		if self.remote_page_url.is_some() && !matches!(self.mode, LaunchMode::Restore { .. }) {
			return Err(EngineError::invalid("remote page restore requires --restore"));
		}
		if self.user_net_restricted && !matches!(self.network, Some(NetworkMode::User)) {
			return Err(EngineError::invalid(
				"restricted user networking requires user-mode networking",
			));
		}
		if self.user_net_restricted && self.user_net_allow_host_port.is_none() {
			return Err(EngineError::invalid(
				"restricted user networking requires an allowed host port",
			));
		}
		if self.user_net_allow_host_port == Some(0) {
			return Err(EngineError::invalid("restricted user-network host port must be nonzero"));
		}
		if !self.user_net_restricted && self.user_net_allow_host_port.is_some() {
			return Err(EngineError::invalid(
				"an allowed host port requires restricted user networking",
			));
		}
		if let Some(mem_mib) = self.mem_mib {
			validate_range(mem_mib, "mem", MAX_MEM_MIB, " MiB")?;
		}
		if let Some(cpus) = self.cpus {
			validate_range(cpus, "cpus", MAX_CPUS, "")?;
		}
		if let Some(timeout_secs) = self.timeout_secs {
			validate_range(timeout_secs, "timeout_secs", MAX_TIMEOUT_SECS, "")?;
		}
		if let Some(owner_lease_secs) = self.owner_lease_secs {
			validate_range(owner_lease_secs, "owner_lease_secs", MAX_TIMEOUT_SECS, "")?;
		}
		for volume in &self.volumes {
			validate_volume_mount(volume)?;
		}
		for mount in &self.remote_fs {
			validate_remote_fs_share(mount)?;
		}
		Ok(())
	}

	/// Validate and build deterministic VMM CLI flags.
	pub fn try_build_args(&self) -> Result<Vec<String>> {
		self.validate()?;
		Ok(build_launch_args(self))
	}
}

/// Build deterministic VMM flags from a validated launch spec.
pub fn build_launch_args(spec: &LaunchSpec) -> Vec<String> {
	let mut args = Vec::new();
	match &spec.mode {
		LaunchMode::Boot => {
			if let Some(kernel) = &spec.kernel {
				push_arg(&mut args, "--kernel", kernel);
			}
			push_arg(&mut args, "--mem", spec.mem_mib.unwrap_or(DEFAULT_MEM_MIB).to_string());
			push_arg(&mut args, "--cpus", spec.cpus.unwrap_or(DEFAULT_CPUS).to_string());
			push_arg(&mut args, "--api-sock", &spec.api_sock);
			push_arg(
				&mut args,
				"--cmdline",
				spec
					.cmdline
					.clone()
					.unwrap_or_else(|| default_cmdline(spec.agent_sock.is_some())),
			);
			append_rootfs_args(&mut args, spec, true);
		},
		LaunchMode::Restore { snapshot } => {
			push_arg(&mut args, "--restore", snapshot);
			push_arg(&mut args, "--api-sock", &spec.api_sock);
			append_network_args(&mut args, spec);
			if let Some(mem_mib) = spec.mem_mib {
				push_arg(&mut args, "--mem", mem_mib.to_string());
			}
			if let Some(cpus) = spec.cpus {
				push_arg(&mut args, "--cpus", cpus.to_string());
			}
			append_rootfs_args(&mut args, spec, false);
			append_after_disk_args(&mut args, spec, AfterDiskFlags {
				agent:  true,
				rng:    false,
				fs:     true,
				remote: true,
			});
			append_start_paused_arg(&mut args, spec);
			return args;
		},
		LaunchMode::Fork { snapshot } => {
			push_arg(&mut args, "--fork-from", snapshot);
			push_arg(&mut args, "--api-sock", &spec.api_sock);
			append_rootfs_args(&mut args, spec, false);
			append_after_disk_args(&mut args, spec, AfterDiskFlags {
				agent:  true,
				rng:    false,
				fs:     false,
				remote: false,
			});
			append_start_paused_arg(&mut args, spec);
			return args;
		},
	}
	append_network_args(&mut args, spec);
	append_after_disk_args(&mut args, spec, AfterDiskFlags {
		agent:  true,
		rng:    true,
		fs:     true,
		remote: false,
	});
	append_start_paused_arg(&mut args, spec);
	args
}

fn append_start_paused_arg(args: &mut Vec<String>, spec: &LaunchSpec) {
	if spec.start_paused {
		args.push("--start-paused".to_owned());
	}
}

fn append_rootfs_args(args: &mut Vec<String>, spec: &LaunchSpec, allow_direct_read_only: bool) {
	match (&spec.disk_overlay_of, &spec.rootfs) {
		(Some(base), Some(rootfs)) => {
			push_arg(args, "--disk-overlay-of", base);
			push_arg(args, "--rootfs", rootfs);
		},
		(None, Some(rootfs)) => {
			push_arg(args, "--rootfs", rootfs);
			if allow_direct_read_only && spec.rootfs_read_only {
				args.push("--rootfs-ro".to_owned());
			}
		},
		_ => {},
	}
}

fn append_network_args(args: &mut Vec<String>, spec: &LaunchSpec) {
	match &spec.network {
		Some(NetworkMode::User) => {
			push_arg(args, "--net", "user");
			if let Some(allowed_host_port) = spec.user_net_allow_host_port {
				args.push("--user-net-restricted".to_owned());
				push_arg(args, "--user-net-allow-host-port", allowed_host_port.to_string());
			}
		},
		Some(NetworkMode::Tap(tap)) => push_arg(args, "--tap", tap),
		None => {},
	}
	if let Some(mac) = &spec.mac {
		push_arg(args, "--mac", mac);
	}
}

#[derive(Clone, Copy)]
struct AfterDiskFlags {
	agent:  bool,
	rng:    bool,
	fs:     bool,
	remote: bool,
}

fn append_after_disk_args(args: &mut Vec<String>, spec: &LaunchSpec, flags: AfterDiskFlags) {
	if flags.agent
		&& let Some(agent_sock) = &spec.agent_sock
	{
		push_arg(args, "--agent-sock", agent_sock);
	}
	if flags.rng && spec.rng {
		args.push("--rng".to_owned());
	}
	if flags.fs
		&& let Some(share) = &spec.fs_share
	{
		push_arg(args, "--fs-tag", &share.tag);
		push_arg(args, "--fs-dir", &share.dir);
	}
	args.extend(volume_args(&spec.volumes));
	args.extend(remote_fs_args(&spec.remote_fs));
	if let Some(timeout_secs) = spec.timeout_secs {
		push_arg(args, "--timeout-secs", timeout_secs.to_string());
	}
	if let Some(owner_lease_secs) = spec.owner_lease_secs {
		push_arg(args, "--owner-lease-secs", owner_lease_secs.to_string());
	}
	if let Some(snapshot_root) = &spec.snapshot_root {
		push_arg(args, "--snapshot-root", snapshot_root);
	}
	if flags.remote
		&& let Some(remote_page_url) = &spec.remote_page_url
	{
		push_arg(args, "--remote-page-url", remote_page_url);
	}
	if spec.console_agent {
		args.push("--console-agent".to_owned());
	}
}

fn push_arg(args: &mut Vec<String>, flag: &str, value: impl ToArgString) {
	args.push(flag.to_owned());
	args.push(value.to_arg_string());
}

trait ToArgString {
	fn to_arg_string(&self) -> String;
}

impl ToArgString for String {
	fn to_arg_string(&self) -> String {
		self.clone()
	}
}
impl ToArgString for &String {
	fn to_arg_string(&self) -> String {
		(*self).clone()
	}
}

impl ToArgString for &str {
	fn to_arg_string(&self) -> String {
		(*self).to_owned()
	}
}

impl ToArgString for PathBuf {
	fn to_arg_string(&self) -> String {
		path_string(self)
	}
}

impl ToArgString for &PathBuf {
	fn to_arg_string(&self) -> String {
		path_string(self)
	}
}

impl ToArgString for &Path {
	fn to_arg_string(&self) -> String {
		path_string(self)
	}
}

/// Format repeatable `--volume` flags exactly like Python's `_volume_args`.
pub fn volume_args(volumes: &[VolumeMount]) -> Vec<String> {
	let mut args = Vec::with_capacity(volumes.len() * 2);
	for volume in volumes {
		args.push("--volume".to_owned());
		let mut spec = format!("{}:{}", volume.tag, path_string(&volume.host_dir));
		if volume.read_only {
			spec.push_str(":ro");
		}
		args.push(spec);
	}
	args
}

/// Format repeatable `--remote-fs` flags for object-proxy virtio-fs mounts.
fn remote_fs_args(mounts: &[RemoteFsShare]) -> Vec<String> {
	let mut args = Vec::with_capacity(mounts.len() * 2);
	for mount in mounts {
		args.push("--remote-fs".to_owned());
		args.push(format!("{}:{}", mount.tag, path_string(&mount.sock)));
	}
	args
}

/// Runtime boundary for sandbox process lifecycle.
///
/// Backends share the [`SandboxVm`] on-disk layout and guest-agent/control
/// protocols while remaining free to launch a different VMM implementation.
pub trait SandboxRuntime: Send + Sync {
	/// Stable runtime identifier persisted with sandbox metadata.
	fn name(&self) -> &'static str;

	/// Address a sandbox by name under the current state directory.
	fn sandbox(&self, name: &str) -> SandboxVm {
		SandboxVm::new(name)
	}

	/// Start a sandbox and wait for its control plane to become ready.
	fn launch(&self, vm: &SandboxVm, spec: &LaunchSpec) -> Result<()>;

	/// Stop a running sandbox.
	fn stop(&self, vm: &SandboxVm, wait: bool) -> Result<()>;

	/// Remove a sandbox and its runtime-owned state.
	fn remove(&self, vm: &SandboxVm) -> Result<()>;

	/// Report whether a sandbox is still running.
	fn is_running(&self, vm: &SandboxVm) -> Result<bool>;

	/// Pause a running sandbox's vCPUs so it can be parked (warm pools park
	/// members paused). Backends without a pause control plane keep the
	/// default: callers fall back to running members.
	fn pause(&self, vm: &SandboxVm) -> Result<()> {
		let _ = vm;
		Err(EngineError::invalid("sandbox runtime does not support pause"))
	}

	/// Resume a sandbox previously paused via [`Self::pause`].
	fn resume(&self, vm: &SandboxVm) -> Result<()> {
		let _ = vm;
		Err(EngineError::invalid("sandbox runtime does not support resume"))
	}
}

/// Runtime backed by the built-in `vmon vmm` process.
#[derive(Clone, Copy, Debug, Default)]
pub struct VmonRuntime;

impl SandboxRuntime for VmonRuntime {
	fn name(&self) -> &'static str {
		"vmon"
	}

	fn launch(&self, vm: &SandboxVm, spec: &LaunchSpec) -> Result<()> {
		vm.launch(spec)
	}

	fn stop(&self, vm: &SandboxVm, wait: bool) -> Result<()> {
		vm.stop(wait)
	}

	fn remove(&self, vm: &SandboxVm) -> Result<()> {
		vm.remove()
	}

	fn is_running(&self, vm: &SandboxVm) -> Result<bool> {
		vm.is_running()
	}

	fn pause(&self, vm: &SandboxVm) -> Result<()> {
		vm.pause()
	}

	fn resume(&self, vm: &SandboxVm) -> Result<()> {
		vm.resume()
	}
}

/// A microVM instance rooted under `$VMON_HOME/vms/<name>`.
#[derive(Clone, Debug)]
pub struct SandboxVm {
	name:      String,
	dir:       PathBuf,
	sock:      PathBuf,
	log:       PathBuf,
	meta_path: PathBuf,
}

impl SandboxVm {
	/// Address a VM by name under the current state directory.
	pub fn new(name: impl Into<String>) -> Self {
		let name = name.into();
		let dir = state_dir().join("vms").join(&name);
		Self::from_dir(name, dir)
	}

	/// Address a VM by name under an explicit VM directory.
	pub fn from_dir(name: impl Into<String>, dir: impl Into<PathBuf>) -> Self {
		let name = name.into();
		let dir = dir.into();
		Self {
			name,
			sock: dir.join("api.sock"),
			log: dir.join("console.log"),
			meta_path: dir.join("meta.json"),
			dir,
		}
	}

	/// VM name.
	pub fn name(&self) -> &str {
		&self.name
	}

	/// VM state directory.
	pub fn dir(&self) -> &Path {
		&self.dir
	}

	/// Default control socket path.
	pub fn api_sock(&self) -> &Path {
		&self.sock
	}

	/// Console log path.
	pub fn log_path(&self) -> &Path {
		&self.log
	}

	/// Metadata JSON path.
	pub fn meta_path(&self) -> &Path {
		&self.meta_path
	}

	/// Read `meta.json`, returning an empty object when it does not exist.
	pub fn meta(&self) -> Result<Map<String, Value>> {
		match fs::read_to_string(&self.meta_path) {
			Ok(text) => match serde_json::from_str::<Value>(&text)? {
				Value::Object(map) => Ok(map),
				_ => Err(EngineError::invalid(format!(
					"metadata {} is not a JSON object",
					self.meta_path.display()
				))),
			},
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Map::new()),
			Err(e) => Err(EngineError::from(e)),
		}
	}

	/// Merge updates into `meta.json` using Python-compatible field names.
	pub fn save_meta(&self, updates: Map<String, Value>) -> Result<()> {
		merge_meta_map(&self.meta_path, updates)
	}

	/// Return the pid recorded in metadata, if any.
	pub fn pid(&self) -> Result<Option<libc::pid_t>> {
		meta_pid(self.meta()?.get("pid"))
	}

	/// Control socket, accounting for jail-root metadata rewrites.
	pub fn control_sock(&self) -> Result<PathBuf> {
		let meta = self.meta()?;
		if let Some(jail_root) = meta_optional_path(meta.get("jail_root"))? {
			return Ok(jail_root
				.join("root")
				.join("run")
				.join("vmon")
				.join("control.sock"));
		}
		meta_path_or(meta.get("sock"), &self.sock)
	}

	/// Agent socket, accounting for jail-root metadata rewrites.
	pub fn agent_sock(&self) -> Result<PathBuf> {
		let meta = self.meta()?;
		if let Some(jail_root) = meta_optional_path(meta.get("jail_root"))? {
			return Ok(jail_root
				.join("root")
				.join("run")
				.join("vmon")
				.join("agent.sock"));
		}
		meta_path_or(meta.get("agent_sock"), &self.dir.join("agent.sock"))
	}

	/// Rootfs image path from metadata or the VM directory default.
	pub fn rootfs_img(&self) -> Result<PathBuf> {
		let meta = self.meta()?;
		meta_path_or(meta.get("rootfs"), &self.dir.join("rootfs.img"))
	}

	/// True when metadata says `running` and the recorded pid is alive.
	pub fn is_running(&self) -> Result<bool> {
		let meta = self.meta()?;
		if meta.get("status").and_then(Value::as_str) != Some("running") {
			return Ok(false);
		}
		let Some(pid) = meta_pid(meta.get("pid"))? else {
			return Ok(false);
		};
		Ok(pid_is_running(pid))
	}

	/// Return console log contents, replacing invalid UTF-8 like Python.
	pub fn logs(&self) -> String {
		fs::read(&self.log)
			.map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
			.unwrap_or_default()
	}

	/// Spawn the current executable as `vmm` and wait for the control socket.
	pub fn launch(&self, spec: &LaunchSpec) -> Result<()> {
		self.launch_with_timeout(spec, DEFAULT_LAUNCH_TIMEOUT)
	}

	/// Spawn with an explicit readiness timeout.
	pub fn launch_with_timeout(&self, spec: &LaunchSpec, launch_timeout: Duration) -> Result<()> {
		let mut launch_args = spec.try_build_args()?;
		let snapshot_root = spec
			.snapshot_root
			.clone()
			.unwrap_or_else(default_snapshot_root);
		fs::create_dir_all(&snapshot_root)?;
		if !contains_flag(&launch_args, "--snapshot-root") {
			push_arg(&mut launch_args, "--snapshot-root", &snapshot_root);
		}
		if no_sandbox_requested() && !contains_flag(&launch_args, "--no-sandbox") {
			launch_args.push("--no-sandbox".to_owned());
		}

		for sock in [&spec.api_sock, spec.agent_sock.as_ref().unwrap_or(&spec.api_sock)] {
			let _ = fs::remove_file(sock);
		}
		fs::create_dir_all(&self.dir)?;
		let mut parents = Vec::new();
		for sock in [&spec.api_sock, spec.agent_sock.as_ref().unwrap_or(&spec.api_sock)] {
			if let Some(parent) = sock.parent() {
				parents.push(parent);
			}
		}
		parents.sort();
		parents.dedup();
		for parent in parents {
			fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
		}

		static BINARY: LazyLock<Option<PathBuf>> = LazyLock::new(doctor::find_vmon_binary);
		let binary = BINARY.clone().ok_or_else(|| {
			EngineError::engine("vmon executable not found; set VMON_BIN or install vmon on PATH")
		})?;
		let log_file = OpenOptions::new()
			.create(true)
			.append(true)
			.open(&self.log)?;
		let stderr = log_file.try_clone()?;
		let mut command = Command::new(&binary);
		command
			.arg("vmm")
			.args(&launch_args)
			.stdin(Stdio::null())
			.stdout(Stdio::from(log_file))
			.stderr(Stdio::from(stderr))
			.process_group(0);
		if let Some(url) = &spec.remote_page_url {
			let token = spec.remote_page_token.as_deref().unwrap_or("");
			command.env("VMON_REMOTE_PAGE_TOKEN", token);
			if url.is_empty() {
				return Err(EngineError::invalid("--remote-page-url must not be empty"));
			}
		}
		let mut child = command.spawn()?;
		self.save_meta(launch_metadata(
			spec,
			child.id(),
			&binary,
			&snapshot_root,
			unix_now_secs_f64(),
		))?;

		match self.wait_ready(&mut child, &spec.api_sock, launch_timeout) {
			Ok(()) => {
				// Reap the child when it eventually exits: without a waiter the
				// dead VMM lingers as a zombie and `kill(pid, 0)` liveness checks
				// would report it running forever (e.g. after a --timeout-secs
				// self-kill).
				thread::spawn(move || {
					let _ = child.wait();
				});
				Ok(())
			},
			Err(e) => {
				let _ =
					self.cleanup_failed_launch(&mut child, &spec.api_sock, spec.agent_sock.as_deref());
				Err(e)
			},
		}
	}

	fn wait_ready(
		&self,
		child: &mut Child,
		control_sock: &Path,
		launch_timeout: Duration,
	) -> Result<()> {
		let deadline = Instant::now() + launch_timeout;
		while Instant::now() < deadline {
			if let Some(status) = child.try_wait()? {
				let tail = self.log_tail(15);
				return Err(EngineError::engine(format!(
					"vmon exited (code {}) before the VM came up:\n{tail}",
					status
						.code()
						.map_or_else(|| "signal".to_owned(), |code| code.to_string())
				)));
			}
			if let Ok(mut control) = ControlClient::connect(control_sock, READY_PROBE_TIMEOUT)
				&& control.ping().is_ok()
			{
				return Ok(());
			}
			thread::sleep(READY_SLEEP);
		}
		Err(EngineError::engine(format!(
			"VM '{}' control socket never came up; see {}",
			self.name,
			self.log.display()
		)))
	}

	fn cleanup_failed_launch(
		&self,
		child: &mut Child,
		control_sock: &Path,
		agent_sock: Option<&Path>,
	) -> Result<()> {
		if child.try_wait()?.is_none() {
			terminate_process_group(child.id(), libc::SIGTERM);
			if !wait_child_exit(child, Duration::from_secs(2))? {
				terminate_process_group(child.id(), libc::SIGKILL);
				let _ = wait_child_exit(child, Duration::from_secs(1));
			}
		}
		let _ = fs::remove_file(control_sock);
		if let Some(agent_sock) = agent_sock {
			let _ = fs::remove_file(agent_sock);
		}
		let returncode = child.try_wait()?.and_then(|status| status.code());
		let mut updates = Map::new();
		updates.insert("status".to_owned(), json!("failed"));
		updates.insert("returncode".to_owned(), json!(returncode));
		self.save_meta(updates)
	}

	/// Pause all vCPUs and device workers via the control socket.
	pub fn pause(&self) -> Result<()> {
		ControlClient::connect(self.control_sock()?, STOP_CONTROL_TIMEOUT)?
			.pause()
			.map(|_| ())
	}

	/// Resume a paused VMM via the control socket.
	pub fn resume(&self) -> Result<()> {
		ControlClient::connect(self.control_sock()?, STOP_CONTROL_TIMEOUT)?
			.resume()
			.map(|_| ())
	}

	/// Stop the VMM with quit, SIGTERM, then SIGKILL escalation.
	pub fn stop(&self, wait: bool) -> Result<()> {
		let pid = self.pid()?;
		match ControlClient::connect(self.control_sock()?, STOP_CONTROL_TIMEOUT)
			.and_then(|mut control| control.quit().map(|_| ()))
		{
			Ok(()) => {},
			Err(_) => {
				if let Some(pid) = pid {
					send_signal(pid, libc::SIGTERM);
				}
			},
		}
		if wait
			&& let Some(pid) = pid
			&& !wait_pid_exit(pid, Duration::from_secs(2))
		{
			send_signal(pid, libc::SIGTERM);
			if !wait_pid_exit(pid, Duration::from_secs(1)) {
				send_signal(pid, libc::SIGKILL);
				let _ = wait_pid_exit(pid, Duration::from_secs(1));
			}
		}
		let mut updates = Map::new();
		updates.insert("status".to_owned(), json!("stopped"));
		self.save_meta(updates)
	}

	/// Remove the VM directory, stopping the VMM first if needed.
	pub fn remove(&self) -> Result<()> {
		if self.is_running()? {
			self.stop(true)?;
		}
		match fs::remove_dir_all(&self.dir) {
			Ok(()) => Ok(()),
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
			Err(e) => Err(EngineError::from(e)),
		}
	}

	/// List VM directories under an explicit `vms/` root.
	pub fn list_in(vms_dir: impl AsRef<Path>) -> Result<Vec<Self>> {
		let vms_dir = vms_dir.as_ref();
		if !vms_dir.is_dir() {
			return Ok(Vec::new());
		}
		let mut dirs = Vec::new();
		for entry in fs::read_dir(vms_dir)? {
			let entry = entry?;
			if entry.file_type()?.is_dir() {
				dirs.push(entry.path());
			}
		}
		dirs.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
		Ok(dirs
			.into_iter()
			.filter_map(|dir| {
				let name = dir.file_name()?.to_string_lossy().into_owned();
				Some(Self::from_dir(name, dir))
			})
			.collect())
	}

	/// List VM directories under `$VMON_HOME/vms`.
	pub fn list() -> Result<Vec<Self>> {
		Self::list_in(state_dir().join("vms"))
	}

	fn log_tail(&self, lines: usize) -> String {
		let logs = self.logs();
		let mut tail = logs.lines().rev().take(lines).collect::<Vec<_>>();
		tail.reverse();
		tail.join("\n")
	}
}

fn launch_metadata(
	spec: &LaunchSpec,
	pid: u32,
	binary: &Path,
	snapshot_root: &Path,
	started: f64,
) -> Map<String, Value> {
	let mut meta = Map::new();
	meta.insert("pid".to_owned(), json!(pid));
	meta.insert("sock".to_owned(), json!(path_string(&spec.api_sock)));
	meta.insert("agent_sock".to_owned(), path_json(spec.agent_sock.as_deref()));
	meta.insert("binary".to_owned(), json!(path_string(binary)));
	meta.insert("snapshot_root".to_owned(), json!(path_string(snapshot_root)));
	meta.insert("started".to_owned(), json!(started));
	meta.insert("status".to_owned(), json!("running"));

	match &spec.mode {
		LaunchMode::Boot => {
			meta.insert("image".to_owned(), nullable_string(spec.image.as_deref()));
			meta.insert("mem".to_owned(), json!(spec.mem_mib.unwrap_or(DEFAULT_MEM_MIB)));
			meta.insert("cpus".to_owned(), json!(spec.cpus.unwrap_or(DEFAULT_CPUS)));
			meta.insert("rootfs".to_owned(), path_json(spec.rootfs.as_deref()));
			meta.insert("tap".to_owned(), nullable_string(tap_name(spec).as_deref()));
			meta.insert("user_net".to_owned(), json!(matches!(spec.network, Some(NetworkMode::User))));
			meta.insert("volumes".to_owned(), volumes_json(&spec.volumes));
			meta.insert("remote_fs".to_owned(), remote_fs_json(&spec.remote_fs));
		},
		LaunchMode::Restore { snapshot } => {
			meta.insert("restored_from".to_owned(), json!(path_string(snapshot)));
			meta.insert("mem".to_owned(), nullable_u64(spec.mem_mib));
			meta.insert("cpus".to_owned(), nullable_u64(spec.cpus));
			meta.insert("tap".to_owned(), nullable_string(tap_name(spec).as_deref()));
			meta.insert("user_net".to_owned(), json!(matches!(spec.network, Some(NetworkMode::User))));
			meta.insert("rootfs".to_owned(), path_json(spec.rootfs.as_deref()));
			meta.insert("volumes".to_owned(), volumes_json(&spec.volumes));
			meta.insert("remote_fs".to_owned(), remote_fs_json(&spec.remote_fs));
			meta
				.insert("remote_page_url".to_owned(), nullable_string(spec.remote_page_url.as_deref()));
			meta.insert(
				"remote_page_digest".to_owned(),
				nullable_string(spec.remote_page_digest.as_deref()),
			);
			let env = spec.remote_page_url.as_ref().map(|_| {
				let mut env = BTreeMap::new();
				env.insert(
					"VMON_REMOTE_PAGE_TOKEN".to_owned(),
					spec.remote_page_token.clone().unwrap_or_default(),
				);
				env
			});
			meta.insert("env".to_owned(), json!(env));
		},
		LaunchMode::Fork { snapshot } => {
			meta.insert("forked_from".to_owned(), json!(path_string(snapshot)));
			meta.insert("rootfs".to_owned(), path_json(spec.rootfs.as_deref()));
			meta.insert("volumes".to_owned(), volumes_json(&spec.volumes));
			meta.insert("remote_fs".to_owned(), remote_fs_json(&spec.remote_fs));
		},
	}
	meta
}

fn volumes_json(volumes: &[VolumeMount]) -> Value {
	Value::Array(
		volumes
			.iter()
			.map(|volume| {
				json!({
					"tag": volume.tag,
					"dir": path_string(&volume.host_dir),
					"ro": volume.read_only,
				})
			})
			.collect(),
	)
}

fn remote_fs_json(mounts: &[RemoteFsShare]) -> Value {
	Value::Array(
		mounts
			.iter()
			.map(|mount| json!({ "tag": mount.tag, "sock": path_string(&mount.sock) }))
			.collect(),
	)
}

fn tap_name(spec: &LaunchSpec) -> Option<String> {
	match &spec.network {
		Some(NetworkMode::Tap(tap)) => Some(tap.clone()),
		_ => None,
	}
}

fn nullable_string(value: Option<&str>) -> Value {
	value.map_or(Value::Null, |value| json!(value))
}

fn nullable_u64(value: Option<u64>) -> Value {
	value.map_or(Value::Null, |value| json!(value))
}

fn path_json(path: Option<&Path>) -> Value {
	path.map_or(Value::Null, |path| json!(path_string(path)))
}

fn meta_pid(value: Option<&Value>) -> Result<Option<libc::pid_t>> {
	match value {
		None | Some(Value::Null) => Ok(None),
		Some(Value::Number(n)) => {
			let Some(pid) = n.as_i64() else {
				return Err(EngineError::invalid("metadata pid must be int"));
			};
			if pid <= 0 {
				return Ok(None);
			}
			libc::pid_t::try_from(pid)
				.map(Some)
				.map_err(|_| EngineError::invalid("metadata pid must fit pid_t"))
		},
		Some(_) => Err(EngineError::invalid("metadata pid must be int")),
	}
}

fn meta_optional_path(value: Option<&Value>) -> Result<Option<PathBuf>> {
	match value {
		None | Some(Value::Null | Value::Bool(false)) => Ok(None),
		Some(Value::String(s)) if s.is_empty() => Ok(None),
		Some(Value::String(s)) => Ok(Some(PathBuf::from(s))),
		Some(_) => Err(EngineError::invalid("metadata path must be str or null")),
	}
}

fn meta_path_or(value: Option<&Value>, fallback: &Path) -> Result<PathBuf> {
	Ok(meta_optional_path(value)?.unwrap_or_else(|| fallback.to_path_buf()))
}

fn pid_is_running(pid: libc::pid_t) -> bool {
	let proc_stat = PathBuf::from(format!("/proc/{pid}/stat"));
	if proc_stat.exists() {
		let Ok(text) = fs::read_to_string(proc_stat) else {
			return false;
		};
		let Some((_, after_name)) = text.split_once(") ") else {
			return false;
		};
		let state = after_name.split(' ').next().unwrap_or_default();
		if matches!(state, "Z" | "X" | "x") {
			let _ = reap_pid_nonblocking(pid);
			return false;
		}
		return true;
	}
	match reap_pid_nonblocking(pid) {
		Ok(Some(reaped)) if reaped == pid => false,
		Ok(_) => true,
		Err(_) => kill_probe(pid),
	}
}

fn wait_pid_exit(pid: libc::pid_t, timeout: Duration) -> bool {
	let deadline = Instant::now() + timeout;
	loop {
		match reap_pid_nonblocking(pid) {
			Ok(Some(reaped)) if reaped == pid => return true,
			Ok(_) => {},
			Err(_) if !kill_probe(pid) => return true,
			Err(_) => {},
		}
		if Instant::now() >= deadline {
			return false;
		}
		thread::sleep(WAIT_POLL);
	}
}

fn reap_pid_nonblocking(pid: libc::pid_t) -> io::Result<Option<libc::pid_t>> {
	let mut status = 0;
	// SAFETY: `waitpid` is called with a positive pid and a valid status pointer.
	let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
	match result.cmp(&0) {
		std::cmp::Ordering::Less => Err(io::Error::last_os_error()),
		std::cmp::Ordering::Equal => Ok(None),
		std::cmp::Ordering::Greater => Ok(Some(result)),
	}
}

fn kill_probe(pid: libc::pid_t) -> bool {
	// SAFETY: `kill(pid, 0)` only probes existence and does not dereference memory.
	let result = unsafe { libc::kill(pid, 0) };
	if result == 0 {
		return true;
	}
	matches!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM))
}

fn send_signal(pid: libc::pid_t, signal: libc::c_int) {
	// SAFETY: Sending a POSIX signal to a positive pid has no Rust aliasing
	// concerns.
	let _ = unsafe { libc::kill(pid, signal) };
}

fn terminate_process_group(pid: u32, signal: libc::c_int) {
	if let Ok(pid) = libc::pid_t::try_from(pid) {
		// SAFETY: Negative pid targets the child's process group; no pointers involved.
		let _ = unsafe { libc::kill(-pid, signal) };
	}
}

fn wait_child_exit(child: &mut Child, timeout: Duration) -> io::Result<bool> {
	let deadline = Instant::now() + timeout;
	loop {
		if child.try_wait()?.is_some() {
			return Ok(true);
		}
		if Instant::now() >= deadline {
			return Ok(false);
		}
		thread::sleep(WAIT_POLL);
	}
}

fn validate_range(value: u64, flag: &str, maximum: u64, unit: &str) -> Result<()> {
	if value == 0 || value > maximum {
		return Err(EngineError::invalid(format!(
			"{flag} must be between 1{unit} and {maximum}{unit} (got {value})"
		)));
	}
	Ok(())
}

fn validate_volume_mount(volume: &VolumeMount) -> Result<()> {
	if !valid_volume_tag(&volume.tag) {
		return Err(EngineError::invalid(format!(
			"volume tag {:?} must match [a-z0-9_]{{1,32}}",
			volume.tag
		)));
	}
	let host = path_string(&volume.host_dir);
	if host.is_empty() {
		return Err(EngineError::invalid("volume host directory must not be empty"));
	}
	if host.contains('\0') {
		return Err(EngineError::invalid("volume host directory must not contain NUL bytes"));
	}
	if !volume.read_only && host.ends_with(":ro") {
		return Err(EngineError::invalid("read-write volume host directory must not end with ':ro'"));
	}
	Ok(())
}

fn validate_remote_fs_share(mount: &RemoteFsShare) -> Result<()> {
	if !valid_volume_tag(&mount.tag) {
		return Err(EngineError::invalid(format!(
			"remote filesystem tag {:?} must match [a-z0-9_]{{1,32}}",
			mount.tag
		)));
	}
	if !mount.sock.is_absolute() {
		return Err(EngineError::invalid(format!(
			"remote filesystem socket must be absolute: {}",
			mount.sock.display()
		)));
	}
	let sock = path_string(&mount.sock);
	if sock.contains('\0') {
		return Err(EngineError::invalid("remote filesystem socket must not contain NUL bytes"));
	}
	Ok(())
}

fn valid_volume_tag(tag: &str) -> bool {
	!tag.is_empty()
		&& tag.len() <= 32
		&& tag
			.bytes()
			.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn contains_flag(args: &[String], flag: &str) -> bool {
	args.iter().any(|arg| arg == flag)
}

fn no_sandbox_requested() -> bool {
	std::env::var("VMON_NO_SANDBOX").is_ok_and(|value| !matches!(value.as_str(), "" | "0"))
}

fn default_snapshot_root() -> PathBuf {
	state_dir().join("snapshots")
}

fn path_string(path: impl AsRef<Path>) -> String {
	path.as_ref().to_string_lossy().into_owned()
}

fn default_cmdline(agent: bool) -> String {
	let mut base = if agent {
		"console=ttyS0 reboot=t panic=-1 init=/.vmon/agent vmon.agent=serve".to_owned()
	} else {
		"console=ttyS0 reboot=t panic=-1 rdinit=/init".to_owned()
	};
	if cfg!(target_arch = "aarch64") {
		base.push_str(" earlycon=uart8250,mmio,0x9000000");
	}
	base
}

fn unix_now_secs_f64() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn p(path: &str) -> PathBuf {
		PathBuf::from(path)
	}

	fn snapshot(name: &str) -> PathBuf {
		PathBuf::from("/state/snapshots").join(name)
	}

	#[test]
	fn spawn_volume_args_formats_repeatable_cli_args() {
		let vols = vec![
			VolumeMount::new("t", "/d", false).unwrap(),
			VolumeMount::new("c", "/c", true).unwrap(),
		];
		assert_eq!(volume_args(&vols), ["--volume", "t:/d", "--volume", "c:/c:ro"]);
	}

	#[test]
	fn spawn_volume_args_preserves_spaces_colons_and_rejects_ambiguous_ro_suffix() {
		let vols = vec![VolumeMount::new("data", "/tmp/has space/a:b", false).unwrap()];
		assert_eq!(volume_args(&vols), ["--volume", "data:/tmp/has space/a:b"]);
		let vols = vec![VolumeMount::new("data", "/tmp/path:ro", true).unwrap()];
		assert_eq!(volume_args(&vols), ["--volume", "data:/tmp/path:ro:ro"]);
		assert!(VolumeMount::new("bad-tag", "/tmp/x", false).is_err());
		assert!(VolumeMount::new("data", "/tmp/path:ro", false).is_err());
	}

	#[test]
	fn start_paused_builder_emits_flag_for_every_launch_mode() {
		let boot =
			LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img").with_start_paused();
		let restore = LaunchSpec::restore("/vm/api.sock", snapshot("snap")).with_start_paused();
		let fork = LaunchSpec::fork_from("/vm/api.sock", snapshot("snap")).with_start_paused();

		let default_boot = LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img");
		assert!(
			!build_launch_args(&default_boot)
				.iter()
				.any(|arg| arg == "--start-paused"),
			"ordinary launches must remain runnable"
		);

		for spec in [&boot, &restore, &fork] {
			assert!(spec.start_paused);
			assert!(
				build_launch_args(spec)
					.iter()
					.any(|arg| arg == "--start-paused"),
				"start-paused launch must emit --start-paused"
			);
		}
	}
	#[test]
	fn spawn_restore_rejects_invalid_resource_args_before_launch() {
		assert!(
			LaunchSpec::restore("/api.sock", snapshot("snap"))
				.with_mem_mib(0)
				.validate()
				.is_err()
		);
		assert!(
			LaunchSpec::restore("/api.sock", snapshot("snap"))
				.with_cpus(0)
				.validate()
				.is_err()
		);
		assert!(
			LaunchSpec::restore("/api.sock", snapshot("snap"))
				.with_timeout_secs(MAX_TIMEOUT_SECS + 1)
				.validate()
				.is_err()
		);
	}

	#[test]
	fn spawn_restore_emits_volume_and_timeout_args() {
		let spec = LaunchSpec::restore("/vm/api.sock", snapshot("snap"))
			.with_volume(VolumeMount::new("t", "/d", false).unwrap())
			.with_timeout_secs(5)
			.with_snapshot_root("/state/snapshots");
		let args = build_launch_args(&spec);
		let idx = args.iter().position(|arg| arg == "--volume").unwrap();
		assert_eq!(&args[idx..idx + 2], ["--volume", "t:/d"]);
		let idx = args.iter().position(|arg| arg == "--timeout-secs").unwrap();
		assert_eq!(args[idx + 1], "5");
		let meta =
			launch_metadata(&spec, 7, Path::new("/bin/vmon"), Path::new("/state/snapshots"), 1.0);
		assert_eq!(meta["volumes"], json!([{ "tag": "t", "dir": "/d", "ro": false }]));
	}

	#[test]
	fn spawn_restore_emits_tap_and_user_net_args() {
		let spec = LaunchSpec::restore("/vm/api.sock", snapshot("snap")).with_tap("tap7");
		let args = build_launch_args(&spec);
		let idx = args.iter().position(|arg| arg == "--tap").unwrap();
		assert_eq!(args[idx + 1], "tap7");
		let meta =
			launch_metadata(&spec, 7, Path::new("/bin/vmon"), Path::new("/state/snapshots"), 1.0);
		assert_eq!(meta["tap"], json!("tap7"));

		let spec = LaunchSpec::restore("/vm/api.sock", snapshot("snap")).with_user_net();
		let args = build_launch_args(&spec);
		let idx = args.iter().position(|arg| arg == "--net").unwrap();
		assert_eq!(&args[idx..idx + 2], ["--net", "user"]);
		let meta =
			launch_metadata(&spec, 7, Path::new("/bin/vmon"), Path::new("/state/snapshots"), 1.0);
		assert_eq!(meta["user_net"], json!(true));
	}

	#[test]
	fn restricted_user_net_requires_user_mode_and_emits_its_flag() {
		let spec = LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img")
			.with_user_net()
			.with_restricted_user_net(8443);
		assert!(spec.validate().is_ok());
		let args = spec
			.try_build_args()
			.expect("valid restricted user network");
		let idx = args
			.iter()
			.position(|arg| arg == "--net")
			.expect("user net flag");
		assert_eq!(&args[idx..idx + 5], [
			"--net",
			"user",
			"--user-net-restricted",
			"--user-net-allow-host-port",
			"8443"
		]);

		assert!(
			LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img")
				.with_restricted_user_net(8443)
				.validate()
				.is_err()
		);
		assert!(
			LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img")
				.with_tap("tap7")
				.with_restricted_user_net(8443)
				.validate()
				.is_err()
		);
		assert!(
			LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img")
				.with_user_net()
				.with_restricted_user_net(0)
				.validate()
				.is_err()
		);
	}

	#[test]
	fn spawn_fork_snapshot_emits_volume_and_timeout_args() {
		let spec = LaunchSpec::fork_from("/vm/api.sock", snapshot("base"))
			.with_volume(VolumeMount::new("t", "/d", false).unwrap())
			.with_timeout_secs(5)
			.with_snapshot_root("/state/snapshots");
		let args = build_launch_args(&spec);
		assert!(args.iter().any(|arg| arg == "--fork-from"));
		let idx = args.iter().position(|arg| arg == "--volume").unwrap();
		assert_eq!(args[idx + 1], "t:/d");
		let idx = args.iter().position(|arg| arg == "--timeout-secs").unwrap();
		assert_eq!(args[idx + 1], "5");
		let meta =
			launch_metadata(&spec, 7, Path::new("/bin/vmon"), Path::new("/state/snapshots"), 1.0);
		assert_eq!(meta["volumes"], json!([{ "tag": "t", "dir": "/d", "ro": false }]));
	}

	#[test]
	fn spawn_boot_rootfs_passes_rng_flag_only_when_requested() {
		let spec =
			LaunchSpec::boot_rootfs("/vm/api.sock", "/tmp/fake-kernel", "/rootfs.img").with_rng();
		assert!(build_launch_args(&spec).iter().any(|arg| arg == "--rng"));

		let spec = LaunchSpec::boot_rootfs("/vm/api.sock", "/tmp/fake-kernel", "/rootfs.img");
		assert!(!build_launch_args(&spec).iter().any(|arg| arg == "--rng"));
	}

	#[test]
	fn spawn_boot_overlay_and_rootfs_ro_follow_python_order() {
		let direct =
			LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img").with_rootfs_read_only();
		let args = build_launch_args(&direct);
		let idx = args.iter().position(|arg| arg == "--rootfs").unwrap();
		assert_eq!(&args[idx..idx + 3], ["--rootfs", "/rootfs.img", "--rootfs-ro"]);

		let overlay = LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/ignored")
			.with_disk_overlay("/base.img", "/vm/rootfs.img");
		let args = build_launch_args(&overlay);
		let idx = args
			.iter()
			.position(|arg| arg == "--disk-overlay-of")
			.unwrap();
		assert_eq!(&args[idx..idx + 4], [
			"--disk-overlay-of",
			"/base.img",
			"--rootfs",
			"/vm/rootfs.img"
		]);
	}

	#[test]
	fn spawn_restore_remote_page_sets_flag_and_metadata_env() {
		let spec = LaunchSpec::restore("/vm/api.sock", snapshot("snap"))
			.with_remote_page(
				"http://peer/pages",
				Some("secret".to_owned()),
				Some("digest".to_owned()),
			)
			.with_snapshot_root("/state/snapshots");
		let args = build_launch_args(&spec);
		let idx = args
			.iter()
			.position(|arg| arg == "--remote-page-url")
			.unwrap();
		assert_eq!(args[idx + 1], "http://peer/pages");
		let meta =
			launch_metadata(&spec, 7, Path::new("/bin/vmon"), Path::new("/state/snapshots"), 1.0);
		assert_eq!(meta["remote_page_digest"], json!("digest"));
		assert_eq!(meta["env"], json!({ "VMON_REMOTE_PAGE_TOKEN": "secret" }));
	}

	#[test]
	fn spawn_agent_mac_fs_and_console_agent_order_is_deterministic() {
		let spec = LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img")
			.with_agent_sock("/vm/agent.sock")
			.with_user_net()
			.with_mac("aa:bb:cc:de:ad:01")
			.with_rng()
			.with_fs_share("host", "/share")
			.with_volume(VolumeMount::new("data", "/data", false).unwrap())
			.with_timeout_secs(9)
			.with_snapshot_root("/snaps")
			.with_console_agent();
		let args = build_launch_args(&spec);
		let flags: Vec<&str> = args
			.iter()
			.filter(|arg| arg.starts_with("--"))
			.map(String::as_str)
			.collect();
		assert_eq!(flags, [
			"--kernel",
			"--mem",
			"--cpus",
			"--api-sock",
			"--cmdline",
			"--rootfs",
			"--net",
			"--mac",
			"--agent-sock",
			"--rng",
			"--fs-tag",
			"--fs-dir",
			"--volume",
			"--timeout-secs",
			"--snapshot-root",
			"--console-agent",
		]);
	}

	#[test]
	fn owner_watchdog_flag_is_opt_in() {
		let single = LaunchSpec::boot_rootfs("/vm/api.sock", "/kernel", "/rootfs.img");
		assert!(
			!build_launch_args(&single)
				.iter()
				.any(|arg| arg == "--owner-lease-secs")
		);
		let production = single.with_owner_lease_secs(15);
		let args = production.try_build_args().unwrap();
		let index = args
			.iter()
			.position(|arg| arg == "--owner-lease-secs")
			.unwrap();
		assert_eq!(args[index + 1], "15");
	}

	#[test]
	fn spawn_list_in_sorts_vm_directories() {
		let tmp = tempfile::tempdir().unwrap();
		fs::create_dir(tmp.path().join("b")).unwrap();
		fs::create_dir(tmp.path().join("a")).unwrap();
		fs::write(tmp.path().join("file"), b"no").unwrap();
		let names: Vec<String> = SandboxVm::list_in(tmp.path())
			.unwrap()
			.into_iter()
			.map(|vm| vm.name().to_owned())
			.collect();
		assert_eq!(names, ["a", "b"]);
	}

	#[test]
	fn spawn_is_running_requires_running_status() {
		let tmp = tempfile::tempdir().unwrap();
		let vm = SandboxVm::from_dir("vm", tmp.path().join("vm"));
		let mut meta = Map::new();
		meta.insert("pid".to_owned(), json!(std::process::id()));
		meta.insert("status".to_owned(), json!("stopped"));
		vm.save_meta(meta).unwrap();
		assert!(!vm.is_running().unwrap());
	}

	#[test]
	fn spawn_socket_paths_honor_jail_root() {
		let tmp = tempfile::tempdir().unwrap();
		let vm = SandboxVm::from_dir("vm", tmp.path().join("vm"));
		let mut meta = Map::new();
		meta.insert("jail_root".to_owned(), json!("/jail"));
		vm.save_meta(meta).unwrap();
		assert_eq!(vm.control_sock().unwrap(), p("/jail/root/run/vmon/control.sock"));
		assert_eq!(vm.agent_sock().unwrap(), p("/jail/root/run/vmon/agent.sock"));
	}
	#[test]
	fn concurrent_stale_metadata_writer_cannot_revert_lifecycle_generation() {
		let tmp = tempfile::tempdir().expect("tempdir");
		let vm = SandboxVm::from_dir("vm", tmp.path().join("vm"));
		let mut generation_one = Map::new();
		generation_one.insert(
			"lifecycle".to_owned(),
			json!({"desired": "running", "observed": "stopped", "generation": 1}),
		);
		generation_one.insert("status".to_owned(), json!("stopped"));
		replace_meta_map(vm.meta_path(), &generation_one).expect("seed metadata");

		let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
		let stale_vm = vm.clone();
		let stale_barrier = std::sync::Arc::clone(&barrier);
		let stale = std::thread::spawn(move || {
			stale_barrier.wait();
			stale_vm
				.save_meta(Map::from_iter([
					("state_generation".to_owned(), json!(1)),
					("desired_state".to_owned(), json!("stopped")),
					("observed_state".to_owned(), json!("stopped")),
					("status".to_owned(), json!("stopped")),
					("last_active".to_owned(), json!(42)),
				]))
				.expect("stale writer");
		});
		let current_path = vm.meta_path().to_path_buf();
		let current_barrier = std::sync::Arc::clone(&barrier);
		let current = std::thread::spawn(move || {
			current_barrier.wait();
			let mut generation_two = Map::new();
			generation_two.insert(
				"lifecycle".to_owned(),
				json!({"desired": "running", "observed": "running", "generation": 2}),
			);
			generation_two.insert("status".to_owned(), json!("running"));
			replace_meta_map(&current_path, &generation_two).expect("current writer");
		});
		barrier.wait();
		stale.join().expect("stale join");
		current.join().expect("current join");

		let metadata = vm.meta().expect("read metadata");
		assert_eq!(metadata["lifecycle"]["generation"], 2);
		assert_eq!(metadata["status"], "running");
	}
}
