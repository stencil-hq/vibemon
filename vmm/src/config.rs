//! Command-line configuration.

use std::path::{Path, PathBuf};

use clap::{Parser, error::ErrorKind};

use crate::{
	bail,
	result::{Result, err},
};

/// Default locally-administered guest MAC (02:00:00:00:00:01).
const DEFAULT_MAC: [u8; 6] = [0x02, 0x00, 0x00, 0x00, 0x00, 0x01];

/// Hard launch-time caps that keep accidental fanout from exhausting the host.
pub const MAX_COUNT: u32 = 32;
pub const MAX_CPUS: u8 = 64;
pub const MAX_MEM_MIB: usize = 64 * 1024;
/// Maximum NVIDIA vGPU virtual functions assignable to one guest.
pub const MAX_VFIO_GPUS: usize = 8;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Transport {
	Mmio,
	Pci,
}

impl Transport {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Mmio => "mmio",
			Self::Pci => "pci",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CgroupMode {
	V2,
	Off,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeccompAction {
	Kill,
	Errno,
	Log,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootMode {
	Direct,
	Uefi,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
	Text,
	Json,
}

/// A named virtio-fs volume: a host directory exposed to the guest under `tag`.
#[derive(Clone)]
pub struct FsMount {
	pub tag:       String,
	pub dir:       PathBuf,
	pub read_only: bool,
}

/// A read-only virtio-fs mount served by a per-VM object proxy.
#[derive(Clone)]
pub struct RemoteFsMount {
	/// Guest-visible virtio-fs tag.
	pub tag:  String,
	/// Absolute host Unix socket path for the proxy.
	pub sock: PathBuf,
}

#[derive(Clone)]
pub struct Config {
	/// Kernel image. Required for a fresh boot; `None` when restoring/forking.
	pub kernel: Option<PathBuf>,
	pub cmdline: Option<String>,
	pub mem_mib: usize,
	/// Host-RAM resident page target in MiB for transparent zram+paging.
	pub mem_target_mib: Option<usize>,
	/// Maximum in-process compressed page store in MiB before spilling to swap.
	pub zram_store_max_mib: Option<usize>,
	/// Optional operator-provided swap file for pager overflow.
	pub zram_swap_file: Option<PathBuf>,
	/// HTTP page source for lazy snapshot restore; token comes from process env.
	pub remote_page_url: Option<String>,
	/// HTTP(S) object containing independently framed zstd root-disk blocks.
	pub rootfs_remote_url: Option<String>,
	/// Persistent sparse local cache for decompressed blocks fetched from
	/// `rootfs_remote_url`.
	pub rootfs_remote_cache: Option<PathBuf>,
	/// Local seek-index JSON needed to map uncompressed blocks to zstd frames.
	pub rootfs_remote_index: Option<PathBuf>,
	/// Guest-visible byte capacity when the remote object is a minimized
	/// filesystem.
	pub rootfs_remote_size: Option<u64>,
	/// Optional bearer credential sent only to the remote root-disk endpoint.
	pub rootfs_remote_bearer: Option<String>,
	/// Hint host KSM to merge identical guest RAM pages across co-resident VMs.
	pub ksm: bool,
	pub cpus: u8,
	pub rootfs: Option<PathBuf>,
	pub rootfs_read_only: bool,
	pub initrd: Option<PathBuf>,
	pub tap: Option<String>,
	/// Attach entitlement-free user-mode NAT networking instead of host
	/// TAP/vmnet.
	pub user_net: bool,
	/// Restrict user-mode networking to DHCP and one configured virtual-gateway
	/// service.
	pub user_net_restricted: bool,
	/// TCP port on the slirp virtual gateway allowed by restricted user
	/// networking.
	pub user_net_allow_host_port: Option<u16>,
	/// Whether `--mac` was explicitly supplied (used for snapshot restore
	/// overrides).
	pub mac_specified: bool,
	pub mac: [u8; 6],
	/// virtio transport (`mmio` everywhere, `pci` on `x86_64` only).
	pub transport: Transport,
	/// Pre-created NVIDIA SR-IOV vGPU virtual functions assigned through VFIO.
	#[cfg_attr(
		not(all(target_os = "linux", target_arch = "x86_64")),
		allow(dead_code, reason = "VFIO assignment exists only on x86_64 Linux")
	)]
	pub vfio_gpus: Vec<PathBuf>,
	/// Stable RFC 4122 VM UUID exposed to NVIDIA's guest driver through SMBIOS.
	#[cfg_attr(
		not(target_arch = "x86_64"),
		allow(dead_code, reason = "SMBIOS UUID publication exists only on x86_64")
	)]
	pub vm_uuid: Option<[u8; 16]>,
	/// One read-only virtio-fs mount tag exposed to the guest.
	pub fs_tag: Option<String>,
	/// Host directory backing the read-only virtio-fs share.
	pub fs_dir: Option<PathBuf>,
	/// Unix control socket (pause/resume/snapshot/quit). `None` disables the
	/// control plane.
	pub api_sock: Option<PathBuf>,
	/// Start vCPU threads paused until the internal control plane resumes them.
	///
	/// This is used by the daemon's durable lifecycle transitions and is not a
	/// public sandbox-create option.
	pub start_paused: bool,
	/// Root for named lifecycle snapshots. `None` disables named snapshots.
	pub snapshot_root: Option<PathBuf>,
	/// Host Unix socket bridged byte-for-byte to the virtio-console agent port.
	pub agent_sock: Option<PathBuf>,
	/// Restore a snapshot directory instead of booting a kernel.
	pub restore: Option<PathBuf>,
	/// Fork (`CoW`) from a template snapshot directory instead of booting.
	pub fork_from: Option<PathBuf>,
	/// Create a `CoW` overlay of this base disk and serve it as `--rootfs`.
	pub disk_overlay_of: Option<PathBuf>,
	/// Number of children to spawn with `--fork-from` (default 1).
	pub count: u32,
	/// Attach a virtio-console agent channel (/dev/hvc0) for warm-boot exec.
	pub console_agent: bool,
	/// Attach a virtio-rng entropy device (/dev/hwrng) seeded from
	/// `/dev/urandom`.
	pub rng: bool,
	/// After (warm) boot, send this command line to the guest agent over the
	/// console.
	pub agent_exec: Option<String>,
	/// Two-process jailer toggle.
	pub jail: bool,
	/// VM/jail identifier.
	pub id: Option<String>,
	/// Jail root directory.
	pub jail_root: Option<PathBuf>,
	/// cgroup v2 cpu.max value.
	pub cgroup_cpu_max: Option<String>,
	/// cgroup v2 memory.max value.
	pub cgroup_mem_max: Option<String>,
	/// cgroup v2 pids.max value.
	pub cgroup_pids_max: Option<u64>,
	/// cgroup mode.
	pub cgroup_mode: CgroupMode,
	/// Seccomp default action.
	pub seccomp_action: SeccompAction,
	/// Operator-supplied network namespace path.
	pub netns: Option<PathBuf>,
	/// Boot mode.
	pub boot_mode: BootMode,
	/// Firmware image for UEFI boot.
	pub firmware: Option<PathBuf>,
	/// Log output format.
	pub log_format: LogFormat,
	/// Log level filter string.
	pub log_level: String,
	/// Explicit opt-out from standalone default-on filters; `--jail` still
	/// forces filters.
	pub no_sandbox: bool,
	/// UID to drop to when filters are enabled and vmon starts as root.
	pub sandbox_uid: Option<u32>,
	/// GID to drop to when filters are enabled and vmon starts as root.
	pub sandbox_gid: Option<u32>,
	/// Wall-clock hard deadline in seconds; the VMM self-terminates when
	/// reached.
	pub timeout_secs: Option<u64>,
	/// Production ownership watchdog in seconds; self-terminates unless the
	/// control plane re-arms it after each authoritative lease renewal.
	pub owner_lease_secs: Option<u64>,
	/// Named writable (or `:ro`) virtio-fs volumes; one device per entry.
	pub volumes: Vec<FsMount>,
	/// Read-only virtio-fs mounts backed by per-VM object proxy sockets.
	pub remote_fs: Vec<RemoteFsMount>,
}

/// Raw command-line surface parsed by clap; flag tokenization only.
///
/// Numeric bounds, host-capability gating, and inter-flag invariants live in
/// [`Config::from_cli`], which lowers this into the runtime [`Config`].
/// Structured values reuse the module's `parse_*` validators as clap value
/// parsers, so a malformed flag reports the same diagnostic through every entry
/// point.
// These doc comments are clap `--help` text, not rustdoc; backticking the file
// paths, MAC literals, and syscall names would render the backticks verbatim in
// the terminal output.
#[allow(clippy::doc_markdown, reason = "field docs are CLI help, not rustdoc")]
#[derive(Parser)]
#[command(
	name = "vmon vmm",
	version,
	about = "a barebones KVM (Linux) / Hypervisor.framework (macOS) monitor for Linux guests",
	override_usage = "vmon vmm (--kernel <image> | --boot-mode uefi --firmware <fd>) [options]",
	long_about = "vmm boots a Linux guest with a serial console and virtio IO on KVM (Linux) or \
	              Apple Hypervisor.framework (macOS, Apple Silicon); the backend is selected at \
	              compile time."
)]
struct CliArgs {
	/// Kernel image (vmlinux/bzImage on x86_64, arm64 Image on aarch64);
	/// required for direct boot
	#[arg(long, value_name = "PATH")]
	kernel: Option<PathBuf>,

	/// Kernel command line (default: console=ttyS0 ...)
	#[arg(long, value_name = "STRING", allow_hyphen_values = true)]
	cmdline: Option<String>,

	/// Guest RAM in MiB
	#[arg(long, value_name = "MIB", default_value_t = 256)]
	mem: u64,

	/// Keep guest host-RAM resident set near <n> MiB (transparent zram+paging)
	#[arg(long, value_name = "MIB")]
	mem_target_mib: Option<u64>,

	/// Cap the in-RAM compressed store before spilling to swap
	#[arg(long, value_name = "MIB")]
	zram_store_max_mib: Option<u64>,

	/// Swap file for paging overflow (default: anonymous temp in $TMPDIR)
	#[arg(long, value_name = "PATH")]
	zram_swap_file: Option<PathBuf>,

	/// Fetch guest RAM pages lazily from this mesh HTTP endpoint during restore
	#[arg(long, value_name = "URL")]
	remote_page_url: Option<String>,

	/// madvise(MADV_MERGEABLE) guest RAM for host KSM dedup
	#[arg(long)]
	ksm: bool,

	/// Number of vCPUs
	#[arg(long, value_name = "N", default_value_t = 1)]
	cpus: u64,

	/// Disk image exposed as virtio-blk /dev/vda
	#[arg(long, value_name = "PATH")]
	rootfs:            Option<PathBuf>,
	/// Independently zstd-framed root disk served with HTTP Range requests
	#[arg(long, value_name = "URL")]
	rootfs_remote_url: Option<String>,

	/// Persistent sparse cache for lazily decompressed root-disk chunks
	#[arg(long, value_name = "PATH")]
	rootfs_remote_cache: Option<PathBuf>,
	/// Local v3 JSON seek index for the independently framed zstd object
	#[arg(long, value_name = "PATH")]
	rootfs_remote_index: Option<PathBuf>,

	/// Guest-visible capacity in bytes (must cover the uncompressed filesystem)
	#[arg(long, value_name = "BYTES")]
	rootfs_remote_size: Option<u64>,

	/// Optional bearer token for the remote root-disk endpoint
	#[arg(long, value_name = "TOKEN")]
	rootfs_remote_bearer: Option<String>,

	/// Open the rootfs read-only
	#[arg(long)]
	rootfs_ro: bool,

	/// initramfs image
	#[arg(long, value_name = "PATH")]
	initrd: Option<PathBuf>,

	/// Host TAP/vmnet interface for virtio-net
	#[arg(long, value_name = "NAME")]
	tap: Option<String>,

	/// Entitlement-free user-mode NAT networking (macOS); only value is `user`
	#[arg(long, value_name = "MODE")]
	net: Option<String>,

	/// Block user-net guest egress except DHCP and one virtual-gateway service.
	#[arg(long)]
	user_net_restricted: bool,

	/// TCP port on the virtual host gateway allowed by restricted user
	/// networking.
	#[arg(long, value_name = "PORT")]
	user_net_allow_host_port: Option<u16>,

	/// Guest MAC (default: 02:00:00:00:00:01)
	#[arg(long, value_name = "ADDR", value_parser = parse_mac)]
	mac: Option<[u8; 6]>,

	/// Virtio transport: mmio|pci (pci x86_64 only)
	#[arg(long, value_name = "KIND", value_parser = parse_transport, default_value = "mmio")]
	transport: Transport,

	/// Pre-created NVIDIA SR-IOV vGPU VF sysfs path; repeatable
	#[arg(long = "vfio-gpu", value_name = "SYSFS")]
	vfio_gpus: Vec<PathBuf>,

	/// Stable VM UUID required by NVIDIA vGPU guests
	#[arg(long, value_name = "UUID", value_parser = parse_uuid)]
	vm_uuid: Option<[u8; 16]>,

	/// virtio-fs mount tag (requires --fs-dir)
	#[arg(long, value_name = "TAG")]
	fs_tag: Option<String>,

	/// Host directory exposed read-only by virtio-fs
	#[arg(long, value_name = "PATH")]
	fs_dir: Option<PathBuf>,

	/// Writable virtio-fs volume <tag>:<host_dir> (:ro read-only); repeatable
	#[arg(long = "volume", value_name = "T:D[:RO]", value_parser = parse_volume)]
	volumes: Vec<FsMount>,

	/// Read-only virtio-fs mount <tag>:<absolute_proxy_socket>; repeatable
	#[arg(long = "remote-fs", value_name = "T:SOCK", value_parser = parse_remote_fs)]
	remote_fs: Vec<RemoteFsMount>,

	/// Unix control socket (pause/resume/snapshot/quit)
	#[arg(long, value_name = "PATH")]
	api_sock:     Option<PathBuf>,
	/// Start paused until an internal control-plane resume request.
	#[arg(long)]
	start_paused: bool,

	/// Root for named JSON lifecycle snapshots
	#[arg(long, value_name = "DIR")]
	snapshot_root: Option<PathBuf>,

	/// Guest-agent socket (under --jail: /run/vmon/*)
	#[arg(long, value_name = "PATH")]
	agent_sock: Option<PathBuf>,

	/// Restore a snapshot directory instead of booting
	#[arg(long, value_name = "DIR")]
	restore: Option<PathBuf>,

	/// Fork (CoW) from a template snapshot directory
	#[arg(long, value_name = "DIR")]
	fork_from: Option<PathBuf>,

	/// Create a new no-overwrite CoW overlay at --rootfs
	#[arg(long, value_name = "BASE")]
	disk_overlay_of: Option<PathBuf>,

	/// Number of children to spawn with --fork-from
	#[arg(long, value_name = "N", default_value_t = 1)]
	count: u64,

	/// Attach a virtio-console agent channel (/dev/hvc0)
	#[arg(long)]
	console_agent: bool,

	/// Attach a virtio-rng entropy device (/dev/hwrng) seeded from the host
	#[arg(long)]
	rng: bool,

	/// Run <cmd> via the guest agent after boot (/bin/sh -c, output on the
	/// guest console); cold boots require --agent-sock
	#[arg(long, value_name = "CMD", allow_hyphen_values = true)]
	agent_exec: Option<String>,

	/// Run inside Linux cgroup/namespace/pivot-root jail
	#[arg(long)]
	jail: bool,

	/// VM/jail id (required with --jail; ASCII alnum, '-' or '_')
	#[arg(long, value_name = "NAME")]
	id: Option<String>,

	/// Absolute jail root directory
	#[arg(long, value_name = "DIR")]
	jail_root: Option<PathBuf>,

	/// cgroup cpu.max value
	#[arg(long, value_name = "V")]
	cgroup_cpu_max: Option<String>,

	/// cgroup memory.max value
	#[arg(long, value_name = "V")]
	cgroup_mem_max: Option<String>,

	/// cgroup pids.max value (>0)
	#[arg(long, value_name = "N")]
	cgroup_pids_max: Option<u64>,

	/// cgroup mode: v2|off
	#[arg(long, value_name = "M", value_parser = parse_cgroup_mode, default_value = "v2")]
	cgroup_mode: CgroupMode,

	/// seccomp action: kill|errno|log
	#[arg(long, value_name = "A", value_parser = parse_seccomp_action, default_value = "errno")]
	seccomp_action: SeccompAction,

	/// Operator-created network namespace path
	#[arg(long, value_name = "PATH")]
	netns: Option<PathBuf>,

	/// Boot mode: direct|uefi
	#[arg(long, value_name = "MODE", value_parser = parse_boot_mode, default_value = "direct")]
	boot_mode: BootMode,

	/// UEFI firmware image for --boot-mode uefi
	#[arg(long, value_name = "PATH")]
	firmware: Option<PathBuf>,

	/// Log format: text|json
	#[arg(long, value_name = "FMT", value_parser = parse_log_format, default_value = "text")]
	log_format: LogFormat,

	/// Log level filter
	#[arg(long, value_name = "LEVEL", default_value = "info")]
	log_level: String,

	/// Disable the default sandbox (not valid with --jail)
	#[arg(long)]
	no_sandbox: bool,

	/// UID drop target (>0); required as root unless --no-sandbox
	#[arg(long, value_name = "UID")]
	sandbox_uid: Option<u32>,

	/// GID drop target (>0); required as root unless --no-sandbox
	#[arg(long, value_name = "GID")]
	sandbox_gid: Option<u32>,

	/// Exit after n seconds (1..=86400)
	#[arg(long, value_name = "N")]
	timeout_secs: Option<u64>,

	/// Production ownership watchdog seconds (1..=86400).
	#[arg(long, value_name = "N")]
	owner_lease_secs: Option<u64>,
}

impl Config {
	/// Parse `args` (flags only, no argv[0]), preserving clap's
	/// `--help`/`--version` exits while returning malformed flags as [`Error`]
	/// for the crate-level error path, then run cross-flag validation.
	pub fn from_args<I: IntoIterator<Item = String>>(args: I) -> Result<Self> {
		let argv = std::iter::once("vmon vmm".to_string()).chain(args);
		match CliArgs::try_parse_from(argv) {
			Ok(cli) => Self::from_cli(cli),
			Err(e) if matches!(e.kind(), ErrorKind::DisplayHelp | ErrorKind::DisplayVersion) => {
				e.exit()
			},
			Err(e) => Err(err(e.to_string())),
		}
	}

	/// Parse `args` without exiting the process. clap failures — including the
	/// `--help`/`--version` requests, which clap models as errors — surface as
	/// [`Error`] so tests can assert on them instead of the process exiting.
	#[cfg(test)]
	pub fn from_iter<I, S>(args: I) -> Result<Self>
	where
		I: IntoIterator<Item = S>,
		S: Into<String>,
	{
		let args: Vec<String> = args.into_iter().map(Into::into).collect();
		let cli = CliArgs::try_parse_from(args).map_err(|e| err(e.to_string()))?;
		Self::from_cli(cli)
	}

	/// Validate parsed flags and lower them into a runtime [`Config`].
	///
	/// Every numeric bound, host-capability gate, and inter-flag invariant lives
	/// here so both entry points share one source of truth.
	fn from_cli(cli: CliArgs) -> Result<Self> {
		let mem_mib = cli.mem;
		let mem_target_mib = cli.mem_target_mib;
		let cpus = cli.cpus;
		let count = cli.count;
		let transport = cli.transport;
		let boot_mode = cli.boot_mode;
		let jail = cli.jail;
		let no_sandbox = cli.no_sandbox;
		// `--net`'s only accepted value is `user`; lower it to the bool the VMM
		// uses, reusing `parse_net` so the diagnostic matches every other flag.
		let user_net = match &cli.net {
			Some(value) => parse_net(value)?,
			None => false,
		};
		let mac_specified = cli.mac.is_some();
		let mac = cli.mac.unwrap_or(DEFAULT_MAC);
		let mut sandbox_uid = cli.sandbox_uid;
		let mut sandbox_gid = cli.sandbox_gid;

		if count == 0 || count > u64::from(MAX_COUNT) {
			bail!("--count must be between 1 and {MAX_COUNT} (got {count})");
		}
		if cpus == 0 || cpus > u64::from(MAX_CPUS) {
			bail!("--cpus must be between 1 and {MAX_CPUS} (got {cpus})");
		}
		if mem_mib == 0 || mem_mib > MAX_MEM_MIB as u64 {
			bail!("--mem must be between 1 and {MAX_MEM_MIB} MiB (got {mem_mib})");
		}
		if cli.rootfs_ro && cli.rootfs.is_none() {
			bail!("--rootfs-ro requires --rootfs");
		}
		if let Some(target) = mem_target_mib
			&& (target == 0 || target >= mem_mib)
		{
			bail!(
				"--mem-target-mib must be between 1 and {} (one less than --mem) (got {target})",
				mem_mib - 1
			);
		}
		if (cli.zram_store_max_mib.is_some() || cli.zram_swap_file.is_some())
			&& mem_target_mib.is_none()
		{
			bail!("--zram-store-max-mib/--zram-swap-file require --mem-target-mib");
		}
		if let Some(store_max) = cli.zram_store_max_mib
			&& (store_max == 0 || store_max > mem_mib)
		{
			bail!("--zram-store-max-mib must be between 1 and {mem_mib} MiB (got {store_max})");
		}
		#[cfg(not(target_os = "linux"))]
		if mem_target_mib.is_some() {
			bail!("--mem-target-mib (transparent paging) requires a Linux host");
		}
		if cli.remote_page_url.as_deref() == Some("") {
			bail!("--remote-page-url must not be empty");
		}
		#[cfg(not(target_os = "linux"))]
		if cli.remote_page_url.is_some() {
			bail!("--remote-page-url requires a Linux host");
		}
		if cli.remote_page_url.is_some() && mem_target_mib.is_some() {
			bail!("--remote-page-url cannot be combined with --mem-target-mib");
		}
		let remote_rootfs = cli.rootfs_remote_url.is_some()
			|| cli.rootfs_remote_cache.is_some()
			|| cli.rootfs_remote_index.is_some()
			|| cli.rootfs_remote_size.is_some()
			|| cli.rootfs_remote_bearer.is_some();
		if cli
			.rootfs_remote_url
			.as_deref()
			.is_some_and(|url| url.trim().is_empty())
		{
			bail!("--rootfs-remote-url must not be empty");
		}
		if let Some(url) = &cli.rootfs_remote_url
			&& !url.starts_with("http://")
			&& !url.starts_with("https://")
		{
			bail!("--rootfs-remote-url must use http:// or https://");
		}
		if cli
			.rootfs_remote_cache
			.as_ref()
			.is_some_and(|path| path.as_os_str().is_empty())
		{
			bail!("--rootfs-remote-cache must not be empty");
		}
		if cli
			.rootfs_remote_index
			.as_ref()
			.is_some_and(|path| path.as_os_str().is_empty())
		{
			bail!("--rootfs-remote-index must not be empty");
		}
		if cli
			.rootfs_remote_bearer
			.as_deref()
			.is_some_and(|token| token.is_empty())
		{
			bail!("--rootfs-remote-bearer must not be empty");
		}
		if remote_rootfs && cli.rootfs_remote_url.is_none() {
			bail!(
				"--rootfs-remote-cache/--rootfs-remote-index/--rootfs-remote-size/\
				 --rootfs-remote-bearer require --rootfs-remote-url"
			);
		}
		if cli.rootfs_remote_url.is_some() && cli.rootfs_remote_cache.is_none() {
			bail!("--rootfs-remote-url requires --rootfs-remote-cache");
		}
		if cli.rootfs_remote_url.is_some() && cli.rootfs_remote_index.is_none() {
			bail!("--rootfs-remote-url requires --rootfs-remote-index");
		}
		if cli.rootfs_remote_url.is_some() && cli.rootfs_remote_size.is_none() {
			bail!("--rootfs-remote-url requires --rootfs-remote-size");
		}
		if cli.rootfs_remote_size == Some(0) {
			bail!("--rootfs-remote-size must be greater than zero");
		}
		if cli.rootfs_remote_url.is_some() && cli.rootfs.is_none() {
			bail!("--rootfs-remote-url requires --rootfs as the private writable overlay");
		}
		if cli.rootfs_remote_url.is_some() && cli.rootfs_ro {
			bail!("--rootfs-remote-url cannot be combined with --rootfs-ro");
		}
		if let (Some(cache), Some(overlay)) = (&cli.rootfs_remote_cache, &cli.rootfs)
			&& cache == overlay
		{
			bail!("--rootfs-remote-cache must differ from --rootfs");
		}
		#[cfg(not(target_os = "linux"))]
		if remote_rootfs {
			bail!("--rootfs-remote-url requires a Linux host");
		}
		if let Some(n) = cli.timeout_secs
			&& !(1..=86_400).contains(&n)
		{
			bail!("--timeout-secs must be between 1 and 86400 (got {n})");
		}
		if let Some(n) = cli.owner_lease_secs
			&& !(1..=86_400).contains(&n)
		{
			bail!("--owner-lease-secs must be between 1 and 86400 (got {n})");
		}
		if cli.cgroup_pids_max == Some(0) {
			bail!("--cgroup-pids-max must be greater than 0");
		}
		if cli.cgroup_cpu_max.as_deref() == Some("") {
			bail!("--cgroup-cpu-max must not be empty");
		}
		if cli.cgroup_mem_max.as_deref() == Some("") {
			bail!("--cgroup-mem-max must not be empty");
		}
		if cli.tap.as_deref() == Some("") {
			bail!("--tap must not be empty");
		}
		if let Some(cmd) = &cli.agent_exec {
			if cmd.len() > 64 * 1024 {
				bail!("--agent-exec command exceeds 64 KiB");
			}
			if cli.agent_sock.is_none() && cli.restore.is_none() && cli.fork_from.is_none() {
				bail!(
					"--agent-exec on a cold boot requires --agent-sock (the hand-off waits for agent \
					 readiness on the agent channel)"
				);
			}
		}

		if cli.fs_tag.is_some() != cli.fs_dir.is_some() {
			bail!("--fs-tag and --fs-dir must be used together");
		}
		if cli.volumes.len() > 8 {
			bail!("at most 8 --volume mounts are supported (got {})", cli.volumes.len());
		}
		{
			let mut seen: Vec<&str> = Vec::new();
			if let Some(tag) = &cli.fs_tag {
				if !is_valid_volume_tag(tag) {
					bail!("--fs-tag must match [a-z0-9_]{{1,32}} (got {tag})");
				}
				seen.push(tag.as_str());
			}
			for m in &cli.volumes {
				let tag = &m.tag;
				if !is_valid_volume_tag(tag) {
					bail!("--volume tag {tag} must match [a-z0-9_]{{1,32}}");
				}
				if seen.contains(&tag.as_str()) {
					bail!("--volume tags must be unique");
				}
				seen.push(tag.as_str());
			}
			for m in &cli.remote_fs {
				let tag = &m.tag;
				if !is_valid_volume_tag(tag) {
					bail!("--remote-fs tag {tag} must match [a-z0-9_]{{1,32}}");
				}
				if !m.sock.is_absolute() {
					bail!("--remote-fs socket path must be absolute (got {})", m.sock.display());
				}
				if seen.contains(&tag.as_str()) {
					bail!("--volume tags must be unique");
				}
				seen.push(tag.as_str());
			}
		}
		if user_net && cli.tap.is_some() {
			bail!("--net user cannot be combined with --tap");
		}
		if cli.user_net_restricted && !user_net {
			bail!("--user-net-restricted requires --net user");
		}
		if cli.user_net_restricted && !cfg!(target_os = "macos") {
			bail!("--user-net-restricted is currently supported only on macOS");
		}
		if cli.user_net_restricted && cli.user_net_allow_host_port.is_none() {
			bail!("--user-net-restricted requires --user-net-allow-host-port");
		}
		if cli.user_net_restricted && cli.user_net_allow_host_port == Some(0) {
			bail!("--user-net-allow-host-port must be nonzero");
		}
		if !cli.user_net_restricted && cli.user_net_allow_host_port.is_some() {
			bail!("--user-net-allow-host-port requires --user-net-restricted");
		}
		if user_net && !cfg!(any(target_os = "macos", target_os = "windows")) {
			bail!("--net user is currently supported only on macOS and Windows");
		}
		if transport == Transport::Pci && !cfg!(target_arch = "x86_64") {
			bail!("--transport pci is only supported on x86_64");
		}
		if !cli.vfio_gpus.is_empty() {
			if !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
				bail!("--vfio-gpu requires an x86_64 Linux KVM host");
			}
			if cli.vfio_gpus.len() > MAX_VFIO_GPUS {
				bail!(
					"at most {MAX_VFIO_GPUS} --vfio-gpu functions are supported (got {})",
					cli.vfio_gpus.len()
				);
			}
			if cli.vm_uuid.is_none() {
				bail!("--vfio-gpu requires --vm-uuid");
			}
			if boot_mode != BootMode::Direct {
				bail!("--vfio-gpu currently requires --boot-mode direct");
			}
			if cli.restore.is_some() || cli.fork_from.is_some() {
				bail!("--vfio-gpu cannot be combined with --restore or --fork-from");
			}
			if mem_target_mib.is_some() || cli.remote_page_url.is_some() {
				bail!("--vfio-gpu cannot be combined with guest-memory paging");
			}
			let mut seen = Vec::with_capacity(cli.vfio_gpus.len());
			for path in &cli.vfio_gpus {
				if !path.is_absolute() || !path.starts_with("/sys/bus/pci/devices") {
					bail!(
						"--vfio-gpu must name an absolute SR-IOV VF under /sys/bus/pci/devices (got {})",
						path.display()
					);
				}
				if seen.contains(path) {
					bail!("--vfio-gpu paths must be unique");
				}
				seen.push(path.clone());
			}
		}
		if count > 1 && cli.fork_from.is_none() {
			bail!("--count greater than 1 requires --fork-from");
		}
		match (boot_mode, cli.firmware.is_some()) {
			(BootMode::Uefi, false) => bail!("--boot-mode uefi requires --firmware"),
			(BootMode::Direct, true) => bail!("--firmware requires --boot-mode uefi"),
			(BootMode::Uefi, true) | (BootMode::Direct, false) => {},
		}
		if boot_mode == BootMode::Uefi {
			if cli.kernel.is_some() {
				bail!("--kernel is only supported with --boot-mode direct");
			}
			if cli.initrd.is_some() {
				bail!("--initrd is only supported with --boot-mode direct");
			}
		}
		if let Some(id) = &cli.id {
			validate_id(id)?;
		}
		if jail && cli.id.is_none() {
			bail!("--jail requires --id");
		}
		if let Some(root) = &cli.jail_root {
			if !jail {
				bail!("--jail-root requires --jail");
			}
			validate_jail_root(root)?;
		}
		if jail && let Some(sock) = &cli.agent_sock {
			validate_jail_agent_sock(sock)?;
		}
		if no_sandbox && jail {
			bail!("--no-sandbox cannot be combined with --jail");
		}
		if no_sandbox && cli.seccomp_action != SeccompAction::Errno {
			bail!("--seccomp-action requires the sandbox or --jail");
		}
		if cli.netns.is_some() && !jail {
			bail!("--netns requires --jail");
		}
		if !jail {
			if cli.cgroup_cpu_max.is_some()
				|| cli.cgroup_mem_max.is_some()
				|| cli.cgroup_pids_max.is_some()
			{
				bail!("--cgroup-cpu-max/--cgroup-mem-max/--cgroup-pids-max require --jail");
			}
			if cli.cgroup_mode == CgroupMode::Off {
				bail!("--cgroup-mode off requires --jail");
			}
		}
		// A fresh direct boot needs a kernel. UEFI boots enter firmware instead;
		// restore/fork reconstruct state from a snapshot.
		if boot_mode == BootMode::Direct
			&& cli.kernel.is_none()
			&& cli.restore.is_none()
			&& cli.fork_from.is_none()
		{
			bail!("--kernel <path> is required for --boot-mode direct (unless --restore/--fork-from)");
		}
		if cli.restore.is_some() && cli.fork_from.is_some() {
			bail!("--restore and --fork-from are mutually exclusive");
		}
		if mem_target_mib.is_some() && cli.fork_from.is_some() {
			bail!(
				"--mem-target-mib is not supported with --fork-from; fork RAM is already demand-paged \
				 through a private file mapping, while pager eviction cannot preserve private dirty \
				 pages"
			);
		}
		if cli.remote_page_url.is_some() && cli.restore.is_none() {
			bail!("--remote-page-url requires --restore");
		}
		if let Some(base) = &cli.disk_overlay_of {
			validate_overlay_paths(base, cli.rootfs.as_deref())?;
		}
		let filters_enabled = jail || !no_sandbox;
		if filters_enabled {
			if sandbox_uid.is_none() {
				sandbox_uid = parse_env_u32("VMON_SANDBOX_UID")?;
			}
			if sandbox_gid.is_none() {
				sandbox_gid = parse_env_u32("VMON_SANDBOX_GID")?;
			}
			validate_sandbox_identity(sandbox_uid, sandbox_gid)?;
		} else if sandbox_uid.is_some() || sandbox_gid.is_some() {
			bail!("--sandbox-uid/--sandbox-gid require the sandbox (drop --no-sandbox) or --jail");
		}

		// Stage-B filters default on, so a root launch must drop to an
		// unprivileged uid/gid (or opt out with --no-sandbox); otherwise the
		// VMM would keep running as root with the sandbox active, defeating the
		// privilege drop. The jailer enforces its own uid/gid requirement after
		// pivot_root, so --jail is exempt here. Non-root launches need no
		// uid/gid — the drop is a no-op when not root.
		#[cfg(target_os = "linux")]
		{
			// SAFETY: `getuid` is thread-safe and has no preconditions.
			let running_as_root = unsafe { libc::getuid() == 0 };
			if filters_enabled
				&& !jail && running_as_root
				&& (sandbox_uid.is_none() || sandbox_gid.is_none())
			{
				bail!(
					"running as root with the sandbox enabled requires --sandbox-uid/--sandbox-gid (or \
					 pass --no-sandbox)"
				);
			}
		}

		let console_agent = cli.console_agent || cli.agent_exec.is_some() || cli.agent_sock.is_some();

		Ok(Self {
			kernel: cli.kernel,
			cmdline: cli.cmdline,
			mem_mib: mem_mib as usize,
			mem_target_mib: mem_target_mib.map(|v| v as usize),
			zram_store_max_mib: cli.zram_store_max_mib.map(|v| v as usize),
			zram_swap_file: cli.zram_swap_file,
			remote_page_url: cli.remote_page_url,
			rootfs_remote_url: cli.rootfs_remote_url,
			rootfs_remote_cache: cli.rootfs_remote_cache,
			rootfs_remote_index: cli.rootfs_remote_index,
			rootfs_remote_size: cli.rootfs_remote_size,
			rootfs_remote_bearer: cli.rootfs_remote_bearer,
			ksm: cli.ksm,
			cpus: cpus as u8,
			rootfs: cli.rootfs,
			rootfs_read_only: cli.rootfs_ro,
			initrd: cli.initrd,
			tap: cli.tap,
			user_net,
			user_net_restricted: cli.user_net_restricted,
			user_net_allow_host_port: cli.user_net_allow_host_port,
			mac_specified,
			mac,
			transport,
			vfio_gpus: cli.vfio_gpus,
			vm_uuid: cli.vm_uuid,
			fs_tag: cli.fs_tag,
			fs_dir: cli.fs_dir,
			api_sock: cli.api_sock,
			start_paused: cli.start_paused,
			snapshot_root: cli.snapshot_root,
			agent_sock: cli.agent_sock,
			restore: cli.restore,
			fork_from: cli.fork_from,
			disk_overlay_of: cli.disk_overlay_of,
			count: count as u32,
			console_agent,
			rng: cli.rng,
			agent_exec: cli.agent_exec,
			jail,
			id: cli.id,
			jail_root: cli.jail_root,
			cgroup_cpu_max: cli.cgroup_cpu_max,
			cgroup_mem_max: cli.cgroup_mem_max,
			cgroup_pids_max: cli.cgroup_pids_max,
			cgroup_mode: cli.cgroup_mode,
			seccomp_action: cli.seccomp_action,
			netns: cli.netns,
			boot_mode,
			firmware: cli.firmware,
			log_format: cli.log_format,
			log_level: cli.log_level,
			no_sandbox,
			sandbox_uid,
			sandbox_gid,
			timeout_secs: cli.timeout_secs,
			owner_lease_secs: cli.owner_lease_secs,
			volumes: cli.volumes,
			remote_fs: cli.remote_fs,
		})
	}
}

fn validate_overlay_paths(base: &Path, rootfs: Option<&Path>) -> Result<()> {
	let Some(dest) = rootfs else {
		bail!("--disk-overlay-of requires --rootfs (the overlay destination)");
	};
	if dest == base {
		bail!("--disk-overlay-of base must differ from --rootfs");
	}

	let base_meta = std::fs::symlink_metadata(base)
		.map_err(|e| err(format!("checking --disk-overlay-of base {}: {e}", base.display())))?;
	if base_meta.file_type().is_symlink() {
		bail!("--disk-overlay-of base {base:?} must be a regular file, not a symlink");
	}
	if !base_meta.is_file() {
		bail!("--disk-overlay-of base {base:?} must be a regular file");
	}

	match std::fs::symlink_metadata(dest) {
		Ok(meta) => {
			let kind = if meta.file_type().is_symlink() {
				"symlink"
			} else if meta.is_dir() {
				"directory"
			} else {
				"file"
			};
			bail!(
				"--rootfs overlay destination {dest:?} already exists as a {kind}; choose a new path"
			);
		},
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
		Err(e) => Err(err(format!("checking --rootfs overlay destination {}: {e}", dest.display()))),
	}
}

fn parse_env_u32(name: &str) -> Result<Option<u32>> {
	match std::env::var(name) {
		Ok(value) => value
			.parse()
			.map(Some)
			.map_err(|_| err(format!("invalid value {value} for {name}"))),
		Err(std::env::VarError::NotPresent) => Ok(None),
		Err(std::env::VarError::NotUnicode(_)) => Err(err(format!("{name} must be valid UTF-8"))),
	}
}

fn parse_mac(s: &str) -> Result<[u8; 6]> {
	let parts: Vec<&str> = s.split(':').collect();
	if parts.len() != 6 {
		bail!("invalid MAC {s} (expected aa:bb:cc:dd:ee:ff)");
	}
	let mut mac = [0u8; 6];
	for (i, p) in parts.iter().enumerate() {
		if p.len() != 2 {
			bail!("invalid MAC byte {p} in {s} (expected two hex digits)");
		}
		mac[i] = u8::from_str_radix(p, 16).map_err(|_| err(format!("invalid MAC byte {p}")))?;
	}
	if mac[0] & 1 != 0 {
		bail!("invalid MAC {s} (guest MAC must be unicast)");
	}
	Ok(mac)
}

fn parse_uuid(value: &str) -> Result<[u8; 16]> {
	let bytes = value.as_bytes();
	if bytes.len() != 36
		|| bytes[8] != b'-'
		|| bytes[13] != b'-'
		|| bytes[18] != b'-'
		|| bytes[23] != b'-'
	{
		bail!("invalid UUID {value:?} (expected xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)");
	}
	let mut uuid = [0u8; 16];
	let mut source = 0;
	for byte in &mut uuid {
		while matches!(source, 8 | 13 | 18 | 23) {
			source += 1;
		}
		let high = hex_nibble(bytes[source])
			.ok_or_else(|| err(format!("invalid hexadecimal digit in UUID {value:?}")))?;
		let low = hex_nibble(bytes[source + 1])
			.ok_or_else(|| err(format!("invalid hexadecimal digit in UUID {value:?}")))?;
		*byte = high << 4 | low;
		source += 2;
	}
	if uuid == [0; 16] {
		bail!("--vm-uuid must not be the nil UUID");
	}
	Ok(uuid)
}

const fn hex_nibble(byte: u8) -> Option<u8> {
	match byte {
		b'0'..=b'9' => Some(byte - b'0'),
		b'a'..=b'f' => Some(byte - b'a' + 10),
		b'A'..=b'F' => Some(byte - b'A' + 10),
		_ => None,
	}
}

fn parse_transport(s: &str) -> Result<Transport> {
	match s {
		"mmio" => Ok(Transport::Mmio),
		"pci" => Ok(Transport::Pci),
		_ => bail!("invalid --transport {s} (expected mmio or pci)"),
	}
}

fn parse_net(s: &str) -> Result<bool> {
	match s {
		"user" => Ok(true),
		_ => bail!("invalid --net {s} (expected user)"),
	}
}

fn parse_cgroup_mode(s: &str) -> Result<CgroupMode> {
	match s {
		"v2" => Ok(CgroupMode::V2),
		"off" => Ok(CgroupMode::Off),
		_ => bail!("invalid --cgroup-mode {s} (expected v2 or off)"),
	}
}

fn parse_seccomp_action(s: &str) -> Result<SeccompAction> {
	match s {
		"kill" => Ok(SeccompAction::Kill),
		"errno" => Ok(SeccompAction::Errno),
		"log" => Ok(SeccompAction::Log),
		_ => bail!("invalid --seccomp-action {s} (expected kill, errno, or log)"),
	}
}

fn parse_boot_mode(s: &str) -> Result<BootMode> {
	match s {
		"direct" => Ok(BootMode::Direct),
		"uefi" => Ok(BootMode::Uefi),
		_ => bail!("invalid --boot-mode {s} (expected direct or uefi)"),
	}
}

fn parse_log_format(s: &str) -> Result<LogFormat> {
	match s {
		"text" => Ok(LogFormat::Text),
		"json" => Ok(LogFormat::Json),
		_ => bail!("invalid --log-format {s} (expected text or json)"),
	}
}

fn parse_volume(s: &str) -> Result<FsMount> {
	let (body, read_only) = match s.strip_suffix(":ro") {
		Some(rest) => (rest, true),
		None => (s, false),
	};
	let (tag, dir) = body
		.split_once(':')
		.ok_or_else(|| err(format!("invalid --volume {s} (expected <tag>:<dir>[:ro])")))?;
	if tag.is_empty() || dir.is_empty() {
		bail!("invalid --volume {s} (expected <tag>:<dir>[:ro])");
	}
	Ok(FsMount { tag: tag.to_string(), dir: PathBuf::from(dir), read_only })
}

fn parse_remote_fs(s: &str) -> Result<RemoteFsMount> {
	let (tag, sock) = s
		.split_once(':')
		.ok_or_else(|| err(format!("invalid --remote-fs {s} (expected <tag>:<absolute-socket>)")))?;
	if tag.is_empty() || sock.is_empty() {
		bail!("invalid --remote-fs {s} (expected <tag>:<absolute-socket>)");
	}
	if !is_valid_volume_tag(tag) {
		bail!("--remote-fs tag {tag} must match [a-z0-9_]{{1,32}}");
	}
	let sock = PathBuf::from(sock);
	if !sock.is_absolute() {
		bail!("--remote-fs socket path must be absolute (got {})", sock.display());
	}
	Ok(RemoteFsMount { tag: tag.to_owned(), sock })
}

fn is_valid_volume_tag(tag: &str) -> bool {
	!tag.is_empty()
		&& tag.len() <= 32
		&& tag
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn validate_id(id: &str) -> Result<()> {
	if id.is_empty()
		|| id.len() > 64
		|| !id
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		bail!("--id must be 1..=64 ASCII letters, digits, '-' or '_'");
	}
	Ok(())
}

fn validate_jail_root(root: &Path) -> Result<()> {
	let escapes = root
		.components()
		.any(|component| matches!(component, std::path::Component::ParentDir));
	if !root.is_absolute() || escapes {
		bail!("--jail-root must be an absolute path without '..' components (got {root:?})");
	}
	Ok(())
}

fn validate_sandbox_identity(uid: Option<u32>, gid: Option<u32>) -> Result<()> {
	if uid == Some(0) {
		bail!("--sandbox-uid/VMON_SANDBOX_UID must be greater than 0");
	}
	if gid == Some(0) {
		bail!("--sandbox-gid/VMON_SANDBOX_GID must be greater than 0");
	}
	Ok(())
}

fn validate_jail_agent_sock(sock: &Path) -> Result<()> {
	let root = Path::new("/run/vmon");
	let escapes = sock
		.components()
		.any(|component| matches!(component, std::path::Component::ParentDir));
	if !sock.is_absolute() || escapes || sock == root || !sock.starts_with(root) {
		bail!("--agent-sock must be inside /run/vmon when --jail is enabled (got {sock:?})");
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicU64, Ordering};

	use super::*;

	static NEXT_TEMP_DIR: AtomicU64 = AtomicU64::new(0);

	struct TestDir {
		path: PathBuf,
	}

	impl TestDir {
		fn new() -> Self {
			let unique = format!(
				"vmon-config-test-{}-{}",
				std::process::id(),
				NEXT_TEMP_DIR.fetch_add(1, Ordering::Relaxed)
			);
			let path = std::env::temp_dir().join(unique);
			std::fs::create_dir(&path).expect("create temp config test dir");
			Self { path }
		}

		fn path(&self) -> &Path {
			&self.path
		}
	}

	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.path);
		}
	}

	fn parse_config(args: &[&str]) -> Result<Config> {
		Config::from_iter(args.iter().copied())
	}

	fn config_err(args: &[&str]) -> String {
		match parse_config(args) {
			Ok(_) => panic!("expected config parse to fail"),
			Err(e) => e.to_string(),
		}
	}

	fn assert_config_err_contains(args: &[&str], expected: &str) {
		let err = config_err(args);
		assert!(err.contains(expected), "expected error containing {expected}, got {err}");
	}
	#[test]
	fn remote_rootfs_flags_require_complete_nonempty_group() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--rootfs-remote-url", "https://example/image"],
			"requires --rootfs-remote-cache",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--rootfs-remote-cache", "/tmp/cache"],
			"require --rootfs-remote-url",
		);
		assert_config_err_contains(
			&[
				"vmon",
				"--kernel",
				"k",
				"--rootfs",
				"/tmp/root",
				"--rootfs-remote-url",
				"",
				"--rootfs-remote-cache",
				"/tmp/cache",
			],
			"must not be empty",
		);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn parses_remote_rootfs_contract() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--rootfs",
			"/tmp/overlay",
			"--rootfs-remote-url",
			"https://example/image",
			"--rootfs-remote-cache",
			"/tmp/cache",
			"--rootfs-remote-index",
			"/tmp/index.json",
			"--rootfs-remote-size",
			"68719476736",
			"--rootfs-remote-bearer",
			"secret",
			"--no-sandbox",
		])
		.expect("remote rootfs flags");
		assert_eq!(cfg.rootfs_remote_url.as_deref(), Some("https://example/image"));
		assert_eq!(cfg.rootfs_remote_cache.as_deref(), Some(Path::new("/tmp/cache")));
		assert_eq!(cfg.rootfs_remote_index.as_deref(), Some(Path::new("/tmp/index.json")));
		assert_eq!(cfg.rootfs_remote_size, Some(68_719_476_736));
		assert_eq!(cfg.rootfs_remote_bearer.as_deref(), Some("secret"));
	}

	#[test]
	fn parses_arguments_from_iter_without_process_args() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"vmlinux",
			"--mem",
			"512",
			"--cpus",
			"2",
			"--fs-tag",
			"hostshare",
			"--fs-dir",
			"/srv/share",
			"--agent-sock",
			"/tmp/vmon-test-agent.sock",
			"--agent-exec",
			"id",
		])
		.expect("parse config from iterator");

		assert_eq!(cfg.mem_mib, 512);
		assert_eq!(cfg.cpus, 2);
		assert!(cfg.console_agent);
		assert!(!cfg.mac_specified);
		assert_eq!(cfg.fs_tag.as_deref(), Some("hostshare"));
		assert_eq!(cfg.fs_dir.as_deref(), Some(Path::new("/srv/share")));
	}

	#[test]
	fn defaults_platform_registry_fields() {
		let cfg = parse_config(&["vmon", "--kernel", "k"]).expect("defaults parse");

		assert!(cfg.snapshot_root.is_none());
		assert!(cfg.agent_sock.is_none());
		assert!(!cfg.jail);
		assert!(cfg.id.is_none());
		assert!(cfg.jail_root.is_none());
		assert!(cfg.cgroup_cpu_max.is_none());
		assert!(cfg.cgroup_mem_max.is_none());
		assert!(cfg.cgroup_pids_max.is_none());
		assert_eq!(cfg.cgroup_mode, CgroupMode::V2);
		assert_eq!(cfg.seccomp_action, SeccompAction::Errno);
		assert!(cfg.netns.is_none());
		assert!(!cfg.user_net);
		assert!(!cfg.user_net_restricted);
		assert_eq!(cfg.boot_mode, BootMode::Direct);
		assert!(cfg.firmware.is_none());
		assert_eq!(cfg.log_format, LogFormat::Text);
		assert_eq!(cfg.log_level, "info");
		assert!(!cfg.no_sandbox);
	}

	#[test]
	fn start_paused_is_an_internal_cli_flag() {
		let cfg = parse_config(&["vmon", "--kernel", "k", "--start-paused"])
			.expect("internal start-paused flag parses");
		assert!(cfg.start_paused);

		let cfg = parse_config(&["vmon", "--kernel", "k"]).expect("default config parses");
		assert!(!cfg.start_paused);
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn parses_platform_registry_flags() {
		let cfg = parse_config(&[
			"vmon",
			"--snapshot-root",
			"/var/lib/vmon/snaps",
			"--agent-sock",
			"/run/vmon/agent.sock",
			"--jail",
			"--id",
			"vm-01_alpha",
			"--jail-root",
			"/srv/jailer/vm-01",
			"--cgroup-cpu-max",
			"100000 100000",
			"--cgroup-mem-max",
			"512M",
			"--cgroup-pids-max",
			"2048",
			"--cgroup-mode",
			"off",
			"--seccomp-action",
			"log",
			"--netns",
			"/run/netns/vm-01",
			"--boot-mode",
			"uefi",
			"--firmware",
			"/usr/share/OVMF/OVMF_CODE.fd",
			"--log-format",
			"json",
			"--log-level",
			"debug",
		])
		.expect("platform registry flags parse");

		assert_eq!(cfg.snapshot_root.as_deref(), Some(Path::new("/var/lib/vmon/snaps")));
		assert_eq!(cfg.agent_sock.as_deref(), Some(Path::new("/run/vmon/agent.sock")));
		assert!(cfg.console_agent);
		assert!(cfg.jail);
		assert_eq!(cfg.id.as_deref(), Some("vm-01_alpha"));
		assert_eq!(cfg.jail_root.as_deref(), Some(Path::new("/srv/jailer/vm-01")));
		assert_eq!(cfg.cgroup_cpu_max.as_deref(), Some("100000 100000"));
		assert_eq!(cfg.cgroup_mem_max.as_deref(), Some("512M"));
		assert_eq!(cfg.cgroup_pids_max, Some(2048));
		assert_eq!(cfg.cgroup_mode, CgroupMode::Off);
		assert_eq!(cfg.seccomp_action, SeccompAction::Log);
		assert_eq!(cfg.netns.as_deref(), Some(Path::new("/run/netns/vm-01")));
		assert_eq!(cfg.boot_mode, BootMode::Uefi);
		assert_eq!(cfg.firmware.as_deref(), Some(Path::new("/usr/share/OVMF/OVMF_CODE.fd")));
		assert_eq!(cfg.log_format, LogFormat::Json);
		assert_eq!(cfg.log_level, "debug");
		assert!(!cfg.no_sandbox);

		// The sandbox is default-on; --no-sandbox opts out.
		let opt_out = parse_config(&["vmon", "--kernel", "k", "--no-sandbox"])
			.expect("--no-sandbox opts out of the default sandbox");
		assert!(opt_out.no_sandbox);
	}

	#[test]
	fn rejects_invalid_platform_enum_values() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--cgroup-mode", "v1"],
			"invalid --cgroup-mode",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--seccomp-action", "trace"],
			"invalid --seccomp-action",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--boot-mode", "bios"],
			"invalid --boot-mode",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--log-format", "yaml"],
			"invalid --log-format",
		);
	}

	#[test]
	fn validates_platform_boot_and_id_flags() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--boot-mode", "uefi"],
			"requires --firmware",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--firmware", "OVMF.fd"],
			"--firmware requires --boot-mode uefi",
		);
		let cfg = parse_config(&["vmon", "--boot-mode", "uefi", "--firmware", "OVMF.fd"])
			.expect("UEFI boot does not require --kernel");
		assert_eq!(cfg.boot_mode, BootMode::Uefi);
		assert_eq!(cfg.firmware.as_deref(), Some(Path::new("OVMF.fd")));
		assert_config_err_contains(&["vmon", "--kernel", "k", "--id", ""], "--id");
		assert_config_err_contains(&["vmon", "--kernel", "k", "--id", "bad.name"], "--id");
		let long_id = "a".repeat(65);
		assert_config_err_contains(&["vmon", "--kernel", "k", "--id", long_id.as_str()], "--id");
		assert_config_err_contains(&["vmon", "--kernel", "k", "--jail"], "--jail requires --id");
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--jail-root", "/srv/jailer/vm1"],
			"--jail-root requires --jail",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--jail", "--id", "vm1", "--jail-root", "relative"],
			"--jail-root must be an absolute path",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--jail", "--id", "vm1", "--agent-sock", "/tmp/agent.sock"],
			"--agent-sock must be inside /run/vmon",
		);
	}

	#[test]
	fn enforces_resource_bounds() {
		assert_config_err_contains(&["vmon", "--kernel", "k", "--count", "0"], "--count");
		let too_many_count = (MAX_COUNT + 1).to_string();
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--count", too_many_count.as_str()],
			"--count",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--count", "2"],
			"--count greater than 1 requires --fork-from",
		);
		let fanout = parse_config(&["vmon", "--fork-from", "snapshot", "--count", "2"])
			.expect("--count > 1 parses with --fork-from");
		assert_eq!(fanout.count, 2);

		assert_config_err_contains(&["vmon", "--kernel", "k", "--cpus", "0"], "--cpus");
		let too_many_cpus = (u16::from(MAX_CPUS) + 1).to_string();
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--cpus", too_many_cpus.as_str()],
			"--cpus",
		);

		assert_config_err_contains(&["vmon", "--kernel", "k", "--mem", "0"], "--mem");
		let too_much_mem = (MAX_MEM_MIB + 1).to_string();
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--mem", too_much_mem.as_str()],
			"--mem",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--cgroup-pids-max", "0"],
			"--cgroup-pids-max must be greater than 0",
		);
	}

	#[test]
	fn requires_fs_tag_and_dir_to_be_paired() {
		assert_config_err_contains(&["vmon", "--kernel", "k", "--fs-tag", "host"], "--fs-tag");
		assert_config_err_contains(&["vmon", "--kernel", "k", "--fs-dir", "/srv"], "--fs-tag");
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--fs-tag", "Bad-Tag", "--fs-dir", "/srv"],
			"--fs-tag must match [a-z0-9_]",
		);

		let cfg = parse_config(&["vmon", "--kernel", "k", "--fs-tag", "host", "--fs-dir", "/srv"])
			.expect("paired virtio-fs flags parse");
		assert_eq!(cfg.fs_tag.as_deref(), Some("host"));
		assert_eq!(cfg.fs_dir.as_deref(), Some(Path::new("/srv")));
	}

	#[test]
	fn rejects_invalid_transport_value() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--transport", "sbus"],
			"invalid --transport",
		);
	}

	#[test]
	fn rejects_invalid_net_value() {
		assert_config_err_contains(&["vmon", "--kernel", "k", "--net", "bridge"], "invalid --net");
	}

	#[test]
	fn rejects_user_net_with_tap() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--net", "user", "--tap", "tap0"],
			"cannot be combined",
		);
	}

	#[cfg(any(target_os = "macos", target_os = "windows"))]
	#[test]
	fn accepts_user_net_on_supported_hosts() {
		let cfg = parse_config(&["vmon", "--kernel", "k", "--net", "user"])
			.expect("--net user parses on macOS");
		assert!(cfg.user_net);
		assert!(cfg.tap.is_none());
		assert!(!cfg.user_net_restricted);
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn accepts_restricted_user_net_on_supported_hosts() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--net",
			"user",
			"--user-net-restricted",
			"--user-net-allow-host-port",
			"8443",
		])
		.expect("restricted user networking parses on supported hosts");
		assert!(cfg.user_net);
		assert!(cfg.user_net_restricted);
		assert_eq!(cfg.user_net_allow_host_port, Some(8443));
	}

	#[cfg(target_os = "macos")]
	#[test]
	fn rejects_restricted_user_net_without_a_nonzero_gateway_port() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--net", "user", "--user-net-restricted"],
			"requires --user-net-allow-host-port",
		);
		assert_config_err_contains(
			&[
				"vmon",
				"--kernel",
				"k",
				"--net",
				"user",
				"--user-net-restricted",
				"--user-net-allow-host-port",
				"0",
			],
			"must be nonzero",
		);
	}

	#[test]
	fn rejects_gateway_port_without_restricted_user_net() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--user-net-allow-host-port", "8443"],
			"requires --user-net-restricted",
		);
	}

	#[cfg(not(target_os = "macos"))]
	#[test]
	fn rejects_restricted_user_net_on_unsupported_hosts() {
		assert_config_err_contains(
			&[
				"vmon",
				"--kernel",
				"k",
				"--net",
				"user",
				"--user-net-restricted",
				"--user-net-allow-host-port",
				"8443",
			],
			"supported only on macOS",
		);
	}

	#[test]
	fn rejects_restricted_user_net_without_user_net() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--user-net-restricted"],
			"requires --net user",
		);
	}

	#[test]
	fn marks_explicit_mac_override() {
		let cfg = parse_config(&["vmon", "--kernel", "k", "--mac", "02:00:00:00:00:2a"])
			.expect("explicit --mac parses");
		assert!(cfg.mac_specified);
		assert_eq!(cfg.mac, [0x02, 0x00, 0x00, 0x00, 0x00, 0x2a]);
	}

	#[test]
	fn rejects_invalid_mac_overrides() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--mac", "2:00:00:00:00:01"],
			"expected two hex digits",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--mac", "03:00:00:00:00:01"],
			"guest MAC must be unicast",
		);
	}

	#[cfg(not(any(target_os = "macos", target_os = "windows")))]
	#[test]
	fn rejects_user_net_on_unsupported_hosts() {
		assert_config_err_contains(&["vmon", "--kernel", "k", "--net", "user"], "macOS and Windows");
	}

	#[cfg(target_arch = "x86_64")]
	#[test]
	fn accepts_pci_transport_on_x86_64() {
		let cfg = parse_config(&["vmon", "--kernel", "k", "--transport", "pci"])
			.expect("pci transport parses on x86_64");
		assert!(matches!(cfg.transport, Transport::Pci));
	}

	#[cfg(not(target_arch = "x86_64"))]
	#[test]
	fn rejects_pci_transport_off_x86_64() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--transport", "pci"],
			"only supported on x86_64",
		);
	}

	#[cfg(not(target_os = "windows"))]
	#[test]
	fn netns_requires_jail_validation() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--netns", "/run/netns/x"],
			"--netns requires --jail",
		);
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--netns",
			"/run/netns/x",
			"--jail",
			"--jail-root",
			"/srv/jailer/vm-01",
			"--id",
			"vm-01",
		])
		.expect("netns with jail works");
		assert_eq!(cfg.netns.as_deref(), Some(std::path::Path::new("/run/netns/x")));
		assert!(cfg.jail);
	}
	#[test]
	fn rejects_invalid_sandbox_identity_flags() {
		// The sandbox is default-on, so identity flags only require an enabled
		// sandbox unless it is explicitly disabled (--no-sandbox) and --jail is unset.
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--no-sandbox", "--sandbox-uid", "1000"],
			"require the sandbox",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--no-sandbox", "--sandbox-gid", "1000"],
			"require the sandbox",
		);
		assert_config_err_contains(&["vmon", "--kernel", "k", "--sandbox-uid", "0"], "--sandbox-uid");
		assert_config_err_contains(&["vmon", "--kernel", "k", "--sandbox-gid", "0"], "--sandbox-gid");
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn parses_explicit_sandbox_identity_when_filters_enabled() {
		let cfg =
			parse_config(&["vmon", "--kernel", "k", "--sandbox-uid", "1000", "--sandbox-gid", "1001"])
				.expect("explicit sandbox identity parses on Linux");
		assert_eq!(cfg.sandbox_uid, Some(1000));
		assert_eq!(cfg.sandbox_gid, Some(1001));
	}

	#[test]
	fn parses_explicit_sandbox_identity_for_jail() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--jail",
			"--id",
			"vm1",
			"--sandbox-uid",
			"1000",
			"--sandbox-gid",
			"1001",
		])
		.expect("explicit jail sandbox identity parses");

		assert!(cfg.jail);
		assert_eq!(cfg.sandbox_uid, Some(1000));
		assert_eq!(cfg.sandbox_gid, Some(1001));
	}

	#[cfg(not(target_os = "linux"))]
	#[test]
	fn sandbox_defaults_on_and_opts_out_on_non_linux() {
		// Filters are default-on and config parsing is host-agnostic, so a
		// non-Linux host parses the default and honors the opt-out.
		let cfg = parse_config(&["vmon", "--kernel", "k"]).expect("default config parses");
		assert!(!cfg.no_sandbox);

		let opt_out =
			parse_config(&["vmon", "--kernel", "k", "--no-sandbox"]).expect("--no-sandbox parses");
		assert!(opt_out.no_sandbox);
	}

	#[test]
	fn rejects_existing_overlay_destination_from_args() {
		let tmp = TestDir::new();
		let base = tmp.path().join("base.img");
		let dest = tmp.path().join("overlay.img");
		std::fs::write(&base, b"base").expect("write base disk");
		std::fs::write(&dest, b"existing").expect("write existing overlay");

		let err = config_err(&[
			"vmon",
			"--kernel",
			"k",
			"--rootfs",
			dest.to_str().expect("utf-8 temp path"),
			"--disk-overlay-of",
			base.to_str().expect("utf-8 temp path"),
		]);
		assert!(err.contains("already exists"), "unexpected error: {err}");
	}

	#[cfg(unix)]
	#[test]
	fn rejects_overlay_base_symlink_from_args() {
		use std::os::unix::fs::symlink;

		let tmp = TestDir::new();
		let real_base = tmp.path().join("base.img");
		let link_base = tmp.path().join("base-link.img");
		let dest = tmp.path().join("overlay.img");
		std::fs::write(&real_base, b"base").expect("write base disk");
		symlink(&real_base, &link_base).expect("create base symlink");

		let err = config_err(&[
			"vmon",
			"--kernel",
			"k",
			"--rootfs",
			dest.to_str().expect("utf-8 temp path"),
			"--disk-overlay-of",
			link_base.to_str().expect("utf-8 temp path"),
		]);
		assert!(err.contains("symlink"), "unexpected error: {err}");
	}

	#[test]
	fn timeout_secs_parses_and_validates_range() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--timeout-secs", "0"],
			"--timeout-secs must be between 1 and 86400",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--timeout-secs", "86401"],
			"--timeout-secs must be between 1 and 86400",
		);
		let cfg = parse_config(&["vmon", "--kernel", "k", "--timeout-secs", "120"])
			.expect("valid --timeout-secs parses");
		assert_eq!(cfg.timeout_secs, Some(120));
	}

	#[test]
	fn owner_lease_secs_parses_and_validates_range() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--owner-lease-secs", "0"],
			"--owner-lease-secs must be between 1 and 86400",
		);
		let cfg = parse_config(&["vmon", "--kernel", "k", "--owner-lease-secs", "15"])
			.expect("valid --owner-lease-secs parses");
		assert_eq!(cfg.owner_lease_secs, Some(15));
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn pager_flags_parse_and_validate() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--no-sandbox",
			"--mem",
			"256",
			"--mem-target-mib",
			"128",
			"--zram-store-max-mib",
			"32",
			"--zram-swap-file",
			"/tmp/vmon.swap",
			"--ksm",
		])
		.expect("pager flags parse");
		assert_eq!(cfg.mem_target_mib, Some(128));
		assert_eq!(cfg.zram_store_max_mib, Some(32));
		assert_eq!(cfg.zram_swap_file.as_deref(), Some(Path::new("/tmp/vmon.swap")));
		assert!(cfg.ksm);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn pager_flags_reject_invalid_combinations() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--mem", "256", "--mem-target-mib", "256"],
			"--mem-target-mib must be between 1 and 255",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--zram-store-max-mib", "64"],
			"--zram-store-max-mib/--zram-swap-file require --mem-target-mib",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--mem-target-mib", "128", "--fork-from", "snapshot"],
			"--mem-target-mib is not supported with --fork-from",
		);
	}

	#[test]
	fn parses_volume_mounts() {
		let cfg = parse_config(&[
			"vmon",
			"--kernel",
			"k",
			"--volume",
			"data:/srv/d",
			"--volume",
			"cache:/srv/c:ro",
		])
		.expect("volume mounts parse");
		assert_eq!(cfg.volumes.len(), 2);
		assert_eq!(cfg.volumes[0].tag, "data");
		assert_eq!(cfg.volumes[0].dir, Path::new("/srv/d"));
		assert!(!cfg.volumes[0].read_only);
		assert_eq!(cfg.volumes[1].tag, "cache");
		assert_eq!(cfg.volumes[1].dir, Path::new("/srv/c"));
		assert!(cfg.volumes[1].read_only);
	}

	#[test]
	fn rejects_too_many_volumes() {
		let specs: Vec<String> = (0..9).map(|i| format!("v{i}:/srv/{i}")).collect();
		let mut args = vec!["vmon", "--kernel", "k"];
		for spec in &specs {
			args.push("--volume");
			args.push(spec.as_str());
		}
		assert_config_err_contains(&args, "at most 8 --volume mounts");
	}

	#[test]
	fn rejects_invalid_volume_tag() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--volume", "Bad-Tag:/srv/d"],
			"must match [a-z0-9_]",
		);
	}

	#[test]
	fn rejects_duplicate_volume_tags() {
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--volume", "data:/srv/a", "--volume", "data:/srv/b"],
			"--volume tags must be unique",
		);
		assert_config_err_contains(
			&[
				"vmon",
				"--kernel",
				"k",
				"--fs-tag",
				"shared",
				"--fs-dir",
				"/srv/s",
				"--volume",
				"shared:/srv/d",
			],
			"--volume tags must be unique",
		);
	}
	#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
	#[test]
	fn validates_nvidia_vfio_launch_contract() {
		const GPU: &str = "/sys/bus/pci/devices/0000:3d:00.4";
		const UUID: &str = "12345678-9abc-def0-1122-334455667788";
		let cfg = parse_config(&[
			"vmon",
			"--no-sandbox",
			"--kernel",
			"k",
			"--vfio-gpu",
			GPU,
			"--vm-uuid",
			UUID,
		])
		.expect("valid NVIDIA vGPU flags");
		assert_eq!(cfg.vfio_gpus, [PathBuf::from(GPU)]);
		assert_eq!(
			cfg.vm_uuid,
			Some([
				0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
				0x77, 0x88,
			])
		);

		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--vfio-gpu", GPU],
			"--vfio-gpu requires --vm-uuid",
		);
		assert_config_err_contains(
			&["vmon", "--boot-mode", "uefi", "--firmware", "fw", "--vfio-gpu", GPU, "--vm-uuid", UUID],
			"--vfio-gpu currently requires --boot-mode direct",
		);
		assert_config_err_contains(
			&["vmon", "--restore", "snap", "--vfio-gpu", GPU, "--vm-uuid", UUID],
			"--vfio-gpu cannot be combined with --restore or --fork-from",
		);
		assert_config_err_contains(
			&[
				"vmon",
				"--kernel",
				"k",
				"--mem",
				"256",
				"--mem-target-mib",
				"128",
				"--vfio-gpu",
				GPU,
				"--vm-uuid",
				UUID,
			],
			"--vfio-gpu cannot be combined with guest-memory paging",
		);
		assert_config_err_contains(
			&["vmon", "--kernel", "k", "--vm-uuid", "00000000-0000-0000-0000-000000000000"],
			"--vm-uuid must not be the nil UUID",
		);
	}
}
