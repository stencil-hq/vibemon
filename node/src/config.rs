//! Node role configuration, built by the `vmon serve` front end from flags,
//! environment, and defaults (ARCHITECTURE.md §3, §8.1).

use std::path::PathBuf;

pub use crate::net::NetMode;

/// Operator overrides of the capacity advertised in gossip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapacityOverrides {
	/// vCPUs offered to sandboxes (default: host logical CPUs).
	pub cpus:      Option<u32>,
	/// Memory offered to sandboxes in MiB (default: host RAM).
	pub memory_mb: Option<u64>,
}

/// Everything `Node::start` needs. All fields are required; `Option`s have
/// the documented fallback.
#[derive(Clone, Debug)]
pub struct NodeConfig {
	/// Durable node state (`builds/`, `sandboxes/`, `paused/`, `files/`, …).
	pub data_dir:                PathBuf,
	/// Short directory for per-VM Unix sockets (see [`default_run_dir`]).
	pub run_dir:                 PathBuf,
	/// Guest kernel; `None` = discovered by [`crate::assets::resolve_kernel`]
	/// (`vmon-kernel-<arch>` beside `vmon_bin`, then `target/kernel/` in a dev
	/// tree, then the pinned download under `<data_dir>/assets/`).
	pub kernel:                  Option<PathBuf>,
	/// Static guest agent; `None` = `vmon-agent-<arch>` beside `vmon_bin`
	/// or `target/test-assets/` in a dev tree.
	pub agent:                   Option<PathBuf>,
	/// Guest NVIDIA driver bundle for GPU sandboxes; `None` =
	/// `vmon-nvidia-<arch>/` beside `vmon_bin`, then `target/kernel/` in a dev
	/// tree ([`crate::assets::resolve_gpu_driver`]).
	pub gpu_driver:              Option<PathBuf>,
	/// This binary (`std::env::current_exe()`), re-executed as `vmon vmm`.
	pub vmon_bin:                PathBuf,
	/// Concurrent VM boots; 0 = 4 × logical CPUs.
	pub boot_concurrency:        usize,
	/// Concurrent template builds; 0 = 2.
	pub max_parallel_builds:     usize,
	/// Disk the build step cache may occupy, in MiB; 0 = 10% of the
	/// filesystem holding `data_dir`.
	pub step_cache_mb:           u64,
	pub net_mode:                NetMode,
	/// TAP lease pool (`--net-pool`, default `172.20.0.0/16`).
	pub net_pool:                String,
	/// Volume root; `None` = `<data_dir>/volumes`.
	pub volumes_dir:             Option<PathBuf>,
	/// Every node sees the same `volumes_dir` (shared storage).
	pub shared_volumes:          bool,
	pub capacity:                CapacityOverrides,
	/// Shared cluster secret (node API bearer).
	pub cluster_secret:          String,
	/// Linux root only: uid/gid VMMs drop to. `None` as root = `--no-sandbox`.
	pub vmm_user:                Option<(u32, u32)>,
	/// CIDRs exempt from the always-denied private ranges in egress policy.
	pub egress_internal_allow:   Vec<String>,
	/// Docker Hub pull-through mirror (`--registry-mirror`), tried first for
	/// anonymous `docker.io` base images.
	pub registry_mirror:         Option<String>,
	/// Operator object store (`--object-store`); `None` keeps paused state
	/// node-local. Advertised as the `object-store` feature.
	pub object_store:            Option<vmon_cloud::s3::S3Client>,
	/// Resume other nodes' paused sandboxes lazily (`--lazy-resume`,
	/// [`crate::lazy_resume`]); otherwise download them whole first.
	pub lazy_resume:             bool,
	/// Let bring-your-own-bucket S3 mounts use loopback/private endpoints
	/// (`--s3-mounts-allow-private`; development and private clouds).
	pub s3_mounts_allow_private: bool,
	/// Disk the CI cache's archive copies may occupy, in MiB; 0 = 10% of the
	/// filesystem holding `data_dir` ([`crate::ci_cache`]).
	pub ci_cache_mb:             u64,
	/// Disk the node-local Bazel remote cache may occupy, in MiB; 0 = 5% of
	/// the filesystem holding `data_dir`, at most a fifth of its free space
	/// (plus the cache's own size) at start.
	pub ci_bazel_cache_mb:       u64,
	/// GitHub results service CI sandboxes' other Twirp calls are forwarded
	/// to; `None` = [`crate::ci_cache::DEFAULT_UPSTREAM`].
	pub ci_results_upstream:     Option<String>,
}

/// Default `--data-dir` (also used by `vmon admin`): `/var/lib/vmon` for root
/// on Linux, `$HOME/.vmon` otherwise.
pub fn default_data_dir() -> PathBuf {
	// SAFETY: geteuid has no preconditions.
	let root = unsafe { libc::geteuid() } == 0;
	if cfg!(target_os = "linux") && root {
		return PathBuf::from("/var/lib/vmon");
	}
	std::env::var_os("HOME")
		.map_or_else(|| PathBuf::from("/var/lib/vmon"), |home| PathBuf::from(home).join(".vmon"))
}

/// Default `--run-dir`: `$XDG_RUNTIME_DIR/vmon`, else `/tmp/vmon-<uid>`
/// (short enough for the 104-byte macOS `sun_path`).
pub fn default_run_dir() -> PathBuf {
	if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR").filter(|dir| !dir.is_empty()) {
		return PathBuf::from(dir).join("vmon");
	}
	// SAFETY: getuid has no preconditions.
	let uid = unsafe { libc::getuid() };
	PathBuf::from(format!("/tmp/vmon-{uid}"))
}

impl NodeConfig {
	/// Effective volume root.
	pub fn volumes_root(&self) -> PathBuf {
		self
			.volumes_dir
			.clone()
			.unwrap_or_else(|| self.data_dir.join("volumes"))
	}
}
