//! vmm — the per-VM monitor core: boots a Linux guest with a serial console
//! and virtio IO. The hypervisor backend is selected at compile time: KVM on
//! Linux (`x86_64`/aarch64), Apple Hypervisor.framework on macOS (aarch64), or
//! Windows Hypervisor Platform on Windows (`x86_64`).
//!
//! The `vmon` binary re-execs itself as `vmon vmm …` for each microVM and
//! hands the remaining flags to [`run_cli`].

mod arch;
pub mod block_image;
mod config;
mod control;
mod devices;
#[cfg(unix)]
pub mod disk_overlay;
mod disk_snapshot;
mod hv;
#[cfg(target_os = "linux")]
mod jail;
mod layout;
mod memory;
mod metrics;
mod os;
#[cfg(not(target_os = "windows"))]
mod pager;
pub mod result;
#[cfg(target_os = "linux")]
mod sandbox;
pub mod snapshot;

/// Remote virtio-fs proxy wire protocol shared with the server-side proxy.
pub mod remotefs {
	pub use crate::virtio::remotefs::proto;
}
mod tap;
mod virtio;
#[cfg(not(target_os = "windows"))]
#[path = "vmm.rs"]
mod vmm;
#[cfg(target_os = "windows")]
#[path = "vmm_windows.rs"]
mod vmm;
#[cfg(target_os = "windows")]
mod windows_pipe;

use config::{Config, LogFormat};
use tracing_subscriber::EnvFilter;
/// Create a no-follow copy-on-write disk overlay, falling back to a
/// sparse-aware copy when reflinking is unavailable.
pub use virtio::block::create_cow_overlay;
/// Materialize the disk a stopped `--rootfs-remote-*` VMM left behind.
#[cfg(unix)]
pub use virtio::materialize_remote_disk;

/// Run the per-VM monitor: parse `args` (flags only, no argv[0]), init
/// tracing, boot or restore, block until shutdown.
pub fn run_cli(args: Vec<String>) -> result::Result<()> {
	if args.first().map(String::as_str) == Some("--print-cpu-baseline") {
		// Probe only the restore-relevant CPU surface for node placement; skip
		// normal CLI parsing.
		println!("{}", arch::state::cpu_baseline()?);
		return Ok(());
	}
	let config = Config::from_args(args)?;
	init_logging(&config)?;
	tracing::info!(
		boot_mode = ?config.boot_mode,
		transport = config.transport.as_str(),
		cpus = config.cpus,
		memory_mib = config.mem_mib,
		restoring = config.restore.is_some(),
		forking = config.fork_from.is_some(),
		"starting virtual machine monitor"
	);
	vmm::run(config)
}

/// Probe the restore-relevant CPU baseline for node placement and restore
/// compatibility checks.
pub fn cpu_baseline() -> result::Result<String> {
	arch::state::cpu_baseline()
}

fn init_logging(config: &Config) -> result::Result<()> {
	let filter = EnvFilter::try_new(&config.log_level)
		.map_err(|e| result::err(format!("invalid --log-level {}: {e}", config.log_level)))?;

	match config.log_format {
		LogFormat::Text => {
			let _ = tracing_subscriber::fmt()
				.with_env_filter(filter)
				.with_writer(std::io::stderr)
				.try_init();
		},
		LogFormat::Json => {
			let _ = tracing_subscriber::fmt()
				.json()
				.with_env_filter(filter)
				.with_writer(std::io::stderr)
				.try_init();
		},
	}
	Ok(())
}
