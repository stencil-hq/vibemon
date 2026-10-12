//! Host facts for capacity reporting (ARCHITECTURE.md §5.1 `NodeCapacity`):
//! backend, OS, CPU baseline, logical CPUs, RAM, and free disk.

use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

use anyhow::{Context, bail};

/// Hypervisor backend of this host.
pub const fn backend() -> &'static str {
	if cfg!(target_os = "macos") {
		"hvf"
	} else {
		"kvm"
	}
}

/// Host OS name as used in capacity metadata.
pub const fn os() -> &'static str {
	if cfg!(target_os = "macos") {
		"macos"
	} else {
		"linux"
	}
}

/// Logical CPUs.
pub fn cpus() -> u32 {
	std::thread::available_parallelism().map_or(1, |n| u32::try_from(n.get()).unwrap_or(u32::MAX))
}

/// Physical memory in MiB.
pub fn memory_mb() -> u64 {
	#[cfg(target_os = "macos")]
	{
		let mut bytes: u64 = 0;
		let mut len = std::mem::size_of::<u64>();
		let name = c"hw.memsize";
		// SAFETY: `name` is NUL-terminated; `bytes`/`len` describe a writable
		// u64.
		let rc = unsafe {
			libc::sysctlbyname(
				name.as_ptr(),
				(&raw mut bytes).cast(),
				&raw mut len,
				std::ptr::null_mut(),
				0,
			)
		};
		if rc == 0 { bytes >> 20 } else { 0 }
	}
	#[cfg(not(target_os = "macos"))]
	{
		std::fs::read_to_string("/proc/meminfo")
			.ok()
			.and_then(|text| {
				text
					.lines()
					.find_map(|line| line.strip_prefix("MemTotal:"))
					.and_then(|rest| {
						rest
							.trim()
							.trim_end_matches("kB")
							.trim()
							.parse::<u64>()
							.ok()
					})
			})
			.map_or(0, |kib| kib >> 10)
	}
}

/// Free space (for unprivileged users) of the filesystem holding `path`, in
/// MiB.
pub fn disk_free_mb(path: &Path) -> u64 {
	let Ok(cpath) = CString::new(path.as_os_str().as_bytes()) else {
		return 0;
	};
	// SAFETY: statvfs is plain old data; zeroed is valid.
	let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
	// SAFETY: `cpath` is NUL-terminated and `stat` is writable.
	if unsafe { libc::statvfs(cpath.as_ptr(), &raw mut stat) } != 0 {
		return 0;
	}
	#[allow(clippy::useless_conversion, reason = "field widths differ between macOS and Linux")]
	let bytes = u64::from(stat.f_bavail) * u64::from(stat.f_frsize);
	bytes >> 20
}

/// Snapshot-compatibility key from `vmon vmm --print-cpu-baseline`.
pub async fn cpu_baseline(vmon_bin: &Path) -> anyhow::Result<String> {
	let output = tokio::process::Command::new(vmon_bin)
		.args(["vmm", "--print-cpu-baseline"])
		.output()
		.await
		.with_context(|| format!("running {} vmm --print-cpu-baseline", vmon_bin.display()))?;
	if !output.status.success() {
		bail!("vmm --print-cpu-baseline failed: {}", String::from_utf8_lossy(&output.stderr).trim());
	}
	let baseline = String::from_utf8_lossy(&output.stdout).trim().to_owned();
	if baseline.is_empty() {
		bail!("vmm --print-cpu-baseline printed nothing");
	}
	Ok(baseline)
}
