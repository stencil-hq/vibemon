//! Host facts for capacity reporting (ARCHITECTURE.md §5.1 `NodeCapacity`):
//! backend, OS, CPU model and baseline, logical CPUs, RAM, and disk.

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

/// Host CPU model name for display (empty when unknown):
/// `machdep.cpu.brand_string` on macOS; on Linux the first `model name` of
/// `/proc/cpuinfo`, else the device-tree model (aarch64 kernels omit `model
/// name`).
pub fn cpu_model() -> String {
	#[cfg(target_os = "macos")]
	{
		let mut buf = [0u8; 256];
		let mut len = buf.len();
		let name = c"machdep.cpu.brand_string";
		// SAFETY: `name` is NUL-terminated; `buf`/`len` describe a writable
		// buffer of `len` bytes.
		let rc = unsafe {
			libc::sysctlbyname(
				name.as_ptr(),
				buf.as_mut_ptr().cast(),
				&raw mut len,
				std::ptr::null_mut(),
				0,
			)
		};
		if rc != 0 {
			return String::new();
		}
		let value = &buf[..len.min(buf.len())];
		let end = value.iter().position(|&b| b == 0).unwrap_or(value.len());
		String::from_utf8_lossy(&value[..end]).trim().to_owned()
	}
	#[cfg(not(target_os = "macos"))]
	{
		std::fs::read_to_string("/proc/cpuinfo")
			.ok()
			.and_then(|text| cpuinfo_model(&text).map(str::to_owned))
			.or_else(|| {
				let raw = std::fs::read("/proc/device-tree/model").ok()?;
				let model = String::from_utf8_lossy(&raw)
					.trim_end_matches('\0')
					.trim()
					.to_owned();
				(!model.is_empty()).then_some(model)
			})
			.unwrap_or_default()
	}
}

/// First non-empty `model name` value of `/proc/cpuinfo` text.
#[cfg_attr(target_os = "macos", allow(dead_code, reason = "Linux only; unit-tested everywhere"))]
fn cpuinfo_model(text: &str) -> Option<&str> {
	text.lines().find_map(|line| {
		let (key, value) = line.split_once(':')?;
		let value = value.trim();
		(key.trim() == "model name" && !value.is_empty()).then_some(value)
	})
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
	#[allow(clippy::useless_conversion, reason = "field widths differ between macOS and Linux")]
	let bytes = statvfs(path).map_or(0, |stat| u64::from(stat.f_bavail) * u64::from(stat.f_frsize));
	bytes >> 20
}

/// Size of the filesystem holding `path`, in MiB (0 when unknown).
pub fn disk_size_mb(path: &Path) -> u64 {
	#[allow(clippy::useless_conversion, reason = "field widths differ between macOS and Linux")]
	let bytes = statvfs(path).map_or(0, |stat| u64::from(stat.f_blocks) * u64::from(stat.f_frsize));
	bytes >> 20
}

fn statvfs(path: &Path) -> Option<libc::statvfs> {
	let cpath = CString::new(path.as_os_str().as_bytes()).ok()?;
	// SAFETY: statvfs is plain old data; zeroed is valid.
	let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
	// SAFETY: `cpath` is NUL-terminated and `stat` is writable.
	(unsafe { libc::statvfs(cpath.as_ptr(), &raw mut stat) } == 0).then_some(stat)
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

#[cfg(test)]
mod tests {
	#[test]
	fn cpuinfo_model_takes_the_first_model_name() {
		let text = "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Xeon(R) Gold \
		            6438N\n\nprocessor\t: 1\nmodel name\t: other\n";
		assert_eq!(super::cpuinfo_model(text), Some("Intel(R) Xeon(R) Gold 6438N"));
		assert_eq!(super::cpuinfo_model("processor\t: 0\nCPU part\t: 0xd0c\n"), None);
	}
}
