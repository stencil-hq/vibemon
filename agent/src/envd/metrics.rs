//! `GET /metrics`: guest resource usage in envd's `Metrics` shape — CPU
//! count and usage since the previous scrape (`/proc/stat` deltas), memory
//! from `/proc/meminfo` (gopsutil semantics), and root filesystem usage.

use std::ffi::CString;

use serde::Serialize;

/// envd `Metrics` (fields we cannot observe are omitted, as envd does).
#[derive(Debug, Serialize, PartialEq)]
pub struct Metrics {
	pub ts:                   i64,
	pub cpu_count:            u32,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub cpu_possible:         Option<u32>,
	pub cpu_target:           u32,
	pub cpu_target_attempts:  u32,
	pub cpu_write_pending_ms: u64,
	pub cpu_used_pct:         f32,
	pub mem_total:            u64,
	pub mem_used:             u64,
	pub mem_cache:            u64,
	pub disk_used:            u64,
	pub disk_total:           u64,
}

/// Previous aggregate CPU sample (busy, total jiffies).
#[derive(Debug, Default)]
pub struct CpuSampler {
	prev: Option<(u64, u64)>,
}

impl CpuSampler {
	/// Usage percent since the previous call (since boot on the first).
	pub fn sample(&mut self, proc_stat: &str) -> f32 {
		let Some((busy, total)) = parse_cpu_line(proc_stat) else {
			return 0.0;
		};
		let (pb, pt) = self.prev.unwrap_or((0, 0));
		self.prev = Some((busy, total));
		let dt = total.saturating_sub(pt);
		if dt == 0 {
			return 0.0;
		}
		#[allow(
			clippy::cast_precision_loss,
			reason = "jiffy counts fit f64 precision for a percentage"
		)]
		let pct = busy.saturating_sub(pb) as f64 / dt as f64 * 100.0;
		#[allow(clippy::cast_possible_truncation, reason = "percent fits f32")]
		let pct = ((pct * 100.0).round() / 100.0) as f32;
		pct
	}
}

/// (busy, total) jiffies from the aggregate `cpu` line; idle = idle + iowait,
/// guest time is already part of user/nice.
fn parse_cpu_line(proc_stat: &str) -> Option<(u64, u64)> {
	let line = proc_stat.lines().find(|l| l.starts_with("cpu "))?;
	let v: Vec<u64> = line
		.split_whitespace()
		.skip(1)
		.filter_map(|x| x.parse().ok())
		.collect();
	if v.len() < 4 {
		return None;
	}
	let total: u64 = v.iter().take(8).sum();
	let idle = v[3] + v.get(4).copied().unwrap_or(0);
	Some((total - idle, total))
}

/// (total, used, cached) bytes; used = total − free − buffers − cached
/// (gopsutil), cached includes reclaimable slab.
pub fn parse_meminfo(meminfo: &str) -> Option<(u64, u64, u64)> {
	let get = |key: &str| -> Option<u64> {
		meminfo
			.lines()
			.find_map(|l| l.strip_prefix(key)?.strip_prefix(':'))
			.and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
			.map(|kb| kb * 1024)
	};
	let total = get("MemTotal")?;
	let free = get("MemFree").unwrap_or(0);
	let buffers = get("Buffers").unwrap_or(0);
	let cached = get("Cached").unwrap_or(0) + get("SReclaimable").unwrap_or(0);
	let used = total.saturating_sub(free + buffers + cached);
	Some((total, used, cached))
}

fn possible_cpus(spec: &str) -> Option<u32> {
	let mut n = 0;
	for part in spec.trim().split(',').filter(|p| !p.is_empty()) {
		let (a, b) = part.split_once('-').unwrap_or((part, part));
		n += b.trim().parse::<u32>().ok()? - a.trim().parse::<u32>().ok()? + 1;
	}
	Some(n)
}

/// (used, total) bytes of the filesystem holding `/`.
fn disk_usage() -> (u64, u64) {
	let root = CString::from(c"/");
	// SAFETY: zeroed statvfs is a valid out-parameter.
	let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
	// SAFETY: valid path and out-pointer.
	if unsafe { libc::statvfs(root.as_ptr(), &mut st) } != 0 {
		return (0, 0);
	}
	#[allow(clippy::useless_conversion, reason = "statvfs field widths differ per libc")]
	let (blocks, bfree, frsize) =
		(u64::from(st.f_blocks), u64::from(st.f_bfree), u64::from(st.f_frsize));
	((blocks - bfree) * frsize, blocks * frsize)
}

/// Collect a snapshot.
pub fn collect(cpu: &mut CpuSampler, cpu_target: (u32, u32)) -> Result<Metrics, String> {
	let stat = std::fs::read_to_string("/proc/stat").map_err(|e| format!("read /proc/stat: {e}"))?;
	let meminfo =
		std::fs::read_to_string("/proc/meminfo").map_err(|e| format!("read /proc/meminfo: {e}"))?;
	let (mem_total, mem_used, mem_cache) =
		parse_meminfo(&meminfo).ok_or("unparsable /proc/meminfo")?;
	// SAFETY: sysconf is always safe to call.
	let online = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
	let (disk_used, disk_total) = disk_usage();
	Ok(Metrics {
		ts: super::time::now_unix(),
		cpu_count: u32::try_from(online).unwrap_or(1),
		cpu_possible: std::fs::read_to_string("/sys/devices/system/cpu/possible")
			.ok()
			.and_then(|s| possible_cpus(&s)),
		cpu_target: cpu_target.0,
		cpu_target_attempts: cpu_target.1,
		cpu_write_pending_ms: 0,
		cpu_used_pct: cpu.sample(&stat),
		mem_total,
		mem_used,
		mem_cache,
		disk_used,
		disk_total,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn cpu_usage_is_delta_since_previous_sample() {
		let mut s = CpuSampler::default();
		// busy = 100+0+100 = 200 (+0 irq...), idle = 700+100 = 800, total 1000.
		let a = "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 2 3 4\n";
		assert!((s.sample(a) - 20.0).abs() < f32::EPSILON);
		// +50 busy, +50 idle → 50%.
		let b = "cpu  150 0 100 750 100 0 0 0 0 0\n";
		assert!((s.sample(b) - 50.0).abs() < f32::EPSILON);
		// No progress → 0, never NaN.
		assert!(s.sample(b).abs() < f32::EPSILON);
	}

	#[test]
	fn meminfo_used_excludes_cache_and_buffers() {
		let m = "MemTotal: 1000 kB\nMemFree: 200 kB\nBuffers: 100 kB\nCached: 300 kB\nSReclaimable: \
		         50 kB\n";
		assert_eq!(parse_meminfo(m), Some((1_024_000, 358_400, 358_400)));
		assert_eq!(parse_meminfo("MemFree: 1 kB\n"), None);
	}

	#[test]
	fn possible_cpu_ranges() {
		assert_eq!(possible_cpus("0-7\n"), Some(8));
		assert_eq!(possible_cpus("0,2-3"), Some(3));
		assert_eq!(possible_cpus("x"), None);
	}
}
