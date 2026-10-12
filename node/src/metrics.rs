//! Per-sandbox metric history (ARCHITECTURE.md §8.7).
//!
//! envd `/metrics` samples are appended every 5 s to
//! `sandboxes/<id>/metrics.jsonl`, served by
//! `GET /node/v1/sandboxes/{id}/metrics?start&end`, and pruned to 7 days.

use std::{
	fs,
	io::{self, BufRead, BufReader, Write},
	path::Path,
	time::Duration,
};

use crate::{api::MetricSample, envd::EnvdMetrics};

/// Scrape cadence.
pub const SCRAPE_INTERVAL: Duration = Duration::from_secs(5);
/// Samples (and ended sandboxes' directories) older than this are dropped.
pub const RETENTION: Duration = Duration::from_hours(7 * 24);

/// Converts an envd scrape taken at `now_ms` into a stored sample.
pub const fn sample(now_ms: u64, metrics: &EnvdMetrics) -> MetricSample {
	MetricSample {
		timestamp_ms: now_ms,
		cpu_count:    metrics.cpu_count,
		cpu_used_pct: metrics.cpu_used_pct,
		mem_used:     metrics.mem_used,
		mem_total:    metrics.mem_total,
		mem_cache:    metrics.mem_cache,
		disk_used:    metrics.disk_used,
		disk_total:   metrics.disk_total,
	}
}

/// Appends one sample as a JSON line.
pub fn append(path: &Path, sample: &MetricSample) -> io::Result<()> {
	let mut line = serde_json::to_vec(sample)?;
	line.push(b'\n');
	fs::OpenOptions::new()
		.create(true)
		.append(true)
		.open(path)?
		.write_all(&line)
}

/// Samples with `start ≤ timestamp ≤ end` (either bound optional), oldest
/// first. Torn or malformed lines (crash mid-append) are skipped.
pub fn read_range(
	path: &Path,
	start_ms: Option<u64>,
	end_ms: Option<u64>,
) -> io::Result<Vec<MetricSample>> {
	let file = match fs::File::open(path) {
		Ok(file) => file,
		Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(e) => return Err(e),
	};
	let start = start_ms.unwrap_or(0);
	let end = end_ms.unwrap_or(u64::MAX);
	let mut out = Vec::new();
	for line in BufReader::new(file).lines() {
		let Ok(sample) = serde_json::from_str::<MetricSample>(&line?) else {
			continue;
		};
		if (start..=end).contains(&sample.timestamp_ms) {
			out.push(sample);
		}
	}
	Ok(out)
}

/// Rewrites the file without samples older than `now - RETENTION`.
/// Returns how many samples were dropped.
pub fn prune(path: &Path, now_ms: u64) -> io::Result<usize> {
	let cutoff = now_ms.saturating_sub(u64::try_from(RETENTION.as_millis()).unwrap_or(u64::MAX));
	let all = read_range(path, None, None)?;
	let keep: Vec<_> = all.iter().filter(|s| s.timestamp_ms >= cutoff).collect();
	let dropped = all.len() - keep.len();
	if dropped == 0 {
		return Ok(0);
	}
	let tmp = path.with_extension("jsonl.tmp");
	{
		let mut file = fs::File::create(&tmp)?;
		for sample in keep {
			let mut line = serde_json::to_vec(sample)?;
			line.push(b'\n');
			file.write_all(&line)?;
		}
	}
	fs::rename(&tmp, path)?;
	Ok(dropped)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn at(ms: u64) -> MetricSample {
		MetricSample { timestamp_ms: ms, cpu_count: 2, mem_total: 100, ..MetricSample::default() }
	}

	#[test]
	fn range_bounds_are_inclusive_and_torn_lines_skipped() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("metrics.jsonl");
		for ms in [1000, 2000, 3000] {
			append(&path, &at(ms)).unwrap();
		}
		fs::OpenOptions::new()
			.append(true)
			.open(&path)
			.unwrap()
			.write_all(b"{\"timestampMs\":40")
			.unwrap();
		let ts = |v: Vec<MetricSample>| v.into_iter().map(|s| s.timestamp_ms).collect::<Vec<_>>();
		assert_eq!(ts(read_range(&path, Some(2000), Some(3000)).unwrap()), [2000, 3000]);
		assert_eq!(ts(read_range(&path, None, Some(1999)).unwrap()), [1000]);
		assert_eq!(ts(read_range(&path, Some(3001), None).unwrap()), Vec::<u64>::new());
		assert_eq!(
			read_range(&dir.path().join("missing"), None, None).unwrap(),
			Vec::<MetricSample>::new()
		);
	}

	#[test]
	fn prune_drops_only_samples_past_retention() {
		let dir = tempfile::tempdir().unwrap();
		let path = dir.path().join("metrics.jsonl");
		let week = u64::try_from(RETENTION.as_millis()).unwrap();
		let now = week + 10_000;
		for ms in [5_000, 10_000, 10_001, now] {
			append(&path, &at(ms)).unwrap();
		}
		assert_eq!(prune(&path, now).unwrap(), 1, "only the sample before now-7d goes");
		let left: Vec<_> = read_range(&path, None, None)
			.unwrap()
			.into_iter()
			.map(|s| s.timestamp_ms)
			.collect();
		assert_eq!(left, [10_000, 10_001, now]);
		assert_eq!(prune(&path, now).unwrap(), 0);
	}

	#[test]
	fn sample_copies_envd_fields() {
		let metrics: EnvdMetrics = serde_json::from_str(
			r#"{"ts":1,"cpu_count":4,"cpu_used_pct":12.5,"mem_total":8,"mem_used":3,"mem_cache":1,"disk_used":5,"disk_total":9}"#,
		)
		.unwrap();
		let s = sample(42, &metrics);
		assert_eq!((s.timestamp_ms, s.cpu_count, s.mem_used, s.disk_total), (42, 4, 3, 9));
		assert!((s.cpu_used_pct - 12.5).abs() < f64::EPSILON);
	}
}
