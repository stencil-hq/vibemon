//! Pre-pause hooks: `/fsfreeze` / `/fsthaw` (FIFREEZE/FITHAW on the root
//! filesystem, idempotent) and `/collapse` (`MADV_COLLAPSE` of envd's own
//! anonymous heap into 2 MiB transparent hugepages).

use std::{io, os::fd::AsRawFd};

use serde::Serialize;

/// `_IOWR('X', 119, int)`.
const FIFREEZE: libc::c_ulong = 0xc004_5877;
/// `_IOWR('X', 120, int)`.
const FITHAW: libc::c_ulong = 0xc004_5878;
/// `MADV_COLLAPSE` (Linux 6.1).
const MADV_COLLAPSE: libc::c_int = 25;
const HUGE: usize = 2 << 20;

fn root_ioctl(request: libc::c_ulong, tolerated: libc::c_int) -> io::Result<()> {
	let root = std::fs::File::open("/")?;
	#[allow(
		clippy::useless_conversion,
		reason = "ioctl request type differs between glibc and musl"
	)]
	let request = request.try_into().unwrap_or_default();
	// SAFETY: FIFREEZE/FITHAW take an int argument that the kernel ignores.
	let rc = unsafe { libc::ioctl(root.as_raw_fd(), request, 0) };
	if rc == 0 {
		return Ok(());
	}
	let err = io::Error::last_os_error();
	if err.raw_os_error() == Some(tolerated) {
		Ok(())
	} else {
		Err(err)
	}
}

/// Freeze the root filesystem; already frozen (`EBUSY`) is success.
pub fn fs_freeze() -> io::Result<()> {
	// Flush first so the frozen image holds everything written so far.
	// SAFETY: sync(2) has no preconditions.
	unsafe { libc::sync() };
	root_ioctl(FIFREEZE, libc::EBUSY)
}

/// Thaw the root filesystem; not frozen (`EINVAL`) is success.
pub fn fs_thaw() -> io::Result<()> {
	root_ioctl(FITHAW, libc::EINVAL)
}

/// envd `CollapseResult`.
#[derive(Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CollapseResult {
	pub regions:      u32,
	pub chunks:       u32,
	pub collapsed:    u32,
	pub already_huge: u32,
	pub skipped:      u32,
	pub elapsed_ms:   u64,
}

/// One anonymous read-write mapping and its `AnonHugePages` (bytes).
#[derive(Debug, PartialEq, Eq)]
struct Region {
	start:     usize,
	end:       usize,
	anon_huge: usize,
}

/// Anonymous private rw mappings (`[heap]` or unnamed) from `/proc/self/smaps`.
fn anon_regions(smaps: &str) -> Vec<Region> {
	let mut out = Vec::new();
	let mut current: Option<Region> = None;
	for line in smaps.lines() {
		let mut cols = line.split_whitespace();
		let first = cols.next().unwrap_or("");
		if let Some((a, b)) = first.split_once('-')
			&& let (Ok(start), Ok(end)) = (usize::from_str_radix(a, 16), usize::from_str_radix(b, 16))
		{
			if let Some(r) = current.take() {
				out.push(r);
			}
			let perms = cols.next().unwrap_or("");
			let path = cols.nth(3);
			if perms.starts_with("rw") && perms.ends_with('p') && matches!(path, None | Some("[heap]"))
			{
				current = Some(Region { start, end, anon_huge: 0 });
			}
		} else if first == "AnonHugePages:"
			&& let Some(r) = current.as_mut()
		{
			r.anon_huge = cols
				.next()
				.and_then(|v| v.parse::<usize>().ok())
				.unwrap_or(0)
				* 1024;
		}
	}
	out.extend(current);
	out
}

/// 2 MiB-aligned chunks fully inside `[start, end)`.
fn aligned_chunks(start: usize, end: usize) -> impl Iterator<Item = usize> {
	let first = start.div_ceil(HUGE) * HUGE;
	(first..end.saturating_sub(HUGE - 1))
		.step_by(HUGE)
		.filter(move |c| c + HUGE <= end)
}

/// Collapse envd's anonymous heap (best effort).
pub fn collapse() -> io::Result<CollapseResult> {
	let started = std::time::Instant::now();
	let smaps = std::fs::read_to_string("/proc/self/smaps")?;
	let mut r = CollapseResult::default();
	for region in anon_regions(&smaps) {
		r.regions += 1;
		let chunks: Vec<usize> = aligned_chunks(region.start, region.end).collect();
		let n = u32::try_from(chunks.len()).unwrap_or(u32::MAX);
		r.chunks += n;
		if chunks.is_empty() {
			continue;
		}
		if region.anon_huge >= chunks.len() * HUGE {
			r.already_huge += n;
			continue;
		}
		for chunk in chunks {
			// SAFETY: the chunk lies inside one of our own live mappings;
			// MADV_COLLAPSE never changes contents.
			let rc = unsafe { libc::madvise(chunk as *mut libc::c_void, HUGE, MADV_COLLAPSE) };
			if rc == 0 {
				r.collapsed += 1;
			} else {
				r.skipped += 1;
			}
		}
	}
	r.elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
	Ok(r)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn smaps_selects_anonymous_rw_private_regions() {
		let smaps = "\
00400000-00600000 r-xp 00000000 08:01 1 /usr/bin/envd
Size: 2048 kB
01000000-01600000 rw-p 00000000 00:00 0 [heap]
AnonHugePages: 2048 kB
7f0000000000-7f0000400000 rw-p 00000000 00:00 0
AnonHugePages: 0 kB
7f1000000000-7f1000400000 rw-s 00000000 00:05 9 /dev/zero
7f2000000000-7f2000001000 rw-p 00000000 00:00 0 [stack]
";
		assert_eq!(anon_regions(smaps), vec![
			Region { start: 0x0100_0000, end: 0x0160_0000, anon_huge: 2 << 20 },
			Region { start: 0x7f00_0000_0000, end: 0x7f00_0040_0000, anon_huge: 0 },
		]);
	}

	#[test]
	fn only_fully_contained_aligned_chunks() {
		assert_eq!(aligned_chunks(0x0010_0000, 0x0060_0000).collect::<Vec<_>>(), [
			0x0020_0000,
			0x0040_0000
		]);
		assert_eq!(aligned_chunks(0x0020_0000, 0x0030_0000).count(), 0);
		assert_eq!(aligned_chunks(0x0020_0000, 0x0040_0000).collect::<Vec<_>>(), [0x0020_0000]);
	}
}
