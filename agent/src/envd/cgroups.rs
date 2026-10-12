//! Workload cgroups (cgroup v2) for envd-spawned processes — `user`
//! (cpu.weight 50), `ptys` (cpu.weight 200), `system` (tag `_system`) — and
//! the `/freeze` / `/unfreeze` sweeps over them (legacy) or over the whole
//! hierarchy outside envd's own ancestor chain (hierarchy). Degrades to a
//! no-op manager when cgroup v2 is missing or read-only.

use std::{
	fs, io,
	os::fd::{AsRawFd, OwnedFd, RawFd},
	path::{Path, PathBuf},
	time::{Duration, Instant},
};

use parking_lot::Mutex;
use serde::Serialize;

/// Where cgroup v2 is expected.
pub const CGROUP_MOUNT: &str = "/sys/fs/cgroup";
/// Default bound on cgroups a hierarchy sweep may visit.
const DEFAULT_MAX_CGROUPS: usize = 4096;
/// Poll cadence while waiting for `frozen 1`.
const FREEZE_POLL: Duration = Duration::from_millis(2);

/// Which workload cgroup a process joins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	User,
	Pty,
	System,
}

impl Kind {
	const fn dir(self) -> &'static str {
		match self {
			Self::User => "user",
			Self::Pty => "ptys",
			Self::System => "system",
		}
	}

	const fn weight(self) -> Option<&'static str> {
		match self {
			Self::User => Some("50"),
			Self::Pty => Some("200"),
			Self::System => None,
		}
	}
}

/// `/freeze?mode=`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
	Legacy,
	Hierarchy,
}

/// envd `FreezeResult`.
#[derive(Debug, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FreezeResult {
	pub mode:         Option<Mode>,
	pub visited:      u32,
	pub allowlisted:  u32,
	pub truncated:    bool,
	pub pre_frozen:   u32,
	pub requested:    u32,
	pub frozen:       u32,
	pub not_frozen:   u32,
	pub failed:       u32,
	pub vanished:     u32,
	pub unobservable: u32,
	pub sweep_ms:     u64,
	pub wait_ms:      u64,
}

struct Workload {
	base:      PathBuf,
	/// Open `cgroup.procs` of user, ptys, system (indexed by `Kind`).
	procs:     [Option<OwnedFd>; 3],
	/// envd's own cgroup and its ancestors, relative to the mount.
	ancestors: Vec<PathBuf>,
}

/// Workload cgroup manager.
pub struct Cgroups {
	mount:        PathBuf,
	workload:     Option<Workload>,
	/// Cgroups the last freeze wrote (and a later thaw must undo).
	frozen_by_us: Mutex<Vec<PathBuf>>,
}

impl Cgroups {
	/// Manager that never places or freezes anything.
	pub fn disabled() -> Self {
		Self {
			mount:        PathBuf::from(CGROUP_MOUNT),
			workload:     None,
			frozen_by_us: Mutex::new(Vec::new()),
		}
	}

	/// Set up workload cgroups under envd's own cgroup at `mount`. When envd
	/// is not in the root cgroup it first moves itself into `<own>/envd` so
	/// `<own>` may enable controllers for its children. Any failure → no-op.
	pub fn init(mount: &Path) -> Self {
		match setup(mount) {
			Ok(workload) => Self {
				mount:        mount.to_owned(),
				workload:     Some(workload),
				frozen_by_us: Mutex::new(Vec::new()),
			},
			Err(e) => {
				crate::logging::emit(
					crate::logging::Level::Warn,
					"envd: cgroups unavailable, using no-op manager",
					serde_json::json!({ "error": e.to_string() }),
				);
				Self::disabled()
			},
		}
	}

	/// `cgroup.procs` fd a child writes `0` to (from `pre_exec`) to join `kind`.
	pub fn procs_fd(&self, kind: Kind) -> Option<RawFd> {
		let idx = match kind {
			Kind::User => 0,
			Kind::Pty => 1,
			Kind::System => 2,
		};
		self.workload.as_ref()?.procs[idx]
			.as_ref()
			.map(AsRawFd::as_raw_fd)
	}

	/// Freeze targets, write `cgroup.freeze`, and (if `max_wait`) poll until
	/// they read back frozen. Best effort: problems are counted, not raised.
	pub async fn freeze(
		&self,
		mode: Mode,
		max_cgroups: Option<usize>,
		max_wait: Option<Duration>,
	) -> FreezeResult {
		let mut result = FreezeResult { mode: Some(mode), ..FreezeResult::default() };
		let Some(workload) = &self.workload else {
			return result;
		};
		let sweep_start = Instant::now();
		let (targets, discovered) = match mode {
			Mode::Legacy => (
				[Kind::User, Kind::Pty]
					.iter()
					.map(|k| workload.base.join(k.dir()))
					.collect(),
				false,
			),
			Mode::Hierarchy => {
				let limit = max_cgroups
					.filter(|&n| n > 0)
					.unwrap_or(DEFAULT_MAX_CGROUPS);
				(hierarchy_targets(&self.mount, workload, limit, &mut result), true)
			},
		};
		let mut written = Vec::new();
		for cg in targets {
			match read_trimmed(&cg.join("cgroup.freeze")) {
				Ok(v) if v == "1" => {
					result.pre_frozen += 1;
					continue;
				},
				Err(e) if e.kind() == io::ErrorKind::NotFound && discovered => {
					result.vanished += 1;
					continue;
				},
				_ => {},
			}
			match fs::write(cg.join("cgroup.freeze"), "1") {
				Ok(()) => {
					result.requested += 1;
					written.push(cg);
				},
				Err(e) if e.kind() == io::ErrorKind::NotFound && discovered => result.vanished += 1,
				Err(_) => result.failed += 1,
			}
		}
		result.sweep_ms = millis(sweep_start.elapsed());
		self.frozen_by_us.lock().extend(written.iter().cloned());

		if let Some(budget) = max_wait {
			let wait_start = Instant::now();
			let mut pending = written;
			loop {
				pending.retain(|cg| match read_events_frozen(cg) {
					Ok(true) => {
						result.frozen += 1;
						false
					},
					Ok(false) => true,
					Err(e) if e.kind() == io::ErrorKind::NotFound => {
						result.vanished += 1;
						false
					},
					Err(_) => {
						result.failed += 1;
						false
					},
				});
				if pending.is_empty() || wait_start.elapsed() >= budget {
					break;
				}
				tokio::time::sleep(FREEZE_POLL).await;
			}
			result.not_frozen = u32::try_from(pending.len()).unwrap_or(u32::MAX);
			result.wait_ms = millis(wait_start.elapsed());
		}
		result
	}

	/// Thaw everything the previous freezes wrote; cgroups the guest froze
	/// itself stay frozen. Returns the first write error (cgroups that
	/// vanished meanwhile are fine).
	pub fn thaw(&self) -> Result<(), String> {
		let frozen = std::mem::take(&mut *self.frozen_by_us.lock());
		let mut first_err = None;
		for cg in frozen {
			if let Err(e) = fs::write(cg.join("cgroup.freeze"), "0")
				&& e.kind() != io::ErrorKind::NotFound
			{
				first_err.get_or_insert_with(|| format!("thaw {}: {e}", cg.display()));
			}
		}
		first_err.map_or(Ok(()), Err)
	}
}

fn millis(d: Duration) -> u64 {
	u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

fn read_trimmed(path: &Path) -> io::Result<String> {
	Ok(fs::read_to_string(path)?.trim().to_owned())
}

fn read_events_frozen(cg: &Path) -> io::Result<bool> {
	Ok(fs::read_to_string(cg.join("cgroup.events"))?
		.lines()
		.any(|l| l.trim() == "frozen 1"))
}

/// envd's cgroup path relative to the v2 mount, from `/proc/self/cgroup`.
pub fn own_cgroup(proc_self_cgroup: &str) -> Option<PathBuf> {
	proc_self_cgroup
		.lines()
		.find_map(|l| l.strip_prefix("0::"))
		.map(|p| PathBuf::from(p.trim().trim_start_matches('/')))
}

fn setup(mount: &Path) -> io::Result<Workload> {
	if !mount.join("cgroup.controllers").exists() {
		return Err(io::Error::new(io::ErrorKind::NotFound, "cgroup v2 not mounted"));
	}
	favor_dynmods(mount);
	let own = own_cgroup(&fs::read_to_string("/proc/self/cgroup")?)
		.ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no cgroup v2 entry for envd"))?;
	let base = mount.join(&own);
	let own_leaf = if own.as_os_str().is_empty() {
		own
	} else {
		// Leave the parent free of processes so it can delegate controllers.
		let leaf = base.join("envd");
		fs::create_dir_all(&leaf)?;
		fs::write(leaf.join("cgroup.procs"), std::process::id().to_string())?;
		own.join("envd")
	};
	// Controllers are optional: freezing works without them.
	let _ = fs::write(base.join("cgroup.subtree_control"), "+cpu +memory");
	let mem_limit = memory_limit();
	let mut procs: [Option<OwnedFd>; 3] = [None, None, None];
	for (i, kind) in [Kind::User, Kind::Pty, Kind::System]
		.into_iter()
		.enumerate()
	{
		let dir = base.join(kind.dir());
		fs::create_dir_all(&dir)?;
		if let Some(w) = kind.weight() {
			let _ = fs::write(dir.join("cpu.weight"), w);
		}
		if kind != Kind::System
			&& let Some(limit) = mem_limit
		{
			let _ = fs::write(dir.join("memory.high"), limit.to_string());
			let _ = fs::write(dir.join("memory.max"), limit.to_string());
		}
		let file = fs::OpenOptions::new()
			.write(true)
			.open(dir.join("cgroup.procs"))?;
		procs[i] = Some(file.into());
	}
	let mut ancestors = Vec::new();
	let mut cur = Some(own_leaf.as_path());
	while let Some(p) = cur {
		ancestors.push(p.to_owned());
		cur = p.parent();
	}
	Ok(Workload { base, procs, ancestors })
}

/// Remounts cgroup v2 at `mount` with `favordynmods` (Linux 6.0+) unless it is
/// already set. Every spawn moves the child into a workload cgroup; without the
/// option a migration after the guest went idle waits for an RCU grace period
/// (the `cgroup_threadgroup_rwsem` writer), ~20 ms on the first command after
/// a restore. The option makes forks and exits marginally more expensive.
/// Best-effort: older kernels keep the slow path.
fn favor_dynmods(mount: &Path) {
	let Some(entry) = fs::read_to_string("/proc/self/mountinfo")
		.ok()
		.and_then(|info| cgroup2_mount(&info, mount))
	else {
		return;
	};
	if entry.super_opts.split(',').any(|opt| opt == "favordynmods") {
		return;
	}
	let data = if entry.super_opts.is_empty() {
		"favordynmods".to_owned()
	} else {
		format!("{},favordynmods", entry.super_opts)
	};
	let (Ok(source), Ok(target), Ok(data)) = (
		std::ffi::CString::new(entry.source),
		std::ffi::CString::new(mount.as_os_str().as_encoded_bytes()),
		std::ffi::CString::new(data),
	) else {
		return;
	};
	// SAFETY: all pointers are NUL-terminated strings that outlive the call.
	let rc = unsafe {
		libc::mount(
			source.as_ptr(),
			target.as_ptr(),
			c"cgroup2".as_ptr(),
			libc::MS_REMOUNT | entry.flags,
			data.as_ptr().cast(),
		)
	};
	if rc != 0 {
		crate::logging::emit(
			crate::logging::Level::Info,
			"envd: cgroup v2 favordynmods unavailable",
			serde_json::json!({ "error": io::Error::last_os_error().to_string() }),
		);
	}
}

/// A mount's source, per-mount `MS_*` flags, and superblock options.
#[derive(Debug, PartialEq, Eq)]
struct MountEntry {
	source:     String,
	flags:      libc::c_ulong,
	super_opts: String,
}

/// The top-most cgroup v2 mount at `mount` in `/proc/self/mountinfo` text.
fn cgroup2_mount(mountinfo: &str, mount: &Path) -> Option<MountEntry> {
	mountinfo.lines().rev().find_map(|line| {
		let (left, right) = line.split_once(" - ")?;
		let mut left = left.split(' ');
		let point = left.nth(4)?;
		let opts = left.next()?;
		let mut right = right.split(' ');
		if Path::new(point) != mount || right.next()? != "cgroup2" {
			return None;
		}
		let source = right.next()?.to_owned();
		let super_opts = right.next().unwrap_or_default().to_owned();
		let flags = opts.split(',').fold(0, |flags, opt| {
			flags
				| match opt {
					"ro" => libc::MS_RDONLY,
					"nosuid" => libc::MS_NOSUID,
					"nodev" => libc::MS_NODEV,
					"noexec" => libc::MS_NOEXEC,
					"noatime" => libc::MS_NOATIME,
					"nodiratime" => libc::MS_NODIRATIME,
					"relatime" => libc::MS_RELATIME,
					"strictatime" => libc::MS_STRICTATIME,
					_ => 0,
				}
		});
		Some(MountEntry { source, flags, super_opts })
	})
}

/// Workload memory cap: total − min(total/8, 128 MiB).
fn memory_limit() -> Option<u64> {
	let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
	let total_kb: u64 = meminfo
		.lines()
		.find_map(|l| l.strip_prefix("MemTotal:"))?
		.split_whitespace()
		.next()?
		.parse()
		.ok()?;
	let total = total_kb * 1024;
	Some(total - (total / 8).min(128 << 20))
}

/// Cgroups the resume path depends on: never frozen in hierarchy mode.
fn allowlisted(name: &str) -> bool {
	name == Kind::System.dir() || name == "init.scope" || name.starts_with("systemd-")
}

/// The maximal subtrees outside envd's ancestor chain (freezing a cgroup
/// freezes its descendants), bounded by `limit` visits.
fn hierarchy_targets(
	mount: &Path,
	workload: &Workload,
	limit: usize,
	result: &mut FreezeResult,
) -> Vec<PathBuf> {
	let mut targets = Vec::new();
	for ancestor in &workload.ancestors {
		let dir = mount.join(ancestor);
		let Ok(entries) = fs::read_dir(&dir) else {
			continue;
		};
		let mut names: Vec<_> = entries
			.filter_map(Result::ok)
			.filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
			.map(|e| e.file_name().to_string_lossy().into_owned())
			.collect();
		names.sort();
		for name in names {
			let rel = ancestor.join(&name);
			if workload.ancestors.contains(&rel) {
				continue;
			}
			if result.visited as usize >= limit {
				result.truncated = true;
				return targets;
			}
			result.visited += 1;
			if allowlisted(&name) {
				result.allowlisted += 1;
				continue;
			}
			targets.push(mount.join(rel));
		}
	}
	targets
}

#[cfg(test)]
mod tests {
	use super::*;
	#[test]
	fn cgroup2_mount_keeps_flags_and_superblock_options_of_topmost_mount() {
		let info = "\
22 1 0:21 / /sys rw,nosuid,nodev,noexec,relatime shared:7 - sysfs sysfs rw
30 22 0:26 / /sys/fs/cgroup ro,relatime shared:9 - cgroup2 old rw
31 22 0:27 / /sys/fs/cgroup rw,nosuid,nodev,noexec,relatime shared:10 - cgroup2 cgroup2 \
		            rw,nsdelegate,memory_recursiveprot
";
		assert_eq!(
			cgroup2_mount(info, Path::new("/sys/fs/cgroup")),
			Some(MountEntry {
				source:     "cgroup2".into(),
				flags:      libc::MS_NOSUID | libc::MS_NODEV | libc::MS_NOEXEC | libc::MS_RELATIME,
				super_opts: "rw,nsdelegate,memory_recursiveprot".into(),
			})
		);
		assert_eq!(cgroup2_mount(info, Path::new("/sys")), None, "not cgroup2");
	}

	#[test]
	fn own_cgroup_parsing() {
		assert_eq!(own_cgroup("0::/\n"), Some(PathBuf::new()));
		assert_eq!(
			own_cgroup("1:name=systemd:/x\n0::/system.slice/envd.service\n"),
			Some(PathBuf::from("system.slice/envd.service"))
		);
		assert_eq!(own_cgroup("1:cpu:/x\n"), None);
	}

	fn fake_tree() -> (PathBuf, Workload) {
		let root = std::env::temp_dir().join(format!(
			"envd-cg-{}-{:?}",
			std::process::id(),
			std::thread::current().id()
		));
		let _ = fs::remove_dir_all(&root);
		for d in [
			"system.slice/envd.service/envd",
			"system.slice/envd.service/user",
			"system.slice/envd.service/ptys",
			"system.slice/envd.service/system",
			"system.slice/systemd-journald.service",
			"system.slice/docker.service",
			"user.slice/user-1000.slice",
			"init.scope",
		] {
			fs::create_dir_all(root.join(d)).unwrap();
		}
		let ancestors = vec![
			PathBuf::from("system.slice/envd.service/envd"),
			PathBuf::from("system.slice/envd.service"),
			PathBuf::from("system.slice"),
			PathBuf::new(),
		];
		let workload = Workload {
			base: root.join("system.slice/envd.service"),
			procs: [None, None, None],
			ancestors,
		};
		(root, workload)
	}

	#[test]
	fn hierarchy_freezes_complement_of_ancestor_chain() {
		let (root, workload) = fake_tree();
		let mut result = FreezeResult::default();
		let targets = hierarchy_targets(&root, &workload, 100, &mut result);
		let rel: Vec<_> = targets
			.iter()
			.map(|t| t.strip_prefix(&root).unwrap().to_owned())
			.collect();
		assert_eq!(rel, vec![
			PathBuf::from("system.slice/envd.service/ptys"),
			PathBuf::from("system.slice/envd.service/user"),
			PathBuf::from("system.slice/docker.service"),
			PathBuf::from("user.slice"),
		]);
		// system (ours), systemd-journald, init.scope are allowlisted.
		assert_eq!(result.allowlisted, 3);
		assert_eq!(result.visited, 7);
		assert!(!result.truncated);
		let mut bounded = FreezeResult::default();
		let targets = hierarchy_targets(&root, &workload, 2, &mut bounded);
		assert!(bounded.truncated);
		assert_eq!(bounded.visited, 2);
		// ptys is frozen, system is allowlisted, then the bound hits.
		assert_eq!(targets, vec![root.join("system.slice/envd.service/ptys")]);
		fs::remove_dir_all(&root).unwrap();
	}

	#[tokio::test]
	async fn freeze_counts_and_thaw_skips_prefrozen() {
		let (root, workload) = fake_tree();
		let base = workload.base.clone();
		// Simulate cgroupfs files: user is already frozen by the guest.
		fs::write(base.join("user/cgroup.freeze"), "1").unwrap();
		fs::write(base.join("ptys/cgroup.freeze"), "0").unwrap();
		fs::write(base.join("ptys/cgroup.events"), "populated 1\nfrozen 0\n").unwrap();
		let cg = Cgroups {
			mount:        root.clone(),
			workload:     Some(workload),
			frozen_by_us: Mutex::new(Vec::new()),
		};
		let r = cg
			.freeze(Mode::Legacy, None, Some(Duration::from_millis(20)))
			.await;
		assert_eq!((r.pre_frozen, r.requested, r.frozen, r.not_frozen), (1, 1, 0, 1));
		assert_eq!(fs::read_to_string(base.join("ptys/cgroup.freeze")).unwrap(), "1");
		fs::write(base.join("ptys/cgroup.events"), "populated 1\nfrozen 1\n").unwrap();
		cg.thaw().unwrap();
		assert_eq!(fs::read_to_string(base.join("ptys/cgroup.freeze")).unwrap(), "0");
		// The guest's own freeze survives the thaw.
		assert_eq!(fs::read_to_string(base.join("user/cgroup.freeze")).unwrap(), "1");
		// No wait → no polling stats.
		let r = cg.freeze(Mode::Legacy, None, None).await;
		assert_eq!((r.frozen, r.not_frozen, r.wait_ms), (0, 0, 0));
		fs::remove_dir_all(&root).unwrap();
	}
}
