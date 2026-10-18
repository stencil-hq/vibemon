//! On-disk sandbox record `sandboxes/<id>/meta.json` (ARCHITECTURE.md §8.1):
//! what restart recovery needs to adopt a live VMM or keep a paused sandbox.
//! Env vars and other driver-store data are never written here; the envd
//! access token is kept (0600) because metric scrapes after a node restart
//! need it.

use std::{
	fs,
	io::Write,
	os::unix::fs::OpenOptionsExt,
	path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
	api::{NetworkPolicy, SandboxInfo, SandboxState, VolumeMount},
	net::TapLease,
};

/// Persisted state of one sandbox on this node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxMeta {
	pub sandbox_id:        String,
	pub team_id:           String,
	pub template_id:       String,
	pub build_id:          String,
	pub state:             SandboxState,
	pub vcpus:             u32,
	pub memory_mb:         u64,
	pub disk_mb:           u64,
	pub started_at_ms:     u64,
	pub end_at_ms:         u64,
	pub envd_version:      String,
	pub envd_access_token: String,
	pub network:           NetworkPolicy,
	pub volume_mounts:     Vec<VolumeMount>,
	/// Live VMM process (running only).
	pub pid:               Option<u32>,
	/// TAP lease of the live VMM (Linux, running only).
	pub tap:               Option<TapLease>,
	/// Set once the sandbox is gone; the dir is kept for metrics/logs retention.
	pub ended_at_ms:       Option<u64>,
}

impl SandboxMeta {
	/// Node API view.
	pub fn info(&self) -> SandboxInfo {
		SandboxInfo {
			sandbox_id:    self.sandbox_id.clone(),
			team_id:       self.team_id.clone(),
			template_id:   self.template_id.clone(),
			build_id:      self.build_id.clone(),
			state:         self.state,
			vcpus:         self.vcpus,
			memory_mb:     self.memory_mb,
			disk_mb:       self.disk_mb,
			started_at_ms: self.started_at_ms,
			end_at_ms:     self.end_at_ms,
			envd_version:  self.envd_version.clone(),
		}
	}
}

/// Directory name of a sandbox's RAM base snapshot, both in the live snapshot
/// root and in `paused/<id>/`.
pub const RAM_BASE: &str = "base";

/// Paths of one sandbox's node state.
#[derive(Clone, Debug)]
pub struct SandboxPaths {
	/// `sandboxes/<id>/`
	pub dir:    PathBuf,
	/// `paused/<id>/`
	pub paused: PathBuf,
}

impl SandboxPaths {
	/// Paths under `data_dir` for `id`.
	pub fn new(data_dir: &Path, id: &str) -> Self {
		Self { dir: data_dir.join("sandboxes").join(id), paused: data_dir.join("paused").join(id) }
	}

	pub fn meta(&self) -> PathBuf {
		self.dir.join("meta.json")
	}

	/// Live copy-on-write rootfs overlay.
	pub fn rootfs(&self) -> PathBuf {
		self.dir.join("rootfs.img")
	}

	pub fn console(&self) -> PathBuf {
		self.dir.join("console.log")
	}

	pub fn metrics(&self) -> PathBuf {
		self.dir.join("metrics.jsonl")
	}

	/// VMM `--snapshot-root` of the live VM.
	pub fn snapshot_root(&self) -> PathBuf {
		self.dir.join("snap")
	}

	/// Hard-linked copy of the full snapshot whose memory file backs the live
	/// VM's RAM (`MAP_PRIVATE`); a sibling of the pause snapshot so the VMM
	/// can write the pause as a delta against it.
	pub fn ram_base(&self) -> PathBuf {
		self.snapshot_root().join(RAM_BASE)
	}

	/// Compiled egress filter for user-net VMMs.
	pub fn net_policy(&self) -> PathBuf {
		self.dir.join("net-policy.json")
	}

	pub fn paused_snapshot(&self) -> PathBuf {
		self.paused.join("snapshot")
	}

	/// The [`Self::ram_base`] a delta `paused_snapshot` refers to (its
	/// `base` resolves against the delta's parent directory).
	pub fn paused_base(&self) -> PathBuf {
		self.paused.join(RAM_BASE)
	}

	pub fn paused_rootfs(&self) -> PathBuf {
		self.paused.join("rootfs.img")
	}

	/// Written and fsynced last, after every other file of the paused state
	/// is durable: without it a crash may have left memory and disk out of
	/// step, so the state must not be resumed.
	pub fn paused_committed(&self) -> PathBuf {
		self.paused.join("committed")
	}

	/// Whether `paused/<id>/` holds a complete, durable pause.
	pub fn paused_resumable(&self) -> bool {
		self.paused_rootfs().is_file() && self.paused_committed().is_file()
	}
}

/// Writes `meta.json` atomically with mode 0600, durably.
pub fn save(paths: &SandboxPaths, meta: &SandboxMeta) -> std::io::Result<()> {
	write(paths, meta, true)
}

/// [`save`] without the fsync, for a freshly created running sandbox. Only a
/// node process restart reads it (to adopt the VMM), and the page cache
/// survives that; a host crash that loses it ends the VMM too, and the
/// directory without metadata then ages out in `SandboxManager::gc`. The
/// fsync of a new file waits for a filesystem journal commit, which takes
/// seconds under heavy write load (ext4 `data=ordered` flushes other files'
/// new data first) and would hold up every create.
pub fn save_unsynced(paths: &SandboxPaths, meta: &SandboxMeta) -> std::io::Result<()> {
	write(paths, meta, false)
}

fn write(paths: &SandboxPaths, meta: &SandboxMeta, durable: bool) -> std::io::Result<()> {
	let tmp = paths.dir.join("meta.json.tmp");
	{
		let mut file = fs::OpenOptions::new()
			.write(true)
			.create(true)
			.truncate(true)
			.mode(0o600)
			.open(&tmp)?;
		file.write_all(&serde_json::to_vec_pretty(meta)?)?;
		if durable {
			file.sync_all()?;
		}
	}
	fs::rename(&tmp, paths.meta())
}

/// Reads `meta.json`; `None` when missing or unreadable.
pub fn load(path: &Path) -> Option<SandboxMeta> {
	serde_json::from_slice(&fs::read(path).ok()?).ok()
}
