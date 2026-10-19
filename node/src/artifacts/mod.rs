//! Build artifact store (ARCHITECTURE.md §8.1, §8.5, §8.6).
//!
//! Layout per build: `builds/<buildId>/{rootfs.ext4, snapshot/…, meta.json,
//! manifest.json, build.log}`. `meta.json` ([`ArtifactMeta`]) marks a
//! complete artifact; `manifest.json` caches the data extents and extent
//! digest of every served file ([`sparse`]) so peers can fetch and verify
//! without rehashing multi-GiB images on each request. Peer fetches transfer
//! only data extents, stage into a hidden directory, and are installed with
//! one rename.

pub mod copy;
pub mod published;
pub mod sparse;

use std::{
	collections::HashMap,
	path::{Component, Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};

use anyhow::{Context, Result, anyhow, bail};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use vmon_cluster::NodeId;

use crate::{
	api::{ArtifactFile, ArtifactInfo, ArtifactManifest},
	client::NodeClient,
};

/// Root filesystem image of a build.
pub const ROOTFS: &str = "rootfs.ext4";
/// VMM snapshot name (directory) inside the build directory.
pub const SNAPSHOT: &str = "snapshot";
/// Marker + metadata of a complete artifact.
pub const META: &str = "meta.json";
/// Cached manifest (file list with digests).
pub const MANIFEST: &str = "manifest.json";
/// Build log (JSON lines of `BuildLogEntry`).
pub const BUILD_LOG: &str = "build.log";

/// Everything recorded about a local artifact: the shared [`ArtifactInfo`]
/// plus what is needed to cold-boot it or regenerate its snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArtifactMeta {
	pub info:          ArtifactInfo,
	pub template_id:   String,
	/// Kernel `init=` for booting the rootfs (`/usr/sbin/init` or
	/// `/usr/bin/envd`).
	pub boot_init:     String,
	pub start_cmd:     Option<String>,
	pub ready_cmd:     Option<String>,
	/// `sha256:` digest of the guest agent installed as `/usr/bin/envd`.
	#[serde(default)]
	pub agent_digest:  String,
	pub created_at_ms: u64,
}

/// Host facts that decide whether a snapshot can be reused here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostIdentity {
	pub node_id:      NodeId,
	pub arch:         String,
	pub backend:      String,
	pub cpu_baseline: String,
}

impl HostIdentity {
	/// Whether a snapshot captured with `info` restores on this host.
	pub fn snapshot_compatible(&self, info: &ArtifactInfo) -> bool {
		info.has_snapshot
			&& info.arch == self.arch
			&& info.backend == self.backend
			&& info.cpu_baseline == self.cpu_baseline
	}
}

/// Kernel command line for booting a template rootfs with `init`
/// (shared by builds and sandbox cold boots).
pub fn kernel_cmdline(arch: &str, init: &str) -> String {
	let earlycon = if arch == "aarch64" {
		" earlycon=uart8250,mmio,0x9000000"
	} else {
		""
	};
	format!("console=ttyS0 reboot=t panic=-1{earlycon} init={init}")
}

/// Local artifact store under `<data>/builds`.
#[derive(Clone, Debug)]
pub struct ArtifactStore {
	inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
	builds:         PathBuf,
	host:           HostIdentity,
	secret:         String,
	http:           reqwest::Client,
	locks:          parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
	/// Per-build serialization of manifest hashing.
	manifest_locks: parking_lot::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
	/// Operator object store: committed builds are published to
	/// `builds/<id>/`, and fetches fall back to it when no peer serves them.
	object_store:   Option<vmon_cloud::s3::S3Client>,
}

impl ArtifactStore {
	/// Opens (creating) `<data_dir>/builds`; `secret` authenticates peer
	/// fetches.
	pub fn new(
		data_dir: &Path,
		host: HostIdentity,
		secret: String,
		http: reqwest::Client,
	) -> std::io::Result<Self> {
		let builds = data_dir.join("builds");
		std::fs::create_dir_all(&builds)?;
		Ok(Self {
			inner: Arc::new(Inner {
				builds,
				host,
				secret,
				http,
				locks: parking_lot::Mutex::new(HashMap::new()),
				manifest_locks: parking_lot::Mutex::new(HashMap::new()),
				object_store: None,
			}),
		})
	}

	/// Attaches the operator object store (call before sharing the store).
	#[must_use]
	pub fn with_object_store(mut self, object_store: Option<vmon_cloud::s3::S3Client>) -> Self {
		Arc::get_mut(&mut self.inner)
			.expect("with_object_store before the store is shared")
			.object_store = object_store;
		self
	}

	/// This node's snapshot-compatibility identity.
	pub fn host(&self) -> &HostIdentity {
		&self.inner.host
	}

	/// Directory of `build_id` (validated: no path separators or dots).
	pub fn build_dir(&self, build_id: &str) -> Result<PathBuf> {
		validate_id(build_id)?;
		Ok(self.inner.builds.join(build_id))
	}

	/// `builds/<id>/rootfs.ext4`.
	pub fn rootfs_path(&self, build_id: &str) -> Result<PathBuf> {
		Ok(self.build_dir(build_id)?.join(ROOTFS))
	}

	/// `builds/<id>/snapshot` (VMM `--snapshot-root builds/<id>`, name
	/// `snapshot`).
	pub fn snapshot_dir(&self, build_id: &str) -> Result<PathBuf> {
		Ok(self.build_dir(build_id)?.join(SNAPSHOT))
	}

	/// Per-build async lock serializing fetch, regeneration, and deletion.
	pub fn lock(&self, build_id: &str) -> Arc<tokio::sync::Mutex<()>> {
		self
			.inner
			.locks
			.lock()
			.entry(build_id.to_owned())
			.or_default()
			.clone()
	}

	/// Metadata of a complete local artifact.
	pub fn local_meta(&self, build_id: &str) -> Option<ArtifactMeta> {
		let dir = self.build_dir(build_id).ok()?;
		let meta: ArtifactMeta = serde_json::from_slice(&std::fs::read(dir.join(META)).ok()?).ok()?;
		if !dir.join(ROOTFS).is_file() {
			return None;
		}
		if meta.info.has_snapshot && !dir.join(SNAPSHOT).join("current-generation").is_file() {
			return None;
		}
		Some(meta)
	}

	/// `ArtifactInfo` of a complete local artifact.
	pub fn local(&self, build_id: &str) -> Option<ArtifactInfo> {
		self.local_meta(build_id).map(|m| m.info)
	}

	/// Kernel `init=` for cold-booting a local artifact.
	pub fn boot_init(&self, build_id: &str) -> Option<String> {
		self.local_meta(build_id).map(|m| m.boot_init)
	}

	/// Build IDs with complete local artifacts, most recently finished first.
	pub fn cached_builds(&self) -> Vec<String> {
		let Ok(entries) = std::fs::read_dir(&self.inner.builds) else {
			return Vec::new();
		};
		let mut builds: Vec<(SystemTime, String)> = entries
			.filter_map(Result::ok)
			.filter_map(|entry| {
				let name = entry.file_name().into_string().ok()?;
				if name.starts_with('.') {
					return None;
				}
				let modified = std::fs::metadata(entry.path().join(META))
					.and_then(|m| m.modified())
					.ok()?;
				Some((modified, name))
			})
			.collect();
		builds.sort_unstable_by_key(|(modified, _)| std::cmp::Reverse(*modified));
		builds.into_iter().map(|(_, id)| id).collect()
	}

	/// Writes `meta.json` and a freshly hashed `manifest.json`, making the
	/// artifact complete. Returns the recorded info (with `bytes` filled in).
	pub async fn commit(&self, build_id: &str, mut meta: ArtifactMeta) -> Result<ArtifactInfo> {
		let dir = self.build_dir(build_id)?;
		meta.info.bytes = tokio::task::spawn_blocking({
			let dir = dir.clone();
			move || disk_usage(&dir)
		})
		.await??;
		let _ = tokio::fs::remove_file(dir.join(MANIFEST)).await;
		write_json_atomic(&dir.join(META), &meta).await?;
		// Hash in the background so the build finishes without waiting; the
		// first manifest request joins the same computation.
		let store = self.clone();
		let id = build_id.to_owned();
		tokio::spawn(async move {
			if let Err(error) = store.manifest(&id).await {
				tracing::warn!(
					build_id = id,
					error = format!("{error:#}"),
					"artifact manifest hashing failed"
				);
				return;
			}
			// Make the build survive the loss of every node holding it.
			if let Err(error) = store.publish(&id).await {
				tracing::warn!(
					build_id = id,
					error = format!("{error:#}"),
					"publishing artifacts to the object store failed"
				);
			}
		});
		Ok(meta.info)
	}

	/// The manifest served to peers (`GET /artifacts/{id}/manifest`),
	/// hashing the served files on first use.
	pub async fn manifest(&self, build_id: &str) -> Result<Option<ArtifactManifest>> {
		let Some(meta) = self.local_meta(build_id) else {
			return Ok(None);
		};
		let dir = self.build_dir(build_id)?;
		let lock = self
			.inner
			.manifest_locks
			.lock()
			.entry(build_id.to_owned())
			.or_default()
			.clone();
		let _guard = lock.lock().await;
		if let Ok(bytes) = tokio::fs::read(dir.join(MANIFEST)).await
			&& let Ok(manifest) = serde_json::from_slice::<ArtifactManifest>(&bytes)
			&& manifest.info == meta.info
		{
			return Ok(Some(manifest));
		}
		let files = tokio::task::spawn_blocking({
			let dir = dir.clone();
			move || hash_tree(&dir)
		})
		.await??;
		let manifest = ArtifactManifest { info: meta.info, files };
		write_json_atomic(&dir.join(MANIFEST), &manifest).await?;
		Ok(Some(manifest))
	}

	/// Resolves a served file and its manifest entry: only paths listed in
	/// the manifest are exposed.
	pub async fn served_file(
		&self,
		build_id: &str,
		rel: &str,
	) -> Result<Option<(PathBuf, ArtifactFile)>> {
		let Some(manifest) = self.manifest(build_id).await? else {
			return Ok(None);
		};
		let Some(file) = manifest.files.into_iter().find(|f| f.path == rel) else {
			return Ok(None);
		};
		Ok(Some((self.build_dir(build_id)?.join(safe_relative(rel)?), file)))
	}

	/// Removes a build's artifacts (GC). Returns whether anything existed.
	pub async fn delete(&self, build_id: &str) -> Result<bool> {
		let lock = self.lock(build_id);
		let _guard = lock.lock().await;
		let dir = self.build_dir(build_id)?;
		if !tokio::fs::try_exists(&dir).await.unwrap_or(false) {
			return Ok(false);
		}
		let trash = self
			.inner
			.builds
			.join(format!(".trash-{build_id}-{:08x}", rand::random::<u32>()));
		tokio::fs::rename(&dir, &trash)
			.await
			.with_context(|| format!("removing {}", dir.display()))?;
		tokio::fs::remove_dir_all(&trash).await?;
		Ok(true)
	}

	/// Removes the outputs of a build that did not complete: rootfs (with
	/// its overlay base), snapshot, meta, and manifest. Logs stay.
	pub fn discard_outputs(&self, build_id: &str) {
		let Ok(dir) = self.build_dir(build_id) else {
			return;
		};
		let _ = copy::remove_disk(&dir.join(ROOTFS));
		for file in [META, MANIFEST] {
			let _ = std::fs::remove_file(dir.join(file));
		}
		let _ = std::fs::remove_dir_all(dir.join(SNAPSHOT));
	}

	/// Reclaims what a previous process left unfinished. Call once at
	/// startup, before builds, fetches, or deletions run: hidden `.trash-*`
	/// and `.incoming-*` directories of interrupted deletions and peer
	/// fetches, and the outputs of builds without `meta.json` (builds cut off
	/// by the restart; their logs stay for `GET /builds/{id}`).
	pub fn sweep_interrupted(&self) {
		let entries = match std::fs::read_dir(&self.inner.builds) {
			Ok(entries) => entries,
			Err(error) => {
				tracing::warn!(%error, "listing builds for cleanup");
				return;
			},
		};
		for entry in entries.filter_map(Result::ok) {
			let Ok(name) = entry.file_name().into_string() else {
				continue;
			};
			let path = entry.path();
			if name.starts_with('.') {
				if let Err(error) = std::fs::remove_dir_all(&path) {
					tracing::warn!(%error, path = %path.display(), "removing an interrupted artifact operation");
				}
			} else if !path.join(META).exists()
				&& (path.join(ROOTFS).exists() || path.join(SNAPSHOT).exists())
			{
				tracing::info!(build = name, "discarding outputs of an interrupted build");
				self.discard_outputs(&name);
			}
		}
	}

	/// Installs a snapshot-as-template build captured from a running sandbox:
	/// moves `snapshot_dir` and `rootfs` (same filesystem) into
	/// `builds/<build_id>/` and commits `meta` (node identity fields are set
	/// from this host, `has_snapshot` follows whether a snapshot was given,
	/// `created_at_ms` is now).
	pub async fn install_snapshot_build(
		&self,
		build_id: &str,
		snapshot_dir: Option<&Path>,
		rootfs: &Path,
		meta: ArtifactMeta,
	) -> Result<ArtifactInfo> {
		let lock = self.lock(build_id);
		let _guard = lock.lock().await;
		let dir = self.build_dir(build_id)?;
		if tokio::fs::try_exists(&dir.join(META))
			.await
			.unwrap_or(false)
		{
			bail!("build {build_id} already has artifacts");
		}
		tokio::fs::create_dir_all(&dir).await?;
		let (src, dst) = (rootfs.to_owned(), dir.join(ROOTFS));
		tokio::task::spawn_blocking(move || copy::move_disk(&src, &dst))
			.await?
			.with_context(|| format!("moving {} into {build_id}", rootfs.display()))?;
		if let Some(snapshot) = snapshot_dir {
			tokio::fs::rename(snapshot, dir.join(SNAPSHOT))
				.await
				.with_context(|| format!("moving {} into {build_id}", snapshot.display()))?;
		}
		let host = &self.inner.host;
		let meta = ArtifactMeta {
			info: ArtifactInfo {
				build_id: build_id.to_owned(),
				node_id: host.node_id,
				arch: host.arch.clone(),
				backend: host.backend.clone(),
				cpu_baseline: host.cpu_baseline.clone(),
				has_snapshot: snapshot_dir.is_some(),
				..meta.info
			},
			created_at_ms: now_ms(),
			..meta
		};
		self.commit(build_id, meta).await
	}

	/// Fetches a build's artifacts from the first reachable peer in
	/// `sources`, verifying every file against the peer's manifest, and
	/// installs them atomically. The snapshot is only transferred when it is
	/// compatible with this host; otherwise the installed meta has
	/// `has_snapshot = false` and the caller regenerates it.
	pub async fn fetch(&self, build_id: &str, sources: &[String]) -> Result<ArtifactMeta> {
		let mut errors = Vec::new();
		for source in sources {
			match self.fetch_from(build_id, source).await {
				Ok(meta) => return Ok(meta),
				Err(error) => {
					tracing::warn!(
						build_id,
						source,
						error = format!("{error:#}"),
						"artifact fetch failed"
					);
					errors.push(format!("{source}: {error:#}"));
				},
			}
		}
		if let Some(s3) = &self.inner.object_store {
			match self.fetch_published(build_id, s3).await {
				Ok(meta) => return Ok(meta),
				Err(error) => errors.push(format!("object store: {error:#}")),
			}
		}
		if errors.is_empty() {
			bail!("build {build_id} is not available locally and no artifact sources were given");
		}
		Err(anyhow!("fetching artifacts of {build_id} failed: {}", errors.join("; ")))
	}

	/// Node API client for the peer at `cluster_url`, authenticated and
	/// pooled like artifact fetches.
	pub(crate) fn peer(&self, cluster_url: &str) -> Result<NodeClient, crate::client::NodeError> {
		NodeClient::new(cluster_url, &self.inner.secret, self.inner.http.clone())
	}

	async fn fetch_from(&self, build_id: &str, source: &str) -> Result<ArtifactMeta> {
		let client = self.peer(source)?;
		let manifest = client.artifact_manifest(build_id).await?;
		self
			.install_fetched(build_id, &manifest, |path| {
				let (client, path) = (client.clone(), path.to_owned());
				async move {
					let response = client.artifact_extents(build_id, &path).await?;
					let stream: published::ByteStream =
						Box::pin(response.bytes_stream().map_err(anyhow::Error::from));
					Ok(stream)
				}
			})
			.await
	}

	/// Downloads the files of `manifest` (snapshot only when compatible here)
	/// through `open`, verifies them, and installs and commits the build.
	async fn install_fetched<F, Fut>(
		&self,
		build_id: &str,
		manifest: &ArtifactManifest,
		open: F,
	) -> Result<ArtifactMeta>
	where
		F: Fn(&str) -> Fut,
		Fut: Future<Output = Result<published::ByteStream>>,
	{
		let with_snapshot = self.inner.host.snapshot_compatible(&manifest.info);
		let wanted = select_fetch_files(manifest, with_snapshot)?;
		let staging = self
			.inner
			.builds
			.join(format!(".incoming-{build_id}-{:08x}", rand::random::<u32>()));
		let result = async {
			for file in &wanted {
				let dest = staging.join(safe_relative(&file.path)?);
				if let Some(parent) = dest.parent() {
					tokio::fs::create_dir_all(parent).await?;
				}
				let stream = open(&file.path).await?;
				sparse::write_extents(stream, file, &dest)
					.await
					.with_context(|| format!("fetching {}", file.path))?;
			}
			let mut meta: ArtifactMeta =
				serde_json::from_slice(&tokio::fs::read(staging.join(META)).await?)
					.context("peer meta.json")?;
			if meta.info.build_id != build_id {
				bail!("peer meta.json describes build {}", meta.info.build_id);
			}
			meta.info.node_id = self.inner.host.node_id;
			meta.info.has_snapshot = with_snapshot;
			let dir = self.build_dir(build_id)?;
			if tokio::fs::try_exists(&dir).await.unwrap_or(false) {
				let stale = self
					.inner
					.builds
					.join(format!(".trash-{build_id}-{:08x}", rand::random::<u32>()));
				tokio::fs::rename(&dir, &stale).await?;
				let _ = tokio::fs::remove_dir_all(&stale).await;
			}
			tokio::fs::rename(&staging, &dir).await?;
			Ok(meta)
		}
		.await;
		if result.is_err() {
			let _ = tokio::fs::remove_dir_all(&staging).await;
		}
		let meta = result?;
		self.commit(build_id, meta.clone()).await?;
		Ok(meta)
	}
}

/// Files to fetch from a peer manifest: the rootfs and meta always, the
/// snapshot only when reusable here. Rejects manifests missing either.
fn select_fetch_files(
	manifest: &ArtifactManifest,
	with_snapshot: bool,
) -> Result<Vec<ArtifactFile>> {
	for required in [ROOTFS, META] {
		if !manifest.files.iter().any(|f| f.path == required) {
			bail!("peer manifest lacks {required}");
		}
	}
	let snapshot_prefix = format!("{SNAPSHOT}/");
	if with_snapshot
		&& !manifest
			.files
			.iter()
			.any(|f| f.path == format!("{SNAPSHOT}/current-generation"))
	{
		bail!("peer manifest claims a snapshot but lacks {SNAPSHOT}/current-generation");
	}
	manifest
		.files
		.iter()
		.filter(|f| with_snapshot || !f.path.starts_with(&snapshot_prefix))
		.map(|f| {
			safe_relative(&f.path)?;
			if f.sha256.len() != 64 || !f.sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
				bail!("peer manifest has an invalid digest for {}", f.path);
			}
			sparse::validate(f)?;
			Ok(f.clone())
		})
		.collect()
}

/// Rejects anything but plain relative paths (`a/b`), preventing traversal.
fn safe_relative(rel: &str) -> Result<PathBuf> {
	let path = Path::new(rel);
	if rel.is_empty() || !path.components().all(|c| matches!(c, Component::Normal(_))) {
		bail!("invalid artifact path {rel:?}");
	}
	Ok(path.to_path_buf())
}

fn validate_id(id: &str) -> Result<()> {
	if id.is_empty()
		|| id.len() > 64
		|| !id
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		bail!("invalid build id {id:?}");
	}
	Ok(())
}

/// Hashes every served file of a build directory: `rootfs.ext4` (plus its
/// raw base `rootfs.ext4.base` when it is a block overlay), `meta.json`, and
/// everything below `snapshot/`, sorted by path.
fn hash_tree(dir: &Path) -> Result<Vec<ArtifactFile>> {
	let mut files = Vec::new();
	for name in [ROOTFS, META] {
		files.push(sparse::describe(dir, name)?);
	}
	let base = copy::disk_base(Path::new(ROOTFS));
	if dir.join(&base).is_file() {
		files.push(sparse::describe(dir, &base.to_string_lossy())?);
	}
	let snapshot = dir.join(SNAPSHOT);
	if snapshot.is_dir() {
		let mut stack = vec![snapshot];
		while let Some(current) = stack.pop() {
			for entry in std::fs::read_dir(&current)? {
				let entry = entry?;
				let file_type = entry.file_type()?;
				let name = entry.file_name();
				if name.to_string_lossy().ends_with(".tmp") {
					continue;
				}
				if file_type.is_dir() {
					stack.push(entry.path());
				} else if file_type.is_file() {
					let rel = entry
						.path()
						.strip_prefix(dir)
						.map_err(|e| anyhow!("{e}"))?
						.to_string_lossy()
						.replace('\\', "/");
					files.push(sparse::describe(dir, &rel)?);
				}
			}
		}
	}
	files.sort_unstable_by(|a, b| a.path.cmp(&b.path));
	Ok(files)
}

/// Allocated bytes of a directory tree (sparse files count what they use).
pub(crate) fn disk_usage(dir: &Path) -> Result<u64> {
	let mut total = 0u64;
	let mut stack = vec![dir.to_path_buf()];
	while let Some(current) = stack.pop() {
		for entry in std::fs::read_dir(&current)? {
			let entry = entry?;
			let meta = entry.metadata()?;
			if meta.is_dir() {
				stack.push(entry.path());
			} else {
				total += allocated(&meta);
			}
		}
	}
	Ok(total)
}

#[cfg(unix)]
fn allocated(meta: &std::fs::Metadata) -> u64 {
	use std::os::unix::fs::MetadataExt;
	meta.blocks() * 512
}

#[cfg(not(unix))]
fn allocated(meta: &std::fs::Metadata) -> u64 {
	meta.len()
}

/// Writes JSON via a temp file + rename.
pub async fn write_json_atomic<T: Serialize + Sync + ?Sized>(path: &Path, value: &T) -> Result<()> {
	let tmp = path.with_extension(format!("tmp-{:08x}", rand::random::<u32>()));
	let bytes = serde_json::to_vec_pretty(value)?;
	tokio::fs::write(&tmp, bytes)
		.await
		.with_context(|| format!("writing {}", tmp.display()))?;
	tokio::fs::rename(&tmp, path)
		.await
		.with_context(|| format!("installing {}", path.display()))?;
	Ok(())
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::*;

	fn host() -> HostIdentity {
		HostIdentity {
			node_id:      NodeId(7),
			arch:         "aarch64".into(),
			backend:      "hvf".into(),
			cpu_baseline: "m4".into(),
		}
	}

	fn info(id: &str) -> ArtifactInfo {
		ArtifactInfo {
			build_id:        id.into(),
			node_id:         NodeId(1),
			arch:            "aarch64".into(),
			backend:         "hvf".into(),
			cpu_baseline:    "m4".into(),
			has_snapshot:    true,
			bytes:           0,
			envd_version:    "0.6.4".into(),
			vcpus:           2,
			memory_mb:       512,
			disk_mb:         1024,
			default_user:    "user".into(),
			default_workdir: None,
			env:             BTreeMap::new(),
		}
	}

	fn meta(id: &str) -> ArtifactMeta {
		ArtifactMeta {
			info:          info(id),
			template_id:   "t".into(),
			boot_init:     "/usr/sbin/init".into(),
			start_cmd:     None,
			ready_cmd:     None,
			agent_digest:  String::new(),
			created_at_ms: 1,
		}
	}

	/// A host on `backend` (otherwise like [`host`]).
	pub(super) fn host_with(backend: &str) -> HostIdentity {
		HostIdentity { backend: backend.into(), ..host() }
	}

	/// A store holding the committed build `id` (sparse rootfs + snapshot).
	pub(super) async fn committed_build(
		id: &str,
		host: HostIdentity,
		object_store: Option<vmon_cloud::s3::S3Client>,
	) -> (tempfile::TempDir, ArtifactStore) {
		let dir = tempfile::tempdir().unwrap();
		let store = ArtifactStore::new(dir.path(), host, "s".into(), reqwest::Client::new())
			.unwrap()
			.with_object_store(object_store);
		let build = store.build_dir(id).unwrap();
		std::fs::create_dir_all(build.join(SNAPSHOT)).unwrap();
		let rootfs = std::fs::File::create(build.join(ROOTFS)).unwrap();
		rootfs.set_len(16 << 20).unwrap();
		std::os::unix::fs::FileExt::write_all_at(&rootfs, &[7; 4096], 8 << 20).unwrap();
		std::fs::write(build.join("snapshot/current-generation"), "1").unwrap();
		std::fs::write(build.join("snapshot/memory.1.bin"), vec![3; 8192]).unwrap();
		write_json_atomic(&build.join(META), &meta(id))
			.await
			.unwrap();
		store.manifest(id).await.unwrap().unwrap();
		(dir, store)
	}

	fn file(path: &str) -> ArtifactFile {
		ArtifactFile {
			path:    path.into(),
			size:    1,
			sha256:  "a".repeat(64),
			extents: vec![[0, 1]],
		}
	}

	#[test]
	fn snapshot_reuse_requires_matching_arch_backend_and_baseline() {
		let host = host();
		assert!(host.snapshot_compatible(&info("b")));
		for mutate in [
			(|i: &mut ArtifactInfo| i.arch = "x86_64".into()) as fn(&mut ArtifactInfo),
			|i| i.backend = "kvm".into(),
			|i| i.cpu_baseline = "m1".into(),
			|i| i.has_snapshot = false,
		] {
			let mut other = info("b");
			mutate(&mut other);
			assert!(!host.snapshot_compatible(&other));
		}
	}

	#[test]
	fn fetch_selection_skips_incompatible_snapshot_and_rejects_bad_manifests() {
		let manifest = ArtifactManifest {
			info:  info("b"),
			files: vec![
				file(META),
				file(ROOTFS),
				file("snapshot/current-generation"),
				file("snapshot/memory.1.bin"),
			],
		};
		assert_eq!(select_fetch_files(&manifest, true).unwrap().len(), 4);
		let without: Vec<String> = select_fetch_files(&manifest, false)
			.unwrap()
			.into_iter()
			.map(|f| f.path)
			.collect();
		assert_eq!(without, [META, ROOTFS]);

		let mut traversal = manifest.clone();
		traversal.files.push(file("../../etc/passwd"));
		assert!(select_fetch_files(&traversal, false).is_err());
		let mut bad_digest = manifest.clone();
		bad_digest.files[0].sha256 = "zz".into();
		assert!(select_fetch_files(&bad_digest, false).is_err());
		let mut bad_extents = manifest;
		bad_extents.files[0].extents = vec![[0, 2]];
		assert!(select_fetch_files(&bad_extents, false).is_err(), "extent past the size");
		let no_rootfs = ArtifactManifest { info: info("b"), files: vec![file(META)] };
		assert!(select_fetch_files(&no_rootfs, false).is_err());
		let no_generation =
			ArtifactManifest { info: info("b"), files: vec![file(META), file(ROOTFS)] };
		assert!(select_fetch_files(&no_generation, true).is_err());
	}

	#[tokio::test]
	async fn commit_hashes_served_files_and_manifest_exposes_only_them() {
		let data = tempfile::tempdir().unwrap();
		let store =
			ArtifactStore::new(data.path(), host(), "s".into(), reqwest::Client::new()).unwrap();
		let dir = store.build_dir("b1").unwrap();
		std::fs::create_dir_all(dir.join(SNAPSHOT)).unwrap();
		std::fs::write(dir.join(ROOTFS), b"rootfs").unwrap();
		std::fs::write(dir.join(SNAPSHOT).join("current-generation"), b"1").unwrap();
		std::fs::write(dir.join(BUILD_LOG), b"{}").unwrap();
		assert!(store.local("b1").is_none(), "no meta yet = incomplete");

		store.commit("b1", meta("b1")).await.unwrap();
		let manifest = store.manifest("b1").await.unwrap().unwrap();
		let paths: Vec<&str> = manifest.files.iter().map(|f| f.path.as_str()).collect();
		assert_eq!(paths, [META, ROOTFS, "snapshot/current-generation"]);
		let rootfs = manifest.files.iter().find(|f| f.path == ROOTFS).unwrap();
		let mut expected = sparse::ExtentHasher::default();
		expected.extent([0, 6]);
		expected.update(b"rootfs");
		assert_eq!(rootfs.sha256, expected.finish(6));
		assert_eq!((rootfs.size, rootfs.extents.as_slice()), (6, [[0, 6]].as_slice()));
		assert!(store.served_file("b1", BUILD_LOG).await.unwrap().is_none());
		assert!(store.served_file("b1", ROOTFS).await.unwrap().is_some());
		assert_eq!(store.cached_builds(), ["b1"]);
		assert_eq!(store.boot_init("b1").as_deref(), Some("/usr/sbin/init"));

		// A snapshot-claiming meta without the generation file is not complete.
		std::fs::remove_file(dir.join(SNAPSHOT).join("current-generation")).unwrap();
		assert!(store.local("b1").is_none());
		assert!(store.delete("b1").await.unwrap());
		assert!(!dir.exists());
		assert!(!store.delete("b1").await.unwrap());
	}

	#[tokio::test]
	async fn startup_sweep_discards_interrupted_builds_and_operations() {
		let (data, store) = committed_build("done", host(), None).await;
		let builds = data.path().join("builds");
		let names = |dir: &Path| {
			let mut names: Vec<String> = std::fs::read_dir(dir)
				.unwrap()
				.map(|e| e.unwrap().file_name().into_string().unwrap())
				.collect();
			names.sort();
			names
		};
		// A build cut off by a restart: partial disk pair, snapshot, log.
		let cut = store.build_dir("cut").unwrap();
		std::fs::create_dir_all(cut.join(SNAPSHOT)).unwrap();
		std::fs::write(cut.join(ROOTFS), b"overlay").unwrap();
		std::fs::write(copy::disk_base(&cut.join(ROOTFS)), b"base").unwrap();
		std::fs::write(cut.join(SNAPSHOT).join("memory.1.bin"), b"ram").unwrap();
		std::fs::write(cut.join(BUILD_LOG), b"{}").unwrap();
		// Interrupted deletion and peer fetch.
		for leftover in [".trash-old-0123abcd", ".incoming-new-89abcdef"] {
			std::fs::create_dir_all(builds.join(leftover)).unwrap();
			std::fs::write(builds.join(leftover).join(ROOTFS), b"x").unwrap();
		}

		store.sweep_interrupted();
		assert_eq!(names(&builds), ["cut", "done"]);
		assert_eq!(names(&cut), [BUILD_LOG], "only the log of the cut-off build stays");
		assert!(store.local("done").is_some(), "complete builds are untouched");
	}

	/// Serves `store`'s artifacts like the node API; `tamper` corrupts
	/// `rootfs.ext4`.
	async fn serve_peer(store: ArtifactStore, tamper: bool) -> String {
		use axum::{Json, Router, extract::Path as UrlPath, routing::get};
		let manifest_store = store.clone();
		let app = Router::new()
			.route(
				"/node/v1/artifacts/{id}/manifest",
				get(move |UrlPath(id): UrlPath<String>| {
					let store = manifest_store.clone();
					async move { Json(store.manifest(&id).await.unwrap().unwrap()) }
				}),
			)
			.route(
				"/node/v1/artifacts/{id}/files/{*path}",
				get(move |UrlPath((id, path)): UrlPath<(String, String)>| {
					let store = store.clone();
					async move {
						let (file, entry) = store.served_file(&id, &path).await.unwrap().unwrap();
						let mut bytes = sparse::read_extents(file, entry.extents)
							.try_fold(Vec::new(), |mut acc, chunk| async move {
								acc.extend_from_slice(&chunk);
								Ok(acc)
							})
							.await
							.unwrap();
						if tamper && path == ROOTFS {
							bytes[0] ^= 0xff;
						}
						bytes
					}
				}),
			);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = format!("http://{}", listener.local_addr().unwrap());
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		url
	}

	async fn peer_with_build(id: &str) -> (tempfile::TempDir, ArtifactStore) {
		let data = tempfile::tempdir().unwrap();
		let peer = ArtifactStore::new(
			data.path(),
			HostIdentity { node_id: NodeId(1), ..host() },
			"s".into(),
			reqwest::Client::new(),
		)
		.unwrap();
		let dir = peer.build_dir(id).unwrap();
		std::fs::create_dir_all(dir.join(SNAPSHOT)).unwrap();
		std::fs::write(dir.join(ROOTFS), b"rootfs-bytes").unwrap();
		std::fs::write(dir.join(SNAPSHOT).join("current-generation"), b"1").unwrap();
		std::fs::write(dir.join(SNAPSHOT).join("memory.1.bin"), b"memory").unwrap();
		peer.commit(id, meta(id)).await.unwrap();
		(data, peer)
	}

	#[tokio::test]
	async fn peer_fetch_verifies_installs_and_skips_incompatible_snapshots() {
		let (_peer_dir, peer) = peer_with_build("b1").await;
		let url = serve_peer(peer, false).await;

		let local_dir = tempfile::tempdir().unwrap();
		let local =
			ArtifactStore::new(local_dir.path(), host(), "s".into(), reqwest::Client::new()).unwrap();
		let fetched = local
			.fetch("b1", &["http://127.0.0.1:1".into(), url.clone()])
			.await
			.unwrap();
		assert!(fetched.info.has_snapshot, "same arch/backend/baseline: snapshot reused");
		assert_eq!(fetched.info.node_id, NodeId(7), "installed info names this node");
		assert_eq!(std::fs::read(local.rootfs_path("b1").unwrap()).unwrap(), b"rootfs-bytes");
		assert!(local.local("b1").is_some());

		let other_dir = tempfile::tempdir().unwrap();
		let other_host = HostIdentity { cpu_baseline: "m1".into(), ..host() };
		let other =
			ArtifactStore::new(other_dir.path(), other_host, "s".into(), reqwest::Client::new())
				.unwrap();
		let fetched = other.fetch("b1", &[url]).await.unwrap();
		assert!(!fetched.info.has_snapshot, "incompatible snapshot is not transferred");
		assert!(!other.snapshot_dir("b1").unwrap().exists());
		assert!(other.local("b1").is_some(), "rootfs-only artifact is complete");
	}

	#[tokio::test]
	async fn tampered_peer_file_is_rejected_and_nothing_is_installed() {
		let (_peer_dir, peer) = peer_with_build("b2").await;
		let url = serve_peer(peer, true).await;
		let local_dir = tempfile::tempdir().unwrap();
		let local =
			ArtifactStore::new(local_dir.path(), host(), "s".into(), reqwest::Client::new()).unwrap();
		let error = local.fetch("b2", &[url]).await.unwrap_err();
		assert!(format!("{error:#}").contains("digest mismatch"), "{error:#}");
		assert!(!local.build_dir("b2").unwrap().exists());
		let leftovers: Vec<_> = std::fs::read_dir(local_dir.path().join("builds"))
			.unwrap()
			.collect();
		assert!(leftovers.is_empty(), "staging dir cleaned up");
	}
	#[test]
	fn ids_and_paths_cannot_escape() {
		let data = tempfile::tempdir().unwrap();
		let store =
			ArtifactStore::new(data.path(), host(), "s".into(), reqwest::Client::new()).unwrap();
		for bad in ["", "..", "a/b", ".hidden", "a b"] {
			assert!(store.build_dir(bad).is_err(), "{bad:?}");
		}
		for bad in ["", "/etc/passwd", "a/../b", "./a"] {
			assert!(safe_relative(bad).is_err(), "{bad:?}");
		}
		assert!(safe_relative("snapshot/memory.1.bin").is_ok());
	}

	#[test]
	fn cmdline_adds_earlycon_only_on_aarch64() {
		assert_eq!(
			kernel_cmdline("x86_64", "/usr/bin/envd"),
			"console=ttyS0 reboot=t panic=-1 init=/usr/bin/envd"
		);
		assert!(
			kernel_cmdline("aarch64", "/usr/sbin/init")
				.contains("earlycon=uart8250,mmio,0x9000000 init=/usr/sbin/init")
		);
	}
}
