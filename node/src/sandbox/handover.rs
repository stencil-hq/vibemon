//! Offline handover of a paused sandbox to another node (ARCHITECTURE.md
//! §8.2).
//!
//! The source pauses with `PauseSandbox::handover` (the state stays
//! node-local) and serves the committed paused state to peers
//! ([`SandboxManager::handover_manifest`], [`SandboxManager::handover_file`]).
//! The target pulls it ([`SandboxManager::adopt_paused`]), hard-linking the
//! files its own copy of the template build holds identically, and later
//! resumes it from local state. Once the driver moved the sandbox, the source
//! drops its copy ([`SandboxManager::drop_paused`]).

use std::{
	collections::{HashMap, HashSet},
	fs,
	os::unix::fs::MetadataExt,
	path::{Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};

use anyhow::{Context, bail};
use futures::TryStreamExt;
use parking_lot::Mutex;

use super::{
	Entry, EntryState, Pending, SandboxManager, SandboxMeta, SandboxPaths, commit_paused, meta,
	remove_path, upload_staging, validate_id,
};
use crate::{
	api::{ArtifactFile, HandoverFile, HandoverManifest, SandboxInfo, SandboxState},
	artifacts::{published::ByteStream, sparse},
	lazy_resume,
	server::ApiError,
	vm::private_dir,
};

/// Files of `sandboxes/<id>/` a handover carries along (the history the logs
/// and metrics routes serve).
const LOGS: [&str; 2] = ["console.log", "metrics.jsonl"];
/// First component of manifest paths inside `paused/<id>/`.
const PAUSED: &str = "paused";

/// Identity and version of a described file: device, inode, length, mtime.
type Stamp = (u64, u64, u64, SystemTime);

fn stamp(path: &Path) -> std::io::Result<Stamp> {
	let meta = fs::metadata(path)?;
	Ok((meta.dev(), meta.ino(), meta.len(), meta.modified()?))
}

/// A computed handover manifest plus the [`Stamp`] of every file it lists
/// when it was described: a file changed since (metrics pruning) makes it
/// stale. Pauses drop it.
#[derive(Clone)]
pub(super) struct CachedManifest {
	manifest: Arc<HandoverManifest>,
	stamps:   Vec<Stamp>,
}

impl CachedManifest {
	/// Describes the committed paused state of `paths` (blocking): every
	/// regular file under `paused/<id>/` but the commit marker and temporary
	/// files, plus the [`LOGS`] present. A file that is a hard link of a file
	/// of `build` takes that file's manifest entry and names it as its
	/// `build_file`; every other file is hashed.
	fn describe(
		paths: &SandboxPaths,
		build_id: &str,
		build: Option<&LocalBuild>,
	) -> anyhow::Result<Self> {
		let mut linked = HashMap::new();
		if let Some(build) = build {
			for file in &build.files {
				if let Ok(meta) = fs::metadata(build.dir.join(&file.path))
					&& meta.len() == file.size
				{
					linked.insert((meta.dev(), meta.ino()), file);
				}
			}
		}
		let mut sources: Vec<(String, &Path, String)> = list_paused(&paths.paused)?
			.into_iter()
			.map(|rel| (format!("{PAUSED}/{rel}"), paths.paused.as_path(), rel))
			.collect();
		for name in LOGS {
			if paths.dir.join(name).is_file() {
				sources.push((name.to_owned(), paths.dir.as_path(), name.to_owned()));
			}
		}
		let mut files = Vec::with_capacity(sources.len());
		let mut stamps = Vec::with_capacity(sources.len());
		for (path, dir, rel) in sources {
			// Stamped first: a change while hashing makes the cache stale.
			let version = stamp(&dir.join(&rel)).with_context(|| format!("reading {path}"))?;
			let file = match linked.get(&(version.0, version.1)) {
				Some(&build_file) => HandoverFile {
					file:       ArtifactFile { path, ..build_file.clone() },
					build_file: Some(build_file.path.clone()),
				},
				None => HandoverFile {
					file:       ArtifactFile { path, ..sparse::describe(dir, &rel)? },
					build_file: None,
				},
			};
			files.push(file);
			stamps.push(version);
		}
		let manifest = HandoverManifest { build_id: build_id.to_owned(), files };
		Ok(Self { manifest: Arc::new(manifest), stamps })
	}

	/// Whether every listed file is still the one described.
	fn fresh(&self, paths: &SandboxPaths) -> bool {
		let mut files = self.manifest.files.iter().zip(&self.stamps);
		files.all(|(file, expected)| {
			stamp(&local_path(paths, &file.file.path)).is_ok_and(|version| version == *expected)
		})
	}
}

/// Paths below `paused` (`/`-separated, sorted) of every regular file but
/// the commit marker and temporary files (dot files, `*.tmp`).
fn list_paused(paused: &Path) -> anyhow::Result<Vec<String>> {
	let mut out = Vec::new();
	let mut stack = vec![paused.to_owned()];
	while let Some(dir) = stack.pop() {
		let entries = fs::read_dir(&dir).with_context(|| format!("listing {}", dir.display()))?;
		for entry in entries {
			let entry = entry?;
			let name = entry.file_name();
			let name = name.to_str().context("paused path is not UTF-8")?;
			if name.starts_with('.')
				|| Path::new(name)
					.extension()
					.is_some_and(|ext| ext.eq_ignore_ascii_case("tmp"))
			{
				continue;
			}
			let path = entry.path();
			let kind = entry.file_type()?;
			if kind.is_dir() {
				stack.push(path);
			} else if kind.is_file() {
				let rel = path
					.strip_prefix(paused)?
					.to_str()
					.context("paused path is not UTF-8")?;
				if rel != meta::COMMITTED {
					out.push(rel.to_owned());
				}
			} else {
				bail!("paused state contains a non-regular file {}", path.display());
			}
		}
	}
	out.sort_unstable();
	Ok(out)
}

/// The part of a manifest path below `paused/`, if it is one.
fn paused_rel(path: &str) -> Option<&str> {
	path.strip_prefix(PAUSED)?.strip_prefix('/')
}

/// Local file of a manifest path (only called for listed paths).
fn local_path(paths: &SandboxPaths, path: &str) -> PathBuf {
	match paused_rel(path) {
		Some(rel) => paths.paused.join(rel),
		None => paths.dir.join(path),
	}
}

/// A local copy of a template build whose files a handover may link: its
/// directory and manifest files.
struct LocalBuild {
	dir:   PathBuf,
	files: Vec<ArtifactFile>,
}

impl LocalBuild {
	/// The local build file `file` names as its `build_file` (only files of
	/// the local manifest), when it holds the same content. Copies of a build
	/// differ in hole layout between nodes and a regenerated snapshot differs
	/// in content, so an unequal manifest entry is compared byte for byte
	/// ([`sparse::matches`]).
	async fn identical(&self, file: &HandoverFile) -> Option<PathBuf> {
		let name = file.build_file.as_deref()?;
		let local = self.files.iter().find(|local| local.path == name)?;
		let path = self.dir.join(&local.path);
		if (local.size, &local.sha256, &local.extents)
			== (file.file.size, &file.file.sha256, &file.file.extents)
		{
			return Some(path);
		}
		let (candidate, described) = (path.clone(), file.file.clone());
		match tokio::task::spawn_blocking(move || sparse::matches(&candidate, &described)).await {
			Ok(Ok(true)) => Some(path),
			Ok(Ok(false)) => None,
			Ok(Err(error)) => {
				tracing::warn!(file = %file.file.path, error = format!("{error:#}"), "comparing a template file failed");
				None
			},
			Err(error) => {
				tracing::warn!(file = %file.file.path, %error, "comparing a template file failed");
				None
			},
		}
	}
}

/// A peer failure during an adoption (transport, or an error the source
/// answered): 502 `unreachable` rather than `internal`.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct PeerError(String);

/// API error of a failed adoption: `unreachable` when the source failed,
/// else `internal` (digest mismatch, local I/O, a bad manifest).
fn adopt_error(id: &str, error: &anyhow::Error) -> ApiError {
	let message = format!("adopting sandbox {id}: {error:#}");
	if error.chain().any(|cause| cause.is::<PeerError>()) {
		ApiError::unreachable(message)
	} else {
		ApiError::internal(message)
	}
}

/// Scratch directory of an adoption, beside `paused/<id>/`.
fn adopt_staging(paths: &SandboxPaths) -> PathBuf {
	paths.paused.with_extension("adopt")
}

/// Removes the staging dirs of adoptions a node restart interrupted from the
/// `paused/` root.
pub(super) fn sweep_staging(paused_root: &Path) {
	let Ok(entries) = fs::read_dir(paused_root) else {
		return;
	};
	for entry in entries.filter_map(Result::ok) {
		if entry.path().extension().is_some_and(|ext| ext == "adopt") {
			remove_path(&entry.path());
		}
	}
}

/// Staging-relative destinations of `manifest`'s files. Rejects (before
/// anything is written) every path but plain relative paths (no empty, `.`
/// or `..` segments) below `paused/` and the [`LOGS`], the commit marker,
/// and duplicates.
fn destinations(manifest: &HandoverManifest) -> anyhow::Result<Vec<PathBuf>> {
	let mut seen = HashSet::new();
	manifest
		.files
		.iter()
		.map(|file| {
			let path = file.file.path.as_str();
			let plain = path
				.split('/')
				.all(|segment| !matches!(segment, "" | "." | ".."));
			let allowed = match paused_rel(path) {
				Some(rel) => rel != meta::COMMITTED,
				None => LOGS.contains(&path),
			};
			if !plain || !allowed || !seen.insert(path) {
				bail!("handover path {path:?} is outside the paused sandbox");
			}
			Ok(PathBuf::from(path))
		})
		.collect()
}

/// What an adoption transferred.
#[derive(Debug)]
struct Pulled {
	/// Extent bytes fetched from the source.
	bytes:  u64,
	/// Files hard-linked from the local build instead.
	linked: usize,
}

/// Pulls `manifest`'s files into a private staging dir beside
/// `paths.paused` (hard links of identical `build` files, the rest through
/// `open`, digest-verified), then installs them: `paused/…` as
/// `paths.paused`, the logs into a fresh `paths.dir` with `meta`, and
/// commits the paused state. Any failure removes everything it created
/// (staging, sandbox dir, paused dir).
async fn install<F, Fut>(
	paths: &SandboxPaths,
	manifest: &HandoverManifest,
	build: Option<&LocalBuild>,
	meta: &SandboxMeta,
	open: F,
) -> anyhow::Result<Pulled>
where
	F: Fn(&str) -> Fut,
	Fut: Future<Output = anyhow::Result<ByteStream>>,
{
	let targets = destinations(manifest)?;
	let staging = adopt_staging(paths);
	remove_path(&staging);
	let result = async {
		private_dir(&staging)?;
		let mut pulled = Pulled { bytes: 0, linked: 0 };
		for (file, target) in manifest.files.iter().zip(&targets) {
			let dest = staging.join(target);
			if let Some(parent) = dest.parent() {
				tokio::fs::create_dir_all(parent).await?;
			}
			let local = match build {
				Some(build) => build.identical(file).await,
				None => None,
			};
			if let Some(local) = local {
				match tokio::fs::hard_link(&local, &dest).await {
					Ok(()) => {
						pulled.linked += 1;
						continue;
					},
					Err(error) => tracing::warn!(
						file = %file.file.path,
						%error,
						"linking a template file failed; fetching it"
					),
				}
			}
			let stream = open(&file.file.path).await?;
			sparse::write_extents(stream, &file.file, &dest)
				.await
				.with_context(|| format!("fetching {}", file.file.path))?;
			pulled.bytes += file
				.file
				.extents
				.iter()
				.map(|extent| extent[1])
				.sum::<u64>();
		}
		let (paths, staging, meta) = (paths.clone(), staging.clone(), meta.clone());
		tokio::task::spawn_blocking(move || place(&paths, &staging, &meta)).await??;
		anyhow::Ok(pulled)
	}
	.await;
	if result.is_err() {
		let paths = paths.clone();
		let _ = tokio::task::spawn_blocking(move || {
			remove_path(&staging);
			remove_path(&paths.paused);
			remove_path(&paths.dir);
		})
		.await;
	}
	result
}

/// Moves a complete staging dir into place (blocking): a fresh sandbox dir
/// with `meta` (so a crash before the commit drops the pause at recovery),
/// the paused state, the logs, and finally the commit marker.
fn place(paths: &SandboxPaths, staging: &Path, meta: &SandboxMeta) -> anyhow::Result<()> {
	let sync_dir = |dir: &Path| fs::File::open(dir)?.sync_all();
	let staged = staging.join(PAUSED);
	// Directory entries of the fetched and linked files (`commit_paused`
	// syncs `paused/<id>/` and `base/` only).
	let mut stack = vec![staged.clone()];
	while let Some(dir) = stack.pop() {
		for entry in fs::read_dir(&dir)? {
			let entry = entry?;
			if entry.file_type()?.is_dir() {
				stack.push(entry.path());
			}
		}
		sync_dir(&dir)?;
	}
	remove_path(&paths.dir);
	remove_path(&paths.paused);
	remove_path(&upload_staging(paths));
	private_dir(&paths.dir)?;
	meta::save(paths, meta)?;
	fs::rename(&staged, &paths.paused)?;
	for name in LOGS {
		let log = staging.join(name);
		if log.is_file() {
			fs::rename(&log, paths.dir.join(name))?;
		}
	}
	sync_dir(&paths.dir)?;
	if let Some(parent) = paths.dir.parent() {
		sync_dir(parent)?;
	}
	remove_path(staging);
	commit_paused(paths).context("committing the adopted paused state")
}

impl SandboxManager {
	/// `GET /sandboxes/{id}/handover`: the committed paused state as a peer
	/// pulls it. Waits for an in-flight pause commit; 409 unless paused and
	/// committed. Cached until the paused state or a listed file changes.
	pub async fn handover_manifest(&self, id: &str) -> Result<Arc<HandoverManifest>, ApiError> {
		let entry = self.entry(id)?;
		let _ops = entry.ops.lock().await;
		let build_id = {
			let state = entry.state.lock();
			if state.live.is_some() || state.meta.state != SandboxState::Paused {
				return Err(ApiError::conflict(format!("sandbox {id} is not paused")));
			}
			state.meta.build_id.clone()
		};
		if !entry.paths.paused_resumable() {
			return Err(ApiError::conflict(format!("paused state of {id} is not committed")));
		}
		let cached = entry.handover.lock().clone();
		if let Some(cached) = cached
			&& cached.fresh(&entry.paths)
		{
			return Ok(cached.manifest);
		}
		let build = self.local_build(&build_id).await;
		let paths = entry.paths.clone();
		let cached = tokio::task::spawn_blocking(move || {
			CachedManifest::describe(&paths, &build_id, build.as_ref())
		})
		.await
		.map_err(ApiError::internal)?
		.map_err(ApiError::internal)?;
		let manifest = Arc::clone(&cached.manifest);
		*entry.handover.lock() = Some(cached);
		Ok(manifest)
	}

	/// `GET /sandboxes/{id}/handover/{path}`: the local file and data extents
	/// of a [`Self::handover_manifest`] entry; any other path is a 404.
	pub async fn handover_file(
		&self,
		id: &str,
		path: &str,
	) -> Result<(PathBuf, Vec<[u64; 2]>), ApiError> {
		let manifest = self.handover_manifest(id).await?;
		let file = manifest
			.files
			.iter()
			.find(|file| file.file.path == path)
			.ok_or_else(|| ApiError::not_found(format!("{path} is not handover state of {id}")))?;
		Ok((local_path(&self.paths(id), path), file.file.extents.clone()))
	}

	/// This node's copy of `build_id` (directory and manifest), if any.
	async fn local_build(&self, build_id: &str) -> Option<LocalBuild> {
		let artifacts = self.builder.artifacts();
		match artifacts.manifest(build_id).await {
			Ok(Some(manifest)) => {
				Some(LocalBuild { dir: artifacts.build_dir(build_id).ok()?, files: manifest.files })
			},
			Ok(None) => None,
			Err(error) => {
				tracing::warn!(
					build_id,
					error = format!("{error:#}"),
					"describing a local build failed"
				);
				None
			},
		}
	}

	/// `POST /sandboxes/{id}/adopt`: pulls the committed paused state the
	/// node at `source` (cluster URL) holds and keeps it paused here, from
	/// `meta` (state paused). Files of the template build are hard-linked
	/// from this node's copy (fetched from `sources`, cluster URLs, if
	/// absent) when identical; everything else is transferred. Never uploads
	/// to the object store. 409 when this node already has the sandbox; any
	/// failure leaves nothing behind.
	pub async fn adopt_paused(
		self: &Arc<Self>,
		id: &str,
		mut meta: SandboxMeta,
		source: &str,
		sources: &[String],
	) -> Result<SandboxInfo, ApiError> {
		validate_id(id)?;
		if meta.sandbox_id != id || meta.state != SandboxState::Paused {
			return Err(ApiError::invalid("adopted metadata does not describe this paused sandbox"));
		}
		meta.pid = None;
		meta.tap = None;
		meta.ended_at_ms = None;
		meta.gpus.clear();
		if self.sandboxes.lock().contains_key(id) || !self.pending.lock().insert(id.to_owned()) {
			return Err(ApiError::conflict(format!("sandbox {id} already exists")));
		}
		let _pending = Pending { manager: self, id: id.to_owned() };
		let started = std::time::Instant::now();
		let client = self
			.builder
			.artifacts()
			.peer(source)
			.map_err(|error| ApiError::unreachable(error.to_string()))?;
		let manifest = client.handover_manifest(id).await.map_err(|error| {
			ApiError::unreachable(format!("handover manifest of {id} from {source}: {error}"))
		})?;
		let build = if manifest.files.iter().any(|file| file.build_file.is_some()) {
			if let Err(error) = self.builder.ensure(&manifest.build_id, sources).await {
				tracing::warn!(
					sandbox = id,
					build_id = %manifest.build_id,
					error = format!("{error:#}"),
					"template build unavailable; fetching its files from the source"
				);
			}
			self.local_build(&manifest.build_id).await
		} else {
			None
		};
		let paths = self.paths(id);
		let pulled = install(&paths, &manifest, build.as_ref(), &meta, |path| {
			let (client, id, path) = (client.clone(), id.to_owned(), path.to_owned());
			async move {
				let response = client
					.handover_extents(&id, &path)
					.await
					.map_err(|error| PeerError(format!("fetching {path}: {error}")))?;
				let stream: ByteStream = Box::pin(
					response
						.bytes_stream()
						.map_err(|error| anyhow::Error::new(PeerError(error.to_string()))),
				);
				anyhow::Ok(stream)
			}
		})
		.await
		.map_err(|error| adopt_error(id, &error))?;
		tracing::info!(
			sandbox = id,
			source,
			bytes = pulled.bytes,
			linked = pulled.linked,
			secs = started.elapsed().as_secs_f64(),
			"adopted a paused sandbox from its holder"
		);
		let info = meta.info();
		let entry = Arc::new(Entry {
			paths,
			ops: Arc::default(),
			state: Mutex::new(EntryState { meta, live: None }),
			upload: Mutex::default(),
			handover: Mutex::default(),
		});
		self.sandboxes.lock().insert(id.to_owned(), entry);
		Ok(info)
	}

	/// `DELETE /sandboxes/{id}/paused`: deletes this node's paused copy
	/// (paused dir, upload staging, lazy files) and forgets the sandbox. The
	/// KV records and the object store are left alone; console and metrics
	/// stay for retention like after a kill. 409 while running.
	pub async fn drop_paused(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
		let entry = self.entry(id)?;
		let ops = Arc::clone(&entry.ops).lock_owned().await;
		{
			let state = entry.state.lock();
			if state.live.is_some() || state.meta.state != SandboxState::Paused {
				return Err(ApiError::conflict(format!("sandbox {id} is not paused")));
			}
		}
		self.forget(id, &entry);
		// The upload reads the paused files.
		Self::join_upload(&entry).await;
		let (manager, id) = (Arc::clone(self), id.to_owned());
		tokio::task::spawn_blocking(move || {
			if let Err(error) = lazy_resume::remove_files(&entry.paths.dir) {
				tracing::warn!(sandbox = %id, %error, "removing lazy resume files failed");
			}
			manager.finish_local(&id, &entry);
			drop(ops);
		})
		.await
		.map_err(ApiError::internal)
	}
}

#[cfg(test)]
mod tests {
	use bytes::Bytes;

	use super::*;
	use crate::api::NetworkPolicy;

	const ID: &str = "ihandover";

	fn sandbox_meta() -> SandboxMeta {
		SandboxMeta {
			sandbox_id:        ID.into(),
			team_id:           "team".into(),
			template_id:       "tpl".into(),
			build_id:          "b1".into(),
			state:             SandboxState::Paused,
			vcpus:             1,
			memory_mb:         256,
			disk_mb:           512,
			started_at_ms:     1,
			end_at_ms:         2,
			envd_version:      String::new(),
			envd_access_token: String::new(),
			network:           NetworkPolicy::default(),
			volume_mounts:     Vec::new(),
			pid:               None,
			tap:               None,
			ended_at_ms:       None,
			gpus:              Vec::new(),
		}
	}

	fn write(path: &Path, bytes: &[u8]) {
		fs::create_dir_all(path.parent().unwrap()).unwrap();
		fs::write(path, bytes).unwrap();
	}

	fn ino(path: &Path) -> u64 {
		fs::metadata(path).unwrap().ino()
	}

	/// A template build under `data/builds/b1/` with its manifest files.
	fn build(data: &Path, memory: &[u8]) -> LocalBuild {
		let dir = data.join("builds/b1");
		write(&dir.join("rootfs.ext4"), b"raw template disk");
		write(&dir.join("meta.json"), b"{}");
		write(&dir.join("snapshot/current-generation"), b"1");
		write(&dir.join("snapshot/vmstate.1.bin"), b"template vmstate");
		write(&dir.join("snapshot/memory.1.bin"), memory);
		let files = [
			"meta.json",
			"rootfs.ext4",
			"snapshot/current-generation",
			"snapshot/memory.1.bin",
			"snapshot/vmstate.1.bin",
		]
		.map(|rel| sparse::describe(&dir, rel).unwrap())
		.to_vec();
		LocalBuild { dir, files }
	}

	/// A committed delta pause of [`ID`] forked from `build`: `base/` and the
	/// disk base are hard links of the build's files.
	fn paused(data: &Path, build: &LocalBuild) -> SandboxPaths {
		let paths = SandboxPaths::new(data, ID);
		let base = paths.paused_base();
		fs::create_dir_all(&base).unwrap();
		for name in ["current-generation", "vmstate.1.bin", "memory.1.bin"] {
			fs::hard_link(build.dir.join("snapshot").join(name), base.join(name)).unwrap();
		}
		write(&paths.paused_snapshot().join("current-generation"), b"2");
		write(&paths.paused_snapshot().join("vmstate.2.bin"), b"sandbox vmstate");
		write(&paths.paused_snapshot().join("memory.2.bin"), b"dirty pages");
		write(&paths.paused_snapshot().join(".memory.2.bin.0123.part"), b"partial");
		write(&paths.paused_rootfs(), b"overlay clusters");
		fs::hard_link(build.dir.join("rootfs.ext4"), paths.paused.join("rootfs.img.base")).unwrap();
		write(&paths.console(), b"[    0.000000] booted\n");
		write(&paths.metrics(), b"{\"ts\":1}\n");
		commit_paused(&paths).unwrap();
		paths
	}

	/// `open` of an adoption reading the source's files directly.
	fn source_open(
		paths: &SandboxPaths,
		manifest: &HandoverManifest,
	) -> impl Fn(&str) -> std::future::Ready<anyhow::Result<ByteStream>> {
		let (paths, manifest) = (paths.clone(), manifest.clone());
		move |path: &str| {
			let file = manifest
				.files
				.iter()
				.find(|file| file.file.path == path)
				.unwrap();
			let stream = sparse::read_extents(local_path(&paths, path), file.file.extents.clone())
				.map_err(anyhow::Error::from);
			std::future::ready(Ok(Box::pin(stream) as ByteStream))
		}
	}

	/// Template files linked into the paused state are described by the
	/// build's manifest entries and named as `build_file`; everything else
	/// is hashed; the commit marker and temporary files are not listed.
	#[test]
	fn manifest_marks_template_links_and_lists_paused_state_and_logs() {
		let data = tempfile::tempdir().unwrap();
		let build = build(data.path(), b"template memory");
		let paths = paused(data.path(), &build);
		let cached = CachedManifest::describe(&paths, "b1", Some(&build)).unwrap();
		let manifest = &cached.manifest;
		assert_eq!(manifest.build_id, "b1");
		let listed: Vec<&str> = manifest
			.files
			.iter()
			.map(|file| file.file.path.as_str())
			.collect();
		assert_eq!(listed, [
			"paused/base/current-generation",
			"paused/base/memory.1.bin",
			"paused/base/vmstate.1.bin",
			"paused/rootfs.img",
			"paused/rootfs.img.base",
			"paused/snapshot/current-generation",
			"paused/snapshot/memory.2.bin",
			"paused/snapshot/vmstate.2.bin",
			"console.log",
			"metrics.jsonl",
		]);
		for file in &manifest.files {
			let expected = match file.file.path.strip_prefix("paused/base/") {
				Some(name) => Some(format!("snapshot/{name}")),
				None if file.file.path == "paused/rootfs.img.base" => Some("rootfs.ext4".to_owned()),
				None => None,
			};
			assert_eq!(file.build_file, expected, "{}", file.file.path);
			let described = if let Some(name) = &expected {
				build
					.files
					.iter()
					.find(|f| &f.path == name)
					.unwrap()
					.clone()
			} else {
				let local = local_path(&paths, &file.file.path);
				let name = local.file_name().unwrap().to_str().unwrap();
				sparse::describe(local.parent().unwrap(), name).unwrap()
			};
			assert_eq!(
				(file.file.size, &file.file.sha256, &file.file.extents),
				(described.size, &described.sha256, &described.extents),
				"{}",
				file.file.path
			);
		}
		assert!(cached.fresh(&paths));
		write(&paths.metrics(), b"{\"ts\":1}\n{\"ts\":2}\n");
		assert!(!cached.fresh(&paths), "a changed file invalidates the manifest");
	}

	/// Template files the target's build holds identically are hard-linked;
	/// one whose local build entry differs is fetched; the result is a
	/// committed pause with the logs and metadata in place.
	#[tokio::test]
	async fn adoption_links_identical_template_files_and_fetches_the_rest() {
		let (source, target) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
		let source_build = build(source.path(), b"template memory");
		let source_paths = paused(source.path(), &source_build);
		let manifest = CachedManifest::describe(&source_paths, "b1", Some(&source_build))
			.unwrap()
			.manifest;
		// The target regenerated its template snapshot: different memory.
		let target_build = build(target.path(), b"regenerated memory");
		let paths = SandboxPaths::new(target.path(), ID);
		let pulled = install(
			&paths,
			&manifest,
			Some(&target_build),
			&sandbox_meta(),
			source_open(&source_paths, &manifest),
		)
		.await
		.unwrap();

		assert_eq!(pulled.linked, 3, "rootfs base, base/vmstate, base/current-generation");
		assert!(paths.paused_resumable());
		assert_eq!(
			ino(&paths.paused.join("rootfs.img.base")),
			ino(&target_build.dir.join("rootfs.ext4"))
		);
		assert_eq!(
			ino(&paths.paused_base().join("vmstate.1.bin")),
			ino(&target_build.dir.join("snapshot/vmstate.1.bin"))
		);
		let memory = paths.paused_base().join("memory.1.bin");
		assert_ne!(ino(&memory), ino(&target_build.dir.join("snapshot/memory.1.bin")));
		assert_eq!(fs::read(&memory).unwrap(), b"template memory");
		for (path, bytes) in [
			(paths.paused_rootfs(), &b"overlay clusters"[..]),
			(paths.paused_snapshot().join("memory.2.bin"), b"dirty pages"),
			(paths.console(), b"[    0.000000] booted\n"),
			(paths.metrics(), b"{\"ts\":1}\n"),
		] {
			assert_eq!(fs::read(&path).unwrap(), bytes, "{}", path.display());
		}
		assert!(
			!paths
				.paused_snapshot()
				.join(".memory.2.bin.0123.part")
				.exists()
		);
		assert_eq!(meta::load(&paths.meta()), Some(sandbox_meta()));
		assert!(!adopt_staging(&paths).exists());
	}

	/// A corrupted transfer fails digest verification (`internal`), a broken
	/// source fails as `unreachable`; neither leaves state behind.
	#[tokio::test]
	async fn failed_transfers_leave_nothing_behind() {
		let (source, target) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
		let source_build = build(source.path(), b"template memory");
		let source_paths = paused(source.path(), &source_build);
		let manifest = CachedManifest::describe(&source_paths, "b1", None)
			.unwrap()
			.manifest;
		let paths = SandboxPaths::new(target.path(), ID);

		let corrupt = |path: &str| {
			let file = manifest
				.files
				.iter()
				.find(|file| file.file.path == path)
				.unwrap();
			let mut bytes = Vec::new();
			let local = fs::read(local_path(&source_paths, path)).unwrap();
			for &[offset, len] in &file.file.extents {
				let start = usize::try_from(offset).unwrap();
				bytes.extend_from_slice(&local[start..start + usize::try_from(len).unwrap()]);
			}
			if path == "paused/snapshot/memory.2.bin" {
				bytes[0] ^= 1;
			}
			let stream: ByteStream = Box::pin(futures::stream::iter([anyhow::Ok(Bytes::from(bytes))]));
			std::future::ready(anyhow::Ok(stream))
		};
		let error = install(&paths, &manifest, None, &sandbox_meta(), corrupt)
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("digest mismatch"), "{error:#}");
		assert_eq!(adopt_error(ID, &error).code, "internal");
		assert!(!paths.paused.exists() && !paths.dir.exists() && !adopt_staging(&paths).exists());

		let broken = |_: &str| {
			std::future::ready(Err::<ByteStream, _>(anyhow::Error::new(PeerError("reset".into()))))
		};
		let error = install(&paths, &manifest, None, &sandbox_meta(), broken)
			.await
			.unwrap_err();
		assert_eq!(adopt_error(ID, &error).code, "unreachable");
		assert!(!paths.paused.exists() && !paths.dir.exists() && !adopt_staging(&paths).exists());
	}

	/// Manifest paths that leave the paused state (absolute, `..`, other
	/// sandbox files, the commit marker) are rejected before anything is
	/// written or fetched.
	#[tokio::test]
	async fn paths_escaping_the_sandbox_are_rejected() {
		let target = tempfile::tempdir().unwrap();
		let paths = SandboxPaths::new(target.path(), ID);
		for bad in [
			"/etc/passwd",
			"../escape",
			"paused/../../escape",
			"paused/./rootfs.img",
			"paused",
			"paused/committed",
			"meta.json",
			"rootfs.img",
		] {
			let file = ArtifactFile {
				path:    bad.to_owned(),
				size:    0,
				sha256:  String::new(),
				extents: Vec::new(),
			};
			let manifest = HandoverManifest {
				build_id: "b1".into(),
				files:    vec![HandoverFile { file, build_file: None }],
			};
			let open = |_: &str| -> std::future::Ready<anyhow::Result<ByteStream>> {
				panic!("nothing is fetched for a rejected manifest")
			};
			let error = install(&paths, &manifest, None, &sandbox_meta(), open)
				.await
				.unwrap_err();
			assert!(format!("{error}").contains("outside the paused sandbox"), "{bad}: {error}");
			assert!(!adopt_staging(&paths).exists() && !paths.dir.exists(), "{bad}");
		}
	}
}
