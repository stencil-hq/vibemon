//! Portable paused sandboxes in the operator object store (ARCHITECTURE.md
//! §8.2, "Portable pauses").
//!
//! After a local pause the holder uploads one *generation* of the paused
//! state under `<store>/paused/<sandboxID>/<uuid>/`:
//!
//! | Object | Content |
//! |---|---|
//! | `meta.json` | [`SandboxMeta`] without the envd access token |
//! | `boot-init.json` | the build's boot init (JSON string; cold-boot fallback) |
//! | `rootfs.blk`, `rootfs.blk.json` | the disk as a block image ([`crate::block_image`]), dense |
//! | `snapshot/…` | the paused VMM snapshot, packed sparse extents |
//! | `base/…` | the fork RAM base of a delta snapshot, unless it is the build's template snapshot ([`PausedManifest::ram_base_build`]) |
//! | `manifest.json` | [`PausedManifest`], written last |
//!
//! "Packed sparse" objects hold only the data extents of the file, in order
//! (artifact format, §8.5). The KV record [`Remote`] (`paused/<id>/remote`)
//! points at the manifest; it is published by the sandbox manager only after
//! the manifest is durable.

use std::{
	collections::BTreeSet,
	fs,
	path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
/// Cooperative cancellation of an upload.
pub use tokio_util::sync::CancellationToken as Cancellation;
use vmon_cloud::s3::{PutOptions, S3Client, S3Config};

use crate::{
	api::ArtifactFile,
	artifacts::{
		HostIdentity,
		sparse::{self, ExtentHasher},
	},
	block_image,
	sandbox::SandboxMeta,
};

/// Node feature advertised in gossip by nodes with an object store.
pub const FEATURE: &str = "object-store";
/// Largest accepted manifest.
const MANIFEST_LIMIT: usize = 8 << 20;
/// Snapshot manifest selecting the published generation of a VMM snapshot.
const CURRENT_GENERATION: &str = "current-generation";

/// Cluster KV record of a complete remote paused generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct Remote {
	/// Store root (`--object-store` URL) without the generation path.
	pub url:          String,
	/// Relative key of the generation's manifest.
	pub manifest:     String,
	/// Guest architecture required for memory restore.
	pub arch:         String,
	/// Hypervisor backend required for memory restore.
	pub backend:      String,
	/// CPU baseline required for memory restore.
	pub cpu_baseline: String,
	/// Transferred bytes of every object except the manifest.
	pub bytes:        u64,
}

/// `manifest.json` of a paused generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct PausedManifest {
	pub sandbox_id:     String,
	/// Template build of the sandbox.
	pub build_id:       String,
	pub arch:           String,
	pub backend:        String,
	pub cpu_baseline:   String,
	/// A memory snapshot (`snapshot/`) is included.
	pub has_snapshot:   bool,
	/// The delta snapshot's RAM base is this build's template snapshot: the
	/// destination links it from its local artifacts instead of `base/`.
	pub ram_base_build: Option<String>,
	/// Guest-visible disk size.
	pub disk_size:      u64,
	/// Sum of the transferred extent bytes of `files`.
	pub bytes:          u64,
	/// Every object of the generation except the manifest.
	pub files:          Vec<ArtifactFile>,
}

/// Cluster KV key of a sandbox's portable paused generation.
pub fn key(id: &str) -> String {
	format!("paused/{id}/remote")
}

/// Cluster KV key naming the node allowed to publish a paused generation.
pub fn owner_key(id: &str) -> String {
	format!("paused/{id}/owner")
}

/// Cluster KV key fencing publication for a killed sandbox (permanent).
pub fn deleted_key(id: &str) -> String {
	format!("paused/{id}/deleted")
}

/// What one upload reads.
pub struct UploadInput<'a> {
	pub id:                &'a str,
	/// `paused/<id>/` (quiescent while the upload runs).
	pub paused_dir:        &'a Path,
	/// Scratch directory on the same filesystem; replaced, removed after.
	pub staging:           &'a Path,
	/// Sandbox metadata; the envd token is dropped before upload.
	pub meta:              &'a SandboxMeta,
	pub boot_init:         &'a str,
	pub host:              &'a HostIdentity,
	/// The local template snapshot of `meta.build_id`, if any (RAM base
	/// identity check).
	pub template_snapshot: Option<&'a Path>,
}

/// S3-backed generation store.
#[derive(Clone, Debug)]
pub struct Store {
	s3: S3Client,
}

impl Store {
	/// A store over the operator object store client.
	pub const fn new(s3: S3Client) -> Self {
		Self { s3 }
	}

	/// The store holding `remote`: `configured` when it is the same store
	/// root, otherwise a client for `remote.url` from the environment.
	pub async fn for_remote(configured: Option<&S3Client>, remote: &Remote) -> Result<Self> {
		if let Some(s3) = configured
			&& s3.location().url() == remote.url
		{
			return Ok(Self::new(s3.clone()));
		}
		let s3 = S3Client::connect(S3Config::from_env(&remote.url))
			.await
			.with_context(|| format!("opening paused-state store {}", remote.url))?;
		Ok(Self::new(s3))
	}

	/// The underlying client (store root).
	pub const fn s3(&self) -> &S3Client {
		&self.s3
	}

	/// Uploads one generation; returns its record. Objects of a failed or
	/// cancelled attempt are removed before returning; the staging directory
	/// is always removed.
	pub async fn upload(&self, input: &UploadInput<'_>, cancel: &Cancellation) -> Result<Remote> {
		validate_component(input.id)?;
		check(cancel)?;
		let staging = input.staging.to_owned();
		let result = self.upload_staged(input, cancel).await;
		let _ = tokio::task::spawn_blocking(move || fs::remove_dir_all(staging)).await;
		result
	}

	async fn upload_staged(&self, input: &UploadInput<'_>, cancel: &Cancellation) -> Result<Remote> {
		let prepared = {
			let paused = input.paused_dir.to_owned();
			let staging = input.staging.to_owned();
			let mut meta = input.meta.clone();
			meta.envd_access_token.clear();
			let boot_init = input.boot_init.to_owned();
			let template = input.template_snapshot.map(Path::to_owned);
			let cancel = cancel.clone();
			tokio::task::spawn_blocking(move || {
				prepare(&paused, &staging, &meta, &boot_init, template.as_deref(), &cancel)
			})
			.await??
		};
		let (files, ram_base_build, disk_size) = prepared;
		let bytes = files.iter().try_fold(0u64, |sum, (file, _)| {
			sum.checked_add(extent_bytes(file)?)
				.context("paused byte count overflow")
		})?;
		let manifest = PausedManifest {
			sandbox_id: input.id.to_owned(),
			build_id: input.meta.build_id.clone(),
			arch: input.host.arch.clone(),
			backend: input.host.backend.clone(),
			cpu_baseline: input.host.cpu_baseline.clone(),
			has_snapshot: files
				.iter()
				.any(|(file, _)| file.path.starts_with("snapshot/")),
			ram_base_build: ram_base_build.then(|| input.meta.build_id.clone()),
			disk_size,
			bytes,
			files: files.iter().map(|(file, _)| file.clone()).collect(),
		};
		validate_manifest(&manifest)?;
		let generation = format!("paused/{}/{}", input.id, uuid::Uuid::new_v4());
		let manifest_key = format!("{generation}/manifest.json");
		let uploaded = async {
			for (file, path) in &files {
				check(cancel)?;
				let body = sparse::read_extents(path.clone(), file.extents.clone());
				let key = format!("{generation}/{}", file.path);
				let options = PutOptions::default();
				let size = extent_bytes(file)?;
				tokio::select! {
					result = self.s3.upload_stream(&key, size, body, &options) => {
						result.with_context(|| format!("uploading {key}"))?;
					},
					() = cancel.cancelled() => bail!("paused upload cancelled"),
				}
			}
			check(cancel)?;
			let body = serde_json::to_vec(&manifest)?;
			if body.len() > MANIFEST_LIMIT {
				bail!("paused manifest exceeds {MANIFEST_LIMIT} bytes");
			}
			self
				.s3
				.put(&manifest_key, Bytes::from(body), &PutOptions::default())
				.await
				.with_context(|| format!("uploading {manifest_key}"))?;
			check(cancel)
		}
		.await;
		if let Err(error) = uploaded {
			// Not raced against cancellation: the owner may only publish or start
			// another operation once this attempt's objects are gone.
			return match self.s3.delete_prefix(&format!("{generation}/")).await {
				Ok(_) => Err(error),
				Err(cleanup) => Err(error.context(format!("removing the failed upload: {cleanup}"))),
			};
		}
		Ok(Remote {
			url: self.s3.location().url().to_owned(),
			manifest: manifest_key,
			arch: manifest.arch,
			backend: manifest.backend,
			cpu_baseline: manifest.cpu_baseline,
			bytes,
		})
	}

	/// Relative key prefix (`paused/<id>/<uuid>`) of the generation of
	/// `remote`, validated.
	pub fn generation_prefix(&self, remote: &Remote) -> Result<String> {
		if remote.url != self.s3.location().url() {
			bail!("paused record names store {}, not {}", remote.url, self.s3.location().url());
		}
		generation_prefix(remote)
	}

	/// Reads and validates the manifest of `remote`.
	pub async fn manifest(&self, remote: &Remote) -> Result<PausedManifest> {
		let generation = self.generation_prefix(remote)?;
		let body = self
			.s3
			.get_bytes(&remote.manifest, MANIFEST_LIMIT)
			.await
			.with_context(|| format!("reading {}", remote.manifest))?;
		let manifest: PausedManifest = serde_json::from_slice(&body)?;
		validate_manifest(&manifest)?;
		if generation.split('/').nth(1) != Some(manifest.sandbox_id.as_str())
			|| manifest.arch != remote.arch
			|| manifest.backend != remote.backend
			|| manifest.cpu_baseline != remote.cpu_baseline
			|| manifest.bytes != remote.bytes
		{
			bail!("paused manifest does not match its record");
		}
		Ok(manifest)
	}

	/// Downloads and verifies the files of `remote` selected by `filter`
	/// (generation-relative paths) into a staging directory beside `dest`,
	/// decodes a fetched block image into `rootfs.img`, then renames the stage
	/// to `dest` (which must be absent or empty). Files stay sparse.
	pub async fn fetch(
		&self,
		remote: &Remote,
		dest: &Path,
		filter: impl Fn(&str) -> bool,
	) -> Result<PausedManifest> {
		let generation = self.generation_prefix(remote)?;
		let manifest = self.manifest(remote).await?;
		let parent = dest.parent().context("paused destination has no parent")?;
		tokio::fs::create_dir_all(parent).await?;
		let stage = parent.join(format!(
			".{}.{}.part",
			dest
				.file_name()
				.and_then(|name| name.to_str())
				.unwrap_or("paused"),
			uuid::Uuid::new_v4()
		));
		tokio::fs::create_dir(&stage).await?;
		let result = async {
			for file in manifest.files.iter().filter(|file| filter(&file.path)) {
				let target = stage.join(&file.path);
				if let Some(dir) = target.parent() {
					tokio::fs::create_dir_all(dir).await?;
				}
				let key = format!("{generation}/{}", file.path);
				let object = self
					.s3
					.get(&key)
					.await
					.with_context(|| format!("fetching {key}"))?;
				let body = object.body.map_err(anyhow::Error::from);
				sparse::write_extents(body, file, &target)
					.await
					.with_context(|| format!("installing {key}"))?;
			}
			let (image, index) = (stage.join(block_image::IMAGE), stage.join(block_image::INDEX));
			if image.is_file() && index.is_file() {
				let rootfs = stage.join("rootfs.img");
				tokio::task::spawn_blocking(move || -> Result<()> {
					block_image::decode_disk(&image, &index, &rootfs)?;
					fs::remove_file(&image)?;
					fs::remove_file(&index)?;
					Ok(())
				})
				.await??;
			}
			// An existing nonempty destination belongs to someone else: never
			// merge into it.
			if tokio::fs::try_exists(dest).await? {
				tokio::fs::remove_dir(dest)
					.await
					.context("paused destination must be absent or empty")?;
			}
			tokio::fs::rename(&stage, dest).await?;
			anyhow::Ok(())
		}
		.await;
		if result.is_err() {
			let _ = tokio::fs::remove_dir_all(&stage).await;
		}
		result.map(|()| manifest)
	}

	/// Deletes every object of the generation of `remote` (retryable).
	pub async fn delete(&self, remote: &Remote) -> Result<()> {
		let generation = self.generation_prefix(remote)?;
		self
			.s3
			.delete_prefix(&format!("{generation}/"))
			.await
			.with_context(|| format!("deleting {generation}"))?;
		Ok(())
	}
}

/// Relative key prefix (`paused/<id>/<uuid>`) of `remote`'s generation.
pub fn generation_prefix(remote: &Remote) -> Result<String> {
	let parts: Vec<_> = remote.manifest.split('/').collect();
	if parts.len() != 4 || parts[0] != "paused" || parts[3] != "manifest.json" {
		bail!("invalid paused manifest key {:?}", remote.manifest);
	}
	validate_component(parts[1])?;
	uuid::Uuid::parse_str(parts[2]).context("invalid paused generation")?;
	Ok(parts[..3].join("/"))
}

fn check(cancel: &Cancellation) -> Result<()> {
	if cancel.is_cancelled() {
		bail!("paused upload cancelled");
	}
	Ok(())
}

fn validate_component(value: &str) -> Result<()> {
	if value.is_empty()
		|| !value
			.bytes()
			.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
	{
		bail!("invalid paused path component {value:?}");
	}
	Ok(())
}

fn extent_bytes(file: &ArtifactFile) -> Result<u64> {
	file.extents.iter().try_fold(0u64, |sum, extent| {
		sum.checked_add(extent[1])
			.context("paused extent byte count overflow")
	})
}

/// Whether `path` may appear in a paused generation.
fn allowed_path(path: &str) -> bool {
	matches!(path, "meta.json" | "boot-init.json" | block_image::IMAGE | block_image::INDEX)
		|| ((path.starts_with("snapshot/") || path.starts_with("base/"))
			&& !path
				.split('/')
				.any(|part| part.is_empty() || part == "." || part == ".." || part.contains('\\')))
}

fn validate_manifest(manifest: &PausedManifest) -> Result<()> {
	validate_component(&manifest.sandbox_id)?;
	let mut paths = BTreeSet::new();
	let mut bytes = 0u64;
	for file in &manifest.files {
		if !allowed_path(&file.path)
			|| !paths.insert(file.path.as_str())
			|| file.sha256.len() != 64
			|| !file.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
		{
			bail!("invalid paused manifest file {:?}", file.path);
		}
		sparse::validate(file)?;
		bytes = bytes
			.checked_add(extent_bytes(file)?)
			.context("paused byte count overflow")?;
	}
	let has_base = paths.iter().any(|path| path.starts_with("base/"));
	if [block_image::IMAGE, block_image::INDEX, "meta.json", "boot-init.json"]
		.iter()
		.any(|required| !paths.contains(required))
		|| manifest.bytes != bytes
		|| manifest.has_snapshot != paths.iter().any(|path| path.starts_with("snapshot/"))
		|| (has_base || manifest.ram_base_build.is_some()) && !manifest.has_snapshot
		|| has_base && manifest.ram_base_build.is_some()
	{
		bail!("incomplete or inconsistent paused manifest");
	}
	Ok(())
}

/// Describes a file as one dense extent (no hole detection), hashing it.
fn dense(dir: &Path, rel: &str) -> Result<ArtifactFile> {
	use std::io::Read;
	let path = dir.join(rel);
	let mut file = fs::File::open(&path).with_context(|| format!("opening {}", path.display()))?;
	let size = file.metadata()?.len();
	let mut hasher = ExtentHasher::default();
	let extents = if size == 0 {
		Vec::new()
	} else {
		vec![[0, size]]
	};
	if size > 0 {
		hasher.extent([0, size]);
		let mut buf = vec![0; 1 << 20];
		loop {
			let read = file.read(&mut buf)?;
			if read == 0 {
				break;
			}
			hasher.update(&buf[..read]);
		}
	}
	Ok(ArtifactFile { path: rel.to_owned(), size, sha256: hasher.finish(size), extents })
}

/// Files of one snapshot directory, relative to `root`, sorted.
fn collect_dir(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
	if !fs::symlink_metadata(dir)?.file_type().is_dir() {
		bail!("{} is not a directory", dir.display());
	}
	let mut entries = fs::read_dir(dir)?.collect::<std::io::Result<Vec<_>>>()?;
	entries.sort_by_key(fs::DirEntry::file_name);
	for entry in entries {
		let kind = entry.file_type()?;
		if kind.is_dir() {
			collect_dir(root, &entry.path(), out)?;
		} else if kind.is_file() {
			out.push(
				entry
					.path()
					.strip_prefix(root)?
					.to_str()
					.context("paused path is not UTF-8")?
					.to_owned(),
			);
		} else {
			bail!("paused state contains a non-regular file {}", entry.path().display());
		}
	}
	Ok(())
}

/// Whether the RAM base `base` is (hard links of) the generation published
/// in the template snapshot `template`.
fn is_template_base(base: &Path, template: &Path) -> bool {
	#[cfg(unix)]
	{
		use std::os::unix::fs::MetadataExt;
		let Ok(generation) = fs::read_to_string(base.join(CURRENT_GENERATION)) else {
			return false;
		};
		let generation = generation.trim();
		[
			CURRENT_GENERATION.to_owned(),
			format!("memory.{generation}.bin"),
			format!("vmstate.{generation}.bin"),
		]
		.iter()
		.all(|name| match (fs::metadata(base.join(name)), fs::metadata(template.join(name))) {
			(Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
			_ => false,
		})
	}
	#[cfg(not(unix))]
	{
		let _ = (base, template);
		false
	}
}

/// Objects to upload as `(file, local path)`, whether the RAM base is the
/// template's, and the disk size.
type Prepared = (Vec<(ArtifactFile, PathBuf)>, bool, u64);

/// Stages metadata and the block image, then describes every object to
/// upload: `(file, local path)`, whether the RAM base is the template's, and
/// the disk size. Blocking.
fn prepare(
	paused: &Path,
	staging: &Path,
	meta: &SandboxMeta,
	boot_init: &str,
	template: Option<&Path>,
	cancel: &Cancellation,
) -> Result<Prepared> {
	if boot_init.is_empty() {
		bail!("paused build has no boot init");
	}
	let _ = fs::remove_dir_all(staging);
	fs::create_dir_all(staging)?;
	fs::write(staging.join("meta.json"), serde_json::to_vec(meta)?)?;
	fs::write(staging.join("boot-init.json"), serde_json::to_vec(boot_init)?)?;
	check(cancel)?;
	let image = block_image::encode_disk(
		&paused.join("rootfs.img"),
		&staging.join(block_image::IMAGE),
		&staging.join(block_image::INDEX),
	)?;
	let mut files = Vec::new();
	for rel in ["meta.json", "boot-init.json", block_image::IMAGE, block_image::INDEX] {
		files.push((dense(staging, rel)?, staging.join(rel)));
	}
	let snapshot = paused.join("snapshot");
	let base = paused.join("base");
	let template_base = snapshot.is_dir()
		&& base.is_dir()
		&& template.is_some_and(|template| is_template_base(&base, template));
	let mut names = Vec::new();
	if snapshot.is_dir() {
		collect_dir(paused, &snapshot, &mut names)?;
		if base.is_dir() && !template_base {
			collect_dir(paused, &base, &mut names)?;
		}
	}
	for name in names {
		check(cancel)?;
		files.push((sparse::describe(paused, &name)?, paused.join(&name)));
	}
	Ok((files, template_base, image.disk_size))
}

#[cfg(test)]
mod tests;
