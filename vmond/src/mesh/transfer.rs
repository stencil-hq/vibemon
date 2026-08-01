//! Template/CAS transfer and remote-page metadata.
//!
//! Port of `python/vmon/server/templates.py` plus the CAS rules from
//! `python/vmon/cas.py`.  The route layer can serve the archive helpers under
//! `/v1/templates/{digest}` and use `pull_template*` for peer fetches.  Full
//! template pulls verify the content-addressed digest before installing and
//! indexing; metadata pulls install a lazy restore stub with
//! `remote-page.json`.

use std::{
	ffi::OsStr,
	fs,
	io::{Read, Seek, SeekFrom},
	net::ToSocketAddrs,
	path::{Path, PathBuf},
	time::{SystemTime, UNIX_EPOCH},
};

use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{bundle, replica_cache::cache_root_name};
use crate::{EngineError, Result, home::state_dir, image::cas};

const MARKER_NAME: &str = "agent-ready.json";
const ROOTFS_NAME: &str = "rootfs.img";
const GENERATION_NAME: &str = "current-generation";
const PAGE_SIZE: usize = 4096;

/// A servable template bundle: the route layer streams it with
/// [`stream_bundle`]; nothing is materialized on disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemplateBundle {
	pub dir:           PathBuf,
	pub metadata_only: bool,
	pub filename:      String,
	pub media_type:    &'static str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemotePageMetadata {
	pub peer_url: String,
	pub digest:   String,
	pub page_url: String,
}

/// Construct a streamable bundle for an actively exported replica snapshot.
///
/// Callers must retain their export guard for the duration of streaming.
pub fn snapshot_bundle_for_path(dir: PathBuf, digest: &str) -> Result<TemplateBundle> {
	validate_digest(digest)?;
	if !dir.is_dir() {
		return Err(EngineError::not_found("replica export is unavailable"));
	}
	if cas::snapshot_digest(&dir)? != digest {
		return Err(EngineError::invalid("replica export digest mismatch"));
	}
	Ok(TemplateBundle {
		dir,
		metadata_only: false,
		filename: format!("{digest}.vbundle"),
		media_type: "application/octet-stream",
	})
}

/// Compress `bundle` into `tx` chunks; run on a blocking thread. Errors are
/// surfaced through the channel so the HTTP body fails visibly.
pub fn stream_bundle(
	bundle: &TemplateBundle,
	tx: tokio::sync::mpsc::Sender<std::io::Result<bytes::Bytes>>,
) {
	let include: &dyn Fn(&Path) -> bool = if bundle.metadata_only {
		&|path| !is_memory_page(path)
	} else {
		&|_| true
	};
	let mut writer = bundle::ChannelWriter::new(tx);
	let result = bundle::write_bundle(&bundle.dir, &mut writer, include);
	writer.finish(result);
}

/// Fetch a metadata-only bundle from a peer and install a lazy remote-page
/// restore stub under `$VMON_HOME/templates/remote-*`.
///
/// Decompression and extraction overlap the download; nothing is buffered on
/// disk.
pub async fn pull_template_metadata(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	let url = format!("{}/v1/templates/{digest}/metadata", peer_url.trim_end_matches('/'));
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;
	let extract_root = temp_dir_in(&templates, ".pull-meta-")?;
	let pulled = pull_and_extract(client, &url, digest, token, extract_root.clone()).await;
	let result = match pulled {
		Ok(()) => {
			let digest = digest.to_owned();
			let peer_url = peer_url.to_owned();
			let root = extract_root.clone();
			tokio::task::spawn_blocking(move || install_lazy_template_stub(&root, &digest, &peer_url))
				.await
				.map_err(|err| {
					EngineError::engine(format!("mesh metadata install task failed: {err}"))
				})?
		},
		Err(err) => Err(err),
	};
	let _ = fs::remove_dir_all(&extract_root);
	result
}

/// Fetch, verify, install, and CAS-index a full template bundle from a peer.
/// Decompression and extraction overlap the download; nothing is buffered on
/// disk.
pub async fn pull_template(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	pull_indexed_bundle(client, peer_url, digest, token, IndexedDigest::Template).await
}

/// Fetch and CAS-index a migration checkpoint using its complete-tree digest.
pub async fn pull_checkpoint(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	pull_indexed_bundle(client, peer_url, digest, token, IndexedDigest::Checkpoint).await
}

async fn pull_indexed_bundle(
	client: &Client,
	peer_url: &str,
	digest: &str,
	token: &str,
	digest_kind: IndexedDigest,
) -> Result<PathBuf> {
	let url = format!("{}/v1/templates/{digest}", peer_url.trim_end_matches('/'));
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;
	let extract_root = temp_dir_in(&templates, ".pull-extract-")?;
	let t0 = std::time::Instant::now();
	let pulled = pull_and_extract(client, &url, digest, token, extract_root.clone()).await;
	let pull_ms = t0.elapsed().as_millis() as u64;
	let result = match pulled {
		Ok(()) => {
			let digest = digest.to_owned();
			let root = extract_root.clone();
			tokio::task::spawn_blocking(move || install_pulled_indexed(&root, &digest, digest_kind))
				.await
				.map_err(|err| {
					EngineError::engine(format!("mesh template install task failed: {err}"))
				})?
		},
		Err(err) => Err(err),
	};
	tracing::info!(
		digest,
		pull_ms,
		install_ms = t0.elapsed().as_millis() as u64 - pull_ms,
		"template pull timings"
	);
	let _ = fs::remove_dir_all(&extract_root);
	result
}

/// Fetch and install a replica checkpoint by its exact source identity.
///
/// Replica snapshots use `snapshot_digest` and a domain-separated local cache;
/// they are never CAS-indexed as image templates.
pub async fn pull_snapshot(
	client: &Client,
	peer_url: &str,
	sid: &str,
	object_key: &str,
	digest: &str,
	token: &str,
) -> Result<PathBuf> {
	validate_digest(digest)?;
	if sid.is_empty() {
		return Err(EngineError::invalid("replica snapshot sid is required"));
	}
	let mut url = reqwest::Url::parse(peer_url.trim_end_matches('/'))
		.map_err(|_| EngineError::invalid("invalid peer URL"))?;
	url.path_segments_mut()
		.map_err(|()| EngineError::invalid("peer URL cannot accept a replica path"))?
		.extend(["v1", "replicas", sid, digest]);
	url.query_pairs_mut().append_pair("object_key", object_key);
	let replicas = state_dir().join("replicas").join("objects");
	fs::create_dir_all(&replicas)?;
	let extract_root = temp_dir_in(&replicas, ".pull-snapshot-")?;
	let pulled = pull_and_extract(client, url.as_str(), digest, token, extract_root.clone()).await;
	let result = match pulled {
		Ok(()) => {
			let root = extract_root.clone();
			let sid = sid.to_owned();
			let object_key = object_key.to_owned();
			let digest = digest.to_owned();
			tokio::task::spawn_blocking(move || {
				install_pulled_snapshot(&root, &sid, &object_key, &digest)
			})
			.await
			.map_err(|error| EngineError::engine(format!("snapshot install task failed: {error}")))?
		},
		Err(error) => Err(error),
	};
	let _ = fs::remove_dir_all(&extract_root);
	result
}

/// Verify and install an extracted replica snapshot under the authoritative
/// replica-cache root without creating a template CAS pointer.
pub fn install_pulled_snapshot(
	extract_root: &Path,
	sid: &str,
	object_key: &str,
	digest: &str,
) -> Result<PathBuf> {
	validate_digest(digest)?;
	let source = extracted_template_root(extract_root)?;
	if cas::snapshot_digest(&source)? != digest {
		return Err(EngineError::invalid("pulled replica snapshot digest mismatch"));
	}
	let name = source
		.file_name()
		.and_then(|name| name.to_str())
		.filter(|name| !name.is_empty() && *name != "." && *name != "..")
		.ok_or_else(|| EngineError::invalid("replica snapshot has no valid root name"))?;
	let cache_root = state_dir()
		.join("replicas")
		.join("objects")
		.join(cache_root_name(sid, object_key, digest));
	fs::create_dir_all(&cache_root)?;
	let target = cache_root.join(name);
	if target.exists() {
		if target.is_dir() && cas::snapshot_digest(&target).ok().as_deref() == Some(digest) {
			return Ok(target);
		}
		remove_path(&target)?;
	}
	fs::rename(source, &target)?;
	Ok(target)
}

#[derive(Clone, Copy)]
enum IndexedDigest {
	Template,
	Checkpoint,
}

impl IndexedDigest {
	fn calculate(self, root: &Path) -> Result<String> {
		match self {
			Self::Template => cas::template_digest(root),
			Self::Checkpoint => cas::snapshot_digest(root),
		}
	}

	const fn mismatch(self) -> &'static str {
		match self {
			Self::Template => "pulled template digest mismatch",
			Self::Checkpoint => "pulled migration checkpoint digest mismatch",
		}
	}
}

/// Verify, install, and CAS-index an extracted template tree.
pub fn install_pulled_template(extract_root: &Path, digest: &str) -> Result<PathBuf> {
	install_pulled_indexed(extract_root, digest, IndexedDigest::Template)
}

fn install_pulled_indexed(
	extract_root: &Path,
	digest: &str,
	digest_kind: IndexedDigest,
) -> Result<PathBuf> {
	validate_digest(digest)?;
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;

	(|| {
		let source = extracted_template_root(extract_root)?;
		let actual = digest_kind.calculate(&source)?;
		if actual != digest {
			return Err(EngineError::invalid(digest_kind.mismatch()));
		}
		let target = templates.join(template_name_from_marker(&source, digest));
		if target.exists() {
			if target.is_dir() && digest_kind.calculate(&target).ok().as_deref() == Some(digest) {
				cas::index_template(&target, Some(digest))?;
				return Ok(target);
			}
			remove_path(&target)?;
		}
		fs::rename(&source, &target)?;
		cas::index_template(&target, Some(digest))?;
		Ok(target)
	})()
}

/// Install a metadata-only lazy template stub and write `remote-page.json`.
///
/// The stub records `{peer_url,digest,page_url}` and is intentionally not
/// CAS-indexed; it exists to feed `--remote-page-url` during restore.
pub fn install_lazy_template_stub(
	extract_root: &Path,
	digest: &str,
	peer_url: &str,
) -> Result<PathBuf> {
	validate_digest(digest)?;
	let templates = state_dir().join("templates");
	fs::create_dir_all(&templates)?;

	(|| {
		let source = extracted_template_root(extract_root)?;
		if marker_digest(&source).as_deref() != Some(digest) {
			return Err(EngineError::invalid("pulled template metadata digest mismatch"));
		}
		let generation = fs::read_to_string(source.join(GENERATION_NAME))?
			.trim()
			.to_owned();
		if !generation.chars().all(|ch| ch.is_ascii_digit())
			|| !source.join(format!("vmstate.{generation}.bin")).is_file()
		{
			return Err(EngineError::invalid("pulled template metadata is missing snapshot state"));
		}
		if !source.join(ROOTFS_NAME).is_file() {
			return Err(EngineError::invalid("pulled template metadata is missing rootfs.img"));
		}
		let target = templates.join(format!("remote-{}", template_name_from_marker(&source, digest)));
		if target.exists() {
			remove_path(&target)?;
		}
		let metadata = RemotePageMetadata {
			peer_url: peer_url.trim_end_matches('/').to_owned(),
			digest:   digest.to_owned(),
			page_url: remote_page_url(peer_url, digest)?,
		};
		fs::write(
			source.join("remote-page.json"),
			serde_json::to_vec_pretty(&metadata).map_err(EngineError::from)?,
		)?;
		fs::rename(&source, &target)?;
		Ok(target)
	})()
}

/// Build the peer memory-page base URL.  For plain HTTP peers, resolve the
/// hostname to an IP address like Python does so the VMM pager receives a URL
/// it can fetch without relying on guest DNS.
pub fn remote_page_url(peer_url: &str, digest: &str) -> Result<String> {
	validate_digest(digest)?;
	let parsed = reqwest::Url::parse(peer_url.trim_end_matches('/'))
		.map_err(|_| EngineError::invalid("invalid peer URL"))?;
	let mut host = parsed
		.host_str()
		.ok_or_else(|| EngineError::invalid("invalid peer URL"))?
		.to_owned();
	let port = parsed.port_or_known_default().unwrap_or(80);
	if parsed.scheme() == "http"
		&& let Ok(mut addrs) = (host.as_str(), port).to_socket_addrs()
		&& let Some(addr) = addrs.next()
	{
		host = addr.ip().to_string();
	}
	let host = if host.contains(':') {
		format!("[{host}]")
	} else {
		host
	};
	let netloc = if parsed.port().is_some() {
		format!("{host}:{port}")
	} else {
		host
	};
	Ok(format!("{}://{netloc}/v1/templates/{digest}/pages", parsed.scheme()))
}

/// Return one 4 KiB memory page from a CAS-resolved template.
pub fn template_memory_page(template_dir: &Path, page: u64) -> Result<Vec<u8>> {
	let generation = fs::read_to_string(template_dir.join(GENERATION_NAME))?
		.trim()
		.to_owned();
	if !generation.chars().all(|ch| ch.is_ascii_digit()) {
		return Err(EngineError::not_found("template has invalid snapshot generation"));
	}
	let memory = template_dir.join(format!("memory.{generation}.bin"));
	let mut file = fs::File::open(memory)?;
	file.seek(SeekFrom::Start(page.saturating_mul(PAGE_SIZE as u64)))?;
	let mut data = vec![0_u8; PAGE_SIZE];
	file.read_exact(&mut data).map_err(|_| {
		EngineError::not_found(format!("template page {page} is outside the memory image"))
	})?;
	Ok(data)
}

/// Resolve a digest into a streamable full bundle for
/// `GET /v1/templates/{digest}` route wiring.
pub fn bundle_for_digest(digest: &str) -> Result<TemplateBundle> {
	let dir = cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	Ok(TemplateBundle {
		dir,
		metadata_only: false,
		filename: format!("{digest}.vbundle"),
		media_type: "application/x-vmon-bundle",
	})
}

/// Resolve a digest into a streamable metadata-only bundle for
/// `GET /v1/templates/{digest}/metadata`.
pub fn metadata_bundle_for_digest(digest: &str) -> Result<TemplateBundle> {
	let dir = cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	Ok(TemplateBundle {
		dir,
		metadata_only: true,
		filename: format!("{digest}.metadata.vbundle"),
		media_type: "application/x-vmon-bundle",
	})
}

/// Resolve and read one memory page for `GET
/// /v1/templates/{digest}/pages/{page}`.
pub fn page_for_digest(digest: &str, page: u64) -> Result<Vec<u8>> {
	let template_dir =
		cas::lookup(digest)?.ok_or_else(|| EngineError::not_found("unknown template"))?;
	template_memory_page(&template_dir, page)
}

/// Stream a peer's bundle response straight into `extract_root`: the HTTP
/// body feeds a channel that a blocking extractor drains, so download,
/// decompression, and file writes all overlap.
async fn pull_and_extract(
	client: &Client,
	url: &str,
	digest: &str,
	token: &str,
	extract_root: PathBuf,
) -> Result<()> {
	validate_digest(digest)?;
	let mut response = client
		.get(url)
		.bearer_auth(token)
		.header("X-Vmon-Mesh-Hop", "1")
		.send()
		.await
		.map_err(|err| EngineError::engine(format!("failed to fetch template: {err}")))?;
	if response.status() == StatusCode::NOT_FOUND {
		return Err(EngineError::not_found(format!("unknown template digest {digest}")));
	}
	if let Err(err) = response.error_for_status_ref() {
		return Err(EngineError::engine(format!("failed to fetch template: {err}")));
	}
	let (tx, rx) = tokio::sync::mpsc::channel::<std::io::Result<bytes::Bytes>>(16);
	let extractor = tokio::task::spawn_blocking(move || {
		bundle::read_bundle(bundle::ChannelReader::new(rx), &extract_root)
	});
	let mut stream_error = None;
	loop {
		match response.chunk().await {
			Ok(Some(chunk)) => {
				if !chunk.is_empty() && tx.send(Ok(chunk)).await.is_err() {
					// Extractor bailed; its error is authoritative.
					break;
				}
			},
			Ok(None) => break,
			Err(err) => {
				stream_error = Some(EngineError::engine(format!("failed to stream template: {err}")));
				let _ = tx.send(Err(std::io::Error::other(err.to_string()))).await;
				break;
			},
		}
	}
	drop(tx);
	let extracted = extractor
		.await
		.map_err(|err| EngineError::engine(format!("mesh extract task failed: {err}")))?;
	match (extracted, stream_error) {
		(Err(err), _) => Err(err),
		(Ok(()), Some(err)) => Err(err),
		(Ok(()), None) => Ok(()),
	}
}

fn extracted_template_root(root: &Path) -> Result<PathBuf> {
	if root.join(MARKER_NAME).is_file() {
		return Ok(root.to_owned());
	}
	for entry in fs::read_dir(root)? {
		let path = entry?.path();
		if path.is_dir() && path.join(MARKER_NAME).is_file() {
			return Ok(path);
		}
	}
	Err(EngineError::invalid("template archive did not contain an agent-ready marker"))
}

fn template_name_from_marker(template_dir: &Path, digest: &str) -> String {
	let data = read_marker_json(template_dir).unwrap_or(Value::Null);
	let raw = data
		.get("tpl_name")
		.and_then(Value::as_str)
		.filter(|value| !value.is_empty());
	let name = raw.unwrap_or_else(|| {
		template_dir
			.file_name()
			.and_then(OsStr::to_str)
			.unwrap_or("")
	});
	let safe = Path::new(name)
		.file_name()
		.and_then(OsStr::to_str)
		.unwrap_or("");
	if safe.is_empty() || matches!(safe, "." | "..") {
		format!("tpl-{}", &digest[..12])
	} else {
		safe.to_owned()
	}
}

fn marker_digest(template_dir: &Path) -> Option<String> {
	let data = read_marker_json(template_dir).ok()?;
	let value = data.get("content_digest")?.as_str()?;
	(value.len() == 64
		&& value
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)))
	.then_some(value.to_owned())
}

fn read_marker_json(template_dir: &Path) -> Result<Value> {
	let raw = fs::read_to_string(template_dir.join(MARKER_NAME))?;
	serde_json::from_str(&raw).map_err(EngineError::from)
}

#[allow(
	clippy::case_sensitive_file_extension_comparisons,
	reason = "checkpoint page files are generated with exact lowercase .bin names"
)]
fn is_memory_page(path: &Path) -> bool {
	path
		.file_name()
		.and_then(OsStr::to_str)
		.is_some_and(|name| name.starts_with("memory.") && name.ends_with(".bin"))
}

fn remove_path(path: &Path) -> Result<()> {
	if path.is_dir() && !fs::symlink_metadata(path)?.file_type().is_symlink() {
		fs::remove_dir_all(path)?;
	} else {
		fs::remove_file(path)?;
	}
	Ok(())
}

fn validate_digest(digest: &str) -> Result<()> {
	let valid = digest.len() == 64
		&& digest
			.bytes()
			.all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
	if valid {
		Ok(())
	} else {
		Err(EngineError::invalid("template digest must be a lowercase SHA-256 hex digest"))
	}
}

fn temp_dir_in(parent: &Path, prefix: &str) -> Result<PathBuf> {
	fs::create_dir_all(parent)?;
	for _ in 0..128 {
		let path = parent.join(format!("{prefix}{}", unique_suffix()));
		match fs::create_dir(&path) {
			Ok(()) => return Ok(path),
			Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {},
			Err(err) => return Err(err.into()),
		}
	}
	Err(EngineError::engine("failed to allocate temporary directory"))
}

fn unique_suffix() -> String {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos());
	format!("{}-{nanos}", std::process::id())
}

/// Return the JSON marker value used by tests and route wiring to assert lazy
/// metadata installs without reading the file directly.
pub fn remote_page_metadata(template_dir: &Path) -> Result<RemotePageMetadata> {
	let raw = fs::read_to_string(template_dir.join("remote-page.json"))?;
	serde_json::from_str(&raw).map_err(EngineError::from)
}

/// Build the exact metadata JSON payload for a peer/digest pair.
pub fn remote_page_metadata_value(peer_url: &str, digest: &str) -> Result<Value> {
	Ok(json!({
		"peer_url": peer_url.trim_end_matches('/'),
		"digest": digest,
		"page_url": remote_page_url(peer_url, digest)?,
	}))
}
