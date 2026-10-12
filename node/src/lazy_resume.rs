//! Lazy cross-node resume of paused sandboxes (ARCHITECTURE.md §8.2).
//!
//! A paused generation in the object store ([`crate::paused_store`]) keeps the
//! rootfs as a seekable block image ([`crate::block_image`]). A node that
//! resumes a sandbox it does not hold boots it without downloading that
//! image:
//!
//! - **rootfs** (Linux and macOS): the VMM's remote block device
//!   (`--rootfs-remote-*`, [`RemoteDisk`]) fetches 1 MiB blocks by HTTP range
//!   from a pre-signed object URL pinned to the object's `ETag`, caching them
//!   in a sparse local file; guest writes land in a private overlay.
//! - **memory** (Linux only): the VMM restores with `--remote-page-url` and
//!   faults guest pages in through userfaultfd from a loopback [`PageServer`]
//!   that maps full-image page indexes onto the base and delta memory files,
//!   fetching object chunks on demand. macOS (HVF has no userfaultfd) downloads
//!   the memory files and forks as usual.
//!
//! The live disk of a lazily resumed sandbox is the trio overlay + cache +
//! index plus the [`SOURCE`] sidecar naming its object. Anything that needs
//! the disk as one image (pause, snapshot-as-template) first quiesces the
//! VMM, then calls [`materialize_disk`], which writes the guest-visible disk
//! (fetching the blocks never touched). The consumed generation stays in the
//! store (its KV record is gone) until then: [`release`] deletes it with the
//! disk files once the disk is materialized or the sandbox ends.

use std::{
	collections::HashMap,
	fs::{self, File},
	io,
	net::{Ipv4Addr, SocketAddr},
	os::unix::fs::FileExt,
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use axum::{
	Router,
	extract::{Path as UrlPath, State},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
	routing::get,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as AsyncMutex;
use vmon_cloud::s3::{S3Client, S3Config};

use crate::{
	api::ArtifactFile,
	block_image::{self, BlockIndex},
	vm::{RemoteDisk, RemotePages},
};

/// Live overlay of a remote disk (same name as a local sandbox disk).
pub const OVERLAY: &str = "rootfs.img";
/// Sparse cache of fetched blocks.
pub const CACHE: &str = "rootfs.cache";
/// Local copy of the image's seek index.
pub const INDEX: &str = block_image::INDEX;
/// Sidecar naming the remote object ([`RemoteSource`]).
pub const SOURCE: &str = "rootfs.remote.json";
/// Directory of [`PageServer`] caches.
pub const PAGES: &str = "pages";
/// Validity of the pre-signed rootfs URL handed to the VMM: the `SigV4`
/// maximum. Blocks first touched after it expires fail with I/O errors.
pub const PRESIGN_TTL: Duration = Duration::from_hours(7 * 24);
/// Guest page size served to `--remote-page-url`.
pub const PAGE_SIZE: u64 = 4096;
/// Object bytes fetched per page-server miss.
const PAGE_CHUNK: u64 = 1 << 20;

/// The object behind a lazily hydrated disk, recorded beside its overlay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteSource {
	/// Root URL of the store holding the generation.
	pub store: String,
	/// Key of the block image (`<generation>/rootfs.blk`), relative to
	/// `store`.
	pub key:   String,
	/// Quoted strong `ETag` the image was pinned to.
	pub etag:  String,
	/// Guest-visible capacity (`--rootfs-remote-size`).
	pub size:  u64,
}

impl RemoteSource {
	/// Reads the sidecar of the remote disk in `sandbox_dir`.
	pub fn load(sandbox_dir: &Path) -> Result<Self> {
		let bytes = fs::read(sandbox_dir.join(SOURCE)).context("reading the remote disk source")?;
		Ok(serde_json::from_slice(&bytes)?)
	}

	/// The paused generation (`paused/<id>/<uuid>`) the image belongs to.
	pub fn generation(&self) -> Result<&str> {
		self
			.key
			.strip_suffix(&format!("/{}", block_image::IMAGE))
			.filter(|generation| {
				generation.starts_with("paused/") && generation.split('/').count() == 3
			})
			.with_context(|| format!("remote disk key {:?} names no paused generation", self.key))
	}

	/// A client for [`Self::store`]: `configured` when it is that store.
	pub async fn client(&self, configured: Option<&S3Client>) -> Result<S3Client> {
		if let Some(s3) = configured
			&& s3.location().url() == self.store
		{
			return Ok(s3.clone());
		}
		S3Client::connect(S3Config::from_env(&self.store))
			.await
			.with_context(|| format!("opening store {}", self.store))
	}
}

/// Whether the live disk in `sandbox_dir` is remote-backed.
pub fn is_remote(sandbox_dir: &Path) -> bool {
	sandbox_dir.join(SOURCE).is_file()
}

/// Ends the remote disk of `sandbox_dir`, if any.
///
/// Removes its files and deletes the consumed generation from the store in
/// the background (best effort; a failure leaks storage only). Call it once
/// the disk was
/// materialized or will never be needed again. Blocking file I/O; needs a
/// Tokio runtime context (a blocking worker is fine).
pub fn release(configured: Option<&S3Client>, sandbox_dir: &Path) {
	let Ok(source) = RemoteSource::load(sandbox_dir) else {
		return;
	};
	if let Err(error) = remove_files(sandbox_dir) {
		tracing::warn!(dir = %sandbox_dir.display(), %error, "removing remote disk files failed");
	}
	let Ok(runtime) = tokio::runtime::Handle::try_current() else {
		tracing::warn!(key = %source.key, "no runtime to delete the consumed paused generation");
		return;
	};
	let configured = configured.cloned();
	runtime.spawn(async move {
		let deleted = async {
			let generation = source.generation()?.to_owned();
			let s3 = source.client(configured.as_ref()).await?;
			s3.delete_prefix(&format!("{generation}/")).await?;
			anyhow::Ok(generation)
		};
		match deleted.await {
			Ok(generation) => tracing::info!(%generation, "deleted the consumed paused generation"),
			Err(error) => tracing::warn!(
				key = %source.key,
				error = format!("{error:#}"),
				"deleting the consumed paused generation failed"
			),
		}
	});
}

/// Removes the lazy-resume files (overlay, cache, index, sidecar, page
/// caches) of `sandbox_dir`; missing files are fine.
pub fn remove_files(sandbox_dir: &Path) -> io::Result<()> {
	crate::artifacts::copy::remove_disk(&sandbox_dir.join(OVERLAY))?;
	for name in [SOURCE, CACHE, INDEX] {
		match fs::remove_file(sandbox_dir.join(name)) {
			Ok(()) => {},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
	}
	match fs::remove_dir_all(sandbox_dir.join(PAGES)) {
		Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
		_ => Ok(()),
	}
}

/// Prepares `sandbox_dir` to boot the block image `<generation>/rootfs.blk`
/// lazily.
///
/// Downloads its index, pins its `ETag`, pre-signs its URL, and records the
/// [`SOURCE`] sidecar. Any previous disk files are replaced.
pub async fn prepare_disk(
	s3: &S3Client,
	generation: &str,
	sandbox_dir: &Path,
) -> Result<RemoteDisk> {
	let image_key = format!("{generation}/{}", block_image::IMAGE);
	let index_key = format!("{generation}/{}", block_image::INDEX);
	let index_bytes = s3
		.get_bytes(&index_key, vmm::block_image::MAX_INDEX_BYTES as usize)
		.await
		.with_context(|| format!("fetching {index_key}"))?;
	let index =
		BlockIndex::from_json(&index_bytes).with_context(|| format!("parsing {index_key}"))?;
	let head = s3
		.head(&image_key)
		.await
		.with_context(|| format!("inspecting {image_key}"))?;
	ensure!(
		head.size == index.compressed_size,
		"{image_key} is {} bytes, its index expects {}",
		head.size,
		index.compressed_size
	);
	let etag = head
		.etag
		.filter(|etag| is_strong_etag(etag))
		.with_context(|| format!("{image_key} has no strong ETag to pin"))?;
	let url = s3
		.presign_get(&image_key, PRESIGN_TTL)
		.await
		.with_context(|| format!("pre-signing {image_key}"))?;
	let source = RemoteSource {
		store: s3.location().url().to_owned(),
		key:   image_key,
		etag:  etag.clone(),
		size:  index.sector_aligned_size(),
	};
	let dir = sandbox_dir.to_owned();
	tokio::task::spawn_blocking(move || -> Result<()> {
		fs::create_dir_all(&dir)?;
		remove_files(&dir)?;
		write_durable(&dir.join(INDEX), &index_bytes)?;
		write_durable(&dir.join(SOURCE), &serde_json::to_vec(&source)?)?;
		Ok(())
	})
	.await??;
	Ok(RemoteDisk {
		overlay: sandbox_dir.join(OVERLAY),
		cache: sandbox_dir.join(CACHE),
		index: sandbox_dir.join(INDEX),
		url: url.to_string(),
		etag,
		size: index.sector_aligned_size(),
	})
}

/// Writes the guest-visible disk of the remote-backed sandbox in
/// `sandbox_dir` as the new raw image `dest`.
///
/// Overlay writes go over cached blocks, fetching every block the guest
/// never touched (batched range
/// requests pinned to the recorded `ETag`). The remote disk stays; the
/// caller [`release`]s it once `dest` is in place. `configured` is the node's
/// object store client (reused when it is the source's store).
pub async fn materialize_disk(
	configured: Option<&S3Client>,
	sandbox_dir: &Path,
	dest: &Path,
) -> Result<()> {
	let source = RemoteSource::load(sandbox_dir)?;
	let s3 = source.client(configured).await?;
	let runtime = tokio::runtime::Handle::current();
	let (dir, dest) = (sandbox_dir.to_owned(), dest.to_owned());
	tokio::task::spawn_blocking(move || {
		let fetch = move |start: u64, end: u64| -> io::Result<Vec<u8>> {
			runtime
				.block_on(s3.get_range_if_match(&source.key, start, end - start, &source.etag))
				.map(|bytes| bytes.to_vec())
				.map_err(io::Error::from)
		};
		vmm::materialize_remote_disk(
			&dir.join(OVERLAY),
			&dir.join(CACHE),
			&dir.join(INDEX),
			source.size,
			fetch,
			&dest,
		)
		.map_err(|e| anyhow::anyhow!("materializing the remote disk: {e}"))
	})
	.await??;
	Ok(())
}

/// Whether guest RAM can be paged in lazily on this host (userfaultfd).
pub const LAZY_MEMORY: bool = cfg!(target_os = "linux");

/// Whether a lazy resume downloads the generation-relative `path`: never
/// the block image (served remotely; [`prepare_disk`] fetches its index),
/// and memory files only where RAM cannot be paged in ([`LAZY_MEMORY`]).
pub fn fetch_filter(path: &str) -> bool {
	if path == block_image::IMAGE || path == block_image::INDEX {
		return false;
	}
	!(LAZY_MEMORY && is_memory_file(path))
}

/// Whether `path` is the memory file of the paused snapshot or its base.
pub fn is_memory_file(path: &str) -> bool {
	matches!(
		path.split_once('/'),
		Some(("snapshot" | "base", name))
			if name.starts_with("memory.")
				&& Path::new(name).extension().is_some_and(|ext| ext == "bin")
	)
}

/// Starts the page source of a lazy `--restore` of `<paused_dir>/snapshot`.
///
/// The memory files stayed in the store ([`fetch_filter`]): the snapshot's
/// own memory file (full, or a delta over `<paused_dir>/<base>`) is read from
/// `<generation>/snapshot/…`; a delta's base memory from the local base
/// snapshot when present (a linked template), else from `<generation>/…`.
/// `files` are the generation's manifest entries; `cache_dir` holds the
/// page caches.
pub async fn page_server(
	s3: &S3Client,
	generation: &str,
	files: &[ArtifactFile],
	paused_dir: &Path,
	cache_dir: &Path,
) -> Result<PageServer> {
	let snapshot_dir = paused_dir.join("snapshot");
	let snapshot = vmm::snapshot::read_snapshot_metadata(&snapshot_dir)
		.map_err(|e| anyhow::anyhow!("reading {}: {e}", snapshot_dir.display()))?;
	let total_bytes: u64 = snapshot
		.snapshot()
		.mem_regions
		.iter()
		.map(|region| region.len)
		.sum();
	let remote = |dir: &str, local: &Path| -> Result<MemoryLayer> {
		let name = local
			.file_name()
			.and_then(|name| name.to_str())
			.context("snapshot memory file name")?;
		let path = format!("{dir}/{name}");
		let file = files
			.iter()
			.find(|file| file.path == path)
			.with_context(|| format!("the paused generation has no {path}"))?
			.clone();
		Ok(MemoryLayer::Remote { key: format!("{generation}/{path}"), file })
	};
	let own = remote("snapshot", snapshot.memory_file())?;
	let image = match &snapshot.snapshot().delta {
		None => PageImage::open(s3, cache_dir, total_bytes, own, None).await?,
		Some(delta) => {
			let base_dir = paused_dir.join(&delta.base);
			let base = vmm::snapshot::read_snapshot_metadata(&base_dir)
				.map_err(|e| anyhow::anyhow!("reading {}: {e}", base_dir.display()))?;
			ensure!(
				base.snapshot().delta.is_none(),
				"the RAM base of a delta must be a full snapshot"
			);
			let base_layer = if base.memory_file().is_file() {
				MemoryLayer::Local(base.memory_file().to_owned())
			} else {
				remote(&delta.base, base.memory_file())?
			};
			PageImage::open(s3, cache_dir, total_bytes, base_layer, Some((delta.changed.clone(), own)))
				.await?
		},
	};
	PageServer::start(image).await
}

fn write_durable(path: &Path, bytes: &[u8]) -> io::Result<()> {
	let file = File::create(path)?;
	file.write_all_at(bytes, 0)?;
	file.sync_all()
}

fn is_strong_etag(etag: &str) -> bool {
	let bytes = etag.as_bytes();
	bytes.len() > 2
		&& bytes[0] == b'"'
		&& bytes[bytes.len() - 1] == b'"'
		&& bytes[1..bytes.len() - 1]
			.iter()
			.all(|&byte| byte == 0x21 || (0x23..=0x7e).contains(&byte))
}

/// One memory file of a snapshot, local or in the object store.
pub enum MemoryLayer {
	/// A local file.
	Local(PathBuf),
	/// A packed sparse object: `file.extents` are its data extents, stored
	/// back to back; everything else reads as zeros.
	Remote { key: String, file: ArtifactFile },
}

/// Full-image guest RAM of a paused snapshot: the base memory file, with a
/// snapshot delta's changed pages on top (`vmm::snapshot::DeltaMemory`).
pub struct PageImage {
	pages: u64,
	base:  Layer,
	delta: Option<Delta>,
}

struct Delta {
	/// One bit per full-image page, LSB-first: the page is in `layer`.
	changed: Vec<u64>,
	/// Set bits before each word of `changed`.
	rank:    Vec<u64>,
	layer:   Layer,
}

impl PageImage {
	/// Opens the RAM image of `total_bytes` (page-aligned) from `base` and,
	/// for a delta snapshot, its `changed` bitmap (one bit per page,
	/// LSB-first) and memory file. `cache_dir` holds sparse caches of remote
	/// layers; remote layers are pinned to their current `ETag`.
	pub async fn open(
		s3: &S3Client,
		cache_dir: &Path,
		total_bytes: u64,
		base: MemoryLayer,
		delta: Option<(Vec<u8>, MemoryLayer)>,
	) -> Result<Self> {
		ensure!(
			total_bytes != 0 && total_bytes.is_multiple_of(PAGE_SIZE),
			"guest RAM size {total_bytes} is not page-aligned"
		);
		let pages = total_bytes / PAGE_SIZE;
		let base = Layer::open(s3, cache_dir, "base", base).await?;
		ensure!(
			base.size() == total_bytes,
			"base memory is {} bytes, guest RAM is {total_bytes}",
			base.size()
		);
		let delta = match delta {
			None => None,
			Some((bitmap, layer)) => {
				ensure!(bitmap.len() as u64 == pages.div_ceil(8), "delta bitmap length mismatch");
				let mut changed = vec![0u64; bitmap.len().div_ceil(8)];
				for (index, byte) in bitmap.iter().enumerate() {
					changed[index / 8] |= u64::from(*byte) << (8 * (index % 8));
				}
				let mut rank = Vec::with_capacity(changed.len());
				let mut set = 0u64;
				for word in &changed {
					rank.push(set);
					set += u64::from(word.count_ones());
				}
				let layer = Layer::open(s3, cache_dir, "delta", layer).await?;
				ensure!(
					layer.size() == set * PAGE_SIZE,
					"delta memory is {} bytes for {set} changed pages",
					layer.size()
				);
				Some(Delta { changed, rank, layer })
			},
		};
		Ok(Self { pages, base, delta })
	}

	/// Number of full-image pages.
	pub const fn pages(&self) -> u64 {
		self.pages
	}

	/// Reads full-image page `page`.
	pub async fn read_page(&self, page: u64) -> Result<Vec<u8>> {
		ensure!(page < self.pages, "page {page} is past the image ({} pages)", self.pages);
		let mut out = vec![0u8; PAGE_SIZE as usize];
		if let Some(delta) = &self.delta {
			let (word, bit) = ((page / 64) as usize, page % 64);
			let bits = delta.changed[word];
			if bits & (1 << bit) != 0 {
				let below = bits & ((1u64 << bit) - 1);
				let slot = delta.rank[word] + u64::from(below.count_ones());
				delta.layer.read(slot * PAGE_SIZE, &mut out).await?;
				return Ok(out);
			}
		}
		self.base.read(page * PAGE_SIZE, &mut out).await?;
		Ok(out)
	}
}

enum Layer {
	Local { file: Arc<File>, size: u64 },
	Remote(Box<RemoteLayer>),
}

impl Layer {
	async fn open(s3: &S3Client, cache_dir: &Path, name: &str, layer: MemoryLayer) -> Result<Self> {
		match layer {
			MemoryLayer::Local(path) => {
				let file = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
				let size = file.metadata()?.len();
				Ok(Self::Local { file: Arc::new(file), size })
			},
			MemoryLayer::Remote { key, file } => {
				crate::artifacts::sparse::validate(&file)?;
				let head = s3
					.head(&key)
					.await
					.with_context(|| format!("inspecting {key}"))?;
				let packed: u64 = file.extents.iter().map(|[_, len]| len).sum();
				ensure!(head.size == packed, "{key} is {} bytes, its extents pack {packed}", head.size);
				let etag = head
					.etag
					.with_context(|| format!("{key} has no ETag to pin"))?;
				fs::create_dir_all(cache_dir)?;
				let cache_path = cache_dir.join(format!("{name}.pages"));
				let cache = File::options()
					.read(true)
					.write(true)
					.create(true)
					.truncate(true)
					.open(&cache_path)?;
				cache.set_len(packed)?;
				let mut packed_at = Vec::with_capacity(file.extents.len());
				let mut at = 0;
				for [_, len] in &file.extents {
					packed_at.push(at);
					at += len;
				}
				Ok(Self::Remote(Box::new(RemoteLayer {
					s3: s3.clone(),
					key,
					etag,
					size: file.size,
					extents: file.extents,
					packed_at,
					packed,
					cache: Arc::new(cache),
					chunks: AsyncMutex::new(HashMap::new()),
				})))
			},
		}
	}

	fn size(&self) -> u64 {
		match self {
			Self::Local { size, .. } => *size,
			Self::Remote(remote) => remote.size,
		}
	}

	async fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
		match self {
			Self::Local { file, size } => {
				out.fill(0);
				let available = size.saturating_sub(offset).min(out.len() as u64) as usize;
				let file = Arc::clone(file);
				let mut buffer = vec![0u8; available];
				let buffer = tokio::task::spawn_blocking(move || {
					file.read_exact_at(&mut buffer, offset).map(|()| buffer)
				})
				.await??;
				out[..available].copy_from_slice(&buffer);
				Ok(())
			},
			Self::Remote(remote) => remote.read(offset, out).await,
		}
	}
}

/// A packed sparse memory object read through a sparse local cache, one
/// [`PAGE_CHUNK`] of the object at a time (single-flight per chunk).
struct RemoteLayer {
	s3:        S3Client,
	key:       String,
	etag:      String,
	/// Logical file size.
	size:      u64,
	/// Logical data extents `[offset, len]`, sorted.
	extents:   Vec<[u64; 2]>,
	/// Object offset of each extent.
	packed_at: Vec<u64>,
	/// Object size.
	packed:    u64,
	cache:     Arc<File>,
	/// Chunk index → fetched flag, guarded per chunk.
	chunks:    AsyncMutex<HashMap<u64, Arc<AsyncMutex<bool>>>>,
}

impl RemoteLayer {
	async fn read(&self, offset: u64, out: &mut [u8]) -> Result<()> {
		out.fill(0);
		let end = offset.saturating_add(out.len() as u64).min(self.size);
		// First extent that ends after `offset`.
		let first = self
			.extents
			.partition_point(|[start, len]| start + len <= offset);
		for (index, &[start, len]) in self.extents.iter().enumerate().skip(first) {
			if start >= end {
				break;
			}
			let from = start.max(offset);
			let to = (start + len).min(end);
			let object = self.packed_at[index] + (from - start);
			let slice = &mut out[(from - offset) as usize..(to - offset) as usize];
			self.read_object(object, slice).await?;
		}
		Ok(())
	}

	async fn read_object(&self, offset: u64, out: &mut [u8]) -> Result<()> {
		let end = offset + out.len() as u64;
		let mut chunk = offset / PAGE_CHUNK;
		while chunk * PAGE_CHUNK < end {
			self.ensure_chunk(chunk).await?;
			chunk += 1;
		}
		let cache = Arc::clone(&self.cache);
		let mut buffer = vec![0u8; out.len()];
		let buffer = tokio::task::spawn_blocking(move || {
			cache.read_exact_at(&mut buffer, offset).map(|()| buffer)
		})
		.await??;
		out.copy_from_slice(&buffer);
		Ok(())
	}

	async fn ensure_chunk(&self, chunk: u64) -> Result<()> {
		let slot = Arc::clone(
			self
				.chunks
				.lock()
				.await
				.entry(chunk)
				.or_insert_with(|| Arc::new(AsyncMutex::new(false))),
		);
		let mut fetched = slot.lock().await;
		if *fetched {
			return Ok(());
		}
		let start = chunk * PAGE_CHUNK;
		let len = PAGE_CHUNK.min(self.packed - start);
		let bytes = self
			.s3
			.get_range_if_match(&self.key, start, len, &self.etag)
			.await
			.with_context(|| format!("fetching {} bytes {start}+{len}", self.key))?;
		if bytes.len() as u64 != len {
			bail!("{} returned {} bytes for {start}+{len}", self.key, bytes.len());
		}
		let cache = Arc::clone(&self.cache);
		tokio::task::spawn_blocking(move || cache.write_all_at(&bytes, start)).await??;
		*fetched = true;
		Ok(())
	}
}

/// Loopback HTTP page source for one restoring VMM (`GET /pages/<n>` with
/// a bearer token). Stops when dropped.
pub struct PageServer {
	pages: RemotePages,
	task:  tokio::task::JoinHandle<()>,
}

impl PageServer {
	/// Serves `image` on an ephemeral loopback port with a fresh token.
	pub async fn start(image: PageImage) -> Result<Self> {
		let listener =
			tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
		let addr = listener.local_addr()?;
		let token = hex::encode(rand::random::<[u8; 32]>());
		let state = Arc::new(PageState { image, token: token.clone() });
		let router = Router::new()
			.route("/pages/{page}", get(serve_page))
			.with_state(state);
		let task = tokio::spawn(async move {
			if let Err(error) = axum::serve(listener, router).await {
				tracing::warn!(%error, "page server stopped");
			}
		});
		Ok(Self { pages: RemotePages { url: format!("http://{addr}/pages"), token }, task })
	}

	/// `--remote-page-url` and its token.
	pub const fn pages(&self) -> &RemotePages {
		&self.pages
	}
}

impl Drop for PageServer {
	fn drop(&mut self) {
		self.task.abort();
	}
}

struct PageState {
	image: PageImage,
	token: String,
}

async fn serve_page(
	State(state): State<Arc<PageState>>,
	UrlPath(page): UrlPath<u64>,
	headers: HeaderMap,
) -> Response {
	let presented = headers
		.get(header::AUTHORIZATION)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.strip_prefix("Bearer "))
		.unwrap_or_default();
	if !constant_time_eq(presented.as_bytes(), state.token.as_bytes()) {
		return StatusCode::UNAUTHORIZED.into_response();
	}
	if page >= state.image.pages() {
		return StatusCode::NOT_FOUND.into_response();
	}
	match state.image.read_page(page).await {
		Ok(bytes) => bytes.into_response(),
		Err(error) => {
			tracing::warn!(page, error = format!("{error:#}"), "serving a guest page failed");
			StatusCode::BAD_GATEWAY.into_response()
		},
	}
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
	a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
	use std::io::{Read, Write};

	use vmon_cloud::s3::{PutOptions, fake::FakeS3};

	use super::*;

	fn page(fill: u8) -> Vec<u8> {
		vec![fill; PAGE_SIZE as usize]
	}

	/// Packs the nonzero pages of `image` as sparse extents.
	fn packed(image: &[u8], path: &str) -> (Vec<u8>, ArtifactFile) {
		let mut data = Vec::new();
		let mut extents: Vec<[u64; 2]> = Vec::new();
		for (index, chunk) in image.chunks(PAGE_SIZE as usize).enumerate() {
			if chunk.iter().all(|&b| b == 0) {
				continue;
			}
			let offset = index as u64 * PAGE_SIZE;
			match extents.last_mut() {
				Some([start, len]) if *start + *len == offset => *len += PAGE_SIZE,
				_ => extents.push([offset, PAGE_SIZE]),
			}
			data.extend_from_slice(chunk);
		}
		let mut hasher = crate::artifacts::sparse::ExtentHasher::default();
		for extent in &extents {
			hasher.extent(*extent);
			let at = extent[0] as usize;
			hasher.update(&image[at..at + extent[1] as usize]);
		}
		let file = ArtifactFile {
			path: path.into(),
			size: image.len() as u64,
			sha256: hasher.finish(image.len() as u64),
			extents,
		};
		(data, file)
	}

	#[tokio::test]
	async fn page_image_merges_local_base_with_remote_packed_delta() {
		let s3 = FakeS3::start().await;
		let client = s3.client("bucket", "store");
		let dir = tempfile::tempdir().expect("tempdir");
		// Base: pages 0..6 filled with their index + 1, page 3 a hole.
		let mut base = Vec::new();
		for index in 0..6u8 {
			base.extend(if index == 3 { page(0) } else { page(index + 1) });
		}
		let base_path = dir.path().join("memory.1.bin");
		fs::write(&base_path, &base).expect("base");
		// Delta: pages 1 and 4 changed; page 4's new contents are zero.
		let delta_file: Vec<u8> = [page(0xd1), page(0)].concat();
		let (packed_delta, artifact) = packed(&delta_file, "snapshot/memory.2.bin");
		client
			.put("gen/snapshot/memory.2.bin", packed_delta.into(), &PutOptions::default())
			.await
			.expect("put delta");
		let image = PageImage::open(
			&client,
			&dir.path().join("cache"),
			base.len() as u64,
			MemoryLayer::Local(base_path),
			Some((vec![0b0001_0010], MemoryLayer::Remote {
				key:  "gen/snapshot/memory.2.bin".into(),
				file: artifact,
			})),
		)
		.await
		.expect("open");
		let expected = [page(1), page(0xd1), page(3), page(0), page(0), page(6)];
		for (index, want) in expected.iter().enumerate() {
			assert_eq!(&image.read_page(index as u64).await.expect("page"), want, "page {index}");
		}
		assert!(image.read_page(6).await.is_err());
		// The delta's zero page is a hole: only one remote range was read.
		let gets = s3
			.requests()
			.into_iter()
			.filter(|r| r.method == axum::http::Method::GET)
			.count();
		assert_eq!(gets, 1);
	}

	#[test]
	fn lazy_fetch_skips_the_block_image_and_pageable_memory() {
		assert!(!fetch_filter("rootfs.blk"));
		assert!(!fetch_filter("rootfs.blk.json"));
		for meta in
			["meta.json", "boot-init.json", "snapshot/vmstate.3.bin", "snapshot/current-generation"]
		{
			assert!(fetch_filter(meta), "{meta}");
		}
		assert!(is_memory_file("snapshot/memory.3.bin") && is_memory_file("base/memory.1.bin"));
		assert!(!is_memory_file("snapshot/vmstate.3.bin") && !is_memory_file("memory.1.bin"));
		assert_eq!(fetch_filter("snapshot/memory.3.bin"), !LAZY_MEMORY);
	}

	#[tokio::test]
	async fn page_server_requires_token_and_serves_pages() {
		let s3 = FakeS3::start().await;
		let client = s3.client("bucket", "store");
		let dir = tempfile::tempdir().expect("tempdir");
		let base: Vec<u8> = [page(7), page(8)].concat();
		let (packed_base, artifact) = packed(&base, "base/memory.1.bin");
		client
			.put("gen/base/memory.1.bin", packed_base.into(), &PutOptions::default())
			.await
			.expect("put base");
		let image = PageImage::open(
			&client,
			&dir.path().join("cache"),
			base.len() as u64,
			MemoryLayer::Remote { key: "gen/base/memory.1.bin".into(), file: artifact },
			None,
		)
		.await
		.expect("open");
		let server = PageServer::start(image).await.expect("server");
		let pages = server.pages().clone();
		let addr = pages
			.url
			.strip_prefix("http://")
			.and_then(|rest| rest.strip_suffix("/pages"))
			.expect("url")
			.to_owned();
		// The VMM's wire format (vmm/src/pager.rs RemotePageSource).
		let fetch = move |page: u64, token: String| {
			let addr = addr.clone();
			tokio::task::spawn_blocking(move || {
				let mut stream = std::net::TcpStream::connect(&addr).expect("connect");
				write!(
					stream,
					"GET /pages/{page} HTTP/1.1\r\nHost: {addr}\r\nAuthorization: Bearer \
					 {token}\r\nX-Vmon-Mesh-Hop: 1\r\nConnection: close\r\n\r\n"
				)
				.expect("request");
				let mut response = Vec::new();
				stream.read_to_end(&mut response).expect("response");
				let split = response
					.windows(4)
					.position(|w| w == b"\r\n\r\n")
					.expect("head");
				let status = String::from_utf8_lossy(&response[..split])
					.lines()
					.next()
					.unwrap_or_default()
					.to_owned();
				(status, response[split + 4..].to_vec())
			})
		};
		let (status, body) = fetch(1, pages.token.clone()).await.expect("fetch");
		assert!(status.contains(" 200 "), "{status}");
		assert_eq!(body, page(8));
		let (status, _) = fetch(1, "wrong".into()).await.expect("fetch");
		assert!(status.contains(" 401 "), "{status}");
		let (status, _) = fetch(2, pages.token.clone()).await.expect("fetch");
		assert!(status.contains(" 404 "), "{status}");
	}

	#[tokio::test]
	async fn prepare_then_materialize_roundtrips_a_disk_through_the_store() {
		let s3 = FakeS3::start().await;
		let client = s3.client("bucket", "store");
		let dir = tempfile::tempdir().expect("tempdir");
		let raw = dir.path().join("paused.img");
		let mut disk = vec![0u8; (2 << 20) + 4096];
		disk[..4096].fill(0x61);
		disk[(2 << 20)..].fill(0x62);
		fs::write(&raw, &disk).expect("disk");
		let (blk, index) = (dir.path().join("rootfs.blk"), dir.path().join("rootfs.blk.json"));
		block_image::encode_disk(&raw, &blk, &index).expect("encode");
		let generation = "paused/sbx/g0";
		for (path, name) in [(&blk, "rootfs.blk"), (&index, "rootfs.blk.json")] {
			let key = format!("{generation}/{name}");
			client
				.put(&key, fs::read(path).expect("read").into(), &PutOptions::default())
				.await
				.expect("put");
		}
		let sandbox = dir.path().join("sandbox");
		let remote = prepare_disk(&client, generation, &sandbox)
			.await
			.expect("prepare");
		assert!(is_remote(&sandbox));
		assert_eq!(remote.size, disk.len() as u64);
		assert!(remote.url.contains("rootfs.blk"));
		// The VMM creates the overlay on boot; an untouched one materializes
		// to the original disk.
		File::create(&remote.overlay).expect("overlay");
		let dest = dir.path().join("out.img");
		materialize_disk(Some(&client), &sandbox, &dest)
			.await
			.expect("materialize");
		assert_eq!(fs::read(&dest).expect("out"), disk);
		assert_eq!(
			RemoteSource::load(&sandbox)
				.expect("source")
				.generation()
				.expect("generation"),
			generation
		);
		remove_files(&sandbox).expect("cleanup");
		assert!(!is_remote(&sandbox));
		assert!(!remote.overlay.exists() && !remote.cache.exists());
	}

	#[tokio::test]
	async fn release_removes_files_and_deletes_the_consumed_generation() {
		let s3 = FakeS3::start().await;
		let client = s3.client("bucket", "store");
		let dir = tempfile::tempdir().expect("tempdir");
		for key in
			["paused/sbx/g1/rootfs.blk", "paused/sbx/g1/snapshot/memory.1.bin", "paused/sbx/g2/x"]
		{
			client
				.put(key, bytes::Bytes::from_static(b"x"), &PutOptions::default())
				.await
				.expect("put");
		}
		let source = RemoteSource {
			store: client.location().url().to_owned(),
			key:   "paused/sbx/g1/rootfs.blk".into(),
			etag:  "\"e\"".into(),
			size:  512,
		};
		fs::write(dir.path().join(SOURCE), serde_json::to_vec(&source).expect("json"))
			.expect("source");
		fs::write(dir.path().join(CACHE), b"c").expect("cache");
		let sandbox = dir.path().to_owned();
		let configured = client.clone();
		tokio::task::spawn_blocking(move || release(Some(&configured), &sandbox))
			.await
			.expect("release");
		assert!(!is_remote(dir.path()) && !dir.path().join(CACHE).exists());
		for _ in 0..100 {
			if s3.keys().len() == 1 {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
		assert_eq!(s3.keys(), vec!["bucket/store/paused/sbx/g2/x".to_owned()]);
	}
}
