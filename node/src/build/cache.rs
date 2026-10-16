//! Step cache (ARCHITECTURE.md §8.6 step 4).
//!
//! Every build layer (base, default-user phase, each step) has a key chained
//! by SHA-256 over the previous key and the layer's definition, mirroring
//! upstream `sha256(prev ";" type ";" join(args," ") ";" filesHash)`. A cached
//! layer is a rootfs image plus the build context after that layer, stored
//! under `cache/steps/<key>/`. `force` (template-wide or per step) disables
//! reuse of the forced layer and everything after it.
//!
//! The cache is bounded by entry count and by the disk its entries occupy
//! (allocated blocks, not apparent image sizes); the least recently used
//! entries go first. Hidden `.<key>.*` directories are in-flight stores and
//! displaced entries.

use std::{
	collections::HashMap,
	path::{Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::steps::BuildContext;
use crate::artifacts::{
	copy::{clone_file, try_reflink},
	disk_usage,
};

/// How a layer's rootfs is captured into the cache.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CopyPolicy {
	/// Reflink when possible, else a sparse copy.
	Always,
	/// Only when the filesystem can reflink (skip otherwise); used for
	/// intermediate layers captured while the build VM is paused.
	ReflinkOnly,
}

/// Cached layers kept per node at most (least recently used evicted first).
const MAX_ENTRIES: usize = 48;
/// Install attempts when other writers race on the same key.
const INSTALL_ATTEMPTS: usize = 32;
const ROOTFS: &str = "rootfs.ext4";
const LAYER: &str = "layer.json";

/// Key of the base layer: image manifest digest (or `template:<buildId>`),
/// disk size, provisioning script fingerprint, and the agent binary digest.
pub fn base_key(
	source: &str,
	disk_mb: u64,
	provision_fingerprint: &str,
	agent_digest: &str,
) -> String {
	sha256_hex(&format!("base;{source};{disk_mb};{provision_fingerprint};{agent_digest}"))
}

/// Key of a layer following `prev`.
pub fn layer_key(prev: &str, kind: &str, args: &[String], files_hash: Option<&str>) -> String {
	sha256_hex(&format!("{prev};{kind};{};{}", args.join(" "), files_hash.unwrap_or("")))
}

/// One planned layer of a build.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Layer {
	pub key:    String,
	/// Must be rebuilt even if cached.
	pub forced: bool,
}

/// Definition of a non-base layer as hashed into the chain.
#[derive(Clone, Debug)]
pub struct LayerDef<'a> {
	pub kind:       &'a str,
	pub args:       &'a [String],
	pub files_hash: Option<&'a str>,
	pub force:      bool,
}

/// Plans the chain: `layers[0]` is the base, then one entry per `defs`.
/// Once a layer is forced every later layer is forced too.
pub fn plan(base: String, template_force: bool, defs: &[LayerDef<'_>]) -> Vec<Layer> {
	let mut layers = Vec::with_capacity(defs.len() + 1);
	let mut forced = template_force;
	layers.push(Layer { key: base, forced });
	for def in defs {
		forced |= def.force;
		let prev = &layers[layers.len() - 1].key;
		let key = layer_key(prev, def.kind, def.args, def.files_hash);
		layers.push(Layer { key, forced });
	}
	layers
}

/// Index of the deepest reusable layer: unforced and present in the cache.
/// `None` means build everything from scratch.
pub fn resume_point(layers: &[Layer], cached: impl Fn(&str) -> bool) -> Option<usize> {
	layers
		.iter()
		.enumerate()
		.rev()
		.find(|(_, layer)| !layer.forced && cached(&layer.key))
		.map(|(i, _)| i)
}

#[derive(Serialize, Deserialize)]
struct LayerRecord {
	key:     String,
	context: BuildContext,
}

/// On-disk layer cache under `<data>/cache/steps`.
#[derive(Clone, Debug)]
pub struct StepCache {
	inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
	dir:       PathBuf,
	/// Disk the entries may occupy (allocated bytes).
	max_bytes: u64,
	/// Reference counts of entries being restored. Eviction and replacing
	/// stores leave them in place; held while entries are moved.
	pinned:    Mutex<HashMap<String, usize>>,
}

/// One cache entry of the stored layers.
#[derive(Debug)]
struct Entry {
	key:   String,
	path:  PathBuf,
	/// Last store or restore.
	used:  SystemTime,
	bytes: u64,
}

/// The deepest reusable layer of a build, pinned until dropped so that
/// neither eviction nor a replacing store removes it while it is restored.
#[derive(Debug)]
pub struct CachedLayer {
	cache: StepCache,
	index: usize,
	key:   String,
}

impl StepCache {
	/// Opens (creating) the cache directory, keeping at most `max_bytes` of
	/// entries. Leftovers of stores interrupted by a node restart are
	/// removed.
	pub fn new(dir: PathBuf, max_bytes: u64) -> std::io::Result<Self> {
		std::fs::create_dir_all(&dir)?;
		for entry in std::fs::read_dir(&dir)? {
			let path = entry?.path();
			if path
				.file_name()
				.is_some_and(|name| name.as_encoded_bytes().starts_with(b"."))
				&& let Err(error) = std::fs::remove_dir_all(&path)
			{
				tracing::warn!(%error, path = %path.display(), "removing an interrupted cache store");
			}
		}
		let cache =
			Self { inner: Arc::new(Inner { dir, max_bytes, pinned: Mutex::new(HashMap::new()) }) };
		cache.evict();
		Ok(cache)
	}

	/// Whether a complete entry exists for `key`.
	pub fn contains(&self, key: &str) -> bool {
		let dir = self.inner.dir.join(key);
		dir.join(LAYER).is_file() && dir.join(ROOTFS).is_file()
	}

	/// Pins the deepest reusable layer of `layers` (see [`resume_point`]);
	/// `None` means build everything from scratch.
	pub fn pin_resume_point(&self, layers: &[Layer]) -> Option<CachedLayer> {
		let mut pinned = self.inner.pinned.lock();
		let index = resume_point(layers, |key| self.contains(key))?;
		let key = layers[index].key.clone();
		*pinned.entry(key.clone()).or_default() += 1;
		Some(CachedLayer { cache: self.clone(), index, key })
	}

	/// Stores a clone of `rootfs` (which must be quiescent) as `key`, then
	/// evicts down to the limits. Returns `false` when `policy` skipped the
	/// copy.
	///
	/// The work runs on a blocking thread that finishes (and removes its
	/// staging directory) even when the caller stops waiting, as a cancelled
	/// build does.
	pub async fn store(
		&self,
		key: &str,
		rootfs: &Path,
		context: &BuildContext,
		policy: CopyPolicy,
	) -> Result<bool> {
		let cache = self.clone();
		let (key, rootfs) = (key.to_owned(), rootfs.to_owned());
		let record = LayerRecord { key: key.clone(), context: context.clone() };
		tokio::task::spawn_blocking(move || {
			let staging = cache
				.inner
				.dir
				.join(format!(".{key}.{:08x}", rand::random::<u32>()));
			let result = cache.install(&staging, &key, &rootfs, &record, policy);
			let _ = std::fs::remove_dir_all(&staging);
			let stored = result?;
			if stored {
				cache.evict();
			}
			Ok(stored)
		})
		.await?
	}

	/// Copies `rootfs` into `staging` and installs it as `key`.
	fn install(
		&self,
		staging: &Path,
		key: &str,
		rootfs: &Path,
		record: &LayerRecord,
		policy: CopyPolicy,
	) -> Result<bool> {
		std::fs::create_dir_all(staging)?;
		let image = staging.join(ROOTFS);
		let copied = match policy {
			CopyPolicy::Always => clone_file(rootfs, &image).map(|()| true),
			CopyPolicy::ReflinkOnly => try_reflink(rootfs, &image),
		}?;
		if !copied {
			return Ok(false);
		}
		std::fs::write(staging.join(LAYER), serde_json::to_vec_pretty(record)?)?;
		// Directory renames never replace a non-empty directory, so an
		// existing entry (forced rebuild, or a concurrent writer of the same
		// key) is moved aside and the install retried; the last writer wins
		// and no reader ever sees a half-written entry. An entry being
		// restored is kept instead: it holds the same layer.
		let final_dir = self.inner.dir.join(key);
		let mut last_error = None;
		for _ in 0..INSTALL_ATTEMPTS {
			let pinned = self.inner.pinned.lock();
			match std::fs::rename(staging, &final_dir) {
				Ok(()) => return Ok(true),
				Err(error) => last_error = Some(error),
			}
			if pinned.contains_key(key) && self.contains(key) {
				return Ok(true);
			}
			let trash = self.trash_path(key);
			let displaced = std::fs::rename(&final_dir, &trash).is_ok();
			drop(pinned);
			if displaced {
				let _ = std::fs::remove_dir_all(&trash);
			}
		}
		if self.contains(key) {
			return Ok(true);
		}
		Err(
			anyhow::Error::from(
				last_error.unwrap_or_else(|| std::io::Error::other("no attempt made")),
			)
			.context(format!("installing cached layer {key}")),
		)
	}

	/// Drops least recently used entries until at most [`MAX_ENTRIES`]
	/// remain and they occupy at most the byte budget. Pinned entries stay.
	fn evict(&self) {
		let doomed: Vec<(PathBuf, u64)> = {
			let pinned = self.inner.pinned.lock();
			let entries = match self.entries() {
				Ok(entries) => entries,
				Err(error) => {
					tracing::warn!(%error, "listing the step cache");
					return;
				},
			};
			victims(entries, MAX_ENTRIES, self.inner.max_bytes, |key| pinned.contains_key(key))
				.into_iter()
				.filter_map(|entry| {
					// Out of the way of new pins before the slow removal.
					let trash = self.trash_path(&entry.key);
					match std::fs::rename(&entry.path, &trash) {
						Ok(()) => Some((trash, entry.bytes)),
						Err(error) => {
							tracing::warn!(%error, layer = entry.key, "evicting cached layer");
							None
						},
					}
				})
				.collect()
		};
		if doomed.is_empty() {
			return;
		}
		let freed: u64 = doomed.iter().map(|(_, bytes)| bytes).sum();
		for (path, _) in &doomed {
			if let Err(error) = std::fs::remove_dir_all(path) {
				tracing::warn!(%error, path = %path.display(), "evicting cached layer");
			}
		}
		tracing::info!(layers = doomed.len(), bytes = freed, "evicted step cache layers");
	}

	/// Every installed entry, complete or not.
	fn entries(&self) -> std::io::Result<Vec<Entry>> {
		let mut entries = Vec::new();
		for dirent in std::fs::read_dir(&self.inner.dir)? {
			let dirent = dirent?;
			let Ok(key) = dirent.file_name().into_string() else {
				continue;
			};
			if key.starts_with('.') {
				continue;
			}
			let path = dirent.path();
			let used = std::fs::metadata(path.join(LAYER))
				.and_then(|m| m.modified())
				.unwrap_or(SystemTime::UNIX_EPOCH);
			let bytes = disk_usage(&path).unwrap_or(0);
			entries.push(Entry { key, path, used, bytes });
		}
		Ok(entries)
	}

	fn trash_path(&self, key: &str) -> PathBuf {
		self
			.inner
			.dir
			.join(format!(".{key}.{:08x}.old", rand::random::<u32>()))
	}
}

impl CachedLayer {
	/// Index of the layer in the planned chain.
	pub const fn index(&self) -> usize {
		self.index
	}

	/// Clones the cached rootfs to `dest` and returns the layer's context.
	pub fn restore(&self, dest: &Path) -> Result<BuildContext> {
		let dir = self.cache.inner.dir.join(&self.key);
		let record: LayerRecord =
			serde_json::from_slice(&std::fs::read(dir.join(LAYER))?).context("cached layer record")?;
		if dest.exists() {
			std::fs::remove_file(dest)?;
		}
		clone_file(&dir.join(ROOTFS), dest)
			.with_context(|| format!("restoring cached layer {}", self.key))?;
		// Touch for LRU eviction.
		let _ = std::fs::File::open(dir.join(LAYER)).and_then(|f| f.set_modified(SystemTime::now()));
		Ok(record.context)
	}
}

impl Drop for CachedLayer {
	fn drop(&mut self) {
		let mut pinned = self.cache.inner.pinned.lock();
		if let Some(count) = pinned.get_mut(&self.key) {
			*count -= 1;
			if *count == 0 {
				pinned.remove(&self.key);
			}
		}
	}
}

/// Entries to evict, least recently used first, until at most
/// `max_entries` remain within `max_bytes`; pinned entries are skipped. A
/// layer larger than the whole budget is not kept.
fn victims(
	mut entries: Vec<Entry>,
	max_entries: usize,
	max_bytes: u64,
	pinned: impl Fn(&str) -> bool,
) -> Vec<Entry> {
	entries.sort_unstable_by_key(|entry| entry.used);
	let mut count = entries.len();
	let mut bytes: u64 = entries.iter().map(|entry| entry.bytes).sum();
	let mut doomed = Vec::new();
	for entry in entries {
		if count <= max_entries && bytes <= max_bytes {
			break;
		}
		if pinned(&entry.key) {
			continue;
		}
		count -= 1;
		bytes -= entry.bytes;
		doomed.push(entry);
	}
	doomed
}

fn sha256_hex(text: &str) -> String {
	hex::encode(Sha256::digest(text.as_bytes()))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(a: &[&str]) -> Vec<String> {
		a.iter().map(ToString::to_string).collect()
	}

	fn defs(steps: &[(String, Vec<String>, Option<String>, bool)]) -> Vec<LayerDef<'_>> {
		steps
			.iter()
			.map(|(kind, args, hash, force)| LayerDef {
				kind,
				args,
				files_hash: hash.as_deref(),
				force: *force,
			})
			.collect()
	}

	/// Pins `key` alone, as a build whose deepest cached layer it is.
	fn pin(cache: &StepCache, key: &str) -> CachedLayer {
		cache
			.pin_resume_point(&[Layer { key: key.into(), forced: false }])
			.expect("cached")
	}

	/// Stores `bytes` as layer `key`.
	async fn store_bytes(cache: &StepCache, dir: &Path, key: &str, bytes: &[u8]) {
		let rootfs = dir.join(format!("rootfs-{key}"));
		std::fs::write(&rootfs, bytes).unwrap();
		let stored = cache
			.store(key, &rootfs, &BuildContext::default(), CopyPolicy::Always)
			.await;
		assert!(stored.unwrap());
		// Distinct last-use times even on coarse filesystem clocks.
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
	}

	fn steps() -> Vec<(String, Vec<String>, Option<String>, bool)> {
		vec![
			("RUN".into(), args(&["apk add curl", "root"]), None, false),
			("COPY".into(), args(&["app", "/app", "", ""]), Some("ab".repeat(32)), false),
			("ENV".into(), args(&["A", "1"]), None, false),
		]
	}

	#[test]
	fn keys_chain_so_a_change_invalidates_only_later_layers() {
		let s = steps();
		let a = plan("base".into(), false, &defs(&s));
		let mut changed = s.clone();
		changed[1].2 = Some("cd".repeat(32)); // different COPY contents
		let b = plan("base".into(), false, &defs(&changed));
		assert_eq!(a[0], b[0]);
		assert_eq!(a[1], b[1], "RUN before the change keeps its key");
		assert_ne!(a[2].key, b[2].key);
		assert_ne!(a[3].key, b[3].key, "ENV after the change is invalidated by chaining");
		let other_base = plan("base2".into(), false, &defs(&s));
		assert!(
			a.iter().zip(&other_base).all(|(x, y)| x.key != y.key),
			"a new base digest invalidates everything"
		);
	}

	#[test]
	fn key_matches_upstream_formula_and_separates_args() {
		let k = layer_key("p", "RUN", &args(&["echo a", "root"]), None);
		assert_eq!(k, sha256_hex("p;RUN;echo a root;"));
		assert_ne!(
			layer_key("p", "RUN", &args(&["a b"]), None),
			layer_key("p", "ENV", &args(&["a b"]), None)
		);
		assert_ne!(
			layer_key("p", "COPY", &args(&["x"]), Some("h1")),
			layer_key("p", "COPY", &args(&["x"]), Some("h2"))
		);
	}

	#[test]
	fn resume_uses_deepest_cached_layer() {
		let layers = plan("base".into(), false, &defs(&steps()));
		let all = |_: &str| true;
		assert_eq!(resume_point(&layers, all), Some(3));
		let up_to_1 = |k: &str| k == layers[0].key || k == layers[1].key;
		assert_eq!(resume_point(&layers, up_to_1), Some(1));
		assert_eq!(resume_point(&layers, |_| false), None);
	}

	#[test]
	fn step_force_invalidates_it_and_everything_downstream() {
		let mut s = steps();
		s[1].3 = true;
		let layers = plan("base".into(), false, &defs(&s));
		assert_eq!(layers.iter().map(|l| l.forced).collect::<Vec<_>>(), [false, false, true, true]);
		assert_eq!(resume_point(&layers, |_| true), Some(1), "cached later layers are ignored");
		// Forcing does not change keys, so a forced rebuild refreshes the same
		// entries.
		assert_eq!(layers[3].key, plan("base".into(), false, &defs(&steps()))[3].key);
	}

	#[test]
	fn template_force_bypasses_even_the_base() {
		let layers = plan("base".into(), true, &defs(&steps()));
		assert!(layers.iter().all(|l| l.forced));
		assert_eq!(resume_point(&layers, |_| true), None);
	}

	#[tokio::test]
	async fn concurrent_and_repeated_stores_of_one_key_leave_a_valid_entry() {
		let dir = tempfile::tempdir().unwrap();
		let cache = StepCache::new(dir.path().join("steps"), u64::MAX).unwrap();
		let ctx = BuildContext::default();
		let mut sources = Vec::new();
		for i in 0..8u8 {
			let path = dir.path().join(format!("rootfs{i}"));
			std::fs::write(&path, [i; 64]).unwrap();
			sources.push(path);
		}
		let stores = sources
			.iter()
			.map(|src| cache.store("k", src, &ctx, CopyPolicy::Always));
		for result in futures::future::join_all(stores).await {
			assert!(result.unwrap(), "every concurrent writer succeeds");
		}
		let out = dir.path().join("out");
		pin(&cache, "k").restore(&out).unwrap();
		let bytes = std::fs::read(&out).unwrap();
		assert!(
			bytes.len() == 64 && bytes.iter().all(|b| *b == bytes[0]),
			"one writer's complete image"
		);

		// A later (forced) store replaces the entry.
		let newer = dir.path().join("newer");
		std::fs::write(&newer, b"newer").unwrap();
		cache
			.store("k", &newer, &ctx, CopyPolicy::Always)
			.await
			.unwrap();
		pin(&cache, "k").restore(&out).unwrap();
		assert_eq!(std::fs::read(&out).unwrap(), b"newer");
		let leftovers = std::fs::read_dir(dir.path().join("steps")).unwrap().count();
		assert_eq!(leftovers, 1, "no staging or displaced directories remain");
	}

	#[test]
	fn opening_reclaims_interrupted_stores() {
		let dir = tempfile::tempdir().unwrap();
		let steps = dir.path().join("steps");
		for leftover in [".k.0123abcd", ".k.89abcdef.old"] {
			std::fs::create_dir_all(steps.join(leftover)).unwrap();
			std::fs::write(steps.join(leftover).join(ROOTFS), b"partial").unwrap();
		}
		StepCache::new(steps.clone(), u64::MAX).unwrap();
		assert_eq!(std::fs::read_dir(&steps).unwrap().count(), 0, "staging dirs are reclaimed");
	}

	#[tokio::test]
	async fn byte_budget_evicts_least_recently_used_layers() {
		let dir = tempfile::tempdir().unwrap();
		// Room for two 1 MiB layers, far below the entry limit.
		let cache = StepCache::new(dir.path().join("steps"), 5 << 19).unwrap();
		let layer = vec![1u8; 1 << 20];
		store_bytes(&cache, dir.path(), "a", &layer).await;
		store_bytes(&cache, dir.path(), "b", &layer).await;
		// Restoring `a` makes `b` the least recently used.
		pin(&cache, "a").restore(&dir.path().join("out")).unwrap();
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		store_bytes(&cache, dir.path(), "c", &layer).await;
		assert!(cache.contains("a") && cache.contains("c"));
		assert!(!cache.contains("b"), "the least recently used layer is evicted");
		let names = std::fs::read_dir(dir.path().join("steps")).unwrap().count();
		assert_eq!(names, 2, "evicted layers leave nothing behind");

		// A budget lowered across a restart applies on open.
		let reopened = StepCache::new(dir.path().join("steps"), 3 << 19).unwrap();
		assert!(reopened.contains("c") && !reopened.contains("a"));
	}

	#[tokio::test]
	async fn pinned_layer_survives_eviction_and_replacement() {
		let dir = tempfile::tempdir().unwrap();
		// Room for one 1 MiB layer.
		let cache = StepCache::new(dir.path().join("steps"), 3 << 19).unwrap();
		let layer = |byte: u8| vec![byte; 1 << 20];
		store_bytes(&cache, dir.path(), "a", &layer(1)).await;
		let restoring = pin(&cache, "a");
		store_bytes(&cache, dir.path(), "b", &layer(2)).await;
		assert!(cache.contains("a"), "a pinned layer is not evicted");
		assert!(!cache.contains("b"), "the budget still holds");
		// A forced rebuild of the same key keeps the entry being restored.
		store_bytes(&cache, dir.path(), "a", &layer(3)).await;
		let out = dir.path().join("out");
		restoring.restore(&out).unwrap();
		assert_eq!(std::fs::read(&out).unwrap(), layer(1));
		drop(restoring);
		store_bytes(&cache, dir.path(), "c", &layer(4)).await;
		assert!(!cache.contains("a") && cache.contains("c"), "unpinned, it is evicted again");
	}

	#[tokio::test]
	async fn abandoned_store_finishes_and_cleans_up() {
		let dir = tempfile::tempdir().unwrap();
		let cache = StepCache::new(dir.path().join("steps"), u64::MAX).unwrap();
		let rootfs = dir.path().join("rootfs");
		std::fs::write(&rootfs, vec![7u8; 4 << 20]).unwrap();
		// A cancelled build drops the store future after its first poll.
		let dropped = tokio::time::timeout(
			std::time::Duration::ZERO,
			cache.store("k", &rootfs, &BuildContext::default(), CopyPolicy::Always),
		)
		.await;
		assert!(dropped.is_err(), "the store was abandoned midway");
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		let names = || -> Vec<String> {
			std::fs::read_dir(dir.path().join("steps"))
				.unwrap()
				.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
				.collect()
		};
		while names() != ["k"] && std::time::Instant::now() < deadline {
			tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		}
		assert_eq!(names(), ["k"], "the copy completes and no staging directory remains");
		assert!(cache.contains("k"));
	}

	#[tokio::test]
	async fn store_and_restore_round_trip() {
		let dir = tempfile::tempdir().unwrap();
		let cache = StepCache::new(dir.path().join("steps"), u64::MAX).unwrap();
		let rootfs = dir.path().join("rootfs");
		std::fs::write(&rootfs, b"image").unwrap();
		let ctx = BuildContext {
			user: "user".into(),
			workdir: Some("/app".into()),
			..BuildContext::default()
		};
		assert!(!cache.contains("k"));
		assert!(
			cache
				.store("k", &rootfs, &ctx, CopyPolicy::Always)
				.await
				.unwrap()
		);
		assert!(cache.contains("k"));
		let out = dir.path().join("out");
		std::fs::write(&out, b"stale").unwrap();
		assert_eq!(pin(&cache, "k").restore(&out).unwrap(), ctx);
		assert_eq!(std::fs::read(&out).unwrap(), b"image");
	}
}
