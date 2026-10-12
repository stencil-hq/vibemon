//! Step cache (ARCHITECTURE.md §8.6 step 4).
//!
//! Every build layer (base, default-user phase, each step) has a key chained
//! by SHA-256 over the previous key and the layer's definition, mirroring
//! upstream `sha256(prev ";" type ";" join(args," ") ";" filesHash)`. A cached
//! layer is a rootfs image plus the build context after that layer, stored
//! under `cache/steps/<key>/`. `force` (template-wide or per step) disables
//! reuse of the forced layer and everything after it.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::steps::BuildContext;
use crate::artifacts::{
	copy::{clone_file, try_reflink},
	write_json_atomic,
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

/// Cached layers kept per node (oldest evicted first).
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
	dir: PathBuf,
}

impl StepCache {
	/// Opens (creating) the cache directory.
	pub fn new(dir: PathBuf) -> std::io::Result<Self> {
		std::fs::create_dir_all(&dir)?;
		Ok(Self { dir })
	}

	/// Whether a complete entry exists for `key`.
	pub fn contains(&self, key: &str) -> bool {
		let dir = self.dir.join(key);
		dir.join(LAYER).is_file() && dir.join(ROOTFS).is_file()
	}

	/// Clones the cached rootfs of `key` to `dest` and returns its context.
	pub fn restore(&self, key: &str, dest: &Path) -> Result<BuildContext> {
		let dir = self.dir.join(key);
		let record: LayerRecord =
			serde_json::from_slice(&std::fs::read(dir.join(LAYER))?).context("cached layer record")?;
		if dest.exists() {
			std::fs::remove_file(dest)?;
		}
		clone_file(&dir.join(ROOTFS), dest)
			.with_context(|| format!("restoring cached layer {key}"))?;
		// Touch for LRU eviction.
		let _ = std::fs::File::open(dir.join(LAYER))
			.and_then(|f| f.set_modified(std::time::SystemTime::now()));
		Ok(record.context)
	}

	/// Stores a clone of `rootfs` (which must be quiescent) as `key`.
	/// Returns `false` when `policy` skipped the copy.
	pub async fn store(
		&self,
		key: &str,
		rootfs: &Path,
		context: &BuildContext,
		policy: CopyPolicy,
	) -> Result<bool> {
		let staging = self
			.dir
			.join(format!(".{key}.{:08x}", rand::random::<u32>()));
		let final_dir = self.dir.join(key);
		let result = async {
			tokio::fs::create_dir_all(&staging).await?;
			let (src, dst) = (rootfs.to_owned(), staging.join(ROOTFS));
			let copied = tokio::task::spawn_blocking(move || match policy {
				CopyPolicy::Always => clone_file(&src, &dst).map(|()| true),
				CopyPolicy::ReflinkOnly => try_reflink(&src, &dst),
			})
			.await??;
			if !copied {
				return Ok(false);
			}
			write_json_atomic(&staging.join(LAYER), &LayerRecord {
				key:     key.to_owned(),
				context: context.clone(),
			})
			.await?;
			// Directory renames never replace a non-empty directory, so an
			// existing entry (forced rebuild, or a concurrent writer of the
			// same key) is moved aside and the install retried; the last
			// writer wins and no reader ever sees a half-written entry.
			let mut last_error = None;
			for _ in 0..INSTALL_ATTEMPTS {
				match tokio::fs::rename(&staging, &final_dir).await {
					Ok(()) => return Ok(true),
					Err(error) => last_error = Some(error),
				}
				let trash = self
					.dir
					.join(format!(".{key}.{:08x}.old", rand::random::<u32>()));
				if tokio::fs::rename(&final_dir, &trash).await.is_ok() {
					let _ = tokio::fs::remove_dir_all(&trash).await;
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
		.await;
		let _ = tokio::fs::remove_dir_all(&staging).await;
		let stored = result?;
		if stored {
			self.evict();
		}
		Ok(stored)
	}

	/// Drops the least recently used entries beyond [`MAX_ENTRIES`].
	fn evict(&self) {
		let Ok(entries) = std::fs::read_dir(&self.dir) else {
			return;
		};
		let mut layers: Vec<(std::time::SystemTime, PathBuf)> = entries
			.filter_map(Result::ok)
			.filter(|e| !e.file_name().to_string_lossy().starts_with('.'))
			.map(|e| {
				let modified = std::fs::metadata(e.path().join(LAYER))
					.and_then(|m| m.modified())
					.unwrap_or(std::time::UNIX_EPOCH);
				(modified, e.path())
			})
			.collect();
		if layers.len() <= MAX_ENTRIES {
			return;
		}
		layers.sort_unstable_by_key(|(modified, _)| std::cmp::Reverse(*modified));
		for (_, path) in layers.into_iter().skip(MAX_ENTRIES) {
			if let Err(error) = std::fs::remove_dir_all(&path) {
				tracing::warn!(%error, path = %path.display(), "evicting cached layer");
			}
		}
	}
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
		let cache = StepCache::new(dir.path().join("steps")).unwrap();
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
		cache.restore("k", &out).unwrap();
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
		cache.restore("k", &out).unwrap();
		assert_eq!(std::fs::read(&out).unwrap(), b"newer");
		let leftovers = std::fs::read_dir(dir.path().join("steps")).unwrap().count();
		assert_eq!(leftovers, 1, "no staging or displaced directories remain");
	}
	#[tokio::test]
	async fn store_and_restore_round_trip() {
		let dir = tempfile::tempdir().unwrap();
		let cache = StepCache::new(dir.path().join("steps")).unwrap();
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
		assert_eq!(cache.restore("k", &out).unwrap(), ctx);
		assert_eq!(std::fs::read(&out).unwrap(), b"image");
	}
}
