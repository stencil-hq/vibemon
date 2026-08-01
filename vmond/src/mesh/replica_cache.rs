//! Controlled local cache roots for materialized shared replicas.
//!
//! A cache root is named from the exact immutable remote object identity.  The
//! collector deliberately considers only direct, well-formed children of the
//! objects directory; metadata and in-flight users are its only roots.

use std::{
	collections::{HashMap, HashSet},
	fs,
	io::ErrorKind,
	path::PathBuf,
	sync::Arc,
	time::{Duration, SystemTime},
};

use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use super::replica::ReplicaRecord;
use crate::Result;

/// Leave a short window for a concurrent metadata replacement and for a
/// process that crashed between extraction and metadata publication.
pub const CACHE_GC_GRACE: Duration = Duration::from_mins(2);

/// Owns generation-scoped replica roots and pins active consumers against GC.
#[derive(Clone, Debug)]
pub struct ReplicaCache {
	objects: PathBuf,
	in_use:  Arc<Mutex<HashMap<PathBuf, usize>>>,
}

impl ReplicaCache {
	/// Open the controlled replica objects directory.
	pub fn new(objects: PathBuf) -> Self {
		Self { objects, in_use: Arc::new(Mutex::new(HashMap::new())) }
	}

	/// Resolve a record to its immutable object-key or local-digest cache root.
	pub fn root_for(&self, sid: &str, object_key: &str, digest: &str) -> PathBuf {
		self.objects.join(cache_root_name(sid, object_key, digest))
	}

	/// Retain this exact immutable object root until the returned guard drops.
	pub fn acquire(&self, sid: &str, object_key: &str, digest: &str) -> ReplicaCacheUse {
		self.acquire_root(self.root_for(sid, object_key, digest))
	}

	/// Whether an exact cache root is currently protected by a restore or
	/// materialization guard.
	pub fn is_in_use(&self, sid: &str, object_key: &str, digest: &str) -> bool {
		self
			.in_use
			.lock()
			.contains_key(&self.root_for(sid, object_key, digest))
	}

	fn acquire_root(&self, root: PathBuf) -> ReplicaCacheUse {
		let mut in_use = self.in_use.lock();
		*in_use.entry(root.clone()).or_default() += 1;
		drop(in_use);
		ReplicaCacheUse { root, in_use: self.in_use.clone() }
	}

	/// Remove unreferenced, unpinned cache roots after the crash grace period.
	pub fn sweep(&self, records: &[ReplicaRecord]) -> Result<()> {
		self.sweep_with_grace(records, CACHE_GC_GRACE)
	}

	fn sweep_with_grace(&self, records: &[ReplicaRecord], grace: Duration) -> Result<()> {
		let roots = records
			.iter()
			.map(|record| self.root_for(&record.sid, &record.object_key, &record.digest))
			.collect::<HashSet<_>>();
		// Keep this lock through deletion: a guard either registers before its
		// root is considered stale, or starts after this sweep finishes.
		let in_use = self.in_use.lock();
		match fs::symlink_metadata(&self.objects) {
			Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {},
			Ok(_) => return Ok(()),
			Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
			Err(error) => return Err(error.into()),
		}
		let entries = fs::read_dir(&self.objects)?;
		let now = SystemTime::now();
		let mut removed = false;
		for entry in entries {
			let Ok(entry) = entry else {
				continue;
			};
			let path = entry.path();
			let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
				continue;
			};
			if !is_controlled_entry_name(&name) || roots.contains(&path) || in_use.contains_key(&path)
			{
				continue;
			}
			let Ok(metadata) = fs::symlink_metadata(&path) else {
				continue;
			};
			// Never recurse through a symlink or touch a non-directory malformed child.
			if metadata.file_type().is_symlink() || !metadata.is_dir() {
				continue;
			}
			let old_enough = metadata
				.modified()
				.ok()
				.and_then(|modified| now.duration_since(modified).ok())
				.is_some_and(|age| age >= grace);
			if !old_enough {
				continue;
			}
			match fs::remove_dir_all(&path) {
				Ok(()) => removed = true,
				Err(error) if error.kind() == ErrorKind::NotFound => {},
				Err(error) => return Err(error.into()),
			}
		}
		if removed {
			// Persist directory-entry removal, rather than merely dropping the cache
			// from the process view before a power loss.
			match fs::File::open(&self.objects).and_then(|directory| directory.sync_all()) {
				Ok(()) => {},
				Err(error) if error.kind() == ErrorKind::Unsupported => {},
				Err(error) => return Err(error.into()),
			}
		}
		Ok(())
	}
}

/// RAII root pin used while a materialization or restore consumes a cache.
pub struct ReplicaCacheUse {
	root:   PathBuf,
	in_use: Arc<Mutex<HashMap<PathBuf, usize>>>,
}

impl Drop for ReplicaCacheUse {
	fn drop(&mut self) {
		let mut in_use = self.in_use.lock();
		if let Some(count) = in_use.get_mut(&self.root) {
			*count -= 1;
			if *count == 0 {
				in_use.remove(&self.root);
			}
		}
	}
}

/// Hash the sandbox and canonical object identity into one controlled root
/// name.
pub(crate) fn cache_root_name(sid: &str, object_key: &str, digest: &str) -> String {
	let identity = if object_key.is_empty() {
		digest
	} else {
		object_key
	};
	hex::encode(Sha256::digest(format!("{sid}\0{identity}").as_bytes()))
}

fn is_cache_root_name(name: &str) -> bool {
	name.len() == 64
		&& name
			.bytes()
			.all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn is_controlled_entry_name(name: &str) -> bool {
	if is_cache_root_name(name) {
		return true;
	}
	let Some((root, partial)) = name.split_once('.') else {
		return false;
	};
	is_cache_root_name(root)
		&& partial
			.strip_suffix(".partial")
			.is_some_and(|id| uuid::Uuid::parse_str(id).is_ok())
}

#[cfg(test)]
mod tests {
	use std::{fs, os::unix::fs::symlink};

	use super::*;

	fn record(sid: &str, object_key: &str) -> ReplicaRecord {
		ReplicaRecord::new_fenced(
			sid,
			"digest",
			"source",
			1,
			1,
			"snapshot",
			Default::default(),
			false,
		)
		.unwrap()
		.with_object_key(object_key)
		.unwrap()
	}

	fn cache() -> (tempfile::TempDir, ReplicaCache) {
		let temp = tempfile::tempdir().unwrap();
		let objects = temp.path().join("objects");
		fs::create_dir(&objects).unwrap();
		(temp, ReplicaCache::new(objects))
	}

	#[test]
	fn key_rotation_retires_old_scoped_root_after_grace() {
		let (_temp, cache) = cache();
		let old = cache.root_for("sid", "old-object", "digest");
		let current = cache.root_for("sid", "new-object", "digest");
		fs::create_dir(&old).unwrap();
		fs::create_dir(&current).unwrap();
		cache
			.sweep_with_grace(&[record("sid", "new-object")], Duration::ZERO)
			.unwrap();
		assert!(!old.exists());
		assert!(current.is_dir());
	}

	#[test]
	fn in_use_root_survives_until_guard_drops() {
		let (_temp, cache) = cache();
		let root = cache.root_for("sid", "old-object", "digest");
		fs::create_dir(&root).unwrap();
		let guard = cache.acquire("sid", "old-object", "digest");
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(root.is_dir());
		drop(guard);
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(!root.exists());
	}

	#[test]
	fn blocked_publication_keeps_uncommitted_root_until_metadata_commit() {
		let (_temp, cache) = cache();
		let root = cache.root_for("target", "new-scoped-object", "digest");
		fs::create_dir(&root).unwrap();

		// This is the guard held by MeshReplicaStore::put while upload and
		// verification are deliberately blocked before PostgreSQL commit.
		let publication = cache.acquire("target", "new-scoped-object", "digest");
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(root.is_dir());

		// Commit makes the root authoritative; it remains retained after the
		// publication guard releases.
		drop(publication);
		cache
			.sweep_with_grace(&[record("target", "new-scoped-object")], Duration::ZERO)
			.unwrap();
		assert!(root.is_dir());

		// Once the authoritative metadata is removed, normal collection wins.
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(!root.exists());
	}

	#[test]
	fn shared_object_identity_remains_per_sid() {
		let (_temp, cache) = cache();
		let first = cache.root_for("first", "shared-object", "digest");
		let second = cache.root_for("second", "shared-object", "digest");
		fs::create_dir(&first).unwrap();
		fs::create_dir(&second).unwrap();
		cache
			.sweep_with_grace(
				&[record("first", "shared-object"), record("second", "shared-object")],
				Duration::ZERO,
			)
			.unwrap();
		assert!(first.is_dir());
		assert!(second.is_dir());
	}

	#[test]
	fn symlink_and_malformed_children_are_never_followed() {
		let (temp, cache) = cache();
		let outside = temp.path().join("outside");
		fs::create_dir(&outside).unwrap();
		fs::write(outside.join("keep"), "keep").unwrap();
		let link = cache.root_for("sid", "object", "digest");
		symlink(&outside, &link).unwrap();
		let malformed = cache.objects.join("not-a-cache-root");
		fs::create_dir(&malformed).unwrap();
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(link.is_symlink());
		assert!(outside.join("keep").is_file());
		assert!(malformed.is_dir());
	}

	#[test]
	fn restart_sweep_keeps_authoritative_root_and_removes_deleted_metadata_root() {
		let (_temp, cache) = cache();
		let live = cache.root_for("sid", "current-scoped-object", "digest");
		let deleted = cache.root_for("deleted-sid", "deleted-scoped-object", "digest");
		fs::create_dir(&live).unwrap();
		fs::create_dir(&deleted).unwrap();
		let partial = cache.objects.join(format!(
			"{}.{}.partial",
			cache_root_name("stale", "object", "digest"),
			uuid::Uuid::new_v4()
		));
		fs::create_dir(&partial).unwrap();
		cache
			.sweep_with_grace(&[record("sid", "current-scoped-object")], Duration::ZERO)
			.unwrap();
		assert!(live.is_dir());
		assert!(!deleted.exists());
		assert!(!partial.exists());
	}

	#[test]
	fn local_digest_identity_survives_sweep_and_is_pinnable() {
		let (_temp, cache) = cache();
		let record = ReplicaRecord::new_fenced(
			"sid",
			"local-digest",
			"source",
			1,
			1,
			"snapshot",
			Default::default(),
			false,
		)
		.unwrap();
		let root = cache.root_for(&record.sid, &record.object_key, &record.digest);
		fs::create_dir(&root).unwrap();
		let guard = cache.acquire(&record.sid, &record.object_key, &record.digest);
		assert!(cache.is_in_use(&record.sid, &record.object_key, &record.digest));
		cache
			.sweep_with_grace(std::slice::from_ref(&record), Duration::ZERO)
			.unwrap();
		assert!(root.is_dir());
		drop(guard);
		cache.sweep_with_grace(&[], Duration::ZERO).unwrap();
		assert!(!root.exists());
	}
}
