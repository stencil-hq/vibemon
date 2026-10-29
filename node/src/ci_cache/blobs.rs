//! Local archive copies under `<data-dir>/ci-cache/`.
//!
//! `blobs/<sha256>` holds complete, verified archives, `tmp/` archives being
//! written, and `uploads/<token>/` staged Azure blocks. Files are written to
//! `tmp/`, fsync'd, and renamed into `blobs/`. The modification time of a
//! blob is its last use (LRU across restarts); copies being read or written
//! are pinned and never evicted.

use std::{
	collections::HashMap,
	fs, io,
	path::{Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};

use anyhow::{Context as _, Result};
use parking_lot::Mutex;

use super::records::valid_digest;

/// One local copy.
#[derive(Clone, Copy, Debug)]
struct Local {
	size:      u64,
	last_used: SystemTime,
}

#[derive(Debug, Default)]
struct State {
	files: HashMap<String, Local>,
	pins:  HashMap<String, usize>,
	total: u64,
}

/// The node's archive copies.
#[derive(Debug)]
pub struct BlobStore {
	root:   PathBuf,
	budget: u64,
	state:  Mutex<State>,
}

/// Keeps a copy from being evicted while it is read or written.
#[derive(Debug)]
pub struct Pin {
	store:  Arc<BlobStore>,
	digest: String,
}

impl Drop for Pin {
	fn drop(&mut self) {
		let mut state = self.store.state.lock();
		if let Some(count) = state.pins.get_mut(&self.digest) {
			*count -= 1;
			if *count == 0 {
				state.pins.remove(&self.digest);
			}
		}
	}
}

impl BlobStore {
	/// Opens `root` (creating it): indexes `blobs/`, clears `tmp/` and
	/// `uploads/` (a previous process's unfinished writes).
	pub fn open(root: &Path, budget: u64) -> Result<Arc<Self>> {
		for dir in ["tmp", "uploads"] {
			let path = root.join(dir);
			match fs::remove_dir_all(&path) {
				Ok(()) => {},
				Err(error) if error.kind() == io::ErrorKind::NotFound => {},
				Err(error) => {
					return Err(error).with_context(|| format!("clearing {}", path.display()));
				},
			}
		}
		for dir in ["blobs", "tmp", "uploads"] {
			let path = root.join(dir);
			fs::create_dir_all(&path).with_context(|| format!("creating {}", path.display()))?;
		}
		let mut state = State::default();
		for entry in fs::read_dir(root.join("blobs"))? {
			let entry = entry?;
			let name = entry.file_name().to_string_lossy().into_owned();
			let meta = entry.metadata()?;
			if !valid_digest(&name) || !meta.is_file() {
				let _ = fs::remove_file(entry.path());
				continue;
			}
			state.total += meta.len();
			state.files.insert(name, Local {
				size:      meta.len(),
				last_used: meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
			});
		}
		Ok(Arc::new(Self { root: root.to_owned(), budget, state: Mutex::new(state) }))
	}

	/// Bytes the local copies may occupy.
	pub const fn budget(&self) -> u64 {
		self.budget
	}

	/// Bytes the local copies occupy.
	pub fn total(&self) -> u64 {
		self.state.lock().total
	}

	/// Path of a complete copy.
	pub fn path(&self, digest: &str) -> PathBuf {
		self.root.join("blobs").join(digest)
	}

	/// A fresh path under `tmp/`.
	pub fn temp_path(&self) -> PathBuf {
		self
			.root
			.join("tmp")
			.join(format!("{:016x}", rand::random::<u64>()))
	}

	/// Directory of an upload's staged blocks.
	pub fn upload_dir(&self, token: &str) -> PathBuf {
		self.root.join("uploads").join(token)
	}

	/// Pins `digest` (whether or not a copy exists yet).
	pub fn pin(self: &Arc<Self>, digest: &str) -> Pin {
		*self.state.lock().pins.entry(digest.to_owned()).or_default() += 1;
		Pin { store: Arc::clone(self), digest: digest.to_owned() }
	}

	/// The size of a local copy, marking it used.
	pub fn touch(&self, digest: &str) -> Option<u64> {
		let now = SystemTime::now();
		let size = {
			let mut state = self.state.lock();
			let local = state.files.get_mut(digest)?;
			local.last_used = now;
			local.size
		};
		if let Ok(file) = fs::File::options().write(true).open(self.path(digest)) {
			let _ = file.set_modified(now);
		}
		Some(size)
	}

	/// Whether a complete copy of `digest` is here.
	pub fn contains(&self, digest: &str) -> bool {
		self.state.lock().files.contains_key(digest)
	}

	/// Installs the fsync'd file `tmp` as the copy of `digest` (`size`
	/// bytes). An existing copy wins (same content); `tmp` is then removed.
	/// Blocking.
	pub fn install(&self, tmp: &Path, digest: &str, size: u64) -> Result<()> {
		let dest = self.path(digest);
		if self.contains(digest) {
			let _ = fs::remove_file(tmp);
			return Ok(());
		}
		fs::rename(tmp, &dest).with_context(|| format!("installing {}", dest.display()))?;
		if let Ok(dir) = fs::File::open(self.root.join("blobs")) {
			let _ = dir.sync_all();
		}
		let mut state = self.state.lock();
		if state
			.files
			.insert(digest.to_owned(), Local { size, last_used: SystemTime::now() })
			.is_none()
		{
			state.total += size;
		}
		Ok(())
	}

	/// Removes the unpinned copy of `digest`; `false` when pinned or absent.
	/// Blocking.
	pub fn remove(&self, digest: &str) -> bool {
		{
			let mut state = self.state.lock();
			if state.pins.contains_key(digest) {
				return false;
			}
			let Some(local) = state.files.remove(digest) else {
				return false;
			};
			state.total -= local.size;
		}
		let _ = fs::remove_file(self.path(digest));
		true
	}

	/// Removes least recently used unpinned copies until the copies fit the
	/// budget with `incoming` more bytes. Returns the evicted digests.
	/// Blocking.
	pub fn evict(&self, incoming: u64) -> Vec<String> {
		let victims = {
			let state = self.state.lock();
			let mut total = state.total + incoming;
			if total <= self.budget {
				return Vec::new();
			}
			let mut files: Vec<_> = state
				.files
				.iter()
				.filter(|(digest, _)| !state.pins.contains_key(*digest))
				.map(|(digest, local)| (local.last_used, digest.clone(), local.size))
				.collect();
			files.sort();
			let mut victims = Vec::new();
			for (_, digest, size) in files {
				if total <= self.budget {
					break;
				}
				total -= size;
				victims.push(digest);
			}
			victims
		};
		victims
			.into_iter()
			.filter(|digest| self.remove(digest))
			.collect()
	}

	/// Digests of the local copies.
	pub fn digests(&self) -> Vec<String> {
		self.state.lock().files.keys().cloned().collect()
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;

	fn add(store: &BlobStore, digest: &str, bytes: &[u8]) {
		let tmp = store.temp_path();
		fs::write(&tmp, bytes).unwrap();
		store.install(&tmp, digest, bytes.len() as u64).unwrap();
	}

	#[test]
	fn eviction_takes_least_recently_used_unpinned_copies() {
		let dir = tempfile::tempdir().unwrap();
		let store = BlobStore::open(dir.path(), 10).unwrap();
		let (a, b, c) = ("a".repeat(64), "b".repeat(64), "c".repeat(64));
		add(&store, &a, b"aaaa");
		std::thread::sleep(Duration::from_millis(5));
		add(&store, &b, b"bbbb");
		std::thread::sleep(Duration::from_millis(5));
		add(&store, &c, b"cc");
		assert_eq!(store.total(), 10);
		assert!(store.evict(0).is_empty(), "within budget");
		// `a` is the oldest but pinned (being read): `b` goes instead.
		let pin = store.pin(&a);
		assert_eq!(store.evict(4), vec![b.clone()]);
		assert!(!store.path(&b).exists() && store.contains(&a));
		drop(pin);
		store.touch(&a);
		assert_eq!(store.evict(5), vec![c]);
		assert_eq!(store.total(), 4);
		// Reopening re-indexes the remaining copy and clears temp files.
		fs::write(store.temp_path(), b"partial").unwrap();
		drop(store);
		let store = BlobStore::open(dir.path(), 10).unwrap();
		assert_eq!(store.digests(), vec![a]);
		assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
	}
}
