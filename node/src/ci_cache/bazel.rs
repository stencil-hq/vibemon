//! Node-local Bazel HTTP remote cache for CI sandboxes.
//!
//! Bazel's HTTP cache protocol (`--remote_cache=http://<gateway>:7948/bazel`)
//! is `GET|HEAD|PUT /bazel/{ac,cas}/<sha256>`. Entries are per repository
//! (the repository id of the sandbox's grant) and live only on this node,
//! without a cluster index (builds issue 10⁵+ small entries):
//! `<data-dir>/ci-cache/bazel/<repo id>/{ac,cas}/<aa>/<sha256>`, written to
//! `tmp/`, fsync'd, and renamed into place. Least recently used entries are
//! evicted beyond the node's budget (`--ci-bazel-cache-mb`); recency is kept
//! in memory and starts from the files' modification times after a restart.
//! An open file survives its eviction (readers hold the descriptor).

use std::{
	collections::{BTreeMap, HashMap},
	fs, io,
	path::{Path, PathBuf},
	sync::Arc,
	time::SystemTime,
};

use anyhow::{Context as _, Result};
use axum::body::Body;
use futures::TryStreamExt as _;
use http::StatusCode;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use super::records::valid_digest;

/// The two Bazel cache namespaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
	/// Action cache: action digest → `ActionResult` (not content-addressed).
	Ac,
	/// Content-addressable store: sha256 of the body → body (verified).
	Cas,
}

impl Kind {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Ac => "ac",
			Self::Cas => "cas",
		}
	}

	pub fn parse(kind: &str) -> Option<Self> {
		match kind {
			"ac" => Some(Self::Ac),
			"cas" => Some(Self::Cas),
			_ => None,
		}
	}
}

/// A failed write (status and message for the response).
#[derive(Debug)]
pub struct PutError {
	pub status:  StatusCode,
	pub message: String,
}

impl PutError {
	fn new(status: StatusCode, message: impl Into<String>) -> Self {
		Self { status, message: message.into() }
	}

	fn internal(error: impl std::fmt::Display) -> Self {
		Self::new(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
	}
}

#[derive(Debug, Default)]
struct State {
	/// Relative path → (size, recency tick).
	files: HashMap<String, (u64, u64)>,
	/// Recency tick → relative path (oldest first).
	order: BTreeMap<u64, String>,
	tick:  u64,
	total: u64,
}

impl State {
	fn touch(&mut self, rel: &str) -> Option<u64> {
		let tick = self.tick + 1;
		let (size, old) = self.files.get_mut(rel).map(|entry| {
			let old = entry.1;
			entry.1 = tick;
			(entry.0, old)
		})?;
		self.tick = tick;
		self.order.remove(&old);
		self.order.insert(tick, rel.to_owned());
		Some(size)
	}

	fn insert(&mut self, rel: String, size: u64) {
		self.tick += 1;
		if let Some((old_size, old_tick)) = self.files.insert(rel.clone(), (size, self.tick)) {
			self.order.remove(&old_tick);
			self.total -= old_size;
		}
		self.order.insert(self.tick, rel);
		self.total += size;
	}
}

/// The node's Bazel cache entries.
#[derive(Debug)]
pub struct BazelStore {
	root:   PathBuf,
	budget: u64,
	state:  Mutex<State>,
}

/// Relative path of an entry.
fn rel(repo_id: u64, kind: Kind, hash: &str) -> String {
	format!("{repo_id}/{}/{}/{hash}", kind.as_str(), &hash[..2])
}

impl BazelStore {
	/// Opens `root` (creating it): indexes the entries oldest-modified first
	/// and clears `tmp/`. Blocking.
	pub fn open(root: &Path, budget: u64) -> Result<Arc<Self>> {
		let tmp = root.join("tmp");
		match fs::remove_dir_all(&tmp) {
			Ok(()) => {},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error).with_context(|| format!("clearing {}", tmp.display())),
		}
		fs::create_dir_all(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
		let mut found = Vec::new();
		for repo in fs::read_dir(root)? {
			let repo = repo?;
			let Some(repo_id) = repo
				.file_name()
				.to_str()
				.and_then(|name| name.parse::<u64>().ok())
			else {
				continue;
			};
			for kind in [Kind::Ac, Kind::Cas] {
				let Ok(shards) = fs::read_dir(repo.path().join(kind.as_str())) else {
					continue;
				};
				for shard in shards {
					for entry in fs::read_dir(shard?.path())? {
						let entry = entry?;
						let meta = entry.metadata()?;
						let name = entry.file_name().to_string_lossy().into_owned();
						if !meta.is_file() || !valid_digest(&name) {
							let _ = fs::remove_file(entry.path());
							continue;
						}
						let modified = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
						found.push((modified, rel(repo_id, kind, &name), meta.len()));
					}
				}
			}
		}
		found.sort();
		let mut state = State::default();
		for (_, rel, size) in found {
			state.insert(rel, size);
		}
		Ok(Arc::new(Self { root: root.to_owned(), budget, state: Mutex::new(state) }))
	}

	/// Bytes the entries may occupy.
	pub const fn budget(&self) -> u64 {
		self.budget
	}

	/// Bytes the entries occupy.
	pub fn total(&self) -> u64 {
		self.state.lock().total
	}

	fn path(&self, rel: &str) -> PathBuf {
		self.root.join(rel)
	}

	/// The size of an entry, marking it used.
	pub fn touch(&self, repo_id: u64, kind: Kind, hash: &str) -> Option<u64> {
		self.state.lock().touch(&rel(repo_id, kind, hash))
	}

	/// Opens an entry for reading (marking it used): the file and its size.
	pub async fn open_entry(
		&self,
		repo_id: u64,
		kind: Kind,
		hash: &str,
	) -> Option<(tokio::fs::File, u64)> {
		let rel = rel(repo_id, kind, hash);
		self.state.lock().touch(&rel)?;
		if let Ok(file) = tokio::fs::File::open(self.path(&rel)).await {
			let size = file.metadata().await.ok()?.len();
			return Some((file, size));
		}
		// Removed underneath the index: forget it.
		let mut state = self.state.lock();
		if let Some((size, tick)) = state.files.remove(&rel) {
			state.order.remove(&tick);
			state.total -= size;
		}
		None
	}

	/// Writes an entry from `body` (temp file, fsync, rename). A CAS body
	/// must hash to `hash` (400 otherwise); bodies over half the budget are
	/// refused (413).
	pub async fn put(
		self: &Arc<Self>,
		repo_id: u64,
		kind: Kind,
		hash: &str,
		body: Body,
	) -> Result<u64, PutError> {
		let limit = self.budget / 2;
		let tmp = self
			.root
			.join("tmp")
			.join(format!("{:016x}", rand::random::<u64>()));
		let written = async {
			let mut file = tokio::fs::File::create(&tmp)
				.await
				.map_err(PutError::internal)?;
			let mut hasher = Sha256::new();
			let mut size = 0u64;
			let mut stream = body.into_data_stream().map_err(io::Error::other);
			while let Some(chunk) = stream.try_next().await.map_err(|error| {
				PutError::new(StatusCode::BAD_REQUEST, format!("upload interrupted: {error}"))
			})? {
				size += chunk.len() as u64;
				if size > limit {
					return Err(PutError::new(
						StatusCode::PAYLOAD_TOO_LARGE,
						format!("entries are limited to {limit} bytes on this node"),
					));
				}
				hasher.update(&chunk);
				file.write_all(&chunk).await.map_err(PutError::internal)?;
			}
			if kind == Kind::Cas {
				let digest = hex::encode(hasher.finalize());
				if digest != hash {
					return Err(PutError::new(
						StatusCode::BAD_REQUEST,
						format!("the body's sha256 is {digest}, not {hash}"),
					));
				}
			}
			file.sync_all().await.map_err(PutError::internal)?;
			Ok(size)
		}
		.await;
		let size = match written {
			Ok(size) => size,
			Err(error) => {
				let _ = tokio::fs::remove_file(&tmp).await;
				return Err(error);
			},
		};
		let (store, rel) = (Arc::clone(self), rel(repo_id, kind, hash));
		tokio::task::spawn_blocking(move || store.install(&tmp, rel, size))
			.await
			.map_err(PutError::internal)?
			.map_err(PutError::internal)?;
		Ok(size)
	}

	/// Renames `tmp` into place, indexes it, and evicts beyond the budget.
	/// Blocking.
	fn install(&self, tmp: &Path, rel: String, size: u64) -> Result<()> {
		let dest = self.path(&rel);
		if let Some(dir) = dest.parent() {
			fs::create_dir_all(dir)?;
		}
		if let Err(error) = fs::rename(tmp, &dest) {
			let _ = fs::remove_file(tmp);
			return Err(error).with_context(|| format!("installing {}", dest.display()));
		}
		self.state.lock().insert(rel, size);
		self.evict();
		Ok(())
	}

	/// Removes least recently used entries until the entries fit the budget.
	/// Returns how many went. Blocking.
	pub fn evict(&self) -> usize {
		let victims = {
			let mut state = self.state.lock();
			let mut victims = Vec::new();
			while state.total > self.budget {
				let Some((_, rel)) = state.order.pop_first() else {
					break;
				};
				if let Some((size, _)) = state.files.remove(&rel) {
					state.total -= size;
				}
				victims.push(rel);
			}
			victims
		};
		for rel in &victims {
			let _ = fs::remove_file(self.path(rel));
		}
		victims.len()
	}
}

#[cfg(test)]
mod tests {
	use http_body_util::BodyExt as _;

	use super::*;

	fn sha(data: &[u8]) -> String {
		hex::encode(Sha256::digest(data))
	}

	async fn read(store: &BazelStore, repo: u64, kind: Kind, hash: &str) -> Option<Vec<u8>> {
		let (file, size) = store.open_entry(repo, kind, hash).await?;
		let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
		let bytes = body.collect().await.unwrap().to_bytes().to_vec();
		assert_eq!(bytes.len() as u64, size);
		Some(bytes)
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn entries_are_verified_namespaced_and_evicted_lru() {
		let dir = tempfile::tempdir().unwrap();
		let store = BazelStore::open(dir.path(), 30).unwrap();
		let (a, b, c) =
			(b"aaaaaaaaaa".as_slice(), b"bbbbbbbbbb".as_slice(), b"cccccccccc".as_slice());
		store
			.put(1, Kind::Cas, &sha(a), Body::from(a))
			.await
			.unwrap();
		// A CAS body must match its key.
		let error = store
			.put(1, Kind::Cas, &sha(c), Body::from(a))
			.await
			.unwrap_err();
		assert_eq!(error.status, StatusCode::BAD_REQUEST);
		// AC values are not content-addressed.
		store
			.put(1, Kind::Ac, &sha(b"action"), Body::from(b))
			.await
			.unwrap();
		assert_eq!(read(&store, 1, Kind::Cas, &sha(a)).await.unwrap(), a);
		assert!(read(&store, 2, Kind::Cas, &sha(a)).await.is_none(), "per repository");
		assert!(read(&store, 1, Kind::Ac, &sha(a)).await.is_none(), "per namespace");
		assert!(
			dir.path()
				.join(format!("1/cas/{}/{}", &sha(a)[..2], sha(a)))
				.is_file()
		);
		// `a` was used after the action result: a fourth entry over the
		// budget evicts the action result (the least recently used).
		store
			.put(2, Kind::Cas, &sha(c), Body::from(c))
			.await
			.unwrap();
		assert_eq!(store.total(), 30);
		assert!(read(&store, 1, Kind::Cas, &sha(a)).await.is_some());
		let d = b"dddddddddd".as_slice();
		store
			.put(1, Kind::Cas, &sha(d), Body::from(d))
			.await
			.unwrap();
		assert_eq!(store.total(), 30);
		assert!(read(&store, 1, Kind::Ac, &sha(b"action")).await.is_none(), "evicted");
		assert!(store.touch(2, Kind::Cas, &sha(c)).is_some());
		// Too large for the budget.
		let big = vec![7u8; 20];
		let error = store
			.put(1, Kind::Cas, &sha(&big), Body::from(big.clone()))
			.await
			.unwrap_err();
		assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
		assert_eq!(fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
		// Reopening re-indexes what is left.
		drop(store);
		let store = BazelStore::open(dir.path(), 30).unwrap();
		assert_eq!(store.total(), 30);
		assert_eq!(read(&store, 1, Kind::Cas, &sha(d)).await.unwrap(), d);
	}
}
