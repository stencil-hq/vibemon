//! Filesystem view of one S3 prefix (ARCHITECTURE.md §8.8).
//!
//! Layout: a file `a/b.txt` is the object `a/b.txt`; a directory is either
//! explicit (an empty `a/` marker object) or implicit (any key below
//! `a/`). POSIX attributes live in user metadata with the s3fs-fuse names
//! (`mode` = decimal `st_mode`, `uid`, `gid`, `mtime` = decimal unix
//! seconds), so buckets stay interoperable with s3fs/rclone-style tools.
//!
//! Metadata (stat and listings) is cached for a short TTL, object bytes in a
//! bounded LRU of fixed-size chunks keyed by `ETag`, so a changed object is
//! never served from stale chunks.

use std::{
	collections::{BTreeMap, HashMap},
	sync::Arc,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use parking_lot::Mutex;
use vmon_cloud::s3::{PutOptions, S3Client, S3Error};

/// Bytes per cached object chunk (and per ranged `GET`).
pub const CHUNK: u64 = 1 << 20;
/// Default chunk-cache budget per mounted prefix.
pub const DEFAULT_CACHE_BYTES: usize = 32 << 20;
/// Keys copied by one directory rename at most; larger trees fail `EXDEV`
/// so userland (`mv`) falls back to copy + delete.
pub const MAX_RENAME_KEYS: usize = 10_000;

/// Metadata names (s3fs-fuse convention).
const META_MODE: &str = "mode";
const META_UID: &str = "uid";
const META_GID: &str = "gid";
const META_MTIME: &str = "mtime";
const S_IFDIR: u32 = 0o040_000;
const S_IFREG: u32 = 0o100_000;

/// Filesystem-level failure, mapped to the proxy's wire error codes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FsError {
	NotFound,
	Exists,
	NotEmpty,
	IsDir,
	NotDir,
	/// Directory tree too large to rename server-side.
	CrossDevice,
	Perm,
	ReadOnly,
	Invalid(String),
	/// The object changed while it was being read in pieces.
	Stale,
	/// Credentials were rejected.
	Access(String),
	Io(String),
}

impl FsError {
	/// Stable proxy wire code (`vmm::remotefs::proto::ErrReply::code`).
	pub const fn code(&self) -> &'static str {
		match self {
			Self::NotFound => "not_found",
			Self::Exists => "exists",
			Self::NotEmpty => "not_empty",
			Self::IsDir => "is_dir",
			Self::NotDir => "not_dir",
			Self::CrossDevice => "xdev",
			Self::Perm => "perm",
			Self::ReadOnly => "rofs",
			Self::Invalid(_) => "bad_request",
			Self::Stale => "stale",
			Self::Access(_) => "access",
			Self::Io(_) => "io",
		}
	}
}

impl std::fmt::Display for FsError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Invalid(message) | Self::Access(message) | Self::Io(message) => {
				write!(f, "{}: {message}", self.code())
			},
			other => f.write_str(other.code()),
		}
	}
}

impl std::error::Error for FsError {}

impl From<S3Error> for FsError {
	fn from(error: S3Error) -> Self {
		match error {
			S3Error::NotFound => Self::NotFound,
			S3Error::PreconditionFailed => Self::Stale,
			S3Error::Auth(message) => Self::Access(message),
			S3Error::Throttled => Self::Io("S3 request throttled".into()),
			S3Error::Other(message) => Self::Io(message),
		}
	}
}

impl From<std::io::Error> for FsError {
	fn from(error: std::io::Error) -> Self {
		Self::Io(error.to_string())
	}
}

/// Result of filesystem operations.
pub type FsResult<T> = Result<T, FsError>;

/// Object kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
	File,
	Dir,
}

/// POSIX attributes stored in object metadata (each optional: objects
/// written by other tools carry none).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Attrs {
	/// Permission bits (`0o7777`).
	pub perm:  Option<u32>,
	pub uid:   Option<u32>,
	pub gid:   Option<u32>,
	/// Unix seconds.
	pub mtime: Option<u64>,
}

impl Attrs {
	/// Parses s3fs-style metadata.
	pub fn from_metadata(metadata: &BTreeMap<String, String>) -> Self {
		let number = |key: &str| {
			metadata
				.get(key)
				.and_then(|value| value.trim().parse::<u64>().ok())
		};
		Self {
			perm:  number(META_MODE).and_then(|mode| u32::try_from(mode & 0o7777).ok()),
			uid:   number(META_UID).and_then(|uid| u32::try_from(uid).ok()),
			gid:   number(META_GID).and_then(|gid| u32::try_from(gid).ok()),
			// s3fs writes integer seconds; tolerate fractional values.
			mtime: metadata.get(META_MTIME).and_then(|value| {
				value
					.trim()
					.split('.')
					.next()
					.and_then(|secs| secs.parse().ok())
			}),
		}
	}

	/// s3fs-style metadata for an object of `kind`; unset fields are omitted.
	pub fn to_metadata(self, kind: Kind) -> BTreeMap<String, String> {
		let mut metadata = BTreeMap::new();
		if let Some(perm) = self.perm {
			let ty = if kind == Kind::Dir { S_IFDIR } else { S_IFREG };
			metadata.insert(META_MODE.to_owned(), (ty | (perm & 0o7777)).to_string());
		}
		if let Some(uid) = self.uid {
			metadata.insert(META_UID.to_owned(), uid.to_string());
		}
		if let Some(gid) = self.gid {
			metadata.insert(META_GID.to_owned(), gid.to_string());
		}
		if let Some(mtime) = self.mtime {
			metadata.insert(META_MTIME.to_owned(), mtime.to_string());
		}
		metadata
	}

	/// `self` with the fields set in `update` replaced.
	#[must_use]
	pub fn merge(self, update: Self) -> Self {
		Self {
			perm:  update.perm.or(self.perm),
			uid:   update.uid.or(self.uid),
			gid:   update.gid.or(self.gid),
			mtime: update.mtime.or(self.mtime),
		}
	}
}

/// Attributes of one path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stat {
	pub kind:   Kind,
	pub size:   u64,
	/// Effective mtime (metadata, else `Last-Modified`), unix seconds.
	pub mtime:  u64,
	pub etag:   Option<String>,
	pub attrs:  Attrs,
	/// Directory backed by a marker object (`false` = implicit).
	pub marker: bool,
}

impl Stat {
	/// The mount root (never an object).
	pub const fn root() -> Self {
		Self {
			kind:   Kind::Dir,
			size:   0,
			mtime:  0,
			etag:   None,
			attrs:  Attrs { perm: None, uid: None, gid: None, mtime: None },
			marker: false,
		}
	}
}

/// One direct child of a listed directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListEntry {
	pub name:  String,
	pub kind:  Kind,
	pub size:  u64,
	pub mtime: u64,
	pub etag:  Option<String>,
}

/// Unix seconds now.
pub fn unix_now() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.as_secs())
}

fn unix_secs(at: Option<SystemTime>) -> u64 {
	at.and_then(|at| at.duration_since(UNIX_EPOCH).ok())
		.map_or(0, |elapsed| elapsed.as_secs())
}

/// Validates a mount-relative path (`""` = root, else `a/b` segments).
pub fn check_path(path: &str) -> FsResult<()> {
	if path.is_empty()
		|| path
			.split('/')
			.all(|seg| !seg.is_empty() && seg != "." && seg != ".." && !seg.contains('\0'))
	{
		Ok(())
	} else {
		Err(FsError::Invalid(format!("invalid path {path:?}")))
	}
}

/// Parent of a mount-relative path (`""` for top-level entries).
pub fn parent(path: &str) -> &str {
	path.rsplit_once('/').map_or("", |(parent, _)| parent)
}

/// Key prefix of a directory's children (`""` for the root).
pub fn dir_prefix(path: &str) -> String {
	if path.is_empty() {
		String::new()
	} else {
		format!("{path}/")
	}
}

/// A bounded LRU of object chunks.
struct ChunkCache {
	budget: usize,
	used:   usize,
	tick:   u64,
	chunks: HashMap<(String, String, u64), (Bytes, u64)>,
}

impl ChunkCache {
	fn get(&mut self, key: &(String, String, u64)) -> Option<Bytes> {
		self.tick += 1;
		let tick = self.tick;
		self.chunks.get_mut(key).map(|(bytes, used)| {
			*used = tick;
			bytes.clone()
		})
	}

	fn insert(&mut self, key: (String, String, u64), bytes: Bytes) {
		if bytes.len() > self.budget {
			return;
		}
		self.tick += 1;
		self.used += bytes.len();
		if let Some((old, _)) = self.chunks.insert(key, (bytes, self.tick)) {
			self.used -= old.len();
		}
		while self.used > self.budget {
			let Some(oldest) = self
				.chunks
				.iter()
				.min_by_key(|(_, (_, used))| *used)
				.map(|(key, _)| key.clone())
			else {
				break;
			};
			if let Some((bytes, _)) = self.chunks.remove(&oldest) {
				self.used -= bytes.len();
			}
		}
	}

	fn forget(&mut self, path: &str) {
		let mut freed = 0;
		self.chunks.retain(|(key, ..), (bytes, _)| {
			let keep = key != path;
			if !keep {
				freed += bytes.len();
			}
			keep
		});
		self.used -= freed;
	}
}

/// A directory listing and when it was fetched.
type CachedList = (Instant, Arc<Vec<ListEntry>>);

/// Cached view of one S3 prefix as a filesystem.
pub struct Bucket {
	client: S3Client,
	ttl:    Duration,
	stats:  Mutex<HashMap<String, (Instant, Option<Stat>)>>,
	lists:  Mutex<HashMap<String, CachedList>>,
	chunks: Mutex<ChunkCache>,
	/// Last chunk read per path, for sequential readahead.
	cursor: Mutex<HashMap<String, u64>>,
}

impl std::fmt::Debug for Bucket {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Bucket")
			.field("location", &self.client.location().url())
			.field("ttl", &self.ttl)
			.finish_non_exhaustive()
	}
}

impl Bucket {
	/// Filesystem over `client`'s location; metadata cached for `ttl`
	/// (zero disables caching), chunks up to `cache_bytes`.
	pub fn new(client: S3Client, ttl: Duration, cache_bytes: usize) -> Self {
		Self {
			client,
			ttl,
			stats: Mutex::new(HashMap::new()),
			lists: Mutex::new(HashMap::new()),
			chunks: Mutex::new(ChunkCache {
				budget: cache_bytes,
				used:   0,
				tick:   0,
				chunks: HashMap::new(),
			}),
			cursor: Mutex::new(HashMap::new()),
		}
	}

	/// The underlying client.
	pub const fn client(&self) -> &S3Client {
		&self.client
	}

	fn fresh(&self, at: Instant) -> bool {
		at.elapsed() < self.ttl
	}

	/// Drops cached metadata and bytes of `path` and its parent listing.
	pub fn invalidate(&self, path: &str) {
		self.stats.lock().remove(path);
		self.lists.lock().remove(parent(path));
		self.lists.lock().remove(path);
		self.chunks.lock().forget(path);
	}

	/// Drops every cached entry (renames of whole trees).
	pub fn invalidate_all(&self) {
		self.stats.lock().clear();
		self.lists.lock().clear();
	}

	/// Records attributes a mutation just produced.
	pub fn remember(&self, path: &str, stat: Option<Stat>) {
		if !self.ttl.is_zero() {
			self
				.stats
				.lock()
				.insert(path.to_owned(), (Instant::now(), stat));
		}
		self.lists.lock().remove(parent(path));
	}

	/// Stat from metadata (`HEAD`s); `light` answers from a fresh parent
	/// listing when one is cached (no user metadata, fewer requests).
	pub async fn stat(&self, path: &str, light: bool) -> FsResult<Option<Stat>> {
		check_path(path)?;
		if path.is_empty() {
			return Ok(Some(Stat::root()));
		}
		if let Some((at, stat)) = self.stats.lock().get(path)
			&& self.fresh(*at)
		{
			return Ok(stat.clone());
		}
		if light {
			let name = path.rsplit('/').next().unwrap_or(path);
			let listed = self
				.lists
				.lock()
				.get(parent(path))
				.filter(|(at, _)| self.fresh(*at))
				.map(|(_, list)| Arc::clone(list));
			if let Some(list) = listed {
				return Ok(list
					.iter()
					.find(|entry| entry.name == name)
					.map(|entry| Stat {
						kind:   entry.kind,
						size:   entry.size,
						mtime:  entry.mtime,
						etag:   entry.etag.clone(),
						attrs:  Attrs::default(),
						marker: false,
					}));
			}
		}
		let stat = self.stat_remote(path).await?;
		self.remember(path, stat.clone());
		Ok(stat)
	}

	async fn stat_remote(&self, path: &str) -> FsResult<Option<Stat>> {
		match self.client.head(path).await {
			Ok(meta) => {
				let attrs = Attrs::from_metadata(&meta.metadata);
				return Ok(Some(Stat {
					kind: Kind::File,
					size: meta.size,
					mtime: attrs.mtime.unwrap_or_else(|| unix_secs(meta.last_modified)),
					etag: meta.etag,
					attrs,
					marker: false,
				}));
			},
			Err(S3Error::NotFound) => {},
			Err(error) => return Err(error.into()),
		}
		let prefix = dir_prefix(path);
		match self.client.head(&prefix).await {
			Ok(meta) => {
				let attrs = Attrs::from_metadata(&meta.metadata);
				return Ok(Some(Stat {
					kind: Kind::Dir,
					size: 0,
					mtime: attrs.mtime.unwrap_or_else(|| unix_secs(meta.last_modified)),
					etag: None,
					attrs,
					marker: true,
				}));
			},
			Err(S3Error::NotFound) => {},
			Err(error) => return Err(error.into()),
		}
		let page = self.client.list(&prefix, None, None, Some(1)).await?;
		Ok((!page.objects.is_empty() || !page.common_prefixes.is_empty()).then(|| Stat {
			kind:   Kind::Dir,
			size:   0,
			mtime:  0,
			etag:   None,
			attrs:  Attrs::default(),
			marker: false,
		}))
	}

	/// Direct children of the directory `path`, sorted by name. A name that
	/// is both an object and a key prefix lists as a directory.
	pub async fn list(&self, path: &str) -> FsResult<Arc<Vec<ListEntry>>> {
		check_path(path)?;
		if let Some((at, list)) = self.lists.lock().get(path)
			&& self.fresh(*at)
		{
			return Ok(Arc::clone(list));
		}
		let prefix = dir_prefix(path);
		let mut entries: BTreeMap<String, ListEntry> = BTreeMap::new();
		let mut token: Option<String> = None;
		loop {
			let page = self
				.client
				.list(&prefix, Some("/"), token.as_deref(), Some(1000))
				.await?;
			for object in page.objects {
				let Some(name) = object.key.strip_prefix(&prefix) else {
					continue;
				};
				// Some services return a child's directory marker (`name/`) as an
				// object instead of a common prefix.
				if let Some(dir) = name.strip_suffix('/')
					&& !dir.is_empty()
					&& !dir.contains('/')
					&& check_path(dir).is_ok()
				{
					entries.insert(dir.to_owned(), ListEntry {
						name:  dir.to_owned(),
						kind:  Kind::Dir,
						size:  0,
						mtime: 0,
						etag:  None,
					});
					continue;
				}
				if name.is_empty() || name.contains('/') || check_path(name).is_err() {
					continue;
				}
				entries.entry(name.to_owned()).or_insert_with(|| ListEntry {
					name:  name.to_owned(),
					kind:  Kind::File,
					size:  object.size,
					mtime: unix_secs(object.last_modified),
					etag:  object.etag,
				});
			}
			for common in page.common_prefixes {
				let Some(name) = common
					.strip_prefix(&prefix)
					.and_then(|rest| rest.strip_suffix('/'))
				else {
					continue;
				};
				if name.is_empty() || name.contains('/') || check_path(name).is_err() {
					continue;
				}
				entries.insert(name.to_owned(), ListEntry {
					name:  name.to_owned(),
					kind:  Kind::Dir,
					size:  0,
					mtime: 0,
					etag:  None,
				});
			}
			match page.next {
				Some(next) => token = Some(next),
				None => break,
			}
		}
		if entries.is_empty() && !path.is_empty() && self.stat(path, false).await?.is_none() {
			return Err(FsError::NotFound);
		}
		let list = Arc::new(entries.into_values().collect::<Vec<_>>());
		if !self.ttl.is_zero() {
			self
				.lists
				.lock()
				.insert(path.to_owned(), (Instant::now(), Arc::clone(&list)));
		}
		Ok(list)
	}

	/// One cached chunk of the object `path` at `etag`.
	async fn chunk(&self, path: &str, etag: &str, index: u64) -> FsResult<Bytes> {
		let key = (path.to_owned(), etag.to_owned(), index);
		let cached = self.chunks.lock().get(&key);
		if let Some(bytes) = cached {
			return Ok(bytes);
		}
		tracing::debug!(path, chunk = index, "s3fs: fetching object chunk");
		let bytes = self
			.client
			.get_range_if_match(path, index * CHUNK, CHUNK, etag)
			.await?;
		self.chunks.lock().insert(key, bytes.clone());
		Ok(bytes)
	}

	/// Reads `len` bytes at `offset` of the file `path` described by `stat`
	/// through the chunk cache; sequential reads prefetch the next chunk.
	pub async fn read(
		self: &Arc<Self>,
		path: &str,
		stat: &Stat,
		offset: u64,
		len: u32,
	) -> FsResult<Vec<u8>> {
		if offset >= stat.size || len == 0 {
			return Ok(Vec::new());
		}
		let end = (offset + u64::from(len)).min(stat.size);
		let Some(etag) = stat.etag.clone() else {
			let bytes = self.client.get_range(path, offset, end - offset).await?;
			return Ok(bytes.to_vec());
		};
		let first = offset / CHUNK;
		let last = (end - 1) / CHUNK;
		let mut out = Vec::with_capacity(usize::try_from(end - offset).unwrap_or(0));
		for index in first..=last {
			let chunk = self.chunk(path, &etag, index).await?;
			let start = offset.max(index * CHUNK) - index * CHUNK;
			let stop = (end.min((index + 1) * CHUNK) - index * CHUNK).min(chunk.len() as u64);
			if start >= stop {
				break;
			}
			out.extend_from_slice(&chunk[start as usize..stop as usize]);
		}
		let sequential = {
			let mut cursor = self.cursor.lock();
			let previous = cursor.insert(path.to_owned(), last);
			previous.is_some_and(|prev| prev + 1 >= first && prev <= last)
		};
		let next = last + 1;
		if sequential && next * CHUNK < stat.size {
			let bucket = Arc::clone(self);
			let path = path.to_owned();
			tokio::spawn(async move {
				if let Err(error) = bucket.chunk(&path, &etag, next).await {
					tracing::debug!(path, %error, "s3fs: readahead failed");
				}
			});
		}
		Ok(out)
	}

	/// Writes a directory marker with `attrs`.
	pub async fn put_marker(&self, path: &str, attrs: Attrs) -> FsResult<()> {
		let options = PutOptions { metadata: attrs.to_metadata(Kind::Dir), ..PutOptions::default() };
		self
			.client
			.put(&dir_prefix(path), Bytes::new(), &options)
			.await?;
		Ok(())
	}

	/// Replaces the user metadata of the object `path` in place.
	pub async fn set_metadata(&self, path: &str, attrs: Attrs) -> FsResult<String> {
		let metadata = attrs.to_metadata(Kind::File);
		Ok(self.client.copy(path, path, Some(&metadata)).await?)
	}

	/// Makes an implicit directory explicit so it survives the removal of
	/// its last child.
	pub async fn ensure_marker(&self, path: &str) -> FsResult<()> {
		if path.is_empty() {
			return Ok(());
		}
		match self.stat(path, false).await? {
			Some(stat) if stat.kind == Kind::Dir && !stat.marker => {
				self.put_marker(path, Attrs::default()).await?;
				let marked = Stat { marker: true, ..stat };
				self.remember(path, Some(marked));
				Ok(())
			},
			_ => Ok(()),
		}
	}

	/// Every key below the directory `path` (recursive), up to `limit`.
	pub async fn keys_below(&self, path: &str, limit: usize) -> FsResult<Vec<String>> {
		let prefix = dir_prefix(path);
		let mut keys = Vec::new();
		let mut token: Option<String> = None;
		loop {
			let page = self
				.client
				.list(&prefix, None, token.as_deref(), Some(1000))
				.await?;
			keys.extend(page.objects.into_iter().map(|object| object.key));
			if keys.len() > limit {
				return Err(FsError::CrossDevice);
			}
			match page.next {
				Some(next) => token = Some(next),
				None => return Ok(keys),
			}
		}
	}

	/// Whether the directory `path` has any child object or prefix.
	pub async fn has_children(&self, path: &str) -> FsResult<bool> {
		let prefix = dir_prefix(path);
		let page = self.client.list(&prefix, None, None, Some(2)).await?;
		Ok(page.objects.iter().any(|object| object.key != prefix))
	}
}

#[cfg(test)]
mod tests {
	use vmon_cloud::s3::fake::FakeS3;

	use super::*;

	#[test]
	fn metadata_round_trip_uses_s3fs_names() {
		let attrs =
			Attrs { perm: Some(0o640), uid: Some(1000), gid: Some(1001), mtime: Some(42) };
		let metadata = attrs.to_metadata(Kind::File);
		assert_eq!(metadata["mode"], (0o100_640).to_string());
		assert_eq!(Attrs::from_metadata(&metadata), attrs);
		let dir = attrs.to_metadata(Kind::Dir);
		assert_eq!(dir["mode"], (0o040_640).to_string());
		let fractional = BTreeMap::from([("mtime".to_owned(), "17.5".to_owned())]);
		assert_eq!(Attrs::from_metadata(&fractional).mtime, Some(17));
		assert_eq!(Attrs::default().to_metadata(Kind::File), BTreeMap::new());
	}

	#[test]
	fn chunk_cache_evicts_least_recent() {
		let mut cache = ChunkCache { budget: 10, used: 0, tick: 0, chunks: HashMap::new() };
		let key = |n| ("p".to_owned(), "e".to_owned(), n);
		cache.insert(key(0), Bytes::from_static(b"aaaa"));
		cache.insert(key(1), Bytes::from_static(b"bbbb"));
		assert!(cache.get(&key(0)).is_some());
		cache.insert(key(2), Bytes::from_static(b"cccc"));
		assert!(cache.get(&key(1)).is_none(), "least recently used chunk evicted");
		assert!(cache.get(&key(0)).is_some() && cache.get(&key(2)).is_some());
		assert_eq!(cache.used, 8);
		cache.forget("p");
		assert_eq!((cache.used, cache.chunks.len()), (0, 0));
	}

	#[tokio::test]
	async fn stat_list_and_lazy_ranged_reads() {
		let fake = FakeS3::start().await;
		fake.insert("bkt/data/a.txt", vec![b'a'; (CHUNK as usize) + 10]);
		fake.insert("bkt/data/dir/b.txt", "nested");
		fake.insert("bkt/data/empty/", "");
		fake.insert("bkt/data/untouched.bin", vec![0u8; 4096]);
		let bucket = Arc::new(Bucket::new(
			fake.client("bkt", "data"),
			Duration::from_secs(5),
			DEFAULT_CACHE_BYTES,
		));

		let names: Vec<_> = bucket
			.list("")
			.await
			.unwrap()
			.iter()
			.map(|entry| (entry.name.clone(), entry.kind))
			.collect();
		assert_eq!(names, [
			("a.txt".to_owned(), Kind::File),
			("dir".to_owned(), Kind::Dir),
			("empty".to_owned(), Kind::Dir),
			("untouched.bin".to_owned(), Kind::File),
		]);
		assert_eq!(bucket.stat("dir", false).await.unwrap().unwrap().kind, Kind::Dir);
		assert!(bucket.stat("empty", false).await.unwrap().unwrap().marker);
		assert_eq!(bucket.stat("missing", false).await.unwrap(), None);
		assert_eq!(bucket.list("missing").await.unwrap_err(), FsError::NotFound);

		let stat = bucket.stat("a.txt", false).await.unwrap().unwrap();
		let tail = bucket.read("a.txt", &stat, CHUNK - 2, 64).await.unwrap();
		assert_eq!(tail, vec![b'a'; 12]);
		// Second read is served from the chunk cache.
		let before = fake.requests().len();
		bucket.read("a.txt", &stat, 0, 16).await.unwrap();
		assert_eq!(fake.requests().len(), before);
		let fetched: Vec<_> = fake
			.requests()
			.into_iter()
			.filter(|request| {
				request.method == http::Method::GET && !request.query.contains_key("list-type")
			})
			.map(|request| request.path)
			.collect();
		assert!(fetched.iter().all(|path| path == "bkt/data/a.txt"), "{fetched:?}");
	}
}
