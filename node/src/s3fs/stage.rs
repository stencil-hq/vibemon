//! Write-back staging of a writable S3 mount (ARCHITECTURE.md §8.8).
//!
//! The first write (or truncate) of a file stages it in a sparse local file.
//! Existing object bytes are hydrated lazily, one [`CHUNK`] at a time and
//! only where a partial write or a read needs them, with `If-Match` on the
//! staged `ETag`. `close`/`fsync` upload the whole file (multipart above the
//! client's part size) with its POSIX attributes as metadata; uploads that
//! outlast [`SYNC_BUDGET`] continue in the background so a large upload
//! never stalls the VM's device thread, and pause/kill wait for them.

use std::{
	collections::{HashMap, HashSet},
	fs::{File, OpenOptions},
	os::unix::fs::FileExt,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use futures::TryStreamExt;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use vmon_cloud::s3::PutOptions;

use super::bucket::{Attrs, Bucket, CHUNK, FsError, FsResult, Kind, Stat, parent, unix_now};

/// How long `close`/`fsync` wait for their upload before letting it finish in
/// the background.
pub const SYNC_BUDGET: Duration = Duration::from_secs(1);

/// The object a staged file was hydrated from.
#[derive(Clone, Debug)]
struct Origin {
	etag:  Option<String>,
	/// Object bytes still valid below this offset (truncation lowers it).
	valid: u64,
}

#[derive(Debug)]
struct StagedState {
	size:       u64,
	origin:     Option<Origin>,
	present:    HashSet<u64>,
	dirty:      bool,
	/// Bumped by every change; an upload only cleans the generation it read.
	generation: u64,
	attrs:      Attrs,
	error:      Option<String>,
}

/// One staged file.
#[derive(Debug)]
struct Staged {
	file:   File,
	disk:   PathBuf,
	state:  Mutex<StagedState>,
	/// Serializes uploads of this file.
	upload: tokio::sync::Mutex<()>,
}

impl Staged {
	fn stat(&self) -> Stat {
		let state = self.state.lock();
		Stat {
			kind:   Kind::File,
			size:   state.size,
			mtime:  state.attrs.mtime.unwrap_or(0),
			etag:   None,
			attrs:  state.attrs,
			marker: false,
		}
	}
}

impl Drop for Staged {
	fn drop(&mut self) {
		let _ = std::fs::remove_file(&self.disk);
	}
}

/// Staged files of one writable mount.
pub struct Stage {
	bucket: Arc<Bucket>,
	dir:    PathBuf,
	files:  Mutex<HashMap<String, Arc<Staged>>>,
	next:   AtomicU64,
}

impl std::fmt::Debug for Stage {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Stage")
			.field("dir", &self.dir)
			.finish_non_exhaustive()
	}
}

impl Stage {
	/// Staging under `dir` (created, emptied of leftovers).
	///
	/// # Errors
	///
	/// Fails when the directory cannot be created.
	pub fn new(bucket: Arc<Bucket>, dir: PathBuf) -> std::io::Result<Self> {
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir)?;
		Ok(Self { bucket, dir, files: Mutex::new(HashMap::new()), next: AtomicU64::new(0) })
	}

	/// Staging directory.
	pub fn dir(&self) -> &Path {
		&self.dir
	}

	fn get(&self, path: &str) -> Option<Arc<Staged>> {
		self.files.lock().get(path).cloned()
	}

	fn new_staged(&self, state: StagedState) -> FsResult<Arc<Staged>> {
		let disk = self
			.dir
			.join(self.next.fetch_add(1, Ordering::Relaxed).to_string());
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create_new(true)
			.open(&disk)?;
		file.set_len(state.size)?;
		Ok(Arc::new(Staged {
			file,
			disk,
			state: Mutex::new(state),
			upload: tokio::sync::Mutex::new(()),
		}))
	}

	/// Attributes of a staged file.
	pub fn stat(&self, path: &str) -> Option<Stat> {
		self.get(path).map(|staged| staged.stat())
	}

	/// Staged files directly inside the directory `dir`.
	#[expect(
		clippy::needless_collect,
		reason = "snapshot the entries so the files lock is released before `stat` runs"
	)]
	pub fn children(&self, dir: &str) -> Vec<(String, Stat)> {
		let staged: Vec<(String, Arc<Staged>)> = self
			.files
			.lock()
			.iter()
			.filter(|(path, _)| parent(path) == dir)
			.map(|(path, staged)| (path.clone(), Arc::clone(staged)))
			.collect();
		staged
			.into_iter()
			.map(|(path, staged)| {
				let name = path.rsplit('/').next().unwrap_or(&path).to_owned();
				(name, staged.stat())
			})
			.collect()
	}

	/// Whether any staged file lives below the directory `dir`.
	pub fn has_below(&self, dir: &str) -> bool {
		let prefix = format!("{dir}/");
		self
			.files
			.lock()
			.keys()
			.any(|path| path.starts_with(&prefix))
	}

	/// Stages a brand-new empty file (its object appears on the first sync).
	pub fn create(&self, path: &str, attrs: Attrs) -> FsResult<Stat> {
		let staged = self.new_staged(StagedState {
			size:       0,
			origin:     None,
			present:    HashSet::new(),
			dirty:      true,
			generation: 1,
			attrs:      Attrs { mtime: Some(unix_now()), ..attrs },
			error:      None,
		})?;
		let stat = staged.stat();
		self.files.lock().insert(path.to_owned(), staged);
		Ok(stat)
	}

	/// The staged file of `path`, staging the existing object `stat` first.
	fn stage_existing(&self, path: &str, stat: &Stat) -> FsResult<Arc<Staged>> {
		if let Some(staged) = self.get(path) {
			return Ok(staged);
		}
		let staged = self.new_staged(StagedState {
			size:       stat.size,
			origin:     Some(Origin { etag: stat.etag.clone(), valid: stat.size }),
			present:    HashSet::new(),
			dirty:      false,
			generation: 0,
			attrs:      Attrs { mtime: Some(stat.mtime), ..stat.attrs },
			error:      None,
		})?;
		Ok(Arc::clone(self.files.lock().entry(path.to_owned()).or_insert(staged)))
	}

	async fn staged(&self, path: &str) -> FsResult<Arc<Staged>> {
		if let Some(staged) = self.get(path) {
			return Ok(staged);
		}
		match self.bucket.stat(path, false).await? {
			None => Err(FsError::NotFound),
			Some(stat) if stat.kind == Kind::Dir => Err(FsError::IsDir),
			Some(stat) => self.stage_existing(path, &stat),
		}
	}

	/// Copies chunk `index` of the origin object into the staged file
	/// unless it is already present.
	async fn hydrate(&self, path: &str, staged: &Staged, index: u64) -> FsResult<()> {
		let origin = {
			let mut state = staged.state.lock();
			if state.present.contains(&index) {
				return Ok(());
			}
			match &state.origin {
				Some(origin) if index * CHUNK < origin.valid => origin.clone(),
				_ => {
					state.present.insert(index);
					return Ok(());
				},
			}
		};
		let start = index * CHUNK;
		let len = CHUNK.min(origin.valid - start);
		tracing::debug!(path, chunk = index, "s3fs: hydrating staged chunk");
		let bytes = match &origin.etag {
			Some(etag) => {
				self
					.bucket
					.client()
					.get_range_if_match(path, start, len, etag)
					.await?
			},
			None => self.bucket.client().get_range(path, start, len).await?,
		};
		let mut state = staged.state.lock();
		if state.present.insert(index) {
			// Truncation may have lowered the valid range meanwhile.
			let valid = state.origin.as_ref().map_or(0, |origin| origin.valid);
			let keep = (bytes.len() as u64).min(valid.saturating_sub(start));
			staged.file.write_all_at(&bytes[..keep as usize], start)?;
		}
		Ok(())
	}

	async fn hydrate_range(
		&self,
		path: &str,
		staged: &Staged,
		start: u64,
		end: u64,
	) -> FsResult<()> {
		if start >= end {
			return Ok(());
		}
		for index in start / CHUNK..=(end - 1) / CHUNK {
			self.hydrate(path, staged, index).await?;
		}
		Ok(())
	}

	/// Reads from the staged file of `path`, if staged.
	pub async fn read(&self, path: &str, offset: u64, len: u32) -> Option<FsResult<Vec<u8>>> {
		let staged = self.get(path)?;
		Some(
			async {
				let size = staged.state.lock().size;
				let end = (offset + u64::from(len)).min(size);
				if offset >= end {
					return Ok(Vec::new());
				}
				self.hydrate_range(path, &staged, offset, end).await?;
				let mut buf = vec![0; usize::try_from(end - offset).unwrap_or(0)];
				let read = staged.file.read_at(&mut buf, offset)?;
				buf.truncate(read);
				Ok(buf)
			}
			.await,
		)
	}

	/// Writes `data` at `offset`, staging (lazily hydrating) the object.
	pub async fn write(&self, path: &str, offset: u64, data: &[u8]) -> FsResult<u32> {
		let staged = self.staged(path).await?;
		let end = offset
			.checked_add(data.len() as u64)
			.ok_or_else(|| FsError::Invalid("write overflows".into()))?;
		if !data.is_empty() {
			for index in offset / CHUNK..=(end - 1) / CHUNK {
				let (chunk_start, chunk_end) = (index * CHUNK, (index + 1) * CHUNK);
				let valid = staged
					.state
					.lock()
					.origin
					.as_ref()
					.map_or(0, |origin| origin.valid);
				let covered = offset <= chunk_start && end >= chunk_end.min(valid);
				if covered {
					staged.state.lock().present.insert(index);
				} else {
					self.hydrate(path, &staged, index).await?;
				}
			}
		}
		let mut state = staged.state.lock();
		staged.file.write_all_at(data, offset)?;
		state.size = state.size.max(end);
		state.dirty = true;
		state.generation += 1;
		state.attrs.mtime = Some(unix_now());
		Ok(u32::try_from(data.len()).unwrap_or(u32::MAX))
	}

	/// Sets the size of `path` (staging it; a truncation to zero never
	/// downloads anything).
	pub async fn truncate(&self, path: &str, size: u64) -> FsResult<()> {
		let staged = match self.get(path) {
			Some(staged) => staged,
			None => match self.bucket.stat(path, false).await? {
				None => return Err(FsError::NotFound),
				Some(stat) if stat.kind == Kind::Dir => return Err(FsError::IsDir),
				Some(stat) => self.stage_existing(path, &stat)?,
			},
		};
		let mut state = staged.state.lock();
		staged.file.set_len(size)?;
		if let Some(origin) = &mut state.origin {
			origin.valid = origin.valid.min(size);
		}
		state.size = size;
		state.dirty = true;
		state.generation += 1;
		state.attrs.mtime = Some(unix_now());
		Ok(())
	}

	/// Updates the attributes of a staged file; `None` when `path` is not
	/// staged or not dirty (the caller then rewrites object metadata).
	pub fn set_attrs_if_dirty(&self, path: &str, update: Attrs) -> Option<Stat> {
		let staged = self.get(path)?;
		{
			let mut state = staged.state.lock();
			if !state.dirty {
				return None;
			}
			state.attrs = state.attrs.merge(update);
			state.generation += 1;
		}
		Some(staged.stat())
	}

	/// Records new attributes and `ETag` of a clean staged file whose object
	/// metadata was rewritten in place.
	pub fn metadata_rewritten(&self, path: &str, update: Attrs, etag: &str) {
		if let Some(staged) = self.get(path) {
			let mut state = staged.state.lock();
			state.attrs = state.attrs.merge(update);
			if let Some(origin) = &mut state.origin {
				origin.etag = Some(etag.to_owned());
			}
		}
	}

	/// Uploads `path` if dirty, waiting at most `budget` (`None` = until
	/// done). An upload that outlasts the budget continues in the
	/// background.
	pub async fn sync(self: &Arc<Self>, path: &str, budget: Option<Duration>) -> FsResult<()> {
		let Some(staged) = self.get(path) else {
			return Ok(());
		};
		let stage = Arc::clone(self);
		let owned = path.to_owned();
		let upload = tokio::spawn(async move { stage.upload(&owned, &staged).await });
		let joined = match budget {
			Some(budget) => {
				if let Ok(joined) = tokio::time::timeout(budget, upload).await {
					joined
				} else {
					tracing::debug!(path, "s3fs: upload continues in the background");
					return Ok(());
				}
			},
			None => upload.await,
		};
		joined.map_err(|error| FsError::Io(error.to_string()))?
	}

	async fn upload(&self, path: &str, staged: &Staged) -> FsResult<()> {
		let _upload = staged.upload.lock().await;
		let (generation, size, attrs) = {
			let state = staged.state.lock();
			if !state.dirty {
				return Ok(());
			}
			(state.generation, state.size, state.attrs)
		};
		let result = async {
			self.hydrate_range(path, staged, 0, size).await?;
			let mut file = tokio::fs::File::from_std(staged.file.try_clone()?);
			file.seek(std::io::SeekFrom::Start(0)).await?;
			let body = tokio_util::io::ReaderStream::new(file.take(size));
			let options =
				PutOptions { metadata: attrs.to_metadata(Kind::File), ..PutOptions::default() };
			tracing::debug!(path, size, "s3fs: uploading staged file");
			let etag = self
				.bucket
				.client()
				.upload_stream(path, size, body.map_err(std::io::Error::other), &options)
				.await?;
			FsResult::Ok(etag)
		}
		.await;
		let mut state = staged.state.lock();
		match result {
			Ok(etag) => {
				state.origin = Some(Origin { etag: Some(etag.clone()), valid: state.size });
				state.present = (0..state.size.div_ceil(CHUNK)).collect();
				if state.generation == generation {
					state.dirty = false;
				}
				state.error = None;
				let stat = Stat {
					kind: Kind::File,
					size,
					mtime: attrs.mtime.unwrap_or(0),
					etag: Some(etag),
					attrs,
					marker: false,
				};
				drop(state);
				self.bucket.remember(path, Some(stat));
				Ok(())
			},
			Err(error) => {
				tracing::warn!(path, %error, "s3fs: upload failed; the file stays dirty");
				state.error = Some(error.to_string());
				Err(error)
			},
		}
	}

	/// Syncs `path` within the budget, then drops its staging when clean
	/// (the guest released a handle; the next write re-stages it).
	pub async fn release(self: &Arc<Self>, path: &str) -> FsResult<()> {
		self.sync(path, Some(SYNC_BUDGET)).await?;
		let mut files = self.files.lock();
		if let Some(staged) = files.get(path)
			&& !staged.state.lock().dirty
			&& staged.upload.try_lock().is_ok()
		{
			files.remove(path);
		}
		Ok(())
	}

	/// Forgets `path` (unlink): waits for an in-flight upload so it cannot
	/// recreate the object afterwards.
	pub async fn discard(&self, path: &str) {
		let staged = self.files.lock().remove(path);
		if let Some(staged) = staged {
			drop(staged.upload.lock().await);
		}
	}

	/// Uploads every dirty file (pause, kill, shutdown) and waits.
	///
	/// # Errors
	///
	/// Returns the first upload failure after trying every file.
	pub async fn sync_all(self: &Arc<Self>) -> FsResult<()> {
		let paths: Vec<String> = self.files.lock().keys().cloned().collect();
		let mut first = None;
		for path in paths {
			if let Err(error) = self.sync(&path, None).await {
				first.get_or_insert(error);
			}
		}
		first.map_or(Ok(()), Err)
	}

	/// Uploads every dirty file below the directory `dir` (before a rename).
	pub async fn sync_below(self: &Arc<Self>, dir: &str) -> FsResult<()> {
		let prefix = format!("{dir}/");
		let paths: Vec<String> = self
			.files
			.lock()
			.keys()
			.filter(|path| path.starts_with(&prefix))
			.cloned()
			.collect();
		for path in paths {
			self.sync(&path, None).await?;
			self.discard(&path).await;
		}
		Ok(())
	}

	/// Whether any file is staged with changes not yet uploaded.
	pub fn dirty(&self) -> bool {
		self
			.files
			.lock()
			.values()
			.any(|staged| staged.state.lock().dirty)
	}
}

impl Drop for Stage {
	fn drop(&mut self) {
		self.files.lock().clear();
		let _ = std::fs::remove_dir_all(&self.dir);
	}
}

#[cfg(test)]
mod tests {
	use vmon_cloud::s3::fake::FakeS3;

	use super::*;

	fn stage(fake: &FakeS3, dir: &Path) -> (Arc<Bucket>, Arc<Stage>) {
		let bucket = Arc::new(Bucket::new(fake.client("bkt", "vol"), Duration::ZERO, 1 << 20));
		let stage = Arc::new(Stage::new(Arc::clone(&bucket), dir.join("stage")).unwrap());
		(bucket, stage)
	}

	fn ranged_gets(fake: &FakeS3) -> usize {
		fake
			.requests()
			.iter()
			.filter(|request| {
				request.method == http::Method::GET && !request.query.contains_key("list-type")
			})
			.count()
	}

	#[tokio::test]
	async fn partial_write_hydrates_only_the_touched_chunk() {
		let fake = FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		let mut original = vec![b'x'; (CHUNK * 3) as usize];
		original[0] = b'A';
		fake.insert("bkt/vol/big.bin", original.clone());
		let (_, stage) = stage(&fake, dir.path());

		stage.write("big.bin", CHUNK + 1, b"yy").await.unwrap();
		assert_eq!(ranged_gets(&fake), 1, "one chunk hydrated for a partial write");
		stage
			.write("big.bin", 2 * CHUNK, &vec![b'z'; CHUNK as usize])
			.await
			.unwrap();
		assert_eq!(ranged_gets(&fake), 1, "a whole-chunk write needs no download");

		stage.sync("big.bin", None).await.unwrap();
		let stored = fake.objects()["bkt/vol/big.bin"].data.clone();
		let mut expected = original;
		expected[(CHUNK + 1) as usize..(CHUNK + 3) as usize].copy_from_slice(b"yy");
		expected[(2 * CHUNK) as usize..].fill(b'z');
		assert_eq!(stored.len(), expected.len());
		assert!(stored == expected, "uploaded bytes match the edited object");
	}

	#[tokio::test]
	async fn truncate_then_extend_reads_zeros_and_keeps_metadata() {
		let fake = FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		fake.insert("bkt/vol/f.txt", "hello world");
		let (_, stage) = stage(&fake, dir.path());

		stage.truncate("f.txt", 5).await.unwrap();
		stage.write("f.txt", 8, b"!").await.unwrap();
		let read = stage.read("f.txt", 0, 64).await.unwrap().unwrap();
		assert_eq!(read, b"hello\0\0\0!");
		let attrs = Attrs { perm: Some(0o600), uid: Some(1000), gid: Some(1000), mtime: None };
		assert!(stage.set_attrs_if_dirty("f.txt", attrs).is_some());
		stage.release("f.txt").await.unwrap();
		assert!(stage.stat("f.txt").is_none(), "clean file unstaged on release");
		let object = &fake.objects()["bkt/vol/f.txt"];
		assert_eq!(object.data.as_ref(), b"hello\0\0\0!");
		assert_eq!(object.metadata["mode"], (0o100_600).to_string());
		assert_eq!(object.metadata["uid"], "1000");
		assert!(object.metadata.contains_key("mtime"));
	}

	#[tokio::test]
	async fn new_files_upload_on_sync_and_unlink_waits_for_uploads() {
		let fake = FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		let (_, stage) = stage(&fake, dir.path());
		stage
			.create("n.txt", Attrs { perm: Some(0o644), ..Attrs::default() })
			.unwrap();
		stage.write("n.txt", 0, b"data").await.unwrap();
		assert_eq!(stage.children("").len(), 1);
		assert!(stage.dirty());
		assert!(fake.keys().is_empty(), "nothing uploaded before sync");
		stage.sync_all().await.unwrap();
		assert!(!stage.dirty());
		assert_eq!(fake.objects()["bkt/vol/n.txt"].data.as_ref(), b"data");
		stage.discard("n.txt").await;
		assert!(stage.stat("n.txt").is_none());
	}

	#[tokio::test]
	async fn failed_upload_keeps_the_file_dirty() {
		let fake = FakeS3::start().await;
		let dir = tempfile::tempdir().unwrap();
		let (_, stage) = stage(&fake, dir.path());
		stage.create("e.txt", Attrs::default()).unwrap();
		stage.write("e.txt", 0, b"x").await.unwrap();
		fake.set_hook(|request| {
			(request.method == http::Method::PUT).then_some(http::StatusCode::FORBIDDEN)
		});
		assert!(stage.sync("e.txt", None).await.is_err());
		assert!(stage.dirty());
		fake.clear_hook();
		stage.sync("e.txt", None).await.unwrap();
		assert!(!stage.dirty());
	}
}
