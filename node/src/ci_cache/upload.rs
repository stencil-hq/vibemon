//! Azure Blob uploads against a signed upload URL, as the Azure SDKs make
//! them (`@azure/storage-blob` `uploadFile` in `@actions/cache`, Go `azblob`
//! `UploadStream` in buildx):
//! - Put Blob: `PUT <url>` with `x-ms-blob-type: BlockBlob` and the whole
//!   archive as the body (single shot, archives up to 128 MiB);
//! - Put Block: `PUT <url>&comp=block&blockid=<base64>` stages one block;
//! - Put Block List: `PUT <url>&comp=blocklist` with
//!   `<BlockList><Latest>id</Latest>…</BlockList>` assembles the staged blocks
//!   in list order.
//!
//! The assembled archive is hashed while it is written, fsync'd, and
//! installed into the [`BlobStore`] under its sha256.

use std::{
	collections::HashMap,
	io,
	path::{Path, PathBuf},
	sync::Arc,
	time::{Duration, Instant},
};

use axum::body::Body;
use futures::TryStreamExt as _;
use http::StatusCode;
use parking_lot::Mutex;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::blobs::BlobStore;

/// An upload session not committed within this long is dropped.
pub const SESSION_TTL: Duration = Duration::from_hours(1);
/// Longest accepted block id.
const MAX_BLOCK_ID: usize = 128;

/// An Azure-style error (`<Error><Code>…</Code><Message>…</Message></Error>`).
#[derive(Debug)]
pub struct BlobError {
	pub status:  StatusCode,
	pub code:    &'static str,
	pub message: String,
}

impl BlobError {
	pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
		Self { status, code, message: message.into() }
	}

	fn internal(error: impl std::fmt::Display) -> Self {
		Self::new(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", error.to_string())
	}

	fn too_big(limit: u64) -> Self {
		Self::new(
			StatusCode::PAYLOAD_TOO_LARGE,
			"RequestBodyTooLarge",
			format!("the cache archive exceeds {limit} bytes"),
		)
	}
}

/// One reserved entry's upload.
#[derive(Debug)]
pub struct Upload {
	/// Sandbox that reserved the entry (the only one that may upload).
	pub sandbox:   String,
	/// KV key of the reserved entry.
	pub entry_key: String,
	/// The complete archive: `(sha256, size)`.
	pub committed: Option<(String, u64)>,
	/// Keeps the committed archive until the entry is finalized (or the
	/// session closes).
	pub pin:       Option<super::blobs::Pin>,
	dir:           PathBuf,
	/// Staged blocks: id → size.
	blocks:        HashMap<String, u64>,
	staged:        u64,
	limit:         u64,
	created:       Instant,
}

/// Open upload sessions by their token.
#[derive(Debug, Default)]
pub struct Uploads {
	sessions: Mutex<HashMap<String, Arc<tokio::sync::Mutex<Upload>>>>,
}

impl Uploads {
	/// Opens a session for `entry_key` (archive at most `limit` bytes) and
	/// returns its token. A previous session of the same entry is dropped.
	pub fn open(&self, store: &BlobStore, sandbox: &str, entry_key: &str, limit: u64) -> String {
		let token = hex::encode(rand::random::<[u8; 24]>());
		let upload = Upload {
			sandbox: sandbox.to_owned(),
			entry_key: entry_key.to_owned(),
			committed: None,
			pin: None,
			dir: store.upload_dir(&token),
			blocks: HashMap::new(),
			staged: 0,
			limit,
			created: Instant::now(),
		};
		let mut sessions = self.sessions.lock();
		sessions.retain(|_, session| {
			!session
				.try_lock()
				.is_ok_and(|session| session.entry_key == entry_key)
		});
		sessions.insert(token.clone(), Arc::new(tokio::sync::Mutex::new(upload)));
		token
	}

	/// The session of `token`.
	pub fn get(&self, token: &str) -> Option<Arc<tokio::sync::Mutex<Upload>>> {
		self.sessions.lock().get(token).cloned()
	}

	/// The session uploading `entry_key` for `sandbox`.
	pub async fn by_entry(
		&self,
		sandbox: &str,
		entry_key: &str,
	) -> Option<(String, Arc<tokio::sync::Mutex<Upload>>)> {
		let sessions: Vec<_> = self
			.sessions
			.lock()
			.iter()
			.map(|(token, session)| (token.clone(), Arc::clone(session)))
			.collect();
		for (token, session) in sessions {
			let upload = session.lock().await;
			if upload.entry_key == entry_key && upload.sandbox == sandbox {
				drop(upload);
				return Some((token, session));
			}
		}
		None
	}

	/// Closes a session and removes its staged blocks.
	pub async fn close(&self, token: &str) {
		let session = self.sessions.lock().remove(token);
		if let Some(session) = session {
			let dir = session.lock().await.dir.clone();
			let _ = tokio::fs::remove_dir_all(dir).await;
		}
	}

	/// Closes sessions older than [`SESSION_TTL`]; returns their entry keys
	/// (reservations to release).
	pub async fn expire(&self) -> Vec<String> {
		let expired: Vec<_> = {
			let sessions = self.sessions.lock();
			sessions
				.iter()
				.filter_map(|(token, session)| {
					let upload = session.try_lock().ok()?;
					(upload.created.elapsed() >= SESSION_TTL)
						.then(|| (token.clone(), upload.entry_key.clone()))
				})
				.collect()
		};
		let mut keys = Vec::new();
		for (token, key) in expired {
			self.close(&token).await;
			keys.push(key);
		}
		keys
	}
}

/// Streams `body` into `file`, hashing it when `hasher` is given; fails past
/// `limit` total bytes (`already` counted before).
async fn copy_body(
	body: Body,
	file: &mut tokio::fs::File,
	mut hasher: Option<&mut Sha256>,
	already: u64,
	limit: u64,
) -> Result<u64, BlobError> {
	let mut size = 0u64;
	let mut stream = body.into_data_stream().map_err(io::Error::other);
	while let Some(chunk) = stream.try_next().await.map_err(|error| {
		BlobError::new(
			StatusCode::BAD_REQUEST,
			"InvalidInput",
			format!("upload interrupted: {error}"),
		)
	})? {
		size += chunk.len() as u64;
		if already + size > limit {
			return Err(BlobError::too_big(limit));
		}
		if let Some(hasher) = hasher.as_deref_mut() {
			hasher.update(&chunk);
		}
		file.write_all(&chunk).await.map_err(BlobError::internal)?;
	}
	Ok(size)
}

/// File name of a staged block (ids are base64, `/` included).
fn block_file(dir: &Path, id: &str) -> PathBuf {
	dir.join(hex::encode(Sha256::digest(id.as_bytes())))
}

impl Upload {
	fn ensure_open(&self) -> Result<(), BlobError> {
		if self.committed.is_some() {
			return Err(BlobError::new(
				StatusCode::CONFLICT,
				"BlobAlreadyExists",
				"the cache archive was already uploaded",
			));
		}
		Ok(())
	}

	/// Put Block: stages one block (a re-staged id replaces the block).
	pub async fn stage_block(&mut self, id: &str, body: Body) -> Result<(), BlobError> {
		self.ensure_open()?;
		if id.is_empty() || id.len() > MAX_BLOCK_ID {
			return Err(BlobError::new(
				StatusCode::BAD_REQUEST,
				"InvalidQueryParameterValue",
				"invalid blockid",
			));
		}
		tokio::fs::create_dir_all(&self.dir)
			.await
			.map_err(BlobError::internal)?;
		let path = block_file(&self.dir, id);
		let tmp = path.with_extension("part");
		let mut file = tokio::fs::File::create(&tmp)
			.await
			.map_err(BlobError::internal)?;
		let previous = self.blocks.get(id).copied().unwrap_or(0);
		let size = match copy_body(body, &mut file, None, self.staged - previous, self.limit).await {
			Ok(size) => size,
			Err(error) => {
				drop(file);
				let _ = tokio::fs::remove_file(&tmp).await;
				return Err(error);
			},
		};
		tokio::fs::rename(&tmp, &path)
			.await
			.map_err(BlobError::internal)?;
		self.staged = self.staged - previous + size;
		self.blocks.insert(id.to_owned(), size);
		Ok(())
	}

	/// Put Blob: the whole archive in one request.
	pub async fn put_blob(
		&mut self,
		store: &Arc<BlobStore>,
		body: Body,
	) -> Result<(String, u64), BlobError> {
		self.ensure_open()?;
		let tmp = store.temp_path();
		let result = async {
			let mut file = tokio::fs::File::create(&tmp)
				.await
				.map_err(BlobError::internal)?;
			let mut hasher = Sha256::new();
			let size = copy_body(body, &mut file, Some(&mut hasher), 0, self.limit).await?;
			file.sync_all().await.map_err(BlobError::internal)?;
			Ok::<_, BlobError>((hex::encode(hasher.finalize()), size))
		}
		.await;
		self.install(store, &tmp, result).await
	}

	/// Put Block List: assembles the staged blocks named by `xml` in order.
	pub async fn commit_blocks(
		&mut self,
		store: &Arc<BlobStore>,
		xml: &[u8],
	) -> Result<(String, u64), BlobError> {
		self.ensure_open()?;
		let ids = std::str::from_utf8(xml)
			.ok()
			.and_then(parse_block_list)
			.ok_or_else(|| {
				BlobError::new(StatusCode::BAD_REQUEST, "InvalidXmlDocument", "invalid block list")
			})?;
		if let Some(missing) = ids.iter().find(|id| !self.blocks.contains_key(*id)) {
			return Err(BlobError::new(
				StatusCode::BAD_REQUEST,
				"InvalidBlockList",
				format!("block {missing} was not staged"),
			));
		}
		let total: u64 = ids.iter().map(|id| self.blocks[id]).sum();
		if total > self.limit {
			return Err(BlobError::too_big(self.limit));
		}
		let tmp = store.temp_path();
		let result = async {
			let mut out = tokio::fs::File::create(&tmp)
				.await
				.map_err(BlobError::internal)?;
			let mut hasher = Sha256::new();
			let mut buf = vec![0u8; 1 << 20];
			for id in &ids {
				let mut block = tokio::fs::File::open(block_file(&self.dir, id))
					.await
					.map_err(BlobError::internal)?;
				loop {
					let n = block.read(&mut buf).await.map_err(BlobError::internal)?;
					if n == 0 {
						break;
					}
					hasher.update(&buf[..n]);
					out.write_all(&buf[..n])
						.await
						.map_err(BlobError::internal)?;
				}
			}
			out.sync_all().await.map_err(BlobError::internal)?;
			Ok::<_, BlobError>((hex::encode(hasher.finalize()), total))
		}
		.await;
		let installed = self.install(store, &tmp, result).await;
		if installed.is_ok() {
			let _ = tokio::fs::remove_dir_all(&self.dir).await;
			self.blocks.clear();
			self.staged = 0;
		}
		installed
	}

	async fn install(
		&mut self,
		store: &Arc<BlobStore>,
		tmp: &Path,
		result: Result<(String, u64), BlobError>,
	) -> Result<(String, u64), BlobError> {
		let (digest, size) = match result {
			Ok(done) => done,
			Err(error) => {
				let _ = tokio::fs::remove_file(tmp).await;
				return Err(error);
			},
		};
		// Pinned before it is installed: an unreferenced copy is otherwise
		// fair game for the node's sweep until the entry is finalized.
		let pin = store.pin(&digest);
		let (store, tmp, key) = (Arc::clone(store), tmp.to_owned(), digest.clone());
		tokio::task::spawn_blocking(move || store.install(&tmp, &key, size))
			.await
			.map_err(BlobError::internal)?
			.map_err(BlobError::internal)?;
		self.pin = Some(pin);
		self.committed = Some((digest.clone(), size));
		Ok((digest, size))
	}
}

/// Block ids of a `Put Block List` body, in order (`Latest`, `Uncommitted`,
/// and `Committed` all name staged blocks here: nothing is ever committed
/// before). `None` when malformed.
pub fn parse_block_list(xml: &str) -> Option<Vec<String>> {
	let start = xml.find("<BlockList>")? + "<BlockList>".len();
	let end = xml.rfind("</BlockList>")?;
	let mut rest = xml.get(start..end)?;
	let mut ids = Vec::new();
	loop {
		rest = rest.trim_start();
		if rest.is_empty() {
			return Some(ids);
		}
		let tag_end = rest.find('>')?;
		let tag = rest.get(1..tag_end)?;
		if !rest.starts_with('<') || !matches!(tag, "Latest" | "Uncommitted" | "Committed") {
			return None;
		}
		let body = &rest[tag_end + 1..];
		let close = format!("</{tag}>");
		let text_end = body.find(&close)?;
		ids.push(unescape(&body[..text_end]));
		rest = &body[text_end + close.len()..];
	}
}

fn unescape(text: &str) -> String {
	text
		.replace("&lt;", "<")
		.replace("&gt;", ">")
		.replace("&quot;", "\"")
		.replace("&apos;", "'")
		.replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn block_lists_parse_in_order() {
		let xml = r#"<?xml version="1.0" encoding="utf-8"?>
<BlockList><Latest>AAA=</Latest>
  <Uncommitted>B/B+</Uncommitted><Latest>AAA=</Latest><Committed>C&amp;</Committed></BlockList>"#;
		assert_eq!(parse_block_list(xml).unwrap(), vec!["AAA=", "B/B+", "AAA=", "C&"]);
		assert_eq!(parse_block_list("<BlockList></BlockList>").unwrap(), Vec::<String>::new());
		assert!(parse_block_list("<BlockList><Other>x</Other></BlockList>").is_none());
		assert!(parse_block_list("<BlockList><Latest>x</BlockList>").is_none());
		assert!(parse_block_list("nope").is_none());
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn staged_blocks_assemble_in_list_order_and_hash() {
		let dir = tempfile::tempdir().unwrap();
		let store = BlobStore::open(dir.path(), 1 << 20).unwrap();
		let uploads = Uploads::default();
		let token = uploads.open(&store, "sbx", "db/ci_cache/1/s/k/v", 100);
		let session = uploads.get(&token).unwrap();
		let mut upload = session.lock().await;
		upload
			.stage_block("Mg==", Body::from("world"))
			.await
			.unwrap();
		upload
			.stage_block("MQ==", Body::from("hello "))
			.await
			.unwrap();
		// Re-staging replaces a block (and its size).
		upload
			.stage_block("Mg==", Body::from("world!"))
			.await
			.unwrap();
		let unknown = upload
			.commit_blocks(&store, b"<BlockList><Latest>Mw==</Latest></BlockList>")
			.await
			.unwrap_err();
		assert_eq!(unknown.code, "InvalidBlockList");
		let xml = b"<?xml version=\"1.0\"?><BlockList><Latest>MQ==</Latest><Latest>Mg==</Latest></BlockList>";
		let (digest, size) = upload.commit_blocks(&store, xml).await.unwrap();
		assert_eq!(size, 12);
		assert_eq!(digest, hex::encode(Sha256::digest(b"hello world!")));
		assert_eq!(std::fs::read(store.path(&digest)).unwrap(), b"hello world!");
		assert!(!store.upload_dir(&token).exists(), "staged blocks are removed");
		// The archive is immutable once committed.
		assert_eq!(
			upload
				.stage_block("NA==", Body::from("x"))
				.await
				.unwrap_err()
				.status,
			StatusCode::CONFLICT
		);
		drop(upload);
		// Single-shot uploads hash the body; the size limit holds.
		let token = uploads.open(&store, "sbx", "db/ci_cache/1/s/k2/v", 5);
		let session = uploads.get(&token).unwrap();
		let mut upload = session.lock().await;
		let error = upload
			.put_blob(&store, Body::from("too long"))
			.await
			.unwrap_err();
		assert_eq!(error.status, StatusCode::PAYLOAD_TOO_LARGE);
		let (digest, size) = upload.put_blob(&store, Body::from("short")).await.unwrap();
		assert_eq!((digest.as_str(), size), (hex::encode(Sha256::digest(b"short")).as_str(), 5));
		assert_eq!(std::fs::read_dir(dir.path().join("tmp")).unwrap().count(), 0);
	}
}
