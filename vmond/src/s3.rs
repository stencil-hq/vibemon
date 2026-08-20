//! Lazy, signed S3 object access for remote virtio-fs mounts.
//!
//! [`S3Client`] owns all S3-specific behavior: addressing, `SigV4` signing,
//! `ListObjectsV2` parsing, short-lived metadata caches, and bounded
//! ranged-read caching. The VMM only exchanges the compact proxy protocol
//! defined by its remote filesystem device.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt, io,
	sync::Arc,
	time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use quick_xml::de::from_str;
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use sha2::Digest;
use tracing::warn;
use vmon_cloud::aws;

use crate::{EngineError, Result};

const LIST_CACHE_TTL: Duration = Duration::from_secs(5);
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const CHUNK_SIZE: usize = 1 << 20;
const MULTIPART_PART_SIZE: usize = 16 << 20;
const MULTIPART_MAX_PARTS: u32 = 10_000;
const MULTIPART_TIMEOUT: Duration = Duration::from_mins(5);
const CHUNK_CACHE_CAP: usize = 256 << 20;
const MAX_LIST_ENTRIES: usize = 100_000;
const COPY_RESPONSE_BODY_CAP: usize = 64 << 10;
type ListCacheEntry = (Instant, Arc<Vec<ObjEntry>>);
const COPY_OBJECT_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024;
const MULTIPART_COPY_PART_SIZE: u64 = COPY_OBJECT_MAX_SIZE;
const S3_OBJECT_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;
type ListCache = HashMap<String, ListCacheEntry>;

const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Resolved S3 connection settings for one sandbox mount.
#[derive(Clone, Debug)]
pub struct S3MountConfig {
	/// Bucket name from the mount URI.
	pub bucket:    String,
	/// Normalized key prefix relative to `bucket`.
	pub prefix:    String,
	/// AWS signing region.
	pub region:    String,
	/// Optional S3-compatible endpoint using path-style addressing.
	pub endpoint:  Option<String>,
	/// Whether the guest mounts the remote layer without an overlay.
	pub read_only: bool,
	/// Credentials selected by the engine, absent for anonymous requests.
	pub creds:     Option<S3Credentials>,
	/// Provenance of the selected credential mode.
	pub auth:      S3Auth,
}

/// Credentials used to sign S3 requests.
#[derive(Clone)]
pub struct S3Credentials {
	/// Access-key identifier.
	pub access_key:    String,
	/// Secret access key.
	pub secret_key:    String,
	/// Temporary-credential session token, when present.
	pub session_token: Option<String>,
}

impl fmt::Debug for S3Credentials {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str("S3Credentials(redacted)")
	}
}

/// How a mount obtained the credentials it uses for S3 requests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum S3Auth {
	/// Credentials were supplied in the sandbox-create request.
	Inline,
	/// Credentials were read from the server environment.
	Env,
	/// Requests carry no Authorization header.
	Anonymous,
}

impl S3Auth {
	/// Returns the stable metadata spelling for this credential provenance.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Inline => "inline",
			Self::Env => "env",
			Self::Anonymous => "anonymous",
		}
	}
}

/// A failure returned by S3 or by the S3 request machinery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum S3Error {
	/// The bucket or object does not exist.
	NotFound,
	/// Credentials lack access to the requested resource.
	Access,
	/// The object `ETag` changed during a ranged read.
	Stale,
	/// The caller supplied an invalid S3 request.
	BadRequest,
	/// A network, protocol, or unexpected service failure.
	Io(String),
}

impl S3Error {
	/// Returns the stable remote-filesystem proxy error code.
	pub const fn code(&self) -> &'static str {
		match self {
			Self::NotFound => "not_found",
			Self::Access => "access",
			Self::Stale => "stale",
			Self::BadRequest => "bad_request",
			Self::Io(_) => "io",
		}
	}
}

impl fmt::Display for S3Error {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotFound => formatter.write_str("S3 object was not found"),
			Self::Access => formatter.write_str("S3 access was denied"),
			Self::Stale => formatter.write_str("S3 object changed during read"),
			Self::BadRequest => formatter.write_str("invalid S3 request"),
			Self::Io(message) => formatter.write_str(message),
		}
	}
}

impl std::error::Error for S3Error {}

/// The kind of object exposed through a mount.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjKind {
	/// A regular S3 object.
	File,
	/// A directory synthesized from a common prefix.
	Dir,
}

/// One direct child in a mounted S3 directory.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjEntry {
	/// One child name, without the parent path.
	pub name:  String,
	/// Child kind.
	pub kind:  ObjKind,
	/// Object length in bytes, or zero for directories.
	pub size:  u64,
	/// Last modification time as Unix seconds.
	pub mtime: u64,
	/// Object `ETag`, absent for directories.
	pub etag:  Option<String>,
}

/// Metadata for an S3 mount-relative path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjStat {
	/// Path kind.
	pub kind:  ObjKind,
	/// Object length in bytes, or zero for directories.
	pub size:  u64,
	/// Last modification time as Unix seconds.
	pub mtime: u64,
	/// Object `ETag`, absent for directories.
	pub etag:  Option<String>,
}

/// One object returned by a bounded maintenance scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ScanEntry {
	/// Mount-relative object key.
	pub key:   String,
	/// Object length in bytes.
	pub size:  u64,
	/// Last modification time as Unix seconds.
	pub mtime: u64,
	/// Object `ETag`.
	pub etag:  Option<String>,
}

/// One bounded page of objects for maintenance callers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct S3ScanPage {
	/// Objects in this page.
	pub entries:                 Vec<S3ScanEntry>,
	/// Opaque continuation token for the following page.
	pub next_continuation_token: Option<String>,
}

/// Signed S3 client with bounded directory and ranged-data caches.
pub struct S3Client {
	http:           reqwest::Client,
	cfg:            S3MountConfig,
	list_cache:     Mutex<ListCache>,
	chunks:         Mutex<ChunkLru>,
	active_uploads: Mutex<HashSet<(String, String)>>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct ChunkKey {
	path:  String,
	etag:  String,
	index: u64,
}

#[derive(Deserialize)]
struct InitiateMultipartUploadResult {
	#[serde(rename = "UploadId")]
	upload_id: String,
}

#[derive(Deserialize)]
#[serde(rename = "Error")]
struct S3ServiceError {
	#[serde(rename = "Code")]
	code:    String,
	#[serde(rename = "Message")]
	message: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename = "CopyPartResult")]
struct CopyPartResult {
	#[serde(rename = "ETag")]
	etag: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename = "ListMultipartUploadsResult")]
struct ListMultipartUploadsResult {
	#[serde(rename = "Upload", default)]
	uploads:               Vec<MultipartUpload>,
	#[serde(rename = "IsTruncated", default)]
	is_truncated:          bool,
	#[serde(rename = "NextKeyMarker")]
	next_key_marker:       Option<String>,
	#[serde(rename = "NextUploadIdMarker")]
	next_upload_id_marker: Option<String>,
}

#[derive(Deserialize)]
struct MultipartUpload {
	#[serde(rename = "Key")]
	key:       String,
	#[serde(rename = "UploadId")]
	upload_id: String,
	#[serde(rename = "Initiated")]
	initiated: String,
}

struct ActiveMultipartUpload<'a> {
	uploads:   &'a Mutex<HashSet<(String, String)>>,
	key:       String,
	upload_id: String,
}

impl Drop for ActiveMultipartUpload<'_> {
	fn drop(&mut self) {
		let key = std::mem::take(&mut self.key);
		let upload_id = std::mem::take(&mut self.upload_id);
		self.uploads.lock().remove(&(key, upload_id));
	}
}
struct CachedChunk {
	data:      Bytes,
	last_used: u64,
}

struct ChunkLru {
	entries: HashMap<ChunkKey, CachedChunk>,
	bytes:   usize,
	clock:   u64,
}

impl ChunkLru {
	fn get(&mut self, key: &ChunkKey) -> Option<Bytes> {
		self.clock = self.clock.wrapping_add(1);
		let entry = self.entries.get_mut(key)?;
		entry.last_used = self.clock;
		Some(entry.data.clone())
	}

	fn insert(&mut self, key: ChunkKey, data: Bytes) {
		if data.len() > CHUNK_CACHE_CAP {
			return;
		}
		if let Some(old) = self.entries.remove(&key) {
			self.bytes = self.bytes.saturating_sub(old.data.len());
		}
		while self.bytes.saturating_add(data.len()) > CHUNK_CACHE_CAP {
			let Some(victim) = self
				.entries
				.iter()
				.min_by_key(|(_, entry)| entry.last_used)
				.map(|(key, _)| key.clone())
			else {
				break;
			};
			if let Some(old) = self.entries.remove(&victim) {
				self.bytes = self.bytes.saturating_sub(old.data.len());
			}
		}
		self.clock = self.clock.wrapping_add(1);
		self.bytes += data.len();
		self
			.entries
			.insert(key, CachedChunk { data, last_used: self.clock });
	}

	fn clear(&mut self) {
		self.entries.clear();
		self.bytes = 0;
	}
}

impl S3Client {
	/// Creates a bounded, ten-second-timeout client for one resolved mount.
	///
	/// # Errors
	///
	/// Returns [`S3Error::BadRequest`] for an invalid endpoint or an I/O error
	/// when reqwest cannot initialize its rustls client.
	pub fn new(cfg: S3MountConfig) -> std::result::Result<Self, S3Error> {
		if cfg.bucket.is_empty() || cfg.region.is_empty() {
			return Err(S3Error::BadRequest);
		}
		if let Some(endpoint) = &cfg.endpoint {
			validate_endpoint(endpoint)?;
		}
		let http = reqwest::Client::builder()
			.timeout(HTTP_TIMEOUT)
			.build()
			.map_err(|error| S3Error::Io(format!("building S3 HTTP client: {error}")))?;
		Ok(Self {
			http,
			cfg,
			list_cache: Mutex::new(ListCache::new()),
			chunks: Mutex::new(ChunkLru { entries: HashMap::new(), bytes: 0, clock: 0 }),
			active_uploads: Mutex::new(HashSet::new()),
		})
	}

	/// Clear the in-memory S3 caches.
	pub fn clear_cache(&self) {
		self.list_cache.lock().clear();
		self.chunks.lock().clear();
	}

	/// Maps a raw bucket key returned by S3 back into this mount's relative
	/// namespace for distributed object locking.
	///
	/// Returns [`S3Error::BadRequest`] when the key is not within the configured
	/// mount prefix or cannot name a valid mount-relative path.
	pub fn mount_relative_object_key(&self, key: &str) -> std::result::Result<String, S3Error> {
		let path = if self.cfg.prefix.is_empty() {
			key
		} else if key == self.cfg.prefix {
			""
		} else {
			key.strip_prefix(&self.cfg.prefix)
				.and_then(|path| path.strip_prefix('/'))
				.ok_or(S3Error::BadRequest)?
		};
		if !valid_relative_path(path) {
			return Err(S3Error::BadRequest);
		}
		Ok(path.to_owned())
	}

	/// Verifies bucket access with a one-key `ListObjectsV2` request.
	pub async fn probe(&self) -> std::result::Result<(), S3Error> {
		let prefix = self.list_prefix("")?;
		self.list_page(&prefix, None, 1).await.map(|_| ())
	}

	/// Lists direct children of `dir`, caching the normalized result for five
	/// seconds.
	pub async fn list_dir(&self, dir: &str) -> std::result::Result<Arc<Vec<ObjEntry>>, S3Error> {
		if !valid_relative_path(dir) {
			return Err(S3Error::BadRequest);
		}
		if let Some((stored, entries)) = self.list_cache.lock().get(dir)
			&& stored.elapsed() < LIST_CACHE_TTL
		{
			return Ok(entries.clone());
		}

		let prefix = self.list_prefix(dir)?;
		let mut entries = BTreeMap::new();
		let mut continuation = None;
		loop {
			let mut page = self
				.list_page(&prefix, continuation.as_deref(), 1000)
				.await?;
			let truncated = page.is_truncated;
			let next_continuation = page.next_continuation_token.take();
			merge_listing(&mut entries, &prefix, page)?;
			if entries.len() >= MAX_LIST_ENTRIES {
				warn!(dir, "S3 directory listing reached the 100000-entry cap; truncating");
				break;
			}
			if !truncated {
				break;
			}
			continuation = next_continuation;
			if continuation.is_none() {
				return Err(S3Error::Io(
					"truncated S3 ListObjectsV2 response lacks a continuation token".to_owned(),
				));
			}
		}
		let entries: Arc<Vec<ObjEntry>> =
			Arc::new(entries.into_values().take(MAX_LIST_ENTRIES).collect());
		self
			.list_cache
			.lock()
			.insert(dir.to_owned(), (Instant::now(), entries.clone()));
		Ok(entries)
	}

	/// Lists up to `max_keys` objects below `prefix` without directory caches.
	///
	/// Callers must retain and pass the opaque continuation token until the page
	/// reports no next token.
	pub async fn scan_keys_page(
		&self,
		prefix: &str,
		continuation: Option<&str>,
		max_keys: u16,
	) -> std::result::Result<S3ScanPage, S3Error> {
		if !valid_relative_path(prefix)
			|| !(1..=1000).contains(&max_keys)
			|| continuation.is_some_and(|token| token.is_empty() || token.contains('\0'))
		{
			return Err(S3Error::BadRequest);
		}
		let raw_prefix = self.list_prefix(prefix)?;
		let mount_root = self.object_key("")?;
		let page = self
			.list_page_flat(&raw_prefix, continuation, max_keys)
			.await?;
		let mut entries = Vec::with_capacity(page.contents.len());
		for entry in page.contents {
			let key = if mount_root.is_empty() {
				entry.key
			} else {
				entry
					.key
					.strip_prefix(&format!("{mount_root}/"))
					.ok_or_else(|| {
						S3Error::Io("S3 scan returned a key outside its mount prefix".to_owned())
					})?
					.to_owned()
			};
			if !(prefix.is_empty() || key.starts_with(&format!("{prefix}/")) || key == prefix) {
				return Err(S3Error::Io(
					"S3 scan returned a key outside its requested prefix".to_owned(),
				));
			}
			entries.push(S3ScanEntry {
				key,
				size: entry.size,
				mtime: parse_mtime(&entry.last_modified)?,
				etag: entry.etag,
			});
		}
		let next_continuation_token = if page.is_truncated {
			let next = page.next_continuation_token.ok_or_else(|| {
				S3Error::Io("truncated S3 ListObjectsV2 response lacks a continuation token".to_owned())
			})?;
			if continuation == Some(next.as_str()) {
				return Err(S3Error::Io(
					"S3 ListObjectsV2 continuation token did not advance".to_owned(),
				));
			}
			Some(next)
		} else {
			None
		};
		Ok(S3ScanPage { entries, next_continuation_token })
	}

	/// Enumerate every object below a mount-relative prefix without UI caching
	/// or the bounded-directory cap.
	pub async fn scan_keys(&self, prefix: &str) -> std::result::Result<Vec<String>, S3Error> {
		let mut keys = Vec::new();
		let mut continuation = None;
		loop {
			let page = self
				.scan_keys_page(prefix, continuation.as_deref(), 1000)
				.await?;
			keys.extend(page.entries.into_iter().map(|entry| entry.key));
			let Some(next) = page.next_continuation_token else {
				return Ok(keys);
			};
			continuation = Some(next);
		}
	}

	/// Resolves metadata from the parent listing instead of issuing a HEAD
	/// request.
	pub async fn stat(&self, path: &str) -> std::result::Result<ObjStat, S3Error> {
		if !valid_relative_path(path) {
			return Err(S3Error::BadRequest);
		}
		if path.is_empty() {
			return Ok(ObjStat { kind: ObjKind::Dir, size: 0, mtime: 0, etag: None });
		}
		let (parent, name) = path
			.rsplit_once('/')
			.map_or(("", path), |(parent, name)| (parent, name));
		let entries = self.list_dir(parent).await?;
		let entry = entries
			.iter()
			.find(|entry| entry.name == name)
			.ok_or(S3Error::NotFound)?;
		Ok(ObjStat {
			kind:  entry.kind,
			size:  entry.size,
			mtime: entry.mtime,
			etag:  entry.etag.clone(),
		})
	}

	/// Fetches exact object metadata with a signed S3 `HEAD` request.
	///
	/// Unlike [`Self::stat`], this bypasses directory listing caches and caps.
	pub async fn head_object(&self, path: &str) -> std::result::Result<ObjStat, S3Error> {
		let key = self.object_key(path)?;
		let response = self.request(Method::HEAD, &key, &[], None, None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let size = response
			.headers()
			.get(reqwest::header::CONTENT_LENGTH)
			.and_then(|value| value.to_str().ok())
			.and_then(|value| value.parse::<u64>().ok())
			.ok_or_else(|| S3Error::Io("S3 HEAD response omitted Content-Length".to_owned()))?;
		let last_modified = response
			.headers()
			.get(reqwest::header::LAST_MODIFIED)
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| S3Error::Io("S3 HEAD response omitted Last-Modified".to_owned()))?;
		let mtime = DateTime::parse_from_rfc2822(last_modified)
			.map_err(|error| S3Error::Io(format!("invalid S3 HEAD Last-Modified: {error}")))?
			.timestamp();
		let mtime = u64::try_from(mtime)
			.map_err(|_| S3Error::Io("S3 HEAD Last-Modified predates Unix epoch".to_owned()))?;
		let etag = response
			.headers()
			.get(reqwest::header::ETAG)
			.and_then(|value| value.to_str().ok())
			.map(str::to_owned);
		Ok(ObjStat { kind: ObjKind::File, size, mtime, etag })
	}

	/// Reads one exact conditional byte range using metadata from
	/// [`Self::head_object`].
	///
	/// This bypasses listing caches and returns [`S3Error::Stale`] when the
	/// object's `ETag` no longer matches `stat`.
	pub async fn read_range_if_match(
		&self,
		path: &str,
		stat: &ObjStat,
		offset: u64,
		len: u32,
	) -> std::result::Result<Bytes, S3Error> {
		if stat.kind != ObjKind::File || len as usize > CHUNK_SIZE {
			return Err(S3Error::BadRequest);
		}
		if offset >= stat.size || len == 0 {
			return Ok(Bytes::new());
		}
		let etag = stat
			.etag
			.as_deref()
			.ok_or_else(|| S3Error::Io("S3 HEAD response omitted ETag".to_owned()))?;
		let expected = u64::from(len).min(stat.size - offset);
		let end = offset + expected - 1;
		let key = self.object_key(path)?;
		let range = format!("bytes={offset}-{end}");
		let response = self
			.request(Method::GET, &key, &[], Some(&range), Some(etag))
			.await?;
		if response.status() == StatusCode::PRECONDITION_FAILED {
			return Err(S3Error::Stale);
		}
		if response.status() == StatusCode::PARTIAL_CONTENT {
			let content_range = response
				.headers()
				.get(reqwest::header::CONTENT_RANGE)
				.and_then(|value| value.to_str().ok())
				.ok_or_else(|| S3Error::Io("S3 range response omitted Content-Range".to_owned()))?;
			validate_content_range(content_range, offset, end, stat.size)?;
		} else if response.status() != StatusCode::OK || offset != 0 || expected != stat.size {
			return Err(response_error(response.status()));
		}
		if response
			.content_length()
			.is_some_and(|length| length != expected)
		{
			return Err(S3Error::Io("S3 range response length did not match its range".to_owned()));
		}
		let body = response
			.bytes()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 exact range response: {error}")))?;
		if body.len() != expected as usize {
			return Err(S3Error::Io(
				"S3 range response body length did not match its range".to_owned(),
			));
		}
		Ok(body)
	}

	/// Put (upload) a whole object to S3.
	pub async fn put(&self, path: &str, body: Vec<u8>) -> std::result::Result<(), S3Error> {
		let key = self.object_key(path)?;
		let response = self
			.request_with_body(Method::PUT, &key, &[], Some(body))
			.await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		// Invalidate list cache for parent directory
		if let Some((parent, _)) = path.rsplit_once('/') {
			self.list_cache.lock().remove(parent);
		} else {
			self.list_cache.lock().remove("");
		}
		Ok(())
	}

	/// Copies one mount-relative object to another using S3's server-side copy.
	///
	/// Successful HTTP responses are inspected for an embedded S3 `<Error>`.
	pub async fn copy_object(
		&self,
		source_relative: &str,
		dest_relative: &str,
		source_size: u64,
	) -> std::result::Result<(), S3Error> {
		if source_size > S3_OBJECT_MAX_SIZE {
			return Err(S3Error::BadRequest);
		}
		let source = self.object_key(source_relative)?;
		let destination = self.object_key(dest_relative)?;
		if source_size <= COPY_OBJECT_MAX_SIZE {
			self.copy_object_single(&source, &destination).await?;
		} else {
			self
				.copy_object_multipart(&source, &destination, source_size)
				.await?;
		}
		self.invalidate_parent(dest_relative);
		Ok(())
	}

	async fn copy_object_single(
		&self,
		source: &str,
		destination: &str,
	) -> std::result::Result<(), S3Error> {
		let response = self.copy_request(source, destination, &[], None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let response_body =
			read_bounded_response(response, COPY_RESPONSE_BODY_CAP, "copy response").await?;
		if let Ok(error) = from_str::<S3ServiceError>(&response_body) {
			let message = error.message.as_deref().unwrap_or("no message");
			return Err(S3Error::Io(format!(
				"copy-object returned S3 error {}: {message}",
				error.code
			)));
		}
		Ok(())
	}

	async fn copy_object_multipart(
		&self,
		source: &str,
		destination: &str,
		source_size: u64,
	) -> std::result::Result<(), S3Error> {
		let part_count = source_size.div_ceil(MULTIPART_COPY_PART_SIZE);
		if part_count > u64::from(MULTIPART_MAX_PARTS) {
			return Err(S3Error::BadRequest);
		}
		let uploads = vec![("uploads".to_owned(), String::new())];
		let response = self
			.request_with_body_timeout(Method::POST, destination, &uploads, None, MULTIPART_TIMEOUT)
			.await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let response_body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading multipart-copy init response: {error}")))?;
		let upload = from_str::<InitiateMultipartUploadResult>(&response_body)
			.map_err(|error| S3Error::Io(format!("invalid multipart-copy init response: {error}")))?;
		if upload.upload_id.is_empty() {
			return Err(S3Error::Io("multipart-copy init response omitted UploadId".to_owned()));
		}
		self
			.active_uploads
			.lock()
			.insert((destination.to_owned(), upload.upload_id.clone()));
		let _active_upload = ActiveMultipartUpload {
			uploads:   &self.active_uploads,
			key:       destination.to_owned(),
			upload_id: upload.upload_id.clone(),
		};

		let result = async {
			let mut parts = Vec::with_capacity(usize::try_from(part_count).unwrap_or(usize::MAX));
			for index in 0..part_count {
				let start = index * MULTIPART_COPY_PART_SIZE;
				let end = start
					.saturating_add(MULTIPART_COPY_PART_SIZE - 1)
					.min(source_size - 1);
				let part_number = u32::try_from(index + 1).map_err(|_| S3Error::BadRequest)?;
				let query = vec![
					("partNumber".to_owned(), part_number.to_string()),
					("uploadId".to_owned(), upload.upload_id.clone()),
				];
				let range = format!("bytes={start}-{end}");
				let response = self
					.copy_request(source, destination, &query, Some(&range))
					.await?;
				if !response.status().is_success() {
					return Err(response_error(response.status()));
				}
				let response_body =
					read_bounded_response(response, COPY_RESPONSE_BODY_CAP, "copy-part response")
						.await?;
				if let Ok(error) = from_str::<S3ServiceError>(&response_body) {
					let message = error.message.as_deref().unwrap_or("no message");
					return Err(S3Error::Io(format!(
						"copy-part returned S3 error {}: {message}",
						error.code
					)));
				}
				let part = from_str::<CopyPartResult>(&response_body)
					.map_err(|error| S3Error::Io(format!("invalid copy-part response: {error}")))?;
				parts.push(
					part
						.etag
						.ok_or_else(|| S3Error::Io("copy-part response omitted ETag".to_owned()))?,
				);
			}
			let mut complete = String::from("<CompleteMultipartUpload>");
			for (index, etag) in parts.iter().enumerate() {
				complete.push_str("<Part><PartNumber>");
				complete.push_str(&(index + 1).to_string());
				complete.push_str("</PartNumber><ETag>");
				complete.push_str(&quick_xml::escape::escape(etag));
				complete.push_str("</ETag></Part>");
			}
			complete.push_str("</CompleteMultipartUpload>");
			let query = vec![("uploadId".to_owned(), upload.upload_id.clone())];
			let response = self
				.request_with_body_timeout(
					Method::POST,
					destination,
					&query,
					Some(complete.into_bytes()),
					MULTIPART_TIMEOUT,
				)
				.await?;
			if !response.status().is_success() {
				return Err(response_error(response.status()));
			}
			let response_body = read_bounded_response(
				response,
				COPY_RESPONSE_BODY_CAP,
				"multipart-copy complete response",
			)
			.await?;
			if let Ok(error) = from_str::<S3ServiceError>(&response_body) {
				let message = error.message.as_deref().unwrap_or("no message");
				return Err(S3Error::Io(format!(
					"multipart-copy complete returned S3 error {}: {message}",
					error.code
				)));
			}
			Ok(())
		}
		.await;
		if let Err(error) = result {
			let query = vec![("uploadId".to_owned(), upload.upload_id)];
			let _ = self
				.request_with_body_timeout(Method::DELETE, destination, &query, None, MULTIPART_TIMEOUT)
				.await;
			return Err(error);
		}
		Ok(())
	}

	/// Streams an arbitrarily large object through S3 multipart upload.
	pub async fn put_multipart(
		&self,
		path: &str,
		mut body: tokio::sync::mpsc::Receiver<io::Result<Bytes>>,
	) -> std::result::Result<(), S3Error> {
		let key = self.object_key(path)?;
		let uploads = vec![("uploads".to_owned(), String::new())];
		let response = self
			.request_with_body_timeout(Method::POST, &key, &uploads, None, MULTIPART_TIMEOUT)
			.await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let response_body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading multipart-init response: {error}")))?;
		let upload = from_str::<InitiateMultipartUploadResult>(&response_body)
			.map_err(|error| S3Error::Io(format!("invalid multipart-init response: {error}")))?;
		if upload.upload_id.is_empty() {
			return Err(S3Error::Io("multipart-init response omitted UploadId".to_owned()));
		}
		self
			.active_uploads
			.lock()
			.insert((key.clone(), upload.upload_id.clone()));
		let _active_upload = ActiveMultipartUpload {
			uploads:   &self.active_uploads,
			key:       key.clone(),
			upload_id: upload.upload_id.clone(),
		};

		let result = async {
			let mut pending = Vec::with_capacity(MULTIPART_PART_SIZE);
			let mut parts = Vec::new();
			while let Some(chunk) = body.recv().await {
				let chunk =
					chunk.map_err(|error| S3Error::Io(format!("producing upload body: {error}")))?;
				let mut offset = 0;
				while offset < chunk.len() {
					let take = (MULTIPART_PART_SIZE - pending.len()).min(chunk.len() - offset);
					pending.extend_from_slice(&chunk[offset..offset + take]);
					offset += take;
					if pending.len() == MULTIPART_PART_SIZE {
						let part =
							std::mem::replace(&mut pending, Vec::with_capacity(MULTIPART_PART_SIZE));
						let part_number =
							u32::try_from(parts.len() + 1).map_err(|_| S3Error::BadRequest)?;
						parts.push(
							self
								.upload_part(&key, &upload.upload_id, part_number, part)
								.await?,
						);
					}
				}
			}
			if !pending.is_empty() || parts.is_empty() {
				let part_number = u32::try_from(parts.len() + 1).map_err(|_| S3Error::BadRequest)?;
				parts.push(
					self
						.upload_part(&key, &upload.upload_id, part_number, pending)
						.await?,
				);
			}
			let mut complete = String::from("<CompleteMultipartUpload>");
			for (index, etag) in parts.iter().enumerate() {
				complete.push_str("<Part><PartNumber>");
				complete.push_str(&(index + 1).to_string());
				complete.push_str("</PartNumber><ETag>");
				complete.push_str(&quick_xml::escape::escape(etag));
				complete.push_str("</ETag></Part>");
			}
			complete.push_str("</CompleteMultipartUpload>");
			let query = vec![("uploadId".to_owned(), upload.upload_id.clone())];
			let response = self
				.request_with_body_timeout(
					Method::POST,
					&key,
					&query,
					Some(complete.into_bytes()),
					MULTIPART_TIMEOUT,
				)
				.await?;
			if !response.status().is_success() {
				return Err(response_error(response.status()));
			}
			let response_body = response.text().await.map_err(|error| {
				S3Error::Io(format!("reading multipart-complete response: {error}"))
			})?;
			if let Ok(error) = from_str::<S3ServiceError>(&response_body) {
				let message = error.message.as_deref().unwrap_or("no message");
				return Err(S3Error::Io(format!(
					"multipart-complete returned S3 error {}: {message}",
					error.code
				)));
			}
			Ok(())
		}
		.await;

		if let Err(error) = result {
			let query = vec![("uploadId".to_owned(), upload.upload_id)];
			let _ = self
				.request_with_body_timeout(Method::DELETE, &key, &query, None, MULTIPART_TIMEOUT)
				.await;
			return Err(error);
		}
		self.invalidate_parent(path);
		Ok(())
	}

	/// Aborts incomplete uploads under a mount-relative prefix initiated before
	/// `older_than`, returning the number successfully aborted.
	pub async fn abort_stale_multipart_uploads(
		&self,
		prefix: &str,
		older_than: DateTime<Utc>,
	) -> std::result::Result<usize, S3Error> {
		const MAX_PAGES: usize = 100;
		const PAGE_SIZE: u16 = 1000;

		let prefix = self.object_key(prefix)?;
		let mut markers: Option<(String, String)> = None;
		let mut aborted = 0;
		for page_number in 0..MAX_PAGES {
			let page = self
				.list_multipart_uploads_page(
					&prefix,
					markers.as_ref().map(|(key, _)| key.as_str()),
					markers.as_ref().map(|(_, upload_id)| upload_id.as_str()),
					PAGE_SIZE,
				)
				.await?;
			for upload in page.uploads {
				if self
					.active_uploads
					.lock()
					.contains(&(upload.key.clone(), upload.upload_id.clone()))
				{
					continue;
				}
				let initiated = DateTime::parse_from_rfc3339(&upload.initiated)
					.map_err(|error| {
						S3Error::Io(format!(
							"invalid S3 multipart Initiated timestamp {:?}: {error}",
							upload.initiated
						))
					})?
					.with_timezone(&Utc);
				if initiated < older_than {
					self
						.abort_multipart_upload(&upload.key, &upload.upload_id)
						.await?;
					aborted += 1;
				}
			}
			if !page.is_truncated {
				return Ok(aborted);
			}
			let key_marker = page.next_key_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks a key marker".to_owned())
			})?;
			let upload_id_marker = page.next_upload_id_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks an upload ID marker".to_owned())
			})?;
			markers = Some((key_marker, upload_id_marker));
			if page_number + 1 == MAX_PAGES {
				warn!(prefix, "S3 multipart listing reached the 100-page cap; truncating");
			}
		}
		Ok(aborted)
	}

	/// Aborts stale uploads only after `guard` has locked the candidate's
	/// mount-relative object key and a fresh listing still identifies the same
	/// stale upload.
	///
	/// Guard-acquisition, revalidation, and abort failures leave that upload
	/// untouched and do not stop the sweep.
	pub async fn abort_stale_multipart_uploads_guarded<G, F, E>(
		&self,
		prefix: &str,
		older_than: DateTime<Utc>,
		mut guard: F,
	) -> std::result::Result<usize, S3Error>
	where
		F: FnMut(&str) -> std::result::Result<G, E>,
		E: fmt::Display,
	{
		const MAX_PAGES: usize = 100;
		const PAGE_SIZE: u16 = 1000;

		let prefix = self.object_key(prefix)?;
		let mut markers: Option<(String, String)> = None;
		let mut aborted = 0;
		for page_number in 0..MAX_PAGES {
			let page = self
				.list_multipart_uploads_page(
					&prefix,
					markers.as_ref().map(|(key, _)| key.as_str()),
					markers.as_ref().map(|(_, upload_id)| upload_id.as_str()),
					PAGE_SIZE,
				)
				.await?;
			for upload in page.uploads {
				if self
					.active_uploads
					.lock()
					.contains(&(upload.key.clone(), upload.upload_id.clone()))
					|| multipart_initiated(&upload.initiated)? >= older_than
				{
					continue;
				}
				let relative_key = self.mount_relative_object_key(&upload.key)?;
				let _guard = match guard(&relative_key) {
					Ok(guard) => guard,
					Err(error) => {
						warn!(key = upload.key, "could not guard stale S3 multipart upload: {error}");
						continue;
					},
				};
				let refreshed = match self
					.find_multipart_upload(&upload.key, &upload.upload_id)
					.await
				{
					Ok(upload) => upload,
					Err(error) => {
						warn!(
							key = upload.key,
							"could not revalidate stale S3 multipart upload: {error}"
						);
						continue;
					},
				};
				let Some(refreshed) = refreshed else {
					continue;
				};
				if self
					.active_uploads
					.lock()
					.contains(&(refreshed.key.clone(), refreshed.upload_id.clone()))
					|| multipart_initiated(&refreshed.initiated)? >= older_than
				{
					continue;
				}
				if let Err(error) = self
					.abort_multipart_upload(&refreshed.key, &refreshed.upload_id)
					.await
				{
					warn!(key = refreshed.key, "could not abort stale S3 multipart upload: {error}");
					continue;
				}
				aborted += 1;
			}
			if !page.is_truncated {
				return Ok(aborted);
			}
			let key_marker = page.next_key_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks a key marker".to_owned())
			})?;
			let upload_id_marker = page.next_upload_id_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks an upload ID marker".to_owned())
			})?;
			markers = Some((key_marker, upload_id_marker));
			if page_number + 1 == MAX_PAGES {
				warn!(prefix, "S3 multipart listing reached the 100-page cap; truncating");
			}
		}
		Ok(aborted)
	}

	async fn find_multipart_upload(
		&self,
		key: &str,
		upload_id: &str,
	) -> std::result::Result<Option<MultipartUpload>, S3Error> {
		const MAX_PAGES: usize = 100;
		const PAGE_SIZE: u16 = 1000;

		let mut markers: Option<(String, String)> = None;
		for _ in 0..MAX_PAGES {
			let page = self
				.list_multipart_uploads_page(
					key,
					markers.as_ref().map(|(key, _)| key.as_str()),
					markers.as_ref().map(|(_, upload_id)| upload_id.as_str()),
					PAGE_SIZE,
				)
				.await?;
			if let Some(upload) = page
				.uploads
				.into_iter()
				.find(|upload| upload.key == key && upload.upload_id == upload_id)
			{
				return Ok(Some(upload));
			}
			if !page.is_truncated {
				return Ok(None);
			}
			let key_marker = page.next_key_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks a key marker".to_owned())
			})?;
			let upload_id_marker = page.next_upload_id_marker.ok_or_else(|| {
				S3Error::Io("truncated S3 multipart listing lacks an upload ID marker".to_owned())
			})?;
			markers = Some((key_marker, upload_id_marker));
		}
		warn!(key, "S3 multipart revalidation reached the 100-page cap; treating upload as missing");
		Ok(None)
	}

	async fn list_multipart_uploads_page(
		&self,
		prefix: &str,
		key_marker: Option<&str>,
		upload_id_marker: Option<&str>,
		max_uploads: u16,
	) -> std::result::Result<ListMultipartUploadsResult, S3Error> {
		let mut query = vec![
			("max-uploads".to_owned(), max_uploads.to_string()),
			("prefix".to_owned(), prefix.to_owned()),
			("uploads".to_owned(), String::new()),
		];
		if let Some(marker) = key_marker {
			query.push(("key-marker".to_owned(), marker.to_owned()));
		}
		if let Some(marker) = upload_id_marker {
			query.push(("upload-id-marker".to_owned(), marker.to_owned()));
		}
		let response = self.request(Method::GET, "", &query, None, None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 multipart listing response: {error}")))?;
		from_str(&body)
			.map_err(|error| S3Error::Io(format!("parsing S3 ListMultipartUploads XML: {error}")))
	}

	async fn abort_multipart_upload(
		&self,
		key: &str,
		upload_id: &str,
	) -> std::result::Result<(), S3Error> {
		let query = vec![("uploadId".to_owned(), upload_id.to_owned())];
		let response = self
			.request_with_body_timeout(Method::DELETE, key, &query, None, MULTIPART_TIMEOUT)
			.await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		Ok(())
	}

	async fn upload_part(
		&self,
		key: &str,
		upload_id: &str,
		part_number: u32,
		body: Vec<u8>,
	) -> std::result::Result<String, S3Error> {
		if part_number == 0 || part_number > MULTIPART_MAX_PARTS {
			return Err(S3Error::BadRequest);
		}
		let query = vec![
			("partNumber".to_owned(), part_number.to_string()),
			("uploadId".to_owned(), upload_id.to_owned()),
		];
		let response = self
			.request_with_body_timeout(Method::PUT, key, &query, Some(body), MULTIPART_TIMEOUT)
			.await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		response
			.headers()
			.get(reqwest::header::ETAG)
			.and_then(|etag| etag.to_str().ok())
			.map(str::to_owned)
			.ok_or_else(|| S3Error::Io("multipart upload response omitted ETag".to_owned()))
	}

	fn invalidate_parent(&self, path: &str) {
		if let Some((parent, _)) = path.rsplit_once('/') {
			self.list_cache.lock().remove(parent);
		} else {
			self.list_cache.lock().remove("");
		}
	}

	/// Delete an object from S3.
	pub async fn remove(&self, path: &str) -> std::result::Result<(), S3Error> {
		let key = self.object_key(path)?;
		let response = self
			.request_with_body(Method::DELETE, &key, &[], None)
			.await?;
		if !response.status().is_success() && response.status() != reqwest::StatusCode::NOT_FOUND {
			return Err(response_error(response.status()));
		}
		// Invalidate list cache for parent directory and chunks
		if let Some((parent, _)) = path.rsplit_once('/') {
			self.list_cache.lock().remove(parent);
		} else {
			self.list_cache.lock().remove("");
		}
		Ok(())
	}

	async fn request_with_body(
		&self,
		method: Method,
		key: &str,
		query: &[(String, String)],
		body: Option<Vec<u8>>,
	) -> std::result::Result<reqwest::Response, S3Error> {
		self
			.request_with_body_timeout(method, key, query, body, HTTP_TIMEOUT)
			.await
	}

	async fn request_with_body_timeout(
		&self,
		method: Method,
		key: &str,
		query: &[(String, String)],
		body: Option<Vec<u8>>,
		timeout: Duration,
	) -> std::result::Result<reqwest::Response, S3Error> {
		let (url, canonical_uri, canonical_query) = self.request_url(key, query)?;
		let host = request_host(&url)?;
		let mut request = self.http.request(method.clone(), url).timeout(timeout);
		if let Some(payload) = &body {
			request = request.body(payload.clone());
		}
		if self.cfg.auth != S3Auth::Anonymous {
			let creds = self.cfg.creds.as_ref().ok_or(S3Error::BadRequest)?;
			let now = Utc::now();
			let date = now.format("%Y%m%dT%H%M%SZ").to_string();
			let payload_hash = if let Some(payload) = &body {
				hex::encode(sha2::Sha256::digest(payload))
			} else {
				UNSIGNED_PAYLOAD.to_owned()
			};
			let mut headers = vec![
				("host".to_owned(), host.clone()),
				("x-amz-content-sha256".to_owned(), payload_hash.clone()),
				("x-amz-date".to_owned(), date.clone()),
			];
			if let Some(token) = &creds.session_token {
				headers.push(("x-amz-security-token".to_owned(), token.clone()));
			}
			let authorization = aws::authorization(
				method.as_str(),
				&canonical_uri,
				&canonical_query,
				&headers,
				&payload_hash,
				now,
				&self.cfg.region,
				"s3",
				&creds.access_key,
				&creds.secret_key,
			);
			request = request
				.header("host", host)
				.header("x-amz-content-sha256", payload_hash)
				.header("x-amz-date", date)
				.header("authorization", authorization);
			if let Some(token) = &creds.session_token {
				request = request.header("x-amz-security-token", token);
			}
		}
		request
			.send()
			.await
			.map_err(|error| S3Error::Io(format!("sending S3 request: {error}")))
	}

	async fn copy_request(
		&self,
		source: &str,
		destination: &str,
		query: &[(String, String)],
		copy_range: Option<&str>,
	) -> std::result::Result<reqwest::Response, S3Error> {
		let (url, canonical_uri, canonical_query) = self.request_url(destination, query)?;
		let host = request_host(&url)?;
		let copy_source = format!("/{}/{}", encode_component(&self.cfg.bucket), encode_path(source));
		let mut request = self
			.http
			.request(Method::PUT, url)
			.timeout(MULTIPART_TIMEOUT)
			.header("x-amz-copy-source", &copy_source);
		if let Some(copy_range) = copy_range {
			request = request.header("x-amz-copy-source-range", copy_range);
		}
		if self.cfg.auth != S3Auth::Anonymous {
			let creds = self.cfg.creds.as_ref().ok_or(S3Error::BadRequest)?;
			let now = Utc::now();
			let date = now.format("%Y%m%dT%H%M%SZ").to_string();
			let mut headers = vec![
				("host".to_owned(), host.clone()),
				("x-amz-content-sha256".to_owned(), UNSIGNED_PAYLOAD.to_owned()),
				("x-amz-copy-source".to_owned(), copy_source.clone()),
				("x-amz-date".to_owned(), date.clone()),
			];
			if let Some(copy_range) = copy_range {
				headers.push(("x-amz-copy-source-range".to_owned(), copy_range.to_owned()));
			}
			if let Some(token) = &creds.session_token {
				headers.push(("x-amz-security-token".to_owned(), token.clone()));
			}
			let authorization = aws::authorization(
				Method::PUT.as_str(),
				&canonical_uri,
				&canonical_query,
				&headers,
				UNSIGNED_PAYLOAD,
				now,
				&self.cfg.region,
				"s3",
				&creds.access_key,
				&creds.secret_key,
			);
			request = request
				.header("host", host)
				.header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
				.header("x-amz-date", date)
				.header("authorization", authorization);
			if let Some(token) = &creds.session_token {
				request = request.header("x-amz-security-token", token);
			}
		}
		request
			.send()
			.await
			.map_err(|error| S3Error::Io(format!("sending S3 copy request: {error}")))
	}

	/// Reads up to `len` bytes using cached one-mebibyte ranged GETs.
	pub async fn read(
		&self,
		path: &str,
		offset: u64,
		len: u32,
	) -> std::result::Result<Bytes, S3Error> {
		if len as usize > CHUNK_SIZE {
			return Err(S3Error::BadRequest);
		}
		let stat = self.stat(path).await?;
		if stat.kind != ObjKind::File {
			return Err(S3Error::BadRequest);
		}
		if offset >= stat.size || len == 0 {
			return Ok(Bytes::new());
		}
		let etag = stat
			.etag
			.ok_or_else(|| S3Error::Io("S3 file listing omitted its ETag".to_owned()))?;
		let wanted = u64::from(len).min(stat.size - offset) as usize;
		let first_chunk = offset / CHUNK_SIZE as u64;
		let last_chunk = (offset + wanted as u64 - 1) / CHUNK_SIZE as u64;
		let mut data = BytesMut::with_capacity(wanted);
		let mut remaining = wanted;
		for index in first_chunk..=last_chunk {
			let chunk = self.chunk(path, &etag, index, stat.size).await?;
			let begin = if index == first_chunk {
				(offset % CHUNK_SIZE as u64) as usize
			} else {
				0
			};
			let available = chunk.get(begin..).ok_or_else(|| {
				S3Error::Io("S3 ranged response ended before the requested offset".to_owned())
			})?;
			let take = available.len().min(remaining);
			if take == 0 {
				return Err(S3Error::Io("S3 ranged response ended before EOF".to_owned()));
			}
			data.extend_from_slice(&available[..take]);
			remaining -= take;
		}
		if remaining != 0 {
			return Err(S3Error::Io("S3 ranged response ended before EOF".to_owned()));
		}
		Ok(data.freeze())
	}

	async fn chunk(
		&self,
		path: &str,
		etag: &str,
		index: u64,
		size: u64,
	) -> std::result::Result<Bytes, S3Error> {
		let key = ChunkKey { path: path.to_owned(), etag: etag.to_owned(), index };
		let cached_chunk = self.chunks.lock().get(&key);
		if let Some(chunk) = cached_chunk {
			return Ok(chunk);
		}
		let start = index.saturating_mul(CHUNK_SIZE as u64);
		if start >= size {
			return Ok(Bytes::new());
		}
		let end = start.saturating_add(CHUNK_SIZE as u64 - 1).min(size - 1);
		let chunk = self.fetch_chunk(path, etag, start, end).await?;
		let expected = usize::try_from(end - start + 1).expect("S3 chunk fits usize");
		if chunk.len() > expected {
			return Err(S3Error::Io("S3 ranged response exceeded its requested range".to_owned()));
		}
		self.chunks.lock().insert(key, chunk.clone());
		Ok(chunk)
	}

	async fn list_page_flat(
		&self,
		prefix: &str,
		continuation: Option<&str>,
		max_keys: u16,
	) -> std::result::Result<ListBucketResult, S3Error> {
		let mut query = vec![
			("list-type".to_owned(), "2".to_owned()),
			("max-keys".to_owned(), max_keys.to_string()),
			("prefix".to_owned(), prefix.to_owned()),
		];
		if let Some(token) = continuation {
			query.push(("continuation-token".to_owned(), token.to_owned()));
		}
		let response = self.request(Method::GET, "", &query, None, None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 listing response: {error}")))?;
		from_str(&body).map_err(|error| S3Error::Io(format!("parsing S3 ListObjectsV2 XML: {error}")))
	}

	async fn fetch_chunk(
		&self,
		path: &str,
		etag: &str,
		start: u64,
		end: u64,
	) -> std::result::Result<Bytes, S3Error> {
		let key = self.object_key(path)?;
		let range = format!("bytes={start}-{end}");
		let response = self
			.request(Method::GET, &key, &[], Some(&range), Some(etag))
			.await?;
		if response.status() == StatusCode::PRECONDITION_FAILED {
			return Err(S3Error::Stale);
		}
		if response.status() != StatusCode::PARTIAL_CONTENT {
			return Err(response_error(response.status()));
		}
		response
			.bytes()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 range response: {error}")))
	}

	async fn list_page(
		&self,
		prefix: &str,
		continuation: Option<&str>,
		max_keys: u16,
	) -> std::result::Result<ListBucketResult, S3Error> {
		let mut query = vec![
			("delimiter".to_owned(), "/".to_owned()),
			("list-type".to_owned(), "2".to_owned()),
			("max-keys".to_owned(), max_keys.to_string()),
			("prefix".to_owned(), prefix.to_owned()),
		];
		if let Some(token) = continuation {
			query.push(("continuation-token".to_owned(), token.to_owned()));
		}
		let response = self.request(Method::GET, "", &query, None, None).await?;
		if !response.status().is_success() {
			return Err(response_error(response.status()));
		}
		let body = response
			.text()
			.await
			.map_err(|error| S3Error::Io(format!("reading S3 listing response: {error}")))?;
		from_str(&body).map_err(|error| S3Error::Io(format!("parsing S3 ListObjectsV2 XML: {error}")))
	}

	async fn request(
		&self,
		method: Method,
		key: &str,
		query: &[(String, String)],
		range: Option<&str>,
		if_match: Option<&str>,
	) -> std::result::Result<reqwest::Response, S3Error> {
		let (url, canonical_uri, canonical_query) = self.request_url(key, query)?;
		let host = request_host(&url)?;
		let mut request = self.http.request(method.clone(), url);
		if let Some(range) = range {
			request = request.header("range", range);
		}
		if let Some(etag) = if_match {
			request = request.header("if-match", etag);
		}
		if self.cfg.auth != S3Auth::Anonymous {
			let creds = self.cfg.creds.as_ref().ok_or(S3Error::BadRequest)?;
			let now = Utc::now();
			let date = now.format("%Y%m%dT%H%M%SZ").to_string();
			let mut headers = vec![
				("host".to_owned(), host.clone()),
				("x-amz-content-sha256".to_owned(), UNSIGNED_PAYLOAD.to_owned()),
				("x-amz-date".to_owned(), date.clone()),
			];
			if let Some(token) = &creds.session_token {
				headers.push(("x-amz-security-token".to_owned(), token.clone()));
			}
			let authorization = aws::authorization(
				method.as_str(),
				&canonical_uri,
				&canonical_query,
				&headers,
				UNSIGNED_PAYLOAD,
				now,
				&self.cfg.region,
				"s3",
				&creds.access_key,
				&creds.secret_key,
			);
			request = request
				.header("host", host)
				.header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
				.header("x-amz-date", date)
				.header("authorization", authorization);
			if let Some(token) = &creds.session_token {
				request = request.header("x-amz-security-token", token);
			}
		}
		request
			.send()
			.await
			.map_err(|error| S3Error::Io(format!("sending S3 request: {error}")))
	}

	fn request_url(
		&self,
		key: &str,
		query: &[(String, String)],
	) -> std::result::Result<(Url, String, String), S3Error> {
		request_url_for(&self.cfg.bucket, &self.cfg.region, self.cfg.endpoint.as_deref(), key, query)
	}

	fn object_key(&self, path: &str) -> std::result::Result<String, S3Error> {
		if !valid_relative_path(path) {
			return Err(S3Error::BadRequest);
		}
		Ok(match (self.cfg.prefix.is_empty(), path.is_empty()) {
			(true, true) => String::new(),
			(true, false) => path.to_owned(),
			(false, true) => self.cfg.prefix.clone(),
			(false, false) => format!("{}/{}", self.cfg.prefix, path),
		})
	}

	fn list_prefix(&self, dir: &str) -> std::result::Result<String, S3Error> {
		let key = self.object_key(dir)?;
		Ok(if key.is_empty() {
			key
		} else {
			format!("{key}/")
		})
	}
}

pub(crate) fn request_url_for(
	bucket: &str,
	region: &str,
	endpoint: Option<&str>,
	key: &str,
	query: &[(String, String)],
) -> std::result::Result<(Url, String, String), S3Error> {
	let canonical_query = canonical_query(query);
	let encoded_key = encode_path(key);
	let canonical_uri = if let Some(endpoint) = endpoint {
		let endpoint = endpoint_url(endpoint)?;
		let mut path = endpoint.path().trim_end_matches('/').to_owned();
		path.push('/');
		path.push_str(&encode_component(bucket));
		if !encoded_key.is_empty() {
			path.push('/');
			path.push_str(&encoded_key);
		}
		path
	} else if encoded_key.is_empty() {
		"/".to_owned()
	} else {
		format!("/{encoded_key}")
	};
	let url = if let Some(endpoint) = endpoint {
		let base = endpoint.trim_end_matches('/');
		format!("{base}{canonical_uri}")
	} else {
		format!("https://{bucket}.s3.{region}.amazonaws.com{canonical_uri}")
	};
	let url = if canonical_query.is_empty() {
		url
	} else {
		format!("{url}?{canonical_query}")
	};
	let url = Url::parse(&url).map_err(|_| S3Error::BadRequest)?;
	Ok((url, canonical_uri, canonical_query))
}

/// Parses an `s3://bucket[/prefix]` mount URI into bucket and normalized
/// prefix.
///
/// # Errors
///
/// Returns [`EngineError::invalid`] when the URI lacks an `s3://` scheme or a
/// bucket.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
	let value = uri
		.strip_prefix("s3://")
		.ok_or_else(|| EngineError::invalid(format!("S3 URI must start with s3:// (got {uri:?})")))?;
	let (bucket, prefix) = value
		.split_once('/')
		.map_or((value, ""), |(bucket, prefix)| (bucket, prefix));
	if bucket.is_empty() {
		return Err(EngineError::invalid("S3 URI must include a bucket"));
	}
	Ok((bucket.to_owned(), prefix.trim_matches('/').to_owned()))
}

fn validate_endpoint(endpoint: &str) -> std::result::Result<(), S3Error> {
	endpoint_url(endpoint).map(|_| ())
}

fn endpoint_url(endpoint: &str) -> std::result::Result<Url, S3Error> {
	let url = Url::parse(endpoint).map_err(|_| S3Error::BadRequest)?;
	if !matches!(url.scheme(), "http" | "https")
		|| url.host_str().is_none()
		|| url.query().is_some()
		|| url.fragment().is_some()
		|| !url.username().is_empty()
		|| url.password().is_some()
	{
		return Err(S3Error::BadRequest);
	}
	Ok(url)
}

fn request_host(url: &Url) -> std::result::Result<String, S3Error> {
	let host = url.host_str().ok_or(S3Error::BadRequest)?;
	Ok(url
		.port()
		.map_or_else(|| host.to_owned(), |port| format!("{host}:{port}")))
}

fn response_error(status: StatusCode) -> S3Error {
	match status {
		StatusCode::FORBIDDEN => S3Error::Access,
		StatusCode::NOT_FOUND => S3Error::NotFound,
		StatusCode::PRECONDITION_FAILED => S3Error::Stale,
		_ => S3Error::Io(format!("unexpected S3 HTTP status {status}")),
	}
}

fn validate_content_range(
	value: &str,
	expected_start: u64,
	expected_end: u64,
	expected_size: u64,
) -> std::result::Result<(), S3Error> {
	let value = value
		.strip_prefix("bytes ")
		.ok_or_else(|| S3Error::Io("invalid S3 Content-Range unit".to_owned()))?;
	let (range, size) = value
		.split_once('/')
		.ok_or_else(|| S3Error::Io("invalid S3 Content-Range".to_owned()))?;
	let (start, end) = range
		.split_once('-')
		.ok_or_else(|| S3Error::Io("invalid S3 Content-Range".to_owned()))?;
	let start = start
		.parse::<u64>()
		.map_err(|_| S3Error::Io("invalid S3 Content-Range start".to_owned()))?;
	let end = end
		.parse::<u64>()
		.map_err(|_| S3Error::Io("invalid S3 Content-Range end".to_owned()))?;
	let size = size
		.parse::<u64>()
		.map_err(|_| S3Error::Io("invalid S3 Content-Range size".to_owned()))?;
	if (start, end, size) != (expected_start, expected_end, expected_size) {
		return Err(S3Error::Io("S3 Content-Range did not match the requested range".to_owned()));
	}
	Ok(())
}

async fn read_bounded_response(
	response: reqwest::Response,
	cap: usize,
	context: &str,
) -> std::result::Result<String, S3Error> {
	if response
		.content_length()
		.is_some_and(|length| length > cap as u64)
	{
		return Err(S3Error::Io(format!("S3 {context} exceeds the {cap}-byte limit")));
	}
	let body = response
		.bytes()
		.await
		.map_err(|error| S3Error::Io(format!("reading S3 {context}: {error}")))?;
	if body.len() > cap {
		return Err(S3Error::Io(format!("S3 {context} exceeds the {cap}-byte limit")));
	}
	String::from_utf8(body.to_vec())
		.map_err(|error| S3Error::Io(format!("S3 {context} was not valid UTF-8: {error}")))
}

fn valid_relative_path(path: &str) -> bool {
	path.is_empty()
		|| path
			.split('/')
			.all(|part| !part.is_empty() && part != "." && part != ".." && !part.contains('\0'))
}

fn direct_file_name<'a>(key: &'a str, prefix: &str) -> Option<&'a str> {
	let name = key.strip_prefix(prefix)?;
	if valid_segment(name) {
		Some(name)
	} else {
		None
	}
}

fn direct_dir_name<'a>(prefix_value: &'a str, prefix: &str) -> Option<&'a str> {
	let name = prefix_value.strip_prefix(prefix)?.strip_suffix('/')?;
	if valid_segment(name) {
		Some(name)
	} else {
		None
	}
}

fn valid_segment(segment: &str) -> bool {
	!segment.is_empty()
		&& segment != "."
		&& segment != ".."
		&& !segment.contains('/')
		&& !segment.contains('\0')
}

fn parse_mtime(value: &str) -> std::result::Result<u64, S3Error> {
	let timestamp = DateTime::parse_from_rfc3339(value)
		.map_err(|error| {
			S3Error::Io(format!("invalid S3 LastModified timestamp {value:?}: {error}"))
		})?
		.timestamp();
	u64::try_from(timestamp)
		.map_err(|_| S3Error::Io(format!("S3 LastModified timestamp predates Unix epoch: {value:?}")))
}

fn multipart_initiated(value: &str) -> std::result::Result<DateTime<Utc>, S3Error> {
	DateTime::parse_from_rfc3339(value)
		.map(|timestamp| timestamp.with_timezone(&Utc))
		.map_err(|error| {
			S3Error::Io(format!("invalid S3 multipart Initiated timestamp {value:?}: {error}"))
		})
}

fn merge_listing(
	entries: &mut BTreeMap<String, ObjEntry>,
	prefix: &str,
	page: ListBucketResult,
) -> std::result::Result<(), S3Error> {
	for object in page.contents {
		let Some(name) = direct_file_name(&object.key, prefix) else {
			continue;
		};
		if entries
			.get(name)
			.is_some_and(|entry| entry.kind == ObjKind::Dir)
		{
			continue;
		}
		entries.insert(name.to_owned(), ObjEntry {
			name:  name.to_owned(),
			kind:  ObjKind::File,
			size:  object.size,
			mtime: parse_mtime(&object.last_modified)?,
			etag:  object.etag,
		});
	}
	for prefix_value in page.common_prefixes {
		let Some(name) = direct_dir_name(&prefix_value.prefix, prefix) else {
			continue;
		};
		entries.insert(name.to_owned(), ObjEntry {
			name:  name.to_owned(),
			kind:  ObjKind::Dir,
			size:  0,
			mtime: 0,
			etag:  None,
		});
	}
	Ok(())
}

fn canonical_query(query: &[(String, String)]) -> String {
	let mut query = query
		.iter()
		.map(|(key, value)| (encode_component(key), encode_component(value)))
		.collect::<Vec<_>>();
	query.sort_unstable();
	query
		.into_iter()
		.map(|(key, value)| format!("{key}={value}"))
		.collect::<Vec<_>>()
		.join("&")
}

fn encode_path(path: &str) -> String {
	path
		.split('/')
		.map(encode_component)
		.collect::<Vec<_>>()
		.join("/")
}

fn encode_component(value: &str) -> String {
	let mut encoded = String::with_capacity(value.len());
	for byte in value.bytes() {
		if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
			encoded.push(char::from(byte));
		} else {
			use std::fmt::Write as _;
			let _ = write!(encoded, "%{byte:02X}");
		}
	}
	encoded
}

#[derive(Deserialize)]
#[serde(rename = "ListBucketResult")]
struct ListBucketResult {
	#[serde(rename = "Contents", default)]
	contents:                Vec<ListContents>,
	#[serde(rename = "CommonPrefixes", default)]
	common_prefixes:         Vec<ListPrefix>,
	#[serde(rename = "IsTruncated", default)]
	is_truncated:            bool,
	#[serde(rename = "NextContinuationToken")]
	next_continuation_token: Option<String>,
}

#[derive(Deserialize)]
struct ListContents {
	#[serde(rename = "Key")]
	key:           String,
	#[serde(rename = "Size")]
	size:          u64,
	#[serde(rename = "LastModified")]
	last_modified: String,
	#[serde(rename = "ETag")]
	etag:          Option<String>,
}

#[derive(Deserialize)]
struct ListPrefix {
	#[serde(rename = "Prefix")]
	prefix: String,
}

#[cfg(test)]
mod tests {
	use std::{collections::VecDeque, sync::Arc};

	use axum::{
		Router,
		body::Body,
		extract::State,
		http::{HeaderValue, Request, StatusCode},
		response::{IntoResponse, Response},
		routing::any,
	};
	use chrono::{Duration, TimeZone, Utc};

	use super::*;

	#[derive(Clone)]
	struct MockS3 {
		replies:        Arc<Mutex<VecDeque<MockReply>>>,
		requests:       Arc<Mutex<Vec<String>>>,
		copy_sources:   Arc<Mutex<Vec<Option<String>>>>,
		copy_ranges:    Arc<Mutex<Vec<Option<String>>>>,
		authorizations: Arc<Mutex<Vec<Option<String>>>>,
		content_ranges: Arc<Mutex<VecDeque<Option<String>>>>,
	}

	struct MockReply {
		status: StatusCode,
		body:   &'static str,
		etag:   Option<&'static str>,
	}

	async fn mock_s3(State(mock): State<MockS3>, request: Request<Body>) -> Response {
		mock.requests.lock().push(format!(
			"{} {}",
			request.method(),
			request.uri().path_and_query().unwrap()
		));
		mock.copy_sources.lock().push(
			request
				.headers()
				.get("x-amz-copy-source")
				.and_then(|value| value.to_str().ok())
				.map(str::to_owned),
		);
		mock.copy_ranges.lock().push(
			request
				.headers()
				.get("x-amz-copy-source-range")
				.and_then(|value| value.to_str().ok())
				.map(str::to_owned),
		);
		mock.authorizations.lock().push(
			request
				.headers()
				.get("authorization")
				.and_then(|value| value.to_str().ok())
				.map(str::to_owned),
		);
		let reply = mock
			.replies
			.lock()
			.pop_front()
			.expect("unexpected S3 request");
		let mut response = (reply.status, reply.body).into_response();
		if let Some(etag) = reply.etag {
			response
				.headers_mut()
				.insert(reqwest::header::ETAG, HeaderValue::from_static(etag));
		}
		if request.headers().contains_key(reqwest::header::RANGE)
			&& let Some(Some(content_range)) = mock.content_ranges.lock().pop_front()
		{
			response.headers_mut().insert(
				reqwest::header::CONTENT_RANGE,
				HeaderValue::from_str(&content_range).expect("valid mock Content-Range"),
			);
		}
		if request.method() == Method::HEAD {
			response
				.headers_mut()
				.insert(reqwest::header::CONTENT_LENGTH, HeaderValue::from_static("4294967296"));
			response.headers_mut().insert(
				reqwest::header::LAST_MODIFIED,
				HeaderValue::from_static("Mon, 01 Jan 2024 00:00:00 GMT"),
			);
		}
		response
	}

	async fn mock_client(
		replies: Vec<MockReply>,
	) -> (S3Client, MockS3, tokio::task::JoinHandle<()>) {
		let mock = MockS3 {
			replies:        Arc::new(Mutex::new(replies.into())),
			requests:       Arc::new(Mutex::new(Vec::new())),
			copy_sources:   Arc::new(Mutex::new(Vec::new())),
			copy_ranges:    Arc::new(Mutex::new(Vec::new())),
			authorizations: Arc::new(Mutex::new(Vec::new())),
			content_ranges: Arc::new(Mutex::new(VecDeque::new())),
		};
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("bind mock S3");
		let endpoint = format!("http://{}", listener.local_addr().expect("mock address"));
		let server_mock = mock.clone();
		let server = tokio::spawn(async move {
			axum::serve(listener, Router::new().fallback(any(mock_s3)).with_state(server_mock))
				.await
				.expect("serve mock S3");
		});
		let client = S3Client::new(S3MountConfig {
			bucket:    "bucket".to_owned(),
			prefix:    String::new(),
			region:    "us-east-1".to_owned(),
			endpoint:  Some(endpoint),
			read_only: false,
			creds:     None,
			auth:      S3Auth::Anonymous,
		})
		.expect("mock S3 config");
		(client, mock, server)
	}

	#[tokio::test]
	async fn copy_object_sends_signed_encoded_copy_source() {
		let (mut client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::OK,
			body:   "<CopyObjectResult><ETag>&quot;copy&quot;</ETag></CopyObjectResult>",
			etag:   None,
		}])
		.await;
		client.cfg.auth = S3Auth::Inline;
		client.cfg.creds = Some(S3Credentials {
			access_key:    "access".to_owned(),
			secret_key:    "secret".to_owned(),
			session_token: None,
		});

		client
			.copy_object("source folder/input", "final/output", 1024)
			.await
			.expect("copy succeeds");
		assert_eq!(&*mock.requests.lock(), &["PUT /bucket/final/output"]);
		assert_eq!(&*mock.copy_sources.lock(), &[Some("/bucket/source%20folder/input".to_owned())]);
		assert!(
			mock.authorizations.lock()[0]
				.as_deref()
				.is_some_and(|value| value.starts_with("AWS4-HMAC-SHA256 "))
		);
		server.abort();
	}

	#[tokio::test]
	async fn copy_object_embedded_error_fails() {
		let (client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::OK,

			body: "<Error><Code>InternalError</Code><Message>retry</Message></Error>",
			etag: None,
		}])
		.await;

		let error = client
			.copy_object("source", "destination", 1024)
			.await
			.expect_err("embedded error fails");
		assert!(error.to_string().contains("InternalError"));
		assert_eq!(&*mock.copy_sources.lock(), &[Some("/bucket/source".to_owned())]);
		server.abort();
	}
	#[tokio::test]
	async fn head_object_bypasses_large_listing_and_returns_exact_metadata() {
		let (client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::OK,
			body:   "",
			etag:   Some("\"head-etag\""),
		}])
		.await;

		let stat = client
			.head_object("beyond-listing-cap/object")
			.await
			.expect("HEAD succeeds without a listing");
		assert_eq!(stat.kind, ObjKind::File);
		assert_eq!(stat.size, 4_294_967_296);
		assert_eq!(stat.etag.as_deref(), Some("\"head-etag\""));
		assert_eq!(
			stat.mtime,
			Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
				.single()
				.unwrap()
				.timestamp() as u64
		);
		assert_eq!(&*mock.requests.lock(), &["HEAD /bucket/beyond-listing-cap/object"]);
		server.abort();
	}

	fn exact_stat() -> ObjStat {
		ObjStat { kind: ObjKind::File, size: 6, mtime: 0, etag: Some("\"etag\"".to_owned()) }
	}

	#[tokio::test]
	async fn exact_range_bypasses_listing_and_validates_content_range() {
		let (client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::PARTIAL_CONTENT,
			body:   "abc",
			etag:   None,
		}])
		.await;
		mock
			.content_ranges
			.lock()
			.push_back(Some("bytes 0-2/6".to_owned()));

		assert_eq!(
			client
				.read_range_if_match("beyond-listing-cap/object", &exact_stat(), 0, 3)
				.await
				.expect("exact range succeeds"),
			Bytes::from_static(b"abc")
		);
		assert_eq!(&*mock.requests.lock(), &["GET /bucket/beyond-listing-cap/object"]);
		server.abort();
	}

	#[tokio::test]
	async fn exact_range_maps_mutation_to_stale() {
		let (client, _mock, server) = mock_client(vec![MockReply {
			status: StatusCode::PRECONDITION_FAILED,
			body:   "",
			etag:   None,
		}])
		.await;

		assert_eq!(
			client
				.read_range_if_match("object", &exact_stat(), 0, 3)
				.await
				.expect_err("If-Match mutation fails"),
			S3Error::Stale
		);
		server.abort();
	}

	#[tokio::test]
	async fn exact_range_rejects_malformed_content_range() {
		let (client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::PARTIAL_CONTENT,
			body:   "abc",
			etag:   None,
		}])
		.await;
		mock
			.content_ranges
			.lock()
			.push_back(Some("not-a-range".to_owned()));

		assert!(matches!(
			client
				.read_range_if_match("object", &exact_stat(), 0, 3)
				.await,
			Err(S3Error::Io(_))
		));
		server.abort();
	}

	#[tokio::test]
	async fn exact_range_rejects_short_or_oversize_bodies() {
		let (client, mock, server) = mock_client(vec![
			MockReply { status: StatusCode::PARTIAL_CONTENT, body: "ab", etag: None },
			MockReply { status: StatusCode::PARTIAL_CONTENT, body: "abcd", etag: None },
		])
		.await;
		mock
			.content_ranges
			.lock()
			.extend([Some("bytes 0-2/6".to_owned()), Some("bytes 0-2/6".to_owned())]);

		for _ in 0..2 {
			assert!(matches!(
				client
					.read_range_if_match("object", &exact_stat(), 0, 3)
					.await,
				Err(S3Error::Io(_))
			));
		}
		server.abort();
	}

	#[tokio::test]
	async fn scan_keys_page_consumes_three_continuation_pages() {
		let (client, _mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>one</\
				         NextContinuationToken><Contents><Key>logs/one</\
				         Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>&quot;one&quot;</\
				         ETag><Size>1</Size></Contents></ListBucketResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>two</\
				         NextContinuationToken><Contents><Key>logs/two</\
				         Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>&quot;two&quot;</\
				         ETag><Size>2</Size></Contents></ListBucketResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>logs/three</\
				         Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>&quot;three&quot;\
				         </ETag><Size>3</Size></Contents></ListBucketResult>",
				etag:   None,
			},
		])
		.await;

		let first = client
			.scan_keys_page("logs", None, 1)
			.await
			.expect("first page");
		let second = client
			.scan_keys_page("logs", first.next_continuation_token.as_deref(), 1)
			.await
			.expect("second page");
		let third = client
			.scan_keys_page("logs", second.next_continuation_token.as_deref(), 1)
			.await
			.expect("third page");
		assert_eq!(first.entries[0].key, "logs/one");
		assert_eq!(second.entries[0].key, "logs/two");
		assert_eq!(third.entries[0].key, "logs/three");
		assert!(third.next_continuation_token.is_none());
		server.abort();
	}

	#[tokio::test]
	async fn large_copy_uses_ranged_multipart_copy_and_completes() {
		let (client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></\
				         InitiateMultipartUploadResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<CopyPartResult><ETag>&quot;part-1&quot;</ETag></CopyPartResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<CopyPartResult><ETag>&quot;part-2&quot;</ETag></CopyPartResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<CompleteMultipartUploadResult/>",
				etag:   None,
			},
		])
		.await;

		client
			.copy_object("source", "destination", COPY_OBJECT_MAX_SIZE + 1)
			.await
			.expect("multipart copy succeeds");
		assert_eq!(&*mock.copy_ranges.lock(), &[
			None,
			Some("bytes=0-5368709119".to_owned()),
			Some("bytes=5368709120-5368709120".to_owned()),
			None,
		]);
		assert!(
			mock
				.requests
				.lock()
				.iter()
				.any(|request| request == "POST /bucket/destination?uploadId=upload-1")
		);
		server.abort();
	}

	#[tokio::test]
	async fn multipart_copy_part_error_aborts_upload() {
		let (client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></\
				         InitiateMultipartUploadResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<Error><Code>InternalError</Code><Message>retry</Message></Error>",
				etag:   None,
			},
			MockReply { status: StatusCode::NO_CONTENT, body: "", etag: None },
		])
		.await;

		let error = client
			.copy_object("source", "destination", COPY_OBJECT_MAX_SIZE + 1)
			.await
			.expect_err("part error fails copy");
		assert!(error.to_string().contains("InternalError"));
		assert!(
			mock
				.requests
				.lock()
				.iter()
				.any(|request| request == "DELETE /bucket/destination?uploadId=upload-1")
		);
		server.abort();
	}

	#[tokio::test]
	async fn multipart_complete_embedded_error_aborts_upload() {
		let (client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<InitiateMultipartUploadResult><UploadId>upload-1</UploadId></\
				         InitiateMultipartUploadResult>",
				etag:   None,
			},
			MockReply { status: StatusCode::OK, body: "", etag: Some("\"part-1\"") },
			MockReply {
				status: StatusCode::OK,
				body:   "<Error><Code>InternalError</Code><Message>retry</Message></Error>",
				etag:   None,
			},
			MockReply { status: StatusCode::NO_CONTENT, body: "", etag: None },
		])
		.await;
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		sender
			.send(Ok(Bytes::from_static(b"body")))
			.await
			.expect("send body");
		drop(sender);

		let error = client
			.put_multipart("object", receiver)
			.await
			.expect_err("embedded error fails");
		assert!(error.to_string().contains("InternalError"));
		assert!(
			mock
				.requests
				.lock()
				.iter()
				.any(|request| request == "DELETE /bucket/object?uploadId=upload-1")
		);
		server.abort();
	}

	#[tokio::test]
	async fn stale_multipart_janitor_aborts_residue_but_retains_young_uploads() {
		let (client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<ListMultipartUploadsResult><IsTruncated>false</\
				         IsTruncated><Upload><Key>residue/dead</Key><UploadId>old-upload</\
				         UploadId><Initiated>2024-01-01T00:00:00Z</Initiated></\
				         Upload><Upload><Key>residue/live</Key><UploadId>young-upload</\
				         UploadId><Initiated>2025-01-01T00:00:00Z</Initiated></Upload></\
				         ListMultipartUploadsResult>",
				etag:   None,
			},
			MockReply { status: StatusCode::NO_CONTENT, body: "", etag: None },
		])
		.await;

		let aborted = client
			.abort_stale_multipart_uploads(
				"residue",
				Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).single().unwrap(),
			)
			.await
			.expect("janitor succeeds");
		assert_eq!(aborted, 1);
		let requests = mock.requests.lock();
		assert!(
			requests
				.iter()
				.any(|request| request == "DELETE /bucket/residue/dead?uploadId=old-upload")
		);
		assert!(
			!requests
				.iter()
				.any(|request| request.contains("young-upload"))
		);
		server.abort();
	}

	#[tokio::test]
	async fn stale_multipart_janitor_skips_old_in_process_upload() {
		let (client, mock, server) = mock_client(vec![MockReply {
			status: StatusCode::OK,
			body:   "<ListMultipartUploadsResult><IsTruncated>false</\
			         IsTruncated><Upload><Key>residue/active</Key><UploadId>active-upload</\
			         UploadId><Initiated>2024-01-01T00:00:00Z</Initiated></Upload></\
			         ListMultipartUploadsResult>",
			etag:   None,
		}])
		.await;
		client
			.active_uploads
			.lock()
			.insert(("residue/active".to_owned(), "active-upload".to_owned()));

		assert_eq!(
			client
				.abort_stale_multipart_uploads(
					"residue",
					Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).single().unwrap(),
				)
				.await
				.expect("janitor succeeds"),
			0
		);
		assert_eq!(mock.requests.lock().len(), 1);
		server.abort();
	}

	#[tokio::test]
	async fn guarded_janitor_revalidates_after_delayed_publisher() {
		let (mut client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<ListMultipartUploadsResult><IsTruncated>false</\
				         IsTruncated><Upload><Key>mount/residue/publishing</Key><UploadId>old-upload</\
				         UploadId><Initiated>2024-01-01T00:00:00Z</Initiated></Upload></\
				         ListMultipartUploadsResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></\
				         ListMultipartUploadsResult>",
				etag:   None,
			},
		])
		.await;
		client.cfg.prefix = "mount".to_owned();
		let guarded_keys = Arc::new(Mutex::new(Vec::new()));
		let observed_keys = guarded_keys.clone();

		assert_eq!(
			client
				.abort_stale_multipart_uploads_guarded(
					"residue",
					Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).single().unwrap(),
					move |key| {
						observed_keys.lock().push(key.to_owned());
						Ok::<_, S3Error>(())
					},
				)
				.await
				.expect("guarded janitor succeeds"),
			0
		);
		assert_eq!(&*guarded_keys.lock(), &["residue/publishing"]);
		assert_eq!(mock.requests.lock().len(), 2);
		server.abort();
	}

	#[tokio::test]
	async fn stale_multipart_janitor_uses_both_pagination_markers() {
		let (client, mock, server) = mock_client(vec![
			MockReply {
				status: StatusCode::OK,
				body:   "<ListMultipartUploadsResult><IsTruncated>true</\
				         IsTruncated><NextKeyMarker>residue/first</\
				         NextKeyMarker><NextUploadIdMarker>first-upload</NextUploadIdMarker></\
				         ListMultipartUploadsResult>",
				etag:   None,
			},
			MockReply {
				status: StatusCode::OK,
				body:   "<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></\
				         ListMultipartUploadsResult>",
				etag:   None,
			},
		])
		.await;

		assert_eq!(
			client
				.abort_stale_multipart_uploads("residue", Utc::now() - Duration::days(1))
				.await
				.expect("janitor succeeds"),
			0
		);
		assert!(mock.requests.lock().iter().any(|request| {
			request
				== "GET /bucket?key-marker=residue%2Ffirst&max-uploads=1000&prefix=residue&\
				    upload-id-marker=first-upload&uploads="
		}));
		server.abort();
	}

	#[test]
	fn parses_s3_uri_and_normalizes_prefix() {
		assert_eq!(parse_s3_uri("s3://bucket").unwrap(), ("bucket".to_owned(), String::new()));
		assert_eq!(
			parse_s3_uri("s3://bucket//prefix/nested/").unwrap(),
			("bucket".to_owned(), "prefix/nested".to_owned())
		);
		assert!(parse_s3_uri("https://bucket/prefix").is_err());
		assert!(parse_s3_uri("s3://").is_err());
	}

	#[test]
	fn signs_published_s3_get_object_vector() {
		let credentials = S3Credentials {
			access_key:    "AKIAIOSFODNN7EXAMPLE".to_owned(),
			secret_key:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_owned(),
			session_token: None,
		};
		let headers = vec![
			("host".to_owned(), "examplebucket.s3.amazonaws.com".to_owned()),
			("range".to_owned(), "bytes=0-9".to_owned()),
			(
				"x-amz-content-sha256".to_owned(),
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_owned(),
			),
			("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
		];
		let timestamp = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).single().unwrap();
		let authorization = aws::authorization(
			"GET",
			"/test.txt",
			"",
			&headers,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			timestamp,
			"us-east-1",
			"s3",
			&credentials.access_key,
			&credentials.secret_key,
		);
		assert_eq!(
			authorization.split("Signature=").nth(1),
			Some("f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
		);
	}

	#[test]
	fn parses_truncated_listing_xml_and_merges_directory_over_file() {
		let page: ListBucketResult = from_str(
			r"<ListBucketResult>
				<IsTruncated>true</IsTruncated>
				<NextContinuationToken>next-token</NextContinuationToken>
				<Contents><Key>prefix/a.txt</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;a&quot;</ETag><Size>5</Size></Contents>
				<Contents><Key>prefix/dir</Key><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>&quot;file-dir&quot;</ETag><Size>7</Size></Contents>
				<CommonPrefixes><Prefix>prefix/dir/</Prefix></CommonPrefixes>
			</ListBucketResult>",
		)
		.expect("valid ListObjectsV2 fixture");
		assert!(page.is_truncated);
		assert_eq!(page.next_continuation_token.as_deref(), Some("next-token"));
		let mut entries = BTreeMap::new();
		merge_listing(&mut entries, "prefix/", page).unwrap();
		assert_eq!(entries["a.txt"].kind, ObjKind::File);
		assert_eq!(entries["dir"].kind, ObjKind::Dir);
	}

	#[test]
	fn credential_debug_output_is_redacted() {
		let credentials = S3Credentials {
			access_key:    "access-secret".to_owned(),
			secret_key:    "secret-secret".to_owned(),
			session_token: Some("session-secret".to_owned()),
		};
		let debug = format!("{credentials:?}");
		assert_eq!(debug, "S3Credentials(redacted)");
		assert!(!debug.contains("secret"));
	}
}
