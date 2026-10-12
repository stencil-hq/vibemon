//! Lazy HTTP range-backed block source with persistent sparse caches.
//!
//! The remote object is a [`crate::block_image`]: one zstd frame per 1 MiB
//! block plus a local seek index. Requests are serviced synchronously with
//! positioned file I/O on the device worker (Linux and macOS).

use std::{
	fs::{File, OpenOptions},
	io::{self, Read},
	os::unix::fs::{FileExt, OpenOptionsExt},
	path::Path,
	thread,
	time::Duration,
};

use reqwest::{StatusCode, Url, blocking::Client, header};
use vmon_cloud::{ObjectAuth, aws, google};

use crate::{
	block_image::{BLOCK_SIZE, BlockIndex, MAX_INDEX_BYTES, decompress_block},
	result::{Result, err},
};

/// Cache granularity: one block-image block.
pub(super) const REMOTE_CHUNK_SIZE: u64 = BLOCK_SIZE;
const SECTOR_SIZE: u64 = 512;
const HEADER_SIZE: u64 = 32;
const CACHE_KIND: u8 = 1;
const OVERLAY_KIND: u8 = 2;
const MAGIC: &[u8; 8] = b"VMONBLK1";
const MAX_ATTEMPTS: usize = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Compressed bytes fetched per range request while materializing a whole
/// disk; zero-chunk runs (tiny frames) collapse into few requests.
const MATERIALIZE_BATCH_BYTES: u64 = 8 * 1024 * 1024;

/// Loads a block-image index and checks it fits a `logical_size` disk.
fn load_index(path: &Path, logical_size: u64) -> Result<BlockIndex> {
	let file = File::open(path)
		.map_err(|e| err(format!("opening remote block index {}: {e}", path.display())))?;
	let mut bytes = Vec::new();
	file
		.take(MAX_INDEX_BYTES + 1)
		.read_to_end(&mut bytes)
		.map_err(|e| err(format!("reading remote block index {}: {e}", path.display())))?;
	let index = BlockIndex::from_json(&bytes)
		.map_err(|e| err(format!("remote block index {}: {e}", path.display())))?;
	validate_logical_size(logical_size, index.uncompressed_size)?;
	Ok(index)
}

fn validate_index(index: &BlockIndex, logical_size: u64) -> Result<()> {
	index
		.validate()
		.map_err(|e| err(format!("remote block index: {e}")))?;
	validate_logical_size(logical_size, index.uncompressed_size)
}

#[derive(Debug)]
struct FetchFailure {
	message:   String,
	transient: bool,
}

#[derive(Debug)]
struct FetchResponse {
	bytes:     Vec<u8>,
	total_len: u64,
}

trait RangeFetcher: Send {
	fn fetch(&mut self, start: u64, end: u64) -> std::result::Result<FetchResponse, FetchFailure>;
}

enum HttpAuth {
	None,
	Bearer(String),
	Google(google::TokenProvider),
	Aws { provider: aws::CredentialProvider, region: String },
}

impl HttpAuth {
	fn invalidate(&mut self) -> bool {
		match self {
			Self::Google(provider) => {
				provider.invalidate();
				true
			},
			Self::Aws { provider, .. } => {
				provider.invalidate();
				true
			},
			Self::None | Self::Bearer(_) => false,
		}
	}
}

struct HttpRangeFetcher {
	client: Client,
	url:    Url,
	auth:   HttpAuth,
	etag:   String,
}

impl HttpRangeFetcher {
	fn new(
		url: String,
		bearer: Option<String>,
		auth: ObjectAuth,
		region: Option<String>,
		etag: Option<String>,
	) -> Result<Self> {
		let client = Client::builder()
			.connect_timeout(CONNECT_TIMEOUT)
			.timeout(REQUEST_TIMEOUT)
			.build()
			.map_err(|e| err(format!("building remote block HTTP client: {e}")))?;
		let url = Url::parse(&url).map_err(|e| err(format!("invalid remote block URL: {e}")))?;
		if !matches!(url.scheme(), "http" | "https") {
			return Err(err("remote block URL must use http:// or https://"));
		}
		if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
			return Err(err("remote block URL must not contain credentials or a fragment"));
		}
		if (auth != ObjectAuth::None || bearer.is_some()) && url.scheme() != "https" {
			return Err(err("remote block authentication requires an https:// URL"));
		}
		if bearer.as_deref().is_some_and(str::is_empty) {
			return Err(err("remote block bearer must not be empty"));
		}
		if auth != ObjectAuth::Aws && region.is_some() {
			return Err(err("remote block region requires AWS workload auth"));
		}
		if auth == ObjectAuth::Aws
			&& region
				.as_deref()
				.is_none_or(|region| !is_valid_aws_region(region))
		{
			return Err(err("AWS remote block auth requires a valid region"));
		}
		validate_workload_url(&url, auth, region.as_deref())?;
		let etag = etag.ok_or_else(|| err("remote block requires an immutable ETag"))?;
		if !is_strong_etag(&etag) {
			return Err(err("remote block ETag must be a quoted strong entity-tag"));
		}
		let auth = match (auth, bearer) {
			(ObjectAuth::None, None) => HttpAuth::None,
			(ObjectAuth::None, Some(token)) => HttpAuth::Bearer(token),
			(ObjectAuth::Google, None) => HttpAuth::Google(
				google::TokenProvider::new()
					.map_err(|e| err(format!("building Google workload auth: {e}")))?,
			),
			(ObjectAuth::Aws, None) => HttpAuth::Aws {
				provider: aws::CredentialProvider::new()
					.map_err(|e| err(format!("building AWS workload auth: {e}")))?,
				region:   region.ok_or_else(|| err("AWS remote block auth requires a region"))?,
			},
			(ObjectAuth::Google | ObjectAuth::Aws, Some(_)) => {
				return Err(err("remote block bearer cannot be combined with workload auth"));
			},
		};
		Ok(Self { client, url, auth, etag })
	}
}

impl RangeFetcher for HttpRangeFetcher {
	fn fetch(&mut self, start: u64, end: u64) -> std::result::Result<FetchResponse, FetchFailure> {
		let range = format!("bytes={start}-{}", end - 1);
		let mut request = self
			.client
			.get(self.url.clone())
			.header(header::RANGE, &range)
			.header(header::IF_MATCH, &self.etag);
		match &mut self.auth {
			HttpAuth::None => {},
			HttpAuth::Bearer(token) => request = request.bearer_auth(token),
			HttpAuth::Google(provider) => {
				let token = provider.token().map_err(|e| FetchFailure {
					message:   format!("Google workload auth failed: {e}"),
					transient: true,
				})?;
				request = request.bearer_auth(token);
			},
			HttpAuth::Aws { provider, region } => {
				let credentials = provider.credentials().map_err(|e| FetchFailure {
					message:   format!("AWS workload auth failed: {e}"),
					transient: true,
				})?;
				let (canonical_uri, canonical_query, host) =
					aws::canonical_url(&self.url).map_err(|e| FetchFailure {
						message:   format!("AWS request URL is invalid: {e}"),
						transient: false,
					})?;
				let mut headers = vec![
					("host".to_owned(), host.clone()),
					("range".to_owned(), range),
					("x-amz-content-sha256".to_owned(), "UNSIGNED-PAYLOAD".to_owned()),
					("if-match".to_owned(), self.etag.clone()),
				];
				if let Some(token) = &credentials.session_token {
					headers.push(("x-amz-security-token".to_owned(), token.clone()));
				}
				let signed = aws::authorization_now(
					"GET",
					&canonical_uri,
					&canonical_query,
					&headers,
					"UNSIGNED-PAYLOAD",
					region,
					"s3",
					&credentials.access_key,
					&credentials.secret_key,
				);
				request = request
					.header("host", host)
					.header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
					.header("x-amz-date", signed.date)
					.header("authorization", signed.authorization);
				if let Some(token) = &credentials.session_token {
					request = request.header("x-amz-security-token", token);
				}
			},
		}
		let mut response = request.send().map_err(|e| FetchFailure {
			message:   format!("request failed: {e}"),
			transient: e.is_timeout() || e.is_connect() || e.is_request() || e.is_body(),
		})?;
		let status = response.status();
		if status != StatusCode::PARTIAL_CONTENT {
			let refreshable_auth_failure =
				matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
					|| (status == StatusCode::BAD_REQUEST
						&& matches!(&self.auth, HttpAuth::Aws { .. })
						&& {
							let mut body = String::new();
							(&mut response).take(4096).read_to_string(&mut body).is_ok()
								&& is_refreshable_aws_auth_error(&body)
						});
			let refreshed = refreshable_auth_failure && self.auth.invalidate();
			return Err(FetchFailure {
				message:   format!("server returned HTTP {status}, expected 206 Partial Content"),
				transient: refreshed
					|| status == StatusCode::REQUEST_TIMEOUT
					|| status == StatusCode::TOO_MANY_REQUESTS
					|| status.is_server_error(),
			});
		}
		let expected_length = end - start;
		if response.content_length() != Some(expected_length) {
			return Err(FetchFailure {
				message:   format!(
					"range response Content-Length {:?} does not match requested {expected_length}",
					response.content_length()
				),
				transient: false,
			});
		}
		let response_etag = response
			.headers()
			.get(header::ETAG)
			.and_then(|value| value.to_str().ok());
		if response_etag != Some(self.etag.as_str()) {
			return Err(FetchFailure {
				message:   format!(
					"response ETag {:?} does not match immutable object ETag {:?}",
					response_etag, self.etag
				),
				transient: false,
			});
		}
		let content_range = response
			.headers()
			.get(header::CONTENT_RANGE)
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| FetchFailure {
				message:   "206 response is missing a valid Content-Range header".to_string(),
				transient: false,
			})?;
		let total_len = parse_content_range(content_range, start, end)?;
		let bytes = response.bytes().map_err(|e| FetchFailure {
			message:   format!("reading response body failed: {e}"),
			transient: true,
		})?;
		if bytes.len() as u64 != end - start {
			return Err(FetchFailure {
				message:   format!(
					"short range body: got {} bytes, expected {}",
					bytes.len(),
					end - start
				),
				transient: true,
			});
		}
		Ok(FetchResponse { bytes: bytes.to_vec(), total_len })
	}
}

/// Range source backed by a caller closure (the node's own object client).
struct FnFetcher<F> {
	fetch:     F,
	total_len: u64,
}

impl<F> RangeFetcher for FnFetcher<F>
where
	F: FnMut(u64, u64) -> io::Result<Vec<u8>> + Send,
{
	fn fetch(&mut self, start: u64, end: u64) -> std::result::Result<FetchResponse, FetchFailure> {
		let bytes = (self.fetch)(start, end).map_err(|e| FetchFailure {
			transient: e.kind() != io::ErrorKind::InvalidData,
			message:   e.to_string(),
		})?;
		if bytes.len() as u64 != end - start {
			return Err(FetchFailure {
				message:   format!("short range: got {} bytes, expected {}", bytes.len(), end - start),
				transient: true,
			});
		}
		Ok(FetchResponse { bytes, total_len: self.total_len })
	}
}

/// Writes the guest-visible disk of a stopped remote-backed VM as the new raw
/// image `destination`.
///
/// See [`RemoteBlockSource::materialize`]: `overlay` and `cache` are the
/// files the VMM used, `index` the image's seek index, and
/// `fetch(start, end)` returns the object's bytes `[start, end)` (it must pin
/// the object version itself). Returns the image length.
pub fn materialize_remote_disk<F>(
	overlay: &Path,
	cache: &Path,
	index: &Path,
	logical_size: u64,
	fetch: F,
	destination: &Path,
) -> Result<u64>
where
	F: FnMut(u64, u64) -> io::Result<Vec<u8>> + Send + 'static,
{
	let index = load_index(index, logical_size)?;
	let overlay_file = OpenOptions::new()
		.read(true)
		.write(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(overlay)
		.map_err(|e| err(format!("opening remote block overlay {}: {e}", overlay.display())))?;
	let total_len = index.compressed_size;
	let mut source = RemoteBlockSource::with_fetcher_and_overlay(
		cache,
		overlay_file,
		index,
		logical_size,
		Box::new(FnFetcher { fetch, total_len }),
	)?;
	source.materialize(destination)
}

fn is_refreshable_aws_auth_error(body: &str) -> bool {
	[
		"<Code>ExpiredToken</Code>",
		"<Code>InvalidToken</Code>",
		"<Code>RequestExpired</Code>",
		"<Code>TokenRefreshRequired</Code>",
	]
	.iter()
	.any(|code| body.contains(code))
}

fn validate_workload_url(url: &Url, auth: ObjectAuth, region: Option<&str>) -> Result<()> {
	match auth {
		ObjectAuth::None => Ok(()),
		ObjectAuth::Google => {
			if url.host_str() != Some("storage.googleapis.com")
				|| url.port_or_known_default() != Some(443)
				|| !has_query_identity(url, "generation", |value| {
					value.parse::<u64>().is_ok_and(|generation| generation != 0)
				}) {
				return Err(err(
					"Google remote block URL must target storage.googleapis.com with a positive \
					 generation",
				));
			}
			Ok(())
		},
		ObjectAuth::Aws => {
			let region = region.ok_or_else(|| err("AWS remote block auth requires a region"))?;
			if !is_trusted_aws_object_origin(url, region)
				|| !has_query_identity(url, "versionId", |value| !value.is_empty() && value != "null")
			{
				return Err(err(
					"AWS remote block URL must target the configured S3 origin with a versionId",
				));
			}
			Ok(())
		},
	}
}

fn has_query_identity(url: &Url, name: &str, valid: impl FnOnce(&str) -> bool) -> bool {
	let mut values = url.query_pairs().filter(|(key, _)| key == name);
	let Some((_, value)) = values.next() else {
		return false;
	};
	values.next().is_none() && valid(&value)
}

fn is_trusted_aws_object_origin(url: &Url, region: &str) -> bool {
	let Some(host) = url.host_str() else {
		return false;
	};
	let standard_suffix = if region.starts_with("cn-") {
		format!(".s3.{region}.amazonaws.com.cn")
	} else {
		format!(".s3.{region}.amazonaws.com")
	};
	let path_style_host = standard_suffix.trim_start_matches('.');
	if (host == path_style_host
		|| (host.ends_with(&standard_suffix) && host.len() > standard_suffix.len()))
		&& url.port_or_known_default() == Some(443)
	{
		return true;
	}
	["AWS_ENDPOINT_URL_S3", "VMON_S3_ENDPOINT"]
		.into_iter()
		.find_map(|name| {
			std::env::var(name)
				.ok()
				.filter(|value| !value.trim().is_empty())
		})
		.and_then(|endpoint| Url::parse(&endpoint).ok())
		.is_some_and(|endpoint| {
			endpoint.scheme() == url.scheme()
				&& endpoint.host_str() == url.host_str()
				&& endpoint.port_or_known_default() == url.port_or_known_default()
		})
}

fn is_valid_aws_region(region: &str) -> bool {
	let bytes = region.as_bytes();
	(3..=64).contains(&bytes.len())
		&& bytes.first().is_some_and(u8::is_ascii_lowercase)
		&& bytes.last().is_some_and(u8::is_ascii_digit)
		&& bytes
			.iter()
			.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
		&& !region.contains("--")
}

fn is_strong_etag(etag: &str) -> bool {
	let bytes = etag.as_bytes();
	bytes.len() > 2
		&& bytes.first() == Some(&b'"')
		&& bytes.last() == Some(&b'"')
		&& bytes[1..bytes.len() - 1]
			.iter()
			.all(|byte| *byte == 0x21 || (0x23..=0x7e).contains(byte))
}

fn parse_content_range(
	value: &str,
	expected_start: u64,
	expected_end: u64,
) -> std::result::Result<u64, FetchFailure> {
	let value = value.strip_prefix("bytes ").ok_or_else(|| FetchFailure {
		message:   format!("invalid Content-Range {value:?}"),
		transient: false,
	})?;
	let (range, total) = value.split_once('/').ok_or_else(|| FetchFailure {
		message:   format!("invalid Content-Range {value:?}"),
		transient: false,
	})?;
	let (start, end) = range.split_once('-').ok_or_else(|| FetchFailure {
		message:   format!("invalid Content-Range {value:?}"),
		transient: false,
	})?;
	let start = start.parse::<u64>().map_err(|e| FetchFailure {
		message:   format!("invalid Content-Range start: {e}"),
		transient: false,
	})?;
	let end = end.parse::<u64>().map_err(|e| FetchFailure {
		message:   format!("invalid Content-Range end: {e}"),
		transient: false,
	})?;
	let total = total.parse::<u64>().map_err(|e| FetchFailure {
		message:   format!("invalid Content-Range total: {e}"),
		transient: false,
	})?;
	if start != expected_start || end.checked_add(1) != Some(expected_end) || expected_end > total {
		return Err(FetchFailure {
			message:   format!(
				"Content-Range bytes {start}-{end}/{total} does not match requested bytes \
				 {expected_start}-{}",
				expected_end - 1
			),
			transient: false,
		});
	}
	Ok(total)
}

fn fetch_with_retry(
	fetcher: &mut dyn RangeFetcher,
	start: u64,
	end: u64,
	mut sleep: impl FnMut(Duration),
) -> Result<FetchResponse> {
	let mut last = String::new();
	for attempt in 1..=MAX_ATTEMPTS {
		match fetcher.fetch(start, end) {
			Ok(response) => return Ok(response),
			Err(failure) => {
				last = failure.message;
				if !failure.transient {
					return Err(err(format!("remote block range {start}-{} failed: {last}", end - 1)));
				}
				if attempt < MAX_ATTEMPTS {
					sleep(Duration::from_millis(50 * (1 << (attempt - 1))));
				}
			},
		}
	}
	Err(err(format!(
		"remote block range {start}-{} failed after {MAX_ATTEMPTS} attempts: {last}",
		end - 1
	)))
}

fn unit_count(image_len: u64, unit: u64) -> Result<u64> {
	image_len
		.checked_add(unit - 1)
		.map(|value| value / unit)
		.ok_or_else(|| err("remote block image size overflow"))
}

fn trailer_offset(image_len: u64) -> Result<u64> {
	image_len
		.checked_add(HEADER_SIZE)
		.ok_or_else(|| err("remote block trailer offset overflow"))
}

fn header(kind: u8, image_len: u64, unit: u64) -> [u8; HEADER_SIZE as usize] {
	let mut value = [0u8; HEADER_SIZE as usize];
	value[..8].copy_from_slice(MAGIC);
	value[8] = kind;
	value[16..24].copy_from_slice(&image_len.to_le_bytes());
	value[24..32].copy_from_slice(&unit.to_le_bytes());
	value
}

fn open_backing(path: &Path, image_len: u64, kind: u8, unit: u64) -> Result<(File, Vec<u8>)> {
	// Explicitly preserve persistent cache contents; compatibility is validated
	// from the metadata trailer below rather than by truncating on open.
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.truncate(false)
		.open(path)
		.map_err(|e| err(format!("opening remote block backing {}: {e}", path.display())))?;
	open_backing_file(file, image_len, kind, unit, &path.display().to_string())
}

fn open_backing_file(
	file: File,
	image_len: u64,
	kind: u8,
	unit: u64,
	description: &str,
) -> Result<(File, Vec<u8>)> {
	let units = unit_count(image_len, unit)?;
	let bitmap_len = if kind == CACHE_KIND {
		units
	} else {
		units.div_ceil(8)
	};
	let trailer = trailer_offset(image_len)?;
	let full_len = trailer
		.checked_add(bitmap_len)
		.ok_or_else(|| err("remote block backing size overflow"))?;
	let expected_header = header(kind, image_len, unit);
	let metadata_len = file.metadata()?.len();
	let mut found_header = [0u8; HEADER_SIZE as usize];
	let valid = metadata_len == full_len
		&& file.read_exact_at(&mut found_header, image_len).is_ok()
		&& found_header == expected_header;
	if !valid {
		if metadata_len != 0 {
			return Err(err(format!(
				"remote block backing {description} has no compatible vmon metadata trailer"
			)));
		}
		file.set_len(full_len)?;
		file.write_all_at(&expected_header, image_len)?;
		file.sync_data()?;
	}
	let bitmap_len = usize::try_from(bitmap_len)
		.map_err(|_| err("remote block bitmap does not fit host address space"))?;
	let mut bitmap = vec![0u8; bitmap_len];
	if valid && bitmap_len != 0 {
		file.read_exact_at(&mut bitmap, trailer)?;
	}
	Ok((file, bitmap))
}

fn chunk_ranges(offset: u64, len: u64, image_len: u64) -> Result<Vec<(u64, u64)>> {
	let end = offset
		.checked_add(len)
		.filter(|end| *end <= image_len)
		.ok_or_else(|| err("remote block read exceeds image"))?;
	if len == 0 {
		return Ok(Vec::new());
	}
	let first = offset / REMOTE_CHUNK_SIZE;
	let last = (end - 1) / REMOTE_CHUNK_SIZE;
	Ok((first..=last)
		.map(|chunk| {
			let start = chunk * REMOTE_CHUNK_SIZE;
			(start, (start + REMOTE_CHUNK_SIZE).min(image_len))
		})
		.collect())
}
fn validate_logical_size(logical_size: u64, uncompressed_size: u64) -> Result<()> {
	if uncompressed_size == 0 {
		return Err(err("remote block index uncompressed_size must be greater than zero"));
	}
	if logical_size < uncompressed_size {
		return Err(err(format!(
			"--rootfs-remote-size {logical_size} is smaller than indexed uncompressed_size \
			 {uncompressed_size}"
		)));
	}
	if !logical_size.is_multiple_of(SECTOR_SIZE) {
		return Err(err(format!("--rootfs-remote-size must be a multiple of {SECTOR_SIZE} bytes")));
	}
	Ok(())
}

/// A remote immutable image layered under a private sparse writable overlay.
///
/// Network operations have 10-second request deadlines and three bounded
/// retries, so servicing a miss can delay this device worker but cannot wait
/// indefinitely and deadlock the virtio queue.
pub(super) struct RemoteBlockSource {
	cache:             File,
	overlay:           File,
	resident:          Vec<u8>,
	written:           Vec<u8>,
	image_len:         u64,
	index:             BlockIndex,
	fetcher:           Box<dyn RangeFetcher>,
	cache_bitmap_at:   u64,
	overlay_bitmap_at: u64,
}

impl RemoteBlockSource {
	pub(super) fn new(
		url: String,
		cache_path: &Path,
		index_path: &Path,
		overlay_path: &Path,
		logical_size: u64,
		bearer: Option<String>,
		auth: ObjectAuth,
		region: Option<String>,
		etag: Option<String>,
	) -> Result<Self> {
		let index = load_index(index_path, logical_size)?;
		let fetcher = Box::new(HttpRangeFetcher::new(url, bearer, auth, region, etag)?);
		Self::with_index(cache_path, overlay_path, index, logical_size, fetcher)
	}

	pub(super) fn new_with_overlay(
		url: String,
		cache_path: &Path,
		index_path: &Path,
		overlay: File,
		logical_size: u64,
		bearer: Option<String>,
		auth: ObjectAuth,
		region: Option<String>,
		etag: Option<String>,
	) -> Result<Self> {
		let index = load_index(index_path, logical_size)?;
		let fetcher = Box::new(HttpRangeFetcher::new(url, bearer, auth, region, etag)?);
		Self::with_fetcher_and_overlay(cache_path, overlay, index, logical_size, fetcher)
	}

	fn with_index(
		cache_path: &Path,
		overlay_path: &Path,
		index: BlockIndex,
		logical_size: u64,
		fetcher: Box<dyn RangeFetcher>,
	) -> Result<Self> {
		// A prior overlay may contain guest writes and its embedded bitmap.
		// Opening it must never truncate either.
		let overlay = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(overlay_path)
			.map_err(|e| {
				err(format!("opening remote block overlay {}: {e}", overlay_path.display()))
			})?;
		Self::with_fetcher_and_overlay(cache_path, overlay, index, logical_size, fetcher)
	}

	fn with_fetcher_and_overlay(
		cache_path: &Path,
		overlay: File,
		index: BlockIndex,
		logical_size: u64,
		fetcher: Box<dyn RangeFetcher>,
	) -> Result<Self> {
		validate_index(&index, logical_size)?;
		let uncompressed_size = index.uncompressed_size;
		let (cache, resident) =
			open_backing(cache_path, uncompressed_size, CACHE_KIND, REMOTE_CHUNK_SIZE)?;
		let (overlay, written) =
			open_backing_file(overlay, logical_size, OVERLAY_KIND, SECTOR_SIZE, "overlay descriptor")?;
		Ok(Self {
			cache,
			overlay,
			resident,
			written,
			image_len: logical_size,
			index,
			fetcher,
			cache_bitmap_at: trailer_offset(uncompressed_size)?,
			overlay_bitmap_at: trailer_offset(logical_size)?,
		})
	}

	pub(super) const fn image_len(&self) -> u64 {
		self.image_len
	}

	pub(super) const fn overlay(&self) -> &File {
		&self.overlay
	}

	fn sector_written(&self, sector: u64) -> bool {
		let byte = self.written[(sector / 8) as usize];
		byte & (1 << (sector % 8)) != 0
	}

	fn mark_sectors_written(&mut self, first: u64, last: u64) -> Result<()> {
		let first_byte = (first / 8) as usize;
		let last_byte = (last / 8) as usize;
		for sector in first..=last {
			self.written[(sector / 8) as usize] |= 1 << (sector % 8);
		}
		self.overlay.write_all_at(
			&self.written[first_byte..=last_byte],
			self.overlay_bitmap_at + first_byte as u64,
		)?;
		self.overlay.sync_data()?;
		Ok(())
	}

	/// Fetches the frames of chunks `first..=last` with one range request
	/// (frames are contiguous in the object) and returns them in order.
	fn fetch_frames(&mut self, first: usize, last: usize) -> Result<Vec<Vec<u8>>> {
		let start = self.index.blocks[first].0;
		let (last_offset, last_length) = self.index.blocks[last];
		let end = last_offset
			.checked_add(last_length)
			.ok_or_else(|| err(format!("remote block {last} compressed range overflow")))?;
		let response = fetch_with_retry(self.fetcher.as_mut(), start, end, thread::sleep)?;
		if response.total_len != self.index.compressed_size {
			return Err(err(format!(
				"remote compressed object length changed from {} to {}",
				self.index.compressed_size, response.total_len
			)));
		}
		Ok(self.index.blocks[first..=last]
			.iter()
			.map(|&(offset, length)| {
				let at = (offset - start) as usize;
				response.bytes[at..at + length as usize].to_vec()
			})
			.collect())
	}

	fn decode_chunk(&self, chunk: usize, frame: &[u8], out: &mut Vec<u8>) -> Result<()> {
		decompress_block(frame, self.index.block_len(chunk), out)
			.map_err(|e| err(format!("decompressing remote block {chunk}: {e}")))
	}

	fn ensure_chunk(&mut self, start: u64) -> Result<()> {
		let chunk = (start / REMOTE_CHUNK_SIZE) as usize;
		if self.resident[chunk] != 0 {
			return Ok(());
		}
		let frames = self.fetch_frames(chunk, chunk)?;
		let mut decoded = Vec::with_capacity(REMOTE_CHUNK_SIZE as usize);
		self.decode_chunk(chunk, &frames[0], &mut decoded)?;
		self.cache.write_all_at(&decoded, start)?;
		self.cache.sync_data()?;
		self.resident[chunk] = 1;
		self
			.cache
			.write_all_at(&[1], self.cache_bitmap_at + chunk as u64)?;
		self.cache.sync_data()?;
		Ok(())
	}

	fn read_base(&mut self, out: &mut [u8], offset: u64) -> Result<()> {
		let uncompressed_size = self.index.uncompressed_size;
		out.fill(0);
		if offset >= uncompressed_size || out.is_empty() {
			return Ok(());
		}
		let available = (uncompressed_size - offset).min(out.len() as u64);
		for (start, _) in chunk_ranges(offset, available, uncompressed_size)? {
			self.ensure_chunk(start)?;
		}
		self
			.cache
			.read_exact_at(&mut out[..available as usize], offset)
			.map_err(|e| err(format!("reading remote block cache at {offset}: {e}")))
	}

	pub(super) fn read_exact_at(&mut self, mut out: &mut [u8], mut offset: u64) -> Result<()> {
		let end = offset
			.checked_add(out.len() as u64)
			.filter(|end| *end <= self.image_len)
			.ok_or_else(|| err("remote block read exceeds image"))?;
		let last_sector = end.saturating_sub(1) / SECTOR_SIZE;
		while offset < end {
			// One read per run of sectors served by the same layer.
			let sector = offset / SECTOR_SIZE;
			let written = self.sector_written(sector);
			let mut next = sector + 1;
			while next <= last_sector && self.sector_written(next) == written {
				next += 1;
			}
			let run_end = (next * SECTOR_SIZE).min(end);
			let (head, tail) = out.split_at_mut((run_end - offset) as usize);
			if written {
				self.overlay.read_exact_at(head, offset)?;
			} else {
				self.read_base(head, offset)?;
			}
			out = tail;
			offset = run_end;
		}
		Ok(())
	}

	/// Writes the guest-visible disk as the new raw image `destination`
	/// (sparse: all-zero chunks stay holes) and returns its length.
	///
	/// Non-resident chunks are fetched in batched range requests and decoded
	/// straight into the image without populating the cache, so a recovery
	/// capture costs one pass over the object instead of a request and two
	/// cache syncs per chunk.
	pub(super) fn materialize(&mut self, destination: &Path) -> Result<u64> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create_new(true)
			.custom_flags(libc::O_NOFOLLOW)
			.open(destination)
			.map_err(|e| err(format!("creating remote disk image {}: {e}", destination.display())))?;
		let result = self.materialize_into(&file);
		if result.is_err() {
			drop(file);
			let _ = std::fs::remove_file(destination);
		}
		result.map(|()| self.image_len)
	}

	fn materialize_into(&mut self, file: &File) -> Result<()> {
		file.set_len(self.image_len)?;
		let chunks = self.index.blocks.len();
		let mut base = Vec::with_capacity(REMOTE_CHUNK_SIZE as usize);
		let mut chunk = 0;
		while chunk < chunks {
			// Batch the run of non-resident chunks starting here.
			let frames = if self.resident[chunk] == 0 {
				let mut last = chunk;
				let mut bytes = self.index.blocks[chunk].1;
				while last + 1 < chunks
					&& self.resident[last + 1] == 0
					&& bytes + self.index.blocks[last + 1].1 <= MATERIALIZE_BATCH_BYTES
				{
					last += 1;
					bytes += self.index.blocks[last].1;
				}
				self.fetch_frames(chunk, last)?
			} else {
				Vec::new()
			};
			let batch = frames.len().max(1);
			for slot in 0..batch {
				let current = chunk + slot;
				let start = current as u64 * REMOTE_CHUNK_SIZE;
				if let Some(frame) = frames.get(slot) {
					self.decode_chunk(current, frame, &mut base)?;
				} else {
					base.resize(self.index.block_len(current) as usize, 0);
					self.cache.read_exact_at(&mut base, start)?;
				}
				// Overlay sectors win over the base inside this chunk.
				let chunk_end = (start + REMOTE_CHUNK_SIZE).min(self.image_len);
				base.resize((chunk_end - start) as usize, 0);
				let mut sector = start / SECTOR_SIZE;
				while sector * SECTOR_SIZE < chunk_end {
					if !self.sector_written(sector) {
						sector += 1;
						continue;
					}
					let run_start = sector;
					while sector * SECTOR_SIZE < chunk_end && self.sector_written(sector) {
						sector += 1;
					}
					let from = (run_start * SECTOR_SIZE - start) as usize;
					let to = ((sector * SECTOR_SIZE).min(chunk_end) - start) as usize;
					self
						.overlay
						.read_exact_at(&mut base[from..to], run_start * SECTOR_SIZE)?;
				}
				if base.iter().any(|&byte| byte != 0) {
					file.write_all_at(&base, start)?;
				}
			}
			chunk += batch;
		}
		// Logical capacity past the indexed image reads from the overlay only.
		let mut offset = self
			.index
			.uncompressed_size
			.next_multiple_of(REMOTE_CHUNK_SIZE);
		while offset < self.image_len {
			let len = (self.image_len - offset).min(REMOTE_CHUNK_SIZE);
			base.resize(len as usize, 0);
			self.read_exact_at(&mut base, offset)?;
			if base.iter().any(|&byte| byte != 0) {
				file.write_all_at(&base, offset)?;
			}
			offset += len;
		}
		file.sync_all()?;
		Ok(())
	}

	pub(super) fn write_all_at(&mut self, data: &[u8], offset: u64) -> Result<()> {
		let end = offset
			.checked_add(data.len() as u64)
			.filter(|end| *end <= self.image_len)
			.ok_or_else(|| err("remote block write exceeds image"))?;
		if data.is_empty() {
			return Ok(());
		}
		let first = offset / SECTOR_SIZE;
		let last = (end - 1) / SECTOR_SIZE;
		for sector in first..=last {
			let sector_start = sector * SECTOR_SIZE;
			let covered_start = offset.max(sector_start);
			let covered_end = end.min(sector_start + SECTOR_SIZE);
			if !self.sector_written(sector)
				&& (covered_start != sector_start || covered_end != sector_start + SECTOR_SIZE)
			{
				let mut seed = [0u8; SECTOR_SIZE as usize];
				self.read_base(&mut seed, sector_start)?;
				self.overlay.write_all_at(&seed, sector_start)?;
			}
		}
		self.overlay.write_all_at(data, offset)?;
		self.overlay.sync_data()?;
		self.mark_sectors_written(first, last)
	}

	pub(super) fn sync_all(&self) -> io::Result<()> {
		self.cache.sync_all()?;
		self.overlay.sync_all()
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::VecDeque,
		path::PathBuf,
		sync::{
			Arc, Mutex,
			atomic::{AtomicUsize, Ordering},
		},
	};

	use super::*;

	struct FakeFetcher {
		calls:   Arc<AtomicUsize>,
		ranges:  Arc<Mutex<Vec<(u64, u64)>>>,
		results: VecDeque<std::result::Result<FetchResponse, FetchFailure>>,
		image:   Vec<u8>,
	}

	impl RangeFetcher for FakeFetcher {
		fn fetch(
			&mut self,
			start: u64,
			end: u64,
		) -> std::result::Result<FetchResponse, FetchFailure> {
			self.calls.fetch_add(1, Ordering::SeqCst);
			self.ranges.lock().expect("ranges").push((start, end));
			if let Some(result) = self.results.pop_front() {
				return result;
			}
			Ok(FetchResponse {
				bytes:     self.image[start as usize..end as usize].to_vec(),
				total_len: self.image.len() as u64,
			})
		}
	}

	struct TestDir(PathBuf);

	impl TestDir {
		fn new() -> Self {
			let path = std::env::temp_dir().join(format!(
				"vmon-remote-block-{}-{}",
				std::process::id(),
				std::thread::current().name().unwrap_or("test")
			));
			let _ = std::fs::remove_dir_all(&path);
			std::fs::create_dir(&path).expect("create test dir");
			Self(path)
		}
	}

	impl Drop for TestDir {
		fn drop(&mut self) {
			let _ = std::fs::remove_dir_all(&self.0);
		}
	}
	fn framed_image(blocks: &[Vec<u8>]) -> (Vec<u8>, BlockIndex) {
		let mut compressed = Vec::new();
		let mut ranges = Vec::with_capacity(blocks.len());
		let mut uncompressed_size = 0u64;
		for block in blocks {
			let frame = zstd::stream::encode_all(block.as_slice(), 3).expect("compress test frame");
			ranges.push((compressed.len() as u64, frame.len() as u64));
			compressed.extend_from_slice(&frame);
			uncompressed_size += block.len() as u64;
		}
		let index = BlockIndex {
			version: crate::block_image::INDEX_VERSION,
			compressed_size: compressed.len() as u64,
			uncompressed_size,
			original_size: uncompressed_size,
			block_size: REMOTE_CHUNK_SIZE,
			blocks: ranges,
		};
		(compressed, index)
	}

	#[test]
	fn expired_aws_credentials_are_classified_for_refresh() {
		assert!(is_refreshable_aws_auth_error(
			"<Error><Code>ExpiredToken</Code><Message>expired</Message></Error>"
		));
		assert!(is_refreshable_aws_auth_error("<Error><Code>TokenRefreshRequired</Code></Error>"));
		assert!(!is_refreshable_aws_auth_error("<Error><Code>AccessDenied</Code></Error>"));
	}

	#[test]
	fn http_fetcher_requires_immutable_etag_and_secure_workload_transport() {
		let Err(missing) = HttpRangeFetcher::new(
			"https://example.com/rootfs".to_owned(),
			None,
			ObjectAuth::None,
			None,
			None,
		) else {
			panic!("missing ETag must fail")
		};
		assert!(missing.to_string().contains("requires an immutable ETag"));

		let Err(weak) = HttpRangeFetcher::new(
			"https://example.com/rootfs".to_owned(),
			None,
			ObjectAuth::None,
			None,
			Some("W/\"mutable\"".to_owned()),
		) else {
			panic!("weak ETag must fail")
		};
		assert!(weak.to_string().contains("quoted strong entity-tag"));

		let Err(insecure) = HttpRangeFetcher::new(
			"http://storage.googleapis.com/rootfs".to_owned(),
			None,
			ObjectAuth::Google,
			None,
			Some("\"immutable\"".to_owned()),
		) else {
			panic!("workload auth over HTTP must fail")
		};
		assert!(insecure.to_string().contains("requires an https:// URL"));
	}

	#[test]
	fn chunk_math_covers_boundaries_and_last_partial_chunk() {
		let image_len = REMOTE_CHUNK_SIZE * 2 + 17;
		assert_eq!(chunk_ranges(0, 1, image_len).expect("first"), vec![(0, REMOTE_CHUNK_SIZE)]);
		assert_eq!(chunk_ranges(REMOTE_CHUNK_SIZE - 1, 2, image_len).expect("spanning"), vec![
			(0, REMOTE_CHUNK_SIZE),
			(REMOTE_CHUNK_SIZE, REMOTE_CHUNK_SIZE * 2)
		]);
		assert_eq!(chunk_ranges(REMOTE_CHUNK_SIZE * 2, 17, image_len).expect("last"), vec![(
			REMOTE_CHUNK_SIZE * 2,
			image_len
		)]);
	}

	#[test]
	fn small_read_fetches_only_covering_frame_then_hits_cache() {
		let dir = TestDir::new();
		let blocks = [vec![0x5a; REMOTE_CHUNK_SIZE as usize], vec![0x6b; 1]];
		let (compressed, index) = framed_image(&blocks);
		let expected_range = {
			let (offset, length) = index.blocks[0];
			(offset, offset + length)
		};
		let logical_size = index.uncompressed_size.div_ceil(SECTOR_SIZE) * SECTOR_SIZE;
		let calls = Arc::new(AtomicUsize::new(0));
		let ranges = Arc::new(Mutex::new(Vec::new()));
		let fetcher = FakeFetcher {
			calls:   calls.clone(),
			ranges:  ranges.clone(),
			results: VecDeque::new(),
			image:   compressed,
		};
		let mut source = RemoteBlockSource::with_index(
			&dir.0.join("cache"),
			&dir.0.join("overlay"),
			index,
			logical_size,
			Box::new(fetcher),
		)
		.expect("source");
		let mut first = [0u8; 32];
		source.read_exact_at(&mut first, 7).expect("miss");
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(*ranges.lock().expect("ranges"), vec![expected_range]);
		let mut second = [0u8; 32];
		source.read_exact_at(&mut second, 19).expect("hit");
		assert_eq!(calls.load(Ordering::SeqCst), 1);
		assert_eq!(second, [0x5a; 32]);
	}
	#[test]
	fn logical_tail_reads_zero_without_fetching_beyond_filesystem() {
		let dir = TestDir::new();
		let blocks = [vec![0x7b; 512]];
		let (compressed, index) = framed_image(&blocks);
		let calls = Arc::new(AtomicUsize::new(0));
		let ranges = Arc::new(Mutex::new(Vec::new()));
		let fetcher =
			FakeFetcher { calls: calls.clone(), ranges, results: VecDeque::new(), image: compressed };
		let mut source = RemoteBlockSource::with_index(
			&dir.0.join("cache"),
			&dir.0.join("overlay"),
			index,
			1024,
			Box::new(fetcher),
		)
		.expect("source");
		let mut out = [0u8; 64];
		source.read_exact_at(&mut out, 480).expect("tail read");
		assert_eq!(&out[..32], &[0x7b; 32]);
		assert_eq!(&out[32..], &[0; 32]);
		assert_eq!(calls.load(Ordering::SeqCst), 1);
	}

	#[test]
	fn reads_spanning_overlay_and_base_coalesce_into_layer_runs() {
		let dir = TestDir::new();
		let blocks = [vec![0x11; REMOTE_CHUNK_SIZE as usize], vec![0x22; REMOTE_CHUNK_SIZE as usize]];
		let (compressed, index) = framed_image(&blocks);
		let calls = Arc::new(AtomicUsize::new(0));
		let fetcher = FakeFetcher {
			calls:   calls.clone(),
			ranges:  Arc::new(Mutex::new(Vec::new())),
			results: VecDeque::new(),
			image:   compressed,
		};
		let mut source = RemoteBlockSource::with_index(
			&dir.0.join("cache"),
			&dir.0.join("overlay"),
			index,
			2 * REMOTE_CHUNK_SIZE,
			Box::new(fetcher),
		)
		.expect("source");
		source
			.write_all_at(&[0xee; 1024], REMOTE_CHUNK_SIZE - 512)
			.expect("write across chunk boundary");
		let mut out = vec![0u8; 4096];
		source
			.read_exact_at(&mut out, REMOTE_CHUNK_SIZE - 2048)
			.expect("mixed read");
		assert_eq!(&out[..1536], &[0x11; 1536]);
		assert_eq!(&out[1536..2560], &[0xee; 1024]);
		assert_eq!(&out[2560..], &[0x22; 1536]);
		assert_eq!(calls.load(Ordering::SeqCst), 2, "one fetch per touched chunk");
	}

	#[test]
	fn materialize_merges_overlay_cache_and_batched_remote_chunks() {
		let dir = TestDir::new();
		let blocks = [
			vec![0x31; REMOTE_CHUNK_SIZE as usize],
			vec![0; REMOTE_CHUNK_SIZE as usize],
			vec![0x33; REMOTE_CHUNK_SIZE as usize],
			vec![0x34; 4096],
		];
		let (compressed, index) = framed_image(&blocks);
		let uncompressed = index.uncompressed_size;
		let calls = Arc::new(AtomicUsize::new(0));
		let ranges = Arc::new(Mutex::new(Vec::new()));
		let fetcher = FakeFetcher {
			calls:   calls.clone(),
			ranges:  ranges.clone(),
			results: VecDeque::new(),
			image:   compressed.clone(),
		};
		// Logical capacity extends one chunk past the indexed image.
		let logical = uncompressed.next_multiple_of(REMOTE_CHUNK_SIZE) + REMOTE_CHUNK_SIZE;
		let mut source = RemoteBlockSource::with_index(
			&dir.0.join("cache"),
			&dir.0.join("overlay"),
			index,
			logical,
			Box::new(fetcher),
		)
		.expect("source");
		// Chunk 0 becomes resident through a guest read; chunk 2 gets an
		// overlay write; the tail past the image gets a write too.
		let mut probe = [0u8; 512];
		source.read_exact_at(&mut probe, 0).expect("warm chunk 0");
		source
			.write_all_at(&[0xaa; 512], 2 * REMOTE_CHUNK_SIZE + 4096)
			.expect("overlay write");
		source
			.write_all_at(&[0xbb; 512], logical - 512)
			.expect("tail write");
		let before = calls.load(Ordering::SeqCst);
		ranges.lock().expect("ranges").clear();

		let image = dir.0.join("disk.img");
		assert_eq!(source.materialize(&image).expect("materialize"), logical);
		// Chunks 1..=3 are non-resident and contiguous: one range request.
		assert_eq!(calls.load(Ordering::SeqCst) - before, 1);
		assert_eq!(*ranges.lock().expect("ranges"), vec![(
			source.index.blocks[1].0,
			compressed.len() as u64
		)]);
		let bytes = std::fs::read(&image).expect("read image");
		assert_eq!(bytes.len() as u64, logical);
		let chunk = REMOTE_CHUNK_SIZE as usize;
		assert!(bytes[..chunk].iter().all(|&b| b == 0x31));
		assert!(bytes[chunk..2 * chunk].iter().all(|&b| b == 0));
		assert!(
			bytes[2 * chunk..2 * chunk + 4096]
				.iter()
				.all(|&b| b == 0x33)
		);
		assert!(
			bytes[2 * chunk + 4096..2 * chunk + 4608]
				.iter()
				.all(|&b| b == 0xaa)
		);
		assert!(
			bytes[2 * chunk + 4608..3 * chunk]
				.iter()
				.all(|&b| b == 0x33)
		);
		assert!(
			bytes[3 * chunk..3 * chunk + 4096]
				.iter()
				.all(|&b| b == 0x34)
		);
		assert!(
			bytes[3 * chunk + 4096..logical as usize - 512]
				.iter()
				.all(|&b| b == 0)
		);
		assert!(bytes[logical as usize - 512..].iter().all(|&b| b == 0xbb));
		// Materializing never populates the cache.
		assert_eq!(source.resident, vec![1, 0, 0, 0]);
		assert!(source.materialize(&image).is_err(), "destination must be new");
	}

	#[test]
	fn stopped_vm_disk_materializes_from_its_overlay_cache_and_closure_fetches() {
		let dir = TestDir::new();
		let blocks = [vec![0x41; REMOTE_CHUNK_SIZE as usize], vec![0x42; 8192]];
		let (compressed, index) = framed_image(&blocks);
		let index_path = dir.0.join("index.json");
		std::fs::write(&index_path, serde_json::to_vec(&index).expect("index json"))
			.expect("write index");
		let logical = index.sector_aligned_size();
		let (overlay, cache) = (dir.0.join("overlay"), dir.0.join("cache"));
		{
			// The "VMM run": one write, chunk 1 cached by a read.
			let fetcher = FakeFetcher {
				calls:   Arc::new(AtomicUsize::new(0)),
				ranges:  Arc::new(Mutex::new(Vec::new())),
				results: VecDeque::new(),
				image:   compressed.clone(),
			};
			let mut source =
				RemoteBlockSource::with_index(&cache, &overlay, index, logical, Box::new(fetcher))
					.expect("source");
			source.write_all_at(&[7; 512], 1024).expect("write");
			let mut probe = [0u8; 512];
			source
				.read_exact_at(&mut probe, REMOTE_CHUNK_SIZE)
				.expect("read chunk 1");
		}
		let fetched = Arc::new(Mutex::new(Vec::new()));
		let image = dir.0.join("disk.img");
		let fetch = {
			let fetched = fetched.clone();
			move |start: u64, end: u64| {
				fetched.lock().expect("fetched").push((start, end));
				Ok(compressed[start as usize..end as usize].to_vec())
			}
		};
		let len = materialize_remote_disk(&overlay, &cache, &index_path, logical, fetch, &image)
			.expect("materialize");
		assert_eq!(len, logical);
		assert_eq!(fetched.lock().expect("fetched").len(), 1, "only chunk 0 is not cached");
		let bytes = std::fs::read(&image).expect("image");
		assert!(bytes[..1024].iter().all(|&b| b == 0x41));
		assert!(bytes[1024..1536].iter().all(|&b| b == 7));
		assert!(
			bytes[1536..REMOTE_CHUNK_SIZE as usize]
				.iter()
				.all(|&b| b == 0x41)
		);
		assert!(
			bytes[REMOTE_CHUNK_SIZE as usize..]
				.iter()
				.all(|&b| b == 0x42)
		);
	}

	#[test]
	fn bounded_retry_reports_exhaustion() {
		let calls = Arc::new(AtomicUsize::new(0));
		let failure = || Err(FetchFailure { message: "temporary outage".into(), transient: true });
		let mut fetcher = FakeFetcher {
			calls:   calls.clone(),
			ranges:  Arc::new(Mutex::new(Vec::new())),
			results: VecDeque::from([failure(), failure(), failure()]),
			image:   Vec::new(),
		};
		let error = fetch_with_retry(&mut fetcher, 0, 512, |_| {}).expect_err("must give up");
		assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
		assert!(
			error
				.to_string()
				.contains("failed after 3 attempts: temporary outage")
		);
	}
}
