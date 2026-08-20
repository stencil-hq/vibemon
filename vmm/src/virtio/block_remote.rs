//! Lazy HTTP range-backed block source with persistent sparse caches.
#![cfg_attr(
	not(target_os = "linux"),
	allow(
		dead_code,
		reason = "production callers are Linux/KVM-only; non-Linux builds compile this module \
		          solely to run its platform-independent tests"
	)
)]

use std::{
	fs::{File, OpenOptions},
	io::{self, Read},
	os::unix::fs::FileExt,
	path::Path,
	thread,
	time::Duration,
};

use reqwest::{StatusCode, blocking::Client, header};
use serde::Deserialize;

use crate::result::{Result, err};

/// One MiB amortizes request latency without pulling a large fraction of a
/// sparse 100 GB image for each filesystem metadata touch. It is also a
/// multiple of the 512-byte virtio sector size.
pub(super) const REMOTE_CHUNK_SIZE: u64 = 1024 * 1024;
const SECTOR_SIZE: u64 = 512;
const HEADER_SIZE: u64 = 32;
const CACHE_KIND: u8 = 1;
const OVERLAY_KIND: u8 = 2;
const MAGIC: &[u8; 8] = b"VMONBLK1";
const MAX_ATTEMPTS: usize = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const INDEX_VERSION: u32 = 3;

#[derive(Deserialize)]
struct RemoteIndex {
	version:           u32,
	compressed_size:   u64,
	uncompressed_size: u64,
	#[serde(rename = "original_size")]
	_original_size:    u64,
	block_size:        u64,
	blocks:            Vec<(u64, u64)>,
}

impl RemoteIndex {
	fn load(path: &Path, logical_size: u64) -> Result<Self> {
		let bytes = std::fs::read(path)
			.map_err(|e| err(format!("reading remote block index {}: {e}", path.display())))?;
		let index: Self = serde_json::from_slice(&bytes)
			.map_err(|e| err(format!("parsing remote block index {}: {e}", path.display())))?;
		index.validate(logical_size)?;
		Ok(index)
	}

	fn validate(&self, logical_size: u64) -> Result<()> {
		if self.version != INDEX_VERSION {
			return Err(err(format!(
				"unsupported remote block index version {}; expected {INDEX_VERSION}",
				self.version
			)));
		}
		if self.block_size == 0 || !self.block_size.is_multiple_of(SECTOR_SIZE) {
			return Err(err(format!(
				"remote block index block_size {} must be nonzero and 512-byte aligned",
				self.block_size
			)));
		}
		if self.block_size != REMOTE_CHUNK_SIZE {
			return Err(err(format!(
				"remote block index block_size {} is unsupported; expected {REMOTE_CHUNK_SIZE}",
				self.block_size
			)));
		}
		validate_logical_size(logical_size, self.uncompressed_size)?;
		let expected = self.uncompressed_size.div_ceil(self.block_size);
		if self.blocks.len() as u64 != expected {
			return Err(err(format!(
				"remote block index has {} blocks, expected {expected} for {} uncompressed bytes",
				self.blocks.len(),
				self.uncompressed_size
			)));
		}
		if self.compressed_size == 0 {
			return Err(err("remote block index compressed_size must be greater than zero"));
		}
		for (block, &(offset, length)) in self.blocks.iter().enumerate() {
			let valid = length != 0
				&& offset
					.checked_add(length)
					.is_some_and(|end| end <= self.compressed_size);
			if !valid {
				return Err(err(format!(
					"remote block index block {block} range [{offset}, {}) exceeds compressed_size {}",
					offset.saturating_add(length),
					self.compressed_size
				)));
			}
		}
		Ok(())
	}
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

struct HttpRangeFetcher {
	client: Client,
	url:    String,
	bearer: Option<String>,
}

impl HttpRangeFetcher {
	fn new(url: String, bearer: Option<String>) -> Result<Self> {
		let client = Client::builder()
			.connect_timeout(CONNECT_TIMEOUT)
			.timeout(REQUEST_TIMEOUT)
			.build()
			.map_err(|e| err(format!("building remote block HTTP client: {e}")))?;
		Ok(Self { client, url, bearer })
	}
}

impl RangeFetcher for HttpRangeFetcher {
	fn fetch(&mut self, start: u64, end: u64) -> std::result::Result<FetchResponse, FetchFailure> {
		let mut request = self
			.client
			.get(&self.url)
			.header(header::RANGE, format!("bytes={start}-{}", end - 1));
		if let Some(token) = &self.bearer {
			request = request.bearer_auth(token);
		}
		let response = request.send().map_err(|e| FetchFailure {
			message:   format!("request failed: {e}"),
			transient: e.is_timeout() || e.is_connect() || e.is_request() || e.is_body(),
		})?;
		let status = response.status();
		if status != StatusCode::PARTIAL_CONTENT {
			return Err(FetchFailure {
				message:   format!("server returned HTTP {status}, expected 206 Partial Content"),
				transient: status == StatusCode::REQUEST_TIMEOUT
					|| status == StatusCode::TOO_MANY_REQUESTS
					|| status.is_server_error(),
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
	index:             RemoteIndex,
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
	) -> Result<Self> {
		let index = RemoteIndex::load(index_path, logical_size)?;
		let fetcher = Box::new(HttpRangeFetcher::new(url, bearer)?);
		Self::with_index(cache_path, overlay_path, index, logical_size, fetcher)
	}

	pub(super) fn new_with_overlay(
		url: String,
		cache_path: &Path,
		index_path: &Path,
		overlay: File,
		logical_size: u64,
		bearer: Option<String>,
	) -> Result<Self> {
		let index = RemoteIndex::load(index_path, logical_size)?;
		let fetcher = Box::new(HttpRangeFetcher::new(url, bearer)?);
		Self::with_fetcher_and_overlay(cache_path, overlay, index, logical_size, fetcher)
	}

	fn with_index(
		cache_path: &Path,
		overlay_path: &Path,
		index: RemoteIndex,
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
		index: RemoteIndex,
		logical_size: u64,
		fetcher: Box<dyn RangeFetcher>,
	) -> Result<Self> {
		index.validate(logical_size)?;
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

	fn ensure_chunk(&mut self, start: u64) -> Result<()> {
		let chunk = (start / REMOTE_CHUNK_SIZE) as usize;
		if self.resident[chunk] != 0 {
			return Ok(());
		}
		let &(compressed_offset, compressed_length) = &self.index.blocks[chunk];
		let compressed_end = compressed_offset
			.checked_add(compressed_length)
			.ok_or_else(|| err(format!("remote block {chunk} compressed range overflow")))?;
		let response =
			fetch_with_retry(self.fetcher.as_mut(), compressed_offset, compressed_end, thread::sleep)?;
		if response.total_len != self.index.compressed_size {
			return Err(err(format!(
				"remote compressed object length changed from {} to {}",
				self.index.compressed_size, response.total_len
			)));
		}
		let expected = (self.index.uncompressed_size - start).min(REMOTE_CHUNK_SIZE);
		let decoder = zstd::stream::read::Decoder::new(response.bytes.as_slice())
			.map_err(|e| err(format!("opening zstd frame for remote block {chunk}: {e}")))?;
		let mut decoded = Vec::with_capacity(expected as usize);
		decoder
			.take(expected + 1)
			.read_to_end(&mut decoded)
			.map_err(|e| err(format!("decompressing remote block {chunk}: {e}")))?;
		if decoded.len() as u64 != expected {
			return Err(err(format!(
				"remote block {chunk} decompressed to {} bytes, expected {expected}",
				decoded.len()
			)));
		}
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
		while offset < end {
			let sector = offset / SECTOR_SIZE;
			let sector_end = ((sector + 1) * SECTOR_SIZE).min(end);
			let count = (sector_end - offset) as usize;
			let (head, tail) = out.split_at_mut(count);
			if self.sector_written(sector) {
				self.overlay.read_exact_at(head, offset)?;
			} else {
				self.read_base(head, offset)?;
			}
			out = tail;
			offset = sector_end;
		}
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
	fn framed_image(blocks: &[Vec<u8>]) -> (Vec<u8>, RemoteIndex) {
		let mut compressed = Vec::new();
		let mut ranges = Vec::with_capacity(blocks.len());
		let mut uncompressed_size = 0u64;
		for block in blocks {
			let frame = zstd::stream::encode_all(block.as_slice(), 3).expect("compress test frame");
			ranges.push((compressed.len() as u64, frame.len() as u64));
			compressed.extend_from_slice(&frame);
			uncompressed_size += block.len() as u64;
		}
		let index = RemoteIndex {
			version: INDEX_VERSION,
			compressed_size: compressed.len() as u64,
			uncompressed_size,
			_original_size: uncompressed_size,
			block_size: REMOTE_CHUNK_SIZE,
			blocks: ranges,
		};
		(compressed, index)
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
