//! GCE-exported disk publication and lazy-consumption metadata.
//!
//! A GCE export is a non-seekable `disk.raw` inside a gzip-compressed tar. The
//! explicit publisher extracts its ext4 root partition once, provisions the
//! guest agent, and uploads independently compressed, indexed blocks. Workers
//! only inspect the small sidecar and let the VMM range-fetch touched frames.

use std::{
	fs::{self, File},
	io::{self, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	process::{Command, Stdio},
};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use flate2::read::MultiGzDecoder;
use md5::Md5;
use reqwest::{
	StatusCode,
	blocking::{Body, Client, Response},
	header::{AUTHORIZATION, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, LOCATION, RANGE},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tar::Archive;

use super::{ImageConfig, find_tool, registry_auth, run_inherited};
use crate::error::{EngineError, Result};

pub(super) const GCS_PREFIX: &str = "gs://";
pub(super) const UNSUPPORTED_LAYOUT_ERROR: &str = "unsupported GCE disk layout: no ext4 root \
                                                   partition found (LVM and non-ext4 roots are \
                                                   not supported)";
const SECTOR_SIZE: u64 = 512;
const EXT4_MAGIC: [u8; 2] = [0x53, 0xef];
const COPY_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const ROOTFS_BLOCK_SIZE: u64 = 1024 * 1024;
const ZSTD_LEVEL: i32 = 3;
const GCS_UPLOAD_CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const GCS_UPLOAD_RETRIES: usize = 5;
const PHASE_PROGRESS_BYTES: u64 = 1024 * 1024 * 1024;
const UPLOAD_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PreparedDiskImage {
	pub reference: String,
	pub digest:    String,
	pub remote:    RemoteRootfs,
	pub spec:      ImageConfig,
}

/// Durable metadata needed to attach a published root filesystem lazily.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RemoteRootfs {
	pub source:            String,
	pub object:            String,
	pub generation:        String,
	pub url:               String,
	pub compressed_size:   u64,
	pub uncompressed_size: u64,
	pub original_size:     u64,
	pub block_size:        u64,
	pub blocks:            Vec<[u64; 2]>,
	pub sha256:            String,
	pub agent_sha256:      String,
	pub logical_size:      u64,
}

/// Result of deliberately publishing one range-addressable GCE root filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedRootfs {
	pub object:            String,
	pub sidecar:           String,
	pub compressed_size:   u64,
	pub uncompressed_size: u64,
	pub original_size:     u64,
	pub sha256:            String,
	pub skipped:           bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ObjectMetadata {
	generation: String,
	#[serde(default)]
	size:       String,
	md5_hash:   Option<String>,
	crc32c:     Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DerivedSidecar {
	version:           u32,
	source:            String,
	source_generation: String,
	source_digest:     String,
	object:            String,
	compressed_size:   u64,
	uncompressed_size: u64,
	original_size:     u64,
	block_size:        u64,
	blocks:            Vec<[u64; 2]>,
	sha256:            String,
	md5_base64:        String,
	agent_sha256:      String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Partition {
	offset: u64,
	length: u64,
}

/// Publish the ext4 root partition next to a GCE `.tar.gz` export.
///
/// This is intentionally explicit because conversion reads the complete export.
/// The tar member is consumed as a stream; the 100 GB `disk.raw` is never
/// stored.
pub fn publish(reference: &str, agent: &Path) -> Result<PublishedRootfs> {
	let (bucket, source_object) = parse_reference(reference)?;
	let (derived_object, sidecar_object) = derived_names(&source_object)?;
	let token = access_token()?;
	let client = gcs_client()?;
	let source_metadata =
		object_metadata(&client, &token, &bucket, &source_object)?.ok_or_else(|| {
			EngineError::not_found(format!("GCS disk image does not exist: {reference}"))
		})?;
	let source_digest = metadata_digest(&source_metadata)?;
	let agent_sha256 = sha256_file(agent)?;
	if let (Some(derived), Some(sidecar)) = (
		object_metadata(&client, &token, &bucket, &derived_object)?,
		download_sidecar(&client, &token, &bucket, &sidecar_object)?,
	) && sidecar_matches(
		&sidecar,
		reference,
		&source_metadata.generation,
		&source_digest,
		&derived_object,
		&derived,
		&agent_sha256,
	) {
		return Ok(PublishedRootfs {
			object:            format!("gs://{bucket}/{derived_object}"),
			sidecar:           format!("gs://{bucket}/{sidecar_object}"),
			compressed_size:   sidecar.compressed_size,
			uncompressed_size: sidecar.uncompressed_size,
			original_size:     sidecar.original_size,
			sha256:            sidecar.sha256,
			skipped:           true,
		});
	}

	let work = tempfile::Builder::new()
		.prefix("vmon-gce-publish-")
		.tempdir()?;
	let rootfs = work.path().join("rootfs.ext4");
	let compressed_size = source_metadata
		.size
		.parse::<u64>()
		.map_err(|error| EngineError::engine(format!("invalid GCS object size: {error}")))?;
	eprintln!(
		"vmon: extracting root filesystem from {reference} ({compressed_size} compressed bytes)"
	);
	let original_size = download_rootfs(
		&client,
		&token,
		&bucket,
		&source_object,
		&source_metadata.generation,
		compressed_size,
		&rootfs,
	)?;
	eprintln!("vmon: extracted {original_size} root filesystem bytes");
	eprintln!("vmon: provisioning guest agent in extracted root filesystem");
	inject_agent(&rootfs, agent, work.path())?;
	eprintln!("vmon: checking and minimizing ext4 root filesystem");
	shrink_filesystem(&rootfs)?;
	let uncompressed_size = rootfs.metadata()?.len();
	eprintln!("vmon: minimized root filesystem from {original_size} to {uncompressed_size} bytes");
	let compressed = work.path().join("rootfs.ext4.zst");
	eprintln!("vmon: compressing root filesystem into independent 1 MiB zstd frames");
	let blocks = compress_indexed(&rootfs, &compressed)?;
	let (sha256, md5_base64) = content_digests(&compressed)?;
	let compressed_size = compressed.metadata()?.len();
	eprintln!("vmon: compressed root filesystem to {compressed_size} bytes; uploading to GCS");
	upload_file(&client, &token, &bucket, &derived_object, &compressed)?;
	let sidecar = DerivedSidecar {
		version: 3,
		source: reference.to_owned(),
		source_generation: source_metadata.generation,
		source_digest,
		object: derived_object.clone(),
		compressed_size,
		uncompressed_size,
		original_size,
		block_size: ROOTFS_BLOCK_SIZE,
		blocks,
		sha256: sha256.clone(),
		md5_base64,
		agent_sha256,
	};
	upload_json(&client, &token, &bucket, &sidecar_object, &sidecar)?;
	Ok(PublishedRootfs {
		object: format!("gs://{bucket}/{derived_object}"),
		sidecar: format!("gs://{bucket}/{sidecar_object}"),
		compressed_size,
		uncompressed_size,
		original_size,
		sha256,
		skipped: false,
	})
}

/// Resolve a `gs://` export to its already-published lazy root filesystem.
pub(super) fn prepare(reference: &str) -> Result<PreparedDiskImage> {
	let (bucket, source_object) = parse_reference(reference)?;
	let (derived_object, sidecar_object) = derived_names(&source_object)?;
	let token = access_token()?;
	let client = gcs_client()?;
	let source_metadata =
		object_metadata(&client, &token, &bucket, &source_object)?.ok_or_else(|| {
			EngineError::not_found(format!("GCS disk image does not exist: {reference}"))
		})?;
	let source_digest = metadata_digest(&source_metadata)?;
	let derived_metadata = object_metadata(&client, &token, &bucket, &derived_object)?
		.ok_or_else(|| missing_derived(reference, &derived_object))?;
	let sidecar = download_sidecar(&client, &token, &bucket, &sidecar_object)?
		.ok_or_else(|| missing_derived(reference, &derived_object))?;
	if !sidecar_matches(
		&sidecar,
		reference,
		&source_metadata.generation,
		&source_digest,
		&derived_object,
		&derived_metadata,
		&sidecar.agent_sha256,
	) {
		return Err(EngineError::invalid(format!(
			"published rootfs metadata is stale or inconsistent for {reference}; run `vmon image \
			 publish-gce-rootfs {reference}`"
		)));
	}
	let url = media_url(&bucket, &derived_object, &derived_metadata.generation);
	Ok(PreparedDiskImage {
		reference: reference.to_owned(),
		digest:    format!("gcs-sha256-{}", sidecar.sha256),
		remote:    RemoteRootfs {
			source: reference.to_owned(),
			object: format!("gs://{bucket}/{derived_object}"),
			generation: derived_metadata.generation,
			url,
			compressed_size: sidecar.compressed_size,
			uncompressed_size: sidecar.uncompressed_size,
			original_size: sidecar.original_size,
			block_size: sidecar.block_size,
			blocks: sidecar.blocks,
			sha256: sidecar.sha256,
			agent_sha256: sidecar.agent_sha256,
			logical_size: sidecar.uncompressed_size,
		},
		spec:      ImageConfig {
			reference:  reference.to_owned(),
			entrypoint: Vec::new(),
			cmd:        vec!["/bin/bash".to_owned()],
			env:        vec![
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned(),
			],
			workdir:    "/".to_owned(),
			user:       String::new(),
		},
	})
}

fn missing_derived(reference: &str, object: &str) -> EngineError {
	EngineError::not_found(format!(
		"GCE disk image {reference} has no published lazy rootfs ({object}); run `vmon image \
		 publish-gce-rootfs {reference}`"
	))
}

fn derived_names(source_object: &str) -> Result<(String, String)> {
	let stem = source_object
		.strip_suffix(".tar.gz")
		.ok_or_else(|| EngineError::invalid("GCE disk image references must end in .tar.gz"))?;
	let object = format!("{stem}.rootfs.ext4.zst");
	let sidecar = format!("{object}.json");
	Ok((object, sidecar))
}

fn parse_reference(reference: &str) -> Result<(String, String)> {
	let rest = reference
		.strip_prefix(GCS_PREFIX)
		.ok_or_else(|| EngineError::invalid("GCE disk images must use gs://bucket/object.tar.gz"))?;
	let (bucket, object) = rest
		.split_once('/')
		.filter(|(bucket, object)| !bucket.is_empty() && !object.is_empty())
		.ok_or_else(|| EngineError::invalid("GCE disk images must use gs://bucket/object.tar.gz"))?;
	if bucket
		.bytes()
		.any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
		|| object.contains(['?', '#'])
	{
		return Err(EngineError::invalid("invalid gs:// disk image reference"));
	}
	Ok((bucket.to_owned(), object.to_owned()))
}

fn gcs_client() -> Result<Client> {
	Client::builder()
		.build()
		.map_err(|error| EngineError::engine(format!("failed to create GCS client: {error}")))
}

fn access_token() -> Result<String> {
	registry_auth::metadata_access_token().ok_or_else(|| {
		EngineError::unauthorized(
			"failed to obtain a GCE metadata-server OAuth token for the GCS disk image",
		)
	})
}

fn object_metadata(
	client: &Client,
	token: &str,
	bucket: &str,
	object: &str,
) -> Result<Option<ObjectMetadata>> {
	let response = client
		.get(format!(
			"https://storage.googleapis.com/storage/v1/b/{}/o/{}",
			percent_encode(bucket),
			percent_encode(object)
		))
		.header(AUTHORIZATION, format!("Bearer {token}"))
		.send()
		.map_err(|error| EngineError::engine(format!("failed to inspect GCS object: {error}")))?;
	if response.status() == StatusCode::NOT_FOUND {
		return Ok(None);
	}
	let response = require_success(response, "inspect GCS object")?;
	response
		.json()
		.map(Some)
		.map_err(|error| EngineError::engine(format!("invalid GCS object metadata: {error}")))
}

fn download_sidecar(
	client: &Client,
	token: &str,
	bucket: &str,
	object: &str,
) -> Result<Option<DerivedSidecar>> {
	let response = client
		.get(media_url(bucket, object, ""))
		.header(AUTHORIZATION, format!("Bearer {token}"))
		.send()
		.map_err(|error| EngineError::engine(format!("failed to read GCS sidecar: {error}")))?;
	if response.status() == StatusCode::NOT_FOUND {
		return Ok(None);
	}
	let response = require_success(response, "read GCS sidecar")?;
	response
		.json()
		.map(Some)
		.map_err(|error| EngineError::engine(format!("invalid GCS rootfs sidecar: {error}")))
}

fn sidecar_matches(
	sidecar: &DerivedSidecar,
	source: &str,
	source_generation: &str,
	source_digest: &str,
	derived_object: &str,
	derived_metadata: &ObjectMetadata,
	agent_sha256: &str,
) -> bool {
	let size = derived_metadata.size.parse::<u64>().ok();
	sidecar.version == 3
		&& sidecar.block_size == ROOTFS_BLOCK_SIZE
		&& valid_index(sidecar)
		&& sidecar.source == source
		&& sidecar.source_generation == source_generation
		&& sidecar.source_digest == source_digest
		&& sidecar.object == derived_object
		&& Some(sidecar.compressed_size) == size
		&& derived_metadata.md5_hash.as_deref() == Some(sidecar.md5_base64.as_str())
		&& sidecar.agent_sha256 == agent_sha256
}

fn valid_index(sidecar: &DerivedSidecar) -> bool {
	if sidecar.uncompressed_size == 0
		|| sidecar.block_size != ROOTFS_BLOCK_SIZE
		|| sidecar.blocks.len() as u64 != sidecar.uncompressed_size.div_ceil(sidecar.block_size)
	{
		return false;
	}
	let mut expected_offset = 0_u64;
	for [offset, length] in &sidecar.blocks {
		if *offset != expected_offset || *length == 0 {
			return false;
		}
		let Some(next) = expected_offset.checked_add(*length) else {
			return false;
		};
		expected_offset = next;
	}
	expected_offset == sidecar.compressed_size
}

fn metadata_digest(metadata: &ObjectMetadata) -> Result<String> {
	let (kind, encoded) = metadata
		.md5_hash
		.as_deref()
		.map(|value| ("md5", value))
		.or_else(|| metadata.crc32c.as_deref().map(|value| ("crc32c", value)))
		.ok_or_else(|| EngineError::engine("GCS object metadata contained no content digest"))?;
	let bytes = B64
		.decode(encoded)
		.map_err(|error| EngineError::engine(format!("invalid GCS {kind} digest: {error}")))?;
	Ok(format!("gcs-{kind}-{}", hex::encode(bytes)))
}

fn media_url(bucket: &str, object: &str, generation: &str) -> String {
	let generation = if generation.is_empty() {
		String::new()
	} else {
		format!("&generation={generation}")
	};
	format!(
		"https://storage.googleapis.com/download/storage/v1/b/{}/o/{}?alt=media{}",
		percent_encode(bucket),
		percent_encode(object),
		generation
	)
}

fn upload_url(bucket: &str, object: &str) -> String {
	format!(
		"https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
		percent_encode(bucket),
		percent_encode(object)
	)
}

fn resumable_upload_url(bucket: &str, object: &str) -> String {
	format!(
		"https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable&name={}",
		percent_encode(bucket),
		percent_encode(object)
	)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UploadState {
	Incomplete(u64),
	Complete,
}

fn upload_file(
	client: &Client,
	token: &str,
	bucket: &str,
	object: &str,
	path: &Path,
) -> Result<()> {
	let size = path.metadata()?.len();
	let metadata = br#"{"contentType":"application/octet-stream"}"#;
	let response = client
		.post(resumable_upload_url(bucket, object))
		.header(AUTHORIZATION, format!("Bearer {token}"))
		.header(CONTENT_TYPE, "application/json; charset=UTF-8")
		.header(CONTENT_LENGTH, metadata.len())
		.header("X-Upload-Content-Type", "application/octet-stream")
		.header("X-Upload-Content-Length", size)
		.body(metadata.as_slice())
		.send()
		.map_err(|error| {
			EngineError::engine(format!("failed to start GCS resumable upload: {error}"))
		})?;
	let response = require_success(response, "start GCS resumable upload")?;
	let session = response
		.headers()
		.get(LOCATION)
		.and_then(|value| value.to_str().ok())
		.filter(|value| !value.is_empty())
		.ok_or_else(|| EngineError::engine("GCS resumable upload omitted its session URI"))?
		.to_owned();
	let mut next_progress = UPLOAD_PROGRESS_BYTES;
	drive_resumable_upload(
		size,
		GCS_UPLOAD_CHUNK_SIZE,
		GCS_UPLOAD_RETRIES,
		|start, end| {
			let length = end - start;
			let mut file = File::open(path)?;
			file.seek(SeekFrom::Start(start))?;
			let response = client
				.put(&session)
				.header(AUTHORIZATION, format!("Bearer {token}"))
				.header(CONTENT_TYPE, "application/octet-stream")
				.header(CONTENT_LENGTH, length)
				.header(CONTENT_RANGE, format!("bytes {start}-{}/{size}", end - 1))
				.body(Body::sized(file.take(length), length))
				.send()
				.map_err(|error| {
					EngineError::engine(format!(
						"GCS resumable chunk request at byte {start} failed: {error}"
					))
				})?;
			parse_upload_response(response, size, "upload rootfs chunk")
		},
		|| {
			let response = client
				.put(&session)
				.header(AUTHORIZATION, format!("Bearer {token}"))
				.header(CONTENT_LENGTH, 0)
				.header(CONTENT_RANGE, format!("bytes */{size}"))
				.body(Body::sized(io::empty(), 0))
				.send()
				.map_err(|error| {
					EngineError::engine(format!("failed to query GCS resumable upload: {error}"))
				})?;
			parse_upload_response(response, size, "query rootfs upload")
		},
		|uploaded, total| {
			if uploaded == total || uploaded >= next_progress {
				eprintln!("vmon: uploaded {uploaded}/{total} root filesystem bytes");
				next_progress = uploaded.saturating_add(UPLOAD_PROGRESS_BYTES);
			}
		},
	)
}

fn parse_upload_response(response: Response, total: u64, action: &str) -> Result<UploadState> {
	let status = response.status();
	if matches!(status, StatusCode::OK | StatusCode::CREATED) {
		return Ok(UploadState::Complete);
	}
	if status.as_u16() == 308 {
		let committed = match response.headers().get(RANGE) {
			None => 0,
			Some(value) => {
				let value = value.to_str().map_err(|error| {
					EngineError::engine(format!(
						"GCS resumable upload returned an invalid Range header: {error}"
					))
				})?;
				let last = value
					.strip_prefix("bytes=0-")
					.ok_or_else(|| {
						EngineError::engine(format!(
							"GCS resumable upload returned invalid Range {value:?}"
						))
					})?
					.parse::<u64>()
					.map_err(|error| {
						EngineError::engine(format!(
							"GCS resumable upload returned invalid Range {value:?}: {error}"
						))
					})?;
				last.checked_add(1).ok_or_else(|| {
					EngineError::engine("GCS resumable upload committed offset overflowed")
				})?
			},
		};
		if committed > total {
			return Err(EngineError::engine(format!(
				"GCS resumable upload reported {committed} committed bytes for a {total}-byte object"
			)));
		}
		return Ok(UploadState::Incomplete(committed));
	}
	let detail = response.text().unwrap_or_default();
	let detail = detail.trim();
	let suffix = if detail.is_empty() {
		String::new()
	} else {
		format!(": {detail}")
	};
	Err(EngineError::engine(format!("failed to {action}: HTTP {}{suffix}", status.as_u16())))
}

fn drive_resumable_upload<S, Q, P>(
	total: u64,
	chunk_size: u64,
	max_retries: usize,
	mut send: S,
	mut query: Q,
	mut progress: P,
) -> Result<()>
where
	S: FnMut(u64, u64) -> Result<UploadState>,
	Q: FnMut() -> Result<UploadState>,
	P: FnMut(u64, u64),
{
	if total == 0 || chunk_size == 0 {
		return Err(EngineError::invalid(
			"GCS resumable upload requires a non-empty object and chunk size",
		));
	}
	let mut offset = 0_u64;
	let mut failures = 0_usize;
	while offset < total {
		let end = offset.saturating_add(chunk_size).min(total);
		match send(offset, end) {
			Ok(UploadState::Complete) if end == total => {
				progress(total, total);
				return Ok(());
			},
			Ok(UploadState::Complete) => {
				return Err(EngineError::engine(format!(
					"GCS resumable upload completed prematurely at byte {offset} of {total}"
				)));
			},
			Ok(UploadState::Incomplete(committed)) => {
				validate_committed_offset(offset, end, committed)?;
				if committed == offset {
					failures += 1;
					if failures > max_retries {
						return Err(upload_retries_exhausted(
							offset,
							max_retries,
							"GCS accepted no bytes from the upload chunk",
						));
					}
				} else {
					offset = committed;
					failures = 0;
					progress(offset, total);
				}
			},
			Err(error) => {
				failures += 1;
				if failures > max_retries {
					return Err(upload_retries_exhausted(offset, max_retries, &error.to_string()));
				}
				loop {
					match query() {
						Ok(UploadState::Complete) => {
							progress(total, total);
							return Ok(());
						},
						Ok(UploadState::Incomplete(committed)) => {
							validate_committed_offset(offset, end, committed)?;
							if committed > offset {
								offset = committed;
								failures = 0;
								progress(offset, total);
							}
							break;
						},
						Err(error) => {
							failures += 1;
							if failures > max_retries {
								return Err(upload_retries_exhausted(
									offset,
									max_retries,
									&error.to_string(),
								));
							}
						},
					}
				}
			},
		}
	}
	Err(EngineError::engine(format!(
		"GCS resumable upload ended without a completion response at byte {offset} of {total}"
	)))
}

fn validate_committed_offset(current: u64, chunk_end: u64, committed: u64) -> Result<()> {
	if !(current..=chunk_end).contains(&committed) {
		return Err(EngineError::engine(format!(
			"GCS resumable upload reported committed byte {committed}, expected \
			 {current}..={chunk_end}"
		)));
	}
	Ok(())
}

fn upload_retries_exhausted(offset: u64, max_retries: usize, cause: &str) -> EngineError {
	EngineError::engine(format!(
		"GCS resumable upload failed at byte {offset} after {max_retries} retries: {cause}"
	))
}

fn upload_json(
	client: &Client,
	token: &str,
	bucket: &str,
	object: &str,
	value: &DerivedSidecar,
) -> Result<()> {
	let body = serde_json::to_vec_pretty(value)?;
	let response = client
		.post(upload_url(bucket, object))
		.header(AUTHORIZATION, format!("Bearer {token}"))
		.header(CONTENT_TYPE, "application/json")
		.header(CONTENT_LENGTH, body.len())
		.body(body)
		.send()
		.map_err(|error| EngineError::engine(format!("failed to upload GCS sidecar: {error}")))?;
	require_success(response, "upload GCS sidecar")?;
	Ok(())
}

fn percent_encode(value: &str) -> String {
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

fn require_success(response: Response, action: &str) -> Result<Response> {
	let status = response.status();
	if status.is_success() {
		return Ok(response);
	}
	let detail = response.text().unwrap_or_default();
	let detail = detail.trim();
	let suffix = if detail.is_empty() {
		String::new()
	} else {
		format!(": {detail}")
	};
	let message = format!("failed to {action}: HTTP {}{suffix}", status.as_u16());
	if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
		Err(EngineError::unauthorized(message))
	} else {
		Err(EngineError::engine(message))
	}
}

const GCS_READ_RETRIES: usize = 5;

struct ResumableReader<F, R> {
	open:          F,
	current:       R,
	offset:        u64,
	expected_size: u64,
	failures:      usize,
	max_retries:   usize,
}

impl<F, R> ResumableReader<F, R>
where
	F: FnMut(u64) -> io::Result<R>,
	R: Read,
{
	fn new(expected_size: u64, max_retries: usize, mut open: F) -> io::Result<Self> {
		let current = open(0)?;
		Ok(Self { open, current, offset: 0, expected_size, failures: 0, max_retries })
	}

	fn recover(&mut self, mut cause: String) -> io::Result<()> {
		loop {
			self.failures += 1;
			if self.failures > self.max_retries {
				return Err(io::Error::other(format!(
					"GCS download failed at compressed byte {} after {} retries: {cause}",
					self.offset, self.max_retries
				)));
			}
			match (self.open)(self.offset) {
				Ok(reader) => {
					self.current = reader;
					return Ok(());
				},
				Err(error) => cause = error.to_string(),
			}
		}
	}
}

impl<F, R> Read for ResumableReader<F, R>
where
	F: FnMut(u64) -> io::Result<R>,
	R: Read,
{
	fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
		loop {
			match self.current.read(buffer) {
				Ok(0) if self.offset == self.expected_size => return Ok(0),
				Ok(0) => self.recover(format!(
					"GCS response ended before the expected compressed size {}",
					self.expected_size
				))?,
				Ok(count) => {
					self.offset = self
						.offset
						.checked_add(count as u64)
						.ok_or_else(|| io::Error::other("GCS compressed download offset overflowed"))?;
					if self.offset > self.expected_size {
						return Err(io::Error::other(format!(
							"GCS response exceeded expected compressed size {} at byte {}",
							self.expected_size, self.offset
						)));
					}
					self.failures = 0;
					return Ok(count);
				},
				Err(error) => self.recover(error.to_string())?,
			}
		}
	}
}

fn open_gcs_range(
	client: &Client,
	token: &str,
	url: &str,
	expected_size: u64,
	offset: u64,
) -> io::Result<Response> {
	let mut request = client
		.get(url)
		.header(AUTHORIZATION, format!("Bearer {token}"));
	if offset > 0 {
		request = request.header(RANGE, format!("bytes={offset}-"));
	}
	let response = request
		.send()
		.map_err(|error| io::Error::other(format!("GCS request failed: {error}")))?;
	let status = response.status();
	if (offset == 0 && !status.is_success()) || (offset > 0 && status != StatusCode::PARTIAL_CONTENT)
	{
		return Err(io::Error::other(format!(
			"GCS range request at compressed byte {offset} returned HTTP {}",
			status.as_u16()
		)));
	}
	if offset > 0 {
		let content_range = response
			.headers()
			.get(CONTENT_RANGE)
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| {
				io::Error::other(format!(
					"GCS range response at compressed byte {offset} omitted Content-Range"
				))
			})?;
		let expected = format!("bytes {offset}-");
		let total = format!("/{expected_size}");
		if !content_range.starts_with(&expected) || !content_range.ends_with(&total) {
			return Err(io::Error::other(format!(
				"GCS range response at compressed byte {offset} had invalid Content-Range \
				 {content_range:?}; expected start {expected:?} and total {expected_size}"
			)));
		}
	}
	Ok(response)
}

fn download_rootfs(
	client: &Client,
	token: &str,
	bucket: &str,
	object: &str,
	generation: &str,
	compressed_size: u64,
	rootfs: &Path,
) -> Result<u64> {
	let url = media_url(bucket, object, generation);
	let reader = ResumableReader::new(compressed_size, GCS_READ_RETRIES, |offset| {
		open_gcs_range(client, token, &url, compressed_size, offset)
	})
	.map_err(|error| EngineError::engine(format!("failed to start GCS disk download: {error}")))?;
	extract_rootfs_archive(MultiGzDecoder::new(reader), rootfs)
}

fn extract_rootfs_archive(reader: impl Read, rootfs: &Path) -> Result<u64> {
	let mut archive = Archive::new(reader);
	let mut entries = archive
		.entries()
		.map_err(|error| EngineError::engine(format!("invalid GCE disk image tar: {error}")))?;
	let Some(entry) = entries.next() else {
		return Err(EngineError::unsupported(
			"unsupported GCE export: expected disk.raw as the first tar member",
		));
	};
	let mut entry =
		entry.map_err(|error| EngineError::engine(format!("invalid GCE disk image tar: {error}")))?;
	let path = entry
		.path()
		.map_err(|error| EngineError::engine(format!("invalid GCE disk image path: {error}")))?;
	let normalized = path.to_string_lossy();
	let entry_type = entry.header().entry_type();
	if !(entry_type.is_file() || entry_type.is_gnu_sparse())
		|| !matches!(normalized.as_ref(), "disk.raw" | "./disk.raw")
	{
		return Err(EngineError::unsupported(
			"unsupported GCE export: expected disk.raw as the first tar member",
		));
	}
	let declared_size = entry.size();
	extract_streamed_root(&mut entry, declared_size, rootfs)
}

fn extract_streamed_root(source: &mut impl Read, declared_size: u64, out: &Path) -> Result<u64> {
	let mut prefix = vec![0_u8; 1024];
	source.read_exact(&mut prefix).map_err(|error| {
		EngineError::engine(format!(
			"failed to read disk.raw GPT prefix at byte 0 (1024 bytes, declared tar size \
			 {declared_size}): {error}"
		))
	})?;
	let header: &[u8; 512] = prefix[512..1024].try_into().expect("fixed slice");
	let (entries_offset, entry_count, entry_size) = gpt_layout(header)?;
	let table_end = entries_offset
		.checked_add(u64::from(entry_count) * u64::from(entry_size))
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	let table_end =
		usize::try_from(table_end).map_err(|_| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	if table_end > prefix.len() {
		let previous = prefix.len();
		prefix.resize(table_end, 0);
		source
			.read_exact(&mut prefix[previous..])
			.map_err(|error| {
				EngineError::engine(format!(
					"failed to read disk.raw GPT entries at byte {previous} ({} bytes, declared tar \
					 size {declared_size}): {error}",
					table_end - previous
				))
			})?;
	}
	let partition = gpt_partitions(&mut std::io::Cursor::new(&prefix))?
		.into_iter()
		.max_by_key(|partition| partition.length)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	let partition_end = partition
		.offset
		.checked_add(partition.length)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	if partition_end > declared_size {
		return Err(EngineError::unsupported(format!(
			"{UNSUPPORTED_LAYOUT_ERROR}: root partition byte range {}..{partition_end} exceeds \
			 disk.raw tar size {declared_size}",
			partition.offset
		)));
	}
	let consumed = u64::try_from(prefix.len()).expect("prefix length fits u64");
	let skip = partition
		.offset
		.checked_sub(consumed)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	let skipped =
		std::io::copy(&mut source.by_ref().take(skip), &mut std::io::sink()).map_err(|error| {
			EngineError::engine(format!(
				"failed to seek forward through disk.raw from byte {consumed} to root partition at \
				 byte {} (declared tar size {declared_size}): {error}",
				partition.offset
			))
		})?;
	if skipped != skip {
		return Err(EngineError::engine(format!(
			"disk.raw ended at byte {} while seeking to root partition at byte {} (declared tar size \
			 {declared_size})",
			consumed + skipped,
			partition.offset
		)));
	}
	let mut destination = File::options()
		.read(true)
		.write(true)
		.create(true)
		.truncate(true)
		.open(out)?;
	let mut remaining = partition.length;
	let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
	let mut next_progress = PHASE_PROGRESS_BYTES;
	while remaining > 0 {
		let amount = usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded by buffer");
		let partition_progress = partition.length - remaining;
		let absolute_offset = partition.offset + partition_progress;
		source.read_exact(&mut buffer[..amount]).map_err(|error| {
			EngineError::engine(format!(
				"failed to read root partition from disk.raw at byte {absolute_offset} ({amount} \
				 bytes requested, partition {}..{partition_end}, declared tar size {declared_size}): \
				 {error}",
				partition.offset
			))
		})?;
		if buffer[..amount].iter().any(|byte| *byte != 0) {
			destination.write_all(&buffer[..amount])?;
		} else {
			destination.seek(SeekFrom::Current(amount as i64))?;
		}
		remaining -= amount as u64;
		let copied = partition.length - remaining;
		if copied == partition.length || copied >= next_progress {
			eprintln!("vmon: extracted {copied}/{} root filesystem bytes", partition.length);
			next_progress = copied.saturating_add(PHASE_PROGRESS_BYTES);
		}
	}
	destination.set_len(partition.length)?;
	destination.seek(SeekFrom::Start(1024 + 56))?;
	let mut magic = [0_u8; 2];
	destination.read_exact(&mut magic).map_err(|error| {
		EngineError::engine(format!(
			"failed to verify ext4 magic in extracted root partition at byte {}: {error}",
			1024 + 56
		))
	})?;
	if magic != EXT4_MAGIC {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	Ok(partition.length)
}

fn gpt_layout(header: &[u8; 512]) -> Result<(u64, u32, u32)> {
	if &header[..8] != b"EFI PART" {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	let header_size = u32::from_le_bytes(header[12..16].try_into().expect("fixed slice"));
	if !(92..=512).contains(&header_size) {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	let entries_lba = u64::from_le_bytes(header[72..80].try_into().expect("fixed slice"));
	let entry_count = u32::from_le_bytes(header[80..84].try_into().expect("fixed slice"));
	let entry_size = u32::from_le_bytes(header[84..88].try_into().expect("fixed slice"));
	if entry_count == 0 || entry_count > 4096 || !(128..=4096).contains(&entry_size) {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	let entries_offset = entries_lba
		.checked_mul(SECTOR_SIZE)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	Ok((entries_offset, entry_count, entry_size))
}

fn gpt_partitions<R: Read + Seek>(disk: &mut R) -> Result<Vec<Partition>> {
	let mut header = [0_u8; 512];
	disk.seek(SeekFrom::Start(SECTOR_SIZE))?;
	disk.read_exact(&mut header)?;
	let (entries_offset, entry_count, entry_size) = gpt_layout(&header)?;
	let mut entry = vec![0_u8; entry_size as usize];
	let mut partitions = Vec::new();
	for index in 0..entry_count {
		let offset = entries_offset
			.checked_add(u64::from(index) * u64::from(entry_size))
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		disk.seek(SeekFrom::Start(offset))?;
		disk.read_exact(&mut entry)?;
		if entry[..16].iter().all(|byte| *byte == 0) {
			continue;
		}
		let first_lba = u64::from_le_bytes(entry[32..40].try_into().expect("fixed slice"));
		let last_lba = u64::from_le_bytes(entry[40..48].try_into().expect("fixed slice"));
		let sectors = last_lba
			.checked_sub(first_lba)
			.and_then(|value| value.checked_add(1))
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		partitions.push(Partition {
			offset: first_lba
				.checked_mul(SECTOR_SIZE)
				.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?,
			length: sectors
				.checked_mul(SECTOR_SIZE)
				.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?,
		});
	}
	Ok(partitions)
}

#[cfg(test)]
fn find_ext4_root<R: Read + Seek>(disk: &mut R) -> Result<Partition> {
	let mut candidates = Vec::new();
	for partition in gpt_partitions(disk)? {
		let magic_offset = partition
			.offset
			.checked_add(1024 + 56)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		let mut magic = [0_u8; 2];
		disk.seek(SeekFrom::Start(magic_offset))?;
		if disk.read_exact(&mut magic).is_ok() && magic == EXT4_MAGIC {
			candidates.push(partition);
		}
	}
	candidates
		.into_iter()
		.max_by_key(|partition| partition.length)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))
}

fn required_tool(name: &str) -> Result<PathBuf> {
	find_tool(name).ok_or_else(|| {
		EngineError::unsupported(format!(
			"{name} not found (install e2fsprogs to publish gs:// disk images)"
		))
	})
}

fn inject_agent(rootfs: &Path, agent: &Path, work: &Path) -> Result<()> {
	let debugfs = required_tool("debugfs")?;
	let staged_agent = work.join("vmon-agent");
	fs::copy(agent, &staged_agent)?;
	let commands = work.join("debugfs.commands");
	fs::write(
		&commands,
		format!(
			"mkdir /.vmon\nrm /.vmon/agent\nwrite {} /.vmon/agent\nset_inode_field /.vmon/agent mode \
			 0100755\n",
			staged_agent.display()
		),
	)?;
	run_inherited(&[
		debugfs.to_string_lossy().into_owned(),
		"-w".to_owned(),
		"-f".to_owned(),
		commands.to_string_lossy().into_owned(),
		rootfs.to_string_lossy().into_owned(),
	])?;
	let output = Command::new(&debugfs)
		.args(["-R", "stat /.vmon/agent"])
		.arg(rootfs)
		.output()?;
	if !output.status.success() || !String::from_utf8_lossy(&output.stdout).contains("Mode:  0755") {
		return Err(EngineError::engine(
			"debugfs did not provision an executable guest agent at /.vmon/agent",
		));
	}
	Ok(())
}

fn shrink_filesystem(rootfs: &Path) -> Result<()> {
	let e2fsck = required_tool("e2fsck")?;
	let resize2fs = required_tool("resize2fs")?;
	let status = Command::new(&e2fsck)
		.args(["-f", "-p"])
		.arg(rootfs)
		.stdin(Stdio::null())
		.stdout(Stdio::inherit())
		.stderr(Stdio::inherit())
		.status()?;
	if !matches!(status.code(), Some(0 | 1)) {
		return Err(EngineError::engine(format!(
			"{} -fp failed with exit code {}",
			e2fsck.display(),
			status
				.code()
				.map_or_else(|| "signal".to_owned(), |code| code.to_string())
		)));
	}
	run_inherited(&[
		resize2fs.to_string_lossy().into_owned(),
		"-M".to_owned(),
		rootfs.to_string_lossy().into_owned(),
	])
}

fn compress_indexed(source: &Path, out: &Path) -> Result<Vec<[u64; 2]>> {
	let mut input = File::open(source)?;
	let input_size = input.metadata()?.len();
	let mut output = File::create(out)?;
	let block_size = usize::try_from(ROOTFS_BLOCK_SIZE).expect("1 MiB fits usize");
	let mut block = vec![0_u8; block_size];
	let mut blocks = Vec::new();
	let mut processed = 0_u64;
	let mut next_progress = PHASE_PROGRESS_BYTES;
	loop {
		let mut filled = 0;
		while filled < block.len() {
			let count = input.read(&mut block[filled..])?;
			if count == 0 {
				break;
			}
			filled += count;
		}
		if filled == 0 {
			break;
		}
		let offset = output.stream_position()?;
		let frame = zstd::stream::encode_all(&block[..filled], ZSTD_LEVEL).map_err(|error| {
			EngineError::engine(format!("failed to compress rootfs block: {error}"))
		})?;
		output.write_all(&frame)?;
		blocks.push([offset, u64::try_from(frame.len()).expect("compressed frame length fits u64")]);
		processed += u64::try_from(filled).expect("block length fits u64");
		if processed == input_size || processed >= next_progress {
			eprintln!("vmon: compressed {processed}/{input_size} root filesystem bytes");
			next_progress = processed.saturating_add(PHASE_PROGRESS_BYTES);
		}
	}
	output.sync_all()?;
	Ok(blocks)
}

fn sha256_file(path: &Path) -> Result<String> {
	let mut file = File::open(path)?;
	let mut digest = Sha256::new();
	std::io::copy(&mut file, &mut digest)?;
	Ok(hex::encode(digest.finalize()))
}

fn content_digests(path: &Path) -> Result<(String, String)> {
	let mut file = File::open(path)?;
	let mut sha256 = Sha256::new();
	let mut md5 = Md5::new();
	let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
	loop {
		let count = file.read(&mut buffer)?;
		if count == 0 {
			break;
		}
		sha256.update(&buffer[..count]);
		md5.update(&buffer[..count]);
	}
	Ok((hex::encode(sha256.finalize()), B64.encode(md5.finalize())))
}

#[cfg(test)]
mod tests {
	use std::{cell::RefCell, io::Cursor, rc::Rc};

	use super::*;

	#[test]
	fn derived_artifact_names_and_sidecar_round_trip() {
		let (object, sidecar) = derived_names("exports/ubuntu.tar.gz").expect("names");
		assert_eq!(object, "exports/ubuntu.rootfs.ext4.zst");
		assert_eq!(sidecar, "exports/ubuntu.rootfs.ext4.zst.json");
		let value = DerivedSidecar {
			version: 3,
			source: "gs://bucket/exports/ubuntu.tar.gz".to_owned(),
			source_generation: "42".to_owned(),
			source_digest: "gcs-md5-deadbeef".to_owned(),
			object,
			compressed_size: 4096,
			uncompressed_size: 2 * ROOTFS_BLOCK_SIZE,
			original_size: 99 * 1024 * 1024,
			block_size: ROOTFS_BLOCK_SIZE,
			blocks: vec![[0, 2048], [2048, 2048]],
			sha256: "abc".to_owned(),
			md5_base64: "bWQ1".to_owned(),
			agent_sha256: "agent".to_owned(),
		};
		let encoded = serde_json::to_vec(&value).expect("serialize");
		let decoded: DerivedSidecar = serde_json::from_slice(&encoded).expect("deserialize");
		assert_eq!(decoded, value);
	}

	#[test]
	fn indexed_compression_emits_independent_one_mib_frames() {
		let dir = tempfile::tempdir().expect("tempdir");
		let source = dir.path().join("rootfs.ext4");
		let compressed = dir.path().join("rootfs.ext4.zst");
		let bytes: Vec<u8> = (0..=255).cycle().take(2 * 1024 * 1024 + 123).collect();
		fs::write(&source, &bytes).expect("write source");
		let blocks = compress_indexed(&source, &compressed).expect("compress");
		assert_eq!(blocks.len(), 3);
		let encoded = fs::read(compressed).expect("read compressed");
		let mut decoded = Vec::new();
		for [offset, length] in blocks {
			let start = usize::try_from(offset).expect("offset");
			let end = usize::try_from(offset + length).expect("end");
			let block =
				zstd::stream::decode_all(&encoded[start..end]).expect("decode independent frame");
			assert!(block.len() <= ROOTFS_BLOCK_SIZE as usize);
			decoded.extend_from_slice(&block);
		}
		assert_eq!(decoded, bytes);
	}

	#[test]
	fn matching_existing_derived_object_is_skipped() {
		let sidecar = DerivedSidecar {
			version:           3,
			source:            "gs://bucket/image.tar.gz".to_owned(),
			source_generation: "7".to_owned(),
			source_digest:     "gcs-md5-aa".to_owned(),
			object:            "image.rootfs.ext4.zst".to_owned(),
			compressed_size:   8192,
			uncompressed_size: 2 * ROOTFS_BLOCK_SIZE,
			original_size:     99 * 1024 * 1024,
			block_size:        ROOTFS_BLOCK_SIZE,
			blocks:            vec![[0, 4096], [4096, 4096]],
			sha256:            "sha".to_owned(),
			md5_base64:        "digest".to_owned(),
			agent_sha256:      "agent".to_owned(),
		};
		let metadata = ObjectMetadata {
			generation: "8".to_owned(),
			size:       "8192".to_owned(),
			md5_hash:   Some("digest".to_owned()),
			crc32c:     None,
		};
		assert!(sidecar_matches(
			&sidecar,
			"gs://bucket/image.tar.gz",
			"7",
			"gcs-md5-aa",
			"image.rootfs.ext4.zst",
			&metadata,
			"agent",
		));
	}

	#[test]
	fn resumable_upload_queries_and_resumes_after_a_mid_chunk_failure() {
		let events = Rc::new(RefCell::new(Vec::new()));
		let send_events = Rc::clone(&events);
		let query_events = Rc::clone(&events);
		let mut attempts = 0;
		drive_resumable_upload(
			20,
			8,
			3,
			move |start, end| {
				send_events.borrow_mut().push(format!("send:{start}-{end}"));
				attempts += 1;
				if attempts == 1 {
					return Err(EngineError::engine("connection dropped after byte 5"));
				}
				if end == 20 {
					Ok(UploadState::Complete)
				} else {
					Ok(UploadState::Incomplete(end))
				}
			},
			move || {
				query_events.borrow_mut().push("query".to_owned());
				Ok(UploadState::Incomplete(5))
			},
			|_, _| {},
		)
		.expect("resume upload");
		assert_eq!(*events.borrow(), ["send:0-8", "query", "send:5-13", "send:13-20",]);
	}

	#[test]
	fn resumable_reader_reopens_a_truncated_body_at_consumed_offset() {
		let payload: Vec<u8> = (0..=255).cycle().take(4096).collect();
		let expected = payload.clone();
		let opens = Rc::new(RefCell::new(Vec::new()));
		let observed_opens = Rc::clone(&opens);
		let source = payload.clone();
		let mut reader = ResumableReader::new(payload.len() as u64, 2, move |offset| {
			observed_opens.borrow_mut().push(offset);
			let start = usize::try_from(offset).expect("offset");
			let end = if offset == 0 { 733 } else { source.len() };
			Ok(Cursor::new(source[start..end].to_vec()))
		})
		.expect("open");
		let mut actual = Vec::new();
		reader.read_to_end(&mut actual).expect("resume");
		assert_eq!(actual, expected);
		assert_eq!(*opens.borrow(), [0, 733]);
	}

	#[test]
	fn chooses_largest_ext4_partition() {
		let mut disk = vec![0_u8; 8 * 1024 * 1024];
		disk[512..520].copy_from_slice(b"EFI PART");
		disk[524..528].copy_from_slice(&92_u32.to_le_bytes());
		disk[584..592].copy_from_slice(&2_u64.to_le_bytes());
		disk[592..596].copy_from_slice(&4_u32.to_le_bytes());
		disk[596..600].copy_from_slice(&128_u32.to_le_bytes());
		for (index, first, last) in [(0_usize, 2048_u64, 4095_u64), (1, 4096, 12287)] {
			let entry = 1024 + index * 128;
			disk[entry] = 1;
			disk[entry + 32..entry + 40].copy_from_slice(&first.to_le_bytes());
			disk[entry + 40..entry + 48].copy_from_slice(&last.to_le_bytes());
			let magic = (first * SECTOR_SIZE + 1024 + 56) as usize;
			disk[magic..magic + 2].copy_from_slice(&EXT4_MAGIC);
		}
		let partition = find_ext4_root(&mut Cursor::new(disk)).expect("partition");
		assert_eq!(partition.offset, 4096 * SECTOR_SIZE);
		assert_eq!(partition.length, 8192 * SECTOR_SIZE);
	}

	#[test]
	fn streamed_conversion_writes_only_the_root_partition() {
		let mut disk = vec![0_u8; 8 * 1024 * 1024];
		disk[512..520].copy_from_slice(b"EFI PART");
		disk[524..528].copy_from_slice(&92_u32.to_le_bytes());
		disk[584..592].copy_from_slice(&2_u64.to_le_bytes());
		disk[592..596].copy_from_slice(&2_u32.to_le_bytes());
		disk[596..600].copy_from_slice(&128_u32.to_le_bytes());
		let entry = 1024;
		let first = 4096_u64;
		let last = 12287_u64;
		disk[entry] = 1;
		disk[entry + 32..entry + 40].copy_from_slice(&first.to_le_bytes());
		disk[entry + 40..entry + 48].copy_from_slice(&last.to_le_bytes());
		let magic = (first * SECTOR_SIZE + 1024 + 56) as usize;
		disk[magic..magic + 2].copy_from_slice(&EXT4_MAGIC);
		let dir = tempfile::tempdir().expect("tempdir");
		let out = dir.path().join("rootfs.ext4");
		extract_streamed_root(&mut Cursor::new(disk), 8 * 1024 * 1024, &out).expect("extract root");
		assert_eq!(out.metadata().expect("metadata").len(), 8192 * SECTOR_SIZE);
		let mut root = File::open(out).expect("open root");
		root.seek(SeekFrom::Start(1024 + 56)).expect("seek");
		let mut actual_magic = [0_u8; 2];
		root.read_exact(&mut actual_magic).expect("read magic");
		assert_eq!(actual_magic, EXT4_MAGIC);
	}

	#[test]
	fn base256_tar_size_streams_root_partition() {
		let mut disk = vec![0_u8; 64 * 1024];
		disk[512..520].copy_from_slice(b"EFI PART");
		disk[524..528].copy_from_slice(&92_u32.to_le_bytes());
		disk[584..592].copy_from_slice(&2_u64.to_le_bytes());
		disk[592..596].copy_from_slice(&1_u32.to_le_bytes());
		disk[596..600].copy_from_slice(&128_u32.to_le_bytes());
		let first = 40_u64;
		let last = 47_u64;
		disk[1024] = 1;
		disk[1056..1064].copy_from_slice(&first.to_le_bytes());
		disk[1064..1072].copy_from_slice(&last.to_le_bytes());
		let magic = (first * SECTOR_SIZE + 1024 + 56) as usize;
		disk[magic..magic + 2].copy_from_slice(&EXT4_MAGIC);
		let tar = base256_disk_tar(&disk);
		assert_eq!(tar[124], 0x80);
		let dir = tempfile::tempdir().expect("tempdir");
		let out = dir.path().join("rootfs.ext4");
		extract_rootfs_archive(Cursor::new(tar), &out).expect("extract base-256 tar");
		assert_eq!(out.metadata().expect("metadata").len(), 8 * SECTOR_SIZE);
	}

	#[test]
	fn concatenated_gzip_members_decode_as_one_tar_stream() {
		let mut disk = vec![0_u8; 64 * 1024];
		disk[512..520].copy_from_slice(b"EFI PART");
		disk[524..528].copy_from_slice(&92_u32.to_le_bytes());
		disk[584..592].copy_from_slice(&2_u64.to_le_bytes());
		disk[592..596].copy_from_slice(&1_u32.to_le_bytes());
		disk[596..600].copy_from_slice(&128_u32.to_le_bytes());
		let first = 40_u64;
		let last = 47_u64;
		disk[1024] = 1;
		disk[1056..1064].copy_from_slice(&first.to_le_bytes());
		disk[1064..1072].copy_from_slice(&last.to_le_bytes());
		let magic = (first * SECTOR_SIZE + 1024 + 56) as usize;
		disk[magic..magic + 2].copy_from_slice(&EXT4_MAGIC);
		let tar = base256_disk_tar(&disk);
		let split = tar.len() / 2;
		let mut compressed = gzip_member(&tar[..split]);
		compressed.extend_from_slice(&gzip_member(&tar[split..]));
		let dir = tempfile::tempdir().expect("tempdir");
		let out = dir.path().join("rootfs.ext4");
		extract_rootfs_archive(MultiGzDecoder::new(Cursor::new(compressed)), &out)
			.expect("extract multi-member gzip");
		assert_eq!(out.metadata().expect("metadata").len(), 8 * SECTOR_SIZE);
	}

	fn gzip_member(bytes: &[u8]) -> Vec<u8> {
		let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
		encoder.write_all(bytes).expect("gzip write");
		encoder.finish().expect("gzip finish")
	}

	fn base256_disk_tar(disk: &[u8]) -> Vec<u8> {
		let mut header = [0_u8; 512];
		header[..8].copy_from_slice(b"disk.raw");
		header[100..108].copy_from_slice(b"0000644\0");
		header[108..116].copy_from_slice(b"0000000\0");
		header[116..124].copy_from_slice(b"0000000\0");
		let size = u64::try_from(disk.len()).expect("fixture size");
		header[128..136].copy_from_slice(&size.to_be_bytes());
		header[124] = 0x80;
		header[136..148].copy_from_slice(b"00000000000\0");
		header[148..156].fill(b' ');
		header[156] = b'0';
		header[257..263].copy_from_slice(b"ustar\0");
		header[263..265].copy_from_slice(b"00");
		let checksum: u64 = header.iter().map(|byte| u64::from(*byte)).sum();
		let encoded = format!("{checksum:06o}\0 ");
		header[148..156].copy_from_slice(encoded.as_bytes());
		let mut archive = header.to_vec();
		archive.extend_from_slice(disk);
		archive.resize(archive.len().next_multiple_of(512), 0);
		archive.resize(archive.len() + 1024, 0);
		archive
	}

	#[test]
	fn rejects_non_gpt_or_non_ext4_layouts() {
		let mut blank = Cursor::new(vec![0_u8; 4096]);
		assert!(find_ext4_root(&mut blank).is_err());
	}
}
