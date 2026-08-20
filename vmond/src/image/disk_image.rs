//! Cloud disk publication and lazy-consumption metadata.
//!
//! Gzip-tar, raw, and fixed-VHD cloud exports are normalized to one
//! ext4 root partition, provisioned with the guest agent, and uploaded beside
//! the source as independently compressed, indexed blocks.

use std::{
	fs::{self, File},
	io::{self, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	process::{Command, Stdio},
};

use flate2::read::MultiGzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tar::Archive;
use vmon_cloud::ObjectAuth;

use super::{
	ImageConfig, find_tool,
	object_store::{ObjectLocation, ObjectMetadata, ObjectStore, is_cloud_reference},
	run_inherited,
};
use crate::error::{EngineError, ErrorCode, Result};

pub(super) const UNSUPPORTED_LAYOUT_ERROR: &str = "unsupported cloud disk layout: no ext4 root \
                                                   partition found (LVM and non-ext4 roots are \
                                                   not supported)";
const SECTOR_SIZE: u64 = 512;
const EXT4_MAGIC: [u8; 2] = [0x53, 0xef];
const EXT4_MAGIC_OFFSET: u64 = 1024 + 56;
const COPY_BUFFER_SIZE: usize = 8 * 1024 * 1024;
const ROOTFS_BLOCK_SIZE: u64 = 1024 * 1024;
const ZSTD_LEVEL: i32 = 3;
const PHASE_PROGRESS_BYTES: u64 = 1024 * 1024 * 1024;
const SIDECAR_VERSION: u32 = 4;

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
	pub version:           String,
	pub url:               String,
	pub auth:              ObjectAuth,
	pub region:            Option<String>,
	pub etag:              Option<String>,
	pub compressed_size:   u64,
	pub uncompressed_size: u64,
	pub original_size:     u64,
	pub block_size:        u64,
	pub blocks:            Vec<[u64; 2]>,
	pub sha256:            String,
	pub agent_sha256:      String,
	pub logical_size:      u64,
}

/// Result of deliberately publishing one range-addressable cloud root
/// filesystem.
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct DerivedSidecar {
	version:           u32,
	source:            String,
	source_version:    String,
	source_digest:     String,
	object:            String,
	object_version:    String,
	object_digest:     String,
	compressed_size:   u64,
	uncompressed_size: u64,
	original_size:     u64,
	block_size:        u64,
	blocks:            Vec<[u64; 2]>,
	sha256:            String,
	agent_sha256:      String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Partition {
	offset: u64,
	length: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiskFormat {
	GzipTar,
	Raw,
}

/// Publish the ext4 root partition beside a cloud disk export.
///
/// Conversion is explicit because it reads the complete source. Gzip-tar
/// exports are consumed as streams; raw and fixed-VHD exports range-read only
/// the selected root partition.
pub fn publish(reference: &str, agent: &Path) -> Result<PublishedRootfs> {
	let (mut store, source) = ObjectStore::open(reference)?;
	let format = disk_format(&source.key)?;
	let (derived_key, sidecar_key) = derived_names(&source.key)?;
	let derived = source.with_key(derived_key);
	let sidecar_location = source.with_key(sidecar_key);
	let source_metadata = store.metadata(&source)?.ok_or_else(|| {
		EngineError::not_found(format!("cloud disk image does not exist: {reference}"))
	})?;
	let agent_sha256 = sha256_file(agent)?;
	let existing_sidecar = match store.read_json_bytes(&sidecar_location) {
		Ok(Some(bytes)) => match serde_json::from_slice::<DerivedSidecar>(&bytes) {
			Ok(sidecar) => Some(sidecar),
			Err(error) => {
				tracing::warn!(
					%error,
					sidecar = %sidecar_location.reference,
					"ignoring malformed published rootfs sidecar during regeneration"
				);
				None
			},
		},
		Ok(None) => None,
		Err(error) if error.code == ErrorCode::Invalid => {
			tracing::warn!(
				%error,
				sidecar = %sidecar_location.reference,
				"ignoring invalid published rootfs sidecar during regeneration"
			);
			None
		},
		Err(error) => return Err(error),
	};
	if let (Some(derived_metadata), Some(sidecar)) = (store.metadata(&derived)?, existing_sidecar)
		&& sidecar_matches(
			&sidecar,
			reference,
			&source_metadata,
			&derived,
			&derived_metadata,
			&agent_sha256,
		) {
		return Ok(PublishedRootfs {
			object:            derived.reference,
			sidecar:           sidecar_location.reference,
			compressed_size:   sidecar.compressed_size,
			uncompressed_size: sidecar.uncompressed_size,
			original_size:     sidecar.original_size,
			sha256:            sidecar.sha256,
			skipped:           true,
		});
	}

	let work = tempfile::Builder::new()
		.prefix("vmon-cloud-publish-")
		.tempdir()?;
	let rootfs = work.path().join("rootfs.ext4");
	eprintln!(
		"vmon: extracting root filesystem from {reference} ({} source bytes)",
		source_metadata.size
	);
	let original_size = download_rootfs(&mut store, &source, &source_metadata, format, &rootfs)?;
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
	let sha256 = sha256_file(&compressed)?;
	let compressed_size = compressed.metadata()?.len();
	eprintln!(
		"vmon: compressed root filesystem to {compressed_size} bytes; uploading to {}",
		store.scheme()
	);
	store.put_file(&derived, &compressed)?;
	let derived_metadata = store.metadata(&derived)?.ok_or_else(|| {
		EngineError::engine(format!(
			"uploaded root filesystem is not visible at {}",
			derived.reference
		))
	})?;
	let sidecar = DerivedSidecar {
		version: SIDECAR_VERSION,
		source: reference.to_owned(),
		source_version: source_metadata.version,
		source_digest: source_metadata.digest,
		object: derived.key.clone(),
		object_version: derived_metadata.version,
		object_digest: derived_metadata.digest,
		compressed_size,
		uncompressed_size,
		original_size,
		block_size: ROOTFS_BLOCK_SIZE,
		blocks,
		sha256: sha256.clone(),
		agent_sha256,
	};
	store.put_json(&sidecar_location, &sidecar)?;
	Ok(PublishedRootfs {
		object: derived.reference,
		sidecar: sidecar_location.reference,
		compressed_size,
		uncompressed_size,
		original_size,
		sha256,
		skipped: false,
	})
}

/// Resolve a cloud disk export to its already-published lazy root filesystem.
pub(super) fn prepare(reference: &str) -> Result<PreparedDiskImage> {
	let (mut store, source) = ObjectStore::open(reference)?;
	let _ = disk_format(&source.key)?;
	let (derived_key, sidecar_key) = derived_names(&source.key)?;
	let derived = source.with_key(derived_key);
	let sidecar_location = source.with_key(sidecar_key);
	let source_metadata = store.metadata(&source)?.ok_or_else(|| {
		EngineError::not_found(format!("cloud disk image does not exist: {reference}"))
	})?;
	let derived_metadata = store
		.metadata(&derived)?
		.ok_or_else(|| missing_derived(reference, &derived.reference))?;
	let sidecar_bytes = store
		.read_json_bytes(&sidecar_location)?
		.ok_or_else(|| missing_derived(reference, &derived.reference))?;
	let sidecar: DerivedSidecar = serde_json::from_slice(&sidecar_bytes).map_err(|error| {
		EngineError::invalid(format!(
			"published rootfs metadata is malformed for {reference}: {error}; run `vmon image \
			 publish-rootfs {reference}`"
		))
	})?;
	if !sidecar_matches(
		&sidecar,
		reference,
		&source_metadata,
		&derived,
		&derived_metadata,
		&sidecar.agent_sha256,
	) {
		return Err(EngineError::invalid(format!(
			"published rootfs metadata is stale or inconsistent for {reference}; run `vmon image \
			 publish-rootfs {reference}`"
		)));
	}
	let url = store.object_url(&derived, &derived_metadata)?;
	Ok(PreparedDiskImage {
		reference: reference.to_owned(),
		digest:    format!("{}-sha256-{}", store.scheme(), sidecar.sha256),
		remote:    RemoteRootfs {
			source: reference.to_owned(),
			object: derived.reference,
			version: derived_metadata.version,
			url,
			auth: store.auth(),
			region: store.region(),
			etag: derived_metadata.etag,
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

pub(super) fn is_reference(reference: &str) -> bool {
	is_cloud_reference(reference)
}

fn missing_derived(reference: &str, object: &str) -> EngineError {
	EngineError::not_found(format!(
		"cloud disk image {reference} has no published lazy rootfs ({object}); run `vmon image \
		 publish-rootfs {reference}`"
	))
}

fn disk_format(value: &str) -> Result<DiskFormat> {
	let value = value.to_ascii_lowercase();
	if value.strip_suffix(".tar.gz").is_some() || value.strip_suffix(".tgz").is_some() {
		Ok(DiskFormat::GzipTar)
	} else if value.strip_suffix(".raw").is_some()
		|| value.strip_suffix(".img").is_some()
		|| value.strip_suffix(".vhd").is_some()
	{
		Ok(DiskFormat::Raw)
	} else {
		Err(EngineError::invalid("cloud disk images must end in .tar.gz, .tgz, .raw, .img, or .vhd"))
	}
}

fn derived_names(source_key: &str) -> Result<(String, String)> {
	let lowercase = source_key.to_ascii_lowercase();
	let stem = [".tar.gz", ".tgz", ".raw", ".img", ".vhd"]
		.into_iter()
		.find(|suffix| lowercase.ends_with(suffix))
		.and_then(|suffix| source_key.get(..source_key.len().saturating_sub(suffix.len())))
		.ok_or_else(|| {
			EngineError::invalid("cloud disk images must end in .tar.gz, .tgz, .raw, .img, or .vhd")
		})?;
	let object = format!("{stem}.rootfs.ext4.zst");
	let sidecar = format!("{object}.json");
	Ok((object, sidecar))
}

fn sidecar_matches(
	sidecar: &DerivedSidecar,
	source: &str,
	source_metadata: &ObjectMetadata,
	derived: &ObjectLocation,
	derived_metadata: &ObjectMetadata,
	agent_sha256: &str,
) -> bool {
	sidecar.version == SIDECAR_VERSION
		&& sidecar.block_size == ROOTFS_BLOCK_SIZE
		&& source_metadata.etag.is_some()
		&& derived_metadata.etag.is_some()
		&& valid_index(sidecar)
		&& sidecar.source == source
		&& sidecar.source_version == source_metadata.version
		&& sidecar.source_digest == source_metadata.digest
		&& sidecar.object == derived.key
		&& sidecar.object_version == derived_metadata.version
		&& sidecar.object_digest == derived_metadata.digest
		&& sidecar.compressed_size == derived_metadata.size
		&& sidecar.agent_sha256 == agent_sha256
}

fn valid_index(sidecar: &DerivedSidecar) -> bool {
	if sidecar.uncompressed_size == 0
		|| sidecar.original_size < sidecar.uncompressed_size
		|| !valid_sha256(&sidecar.sha256)
		|| !valid_sha256(&sidecar.agent_sha256)
		|| sidecar.block_size != ROOTFS_BLOCK_SIZE
		|| sidecar.blocks.len() as u64 != sidecar.uncompressed_size.div_ceil(sidecar.block_size)
	{
		return false;
	}
	let mut expected_offset = 0_u64;
	for [offset, length] in &sidecar.blocks {
		if *offset != expected_offset || *length == 0 || *length > ROOTFS_BLOCK_SIZE * 2 {
			return false;
		}
		let Some(next) = expected_offset.checked_add(*length) else {
			return false;
		};
		expected_offset = next;
	}
	expected_offset == sidecar.compressed_size
}

fn valid_sha256(value: &str) -> bool {
	value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

const CLOUD_READ_RETRIES: usize = 5;

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
					"cloud download failed at byte {} after {} retries: {cause}",
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
					"response ended before the expected object size {}",
					self.expected_size
				))?,
				Ok(count) => {
					self.offset = self
						.offset
						.checked_add(count as u64)
						.ok_or_else(|| io::Error::other("cloud download offset overflowed"))?;
					if self.offset > self.expected_size {
						return Err(io::Error::other(format!(
							"response exceeded expected object size {} at byte {}",
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

fn download_rootfs(
	store: &mut ObjectStore,
	location: &ObjectLocation,
	metadata: &ObjectMetadata,
	format: DiskFormat,
	rootfs: &Path,
) -> Result<u64> {
	match format {
		DiskFormat::GzipTar => {
			let reader = ResumableReader::new(metadata.size, CLOUD_READ_RETRIES, |offset| {
				store.open_range(location, metadata, offset)
			})
			.map_err(|error| {
				EngineError::engine(format!("failed to start cloud disk download: {error}"))
			})?;
			extract_rootfs_archive(MultiGzDecoder::new(reader), rootfs)
		},
		DiskFormat::Raw => extract_remote_root(store, location, metadata, rootfs),
	}
}

fn extract_remote_root(
	store: &mut ObjectStore,
	location: &ObjectLocation,
	metadata: &ObjectMetadata,
	rootfs: &Path,
) -> Result<u64> {
	let mut prefix = read_remote_exact(store, location, metadata, 0, 1024)?;
	let header: &[u8; 512] = prefix[512..1024].try_into().expect("fixed slice");
	let (entries_offset, entry_count, entry_size) = gpt_layout(header)?;
	let table_end = entries_offset
		.checked_add(u64::from(entry_count) * u64::from(entry_size))
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	if table_end > metadata.size {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	let table_end =
		usize::try_from(table_end).map_err(|_| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	if table_end > prefix.len() {
		prefix.resize(table_end, 0);
		let remaining =
			read_remote_exact(store, location, metadata, 1024, table_end.saturating_sub(1024))?;
		prefix[1024..].copy_from_slice(&remaining);
	}
	let partitions = gpt_partitions(&mut std::io::Cursor::new(&prefix))?;
	let mut candidates = Vec::new();
	for partition in partitions {
		let partition_end = partition
			.offset
			.checked_add(partition.length)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		if partition_end > metadata.size {
			continue;
		}
		let magic_offset = partition
			.offset
			.checked_add(EXT4_MAGIC_OFFSET)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		if read_remote_exact(store, location, metadata, magic_offset, 2)? == EXT4_MAGIC {
			candidates.push(partition);
		}
	}
	let partition = candidates
		.into_iter()
		.max_by_key(|partition| partition.length)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	let mut reader = ResumableReader::new(partition.length, CLOUD_READ_RETRIES, |relative| {
		store.open_range(location, metadata, partition.offset.saturating_add(relative))
	})
	.map_err(|error| EngineError::engine(format!("failed to read root partition: {error}")))?;
	let mut destination = File::options()
		.read(true)
		.write(true)
		.create(true)
		.truncate(true)
		.open(rootfs)?;
	let copied = io::copy(&mut reader.by_ref().take(partition.length), &mut destination)?;
	if copied != partition.length {
		return Err(EngineError::engine(format!(
			"cloud disk ended after {copied} of {} root partition bytes",
			partition.length
		)));
	}
	Ok(copied)
}

fn read_remote_exact(
	store: &mut ObjectStore,
	location: &ObjectLocation,
	metadata: &ObjectMetadata,
	offset: u64,
	length: usize,
) -> Result<Vec<u8>> {
	let length_u64 =
		u64::try_from(length).map_err(|_| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
	if offset
		.checked_add(length_u64)
		.is_none_or(|end| end > metadata.size)
	{
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
	let mut response = store.open_range(location, metadata, offset)?;
	let mut bytes = vec![0_u8; length];
	response.read_exact(&mut bytes)?;
	Ok(bytes)
}

fn extract_rootfs_archive(reader: impl Read, rootfs: &Path) -> Result<u64> {
	let mut archive = Archive::new(reader);
	let mut entries = archive
		.entries()
		.map_err(|error| EngineError::engine(format!("invalid cloud disk image tar: {error}")))?;
	let Some(entry) = entries.next() else {
		return Err(EngineError::unsupported(
			"unsupported cloud export: expected disk.raw as the first tar member",
		));
	};
	let mut entry = entry
		.map_err(|error| EngineError::engine(format!("invalid cloud disk image tar: {error}")))?;
	let path = entry
		.path()
		.map_err(|error| EngineError::engine(format!("invalid cloud disk image path: {error}")))?;
	let normalized = path.to_string_lossy();
	let entry_type = entry.header().entry_type();
	if !(entry_type.is_file() || entry_type.is_gnu_sparse())
		|| !matches!(normalized.as_ref(), "disk.raw" | "./disk.raw")
	{
		return Err(EngineError::unsupported(
			"unsupported cloud export: expected disk.raw as the first tar member",
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
	if table_end > declared_size {
		return Err(EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR));
	}
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

	let mut partitions = gpt_partitions(&mut std::io::Cursor::new(&prefix))?;
	partitions.sort_by_key(|partition| partition.offset);
	let mut consumed = u64::try_from(prefix.len()).expect("prefix length fits u64");
	let mut selected = None;
	let mut buffer = vec![0_u8; COPY_BUFFER_SIZE];
	for partition in partitions {
		let partition_end = partition
			.offset
			.checked_add(partition.length)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		if partition_end > declared_size {
			continue;
		}
		let skip = partition
			.offset
			.checked_sub(consumed)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		let skipped =
			io::copy(&mut source.by_ref().take(skip), &mut io::sink()).map_err(|error| {
				EngineError::engine(format!(
					"failed to scan disk.raw from byte {consumed} to partition at byte {} (declared \
					 tar size {declared_size}): {error}",
					partition.offset
				))
			})?;
		if skipped != skip {
			return Err(EngineError::engine(format!(
				"disk.raw ended at byte {} while scanning for an ext4 root partition (declared tar \
				 size {declared_size})",
				consumed + skipped
			)));
		}
		consumed = partition.offset;

		let probe_length = partition.length.min(EXT4_MAGIC_OFFSET + 2);
		let probe_length_usize = usize::try_from(probe_length)
			.map_err(|_| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		let mut probe = vec![0_u8; probe_length_usize];
		source.read_exact(&mut probe).map_err(|error| {
			EngineError::engine(format!(
				"failed to inspect disk.raw partition at byte {} (declared tar size {declared_size}): \
				 {error}",
				partition.offset
			))
		})?;
		consumed = consumed
			.checked_add(probe_length)
			.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))?;
		let is_ext4 = probe_length == EXT4_MAGIC_OFFSET + 2
			&& probe[EXT4_MAGIC_OFFSET as usize..][..2] == EXT4_MAGIC;
		let replace_selected =
			is_ext4 && selected.is_none_or(|current: Partition| partition.length > current.length);
		let mut destination = if replace_selected {
			Some(
				File::options()
					.read(true)
					.write(true)
					.create(true)
					.truncate(true)
					.open(out)?,
			)
		} else {
			None
		};
		if let Some(destination) = destination.as_mut() {
			write_sparse(destination, &probe)?;
		}

		let mut remaining = partition.length - probe_length;
		let mut next_progress = PHASE_PROGRESS_BYTES;
		while remaining > 0 {
			let amount =
				usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded by buffer");
			source.read_exact(&mut buffer[..amount]).map_err(|error| {
				EngineError::engine(format!(
					"failed to read disk.raw partition at byte {consumed} ({amount} bytes requested, \
					 partition {}..{partition_end}, declared tar size {declared_size}): {error}",
					partition.offset
				))
			})?;
			if let Some(destination) = destination.as_mut() {
				write_sparse(destination, &buffer[..amount])?;
			}
			remaining -= amount as u64;
			consumed += amount as u64;
			if replace_selected {
				let copied = partition.length - remaining;
				if copied == partition.length || copied >= next_progress {
					eprintln!("vmon: extracted {copied}/{} root filesystem bytes", partition.length);
					next_progress = copied.saturating_add(PHASE_PROGRESS_BYTES);
				}
			}
		}
		if let Some(destination) = destination {
			destination.set_len(partition.length)?;
			selected = Some(partition);
		}
	}
	selected
		.map(|partition| partition.length)
		.ok_or_else(|| EngineError::unsupported(UNSUPPORTED_LAYOUT_ERROR))
}

fn write_sparse(destination: &mut File, bytes: &[u8]) -> io::Result<()> {
	if bytes.iter().any(|byte| *byte != 0) {
		destination.write_all(bytes)
	} else {
		destination.seek(SeekFrom::Current(bytes.len() as i64))?;
		Ok(())
	}
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
			.checked_add(EXT4_MAGIC_OFFSET)
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
			"{name} not found (install e2fsprogs to publish cloud disk images)"
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

#[cfg(test)]
mod tests {
	use std::{cell::RefCell, io::Cursor, rc::Rc};

	use super::*;

	#[test]
	fn accepts_supported_export_formats() {
		assert!(matches!(disk_format("image.tar.gz"), Ok(DiskFormat::GzipTar)));
		assert!(matches!(disk_format("image.tgz"), Ok(DiskFormat::GzipTar)));
		for suffix in ["raw", "img", "vhd"] {
			assert!(matches!(disk_format(&format!("image.{suffix}")), Ok(DiskFormat::Raw)));
		}
		assert!(disk_format("image.qcow2").is_err());
	}

	#[test]
	fn derived_artifact_names_and_sidecar_round_trip() {
		let (object, sidecar) = derived_names("exports/ubuntu.tar.gz").expect("names");
		assert_eq!(object, "exports/ubuntu.rootfs.ext4.zst");
		assert_eq!(sidecar, "exports/ubuntu.rootfs.ext4.zst.json");
		let value = DerivedSidecar {
			version: SIDECAR_VERSION,
			source: "gs://bucket/exports/ubuntu.tar.gz".to_owned(),
			source_version: "42".to_owned(),
			source_digest: "gcs-md5-deadbeef".to_owned(),
			object,
			object_version: "43".to_owned(),
			object_digest: "gcs-md5-cafebabe".to_owned(),
			compressed_size: 4096,
			uncompressed_size: 2 * ROOTFS_BLOCK_SIZE,
			original_size: 99 * 1024 * 1024,
			block_size: ROOTFS_BLOCK_SIZE,
			blocks: vec![[0, 2048], [2048, 2048]],
			sha256: "abc".to_owned(),
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
			version:           SIDECAR_VERSION,
			source:            "gs://bucket/image.tar.gz".to_owned(),
			source_version:    "7".to_owned(),
			source_digest:     "gcs-md5-aa".to_owned(),
			object:            "image.rootfs.ext4.zst".to_owned(),
			object_version:    "8".to_owned(),
			object_digest:     "gcs-md5-digest".to_owned(),
			compressed_size:   8192,
			uncompressed_size: 2 * ROOTFS_BLOCK_SIZE,
			original_size:     99 * 1024 * 1024,
			block_size:        ROOTFS_BLOCK_SIZE,
			blocks:            vec![[0, 4096], [4096, 4096]],
			sha256:            "a".repeat(64),
			agent_sha256:      "b".repeat(64),
		};
		let source = ObjectMetadata {
			version:    "7".to_owned(),
			version_id: None,
			size:       99 * 1024 * 1024,
			digest:     "gcs-md5-aa".to_owned(),
			etag:       Some("\"source\"".to_owned()),
		};
		let derived = ObjectLocation {
			reference: "gs://bucket/image.rootfs.ext4.zst".to_owned(),
			bucket:    "bucket".to_owned(),
			key:       "image.rootfs.ext4.zst".to_owned(),
		};
		let metadata = ObjectMetadata {
			version:    "8".to_owned(),
			version_id: None,
			size:       8192,
			digest:     "gcs-md5-digest".to_owned(),
			etag:       Some("\"derived\"".to_owned()),
		};
		assert!(sidecar_matches(
			&sidecar,
			"gs://bucket/image.tar.gz",
			&source,
			&derived,
			&metadata,
			&"b".repeat(64),
		));
		let mut malformed = sidecar;
		malformed.sha256 = "not-a-sha256".to_owned();
		assert!(!sidecar_matches(
			&malformed,
			"gs://bucket/image.tar.gz",
			&source,
			&derived,
			&metadata,
			&"b".repeat(64),
		));
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
	fn streamed_conversion_ignores_a_larger_non_ext4_partition() {
		let mut disk = vec![0_u8; 8 * 1024 * 1024];
		disk[512..520].copy_from_slice(b"EFI PART");
		disk[524..528].copy_from_slice(&92_u32.to_le_bytes());
		disk[584..592].copy_from_slice(&2_u64.to_le_bytes());
		disk[592..596].copy_from_slice(&2_u32.to_le_bytes());
		disk[596..600].copy_from_slice(&128_u32.to_le_bytes());
		for (index, first, last) in [(0_usize, 2048_u64, 4095_u64), (1, 4096, 12287)] {
			let entry = 1024 + index * 128;
			disk[entry] = 1;
			disk[entry + 32..entry + 40].copy_from_slice(&first.to_le_bytes());
			disk[entry + 40..entry + 48].copy_from_slice(&last.to_le_bytes());
		}
		let root_offset = 2048 * SECTOR_SIZE;
		let magic = (root_offset + EXT4_MAGIC_OFFSET) as usize;
		disk[magic..magic + 2].copy_from_slice(&EXT4_MAGIC);
		let dir = tempfile::tempdir().expect("tempdir");
		let out = dir.path().join("rootfs.ext4");
		let extracted =
			extract_streamed_root(&mut Cursor::new(disk), 8 * 1024 * 1024, &out).expect("extract");
		assert_eq!(extracted, 2048 * SECTOR_SIZE);
		assert_eq!(out.metadata().expect("metadata").len(), 2048 * SECTOR_SIZE);
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
