//! Sparse-aware artifact files (ARCHITECTURE.md §8.5).
//!
//! Build images are large sparse files: a template rootfs has a 20 GiB
//! apparent size but typically a few hundred MiB of data, and snapshot memory
//! files are mostly holes too. Manifests therefore describe every served file
//! by its data extents plus a digest over those extents only, and peers
//! transfer only extent bytes. Hashing, transfer, and verification cost
//! O(data) instead of O(apparent size), and fetched files stay sparse.

use std::{
	fs::File,
	io::{self, Read, Seek, SeekFrom},
	path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::api::ArtifactFile;

/// Holes shorter than this between two data extents are folded into one
/// extent (fewer, larger reads; the folded zeros are transferred).
const MERGE_GAP: u64 = 64 << 10;
/// Read size when streaming extents.
const CHUNK: usize = 256 << 10;

/// Digest of a sparse file: SHA-256 over every data extent as
/// `offset ‖ len ‖ bytes` (offsets and lengths as big-endian `u64`), followed
/// by the logical file size.
pub struct ExtentHasher(Sha256);

impl Default for ExtentHasher {
	fn default() -> Self {
		Self(Sha256::new())
	}
}

impl ExtentHasher {
	/// Starts the next extent; its bytes follow via [`Self::update`].
	pub fn extent(&mut self, [offset, len]: [u64; 2]) {
		self.0.update(offset.to_be_bytes());
		self.0.update(len.to_be_bytes());
	}

	/// Feeds bytes of the current extent.
	pub fn update(&mut self, data: &[u8]) {
		self.0.update(data);
	}

	/// Lowercase hex digest for a file of `size` logical bytes.
	pub fn finish(mut self, size: u64) -> String {
		self.0.update(size.to_be_bytes());
		hex::encode(self.0.finalize())
	}
}

/// Data extents `[offset, len]` of `file` (`SEEK_DATA`/`SEEK_HOLE`), sorted,
/// with holes shorter than [`MERGE_GAP`] folded in. Filesystems without hole
/// reporting yield one extent covering the whole file.
pub fn data_extents(file: &File, size: u64) -> io::Result<Vec<[u64; 2]>> {
	let mut out: Vec<[u64; 2]> = Vec::new();
	#[cfg(unix)]
	{
		use std::os::fd::AsRawFd;
		let fd = file.as_raw_fd();
		let mut pos = 0u64;
		while pos < size {
			let offset = libc::off_t::try_from(pos).map_err(io::Error::other)?;
			// SAFETY: `lseek` on a descriptor borrowed from a live `File`
			// only moves its offset; every read below seeks explicitly.
			let data = unsafe { libc::lseek(fd, offset, libc::SEEK_DATA) };
			if data < 0 {
				let error = io::Error::last_os_error();
				match error.raw_os_error() {
					// No data at or after `pos`: the rest is a hole.
					Some(libc::ENXIO) => break,
					Some(libc::EINVAL | libc::ENOTSUP) if pos == 0 => return Ok(vec![[0, size]]),
					_ => return Err(error),
				}
			}
			// SAFETY: as above.
			let hole = unsafe { libc::lseek(fd, data, libc::SEEK_HOLE) };
			if hole < 0 {
				return Err(io::Error::last_os_error());
			}
			let (start, end) = (data.cast_unsigned(), hole.cast_unsigned().min(size));
			if end <= start {
				break;
			}
			push_extent(&mut out, start, end);
			pos = end;
		}
	}
	#[cfg(not(unix))]
	{
		let _ = file;
		if size > 0 {
			out.push([0, size]);
		}
	}
	Ok(out)
}

fn push_extent(out: &mut Vec<[u64; 2]>, start: u64, end: u64) {
	if let Some(last) = out.last_mut()
		&& start.saturating_sub(last[0] + last[1]) < MERGE_GAP
	{
		last[1] = end - last[0];
		return;
	}
	out.push([start, end - start]);
}

/// Describes `dir/rel` for the manifest: size, data extents, and extent
/// digest (blocking).
pub fn describe(dir: &Path, rel: &str) -> Result<ArtifactFile> {
	let path = dir.join(rel);
	let mut file = File::open(&path).with_context(|| format!("opening {}", path.display()))?;
	let size = file.metadata()?.len();
	let extents =
		data_extents(&file, size).with_context(|| format!("mapping {}", path.display()))?;
	let mut hasher = ExtentHasher::default();
	let mut buf = vec![0u8; CHUNK];
	for &[offset, len] in &extents {
		hasher.extent([offset, len]);
		file.seek(SeekFrom::Start(offset))?;
		let mut left = len;
		while left > 0 {
			let want = usize::try_from(left).unwrap_or(usize::MAX).min(buf.len());
			file
				.read_exact(&mut buf[..want])
				.with_context(|| format!("reading {}", path.display()))?;
			hasher.update(&buf[..want]);
			left -= want as u64;
		}
	}
	Ok(ArtifactFile { path: rel.to_owned(), size, sha256: hasher.finish(size), extents })
}

/// Rejects extent lists that are empty-length, unsorted, overlapping, or
/// reach past the file size.
pub fn validate(file: &ArtifactFile) -> Result<()> {
	let mut end = 0u64;
	for &[offset, len] in &file.extents {
		let Some(stop) = offset.checked_add(len) else {
			bail!("extent overflow in {}", file.path);
		};
		if len == 0 || offset < end || stop > file.size {
			bail!("invalid extents in {}", file.path);
		}
		end = stop;
	}
	Ok(())
}

/// Streams the bytes of `extents` of `path`, in order, from a blocking reader
/// task. Must be called inside a tokio runtime.
pub fn read_extents(
	path: PathBuf,
	extents: Vec<[u64; 2]>,
) -> impl Stream<Item = io::Result<Bytes>> + Send + 'static {
	let (tx, rx) = tokio::sync::mpsc::channel::<io::Result<Bytes>>(4);
	tokio::task::spawn_blocking(move || {
		let result = (|| -> io::Result<()> {
			let mut file = File::open(&path)?;
			for [offset, len] in extents {
				file.seek(SeekFrom::Start(offset))?;
				let mut left = len;
				while left > 0 {
					let want = usize::try_from(left).unwrap_or(usize::MAX).min(CHUNK);
					let mut buf = vec![0u8; want];
					file.read_exact(&mut buf)?;
					left -= want as u64;
					if tx.blocking_send(Ok(Bytes::from(buf))).is_err() {
						return Ok(());
					}
				}
			}
			Ok(())
		})();
		if let Err(error) = result {
			let _ = tx.blocking_send(Err(error));
		}
	});
	futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|item| (item, rx)) })
}

/// Writes the extent bytes of `file` (as produced by [`read_extents`]) into a
/// sparse `dest`, verifying length and digest before the final rename.
pub async fn write_extents(
	stream: impl Stream<Item = Result<Bytes>>,
	file: &ArtifactFile,
	dest: &Path,
) -> Result<()> {
	validate(file)?;
	let tmp = dest.with_file_name(format!(
		".{}.{:016x}.part",
		dest
			.file_name()
			.and_then(|n| n.to_str())
			.unwrap_or("artifact"),
		rand::random::<u64>()
	));
	let result = async {
		let mut out = tokio::fs::File::create(&tmp)
			.await
			.with_context(|| format!("creating {}", tmp.display()))?;
		out.set_len(file.size).await?;
		let mut hasher = ExtentHasher::default();
		let mut extents = file.extents.iter().copied();
		let mut left = 0u64;
		let mut stream = std::pin::pin!(stream);
		while let Some(chunk) = stream.next().await {
			let mut chunk = chunk?;
			while !chunk.is_empty() {
				if left == 0 {
					let Some(extent) = extents.next() else {
						bail!("peer sent more than the extents of {}", file.path);
					};
					hasher.extent(extent);
					out.seek(SeekFrom::Start(extent[0])).await?;
					left = extent[1];
				}
				let take = usize::try_from(left).unwrap_or(usize::MAX).min(chunk.len());
				let part = chunk.split_to(take);
				hasher.update(&part);
				if part.iter().all(|b| *b == 0) {
					// Already a hole after `set_len`; keep it one.
					out.seek(SeekFrom::Current(i64::try_from(take)?)).await?;
				} else {
					out.write_all(&part).await?;
				}
				left -= take as u64;
			}
		}
		if left > 0 || extents.next().is_some() {
			bail!("peer sent a truncated {}", file.path);
		}
		out.flush().await?;
		out.sync_all().await?;
		let actual = hasher.finish(file.size);
		if actual != file.sha256 {
			bail!("{} digest mismatch: expected {}, got {actual}", file.path, file.sha256);
		}
		tokio::fs::rename(&tmp, dest)
			.await
			.with_context(|| format!("installing {}", dest.display()))?;
		Ok(())
	}
	.await;
	if result.is_err() {
		let _ = tokio::fs::remove_file(&tmp).await;
	}
	result
}

#[cfg(test)]
mod tests {
	use std::io::Write;

	use super::*;

	const MIB: u64 = 1 << 20;

	/// 64 MiB file with data at 0..4 KiB and 32 MiB..32 MiB+8 KiB.
	fn sparse_file(dir: &Path) -> PathBuf {
		let path = dir.join("rootfs.ext4");
		let mut file = File::create(&path).unwrap();
		file.set_len(64 * MIB).unwrap();
		file.write_all(&[0xaa; 4096]).unwrap();
		file.seek(SeekFrom::Start(32 * MIB)).unwrap();
		file.write_all(&[0x55; 8192]).unwrap();
		file.sync_all().unwrap();
		path
	}

	fn allocated(path: &Path) -> u64 {
		use std::os::unix::fs::MetadataExt;
		std::fs::metadata(path).unwrap().blocks() * 512
	}

	#[test]
	fn extents_cover_data_and_skip_holes() {
		let dir = tempfile::tempdir().unwrap();
		let path = sparse_file(dir.path());
		let file = File::open(&path).unwrap();
		let extents = data_extents(&file, 64 * MIB).unwrap();
		let covered: u64 = extents.iter().map(|e| e[1]).sum();
		assert!(covered < 8 * MIB, "holes are skipped: {extents:?}");
		for (start, len) in [(0, 4096), (32 * MIB, 8192)] {
			assert!(
				extents
					.iter()
					.any(|&[offset, size]| (offset..=offset + size).contains(&start)
						&& start + len <= offset + size),
				"{start}+{len} covered by {extents:?}"
			);
		}
		let described = describe(dir.path(), "rootfs.ext4").unwrap();
		assert_eq!(described.size, 64 * MIB);
		assert_eq!(described.extents, extents);
		validate(&described).unwrap();
	}

	#[test]
	fn digest_depends_on_offsets_and_size() {
		let digest = |offset: u64, size: u64| {
			let mut h = ExtentHasher::default();
			h.extent([offset, 2]);
			h.update(b"ab");
			h.finish(size)
		};
		assert_ne!(digest(0, 10), digest(4, 10), "same bytes elsewhere differ");
		assert_ne!(digest(0, 10), digest(0, 11), "trailing hole length counts");
	}

	#[test]
	fn validate_rejects_bad_extent_lists() {
		let file = |extents: Vec<[u64; 2]>| ArtifactFile {
			path: "f".into(),
			size: 100,
			sha256: "0".repeat(64),
			extents,
		};
		validate(&file(vec![[0, 10], [10, 90]])).unwrap();
		validate(&file(vec![])).unwrap();
		for bad in
			[vec![[0, 0]], vec![[50, 10], [0, 10]], vec![[0, 20], [10, 10]], vec![[90, 20]], vec![[
				u64::MAX,
				2,
			]]] {
			assert!(validate(&file(bad.clone())).is_err(), "{bad:?}");
		}
	}

	#[tokio::test]
	async fn round_trip_stays_sparse_and_detects_tampering_and_truncation() {
		let src_dir = tempfile::tempdir().unwrap();
		let path = sparse_file(src_dir.path());
		let file = describe(src_dir.path(), "rootfs.ext4").unwrap();
		let chunks: Vec<Bytes> = read_extents(path.clone(), file.extents.clone())
			.map(|c| c.unwrap())
			.collect()
			.await;
		let sent: u64 = chunks.iter().map(|c| c.len() as u64).sum();
		assert_eq!(sent, file.extents.iter().map(|e| e[1]).sum::<u64>(), "only extents travel");

		let dst_dir = tempfile::tempdir().unwrap();
		let dest = dst_dir.path().join("rootfs.ext4");
		let stream = || futures::stream::iter(chunks.clone().into_iter().map(Ok));
		write_extents(stream(), &file, &dest).await.unwrap();
		assert_eq!(std::fs::read(&dest).unwrap(), std::fs::read(&path).unwrap());
		assert!(allocated(&dest) < 8 * MIB, "destination stays sparse");

		let mut tampered = chunks.clone();
		let mut first = tampered[0].to_vec();
		first[0] ^= 0xff;
		tampered[0] = Bytes::from(first);
		let other = dst_dir.path().join("tampered");
		let error = write_extents(futures::stream::iter(tampered.into_iter().map(Ok)), &file, &other)
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("digest mismatch"), "{error:#}");
		assert!(!other.exists());

		let mut short = chunks.clone();
		short.pop();
		let error = write_extents(futures::stream::iter(short.into_iter().map(Ok)), &file, &other)
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("truncated"), "{error:#}");

		let mut long = chunks;
		long.push(Bytes::from_static(b"extra"));
		let error = write_extents(futures::stream::iter(long.into_iter().map(Ok)), &file, &other)
			.await
			.unwrap_err();
		assert!(format!("{error:#}").contains("more than the extents"), "{error:#}");
		let leftovers = std::fs::read_dir(dst_dir.path()).unwrap().count();
		assert_eq!(leftovers, 1, "only the installed file remains");
	}
}
