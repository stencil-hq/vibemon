//! Paused rootfs block images ([`vmm::block_image`]).
//!
//! The object-store form of a paused sandbox disk. One object serves both a
//! full download
//! ([`decode_disk`]) and lazy hydration by the VMM's remote block device
//! (`--rootfs-remote-*`, see [`crate::lazy_resume`]).
//!
//! Both functions block; call them from a blocking worker.

use std::{
	fs::{self, File, OpenOptions},
	io::{BufReader, BufWriter, Write},
	os::unix::fs::{FileExt, OpenOptionsExt},
	path::Path,
};

use anyhow::{Context, Result, bail};
pub use vmm::block_image::BlockIndex;

use crate::artifacts::copy::is_overlay_disk;

/// Object name of the compressed image inside a paused generation.
pub const IMAGE: &str = "rootfs.blk";
/// Object name of its seek index.
pub const INDEX: &str = "rootfs.blk.json";

/// Summary of an encoded image.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockImage {
	/// Bytes of the compressed image (`rootfs.blk`).
	pub compressed_size: u64,
	/// Guest-visible disk size.
	pub disk_size:       u64,
}

/// Encodes the guest-visible contents of the quiescent disk `src_disk`.
///
/// `src_disk` is a raw image or a block overlay with its `.base`; the output
/// is the new files `blk` (frames) and `index` (JSON seek index). Both are
/// durable on return; on failure neither is left behind.
pub fn encode_disk(src_disk: &Path, blk: &Path, index: &Path) -> Result<BlockImage> {
	let output = create_new(blk)?;
	let index_file = match create_new(index) {
		Ok(file) => file,
		Err(error) => {
			drop(output);
			let _ = fs::remove_file(blk);
			return Err(error);
		},
	};
	let result = encode_into(src_disk, &output, index_file);
	if result.is_err() {
		let _ = fs::remove_file(blk);
		let _ = fs::remove_file(index);
	}
	result
}

fn encode_into(src_disk: &Path, output: &File, mut index_file: File) -> Result<BlockImage> {
	let mut writer = BufWriter::with_capacity(1 << 20, output);
	let encoded = if is_overlay_disk(src_disk)
		.with_context(|| format!("probing disk {}", src_disk.display()))?
	{
		let overlay = vmm::disk_overlay::Overlay::open(src_disk, true)
			.with_context(|| format!("opening overlay disk {}", src_disk.display()))?;
		vmm::block_image::encode(overlay.disk_size(), |at, buf| overlay.read_at(buf, at), &mut writer)
	} else {
		let disk =
			File::open(src_disk).with_context(|| format!("opening disk {}", src_disk.display()))?;
		let len = disk.metadata()?.len();
		vmm::block_image::encode(len, |at, buf| disk.read_exact_at(buf, at), &mut writer)
	}
	.with_context(|| format!("encoding block image of {}", src_disk.display()))?;
	writer.flush()?;
	drop(writer);
	output.sync_all()?;
	index_file.write_all(&serde_json::to_vec(&encoded)?)?;
	index_file.sync_all()?;
	Ok(BlockImage {
		compressed_size: encoded.compressed_size,
		disk_size:       encoded.uncompressed_size,
	})
}

/// Reads and validates the seek index at `path`.
pub fn read_index(path: &Path) -> Result<BlockIndex> {
	let bytes = fs::read(path).with_context(|| format!("reading block index {}", path.display()))?;
	BlockIndex::from_json(&bytes).with_context(|| format!("block index {}", path.display()))
}

/// Decodes `blk` (with its seek index `index`) into the new raw image `dest`.
/// All-zero blocks stay holes. The image is durable on return; on failure it
/// is removed.
pub fn decode_disk(blk: &Path, index: &Path, dest: &Path) -> Result<()> {
	let index = read_index(index)?;
	let input = File::open(blk).with_context(|| format!("opening {}", blk.display()))?;
	if input.metadata()?.len() != index.compressed_size {
		bail!(
			"block image {} is {} bytes, its index expects {}",
			blk.display(),
			input.metadata()?.len(),
			index.compressed_size
		);
	}
	let output = create_new(dest)?;
	let result = (|| -> Result<()> {
		output.set_len(index.uncompressed_size)?;
		vmm::block_image::decode(
			&index,
			&mut BufReader::with_capacity(1 << 20, input),
			|at, bytes| output.write_all_at(bytes, at),
		)
		.with_context(|| format!("decoding block image {}", blk.display()))?;
		output.sync_all()?;
		Ok(())
	})();
	if result.is_err() {
		drop(output);
		let _ = fs::remove_file(dest);
	}
	result
}

fn create_new(path: &Path) -> Result<File> {
	OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(path)
		.with_context(|| format!("creating {}", path.display()))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn pattern(len: usize) -> Vec<u8> {
		let mut bytes = vec![0u8; len];
		for (i, byte) in bytes.iter_mut().enumerate().take(300_000) {
			*byte = (i % 251) as u8;
		}
		let tail = len - 4096;
		bytes[tail..].fill(0x5a);
		bytes
	}

	#[test]
	fn raw_and_overlay_disks_roundtrip_sparse() {
		let dir = tempfile::tempdir().expect("tempdir");
		let len = (3 << 20) + 8192;
		let raw = dir.path().join("raw.img");
		fs::write(&raw, pattern(len)).expect("write raw");

		let (blk, index, out) =
			(dir.path().join("raw.blk"), dir.path().join("raw.json"), dir.path().join("raw.out"));
		let image = encode_disk(&raw, &blk, &index).expect("encode raw");
		assert_eq!(image.disk_size, len as u64);
		assert_eq!(fs::metadata(&blk).expect("blk").len(), image.compressed_size);
		assert!(encode_disk(&raw, &blk, &index).is_err(), "outputs must be new");
		decode_disk(&blk, &index, &out).expect("decode raw");
		assert_eq!(fs::read(&out).expect("out"), pattern(len));

		// An overlay encodes its guest-visible view (base plus written clusters).
		let overlay = dir.path().join("overlay.img");
		vmm::disk_overlay::link_base(&raw, &overlay).expect("link base");
		let mut writable =
			vmm::disk_overlay::Overlay::create(&raw, &overlay).expect("create overlay");
		writable
			.write_at(&[0xee; 4096], 1 << 20)
			.expect("overlay write");
		writable.persist().expect("persist");
		drop(writable);
		let (oblk, oindex, oout) =
			(dir.path().join("o.blk"), dir.path().join("o.json"), dir.path().join("o.out"));
		encode_disk(&overlay, &oblk, &oindex).expect("encode overlay");
		decode_disk(&oblk, &oindex, &oout).expect("decode overlay");
		let mut expected = pattern(len);
		expected[1 << 20..(1 << 20) + 4096].fill(0xee);
		assert_eq!(fs::read(&oout).expect("overlay out"), expected);
	}

	#[test]
	fn decode_rejects_mismatched_image_and_leaves_nothing() {
		let dir = tempfile::tempdir().expect("tempdir");
		let raw = dir.path().join("raw.img");
		fs::write(&raw, pattern(1 << 20)).expect("write raw");
		let (blk, index, out) =
			(dir.path().join("raw.blk"), dir.path().join("raw.json"), dir.path().join("raw.out"));
		encode_disk(&raw, &blk, &index).expect("encode");
		let mut bytes = fs::read(&blk).expect("blk");
		bytes.pop();
		fs::write(&blk, bytes).expect("truncate");
		assert!(decode_disk(&blk, &index, &out).is_err());
		assert!(!out.exists());
	}
}
