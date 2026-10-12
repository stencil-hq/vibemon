//! Seekable compressed block images: the object format behind
//! `--rootfs-remote-*` (ARCHITECTURE.md §8.2).
//!
//! An image is a disk cut into [`BLOCK_SIZE`] blocks (the last may be short),
//! each compressed as one independent zstd frame; the frames are concatenated
//! in block order into one object. A JSON [`BlockIndex`] records each frame's
//! `(offset, length)` so a reader can fetch and decompress any block with one
//! HTTP range request. All-zero blocks still get a (tiny) frame: the index
//! stays dense and contiguous, which is what the validation below requires.
//!
//! The producer ([`encode`]) reads the guest-visible disk through a caller
//! callback, so overlays and raw images encode alike; the consumers are the
//! VMM's lazy remote block device and [`decode`] (a full local download).

use std::io::{self, Read, Write};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};

/// Uncompressed bytes per block. One MiB amortizes request latency without
/// pulling a large fraction of a sparse image per metadata touch, and is a
/// multiple of the 512-byte virtio sector.
pub const BLOCK_SIZE: u64 = 1024 * 1024;
/// [`BlockIndex::version`] this build reads and writes.
pub const INDEX_VERSION: u32 = 3;
/// Upper bound of one compressed frame (incompressible data expands
/// slightly; anything larger is corrupt).
pub const MAX_FRAME_SIZE: u64 = BLOCK_SIZE * 2;
/// Upper bound of a serialized index (a 16 TiB disk is ~400 KiB of JSON).
pub const MAX_INDEX_BYTES: u64 = 32 * 1024 * 1024;
const SECTOR_SIZE: u64 = 512;
/// Blocks compressed per parallel batch (64 MiB of input in flight).
const BATCH_BLOCKS: usize = 64;
/// zstd level: fast enough to keep pace with local disk reads.
const LEVEL: i32 = 3;

/// Seek index of one block image (`<image>.json`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockIndex {
	/// Always [`INDEX_VERSION`].
	pub version:           u32,
	/// Length of the compressed object.
	pub compressed_size:   u64,
	/// Guest-visible bytes covered by the frames.
	pub uncompressed_size: u64,
	/// Size of the source disk (equal to `uncompressed_size` for images
	/// written by [`encode`]).
	pub original_size:     u64,
	/// Always [`BLOCK_SIZE`].
	pub block_size:        u64,
	/// `(offset, length)` of each block's frame, contiguous from 0.
	pub blocks:            Vec<(u64, u64)>,
}

impl BlockIndex {
	/// Parses and validates a serialized index.
	pub fn from_json(bytes: &[u8]) -> io::Result<Self> {
		if bytes.len() as u64 > MAX_INDEX_BYTES {
			return Err(invalid(format!("block index exceeds the {MAX_INDEX_BYTES}-byte limit")));
		}
		let index: Self =
			serde_json::from_slice(bytes).map_err(|e| invalid(format!("parsing block index: {e}")))?;
		index.validate()?;
		Ok(index)
	}

	/// Checks the structural invariants every reader relies on: known
	/// version and block size, one frame per block, and frames that tile
	/// `[0, compressed_size)` exactly.
	pub fn validate(&self) -> io::Result<()> {
		if self.version != INDEX_VERSION {
			return Err(invalid(format!(
				"unsupported block index version {}; expected {INDEX_VERSION}",
				self.version
			)));
		}
		if self.block_size != BLOCK_SIZE {
			return Err(invalid(format!(
				"block index block_size {} is unsupported; expected {BLOCK_SIZE}",
				self.block_size
			)));
		}
		if self.uncompressed_size == 0 {
			return Err(invalid("block index uncompressed_size must be greater than zero"));
		}
		let expected = self.uncompressed_size.div_ceil(self.block_size);
		if self.blocks.len() as u64 != expected {
			return Err(invalid(format!(
				"block index has {} blocks, expected {expected} for {} uncompressed bytes",
				self.blocks.len(),
				self.uncompressed_size
			)));
		}
		if self.compressed_size == 0 {
			return Err(invalid("block index compressed_size must be greater than zero"));
		}
		if self.original_size < self.uncompressed_size {
			return Err(invalid(
				"block index original_size must not be smaller than uncompressed_size",
			));
		}
		let mut expected_offset = 0_u64;
		for (block, &(offset, length)) in self.blocks.iter().enumerate() {
			let valid = offset == expected_offset
				&& length != 0
				&& length <= MAX_FRAME_SIZE
				&& offset
					.checked_add(length)
					.is_some_and(|end| end <= self.compressed_size);
			if !valid {
				return Err(invalid(format!(
					"block index block {block} range [{offset}, {}) is not the next contiguous range \
					 within compressed_size {}",
					offset.saturating_add(length),
					self.compressed_size
				)));
			}
			expected_offset += length;
		}
		if expected_offset != self.compressed_size {
			return Err(invalid(format!(
				"block index covers {expected_offset} compressed bytes, expected {}",
				self.compressed_size
			)));
		}
		Ok(())
	}

	/// Uncompressed length of block `block`.
	pub fn block_len(&self, block: usize) -> u64 {
		(self.uncompressed_size - block as u64 * self.block_size).min(self.block_size)
	}

	/// Smallest virtio capacity (a 512-byte multiple) that holds the image.
	pub const fn sector_aligned_size(&self) -> u64 {
		self.uncompressed_size.div_ceil(SECTOR_SIZE) * SECTOR_SIZE
	}
}

/// Decompresses one frame that must yield exactly `expected` bytes.
pub fn decompress_block(frame: &[u8], expected: u64, out: &mut Vec<u8>) -> io::Result<()> {
	out.clear();
	zstd::stream::read::Decoder::new(frame)?
		.take(expected + 1)
		.read_to_end(out)?;
	if out.len() as u64 != expected {
		return Err(invalid(format!(
			"block frame decompressed to {} bytes, expected {expected}",
			out.len()
		)));
	}
	Ok(())
}

/// Encodes a `len`-byte disk into `out`, returning its index.
///
/// `read` fills a buffer with the guest-visible bytes at an offset (a full
/// block, or the short tail). Blocks are compressed in parallel batches and
/// written in order, so `out` may be a plain sequential stream. Zero blocks
/// reuse one precomputed frame.
pub fn encode(
	len: u64,
	mut read: impl FnMut(u64, &mut [u8]) -> io::Result<()>,
	out: &mut impl Write,
) -> io::Result<BlockIndex> {
	if len == 0 {
		return Err(invalid("cannot encode an empty disk"));
	}
	let count = usize::try_from(len.div_ceil(BLOCK_SIZE))
		.map_err(|_| invalid("disk is too large to index"))?;
	let full_zero = compressor()?.compress(&vec![0; BLOCK_SIZE as usize])?;
	let mut blocks = Vec::with_capacity(count);
	let mut offset = 0_u64;
	let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(BATCH_BLOCKS);
	let mut first = 0;
	while first < count {
		let batch = BATCH_BLOCKS.min(count - first);
		buffers.resize_with(batch, Vec::new);
		for (slot, buffer) in buffers.iter_mut().enumerate() {
			let start = (first + slot) as u64 * BLOCK_SIZE;
			buffer.resize((len - start).min(BLOCK_SIZE) as usize, 0);
			read(start, buffer)?;
		}
		let frames = buffers
			.par_iter()
			.map_init(compressor, |compressor, buffer| {
				if buffer.len() as u64 == BLOCK_SIZE && buffer.iter().all(|&byte| byte == 0) {
					return Ok(None);
				}
				match compressor {
					Ok(compressor) => compressor.compress(buffer).map(Some),
					Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
				}
			})
			.collect::<io::Result<Vec<_>>>()?;
		for frame in &frames {
			let frame = frame.as_deref().unwrap_or(&full_zero);
			out.write_all(frame)?;
			blocks.push((offset, frame.len() as u64));
			offset += frame.len() as u64;
		}
		first += batch;
	}
	out.flush()?;
	let index = BlockIndex {
		version: INDEX_VERSION,
		compressed_size: offset,
		uncompressed_size: len,
		original_size: len,
		block_size: BLOCK_SIZE,
		blocks,
	};
	index.validate()?;
	Ok(index)
}

/// Decodes a whole image streamed from `input` (frames in index order).
///
/// `write` receives each nonzero block's bytes at its offset; zero blocks
/// are skipped, so a destination pre-sized to `uncompressed_size` stays
/// sparse.
pub fn decode(
	index: &BlockIndex,
	input: &mut impl Read,
	mut write: impl FnMut(u64, &[u8]) -> io::Result<()>,
) -> io::Result<()> {
	index.validate()?;
	let mut frame = Vec::with_capacity(BLOCK_SIZE as usize);
	let mut block = Vec::with_capacity(BLOCK_SIZE as usize);
	for (number, &(_, length)) in index.blocks.iter().enumerate() {
		frame.resize(length as usize, 0);
		input.read_exact(&mut frame)?;
		decompress_block(&frame, index.block_len(number), &mut block)?;
		if block.iter().any(|&byte| byte != 0) {
			write(number as u64 * BLOCK_SIZE, &block)?;
		}
	}
	let mut trailing = [0u8; 1];
	if input.read(&mut trailing)? != 0 {
		return Err(invalid("block image is longer than its index"));
	}
	Ok(())
}

/// A frame compressor whose frames carry a content checksum, so a corrupted
/// range fails to decode instead of reaching the guest.
fn compressor() -> io::Result<zstd::bulk::Compressor<'static>> {
	let mut compressor = zstd::bulk::Compressor::new(LEVEL)?;
	compressor.include_checksum(true)?;
	Ok(compressor)
}

fn invalid(message: impl Into<String>) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn disk(len: usize) -> Vec<u8> {
		let mut disk = vec![0u8; len];
		// Block 0: text; block 1: zero; block 2 (short tail): noise.
		for (i, byte) in disk[..4096].iter_mut().enumerate() {
			*byte = b"vibemon "[i % 8];
		}
		let mut state = 0x9e37_79b9_u32;
		for byte in &mut disk[2 * BLOCK_SIZE as usize..] {
			state ^= state << 13;
			state ^= state >> 17;
			state ^= state << 5;
			*byte = state as u8;
		}
		disk
	}

	#[test]
	fn encode_decode_roundtrip_keeps_zero_blocks_sparse() {
		let source = disk(2 * BLOCK_SIZE as usize + 12_345);
		let mut object = Vec::new();
		let index = encode(
			source.len() as u64,
			|offset, buffer| {
				buffer.copy_from_slice(&source[offset as usize..offset as usize + buffer.len()]);
				Ok(())
			},
			&mut object,
		)
		.expect("encode");
		assert_eq!(index.blocks.len(), 3);
		assert_eq!(index.compressed_size, object.len() as u64);
		assert!(index.blocks[1].1 < 1024, "zero block frame must be tiny");
		let reparsed =
			BlockIndex::from_json(&serde_json::to_vec(&index).expect("json")).expect("parse");
		assert_eq!(reparsed, index);

		let mut out = vec![0xffu8; source.len()];
		let mut written = Vec::new();
		decode(&index, &mut object.as_slice(), |offset, bytes| {
			written.push(offset);
			out[offset as usize..offset as usize + bytes.len()].copy_from_slice(bytes);
			Ok(())
		})
		.expect("decode");
		assert_eq!(written, vec![0, 2 * BLOCK_SIZE], "zero block must not be written");
		out[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize].fill(0);
		assert_eq!(out, source);
	}

	#[test]
	fn frames_decode_independently_by_range() {
		let source = disk(3 * BLOCK_SIZE as usize);
		let mut object = Vec::new();
		let index = encode(
			source.len() as u64,
			|offset, buffer| {
				buffer.copy_from_slice(&source[offset as usize..offset as usize + buffer.len()]);
				Ok(())
			},
			&mut object,
		)
		.expect("encode");
		let (offset, length) = index.blocks[2];
		let mut block = Vec::new();
		decompress_block(
			&object[offset as usize..(offset + length) as usize],
			index.block_len(2),
			&mut block,
		)
		.expect("block 2");
		assert_eq!(block, source[2 * BLOCK_SIZE as usize..]);
	}

	#[test]
	fn validation_rejects_gaps_truncation_and_trailing_bytes() {
		let source = disk(2 * BLOCK_SIZE as usize + 1);
		let mut object = Vec::new();
		let index = encode(
			source.len() as u64,
			|offset, buffer| {
				buffer.copy_from_slice(&source[offset as usize..offset as usize + buffer.len()]);
				Ok(())
			},
			&mut object,
		)
		.expect("encode");
		let mut gap = index.clone();
		gap.blocks[1].0 += 1;
		assert!(gap.validate().is_err());
		let mut short = index.clone();
		short.blocks.pop();
		assert!(short.validate().is_err());
		let mut longer = object.clone();
		longer.push(0);
		assert!(decode(&index, &mut longer.as_slice(), |_, _| Ok(())).is_err());
		assert!(
			decode(&index, &mut &object[..object.len() - 1], |_, _| Ok(())).is_err(),
			"truncated object must fail"
		);
		// A flipped payload byte fails the frame checksum.
		let (offset, length) = index.blocks[2];
		let mut corrupt = object[offset as usize..(offset + length) as usize].to_vec();
		let middle = corrupt.len() / 2;
		corrupt[middle] ^= 0x40;
		let mut out = Vec::new();
		assert!(decompress_block(&corrupt, index.block_len(2), &mut out).is_err());
	}
}
