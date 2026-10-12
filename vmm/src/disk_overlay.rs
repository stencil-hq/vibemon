//! Block-level copy-on-write overlay images (ARCHITECTURE.md §8.2).
//!
//! An overlay makes a guest disk copy-on-write on any host filesystem. The
//! guest sees a disk the size of an immutable raw *base* image; clusters it
//! has written live in the overlay file at their own offset, every other
//! cluster is read from the base. Overlay file layout:
//!
//! | range | contents |
//! |---|---|
//! | `[0, disk_size)` | written clusters at identity offsets, holes elsewhere |
//! | `[bitmap_off, +bitmap_len)` | allocation bitmap: one bit per 4 KiB cluster, little-endian `u64` words |
//! | last 4 KiB | footer: magic, geometry, base fingerprint, checksum |
//!
//! The base is never referenced by path: it is the hard link `<overlay>.base`
//! next to the overlay ([`base_path`]). Link counts keep a base alive after
//! its template is deleted, and moving an overlay means moving both files. A
//! base is always a raw image: an overlay of an overlay clones the source's
//! data and bitmap (reflink when possible, otherwise its allocated extents)
//! and links the same raw base, so chains never form and every copy costs
//! O(written data).
//!
//! The bitmap is authoritative, holes are not: a clear bit reads the base
//! even where the file has data, a set bit over a hole reads zeroes. It is
//! kept in memory while a VMM runs and persisted when the device quiesces
//! (pause/snapshot), on durable flushes, and on drop.

use std::{
	fs::{self, File, OpenOptions},
	io::{self, Seek, SeekFrom},
	os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt},
	path::{Path, PathBuf},
};

/// Suffix of the hard-linked raw base next to an overlay.
pub const BASE_SUFFIX: &str = ".base";
/// log2 of the copy-on-write granularity.
const CLUSTER_SHIFT: u32 = 12;
/// Copy-on-write granularity (one guest filesystem block).
pub const CLUSTER_SIZE: u64 = 1 << CLUSTER_SHIFT;
/// Alignment of the bitmap and footer regions.
const REGION_ALIGN: u64 = 4096;
/// Length of the footer region at the end of the file.
const FOOTER_LEN: u64 = 4096;
const MAGIC: [u8; 8] = *b"VMONCOW1";
const VERSION: u32 = 1;
/// Encoded footer bytes: magic, version, cluster shift, four `u64` fields,
/// checksum.
const FOOTER_BYTES: usize = 8 + 4 + 4 + 8 * 4 + 8;
/// Bytes of the base hashed into its fingerprint.
const FINGERPRINT_LEN: u64 = 64 << 10;
/// Copy buffer for extent copies and materialization.
const COPY_CHUNK: usize = 1 << 20;

/// The hard-linked raw base of the overlay at `overlay`.
pub fn base_path(overlay: &Path) -> PathBuf {
	let mut name = overlay.as_os_str().to_owned();
	name.push(BASE_SUFFIX);
	PathBuf::from(name)
}

/// Whether `path` is an overlay image (as opposed to a raw disk image).
pub fn is_overlay(path: &Path) -> io::Result<bool> {
	Ok(probe(&File::open(path)?)?.is_some())
}

/// Geometry and base identity recorded in the footer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Footer {
	disk_size:        u64,
	bitmap_off:       u64,
	bitmap_len:       u64,
	base_fingerprint: u64,
}

impl Footer {
	const fn new(disk_size: u64, base_fingerprint: u64) -> Self {
		let clusters = disk_size.div_ceil(CLUSTER_SIZE);
		Self {
			disk_size,
			bitmap_off: disk_size.next_multiple_of(REGION_ALIGN),
			bitmap_len: clusters.div_ceil(64) * 8,
			base_fingerprint,
		}
	}

	const fn clusters(&self) -> u64 {
		self.disk_size.div_ceil(CLUSTER_SIZE)
	}

	const fn footer_off(&self) -> u64 {
		(self.bitmap_off + self.bitmap_len).next_multiple_of(REGION_ALIGN)
	}

	const fn file_len(&self) -> u64 {
		self.footer_off() + FOOTER_LEN
	}

	fn encode(&self) -> [u8; FOOTER_BYTES] {
		let mut out = [0u8; FOOTER_BYTES];
		out[..8].copy_from_slice(&MAGIC);
		out[8..12].copy_from_slice(&VERSION.to_le_bytes());
		out[12..16].copy_from_slice(&CLUSTER_SHIFT.to_le_bytes());
		out[16..24].copy_from_slice(&self.disk_size.to_le_bytes());
		out[24..32].copy_from_slice(&self.bitmap_off.to_le_bytes());
		out[32..40].copy_from_slice(&self.bitmap_len.to_le_bytes());
		out[40..48].copy_from_slice(&self.base_fingerprint.to_le_bytes());
		let checksum = fnv1a(FNV_OFFSET, &out[..48]);
		out[48..56].copy_from_slice(&checksum.to_le_bytes());
		out
	}

	fn decode(bytes: &[u8; FOOTER_BYTES]) -> Option<Self> {
		let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().expect("4 bytes"));
		let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().expect("8 bytes"));
		if bytes[..8] != MAGIC
			|| u32_at(8) != VERSION
			|| u32_at(12) != CLUSTER_SHIFT
			|| u64_at(48) != fnv1a(FNV_OFFSET, &bytes[..48])
		{
			return None;
		}
		let footer = Self::new(u64_at(16), u64_at(40));
		(footer.bitmap_off == u64_at(24) && footer.bitmap_len == u64_at(32)).then_some(footer)
	}
}

/// Reads the footer of `file`; `None` for raw images.
fn probe(file: &File) -> io::Result<Option<Footer>> {
	let len = file.metadata()?.len();
	if len < FOOTER_LEN || !len.is_multiple_of(REGION_ALIGN) {
		return Ok(None);
	}
	let mut bytes = [0u8; FOOTER_BYTES];
	file.read_exact_at(&mut bytes, len - FOOTER_LEN)?;
	Ok(Footer::decode(&bytes).filter(|footer| footer.file_len() == len))
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

fn fnv1a(mut hash: u64, bytes: &[u8]) -> u64 {
	for byte in bytes {
		hash ^= u64::from(*byte);
		hash = hash.wrapping_mul(0x0100_0000_01b3);
	}
	hash
}

/// Identity of a raw base: its length and a hash of its first 64 KiB (the
/// ext4 superblock and group descriptors). Catches a mis-paired base.
fn fingerprint(base: &File, len: u64) -> io::Result<u64> {
	let mut head = vec![0u8; FINGERPRINT_LEN.min(len) as usize];
	base.read_exact_at(&mut head, 0)?;
	Ok(fnv1a(fnv1a(FNV_OFFSET, &len.to_le_bytes()), &head))
}

/// One bit per cluster.
struct Bitmap {
	words: Vec<u64>,
	/// Word range modified since the last persist (`start..end`).
	dirty: Option<(usize, usize)>,
}

impl Bitmap {
	fn new(clusters: u64) -> Self {
		Self { words: vec![0; clusters.div_ceil(64) as usize], dirty: None }
	}

	fn get(&self, cluster: u64) -> bool {
		self.words[(cluster / 64) as usize] & (1 << (cluster % 64)) != 0
	}

	/// First cluster in `from..limit` whose bit equals `value`, else `limit`.
	fn find(&self, from: u64, limit: u64, value: bool) -> u64 {
		let mut cluster = from;
		while cluster < limit {
			let index = (cluster / 64) as usize;
			let word = if value {
				self.words[index]
			} else {
				!self.words[index]
			} & (u64::MAX << (cluster % 64));
			if word != 0 {
				return (index as u64 * 64 + u64::from(word.trailing_zeros())).min(limit);
			}
			cluster = (index as u64 + 1) * 64;
		}
		limit
	}

	/// Sets the bits of clusters `start..end`.
	fn set(&mut self, start: u64, end: u64) {
		if start >= end {
			return;
		}
		let (first, last) = ((start / 64) as usize, ((end - 1) / 64) as usize);
		for index in first..=last {
			let lo = if index == first { start % 64 } else { 0 };
			let hi = if index == last { (end - 1) % 64 } else { 63 };
			let mask = (u64::MAX << lo) & (u64::MAX >> (63 - hi));
			self.words[index] |= mask;
		}
		self.dirty = Some(match self.dirty {
			Some((lo, hi)) => (lo.min(first), hi.max(last + 1)),
			None => (first, last + 1),
		});
	}
}

/// A contiguous byte range served by one layer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Run {
	/// `true`: the overlay file; `false`: the base image.
	pub overlay: bool,
	pub offset:  u64,
	pub len:     u64,
}

/// An open overlay image and its base.
pub struct Overlay {
	file:      File,
	base:      File,
	footer:    Footer,
	bitmap:    Bitmap,
	read_only: bool,
}

impl Overlay {
	/// Creates the overlay `dest` over `base` (`dest` and `<dest>.base` must
	/// not exist, except a `<dest>.base` that already links the right base).
	///
	/// A raw `base` gets an empty overlay in O(1). An overlay `base` is
	/// cloned (data and bitmap) onto the same raw base.
	pub fn create(base: &Path, dest: &Path) -> io::Result<Self> {
		let source = open_regular(base)?;
		if probe(&source)?.is_some() {
			drop(source);
			clone_overlay(base, dest)?;
			return Self::open(dest, false);
		}
		ensure_absent(dest)?;
		let disk_size = source.metadata()?.len();
		let footer = Footer::new(disk_size, fingerprint(&source, disk_size)?);
		let link = base_path(dest);
		let linked = ensure_link(base, &source, &link)?;
		let created = create_new(dest).and_then(|file| {
			file.set_len(footer.file_len())?;
			file.write_all_at(&footer.encode(), footer.footer_off())?;
			Ok(file)
		});
		match created {
			Ok(file) => Ok(Self {
				file,
				base: source,
				bitmap: Bitmap::new(footer.clusters()),
				footer,
				read_only: false,
			}),
			Err(error) => {
				let _ = fs::remove_file(dest);
				if linked {
					let _ = fs::remove_file(&link);
				}
				Err(error)
			},
		}
	}

	/// Opens the existing overlay at `path` in place.
	pub fn open(path: &Path, read_only: bool) -> io::Result<Self> {
		let file = OpenOptions::new()
			.read(true)
			.write(!read_only)
			.custom_flags(libc::O_NOFOLLOW)
			.open(path)
			.map_err(|e| context(e, format!("opening overlay {}", path.display())))?;
		let footer = probe(&file)?.ok_or_else(|| {
			io::Error::new(
				io::ErrorKind::InvalidData,
				format!("{} is not an overlay image", path.display()),
			)
		})?;
		let base_file = base_path(path);
		let base = open_regular(&base_file)?;
		verify_base(&base, &footer, &base_file)?;
		let mut bytes = vec![0u8; footer.bitmap_len as usize];
		file.read_exact_at(&mut bytes, footer.bitmap_off)?;
		let mut bitmap = Bitmap::new(footer.clusters());
		for (word, chunk) in bitmap.words.iter_mut().zip(bytes.chunks_exact(8)) {
			*word = u64::from_le_bytes(chunk.try_into().expect("8-byte chunk"));
		}
		Ok(Self { file, base, footer, bitmap, read_only })
	}

	/// Logical disk size in bytes.
	pub const fn disk_size(&self) -> u64 {
		self.footer.disk_size
	}

	/// The overlay file (written clusters).
	pub const fn file(&self) -> &File {
		&self.file
	}

	/// The raw base image.
	pub const fn base(&self) -> &File {
		&self.base
	}

	/// Splits `offset..offset + len` into maximal single-layer runs.
	pub fn runs(&self, offset: u64, len: u64) -> impl Iterator<Item = Run> + '_ {
		let end = offset + len;
		let mut pos = offset;
		std::iter::from_fn(move || {
			if pos >= end {
				return None;
			}
			let cluster = pos >> CLUSTER_SHIFT;
			let overlay = self.bitmap.get(cluster);
			let limit = ((end - 1) >> CLUSTER_SHIFT) + 1;
			let next = self.bitmap.find(cluster, limit, !overlay);
			let run_end = (next << CLUSTER_SHIFT).min(end);
			let run = Run { overlay, offset: pos, len: run_end - pos };
			pos = run_end;
			Some(run)
		})
	}

	/// Whether writing `offset..offset + len` straight to the overlay file
	/// would lose base bytes: a partially covered cluster that is not yet in
	/// the overlay needs a read-modify-write ([`Self::write_at`]).
	pub fn needs_copy_up(&self, offset: u64, len: u64) -> bool {
		if len == 0 {
			return false;
		}
		let end = offset + len;
		let (first, last) = (offset >> CLUSTER_SHIFT, (end - 1) >> CLUSTER_SHIFT);
		[first, last]
			.into_iter()
			.any(|cluster| !covers(offset, end, cluster) && !self.bitmap.get(cluster))
	}

	/// Records that `offset..offset + len` now lives in the overlay file.
	/// Only valid once those bytes were written there and every partially
	/// covered cluster was already present ([`Self::needs_copy_up`]).
	pub fn mark_written(&mut self, offset: u64, len: u64) {
		if len == 0 {
			return;
		}
		let end = offset + len;
		self
			.bitmap
			.set(offset >> CLUSTER_SHIFT, ((end - 1) >> CLUSTER_SHIFT) + 1);
	}

	/// Reads guest-visible bytes.
	pub fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
		for run in self.runs(offset, buf.len() as u64) {
			let start = (run.offset - offset) as usize;
			let dst = &mut buf[start..start + run.len as usize];
			if run.overlay {
				self.file.read_exact_at(dst, run.offset)?;
			} else {
				self.base.read_exact_at(dst, run.offset)?;
			}
		}
		Ok(())
	}

	/// Writes guest bytes, copying base bytes up into partially covered
	/// clusters that are not in the overlay yet.
	pub fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
		if buf.is_empty() {
			return Ok(());
		}
		let end = offset + buf.len() as u64;
		let (first, last) = (offset >> CLUSTER_SHIFT, (end - 1) >> CLUSTER_SHIFT);
		let mut direct = (offset, end);
		if !covers(offset, end, first) && !self.bitmap.get(first) {
			self.copy_up(first, buf, offset)?;
			direct.0 = ((first + 1) << CLUSTER_SHIFT).min(end);
		}
		if last != first && !covers(offset, end, last) && !self.bitmap.get(last) {
			self.copy_up(last, buf, offset)?;
			direct.1 = last << CLUSTER_SHIFT;
		}
		if direct.0 < direct.1 {
			let from = (direct.0 - offset) as usize;
			let to = (direct.1 - offset) as usize;
			self.file.write_all_at(&buf[from..to], direct.0)?;
		}
		self.mark_written(offset, buf.len() as u64);
		Ok(())
	}

	/// Writes cluster `cluster` as its base bytes patched with the part of
	/// `buf` (at disk offset `offset`) that overlaps it.
	fn copy_up(&self, cluster: u64, buf: &[u8], offset: u64) -> io::Result<()> {
		let start = cluster << CLUSTER_SHIFT;
		let mut block = [0u8; CLUSTER_SIZE as usize];
		let base_len = self.disk_size().saturating_sub(start).min(CLUSTER_SIZE) as usize;
		self.base.read_exact_at(&mut block[..base_len], start)?;
		let from = offset.max(start);
		let to = (offset + buf.len() as u64).min(start + CLUSTER_SIZE);
		block[(from - start) as usize..(to - start) as usize]
			.copy_from_slice(&buf[(from - offset) as usize..(to - offset) as usize]);
		self.file.write_all_at(&block, start)
	}

	/// Writes bitmap changes into the file (page cache; no fsync).
	pub fn persist(&mut self) -> io::Result<()> {
		let Some((first, end)) = self.bitmap.dirty else {
			return Ok(());
		};
		if self.read_only {
			return Ok(());
		}
		let bytes: Vec<u8> = self.bitmap.words[first..end]
			.iter()
			.flat_map(|word| word.to_le_bytes())
			.collect();
		self
			.file
			.write_all_at(&bytes, self.footer.bitmap_off + first as u64 * 8)?;
		self.bitmap.dirty = None;
		Ok(())
	}

	/// [`Self::persist`] plus `fdatasync`: the image is durable.
	pub fn sync(&mut self) -> io::Result<()> {
		self.persist()?;
		self.file.sync_data()
	}

	/// Writes the guest-visible disk as a new raw image `dest` (base clone,
	/// then every overlay cluster). Returns whether the base was reflinked.
	pub fn materialize(&self, dest: &Path) -> io::Result<bool> {
		let (out, reflinked) = copy_new(&self.base, dest)?;
		let result = (|| {
			let clusters = self.footer.clusters();
			let mut buf = vec![0u8; COPY_CHUNK];
			let mut cluster = 0;
			while cluster < clusters {
				let start = self.bitmap.find(cluster, clusters, true);
				if start >= clusters {
					break;
				}
				let end = self.bitmap.find(start, clusters, false);
				let range = (start << CLUSTER_SHIFT, (end << CLUSTER_SHIFT).min(self.disk_size()));
				copy_range(&self.file, &out, range, &mut buf)?;
				cluster = end;
			}
			out.set_len(self.disk_size())?;
			out.sync_all()
		})();
		if let Err(error) = result {
			drop(out);
			let _ = fs::remove_file(dest);
			return Err(error);
		}
		Ok(reflinked)
	}
}

impl Drop for Overlay {
	fn drop(&mut self) {
		if let Err(error) = self.persist() {
			tracing::warn!(%error, "persisting overlay bitmap on close failed");
		}
	}
}

/// Whether `offset..end` covers all of `cluster`.
const fn covers(offset: u64, end: u64, cluster: u64) -> bool {
	let start = cluster << CLUSTER_SHIFT;
	offset <= start && end >= start + CLUSTER_SIZE
}

/// Clones the overlay `src` (whose VMM is quiesced or gone) into the new
/// overlay `dst` on the same raw base. Reflinks when the filesystem can,
/// otherwise copies the allocated extents only.
pub fn clone_overlay(src: &Path, dst: &Path) -> io::Result<()> {
	let file = open_regular(src)?;
	let footer = probe(&file)?.ok_or_else(|| {
		io::Error::new(io::ErrorKind::InvalidData, format!("{} is not an overlay", src.display()))
	})?;
	let base_file = base_path(src);
	let base = open_regular(&base_file)?;
	verify_base(&base, &footer, &base_file)?;
	ensure_absent(dst)?;
	let link = base_path(dst);
	let linked = ensure_link(&base_file, &base, &link)?;
	match copy_new(&file, dst) {
		Ok(_) => Ok(()),
		Err(error) => {
			if linked {
				let _ = fs::remove_file(&link);
			}
			Err(error)
		},
	}
}

/// Hard-links the raw base of `base` as `<dest>.base`. Idempotent.
///
/// The raw base is `base` itself when raw, its `.base` when an overlay. Linking
/// up front lets a VMM that cannot create links (privilege drop,
/// `protected_hardlinks`) create the overlay `dest`.
pub fn link_base(base: &Path, dest: &Path) -> io::Result<()> {
	let root = if is_overlay(base)? {
		base_path(base)
	} else {
		base.to_owned()
	};
	let file = open_regular(&root)?;
	ensure_link(&root, &file, &base_path(dest)).map(drop)
}

/// Writes the guest-visible contents of the overlay `src` as the raw image
/// `dst`.
pub fn materialize(src: &Path, dst: &Path) -> io::Result<()> {
	Overlay::open(src, true)?.materialize(dst).map(drop)
}

fn verify_base(base: &File, footer: &Footer, path: &Path) -> io::Result<()> {
	let len = base.metadata()?.len();
	if len != footer.disk_size || fingerprint(base, len)? != footer.base_fingerprint {
		return Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("base {} does not match its overlay", path.display()),
		));
	}
	Ok(())
}

fn context(error: io::Error, what: String) -> io::Error {
	io::Error::new(error.kind(), format!("{what}: {error}"))
}

/// Opens an existing regular file read-only without following a symlink.
fn open_regular(path: &Path) -> io::Result<File> {
	let file = OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(path)
		.map_err(|e| context(e, format!("opening {} without following symlinks", path.display())))?;
	if !file.metadata()?.is_file() {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("{} must be a regular file", path.display()),
		));
	}
	Ok(file)
}

fn ensure_absent(path: &Path) -> io::Result<()> {
	match fs::symlink_metadata(path) {
		Ok(_) => Err(io::Error::new(
			io::ErrorKind::AlreadyExists,
			format!("{} already exists; refusing to overwrite", path.display()),
		)),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
		Err(e) => Err(context(e, format!("checking {}", path.display()))),
	}
}

/// Makes `link` a hard link of the open `target` (at `target_path`).
/// Returns whether a link was created; an existing link must already name
/// the same inode.
fn ensure_link(target_path: &Path, target: &File, link: &Path) -> io::Result<bool> {
	let want = target.metadata()?;
	let same = |meta: &fs::Metadata| meta.dev() == want.dev() && meta.ino() == want.ino();
	match fs::symlink_metadata(link) {
		Ok(meta) if meta.is_file() && same(&meta) => return Ok(false),
		Ok(_) => {
			return Err(io::Error::new(
				io::ErrorKind::AlreadyExists,
				format!("{} exists and is not a link of {}", link.display(), target_path.display()),
			));
		},
		Err(e) if e.kind() == io::ErrorKind::NotFound => {},
		Err(e) => return Err(context(e, format!("checking {}", link.display()))),
	}
	fs::hard_link(target_path, link).map_err(|e| {
		context(e, format!("linking base {} as {}", target_path.display(), link.display()))
	})?;
	if !same(&fs::symlink_metadata(link)?) {
		let _ = fs::remove_file(link);
		return Err(io::Error::other(format!(
			"base {} changed while linking it",
			target_path.display()
		)));
	}
	Ok(true)
}

/// Creates `path` exclusively, read-write, without following symlinks.
fn create_new(path: &Path) -> io::Result<File> {
	OpenOptions::new()
		.read(true)
		.write(true)
		.create_new(true)
		.custom_flags(libc::O_NOFOLLOW)
		.open(path)
		.map_err(|e| context(e, format!("creating {} exclusively", path.display())))
}

/// Creates `dest` as a copy of `src`: a filesystem clone when supported,
/// otherwise a copy of the allocated extents (holes stay holes). Returns the
/// new read-write file and whether it was cloned.
pub(crate) fn copy_new(src: &File, dest: &Path) -> io::Result<(File, bool)> {
	#[cfg(target_os = "macos")]
	if let macos::CloneOutcome::Cloned(file) = macos::clone_into_new(src, &src.metadata()?, dest)? {
		return Ok((file, true));
	}
	let out = create_new(dest)?;
	#[cfg(target_os = "linux")]
	if try_reflink(src, &out).is_none() {
		return Ok((out, true));
	}
	let mut source = src.try_clone()?;
	if let Err(error) = sparse_copy(&mut source, &out) {
		drop(out);
		let _ = fs::remove_file(dest);
		return Err(error);
	}
	Ok((out, false))
}

fn copy_range(src: &File, dst: &File, (start, end): (u64, u64), buf: &mut [u8]) -> io::Result<()> {
	let mut at = start;
	while at < end {
		let count = ((end - at) as usize).min(buf.len());
		src.read_exact_at(&mut buf[..count], at)?;
		dst.write_all_at(&buf[..count], at)?;
		at += count as u64;
	}
	Ok(())
}

/// `FICLONE` ioctl: `_IOW(0x94, 9, int)`. Reflinks one file's extents into
/// another (instant copy-on-write on btrfs/XFS).
#[cfg(target_os = "linux")]
const FICLONE: crate::os::libc_abi::IoctlRequest = 0x4004_9409;

/// Reflinks all of `src` into `dst`; `None` on success, the error otherwise.
#[cfg(target_os = "linux")]
pub(crate) fn try_reflink(src: &File, dst: &File) -> Option<io::Error> {
	use std::os::fd::AsRawFd;
	// SAFETY: both fds are valid open files; FICLONE clones src's extents into
	// dst.
	let ret = unsafe { libc::ioctl(dst.as_raw_fd(), FICLONE, src.as_raw_fd()) };
	if ret == 0 {
		None
	} else {
		Some(io::Error::last_os_error())
	}
}

/// Copy `src` into `dst` without materializing holes, then size `dst` to the
/// full logical length.
pub(crate) fn sparse_copy(src: &mut File, dst: &File) -> io::Result<()> {
	if sparse_copy_extents(src, dst)? {
		return Ok(());
	}
	sparse_copy_by_scanning(src, dst)
}

/// Copy only filesystem-allocated extents (`SEEK_DATA`/`SEEK_HOLE`), so the
/// cost is proportional to the data, not the logical size. `false` when the
/// filesystem cannot report extents.
pub(crate) fn sparse_copy_extents(src: &File, dst: &File) -> io::Result<bool> {
	let logical_len = src.metadata()?.len();
	let mut offset = 0u64;
	let mut copied_extent = false;
	let mut buf = vec![0u8; COPY_CHUNK];
	while offset < logical_len {
		let data = match seek_extent(src, offset, libc::SEEK_DATA) {
			Ok(Some(data)) => data,
			Ok(None) => break,
			Err(e) if !copied_extent && extent_seeking_unsupported(&e) => return Ok(false),
			Err(e) => return Err(e),
		};
		let hole = seek_extent(src, data, libc::SEEK_HOLE)?
			.unwrap_or(logical_len)
			.min(logical_len);
		copy_range(src, dst, (data, hole), &mut buf)?;
		copied_extent = true;
		offset = hole;
	}
	dst.set_len(logical_len)?;
	Ok(true)
}

fn seek_extent(file: &File, offset: u64, whence: libc::c_int) -> io::Result<Option<u64>> {
	use std::os::fd::AsRawFd;
	let offset = libc::off_t::try_from(offset)
		.map_err(|_| io::Error::other("sparse extent offset exceeds off_t"))?;
	// SAFETY: `file` owns a valid descriptor and lseek does not dereference
	// memory.
	let result = unsafe { libc::lseek(file.as_raw_fd(), offset, whence) };
	if result >= 0 {
		return Ok(Some(result as u64));
	}
	let error = io::Error::last_os_error();
	if error.raw_os_error() == Some(libc::ENXIO) {
		Ok(None)
	} else {
		Err(error)
	}
}

fn extent_seeking_unsupported(error: &io::Error) -> bool {
	matches!(error.raw_os_error(), Some(libc::EINVAL | libc::ENOTSUP))
}

/// Fallback for filesystems without extent reporting: skips all-zero 64 KiB
/// blocks. Correct for overlays too, since their bitmap (not holes) decides
/// which layer serves a cluster.
fn sparse_copy_by_scanning(src: &mut File, dst: &File) -> io::Result<()> {
	use std::io::Read;
	const BLOCK: usize = 64 * 1024;
	src.seek(SeekFrom::Start(0))?;
	dst.set_len(0)?;
	let mut buf = vec![0u8; BLOCK];
	let mut offset = 0u64;
	loop {
		let mut filled = 0usize;
		while filled < BLOCK {
			let n = src.read(&mut buf[filled..])?;
			if n == 0 {
				break;
			}
			filled += n;
		}
		if filled == 0 {
			break;
		}
		if !crate::memory::is_zero(&buf[..filled]) {
			dst.write_all_at(&buf[..filled], offset)?;
		}
		offset += filled as u64;
		if filled < BLOCK {
			break;
		}
	}
	dst.set_len(offset)?;
	Ok(())
}

/// APFS `fclonefileat` fast path for [`copy_new`] and raw overlay clones.
#[cfg(target_os = "macos")]
pub(crate) mod macos {
	use std::{
		ffi::CString,
		fs::{File, Metadata},
		io,
		os::{
			fd::{AsRawFd, FromRawFd, OwnedFd},
			unix::{ffi::OsStrExt, fs::MetadataExt},
		},
		path::Path,
	};

	/// Result of trying to clone the base into a new destination entry.
	pub(crate) enum CloneOutcome {
		/// The clone exists and was reopened and verified; nothing to copy.
		Cloned(File),
		/// The filesystem cannot clone (not APFS, cross-device); the
		/// destination was not created and the caller should copy instead.
		Unsupported(io::Error),
	}

	/// Clones the already-open, verified base `src` to the new entry `dest`.
	///
	/// `fclonefileat` cannot return a descriptor, so the created entry is
	/// reopened relative to the same directory descriptor with `O_NOFOLLOW`
	/// and verified: the fd and a no-follow `fstatat` must name the same
	/// dev/ino, on the directory's filesystem, as a single-link regular file
	/// owned by us with the base's length. A swapped entry is rejected
	/// rather than handed to the guest as its disk.
	pub(crate) fn clone_into_new(
		src: &File,
		src_meta: &Metadata,
		dest: &Path,
	) -> io::Result<CloneOutcome> {
		let parent = dest
			.parent()
			.filter(|parent| !parent.as_os_str().is_empty())
			.unwrap_or_else(|| Path::new("."));
		let name = dest.file_name().ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidInput, "overlay destination has no file name")
		})?;
		let parent_c = CString::new(parent.as_os_str().as_bytes())?;
		let name_c = CString::new(name.as_bytes())?;

		// SAFETY: `parent_c` is a NUL-terminated path; open returns a new fd or
		// -1.
		let raw_dir = unsafe {
			libc::open(parent_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC)
		};
		if raw_dir < 0 {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: `raw_dir` is a freshly opened descriptor owned by nobody else.
		let dir = unsafe { OwnedFd::from_raw_fd(raw_dir) };

		// SAFETY: both descriptors are open; `name_c` is NUL-terminated; flags 0
		// clones data and metadata and fails if `name` exists in any form.
		let rc = unsafe { libc::fclonefileat(src.as_raw_fd(), dir.as_raw_fd(), name_c.as_ptr(), 0) };
		if rc != 0 {
			let error = io::Error::last_os_error();
			return match error.raw_os_error() {
				Some(libc::ENOTSUP | libc::EXDEV | libc::ENOSYS) => {
					Ok(CloneOutcome::Unsupported(error))
				},
				Some(libc::EEXIST) => Err(io::Error::new(
					io::ErrorKind::AlreadyExists,
					format!(
						"overlay destination {} already exists; refusing to overwrite",
						dest.display()
					),
				)),
				_ => Err(error),
			};
		}

		// SAFETY: `libc::stat` is plain old data; all-zero is a valid value.
		let mut entry: libc::stat = unsafe { std::mem::zeroed() };
		// SAFETY: `dir` is open, `name_c` NUL-terminated, `entry` writable.
		if unsafe {
			libc::fstatat(dir.as_raw_fd(), name_c.as_ptr(), &raw mut entry, libc::AT_SYMLINK_NOFOLLOW)
		} != 0
		{
			return Err(io::Error::last_os_error());
		}
		// SAFETY: as above; O_NOFOLLOW refuses a symlink swapped into place.
		let raw_file = unsafe {
			libc::openat(
				dir.as_raw_fd(),
				name_c.as_ptr(),
				libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC,
			)
		};
		if raw_file < 0 {
			return Err(io::Error::last_os_error());
		}
		// SAFETY: `raw_file` is a freshly opened descriptor owned by nobody else.
		let file = File::from(unsafe { OwnedFd::from_raw_fd(raw_file) });
		let opened = file.metadata()?;
		let dir_meta = File::from(dir).metadata()?;
		// SAFETY: geteuid has no preconditions.
		let euid = unsafe { libc::geteuid() };
		let entry_dev = u64::try_from(entry.st_dev).unwrap_or(u64::MAX);
		if !opened.is_file()
			|| opened.dev() != entry_dev
			|| opened.ino() != entry.st_ino
			|| opened.dev() != dir_meta.dev()
			|| opened.nlink() != 1
			|| opened.uid() != euid
			|| opened.len() != src_meta.len()
		{
			return Err(io::Error::other(format!(
				"cloned overlay {} was replaced before it could be opened",
				dest.display()
			)));
		}
		Ok(CloneOutcome::Cloned(file))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn tempdir(tag: &str) -> PathBuf {
		static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
		let dir = std::env::temp_dir().join(format!(
			"vmon-overlay-{tag}-{}-{}",
			std::process::id(),
			NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
		));
		let _ = fs::remove_dir_all(&dir);
		fs::create_dir(&dir).expect("create temp dir");
		dir
	}

	/// A raw base whose every byte is `(offset % 251) as u8`, with a size that
	/// is not a multiple of the cluster size.
	fn patterned_base(dir: &Path) -> (PathBuf, Vec<u8>) {
		let bytes: Vec<u8> = (0..(5 * CLUSTER_SIZE + 1536))
			.map(|i| (i % 251) as u8)
			.collect();
		let path = dir.join("base.img");
		fs::write(&path, &bytes).expect("write base");
		(path, bytes)
	}

	fn read_all(overlay: &Overlay) -> Vec<u8> {
		let mut out = vec![0u8; overlay.disk_size() as usize];
		overlay.read_at(&mut out, 0).expect("read overlay");
		out
	}

	#[test]
	fn partial_and_full_writes_merge_with_base_and_survive_reopen() {
		let dir = tempdir("rw");
		let (base, mut expect) = patterned_base(&dir);
		let pristine = expect.clone();
		let dest = dir.join("disk.img");
		let mut overlay = Overlay::create(&base, &dest).expect("create");
		assert_eq!(read_all(&overlay), expect, "fresh overlay reads the base");

		// Sub-cluster write in the middle of cluster 1, a full cluster 3,
		// zeroes over part of cluster 4 into 5, and the unaligned disk tail.
		let writes: [(u64, Vec<u8>); 4] = [
			(CLUSTER_SIZE + 512, vec![0xaa; 1024]),
			(3 * CLUSTER_SIZE, vec![0xbb; CLUSTER_SIZE as usize]),
			(4 * CLUSTER_SIZE + 3584, vec![0; 1024]),
			(5 * CLUSTER_SIZE + 1024, vec![0xcc; 512]),
		];
		for (offset, data) in &writes {
			overlay.write_at(data, *offset).expect("write");
			expect[*offset as usize..*offset as usize + data.len()].copy_from_slice(data);
		}
		assert_eq!(read_all(&overlay), expect);
		let runs: Vec<_> = overlay
			.runs(0, overlay.disk_size())
			.map(|r| r.overlay)
			.collect();
		assert_eq!(runs, [false, true, false, true], "clusters 1 and 3..=5 are in the overlay");
		drop(overlay);

		assert_eq!(fs::read(&base).expect("reread base"), pristine, "base stays pristine");
		let reopened = Overlay::open(&dest, true).expect("reopen");
		assert_eq!(read_all(&reopened), expect, "bitmap persisted on drop");
		drop(reopened);

		let flat = dir.join("flat.img");
		materialize(&dest, &flat).expect("materialize");
		assert_eq!(fs::read(&flat).expect("read flat"), expect);
		let _ = fs::remove_dir_all(dir);
	}

	#[test]
	fn overlay_of_overlay_clones_onto_the_same_raw_base_and_isolates_writes() {
		let dir = tempdir("chain");
		let (base, pristine) = patterned_base(&dir);
		let parent = dir.join("parent.img");
		let mut first = Overlay::create(&base, &parent).expect("create parent");
		first.write_at(&[1; 4096], 0).expect("parent write");
		first.persist().expect("persist parent");

		let child = dir.join("child.img");
		let mut second = Overlay::create(&parent, &child).expect("create child");
		let mut expect = pristine;
		expect[..4096].fill(1);
		assert_eq!(read_all(&second), expect, "child starts from the parent's state");
		let ino = |path: &Path| fs::metadata(path).expect("stat").ino();
		assert_eq!(ino(&base_path(&child)), ino(&base), "child links the raw base");

		second.write_at(&[2; 4096], 4096).expect("child write");
		first
			.write_at(&[3; 4096], 8192)
			.expect("parent write after clone");
		let mut child_expect = expect.clone();
		child_expect[4096..8192].fill(2);
		assert_eq!(read_all(&second), child_expect);
		expect[8192..12288].fill(3);
		assert_eq!(read_all(&first), expect);

		drop((first, second));
		fs::remove_file(&base).expect("delete template base");
		assert_eq!(read_all(&Overlay::open(&child, true).expect("reopen child")), child_expect);
		let _ = fs::remove_dir_all(dir);
	}

	#[test]
	fn mismatched_or_missing_base_and_existing_destination_are_rejected() {
		let dir = tempdir("reject");
		let (base, _) = patterned_base(&dir);
		let dest = dir.join("disk.img");
		drop(Overlay::create(&base, &dest).expect("create"));
		assert_eq!(
			Overlay::create(&base, &dest).err().expect("exists").kind(),
			io::ErrorKind::AlreadyExists
		);

		let link = base_path(&dest);
		fs::remove_file(&link).expect("unlink base");
		fs::write(&link, b"other").expect("write impostor");
		assert_eq!(
			Overlay::open(&dest, true).err().expect("mismatch").kind(),
			io::ErrorKind::InvalidData
		);
		assert!(!is_overlay(&base).expect("probe raw"));
		assert!(is_overlay(&dest).expect("probe overlay"));
		let _ = fs::remove_dir_all(dir);
	}

	#[test]
	fn prelinked_base_is_accepted_and_runs_split_on_cluster_state() {
		let dir = tempdir("prelink");
		let (base, _) = patterned_base(&dir);
		let dest = dir.join("disk.img");
		link_base(&base, &dest).expect("prelink");
		let mut overlay = Overlay::create(&base, &dest).expect("create over prelinked base");
		overlay
			.write_at(&[9; 8192], 2 * CLUSTER_SIZE)
			.expect("write");
		let runs: Vec<_> = overlay.runs(512, 4 * CLUSTER_SIZE).collect();
		assert_eq!(runs, [
			Run { overlay: false, offset: 512, len: 2 * CLUSTER_SIZE - 512 },
			Run { overlay: true, offset: 2 * CLUSTER_SIZE, len: 2 * CLUSTER_SIZE },
			Run { overlay: false, offset: 4 * CLUSTER_SIZE, len: 512 },
		]);
		assert!(overlay.needs_copy_up(512, 1024));
		assert!(!overlay.needs_copy_up(2 * CLUSTER_SIZE + 512, 1024));
		assert!(!overlay.needs_copy_up(0, CLUSTER_SIZE));
		let _ = fs::remove_dir_all(dir);
	}

	#[test]
	fn bitmap_find_and_set_cross_word_boundaries() {
		let mut bitmap = Bitmap::new(300);
		bitmap.set(60, 130);
		assert_eq!(bitmap.find(0, 300, true), 60);
		assert_eq!(bitmap.find(60, 300, false), 130);
		assert_eq!(bitmap.find(130, 300, true), 300);
		assert_eq!(bitmap.dirty, Some((0, 3)));
		assert!(bitmap.get(64) && bitmap.get(129) && !bitmap.get(59) && !bitmap.get(130));
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn sparse_extent_copy_preserves_large_logical_holes_and_data() {
		let dir = tempdir("sparse");
		let base_path = dir.join("sparse-base.img");
		let dest_path = dir.join("sparse-overlay.img");
		let base = create_new(&base_path).expect("create sparse base");
		let logical_len = 8u64 << 30;
		base.set_len(logical_len).expect("size sparse base");
		base
			.write_all_at(b"front", 4096)
			.expect("write front extent");
		base
			.write_all_at(b"back", logical_len - 4096)
			.expect("write back extent");
		let dest = create_new(&dest_path).expect("create sparse destination");

		assert!(
			sparse_copy_extents(&base, &dest).expect("copy sparse extents"),
			"test filesystem must support SEEK_DATA/SEEK_HOLE"
		);
		assert_eq!(dest.metadata().expect("stat destination").len(), logical_len);
		let mut front = [0u8; 5];
		let mut back = [0u8; 4];
		dest
			.read_exact_at(&mut front, 4096)
			.expect("read front extent");
		dest
			.read_exact_at(&mut back, logical_len - 4096)
			.expect("read back extent");
		assert_eq!(&front, b"front");
		assert_eq!(&back, b"back");
		let _ = fs::remove_dir_all(dir);
	}
}
