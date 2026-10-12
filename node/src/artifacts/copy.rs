//! Copy-on-write file cloning for rootfs images: APFS `clonefile` on macOS,
//! `FICLONE` reflinks on Linux, and a hole-preserving copy elsewhere.
//! Used for the build step cache and `fromTemplate` bases.
//!
//! Sandbox disks and snapshot-build rootfs images may instead be VMM block
//! overlays ([`vmm::disk_overlay`]): the image plus its hard-linked raw base
//! `<image>.base`. The `*_disk` helpers treat that pair as one disk.

use std::{
	fs::{self, File},
	io::{self, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
};

pub use vmm::disk_overlay::base_path as disk_base;

/// Whether the disk image at `path` is a block overlay.
pub fn is_overlay_disk(path: &Path) -> io::Result<bool> {
	vmm::disk_overlay::is_overlay(path)
}

/// Copies the quiescent disk `src` to the new `dst`: an overlay is cloned in
/// O(written data) onto the same raw base (reflink when possible), a raw
/// image via [`clone_file`]. The copy is durable on return.
pub fn clone_disk(src: &Path, dst: &Path) -> io::Result<()> {
	if !is_overlay_disk(src)? {
		return clone_file(src, dst);
	}
	vmm::disk_overlay::clone_overlay(src, dst)?;
	let synced = File::open(dst).and_then(|file| file.sync_all());
	if synced.is_err() {
		let _ = remove_disk(dst);
	}
	synced
}

/// Renames the disk `src` (and its overlay base, if any) to `dst`.
pub fn move_disk(src: &Path, dst: &Path) -> io::Result<()> {
	let (src_base, dst_base) = (disk_base(src), disk_base(dst));
	let has_base = fs::symlink_metadata(&src_base).is_ok();
	if has_base {
		fs::rename(&src_base, &dst_base)?;
	}
	let moved = fs::rename(src, dst);
	if moved.is_err() && has_base {
		let _ = fs::rename(&dst_base, &src_base);
	}
	moved
}

/// Deletes the disk `path` and its overlay base link; missing files are
/// fine. Returns the first other error.
pub fn remove_disk(path: &Path) -> io::Result<()> {
	let mut result = Ok(());
	for file in [path.to_owned(), disk_base(path)] {
		match fs::remove_file(&file) {
			Err(e) if e.kind() != io::ErrorKind::NotFound && result.is_ok() => result = Err(e),
			_ => {},
		}
	}
	result
}

/// Hard-links the raw base under the overlay `dest` the VMM will create from
/// `base`.
///
/// Matches `--disk-overlay-of base --rootfs dest`. The node links it because a
/// VMM that dropped privileges may not (`fs.protected_hardlinks`).
pub fn link_disk_base(base: &Path, dest: &Path) -> io::Result<PathBuf> {
	vmm::disk_overlay::link_base(base, dest)?;
	Ok(disk_base(dest))
}

/// Writes the guest-visible contents of the disk `src` as the new raw image
/// `dst` (for host tools such as `e2fsck`/`resize2fs`).
pub fn materialize_disk(src: &Path, dst: &Path) -> io::Result<()> {
	if is_overlay_disk(src)? {
		vmm::disk_overlay::materialize(src, dst)
	} else {
		clone_file(src, dst)
	}
}

/// Clones `src` to the new file `dst` (which must not exist), sharing blocks
/// when the filesystem supports it and preserving sparseness otherwise.
pub fn clone_file(src: &Path, dst: &Path) -> io::Result<()> {
	if fs::symlink_metadata(dst).is_ok() {
		return Err(io::Error::new(
			io::ErrorKind::AlreadyExists,
			format!("{} already exists", dst.display()),
		));
	}
	if reflink(src, dst)? {
		return Ok(());
	}
	let result = sparse_copy(src, dst);
	if result.is_err() {
		let _ = fs::remove_file(dst);
	}
	result
}

/// Clones `src` to `dst` only when the filesystem can share blocks; returns
/// `false` (creating nothing) otherwise. Used where a full copy is too costly.
pub fn try_reflink(src: &Path, dst: &Path) -> io::Result<bool> {
	if fs::symlink_metadata(dst).is_ok() {
		return Err(io::Error::new(
			io::ErrorKind::AlreadyExists,
			format!("{} already exists", dst.display()),
		));
	}
	reflink(src, dst)
}
#[cfg(target_os = "macos")]
fn reflink(src: &Path, dst: &Path) -> io::Result<bool> {
	use std::{ffi::CString, os::unix::ffi::OsStrExt};
	let src_c = CString::new(src.as_os_str().as_bytes())?;
	let dst_c = CString::new(dst.as_os_str().as_bytes())?;
	// SAFETY: both arguments are valid NUL-terminated paths that outlive the
	// call.
	let rc = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
	if rc == 0 {
		return Ok(true);
	}
	let error = io::Error::last_os_error();
	match error.raw_os_error() {
		Some(libc::ENOTSUP | libc::EXDEV) => Ok(false),
		_ => Err(error),
	}
}

#[cfg(target_os = "linux")]
fn reflink(src: &Path, dst: &Path) -> io::Result<bool> {
	use std::os::fd::AsRawFd;
	/// `_IOW(0x94, 9, int)`.
	const FICLONE: libc::c_ulong = 0x4004_9409;
	let source = File::open(src)?;
	let dest = fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(dst)?;
	// SAFETY: both descriptors are open for the duration of the call; FICLONE
	// takes the source descriptor as its integer argument.
	let rc = unsafe { libc::ioctl(dest.as_raw_fd(), FICLONE as _, source.as_raw_fd()) };
	if rc == 0 {
		return Ok(true);
	}
	drop(dest);
	fs::remove_file(dst)?;
	Ok(false)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn reflink(_src: &Path, _dst: &Path) -> io::Result<bool> {
	Ok(false)
}

/// Copies the data extents of `src` (holes stay holes; cost is O(data), not
/// O(apparent size)), skipping all-zero 1 MiB blocks within them.
fn sparse_copy(src: &Path, dst: &Path) -> io::Result<()> {
	const BLOCK: usize = 1 << 20;
	let mut source = File::open(src)?;
	let len = source.metadata()?.len();
	let mut dest = fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(dst)?;
	let mut buf = vec![0u8; BLOCK];
	for [start, extent_len] in super::sparse::data_extents(&source, len)? {
		source.seek(SeekFrom::Start(start))?;
		let mut offset = start;
		let end = start + extent_len;
		while offset < end {
			let want = usize::try_from(end - offset).unwrap_or(BLOCK).min(BLOCK);
			source.read_exact(&mut buf[..want])?;
			if buf[..want].iter().any(|b| *b != 0) {
				dest.seek(SeekFrom::Start(offset))?;
				dest.write_all(&buf[..want])?;
			}
			offset += want as u64;
		}
	}
	dest.set_len(len)?;
	dest.sync_all()?;
	if let Ok(meta) = fs::metadata(src) {
		let _ = fs::set_permissions(dst, meta.permissions());
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn clone_preserves_content_and_length_and_refuses_existing_dest() {
		let dir = tempfile::tempdir().unwrap();
		let src = dir.path().join("src");
		let mut file = File::create(&src).unwrap();
		file.write_all(b"head").unwrap();
		file.seek(SeekFrom::Start(3 << 20)).unwrap();
		file.write_all(b"tail").unwrap();
		drop(file);
		let dst = dir.path().join("dst");
		clone_file(&src, &dst).unwrap();
		assert_eq!(fs::read(&src).unwrap(), fs::read(&dst).unwrap());
		assert_eq!(clone_file(&src, &dst).unwrap_err().kind(), io::ErrorKind::AlreadyExists);
	}

	/// Overlay disks travel with their base link: clone, move, materialize,
	/// and remove all operate on the pair, and a clone survives deleting the
	/// template base and the source.
	#[test]
	fn overlay_disk_pair_is_cloned_moved_materialized_and_removed_together() {
		let dir = tempfile::tempdir().unwrap();
		let template = dir.path().join("rootfs.ext4");
		fs::write(&template, vec![7u8; 64 << 10]).unwrap();
		let live = dir.path().join("rootfs.img");
		link_disk_base(&template, &live).unwrap();
		let mut overlay = vmm::disk_overlay::Overlay::create(&template, &live).unwrap();
		overlay.write_at(&[9u8; 4096], 8192).unwrap();
		drop(overlay);
		assert!(is_overlay_disk(&live).unwrap() && !is_overlay_disk(&template).unwrap());

		let paused = dir.path().join("paused.img");
		move_disk(&live, &paused).unwrap();
		assert!(!live.exists() && !disk_base(&live).exists() && disk_base(&paused).is_file());
		let copy = dir.path().join("build.ext4");
		clone_disk(&paused, &copy).unwrap();
		remove_disk(&paused).unwrap();
		fs::remove_file(&template).unwrap();
		assert!(!paused.exists() && !disk_base(&paused).exists());

		let flat = dir.path().join("flat.ext4");
		materialize_disk(&copy, &flat).unwrap();
		let mut expect = vec![7u8; 64 << 10];
		expect[8192..12288].fill(9);
		assert_eq!(fs::read(&flat).unwrap(), expect);
		remove_disk(&copy).unwrap();
		assert!(!disk_base(&copy).exists());
	}

	#[test]
	fn sparse_copy_keeps_trailing_hole_length() {
		let dir = tempfile::tempdir().unwrap();
		let src = dir.path().join("src");
		let file = File::create(&src).unwrap();
		file.set_len(5 << 20).unwrap();
		drop(file);
		let dst = dir.path().join("dst");
		sparse_copy(&src, &dst).unwrap();
		assert_eq!(fs::metadata(&dst).unwrap().len(), 5 << 20);
		assert!(fs::read(&dst).unwrap().iter().all(|b| *b == 0));
	}
}
