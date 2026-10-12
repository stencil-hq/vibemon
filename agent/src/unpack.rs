//! OCI/Docker layer application for the GC4 `oci_unpack` op (template builds).
//!
//! Layers (gzip, zstd, or plain tar; sniffed by magic) are applied in order
//! onto `dest` as root inside the unpack VM, so ownership, setuid bits,
//! capabilities, and device nodes come out exactly as in the image. Every
//! path is resolved with `openat2(RESOLVE_IN_ROOT)` below `dest`, so hostile
//! symlinks in a layer cannot escape it.

use std::{
	collections::HashSet,
	ffi::{CStr, CString, OsStr},
	fs::File,
	io::{self, BufRead, BufReader, Read},
	os::{
		fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
		unix::ffi::OsStrExt,
	},
	path::{Component, Path, PathBuf},
};

use tar::EntryType;

/// Totals reported in the `oci_unpack` RESP.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct UnpackStats {
	/// Regular-file payload bytes written.
	pub bytes:     u64,
	/// Entries materialized (files, dirs, links, devices).
	pub files:     u64,
	/// Whiteouts applied (regular + opaque).
	pub whiteouts: u64,
}

/// Apply `layers` in order onto `dest`; called by the GC4 `oci_unpack` op.
pub fn apply_layers(layers: &[PathBuf], dest: &Path) -> io::Result<UnpackStats> {
	std::fs::create_dir_all(dest)?;
	let root = open_dir(libc::AT_FDCWD, &cstr(dest.as_os_str())?, 0)?;
	let mut stats = UnpackStats::default();
	for layer in layers {
		let file =
			File::open(layer).map_err(|e| annotate(e, &format!("open layer {}", layer.display())))?;
		apply_layer(&root, open_decompressed(file)?, &mut stats)
			.map_err(|e| annotate(e, &format!("layer {}", layer.display())))?;
	}
	Ok(stats)
}

/// Wrap a layer blob in the decompressor its magic bytes call for.
fn open_decompressed(file: File) -> io::Result<Box<dyn Read>> {
	let mut reader = BufReader::with_capacity(256 * 1024, file);
	let magic = reader.fill_buf()?;
	if magic.starts_with(&[0x1f, 0x8b]) {
		Ok(Box::new(flate2::bufread::MultiGzDecoder::new(reader)))
	} else if magic.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
		let decoder = ruzstd::decoding::StreamingDecoder::new(reader)
			.map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("zstd: {e}")))?;
		Ok(Box::new(decoder))
	} else {
		Ok(Box::new(reader))
	}
}

/// Apply one decompressed tar stream onto the directory `root`.
pub fn apply_layer(root: &OwnedFd, reader: impl Read, stats: &mut UnpackStats) -> io::Result<()> {
	let mut archive = tar::Archive::new(reader);
	let mut created: HashSet<PathBuf> = HashSet::new();
	let mut opaque: Vec<PathBuf> = Vec::new();
	let mut dir_times: Vec<(PathBuf, i64)> = Vec::new();
	for entry in archive.entries()? {
		let mut entry = entry?;
		let Some(rel) = normalize(&entry.path()?) else {
			continue;
		};
		let name = rel
			.file_name()
			.map(OsStr::as_bytes)
			.unwrap_or_default()
			.to_vec();
		let parent_rel = rel.parent().map(Path::to_path_buf).unwrap_or_default();

		if name == b".wh..wh..opq" {
			opaque.push(parent_rel);
			stats.whiteouts += 1;
			continue;
		}
		if let Some(hidden) = name.strip_prefix(b".wh.") {
			let parent = ensure_parent(root, &rel)?;
			remove_any(&parent, &cstr(OsStr::from_bytes(hidden))?)?;
			stats.whiteouts += 1;
			continue;
		}

		let meta = EntryMeta::read(&mut entry)?;
		let parent = ensure_parent(root, &rel)?;
		let leaf = cstr(OsStr::from_bytes(&name))?;
		let kind = entry.header().entry_type();
		let existing = lstat_at(&parent, &leaf)?;
		let keep_dir = kind == EntryType::Directory && existing.is_some_and(is_dir);
		if existing.is_some() && !keep_dir {
			remove_any(&parent, &leaf)?;
		}

		match kind {
			EntryType::Directory => {
				if !keep_dir {
					check(unsafe_mkdirat(&parent, &leaf, 0o700))?;
				}
				dir_times.push((rel.clone(), meta.mtime));
			},
			EntryType::Regular | EntryType::Continuous | EntryType::GNUSparse => {
				// SAFETY: `parent` is an open directory fd and `leaf` a valid C
				// string.
				let fd = unsafe {
					libc::openat(
						parent.as_raw_fd(),
						leaf.as_ptr(),
						libc::O_WRONLY
							| libc::O_CREAT
							| libc::O_EXCL | libc::O_NOFOLLOW
							| libc::O_CLOEXEC,
						0o600,
					)
				};
				check(fd)?;
				// SAFETY: `fd` was just returned by `openat` and is owned here.
				let mut file = unsafe { File::from_raw_fd(fd) };
				stats.bytes += io::copy(&mut entry, &mut file)?;
			},
			EntryType::Symlink => {
				let target = entry
					.link_name()?
					.ok_or_else(|| invalid("symlink without target"))?;
				let target = cstr(target.as_os_str())?;
				// SAFETY: both C strings are valid; `parent` is an open directory.
				check(unsafe { libc::symlinkat(target.as_ptr(), parent.as_raw_fd(), leaf.as_ptr()) })?;
			},
			EntryType::Link => {
				let target = entry
					.link_name()?
					.ok_or_else(|| invalid("hardlink without target"))?;
				let target = normalize(&target).ok_or_else(|| invalid("hardlink to root"))?;
				let target_parent = ensure_parent(root, &target)?;
				let target_leaf = cstr(target.file_name().unwrap_or_default())?;
				// SAFETY: both directory fds are open and names are valid C
				// strings.
				check(unsafe {
					libc::linkat(
						target_parent.as_raw_fd(),
						target_leaf.as_ptr(),
						parent.as_raw_fd(),
						leaf.as_ptr(),
						0,
					)
				})?;
				mark_created(&mut created, &rel);
				stats.files += 1;
				// A hardlink shares the target inode's metadata.
				continue;
			},
			EntryType::Char | EntryType::Block | EntryType::Fifo => {
				let file_type = match kind {
					EntryType::Char => libc::S_IFCHR,
					EntryType::Block => libc::S_IFBLK,
					_ => libc::S_IFIFO,
				};
				let major = entry.header().device_major()?.unwrap_or(0);
				let minor = entry.header().device_minor()?.unwrap_or(0);
				let dev = libc::makedev(major, minor);
				// SAFETY: `parent` is an open directory and `leaf` a valid C
				// string.
				check(unsafe {
					libc::mknodat(parent.as_raw_fd(), leaf.as_ptr(), file_type | 0o600, dev)
				})?;
			},
			// PAX/GNU metadata records are consumed by the tar reader itself.
			_ => continue,
		}
		apply_meta(&parent, &leaf, &meta, kind == EntryType::Symlink)?;
		mark_created(&mut created, &rel);
		stats.files += 1;
	}

	for dir in opaque {
		clear_lower(root, &dir, &created)?;
	}
	for (dir, mtime) in dir_times {
		let parent = ensure_parent(root, &dir)?;
		let leaf = cstr(dir.file_name().unwrap_or_default())?;
		set_mtime(&parent, &leaf, mtime)?;
	}
	Ok(())
}

/// Record `rel` and its ancestors as produced by the current layer.
fn mark_created(created: &mut HashSet<PathBuf>, rel: &Path) {
	for ancestor in rel.ancestors().filter(|a| !a.as_os_str().is_empty()) {
		if !created.insert(ancestor.to_path_buf()) {
			break;
		}
	}
}

/// Ownership/mode/time/xattr fields of one tar entry (PAX overrides applied).
struct EntryMeta {
	uid:    u32,
	gid:    u32,
	mode:   u32,
	mtime:  i64,
	xattrs: Vec<(CString, Vec<u8>)>,
}

impl EntryMeta {
	fn read<R: Read>(entry: &mut tar::Entry<'_, R>) -> io::Result<Self> {
		let header = entry.header();
		let mut meta = Self {
			uid:    u32::try_from(header.uid()?).map_err(|_| invalid("uid out of range"))?,
			gid:    u32::try_from(header.gid()?).map_err(|_| invalid("gid out of range"))?,
			mode:   header.mode()? & 0o7777,
			mtime:  i64::try_from(header.mtime()?).unwrap_or(i64::MAX),
			xattrs: Vec::new(),
		};
		if let Some(extensions) = entry.pax_extensions()? {
			for extension in extensions {
				let extension = extension?;
				let Ok(key) = extension.key() else {
					continue;
				};
				let value = extension.value_bytes();
				let number = || {
					std::str::from_utf8(value)
						.ok()
						.and_then(|v| v.split('.').next()?.parse::<i64>().ok())
				};
				match key {
					"uid" => {
						meta.uid = number()
							.and_then(|v| u32::try_from(v).ok())
							.ok_or_else(|| invalid("pax uid"))?;
					},
					"gid" => {
						meta.gid = number()
							.and_then(|v| u32::try_from(v).ok())
							.ok_or_else(|| invalid("pax gid"))?;
					},
					"mtime" => meta.mtime = number().ok_or_else(|| invalid("pax mtime"))?,
					_ => {
						if let Some(name) = key.strip_prefix("SCHILY.xattr.") {
							meta.xattrs.push((cstr(OsStr::new(name))?, value.to_vec()));
						}
					},
				}
			}
		}
		Ok(meta)
	}
}

/// chown, then chmod (chown clears setuid/setgid), then xattrs (chown clears
/// `security.capability`), then mtime.
fn apply_meta(parent: &OwnedFd, leaf: &CStr, meta: &EntryMeta, symlink: bool) -> io::Result<()> {
	let dirfd = parent.as_raw_fd();
	// SAFETY: open directory fd and valid C string; AT_SYMLINK_NOFOLLOW never
	// follows `leaf`.
	check(unsafe {
		libc::fchownat(dirfd, leaf.as_ptr(), meta.uid, meta.gid, libc::AT_SYMLINK_NOFOLLOW)
	})?;
	if !symlink {
		// SAFETY: `leaf` is a just-created non-symlink entry in `parent`.
		check(unsafe { libc::fchmodat(dirfd, leaf.as_ptr(), meta.mode, 0) })?;
	}
	if !meta.xattrs.is_empty() {
		let mut path = format!("/proc/self/fd/{dirfd}/").into_bytes();
		path.extend_from_slice(leaf.to_bytes());
		let path = CString::new(path).map_err(|_| invalid("nul in path"))?;
		for (name, value) in &meta.xattrs {
			// SAFETY: path/name are valid C strings; value outlives the call.
			let rc = unsafe {
				libc::lsetxattr(path.as_ptr(), name.as_ptr(), value.as_ptr().cast(), value.len(), 0)
			};
			if rc != 0 {
				let err = io::Error::last_os_error();
				return Err(annotate(err, &format!("setxattr {}", name.to_string_lossy())));
			}
		}
	}
	set_mtime(parent, leaf, meta.mtime)
}

fn set_mtime(parent: &OwnedFd, leaf: &CStr, mtime: i64) -> io::Result<()> {
	let time = libc::timespec { tv_sec: mtime, tv_nsec: 0 };
	let times = [time, time];
	// SAFETY: `times` holds two initialized timespecs; `leaf` is valid.
	check(unsafe {
		libc::utimensat(parent.as_raw_fd(), leaf.as_ptr(), times.as_ptr(), libc::AT_SYMLINK_NOFOLLOW)
	})
}

/// Delete everything below `dir` that this layer did not create (opaque
/// whiteout: hide all lower-layer contents, including inside directories the
/// layer re-declares).
fn clear_lower(root: &OwnedFd, dir: &Path, created: &HashSet<PathBuf>) -> io::Result<()> {
	let handle = if dir.as_os_str().is_empty() {
		dup(root)?
	} else {
		match open_dir(root.as_raw_fd(), &cstr(dir.as_os_str())?, libc::RESOLVE_IN_ROOT) {
			Ok(fd) => fd,
			Err(err) if err.raw_os_error() == Some(libc::ENOENT) => return Ok(()),
			Err(err) => return Err(err),
		}
	};
	let listing = std::fs::read_dir(format!("/proc/self/fd/{}", handle.as_raw_fd()))?;
	for child in listing {
		let child = child?.file_name();
		let path = dir.join(&child);
		let leaf = cstr(&child)?;
		if !created.contains(&path) {
			remove_any(&handle, &leaf)?;
		} else if lstat_at(&handle, &leaf)?.is_some_and(is_dir) {
			clear_lower(root, &path, created)?;
		}
	}
	Ok(())
}

/// Remove `leaf` in `parent` whatever it is (directories recursively,
/// symlinks never followed); a missing entry is fine.
fn remove_any(parent: &OwnedFd, leaf: &CStr) -> io::Result<()> {
	let Some(mode) = lstat_at(parent, leaf)? else {
		return Ok(());
	};
	if is_dir(mode) {
		let mut path = format!("/proc/self/fd/{}/", parent.as_raw_fd()).into_bytes();
		path.extend_from_slice(leaf.to_bytes());
		std::fs::remove_dir_all(OsStr::from_bytes(&path))
	} else {
		// SAFETY: open directory fd and valid C string.
		check(unsafe { libc::unlinkat(parent.as_raw_fd(), leaf.as_ptr(), 0) })
	}
}

/// Open (creating 0755 root-owned dirs as needed) the parent directory of
/// `rel` under `root`.
fn ensure_parent(root: &OwnedFd, rel: &Path) -> io::Result<OwnedFd> {
	let Some(parent) = rel.parent().filter(|p| !p.as_os_str().is_empty()) else {
		return dup(root);
	};
	match open_dir(root.as_raw_fd(), &cstr(parent.as_os_str())?, libc::RESOLVE_IN_ROOT) {
		Ok(fd) => return Ok(fd),
		Err(err) if err.raw_os_error() != Some(libc::ENOENT) => return Err(err),
		Err(_) => {},
	}
	let mut current = dup(root)?;
	let mut prefix = PathBuf::new();
	for component in parent.components() {
		prefix.push(component);
		let name = cstr(component.as_os_str())?;
		let rc = unsafe_mkdirat(&current, &name, 0o755);
		if rc != 0 {
			let err = io::Error::last_os_error();
			if err.raw_os_error() != Some(libc::EEXIST) {
				return Err(err);
			}
		}
		// Re-resolve from `root` so symlinked components stay rooted at dest.
		current = open_dir(root.as_raw_fd(), &cstr(prefix.as_os_str())?, libc::RESOLVE_IN_ROOT)?;
	}
	Ok(current)
}

fn open_dir(dirfd: RawFd, path: &CStr, resolve: u64) -> io::Result<OwnedFd> {
	// SAFETY: an all-zero `open_how` is valid; fields are set below.
	let mut how: libc::open_how = unsafe { std::mem::zeroed() };
	how.flags = (libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_RDONLY) as u64;
	how.resolve = resolve;
	// SAFETY: `how` is initialized and sized correctly; `path` is a valid C
	// string.
	let fd = unsafe {
		libc::syscall(
			libc::SYS_openat2,
			dirfd,
			path.as_ptr(),
			&raw const how,
			std::mem::size_of::<libc::open_how>(),
		)
	};
	if fd < 0 {
		return Err(io::Error::last_os_error());
	}
	// SAFETY: openat2 returned a fresh descriptor we now own.
	Ok(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
}

fn dup(fd: &OwnedFd) -> io::Result<OwnedFd> {
	fd.try_clone()
}

fn lstat_at(parent: &OwnedFd, leaf: &CStr) -> io::Result<Option<u32>> {
	// SAFETY: all-zero `stat` is a valid output buffer.
	let mut st: libc::stat = unsafe { std::mem::zeroed() };
	// SAFETY: valid fd, C string, and writable `st`.
	let rc = unsafe {
		libc::fstatat(parent.as_raw_fd(), leaf.as_ptr(), &mut st, libc::AT_SYMLINK_NOFOLLOW)
	};
	if rc == 0 {
		return Ok(Some(st.st_mode));
	}
	let err = io::Error::last_os_error();
	if err.raw_os_error() == Some(libc::ENOENT) {
		Ok(None)
	} else {
		Err(err)
	}
}

const fn is_dir(mode: u32) -> bool {
	mode & libc::S_IFMT == libc::S_IFDIR
}

fn unsafe_mkdirat(parent: &OwnedFd, leaf: &CStr, mode: u32) -> i32 {
	// SAFETY: open directory fd and valid C string.
	unsafe { libc::mkdirat(parent.as_raw_fd(), leaf.as_ptr(), mode) }
}

/// Normalize a tar path to a relative path without `.`/`..`/root parts;
/// `None` for the archive root itself.
fn normalize(path: &Path) -> Option<PathBuf> {
	let mut out = PathBuf::new();
	for component in path.components() {
		match component {
			Component::Normal(part) => out.push(part),
			Component::ParentDir => {
				out.pop();
			},
			Component::RootDir | Component::CurDir | Component::Prefix(_) => {},
		}
	}
	(!out.as_os_str().is_empty()).then_some(out)
}

fn cstr(value: &OsStr) -> io::Result<CString> {
	CString::new(value.as_bytes()).map_err(|_| invalid("nul byte in path"))
}

fn check(rc: i32) -> io::Result<()> {
	if rc < 0 {
		Err(io::Error::last_os_error())
	} else {
		Ok(())
	}
}

fn invalid(msg: &str) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidData, msg.to_owned())
}

fn annotate(err: io::Error, context: &str) -> io::Error {
	io::Error::new(err.kind(), format!("{context}: {err}"))
}

#[cfg(test)]
mod tests {
	use std::{
		fs,
		os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt},
	};

	use tar::{Builder, Header};

	use super::*;

	struct Layer(Builder<Vec<u8>>);

	impl Layer {
		fn new() -> Self {
			Self(Builder::new(Vec::new()))
		}

		fn header(kind: EntryType, mode: u32, uid: u64) -> Header {
			let mut header = Header::new_gnu();
			header.set_entry_type(kind);
			header.set_mode(mode);
			header.set_uid(uid);
			header.set_gid(uid + 1);
			header.set_mtime(1_000_000);
			header.set_size(0);
			header
		}

		fn dir(mut self, path: &str, mode: u32) -> Self {
			let mut h = Self::header(EntryType::Directory, mode, 0);
			self.0.append_data(&mut h, path, io::empty()).unwrap();
			self
		}

		fn file(mut self, path: &str, body: &[u8], mode: u32, uid: u64) -> Self {
			let mut h = Self::header(EntryType::Regular, mode, uid);
			h.set_size(body.len() as u64);
			self.0.append_data(&mut h, path, body).unwrap();
			self
		}

		fn link(mut self, kind: EntryType, path: &str, target: &str) -> Self {
			let mut h = Self::header(kind, 0o777, 0);
			self.0.append_link(&mut h, path, target).unwrap();
			self
		}

		fn special(mut self, kind: EntryType, path: &str, major: u32, minor: u32) -> Self {
			let mut h = Self::header(kind, 0o640, 0);
			h.set_device_major(major).unwrap();
			h.set_device_minor(minor).unwrap();
			self.0.append_data(&mut h, path, io::empty()).unwrap();
			self
		}

		fn pax_file(mut self, path: &str, pax: &[(&str, &[u8])], mode: u32) -> Self {
			self.0.append_pax_extensions(pax.iter().copied()).unwrap();
			self.file_inner(path, mode)
		}

		fn file_inner(mut self, path: &str, mode: u32) -> Self {
			let mut h = Self::header(EntryType::Regular, mode, 0);
			self.0.append_data(&mut h, path, io::empty()).unwrap();
			self
		}

		fn bytes(self) -> Vec<u8> {
			self.0.into_inner().unwrap()
		}
	}

	fn tmp(name: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!("vmon-unpack-{name}-{}", std::process::id()));
		let _ = fs::remove_dir_all(&dir);
		fs::create_dir_all(&dir).unwrap();
		dir
	}

	fn write_layer(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
		let path = dir.join(name);
		fs::write(&path, bytes).unwrap();
		path
	}

	#[test]
	fn applies_ownership_modes_links_devices_and_whiteouts() {
		let work = tmp("full");
		let dest = work.join("root");
		let lower = Layer::new()
			.dir("etc/", 0o755)
			.file("etc/keep", b"keep", 0o644, 0)
			.file("etc/gone", b"x", 0o644, 0)
			.dir("srv/", 0o755)
			.dir("opt/", 0o755)
			.file("opt/old", b"old", 0o644, 0)
			.file("opt/sub/lowerfile", b"l", 0o644, 0)
			.file("bin/su", b"su", 0o4755, 0)
			.file("home/u/f", b"data", 0o640, 1000)
			.bytes();
		let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
		io::Write::write_all(&mut gz, &lower).unwrap();
		let lower_path = write_layer(&work, "l1.tar.gz", &gz.finish().unwrap());

		let upper = Layer::new()
			.file("etc/.wh.gone", b"", 0o644, 0)
			.file("opt/new", b"new", 0o600, 0)
			.file("opt/sub/newfile", b"n", 0o644, 0)
			.file("opt/.wh..wh..opq", b"", 0o644, 0)
			.link(EntryType::Link, "bin/su2", "bin/su")
			.link(EntryType::Symlink, "etc/link", "/etc/keep")
			.special(EntryType::Char, "dev/null", 1, 3)
			.special(EntryType::Fifo, "run/fifo", 0, 0)
			.bytes();
		let upper_path = write_layer(&work, "l2.tar", &upper);

		let stats = apply_layers(&[lower_path, upper_path], &dest).unwrap();
		assert_eq!(stats.whiteouts, 2);
		assert_eq!(fs::read(dest.join("etc/keep")).unwrap(), b"keep");
		assert!(!dest.join("etc/gone").exists());
		assert!(!dest.join("opt/old").exists(), "opaque dir must hide lower contents");
		assert_eq!(
			fs::read(dest.join("opt/new")).unwrap(),
			b"new",
			"same-layer entries survive opaque"
		);
		assert!(dest.join("opt/sub/newfile").exists());
		assert!(
			!dest.join("opt/sub/lowerfile").exists(),
			"opaque must also hide lower files in re-declared dirs"
		);

		let su = fs::metadata(dest.join("bin/su")).unwrap();
		assert_eq!(su.permissions().mode() & 0o7777, 0o4755);
		assert_eq!(su.ino(), fs::metadata(dest.join("bin/su2")).unwrap().ino());
		let home = fs::metadata(dest.join("home/u/f")).unwrap();
		assert_eq!((home.uid(), home.gid(), home.mode() & 0o777), (1000, 1001, 0o640));
		assert_eq!(home.mtime(), 1_000_000);
		assert_eq!(fs::read_link(dest.join("etc/link")).unwrap(), Path::new("/etc/keep"));
		let null = fs::symlink_metadata(dest.join("dev/null")).unwrap();
		assert!(null.file_type().is_char_device());
		assert_eq!(null.rdev(), libc::makedev(1, 3));
		assert!(
			fs::symlink_metadata(dest.join("run/fifo"))
				.unwrap()
				.file_type()
				.is_fifo()
		);
		// Directory mtimes are restored after their children were written.
		assert_eq!(fs::metadata(dest.join("srv")).unwrap().mtime(), 1_000_000);
		fs::remove_dir_all(&work).unwrap();
	}

	#[test]
	fn file_replaces_dir_and_symlink_cannot_escape_dest() {
		let work = tmp("escape");
		let dest = work.join("root");
		let outside = work.join("outside");
		fs::create_dir_all(&outside).unwrap();
		let l1 = Layer::new()
			.dir("x/", 0o755)
			.file("x/inner", b"i", 0o644, 0)
			// Naively `dest/../outside` = work/outside; confined it is dest/outside.
			.dir("outside/", 0o755)
			.link(EntryType::Symlink, "evil", "../outside")
			.bytes();
		let l2 = Layer::new()
			.file("x", b"now a file", 0o644, 0)
			.file("evil/pwned", b"p", 0o644, 0)
			.bytes();
		let p1 = write_layer(&work, "1.tar", &l1);
		let p2 = write_layer(&work, "2.tar", &l2);
		apply_layers(&[p1, p2], &dest).unwrap();
		assert_eq!(fs::read(dest.join("x")).unwrap(), b"now a file");
		assert!(!outside.join("pwned").exists(), "symlink escaped dest");
		// The write landed inside dest, at the symlink target re-rooted.
		assert!(dest.join("outside/pwned").exists());
		fs::remove_dir_all(&work).unwrap();
	}

	#[test]
	fn pax_xattrs_and_capabilities_survive_chown() {
		let work = tmp("xattr");
		let dest = work.join("root");
		// VFS_CAP_REVISION_2 capability blob granting cap_net_raw (bit 13).
		let mut cap = vec![0u8; 20];
		cap[..4].copy_from_slice(&0x0200_0001u32.to_le_bytes());
		cap[4..8].copy_from_slice(&(1u32 << 13).to_le_bytes());
		let layer = Layer::new()
			.pax_file(
				"bin/ping",
				&[
					("SCHILY.xattr.security.capability", &cap),
					("SCHILY.xattr.user.k", b"v"),
					("uid", b"70000"),
				],
				0o755,
			)
			.bytes();
		let path = write_layer(&work, "x.tar", &layer);
		apply_layers(&[path], &dest).unwrap();
		let target = CString::new(dest.join("bin/ping").as_os_str().as_bytes()).unwrap();
		let mut buf = [0u8; 64];
		for (name, want) in [("security.capability", &cap[..]), ("user.k", b"v")] {
			let name = CString::new(name).unwrap();
			// SAFETY: valid C strings and buffer.
			let n = unsafe {
				libc::lgetxattr(target.as_ptr(), name.as_ptr(), buf.as_mut_ptr().cast(), buf.len())
			};
			assert!(n >= 0, "{name:?}: {}", io::Error::last_os_error());
			assert_eq!(&buf[..n as usize], want);
		}
		assert_eq!(fs::metadata(dest.join("bin/ping")).unwrap().uid(), 70000);
		fs::remove_dir_all(&work).unwrap();
	}

	#[test]
	fn zstd_layers_are_sniffed() {
		let work = tmp("zstd");
		let layer = Layer::new().file("a", b"zz", 0o644, 0).bytes();
		let compressed =
			ruzstd::encoding::compress_to_vec(&layer[..], ruzstd::encoding::CompressionLevel::Fastest);
		let path = write_layer(&work, "z.tar.zst", &compressed);
		let stats = apply_layers(&[path], &work.join("root")).unwrap();
		assert_eq!((stats.files, stats.bytes), (1, 2));
		fs::remove_dir_all(&work).unwrap();
	}

	#[test]
	fn normalize_strips_root_and_dotdot() {
		assert_eq!(normalize(Path::new("./a/../../b/c")), Some(PathBuf::from("b/c")));
		assert_eq!(normalize(Path::new("/")), None);
		assert_eq!(normalize(Path::new("./")), None);
	}
}
