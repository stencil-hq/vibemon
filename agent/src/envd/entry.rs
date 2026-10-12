//! `filesystem.EntryInfo` construction from `lstat`: type (following
//! symlinks), Go-style permission string, owner/group names, mtime, symlink
//! target, and `user.e2b.*` xattr metadata. Used by Stat/ListDir/MakeDir/Move,
//! watch events, and `/files/compose`.

use std::{
	collections::HashMap,
	ffi::CString,
	fs::Metadata,
	io,
	os::unix::{ffi::OsStrExt, fs::MetadataExt},
	path::Path,
};

use super::{pb::filesystem as pb, users::NameCache};

/// xattr namespace for envd file metadata (the prefix is stripped in APIs).
pub const XATTR_PREFIX: &str = "user.e2b.";

/// Build the entry for `path` (not following a final symlink for identity).
pub fn entry_info(path: &Path, names: &mut NameCache) -> io::Result<pb::EntryInfo> {
	let meta = std::fs::symlink_metadata(path)?;
	Ok(entry_from_meta(path, &meta, names))
}

/// Build the entry from already-fetched `lstat` metadata.
pub fn entry_from_meta(path: &Path, meta: &Metadata, names: &mut NameCache) -> pb::EntryInfo {
	let is_symlink = meta.file_type().is_symlink();
	// Upstream semantics: a symlink reports its target's type and permission
	// bits; a dangling one is FILE_TYPE_UNSPECIFIED with mode 0.
	let (file_type, mode, symlink_target) = if is_symlink {
		let target = std::fs::canonicalize(path)
			.ok()
			.or_else(|| std::fs::read_link(path).ok());
		let (ty, mode) = std::fs::metadata(path)
			.map_or((pb::FileType::Unspecified, 0), |m| (type_of(&m), m.mode() & 0o777));
		(ty, mode, target.map(|t| t.to_string_lossy().into_owned()))
	} else {
		(type_of(meta), meta.mode() & 0o777, None)
	};
	let name = path
		.file_name()
		.map_or_else(|| path.to_string_lossy().into_owned(), |n| n.to_string_lossy().into_owned());
	pb::EntryInfo {
		name,
		r#type: file_type as i32,
		path: path.to_string_lossy().into_owned(),
		size: meta.size().cast_signed(),
		mode,
		permissions: go_mode_string(meta.mode()),
		owner: names.user(meta.uid()),
		group: names.group(meta.gid()),
		modified_time: Some(pbjson_types::Timestamp {
			seconds: meta.mtime(),
			nanos:   i32::try_from(meta.mtime_nsec()).unwrap_or(0),
		}),
		symlink_target,
		metadata: read_metadata(path),
		// Released envd (≤ 0.9.0, and the 0.6.6 we advertise) never sets this
		// flag; SDKs map it to FILE_TYPE_SYMLINK, which would turn a symlink to a
		// file into `symlink` instead of upstream's `file`.
		is_symlink: false,
	}
}

fn type_of(meta: &Metadata) -> pb::FileType {
	let ft = meta.file_type();
	if ft.is_dir() {
		pb::FileType::Directory
	} else if ft.is_file() {
		pb::FileType::File
	} else if ft.is_symlink() {
		pb::FileType::Symlink
	} else {
		// Devices, fifos, sockets: upstream reports an unknown type.
		pb::FileType::Unspecified
	}
}

/// Go `fs.FileMode.String()` for a Unix `st_mode`, e.g. `drwxr-xr-x`,
/// `Lrwxrwxrwx`, `urwxr-xr-x`, `Dcrw-rw-rw-`.
pub fn go_mode_string(mode: u32) -> String {
	let fmt = mode & libc::S_IFMT;
	let mut out = String::with_capacity(12);
	// Order of Go's "dalTLDpSugct?" type/flag letters.
	if fmt == libc::S_IFDIR {
		out.push('d');
	}
	if fmt == libc::S_IFLNK {
		out.push('L');
	}
	if fmt == libc::S_IFBLK || fmt == libc::S_IFCHR {
		out.push('D');
	}
	if fmt == libc::S_IFIFO {
		out.push('p');
	}
	if fmt == libc::S_IFSOCK {
		out.push('S');
	}
	if mode & libc::S_ISUID != 0 {
		out.push('u');
	}
	if mode & libc::S_ISGID != 0 {
		out.push('g');
	}
	if fmt == libc::S_IFCHR {
		out.push('c');
	}
	if mode & libc::S_ISVTX != 0 {
		out.push('t');
	}
	if out.is_empty() {
		out.push('-');
	}
	for (i, c) in "rwxrwxrwx".chars().enumerate() {
		out.push(if mode & (1 << (8 - i)) != 0 { c } else { '-' });
	}
	out
}

fn c_path(path: &Path) -> Option<CString> {
	CString::new(path.as_os_str().as_bytes()).ok()
}

/// `user.e2b.*` xattrs of `path` (prefix stripped); empty when unsupported.
pub fn read_metadata(path: &Path) -> HashMap<String, String> {
	let mut out = HashMap::new();
	let Some(cpath) = c_path(path) else {
		return out;
	};
	let mut names = vec![0u8; 4096];
	// SAFETY: names is a valid buffer of the given length.
	let len = unsafe { libc::llistxattr(cpath.as_ptr(), names.as_mut_ptr().cast(), names.len()) };
	let Ok(len) = usize::try_from(len) else {
		return out;
	};
	for raw in names[..len].split(|&b| b == 0).filter(|n| !n.is_empty()) {
		let Ok(name) = std::str::from_utf8(raw) else {
			continue;
		};
		let Some(key) = name.strip_prefix(XATTR_PREFIX) else {
			continue;
		};
		let Ok(cname) = CString::new(raw) else {
			continue;
		};
		let mut value = vec![0u8; 4096];
		// SAFETY: value is a valid buffer of the given length.
		let n = unsafe {
			libc::lgetxattr(cpath.as_ptr(), cname.as_ptr(), value.as_mut_ptr().cast(), value.len())
		};
		if let Ok(n) = usize::try_from(n) {
			out.insert(key.to_owned(), String::from_utf8_lossy(&value[..n]).into_owned());
		}
	}
	out
}

/// Replace the `user.e2b.*` xattrs of `path` with `metadata`. Filesystems
/// without user xattrs (`ENOTSUP`) are tolerated, as in envd.
pub fn write_metadata(path: &Path, metadata: &[(String, String)]) -> io::Result<()> {
	let Some(cpath) = c_path(path) else {
		return Err(io::Error::from(io::ErrorKind::InvalidInput));
	};
	for key in read_metadata(path).keys() {
		if let Ok(cname) = CString::new(format!("{XATTR_PREFIX}{key}")) {
			// SAFETY: valid NUL-terminated strings.
			unsafe { libc::lremovexattr(cpath.as_ptr(), cname.as_ptr()) };
		}
	}
	for (key, value) in metadata {
		let Ok(cname) = CString::new(format!("{XATTR_PREFIX}{key}")) else {
			continue;
		};
		// SAFETY: valid NUL-terminated name and a value buffer of the given
		// length.
		let rc = unsafe {
			libc::lsetxattr(cpath.as_ptr(), cname.as_ptr(), value.as_ptr().cast(), value.len(), 0)
		};
		if rc != 0 {
			let err = io::Error::last_os_error();
			if err.raw_os_error() == Some(libc::ENOTSUP) {
				return Ok(());
			}
			return Err(err);
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn go_mode_strings() {
		assert_eq!(go_mode_string(libc::S_IFREG | 0o644), "-rw-r--r--");
		assert_eq!(go_mode_string(libc::S_IFDIR | 0o755), "drwxr-xr-x");
		assert_eq!(go_mode_string(libc::S_IFLNK | 0o777), "Lrwxrwxrwx");
		assert_eq!(go_mode_string(libc::S_IFREG | libc::S_ISUID | 0o755), "urwxr-xr-x");
		assert_eq!(go_mode_string(libc::S_IFCHR | 0o666), "Dcrw-rw-rw-");
		assert_eq!(go_mode_string(libc::S_IFDIR | libc::S_ISVTX | 0o777), "dtrwxrwxrwx");
		assert_eq!(go_mode_string(libc::S_IFIFO | 0o600), "prw-------");
	}

	#[test]
	fn entry_for_symlink_describes_target_type_like_released_envd() {
		let dir = std::env::temp_dir().join(format!("envd-entry-{}", std::process::id()));
		std::fs::create_dir_all(dir.join("real")).unwrap();
		let link = dir.join("link");
		let _ = std::fs::remove_file(&link);
		std::os::unix::fs::symlink(dir.join("real"), &link).unwrap();
		let mut names = NameCache::default();
		let e = entry_info(&link, &mut names).unwrap();
		assert!(!e.is_symlink);
		assert_eq!(e.r#type, pb::FileType::Directory as i32);
		assert_eq!(e.symlink_target.as_deref(), Some(dir.join("real").to_str().unwrap()));
		assert!(e.permissions.starts_with('L'));
		assert_eq!(e.name, "link");
		let dangling = dir.join("dangling");
		let _ = std::fs::remove_file(&dangling);
		std::os::unix::fs::symlink(dir.join("missing"), &dangling).unwrap();
		let e = entry_info(&dangling, &mut names).unwrap();
		assert_eq!((e.r#type, e.mode), (pb::FileType::Unspecified as i32, 0));
		assert!(!e.is_symlink);
		// A FIFO is neither a file nor a directory.
		let fifo = dir.join("fifo");
		let c = std::ffi::CString::new(fifo.to_str().unwrap()).unwrap();
		// SAFETY: valid NUL-terminated path.
		assert_eq!(unsafe { libc::mkfifo(c.as_ptr(), 0o600) }, 0);
		let e = entry_info(&fifo, &mut names).unwrap();
		assert_eq!((e.r#type, e.mode), (pb::FileType::Unspecified as i32, 0o600));
		assert_eq!(e.permissions, "prw-------");
		std::fs::remove_dir_all(&dir).unwrap();
	}
}
