//! `filesystem.Filesystem` unary RPCs (`Stat`, `MakeDir`, `Move`, `ListDir`,
//! `Remove`) plus the ownership-preserving directory creation shared with
//! `/files`. Blocking filesystem work runs on tokio's blocking pool.

use std::{
	io,
	os::unix::fs::{DirBuilderExt, MetadataExt},
	path::{Path, PathBuf},
};

use super::{
	connect::{Code, RpcError},
	entry::{entry_from_meta, entry_info},
	paths,
	pb::filesystem as pb,
	users::{NameCache, User},
};

/// Resolve a request path for `user` (`invalid_argument` on bad input).
pub fn resolve(path: &str, user: &User, workdir: Option<&str>) -> Result<PathBuf, RpcError> {
	paths::resolve(path, &user.home, workdir).map_err(RpcError::invalid)
}

/// Map an I/O error on `path` to the Connect code envd uses.
pub fn io_rpc_error(path: &Path, e: &io::Error) -> RpcError {
	match e.kind() {
		io::ErrorKind::NotFound | io::ErrorKind::NotADirectory => {
			RpcError::not_found(format!("path '{}' does not exist", path.display()))
		},
		io::ErrorKind::PermissionDenied => RpcError::new(
			Code::PermissionDenied,
			format!("permission denied for '{}': {e}", path.display()),
		),
		_ => RpcError::internal(format!("error accessing '{}': {e}", path.display())),
	}
}

/// `chown` to the user (no-op when envd is not root or already the owner).
pub fn chown_to(path: &Path, user: &User) -> io::Result<()> {
	std::os::unix::fs::lchown(path, Some(user.uid), Some(user.gid)).or_else(|e| {
		if e.kind() == io::ErrorKind::PermissionDenied
			&& std::fs::symlink_metadata(path)?.uid() == user.uid
		{
			Ok(())
		} else {
			Err(e)
		}
	})
}

/// Create `dir` and missing parents (0755), chowning each created
/// directory to `user` (envd `EnsureDirs`).
pub fn ensure_dirs(dir: &Path, user: &User) -> io::Result<()> {
	let mut missing = Vec::new();
	let mut cur = Some(dir);
	while let Some(p) = cur {
		match std::fs::metadata(p) {
			Ok(m) if m.is_dir() => break,
			Ok(_) => {
				return Err(io::Error::new(
					io::ErrorKind::AlreadyExists,
					format!("'{}' is not a directory", p.display()),
				));
			},
			// ENOTDIR: keep walking up to report the non-directory ancestor.
			Err(e) if matches!(e.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
				missing.push(p.to_owned());
			},
			Err(e) => return Err(e),
		}
		cur = p.parent();
	}
	for p in missing.iter().rev() {
		match std::fs::DirBuilder::new().mode(0o755).create(p) {
			Ok(()) => chown_to(p, user)?,
			Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {},
			Err(e) => return Err(e),
		}
	}
	Ok(())
}

/// `Stat`.
pub fn stat(path: &Path) -> Result<pb::StatResponse, RpcError> {
	let entry = entry_info(path, &mut NameCache::default()).map_err(|e| io_rpc_error(path, &e))?;
	Ok(pb::StatResponse { entry: Some(entry) })
}

/// `MakeDir`: `already_exists` for an existing directory, `invalid_argument`
/// when a non-directory is in the way.
pub fn make_dir(path: &Path, user: &User) -> Result<pb::MakeDirResponse, RpcError> {
	match std::fs::metadata(path) {
		Ok(m) if m.is_dir() => {
			return Err(RpcError::new(
				Code::AlreadyExists,
				format!("directory '{}' already exists", path.display()),
			));
		},
		Ok(_) => {
			return Err(RpcError::invalid(format!(
				"path '{}' already exists and is not a directory",
				path.display()
			)));
		},
		// ENOTDIR (a file in the middle of the path) is reported by
		// `ensure_dirs` as invalid_argument below.
		Err(e) if !matches!(e.kind(), io::ErrorKind::NotFound | io::ErrorKind::NotADirectory) => {
			return Err(io_rpc_error(path, &e));
		},
		Err(_) => {},
	}
	ensure_dirs(path, user).map_err(|e| match e.kind() {
		io::ErrorKind::AlreadyExists => RpcError::invalid(e.to_string()),
		_ => RpcError::internal(format!("error creating directory '{}': {e}", path.display())),
	})?;
	let entry = entry_info(path, &mut NameCache::default()).map_err(|e| io_rpc_error(path, &e))?;
	Ok(pb::MakeDirResponse { entry: Some(entry) })
}

/// `Move`: creates destination parents; missing source → `not_found`.
pub fn rename(
	source: &Path,
	destination: &Path,
	user: &User,
) -> Result<pb::MoveResponse, RpcError> {
	std::fs::symlink_metadata(source).map_err(|e| io_rpc_error(source, &e))?;
	if let Some(parent) = destination.parent() {
		ensure_dirs(parent, user)
			.map_err(|e| RpcError::internal(format!("error creating destination parent: {e}")))?;
	}
	std::fs::rename(source, destination).map_err(|e| match e.kind() {
		io::ErrorKind::NotFound => io_rpc_error(source, &e),
		_ => RpcError::internal(format!(
			"error renaming '{}' to '{}': {e}",
			source.display(),
			destination.display()
		)),
	})?;
	let entry = entry_info(destination, &mut NameCache::default())
		.map_err(|e| io_rpc_error(destination, &e))?;
	Ok(pb::MoveResponse { entry: Some(entry) })
}

/// `Remove`: like `os.RemoveAll` — a missing path succeeds; a symlink is
/// removed, never followed.
pub fn remove(path: &Path) -> Result<pb::RemoveResponse, RpcError> {
	let result = match std::fs::symlink_metadata(path) {
		Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
		Ok(_) => std::fs::remove_file(path),
		Err(e) => Err(e),
	};
	match result {
		Ok(()) => Ok(pb::RemoveResponse {}),
		Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(pb::RemoveResponse {}),
		Err(e) => Err(RpcError::internal(format!("error removing '{}': {e}", path.display()))),
	}
}

/// `ListDir`: entries up to `depth` (0 → 1) in lexical walk order; a
/// symlinked root is followed, nested symlinks are not. Paths are reported
/// under the requested path.
pub fn list_dir(path: &Path, depth: u32) -> Result<pb::ListDirResponse, RpcError> {
	let depth = depth.max(1);
	let meta = std::fs::metadata(path).map_err(|e| io_rpc_error(path, &e))?;
	if !meta.is_dir() {
		return Err(RpcError::invalid(format!("path '{}' is not a directory", path.display())));
	}
	let real = std::fs::canonicalize(path).map_err(|e| io_rpc_error(path, &e))?;
	let mut names = NameCache::default();
	let mut entries = Vec::new();
	walk(&real, path, 1, depth, &mut names, &mut entries).map_err(|e| {
		RpcError::internal(format!("error reading directory '{}': {e}", path.display()))
	})?;
	Ok(pb::ListDirResponse { entries })
}

fn walk(
	real: &Path,
	shown: &Path,
	level: u32,
	depth: u32,
	names: &mut NameCache,
	out: &mut Vec<pb::EntryInfo>,
) -> io::Result<()> {
	let mut children: Vec<_> = std::fs::read_dir(real)?.filter_map(Result::ok).collect();
	children.sort_by_key(std::fs::DirEntry::file_name);
	for child in children {
		let shown_path = shown.join(child.file_name());
		let Ok(meta) = std::fs::symlink_metadata(child.path()) else {
			continue;
		};
		out.push(entry_from_meta(&shown_path, &meta, names));
		if meta.is_dir() && level < depth {
			// Unreadable subdirectories are listed but not descended into.
			let _ = walk(&child.path(), &shown_path, level + 1, depth, names, out);
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn me() -> User {
		// SAFETY: getuid/getgid are always safe.
		let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
		User {
			name: "me".into(),
			uid,
			gid,
			home: std::env::temp_dir(),
			shell: "/bin/sh".into(),
			groups: vec![gid],
		}
	}

	fn tmp(name: &str) -> PathBuf {
		let p = std::env::temp_dir().join(format!("envd-fs-{name}-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&p);
		p
	}

	#[test]
	fn make_dir_codes() {
		let root = tmp("mkdir");
		let nested = root.join("a/b/c");
		let r = make_dir(&nested, &me()).unwrap();
		assert_eq!(r.entry.unwrap().r#type, pb::FileType::Directory as i32);
		assert_eq!(make_dir(&nested, &me()).unwrap_err().code, Code::AlreadyExists);
		std::fs::write(root.join("file"), "x").unwrap();
		assert_eq!(make_dir(&root.join("file"), &me()).unwrap_err().code, Code::InvalidArgument);
		// A file in the middle of the path.
		assert_eq!(make_dir(&root.join("file/sub"), &me()).unwrap_err().code, Code::InvalidArgument);
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn remove_is_idempotent_and_does_not_follow_symlinks() {
		let root = tmp("rm");
		std::fs::create_dir_all(root.join("keep")).unwrap();
		std::fs::write(root.join("keep/f"), "x").unwrap();
		std::os::unix::fs::symlink(root.join("keep"), root.join("link")).unwrap();
		remove(&root.join("link")).unwrap();
		assert!(root.join("keep/f").exists());
		remove(&root.join("missing")).unwrap();
		remove(&root.join("keep")).unwrap();
		assert!(!root.join("keep").exists());
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn list_dir_depth_and_errors() {
		let root = tmp("ls");
		std::fs::create_dir_all(root.join("d/e")).unwrap();
		std::fs::write(root.join("b.txt"), "hello").unwrap();
		std::fs::write(root.join("d/e/deep"), "").unwrap();
		let names = |r: pb::ListDirResponse| {
			r.entries
				.into_iter()
				.map(|e| {
					e.path
						.strip_prefix(root.to_str().unwrap())
						.unwrap()
						.to_owned()
				})
				.collect::<Vec<_>>()
		};
		assert_eq!(names(list_dir(&root, 0).unwrap()), ["/b.txt", "/d"]);
		assert_eq!(names(list_dir(&root, 2).unwrap()), ["/b.txt", "/d", "/d/e"]);
		assert_eq!(names(list_dir(&root, 3).unwrap()), ["/b.txt", "/d", "/d/e", "/d/e/deep"]);
		let b = list_dir(&root, 1).unwrap().entries.remove(0);
		assert_eq!((b.size, b.name.as_str()), (5, "b.txt"));
		assert_eq!(list_dir(&root.join("b.txt"), 1).unwrap_err().code, Code::InvalidArgument);
		assert_eq!(list_dir(&root.join("nope"), 1).unwrap_err().code, Code::NotFound);
		// Symlinked root is followed; paths stay under the requested path.
		std::os::unix::fs::symlink(root.join("d"), root.join("ld")).unwrap();
		let r = list_dir(&root.join("ld"), 1).unwrap();
		assert_eq!(r.entries[0].path, root.join("ld/e").to_str().unwrap());
		std::fs::remove_dir_all(&root).unwrap();
	}

	#[test]
	fn move_creates_parents_and_reports_missing_source() {
		let root = tmp("mv");
		std::fs::create_dir_all(&root).unwrap();
		std::fs::write(root.join("src"), "x").unwrap();
		let r = rename(&root.join("src"), &root.join("x/y/dst"), &me()).unwrap();
		assert_eq!(r.entry.unwrap().name, "dst");
		assert_eq!(
			rename(&root.join("src"), &root.join("z"), &me())
				.unwrap_err()
				.code,
			Code::NotFound
		);
		assert_eq!(stat(&root.join("src")).unwrap_err().code, Code::NotFound);
		std::fs::remove_dir_all(&root).unwrap();
	}
}
