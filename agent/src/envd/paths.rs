//! envd path resolution (`ExpandAndResolve`): `~` expansion, relative paths
//! against the user's home, empty path → default workdir, lexical cleaning.
//! Used by every filesystem RPC, `cwd` of processes, and `/files`.

use std::path::{Path, PathBuf};

/// Resolve a client path for `home`. Errors are client mistakes
/// (`invalid_argument` / HTTP 400).
pub fn resolve(path: &str, home: &Path, default_workdir: Option<&str>) -> Result<PathBuf, String> {
	if path.is_empty() {
		if let Some(workdir) = default_workdir.filter(|w| !w.is_empty()) {
			return resolve(workdir, home, None);
		}
		return Ok(clean(&home.to_string_lossy()));
	}
	let expanded = if path == "~" {
		home.to_string_lossy().into_owned()
	} else if let Some(rest) = path.strip_prefix("~/") {
		format!("{}/{rest}", home.to_string_lossy())
	} else if path.starts_with('~') {
		return Err(format!("path '{path}' is invalid: cannot expand user-specific home dir"));
	} else {
		path.to_owned()
	};
	if expanded.starts_with('/') {
		Ok(clean(&expanded))
	} else {
		Ok(clean(&format!("{}/{expanded}", home.to_string_lossy())))
	}
}

/// Lexical clean of an absolute path (Go `filepath.Clean` semantics):
/// collapses `//`, `.`, and `..` (never above `/`).
pub fn clean(path: &str) -> PathBuf {
	let mut parts: Vec<&str> = Vec::new();
	for seg in path.split('/') {
		match seg {
			"" | "." => {},
			".." => {
				parts.pop();
			},
			s => parts.push(s),
		}
	}
	let mut out = String::with_capacity(path.len());
	for p in &parts {
		out.push('/');
		out.push_str(p);
	}
	if out.is_empty() {
		out.push('/');
	}
	PathBuf::from(out)
}

#[cfg(test)]
mod tests {
	use super::*;

	fn r(p: &str, workdir: Option<&str>) -> Result<String, String> {
		resolve(p, Path::new("/home/user"), workdir).map(|p| p.to_string_lossy().into_owned())
	}

	#[test]
	fn relative_paths_resolve_against_home_not_workdir() {
		assert_eq!(r("a.txt", Some("/app")).unwrap(), "/home/user/a.txt");
		assert_eq!(r("./x/../y", None).unwrap(), "/home/user/y");
		assert_eq!(r("../../../../etc", None).unwrap(), "/etc");
	}

	#[test]
	fn tilde_expands_only_for_own_home() {
		assert_eq!(r("~", None).unwrap(), "/home/user");
		assert_eq!(r("~/a//b/", None).unwrap(), "/home/user/a/b");
		assert!(r("~root/x", None).is_err());
	}

	#[test]
	fn empty_path_uses_workdir_then_home() {
		assert_eq!(r("", Some("/app/src")).unwrap(), "/app/src");
		assert_eq!(r("", Some("")).unwrap(), "/home/user");
		assert_eq!(r("", None).unwrap(), "/home/user");
		// A relative workdir is itself resolved against home.
		assert_eq!(r("", Some("proj")).unwrap(), "/home/user/proj");
	}

	#[test]
	fn absolute_paths_are_cleaned() {
		assert_eq!(r("/tmp/../var//log/.", None).unwrap(), "/var/log");
		assert_eq!(r("/..", None).unwrap(), "/");
		assert_eq!(clean("///").to_string_lossy(), "/");
	}
}
