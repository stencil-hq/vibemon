//! Team volumes on this node (ARCHITECTURE.md §9.6).
//!
//! One directory per volume under the volume root, plus the volume-content
//! API (`spec/e2b/sdk/spec/openapi-volumecontent.yml`) served at
//! `/node/v1/volumes/{id}/content/{path|dir|file}?path=…`. The driver
//! authenticates the volume token and forwards requests unchanged.
//!
//! Every path is confined to the volume directory: `..` is rejected, the
//! parent is canonicalized and must stay under the root, and the final
//! component is never followed when it is a symlink.

use std::{
	fs,
	io::{self, ErrorKind},
	os::unix::fs::{MetadataExt, PermissionsExt},
	path::{Component, Path, PathBuf},
};

use axum::{
	Json, Router,
	body::Body,
	extract::{Path as UrlPath, Query, State},
	http::StatusCode,
	response::{IntoResponse, Response},
	routing::{any, post},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

/// Volume-content error: HTTP status plus the spec's `{code, message}` body.
#[derive(Debug)]
pub struct VolumeError {
	status:  StatusCode,
	code:    &'static str,
	message: String,
}

impl VolumeError {
	/// Error with an explicit status and spec `code`.
	pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
		Self { status, code, message: message.into() }
	}

	/// `400 invalid`.
	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_REQUEST, "invalid", message)
	}

	fn io(error: &io::Error, path: &str) -> Self {
		match error.kind() {
			ErrorKind::NotFound => {
				Self::new(StatusCode::NOT_FOUND, "not_found", format!("path {path} not found"))
			},
			ErrorKind::AlreadyExists => {
				Self::new(StatusCode::CONFLICT, "conflict", format!("path {path} already exists"))
			},
			ErrorKind::PermissionDenied => {
				Self::new(StatusCode::FORBIDDEN, "forbidden", format!("{path}: {error}"))
			},
			ErrorKind::NotADirectory
			| ErrorKind::IsADirectory
			| ErrorKind::DirectoryNotEmpty
			| ErrorKind::InvalidInput => Self::invalid(format!("{path}: {error}")),
			_ if error.raw_os_error() == Some(libc::ELOOP) => {
				Self::invalid(format!("{path} is a symlink"))
			},
			_ => Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", format!("{path}: {error}")),
		}
	}
}

impl IntoResponse for VolumeError {
	fn into_response(self) -> Response {
		(self.status, Json(serde_json::json!({ "code": self.code, "message": self.message })))
			.into_response()
	}
}

/// Validates a volume ID (UUID-like token) before it becomes a directory name.
fn check_volume_id(id: &str) -> Result<(), VolumeError> {
	if id.is_empty() || id.len() > 64 || !id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
	{
		return Err(VolumeError::invalid(format!("invalid volume id {id:?}")));
	}
	Ok(())
}

/// Normalizes a client path to `/`-rooted relative components; rejects `..`.
pub fn components(path: &str) -> Result<Vec<String>, VolumeError> {
	let mut parts = Vec::new();
	for component in Path::new(path).components() {
		match component {
			Component::RootDir | Component::CurDir => {},
			Component::Normal(part) => {
				parts.push(
					part
						.to_str()
						.ok_or_else(|| VolumeError::invalid("path is not UTF-8"))?
						.to_owned(),
				);
			},
			Component::ParentDir | Component::Prefix(_) => {
				return Err(VolumeError::invalid(format!("path {path:?} escapes the volume")));
			},
		}
	}
	Ok(parts)
}

/// Resolves `path` inside `root` without escaping it: every existing
/// ancestor must canonicalize under the canonical root (no symlinked parent
/// leads outside); the final component is returned unresolved.
pub fn resolve(root: &Path, path: &str) -> Result<PathBuf, VolumeError> {
	let parts = components(path)?;
	let root = root.canonicalize().map_err(|e| VolumeError::io(&e, "/"))?;
	let Some((last, parents)) = parts.split_last() else {
		return Ok(root);
	};
	let mut current = root.clone();
	for part in parents {
		current.push(part);
		match current.canonicalize() {
			Ok(real) if real.starts_with(&root) => current = real,
			Ok(_) => {
				return Err(VolumeError::new(
					StatusCode::FORBIDDEN,
					"forbidden",
					format!("path {path:?} escapes the volume"),
				));
			},
			// Missing ancestors are fine (created later or reported as not found).
			Err(e) if e.kind() == ErrorKind::NotFound => {},
			Err(e) => return Err(VolumeError::io(&e, path)),
		}
	}
	current.push(last);
	Ok(current)
}

/// One entry of the volume-content API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct VolumeEntryStat {
	pub name:   String,
	#[serde(rename = "type")]
	pub kind:   &'static str,
	pub path:   String,
	pub size:   i64,
	pub mode:   u32,
	pub uid:    u32,
	pub gid:    u32,
	pub atime:  String,
	pub mtime:  String,
	pub ctime:  String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub target: Option<String>,
}

fn rfc3339(secs: i64, nanos: i64) -> String {
	chrono::DateTime::from_timestamp(secs, u32::try_from(nanos).unwrap_or(0))
		.unwrap_or_default()
		.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// Stats `full` (never following a final symlink) and reports it under the
/// volume-relative `rel` path.
fn stat_entry(full: &Path, rel: &str) -> io::Result<VolumeEntryStat> {
	let meta = fs::symlink_metadata(full)?;
	let file_type = meta.file_type();
	let kind = if file_type.is_symlink() {
		"symlink"
	} else if file_type.is_dir() {
		"directory"
	} else if file_type.is_file() {
		"file"
	} else {
		"unknown"
	};
	let target = file_type
		.is_symlink()
		.then(|| fs::read_link(full).map(|t| t.to_string_lossy().into_owned()))
		.transpose()?;
	let name = Path::new(rel)
		.file_name()
		.map_or_else(|| "/".to_owned(), |n| n.to_string_lossy().into_owned());
	Ok(VolumeEntryStat {
		name,
		kind,
		path: rel.to_owned(),
		size: i64::try_from(meta.len()).unwrap_or(i64::MAX),
		mode: meta.mode() & 0o7777,
		uid: meta.uid(),
		gid: meta.gid(),
		atime: rfc3339(meta.atime(), meta.atime_nsec()),
		mtime: rfc3339(meta.mtime(), meta.mtime_nsec()),
		ctime: rfc3339(meta.ctime(), meta.ctime_nsec()),
		target,
	})
}

/// Canonical `/a/b` form of a client path.
fn display_path(path: &str) -> Result<String, VolumeError> {
	Ok(format!("/{}", components(path)?.join("/")))
}

/// Applies requested ownership. Unprivileged nodes cannot give files away;
/// that `EPERM` is skipped and the returned stat shows the real owner.
fn apply_owner(full: &Path, uid: Option<u32>, gid: Option<u32>) -> io::Result<()> {
	if uid.is_none() && gid.is_none() {
		return Ok(());
	}
	match std::os::unix::fs::lchown(full, uid, gid) {
		// SAFETY: geteuid has no preconditions.
		Err(e) if e.kind() == ErrorKind::PermissionDenied && unsafe { libc::geteuid() } != 0 => {
			tracing::debug!(path = %full.display(), "unprivileged node: ownership change skipped");
			Ok(())
		},
		other => other,
	}
}

fn apply_mode(full: &Path, mode: Option<u32>) -> io::Result<()> {
	match mode {
		Some(mode) => fs::set_permissions(full, fs::Permissions::from_mode(mode & 0o7777)),
		None => Ok(()),
	}
}

fn list(dir: &Path, rel: &str, depth: u32, out: &mut Vec<VolumeEntryStat>) -> io::Result<()> {
	let mut entries: Vec<_> = fs::read_dir(dir)?.collect::<Result<_, _>>()?;
	entries.sort_by_key(fs::DirEntry::file_name);
	for entry in entries {
		let name = entry.file_name().to_string_lossy().into_owned();
		let child_rel = if rel == "/" {
			format!("/{name}")
		} else {
			format!("{rel}/{name}")
		};
		let stat = stat_entry(&entry.path(), &child_rel)?;
		let recurse = stat.kind == "directory" && depth > 1;
		out.push(stat);
		if recurse {
			list(&entry.path(), &child_rel, depth - 1, out)?;
		}
	}
	Ok(())
}

/// Volume directories under one root.
#[derive(Clone, Debug)]
pub struct VolumeStore {
	root: PathBuf,
}

impl VolumeStore {
	/// Store rooted at `root` (created if missing).
	pub fn new(root: PathBuf) -> io::Result<Self> {
		fs::create_dir_all(&root)?;
		Ok(Self { root })
	}

	/// Directory of volume `id` (validated).
	pub fn dir(&self, id: &str) -> Result<PathBuf, VolumeError> {
		check_volume_id(id)?;
		Ok(self.root.join(id))
	}

	/// Creates the volume directory (idempotent).
	pub fn create(&self, id: &str) -> Result<(), VolumeError> {
		let dir = self.dir(id)?;
		fs::create_dir_all(&dir).map_err(|e| VolumeError::io(&e, "/"))?;
		fs::set_permissions(&dir, fs::Permissions::from_mode(0o777))
			.map_err(|e| VolumeError::io(&e, "/"))
	}

	/// Deletes the volume directory and its contents (idempotent).
	pub fn delete(&self, id: &str) -> Result<(), VolumeError> {
		match fs::remove_dir_all(self.dir(id)?) {
			Err(e) if e.kind() != ErrorKind::NotFound => Err(VolumeError::io(&e, "/")),
			_ => Ok(()),
		}
	}

	fn existing(&self, id: &str) -> Result<PathBuf, VolumeError> {
		let dir = self.dir(id)?;
		if !dir.is_dir() {
			return Err(VolumeError::new(
				StatusCode::NOT_FOUND,
				"not_found",
				format!("volume {id} not found"),
			));
		}
		Ok(dir)
	}
}

/// Query of every volume-content operation.
#[derive(Debug, Default, Deserialize)]
pub struct ContentQuery {
	pub path:  Option<String>,
	pub depth: Option<u32>,
	pub uid:   Option<u32>,
	pub gid:   Option<u32>,
	pub mode:  Option<u32>,
	pub force: Option<bool>,
}

/// `PATCH …/path` body.
#[derive(Debug, Default, Deserialize)]
pub struct MetadataPatch {
	pub uid:  Option<u32>,
	pub gid:  Option<u32>,
	pub mode: Option<u32>,
}

/// Node routes (relative to `/node/v1`): volume create/delete and content.
pub fn routes(store: VolumeStore) -> Router {
	Router::new()
		.route("/volumes/{id}", post(create_volume).delete(delete_volume))
		.route("/volumes/{id}/content/{op}", any(content))
		.with_state(store)
}

async fn create_volume(
	State(store): State<VolumeStore>,
	UrlPath(id): UrlPath<String>,
) -> Result<StatusCode, VolumeError> {
	tokio::task::spawn_blocking(move || store.create(&id))
		.await
		.map_err(join_error)??;
	Ok(StatusCode::CREATED)
}

async fn delete_volume(
	State(store): State<VolumeStore>,
	UrlPath(id): UrlPath<String>,
) -> Result<StatusCode, VolumeError> {
	tokio::task::spawn_blocking(move || store.delete(&id))
		.await
		.map_err(join_error)??;
	Ok(StatusCode::NO_CONTENT)
}

fn join_error(error: tokio::task::JoinError) -> VolumeError {
	VolumeError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", error.to_string())
}

async fn content(
	State(store): State<VolumeStore>,
	UrlPath((id, op)): UrlPath<(String, String)>,
	Query(query): Query<ContentQuery>,
	request: axum::extract::Request,
) -> Result<Response, VolumeError> {
	let root = store.existing(&id)?;
	let rel_input = query
		.path
		.clone()
		.filter(|p| !p.is_empty())
		.ok_or_else(|| VolumeError::invalid("missing path"))?;
	let rel = display_path(&rel_input)?;
	let full = resolve(&root, &rel_input)?;
	let method = request.method().clone();
	match (op.as_str(), method.as_str()) {
		("path", "GET") => {
			blocking(move || {
				stat_entry(&full, &rel)
					.map(Json)
					.map_err(|e| VolumeError::io(&e, &rel))
			})
			.await
		},
		("path", "PATCH") => {
			let body = axum::body::to_bytes(request.into_body(), 64 * 1024)
				.await
				.map_err(|e| VolumeError::invalid(e.to_string()))?;
			let patch: MetadataPatch =
				serde_json::from_slice(&body).map_err(|e| VolumeError::invalid(e.to_string()))?;
			blocking(move || {
				let meta = fs::symlink_metadata(&full).map_err(|e| VolumeError::io(&e, &rel))?;
				if patch.mode.is_some() && meta.file_type().is_symlink() {
					return Err(VolumeError::invalid("cannot change the mode of a symlink"));
				}
				apply_owner(&full, patch.uid, patch.gid).map_err(|e| VolumeError::io(&e, &rel))?;
				apply_mode(&full, patch.mode).map_err(|e| VolumeError::io(&e, &rel))?;
				stat_entry(&full, &rel)
					.map(Json)
					.map_err(|e| VolumeError::io(&e, &rel))
			})
			.await
		},
		("path", "DELETE") => {
			if full == root.canonicalize().unwrap_or_else(|_| root.clone()) {
				return Err(VolumeError::invalid("cannot delete the volume root"));
			}
			blocking(move || {
				let meta = fs::symlink_metadata(&full).map_err(|e| VolumeError::io(&e, &rel))?;
				let result = if meta.is_dir() {
					fs::remove_dir_all(&full)
				} else {
					fs::remove_file(&full)
				};
				result
					.map(|()| StatusCode::NO_CONTENT)
					.map_err(|e| VolumeError::io(&e, &rel))
			})
			.await
		},
		("dir", "GET") => {
			let depth = query.depth.unwrap_or(1).max(1);
			blocking(move || {
				let meta = fs::symlink_metadata(&full).map_err(|e| VolumeError::io(&e, &rel))?;
				if !meta.is_dir() {
					return Err(VolumeError::invalid(format!("{rel} is not a directory")));
				}
				let mut out = Vec::new();
				list(&full, &rel, depth, &mut out).map_err(|e| VolumeError::io(&e, &rel))?;
				Ok(Json(out))
			})
			.await
		},
		("dir", "POST") => {
			let parents = query.force.unwrap_or(false);
			blocking(move || {
				let created = if parents {
					fs::create_dir_all(&full)
				} else {
					fs::create_dir(&full)
				};
				created.map_err(|e| VolumeError::io(&e, &rel))?;
				apply_owner(&full, query.uid, query.gid).map_err(|e| VolumeError::io(&e, &rel))?;
				apply_mode(&full, query.mode).map_err(|e| VolumeError::io(&e, &rel))?;
				stat_entry(&full, &rel)
					.map(|stat| (StatusCode::CREATED, Json(stat)))
					.map_err(|e| VolumeError::io(&e, &rel))
			})
			.await
		},
		("file", "GET") => {
			let file = tokio::fs::OpenOptions::new()
				.read(true)
				.custom_flags(libc::O_NOFOLLOW)
				.open(&full)
				.await
				.map_err(|e| VolumeError::io(&e, &rel))?;
			let meta = file
				.metadata()
				.await
				.map_err(|e| VolumeError::io(&e, &rel))?;
			if !meta.is_file() {
				return Err(VolumeError::invalid(format!("{rel} is not a file")));
			}
			Ok((
				[
					(axum::http::header::CONTENT_TYPE, "application/octet-stream"),
					(axum::http::header::CONTENT_LENGTH, &meta.len().to_string()),
				],
				Body::from_stream(ReaderStream::new(file)),
			)
				.into_response())
		},
		("file", "PUT") => upload(&root, full, rel, query, request.into_body()).await,
		_ => Err(VolumeError::new(
			StatusCode::METHOD_NOT_ALLOWED,
			"invalid",
			format!("{method} {op} is not supported"),
		)),
	}
}

async fn blocking<T: IntoResponse + Send + 'static>(
	work: impl FnOnce() -> Result<T, VolumeError> + Send + 'static,
) -> Result<Response, VolumeError> {
	tokio::task::spawn_blocking(work)
		.await
		.map_err(join_error)?
		.map(IntoResponse::into_response)
}

async fn upload(
	root: &Path,
	full: PathBuf,
	rel: String,
	query: ContentQuery,
	body: Body,
) -> Result<Response, VolumeError> {
	if let Some(parent) = full.parent() {
		tokio::fs::create_dir_all(parent)
			.await
			.map_err(|e| VolumeError::io(&e, &rel))?;
		// Re-check confinement now that the parents exist.
		let real = parent
			.canonicalize()
			.map_err(|e| VolumeError::io(&e, &rel))?;
		if !real.starts_with(root.canonicalize().map_err(|e| VolumeError::io(&e, "/"))?) {
			return Err(VolumeError::new(
				StatusCode::FORBIDDEN,
				"forbidden",
				format!("path {rel} escapes the volume"),
			));
		}
	}
	let mut options = tokio::fs::OpenOptions::new();
	options
		.write(true)
		.custom_flags(libc::O_NOFOLLOW)
		.mode(query.mode.unwrap_or(0o644) & 0o7777);
	if query.force.unwrap_or(false) {
		options.create(true).truncate(true);
	} else {
		options.create_new(true);
	}
	let mut file = options
		.open(&full)
		.await
		.map_err(|e| VolumeError::io(&e, &rel))?;
	let mut stream = body.into_data_stream().map_err(io::Error::other);
	while let Some(chunk) = stream
		.try_next()
		.await
		.map_err(|e| VolumeError::io(&e, &rel))?
	{
		file
			.write_all(&chunk)
			.await
			.map_err(|e| VolumeError::io(&e, &rel))?;
	}
	file.flush().await.map_err(|e| VolumeError::io(&e, &rel))?;
	drop(file);
	blocking(move || {
		apply_owner(&full, query.uid, query.gid).map_err(|e| VolumeError::io(&e, &rel))?;
		apply_mode(&full, query.mode).map_err(|e| VolumeError::io(&e, &rel))?;
		stat_entry(&full, &rel)
			.map(|stat| (StatusCode::CREATED, Json(stat)))
			.map_err(|e| VolumeError::io(&e, &rel))
	})
	.await
}

#[cfg(test)]
mod tests {
	use super::*;

	fn volume() -> (tempfile::TempDir, PathBuf) {
		let dir = tempfile::tempdir().unwrap();
		let root = dir.path().join("vol");
		fs::create_dir(&root).unwrap();
		(dir, root)
	}

	#[test]
	fn dotdot_and_symlinked_parents_cannot_escape() {
		let (dir, root) = volume();
		let outside = dir.path().join("outside");
		fs::create_dir(&outside).unwrap();
		fs::write(outside.join("secret"), b"x").unwrap();
		std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();

		assert_eq!(resolve(&root, "../outside/secret").unwrap_err().status, StatusCode::BAD_REQUEST);
		assert_eq!(resolve(&root, "/a/../../outside").unwrap_err().status, StatusCode::BAD_REQUEST);
		assert_eq!(resolve(&root, "link/secret").unwrap_err().status, StatusCode::FORBIDDEN);
		let real_root = root.canonicalize().unwrap();
		assert_eq!(resolve(&root, "/").unwrap(), real_root);
		assert_eq!(resolve(&root, "//a/./b").unwrap(), real_root.join("a/b"));
		// A final-component symlink is returned unresolved (stat shows it as a
		// link).
		let link = resolve(&root, "link").unwrap();
		assert_eq!(link, real_root.join("link"));
		let stat = stat_entry(&link, "/link").unwrap();
		assert_eq!((stat.kind, stat.target.as_deref()), ("symlink", Some(outside.to_str().unwrap())));
	}

	#[test]
	fn listing_respects_depth_and_does_not_follow_links() {
		let (dir, root) = volume();
		fs::create_dir_all(root.join("a/b/c")).unwrap();
		fs::write(root.join("a/f.txt"), b"hello").unwrap();
		std::os::unix::fs::symlink(dir.path(), root.join("a/escape")).unwrap();
		let mut depth1 = Vec::new();
		list(&root, "/", 1, &mut depth1).unwrap();
		assert_eq!(depth1.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(), ["/a"]);
		let mut depth3 = Vec::new();
		list(&root, "/", 3, &mut depth3).unwrap();
		let paths: Vec<_> = depth3.iter().map(|e| e.path.as_str()).collect();
		assert_eq!(paths, ["/a", "/a/b", "/a/b/c", "/a/escape", "/a/f.txt"]);
		let file = depth3.iter().find(|e| e.path == "/a/f.txt").unwrap();
		assert_eq!((file.kind, file.size, file.name.as_str()), ("file", 5, "f.txt"));
	}

	/// Drives the content API over real HTTP like the driver's forwarder.
	#[tokio::test]
	async fn content_api_roundtrip_and_error_codes() {
		let root = tempfile::tempdir().unwrap();
		let store = VolumeStore::new(root.path().to_owned()).unwrap();
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let base = format!("http://{}/volumes/vol1", listener.local_addr().unwrap());
		tokio::spawn(async move { axum::serve(listener, routes(store)).await });
		let http = reqwest::Client::new();

		// Content calls on a volume that was never created are 404.
		let missing = http
			.get(format!("{base}/content/path?path=/"))
			.send()
			.await
			.unwrap();
		assert_eq!(missing.status(), 404);
		assert_eq!(http.post(&base).send().await.unwrap().status(), 201);

		let put = |path: &str, force: bool, body: &'static str| {
			http
				.put(format!("{base}/content/file?path={path}&mode=384&force={force}"))
				.body(body)
				.send()
		};
		let created = put("/a/b/hello.txt", false, "hi there").await.unwrap();
		assert_eq!(created.status(), 201);
		let stat: serde_json::Value = created.json().await.unwrap();
		assert_eq!(
			(stat["type"].as_str(), stat["size"].as_i64(), stat["mode"].as_u64()),
			(Some("file"), Some(8), Some(0o600))
		);
		assert_eq!(stat["path"], "/a/b/hello.txt");
		// Without force an existing file is a conflict; with force it is
		// replaced.
		assert_eq!(put("/a/b/hello.txt", false, "x").await.unwrap().status(), 409);
		assert_eq!(
			put("/a/b/hello.txt", true, "replaced")
				.await
				.unwrap()
				.status(),
			201
		);
		let body = http
			.get(format!("{base}/content/file?path=a/b/hello.txt"))
			.send()
			.await
			.unwrap();
		assert_eq!(body.status(), 200);
		assert_eq!(body.text().await.unwrap(), "replaced");

		let listing: Vec<serde_json::Value> = http
			.get(format!("{base}/content/dir?path=/&depth=3"))
			.send()
			.await
			.unwrap()
			.json()
			.await
			.unwrap();
		let paths: Vec<_> = listing
			.iter()
			.map(|e| e["path"].as_str().unwrap().to_owned())
			.collect();
		assert_eq!(paths, ["/a", "/a/b", "/a/b/hello.txt"]);

		let patched = http
			.patch(format!("{base}/content/path?path=/a/b/hello.txt"))
			.json(&serde_json::json!({ "mode": 0o640 }))
			.send()
			.await
			.unwrap();
		assert_eq!(patched.json::<serde_json::Value>().await.unwrap()["mode"], 0o640);
		assert_eq!(
			http
				.post(format!("{base}/content/dir?path=/x/y"))
				.send()
				.await
				.unwrap()
				.status(),
			404,
			"no parents without force"
		);
		assert_eq!(
			http
				.post(format!("{base}/content/dir?path=/x/y&force=true"))
				.send()
				.await
				.unwrap()
				.status(),
			201
		);

		let escape = http
			.get(format!("{base}/content/file?path=../../etc/passwd"))
			.send()
			.await
			.unwrap();
		assert_eq!(escape.status(), 400);
		let error: serde_json::Value = escape.json().await.unwrap();
		assert_eq!(error["code"], "invalid");
		assert_eq!(
			http
				.delete(format!("{base}/content/path?path=/"))
				.send()
				.await
				.unwrap()
				.status(),
			400,
			"root is not deletable"
		);
		assert_eq!(
			http
				.delete(format!("{base}/content/path?path=/a"))
				.send()
				.await
				.unwrap()
				.status(),
			204
		);
		assert_eq!(
			http
				.get(format!("{base}/content/path?path=/a"))
				.send()
				.await
				.unwrap()
				.status(),
			404
		);
	}

	#[test]
	fn volume_ids_are_validated() {
		let store = VolumeStore::new(tempfile::tempdir().unwrap().keep()).unwrap();
		assert!(store.dir("../x").is_err());
		assert!(store.dir("a/b").is_err());
		assert!(store.dir("0b8f3c9e-1d2a-4f5b-8c7d-6e5f4a3b2c1d").is_ok());
		store.create("v1").unwrap();
		store.create("v1").unwrap();
		store.delete("v1").unwrap();
		store.delete("v1").unwrap();
	}
}
