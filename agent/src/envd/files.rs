//! envd file transfer: `GET /files` (streamed download, gzip / identity
//! negotiation, single byte ranges, conditional GET), `POST /files`
//! (multipart `file` parts or raw octet-stream, gzip request bodies,
//! `X-Metadata-*` → xattrs, parents created and everything chowned to the
//! user), and `POST /files/compose`.

use std::{
	collections::{HashMap, HashSet},
	io::{self, Write},
	os::unix::fs::{MetadataExt, OpenOptionsExt},
	path::{Path, PathBuf},
	sync::Arc,
};

use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::{HeaderMap, HeaderValue, StatusCode, header};
use hyper::body::Incoming;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use super::{
	auth::{self, FileOp, SignedQuery},
	entry::write_metadata,
	filesystem::{chown_to, ensure_dirs},
	http::{Resp, body_stream, channel, error, json, read_body, response},
	paths,
	state::{EnvdState, UserError},
	time,
	users::User,
};

/// Download chunk size.
const CHUNK: usize = 64 << 10;
/// Metadata limits (envd `upload.go`).
const MAX_METADATA_KEY: usize = 246;
const MAX_METADATA_TOTAL: usize = 4096;
const METADATA_HEADER_PREFIX: &str = "x-metadata-";

/// Decoded query parameters (`+` is not treated as space, like Go's
/// `url.ParseQuery` for paths percent-encoded by the SDKs).
pub fn parse_query(query: Option<&str>) -> HashMap<String, String> {
	let mut out = HashMap::new();
	for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
		let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
		let dec = |s: &str| {
			percent_encoding::percent_decode_str(&s.replace('+', " "))
				.decode_utf8_lossy()
				.into_owned()
		};
		out.entry(dec(k)).or_insert_with(|| dec(v));
	}
	out
}

/// Authorize, then resolve user + path for a `/files` request.
fn prepare(
	state: &EnvdState,
	headers: &HeaderMap,
	query: &HashMap<String, String>,
	op: FileOp,
) -> Result<(User, Option<String>), Box<Resp>> {
	let get = |k: &str| query.get(k).map(String::as_str);
	let signed = SignedQuery {
		path:       get("path"),
		username:   get("username"),
		signature:  get("signature"),
		expiration: get("signature_expiration"),
	};
	let token = state.access_token();
	auth::validate_file_access(headers, token.as_deref(), op, &signed, time::now_unix())
		.map_err(|msg| Box::new(error(StatusCode::UNAUTHORIZED, msg)))?;
	let user = state.resolve_user(get("username")).map_err(|e| match e {
		UserError::Unknown(_) => Box::new(error(StatusCode::UNAUTHORIZED, e.to_string())),
		UserError::Lookup(_) => Box::new(error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
	})?;
	Ok((user, get("path").map(str::to_owned)))
}

/// Content encodings a client accepts, best first (`None` = 406).
pub fn negotiate_encoding(accept: Option<&str>) -> Option<&'static str> {
	let Some(accept) = accept.filter(|a| !a.trim().is_empty()) else {
		return Some("identity");
	};
	let mut gzip_q = None;
	let mut identity_q = None;
	let mut star_q = None;
	for part in accept.split(',') {
		let mut it = part.split(';');
		let coding = it.next().unwrap_or("").trim().to_ascii_lowercase();
		let q = it
			.find_map(|p| {
				p.trim()
					.strip_prefix("q=")
					.and_then(|v| v.trim().parse::<f32>().ok())
			})
			.unwrap_or(1.0);
		match coding.as_str() {
			"gzip" => gzip_q = Some(q),
			"identity" => identity_q = Some(q),
			"*" => star_q = Some(q),
			_ => {},
		}
	}
	let gzip = gzip_q.or(star_q).unwrap_or(0.0);
	// identity is acceptable unless explicitly (or via *) refused.
	let identity = identity_q
		.or_else(|| star_q.filter(|&q| q <= 0.0))
		.unwrap_or(1.0);
	if gzip > 0.0 && gzip >= identity {
		Some("gzip")
	} else if identity > 0.0 {
		Some("identity")
	} else if gzip > 0.0 {
		Some("gzip")
	} else {
		None
	}
}

/// MIME type by extension (Go `mime.TypeByExtension` subset).
fn mime_for(path: &Path) -> &'static str {
	match path
		.extension()
		.and_then(|e| e.to_str())
		.map(str::to_ascii_lowercase)
		.as_deref()
	{
		Some("txt" | "log" | "md") => "text/plain; charset=utf-8",
		Some("html" | "htm") => "text/html; charset=utf-8",
		Some("css") => "text/css; charset=utf-8",
		Some("js" | "mjs") => "text/javascript; charset=utf-8",
		Some("json") => "application/json",
		Some("xml") => "text/xml; charset=utf-8",
		Some("csv") => "text/csv; charset=utf-8",
		Some("png") => "image/png",
		Some("jpg" | "jpeg") => "image/jpeg",
		Some("gif") => "image/gif",
		Some("svg") => "image/svg+xml",
		Some("webp") => "image/webp",
		Some("pdf") => "application/pdf",
		Some("zip") => "application/zip",
		Some("gz") => "application/gzip",
		Some("tar") => "application/x-tar",
		Some("wasm") => "application/wasm",
		_ => "application/octet-stream",
	}
}

/// Parse a single `bytes=a-b` / `bytes=a-` / `bytes=-n` range against `size`.
/// `Ok(None)` = ignore the header; `Err` = 416.
pub fn parse_range(value: &str, size: u64) -> Result<Option<(u64, u64)>, ()> {
	let Some(spec) = value.trim().strip_prefix("bytes=") else {
		return Ok(None);
	};
	if spec.contains(',') {
		// Multipart ranges are not supported: serve the whole file.
		return Ok(None);
	}
	let (a, b) = spec.split_once('-').ok_or(())?;
	let (start, end) = match (a.trim(), b.trim()) {
		("", n) => {
			let n: u64 = n.parse().map_err(|_| ())?;
			if n == 0 {
				return Err(());
			}
			(size.saturating_sub(n), size.checked_sub(1).ok_or(())?)
		},
		(s, "") => (s.parse().map_err(|_| ())?, size.checked_sub(1).ok_or(())?),
		(s, e) => {
			let (s, e): (u64, u64) = (s.parse().map_err(|_| ())?, e.parse().map_err(|_| ())?);
			(s, e.min(size.saturating_sub(1)))
		},
	};
	if start >= size || start > end {
		return Err(());
	}
	Ok(Some((start, end)))
}

/// `GET /files`.
pub async fn download(state: &Arc<EnvdState>, headers: &HeaderMap, query: Option<&str>) -> Resp {
	let query = parse_query(query);
	let (user, raw_path) = match prepare(state, headers, &query, FileOp::Read) {
		Ok(v) => v,
		Err(resp) => return *resp,
	};
	let workdir = state.workdir();
	let path =
		match paths::resolve(raw_path.as_deref().unwrap_or(""), &user.home, workdir.as_deref()) {
			Ok(p) => p,
			Err(e) => return error(StatusCode::BAD_REQUEST, e),
		};
	let meta = match tokio::fs::metadata(&path).await {
		Ok(m) => m,
		Err(e) if e.kind() == io::ErrorKind::NotFound => {
			return error(StatusCode::NOT_FOUND, format!("path '{}' does not exist", path.display()));
		},
		Err(e) => {
			return error(StatusCode::INTERNAL_SERVER_ERROR, format!("error checking path: {e}"));
		},
	};
	if meta.is_dir() {
		return error(StatusCode::BAD_REQUEST, format!("path '{}' is a directory", path.display()));
	}
	let hv = |h: header::HeaderName| headers.get(h).and_then(|v| v.to_str().ok());
	let wants_partial = headers.contains_key(header::RANGE)
		|| headers.contains_key(header::IF_MODIFIED_SINCE)
		|| headers.contains_key(header::IF_NONE_MATCH);
	let Some(mut encoding) = negotiate_encoding(hv(header::ACCEPT_ENCODING)) else {
		return error(StatusCode::NOT_ACCEPTABLE, "no acceptable encoding found, supported: [gzip]");
	};
	if wants_partial {
		if negotiate_encoding(
			hv(header::ACCEPT_ENCODING)
				.map(|a| format!("{a}, gzip;q=0"))
				.as_deref(),
		) != Some("identity")
		{
			return error(
				StatusCode::NOT_ACCEPTABLE,
				"no acceptable encoding found, supported: [identity]",
			);
		}
		encoding = "identity";
	}
	let file = match tokio::fs::File::open(&path).await {
		Ok(f) => f,
		Err(e) => {
			return error(StatusCode::INTERNAL_SERVER_ERROR, format!("error opening file: {e}"));
		},
	};
	let name = path
		.file_name()
		.map(|n| n.to_string_lossy().replace('"', "\\\""))
		.unwrap_or_default();
	let size = meta.len();
	let mtime = meta.mtime();

	let mut resp = if encoding == "gzip" {
		let (tx, body) = channel(4);
		let mut file = file.into_std().await;
		tokio::task::spawn_blocking(move || {
			struct ChanWriter(tokio::sync::mpsc::Sender<Result<Bytes, io::Error>>);
			impl Write for ChanWriter {
				fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
					self
						.0
						.blocking_send(Ok(Bytes::copy_from_slice(buf)))
						.map_err(|_| io::Error::from(io::ErrorKind::BrokenPipe))?;
					Ok(buf.len())
				}

				fn flush(&mut self) -> io::Result<()> {
					Ok(())
				}
			}
			let err_tx = tx.clone();
			let mut enc = flate2::write::GzEncoder::new(
				io::BufWriter::with_capacity(CHUNK, ChanWriter(tx)),
				flate2::Compression::fast(),
			);
			let result = io::copy(&mut file, &mut enc).and_then(|_| enc.finish()?.flush());
			if let Err(e) = result
				&& e.kind() != io::ErrorKind::BrokenPipe
			{
				let _ = err_tx.blocking_send(Err(e));
			}
		});
		let mut r = response(StatusCode::OK, body);
		r.headers_mut()
			.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
		r.headers_mut()
			.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime_for(&path)));
		r
	} else {
		if let Some(since) = hv(header::IF_MODIFIED_SINCE).and_then(time::parse_http_date)
			&& mtime <= since
		{
			let mut r = response(StatusCode::NOT_MODIFIED, super::http::empty());
			r.headers_mut().insert(
				header::LAST_MODIFIED,
				HeaderValue::from_str(&time::http_date(mtime)).unwrap_or(HeaderValue::from_static("")),
			);
			return r;
		}
		let range = match hv(header::RANGE).map(|r| parse_range(r, size)) {
			Some(Err(())) => {
				let mut r = error(StatusCode::RANGE_NOT_SATISFIABLE, "invalid range");
				if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
					r.headers_mut().insert(header::CONTENT_RANGE, v);
				}
				return r;
			},
			Some(Ok(r)) => r,
			None => None,
		};
		let (start, len) = range.map_or((0, size), |(s, e)| (s, e - s + 1));
		let (tx, body) = channel(4);
		tokio::spawn(async move {
			let mut file = file;
			if start > 0 && file.seek(io::SeekFrom::Start(start)).await.is_err() {
				return;
			}
			let mut remaining = len;
			let mut buf = vec![0u8; CHUNK];
			while remaining > 0 {
				let want = usize::try_from(remaining.min(CHUNK as u64)).unwrap_or(CHUNK);
				match file.read(&mut buf[..want]).await {
					Ok(0) => break,
					Ok(n) => {
						remaining -= n as u64;
						if tx
							.send(Ok(Bytes::copy_from_slice(&buf[..n])))
							.await
							.is_err()
						{
							return;
						}
					},
					Err(e) => {
						let _ = tx.send(Err(e)).await;
						return;
					},
				}
			}
		});
		let status = if range.is_some() {
			StatusCode::PARTIAL_CONTENT
		} else {
			StatusCode::OK
		};
		let mut r = response(status, body);
		let h = r.headers_mut();
		h.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
		h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
		h.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime_for(&path)));
		if let Ok(v) = HeaderValue::from_str(&time::http_date(mtime)) {
			h.insert(header::LAST_MODIFIED, v);
		}
		if let Some((s, e)) = range
			&& let Ok(v) = HeaderValue::from_str(&format!("bytes {s}-{e}/{size}"))
		{
			h.insert(header::CONTENT_RANGE, v);
		}
		r
	};
	let h = resp.headers_mut();
	h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
	if let Ok(v) = HeaderValue::from_str(&format!("inline; filename=\"{name}\"")) {
		h.insert(header::CONTENT_DISPOSITION, v);
	}
	resp
}

/// One written file in the upload response.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct WrittenEntry {
	pub path:     String,
	pub name:     String,
	#[serde(rename = "type")]
	pub kind:     &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub metadata: Option<HashMap<String, String>>,
}

/// `X-Metadata-*` headers → (lowercased key, value), validated.
pub fn parse_metadata(headers: &HeaderMap) -> Result<Vec<(String, String)>, String> {
	let mut out: Vec<(String, String)> = Vec::new();
	let mut total = 0;
	for name in headers.keys() {
		let Some(key) = name.as_str().strip_prefix(METADATA_HEADER_PREFIX) else {
			continue;
		};
		if key.is_empty() {
			return Err("metadata key must not be empty".into());
		}
		let value = headers
			.get(name)
			.and_then(|v| v.to_str().ok())
			.ok_or_else(|| format!("metadata value for '{key}' must be printable ASCII"))?;
		let printable = |s: &str| s.bytes().all(|b| (0x20..0x7f).contains(&b));
		if !printable(key) || !printable(value) {
			return Err(format!("metadata '{key}' must be printable ASCII"));
		}
		if key.len() > MAX_METADATA_KEY {
			return Err(format!("metadata key '{key}' exceeds {MAX_METADATA_KEY} bytes"));
		}
		total += key.len() + value.len();
		if total > MAX_METADATA_TOTAL {
			return Err(format!("metadata exceeds {MAX_METADATA_TOTAL} bytes in total"));
		}
		out.push((key.to_owned(), value.to_owned()));
	}
	out.sort();
	Ok(out)
}

enum UploadError {
	BadRequest(String),
	NoSpace(String),
	Internal(String),
}

impl UploadError {
	fn from_io(context: &str, e: &io::Error) -> Self {
		match e.raw_os_error() {
			Some(libc::ENOSPC | libc::EDQUOT) => {
				Self::NoSpace(format!("not enough disk space available: {context}: {e}"))
			},
			_ => Self::Internal(format!("{context}: {e}")),
		}
	}

	fn into_resp(self) -> Resp {
		match self {
			Self::BadRequest(m) => error(StatusCode::BAD_REQUEST, m),
			Self::NoSpace(m) => error(StatusCode::INSUFFICIENT_STORAGE, m),
			Self::Internal(m) => error(StatusCode::INTERNAL_SERVER_ERROR, m),
		}
	}
}

/// Streaming gzip decoder over a chunk stream.
fn gunzip(
	stream: impl Stream<Item = Result<Bytes, io::Error>> + Send + 'static,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send {
	let decoder = flate2::write::GzDecoder::new(Vec::new());
	futures::stream::unfold(
		(Box::pin(stream), Some(decoder)),
		|(mut stream, mut decoder)| async move {
			let dec = decoder.as_mut()?;
			loop {
				match stream.next().await {
					Some(Ok(chunk)) => {
						if let Err(e) = dec.write_all(&chunk) {
							return Some((
								Err(io::Error::new(io::ErrorKind::InvalidData, e)),
								(stream, None),
							));
						}
						let out = std::mem::take(dec.get_mut());
						if !out.is_empty() {
							return Some((Ok(Bytes::from(out)), (stream, decoder)));
						}
					},
					Some(Err(e)) => return Some((Err(e), (stream, None))),
					None => {
						let mut dec = decoder.take()?;
						return match dec.try_finish() {
							Ok(()) => {
								let out = std::mem::take(dec.get_mut());
								Some((Ok(Bytes::from(out)), (stream, None)))
							},
							Err(e) => {
								Some((Err(io::Error::new(io::ErrorKind::InvalidData, e)), (stream, None)))
							},
						};
					},
				}
			}
		},
	)
}

/// Write one file from a chunk stream (envd `processFile`).
async fn write_file<S>(
	path: &Path,
	user: &User,
	mut data: S,
	metadata: &[(String, String)],
) -> Result<WrittenEntry, UploadError>
where
	S: Stream<Item = Result<Bytes, io::Error>> + Unpin,
{
	if let Some(parent) = path.parent() {
		ensure_dirs(parent, user).map_err(|e| match e.kind() {
			io::ErrorKind::AlreadyExists => {
				UploadError::BadRequest(format!("error ensuring directories: {e}"))
			},
			_ => UploadError::from_io("error ensuring directories", &e),
		})?;
	}
	if tokio::fs::metadata(path).await.is_ok_and(|m| m.is_dir()) {
		return Err(UploadError::BadRequest(format!("path '{}' is a directory", path.display())));
	}
	let mut file = tokio::fs::OpenOptions::new()
		.write(true)
		.create(true)
		.truncate(true)
		.mode(0o666)
		.open(path)
		.await
		.map_err(|e| UploadError::from_io("error opening file", &e))?;
	chown_to(path, user).map_err(|e| UploadError::from_io("error changing file ownership", &e))?;
	while let Some(chunk) = data.next().await {
		let chunk = chunk.map_err(|e| match e.kind() {
			io::ErrorKind::InvalidData => {
				UploadError::BadRequest(format!("error decoding request body: {e}"))
			},
			_ => UploadError::Internal(format!("error reading request body: {e}")),
		})?;
		file
			.write_all(&chunk)
			.await
			.map_err(|e| UploadError::from_io("error writing file", &e))?;
	}
	file
		.flush()
		.await
		.map_err(|e| UploadError::from_io("error writing file", &e))?;
	drop(file);
	if let Err(e) = write_metadata(path, metadata) {
		return Err(UploadError::from_io("error writing metadata", &e));
	}
	Ok(WrittenEntry {
		path:     path.to_string_lossy().into_owned(),
		name:     path
			.file_name()
			.map(|n| n.to_string_lossy().into_owned())
			.unwrap_or_default(),
		kind:     "file",
		metadata: (!metadata.is_empty()).then(|| metadata.iter().cloned().collect()),
	})
}

/// `POST /files`.
pub async fn upload(
	state: &Arc<EnvdState>,
	headers: &HeaderMap,
	query: Option<&str>,
	body: Incoming,
) -> Resp {
	let query = parse_query(query);
	let (user, query_path) = match prepare(state, headers, &query, FileOp::Write) {
		Ok(v) => v,
		Err(resp) => return *resp,
	};
	let gzip = match headers
		.get(header::CONTENT_ENCODING)
		.map(|v| v.to_str().unwrap_or("?").trim().to_ascii_lowercase())
	{
		None => false,
		Some(e) if e.is_empty() || e == "identity" => false,
		Some(e) if e == "gzip" => true,
		Some(e) => {
			return error(StatusCode::BAD_REQUEST, format!("unsupported Content-Encoding: {e}"));
		},
	};
	let metadata = match parse_metadata(headers) {
		Ok(m) => m,
		Err(e) => return error(StatusCode::BAD_REQUEST, e),
	};
	let raw = body_stream(body);
	let data: std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>> = if gzip {
		Box::pin(gunzip(raw))
	} else {
		Box::pin(raw)
	};
	let content_type = headers
		.get(header::CONTENT_TYPE)
		.and_then(|v| v.to_str().ok())
		.unwrap_or("");
	let essence = content_type
		.split(';')
		.next()
		.unwrap_or("")
		.trim()
		.to_ascii_lowercase();
	let workdir = state.workdir();
	let resolve = |p: &str| paths::resolve(p, &user.home, workdir.as_deref());

	if essence == "application/octet-stream" {
		let Some(raw_path) = query_path.filter(|p| !p.is_empty()) else {
			return error(
				StatusCode::BAD_REQUEST,
				"path query parameter is required for raw body upload",
			);
		};
		let path = match resolve(&raw_path) {
			Ok(p) => p,
			Err(e) => return error(StatusCode::BAD_REQUEST, e),
		};
		return match write_file(&path, &user, data, &metadata).await {
			Ok(entry) => json(StatusCode::OK, &vec![entry]),
			Err(e) => e.into_resp(),
		};
	}
	if !essence.starts_with("multipart/") {
		return error(StatusCode::BAD_REQUEST, format!("unsupported content type: {content_type}"));
	}
	let boundary = match multer::parse_boundary(content_type) {
		Ok(b) => b,
		Err(e) => {
			return error(StatusCode::BAD_REQUEST, format!("invalid multipart content type: {e}"));
		},
	};
	let mut multipart = multer::Multipart::new(data, boundary);
	let mut written = Vec::new();
	let mut seen: HashSet<PathBuf> = HashSet::new();
	loop {
		let field = match multipart.next_field().await {
			Ok(Some(f)) => f,
			Ok(None) => break,
			Err(e) => {
				return error(StatusCode::BAD_REQUEST, format!("error parsing multipart body: {e}"));
			},
		};
		if field.name() != Some("file") {
			continue;
		}
		let target = match query_path.as_deref().filter(|p| !p.is_empty()) {
			Some(p) => p.to_owned(),
			None => match field.file_name() {
				Some(f) if !f.is_empty() => f.to_owned(),
				_ => {
					return error(
						StatusCode::BAD_REQUEST,
						"file part has no filename and no path query parameter",
					);
				},
			},
		};
		let path = match resolve(&target) {
			Ok(p) => p,
			Err(e) => return error(StatusCode::BAD_REQUEST, e),
		};
		if !seen.insert(path.clone()) {
			return error(
				StatusCode::BAD_REQUEST,
				format!(
					"you cannot upload multiple files to the same path '{}' in one upload request, \
					 only the last specified one will be uploaded",
					path.display()
				),
			);
		}
		let stream = field.map(|r| r.map_err(io::Error::other));
		match write_file(&path, &user, stream, &metadata).await {
			Ok(entry) => written.push(entry),
			Err(e) => return e.into_resp(),
		}
	}
	json(StatusCode::OK, &written)
}

#[derive(Deserialize)]
struct ComposeRequest {
	source_paths: Vec<String>,
	destination:  String,
	#[serde(default)]
	username:     Option<String>,
}

/// `POST /files/compose`: concatenate sources (`copy_file_range` via
/// `io::copy`) into a temp file next to the destination, rename it into
/// place, then delete the sources.
pub async fn compose(state: &Arc<EnvdState>, body: Incoming) -> Resp {
	let body = match read_body(body, 1 << 20).await {
		Ok(b) => b,
		Err(e) => return error(StatusCode::BAD_REQUEST, e.to_string()),
	};
	let req: ComposeRequest = match serde_json::from_slice(&body) {
		Ok(r) => r,
		Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid request body: {e}")),
	};
	if req.source_paths.is_empty() {
		return error(StatusCode::BAD_REQUEST, "source_paths must not be empty");
	}
	let user = match state.resolve_user(req.username.as_deref()) {
		Ok(u) => u,
		Err(e @ UserError::Unknown(_)) => return error(StatusCode::UNAUTHORIZED, e.to_string()),
		Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
	};
	let workdir = state.workdir();
	let resolve = |p: &str| paths::resolve(p, &user.home, workdir.as_deref());
	let mut sources = Vec::new();
	for s in &req.source_paths {
		match resolve(s) {
			Ok(p) => sources.push(p),
			Err(e) => return error(StatusCode::BAD_REQUEST, e),
		}
	}
	let destination = match resolve(&req.destination) {
		Ok(p) => p,
		Err(e) => return error(StatusCode::BAD_REQUEST, e),
	};
	let result =
		tokio::task::spawn_blocking(move || compose_blocking(&sources, &destination, &user))
			.await
			.unwrap_or_else(|e| Err(UploadError::Internal(e.to_string())));
	match result {
		Ok(entry) => json(StatusCode::OK, &entry),
		Err(UploadError::BadRequest(m)) if m.starts_with("source not found") => {
			error(StatusCode::NOT_FOUND, m)
		},
		Err(e) => e.into_resp(),
	}
}

fn compose_blocking(
	sources: &[PathBuf],
	destination: &Path,
	user: &User,
) -> Result<WrittenEntry, UploadError> {
	for s in sources {
		match std::fs::metadata(s) {
			Ok(m) if m.is_dir() => {
				return Err(UploadError::BadRequest(format!(
					"source '{}' is a directory",
					s.display()
				)));
			},
			Ok(_) => {},
			Err(e) if e.kind() == io::ErrorKind::NotFound => {
				return Err(UploadError::BadRequest(format!("source not found: '{}'", s.display())));
			},
			Err(e) => return Err(UploadError::from_io("error checking source", &e)),
		}
	}
	if std::fs::metadata(destination).is_ok_and(|m| m.is_dir()) {
		return Err(UploadError::BadRequest(format!(
			"destination '{}' is a directory",
			destination.display()
		)));
	}
	let parent = destination.parent().unwrap_or_else(|| Path::new("/"));
	ensure_dirs(parent, user).map_err(|e| UploadError::from_io("error ensuring directories", &e))?;
	let tmp = parent.join(format!(
		".{}.compose-{}",
		destination
			.file_name()
			.map(|n| n.to_string_lossy().into_owned())
			.unwrap_or_default(),
		std::process::id()
	));
	let result = (|| {
		let mut out = std::fs::OpenOptions::new()
			.write(true)
			.create(true)
			.truncate(true)
			.mode(0o666)
			.open(&tmp)?;
		for s in sources {
			io::copy(&mut std::fs::File::open(s)?, &mut out)?;
		}
		out.sync_data()?;
		Ok::<(), io::Error>(())
	})();
	if let Err(e) = result {
		let _ = std::fs::remove_file(&tmp);
		return Err(UploadError::from_io("error composing file", &e));
	}
	chown_to(&tmp, user).map_err(|e| UploadError::from_io("error changing file ownership", &e))?;
	std::fs::rename(&tmp, destination)
		.map_err(|e| UploadError::from_io("error moving composed file", &e))?;
	for s in sources {
		if s != destination {
			let _ = std::fs::remove_file(s);
		}
	}
	Ok(WrittenEntry {
		path:     destination.to_string_lossy().into_owned(),
		name:     destination
			.file_name()
			.map(|n| n.to_string_lossy().into_owned())
			.unwrap_or_default(),
		kind:     "file",
		metadata: None,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn query_decoding() {
		let q = parse_query(Some("path=%2Fhome%2Fuser%2Fa%20b.txt&username=user&path=ignored"));
		assert_eq!(q["path"], "/home/user/a b.txt");
		assert_eq!(q["username"], "user");
		assert!(parse_query(None).is_empty());
	}

	#[test]
	fn encoding_negotiation() {
		assert_eq!(negotiate_encoding(None), Some("identity"));
		assert_eq!(negotiate_encoding(Some("gzip")), Some("gzip"));
		assert_eq!(negotiate_encoding(Some("gzip, deflate, br")), Some("gzip"));
		assert_eq!(negotiate_encoding(Some("br")), Some("identity"));
		assert_eq!(negotiate_encoding(Some("gzip;q=0.5, identity")), Some("identity"));
		assert_eq!(negotiate_encoding(Some("gzip;q=0")), Some("identity"));
		assert_eq!(negotiate_encoding(Some("identity;q=0, gzip;q=0")), None);
		assert_eq!(negotiate_encoding(Some("*;q=0")), None);
		assert_eq!(negotiate_encoding(Some("br, *;q=0")), None);
		assert_eq!(negotiate_encoding(Some("*")), Some("gzip"));
		assert_eq!(negotiate_encoding(Some("identity;q=0, gzip")), Some("gzip"));
	}

	#[test]
	fn range_parsing() {
		assert_eq!(parse_range("bytes=0-4", 10), Ok(Some((0, 4))));
		assert_eq!(parse_range("bytes=5-", 10), Ok(Some((5, 9))));
		assert_eq!(parse_range("bytes=-3", 10), Ok(Some((7, 9))));
		assert_eq!(parse_range("bytes=8-100", 10), Ok(Some((8, 9))));
		assert_eq!(parse_range("bytes=10-", 10), Err(()));
		assert_eq!(parse_range("bytes=5-2", 10), Err(()));
		assert_eq!(parse_range("bytes=0-1,3-4", 10), Ok(None));
		assert_eq!(parse_range("items=0-1", 10), Ok(None));
		assert_eq!(parse_range("bytes=0-0", 0), Err(()));
	}

	#[test]
	fn metadata_validation() {
		let mut h = HeaderMap::new();
		h.insert("x-metadata-Owner", "alice".parse().unwrap());
		h.insert("x-metadata-tag", "v1".parse().unwrap());
		h.insert("x-other", "ignored".parse().unwrap());
		assert_eq!(parse_metadata(&h).unwrap(), vec![
			("owner".to_owned(), "alice".to_owned()),
			("tag".to_owned(), "v1".to_owned())
		]);
		let mut big = HeaderMap::new();
		big.insert("x-metadata-a", "x".repeat(4096).parse().unwrap());
		assert!(parse_metadata(&big).is_err());
		let mut long_key = HeaderMap::new();
		long_key.insert(
			http::HeaderName::from_bytes(format!("x-metadata-{}", "k".repeat(247)).as_bytes())
				.unwrap(),
			"v".parse().unwrap(),
		);
		assert!(parse_metadata(&long_key).is_err());
		let mut nonprint = HeaderMap::new();
		nonprint.insert("x-metadata-a", HeaderValue::from_bytes(b"caf\xc3\xa9").unwrap());
		assert!(parse_metadata(&nonprint).is_err());
	}

	#[tokio::test]
	async fn gunzip_stream_handles_split_input_and_rejects_garbage() {
		let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
		enc.write_all(&b"hello world ".repeat(1000)).unwrap();
		let gz = enc.finish().unwrap();
		let chunks: Vec<Result<Bytes, io::Error>> = gz
			.chunks(7)
			.map(|c| Ok(Bytes::copy_from_slice(c)))
			.collect();
		let out: Vec<u8> = gunzip(futures::stream::iter(chunks))
			.map(|r| r.unwrap())
			.collect::<Vec<_>>()
			.await
			.concat();
		assert_eq!(out, b"hello world ".repeat(1000));
		let bad = gunzip(futures::stream::iter(vec![Ok(Bytes::from_static(b"not gzip at all"))]));
		let results: Vec<_> = bad.collect().await;
		assert!(results.iter().any(Result::is_err));
	}

	#[test]
	fn compose_concatenates_and_allows_destination_among_sources() {
		// SAFETY: getuid/getgid are always safe.
		let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
		let user = User {
			name: "me".into(),
			uid,
			gid,
			home: std::env::temp_dir(),
			shell: "/bin/sh".into(),
			groups: vec![gid],
		};
		let dir = std::env::temp_dir().join(format!("envd-compose-{}", std::process::id()));
		let _ = std::fs::remove_dir_all(&dir);
		std::fs::create_dir_all(&dir).unwrap();
		std::fs::write(dir.join("a"), "AA").unwrap();
		std::fs::write(dir.join("b"), "BB").unwrap();
		let e = compose_blocking(&[dir.join("a"), dir.join("b")], &dir.join("a"), &user)
			.ok()
			.unwrap();
		assert_eq!(e.name, "a");
		assert_eq!(std::fs::read_to_string(dir.join("a")).unwrap(), "AABB");
		assert!(!dir.join("b").exists());
		let missing = compose_blocking(&[dir.join("nope")], &dir.join("c"), &user);
		assert!(
			matches!(missing, Err(UploadError::BadRequest(m)) if m.starts_with("source not found"))
		);
		std::fs::remove_dir_all(&dir).unwrap();
	}
}
