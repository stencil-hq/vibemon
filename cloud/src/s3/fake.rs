//! In-process S3 server for unit tests (cargo feature `fake`).
//!
//! Serves one path-style endpoint on loopback with any number of buckets
//! (created on first write) and verifies the `SigV4` signature of every
//! request against [`FakeS3::credentials`]. Supports `PUT`/`GET` (ranges,
//! `If-Match`)/`HEAD`/`DELETE`, conditional writes, user metadata,
//! `ListObjectsV2` (prefix, delimiter, continuation, max-keys), multipart
//! uploads, `CopyObject`, and `UploadPartCopy`. [`FakeS3::set_hook`] injects
//! failures or observes requests.

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt::Write as _,
	sync::Arc,
	time::SystemTime,
};

use axum::{
	body::Body,
	extract::{OriginalUri, State},
	http::{HeaderMap, Method, StatusCode, header},
	response::{IntoResponse, Response},
};
use bytes::Bytes;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};

use super::{S3Auth, S3Client, S3Config};
use crate::aws::{self, Credentials};

/// Access key the fake accepts.
pub const ACCESS_KEY: &str = "fake-access";
/// Secret key the fake accepts.
pub const SECRET_KEY: &str = "fake-secret";

/// One stored object.
#[derive(Clone, Debug)]
pub struct FakeObject {
	/// Contents.
	pub data:         Bytes,
	/// Quoted `ETag`.
	pub etag:         String,
	/// User metadata (lowercase names without `x-amz-meta-`).
	pub metadata:     BTreeMap<String, String>,
	/// `Content-Type`, if one was given.
	pub content_type: Option<String>,
	/// Last write time.
	pub modified:     SystemTime,
}

/// What a [`FakeS3::set_hook`] callback sees of each request.
#[derive(Clone, Debug)]
pub struct FakeRequest {
	/// HTTP method.
	pub method: Method,
	/// Decoded `bucket/key` (`bucket/` for bucket-level requests).
	pub path:   String,
	/// Decoded query parameters.
	pub query:  BTreeMap<String, String>,
	/// Body length.
	pub len:    usize,
}

type Hook = Arc<dyn Fn(&FakeRequest) -> Option<StatusCode> + Send + Sync>;

#[derive(Default)]
struct Upload {
	path:     String,
	parts:    BTreeMap<u32, (String, Bytes)>,
	metadata: BTreeMap<String, String>,
}

#[derive(Default)]
struct FakeState {
	objects:  BTreeMap<String, FakeObject>,
	uploads:  BTreeMap<String, Upload>,
	requests: Vec<FakeRequest>,
	hook:     Option<Hook>,
	aborted:  usize,
}

/// A running fake S3 endpoint; stops when dropped.
pub struct FakeS3 {
	state:    Arc<Mutex<FakeState>>,
	endpoint: String,
	server:   tokio::task::JoinHandle<()>,
}

impl Drop for FakeS3 {
	fn drop(&mut self) {
		self.server.abort();
	}
}

impl FakeS3 {
	/// Starts the server on an ephemeral loopback port.
	pub async fn start() -> Self {
		let state = Arc::new(Mutex::new(FakeState::default()));
		let router = axum::Router::new()
			.fallback(handle)
			.layer(axum::extract::DefaultBodyLimit::max(1 << 30))
			.with_state(Arc::clone(&state));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("binding the fake S3 listener");
		let endpoint = format!("http://{}", listener.local_addr().expect("fake S3 address"));
		let server = tokio::spawn(async move {
			let _ = axum::serve(listener, router).await;
		});
		Self { state, endpoint, server }
	}

	/// `http://127.0.0.1:<port>`.
	pub fn endpoint(&self) -> &str {
		&self.endpoint
	}

	/// The credentials every request must be signed with.
	pub fn credentials() -> Credentials {
		Credentials::permanent(ACCESS_KEY, SECRET_KEY, None)
	}

	/// Client settings for `http://<endpoint>/<bucket>/<prefix>`.
	pub fn config(&self, bucket: &str, prefix: &str) -> S3Config {
		S3Config {
			location:   format!("{}/{bucket}/{prefix}", self.endpoint),
			region:     Some("us-east-1".into()),
			endpoint:   None,
			auth:       S3Auth::Explicit(Self::credentials()),
			allow_http: true,
			addressing: None,
		}
	}

	/// A client for `bucket/prefix`.
	pub fn client(&self, bucket: &str, prefix: &str) -> S3Client {
		S3Client::new(&self.config(bucket, prefix)).expect("fake S3 client")
	}

	/// Stored objects by `bucket/key`.
	pub fn objects(&self) -> BTreeMap<String, FakeObject> {
		self.state.lock().objects.clone()
	}

	/// Stored `bucket/key` names.
	pub fn keys(&self) -> Vec<String> {
		self.state.lock().objects.keys().cloned().collect()
	}

	/// Overwrites (or creates) the object `bucket/key` directly.
	pub fn insert(&self, path: &str, data: impl Into<Bytes>) {
		let data = data.into();
		self
			.state
			.lock()
			.objects
			.insert(path.to_owned(), object(data, BTreeMap::new(), None));
	}

	/// Every request served so far.
	pub fn requests(&self) -> Vec<FakeRequest> {
		self.state.lock().requests.clone()
	}

	/// Multipart uploads neither completed nor aborted.
	pub fn open_uploads(&self) -> usize {
		self.state.lock().uploads.len()
	}

	/// Multipart uploads aborted so far.
	pub fn aborted_uploads(&self) -> usize {
		self.state.lock().aborted
	}

	/// Calls `hook` before serving each request; `Some(status)` fails the
	/// request with that status instead.
	pub fn set_hook(
		&self,
		hook: impl Fn(&FakeRequest) -> Option<StatusCode> + Send + Sync + 'static,
	) {
		self.state.lock().hook = Some(Arc::new(hook));
	}

	/// Removes the hook.
	pub fn clear_hook(&self) {
		self.state.lock().hook = None;
	}
}

fn object(
	data: Bytes,
	metadata: BTreeMap<String, String>,
	content_type: Option<String>,
) -> FakeObject {
	let etag = format!("\"{}\"", hex::encode(&Sha256::digest(&data)[..16]));
	FakeObject { data, etag, metadata, content_type, modified: SystemTime::now() }
}

fn error(status: StatusCode, code: &str) -> Response {
	(
		status,
		[(header::CONTENT_TYPE, "application/xml")],
		format!("<?xml version=\"1.0\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>"),
	)
		.into_response()
}

fn xml(body: String) -> Response {
	([(header::CONTENT_TYPE, "application/xml")], body).into_response()
}

fn escape(value: &str) -> String {
	super::xml_escape(value)
}

fn decode(value: &str) -> String {
	let bytes = value.as_bytes();
	let mut out = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] == b'%'
			&& i + 2 < bytes.len()
			&& let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
			&& let Ok(byte) = u8::from_str_radix(hex, 16)
		{
			out.push(byte);
			i += 3;
		} else {
			out.push(bytes[i]);
			i += 1;
		}
	}
	String::from_utf8_lossy(&out).into_owned()
}

fn metadata(headers: &HeaderMap) -> BTreeMap<String, String> {
	headers
		.iter()
		.filter_map(|(name, value)| {
			let name = name.as_str().strip_prefix("x-amz-meta-")?;
			Some((name.to_owned(), value.to_str().ok()?.to_owned()))
		})
		.collect()
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
	headers.get(name).and_then(|value| value.to_str().ok())
}

/// Recomputes the `SigV4` authorization of the request.
fn verify_signature(
	method: &Method,
	uri: &axum::http::Uri,
	headers: &HeaderMap,
	body: &[u8],
) -> bool {
	let Some(authorization) = header_str(headers, "authorization") else {
		return false;
	};
	let Some(signed) = authorization
		.split("SignedHeaders=")
		.nth(1)
		.and_then(|rest| rest.split(',').next())
	else {
		return false;
	};
	let Some(date) = header_str(headers, "x-amz-date") else {
		return false;
	};
	let Ok(timestamp) = chrono::NaiveDateTime::parse_from_str(date, "%Y%m%dT%H%M%SZ") else {
		return false;
	};
	let hash = header_str(headers, "x-amz-content-sha256").unwrap_or_default();
	if hash != super::UNSIGNED_PAYLOAD && hash != aws::sha256_hex(body) {
		return false;
	}
	let mut signed_headers = Vec::new();
	for name in signed.split(';') {
		let Some(value) = header_str(headers, name) else {
			return false;
		};
		signed_headers.push((name.to_owned(), value.to_owned()));
	}
	let Some(host) = header_str(headers, "host") else {
		return false;
	};
	let Ok(url) = reqwest::Url::parse(&format!("http://{host}{uri}")) else {
		return false;
	};
	let Ok((canonical_uri, query, _)) = aws::canonical_url(&url) else {
		return false;
	};
	let expected = aws::authorization(
		method.as_str(),
		&canonical_uri,
		&query,
		&signed_headers,
		hash,
		timestamp.and_utc(),
		"us-east-1",
		"s3",
		ACCESS_KEY,
		SECRET_KEY,
	);
	expected == authorization
}

async fn handle(
	State(state): State<Arc<Mutex<FakeState>>>,
	OriginalUri(uri): OriginalUri,
	method: Method,
	headers: HeaderMap,
	body: Bytes,
) -> Response {
	if !verify_signature(&method, &uri, &headers, &body) {
		return error(StatusCode::FORBIDDEN, "SignatureDoesNotMatch");
	}
	let path = decode(uri.path().trim_start_matches('/'));
	let query: BTreeMap<String, String> = uri
		.query()
		.unwrap_or_default()
		.split('&')
		.filter(|pair| !pair.is_empty())
		.map(|pair| {
			let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
			(decode(name), decode(value))
		})
		.collect();
	let request = FakeRequest { method, path, query, len: body.len() };
	let hook = {
		let mut state = state.lock();
		state.requests.push(request.clone());
		state.hook.clone()
	};
	if let Some(status) = hook.and_then(|hook| hook(&request))
		&& status != StatusCode::OK
	{
		return error(status, "InjectedFault");
	}
	let mut state = state.lock();
	serve(&mut state, &request, &headers, body)
}

fn serve(
	state: &mut FakeState,
	request: &FakeRequest,
	headers: &HeaderMap,
	body: Bytes,
) -> Response {
	let (path, query, method) = (&request.path, &request.query, &request.method);
	let (bucket, key) = path.split_once('/').unwrap_or((path.as_str(), ""));
	if key.is_empty() {
		return if *method == Method::GET && query.get("list-type").map(String::as_str) == Some("2") {
			list(state, bucket, query)
		} else {
			error(StatusCode::NOT_IMPLEMENTED, "NotImplemented")
		};
	}
	if *method == Method::POST && query.contains_key("uploads") {
		let id = format!("{:032x}", state.uploads.len() as u128 + rand_id(path));
		state.uploads.insert(id.clone(), Upload {
			path:     path.clone(),
			parts:    BTreeMap::new(),
			metadata: metadata(headers),
		});
		return xml(format!(
			"<InitiateMultipartUploadResult><Bucket>{bucket}</Bucket><Key>{}</Key><UploadId>{id}</\
			 UploadId></InitiateMultipartUploadResult>",
			escape(key)
		));
	}
	if let Some(id) = query.get("uploadId") {
		let Some(upload) = state.uploads.get(id) else {
			return error(StatusCode::NOT_FOUND, "NoSuchUpload");
		};
		if upload.path != *path {
			return error(StatusCode::BAD_REQUEST, "InvalidRequest");
		}
		if *method == Method::DELETE {
			state.uploads.remove(id);
			state.aborted += 1;
			return StatusCode::NO_CONTENT.into_response();
		}
		if *method == Method::PUT {
			let Some(number) = query.get("partNumber").and_then(|n| n.parse::<u32>().ok()) else {
				return error(StatusCode::BAD_REQUEST, "InvalidArgument");
			};
			let data = if let Some(source) = header_str(headers, "x-amz-copy-source") {
				let Some(source) = state.objects.get(&decode(source.trim_start_matches('/'))) else {
					return error(StatusCode::NOT_FOUND, "NoSuchKey");
				};
				let range = header_str(headers, "x-amz-copy-source-range")
					.and_then(|range| parse_range(range, source.data.len() as u64));
				match range {
					Some((start, end)) => source.data.slice(start as usize..=end as usize),
					None => source.data.clone(),
				}
			} else {
				body
			};
			let etag = object(data.clone(), BTreeMap::new(), None).etag;
			let copied = header_str(headers, "x-amz-copy-source").is_some();
			state
				.uploads
				.get_mut(id)
				.expect("checked above")
				.parts
				.insert(number, (etag.clone(), data));
			return if copied {
				xml(format!("<CopyPartResult><ETag>{}</ETag></CopyPartResult>", escape(&etag)))
			} else {
				([(header::ETAG, etag)], "").into_response()
			};
		}
		if *method == Method::POST {
			let document = String::from_utf8_lossy(&body).into_owned();
			let mut data = Vec::new();
			let mut previous = 0;
			for part in super::xml_blocks(&document, "Part") {
				let number: u32 = super::xml_value(part, "PartNumber")
					.and_then(|n| n.parse().ok())
					.unwrap_or(0);
				let etag = super::xml_value(part, "ETag").unwrap_or_default();
				let upload = &state.uploads[id];
				match upload.parts.get(&number) {
					Some((stored, bytes)) if *stored == etag && number > previous => {
						data.extend_from_slice(bytes);
						previous = number;
					},
					_ => {
						return xml(
							"<Error><Code>InvalidPart</Code><Message>bad part</Message></Error>".into(),
						);
					},
				}
			}
			if header_str(headers, "if-none-match") == Some("*") && state.objects.contains_key(path) {
				return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
			}
			let upload = state.uploads.remove(id).expect("checked above");
			let stored = object(Bytes::from(data), upload.metadata, None);
			let etag = stored.etag.clone();
			state.objects.insert(path.clone(), stored);
			return xml(format!(
				"<CompleteMultipartUploadResult><Key>{}</Key><ETag>{}</ETag></\
				 CompleteMultipartUploadResult>",
				escape(key),
				escape(&etag)
			));
		}
		return error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed");
	}
	match *method {
		Method::PUT => {
			let current = state.objects.get(path);
			if header_str(headers, "if-none-match") == Some("*") && current.is_some() {
				return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
			}
			if let Some(expected) = header_str(headers, "if-match")
				&& current.is_none_or(|object| object.etag != expected)
			{
				return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
			}
			if let Some(source) = header_str(headers, "x-amz-copy-source") {
				let Some(source) = state
					.objects
					.get(&decode(source.trim_start_matches('/')))
					.cloned()
				else {
					return error(StatusCode::NOT_FOUND, "NoSuchKey");
				};
				if let Some(expected) = header_str(headers, "x-amz-copy-source-if-match")
					&& source.etag != expected
				{
					return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
				}
				let replace = header_str(headers, "x-amz-metadata-directive") == Some("REPLACE");
				let stored = object(
					source.data,
					if replace {
						metadata(headers)
					} else {
						source.metadata
					},
					source.content_type,
				);
				let etag = stored.etag.clone();
				state.objects.insert(path.clone(), stored);
				return xml(format!(
					"<CopyObjectResult><ETag>{}</ETag></CopyObjectResult>",
					escape(&etag)
				));
			}
			let stored =
				object(body, metadata(headers), header_str(headers, "content-type").map(str::to_owned));
			let etag = stored.etag.clone();
			state.objects.insert(path.clone(), stored);
			([(header::ETAG, etag)], "").into_response()
		},
		Method::GET | Method::HEAD => {
			let Some(object) = state.objects.get(path) else {
				return error(StatusCode::NOT_FOUND, "NoSuchKey");
			};
			if let Some(expected) = header_str(headers, "if-match")
				&& object.etag != expected
			{
				return error(StatusCode::PRECONDITION_FAILED, "PreconditionFailed");
			}
			let mut response = Response::builder()
				.header(header::ETAG, &object.etag)
				.header(header::LAST_MODIFIED, httpdate(object.modified));
			for (name, value) in &object.metadata {
				response = response.header(format!("x-amz-meta-{name}"), value);
			}
			let size = object.data.len() as u64;
			let (status, data) = match header_str(headers, "range") {
				Some(range) => match parse_range(range, size) {
					Some((start, end)) => {
						(StatusCode::PARTIAL_CONTENT, object.data.slice(start as usize..=end as usize))
					},
					None => return error(StatusCode::RANGE_NOT_SATISFIABLE, "InvalidRange"),
				},
				None => (StatusCode::OK, object.data.clone()),
			};
			response = response
				.status(status)
				.header(header::CONTENT_LENGTH, data.len());
			let body = if *method == Method::HEAD {
				Body::empty()
			} else {
				Body::from(data)
			};
			response.body(body).expect("valid fake response")
		},
		Method::DELETE => {
			state.objects.remove(path);
			StatusCode::NO_CONTENT.into_response()
		},
		_ => error(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
	}
}

fn rand_id(path: &str) -> u128 {
	let digest = Sha256::digest(format!("{path}{:?}", SystemTime::now()).as_bytes());
	u128::from_be_bytes(digest[..16].try_into().expect("16 bytes"))
}

/// `bytes=a-b` within an object of `size` bytes, clamped to its end.
fn parse_range(range: &str, size: u64) -> Option<(u64, u64)> {
	let (start, end) = range.strip_prefix("bytes=")?.split_once('-')?;
	let start: u64 = start.parse().ok()?;
	let end: u64 = end.parse().ok()?;
	if start >= size || end < start {
		return None;
	}
	Some((start, end.min(size - 1)))
}

fn httpdate(time: SystemTime) -> String {
	chrono::DateTime::<chrono::Utc>::from(time)
		.format("%a, %d %b %Y %H:%M:%S GMT")
		.to_string()
}

fn list(state: &FakeState, bucket: &str, query: &BTreeMap<String, String>) -> Response {
	let prefix = query.get("prefix").map_or("", String::as_str);
	let delimiter = query.get("delimiter").filter(|d| !d.is_empty());
	let after = query.get("continuation-token").map(String::as_str);
	let max: usize = query
		.get("max-keys")
		.and_then(|max| max.parse().ok())
		.unwrap_or(1000);
	let root = format!("{bucket}/");
	let mut contents = Vec::new();
	let mut prefixes = BTreeSet::new();
	let mut last = String::new();
	let mut truncated = None;
	for (path, object) in state.objects.range(format!("{root}{prefix}")..) {
		let Some(key) = path.strip_prefix(&root) else {
			break;
		};
		if !key.starts_with(prefix) {
			break;
		}
		let entry =
			match delimiter.and_then(|d| key[prefix.len()..].find(d.as_str()).map(|at| (d, at))) {
				Some((d, at)) => {
					let common = format!("{prefix}{}{d}", &key[prefix.len()..prefix.len() + at]);
					if prefixes.contains(&common) {
						continue;
					}
					common
				},
				None => key.to_owned(),
			};
		if after.is_some_and(|after| entry.as_str() <= after) {
			continue;
		}
		if contents.len() + prefixes.len() == max {
			truncated = Some(last);
			break;
		}
		last.clone_from(&entry);
		if entry == key {
			contents.push((key.to_owned(), object));
		} else {
			prefixes.insert(entry);
		}
	}
	let mut document = String::from("<ListBucketResult>");
	let _ = write!(document, "<Name>{bucket}</Name><Prefix>{}</Prefix>", escape(prefix));
	for (key, object) in &contents {
		let modified =
			chrono::DateTime::<chrono::Utc>::from(object.modified).format("%Y-%m-%dT%H:%M:%S%.3fZ");
		let _ = write!(
			document,
			"<Contents><Key>{}</Key><LastModified>{modified}</LastModified><ETag>{}</ETag><Size>{}</\
			 Size></Contents>",
			escape(key),
			escape(&object.etag),
			object.data.len()
		);
	}
	for common in &prefixes {
		let _ =
			write!(document, "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>", escape(common));
	}
	match truncated {
		Some(token) => {
			let _ = write!(
				document,
				"<IsTruncated>true</IsTruncated><NextContinuationToken>{}</NextContinuationToken>",
				escape(&token)
			);
		},
		None => document.push_str("<IsTruncated>false</IsTruncated>"),
	}
	document.push_str("</ListBucketResult>");
	xml(document)
}
