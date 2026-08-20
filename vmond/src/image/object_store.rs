//! Provider-neutral object access for published root filesystems.

use std::{
	cell::RefCell,
	fs::File,
	future::Future,
	io::{self, Read, Seek, SeekFrom},
	path::Path,
	thread,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use reqwest::{
	Method, StatusCode,
	blocking::{Body, Client, Response},
	header::{CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, LOCATION, RANGE},
};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use vmon_cloud::{ObjectAuth, aws, google};

use crate::{
	error::{EngineError, Result},
	s3::{S3Auth, S3Client, S3Credentials, S3Error, S3MountConfig, request_url_for},
};

const GCS_PREFIX: &str = "gs://";
const S3_PREFIX: &str = "s3://";
const GCS_UPLOAD_CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const GCS_UPLOAD_RETRIES: usize = 5;
const UPLOAD_PROGRESS_BYTES: u64 = 256 * 1024 * 1024;
const MAX_JSON_BYTES: u64 = 32 * 1024 * 1024;
const S3_STREAM_CHUNK_SIZE: usize = 1024 * 1024;
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Parsed object location within one cloud bucket.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObjectLocation {
	pub reference: String,
	pub bucket:    String,
	pub key:       String,
}

impl ObjectLocation {
	pub fn with_key(&self, key: String) -> Self {
		let scheme = self
			.reference
			.split_once("://")
			.map_or("", |(scheme, _)| scheme);
		Self {
			reference: format!("{scheme}://{}/{key}", self.bucket),
			bucket: self.bucket.clone(),
			key,
		}
	}
}

/// Immutable identity and range-validation metadata for one object.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ObjectMetadata {
	pub version:    String,
	pub version_id: Option<String>,
	pub size:       u64,
	pub digest:     String,
	pub etag:       Option<String>,
}

/// Return whether a reference uses a supported cloud object scheme.
pub(super) fn is_cloud_reference(reference: &str) -> bool {
	reference.starts_with(GCS_PREFIX) || reference.starts_with(S3_PREFIX)
}

/// Provider-specific object store selected from a cloud URI.
pub(super) enum ObjectStore {
	Google(GoogleStore),
	Aws(Box<AwsStore>),
}

impl ObjectStore {
	/// Parse `gs://` and `s3://` references and initialize workload identity.
	pub fn open(reference: &str) -> Result<(Self, ObjectLocation)> {
		if reference.starts_with(GCS_PREFIX) {
			let location = parse_location(reference, GCS_PREFIX, "GCS")?;
			return Ok((Self::Google(GoogleStore::new()?), location));
		}
		if reference.starts_with(S3_PREFIX) {
			let location = parse_location(reference, S3_PREFIX, "S3")?;
			return Ok((Self::Aws(Box::new(AwsStore::new(location.bucket.clone())?)), location));
		}
		Err(EngineError::invalid(
			"disk image references must use gs://bucket/object or s3://bucket/object",
		))
	}

	pub fn metadata(&mut self, location: &ObjectLocation) -> Result<Option<ObjectMetadata>> {
		match self {
			Self::Google(store) => store.metadata(location),
			Self::Aws(store) => store.metadata(location),
		}
	}

	pub fn read_json_bytes(&mut self, location: &ObjectLocation) -> Result<Option<Vec<u8>>> {
		let Some(metadata) = self.metadata(location)? else {
			return Ok(None);
		};
		if metadata.size > MAX_JSON_BYTES {
			return Err(EngineError::invalid(format!(
				"cloud object {} is too large for JSON metadata",
				location.reference
			)));
		}
		if metadata.size == 0 {
			return Ok(Some(Vec::new()));
		}
		let mut response = self.open_range(location, &metadata, 0)?;
		let mut bytes = Vec::with_capacity(metadata.size as usize);
		response.read_to_end(&mut bytes)?;
		if bytes.len() as u64 != metadata.size {
			return Err(EngineError::engine(format!(
				"short cloud object read for {}: got {}, expected {} bytes",
				location.reference,
				bytes.len(),
				metadata.size
			)));
		}
		Ok(Some(bytes))
	}

	pub fn open_range(
		&mut self,
		location: &ObjectLocation,
		metadata: &ObjectMetadata,
		offset: u64,
	) -> io::Result<Response> {
		match self {
			Self::Google(store) => store.open_range(location, metadata, offset),
			Self::Aws(store) => store.open_range(location, metadata, offset),
		}
	}

	pub fn put_file(&mut self, location: &ObjectLocation, path: &Path) -> Result<()> {
		match self {
			Self::Google(store) => store.put_file(location, path),
			Self::Aws(store) => store.put_file(location, path),
		}
	}

	pub fn put_json<T: Serialize>(&mut self, location: &ObjectLocation, value: &T) -> Result<()> {
		match self {
			Self::Google(store) => store.put_json(location, value),
			Self::Aws(store) => store.put_json(location, value),
		}
	}

	pub fn object_url(
		&self,
		location: &ObjectLocation,
		metadata: &ObjectMetadata,
	) -> Result<String> {
		match self {
			Self::Google(_) => Ok(GoogleStore::object_url(location, metadata)),
			Self::Aws(store) => store.object_url(location, metadata),
		}
	}

	pub const fn auth(&self) -> ObjectAuth {
		match self {
			Self::Google(_) => ObjectAuth::Google,
			Self::Aws(_) => ObjectAuth::Aws,
		}
	}

	pub fn region(&self) -> Option<String> {
		match self {
			Self::Google(_) => None,
			Self::Aws(store) => Some(store.region.clone()),
		}
	}

	pub const fn scheme(&self) -> &'static str {
		match self {
			Self::Google(_) => "gcs",
			Self::Aws(_) => "s3",
		}
	}
}

fn parse_location(reference: &str, prefix: &str, provider: &str) -> Result<ObjectLocation> {
	let rest = reference.strip_prefix(prefix).unwrap_or_default();
	let (bucket, key) = rest
		.split_once('/')
		.filter(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
		.ok_or_else(|| {
			EngineError::invalid(format!("{provider} disk images must use {prefix}bucket/object"))
		})?;
	if bucket
		.bytes()
		.any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')))
		|| key.contains(['?', '#'])
	{
		return Err(EngineError::invalid(format!("invalid {prefix} disk image reference")));
	}
	Ok(ObjectLocation {
		reference: reference.to_owned(),
		bucket:    bucket.to_owned(),
		key:       key.to_owned(),
	})
}

pub(super) struct GoogleStore {
	client: Client,
	tokens: google::TokenProvider,
}

impl GoogleStore {
	fn new() -> Result<Self> {
		let client = Client::builder()
			.build()
			.map_err(|error| EngineError::engine(format!("failed to create GCS client: {error}")))?;
		let tokens = google::TokenProvider::new()
			.map_err(|error| EngineError::unauthorized(error.to_string()))?;
		Ok(Self { client, tokens })
	}

	fn token(&mut self) -> Result<String> {
		self
			.tokens
			.token()
			.map_err(|error| EngineError::unauthorized(error.to_string()))
	}

	fn metadata(&mut self, location: &ObjectLocation) -> Result<Option<ObjectMetadata>> {
		let token = self.token()?;
		let response = self
			.client
			.get(format!(
				"https://storage.googleapis.com/storage/v1/b/{}/o/{}",
				percent_encode(&location.bucket),
				percent_encode(&location.key)
			))
			.bearer_auth(token)
			.send()
			.map_err(|error| EngineError::engine(format!("failed to inspect GCS object: {error}")))?;
		if response.status() == StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
			self.tokens.invalidate();
		}
		let response = require_success(response, "inspect GCS object")?;
		let wire: GcsMetadata = response
			.json()
			.map_err(|error| EngineError::engine(format!("invalid GCS object metadata: {error}")))?;
		let size = wire
			.size
			.parse::<u64>()
			.map_err(|error| EngineError::engine(format!("invalid GCS object size: {error}")))?;
		let generation = wire
			.generation
			.parse::<u64>()
			.ok()
			.filter(|generation| *generation != 0)
			.ok_or_else(|| EngineError::engine("GCS object metadata contained an invalid generation"))?
			.to_string();
		let digest = gcs_digest(&wire)?;
		let etag = canonical_opaque_etag(&wire.etag, "GCS")?;
		Ok(Some(ObjectMetadata {
			version: generation,
			version_id: None,
			size,
			digest,
			etag: Some(etag),
		}))
	}

	fn open_range(
		&mut self,
		location: &ObjectLocation,
		metadata: &ObjectMetadata,
		offset: u64,
	) -> io::Result<Response> {
		let token = self
			.token()
			.map_err(|error| io::Error::other(error.to_string()))?;
		let response = self
			.client
			.get(Self::object_url(location, metadata))
			.bearer_auth(token)
			.header(RANGE, format!("bytes={offset}-"))
			.header(
				IF_MATCH,
				metadata
					.etag
					.as_deref()
					.ok_or_else(|| io::Error::other("GCS object metadata omitted ETag"))?,
			)
			.send()
			.map_err(|error| io::Error::other(format!("GCS range request failed: {error}")))?;
		if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
			self.tokens.invalidate();
		}
		validate_range_response(response, metadata, offset, "GCS")
	}

	fn object_url(location: &ObjectLocation, metadata: &ObjectMetadata) -> String {
		format!(
			"https://storage.googleapis.com/download/storage/v1/b/{}/o/{}?alt=media&generation={}",
			percent_encode(&location.bucket),
			percent_encode(&location.key),
			metadata.version
		)
	}

	fn put_file(&mut self, location: &ObjectLocation, path: &Path) -> Result<()> {
		let token = self.token()?;
		let size = path.metadata()?.len();
		let metadata = br#"{"contentType":"application/octet-stream"}"#;
		let response = self
			.client
			.post(gcs_resumable_upload_url(location))
			.bearer_auth(&token)
			.header(CONTENT_TYPE, "application/json; charset=UTF-8")
			.header(CONTENT_LENGTH, metadata.len())
			.header("X-Upload-Content-Type", "application/octet-stream")
			.header("X-Upload-Content-Length", size)
			.body(metadata.as_slice())
			.send()
			.map_err(|error| EngineError::engine(format!("starting GCS upload: {error}")))?;
		if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
			self.tokens.invalidate();
		}
		let response = require_success(response, "start GCS resumable upload")?;
		let session = response
			.headers()
			.get(LOCATION)
			.and_then(|value| value.to_str().ok())
			.filter(|value| !value.is_empty())
			.ok_or_else(|| EngineError::engine("GCS resumable upload omitted its session URI"))?
			.to_owned();
		let mut next_progress = UPLOAD_PROGRESS_BYTES;
		let client = self.client.clone();
		let tokens = RefCell::new(&mut self.tokens);
		drive_resumable_upload(
			size,
			|start, end| {
				let length = end - start;
				let mut file = File::open(path)?;
				file.seek(SeekFrom::Start(start))?;
				let token = tokens
					.borrow_mut()
					.token()
					.map_err(|error| EngineError::unauthorized(error.to_string()))?;
				let response = client
					.put(&session)
					.bearer_auth(token)
					.header(CONTENT_TYPE, "application/octet-stream")
					.header(CONTENT_LENGTH, length)
					.header(CONTENT_RANGE, format!("bytes {start}-{}/{size}", end - 1))
					.body(Body::sized(file.take(length), length))
					.send()
					.map_err(|error| EngineError::engine(format!("uploading GCS chunk: {error}")))?;
				if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
					tokens.borrow_mut().invalidate();
				}
				parse_gcs_upload_response(response, size)
			},
			|| {
				let token = tokens
					.borrow_mut()
					.token()
					.map_err(|error| EngineError::unauthorized(error.to_string()))?;
				let response = client
					.put(&session)
					.bearer_auth(token)
					.header(CONTENT_LENGTH, 0)
					.header(CONTENT_RANGE, format!("bytes */{size}"))
					.body(Body::sized(io::empty(), 0))
					.send()
					.map_err(|error| EngineError::engine(format!("querying GCS upload: {error}")))?;
				if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
					tokens.borrow_mut().invalidate();
				}
				parse_gcs_upload_response(response, size)
			},
			|uploaded| {
				if uploaded == size || uploaded >= next_progress {
					eprintln!("vmon: uploaded {uploaded}/{size} root filesystem bytes");
					next_progress = uploaded.saturating_add(UPLOAD_PROGRESS_BYTES);
				}
			},
		)
	}

	fn put_json<T: Serialize>(&mut self, location: &ObjectLocation, value: &T) -> Result<()> {
		let token = self.token()?;
		let body = serde_json::to_vec_pretty(value)?;
		if body.len() as u64 > MAX_JSON_BYTES {
			return Err(EngineError::invalid("cloud JSON metadata exceeds the 32 MiB limit"));
		}
		let response = self
			.client
			.post(gcs_upload_url(location))
			.bearer_auth(token)
			.header(CONTENT_TYPE, "application/json")
			.header(CONTENT_LENGTH, body.len())
			.body(body)
			.send()
			.map_err(|error| EngineError::engine(format!("uploading GCS JSON: {error}")))?;
		if matches!(response.status(), StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
			self.tokens.invalidate();
		}
		require_success(response, "upload GCS JSON")?;
		Ok(())
	}
}

pub(super) struct AwsStore {
	bucket:      String,
	region:      String,
	endpoint:    Option<String>,
	client:      Client,
	credentials: aws::CredentialProvider,
	runtime:     Runtime,
}

impl AwsStore {
	fn new(bucket: String) -> Result<Self> {
		let region = aws::region().map_err(|error| EngineError::unauthorized(error.to_string()))?;
		if !is_valid_aws_region(&region) {
			return Err(EngineError::invalid("AWS region is invalid"));
		}
		let endpoint = std::env::var("AWS_ENDPOINT_URL_S3")
			.or_else(|_| std::env::var("VMON_S3_ENDPOINT"))
			.ok()
			.filter(|value| !value.is_empty())
			.or_else(|| {
				region
					.starts_with("cn-")
					.then(|| format!("https://s3.{region}.amazonaws.com.cn"))
			});
		let client = Client::builder()
			.build()
			.map_err(|error| EngineError::engine(format!("failed to create S3 client: {error}")))?;
		let credentials = aws::CredentialProvider::new()
			.map_err(|error| EngineError::unauthorized(error.to_string()))?;
		let runtime = Runtime::new()
			.map_err(|error| EngineError::engine(format!("starting S3 object runtime: {error}")))?;
		Ok(Self { bucket, region, endpoint, client, credentials, runtime })
	}

	fn metadata(&mut self, location: &ObjectLocation) -> Result<Option<ObjectMetadata>> {
		let response = self
			.signed_request(Method::HEAD, &location.key, None, None, None)?
			.send()
			.map_err(|error| EngineError::engine(format!("failed to inspect S3 object: {error}")))?;
		if response.status() == StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if matches!(
			response.status(),
			StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
		) {
			self.credentials.invalidate();
		}
		let response = require_success(response, "inspect S3 object")?;
		let size = response
			.headers()
			.get(CONTENT_LENGTH)
			.and_then(|value| value.to_str().ok())
			.and_then(|value| value.parse::<u64>().ok())
			.ok_or_else(|| EngineError::engine("S3 HEAD response omitted Content-Length"))?;
		let etag = response
			.headers()
			.get(ETAG)
			.and_then(|value| value.to_str().ok())
			.filter(|value| !value.is_empty())
			.ok_or_else(|| EngineError::engine("S3 HEAD response omitted ETag"))?;
		let etag = canonical_http_etag(etag, "S3")?;
		let version_id = response
			.headers()
			.get("x-amz-version-id")
			.and_then(|value| value.to_str().ok())
			.filter(|value| !value.is_empty() && *value != "null")
			.map(str::to_owned)
			.ok_or_else(|| {
				EngineError::invalid(
					"S3 lazy rootfs objects require bucket versioning (HEAD omitted x-amz-version-id)",
				)
			})?;
		let version = version_id.clone();
		let digest = format!("s3-etag-{}", etag.trim_matches('"'));
		Ok(Some(ObjectMetadata {
			version,
			version_id: Some(version_id),
			size,
			digest,
			etag: Some(etag),
		}))
	}

	fn open_range(
		&mut self,
		location: &ObjectLocation,
		metadata: &ObjectMetadata,
		offset: u64,
	) -> io::Result<Response> {
		let response = self
			.signed_request(
				Method::GET,
				&location.key,
				Some(format!("bytes={offset}-")),
				metadata.etag.as_deref(),
				metadata.version_id.as_deref(),
			)
			.and_then(|request| {
				request
					.send()
					.map_err(|error| EngineError::engine(format!("failed S3 range request: {error}")))
			})
			.map_err(|error| io::Error::other(error.to_string()))?;
		if matches!(
			response.status(),
			StatusCode::BAD_REQUEST | StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN
		) {
			self.credentials.invalidate();
		}
		validate_range_response(response, metadata, offset, "S3")
	}

	fn signed_request(
		&mut self,
		method: Method,
		key: &str,
		range: Option<String>,
		etag: Option<&str>,
		version_id: Option<&str>,
	) -> Result<reqwest::blocking::RequestBuilder> {
		let credentials = self
			.credentials
			.credentials()
			.map_err(|error| EngineError::unauthorized(error.to_string()))?;
		let query = version_query(version_id);
		let query = query.as_ref().map_or(&[][..], |query| query.as_slice());
		let (url, canonical_uri, canonical_query) =
			request_url_for(&self.bucket, &self.region, self.endpoint.as_deref(), key, query)
				.map_err(|error| EngineError::invalid(error.to_string()))?;
		let (_, _, host) =
			aws::canonical_url(&url).map_err(|error| EngineError::invalid(error.to_string()))?;
		let mut headers = vec![
			("host".to_owned(), host.clone()),
			("x-amz-content-sha256".to_owned(), UNSIGNED_PAYLOAD.to_owned()),
		];
		if let Some(range) = &range {
			headers.push(("range".to_owned(), range.clone()));
		}
		if let Some(etag) = etag {
			headers.push(("if-match".to_owned(), etag.to_owned()));
		}
		if let Some(token) = &credentials.session_token {
			headers.push(("x-amz-security-token".to_owned(), token.clone()));
		}
		let signed = aws::authorization_now(
			method.as_str(),
			&canonical_uri,
			&canonical_query,
			&headers,
			UNSIGNED_PAYLOAD,
			&self.region,
			"s3",
			&credentials.access_key,
			&credentials.secret_key,
		);
		let mut request = self
			.client
			.request(method, url)
			.header("host", host)
			.header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
			.header("x-amz-date", signed.date)
			.header("authorization", signed.authorization);
		if let Some(range) = range {
			request = request.header(RANGE, range);
		}
		if let Some(etag) = etag {
			request = request.header(IF_MATCH, etag);
		}
		if let Some(token) = &credentials.session_token {
			request = request.header("x-amz-security-token", token);
		}
		Ok(request)
	}

	fn object_url(&self, location: &ObjectLocation, metadata: &ObjectMetadata) -> Result<String> {
		let query = version_query(metadata.version_id.as_deref());
		let query = query.as_ref().map_or(&[][..], |query| query.as_slice());
		let (url, ..) = request_url_for(
			&self.bucket,
			&self.region,
			self.endpoint.as_deref(),
			&location.key,
			query,
		)
		.map_err(|error| EngineError::invalid(error.to_string()))?;
		Ok(url.to_string())
	}

	fn s3_client(&mut self) -> Result<S3Client> {
		let credentials = self
			.credentials
			.credentials()
			.map_err(|error| EngineError::unauthorized(error.to_string()))?;
		S3Client::new(S3MountConfig {
			bucket:    self.bucket.clone(),
			prefix:    String::new(),
			region:    self.region.clone(),
			endpoint:  self.endpoint.clone(),
			read_only: false,
			creds:     Some(S3Credentials {
				access_key:    credentials.access_key,
				secret_key:    credentials.secret_key,
				session_token: credentials.session_token,
			}),
			auth:      S3Auth::Env,
		})
		.map_err(|error| EngineError::engine(format!("creating S3 client: {error}")))
	}

	fn put_file(&mut self, location: &ObjectLocation, path: &Path) -> Result<()> {
		let mut file = File::open(path)?;
		if file.metadata()?.len() == 0 {
			return Err(EngineError::invalid("cloud uploads require a non-empty object"));
		}
		let client = self.s3_client()?;
		let (tx, rx) = tokio::sync::mpsc::channel(8);
		let producer = thread::spawn(move || -> io::Result<()> {
			let mut buffer = vec![0_u8; S3_STREAM_CHUNK_SIZE];
			loop {
				let read = match file.read(&mut buffer) {
					Ok(read) => read,
					Err(error) => {
						let detail = error.to_string();
						tx.blocking_send(Err(error)).map_err(|_| {
							io::Error::new(
								io::ErrorKind::BrokenPipe,
								format!("S3 upload stopped after a file read failure: {detail}"),
							)
						})?;
						return Ok(());
					},
				};
				if read == 0 {
					return Ok(());
				}
				tx.blocking_send(Ok(bytes::Bytes::copy_from_slice(&buffer[..read])))
					.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "S3 upload stopped"))?;
			}
		});
		let upload = self.run(client.put_multipart(&location.key, rx));
		let produced = producer
			.join()
			.map_err(|_| EngineError::engine("S3 upload reader panicked"))?;
		match upload {
			Err(error) => Err(EngineError::engine(format!("uploading S3 rootfs: {error}"))),
			Ok(()) => {
				produced?;
				Ok(())
			},
		}
	}

	fn put_json<T: Serialize>(&mut self, location: &ObjectLocation, value: &T) -> Result<()> {
		let body = serde_json::to_vec_pretty(value)?;
		if body.len() as u64 > MAX_JSON_BYTES {
			return Err(EngineError::invalid("cloud JSON metadata exceeds the 32 MiB limit"));
		}
		let client = self.s3_client()?;
		self
			.run(client.put(&location.key, body))
			.map_err(|error| EngineError::engine(format!("uploading S3 JSON: {error}")))
	}

	fn run<F, T>(&self, future: F) -> std::result::Result<T, S3Error>
	where
		F: Future<Output = std::result::Result<T, S3Error>> + Send,
		T: Send,
	{
		thread::scope(|scope| {
			scope
				.spawn(|| self.runtime.handle().block_on(future))
				.join()
				.unwrap_or_else(|_| Err(S3Error::Io("S3 runtime thread panicked".to_owned())))
		})
	}
}

fn validate_range_response(
	response: Response,
	metadata: &ObjectMetadata,
	offset: u64,
	provider: &str,
) -> io::Result<Response> {
	let status = response.status();
	let expected_size = metadata.size;
	if expected_size == 0 {
		return Err(io::Error::other(format!("{provider} cannot range-read an empty object")));
	}
	if status != StatusCode::PARTIAL_CONTENT && !(offset == 0 && status == StatusCode::OK) {
		return Err(io::Error::other(format!("{provider} range request returned HTTP {status}")));
	}
	let expected = expected_size.checked_sub(offset).ok_or_else(|| {
		io::Error::other(format!(
			"{provider} range offset {offset} exceeds object size {expected_size}"
		))
	})?;
	match response.content_length() {
		Some(length) if length == expected => {},
		Some(length) => {
			return Err(io::Error::new(
				io::ErrorKind::UnexpectedEof,
				format!("{provider} range response length {length} did not match expected {expected}"),
			));
		},
		None => {
			return Err(io::Error::other(format!("{provider} range response omitted Content-Length")));
		},
	}
	let expected_etag = metadata
		.etag
		.as_deref()
		.ok_or_else(|| io::Error::other(format!("{provider} object metadata omitted ETag")))?;
	let actual_etag = response
		.headers()
		.get(ETAG)
		.and_then(|value| value.to_str().ok())
		.ok_or_else(|| io::Error::other(format!("{provider} range response omitted ETag")))?;
	let actual_etag = canonical_http_etag(actual_etag, provider)
		.map_err(|error| io::Error::other(error.to_string()))?;
	if actual_etag != expected_etag {
		return Err(io::Error::other(format!(
			"{provider} range response ETag {actual_etag} did not match {expected_etag}"
		)));
	}
	if let Some(expected_version) = &metadata.version_id {
		let actual_version = response
			.headers()
			.get("x-amz-version-id")
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| {
				io::Error::other(format!("{provider} range response omitted its object version"))
			})?;
		if actual_version != expected_version {
			return Err(io::Error::other(format!(
				"{provider} range response version {actual_version:?} did not match \
				 {expected_version:?}"
			)));
		}
	}
	if status == StatusCode::PARTIAL_CONTENT {
		let value = response
			.headers()
			.get(CONTENT_RANGE)
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| {
				io::Error::other(format!("{provider} range response omitted Content-Range"))
			})?;
		let value = value
			.strip_prefix("bytes ")
			.ok_or_else(|| io::Error::other(format!("invalid {provider} Content-Range {value:?}")))?;
		let (range, total) = value
			.split_once('/')
			.ok_or_else(|| io::Error::other(format!("invalid {provider} Content-Range {value:?}")))?;
		let (start, end) = range
			.split_once('-')
			.ok_or_else(|| io::Error::other(format!("invalid {provider} Content-Range {value:?}")))?;
		let start = start.parse::<u64>().map_err(|error| {
			io::Error::other(format!("invalid {provider} Content-Range start: {error}"))
		})?;
		let end = end.parse::<u64>().map_err(|error| {
			io::Error::other(format!("invalid {provider} Content-Range end: {error}"))
		})?;
		let total = total.parse::<u64>().map_err(|error| {
			io::Error::other(format!("invalid {provider} Content-Range total: {error}"))
		})?;
		if start != offset || end.checked_add(1) != Some(expected_size) || total != expected_size {
			return Err(io::Error::other(format!(
				"{provider} Content-Range bytes {start}-{end}/{total} does not match requested \
				 {offset}-{}",
				expected_size - 1
			)));
		}
	}
	Ok(response)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsMetadata {
	generation: String,
	size:       String,
	etag:       String,
	md5_hash:   Option<String>,
	crc32c:     Option<String>,
}

fn is_valid_aws_region(region: &str) -> bool {
	let bytes = region.as_bytes();
	(3..=64).contains(&bytes.len())
		&& bytes.first().is_some_and(u8::is_ascii_lowercase)
		&& bytes.last().is_some_and(u8::is_ascii_digit)
		&& bytes
			.iter()
			.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
		&& !region.contains("--")
}

fn version_query(version_id: Option<&str>) -> Option<[(String, String); 1]> {
	version_id.map(|version| [("versionId".to_owned(), version.to_owned())])
}

fn canonical_http_etag(value: &str, provider: &str) -> Result<String> {
	let value = value.trim();
	let opaque = value
		.strip_prefix('"')
		.and_then(|value| value.strip_suffix('"'))
		.ok_or_else(|| {
			EngineError::engine(format!("{provider} object metadata contained no quoted strong ETag"))
		})?;
	canonical_opaque_etag(opaque, provider)
}

fn canonical_opaque_etag(value: &str, provider: &str) -> Result<String> {
	let opaque = value.trim();
	if opaque.is_empty()
		|| opaque == "*"
		|| opaque.starts_with("W/")
		|| opaque
			.bytes()
			.any(|byte| byte != 0x21 && !(0x23..=0x7e).contains(&byte))
	{
		return Err(EngineError::engine(format!(
			"{provider} object metadata contained an invalid strong ETag"
		)));
	}
	Ok(format!("\"{opaque}\""))
}

fn gcs_digest(metadata: &GcsMetadata) -> Result<String> {
	let (kind, encoded) = metadata
		.md5_hash
		.as_deref()
		.map(|value| ("md5", value))
		.or_else(|| metadata.crc32c.as_deref().map(|value| ("crc32c", value)))
		.ok_or_else(|| EngineError::engine("GCS object metadata contained no content digest"))?;
	let bytes = B64
		.decode(encoded)
		.map_err(|error| EngineError::engine(format!("invalid GCS {kind} digest: {error}")))?;
	let expected_len = if kind == "md5" { 16 } else { 4 };
	if bytes.len() != expected_len {
		return Err(EngineError::engine(format!(
			"invalid GCS {kind} digest length: got {}, expected {expected_len} bytes",
			bytes.len()
		)));
	}
	Ok(format!("gcs-{kind}-{}", hex::encode(bytes)))
}

fn gcs_upload_url(location: &ObjectLocation) -> String {
	format!(
		"https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
		percent_encode(&location.bucket),
		percent_encode(&location.key)
	)
}

fn gcs_resumable_upload_url(location: &ObjectLocation) -> String {
	format!(
		"https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=resumable&name={}",
		percent_encode(&location.bucket),
		percent_encode(&location.key)
	)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UploadState {
	Incomplete(u64),
	Complete,
}

fn parse_gcs_upload_response(response: Response, total: u64) -> Result<UploadState> {
	let status = response.status();
	if matches!(status, StatusCode::OK | StatusCode::CREATED) {
		return Ok(UploadState::Complete);
	}
	if status.as_u16() == 308 {
		let committed = match response.headers().get(RANGE) {
			None => 0,
			Some(value) => {
				let value = value.to_str().map_err(|error| {
					EngineError::engine(format!("invalid GCS upload Range: {error}"))
				})?;
				value
					.strip_prefix("bytes=0-")
					.ok_or_else(|| EngineError::engine(format!("invalid GCS upload Range {value:?}")))?
					.parse::<u64>()
					.map_err(|error| EngineError::engine(format!("invalid GCS upload Range: {error}")))?
					.checked_add(1)
					.ok_or_else(|| EngineError::engine("GCS upload offset overflowed"))?
			},
		};
		if committed > total {
			return Err(EngineError::engine("GCS upload committed beyond object size"));
		}
		return Ok(UploadState::Incomplete(committed));
	}
	require_success(response, "continue GCS resumable upload")?;
	unreachable!()
}

fn drive_resumable_upload<S, Q, P>(
	total: u64,
	mut send: S,
	mut query: Q,
	mut progress: P,
) -> Result<()>
where
	S: FnMut(u64, u64) -> Result<UploadState>,
	Q: FnMut() -> Result<UploadState>,
	P: FnMut(u64),
{
	if total == 0 {
		return Err(EngineError::invalid("cloud uploads require a non-empty object"));
	}
	let mut offset = 0_u64;
	let mut failures = 0_usize;
	while offset < total {
		let end = offset.saturating_add(GCS_UPLOAD_CHUNK_SIZE).min(total);
		match send(offset, end) {
			Ok(UploadState::Complete) if end == total => {
				progress(total);
				return Ok(());
			},
			Ok(UploadState::Complete) => {
				return Err(EngineError::engine("GCS upload completed before its final chunk"));
			},
			Ok(UploadState::Incomplete(committed)) => {
				validate_committed_offset(offset, end, committed)?;
				if committed == offset {
					failures += 1;
				} else {
					offset = committed;
					failures = 0;
					progress(offset);
				}
			},
			Err(error) => {
				failures += 1;
				if failures > GCS_UPLOAD_RETRIES {
					return Err(upload_retries_exhausted(offset, &error.to_string()));
				}
				match query()? {
					UploadState::Complete => {
						progress(total);
						return Ok(());
					},
					UploadState::Incomplete(committed) => {
						validate_committed_offset(offset, end, committed)?;
						if committed > offset {
							offset = committed;
							failures = 0;
							progress(offset);
						}
					},
				}
			},
		}
		if failures > GCS_UPLOAD_RETRIES {
			return Err(upload_retries_exhausted(offset, "GCS accepted no chunk bytes"));
		}
	}
	Err(EngineError::engine("GCS upload ended without a completion response"))
}

fn validate_committed_offset(current: u64, end: u64, committed: u64) -> Result<()> {
	if !(current..=end).contains(&committed) {
		return Err(EngineError::engine(format!(
			"GCS upload reported byte {committed}, expected {current}..={end}"
		)));
	}
	Ok(())
}

fn upload_retries_exhausted(offset: u64, cause: &str) -> EngineError {
	EngineError::engine(format!(
		"GCS upload failed at byte {offset} after {GCS_UPLOAD_RETRIES} retries: {cause}"
	))
}

fn percent_encode(value: &str) -> String {
	let mut encoded = String::with_capacity(value.len());
	for byte in value.bytes() {
		if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
			encoded.push(char::from(byte));
		} else {
			use std::fmt::Write as _;
			let _ = write!(encoded, "%{byte:02X}");
		}
	}
	encoded
}

fn require_success(response: Response, action: &str) -> Result<Response> {
	let status = response.status();
	if status.is_success() {
		return Ok(response);
	}
	let detail = response.text().unwrap_or_default();
	let message = format!("failed to {action}: HTTP {status}: {}", detail.trim());
	if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
		Err(EngineError::unauthorized(message))
	} else {
		Err(EngineError::engine(message))
	}
}

#[cfg(test)]
mod tests {
	use std::{cell::RefCell, rc::Rc};

	use super::*;

	#[test]
	fn parses_google_and_aws_locations() {
		let gcs = parse_location("gs://images/export.tar.gz", GCS_PREFIX, "GCS").expect("GCS URI");
		assert_eq!(gcs.bucket, "images");
		assert_eq!(gcs.key, "export.tar.gz");
		assert_eq!(
			gcs.with_key("export.rootfs.ext4.zst".to_owned()).reference,
			"gs://images/export.rootfs.ext4.zst"
		);
		let s3 = parse_location("s3://images/export.raw", S3_PREFIX, "S3").expect("S3 URI");
		assert_eq!(s3.bucket, "images");
		assert_eq!(s3.key, "export.raw");
		assert!(parse_location("s3://missing-key", S3_PREFIX, "S3").is_err());
	}

	#[test]
	fn normalizes_provider_etags_without_accepting_malformed_http_values() {
		assert_eq!(canonical_opaque_etag("opaque", "test").expect("etag"), "\"opaque\"");
		assert_eq!(canonical_http_etag("\"opaque\"", "test").expect("etag"), "\"opaque\"");
		for invalid in ["", "opaque", "\"\"", "*", "W/\"weak\"", "\"unterminated"] {
			assert!(canonical_http_etag(invalid, "test").is_err());
		}
	}

	#[test]
	fn s3_version_query_pins_a_specific_object_version() {
		assert!(version_query(None).is_none());
		assert_eq!(
			version_query(Some("3/L4kqtJlcpXroDTDmJ+3Dc8kN2gPHrb")),
			Some([("versionId".to_owned(), "3/L4kqtJlcpXroDTDmJ+3Dc8kN2gPHrb".to_owned())])
		);
	}

	#[test]
	fn resumable_upload_queries_after_a_mid_request_failure() {
		let events = Rc::new(RefCell::new(Vec::new()));
		let send_events = Rc::clone(&events);
		let query_events = Rc::clone(&events);
		let mut attempts = 0;
		drive_resumable_upload(
			20,
			move |start, end| {
				send_events.borrow_mut().push(format!("send:{start}-{end}"));
				attempts += 1;
				if attempts == 1 {
					Err(EngineError::engine("connection dropped after byte 5"))
				} else {
					Ok(UploadState::Complete)
				}
			},
			move || {
				query_events.borrow_mut().push("query".to_owned());
				Ok(UploadState::Incomplete(5))
			},
			|_| {},
		)
		.expect("resume upload");
		assert_eq!(*events.borrow(), ["send:0-20", "query", "send:5-20"]);
	}
}
