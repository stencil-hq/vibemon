//! Async S3 client for the operator object store and user-supplied buckets.
//!
//! [`S3Client`] signs every request with `SigV4` ([`crate::aws`]), supports
//! path-style and virtual-hosted addressing, and scopes all object keys to the
//! prefix of its [`S3Location`]. Credentials come from explicit keys, the AWS
//! environment/workload provider chain, or nothing (anonymous). Requests with
//! replayable bodies are retried on throttling, 5xx, and transport errors.

use std::{
	collections::BTreeMap,
	fmt, io,
	pin::Pin,
	sync::Arc,
	time::{Duration, SystemTime},
};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, TryStreamExt, stream::FuturesUnordered};
use parking_lot::Mutex;
use reqwest::{Method, StatusCode, Url, header::HeaderMap};
use url::Host;

use crate::aws::{self, CredentialProvider, Credentials};

#[cfg(any(test, feature = "fake"))]
pub mod fake;

/// Part size of multipart uploads (every non-final part has this size unless
/// the object needs more than [`MAX_PARTS`] parts).
pub const PART_BYTES: usize = 8 << 20;
/// S3's part-count limit of one multipart upload.
pub const MAX_PARTS: u64 = 10_000;
/// Largest object a single `CopyObject` may copy.
const MAX_COPY_BYTES: u64 = 5 << 30;
/// Part size of multipart copies.
const COPY_PART_BYTES: u64 = 1 << 30;
/// Largest XML document read from a response.
const DOCUMENT_LIMIT: usize = 8 << 20;
/// Parts of one upload in flight at once.
const UPLOAD_CONCURRENCY: usize = 4;
/// Deletes of one [`S3Client::delete_prefix`] in flight at once.
const DELETE_CONCURRENCY: usize = 16;
/// Extra attempts of a failed replayable request.
const RETRIES: u32 = 3;
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
const META_PREFIX: &str = "x-amz-meta-";

/// Failure of an S3 operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum S3Error {
	/// The object, upload, or bucket does not exist.
	NotFound,
	/// An `If-Match`/`If-None-Match` condition did not hold.
	PreconditionFailed,
	/// The service asked to slow down and retries were exhausted.
	Throttled,
	/// Credentials were missing, invalid, expired, or denied.
	Auth(String),
	/// Any other service, protocol, transport, or argument failure.
	Other(String),
}

impl S3Error {
	fn other(message: impl Into<String>) -> Self {
		Self::Other(message.into())
	}
}

impl fmt::Display for S3Error {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotFound => formatter.write_str("S3 object not found"),
			Self::PreconditionFailed => formatter.write_str("S3 precondition failed"),
			Self::Throttled => formatter.write_str("S3 request throttled"),
			Self::Auth(message) => write!(formatter, "S3 authorization failed: {message}"),
			Self::Other(message) => write!(formatter, "S3: {message}"),
		}
	}
}

impl std::error::Error for S3Error {}

impl From<S3Error> for io::Error {
	fn from(error: S3Error) -> Self {
		let kind = match &error {
			S3Error::NotFound => io::ErrorKind::NotFound,
			S3Error::Auth(_) => io::ErrorKind::PermissionDenied,
			S3Error::PreconditionFailed => io::ErrorKind::AlreadyExists,
			S3Error::Throttled => io::ErrorKind::WouldBlock,
			S3Error::Other(_) => io::ErrorKind::Other,
		};
		Self::new(kind, error)
	}
}

/// Result of an S3 operation.
pub type Result<T, E = S3Error> = std::result::Result<T, E>;

/// Stream of object bytes.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes>> + Send>>;

/// How requests are authenticated.
#[derive(Clone, Debug, Default)]
pub enum S3Auth {
	/// `AWS_*` / `VMON_S3_*` environment keys, then ECS task or EC2 instance
	/// role credentials, refreshed before they expire.
	#[default]
	Workload,
	/// Fixed keys.
	Explicit(Credentials),
	/// Unsigned requests (public buckets).
	Anonymous,
}

/// Bucket addressing style.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Addressing {
	/// `https://endpoint/bucket/key`.
	Path,
	/// `https://bucket.endpoint/key`.
	VirtualHosted,
}

/// Connection settings of one [`S3Client`].
#[derive(Clone, Debug, Default)]
pub struct S3Config {
	/// `s3://bucket[/prefix]` or `http(s)://endpoint/bucket[/prefix]`.
	pub location:   String,
	/// Signing region; `None` = `AWS_REGION`/`AWS_DEFAULT_REGION`/
	/// `VMON_S3_REGION`, then EC2 metadata for AWS endpoints, `us-east-1` for
	/// custom endpoints.
	pub region:     Option<String>,
	/// Endpoint (`http(s)://host[:port]`) that serves `s3://` locations
	/// instead of AWS.
	pub endpoint:   Option<String>,
	/// Request authentication.
	pub auth:       S3Auth,
	/// Accept plain-HTTP endpoints. Off by default: credentials and data
	/// would cross the network unprotected.
	pub allow_http: bool,
	/// Addressing style; `None` = virtual-hosted on AWS for DNS-compatible
	/// bucket names, path-style otherwise.
	pub addressing: Option<Addressing>,
}

impl S3Config {
	/// Operator-store settings for `location`: `VMON_S3_ENDPOINT` (for
	/// `s3://` locations) and region from the environment, workload
	/// credentials (`VMON_S3_ACCESS_KEY`/`VMON_S3_SECRET_KEY`/
	/// `VMON_S3_SESSION_TOKEN` or the AWS chain). The operator chose the URL,
	/// so plain HTTP is allowed.
	pub fn from_env(location: &str) -> Self {
		let env = |key: &str| {
			std::env::var(key)
				.ok()
				.filter(|value| !value.trim().is_empty())
		};
		Self {
			location:   location.to_owned(),
			region:     env("VMON_S3_REGION")
				.or_else(|| env("AWS_REGION"))
				.or_else(|| env("AWS_DEFAULT_REGION")),
			endpoint:   env("VMON_S3_ENDPOINT"),
			auth:       S3Auth::Workload,
			allow_http: true,
			addressing: None,
		}
	}
}

/// A bucket and key prefix that scopes every object key of a client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
	url:    String,
	/// Bucket root, ending in `/`.
	base:   Url,
	bucket: String,
	/// Without leading/trailing `/`; may be empty.
	prefix: String,
}

impl S3Location {
	/// Resolves `config.location` to a bucket root for `region`.
	pub fn parse(config: &S3Config, region: &str) -> Result<Self> {
		let value = config.location.trim();
		let parsed = Url::parse(value)
			.map_err(|error| S3Error::other(format!("invalid S3 location {value:?}: {error}")))?;
		if !parsed.username().is_empty()
			|| parsed.password().is_some()
			|| parsed.query().is_some()
			|| parsed.fragment().is_some()
		{
			return Err(S3Error::other(
				"S3 location cannot contain credentials, a query, or a fragment",
			));
		}
		let check_scheme = |url: &Url| match url.scheme() {
			"https" => Ok(()),
			"http" if config.allow_http => Ok(()),
			"http" => Err(S3Error::other("plain-HTTP S3 endpoints are not allowed here")),
			scheme => Err(S3Error::other(format!("unsupported S3 endpoint scheme {scheme:?}"))),
		};
		let (endpoint, bucket, prefix, aws) = match parsed.scheme() {
			"s3" => {
				let bucket = parsed
					.host_str()
					.filter(|bucket| !bucket.is_empty())
					.ok_or_else(|| S3Error::other("S3 location omits the bucket"))?
					.to_owned();
				if parsed.port().is_some() {
					return Err(S3Error::other("s3:// locations cannot contain a port"));
				}
				let prefix = parsed.path().to_owned();
				if let Some(endpoint) = &config.endpoint {
					let endpoint = Url::parse(endpoint).map_err(|error| {
						S3Error::other(format!("invalid S3 endpoint {endpoint:?}: {error}"))
					})?;
					if !matches!(endpoint.path(), "" | "/")
						|| endpoint.query().is_some()
						|| !endpoint.username().is_empty()
					{
						return Err(S3Error::other(
							"S3 endpoint must be scheme://host[:port] without a path",
						));
					}
					(endpoint, bucket, prefix, false)
				} else {
					let endpoint = Url::parse(&format!("https://s3.{region}.amazonaws.com/"))
						.map_err(|error| S3Error::other(format!("invalid S3 region: {error}")))?;
					(endpoint, bucket, prefix, true)
				}
			},
			"http" | "https" => {
				if config.endpoint.is_some() {
					return Err(S3Error::other(
						"an S3 endpoint override only applies to s3:// locations",
					));
				}
				let path = parsed.path().trim_start_matches('/');
				let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
				if bucket.is_empty() {
					return Err(S3Error::other("S3 endpoint location omits the bucket"));
				}
				let mut endpoint = parsed.clone();
				endpoint.set_path("/");
				let aws = endpoint
					.host_str()
					.is_some_and(|host| host.ends_with(".amazonaws.com"));
				(endpoint, bucket.to_owned(), prefix.to_owned(), aws)
			},
			scheme => {
				return Err(S3Error::other(format!(
					"S3 location must use s3://, https://, or http:// (got {scheme}://)"
				)));
			},
		};
		check_scheme(&endpoint)?;
		if !bucket
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
		{
			return Err(S3Error::other(format!("invalid S3 bucket name {bucket:?}")));
		}
		let prefix = normalize_prefix(&prefix)?;
		let addressing = config.addressing.unwrap_or_else(|| {
			if aws && !bucket.contains('.') && !bucket.contains('_') {
				Addressing::VirtualHosted
			} else {
				Addressing::Path
			}
		});
		let mut base = endpoint;
		match addressing {
			Addressing::Path => base.set_path(&format!("/{}/", aws::encode_component(&bucket))),
			Addressing::VirtualHosted => {
				let host = match base.host() {
					Some(Host::Domain(host)) => host.to_owned(),
					_ => {
						return Err(S3Error::other(
							"virtual-hosted addressing needs a DNS endpoint host",
						));
					},
				};
				base
					.set_host(Some(&format!("{bucket}.{host}")))
					.map_err(|error| S3Error::other(format!("invalid bucket host: {error}")))?;
				base.set_path("/");
			},
		}
		Ok(Self { url: value.trim_end_matches('/').to_owned(), base, bucket, prefix })
	}

	/// The location as configured (without a trailing `/`).
	pub fn url(&self) -> &str {
		&self.url
	}

	/// Bucket name.
	pub fn bucket(&self) -> &str {
		&self.bucket
	}

	/// Key prefix every relative key is joined to (may be empty).
	pub fn prefix(&self) -> &str {
		&self.prefix
	}

	/// This location narrowed to the relative prefix `sub`.
	pub fn child(&self, sub: &str) -> Result<Self> {
		let sub = normalize_prefix(sub)?;
		if sub.is_empty() {
			return Ok(self.clone());
		}
		let prefix = self.full_key(&sub);
		Ok(Self { url: format!("{}/{sub}", self.url), prefix, ..self.clone() })
	}

	fn full_key(&self, key: &str) -> String {
		if self.prefix.is_empty() {
			key.to_owned()
		} else {
			format!("{}/{key}", self.prefix)
		}
	}

	/// Full key (with the location prefix) of the relative object `key`.
	/// Keys are `/`-separated, non-empty segments other than `.`/`..`; a
	/// trailing `/` (directory marker) is allowed.
	pub fn object_key(&self, key: &str) -> Result<String> {
		validate_key(key)?;
		Ok(self.full_key(key))
	}

	/// URL of the relative object `key`.
	pub fn object_url(&self, key: &str) -> Result<Url> {
		let full = self.object_key(key)?;
		let mut url = self.base.clone();
		let encoded = full
			.split('/')
			.map(aws::encode_component)
			.collect::<Vec<_>>()
			.join("/");
		url.set_path(&format!("{}{encoded}", self.base.path()));
		Ok(url)
	}

	/// Full key prefix for listing the relative `prefix` (may be empty or a
	/// partial name).
	fn list_prefix(&self, prefix: &str) -> Result<String> {
		if prefix.starts_with('/') || prefix.split('/').any(|part| part == "." || part == "..") {
			return Err(S3Error::other(format!("invalid S3 key prefix {prefix:?}")));
		}
		Ok(if self.prefix.is_empty() {
			prefix.to_owned()
		} else {
			format!("{}/{prefix}", self.prefix)
		})
	}

	/// Strips the location prefix from a full key returned by the service.
	fn relative<'a>(&self, full: &'a str) -> Option<&'a str> {
		if self.prefix.is_empty() {
			return Some(full);
		}
		full
			.strip_prefix(self.prefix.as_str())
			.and_then(|rest| rest.strip_prefix('/'))
	}
}

fn normalize_prefix(prefix: &str) -> Result<String> {
	let prefix = prefix.trim_matches('/');
	if prefix.is_empty() {
		return Ok(String::new());
	}
	if prefix
		.split('/')
		.any(|part| part.is_empty() || part == "." || part == "..")
	{
		return Err(S3Error::other(format!("invalid S3 key prefix {prefix:?}")));
	}
	Ok(prefix.to_owned())
}

fn validate_key(key: &str) -> Result<()> {
	let body = key.strip_suffix('/').unwrap_or(key);
	if body.is_empty()
		|| body
			.split('/')
			.any(|part| part.is_empty() || part == "." || part == "..")
	{
		return Err(S3Error::other(format!("invalid S3 object key {key:?}")));
	}
	Ok(())
}

/// Object metadata from `HEAD`, `GET`, or a listing.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectMeta {
	/// Key relative to the client's location.
	pub key:           String,
	/// Object size in bytes.
	pub size:          u64,
	/// `ETag` as returned by the service (quoted).
	pub etag:          Option<String>,
	/// Last modification time.
	pub last_modified: Option<SystemTime>,
	/// User metadata (`x-amz-meta-*`), lowercase names without the prefix.
	/// Empty in listings.
	pub metadata:      BTreeMap<String, String>,
}

/// A streaming `GET`.
pub struct GetObject {
	/// Metadata from the response headers.
	pub meta: ObjectMeta,
	/// Object bytes.
	pub body: ByteStream,
}

/// One page of a `ListObjectsV2`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListPage {
	/// Objects (relative keys, no user metadata).
	pub objects:         Vec<ObjectMeta>,
	/// Relative common prefixes (with the trailing delimiter) when a
	/// delimiter was given.
	pub common_prefixes: Vec<String>,
	/// Continuation token of the next page.
	pub next:            Option<String>,
}

/// Options of a `PUT`, multipart creation, or completion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutOptions {
	/// User metadata (`x-amz-meta-<name>`); names are lowercased by S3.
	pub metadata:      BTreeMap<String, String>,
	/// `Content-Type`.
	pub content_type:  Option<String>,
	/// `If-None-Match: *`: fail with [`S3Error::PreconditionFailed`] when the
	/// key exists.
	pub if_none_match: bool,
	/// `If-Match`: fail with [`S3Error::PreconditionFailed`] unless the
	/// current `ETag` matches.
	pub if_match:      Option<String>,
}

/// One uploaded part of a multipart upload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPart {
	/// 1-based part number.
	pub number: u32,
	/// `ETag` returned by `UploadPart`.
	pub etag:   String,
}

enum AuthState {
	Anonymous,
	Explicit(Credentials),
	Workload { provider: Arc<Mutex<CredentialProvider>>, cached: Mutex<Option<Credentials>> },
}

struct Inner {
	http:   reqwest::Client,
	region: String,
	auth:   AuthState,
}

/// Async S3 client scoped to one [`S3Location`]. Cloning is cheap and shares
/// the connection pool and credential cache.
#[derive(Clone)]
pub struct S3Client {
	inner:    Arc<Inner>,
	location: S3Location,
}

impl fmt::Debug for S3Client {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("S3Client")
			.field("location", &self.location.url)
			.field("region", &self.inner.region)
			.finish_non_exhaustive()
	}
}

/// Request body plus the headers that go with it.
struct Request<'a> {
	method: Method,
	url:    Url,
	/// `x-amz-*` headers (signed), lowercase names.
	amz:    Vec<(String, String)>,
	/// Other headers (unsigned).
	plain:  Vec<(&'static str, String)>,
	body:   Bytes,
	/// Statuses other than 2xx handed back to the caller as responses.
	accept: &'a [StatusCode],
}

impl Request<'_> {
	const fn new(method: Method, url: Url) -> Self {
		Self { method, url, amz: Vec::new(), plain: Vec::new(), body: Bytes::new(), accept: &[] }
	}
}

impl S3Client {
	/// Builds a client. Blocks on EC2 metadata only when `config.region` is
	/// unset, no region variable is set, and the location is on AWS; use
	/// [`Self::connect`] from async code.
	pub fn new(config: &S3Config) -> Result<Self> {
		let region = if let Some(region) = &config.region {
			region.clone()
		} else {
			let custom = config.endpoint.is_some() || !config.location.starts_with("s3://");
			match aws::region() {
				Ok(region) => region,
				Err(_) if custom => "us-east-1".to_owned(),
				Err(error) => return Err(S3Error::other(format!("resolving S3 region: {error}"))),
			}
		};
		let location = S3Location::parse(config, &region)?;
		let http = reqwest::Client::builder()
			.redirect(reqwest::redirect::Policy::none())
			.connect_timeout(Duration::from_secs(10))
			.read_timeout(Duration::from_secs(60))
			.build()
			.map_err(|error| S3Error::other(format!("building the HTTP client: {error}")))?;
		let auth = match &config.auth {
			S3Auth::Anonymous => AuthState::Anonymous,
			S3Auth::Explicit(credentials) => AuthState::Explicit(credentials.clone()),
			S3Auth::Workload => AuthState::Workload {
				provider: Arc::new(Mutex::new(
					CredentialProvider::new().map_err(|error| S3Error::Auth(error.to_string()))?,
				)),
				cached:   Mutex::new(None),
			},
		};
		Ok(Self { inner: Arc::new(Inner { http, region, auth }), location })
	}

	/// [`Self::new`] on a blocking worker.
	pub async fn connect(config: S3Config) -> Result<Self> {
		tokio::task::spawn_blocking(move || Self::new(&config))
			.await
			.map_err(|error| S3Error::other(format!("S3 client setup panicked: {error}")))?
	}

	/// Operator store client: [`S3Config::from_env`] + [`Self::connect`].
	pub async fn from_env(location: &str) -> Result<Self> {
		Self::connect(S3Config::from_env(location)).await
	}

	/// The location every key is relative to.
	pub const fn location(&self) -> &S3Location {
		&self.location
	}

	/// Signing region.
	pub fn region(&self) -> &str {
		&self.inner.region
	}

	/// A client for the relative prefix `sub` sharing this client's
	/// connections and credentials.
	pub fn with_prefix(&self, sub: &str) -> Result<Self> {
		Ok(Self { inner: Arc::clone(&self.inner), location: self.location.child(sub)? })
	}

	async fn credentials(&self) -> Result<Option<Credentials>> {
		match &self.inner.auth {
			AuthState::Anonymous => Ok(None),
			AuthState::Explicit(credentials) => Ok(Some(credentials.clone())),
			AuthState::Workload { provider, cached } => {
				if let Some(credentials) = cached.lock().clone()
					&& credentials.is_fresh()
				{
					return Ok(Some(credentials));
				}
				let provider = Arc::clone(provider);
				let credentials = tokio::task::spawn_blocking(move || provider.lock().credentials())
					.await
					.map_err(|error| S3Error::Auth(format!("credential provider panicked: {error}")))?
					.map_err(|error| S3Error::Auth(error.to_string()))?;
				*cached.lock() = Some(credentials.clone());
				Ok(Some(credentials))
			},
		}
	}

	fn invalidate_credentials(&self) {
		if let AuthState::Workload { provider, cached } = &self.inner.auth {
			*cached.lock() = None;
			if let Some(mut provider) = provider.try_lock() {
				provider.invalidate();
			}
		}
	}

	async fn send_once(&self, request: &Request<'_>) -> Result<reqwest::Response> {
		let mut builder = self
			.inner
			.http
			.request(request.method.clone(), request.url.clone());
		if let Some(credentials) = self.credentials().await? {
			let hash = if request.url.scheme() == "https" {
				UNSIGNED_PAYLOAD.to_owned()
			} else {
				aws::sha256_hex(&request.body)
			};
			let (uri, query, host) =
				aws::canonical_url(&request.url).map_err(|error| S3Error::other(error.to_string()))?;
			let mut headers = Vec::with_capacity(request.amz.len() + 3);
			headers.push(("host".to_owned(), host));
			headers.push(("x-amz-content-sha256".to_owned(), hash.clone()));
			if let Some(token) = &credentials.session_token {
				headers.push(("x-amz-security-token".to_owned(), token.clone()));
			}
			headers.extend(request.amz.iter().cloned());
			let signed = aws::authorization_now(
				request.method.as_str(),
				&uri,
				&query,
				&headers,
				&hash,
				&self.inner.region,
				"s3",
				&credentials.access_key,
				&credentials.secret_key,
			);
			builder = builder
				.header("x-amz-date", signed.date)
				.header("authorization", signed.authorization);
			for (name, value) in headers.into_iter().skip(1) {
				builder = builder.header(name, value);
			}
		} else {
			for (name, value) in &request.amz {
				builder = builder.header(name, value);
			}
		}
		for (name, value) in &request.plain {
			builder = builder.header(*name, value);
		}
		builder
			.body(request.body.clone())
			.send()
			.await
			.map_err(|error| {
				S3Error::other(format!("{} {}: {error}", request.method, redact(&request.url)))
			})
	}

	/// Sends `request`, retrying transient failures (bodies are replayable),
	/// and maps error responses to [`S3Error`].
	async fn send(&self, request: Request<'_>) -> Result<reqwest::Response> {
		let mut attempt = 0u32;
		let mut refreshed = false;
		loop {
			let error = match self.send_once(&request).await {
				Ok(response)
					if response.status().is_success() || request.accept.contains(&response.status()) =>
				{
					return Ok(response);
				},
				Ok(response) => response_error(response).await,
				Err(error) => (error, true),
			};
			let (error, transient) = error;
			if let S3Error::Auth(message) = &error
				&& !refreshed
				&& (message.contains("ExpiredToken") || message.contains("TokenRefreshRequired"))
				&& matches!(self.inner.auth, AuthState::Workload { .. })
			{
				refreshed = true;
				self.invalidate_credentials();
				continue;
			}
			if !(transient && attempt < RETRIES) {
				return Err(error);
			}
			tokio::time::sleep(Duration::from_millis(100 * 4u64.pow(attempt))).await;
			attempt += 1;
		}
	}

	/// `HEAD`: metadata of `key`.
	pub async fn head(&self, key: &str) -> Result<ObjectMeta> {
		let url = self.location.object_url(key)?;
		let response = self.send(Request::new(Method::HEAD, url)).await?;
		Ok(meta_from_headers(key, response.headers(), None))
	}

	/// Whether `key` exists; other failures propagate.
	pub async fn exists(&self, key: &str) -> Result<bool> {
		match self.head(key).await {
			Ok(_) => Ok(true),
			Err(S3Error::NotFound) => Ok(false),
			Err(error) => Err(error),
		}
	}

	/// Streaming `GET` of `key`.
	pub async fn get(&self, key: &str) -> Result<GetObject> {
		let url = self.location.object_url(key)?;
		let response = self.send(Request::new(Method::GET, url)).await?;
		let meta = meta_from_headers(key, response.headers(), response.content_length());
		Ok(GetObject { meta, body: body_stream(response) })
	}

	/// `GET` of `key` into memory; fails when it exceeds `max_len` bytes.
	pub async fn get_bytes(&self, key: &str, max_len: usize) -> Result<Bytes> {
		let object = self.get(key).await?;
		if object.meta.size > max_len as u64 {
			return Err(S3Error::other(format!("{key} exceeds {max_len} bytes")));
		}
		collect(object.body, max_len, key).await
	}

	/// Ranged `GET` of `len` bytes at `offset`. Reads past the end are
	/// truncated; a range starting at or past the end is empty.
	pub async fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Bytes> {
		self.range(key, offset, len, None).await
	}

	/// [`Self::get_range`] that fails with [`S3Error::PreconditionFailed`]
	/// when the object's `ETag` is no longer `etag`.
	pub async fn get_range_if_match(
		&self,
		key: &str,
		offset: u64,
		len: u64,
		etag: &str,
	) -> Result<Bytes> {
		self.range(key, offset, len, Some(etag)).await
	}

	async fn range(&self, key: &str, offset: u64, len: u64, etag: Option<&str>) -> Result<Bytes> {
		if len == 0 {
			return Ok(Bytes::new());
		}
		let last = offset
			.checked_add(len - 1)
			.ok_or_else(|| S3Error::other("range overflows"))?;
		let limit = usize::try_from(len).map_err(|_| S3Error::other("range too large"))?;
		let url = self.location.object_url(key)?;
		let mut request = Request::new(Method::GET, url);
		request
			.plain
			.push(("range", format!("bytes={offset}-{last}")));
		if let Some(etag) = etag {
			request.plain.push(("if-match", etag.to_owned()));
		}
		request.accept = &[StatusCode::RANGE_NOT_SATISFIABLE];
		let response = self.send(request).await?;
		match response.status() {
			StatusCode::RANGE_NOT_SATISFIABLE => Ok(Bytes::new()),
			StatusCode::PARTIAL_CONTENT => collect(body_stream(response), limit, key).await,
			// The service ignored the range: skip to it.
			_ => {
				let mut body = body_stream(response);
				let mut skip = offset;
				let mut out = BytesMut::with_capacity(limit.min(PART_BYTES));
				while let Some(chunk) = body.next().await {
					let mut chunk = chunk?;
					if skip > 0 {
						let drop = usize::try_from(skip.min(chunk.len() as u64)).unwrap_or(usize::MAX);
						let _ = chunk.split_to(drop);
						skip -= drop as u64;
					}
					let take = chunk.len().min(limit - out.len());
					out.extend_from_slice(&chunk[..take]);
					if out.len() == limit {
						break;
					}
				}
				Ok(out.freeze())
			},
		}
	}

	/// `PUT` of `body` at `key`; returns the new `ETag`.
	pub async fn put(&self, key: &str, body: Bytes, options: &PutOptions) -> Result<String> {
		let url = self.location.object_url(key)?;
		let mut request = Request::new(Method::PUT, url);
		apply_put_options(&mut request, options, true)?;
		request.body = body;
		let response = self.send(request).await?;
		Ok(etag(response.headers()).unwrap_or_default())
	}

	/// Uploads exactly `size` bytes from `body`: one `PUT` up to
	/// [`PART_BYTES`], a multipart upload (a few parts in flight, bounded
	/// memory) above. A failed or dropped multipart upload is aborted.
	pub async fn upload_stream<S>(
		&self,
		key: &str,
		size: u64,
		body: S,
		options: &PutOptions,
	) -> Result<String>
	where
		S: Stream<Item = io::Result<Bytes>> + Send,
	{
		let mut body = PartReader { body: std::pin::pin!(body), pending: Bytes::new(), key, size };
		let part_size = part_size(size)?;
		if size <= part_size {
			let bytes = body.next_part(size).await?;
			body.finish().await?;
			return self.put(key, bytes, options).await;
		}
		let upload_id = self.create_multipart(key, options).await?;
		let guard = AbortGuard { client: self.clone(), key: key.to_owned(), upload_id, done: false };
		let mut parts = Vec::new();
		let mut inflight = FuturesUnordered::new();
		let mut left = size;
		let mut number = 0u32;
		while left > 0 {
			let want = left.min(part_size);
			let bytes = body.next_part(want).await?;
			left -= want;
			number += 1;
			let part = number;
			let upload_id = guard.upload_id.as_str();
			inflight.push(async move {
				let etag = self.upload_part(key, upload_id, part, bytes).await?;
				Ok::<_, S3Error>(CompletedPart { number: part, etag })
			});
			if inflight.len() >= UPLOAD_CONCURRENCY
				&& let Some(done) = inflight.next().await
			{
				parts.push(done?);
			}
		}
		while let Some(done) = inflight.next().await {
			parts.push(done?);
		}
		drop(inflight);
		body.finish().await?;
		parts.sort_by_key(|part| part.number);
		let etag = self
			.complete_multipart_with(key, &guard.upload_id, &parts, options)
			.await?;
		guard.disarm();
		Ok(etag)
	}

	/// `CreateMultipartUpload`; returns the upload ID.
	pub async fn create_multipart(&self, key: &str, options: &PutOptions) -> Result<String> {
		let mut url = self.location.object_url(key)?;
		set_query(&mut url, &[("uploads", "")]);
		let mut request = Request::new(Method::POST, url);
		apply_put_options(&mut request, options, false)?;
		let response = self.send(request).await?;
		let document = read_document(response).await?;
		xml_value(&document, "UploadId")
			.ok_or_else(|| S3Error::other("CreateMultipartUpload response omitted UploadId"))
	}

	/// `UploadPart` (1-based `number`); returns the part's `ETag`.
	pub async fn upload_part(
		&self,
		key: &str,
		upload_id: &str,
		number: u32,
		body: Bytes,
	) -> Result<String> {
		let mut url = self.location.object_url(key)?;
		set_query(&mut url, &[("partNumber", &number.to_string()), ("uploadId", upload_id)]);
		let mut request = Request::new(Method::PUT, url);
		request.body = body;
		let response = self.send(request).await?;
		etag(response.headers()).ok_or_else(|| S3Error::other("UploadPart response omitted ETag"))
	}

	/// `CompleteMultipartUpload` of `parts` (ascending numbers); returns the
	/// object's `ETag`.
	pub async fn complete_multipart(
		&self,
		key: &str,
		upload_id: &str,
		parts: &[CompletedPart],
	) -> Result<String> {
		self
			.complete_multipart_with(key, upload_id, parts, &PutOptions::default())
			.await
	}

	async fn complete_multipart_with(
		&self,
		key: &str,
		upload_id: &str,
		parts: &[CompletedPart],
		options: &PutOptions,
	) -> Result<String> {
		use std::fmt::Write as _;
		let mut url = self.location.object_url(key)?;
		set_query(&mut url, &[("uploadId", upload_id)]);
		let mut document = String::from("<CompleteMultipartUpload>");
		for part in parts {
			let _ = write!(
				document,
				"<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
				part.number,
				xml_escape(&part.etag)
			);
		}
		document.push_str("</CompleteMultipartUpload>");
		let mut request = Request::new(Method::POST, url);
		if options.if_none_match {
			request.plain.push(("if-none-match", "*".to_owned()));
		}
		if let Some(etag) = &options.if_match {
			request.plain.push(("if-match", etag.clone()));
		}
		request
			.plain
			.push(("content-type", "application/xml".to_owned()));
		request.body = Bytes::from(document);
		let response = self.send(request).await?;
		// Completion can fail after a 200 status line.
		let document = read_document(response).await?;
		if let Some(error) = embedded_error(&document) {
			return Err(error);
		}
		Ok(xml_value(&document, "ETag").unwrap_or_default())
	}

	/// `AbortMultipartUpload`; an unknown upload is not an error.
	pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
		let mut url = self.location.object_url(key)?;
		set_query(&mut url, &[("uploadId", upload_id)]);
		match self.send(Request::new(Method::DELETE, url)).await {
			Ok(_) | Err(S3Error::NotFound) => Ok(()),
			Err(error) => Err(error),
		}
	}

	/// One `ListObjectsV2` page of keys under the relative `prefix`.
	pub async fn list(
		&self,
		prefix: &str,
		delimiter: Option<&str>,
		continuation: Option<&str>,
		max_keys: Option<u32>,
	) -> Result<ListPage> {
		let full = self.location.list_prefix(prefix)?;
		let mut url = self.location.base.clone();
		let max = max_keys.map(|max| max.to_string());
		let mut query = vec![("list-type", "2"), ("prefix", full.as_str())];
		if let Some(delimiter) = delimiter {
			query.push(("delimiter", delimiter));
		}
		if let Some(token) = continuation {
			query.push(("continuation-token", token));
		}
		if let Some(max) = &max {
			query.push(("max-keys", max));
		}
		set_query(&mut url, &query);
		let response = self.send(Request::new(Method::GET, url)).await?;
		let document = read_document(response).await?;
		let mut page = ListPage::default();
		for block in xml_blocks(&document, "Contents") {
			let Some(full_key) = xml_value(block, "Key") else {
				continue;
			};
			let Some(key) = self.location.relative(&full_key) else {
				continue;
			};
			page.objects.push(ObjectMeta {
				key:           key.to_owned(),
				size:          xml_value(block, "Size")
					.and_then(|size| size.parse().ok())
					.unwrap_or(0),
				etag:          xml_value(block, "ETag"),
				last_modified: xml_value(block, "LastModified")
					.and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
					.map(SystemTime::from),
				metadata:      BTreeMap::new(),
			});
		}
		for block in xml_blocks(&document, "CommonPrefixes") {
			if let Some(full_prefix) = xml_value(block, "Prefix")
				&& let Some(prefix) = self.location.relative(&full_prefix)
			{
				page.common_prefixes.push(prefix.to_owned());
			}
		}
		if xml_value(&document, "IsTruncated").as_deref() == Some("true") {
			page.next = xml_value(&document, "NextContinuationToken");
			if page.next.is_none() {
				return Err(S3Error::other("truncated listing omitted NextContinuationToken"));
			}
		}
		Ok(page)
	}

	/// `DELETE` of `key`; a missing key is not an error.
	pub async fn delete(&self, key: &str) -> Result<()> {
		let url = self.location.object_url(key)?;
		match self.send(Request::new(Method::DELETE, url)).await {
			Ok(_) | Err(S3Error::NotFound) => Ok(()),
			Err(error) => Err(error),
		}
	}

	/// Deletes every key under the relative `prefix` (an empty prefix means
	/// the whole location); returns how many keys were deleted.
	pub async fn delete_prefix(&self, prefix: &str) -> Result<u64> {
		let mut deleted = 0u64;
		loop {
			// Always restart from the first page: deleting while paginating can
			// make continuation tokens skip keys on some services.
			let page = self.list(prefix, None, None, Some(1000)).await?;
			if page.objects.is_empty() {
				return Ok(deleted);
			}
			let count = page.objects.len() as u64;
			futures::stream::iter(page.objects)
				.map(|object| async move { self.delete(&object.key).await })
				.buffer_unordered(DELETE_CONCURRENCY)
				.try_collect::<Vec<()>>()
				.await?;
			deleted += count;
		}
	}

	/// Server-side copy of `src` to `dst` (both relative). `metadata: Some`
	/// replaces the user metadata, `None` keeps the source's. Objects above 5
	/// GiB are copied with `UploadPartCopy`.
	pub async fn copy(
		&self,
		src: &str,
		dst: &str,
		metadata: Option<&BTreeMap<String, String>>,
	) -> Result<String> {
		let source = self.location.object_key(src)?;
		let source = format!(
			"/{}/{}",
			self.location.bucket,
			source
				.split('/')
				.map(aws::encode_component)
				.collect::<Vec<_>>()
				.join("/")
		);
		let head = self.head(src).await?;
		if head.size > MAX_COPY_BYTES {
			let options = PutOptions {
				metadata: metadata.cloned().unwrap_or(head.metadata),
				..PutOptions::default()
			};
			return self.copy_multipart(&source, dst, head.size, &options).await;
		}
		let url = self.location.object_url(dst)?;
		let mut request = Request::new(Method::PUT, url);
		request.amz.push(("x-amz-copy-source".to_owned(), source));
		if let Some(etag) = &head.etag {
			request
				.amz
				.push(("x-amz-copy-source-if-match".to_owned(), etag.clone()));
		}
		if let Some(metadata) = metadata {
			request
				.amz
				.push(("x-amz-metadata-directive".to_owned(), "REPLACE".to_owned()));
			apply_put_options(
				&mut request,
				&PutOptions { metadata: metadata.clone(), ..PutOptions::default() },
				true,
			)?;
		}
		let response = self.send(request).await?;
		let document = read_document(response).await?;
		if let Some(error) = embedded_error(&document) {
			return Err(error);
		}
		Ok(xml_value(&document, "ETag").unwrap_or_default())
	}

	async fn copy_multipart(
		&self,
		source: &str,
		dst: &str,
		size: u64,
		options: &PutOptions,
	) -> Result<String> {
		let upload_id = self.create_multipart(dst, options).await?;
		let guard = AbortGuard { client: self.clone(), key: dst.to_owned(), upload_id, done: false };
		let mut parts = Vec::new();
		let mut offset = 0u64;
		while offset < size {
			let end = (offset + COPY_PART_BYTES).min(size) - 1;
			let number = u32::try_from(parts.len() + 1).unwrap_or(u32::MAX);
			let mut url = self.location.object_url(dst)?;
			set_query(&mut url, &[
				("partNumber", &number.to_string()),
				("uploadId", &guard.upload_id),
			]);
			let mut request = Request::new(Method::PUT, url);
			request
				.amz
				.push(("x-amz-copy-source".to_owned(), source.to_owned()));
			request
				.amz
				.push(("x-amz-copy-source-range".to_owned(), format!("bytes={offset}-{end}")));
			let document = read_document(self.send(request).await?).await?;
			if let Some(error) = embedded_error(&document) {
				return Err(error);
			}
			let etag = xml_value(&document, "ETag")
				.ok_or_else(|| S3Error::other("UploadPartCopy response omitted ETag"))?;
			parts.push(CompletedPart { number, etag });
			offset = end + 1;
		}
		let etag = self
			.complete_multipart_with(dst, &guard.upload_id, &parts, &PutOptions::default())
			.await?;
		guard.disarm();
		Ok(etag)
	}

	/// Pre-signed `GET` URL of `key`, valid for `expires` (1 s..7 days). An
	/// anonymous client returns the plain object URL.
	pub async fn presign_get(&self, key: &str, expires: Duration) -> Result<Url> {
		let url = self.location.object_url(key)?;
		let Some(credentials) = self.credentials().await? else {
			return Ok(url);
		};
		aws::presign(
			"GET",
			&url,
			&self.inner.region,
			"s3",
			&credentials,
			expires.as_secs(),
			chrono::Utc::now(),
		)
		.map_err(|error| S3Error::other(error.to_string()))
	}
}

/// Cuts an upload body stream into parts of exact sizes.
struct PartReader<'a, S> {
	body:    Pin<&'a mut S>,
	/// Unconsumed tail of the last chunk.
	pending: Bytes,
	key:     &'a str,
	size:    u64,
}

impl<S: Stream<Item = io::Result<Bytes>>> PartReader<'_, S> {
	async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
		if !self.pending.is_empty() {
			return Ok(Some(std::mem::take(&mut self.pending)));
		}
		match self.body.next().await {
			Some(Ok(chunk)) => Ok(Some(chunk)),
			Some(Err(error)) => Err(S3Error::other(format!("reading {}: {error}", self.key))),
			None => Ok(None),
		}
	}

	/// The next `want` bytes (a single chunk is passed through uncopied).
	async fn next_part(&mut self, want: u64) -> Result<Bytes> {
		let want = usize::try_from(want).map_err(|_| S3Error::other("part too large"))?;
		let mut part = BytesMut::new();
		while part.len() < want {
			let Some(mut chunk) = self.next_chunk().await? else {
				return Err(S3Error::other(format!(
					"{}: body is shorter than {} bytes",
					self.key, self.size
				)));
			};
			let need = want - part.len();
			if chunk.len() > need {
				self.pending = chunk.split_off(need);
			}
			if part.is_empty() && chunk.len() == want {
				return Ok(chunk);
			}
			if part.capacity() == 0 {
				part.reserve(want);
			}
			part.extend_from_slice(&chunk);
		}
		Ok(part.freeze())
	}

	/// Fails when the body continues past the declared size.
	async fn finish(&mut self) -> Result<()> {
		while let Some(chunk) = self.next_chunk().await? {
			if !chunk.is_empty() {
				return Err(S3Error::other(format!(
					"{}: body is longer than {} bytes",
					self.key, self.size
				)));
			}
		}
		Ok(())
	}
}

/// Aborts an unfinished multipart upload when dropped (error or
/// cancellation of the owning future).
struct AbortGuard {
	client:    S3Client,
	key:       String,
	upload_id: String,
	done:      bool,
}

impl AbortGuard {
	fn disarm(mut self) {
		self.done = true;
	}
}

impl Drop for AbortGuard {
	fn drop(&mut self) {
		if self.done {
			return;
		}
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		let (client, key, upload_id) =
			(self.client.clone(), std::mem::take(&mut self.key), std::mem::take(&mut self.upload_id));
		// Best effort: an upload whose abort fails is reclaimed by the bucket's
		// incomplete-multipart lifecycle rule.
		runtime.spawn(async move {
			let _ = client.abort_multipart(&key, &upload_id).await;
		});
	}
}

fn part_size(size: u64) -> Result<u64> {
	let mut part = PART_BYTES as u64;
	if size.div_ceil(part) > MAX_PARTS {
		part = size.div_ceil(MAX_PARTS).next_multiple_of(1 << 20);
	}
	if part > 5 << 30 {
		return Err(S3Error::other("object exceeds the S3 multipart size limit"));
	}
	Ok(part)
}

fn apply_put_options(
	request: &mut Request<'_>,
	options: &PutOptions,
	conditional: bool,
) -> Result<()> {
	for (name, value) in &options.metadata {
		let name = name.to_ascii_lowercase();
		if name.is_empty()
			|| !name
				.bytes()
				.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
			|| !value
				.bytes()
				.all(|b| b == b'\t' || (0x20..0x7f).contains(&b))
		{
			return Err(S3Error::other(format!("invalid S3 user metadata {name:?}")));
		}
		request
			.amz
			.push((format!("{META_PREFIX}{name}"), value.clone()));
	}
	if let Some(content_type) = &options.content_type {
		request.plain.push(("content-type", content_type.clone()));
	}
	if conditional {
		if options.if_none_match {
			request.plain.push(("if-none-match", "*".to_owned()));
		}
		if let Some(etag) = &options.if_match {
			request.plain.push(("if-match", etag.clone()));
		}
	}
	Ok(())
}

/// Sets the query to exactly its `SigV4` canonical form (sorted, RFC 3986
/// encoded), so the signed and the sent query agree.
fn set_query(url: &mut Url, pairs: &[(&str, &str)]) {
	let mut encoded = pairs
		.iter()
		.map(|(name, value)| (aws::encode_component(name), aws::encode_component(value)))
		.collect::<Vec<_>>();
	encoded.sort_unstable();
	let query = encoded
		.into_iter()
		.map(|(name, value)| format!("{name}={value}"))
		.collect::<Vec<_>>()
		.join("&");
	url.set_query(Some(&query));
}

fn redact(url: &Url) -> String {
	let mut url = url.clone();
	url.set_query(None);
	url.to_string()
}

fn etag(headers: &HeaderMap) -> Option<String> {
	headers
		.get(reqwest::header::ETAG)
		.and_then(|value| value.to_str().ok())
		.map(str::to_owned)
}

fn meta_from_headers(key: &str, headers: &HeaderMap, length: Option<u64>) -> ObjectMeta {
	let size = headers
		.get(reqwest::header::CONTENT_LENGTH)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.parse().ok())
		.or(length)
		.unwrap_or(0);
	let last_modified = headers
		.get(reqwest::header::LAST_MODIFIED)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| chrono::DateTime::parse_from_rfc2822(value).ok())
		.map(SystemTime::from);
	let metadata = headers
		.iter()
		.filter_map(|(name, value)| {
			let name = name.as_str().strip_prefix(META_PREFIX)?;
			Some((name.to_owned(), String::from_utf8_lossy(value.as_bytes()).into_owned()))
		})
		.collect();
	ObjectMeta { key: key.to_owned(), size, etag: etag(headers), last_modified, metadata }
}

fn body_stream(response: reqwest::Response) -> ByteStream {
	Box::pin(
		response
			.bytes_stream()
			.map_err(|error| S3Error::other(format!("reading S3 response body: {error}"))),
	)
}

async fn collect(mut body: ByteStream, limit: usize, key: &str) -> Result<Bytes> {
	let mut out = BytesMut::new();
	while let Some(chunk) = body.next().await {
		let chunk = chunk?;
		if out.len() + chunk.len() > limit {
			return Err(S3Error::other(format!("{key}: response exceeds {limit} bytes")));
		}
		out.extend_from_slice(&chunk);
	}
	Ok(out.freeze())
}

async fn read_document(response: reqwest::Response) -> Result<String> {
	let bytes = collect(body_stream(response), DOCUMENT_LIMIT, "S3 response").await?;
	String::from_utf8(bytes.to_vec()).map_err(|_| S3Error::other("S3 response is not UTF-8"))
}

/// Maps an error response; the flag tells whether a retry may succeed.
async fn response_error(response: reqwest::Response) -> (S3Error, bool) {
	let status = response.status();
	let document = read_document(response).await.unwrap_or_default();
	let code = xml_value(&document, "Code").unwrap_or_default();
	let message = xml_value(&document, "Message").unwrap_or_default();
	classify(status, &code, &message)
}

fn classify(status: StatusCode, code: &str, message: &str) -> (S3Error, bool) {
	let detail = || {
		if code.is_empty() {
			format!("HTTP {status}")
		} else {
			format!("HTTP {status} {code}: {message}")
		}
	};
	match (status, code) {
		(StatusCode::NOT_FOUND, _) => (S3Error::NotFound, false),
		(StatusCode::PRECONDITION_FAILED, _)
		| (StatusCode::CONFLICT, "ConditionalRequestConflict") => (S3Error::PreconditionFailed, false),
		(StatusCode::TOO_MANY_REQUESTS, _)
		| (_, "SlowDown" | "Throttling" | "RequestLimitExceeded") => (S3Error::Throttled, true),
		(StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN, _) => (S3Error::Auth(detail()), false),
		(StatusCode::SERVICE_UNAVAILABLE, _) => (S3Error::Throttled, true),
		(_, "InternalError" | "RequestTimeout") => (S3Error::other(detail()), true),
		(status, _) if status.is_server_error() => (S3Error::other(detail()), true),
		_ => (S3Error::other(detail()), false),
	}
}

fn embedded_error(document: &str) -> Option<S3Error> {
	let block = xml_blocks(document, "Error").next()?;
	let code = xml_value(block, "Code").unwrap_or_default();
	let message = xml_value(block, "Message").unwrap_or_default();
	let status = match code.as_str() {
		"PreconditionFailed" => StatusCode::PRECONDITION_FAILED,
		"NoSuchKey" | "NoSuchUpload" | "NoSuchBucket" => StatusCode::NOT_FOUND,
		"AccessDenied" => StatusCode::FORBIDDEN,
		_ => StatusCode::INTERNAL_SERVER_ERROR,
	};
	Some(classify(status, &code, &message).0)
}

/// Inner text of each top-level `<tag>…</tag>` element of `document`.
fn xml_blocks<'a>(document: &'a str, tag: &str) -> impl Iterator<Item = &'a str> {
	let open = format!("<{tag}>");
	let close = format!("</{tag}>");
	let mut rest = document;
	std::iter::from_fn(move || {
		let start = rest.find(&open)? + open.len();
		let len = rest[start..].find(&close)?;
		let block = &rest[start..start + len];
		rest = &rest[start + len + close.len()..];
		Some(block)
	})
}

/// Unescaped text of the first `<tag>` element of `document`.
fn xml_value(document: &str, tag: &str) -> Option<String> {
	xml_blocks(document, tag).next().map(xml_unescape)
}

fn xml_unescape(value: &str) -> String {
	let mut out = String::with_capacity(value.len());
	let mut rest = value;
	while let Some(at) = rest.find('&') {
		out.push_str(&rest[..at]);
		rest = &rest[at..];
		let Some(end) = rest.find(';') else {
			break;
		};
		let entity = &rest[1..end];
		let decoded = match entity {
			"amp" => Some('&'),
			"lt" => Some('<'),
			"gt" => Some('>'),
			"quot" => Some('"'),
			"apos" => Some('\''),
			_ => entity
				.strip_prefix("#x")
				.or_else(|| entity.strip_prefix("#X"))
				.and_then(|hex| u32::from_str_radix(hex, 16).ok())
				.or_else(|| entity.strip_prefix('#').and_then(|dec| dec.parse().ok()))
				.and_then(char::from_u32),
		};
		if let Some(c) = decoded {
			out.push(c);
			rest = &rest[end + 1..];
		} else {
			out.push('&');
			rest = &rest[1..];
		}
	}
	out.push_str(rest);
	out
}

fn xml_escape(value: &str) -> String {
	value
		.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
		.replace('"', "&quot;")
		.replace('\'', "&apos;")
}

#[cfg(test)]
mod tests;
