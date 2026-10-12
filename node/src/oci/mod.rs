//! OCI registry client used by template builds (ARCHITECTURE.md §8.6 step 1).
//!
//! Speaks Docker Registry v2 / OCI distribution directly (no skopeo/umoci):
//! resolves references, runs the bearer-token flow, picks the node's
//! `linux/<arch>` manifest from multi-arch indexes, and downloads layers into
//! the shared, digest-addressed cache `cache/oci/blobs/sha256/<hex>` with
//! verification and deduplication. Layers are applied later inside the
//! unpack VM (GC4 `oci_unpack`), never on the host.

pub mod auth;
pub mod manifest;
pub mod reference;

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use http::{Method, StatusCode, header};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use self::{
	auth::{BasicCredentials, Challenge, Scheme, TokenResponse},
	manifest::{ConfigBlob, Document, ImageConfig},
	reference::Reference,
};
use crate::api::RegistryAuth;

/// Parallel layer downloads per pull.
const LAYER_PARALLELISM: usize = 3;
/// Upper bound for manifest and config documents.
const MAX_DOCUMENT_BYTES: usize = 4 << 20;
/// Attempts per registry request (transport errors, 408/429/5xx, broken
/// bodies).
const MAX_ATTEMPTS: u32 = 6;
/// First retry delay; doubles per attempt (with jitter), capped at 32×.
#[cfg(not(test))]
const RETRY_BASE: std::time::Duration = std::time::Duration::from_secs(1);
#[cfg(test)]
const RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(5);

/// Marks an error as transient (worth retrying): connection failures,
/// truncated bodies, and registry 408/429/5xx answers.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct Transient(String);

fn transient(message: String) -> anyhow::Error {
	anyhow::Error::new(Transient(message))
}

fn is_transient(error: &anyhow::Error) -> bool {
	error.chain().any(|cause| cause.is::<Transient>())
}

/// Delay before retry number `attempt` (1-based): base·2^(attempt-1), ±25 %
/// jitter.
fn backoff(attempt: u32) -> std::time::Duration {
	use rand::RngExt as _;
	let factor = 1u32 << (attempt.saturating_sub(1)).min(5);
	let base = RETRY_BASE * factor;
	base.mul_f64(rand::rng().random_range(0.75..1.25))
}

/// Runs `op` until it succeeds, fails permanently, or runs out of attempts.
async fn retrying<T, F, Fut>(what: &str, mut op: F) -> Result<T>
where
	F: FnMut() -> Fut,
	Fut: Future<Output = Result<T>>,
{
	let mut attempt = 1;
	loop {
		match op().await {
			Ok(value) => return Ok(value),
			Err(error) if attempt < MAX_ATTEMPTS && is_transient(&error) => {
				tracing::warn!(
					what,
					attempt,
					error = format!("{error:#}"),
					"registry request failed; retrying"
				);
				tokio::time::sleep(backoff(attempt)).await;
				attempt += 1;
			},
			Err(error) => return Err(error),
		}
	}
}

/// A resolved image: platform manifest, layer descriptors, and config.
/// Layers are downloaded separately ([`OciClient::fetch_layers`]) so a build
/// whose base layer is cached never transfers them.
#[derive(Clone, Debug)]
pub struct ResolvedImage {
	pub reference:       Reference,
	/// Digest of the platform-specific manifest (base of the step-cache chain).
	pub manifest_digest: String,
	pub layers:          Vec<manifest::Descriptor>,
	pub config:          ImageConfig,
	credentials:         Option<BasicCredentials>,
	/// Registry API bases to fetch layers from, in order (mirror first when
	/// it served the manifest).
	bases:               Vec<String>,
}

/// One downloaded layer blob.
#[derive(Clone, Debug)]
pub struct Layer {
	pub digest:     String,
	pub size:       u64,
	pub media_type: String,
	pub path:       PathBuf,
}

/// Registry client bound to a node's blob cache.
#[derive(Clone, Debug)]
pub struct OciClient {
	http:      reqwest::Client,
	blobs:     PathBuf,
	manifests: PathBuf,
	/// Docker Hub pull-through mirror API base (`https://mirror.gcr.io`).
	mirror:    Option<String>,
}

impl OciClient {
	/// `cache_dir` is `<data>/cache/oci`; blobs live in
	/// `cache_dir/blobs/sha256`, manifest documents in
	/// `cache_dir/manifests/sha256` (both content-addressed). `mirror` is an
	/// optional Docker Hub mirror tried first for anonymous `docker.io` pulls.
	pub fn new(http: reqwest::Client, cache_dir: &Path, mirror: Option<&str>) -> Self {
		Self {
			http,
			blobs: cache_dir.join("blobs").join("sha256"),
			manifests: cache_dir.join("manifests").join("sha256"),
			mirror: mirror
				.map(|m| m.trim().trim_end_matches('/').to_owned())
				.filter(|m| !m.is_empty()),
		}
	}

	/// API bases to try for `reference`: the mirror (Docker Hub, anonymous
	/// pulls only: credentials are never sent to it) before the registry.
	fn bases(&self, reference: &Reference, anonymous: bool) -> Vec<String> {
		let mut bases = Vec::with_capacity(2);
		if let Some(mirror) = &self.mirror
			&& anonymous
			&& reference.registry == reference::DOCKER_HUB
		{
			bases.push(mirror.clone());
		}
		bases.push(reference.api_base());
		bases
	}

	/// Manifest document `digest`: from the content-addressed cache when
	/// present and intact, else fetched (GET) and cached.
	async fn manifest_by_digest(
		&self,
		session: &Session<'_>,
		digest: &str,
	) -> Result<(Option<String>, Bytes)> {
		let digest = reference::parse_digest(digest)?;
		let path = self.manifests.join(&digest["sha256:".len()..]);
		if let Some(body) = read_cached(&path, &digest).await {
			return Ok((None, body));
		}
		let (content_type, body) = session.manifest(&digest).await?;
		let actual = sha256_digest(&body);
		if actual != digest {
			bail!(
				"manifest digest mismatch for {}: expected {digest}, got {actual}",
				session.reference
			);
		}
		store_cached(&self.manifests, &path, &body).await;
		Ok((content_type, body))
	}

	/// The top-level manifest of `reference` and its digest. Tags are resolved
	/// with `HEAD` (Docker Hub does not count HEAD against pull rate limits)
	/// and served from the cache when the digest is known; registries that
	/// omit `Docker-Content-Digest` fall back to `GET`.
	async fn top_manifest(&self, session: &Session<'_>) -> Result<(String, Option<String>, Bytes)> {
		let reference = session.reference;
		let digest = match &reference.digest {
			Some(digest) => Some(digest.clone()),
			None => {
				session
					.head_manifest_digest(reference.manifest_reference())
					.await
			},
		};
		if let Some(digest) = digest {
			let (content_type, body) = self.manifest_by_digest(session, &digest).await?;
			return Ok((digest, content_type, body));
		}
		let (content_type, body) = session.manifest(reference.manifest_reference()).await?;
		let digest = sha256_digest(&body);
		store_cached(&self.manifests, &self.manifests.join(&digest["sha256:".len()..]), &body).await;
		Ok((digest, content_type, body))
	}

	/// Image config blob: from the blob cache when present, else fetched and
	/// cached.
	async fn config_blob(&self, session: &Session<'_>, digest: &str) -> Result<Bytes> {
		let digest = reference::parse_digest(digest)?;
		let path = self.blob_path(&digest)?;
		if let Some(body) = read_cached(&path, &digest).await {
			return Ok(body);
		}
		let body = session.blob_bytes(&digest).await?;
		if sha256_digest(&body) != digest {
			bail!("image config digest mismatch for {}", session.reference);
		}
		store_cached(&self.blobs, &path, &body).await;
		Ok(body)
	}

	/// Directory holding verified blobs, named by hex digest (shared read-only
	/// with unpack VMs).
	pub fn blob_dir(&self) -> &Path {
		&self.blobs
	}

	/// Resolves `image` to its `linux/<arch>` manifest and config.
	pub async fn resolve(
		&self,
		image: &str,
		registry_auth: Option<&RegistryAuth>,
		arch: &str,
	) -> Result<ResolvedImage> {
		let reference = Reference::parse(image)?;
		let credentials = match registry_auth {
			Some(auth) => Some(
				auth::resolve(&self.http, auth)
					.await
					.context("resolving registry credentials")?,
			),
			None => None,
		};
		let bases = self.bases(&reference, credentials.is_none());
		self.resolve_from(reference, credentials, bases, arch).await
	}

	/// [`Self::resolve`] against explicit API bases, tried in order.
	async fn resolve_from(
		&self,
		reference: Reference,
		credentials: Option<BasicCredentials>,
		bases: Vec<String>,
		arch: &str,
	) -> Result<ResolvedImage> {
		let mut last_error = None;
		for (index, base) in bases.iter().enumerate() {
			let session = Session {
				http:        &self.http,
				reference:   &reference,
				base:        base.clone(),
				credentials: credentials.clone(),
				token:       tokio::sync::Mutex::new(None),
			};
			match self.resolve_with(&session, arch).await {
				Ok((manifest_digest, layers, config)) => {
					return Ok(ResolvedImage {
						reference: reference.clone(),
						manifest_digest,
						layers,
						config,
						credentials,
						bases: bases[index..].to_vec(),
					});
				},
				Err(error) if index + 1 < bases.len() => {
					tracing::warn!(%reference, mirror = base, error = format!("{error:#}"), "registry mirror failed; using the registry");
					last_error = Some(error);
				},
				Err(error) => return Err(error),
			}
		}
		Err(last_error.unwrap_or_else(|| anyhow!("no registry to pull {reference} from")))
	}

	/// Resolves through one registry base: platform manifest digest, layers,
	/// config.
	async fn resolve_with(
		&self,
		session: &Session<'_>,
		arch: &str,
	) -> Result<(String, Vec<manifest::Descriptor>, ImageConfig)> {
		let reference = session.reference;
		let (top_digest, content_type, body) = self.top_manifest(session).await?;
		let (manifest_digest, manifest) =
			match Document::parse(content_type.as_deref(), &body).context("parsing manifest")? {
				Document::Manifest(manifest) => (top_digest, manifest),
				Document::Index(index) => {
					let entry = manifest::select_platform(&index, arch).ok_or_else(|| {
						let available: Vec<String> = index
							.manifests
							.iter()
							.filter_map(|d| {
								d.platform
									.as_ref()
									.map(|p| format!("{}/{}", p.os, p.architecture))
							})
							.collect();
						anyhow!(
							"image {reference} has no linux/{} variant (available: {})",
							manifest::oci_arch(arch),
							available.join(", ")
						)
					})?;
					let (content_type, body) = self.manifest_by_digest(session, &entry.digest).await?;
					match Document::parse(content_type.as_deref(), &body)? {
						Document::Manifest(manifest) => (entry.digest.clone(), manifest),
						Document::Index(_) => bail!("nested image index in {reference} is not supported"),
					}
				},
			};

		let config_bytes = self
			.config_blob(session, &manifest.config.digest)
			.await
			.context("fetching image config")?;
		let config_blob: ConfigBlob =
			serde_json::from_slice(&config_bytes).context("parsing image config")?;
		if let Some(config_arch) = config_blob.architecture.as_deref()
			&& config_arch != manifest::oci_arch(arch)
		{
			bail!(
				"image {reference} is built for {config_arch}, this node runs {}",
				manifest::oci_arch(arch)
			);
		}
		Ok((manifest_digest, manifest.layers, config_blob.config.unwrap_or_default()))
	}

	/// Downloads (or reuses cached) layers of `image` in application order,
	/// reporting progress lines through `log`.
	pub async fn fetch_layers(
		&self,
		image: &ResolvedImage,
		log: &(dyn Fn(String) + Send + Sync),
	) -> Result<Vec<Layer>> {
		let reference = &image.reference;
		let sessions: Vec<Session<'_>> = image
			.bases
			.iter()
			.map(|base| Session {
				http: &self.http,
				reference,
				base: base.clone(),
				credentials: image.credentials.clone(),
				token: tokio::sync::Mutex::new(None),
			})
			.collect();
		tokio::fs::create_dir_all(&self.blobs)
			.await
			.with_context(|| format!("creating {}", self.blobs.display()))?;
		let total: u64 = image.layers.iter().map(|l| l.size).sum();
		log(format!("Pulling {} layers ({}) of {reference}", image.layers.len(), human_bytes(total)));
		futures::stream::iter(image.layers.iter().cloned())
			.map(|descriptor| {
				let sessions = &sessions;
				async move {
					check_layer_media_type(&descriptor.media_type)?;
					let path = self.blob_path(&descriptor.digest)?;
					if tokio::fs::try_exists(&path).await.unwrap_or(false) {
						log(format!("Layer {} already cached", short(&descriptor.digest)));
					} else {
						let mut result = Err(anyhow!("no registry to download from"));
						for (index, session) in sessions.iter().enumerate() {
							result =
								download_blob(session, &descriptor.digest, descriptor.size, &path, log)
									.await;
							match &result {
								Ok(()) => break,
								Err(error) if index + 1 < sessions.len() => log(format!(
									"Layer {} not available from mirror {} ({error:#}); using the registry",
									short(&descriptor.digest),
									session.base
								)),
								Err(_) => {},
							}
						}
						result?;
						log(format!(
							"Downloaded layer {} ({})",
							short(&descriptor.digest),
							human_bytes(descriptor.size)
						));
					}
					Ok::<_, anyhow::Error>(Layer {
						digest: descriptor.digest,
						size: descriptor.size,
						media_type: descriptor.media_type,
						path,
					})
				}
			})
			.buffered(LAYER_PARALLELISM)
			.try_collect::<Vec<_>>()
			.await
	}

	/// Cache path of a blob (`blobs/sha256/<hex>`).
	pub fn blob_path(&self, digest: &str) -> Result<PathBuf> {
		let digest = reference::parse_digest(digest)?;
		Ok(self.blobs.join(&digest["sha256:".len()..]))
	}
}

/// Per-pull registry session against one API base (registry or mirror):
/// credentials plus the current bearer token.
struct Session<'a> {
	http:        &'a reqwest::Client,
	reference:   &'a Reference,
	/// `scheme://host[:port]` serving the registry API.
	base:        String,
	credentials: Option<BasicCredentials>,
	token:       tokio::sync::Mutex<Option<Authorization>>,
}

#[derive(Clone)]
enum Authorization {
	Bearer(String),
	Basic,
}

impl Session<'_> {
	async fn manifest(&self, reference: &str) -> Result<(Option<String>, Bytes)> {
		let url = format!("{}/v2/{}/manifests/{reference}", self.base, self.reference.repository);
		retrying(&url, || async {
			let response = self.get(&url, Some(manifest::ACCEPT), None).await?;
			let content_type = response
				.headers()
				.get(header::CONTENT_TYPE)
				.and_then(|v| v.to_str().ok())
				.map(str::to_owned);
			let body = read_limited(response).await?;
			Ok((content_type, body))
		})
		.await
	}

	/// `Docker-Content-Digest` of `HEAD /manifests/<reference>`; `None` when
	/// the registry omits it or the HEAD fails (callers fall back to GET).
	async fn head_manifest_digest(&self, reference: &str) -> Option<String> {
		let url = format!("{}/v2/{}/manifests/{reference}", self.base, self.reference.repository);
		// Single attempt: the GET fallback carries the retries.
		let response = self
			.request(Method::HEAD, &url, Some(manifest::ACCEPT), None)
			.await
			.inspect_err(|error| {
				tracing::debug!(url, error = format!("{error:#}"), "manifest HEAD failed; using GET");
			})
			.ok()?;
		let digest = response
			.headers()
			.get("docker-content-digest")?
			.to_str()
			.ok()?;
		reference::parse_digest(digest).ok()
	}

	async fn blob_bytes(&self, digest: &str) -> Result<Bytes> {
		let url = self.blob_url(digest);
		retrying(&url, || async { read_limited(self.get(&url, None, None).await?).await }).await
	}

	fn blob_url(&self, digest: &str) -> String {
		format!("{}/v2/{}/blobs/{digest}", self.base, self.reference.repository)
	}

	/// GET with the cached authorization (optionally from byte `range_from`);
	/// on 401 runs the challenge flow once.
	async fn get(
		&self,
		url: &str,
		accept: Option<&str>,
		range_from: Option<u64>,
	) -> Result<reqwest::Response> {
		self.request(Method::GET, url, accept, range_from).await
	}

	/// `method` with the cached authorization; on 401 runs the challenge flow
	/// once.
	async fn request(
		&self,
		method: Method,
		url: &str,
		accept: Option<&str>,
		range_from: Option<u64>,
	) -> Result<reqwest::Response> {
		let cached = self.token.lock().await.clone();
		let response = self
			.send(&method, url, accept, cached.as_ref(), range_from)
			.await?;
		if response.status() != StatusCode::UNAUTHORIZED {
			return check_status(response, url).await;
		}
		let challenge = response
			.headers()
			.get(header::WWW_AUTHENTICATE)
			.and_then(|v| v.to_str().ok())
			.and_then(Challenge::parse)
			.ok_or_else(|| {
				anyhow!(
					"registry {} answered 401 without a usable WWW-Authenticate challenge",
					self.reference.registry
				)
			})?;
		let authorization = match challenge.scheme {
			Scheme::Basic => {
				if self.credentials.is_none() {
					bail!("registry {} requires credentials (unauthorized)", self.reference.registry);
				}
				Authorization::Basic
			},
			Scheme::Bearer => Authorization::Bearer(self.fetch_token(&challenge).await?),
		};
		*self.token.lock().await = Some(authorization.clone());
		let response = self
			.send(&method, url, accept, Some(&authorization), range_from)
			.await?;
		check_status(response, url).await
	}

	async fn send(
		&self,
		method: &Method,
		url: &str,
		accept: Option<&str>,
		authorization: Option<&Authorization>,
		range_from: Option<u64>,
	) -> Result<reqwest::Response> {
		let mut request = self.http.request(method.clone(), url);
		if let Some(accept) = accept {
			request = request.header(header::ACCEPT, accept);
		}
		if let Some(start) = range_from {
			request = request.header(header::RANGE, format!("bytes={start}-"));
		}
		match (authorization, &self.credentials) {
			(Some(Authorization::Bearer(token)), _) => request = request.bearer_auth(token),
			(Some(Authorization::Basic), Some(creds)) => {
				request = request.basic_auth(&creds.username, Some(&creds.password));
			},
			_ => {},
		}
		request
			.send()
			.await
			.map_err(|error| transient(format!("{method} {url}: {:#}", anyhow::Error::from(error))))
	}

	async fn fetch_token(&self, challenge: &Challenge) -> Result<String> {
		let url = challenge.token_url(&self.reference.repository)?;
		let mut request = self.http.get(url.clone());
		if let Some(creds) = &self.credentials {
			request = request.basic_auth(&creds.username, Some(&creds.password));
		}
		let response = request
			.send()
			.await
			.map_err(|error| transient(format!("GET {url}: {:#}", anyhow::Error::from(error))))?;
		let status = response.status();
		if !status.is_success() {
			let text = response.text().await.unwrap_or_default();
			let message = format!("registry token request failed with HTTP {status}: {}", text.trim());
			return Err(if retryable_status(status) {
				transient(message)
			} else {
				anyhow!(message)
			});
		}
		let body: TokenResponse = response
			.json()
			.await
			.context("parsing registry token response")?;
		body.into_token()
	}
}

async fn check_status(response: reqwest::Response, url: &str) -> Result<reqwest::Response> {
	let status = response.status();
	if status.is_success() {
		return Ok(response);
	}
	if status == StatusCode::TOO_MANY_REQUESTS {
		let retry_after = response
			.headers()
			.get(header::RETRY_AFTER)
			.and_then(|v| v.to_str().ok())
			.map(str::to_owned);
		let text = response.text().await.unwrap_or_default();
		return Err(rate_limited(url, &text, retry_after.as_deref()));
	}
	let text = response.text().await.unwrap_or_default();
	let text: String = text.trim().chars().take(512).collect();
	match status {
		StatusCode::NOT_FOUND => bail!("not found in registry: {url} ({text})"),
		StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
			bail!("registry denied access to {url}: HTTP {status} {text}")
		},
		status if retryable_status(status) => {
			Err(transient(format!("registry request {url} failed: HTTP {status} {text}")))
		},
		_ => bail!("registry request {url} failed: HTTP {status} {text}"),
	}
}

/// Contents of a content-addressed cache file if it exists and still hashes to
/// `digest`.
async fn read_cached(path: &Path, digest: &str) -> Option<Bytes> {
	let body = tokio::fs::read(path).await.ok()?;
	if sha256_digest(&body) == digest {
		return Some(body.into());
	}
	tracing::warn!(path = %path.display(), "cached registry document is corrupt; refetching");
	let _ = tokio::fs::remove_file(path).await;
	None
}

/// Best-effort atomic write of a verified document into a content-addressed
/// cache.
async fn store_cached(dir: &Path, path: &Path, body: &[u8]) {
	let tmp = path.with_extension(format!("tmp-{:08x}", rand::random::<u32>()));
	let result = async {
		tokio::fs::create_dir_all(dir).await?;
		tokio::fs::write(&tmp, body).await?;
		tokio::fs::rename(&tmp, path).await
	}
	.await;
	if let Err(error) = result {
		tracing::warn!(%error, path = %path.display(), "caching registry document failed");
		let _ = tokio::fs::remove_file(&tmp).await;
	}
}
/// Longest `Retry-After` still worth waiting for inside one build.
const MAX_RETRY_AFTER_SECS: u64 = 60;

/// Error for HTTP 429: carries the registry's message and `Retry-After`, and
/// is only retried when the registry asks for a short wait.
fn rate_limited(url: &str, body: &str, retry_after: Option<&str>) -> anyhow::Error {
	let detail = registry_error_message(body);
	let wait = retry_after.map_or_else(String::new, |v| format!("; Retry-After: {v}"));
	let message = format!(
		"registry rate limit exceeded (HTTP 429) for {url}: {detail}{wait}. Configure registry \
		 credentials or a pull-through mirror (--registry-mirror) to raise the limit"
	);
	let short_wait = retry_after
		.and_then(|v| v.trim().parse::<u64>().ok())
		.is_some_and(|secs| secs <= MAX_RETRY_AFTER_SECS);
	if retry_after.is_none() || short_wait {
		transient(message)
	} else {
		anyhow!(message)
	}
}

/// The human message of a registry error body
/// (`{"errors":[{"code","message"}]}`), else the trimmed body.
fn registry_error_message(body: &str) -> String {
	#[derive(serde::Deserialize)]
	struct Errors {
		errors: Vec<ErrorEntry>,
	}
	#[derive(serde::Deserialize)]
	struct ErrorEntry {
		#[serde(default)]
		code:    String,
		#[serde(default)]
		message: String,
	}
	match serde_json::from_str::<Errors>(body) {
		Ok(parsed) if !parsed.errors.is_empty() => parsed
			.errors
			.iter()
			.map(|e| {
				if e.code.is_empty() {
					e.message.clone()
				} else {
					format!("{}: {}", e.code, e.message)
				}
			})
			.collect::<Vec<_>>()
			.join("; "),
		_ => body.trim().chars().take(512).collect(),
	}
}
const fn retryable_status(status: StatusCode) -> bool {
	matches!(status.as_u16(), 408 | 429 | 500..=599)
}

/// Downloads blob `digest` to `dest` with retries. A broken transfer resumes
/// from the bytes already received (`Range`) when the registry honours it;
/// the SHA-256 and size are verified before the atomic rename.
async fn download_blob(
	session: &Session<'_>,
	digest: &str,
	size: u64,
	dest: &Path,
	log: &(dyn Fn(String) + Send + Sync),
) -> Result<()> {
	let digest = reference::parse_digest(digest)?;
	let url = session.blob_url(&digest);
	let tmp = dest.with_file_name(format!(
		".{}.{:016x}.part",
		dest.file_name().and_then(|n| n.to_str()).unwrap_or("blob"),
		rand::random::<u64>()
	));
	let result = async {
		let mut part = PartialBlob {
			file:    tokio::fs::File::create(&tmp)
				.await
				.with_context(|| format!("creating {}", tmp.display()))?,
			hasher:  Sha256::new(),
			written: 0,
		};
		let mut attempt = 1;
		loop {
			match part.fetch(session, &url, size).await {
				Ok(()) => break,
				Err(error) if attempt < MAX_ATTEMPTS && is_transient(&error) => {
					log(format!(
						"Layer {} download interrupted at {} of {} ({error:#}); retrying (attempt \
						 {}/{MAX_ATTEMPTS})",
						short(&digest),
						human_bytes(part.written),
						human_bytes(size),
						attempt + 1,
					));
					tokio::time::sleep(backoff(attempt)).await;
					attempt += 1;
				},
				Err(error) => return Err(error),
			}
		}
		part.file.sync_all().await?;
		let actual = format!("sha256:{}", hex::encode(part.hasher.finalize()));
		if actual != digest {
			bail!("blob digest mismatch: expected {digest}, got {actual}");
		}
		if part.written != size {
			bail!("blob {digest} size mismatch: expected {size} bytes, got {}", part.written);
		}
		tokio::fs::rename(&tmp, dest)
			.await
			.with_context(|| format!("installing {}", dest.display()))?;
		Ok(())
	}
	.await;
	if result.is_err() {
		let _ = tokio::fs::remove_file(&tmp).await;
	}
	result
}

/// A blob download in progress: bytes on disk and the running digest.
struct PartialBlob {
	file:    tokio::fs::File,
	hasher:  Sha256,
	written: u64,
}

impl PartialBlob {
	/// One attempt: requests the remaining bytes and appends them.
	async fn fetch(&mut self, session: &Session<'_>, url: &str, size: u64) -> Result<()> {
		if self.written == size {
			return Ok(());
		}
		let resume_from = (self.written > 0).then_some(self.written);
		let response = session.get(url, None, resume_from).await?;
		if resume_from.is_some() && !range_resumes_at(&response, self.written) {
			// The registry ignored the range (or answered another one): start
			// over.
			self.restart().await?;
		}
		let mut stream = response.bytes_stream();
		while let Some(chunk) = stream.next().await {
			let chunk = chunk.map_err(|error| {
				transient(format!("reading {url}: {:#}", anyhow::Error::from(error)))
			})?;
			if self.written + chunk.len() as u64 > size {
				bail!("blob is larger than its declared {size} bytes");
			}
			self.file.write_all(&chunk).await?;
			self.hasher.update(&chunk);
			self.written += chunk.len() as u64;
		}
		if self.written < size {
			return Err(transient(format!(
				"{url}: body ended after {} of {size} bytes",
				self.written
			)));
		}
		Ok(())
	}

	async fn restart(&mut self) -> Result<()> {
		self.file.set_len(0).await?;
		self.file.seek(std::io::SeekFrom::Start(0)).await?;
		self.hasher = Sha256::new();
		self.written = 0;
		Ok(())
	}
}

/// Whether `response` is a 206 whose `Content-Range` starts at `offset`.
fn range_resumes_at(response: &reqwest::Response, offset: u64) -> bool {
	response.status() == StatusCode::PARTIAL_CONTENT
		&& response
			.headers()
			.get(header::CONTENT_RANGE)
			.and_then(|v| v.to_str().ok())
			.and_then(|v| v.strip_prefix("bytes "))
			.and_then(|v| v.split_once('-'))
			.and_then(|(start, _)| start.trim().parse::<u64>().ok())
			== Some(offset)
}

async fn read_limited(response: reqwest::Response) -> Result<Bytes> {
	if response
		.content_length()
		.is_some_and(|len| len > MAX_DOCUMENT_BYTES as u64)
	{
		bail!("registry document exceeds {MAX_DOCUMENT_BYTES} bytes");
	}
	let mut stream = response.bytes_stream();
	let mut body = Vec::new();
	while let Some(chunk) = stream.next().await {
		let chunk = chunk.map_err(|error| {
			transient(format!("reading registry document: {:#}", anyhow::Error::from(error)))
		})?;
		body.extend_from_slice(&chunk);
		if body.len() > MAX_DOCUMENT_BYTES {
			bail!("registry document exceeds {MAX_DOCUMENT_BYTES} bytes");
		}
	}
	Ok(body.into())
}

fn check_layer_media_type(media_type: &str) -> Result<()> {
	const SUPPORTED: &[&str] = &[
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
		"application/vnd.oci.image.layer.v1.tar",
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.v1.tar+zstd",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
	];
	if media_type.is_empty() || SUPPORTED.contains(&media_type) {
		Ok(())
	} else {
		bail!("unsupported layer media type {media_type}")
	}
}

/// Streams `stream` into `dest`, verifying the SHA-256 digest (and size when
/// known) before an atomic rename. A mismatch leaves nothing behind.
pub async fn write_verified(
	stream: impl Stream<Item = Result<Bytes>>,
	expected_digest: &str,
	expected_size: Option<u64>,
	dest: &Path,
) -> Result<()> {
	let expected_digest = reference::parse_digest(expected_digest)?;
	let tmp = dest.with_file_name(format!(
		".{}.{:016x}.part",
		dest.file_name().and_then(|n| n.to_str()).unwrap_or("blob"),
		rand::random::<u64>()
	));
	let result = async {
		let mut file = tokio::fs::File::create(&tmp)
			.await
			.with_context(|| format!("creating {}", tmp.display()))?;
		let mut hasher = Sha256::new();
		let mut written = 0u64;
		let mut stream = std::pin::pin!(stream);
		while let Some(chunk) = stream.next().await {
			let chunk = chunk?;
			hasher.update(&chunk);
			written += chunk.len() as u64;
			if expected_size.is_some_and(|size| written > size) {
				bail!(
					"blob {expected_digest} is larger than its declared {} bytes",
					expected_size.unwrap_or(0)
				);
			}
			if chunk.iter().all(|b| *b == 0) {
				// Keep sparse images sparse on the receiving side.
				file
					.seek(std::io::SeekFrom::Current(i64::try_from(chunk.len())?))
					.await?;
			} else {
				file.write_all(&chunk).await?;
			}
		}
		file.set_len(written).await?;
		file.sync_all().await?;
		let actual = format!("sha256:{}", hex::encode(hasher.finalize()));
		if actual != expected_digest {
			bail!("blob digest mismatch: expected {expected_digest}, got {actual}");
		}
		if let Some(size) = expected_size
			&& size != written
		{
			bail!("blob {expected_digest} size mismatch: expected {size} bytes, got {written}");
		}
		tokio::fs::rename(&tmp, dest)
			.await
			.with_context(|| format!("installing {}", dest.display()))?;
		Ok(())
	}
	.await;
	if result.is_err() {
		let _ = tokio::fs::remove_file(&tmp).await;
	}
	result
}

/// `sha256:<hex>` of `bytes`.
pub fn sha256_digest(bytes: &[u8]) -> String {
	format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn short(digest: &str) -> &str {
	let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
	&hex[..hex.len().min(12)]
}

/// Human-readable byte count for build logs (`12.3 MiB`).
pub fn human_bytes(bytes: u64) -> String {
	const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
	#[allow(clippy::cast_precision_loss, reason = "display only")]
	let mut value = bytes as f64;
	let mut unit = 0;
	while value >= 1024.0 && unit < UNITS.len() - 1 {
		value /= 1024.0;
		unit += 1;
	}
	if unit == 0 {
		format!("{bytes} B")
	} else {
		format!("{value:.1} {}", UNITS[unit])
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn chunks(parts: &[&'static [u8]]) -> impl Stream<Item = Result<Bytes>> {
		futures::stream::iter(parts.iter().map(|p| Ok(Bytes::from_static(p))))
	}

	#[tokio::test]
	async fn verified_write_installs_matching_blob() {
		let dir = tempfile::tempdir().unwrap();
		let dest = dir.path().join("blob");
		let digest = sha256_digest(b"hello world");
		write_verified(chunks(&[b"hello ", b"world"]), &digest, Some(11), &dest)
			.await
			.unwrap();
		assert_eq!(std::fs::read(&dest).unwrap(), b"hello world");
		// Zero chunks (holes), including a trailing one, keep content and length.
		let sparse = dir.path().join("sparse");
		let digest = sha256_digest(b"a\0\0\0b\0\0");
		write_verified(chunks(&[b"a", b"\0\0\0", b"b", b"\0\0"]), &digest, Some(7), &sparse)
			.await
			.unwrap();
		assert_eq!(std::fs::read(&sparse).unwrap(), b"a\0\0\0b\0\0");
		assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2, "no temp files left");
	}

	#[tokio::test]
	async fn digest_mismatch_fails_and_leaves_nothing() {
		let dir = tempfile::tempdir().unwrap();
		let dest = dir.path().join("blob");
		let err = write_verified(chunks(&[b"tampered"]), &sha256_digest(b"original"), None, &dest)
			.await
			.unwrap_err();
		assert!(err.to_string().contains("digest mismatch"), "{err}");
		assert!(!dest.exists());
		assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
	}

	#[tokio::test]
	async fn size_overrun_and_short_body_are_rejected() {
		let dir = tempfile::tempdir().unwrap();
		let dest = dir.path().join("blob");
		let digest = sha256_digest(b"abcdef");
		assert!(
			write_verified(chunks(&[b"abc", b"def"]), &digest, Some(4), &dest)
				.await
				.unwrap_err()
				.to_string()
				.contains("larger")
		);
		assert!(
			write_verified(chunks(&[b"abcdef"]), &digest, Some(8), &dest)
				.await
				.unwrap_err()
				.to_string()
				.contains("size mismatch")
		);
		let err = write_verified(
			futures::stream::iter(vec![
				Ok(Bytes::from_static(b"ab")),
				Err(anyhow!("connection reset")),
			]),
			&digest,
			None,
			&dest,
		)
		.await
		.unwrap_err();
		assert!(err.to_string().contains("connection reset"));
		assert!(!dest.exists());
	}

	/// What the fake registry does on the n-th blob request (0-based).
	#[derive(Clone, Copy)]
	enum Reply {
		/// 200 with the full length declared but only the first `n` bytes sent.
		Truncate(usize),
		/// Honour `Range` with a 206 (full 200 without one).
		Ranged,
		/// Ignore `Range`; always the full 200 body.
		Full,
		Status(u16),
	}

	struct FakeRegistry {
		url:    String,
		/// `Range` header of every request, in order.
		ranges: std::sync::Arc<parking_lot::Mutex<Vec<Option<String>>>>,
	}

	async fn fake_registry(blob: &'static [u8], script: Vec<Reply>) -> FakeRegistry {
		use axum::{body::Body, http::HeaderMap, response::Response, routing::get};
		let ranges = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
		let seen = ranges.clone();
		let app = axum::Router::new().route(
			"/v2/test/img/blobs/{digest}",
			get(move |headers: HeaderMap| {
				let seen = seen.clone();
				let script = script.clone();
				async move {
					let range = headers.get("range").map(|v| v.to_str().unwrap().to_owned());
					let index = {
						let mut seen = seen.lock();
						seen.push(range.clone());
						seen.len() - 1
					};
					let reply = script.get(index).copied().unwrap_or(Reply::Ranged);
					let builder = Response::builder();
					match reply {
						Reply::Status(code) => builder.status(code).body(Body::from("busy")).unwrap(),
						Reply::Truncate(n) => {
							let parts: Vec<Result<Bytes, std::io::Error>> = vec![
								Ok(Bytes::from_static(&blob[..n])),
								Err(std::io::Error::other("connection reset")),
							];
							// Pause between items so the prefix reaches the client
							// before the reset.
							let paced = futures::stream::iter(parts).then(|item| async move {
								tokio::time::sleep(std::time::Duration::from_millis(30)).await;
								item
							});
							builder
								.header("content-length", blob.len())
								.body(Body::from_stream(paced))
								.unwrap()
						},
						Reply::Full => builder.body(Body::from(blob)).unwrap(),
						Reply::Ranged => match range.and_then(|r| {
							r.strip_prefix("bytes=")
								.map(|r| r.trim_end_matches('-').parse::<usize>().unwrap())
						}) {
							Some(start) => builder
								.status(206)
								.header(
									"content-range",
									format!("bytes {start}-{}/{}", blob.len() - 1, blob.len()),
								)
								.body(Body::from(&blob[start..]))
								.unwrap(),
							None => builder.body(Body::from(blob)).unwrap(),
						},
					}
				}
			}),
		);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = listener.local_addr().unwrap().to_string();
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		FakeRegistry { url, ranges }
	}

	const BLOB: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";

	async fn pull(registry: &FakeRegistry, dest: &Path) -> Result<()> {
		let reference = Reference::parse(&format!("{}/test/img:t", registry.url)).unwrap();
		let http = reqwest::Client::new();
		let session = Session {
			http:        &http,
			reference:   &reference,
			base:        reference.api_base(),
			credentials: None,
			token:       tokio::sync::Mutex::new(None),
		};
		download_blob(&session, &sha256_digest(BLOB), BLOB.len() as u64, dest, &|_| {}).await
	}

	#[tokio::test]
	async fn interrupted_download_resumes_from_the_received_offset() {
		let registry = fake_registry(BLOB, vec![Reply::Truncate(10), Reply::Ranged]).await;
		let dir = tempfile::tempdir().unwrap();
		let dest = dir.path().join("blob");
		pull(&registry, &dest).await.unwrap();
		assert_eq!(std::fs::read(&dest).unwrap(), BLOB);
		assert_eq!(
			*registry.ranges.lock(),
			[None, Some("bytes=10-".to_owned())],
			"only the missing bytes are requested"
		);
		assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1, "no partial files left");
	}

	#[tokio::test]
	async fn registry_ignoring_range_restarts_cleanly() {
		let registry = fake_registry(BLOB, vec![Reply::Truncate(7), Reply::Full]).await;
		let dir = tempfile::tempdir().unwrap();
		let dest = dir.path().join("blob");
		pull(&registry, &dest).await.unwrap();
		assert_eq!(std::fs::read(&dest).unwrap(), BLOB, "restart discards the stale prefix");
		assert_eq!(registry.ranges.lock().len(), 2);
	}

	#[tokio::test]
	async fn server_errors_and_throttling_are_retried_but_not_found_is_not() {
		let registry =
			fake_registry(BLOB, vec![Reply::Status(503), Reply::Status(429), Reply::Ranged]).await;
		let dir = tempfile::tempdir().unwrap();
		pull(&registry, &dir.path().join("a")).await.unwrap();
		assert_eq!(registry.ranges.lock().len(), 3);

		let registry = fake_registry(BLOB, vec![Reply::Status(404)]).await;
		let error = pull(&registry, &dir.path().join("b")).await.unwrap_err();
		assert!(error.to_string().contains("not found"), "{error:#}");
		assert_eq!(registry.ranges.lock().len(), 1, "404 is permanent");
	}

	#[tokio::test]
	async fn persistent_failures_give_up_after_max_attempts() {
		let registry = fake_registry(BLOB, vec![Reply::Status(502); MAX_ATTEMPTS as usize + 2]).await;
		let dir = tempfile::tempdir().unwrap();
		let error = pull(&registry, &dir.path().join("blob")).await.unwrap_err();
		assert!(format!("{error:#}").contains("502"), "{error:#}");
		assert_eq!(registry.ranges.lock().len(), MAX_ATTEMPTS as usize);
		assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0, "partial file removed");
	}
	/// Request counters of [`manifest_registry`].
	#[derive(Default)]
	struct Counts {
		head:      std::sync::atomic::AtomicUsize,
		get:       std::sync::atomic::AtomicUsize,
		blob_gets: std::sync::atomic::AtomicUsize,
	}

	impl Counts {
		fn get(&self) -> (usize, usize, usize) {
			use std::sync::atomic::Ordering::SeqCst;
			(self.head.load(SeqCst), self.get.load(SeqCst), self.blob_gets.load(SeqCst))
		}
	}

	/// Registry serving `test/img:t` as an index → arm64 manifest → config,
	/// counting requests. `head_digest` controls `Docker-Content-Digest` on
	/// HEAD.
	async fn manifest_registry(head_digest: bool) -> (String, std::sync::Arc<Counts>, String) {
		use std::sync::atomic::Ordering::SeqCst;

		use axum::{
			body::Body,
			extract::Path as UrlPath,
			http::Method as HttpMethod,
			response::Response,
			routing::{any, get},
		};
		let config = br#"{"architecture":"arm64","config":{"Env":["A=1"]}}"#.to_vec();
		let config_digest = sha256_digest(&config);
		let platform = serde_json::json!({
			"schemaVersion": 2,
			"mediaType": manifest::OCI_MANIFEST,
			"config": {"digest": config_digest, "size": config.len()},
			"layers": [],
		})
		.to_string();
		let platform_digest = sha256_digest(platform.as_bytes());
		let index = serde_json::json!({
			"schemaVersion": 2,
			"mediaType": manifest::OCI_INDEX,
			"manifests": [{"mediaType": manifest::OCI_MANIFEST, "digest": platform_digest, "size": platform.len(),
				"platform": {"architecture": "arm64", "os": "linux"}}],
		})
		.to_string();
		let index_digest = sha256_digest(index.as_bytes());
		let docs: std::collections::HashMap<String, String> = [
			("t".to_owned(), index.clone()),
			(index_digest.clone(), index),
			(platform_digest.clone(), platform),
		]
		.into();
		let counts = std::sync::Arc::new(Counts::default());
		let (manifest_counts, blob_counts) = (counts.clone(), counts.clone());
		let app = axum::Router::new()
			.route(
				"/v2/test/img/manifests/{reference}",
				any(move |method: HttpMethod, UrlPath(reference): UrlPath<String>| {
					let counts = manifest_counts.clone();
					let body = docs.get(&reference).cloned();
					async move {
						let Some(body) = body else {
							return Response::builder().status(404).body(Body::empty()).unwrap();
						};
						let mut response = Response::builder().header("content-type", "application/json");
						if method == HttpMethod::HEAD {
							counts.head.fetch_add(1, SeqCst);
							if head_digest {
								response =
									response.header("docker-content-digest", sha256_digest(body.as_bytes()));
							}
							return response.body(Body::empty()).unwrap();
						}
						counts.get.fetch_add(1, SeqCst);
						response.body(Body::from(body)).unwrap()
					}
				}),
			)
			.route(
				"/v2/test/img/blobs/{digest}",
				get(move || {
					let counts = blob_counts.clone();
					let config = config.clone();
					async move {
						counts.blob_gets.fetch_add(1, SeqCst);
						config
					}
				}),
			);
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = listener.local_addr().unwrap().to_string();
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		(url, counts, platform_digest)
	}

	#[tokio::test]
	async fn cached_tags_resolve_with_head_only_and_pinned_digests_offline() {
		let (registry, counts, platform_digest) = manifest_registry(true).await;
		let cache = tempfile::tempdir().unwrap();
		let client = || OciClient::new(reqwest::Client::new(), cache.path(), None);

		let first = client()
			.resolve(&format!("{registry}/test/img:t"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(first.manifest_digest, platform_digest);
		assert_eq!(first.config.env_map()["A"], "1");
		assert_eq!(
			counts.get(),
			(1, 2, 1),
			"cold: HEAD tag, GET index + platform by digest, GET config"
		);

		let second = client()
			.resolve(&format!("{registry}/test/img:t"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(second.manifest_digest, platform_digest);
		assert_eq!(counts.get(), (2, 2, 1), "warm tag: one HEAD, no GETs");

		let index_digest = sha256_digest(
			&std::fs::read(
				cache
					.path()
					.join("manifests/sha256")
					.read_dir()
					.unwrap()
					.find_map(|e| {
						let path = e.unwrap().path();
						let body = std::fs::read_to_string(&path).unwrap();
						body.contains("\"manifests\"").then_some(path)
					})
					.unwrap(),
			)
			.unwrap(),
		);
		client()
			.resolve(&format!("{registry}/test/img@{index_digest}"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(counts.get(), (2, 2, 1), "pinned and cached: no requests at all");
	}

	#[tokio::test]
	async fn missing_head_digest_falls_back_to_get_and_corrupt_cache_is_refetched() {
		let (registry, counts, _) = manifest_registry(false).await;
		let cache = tempfile::tempdir().unwrap();
		let client = || OciClient::new(reqwest::Client::new(), cache.path(), None);
		client()
			.resolve(&format!("{registry}/test/img:t"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(counts.get(), (1, 2, 1), "HEAD without digest, GET tag, GET platform, GET config");
		client()
			.resolve(&format!("{registry}/test/img:t"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(counts.get(), (2, 3, 1), "tag GET each time; platform + config still cached");

		for entry in std::fs::read_dir(cache.path().join("manifests/sha256")).unwrap() {
			std::fs::write(entry.unwrap().path(), b"corrupt").unwrap();
		}
		client()
			.resolve(&format!("{registry}/test/img:t"), None, "aarch64")
			.await
			.unwrap();
		assert_eq!(counts.get(), (3, 5, 1), "corrupt cached platform manifest is refetched");
	}
	/// A registry that answers 404 to everything (a mirror lacking the image).
	async fn empty_registry() -> String {
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let url = format!("http://{}", listener.local_addr().unwrap());
		tokio::spawn(async move { axum::serve(listener, axum::Router::new()).await.unwrap() });
		url
	}

	#[test]
	fn mirror_applies_only_to_anonymous_docker_hub_pulls() {
		let client = OciClient::new(
			reqwest::Client::new(),
			Path::new("/nonexistent"),
			Some(" https://mirror.gcr.io/ "),
		);
		let hub = Reference::parse("e2bdev/base").unwrap();
		assert_eq!(client.bases(&hub, true), [
			"https://mirror.gcr.io",
			"https://registry-1.docker.io"
		]);
		assert_eq!(
			client.bases(&hub, false),
			["https://registry-1.docker.io"],
			"credentials never go to the mirror"
		);
		let ghcr = Reference::parse("ghcr.io/org/img").unwrap();
		assert_eq!(client.bases(&ghcr, true), ["https://ghcr.io"]);
		let none = OciClient::new(reqwest::Client::new(), Path::new("/nonexistent"), Some(""));
		assert_eq!(none.bases(&hub, true), ["https://registry-1.docker.io"]);
	}

	#[tokio::test]
	async fn mirror_is_tried_first_and_the_registry_is_the_fallback() {
		let (good, counts, platform_digest) = manifest_registry(true).await;
		let good = format!("http://{good}");
		let reference = || Reference::parse("docker.io/test/img:t").unwrap();

		// Mirror serves everything: the registry is never contacted.
		let cache = tempfile::tempdir().unwrap();
		let client = OciClient::new(reqwest::Client::new(), cache.path(), None);
		let dead = "http://127.0.0.1:1".to_owned();
		let image = client
			.resolve_from(reference(), None, vec![good.clone(), dead.clone()], "aarch64")
			.await
			.unwrap();
		assert_eq!(image.manifest_digest, platform_digest);
		assert_eq!(image.bases, [good.clone(), dead], "layers also prefer the mirror");

		// Mirror lacks the image: fall back to the registry, layers skip the
		// mirror.
		let cache = tempfile::tempdir().unwrap();
		let client = OciClient::new(reqwest::Client::new(), cache.path(), None);
		let mirror = empty_registry().await;
		let before = counts.get();
		let image = client
			.resolve_from(reference(), None, vec![mirror, good.clone()], "aarch64")
			.await
			.unwrap();
		assert_eq!(image.manifest_digest, platform_digest);
		assert_eq!(image.bases, [good]);
		assert_eq!(counts.get().1 - before.1, 2, "the registry served index + platform manifest");
	}

	#[tokio::test]
	async fn layer_downloads_fall_back_from_mirror_to_registry() {
		let registry = fake_registry(BLOB, vec![]).await;
		let cache = tempfile::tempdir().unwrap();
		let client = OciClient::new(reqwest::Client::new(), cache.path(), None);
		let image = ResolvedImage {
			reference:       Reference::parse(&format!("{}/test/img:t", registry.url)).unwrap(),
			manifest_digest: String::new(),
			layers:          vec![manifest::Descriptor {
				media_type: String::new(),
				digest:     sha256_digest(BLOB),
				size:       BLOB.len() as u64,
				platform:   None,
			}],
			config:          ImageConfig::default(),
			credentials:     None,
			bases:           vec![empty_registry().await, format!("http://{}", registry.url)],
		};
		let lines = parking_lot::Mutex::new(Vec::new());
		let layers = client
			.fetch_layers(&image, &|line| lines.lock().push(line))
			.await
			.unwrap();
		assert_eq!(std::fs::read(&layers[0].path).unwrap(), BLOB);
		assert!(
			lines
				.lock()
				.iter()
				.any(|l| l.contains("not available from mirror")),
			"{:?}",
			lines.lock()
		);
	}

	#[tokio::test]
	async fn rate_limit_reports_registry_message_and_retry_after() {
		use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
		let hits = std::sync::Arc::new(AtomicUsize::new(0));
		let seen = hits.clone();
		let app = axum::Router::new().fallback(move || {
			let seen = seen.clone();
			async move {
				seen.fetch_add(1, SeqCst);
				axum::response::Response::builder()
					.status(429)
					.header("retry-after", "3600")
					.body(axum::body::Body::from(
						r#"{"errors":[{"code":"TOOMANYREQUESTS","message":"You have reached your unauthenticated pull rate limit."}]}"#,
					))
					.unwrap()
			}
		});
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		let cache = tempfile::tempdir().unwrap();
		let client = OciClient::new(reqwest::Client::new(), cache.path(), None);
		let error = client
			.resolve(&format!("{addr}/test/img:t"), None, "aarch64")
			.await
			.unwrap_err();
		let message = format!("{error:#}");
		assert!(message.contains("rate limit exceeded (HTTP 429)"), "{message}");
		assert!(
			message
				.contains("TOOMANYREQUESTS: You have reached your unauthenticated pull rate limit."),
			"{message}"
		);
		assert!(message.contains("Retry-After: 3600"), "{message}");
		assert!(message.contains("--registry-mirror"), "{message}");
		assert_eq!(hits.load(SeqCst), 2, "one HEAD, one GET: a long Retry-After is not waited out");
	}

	#[test]
	fn short_retry_after_is_retried_long_or_missing_is_decided_by_value() {
		assert!(is_transient(&rate_limited("u", "", Some("5"))));
		assert!(is_transient(&rate_limited("u", "", None)), "no hint: normal backoff");
		assert!(!is_transient(&rate_limited("u", "", Some("3600"))));
		assert!(
			!is_transient(&rate_limited("u", "", Some("Wed, 21 Oct 2026 07:28:00 GMT"))),
			"HTTP-date: give up now"
		);
		assert_eq!(registry_error_message("  plain text  "), "plain text");
	}
	#[test]
	fn layer_media_types() {
		assert!(check_layer_media_type("application/vnd.oci.image.layer.v1.tar+zstd").is_ok());
		assert!(check_layer_media_type("application/vnd.docker.image.rootfs.diff.tar.gzip").is_ok());
		assert!(check_layer_media_type("application/vnd.in-toto+json").is_err());
	}
}
