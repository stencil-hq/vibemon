//! AWS workload credentials and Signature Version 4.

use std::{env, path::Path, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use hmac::{Hmac, Mac};
use reqwest::{
	Url,
	blocking::{Client, RequestBuilder},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Host;

use crate::{Error, Result};

const METADATA_ENDPOINT: &str = "http://169.254.169.254";
const ECS_ENDPOINT: &str = "http://169.254.170.2";
const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const REFRESH_MARGIN: Duration = Duration::minutes(5);
const IMDS_TOKEN_TTL: &str = "21600";

type HmacSha256 = Hmac<Sha256>;

/// AWS access keys used to sign one request.
#[derive(Clone)]
pub struct Credentials {
	/// Access-key identifier.
	pub access_key:    String,
	/// Secret signing key.
	pub secret_key:    String,
	/// Session token required for temporary credentials.
	pub session_token: Option<String>,
	expires_at:        Option<DateTime<Utc>>,
}

impl std::fmt::Debug for Credentials {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str("Credentials(redacted)")
	}
}

impl Credentials {
	/// Construct non-expiring credentials, primarily for explicit environment
	/// keys.
	pub fn permanent(
		access_key: impl Into<String>,
		secret_key: impl Into<String>,
		session_token: Option<String>,
	) -> Self {
		Self {
			access_key: access_key.into(),
			secret_key: secret_key.into(),
			session_token,
			expires_at: None,
		}
	}

	fn temporary(wire: CredentialDocument) -> Result<Self> {
		let expires_at = DateTime::parse_from_rfc3339(&wire.expiration)
			.map_err(|error| Error::new(format!("invalid AWS credential expiration: {error}")))?
			.with_timezone(&Utc);
		if wire.access_key_id.is_empty() || wire.secret_access_key.is_empty() || wire.token.is_empty()
		{
			return Err(Error::new("AWS credential response omitted required fields"));
		}
		Ok(Self {
			access_key:    wire.access_key_id,
			secret_key:    wire.secret_access_key,
			session_token: Some(wire.token),
			expires_at:    Some(expires_at),
		})
	}

	fn is_fresh(&self) -> bool {
		self
			.expires_at
			.is_none_or(|expiration| expiration - Utc::now() > REFRESH_MARGIN)
	}
}

/// Resolves and refreshes AWS environment, ECS task-role, or EC2 instance-role
/// credentials.
pub struct CredentialProvider {
	client: Client,
	cached: Option<Credentials>,
}

impl CredentialProvider {
	/// Create a provider with short metadata-service deadlines.
	pub fn new() -> Result<Self> {
		let client = Client::builder()
			.no_proxy()
			.connect_timeout(REQUEST_TIMEOUT)
			.timeout(REQUEST_TIMEOUT)
			.build()
			.map_err(|error| Error::new(format!("building AWS metadata client: {error}")))?;
		Ok(Self { client, cached: None })
	}

	/// Return fresh credentials using the standard AWS workload-provider order.
	pub fn credentials(&mut self) -> Result<Credentials> {
		if let Some(credentials) = &self.cached
			&& credentials.is_fresh()
		{
			return Ok(credentials.clone());
		}
		let credentials = if let Some(credentials) = environment_credentials()? {
			credentials
		} else if let Some(endpoint) = ecs_credential_endpoint()? {
			self.ecs_credentials(&endpoint)?
		} else {
			self.imds_credentials()?
		};
		self.cached = Some(credentials.clone());
		Ok(credentials)
	}

	/// Force the next request to resolve fresh workload credentials.
	pub fn invalidate(&mut self) {
		self.cached = None;
	}

	fn ecs_credentials(&self, endpoint: &Url) -> Result<Credentials> {
		let mut request = self.client.get(endpoint.clone());
		if let Some(token) = ecs_authorization_token()? {
			request = request.header(reqwest::header::AUTHORIZATION, token);
		}
		let response = request
			.send()
			.map_err(|error| Error::new(format!("fetching ECS task credentials: {error}")))?;
		parse_credential_response(response, "ECS task credentials")
	}

	fn imds_credentials(&self) -> Result<Credentials> {
		if env::var("AWS_EC2_METADATA_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
		{
			return Err(Error::new(
				"AWS workload credentials are unavailable: EC2 metadata is disabled",
			));
		}
		let endpoint = metadata_endpoint()?;
		let token = imds_token(&self.client, &endpoint)?;
		let role =
			imds_get(&self.client, &endpoint, "/latest/meta-data/iam/security-credentials/", &token)?;
		let role = role
			.lines()
			.next()
			.map(str::trim)
			.filter(|role| !role.is_empty())
			.ok_or_else(|| Error::new("EC2 metadata returned no IAM role name"))?;
		let path = format!("/latest/meta-data/iam/security-credentials/{}", encode_component(role));
		let response = imds_request(&self.client, &endpoint, &path, &token)
			.send()
			.map_err(|error| Error::new(format!("fetching EC2 role credentials: {error}")))?;
		parse_credential_response(response, "EC2 role credentials")
	}
}

/// Discover the current AWS region from environment variables or EC2 metadata.
pub fn region() -> Result<String> {
	for key in ["AWS_REGION", "AWS_DEFAULT_REGION", "VMON_S3_REGION"] {
		if let Some(value) = env::var(key).ok().filter(|value| !value.trim().is_empty()) {
			return Ok(value);
		}
	}
	if env::var("AWS_EC2_METADATA_DISABLED").is_ok_and(|value| value.eq_ignore_ascii_case("true")) {
		return Err(Error::new("AWS region is unset and EC2 metadata is disabled"));
	}
	let client = Client::builder()
		.no_proxy()
		.connect_timeout(REQUEST_TIMEOUT)
		.timeout(REQUEST_TIMEOUT)
		.build()
		.map_err(|error| Error::new(format!("building AWS metadata client: {error}")))?;
	let endpoint = metadata_endpoint()?;
	let token = imds_token(&client, &endpoint)?;
	let document =
		imds_get(&client, &endpoint, "/latest/dynamic/instance-identity/document", &token)?;
	let identity: IdentityDocument = serde_json::from_str(&document)
		.map_err(|error| Error::new(format!("invalid EC2 identity document: {error}")))?;
	if identity.region.is_empty() {
		return Err(Error::new("EC2 identity document omitted region"));
	}
	Ok(identity.region)
}

fn environment_credentials() -> Result<Option<Credentials>> {
	let (access_key, secret_key) = environment_pair("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY");
	let (access_key, secret_key, session_token, error) = if access_key.is_some()
		|| secret_key.is_some()
	{
		(
			access_key,
			secret_key,
			env::var("AWS_SESSION_TOKEN")
				.ok()
				.filter(|value| !value.is_empty()),
			"AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set together",
		)
	} else {
		let (access_key, secret_key) = environment_pair("VMON_S3_ACCESS_KEY", "VMON_S3_SECRET_KEY");
		(
			access_key,
			secret_key,
			env::var("VMON_S3_SESSION_TOKEN")
				.ok()
				.filter(|value| !value.is_empty()),
			"VMON_S3_ACCESS_KEY and VMON_S3_SECRET_KEY must be set together",
		)
	};
	match (access_key, secret_key) {
		(None, None) => Ok(None),
		(Some(access_key), Some(secret_key)) => {
			Ok(Some(Credentials::permanent(access_key, secret_key, session_token)))
		},
		_ => Err(Error::new(error)),
	}
}

fn environment_pair(access_key: &str, secret_key: &str) -> (Option<String>, Option<String>) {
	(
		env::var(access_key).ok().filter(|value| !value.is_empty()),
		env::var(secret_key).ok().filter(|value| !value.is_empty()),
	)
}

fn ecs_credential_endpoint() -> Result<Option<Url>> {
	if let Some(relative) = env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
		.ok()
		.filter(|value| !value.is_empty())
	{
		if !relative.starts_with('/') {
			return Err(Error::new("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI must start with /"));
		}
		return Url::parse(&format!("{ECS_ENDPOINT}{relative}"))
			.map(Some)
			.map_err(|error| Error::new(format!("invalid ECS credential URI: {error}")));
	}
	let Some(full) = env::var("AWS_CONTAINER_CREDENTIALS_FULL_URI")
		.ok()
		.filter(|value| !value.is_empty())
	else {
		return Ok(None);
	};
	let url = Url::parse(&full).map_err(|error| {
		Error::new(format!("invalid AWS_CONTAINER_CREDENTIALS_FULL_URI: {error}"))
	})?;
	if url.scheme() != "https" && !(url.scheme() == "http" && trusted_http_credential_host(&url)) {
		return Err(Error::new(
			"AWS_CONTAINER_CREDENTIALS_FULL_URI must use HTTPS or a trusted container-credential \
			 HTTP host",
		));
	}
	Ok(Some(url))
}

fn trusted_http_credential_host(url: &Url) -> bool {
	match url.host() {
		Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
		Some(Host::Ipv4(address)) => address.is_loopback() || address.octets()[0..2] == [169, 254],
		Some(Host::Ipv6(address)) => {
			address.is_loopback() || address.segments() == [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23]
		},
		None => false,
	}
}

fn ecs_authorization_token() -> Result<Option<String>> {
	if let Some(token) = env::var("AWS_CONTAINER_AUTHORIZATION_TOKEN")
		.ok()
		.filter(|value| !value.is_empty())
	{
		return Ok(Some(token));
	}
	let Some(path) = env::var_os("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") else {
		return Ok(None);
	};
	let token = std::fs::read_to_string(Path::new(&path))
		.map_err(|error| Error::new(format!("reading ECS authorization token: {error}")))?;
	let token = token.trim();
	if token.is_empty() {
		return Err(Error::new("ECS authorization token file is empty"));
	}
	Ok(Some(token.to_owned()))
}

fn metadata_endpoint() -> Result<Url> {
	let endpoint = env::var("AWS_EC2_METADATA_SERVICE_ENDPOINT")
		.unwrap_or_else(|_| METADATA_ENDPOINT.to_owned());
	let endpoint = endpoint.trim_end_matches('/');
	let url = Url::parse(endpoint)
		.map_err(|error| Error::new(format!("invalid EC2 metadata endpoint: {error}")))?;
	if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
		return Err(Error::new("EC2 metadata endpoint must be an HTTP(S) URL"));
	}
	Ok(url)
}

fn imds_token(client: &Client, endpoint: &Url) -> Result<String> {
	let url = endpoint
		.join("/latest/api/token")
		.map_err(|error| Error::new(format!("invalid EC2 metadata token URL: {error}")))?;
	let response = client
		.put(url)
		.header("X-aws-ec2-metadata-token-ttl-seconds", IMDS_TOKEN_TTL)
		.send()
		.map_err(|error| Error::new(format!("fetching EC2 metadata token: {error}")))?;
	let response = require_success(response, "EC2 metadata token")?;
	let token = response
		.text()
		.map_err(|error| Error::new(format!("reading EC2 metadata token: {error}")))?;
	let token = token.trim();
	if token.is_empty() {
		return Err(Error::new("EC2 metadata token response was empty"));
	}
	Ok(token.to_owned())
}

fn imds_request(client: &Client, endpoint: &Url, path: &str, token: &str) -> RequestBuilder {
	let url = endpoint
		.join(path)
		.expect("validated absolute metadata path");
	client.get(url).header("X-aws-ec2-metadata-token", token)
}

fn imds_get(client: &Client, endpoint: &Url, path: &str, token: &str) -> Result<String> {
	let response = imds_request(client, endpoint, path, token)
		.send()
		.map_err(|error| Error::new(format!("fetching EC2 metadata {path}: {error}")))?;
	require_success(response, "EC2 metadata")?
		.text()
		.map_err(|error| Error::new(format!("reading EC2 metadata {path}: {error}")))
}

fn parse_credential_response(
	response: reqwest::blocking::Response,
	context: &str,
) -> Result<Credentials> {
	let response = require_success(response, context)?;
	let wire: CredentialDocument = response
		.json()
		.map_err(|error| Error::new(format!("invalid {context} response: {error}")))?;
	if wire.code.as_deref().is_some_and(|code| code != "Success") {
		return Err(Error::new(format!("{context} returned {}", wire.code.unwrap_or_default())));
	}
	Credentials::temporary(wire)
}

fn require_success(
	response: reqwest::blocking::Response,
	context: &str,
) -> Result<reqwest::blocking::Response> {
	if response.status().is_success() {
		return Ok(response);
	}
	let status = response.status();
	let body = response.text().unwrap_or_default();
	Err(Error::new(format!("{context} returned HTTP {status}: {}", body.trim())))
}

fn encode_component(value: &str) -> String {
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

/// Canonical URI, query, and host fields required to sign an HTTP URL.
pub fn canonical_url(url: &Url) -> Result<(String, String, String)> {
	let host = url
		.host_str()
		.ok_or_else(|| Error::new("AWS request URL omitted a host"))?;
	let host = match url.port() {
		Some(port) => format!("{host}:{port}"),
		None => host.to_owned(),
	};
	let uri = if url.path().is_empty() {
		"/".to_owned()
	} else {
		url.path().to_owned()
	};
	let mut query = url
		.query_pairs()
		.map(|(key, value)| (encode_component(&key), encode_component(&value)))
		.collect::<Vec<_>>();
	query.sort_unstable();
	let query = query
		.into_iter()
		.map(|(key, value)| format!("{key}={value}"))
		.collect::<Vec<_>>()
		.join("&");
	Ok((uri, query, host))
}

/// SHA-256 payload digest required by `SigV4`.
pub fn sha256_hex(payload: &[u8]) -> String {
	hex::encode(Sha256::digest(payload))
}

/// Date and Authorization headers produced for one `SigV4` request.
pub struct SignedAuthorization {
	/// `x-amz-date` header value included in the signature.
	pub date:          String,
	/// `Authorization` header value.
	pub authorization: String,
}

/// Sign one request at the current UTC time.
pub fn authorization_now(
	method: &str,
	canonical_uri: &str,
	canonical_query: &str,
	headers: &[(String, String)],
	payload_hash: &str,
	region: &str,
	service: &str,
	access_key: &str,
	secret_key: &str,
) -> SignedAuthorization {
	let timestamp = Utc::now();
	let date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
	let mut headers = headers
		.iter()
		.filter(|(name, _)| !name.eq_ignore_ascii_case("x-amz-date"))
		.cloned()
		.collect::<Vec<_>>();
	headers.push(("x-amz-date".to_owned(), date.clone()));
	let authorization = authorization(
		method,
		canonical_uri,
		canonical_query,
		&headers,
		payload_hash,
		timestamp,
		region,
		service,
		access_key,
		secret_key,
	);
	SignedAuthorization { date, authorization }
}

/// Build an AWS Signature Version 4 Authorization header.
pub fn authorization(
	method: &str,
	canonical_uri: &str,
	canonical_query: &str,
	headers: &[(String, String)],
	payload_hash: &str,
	timestamp: DateTime<Utc>,
	region: &str,
	service: &str,
	access_key: &str,
	secret_key: &str,
) -> String {
	let mut headers = headers
		.iter()
		.map(|(name, value)| (name.to_ascii_lowercase(), normalize_header(value)))
		.collect::<Vec<_>>();
	headers.sort_unstable_by(|left, right| left.0.cmp(&right.0));
	use std::fmt::Write as _;

	let mut canonical_headers = String::new();
	for (name, value) in &headers {
		let _ = writeln!(canonical_headers, "{name}:{value}");
	}
	let signed_headers = headers
		.iter()
		.map(|(name, _)| name.as_str())
		.collect::<Vec<_>>()
		.join(";");
	let canonical_request =
		[method, canonical_uri, canonical_query, &canonical_headers, &signed_headers, payload_hash]
			.join("\n");
	let date = timestamp.format("%Y%m%d").to_string();
	let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
	let scope = format!("{date}/{region}/{service}/aws4_request");
	let string_to_sign = format!(
		"AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
		hex::encode(Sha256::digest(canonical_request.as_bytes()))
	);
	let date_key = hmac(format!("AWS4{secret_key}").as_bytes(), &date);
	let region_key = hmac(&date_key, region);
	let service_key = hmac(&region_key, service);
	let signing_key = hmac(&service_key, "aws4_request");
	let signature = hex::encode(hmac(&signing_key, &string_to_sign));
	format!(
		"AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, \
		 Signature={signature}"
	)
}

fn normalize_header(value: &str) -> String {
	value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn hmac(key: &[u8], value: &str) -> [u8; 32] {
	let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key sizes");
	mac.update(value.as_bytes());
	let output = mac.finalize().into_bytes();
	let mut bytes = [0; 32];
	bytes.copy_from_slice(&output);
	bytes
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CredentialDocument {
	#[serde(default)]
	code:              Option<String>,
	access_key_id:     String,
	secret_access_key: String,
	token:             String,
	expiration:        String,
}

#[derive(Deserialize)]
struct IdentityDocument {
	region: String,
}

#[cfg(test)]
mod tests {
	use chrono::TimeZone;

	use super::*;

	#[test]
	fn signs_the_unsigned_s3_request_vector() {
		let credentials = Credentials::permanent(
			"AKIAIOSFODNN7EXAMPLE",
			"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			None,
		);
		let timestamp = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).single().unwrap();
		let headers = vec![
			("host".to_owned(), "examplebucket.s3.amazonaws.com".to_owned()),
			("range".to_owned(), "bytes=0-9".to_owned()),
			("x-amz-content-sha256".to_owned(), "UNSIGNED-PAYLOAD".to_owned()),
			("x-amz-date".to_owned(), "20130524T000000Z".to_owned()),
		];
		let value = authorization(
			"GET",
			"/test.txt",
			"",
			&headers,
			"UNSIGNED-PAYLOAD",
			timestamp,
			"us-east-1",
			"s3",
			&credentials.access_key,
			&credentials.secret_key,
		);
		assert_eq!(
			value,
			"AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
			 SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
			 Signature=edacce68e5445863e1f916719fac26d3be9c1581fccd7878ade0879597fc0dc1"
		);
	}

	#[test]
	fn credential_debug_output_is_redacted() {
		let credentials = Credentials::permanent("access-secret", "signing-secret", None);
		let debug = format!("{credentials:?}");
		assert_eq!(debug, "Credentials(redacted)");
		assert!(!debug.contains("secret"));
	}

	#[test]
	fn trusted_http_hosts_reject_public_endpoints() {
		assert!(trusted_http_credential_host(&Url::parse("http://127.0.0.1/creds").unwrap()));
		assert!(trusted_http_credential_host(&Url::parse("http://169.254.170.2/creds").unwrap()));
		assert!(trusted_http_credential_host(
			&Url::parse("http://[fd00:ec2::23]/v1/credentials").unwrap()
		));
		assert!(!trusted_http_credential_host(&Url::parse("http://example.com/creds").unwrap()));
		assert!(!trusted_http_credential_host(
			&Url::parse("http://[fd00:ec2::24]/v1/credentials").unwrap()
		));
	}
}
