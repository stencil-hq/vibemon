//! Short-lived credentials for private OCI registries.
//!
//! `skopeo` receives a temporary Docker-style auth file instead of credentials
//! in argv. Google Artifact Registry uses the GCE workload token; Amazon ECR
//! uses environment, ECS task-role, or EC2 instance-role credentials. Other
//! registries use the operator-managed `VMON_REGISTRY_AUTH_FILE` override.

use std::{
	path::{Path, PathBuf},
	time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use chrono::Utc;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::json;
use tempfile::NamedTempFile;
use vmon_cloud::{aws, google};

/// Environment override naming a caller-managed Docker auth file.
const AUTH_FILE_ENV: &str = "VMON_REGISTRY_AUTH_FILE";
const OAUTH_USER: &str = "oauth2accesstoken";
const ECR_TARGET: &str = "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken";
const ECR_CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// An auth file for skopeo's `--authfile`.
///
/// Either borrowed from [`AUTH_FILE_ENV`] or a temporary file removed on drop.
pub enum AuthFile {
	/// Operator-managed file; vmon only reads the path.
	Managed(PathBuf),
	/// Token minted for this pull, deleted when the guard falls out of scope.
	Ephemeral(NamedTempFile),
}

impl AuthFile {
	/// Path to hand to `skopeo --authfile`.
	pub fn path(&self) -> &Path {
		match self {
			Self::Managed(path) => path,
			Self::Ephemeral(file) => file.path(),
		}
	}
}

/// Resolve credentials for `reference`, or `None` when the registry needs none.
///
/// Best-effort by design: public images remain pullable without cloud metadata;
/// `skopeo` reports the authoritative error when a private pull has no usable
/// workload identity.
pub fn auth_file_for(reference: &str) -> Option<AuthFile> {
	if let Some(path) = std::env::var_os(AUTH_FILE_ENV) {
		return Some(AuthFile::Managed(PathBuf::from(path)));
	}
	let host = registry_host(reference)?;
	let credentials = automatic_credentials(&host)
		.inspect_err(|error| tracing::warn!(%error, host, "registry workload auth failed"))
		.ok()??;
	write_auth_file(&host, &credentials.0, &credentials.1)
		.inspect_err(|error| tracing::warn!(%error, host, "registry auth file write failed"))
		.ok()
		.map(AuthFile::Ephemeral)
}

fn automatic_credentials(host: &str) -> Result<Option<(String, String)>, String> {
	if is_google_registry(host) {
		let mut provider = google::TokenProvider::new().map_err(|error| error.to_string())?;
		return provider
			.token()
			.map(|token| Some((OAUTH_USER.to_owned(), token)))
			.map_err(|error| error.to_string());
	}
	let Some(registry) = parse_ecr_registry(host) else {
		return Ok(None);
	};
	ecr_credentials(&registry).map(Some)
}

/// The registry host of a skopeo reference, if it names one.
fn registry_host(reference: &str) -> Option<String> {
	let rest = if let Some(rest) = reference.strip_prefix("docker://") {
		rest
	} else {
		let local = super::IMAGE_TRANSPORT_PREFIXES
			.iter()
			.any(|prefix| *prefix != "docker://" && reference.starts_with(prefix));
		if local {
			return None;
		}
		reference
	};
	let (candidate, _path) = rest.split_once('/')?;
	let looks_like_host =
		candidate.contains('.') || candidate.contains(':') || candidate == "localhost";
	looks_like_host.then(|| candidate.to_ascii_lowercase())
}

fn is_google_registry(host: &str) -> bool {
	let host = host.split_once(':').map_or(host, |(name, _port)| name);
	host == "gcr.io" || host.ends_with(".gcr.io") || host.ends_with("-docker.pkg.dev")
}

#[derive(Debug, Eq, PartialEq)]
struct EcrRegistry {
	region:   String,
	api_host: String,
}

fn parse_ecr_registry(host: &str) -> Option<EcrRegistry> {
	let host = host.split_once(':').map_or(host, |(name, _port)| name);
	let (account, region, api_host) = if let Some(prefix) = host.strip_suffix(".amazonaws.com.cn") {
		let (account, region) = prefix.split_once(".dkr.ecr.")?;
		(account, region, format!("ecr.{region}.amazonaws.com.cn"))
	} else if let Some(prefix) = host.strip_suffix(".amazonaws.com") {
		let (account, region) = prefix.split_once(".dkr.ecr.")?;
		(account, region, format!("ecr.{region}.amazonaws.com"))
	} else if let Some(prefix) = host.strip_suffix(".on.aws") {
		let (account, region) = prefix.split_once(".dkr-ecr.")?;
		(account, region, format!("ecr.{region}.api.aws"))
	} else {
		return None;
	};
	if account.len() != 12
		|| !account.bytes().all(|byte| byte.is_ascii_digit())
		|| !valid_aws_region(region)
	{
		return None;
	}
	Some(EcrRegistry { region: region.to_owned(), api_host })
}

fn valid_aws_region(region: &str) -> bool {
	let mut parts = region.split('-').peekable();
	let Some(first) = parts.next() else {
		return false;
	};
	if first.is_empty() || !first.bytes().all(|byte| byte.is_ascii_lowercase()) {
		return false;
	}
	let mut middle_parts = 0;
	while let Some(part) = parts.next() {
		if part.is_empty() {
			return false;
		}
		if parts.peek().is_none() {
			return middle_parts != 0 && part.bytes().all(|byte| byte.is_ascii_digit());
		}
		if !part.bytes().all(|byte| byte.is_ascii_lowercase()) {
			return false;
		}
		middle_parts += 1;
	}
	false
}

fn ecr_credentials(registry: &EcrRegistry) -> Result<(String, String), String> {
	let mut provider = aws::CredentialProvider::new().map_err(|error| error.to_string())?;
	let credentials = provider.credentials().map_err(|error| error.to_string())?;
	let body = b"{}";
	let payload_hash = aws::sha256_hex(body);
	let timestamp = Utc::now();
	let date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();
	let host = &registry.api_host;
	let mut signed_headers = vec![
		("content-type".to_owned(), ECR_CONTENT_TYPE.to_owned()),
		("host".to_owned(), host.to_owned()),
		("x-amz-date".to_owned(), date.clone()),
		("x-amz-target".to_owned(), ECR_TARGET.to_owned()),
	];
	if let Some(token) = &credentials.session_token {
		signed_headers.push(("x-amz-security-token".to_owned(), token.clone()));
	}
	let authorization = aws::authorization(
		"POST",
		"/",
		"",
		&signed_headers,
		&payload_hash,
		timestamp,
		&registry.region,
		"ecr",
		&credentials.access_key,
		&credentials.secret_key,
	);
	let client = Client::builder()
		.connect_timeout(REQUEST_TIMEOUT)
		.timeout(REQUEST_TIMEOUT)
		.build()
		.map_err(|error| format!("building ECR client: {error}"))?;
	let mut request = client
		.post(format!("https://{host}/"))
		.header("content-type", ECR_CONTENT_TYPE)
		.header("host", host)
		.header("x-amz-date", date)
		.header("x-amz-target", ECR_TARGET)
		.header("authorization", authorization)
		.body(body.as_slice());
	if let Some(token) = &credentials.session_token {
		request = request.header("x-amz-security-token", token);
	}
	let response = request
		.send()
		.map_err(|error| format!("requesting ECR token: {error}"))?;
	if !response.status().is_success() {
		let status = response.status();
		let body = response.text().unwrap_or_default();
		return Err(format!("ECR token API returned HTTP {status}: {}", body.trim()));
	}
	let document: EcrTokenResponse = response
		.json()
		.map_err(|error| format!("invalid ECR token response: {error}"))?;
	let token = document
		.authorization_data
		.into_iter()
		.next()
		.ok_or_else(|| "ECR token response contained no authorization data".to_owned())?
		.authorization_token;
	decode_ecr_token(&token)
}

fn decode_ecr_token(token: &str) -> Result<(String, String), String> {
	let decoded = B64
		.decode(token)
		.map_err(|error| format!("invalid ECR authorization token: {error}"))?;
	let decoded = String::from_utf8(decoded)
		.map_err(|error| format!("ECR authorization token was not UTF-8: {error}"))?;
	let (user, password) = decoded
		.split_once(':')
		.filter(|(user, password)| !user.is_empty() && !password.is_empty())
		.ok_or_else(|| "ECR authorization token omitted username or password".to_owned())?;
	Ok((user.to_owned(), password.to_owned()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EcrTokenResponse {
	authorization_data: Vec<EcrAuthorizationData>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EcrAuthorizationData {
	authorization_token: String,
}

fn write_auth_file(host: &str, user: &str, secret: &str) -> std::io::Result<NamedTempFile> {
	use std::io::Write as _;

	let document = json!({
		"auths": { host: { "auth": B64.encode(format!("{user}:{secret}")) } }
	});
	let mut file = NamedTempFile::new()?;
	restrict(&file)?;
	file.write_all(document.to_string().as_bytes())?;
	file.flush()?;
	Ok(file)
}

#[cfg(unix)]
fn restrict(file: &NamedTempFile) -> std::io::Result<()> {
	use std::os::unix::fs::PermissionsExt;
	file
		.as_file()
		.set_permissions(std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn restrict(_file: &NamedTempFile) -> std::io::Result<()> {
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn registry_hosts_are_recognized_only_for_remote_references() {
		assert_eq!(
			registry_host("docker://us-central1-docker.pkg.dev/p/r/i:t").as_deref(),
			Some("us-central1-docker.pkg.dev")
		);
		assert_eq!(
			registry_host("123456789012.dkr.ecr.us-east-1.amazonaws.com/team/image:t").as_deref(),
			Some("123456789012.dkr.ecr.us-east-1.amazonaws.com")
		);
		assert_eq!(registry_host("localhost:5000/img:t").as_deref(), Some("localhost:5000"));
		assert_eq!(registry_host("alpine:3.20"), None);
		assert_eq!(registry_host("library/alpine:3.20"), None);
		assert_eq!(registry_host("oci:/var/lib/vmon/img:latest"), None);
		assert_eq!(registry_host("dir:/var/lib/vmon/img"), None);
	}

	#[test]
	fn provider_registries_are_matched_by_suffix_not_substring() {
		assert!(is_google_registry("gcr.io"));
		assert!(is_google_registry("eu.gcr.io"));
		assert!(is_google_registry("us-central1-docker.pkg.dev"));
		assert_eq!(
			parse_ecr_registry("123456789012.dkr.ecr.us-east-1.amazonaws.com"),
			Some(EcrRegistry {
				region:   "us-east-1".to_owned(),
				api_host: "ecr.us-east-1.amazonaws.com".to_owned(),
			})
		);
		assert_eq!(
			parse_ecr_registry("123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn"),
			Some(EcrRegistry {
				region:   "cn-north-1".to_owned(),
				api_host: "ecr.cn-north-1.amazonaws.com.cn".to_owned(),
			})
		);
		assert_eq!(
			parse_ecr_registry("123456789012.dkr-ecr.eu-west-1.on.aws"),
			Some(EcrRegistry {
				region:   "eu-west-1".to_owned(),
				api_host: "ecr.eu-west-1.api.aws".to_owned(),
			})
		);
		assert!(!is_google_registry("gcr.io.evil.example"));
		assert!(!is_google_registry("notgcr.io.example.com"));
		assert_eq!(parse_ecr_registry("123456789012.dkr.ecr.us-east-1.amazonaws.com.evil"), None);
		assert_eq!(parse_ecr_registry("not-an-account.dkr.ecr.us-east-1.amazonaws.com"), None);
		assert_eq!(parse_ecr_registry("123456789012.dkr.ecr.not-a-region.amazonaws.com"), None);
		assert_eq!(parse_ecr_registry("123456789012.dkr-ecr.eu-west-1.on.aws.evil"), None);
	}

	#[test]
	fn ecr_tokens_decode_as_docker_credentials() {
		let token = B64.encode("AWS:temporary:password");
		assert_eq!(decode_ecr_token(&token), Ok(("AWS".to_owned(), "temporary:password".to_owned())));
		assert!(decode_ecr_token(&B64.encode("missing-separator")).is_err());
		assert!(decode_ecr_token(&B64.encode(":missing-user")).is_err());
		assert!(decode_ecr_token(&B64.encode("AWS:")).is_err());
		assert!(decode_ecr_token("not-base64").is_err());
	}

	#[test]
	fn auth_file_encodes_docker_credentials_and_stays_private() {
		let file = write_auth_file("us-central1-docker.pkg.dev", OAUTH_USER, "tok").expect("write");
		let body = std::fs::read_to_string(file.path()).expect("read");
		let parsed: serde_json::Value = serde_json::from_str(&body).expect("json");
		let entry = parsed["auths"]["us-central1-docker.pkg.dev"]["auth"]
			.as_str()
			.expect("auth entry");
		let decoded = String::from_utf8(B64.decode(entry).expect("base64")).expect("utf8");
		assert_eq!(decoded, "oauth2accesstoken:tok");
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			let mode = std::fs::metadata(file.path())
				.expect("stat")
				.permissions()
				.mode();
			assert_eq!(mode & 0o077, 0, "credentials must not be group/world readable");
		}
	}
}
