//! Registry authentication.
//!
//! Parses `WWW-Authenticate` challenges (Docker token flow) and resolves the
//! E2B `fromImageRegistry` credential kinds (plain registry login, AWS ECR
//! `GetAuthorizationToken`, GCP service-account OAuth).

use std::collections::BTreeMap;

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde::{Deserialize, Serialize};

use crate::api::RegistryAuth;

const ECR_TARGET: &str = "AmazonEC2ContainerRegistry_V20150921.GetAuthorizationToken";
const ECR_CONTENT_TYPE: &str = "application/x-amz-json-1.1";
const GCP_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";
const GCP_DEFAULT_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
/// Username Google registries expect alongside an OAuth access token.
const GCP_OAUTH_USER: &str = "oauth2accesstoken";

/// Username/password pair presented to a registry (Basic auth or token realm).
#[derive(Clone, PartialEq, Eq)]
pub struct BasicCredentials {
	pub username: String,
	pub password: String,
}

impl std::fmt::Debug for BasicCredentials {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("BasicCredentials")
			.field("username", &self.username)
			.finish_non_exhaustive()
	}
}

/// Authentication scheme of a registry challenge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
	Basic,
	Bearer,
}

/// A parsed `WWW-Authenticate` header.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Challenge {
	pub scheme: Scheme,
	/// Lowercased parameter names (`realm`, `service`, `scope`, …).
	pub params: BTreeMap<String, String>,
}

impl Challenge {
	/// Parses `Bearer realm="…",service="…",scope="…"` (quoted values may
	/// contain commas and backslash escapes). Returns `None` for unknown
	/// schemes.
	pub fn parse(header: &str) -> Option<Self> {
		let header = header.trim();
		let (scheme, rest) = header
			.split_once(char::is_whitespace)
			.unwrap_or((header, ""));
		let scheme = match scheme.to_ascii_lowercase().as_str() {
			"bearer" => Scheme::Bearer,
			"basic" => Scheme::Basic,
			_ => return None,
		};
		let mut params = BTreeMap::new();
		let mut chars = rest.chars().peekable();
		loop {
			while chars.next_if(|c| c.is_whitespace() || *c == ',').is_some() {}
			let mut key = String::new();
			while let Some(c) = chars.next_if(|c| *c != '=' && *c != ',') {
				key.push(c);
			}
			if key.trim().is_empty() {
				break;
			}
			if chars.next() != Some('=') {
				break;
			}
			let mut value = String::new();
			if chars.next_if_eq(&'"').is_some() {
				while let Some(c) = chars.next() {
					match c {
						'\\' => value.extend(chars.next()),
						'"' => break,
						c => value.push(c),
					}
				}
			} else {
				while let Some(c) = chars.next_if(|c| *c != ',') {
					value.push(c);
				}
				let trimmed = value.trim().to_owned();
				value = trimmed;
			}
			params.insert(key.trim().to_ascii_lowercase(), value);
		}
		Some(Self { scheme, params })
	}

	/// Token endpoint URL for a Bearer challenge, with `service` and the
	/// pull `scope` for `repository` appended.
	pub fn token_url(&self, repository: &str) -> Result<url::Url> {
		let realm = self
			.params
			.get("realm")
			.ok_or_else(|| anyhow!("bearer challenge without realm"))?;
		let mut url =
			url::Url::parse(realm).with_context(|| format!("invalid token realm {realm:?}"))?;
		{
			let mut query = url.query_pairs_mut();
			if let Some(service) = self.params.get("service") {
				query.append_pair("service", service);
			}
			let scope = self
				.params
				.get("scope")
				.cloned()
				.unwrap_or_else(|| format!("repository:{repository}:pull"));
			query.append_pair("scope", &scope);
		}
		Ok(url)
	}
}

/// Body of a Docker token endpoint response.
#[derive(Deserialize)]
pub struct TokenResponse {
	pub token:        Option<String>,
	pub access_token: Option<String>,
}

impl TokenResponse {
	/// The bearer token (`token` preferred, `access_token` for OAuth2-style
	/// realms).
	pub fn into_token(self) -> Result<String> {
		self
			.token
			.filter(|t| !t.is_empty())
			.or_else(|| self.access_token.filter(|t| !t.is_empty()))
			.ok_or_else(|| anyhow!("token endpoint returned no token"))
	}
}

/// Resolves E2B registry credentials into the Basic credentials presented to
/// the registry (ECR and GCP mint short-lived passwords first).
pub async fn resolve(http: &reqwest::Client, auth: &RegistryAuth) -> Result<BasicCredentials> {
	match auth {
		RegistryAuth::Registry { username, password } => {
			Ok(BasicCredentials { username: username.clone(), password: password.clone() })
		},
		RegistryAuth::Aws { aws_access_key_id, aws_secret_access_key, aws_region } => {
			ecr_credentials(http, aws_access_key_id, aws_secret_access_key, aws_region).await
		},
		RegistryAuth::Gcp { service_account_json } => {
			let token = gcp_access_token(http, service_account_json).await?;
			Ok(BasicCredentials { username: GCP_OAUTH_USER.to_owned(), password: token })
		},
	}
}

/// Calls ECR `GetAuthorizationToken` signed with `SigV4` and decodes the
/// `user:password` token.
async fn ecr_credentials(
	http: &reqwest::Client,
	access_key: &str,
	secret_key: &str,
	region: &str,
) -> Result<BasicCredentials> {
	if region.is_empty()
		|| !region
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
	{
		bail!("invalid AWS region {region:?}");
	}
	let host = format!("api.ecr.{region}.amazonaws.com");
	let body = b"{}";
	let headers = [
		("content-type".to_owned(), ECR_CONTENT_TYPE.to_owned()),
		("host".to_owned(), host.clone()),
		("x-amz-target".to_owned(), ECR_TARGET.to_owned()),
	];
	let signed = vmon_cloud::aws::authorization_now(
		"POST",
		"/",
		"",
		&headers,
		&vmon_cloud::aws::sha256_hex(body),
		region,
		"ecr",
		access_key,
		secret_key,
	);
	let response = http
		.post(format!("https://{host}/"))
		.header("content-type", ECR_CONTENT_TYPE)
		.header("x-amz-target", ECR_TARGET)
		.header("x-amz-date", signed.date)
		.header("authorization", signed.authorization)
		.body(body.as_slice())
		.send()
		.await
		.context("requesting ECR authorization token")?;
	let status = response.status();
	if !status.is_success() {
		let text = response.text().await.unwrap_or_default();
		bail!("ECR GetAuthorizationToken returned HTTP {status}: {}", text.trim());
	}
	#[derive(Deserialize)]
	#[serde(rename_all = "camelCase")]
	struct Data {
		authorization_token: String,
	}
	#[derive(Deserialize)]
	#[serde(rename_all = "camelCase")]
	struct Body {
		authorization_data: Vec<Data>,
	}
	let parsed: Body = response
		.json()
		.await
		.context("invalid ECR token response")?;
	let token = parsed
		.authorization_data
		.into_iter()
		.next()
		.ok_or_else(|| anyhow!("ECR returned no authorization data"))?;
	decode_basic_token(&token.authorization_token)
}

/// Decodes a base64 `user:password` token (ECR authorization tokens).
pub fn decode_basic_token(token: &str) -> Result<BasicCredentials> {
	let decoded = B64
		.decode(token.trim())
		.context("authorization token is not base64")?;
	let decoded = String::from_utf8(decoded).context("authorization token is not UTF-8")?;
	match decoded.split_once(':') {
		Some((user, password)) if !user.is_empty() && !password.is_empty() => {
			Ok(BasicCredentials { username: user.to_owned(), password: password.to_owned() })
		},
		_ => bail!("authorization token lacks user:password"),
	}
}

/// Fields of a Google service-account key file used for the JWT grant.
#[derive(Deserialize)]
struct ServiceAccount {
	client_email: String,
	private_key:  String,
	#[serde(default)]
	token_uri:    Option<String>,
}

#[derive(Serialize)]
struct JwtClaims<'a> {
	iss:   &'a str,
	scope: &'a str,
	aud:   &'a str,
	iat:   u64,
	exp:   u64,
}

/// Exchanges a service-account key for an OAuth access token (RFC 7523 JWT
/// bearer grant).
async fn gcp_access_token(http: &reqwest::Client, json: &str) -> Result<String> {
	let account: ServiceAccount =
		serde_json::from_str(json).context("invalid GCP service account JSON")?;
	let token_uri = account
		.token_uri
		.as_deref()
		.unwrap_or(GCP_DEFAULT_TOKEN_URI);
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |d| d.as_secs());
	let claims = JwtClaims {
		iss:   &account.client_email,
		scope: GCP_SCOPE,
		aud:   token_uri,
		iat:   now,
		exp:   now + 3600,
	};
	let key = jsonwebtoken::EncodingKey::from_rsa_pem(account.private_key.as_bytes())
		.context("invalid service account private key")?;
	let assertion = jsonwebtoken::encode(
		&jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
		&claims,
		&key,
	)
	.context("signing service account JWT")?;
	let response = http
		.post(token_uri)
		.form(&[
			("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
			("assertion", assertion.as_str()),
		])
		.send()
		.await
		.context("requesting GCP access token")?;
	let status = response.status();
	if !status.is_success() {
		let text = response.text().await.unwrap_or_default();
		bail!("GCP token endpoint returned HTTP {status}: {}", text.trim());
	}
	let body: TokenResponse = response
		.json()
		.await
		.context("invalid GCP token response")?;
	body.into_token()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_docker_hub_bearer_challenge() {
		let c = Challenge::parse(r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull""#)
			.unwrap();
		assert_eq!(c.scheme, Scheme::Bearer);
		assert_eq!(c.params["realm"], "https://auth.docker.io/token");
		assert_eq!(c.params["service"], "registry.docker.io");
		assert_eq!(c.params["scope"], "repository:library/alpine:pull");
		let url = c.token_url("library/alpine").unwrap();
		assert_eq!(url.as_str(), "https://auth.docker.io/token?service=registry.docker.io&scope=repository%3Alibrary%2Falpine%3Apull");
	}

	#[test]
	fn quoted_values_keep_commas_escapes_and_case_insensitive_keys() {
		let c = Challenge::parse(
			r#"bearer Realm="https://r/t?a=1", scope="repository:a/b:pull,push", error="x\"y""#,
		)
		.unwrap();
		assert_eq!(c.params["realm"], "https://r/t?a=1");
		assert_eq!(c.params["scope"], "repository:a/b:pull,push");
		assert_eq!(c.params["error"], "x\"y");
		// The realm's existing query is preserved; a missing scope defaults to
		// pull.
		let c = Challenge::parse(r#"Bearer realm="https://r/t?a=1""#).unwrap();
		assert_eq!(
			c.token_url("o/i").unwrap().as_str(),
			"https://r/t?a=1&scope=repository%3Ao%2Fi%3Apull"
		);
	}

	#[test]
	fn basic_and_unknown_schemes() {
		let c = Challenge::parse(r#"Basic realm="Registry Realm""#).unwrap();
		assert_eq!(c.scheme, Scheme::Basic);
		assert_eq!(c.params["realm"], "Registry Realm");
		assert!(Challenge::parse("Negotiate abc").is_none());
		assert!(Challenge::parse("Bearer").unwrap().token_url("x").is_err(), "realm is required");
	}

	#[test]
	fn unquoted_values_and_trailing_commas() {
		let c = Challenge::parse("Bearer realm=https://r/token , service=svc,").unwrap();
		assert_eq!(c.params["realm"], "https://r/token");
		assert_eq!(c.params["service"], "svc");
	}

	#[test]
	fn ecr_token_decoding() {
		let creds = decode_basic_token(&B64.encode("AWS:secret:with:colons")).unwrap();
		assert_eq!(creds.username, "AWS");
		assert_eq!(creds.password, "secret:with:colons");
		assert!(decode_basic_token(&B64.encode("nopassword")).is_err());
		assert!(decode_basic_token(&B64.encode(":x")).is_err());
		assert!(decode_basic_token("!!!").is_err());
	}

	#[test]
	fn token_response_prefers_token_over_access_token() {
		let r: TokenResponse = serde_json::from_str(r#"{"token":"a","access_token":"b"}"#).unwrap();
		assert_eq!(r.into_token().unwrap(), "a");
		let r: TokenResponse = serde_json::from_str(r#"{"token":"","access_token":"b"}"#).unwrap();
		assert_eq!(r.into_token().unwrap(), "b");
		let r: TokenResponse = serde_json::from_str("{}").unwrap();
		assert!(r.into_token().is_err());
	}
}
