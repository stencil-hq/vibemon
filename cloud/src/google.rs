//! Google workload OAuth tokens from the Compute metadata server.

use std::{env, time::Duration as StdDuration};

use chrono::{DateTime, Duration, Utc};
use reqwest::{Url, blocking::Client};
use serde::Deserialize;

use crate::{Error, Result};

const DEFAULT_METADATA_HOST: &str = "metadata.google.internal";
const LINK_LOCAL_METADATA_HOST: &str = "169.254.169.254";
const TOKEN_PATH: &str = "/computeMetadata/v1/instance/service-accounts/default/token?\
                          scopes=https%3A%2F%2Fwww.googleapis.com%2Fauth%2Fcloud-platform";
const REQUEST_TIMEOUT: StdDuration = StdDuration::from_secs(2);
const REFRESH_MARGIN: Duration = Duration::minutes(5);

/// Resolves and refreshes Google access tokens from workload metadata.
pub struct TokenProvider {
	client: Client,
	cached: Option<AccessToken>,
}

impl TokenProvider {
	/// Create a provider with short metadata-service deadlines.
	pub fn new() -> Result<Self> {
		let client = Client::builder()
			.no_proxy()
			.connect_timeout(REQUEST_TIMEOUT)
			.timeout(REQUEST_TIMEOUT)
			.build()
			.map_err(|error| Error::new(format!("building Google metadata client: {error}")))?;
		Ok(Self { client, cached: None })
	}

	/// Return a fresh cloud-platform OAuth access token.
	pub fn token(&mut self) -> Result<String> {
		if let Some(token) = &self.cached
			&& token.expires_at - Utc::now() > REFRESH_MARGIN
		{
			return Ok(token.value.clone());
		}
		let token = self.fetch()?;
		let value = token.value.clone();
		self.cached = Some(token);
		Ok(value)
	}

	/// Force the next request to mint a new metadata token.
	pub fn invalidate(&mut self) {
		self.cached = None;
	}

	fn fetch(&self) -> Result<AccessToken> {
		let configured = env::var("GCE_METADATA_HOST")
			.ok()
			.filter(|value| !value.is_empty());
		let hosts = configured
			.as_deref()
			.map_or_else(|| vec![DEFAULT_METADATA_HOST, LINK_LOCAL_METADATA_HOST], |host| vec![host]);
		let mut errors = Vec::new();
		for host in hosts {
			match self.fetch_from(host) {
				Ok(token) => return Ok(token),
				Err(error) => errors.push(error.to_string()),
			}
		}
		Err(Error::new(format!("Google workload token is unavailable: {}", errors.join("; "))))
	}

	fn fetch_from(&self, host: &str) -> Result<AccessToken> {
		let base = if host.starts_with("http://") || host.starts_with("https://") {
			host.trim_end_matches('/').to_owned()
		} else {
			format!("http://{host}")
		};
		let url = Url::parse(&format!("{base}{TOKEN_PATH}"))
			.map_err(|error| Error::new(format!("invalid Google metadata URL: {error}")))?;
		let response = self
			.client
			.get(url)
			.header("Metadata-Flavor", "Google")
			.send()
			.map_err(|error| Error::new(format!("request to {host} failed: {error}")))?;
		if !response.status().is_success() {
			let status = response.status();
			let body = response.text().unwrap_or_default();
			return Err(Error::new(format!("{host} returned HTTP {status}: {}", body.trim())));
		}
		let wire: TokenDocument = response
			.json()
			.map_err(|error| Error::new(format!("invalid Google metadata token: {error}")))?;
		access_token_from_document(wire)
	}
}

fn access_token_from_document(wire: TokenDocument) -> Result<AccessToken> {
	if wire.access_token.trim().is_empty() || wire.expires_in <= 0 {
		return Err(Error::new("Google metadata token omitted required fields"));
	}
	let lifetime = Duration::try_seconds(wire.expires_in)
		.ok_or_else(|| Error::new("Google metadata token returned an invalid expiration"))?;
	let expires_at = Utc::now()
		.checked_add_signed(lifetime)
		.ok_or_else(|| Error::new("Google metadata token expiration is out of range"))?;
	Ok(AccessToken { value: wire.access_token, expires_at })
}

struct AccessToken {
	value:      String,
	expires_at: DateTime<Utc>,
}

#[derive(Deserialize)]
struct TokenDocument {
	access_token: String,
	expires_in:   i64,
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn rejects_unusable_token_documents() {
		assert!(
			access_token_from_document(TokenDocument {
				access_token: " ".to_owned(),
				expires_in:   3600,
			})
			.is_err()
		);
		assert!(
			access_token_from_document(TokenDocument {
				access_token: "token".to_owned(),
				expires_in:   i64::MAX,
			})
			.is_err()
		);
	}

	#[test]
	fn metadata_host_accepts_hostnames_and_urls() {
		for host in ["metadata.google.internal", "http://127.0.0.1:8080/"] {
			let base = if host.starts_with("http://") || host.starts_with("https://") {
				host.trim_end_matches('/').to_owned()
			} else {
				format!("http://{host}")
			};
			assert!(Url::parse(&format!("{base}{TOKEN_PATH}")).is_ok());
		}
	}
}
