//! Bring-your-own-bucket mounts: the `vmonS3Mounts` create extension
//! (ARCHITECTURE.md §9.6, docs/e2b-compat.md "Extensions").
//!
//! Validated at create; the non-secret part of every mount is stored in
//! `sandboxes.s3_mounts`, inline credentials only encrypted
//! (`sandboxes.s3_mounts_enc`), and `credentialsSecret` references are
//! re-resolved from the team's secrets on every create, resume, and fork, so
//! a rotated secret applies at the next resume. Credentials are never logged
//! or returned.

use serde::Deserialize;
use vmon_cloud::s3::{S3Config, S3Location};
use vmon_node::api::{S3Mount, S3MountCredentials, S3MountMode, VolumeMount};

use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
	store::sandboxes::{S3MountRecord, SandboxRow},
};

/// Most S3 mounts (bring-your-own plus object-store volumes) per sandbox.
pub const MAX_S3_MOUNTS: usize = 8;

/// One `vmonS3Mounts[]` item of a create request.
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct S3MountRequest {
	pub path:               String,
	pub uri:                String,
	#[serde(default)]
	pub mode:               Option<String>,
	#[serde(default)]
	pub endpoint:           Option<String>,
	#[serde(default)]
	pub region:             Option<String>,
	#[serde(default)]
	pub credentials:        Option<S3MountCredentials>,
	/// Name (or id) of a team secret whose value is the credentials JSON.
	#[serde(default)]
	pub credentials_secret: Option<String>,
}

impl std::fmt::Debug for S3MountRequest {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("S3MountRequest")
			.field("path", &self.path)
			.field("uri", &self.uri)
			.field("mode", &self.mode)
			.field("credentials_secret", &self.credentials_secret)
			.finish_non_exhaustive()
	}
}

/// Resolved mounts of a launch: what the node gets and what `MySQL` keeps.
#[derive(Clone, Debug, Default)]
pub struct ResolvedS3Mounts {
	/// Forwarded to the node (with credentials).
	pub mounts:  Vec<S3Mount>,
	/// `sandboxes.s3_mounts` (no credentials).
	pub records: Vec<S3MountRecord>,
	/// `sandboxes.s3_mounts_enc`: encrypted inline credentials by index.
	pub enc:     Option<Vec<u8>>,
}

fn parse_mode(mode: Option<&str>) -> ApiResult<S3MountMode> {
	match mode.unwrap_or("ro") {
		"ro" => Ok(S3MountMode::Ro),
		"overlay" => Ok(S3MountMode::Overlay),
		"writeback" => Ok(S3MountMode::Writeback),
		other => Err(ApiError::bad_request(format!(
			"vmonS3Mounts mode {other:?} must be one of ro, overlay, writeback"
		))),
	}
}

fn overlaps(left: &str, right: &str) -> bool {
	left == right || left.starts_with(&format!("{right}/")) || right.starts_with(&format!("{left}/"))
}

/// Syntactic validation of `requests` against each other and the volume
/// mount paths (no I/O).
///
/// # Errors
///
/// `400` describing the first invalid mount.
pub fn validate(requests: &[S3MountRequest], volume_paths: &[String]) -> ApiResult<()> {
	if requests.len() > MAX_S3_MOUNTS {
		return Err(ApiError::bad_request(format!(
			"at most {MAX_S3_MOUNTS} vmonS3Mounts are supported"
		)));
	}
	let mut paths: Vec<&str> = volume_paths.iter().map(String::as_str).collect();
	for request in requests {
		super::validate_mount_path(&request.path)?;
		if let Some(other) = paths.iter().find(|other| overlaps(&request.path, other)) {
			return Err(ApiError::bad_request(format!(
				"vmonS3Mounts path {} overlaps mount path {other}",
				request.path
			)));
		}
		paths.push(&request.path);
		parse_mode(request.mode.as_deref())?;
		if request.credentials.is_some() && request.credentials_secret.is_some() {
			return Err(ApiError::bad_request(
				"vmonS3Mounts items take credentials or credentialsSecret, not both",
			));
		}
		if let Some(credentials) = &request.credentials
			&& (credentials.access_key_id.is_empty() || credentials.secret_access_key.is_empty())
		{
			return Err(ApiError::bad_request(
				"vmonS3Mounts credentials need accessKeyId and secretAccessKey",
			));
		}
		if let Some(endpoint) = &request.endpoint {
			let url = url::Url::parse(endpoint).map_err(|_| {
				ApiError::bad_request(format!("vmonS3Mounts endpoint {endpoint:?} is not a URL"))
			})?;
			if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
				return Err(ApiError::bad_request(format!(
					"vmonS3Mounts endpoint {endpoint:?} must be an http(s) URL"
				)));
			}
		}
		let config = S3Config {
			location: request.uri.clone(),
			endpoint: request.endpoint.clone(),
			allow_http: true,
			..S3Config::default()
		};
		let region = request.region.as_deref().unwrap_or("us-east-1");
		S3Location::parse(&config, region).map_err(|error| {
			ApiError::bad_request(format!("vmonS3Mounts uri {:?}: {error}", request.uri))
		})?;
	}
	Ok(())
}

/// Credentials stored in a team secret.
async fn secret_credentials(
	state: &AppState,
	team_id: &str,
	name: &str,
) -> ApiResult<S3MountCredentials> {
	let enc = state
		.store
		.secret_value(team_id, name, None)
		.await?
		.ok_or_else(|| {
			ApiError::bad_request(format!("vmonS3Mounts credentialsSecret '{name}' not found"))
		})?;
	let plain = state
		.keys
		.decrypt(&enc)
		.map_err(|_| ApiError::internal(format!("secret '{name}' cannot be decrypted")))?;
	serde_json::from_slice(&plain).map_err(|_| {
		ApiError::bad_request(format!(
			"secret '{name}' must hold {{\"accessKeyId\", \"secretAccessKey\", \"sessionToken\"?}}"
		))
	})
}

/// Resolves validated `requests` of a new sandbox.
///
/// # Errors
///
/// `400` for unknown or malformed credential secrets.
pub async fn resolve(
	state: &AppState,
	team_id: &str,
	requests: &[S3MountRequest],
) -> ApiResult<ResolvedS3Mounts> {
	let mut resolved = ResolvedS3Mounts::default();
	let mut inline = Vec::with_capacity(requests.len());
	for request in requests {
		let credentials = match (&request.credentials, &request.credentials_secret) {
			(Some(credentials), _) => Some(credentials.clone()),
			(None, Some(name)) => Some(secret_credentials(state, team_id, name).await?),
			(None, None) => None,
		};
		let mode = parse_mode(request.mode.as_deref())?;
		resolved.records.push(S3MountRecord {
			path: request.path.clone(),
			uri: request.uri.clone(),
			mode,
			endpoint: request.endpoint.clone(),
			region: request.region.clone(),
			credentials_secret: request.credentials_secret.clone(),
		});
		inline.push(request.credentials.clone());
		resolved.mounts.push(S3Mount {
			path: request.path.clone(),
			uri: request.uri.clone(),
			mode,
			endpoint: request.endpoint.clone(),
			region: request.region.clone(),
			credentials,
		});
	}
	if inline.iter().any(Option::is_some) {
		let plain = serde_json::to_vec(&inline).expect("credentials serialize");
		resolved.enc = Some(state.keys.encrypt(&plain));
	}
	Ok(resolved)
}

/// The mounts of an existing sandbox (resume, fork), credentials re-read
/// from the encrypted column and the referenced secrets.
///
/// # Errors
///
/// `500` when stored credentials cannot be decrypted; `400` when a
/// referenced secret was deleted or no longer parses.
pub async fn restore(state: &AppState, row: &SandboxRow) -> ApiResult<Vec<S3Mount>> {
	let Some(records) = row.s3_mounts.as_ref().map(|json| &json.0) else {
		return Ok(Vec::new());
	};
	let inline: Vec<Option<S3MountCredentials>> = match row.s3_mounts_enc.as_deref() {
		Some(enc) => {
			let plain = state.keys.decrypt(enc).map_err(|_| {
				ApiError::internal(format!("sandbox {} S3 mount credentials are unreadable", row.id))
			})?;
			serde_json::from_slice(&plain)
				.map_err(|error| ApiError::internal(format!("stored S3 mount credentials: {error}")))?
		},
		None => Vec::new(),
	};
	let mut mounts = Vec::with_capacity(records.len());
	for (index, record) in records.iter().enumerate() {
		let credentials = match &record.credentials_secret {
			Some(name) => Some(secret_credentials(state, &row.team_id, name).await?),
			None => inline.get(index).cloned().flatten(),
		};
		mounts.push(S3Mount {
			path: record.path.clone(),
			uri: record.uri.clone(),
			mode: record.mode,
			endpoint: record.endpoint.clone(),
			region: record.region.clone(),
			credentials,
		});
	}
	Ok(mounts)
}

/// Total S3-backed mounts (user buckets + object-store volumes) fit the
/// device budget.
///
/// # Errors
///
/// `400` beyond [`MAX_S3_MOUNTS`].
pub fn check_budget(s3_mounts: usize, volumes: &[VolumeMount]) -> ApiResult<()> {
	let total = s3_mounts + volumes.iter().filter(|volume| volume.object_store).count();
	if total > MAX_S3_MOUNTS {
		return Err(ApiError::bad_request(format!(
			"at most {MAX_S3_MOUNTS} S3-backed mounts (vmonS3Mounts + object-store volumes) are \
			 supported"
		)));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn request(path: &str) -> S3MountRequest {
		serde_json::from_value(serde_json::json!({"path": path, "uri": "s3://bucket/prefix"}))
			.unwrap()
	}

	#[test]
	fn validation_rejects_bad_paths_modes_and_overlaps() {
		assert!(validate(&[request("/data")], &[]).is_ok());
		for bad in ["data", "/", "/a/../b", "/a/"] {
			assert!(validate(&[request(bad)], &[]).is_err(), "{bad}");
		}
		assert!(validate(&[request("/data"), request("/data/sub")], &[]).is_err());
		assert!(validate(&[request("/vol/x")], &["/vol".to_owned()]).is_err());
		assert!(validate(&[request("/volume")], &["/vol".to_owned()]).is_ok());
		let mut mode = request("/m");
		mode.mode = Some("rw".into());
		assert!(validate(&[mode], &[]).is_err());
		let mut both = request("/m");
		both.credentials = Some(S3MountCredentials {
			access_key_id:     "a".into(),
			secret_access_key: "b".into(),
			session_token:     None,
		});
		both.credentials_secret = Some("s".into());
		assert!(validate(&[both], &[]).is_err());
		let mut uri = request("/m");
		uri.uri = "ftp://x/y".into();
		assert!(validate(&[uri], &[]).is_err());
		let mut endpoint = request("/m");
		endpoint.endpoint = Some("http://127.0.0.1:19000".into());
		assert!(validate(&[endpoint], &[]).is_ok());
		let many: Vec<_> = (0..=MAX_S3_MOUNTS)
			.map(|n| request(&format!("/m{n}")))
			.collect();
		assert!(validate(&many, &[]).is_err());
	}

	#[test]
	fn unknown_fields_and_debug_never_leak_credentials() {
		let parsed: Result<S3MountRequest, _> = serde_json::from_value(serde_json::json!({
			"path": "/m", "uri": "s3://b", "secretKey": "x"
		}));
		assert!(parsed.is_err(), "unknown fields are rejected");
		let request: S3MountRequest = serde_json::from_value(serde_json::json!({
			"path": "/m", "uri": "s3://b",
			"credentials": {"accessKeyId": "AKIA", "secretAccessKey": "TOPSECRET"}
		}))
		.unwrap();
		assert!(!format!("{request:?}").contains("TOPSECRET"));
		assert!(!format!("{:?}", request.credentials).contains("TOPSECRET"));
	}
}
