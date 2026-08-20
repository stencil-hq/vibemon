//! Credentials for private image registries.
//!
//! skopeo authenticates from a Docker-style auth file. vmon materializes one
//! per pull rather than persisting it, because the cloud tokens it carries are
//! short-lived: minting on demand keeps a long-running worker from going stale
//! an hour after boot. The file also keeps secrets out of `--creds`, which
//! would expose them in argv to every local process through `/proc`.
//!
//! Today the only automatic provider is Google Artifact Registry / Container
//! Registry via the GCE metadata server. `VMON_REGISTRY_AUTH_FILE` overrides
//! everything for operators who manage credentials themselves (ECR, GHCR, a
//! private Harbor, …).

use std::{
	io::{Read, Write},
	net::TcpStream,
	path::{Path, PathBuf},
	time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde_json::json;
use tempfile::NamedTempFile;

/// Environment override naming a caller-managed Docker auth file.
const AUTH_FILE_ENV: &str = "VMON_REGISTRY_AUTH_FILE";
/// GCE metadata endpoint issuing the instance service account's token.
const METADATA_HOST: &str = "metadata.google.internal";
const METADATA_TOKEN_PATH: &str =
   "/computeMetadata/v1/instance/service-accounts/default/token?scopes=https://www.googleapis.com/auth/cloud-platform";
/// The metadata server is link-local; a slow reply means it is absent.
const METADATA_TIMEOUT: Duration = Duration::from_secs(2);
/// Username Google registries expect alongside an OAuth access token.
const OAUTH_USER: &str = "oauth2accesstoken";

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
/// Best-effort by design: a public image must still pull on a host with no
/// metadata server, so every failure degrades to an anonymous pull and lets
/// skopeo report the authoritative error.
pub fn auth_file_for(reference: &str) -> Option<AuthFile> {
	if let Some(path) = std::env::var_os(AUTH_FILE_ENV) {
		return Some(AuthFile::Managed(PathBuf::from(path)));
	}
	let host = registry_host(reference)?;
	if !is_google_registry(&host) {
		return None;
	}
	let token = metadata_access_token()?;
	write_auth_file(&host, OAUTH_USER, &token)
		.inspect_err(|error| tracing::warn!(%error, host, "registry auth file write failed"))
		.ok()
		.map(AuthFile::Ephemeral)
}

/// The registry host of a skopeo reference, if it names one.
///
/// Local transports (`oci:`, `dir:`, …) are filesystem paths and never carry a
/// host. Within a registry reference, the first path component is the host only
/// when it looks like one — `library/alpine` is a Docker Hub namespace, and a
/// reference with no `/` at all (`alpine:3.20`) is a bare repository and tag.
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

/// Whether `host` is served by Google Artifact Registry or Container Registry.
fn is_google_registry(host: &str) -> bool {
	let host = host.split_once(':').map_or(host, |(name, _port)| name);
	host == "gcr.io" || host.ends_with(".gcr.io") || host.ends_with("-docker.pkg.dev")
}

/// Fetch the instance service account's access token from the metadata server.
///
/// Deliberately a hand-rolled plaintext GET against a link-local address: the
/// image pipeline is synchronous, so borrowing the async HTTP client here would
/// mean blocking on a runtime from inside a worker thread.
pub(super) fn metadata_access_token() -> Option<String> {
	let mut stream = TcpStream::connect((METADATA_HOST, 80))
		.or_else(|_| TcpStream::connect(("169.254.169.254", 80)))
		.ok()?;
	stream.set_read_timeout(Some(METADATA_TIMEOUT)).ok()?;
	stream.set_write_timeout(Some(METADATA_TIMEOUT)).ok()?;
	let request = format!(
		"GET {METADATA_TOKEN_PATH} HTTP/1.1\r\nHost: {METADATA_HOST}\r\nMetadata-Flavor: \
		 Google\r\nConnection: close\r\n\r\n"
	);
	stream.write_all(request.as_bytes()).ok()?;
	let mut response = Vec::new();
	stream.take(64 * 1024).read_to_end(&mut response).ok()?;
	parse_access_token(&String::from_utf8_lossy(&response))
}

/// Pull `access_token` out of a metadata-server HTTP response.
fn parse_access_token(response: &str) -> Option<String> {
	let (head, body) = response.split_once("\r\n\r\n")?;
	if !head.starts_with("HTTP/1.1 200") && !head.starts_with("HTTP/1.0 200") {
		return None;
	}
	let parsed: serde_json::Value = serde_json::from_str(body.trim()).ok()?;
	let token = parsed.get("access_token")?.as_str()?;
	(!token.is_empty()).then(|| token.to_owned())
}

/// Write a 0600 Docker-style auth file for one registry host.
fn write_auth_file(host: &str, user: &str, secret: &str) -> std::io::Result<NamedTempFile> {
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
			registry_host("us-central1-docker.pkg.dev/p/r/i:t").as_deref(),
			Some("us-central1-docker.pkg.dev")
		);
		assert_eq!(registry_host("localhost:5000/img:t").as_deref(), Some("localhost:5000"));
		// Docker Hub short names carry no host component.
		assert_eq!(registry_host("alpine:3.20"), None);
		assert_eq!(registry_host("library/alpine:3.20"), None);
		// Local transports are paths, never registries.
		assert_eq!(registry_host("oci:/var/lib/vmon/img:latest"), None);
		assert_eq!(registry_host("dir:/var/lib/vmon/img"), None);
	}

	#[test]
	fn google_registries_are_matched_by_suffix_not_substring() {
		assert!(is_google_registry("gcr.io"));
		assert!(is_google_registry("eu.gcr.io"));
		assert!(is_google_registry("us-central1-docker.pkg.dev"));
		// A lookalike domain must not receive Google credentials.
		assert!(!is_google_registry("gcr.io.evil.example"));
		assert!(!is_google_registry("notgcr.io.example.com"));
		assert!(!is_google_registry("registry-1.docker.io"));
	}

	#[test]
	fn access_token_is_read_only_from_a_successful_response() {
		let ok = "HTTP/1.1 200 OK\r\nContent-Type: \
		          application/json\r\n\r\n{\"access_token\":\"ya29.abc\",\"expires_in\":3599}";
		assert_eq!(parse_access_token(ok).as_deref(), Some("ya29.abc"));
		let forbidden = "HTTP/1.1 403 Forbidden\r\n\r\n{\"access_token\":\"ya29.abc\"}";
		assert_eq!(parse_access_token(forbidden), None);
		let empty = "HTTP/1.1 200 OK\r\n\r\n{\"access_token\":\"\"}";
		assert_eq!(parse_access_token(empty), None);
		assert_eq!(parse_access_token("HTTP/1.1 200 OK\r\n\r\nnot json"), None);
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
