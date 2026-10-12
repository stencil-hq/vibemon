//! OCI image references (`alpine:3.20`, `ghcr.io/org/img@sha256:…`) with
//! Docker Hub normalization (`docker.io`, `library/`, default tag `latest`).

use std::fmt;

use anyhow::{Result, bail};

/// Docker Hub's canonical registry name.
pub const DOCKER_HUB: &str = "docker.io";
/// Host serving the Docker Hub registry API.
const DOCKER_HUB_API: &str = "registry-1.docker.io";

/// A parsed, normalized image reference.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reference {
	/// Registry host (and port), e.g. `docker.io`, `ghcr.io`, `localhost:5000`.
	pub registry:   String,
	/// Repository path, e.g. `library/alpine`.
	pub repository: String,
	/// Tag; `None` only when a digest is given without a tag.
	pub tag:        Option<String>,
	/// Manifest digest (`sha256:<hex>`), if pinned.
	pub digest:     Option<String>,
}

impl Reference {
	/// Parses `image` the way `docker pull` does.
	pub fn parse(image: &str) -> Result<Self> {
		let image = image.trim();
		let image = image.strip_prefix("docker://").unwrap_or(image);
		if image.is_empty() {
			bail!("empty image reference");
		}
		let (name, digest) = match image.split_once('@') {
			Some((name, digest)) => (name, Some(parse_digest(digest)?)),
			None => (image, None),
		};
		let last_slash = name.rfind('/').map_or(0, |i| i + 1);
		let (name, tag) = match name[last_slash..].rfind(':') {
			Some(colon) => {
				let colon = last_slash + colon;
				(&name[..colon], Some(name[colon + 1..].to_owned()))
			},
			None => (name, None),
		};
		if let Some(tag) = &tag {
			validate_tag(tag)?;
		}
		let (registry, repository) = match name.split_once('/') {
			Some((first, rest))
				if first.contains(['.', ':'])
					|| first == "localhost"
					|| first.chars().any(|c| c.is_ascii_uppercase()) =>
			{
				(first.to_owned(), rest.to_owned())
			},
			_ => (DOCKER_HUB.to_owned(), name.to_owned()),
		};
		let registry = match registry.as_str() {
			"index.docker.io" | "registry-1.docker.io" => DOCKER_HUB.to_owned(),
			_ => registry,
		};
		let repository = if registry == DOCKER_HUB && !repository.contains('/') {
			format!("library/{repository}")
		} else {
			repository
		};
		validate_repository(&repository)?;
		let tag = if tag.is_none() && digest.is_none() {
			Some("latest".to_owned())
		} else {
			tag
		};
		Ok(Self { registry, repository, tag, digest })
	}

	/// Host (and port) serving the registry API.
	pub fn api_host(&self) -> &str {
		if self.registry == DOCKER_HUB {
			DOCKER_HUB_API
		} else {
			&self.registry
		}
	}

	/// Base URL of the registry API (`https://host`; plain HTTP only for loopback registries).
	pub fn api_base(&self) -> String {
		let host = self.api_host();
		let hostname = host.rsplit_once(':').map_or(host, |(name, _)| name);
		let scheme = if matches!(hostname, "localhost" | "127.0.0.1" | "[::1]") {
			"http"
		} else {
			"https"
		};
		format!("{scheme}://{host}")
	}

	/// What to put in `/manifests/<reference>`: the digest when pinned, else the
	/// tag.
	pub fn manifest_reference(&self) -> &str {
		self
			.digest
			.as_deref()
			.or(self.tag.as_deref())
			.unwrap_or("latest")
	}
}

impl fmt::Display for Reference {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}/{}", self.registry, self.repository)?;
		if let Some(tag) = &self.tag {
			write!(f, ":{tag}")?;
		}
		if let Some(digest) = &self.digest {
			write!(f, "@{digest}")?;
		}
		Ok(())
	}
}

/// Validates a `sha256:<64 hex>` digest and returns it lowercased.
pub fn parse_digest(digest: &str) -> Result<String> {
	let Some(hex) = digest.strip_prefix("sha256:") else {
		bail!("unsupported digest {digest:?} (only sha256 is supported)");
	};
	if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
		bail!("invalid sha256 digest {digest:?}");
	}
	Ok(format!("sha256:{}", hex.to_ascii_lowercase()))
}

fn validate_tag(tag: &str) -> Result<()> {
	let mut chars = tag.chars();
	let first_ok = chars
		.next()
		.is_some_and(|c| c.is_ascii_alphanumeric() || c == '_');
	if !first_ok
		|| tag.len() > 128
		|| !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
	{
		bail!("invalid image tag {tag:?}");
	}
	Ok(())
}

fn validate_repository(repository: &str) -> Result<()> {
	let valid_component = |c: &str| {
		!c.is_empty()
			&& c.bytes().all(|b| {
				b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
			}) && c.as_bytes()[0].is_ascii_alphanumeric()
			&& c.as_bytes()[c.len() - 1].is_ascii_alphanumeric()
	};
	if repository.len() > 255 || !repository.split('/').all(valid_component) {
		bail!(
			"invalid image repository {repository:?} (must be lowercase [a-z0-9._-] path components)"
		);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	const DIGEST: &str = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

	/// `(input, registry, repository, tag, digest)`.
	type Case =
		(&'static str, &'static str, &'static str, Option<&'static str>, Option<&'static str>);

	#[test]
	fn normalizes_like_docker_pull() {
		let cases: &[Case] = &[
			("alpine", DOCKER_HUB, "library/alpine", Some("latest"), None),
			("alpine:3.20", DOCKER_HUB, "library/alpine", Some("3.20"), None),
			("e2bdev/base", DOCKER_HUB, "e2bdev/base", Some("latest"), None),
			("docker.io/python:3.12-slim", DOCKER_HUB, "library/python", Some("3.12-slim"), None),
			(
				"index.docker.io/library/ubuntu:22.04",
				DOCKER_HUB,
				"library/ubuntu",
				Some("22.04"),
				None,
			),
			("ghcr.io/org/team/img:v1", "ghcr.io", "org/team/img", Some("v1"), None),
			("localhost:5000/img", "localhost:5000", "img", Some("latest"), None),
			("localhost/img:t", "localhost", "img", Some("t"), None),
			(
				"registry.example.com:8443/a/b:1.0",
				"registry.example.com:8443",
				"a/b",
				Some("1.0"),
				None,
			),
			("oven/bun:latest", DOCKER_HUB, "oven/bun", Some("latest"), None),
			("docker://ubuntu", DOCKER_HUB, "library/ubuntu", Some("latest"), None),
		];
		for (input, registry, repository, tag, digest) in cases {
			let r = Reference::parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
			assert_eq!(
				(r.registry.as_str(), r.repository.as_str()),
				(*registry, *repository),
				"{input}"
			);
			assert_eq!((r.tag.as_deref(), r.digest.as_deref()), (*tag, *digest), "{input}");
		}
	}

	#[test]
	fn digest_pins_without_default_tag_and_wins_for_manifest_lookup() {
		let r = Reference::parse(&format!("ubuntu@{DIGEST}")).unwrap();
		assert_eq!(r.tag, None);
		assert_eq!(r.manifest_reference(), DIGEST);
		let r = Reference::parse(&format!(
			"ghcr.io/a/b:v2@{}",
			DIGEST.to_uppercase().replace("SHA256", "sha256")
		))
		.unwrap();
		assert_eq!(r.tag.as_deref(), Some("v2"));
		assert_eq!(r.manifest_reference(), DIGEST);
	}

	#[test]
	fn port_colon_is_not_a_tag() {
		let r = Reference::parse("myhost:5000/repo").unwrap();
		assert_eq!(r.registry, "myhost:5000");
		assert_eq!(r.tag.as_deref(), Some("latest"));
		assert_eq!(r.api_base(), "https://myhost:5000");
		assert_eq!(
			Reference::parse("localhost:5000/repo").unwrap().api_base(),
			"http://localhost:5000"
		);
		assert_eq!(Reference::parse("alpine").unwrap().api_base(), "https://registry-1.docker.io");
	}

	#[test]
	fn rejects_malformed_references() {
		for bad in [
			"",
			"Alpine",
			"alpine:",
			"alpine:-x",
			"a//b",
			"alpine@sha256:abc",
			"alpine@md5:0123",
			"ghcr.io/Org/img",
		] {
			assert!(Reference::parse(bad).is_err(), "{bad:?} should be rejected");
		}
	}
}
