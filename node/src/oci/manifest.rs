//! Docker v2 / OCI manifest, index, and image-config documents, plus
//! platform selection for multi-arch indexes.

use serde::Deserialize;

/// Docker schema2 single-platform manifest.
pub const DOCKER_MANIFEST: &str = "application/vnd.docker.distribution.manifest.v2+json";
/// Docker manifest list (multi-arch).
pub const DOCKER_MANIFEST_LIST: &str = "application/vnd.docker.distribution.manifest.list.v2+json";
/// OCI single-platform manifest.
pub const OCI_MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";
/// OCI image index (multi-arch).
pub const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";

/// `Accept` header for manifest requests: every media type we can parse.
pub const ACCEPT: &str = "application/vnd.oci.image.index.v1+json, \
                          application/vnd.docker.distribution.manifest.list.v2+json, \
                          application/vnd.oci.image.manifest.v1+json, \
                          application/vnd.docker.distribution.manifest.v2+json";

/// A content descriptor (config, layer, or index entry).
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Descriptor {
	#[serde(default)]
	pub media_type: String,
	pub digest:     String,
	#[serde(default)]
	pub size:       u64,
	#[serde(default)]
	pub platform:   Option<Platform>,
}

/// Platform of an index entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Platform {
	pub architecture: String,
	pub os:           String,
	#[serde(default)]
	pub variant:      Option<String>,
}

/// A single-platform image manifest.
#[derive(Clone, Debug, Deserialize)]
pub struct Manifest {
	pub config: Descriptor,
	pub layers: Vec<Descriptor>,
}

/// A multi-platform index / manifest list.
#[derive(Clone, Debug, Deserialize)]
pub struct Index {
	pub manifests: Vec<Descriptor>,
}

/// A fetched manifest document of either kind.
#[derive(Clone, Debug)]
pub enum Document {
	Manifest(Manifest),
	Index(Index),
}

impl Document {
	/// Parses a manifest body using the response `Content-Type`, falling back
	/// to the body's `mediaType` and shape for registries that omit it.
	pub fn parse(content_type: Option<&str>, body: &[u8]) -> anyhow::Result<Self> {
		#[derive(Deserialize)]
		#[serde(rename_all = "camelCase")]
		struct Probe {
			#[serde(default)]
			media_type:     Option<String>,
			#[serde(default)]
			manifests:      Option<serde_json::Value>,
			#[serde(default)]
			schema_version: Option<u32>,
		}
		let probe: Probe = serde_json::from_slice(body)?;
		let content_type = content_type
			.map(|c| c.split(';').next().unwrap_or(c).trim())
			.filter(|c| !c.is_empty());
		let media_type = probe.media_type.as_deref().or(content_type).unwrap_or("");
		let is_index = match media_type {
			DOCKER_MANIFEST_LIST | OCI_INDEX => true,
			DOCKER_MANIFEST | OCI_MANIFEST => false,
			_ => probe.manifests.is_some(),
		};
		if !is_index && probe.schema_version == Some(1) {
			anyhow::bail!("docker schema1 manifests are not supported");
		}
		Ok(if is_index {
			Self::Index(serde_json::from_slice(body)?)
		} else {
			Self::Manifest(serde_json::from_slice(body)?)
		})
	}
}

/// OCI architecture name for a node arch (`aarch64` → `arm64`, `x86_64` →
/// `amd64`).
pub fn oci_arch(arch: &str) -> &str {
	match arch {
		"aarch64" | "arm64" => "arm64",
		"x86_64" | "amd64" => "amd64",
		other => other,
	}
}

/// Picks the `linux/<arch>` entry of an index. For arm64 the `v8` variant
/// (or no variant) wins over others; attestation entries (`unknown/unknown`)
/// never match.
pub fn select_platform<'a>(index: &'a Index, node_arch: &str) -> Option<&'a Descriptor> {
	let arch = oci_arch(node_arch);
	let rank = |d: &Descriptor| -> Option<u8> {
		let platform = d.platform.as_ref()?;
		if platform.os != "linux" || platform.architecture != arch {
			return None;
		}
		match (arch, platform.variant.as_deref()) {
			(_, None) => Some(0),
			("arm64", Some("v8")) => Some(0),
			(_, Some(_)) => Some(1),
		}
	};
	index
		.manifests
		.iter()
		.filter_map(|d| rank(d).map(|r| (r, d)))
		.min_by_key(|(r, _)| *r)
		.map(|(_, d)| d)
}

/// The subset of an image config that seeds template defaults.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, serde::Serialize)]
pub struct ImageConfig {
	#[serde(rename = "Env", default)]
	pub env:         Option<Vec<String>>,
	#[serde(rename = "WorkingDir", default)]
	pub working_dir: Option<String>,
	#[serde(rename = "User", default)]
	pub user:        Option<String>,
	#[serde(rename = "Entrypoint", default)]
	pub entrypoint:  Option<Vec<String>>,
	#[serde(rename = "Cmd", default)]
	pub cmd:         Option<Vec<String>>,
}

/// Top-level image config blob (`application/vnd.oci.image.config.v1+json`).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ConfigBlob {
	#[serde(default)]
	pub architecture: Option<String>,
	#[serde(default)]
	pub config:       Option<ImageConfig>,
}

impl ImageConfig {
	/// `Env` as an ordered map (later duplicates win, entries without `=` are
	/// ignored).
	pub fn env_map(&self) -> std::collections::BTreeMap<String, String> {
		self
			.env
			.iter()
			.flatten()
			.filter_map(|kv| {
				kv.split_once('=')
					.map(|(k, v)| (k.to_owned(), v.to_owned()))
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn entry(arch: &str, os: &str, variant: Option<&str>, digest: &str) -> serde_json::Value {
		let mut platform = serde_json::json!({"architecture": arch, "os": os});
		if let Some(v) = variant {
			platform["variant"] = v.into();
		}
		serde_json::json!({"mediaType": OCI_MANIFEST, "digest": digest, "size": 1, "platform": platform})
	}

	fn index(entries: Vec<serde_json::Value>) -> Index {
		serde_json::from_value(serde_json::json!({ "manifests": entries })).unwrap()
	}

	#[test]
	fn selects_linux_arm64_over_variants_and_attestations() {
		let idx = index(vec![
			entry("amd64", "linux", None, "a"),
			entry("arm", "linux", Some("v7"), "b"),
			entry("unknown", "unknown", None, "att"),
			entry("arm64", "linux", Some("v8"), "c"),
			entry("arm64", "windows", None, "w"),
		]);
		assert_eq!(select_platform(&idx, "aarch64").unwrap().digest, "c");
		assert_eq!(select_platform(&idx, "x86_64").unwrap().digest, "a");
		assert!(select_platform(&idx, "riscv64").is_none());
	}

	#[test]
	fn prefers_plain_or_v8_over_other_arm64_variants() {
		let idx = index(vec![
			entry("arm64", "linux", Some("v9"), "v9"),
			entry("arm64", "linux", None, "plain"),
		]);
		assert_eq!(select_platform(&idx, "aarch64").unwrap().digest, "plain");
		let idx = index(vec![entry("arm64", "linux", Some("v9"), "v9")]);
		assert_eq!(
			select_platform(&idx, "aarch64").unwrap().digest,
			"v9",
			"an odd variant still beats nothing"
		);
		let idx = index(vec![entry("arm64", "linux", None, "x")]);
		assert!(select_platform(&idx, "x86_64").is_none(), "only arm64 available on an amd64 node");
	}

	#[test]
	fn document_kind_from_content_type_media_type_or_shape() {
		let manifest = br#"{"schemaVersion":2,"config":{"digest":"sha256:c","size":1},"layers":[{"digest":"sha256:l","size":2}]}"#;
		assert!(matches!(
			Document::parse(Some(DOCKER_MANIFEST), manifest).unwrap(),
			Document::Manifest(_)
		));
		assert!(matches!(Document::parse(None, manifest).unwrap(), Document::Manifest(_)));
		let list = br#"{"schemaVersion":2,"manifests":[]}"#;
		assert!(matches!(
			Document::parse(Some("application/json; charset=utf-8"), list).unwrap(),
			Document::Index(_)
		));
		let typed = br#"{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}"#;
		assert!(
			matches!(Document::parse(Some(OCI_MANIFEST), typed).unwrap(), Document::Index(_)),
			"body mediaType wins"
		);
		assert!(Document::parse(None, br#"{"schemaVersion":1,"fsLayers":[]}"#).is_err());
	}

	#[test]
	fn env_map_keeps_last_duplicate_and_values_with_equals() {
		let config = ImageConfig {
			env: Some(vec!["A=1".into(), "B=x=y".into(), "A=2".into(), "BAD".into()]),
			..Default::default()
		};
		let env = config.env_map();
		assert_eq!(env["A"], "2");
		assert_eq!(env["B"], "x=y");
		assert_eq!(env.len(), 2);
	}
}
