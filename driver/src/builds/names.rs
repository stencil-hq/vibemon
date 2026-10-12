//! Pure parsing and validation for the template API.
//!
//! Template names and references (`[namespace/]alias[:tag]`), tags, files
//! hashes, build steps, resources, and the User-Agent template version
//! (`local://e2b-api.md` Part D §2, §4, §10).

use vmon_node::api::{BuildStep, TemplateVersion};

/// Tag meaning "no tag".
pub const DEFAULT_TAG: &str = "default";
/// Maximum `name` length accepted by `POST /v3/templates`.
pub const MAX_NAME_LEN: usize = 128;
/// Template ids are 20 characters of `[a-z0-9]`.
pub const TEMPLATE_ID_LEN: usize = 20;
/// Default `cpuCount` of a template build.
pub const DEFAULT_VCPUS: u32 = 2;
/// Default `memoryMB` of a template build.
pub const DEFAULT_MEMORY_MB: u64 = 1024;
/// Upper bound on `cpuCount`.
pub const MAX_VCPUS: u32 = 32;
/// Lower bound on `memoryMB`.
pub const MIN_MEMORY_MB: u64 = 128;

/// A parsed `[namespace/]alias[:tag]` reference (lowercased, trimmed; the
/// `default` tag reads as no tag).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateName {
	pub namespace: Option<String>,
	pub alias:     String,
	pub tag:       Option<String>,
}

impl TemplateName {
	/// Tag to resolve (`default` when none was given).
	pub fn tag_or_default(&self) -> &str {
		self.tag.as_deref().unwrap_or(DEFAULT_TAG)
	}
}

/// Parses `[namespace/]alias[:tag]` like upstream `id.ParseName`.
pub fn parse_name(raw: &str) -> Result<TemplateName, String> {
	let raw = raw.trim().to_ascii_lowercase();
	if raw.is_empty() {
		return Err("name must not be empty".into());
	}
	let (rest, tag) = match raw.rsplit_once(':') {
		Some((rest, tag)) => (rest, Some(tag.trim())),
		None => (raw.as_str(), None),
	};
	let (namespace, alias) = match rest.split_once('/') {
		Some((namespace, alias)) => (Some(namespace.trim()), alias.trim()),
		None => (None, rest.trim()),
	};
	if let Some(namespace) = namespace
		&& !is_identifier(namespace)
	{
		return Err(format!("invalid namespace '{namespace}': must match ^[a-z0-9-_]+$"));
	}
	if !is_identifier(alias) {
		return Err(format!("invalid alias '{alias}': must match ^[a-z0-9-_]+$"));
	}
	let tag = match tag {
		Some(tag) => validate_tag(tag)?,
		None => None,
	};
	Ok(TemplateName { namespace: namespace.map(str::to_owned), alias: alias.to_owned(), tag })
}

/// Validates one tag (`^[a-z0-9-_.]+$`, not a UUID); `Ok(None)` for `default`.
pub fn validate_tag(raw: &str) -> Result<Option<String>, String> {
	let tag = raw.trim().to_ascii_lowercase();
	if tag.is_empty()
		|| !tag
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-_.".contains(&b))
	{
		return Err(format!("invalid tag '{raw}': must match ^[a-z0-9-_.]+$"));
	}
	if uuid::Uuid::parse_str(&tag).is_ok() {
		return Err(format!("invalid tag '{raw}': tags cannot be UUIDs"));
	}
	Ok((tag != DEFAULT_TAG).then_some(tag))
}

/// Final tag list of a build: the tag from `name` first, then `tags`,
/// validated and deduplicated; `["default"]` when empty.
pub fn build_tags(name_tag: Option<&str>, tags: &[String]) -> Result<Vec<String>, String> {
	let mut out: Vec<String> = Vec::new();
	for raw in name_tag
		.into_iter()
		.map(str::to_owned)
		.chain(tags.iter().cloned())
	{
		let tag = validate_tag(&raw)?.unwrap_or_else(|| DEFAULT_TAG.to_owned());
		if !out.contains(&tag) {
			out.push(tag);
		}
	}
	if out.is_empty() {
		out.push(DEFAULT_TAG.to_owned());
	}
	Ok(out)
}

fn is_identifier(value: &str) -> bool {
	!value.is_empty()
		&& value
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
}

/// Whether `value` has the template-id shape (20 × `[a-z0-9]`).
pub fn looks_like_template_id(value: &str) -> bool {
	value.len() == TEMPLATE_ID_LEN
		&& value
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// New random template id.
pub fn new_template_id() -> String {
	use rand::RngExt;
	const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
	let mut rng = rand::rng();
	(0..TEMPLATE_ID_LEN)
		.map(|_| char::from(ALPHABET[rng.random_range(0..ALPHABET.len())]))
		.collect()
}

/// `filesHash` shape: 64 lowercase hex characters.
pub fn valid_files_hash(hash: &str) -> bool {
	hash.len() == 64
		&& hash
			.bytes()
			.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Validated `cpuCount` (default 2; 1 or even; ≤ 32; ≤ the team limit).
pub fn validate_vcpus(requested: Option<i64>, team_max: u32) -> Result<u32, String> {
	let Some(cpus) = requested else {
		return Ok(DEFAULT_VCPUS.min(team_max.max(1)));
	};
	if cpus < 1 {
		return Err("CPU count must be at least 1".into());
	}
	if cpus != 1 && cpus % 2 != 0 {
		return Err("CPU count must be 1 or an even number".into());
	}
	if cpus > i64::from(MAX_VCPUS) {
		return Err(format!("CPU count cannot be greater than {MAX_VCPUS}"));
	}
	if cpus > i64::from(team_max) {
		return Err(format!("CPU count exceeds team limits ({team_max})"));
	}
	Ok(u32::try_from(cpus).unwrap_or(DEFAULT_VCPUS))
}

/// Validated `memoryMB` (default 1024; even; ≥ 128; ≤ the team limit).
pub fn validate_memory(requested: Option<i64>, team_max: u64) -> Result<u64, String> {
	let Some(memory) = requested else {
		return Ok(DEFAULT_MEMORY_MB.min(team_max.max(MIN_MEMORY_MB)));
	};
	if memory < i64::try_from(MIN_MEMORY_MB).unwrap_or(128) {
		return Err(format!("Memory must be at least {MIN_MEMORY_MB} MiB"));
	}
	if memory % 2 != 0 {
		return Err("Memory must be divisible by 2".into());
	}
	let memory = u64::try_from(memory).unwrap_or(0);
	if memory > team_max {
		return Err(format!("Memory exceeds team limits ({team_max} MiB)"));
	}
	Ok(memory)
}

/// Validated `minFreeDiskMb` (0 ≤ x ≤ the team disk limit).
pub fn validate_min_free_disk(
	requested: Option<i64>,
	team_max: u64,
) -> Result<Option<u64>, String> {
	let Some(disk) = requested else {
		return Ok(None);
	};
	let disk =
		u64::try_from(disk).map_err(|_| "Minimum free disk must not be negative".to_owned())?;
	if disk > team_max {
		return Err(format!("Minimum free disk exceeds team limits ({team_max} MiB)"));
	}
	Ok(Some(disk))
}

/// A `TemplateStep` as received (unknown keys such as `forceUpload`,
/// `resolveSymlinks`, `gzip` are ignored by serde).
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StepInput {
	#[serde(rename = "type")]
	pub kind:       String,
	#[serde(default)]
	pub args:       Vec<String>,
	#[serde(default)]
	pub files_hash: Option<String>,
	#[serde(default)]
	pub force:      bool,
}

/// Validates steps and lowers them to node [`BuildStep`]s.
///
/// Types are case-insensitive (`ADD`→`COPY`, `ARG`→`ENV`), argument counts
/// are checked, `COPY` requires a `filesHash`, and `force` propagates to
/// every later step.
pub fn lower_steps(steps: &[StepInput]) -> Result<Vec<BuildStep>, String> {
	let mut forced = false;
	let mut out = Vec::with_capacity(steps.len());
	for (index, step) in steps.iter().enumerate() {
		let number = index + 1;
		let fail = |detail: String| format!("invalid step {number} ({}): {detail}", step.kind);
		let kind = match step.kind.trim().to_ascii_uppercase().as_str() {
			"RUN" => "RUN",
			"COPY" | "ADD" => "COPY",
			"ENV" | "ARG" => "ENV",
			"WORKDIR" => "WORKDIR",
			"USER" => "USER",
			other => return Err(format!("command type {other} is not implemented")),
		};
		let args = step.args.len();
		match kind {
			"RUN" if !(1..=2).contains(&args) || step.args[0].trim().is_empty() => {
				return Err(fail("expected [command, user?]".into()));
			},
			"COPY"
				if !(2..=4).contains(&args) || step.args[0].is_empty() || step.args[1].is_empty() =>
			{
				return Err(fail("expected [src, dest, owner?, mode?]".into()));
			},
			"COPY"
				if step
					.args
					.get(3)
					.is_some_and(|mode| !mode.is_empty() && !is_octal_mode(mode)) =>
			{
				return Err(fail(format!("invalid mode '{}'", step.args[3])));
			},
			"ENV" if args == 0 || args % 2 != 0 => {
				return Err(fail("expected key/value pairs".into()));
			},
			"WORKDIR" if args != 1 || step.args[0].trim().is_empty() => {
				return Err(fail("expected [path]".into()));
			},
			"USER" if !(1..=2).contains(&args) || step.args[0].trim().is_empty() => {
				return Err(fail("expected [user]".into()));
			},
			_ => {},
		}
		let files_hash = match (&step.files_hash, kind) {
			(Some(hash), _) if !valid_files_hash(hash) => {
				return Err(fail("Invalid files hash".into()));
			},
			(None, "COPY") => return Err(fail("filesHash is required".into())),
			(hash, _) => hash.clone(),
		};
		forced |= step.force;
		out.push(BuildStep {
			kind: kind.to_owned(),
			args: step.args.clone(),
			files_hash,
			force: forced,
		});
	}
	Ok(out)
}

fn is_octal_mode(mode: &str) -> bool {
	(3..=4).contains(&mode.len()) && mode.bytes().all(|b| (b'0'..=b'7').contains(&b))
}

/// Template version selected by the client's `User-Agent` (Part D §10.3).
///
/// An `e2b-js-sdk/<v>` or `e2b-python-sdk/<v>` token below 2.3.0 selects
/// v2.0.0; anything else (including unknown agents) v2.1.0. An unparsable
/// SDK version is an error (400).
pub fn template_version_from_user_agent(
	user_agent: Option<&str>,
) -> Result<TemplateVersion, String> {
	let Some(user_agent) = user_agent else {
		return Ok(TemplateVersion::V2_1);
	};
	for token in user_agent.split_whitespace() {
		let version = token
			.strip_prefix("e2b-js-sdk/")
			.or_else(|| token.strip_prefix("e2b-python-sdk/"));
		let Some(version) = version else { continue };
		let parsed = parse_semver(version)
			.ok_or_else(|| format!("invalid SDK version in User-Agent: {version}"))?;
		return Ok(if parsed < (2, 3, 0, true) {
			TemplateVersion::V2_0
		} else {
			TemplateVersion::V2_1
		});
	}
	Ok(TemplateVersion::V2_1)
}

/// `(major, minor, patch, is_release)`; a pre-release sorts below its release.
fn parse_semver(raw: &str) -> Option<(u64, u64, u64, bool)> {
	let raw = raw.strip_prefix('v').unwrap_or(raw);
	// Build metadata (`+…`) is ignored; a `-…` suffix marks a pre-release.
	let raw = raw.split_once('+').map_or(raw, |(version, _)| version);
	let (core, pre) = match raw.split_once('-') {
		Some((core, suffix)) => (core, !suffix.is_empty()),
		None => (raw, false),
	};
	let mut parts = core.split('.');
	let major = parts.next()?.parse().ok()?;
	let minor = parts.next()?.parse().ok()?;
	let patch = parts.next()?.parse().ok()?;
	if parts.next().is_some() {
		return None;
	}
	Some((major, minor, patch, !pre))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn name(ns: Option<&str>, alias: &str, tag: Option<&str>) -> TemplateName {
		TemplateName {
			namespace: ns.map(Into::into),
			alias:     alias.into(),
			tag:       tag.map(Into::into),
		}
	}

	#[test]
	fn parses_namespace_alias_and_tag() {
		assert_eq!(
			parse_name(" Team-1/My_Tpl:V1.2 ").unwrap(),
			name(Some("team-1"), "my_tpl", Some("v1.2"))
		);
		assert_eq!(parse_name("alias").unwrap(), name(None, "alias", None));
		assert_eq!(parse_name("alias:default").unwrap(), name(None, "alias", None));
		assert_eq!(
			parse_name("abcdefghij0123456789").unwrap(),
			name(None, "abcdefghij0123456789", None)
		);
	}

	#[test]
	fn rejects_malformed_names() {
		for bad in
			["", "a/b/c", "a b", "/alias", "ns/", "alias:", "alias:bad tag", "al.ias", "ns:x/alias"]
		{
			assert!(parse_name(bad).is_err(), "{bad:?} should be rejected");
		}
		assert!(
			parse_name("alias:4d7e3b8e-1f0a-4c2b-9d3e-0a1b2c3d4e5f")
				.unwrap_err()
				.contains("UUID")
		);
	}

	#[test]
	fn build_tags_prepends_name_tag_dedups_and_defaults() {
		assert_eq!(build_tags(None, &[]).unwrap(), ["default"]);
		assert_eq!(build_tags(Some("v2"), &["latest".into(), "v2".into()]).unwrap(), [
			"v2", "latest"
		]);
		assert_eq!(build_tags(None, &["DEFAULT".into(), "x".into()]).unwrap(), ["default", "x"]);
		assert!(build_tags(None, &["no spaces".into()]).is_err());
	}

	#[test]
	fn resource_validation_boundaries() {
		assert_eq!(validate_vcpus(None, 8), Ok(2));
		assert_eq!(validate_vcpus(Some(1), 8), Ok(1));
		assert!(validate_vcpus(Some(3), 8).unwrap_err().contains("even"));
		assert!(validate_vcpus(Some(0), 8).is_err());
		assert!(validate_vcpus(Some(34), 64).unwrap_err().contains("32"));
		assert!(validate_vcpus(Some(16), 8).unwrap_err().contains("team"));
		assert_eq!(validate_memory(None, 8192), Ok(1024));
		assert_eq!(validate_memory(Some(128), 8192), Ok(128));
		assert!(validate_memory(Some(127), 8192).is_err());
		assert!(
			validate_memory(Some(1025), 8192)
				.unwrap_err()
				.contains("divisible")
		);
		assert!(validate_memory(Some(16384), 8192).is_err());
		assert_eq!(validate_min_free_disk(Some(0), 100), Ok(Some(0)));
		assert!(validate_min_free_disk(Some(-1), 100).is_err());
		assert!(validate_min_free_disk(Some(101), 100).is_err());
	}

	fn step(kind: &str, args: &[&str], hash: Option<&str>, force: bool) -> StepInput {
		StepInput {
			kind: kind.into(),
			args: args.iter().map(ToString::to_string).collect(),
			files_hash: hash.map(Into::into),
			force,
		}
	}

	#[test]
	fn steps_are_canonicalized_and_force_propagates() {
		let hash = "a".repeat(64);
		let lowered = lower_steps(&[
			step("run", &["echo hi"], None, false),
			step("ADD", &["src", "/dst", "", "0755"], Some(&hash), true),
			step("arg", &["K", "V"], None, false),
			step("User", &["root"], None, false),
		])
		.unwrap();
		let kinds: Vec<_> = lowered.iter().map(|s| s.kind.as_str()).collect();
		assert_eq!(kinds, ["RUN", "COPY", "ENV", "USER"]);
		let forces: Vec<_> = lowered.iter().map(|s| s.force).collect();
		assert_eq!(forces, [false, true, true, true]);
		assert_eq!(lowered[1].files_hash.as_deref(), Some(hash.as_str()));
	}

	#[test]
	fn step_validation_rejects_bad_shapes() {
		let hash = "0".repeat(64);
		let cases = [
			(step("COPY", &["src", "/dst"], None, false), "filesHash is required"),
			(step("COPY", &["src", "/dst"], Some("ABC"), false), "Invalid files hash"),
			(step("COPY", &["src", "/dst", "", "999"], Some(&hash), false), "invalid mode"),
			(step("ENV", &["K"], None, false), "key/value"),
			(step("WORKDIR", &[], None, false), "path"),
			(step("RUN", &[], None, false), "command"),
			(step("HEALTHCHECK", &["x"], None, false), "not implemented"),
		];
		for (input, expected) in cases {
			let err = lower_steps(&[input]).unwrap_err();
			assert!(err.contains(expected), "{err} should mention {expected}");
		}
		assert!(
			lower_steps(&[step("RUN", &["ls"], None, false), step("ENV", &["K"], None, false)])
				.unwrap_err()
				.starts_with("invalid step 2")
		);
	}

	#[test]
	fn unknown_step_keys_are_ignored() {
		let parsed: StepInput = serde_json::from_value(serde_json::json!({
			"type": "COPY", "args": ["a", "b", "", ""], "filesHash": "f".repeat(64),
			"forceUpload": true, "resolveSymlinks": false, "gzip": true
		}))
		.unwrap();
		assert_eq!(parsed.kind, "COPY");
		assert!(!parsed.force);
	}

	#[test]
	fn files_hash_shape() {
		assert!(valid_files_hash(&"0123456789abcdef".repeat(4)));
		assert!(!valid_files_hash(&"0123456789ABCDEF".repeat(4)));
		assert!(!valid_files_hash(&"a".repeat(63)));
	}

	#[test]
	fn user_agent_selects_template_version() {
		let v = |ua: &str| template_version_from_user_agent(Some(ua));
		assert_eq!(v("e2b-js-sdk/2.2.9"), Ok(TemplateVersion::V2_0));
		assert_eq!(v("e2b-js-sdk/2.3.0 e2b-cli/2.0.0"), Ok(TemplateVersion::V2_1));
		assert_eq!(v("e2b-python-sdk/2.3.0-beta.1"), Ok(TemplateVersion::V2_0));
		assert_eq!(v("e2b-python-sdk/10.0.0"), Ok(TemplateVersion::V2_1));
		assert_eq!(v("curl/8.0"), Ok(TemplateVersion::V2_1));
		assert_eq!(template_version_from_user_agent(None), Ok(TemplateVersion::V2_1));
		assert!(v("e2b-js-sdk/not-a-version").is_err());
	}

	#[test]
	fn template_ids_have_the_upstream_shape() {
		let id = new_template_id();
		assert!(looks_like_template_id(&id), "{id}");
		assert!(!looks_like_template_id("i0123456789abcdefghij"));
	}
}
