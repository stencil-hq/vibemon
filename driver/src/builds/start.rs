//! Build registration (`POST /v3/templates`), build start on a builder node
//! (`POST /v2/templates/{id}/builds/{buildID}`), cancellation, and template
//! deletion (`local://e2b-api.md` Part D §2.1, §2.4, §10).

use std::collections::{BTreeSet, HashMap};

use serde::Deserialize;
use vmon_cluster::{MemberMeta, NodeId};
use vmon_node::api::{
	BuildBase, BuildReason, BuildStep, RegistryAuth, StartBuild, TemplateVersion,
};

use super::{
	artifact_sources,
	files::file_location,
	names::{
		StepInput, build_tags, lower_steps, new_template_id, parse_name,
		template_version_from_user_agent, validate_memory, validate_min_free_disk, validate_vcpus,
	},
	resolve::{TemplateAccess, find_template, template_not_found},
};
use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
	store::{
		Team,
		builds::{BuildRow, BuildStart, BuildStatus},
		templates::{RegisterBuild, RegisterOutcome},
	},
};

/// Builder nodes tried before giving up on a start.
const MAX_START_ATTEMPTS: usize = 3;
/// Architecture preferred when the cluster is evenly split (upstream
/// default templates target `x86_64`).
const PREFERRED_ARCH: &str = "x86_64";

/// `TemplateBuildRequestV3`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuildRequest {
	pub name:             Option<String>,
	/// Deprecated alias of `name`.
	pub alias:            Option<String>,
	pub tags:             Option<Vec<String>>,
	/// Deprecated team selector; must be the caller's team when present.
	#[serde(rename = "teamID")]
	pub team_id:          Option<String>,
	pub cpu_count:        Option<i64>,
	#[serde(rename = "memoryMB")]
	pub memory_mb:        Option<i64>,
	pub min_free_disk_mb: Option<i64>,
}

/// Result of [`register_build`] (`TemplateRequestResponseV3` fields).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredBuild {
	pub template_id: String,
	pub build_id:    String,
	pub public:      bool,
	/// `["<team-slug>/<alias>"]`.
	pub names:       Vec<String>,
	pub tags:        Vec<String>,
	/// `["<alias>"]`.
	pub aliases:     Vec<String>,
}

/// `TemplateBuildStartV2`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartRequest {
	pub from_image:          Option<String>,
	pub from_template:       Option<String>,
	pub from_image_registry: Option<RegistryAuth>,
	#[serde(default)]
	pub force:               bool,
	#[serde(default)]
	pub steps:               Vec<StepInput>,
	pub start_cmd:           Option<String>,
	pub ready_cmd:           Option<String>,
}

/// Validates a build request, reuses or creates the template for its alias,
/// and registers a new `waiting` build.
pub async fn register_build(
	state: &AppState,
	team: &Team,
	req: &BuildRequest,
) -> ApiResult<RegisteredBuild> {
	if let Some(team_id) = &req.team_id
		&& *team_id != team.id
	{
		return Err(ApiError::forbidden("You are not allowed to access this team"));
	}
	let raw = req
		.name
		.as_deref()
		.or(req.alias.as_deref())
		.filter(|n| !n.trim().is_empty());
	let raw = raw.ok_or_else(|| ApiError::bad_request("Name is required"))?;
	if raw.len() > super::names::MAX_NAME_LEN {
		return Err(ApiError::bad_request("Name must be at most 128 characters"));
	}
	let name = parse_name(raw).map_err(|e| ApiError::bad_request(format!("Invalid name: {e}")))?;
	if let Some(namespace) = &name.namespace
		&& *namespace != team.slug
	{
		return Err(ApiError::bad_request(format!(
			"Namespace '{namespace}' does not match the team namespace '{}'",
			team.slug
		)));
	}
	let tags = build_tags(name.tag.as_deref(), req.tags.as_deref().unwrap_or_default())
		.map_err(ApiError::bad_request)?;
	let limits = team.limits;
	let vcpus = validate_vcpus(req.cpu_count, limits.max_vcpus).map_err(ApiError::bad_request)?;
	let memory_mb =
		validate_memory(req.memory_mb, limits.max_memory_mb).map_err(ApiError::bad_request)?;
	let min_free_disk_mb = validate_min_free_disk(req.min_free_disk_mb, limits.max_disk_mb)
		.map_err(ApiError::bad_request)?;
	let new_id = new_template_id();
	let build_id = uuid::Uuid::new_v4().to_string();
	let outcome = state
		.store
		.register_build(&RegisterBuild {
			team_id: &team.id,
			namespace: &team.slug,
			alias: &name.alias,
			new_template_id: &new_id,
			build_id: &build_id,
			tags: &tags,
			vcpus,
			memory_mb,
			disk_mb: limits.max_disk_mb,
			min_free_disk_mb,
			max_concurrent_builds: limits.max_concurrent_builds,
		})
		.await?;
	match outcome {
		RegisterOutcome::Registered { template_id, public } => Ok(RegisteredBuild {
			template_id,
			build_id,
			public,
			names: vec![format!("{}/{}", team.slug, name.alias)],
			tags,
			aliases: vec![name.alias],
		}),
		RegisterOutcome::AliasTaken => {
			Err(ApiError::conflict(format!("Alias '{}' is already used", name.alias)))
		},
		RegisterOutcome::ConcurrencyLimit { limit } => Err(ApiError::too_many_requests(
			format!(
				"you have reached the maximum number of concurrent template builds ({limit}). Please \
				 wait for existing builds to complete or contact support if you need more concurrent \
				 builds."
			),
			30,
		)),
	}
}

/// Validates a start request for a `waiting` build, cancels the template's
/// other running builds, picks a builder node, and starts the build there.
/// The supervisor takes over from `building`.
pub async fn start_build(
	state: &AppState,
	team: &Team,
	template_ref: &str,
	build_id: &str,
	req: StartRequest,
	user_agent: Option<&str>,
) -> ApiResult<()> {
	let from_image = req.from_image.as_deref().filter(|s| !s.is_empty());
	let from_template = req.from_template.as_deref().filter(|s| !s.is_empty());
	match (from_image, from_template) {
		(None, None) => {
			return Err(ApiError::bad_request("must specify either fromImage or fromTemplate"));
		},
		(Some(_), Some(_)) => {
			return Err(ApiError::bad_request("cannot specify both fromImage and fromTemplate"));
		},
		_ => {},
	}
	if uuid::Uuid::parse_str(build_id).is_err() {
		return Err(ApiError::bad_request("Invalid build ID"));
	}
	let template = state
		.store
		.template(template_ref)
		.await?
		.ok_or_else(|| template_not_found(template_ref))?;
	let build = state
		.store
		.build(build_id)
		.await?
		.filter(|b| b.template_id == template.id)
		.ok_or_else(|| ApiError::not_found(format!("build '{build_id}' not found")))?;
	if template.team_id != team.id {
		return Err(ApiError::forbidden(format!(
			"you don't have access to template '{}'",
			template.id
		)));
	}
	if build.status() != BuildStatus::Waiting {
		return Err(ApiError::bad_request("build is not in waiting state"));
	}
	let version = template_version_from_user_agent(user_agent).map_err(ApiError::bad_request)?;
	let steps = lower_steps(&req.steps).map_err(ApiError::bad_request)?;
	let file_sources = file_sources(state, &template.id, &steps).await?;

	let (base, required_arch, start_cmd, ready_cmd) = match (from_image, from_template) {
		(Some(image), _) => {
			let base = BuildBase::Image {
				image:    image.to_owned(),
				registry: req.from_image_registry.clone(),
			};
			(base, None, req.start_cmd.clone(), req.ready_cmd.clone())
		},
		(None, Some(parent_ref)) => {
			let parent = parent_build(state, team, parent_ref).await?;
			let arch = parent.arch.clone();
			let sources = artifact_sources(state, &parent.id).await?;
			if sources.is_empty() {
				return Err(ApiError::unavailable(format!(
					"no node holding template '{parent_ref}' is available right now"
				)));
			}
			// Neither command set: inherit the parent's pair (Part D §6).
			let (start_cmd, ready_cmd) = if req.start_cmd.is_none() && req.ready_cmd.is_none() {
				(parent.start_cmd.clone(), parent.ready_cmd.clone())
			} else {
				(req.start_cmd.clone(), req.ready_cmd.clone())
			};
			let base = BuildBase::Build {
				build_id: parent.id.clone(),
				sources:  sources.into_iter().map(|s| s.cluster_url).collect(),
			};
			(base, arch, start_cmd, ready_cmd)
		},
		(None, None) => unreachable!("validated above"),
	};

	for other in state
		.store
		.template_building_builds(&template.id, &build.tags.0)
		.await?
	{
		let reason = "The build was canceled because a newer build of the template was started.";
		cancel_build(state, &other, reason).await;
	}

	let spec = serde_json::json!({
		"fromImage": from_image,
		"fromTemplate": from_template,
		"fromImageRegistry": req.from_image_registry.as_ref().map(registry_kind),
		"force": req.force,
		"steps": steps,
	});
	let statuses = node_statuses(state).await?;
	let candidates = builder_candidates(&state.node_members(), required_arch.as_deref(), &statuses);
	if candidates.is_empty() {
		return Err(
			ApiError::unavailable(match required_arch {
				Some(arch) => format!("no {arch} node is available to build this template"),
				None => "no node is available to build templates".into(),
			})
			.with_code("sandbox_no_compatible_node"),
		);
	}
	let start = StartBuild {
		build_id: build.id.clone(),
		template_id: template.id.clone(),
		team_id: team.id.clone(),
		from: base,
		steps,
		start_cmd: start_cmd.clone(),
		ready_cmd: ready_cmd.clone(),
		vcpus: build.vcpus,
		memory_mb: build.memory_mb,
		disk_mb: build.disk_mb,
		file_sources,
		force: req.force,
		template_version: version,
	};
	let mut last_error = None;
	for (node_id, meta) in candidates.into_iter().take(MAX_START_ATTEMPTS) {
		let client = state.node_at(&meta.cluster_url)?;
		match client.start_build(&start).await {
			Ok(()) => {
				let arch = meta
					.node
					.as_ref()
					.map(|cap| cap.arch.clone())
					.unwrap_or_default();
				let recorded = state
					.store
					.mark_build_started(&build.id, &BuildStart {
						node_id:          &node_id.to_string(),
						arch:             &arch,
						spec:             &spec,
						template_version: version_str(version),
						start_cmd:        start_cmd.as_deref(),
						ready_cmd:        ready_cmd.as_deref(),
					})
					.await?;
				if !recorded {
					// Raced with a cancellation/supersede: undo on the node.
					let _ = client.cancel_build(&build.id).await;
					return Err(ApiError::bad_request("build is not in waiting state"));
				}
				tracing::info!(build = %build.id, template = %template.id, node = %node_id, "build started");
				return Ok(());
			},
			Err(error) => {
				tracing::warn!(build = %build.id, node = %node_id, %error, "builder refused build");
				last_error = Some(error);
			},
		}
	}
	Err(
		last_error
			.map_or_else(|| ApiError::unavailable("no builder accepted the build"), ApiError::from),
	)
}

/// Cancels an unfinished build (node first, best effort) and records
/// `reason`. Returns whether the build was still unfinished.
pub async fn cancel_build(state: &AppState, build: &BuildRow, reason: &str) -> bool {
	if build.status() == BuildStatus::Building
		&& let Some(node_id) = build
			.node_id
			.as_deref()
			.and_then(|id| id.parse::<NodeId>().ok())
		&& let Ok(client) = state.node(node_id)
		&& let Err(error) = client.cancel_build(&build.id).await
		&& !error.is_not_found()
	{
		tracing::warn!(build = %build.id, %error, "cancelling build on node failed");
	}
	match state
		.store
		.fail_build(&build.id, &BuildReason { message: reason.to_owned(), step: None })
		.await
	{
		Ok(changed) => changed,
		Err(error) => {
			tracing::warn!(build = %build.id, %error, "recording build cancellation failed");
			false
		},
	}
}

/// Soft-deletes a template (aliases and tags freed, running builds
/// cancelled on their nodes); artifacts are collected later by the
/// supervisor's GC once no sandbox uses them.
pub async fn delete_template(state: &AppState, template_id: &str) -> ApiResult<()> {
	let building = state
		.store
		.soft_delete_template(template_id)
		.await?
		.ok_or_else(|| template_not_found(template_id))?;
	for build in building {
		if let Some(node_id) = build
			.node_id
			.as_deref()
			.and_then(|id| id.parse::<NodeId>().ok())
			&& let Ok(client) = state.node(node_id)
			&& let Err(error) = client.cancel_build(&build.id).await
			&& !error.is_not_found()
		{
			tracing::warn!(build = %build.id, %error, "cancelling build of deleted template failed");
		}
	}
	Ok(())
}

/// Ready build a `fromTemplate` reference points to (team-owned or public).
async fn parent_build(state: &AppState, team: &Team, reference: &str) -> ApiResult<BuildRow> {
	let name = parse_name(reference)
		.map_err(|e| ApiError::bad_request(format!("Invalid fromTemplate: {e}")))?;
	let Some((template, _)) = find_template(state, team, &name).await? else {
		return Err(ApiError::bad_request(format!("template '{reference}' not found")));
	};
	if TemplateAccess::of(&template, &team.id) == TemplateAccess::Denied {
		return Err(ApiError::forbidden(format!("you don't have access to template '{reference}'")));
	}
	let build_id = match state
		.store
		.tag_build(&template.id, name.tag_or_default())
		.await?
	{
		Some(id) => Some(id),
		None if name.tag.is_none() => state
			.store
			.latest_ready_builds(std::slice::from_ref(&template.id))
			.await?
			.into_iter()
			.next()
			.map(|b| b.id),
		None => None,
	};
	let build = match build_id {
		Some(id) => state.store.build(&id).await?,
		None => None,
	};
	build
		.filter(|b| b.status() == BuildStatus::Ready)
		.ok_or_else(|| ApiError::bad_request(format!("template '{reference}' has no ready build")))
}

/// Cluster URLs of the nodes holding every `COPY` archive; 400 when one was
/// never uploaded (or its node is gone — the client re-uploads).
async fn file_sources(
	state: &AppState,
	template_id: &str,
	steps: &[BuildStep],
) -> ApiResult<Vec<String>> {
	let mut urls = BTreeSet::new();
	for (index, step) in steps.iter().enumerate() {
		let Some(hash) = &step.files_hash else {
			continue;
		};
		if step.kind != "COPY" {
			continue;
		}
		let url = file_location(state, template_id, hash)
			.await?
			.ok_or_else(|| {
				ApiError::bad_request(format!(
					"files for step {} ({hash}) were not uploaded",
					index + 1
				))
			})?;
		urls.insert(url);
	}
	Ok(urls.into_iter().collect())
}

/// Admin node status overrides (`node/<id>/status`).
async fn node_statuses(state: &AppState) -> ApiResult<HashMap<NodeId, String>> {
	let entries = state.cluster.kv.list("node/").await?;
	Ok(entries
		.into_iter()
		.filter_map(|entry| {
			let id = entry
				.key
				.strip_prefix("node/")?
				.strip_suffix("/status")?
				.parse()
				.ok()?;
			let status = serde_json::from_slice::<String>(&entry.value).ok()?;
			Some((id, status))
		})
		.collect())
}

/// Builder candidates in preference order: alive nodes with capacity info,
/// not draining (by gossip or admin status), of `required_arch` — else of
/// the most common architecture (ties prefer `x86_64`) — by free capacity.
pub(super) fn builder_candidates(
	members: &[(NodeId, MemberMeta)],
	required_arch: Option<&str>,
	statuses: &HashMap<NodeId, String>,
) -> Vec<(NodeId, MemberMeta)> {
	let eligible: Vec<&(NodeId, MemberMeta)> = members
		.iter()
		.filter(|(id, meta)| {
			meta.node.as_ref().is_some_and(|cap| !cap.draining)
				&& statuses.get(id).is_none_or(|status| status == "ready")
		})
		.collect();
	let arch = if let Some(arch) = required_arch {
		arch.to_owned()
	} else {
		let mut counts: HashMap<&str, usize> = HashMap::new();
		for (_, meta) in &eligible {
			if let Some(cap) = &meta.node {
				*counts.entry(cap.arch.as_str()).or_default() += 1;
			}
		}
		let Some(arch) = counts
			.iter()
			.max_by_key(|(arch, count)| (**count, **arch == PREFERRED_ARCH))
		else {
			return Vec::new();
		};
		(*arch.0).to_owned()
	};
	let mut out: Vec<(NodeId, MemberMeta)> = eligible
		.into_iter()
		.filter(|(_, meta)| meta.node.as_ref().is_some_and(|cap| cap.arch == arch))
		.cloned()
		.collect();
	out.sort_by_key(|(id, meta)| (std::cmp::Reverse(free_score(meta)), *id));
	out
}

/// Free capacity score: memory headroom plus 1 GiB per free vCPU, minus
/// 2 GiB per running build.
fn free_score(meta: &MemberMeta) -> i64 {
	let Some(cap) = &meta.node else {
		return i64::MIN;
	};
	let free_mem =
		i64::try_from(cap.memory_mb.saturating_sub(cap.memory_used_mb)).unwrap_or(i64::MAX / 4);
	let free_cpus = i64::from(cap.cpus.saturating_sub(cap.vcpus_used));
	free_mem + free_cpus * 1024 - i64::from(cap.builds) * 2048
}

const fn registry_kind(auth: &RegistryAuth) -> &'static str {
	match auth {
		RegistryAuth::Registry { .. } => "registry",
		RegistryAuth::Aws { .. } => "aws",
		RegistryAuth::Gcp { .. } => "gcp",
	}
}

const fn version_str(version: TemplateVersion) -> &'static str {
	match version {
		TemplateVersion::V2_0 => "2.0.0",
		TemplateVersion::V2_1 => "2.1.0",
	}
}

#[cfg(test)]
mod tests {
	use vmon_cluster::{NodeCapacity, Roles};

	use super::*;

	fn node(
		id: u64,
		arch: &str,
		memory_free: u64,
		builds: u32,
		draining: bool,
	) -> (NodeId, MemberMeta) {
		let cap = NodeCapacity {
			arch: arch.into(),
			backend: "kvm".into(),
			os: "linux".into(),
			cpu_baseline: "x".into(),
			cpus: 8,
			memory_mb: 16384,
			disk_free_mb: 100_000,
			vcpus_used: 0,
			memory_used_mb: 16384 - memory_free,
			sandboxes: 0,
			builds,
			draining,
			features: vec![],
			cached_builds: vec![],
		};
		let meta = MemberMeta {
			roles:         Roles { node: true, driver: false },
			cluster_url:   format!("http://n{id}:7947"),
			public_url:    None,
			version:       "t".into(),
			started_at_ms: 0,
			node:          Some(cap),
			raft:          None,
		};
		(NodeId(id), meta)
	}

	fn ids(candidates: &[(NodeId, MemberMeta)]) -> Vec<u64> {
		candidates.iter().map(|(id, _)| id.0).collect()
	}

	#[test]
	fn majority_arch_then_most_free_capacity() {
		let members = vec![
			node(1, "aarch64", 1000, 0, false),
			node(2, "x86_64", 8000, 0, false),
			node(3, "aarch64", 4000, 0, false),
			node(4, "aarch64", 9000, 3, false),
		];
		assert_eq!(ids(&builder_candidates(&members, None, &HashMap::new())), [3, 4, 1]);
	}

	#[test]
	fn arch_tie_prefers_x86_64_and_required_arch_is_a_hard_filter() {
		let members = vec![node(1, "aarch64", 9000, 0, false), node(2, "x86_64", 1000, 0, false)];
		assert_eq!(ids(&builder_candidates(&members, None, &HashMap::new())), [2]);
		assert_eq!(ids(&builder_candidates(&members, Some("aarch64"), &HashMap::new())), [1]);
		let candidates = builder_candidates(&members, Some("riscv64"), &HashMap::new());
		assert!(candidates.is_empty(), "unexpected candidates: {candidates:?}");
	}

	#[test]
	fn draining_and_admin_status_exclude_nodes() {
		let members = vec![
			node(1, "x86_64", 9000, 0, true),
			node(2, "x86_64", 8000, 0, false),
			node(3, "x86_64", 100, 0, false),
		];
		let statuses =
			HashMap::from([(NodeId(2), "unhealthy".to_owned()), (NodeId(3), "ready".to_owned())]);
		assert_eq!(ids(&builder_candidates(&members, None, &statuses)), [3]);
	}
}
