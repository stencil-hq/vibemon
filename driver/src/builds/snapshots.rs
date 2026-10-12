//! Snapshot templates (`POST /sandboxes/{id}/snapshots`, `GET /snapshots`)
//! and hidden fork builds (`POST /sandboxes/{id}/fork`). The sandbox API
//! drives the node snapshot; this module owns the template/build rows and
//! retires fork templates once their children are started.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::Serialize;
use vmon_cluster::NodeId;
use vmon_node::api::{ArtifactInfo, BuildReason};

use super::{
	names::{DEFAULT_TAG, new_template_id, parse_name},
	record_artifact,
};
use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
	store::{
		Cursor, Team, micros,
		templates::{RegisterSnapshot, SnapshotOutcome, TemplateFilter, TemplateKind},
	},
};

/// What a snapshot build is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotKind {
	/// User-visible snapshot template (optionally named).
	Named,
	/// Hidden template backing a fork; never listed.
	Fork,
}

/// Input of [`begin_snapshot`].
#[derive(Clone, Debug)]
pub struct SnapshotRequest<'a> {
	pub source_sandbox_id: &'a str,
	/// Node running the source sandbox (it writes the build's artifacts).
	pub node_id:           NodeId,
	/// `[namespace/]alias[:tag]`; ignored for [`SnapshotKind::Fork`].
	pub name:              Option<&'a str>,
	pub kind:              SnapshotKind,
	pub vcpus:             u32,
	pub memory_mb:         u64,
	pub disk_mb:           u64,
	pub envd_version:      &'a str,
}

/// A snapshot build in progress; pass it to [`finish_snapshot`] or
/// [`fail_snapshot`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotTarget {
	pub template_id: String,
	pub build_id:    String,
	/// `SnapshotInfo.snapshotID`.
	pub snapshot_id: String,
	/// `SnapshotInfo.names`.
	pub names:       Vec<String>,
}

/// E2B `SnapshotInfo`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct SnapshotInfo {
	#[serde(rename = "snapshotID")]
	pub snapshot_id: String,
	pub names:       Vec<String>,
}

/// Creates (or reuses, for an existing name of the team) the snapshot
/// template and a `building` snapshot build. 400 on a bad name or a
/// namespace other than the team slug; 409 when the name is taken.
pub async fn begin_snapshot(
	state: &AppState,
	team: &Team,
	req: SnapshotRequest<'_>,
) -> ApiResult<SnapshotTarget> {
	let parsed = match (req.kind, req.name) {
		(SnapshotKind::Named, Some(raw)) => {
			let name = parse_name(raw).map_err(ApiError::bad_request)?;
			if let Some(namespace) = &name.namespace
				&& *namespace != team.slug
			{
				return Err(ApiError::bad_request(format!(
					"namespace '{namespace}' does not match the team slug '{}'",
					team.slug
				)));
			}
			Some(name)
		},
		_ => None,
	};
	let tag = parsed
		.as_ref()
		.map_or(DEFAULT_TAG, |name| name.tag_or_default())
		.to_owned();
	let new_id = new_template_id();
	let build_id = uuid::Uuid::new_v4().to_string();
	let node_id = req.node_id.to_string();
	let outcome = state
		.store
		.register_snapshot(&RegisterSnapshot {
			team_id:           &team.id,
			kind:              match req.kind {
				SnapshotKind::Named => TemplateKind::Snapshot,
				SnapshotKind::Fork => TemplateKind::Fork,
			},
			source_sandbox_id: req.source_sandbox_id,
			node_id:           &node_id,
			name:              parsed
				.as_ref()
				.map(|name| (team.slug.as_str(), name.alias.as_str())),
			tag:               &tag,
			new_template_id:   &new_id,
			build_id:          &build_id,
			vcpus:             req.vcpus,
			memory_mb:         req.memory_mb,
			disk_mb:           req.disk_mb,
			envd_version:      req.envd_version,
		})
		.await?;
	let template_id = match outcome {
		SnapshotOutcome::Registered { template_id } => template_id,
		SnapshotOutcome::AliasTaken => {
			let alias = parsed.map(|name| name.alias).unwrap_or_default();
			return Err(ApiError::conflict(format!("Alias '{alias}' is already used")));
		},
	};
	let (snapshot_id, names) = match &parsed {
		Some(name) => {
			let full = format!("{}/{}:{tag}", team.slug, name.alias);
			(full.clone(), vec![full])
		},
		None => (format!("{template_id}:{tag}"), Vec::new()),
	};
	Ok(SnapshotTarget { template_id, build_id, snapshot_id, names })
}

/// Completes a snapshot build: records the artifact location in KV, marks
/// the build ready, and points its tag at it. 409 when the build was
/// already finished (cancelled or its template deleted).
pub async fn finish_snapshot(
	state: &AppState,
	target: &SnapshotTarget,
	artifact: &ArtifactInfo,
) -> ApiResult<()> {
	record_artifact(state, artifact).await?;
	if state
		.store
		.finish_build_ready(&target.build_id, artifact)
		.await?
	{
		Ok(())
	} else {
		Err(ApiError::conflict(format!(
			"snapshot build {} is no longer in progress",
			target.build_id
		)))
	}
}

/// Marks a snapshot build failed (best effort; errors are logged).
pub async fn fail_snapshot(state: &AppState, target: &SnapshotTarget, reason: &str) {
	let reason = BuildReason { message: reason.to_owned(), step: None };
	if let Err(error) = state.store.fail_build(&target.build_id, &reason).await {
		tracing::warn!(build = %target.build_id, %error, "recording snapshot failure");
	}
}

/// Retires the hidden template of a fork once its children started or the fork
/// failed.
///
/// Nothing resolves it any more, and artifact GC reclaims its build once no
/// running or paused sandbox uses it. Best effort: the leader's housekeeping
/// retires forks this misses ([`retire_abandoned_forks`]).
pub async fn retire_fork(state: &AppState, target: &SnapshotTarget) {
	if let Err(error) = state.store.soft_delete_template(&target.template_id).await {
		tracing::warn!(template = %target.template_id, %error, "retiring fork template failed");
	}
}

/// Retires fork templates created before `before` that were never retired
/// (their driver died mid-fork) — older than any fork operation can run.
pub(super) async fn retire_abandoned_forks(
	state: &AppState,
	before: DateTime<Utc>,
) -> anyhow::Result<()> {
	for template_id in state.store.unretired_fork_templates(before).await? {
		tracing::info!(template = %template_id, "retiring abandoned fork template");
		state.store.soft_delete_template(&template_id).await?;
	}
	Ok(())
}

/// `GET /snapshots`: the team's snapshot templates with a ready build,
/// newest first, filtered by source sandbox and/or name; returns the page
/// and the next token.
pub async fn list_snapshots(
	state: &AppState,
	team: &Team,
	sandbox_id: Option<&str>,
	name: Option<&str>,
	limit: u32,
	next_token: Option<&str>,
) -> ApiResult<(Vec<SnapshotInfo>, Option<String>)> {
	if !(1..=100).contains(&limit) {
		return Err(ApiError::bad_request("limit must be between 1 and 100"));
	}
	let cursor = match next_token {
		Some(token) => {
			Some(Cursor::decode(token).ok_or_else(|| ApiError::bad_request("Invalid next token"))?)
		},
		None => None,
	};
	let alias = match name {
		Some(raw) => {
			let name = parse_name(raw).map_err(ApiError::bad_request)?;
			if name.namespace.as_ref().is_some_and(|ns| *ns != team.slug) {
				return Ok((Vec::new(), None));
			}
			Some(name.alias)
		},
		None => None,
	};
	let filter = TemplateFilter {
		source_sandbox_id: sandbox_id,
		alias:             alias.as_deref(),
		ready_only:        true,
	};
	let mut rows = state
		.store
		.list_templates(&team.id, TemplateKind::Snapshot, &filter, cursor.as_ref(), Some(limit + 1))
		.await?;
	let next = if rows.len() > limit as usize {
		rows.truncate(limit as usize);
		rows
			.last()
			.map(|row| Cursor { created_at_us: micros(row.created_at), id: row.id.clone() }.encode())
	} else {
		None
	};
	let ids: Vec<String> = rows.iter().map(|row| row.id.clone()).collect();
	let mut aliases: HashMap<String, Vec<String>> = HashMap::new();
	for alias in state.store.aliases_of(&ids).await? {
		aliases
			.entry(alias.template_id.clone())
			.or_default()
			.push(alias.name());
	}
	let mut tags: HashMap<String, Vec<(String, String)>> = HashMap::new();
	for tag in state.store.tags_of(&ids).await? {
		tags
			.entry(tag.template_id)
			.or_default()
			.push((tag.tag, tag.build_id));
	}
	let latest: HashMap<String, String> = state
		.store
		.latest_ready_builds(&ids)
		.await?
		.into_iter()
		.map(|b| (b.template_id, b.id))
		.collect();
	let infos = rows
		.iter()
		.map(|row| {
			snapshot_info(
				&row.id,
				aliases.get(&row.id).map_or(&[][..], Vec::as_slice),
				tags.get(&row.id).map_or(&[][..], Vec::as_slice),
				latest.get(&row.id).map(String::as_str),
			)
		})
		.collect();
	Ok((infos, next))
}

/// `SnapshotInfo` of one template: `snapshotID` names the tag of the latest
/// ready build (preferring `default`), `names` every `name:tag`.
fn snapshot_info(
	template_id: &str,
	names: &[String],
	tags: &[(String, String)],
	latest_build: Option<&str>,
) -> SnapshotInfo {
	let tag = tags
		.iter()
		.filter(|(_, build)| Some(build.as_str()) == latest_build)
		.map(|(tag, _)| tag.as_str())
		.min_by_key(|tag| *tag != DEFAULT_TAG)
		.unwrap_or(DEFAULT_TAG);
	let base = names.first().map_or(template_id, String::as_str);
	let all = names
		.iter()
		.flat_map(|name| tags.iter().map(move |(tag, _)| format!("{name}:{tag}")))
		.collect();
	SnapshotInfo { snapshot_id: format!("{base}:{tag}"), names: all }
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snapshot_info_prefers_the_default_tag_of_the_latest_build() {
		let tags = vec![
			("v1".to_owned(), "b1".to_owned()),
			("default".into(), "b2".into()),
			("v2".into(), "b2".into()),
		];
		let info = snapshot_info("tid", &["acme/snap".into()], &tags, Some("b2"));
		assert_eq!(info.snapshot_id, "acme/snap:default");
		assert_eq!(info.names, ["acme/snap:v1", "acme/snap:default", "acme/snap:v2"]);
	}

	#[test]
	fn unnamed_snapshot_falls_back_to_the_template_id() {
		let info = snapshot_info("tid", &[], &[("v3".into(), "b9".into())], Some("b9"));
		assert_eq!(info, SnapshotInfo { snapshot_id: "tid:v3".into(), names: vec![] });
	}
}
