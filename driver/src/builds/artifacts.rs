//! Artifact location records in KV (`art/<buildID>/<nodeID>`) and garbage
//! collection of deleted templates' artifacts on their nodes.

use std::collections::{HashMap, HashSet};

use vmon_cluster::{MemberMeta, NodeId, gossip::MemberState};
use vmon_node::api::{ArtifactInfo, SandboxRoute};

use super::{ArtifactSource, art_key, art_prefix};
use crate::{error::ApiResult, state::AppState};

/// Builds whose artifacts are collected per GC pass.
const GC_BATCH: u32 = 50;

/// Records that `info.node_id` holds `info.build_id`'s artifacts.
pub async fn record_artifact(state: &AppState, info: &ArtifactInfo) -> ApiResult<()> {
	let value =
		serde_json::to_vec(info).map_err(|e| crate::error::ApiError::internal(e.to_string()))?;
	state
		.cluster
		.kv
		.put(&art_key(&info.build_id, info.node_id), value, None)
		.await?;
	Ok(())
}

/// Records `node` (served at `node_url`) as a holder of `build_id`.
///
/// Reads the node's own manifest. Called after a sandbox create placed on a
/// node that fetched the artifacts from a peer, so placement and later
/// fetches keep finding copies once the original builder is gone.
pub async fn record_holder(
	state: &AppState,
	node: NodeId,
	node_url: &str,
	build_id: &str,
) -> ApiResult<()> {
	let manifest = state.node_at(node_url)?.artifact_manifest(build_id).await?;
	if manifest.info.node_id != node || manifest.info.build_id != build_id {
		return Err(crate::error::ApiError::internal(format!(
			"node {node} described build {} on node {}",
			manifest.info.build_id, manifest.info.node_id
		)));
	}
	record_artifact(state, &manifest.info).await
}

/// Every recorded holder of a build's artifacts (any liveness).
pub(super) async fn artifact_records(
	state: &AppState,
	build_id: &str,
) -> ApiResult<Vec<ArtifactInfo>> {
	let entries = state.cluster.kv.list(&art_prefix(build_id)).await?;
	Ok(entries
		.into_iter()
		.filter_map(|entry| match serde_json::from_slice::<ArtifactInfo>(&entry.value) {
			Ok(info) => Some(info),
			Err(error) => {
				tracing::warn!(key = %entry.key, %error, "undecodable artifact record");
				None
			},
		})
		.collect())
}

/// Whether the build is published in the operator object store, where any
/// node can fetch it without a live holder.
pub async fn published(state: &AppState, build_id: &str) -> bool {
	let Some(s3) = &state.config.object_store else {
		return false;
	};
	let key = vmon_node::artifacts::published::object_key(build_id, vmon_node::artifacts::MANIFEST);
	match s3.exists(&key).await {
		Ok(found) => found,
		Err(error) => {
			tracing::warn!(build = build_id, %error, "checking the published build failed");
			false
		},
	}
}

/// Alive node members holding a build's artifacts.
pub async fn artifact_sources(state: &AppState, build_id: &str) -> ApiResult<Vec<ArtifactSource>> {
	let records = artifact_records(state, build_id).await?;
	Ok(join_alive(records, &state.node_members()))
}

/// Keeps the records whose node is an alive member, attaching its URL.
pub(super) fn join_alive(
	records: Vec<ArtifactInfo>,
	alive: &[(NodeId, MemberMeta)],
) -> Vec<ArtifactSource> {
	records
		.into_iter()
		.filter_map(|info| {
			let (_, meta) = alive.iter().find(|(id, _)| *id == info.node_id)?;
			Some(ArtifactSource { node_id: info.node_id, cluster_url: meta.cluster_url.clone(), info })
		})
		.collect()
}

/// One GC pass (raft leader only): deletes the artifacts of finished builds
/// of deleted templates from every holder, unless a live route or a paused
/// sandbox still references the build.
pub(super) async fn collect_garbage(state: &AppState) -> anyhow::Result<()> {
	let candidates = state.store.builds_pending_gc(GC_BATCH).await?;
	if candidates.is_empty() {
		return Ok(());
	}
	// A sandbox always has a route or a paused row (pause writes the row before
	// dropping the route, resume admits the route before updating the row), but
	// both are read at different instants: reading paused rows on both sides of
	// the route listing also catches a sandbox pausing or resuming meanwhile.
	// A failed read aborts the pass before anything is deleted.
	let mut in_use: HashSet<String> = state
		.store
		.builds_of_paused_sandboxes()
		.await?
		.into_iter()
		.collect();
	in_use.extend(
		state
			.cluster
			.kv
			.list("sbx/")
			.await?
			.iter()
			.filter_map(|entry| serde_json::from_slice::<SandboxRoute>(&entry.value).ok())
			.map(|route| route.build_id),
	);
	in_use.extend(state.store.builds_of_paused_sandboxes().await?);
	let members: HashMap<NodeId, (MemberState, String)> = state
		.cluster
		.members()
		.into_iter()
		.filter(|(_, meta)| meta.roles.node)
		.map(|(member, meta)| (member.id, (member.state, meta.cluster_url)))
		.collect();
	for build in candidates {
		if in_use.contains(&build.id) {
			continue;
		}
		let records = artifact_records(state, &build.id)
			.await
			.map_err(|e| anyhow::anyhow!(e.message))?;
		let mut holders: Vec<NodeId> = records.into_iter().map(|info| info.node_id).collect();
		if let Some(builder) = build.node_id.as_deref().and_then(|id| id.parse().ok())
			&& !holders.contains(&builder)
		{
			holders.push(builder);
		}
		let mut complete = true;
		for node_id in holders {
			match members.get(&node_id) {
				Some((MemberState::Alive, url)) => match state.node_at(url) {
					Ok(client) => match client.delete_artifact(&build.id).await {
						Ok(()) => {
							state
								.cluster
								.kv
								.delete(&art_key(&build.id, node_id))
								.await?;
						},
						Err(error) if error.is_not_found() => {
							state
								.cluster
								.kv
								.delete(&art_key(&build.id, node_id))
								.await?;
						},
						Err(error) => {
							tracing::warn!(build = %build.id, node = %node_id, %error, "artifact gc failed");
							complete = false;
						},
					},
					Err(_) => complete = false,
				},
				// Suspect or Dead members may come back with the files: retry later.
				Some(_) => complete = false,
				// Reaped from the member table: the node is gone for good.
				None => {
					state
						.cluster
						.kv
						.delete(&art_key(&build.id, node_id))
						.await?;
				},
			}
		}
		if let Some(s3) = &state.config.object_store {
			match vmon_node::artifacts::published::delete_published(s3, &build.id).await {
				Ok(_) => {},
				Err(error) => {
					tracing::warn!(build = %build.id, error = format!("{error:#}"), "published artifact gc failed");
					complete = false;
				},
			}
		}
		if complete {
			state.store.mark_artifacts_collected(&build.id).await?;
			tracing::info!(build = %build.id, "collected artifacts of deleted template build");
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use vmon_cluster::Roles;

	use super::*;

	fn info(build: &str, node: u64) -> ArtifactInfo {
		ArtifactInfo {
			build_id:        build.into(),
			node_id:         NodeId(node),
			arch:            "aarch64".into(),
			backend:         "hvf".into(),
			cpu_baseline:    "b".into(),
			has_snapshot:    true,
			bytes:           1,
			envd_version:    "0.6.4".into(),
			vcpus:           2,
			memory_mb:       1024,
			disk_mb:         2048,
			default_user:    "user".into(),
			default_workdir: None,
			env:             BTreeMap::new(),
		}
	}

	fn meta(url: &str) -> MemberMeta {
		MemberMeta {
			roles:         Roles { node: true, driver: false },
			cluster_url:   url.into(),
			public_url:    None,
			version:       "test".into(),
			started_at_ms: 0,
			node:          None,
			raft:          None,
		}
	}

	#[test]
	fn sources_keep_only_alive_holders_with_their_urls() {
		let alive = vec![(NodeId(1), meta("http://n1:7947")), (NodeId(3), meta("http://n3:7947"))];
		let sources = join_alive(vec![info("b", 1), info("b", 2), info("b", 3)], &alive);
		let urls: Vec<_> = sources
			.iter()
			.map(|s| (s.node_id, s.cluster_url.as_str()))
			.collect();
		assert_eq!(urls, [(NodeId(1), "http://n1:7947"), (NodeId(3), "http://n3:7947")]);
	}
}
