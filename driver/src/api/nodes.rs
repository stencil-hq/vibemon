//! Admin node endpoints: `GET /nodes`, `GET|POST /nodes/{nodeID}`, and the
//! dashboard's `GET /admin/cluster` (vmon extension).
//!
//! Nodes are gossip members with the node role (E2B `Node`, `NodeDetail`,
//! `NodeStatusChange`); the admin status override lives in KV
//! `node/<id>/status`.

use std::collections::HashMap;

use axum::{
	Json, Router,
	body::Bytes,
	extract::{Path, State},
	http::StatusCode,
	routing::get,
};
use serde::{Deserialize, Serialize};
use vmon_cluster::{MemberMeta, NodeId, RaftStatus, Roles, gossip::MemberState};
use vmon_node::api::SandboxInfo;

use crate::{
	api::sandboxes::dto::rfc3339_ms,
	auth::AdminAuth,
	error::{ApiError, ApiResult},
	lifecycle::datetime,
	scheduler::{
		self, MEMORY_OVERCOMMIT, NOMINAL_SANDBOX_MEMORY_MB, NodeCounters, NodeStatus, status_key,
	},
	state::AppState,
	stats::FleetSample,
};

/// Node routes (merged by [`crate::api::router`]).
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/admin/cluster", get(cluster))
		.route("/nodes", get(list))
		.route("/nodes/{node_id}", get(detail).post(set_status))
}

/// `MachineInfo`.
#[derive(Clone, Debug, Serialize)]
#[allow(clippy::struct_field_names, reason = "E2B wire field names")]
#[serde(rename_all = "camelCase")]
struct MachineInfo {
	cpu_family:       String,
	cpu_model:        String,
	cpu_model_name:   String,
	cpu_architecture: String,
}

/// `NodeMetrics`. The `hugePages*` fields report 0 and `disks` is empty:
/// nodes do not gossip their hugetlb pool (`--hugepage-pool-mb`) or per-disk
/// usage.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeMetrics {
	#[serde(rename = "allocatedCPU")]
	allocated_cpu:          u32,
	cpu_percent:            u32,
	cpu_count:              u32,
	allocated_memory_bytes: u64,
	memory_used_bytes:      u64,
	memory_total_bytes:     u64,
	huge_pages_total:       u64,
	huge_pages_used:        u64,
	huge_pages_reserved:    u64,
	huge_page_size_bytes:   u64,
	disks:                  Vec<serde_json::Value>,
}

/// `Node` / `NodeDetail` (detail adds the node's sandboxes and cached builds).
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct NodeDto {
	version:                String,
	commit:                 String,
	id:                     String,
	#[serde(rename = "serviceInstanceID")]
	service_instance_id:    String,
	#[serde(rename = "clusterID")]
	cluster_id:             String,
	machine_info:           MachineInfo,
	status:                 &'static str,
	status_changed_at:      String,
	sandbox_count:          u32,
	max_sandboxes:          i64,
	outstanding_work:       u64,
	metrics:                NodeMetrics,
	create_successes:       u64,
	create_fails:           u64,
	sandbox_starting_count: u32,
	#[serde(skip_serializing_if = "Option::is_none")]
	sandboxes:              Option<Vec<SandboxInfo>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	cached_builds:          Option<Vec<String>>,
}

/// `NodeStatusChange`.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusChange {
	status:      Option<String>,
	#[serde(rename = "clusterID")]
	_cluster_id: Option<String>,
}

/// `GET /admin/cluster` body.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ClusterDto {
	#[serde(rename = "clusterID")]
	cluster_id: String,
	#[serde(rename = "localID")]
	local_id:   String,
	members:    Vec<MemberDto>,
	history:    Vec<FleetSample>,
}

/// One gossip member.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MemberDto {
	id:          String,
	state:       MemberState,
	roles:       Roles,
	version:     String,
	started_at:  String,
	#[serde(rename = "clusterURL")]
	cluster_url: String,
	#[serde(rename = "publicURL")]
	public_url:  Option<String>,
	raft:        Option<RaftStatus>,
	node:        Option<MemberNodeDto>,
}

/// Capacity and placement view of a node member.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MemberNodeDto {
	status:           &'static str,
	arch:             String,
	os:               String,
	backend:          String,
	cpu_baseline:     String,
	cpus:             u32,
	vcpus_used:       u32,
	#[serde(rename = "memoryMB")]
	memory_mb:        u64,
	#[serde(rename = "memoryUsedMB")]
	memory_used_mb:   u64,
	#[serde(rename = "diskFreeMB")]
	disk_free_mb:     u64,
	sandboxes:        u32,
	builds:           u32,
	features:         Vec<String>,
	cached_builds:    usize,
	create_successes: u64,
	create_fails:     u64,
	inflight:         u32,
}

/// Public status: gossip liveness first, then the admin override / node drain
/// flag.
fn effective_status(
	member_state: MemberState,
	meta: &MemberMeta,
	admin: Option<NodeStatus>,
) -> &'static str {
	match member_state {
		MemberState::Dead => return "unhealthy",
		MemberState::Left => return "shutting_down",
		MemberState::Suspect => return "connecting",
		MemberState::Alive => {},
	}
	match admin.unwrap_or_default() {
		NodeStatus::Ready if meta.node.as_ref().is_some_and(|n| n.draining) => "draining",
		status => status.as_str(),
	}
}

fn node_dto(
	id: NodeId,
	member_state: MemberState,
	meta: &MemberMeta,
	admin: Option<NodeStatus>,
	counters: NodeCounters,
	cluster_id: &str,
) -> NodeDto {
	let cap = meta.node.clone().unwrap_or_default();
	let max_sandboxes = (cap.memory_mb * MEMORY_OVERCOMMIT) / NOMINAL_SANDBOX_MEMORY_MB;
	NodeDto {
		version:                meta.version.clone(),
		commit:                 meta.version.clone(),
		id:                     id.to_string(),
		service_instance_id:    id.to_string(),
		cluster_id:             cluster_id.to_owned(),
		machine_info:           MachineInfo {
			cpu_family:       cap.backend.clone(),
			cpu_model:        cap.cpu_baseline.clone(),
			cpu_model_name:   format!("{} {}", cap.os, cap.cpu_baseline).trim().to_owned(),
			cpu_architecture: cap.arch.clone(),
		},
		status:                 effective_status(member_state, meta, admin),
		status_changed_at:      rfc3339_ms(meta.started_at_ms),
		sandbox_count:          cap.sandboxes,
		max_sandboxes:          i64::try_from(max_sandboxes).unwrap_or(i64::MAX),
		outstanding_work:       u64::from(cap.sandboxes) + u64::from(cap.builds),
		metrics:                NodeMetrics {
			allocated_cpu:          cap.vcpus_used,
			cpu_percent:            0,
			cpu_count:              cap.cpus,
			allocated_memory_bytes: cap.memory_used_mb << 20,
			memory_used_bytes:      cap.memory_used_mb << 20,
			memory_total_bytes:     cap.memory_mb << 20,
			huge_pages_total:       0,
			huge_pages_used:        0,
			huge_pages_reserved:    0,
			huge_page_size_bytes:   0,
			disks:                  Vec::new(),
		},
		create_successes:       counters.successes,
		create_fails:           counters.fails,
		sandbox_starting_count: counters.inflight,
		sandboxes:              None,
		cached_builds:          None,
	}
}

fn cluster_id(state: &AppState) -> String {
	let local = state.cluster.local_id();
	state
		.cluster
		.members()
		.into_iter()
		.find(|(member, _)| member.id == local)
		.and_then(|(_, meta)| meta.raft.and_then(|raft| raft.cluster_id))
		.unwrap_or_default()
}

async fn statuses(state: &AppState) -> ApiResult<HashMap<NodeId, NodeStatus>> {
	Ok(scheduler::load_statuses(&state.cluster.kv).await?)
}

fn member_dto(
	id: NodeId,
	member_state: MemberState,
	meta: MemberMeta,
	admin: Option<NodeStatus>,
	counters: NodeCounters,
) -> MemberDto {
	let node = meta
		.node
		.as_ref()
		.filter(|_| meta.roles.node)
		.map(|cap| MemberNodeDto {
			status:           effective_status(member_state, &meta, admin),
			arch:             cap.arch.clone(),
			os:               cap.os.clone(),
			backend:          cap.backend.clone(),
			cpu_baseline:     cap.cpu_baseline.clone(),
			cpus:             cap.cpus,
			vcpus_used:       cap.vcpus_used,
			memory_mb:        cap.memory_mb,
			memory_used_mb:   cap.memory_used_mb,
			disk_free_mb:     cap.disk_free_mb,
			sandboxes:        cap.sandboxes,
			builds:           cap.builds,
			features:         cap.features.clone(),
			cached_builds:    cap.cached_builds.len(),
			create_successes: counters.successes,
			create_fails:     counters.fails,
			inflight:         counters.inflight,
		});
	MemberDto {
		id: id.to_string(),
		state: member_state,
		roles: meta.roles,
		version: meta.version,
		started_at: rfc3339_ms(meta.started_at_ms),
		cluster_url: meta.cluster_url,
		public_url: meta.public_url,
		raft: meta.raft.filter(|_| meta.roles.driver),
		node,
	}
}

/// `GET /admin/cluster`: every gossip member plus this driver's fleet history.
async fn cluster(State(state): State<AppState>, _admin: AdminAuth) -> ApiResult<Json<ClusterDto>> {
	let statuses = statuses(&state).await?;
	let counters = state.scheduler.counters();
	let mut members: Vec<MemberDto> = state
		.cluster
		.members()
		.into_iter()
		.map(|(member, meta)| {
			let c = counters.get(&member.id).copied().unwrap_or_default();
			member_dto(member.id, member.state, meta, statuses.get(&member.id).copied(), c)
		})
		.collect();
	members.sort_by(|a, b| a.id.cmp(&b.id));
	Ok(Json(ClusterDto {
		cluster_id: cluster_id(&state),
		local_id: state.cluster.local_id().to_string(),
		members,
		history: state.stats.snapshot(),
	}))
}

async fn list(State(state): State<AppState>, _admin: AdminAuth) -> ApiResult<Json<Vec<NodeDto>>> {
	let statuses = statuses(&state).await?;
	let counters = state.scheduler.counters();
	let cluster_id = cluster_id(&state);
	let mut nodes: Vec<NodeDto> = state
		.cluster
		.members()
		.into_iter()
		.filter(|(_, meta)| meta.roles.node)
		.map(|(member, meta)| {
			let c = counters.get(&member.id).copied().unwrap_or_default();
			node_dto(member.id, member.state, &meta, statuses.get(&member.id).copied(), c, &cluster_id)
		})
		.collect();
	nodes.sort_by(|a, b| a.id.cmp(&b.id));
	Ok(Json(nodes))
}

fn parse_node_id(raw: &str) -> ApiResult<NodeId> {
	raw.parse()
		.map_err(|_| ApiError::not_found(format!("node '{raw}' not found")))
}

async fn detail(
	State(state): State<AppState>,
	_admin: AdminAuth,
	Path(raw): Path<String>,
) -> ApiResult<Json<NodeDto>> {
	let id = parse_node_id(&raw)?;
	let (member, meta) = state
		.cluster
		.members()
		.into_iter()
		.find(|(member, meta)| member.id == id && meta.roles.node)
		.ok_or_else(|| ApiError::not_found(format!("node '{raw}' not found")))?;
	let admin = statuses(&state).await?.get(&id).copied();
	let mut dto = node_dto(
		id,
		member.state,
		&meta,
		admin,
		state.scheduler.node_counters(id),
		&cluster_id(&state),
	);
	if member.state == MemberState::Alive {
		dto.sandboxes = Some(state.node_at(&meta.cluster_url)?.list_sandboxes().await?);
	}
	dto.cached_builds = Some(meta.node.map(|n| n.cached_builds).unwrap_or_default());
	Ok(Json(dto))
}

/// Parses a requested status; only the statuses vibemon enforces are accepted.
fn parse_status(raw: Option<&str>) -> ApiResult<NodeStatus> {
	match raw {
		Some("ready") => Ok(NodeStatus::Ready),
		Some("draining") => Ok(NodeStatus::Draining),
		Some("unhealthy") => Ok(NodeStatus::Unhealthy),
		Some(other @ ("connecting" | "standby" | "shutting_down")) => {
			Err(ApiError::bad_request(format!("node status '{other}' cannot be set by an operator")))
		},
		Some(other) => Err(ApiError::bad_request(format!("invalid node status '{other}'"))),
		None => Err(ApiError::bad_request("status is required")),
	}
}

async fn set_status(
	State(state): State<AppState>,
	_admin: AdminAuth,
	Path(raw): Path<String>,
	body: Bytes,
) -> ApiResult<StatusCode> {
	let id = parse_node_id(&raw)?;
	let change: StatusChange = if body.iter().all(u8::is_ascii_whitespace) {
		StatusChange::default()
	} else {
		serde_json::from_slice(&body)
			.map_err(|e| ApiError::bad_request(format!("validation error: {e}")))?
	};
	let status = parse_status(change.status.as_deref())?;
	if !state
		.cluster
		.members()
		.iter()
		.any(|(member, meta)| member.id == id && meta.roles.node)
	{
		return Err(ApiError::not_found(format!("node '{raw}' not found")));
	}
	let kv = &state.cluster.kv;
	match status {
		NodeStatus::Ready => {
			kv.delete(&status_key(id)).await?;
		},
		other => {
			kv.put(&status_key(id), serde_json::to_vec(&other).expect("status serializes"), None)
				.await?;
		},
	}
	tracing::info!(node = %id, status = status.as_str(), at = %datetime(crate::lifecycle::now_ms()), "node status changed");
	Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
	use vmon_cluster::NodeCapacity;

	use super::*;

	#[test]
	fn status_precedence_liveness_then_admin_then_drain_flag() {
		let draining = MemberMeta {
			node: Some(NodeCapacity { draining: true, ..NodeCapacity::default() }),
			..MemberMeta::default()
		};
		let normal = MemberMeta { node: Some(NodeCapacity::default()), ..MemberMeta::default() };
		assert_eq!(effective_status(MemberState::Alive, &normal, None), "ready");
		assert_eq!(effective_status(MemberState::Alive, &draining, None), "draining");
		assert_eq!(
			effective_status(MemberState::Alive, &draining, Some(NodeStatus::Unhealthy)),
			"unhealthy"
		);
		assert_eq!(
			effective_status(MemberState::Dead, &normal, Some(NodeStatus::Ready)),
			"unhealthy"
		);
		assert_eq!(effective_status(MemberState::Suspect, &normal, None), "connecting");
		assert_eq!(effective_status(MemberState::Left, &normal, None), "shutting_down");
	}

	#[test]
	fn only_enforced_statuses_can_be_set() {
		assert_eq!(parse_status(Some("draining")).unwrap(), NodeStatus::Draining);
		assert!(parse_status(Some("standby")).is_err());
		assert!(parse_status(Some("bogus")).is_err());
		assert!(parse_status(None).is_err());
	}

	#[test]
	fn status_is_stored_as_json_string() {
		assert_eq!(serde_json::to_vec(&NodeStatus::Draining).unwrap(), b"\"draining\"");
	}
}
