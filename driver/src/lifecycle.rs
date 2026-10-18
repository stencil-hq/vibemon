//! Driver-side sandbox lifecycle (ARCHITECTURE.md §9.4).
//!
//! Create (admission, placement, node create), kill, pause, resume/connect,
//! timeouts, network updates, snapshots, and forks. Every transition of an
//! existing sandbox runs under `lock/sbx/<id>`.
//!
//! Live sandboxes have a KV route (`sbx/<id>`); paused sandboxes have a
//! store record (`state='paused'`, `node_id` = holder of the paused state) and,
//! with an object store, a portable generation at `paused/<id>/remote` that
//! lets another node resume them. The raft leader runs the reaper and
//! reconciler ([`spawn_background`]).

mod background;
pub mod limits;
pub mod network;
pub mod routes;
pub mod s3mounts;
#[cfg(test)]
mod tests;

use std::{
	collections::BTreeMap,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::http::StatusCode;
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::json;
use vmon_cluster::NodeId;
use vmon_node::{
	api::{
		ArtifactInfo, CreateSandbox, GuestInit, NetworkPolicy, PauseSandbox, ResumeSandbox,
		RouteState, SandboxInfo, SandboxRoute, SandboxState as NodeSandboxState, SnapshotSandbox,
		VolumeMount,
	},
	client::{NodeClient, NodeError},
	paused_store::{self, Remote, Store as PausedStore},
	sandbox::SandboxMeta,
};

pub use self::background::spawn_background;
use self::{
	limits::{LifecycleFlags, clamp_end, extend_end, max_length_ms, timeout_end},
	network::{Ingress, NetworkConfig, NetworkUpdate, egress_policy, required_features},
	routes::{SandboxLock, get_route, get_route_rev, put_route, remove_route},
};
use crate::{
	builds::{self, ArtifactSource, SnapshotKind, SnapshotRequest, SnapshotTarget},
	error::{ApiError, ApiResult},
	scheduler::{self, ArtifactLocation, CreateOutcome, PlacementRequest, Unplaceable},
	state::AppState,
	store::{
		NewSandboxEvent, Team, event_types,
		sandboxes::{LifecycleExtra, NewSandboxRow, ResumedExecution, SandboxRow, SandboxState},
	},
};

/// Why a sandbox was killed (`kill_reason` in `sandbox.lifecycle.killed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillReason {
	/// `DELETE /sandboxes/{id}`.
	Request,
	/// Expired without `autoPause`.
	Timeout,
	/// Admin team kill.
	Admin,
	/// The node or VM disappeared (reconciler).
	Orphaned,
}

impl KillReason {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Request => "request",
			Self::Timeout => "timeout",
			Self::Admin => "admin",
			Self::Orphaned => "orphaned",
		}
	}
}

/// What the API needs to render an E2B `Sandbox` for a started sandbox.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SandboxRef {
	pub sandbox_id:           String,
	pub template_id:          String,
	pub alias:                Option<String>,
	pub envd_version:         String,
	/// `false` ⇒ the response carries `trafficAccessToken`.
	pub allow_public_traffic: bool,
}

/// A requested volume mount (`SandboxVolumeMount`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MountRequest {
	pub name: String,
	pub path: String,
}

/// Validated create request (API defaults already applied).
#[derive(Clone, Debug)]
pub struct CreateSpec {
	pub template_ref:          String,
	/// Validated timeout in ms.
	pub timeout_ms:            u64,
	pub flags:                 LifecycleFlags,
	pub allow_internet_access: Option<bool>,
	pub network:               NetworkConfig,
	pub metadata:              BTreeMap<String, String>,
	pub env_vars:              BTreeMap<String, String>,
	pub volume_mounts:         Vec<MountRequest>,
	/// `vmonS3Mounts` (validated syntax; credentials resolved at launch).
	pub s3_mounts:             Vec<s3mounts::S3MountRequest>,
}

/// Result of `connect`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Connected {
	/// It was running; only the timeout was (possibly) extended → 200.
	Running(SandboxRef),
	/// It was paused and has been resumed → 201.
	Resumed(SandboxRef),
}

/// Everything needed to start one sandbox on a node.
struct Plan {
	team:                  Team,
	sandbox_id:            String,
	template_id:           String,
	build_id:              String,
	alias:                 Option<String>,
	artifact:              ArtifactInfo,
	sources:               Vec<ArtifactSource>,
	pinned:                Option<NodeId>,
	timeout_ms:            u64,
	env_vars:              BTreeMap<String, String>,
	metadata:              BTreeMap<String, String>,
	flags:                 LifecycleFlags,
	auto_resume_timeout_s: u64,
	allow_internet_access: Option<bool>,
	network:               NetworkConfig,
	policy:                NetworkPolicy,
	ingress:               Ingress,
	volume_mounts:         Vec<VolumeMount>,
	s3:                    s3mounts::ResolvedS3Mounts,
	/// GPUs to pass through ([`limits::requested_gpus`]).
	gpus:                  u32,
}

/// Current unix time in ms.
pub fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Unix ms → `DATETIME(3)` value.
pub fn datetime(ms: u64) -> DateTime<Utc> {
	DateTime::from_timestamp_millis(i64::try_from(ms).unwrap_or(i64::MAX)).unwrap_or_default()
}

/// `DATETIME(3)` value → unix ms.
pub fn unix_ms(at: DateTime<Utc>) -> u64 {
	u64::try_from(at.timestamp_millis()).unwrap_or_default()
}

/// Creates a sandbox from a template reference (`POST /sandboxes`, `POST
/// /v2/sandboxes`).
pub async fn create(state: &AppState, team: &Team, spec: CreateSpec) -> ApiResult<SandboxRef> {
	let resolved = builds::resolve_template(state, team, &spec.template_ref).await?;
	let artifact = resolved.artifact;
	check_resources(team, &artifact)?;
	let gpus = limits::requested_gpus(&spec.metadata, &artifact.env)?;
	let policy = egress_policy(&spec.network.egress(spec.allow_internet_access))?;
	let ingress = spec.network.ingress()?;
	let (volume_mounts, volume_node) = resolve_mounts(state, team, &spec.volume_mounts).await?;
	s3mounts::check_budget(spec.s3_mounts.len(), &volume_mounts)?;
	let s3 = s3mounts::resolve(state, &team.id, &spec.s3_mounts).await?;
	let timeout_s = spec.timeout_ms / 1000;
	let plan = Plan {
		team: team.clone(),
		sandbox_id: limits::new_sandbox_id(),
		template_id: resolved.template_id,
		build_id: resolved.build_id,
		alias: resolved.alias,
		artifact,
		sources: resolved.sources,
		pinned: volume_node,
		timeout_ms: spec.timeout_ms,
		env_vars: spec.env_vars,
		metadata: spec.metadata,
		flags: spec.flags,
		auto_resume_timeout_s: limits::auto_resume_timeout_s(timeout_s, team.limits.max_length_hours),
		allow_internet_access: spec.allow_internet_access,
		network: spec.network,
		policy,
		ingress,
		volume_mounts,
		s3,
		gpus,
	};
	launch(state, plan).await
}

fn check_resources(team: &Team, artifact: &ArtifactInfo) -> ApiResult<()> {
	let limits = &team.limits;
	if artifact.vcpus > limits.max_vcpus {
		return Err(ApiError::bad_request(format!(
			"Template requires {} vCPUs, the team limit is {}",
			artifact.vcpus, limits.max_vcpus
		)));
	}
	if artifact.memory_mb > limits.max_memory_mb {
		return Err(ApiError::bad_request(format!(
			"Template requires {} MiB of memory, the team limit is {} MiB",
			artifact.memory_mb, limits.max_memory_mb
		)));
	}
	if artifact.disk_mb > limits.max_disk_mb {
		return Err(ApiError::bad_request(format!(
			"Template requires {} MiB of disk, the team limit is {} MiB",
			artifact.disk_mb, limits.max_disk_mb
		)));
	}
	Ok(())
}

/// Resolves `[{name, path}]` to team volumes; returns the node every volume
/// lives on (placement pin) unless volumes are shared or in the object
/// store (any node with feature `object-store` mounts those).
async fn resolve_mounts(
	state: &AppState,
	team: &Team,
	mounts: &[MountRequest],
) -> ApiResult<(Vec<VolumeMount>, Option<NodeId>)> {
	let mut out = Vec::with_capacity(mounts.len());
	let mut pin: Option<NodeId> = None;
	for mount in mounts {
		validate_mount_path(&mount.path)?;
		if out.iter().any(|m: &VolumeMount| m.path == mount.path) {
			return Err(ApiError::bad_request(format!("duplicate volume mount path {}", mount.path)));
		}
		let volume = state
			.store
			.volume_by_name(&team.id, &mount.name)
			.await?
			.ok_or_else(|| ApiError::bad_request(format!("volume '{}' not found", mount.name)))?;
		if volume.object_store && state.config.object_store.is_none() {
			return Err(ApiError::unavailable(format!(
				"volume '{}' lives in the object store, which this driver is not configured with",
				volume.name
			)));
		}
		if !state.config.shared_volumes
			&& !volume.object_store
			&& let Some(raw) = volume.node_id.as_deref()
		{
			let node: NodeId = raw
				.parse()
				.map_err(|_| ApiError::internal(format!("volume {} has a bad node id", volume.id)))?;
			if pin.is_some_and(|p| p != node) {
				return Err(ApiError::bad_request("all mounted volumes must live on the same node"));
			}
			pin = Some(node);
		}
		out.push(VolumeMount {
			volume_id:    volume.id,
			name:         volume.name,
			path:         mount.path.clone(),
			object_store: volume.object_store,
		});
	}
	Ok((out, pin))
}

/// Mount paths must be absolute, clean, and not `/`.
fn validate_mount_path(path: &str) -> ApiResult<()> {
	let clean = path.starts_with('/')
		&& path.len() > 1
		&& !path.ends_with('/')
		&& path[1..]
			.split('/')
			.all(|seg| !seg.is_empty() && seg != "." && seg != "..");
	if clean {
		Ok(())
	} else {
		Err(ApiError::bad_request(format!("volume mount path {path:?} must be absolute and clean")))
	}
}

/// Runs a lifecycle operation to completion even when the API request that
/// started it is dropped (client disconnect, SDK timeout): cancelled midway,
/// a transition would strand admission slots, routes in transitional states,
/// VMs, or hidden fork templates.
///
/// Spawns when called (not on first poll), so the returned future only holds
/// the task handle: checking that it is `Send` never walks the operation's
/// (deep) future type again.
fn detached<T: Send + 'static>(
	operation: impl Future<Output = ApiResult<T>> + Send + 'static,
) -> impl Future<Output = ApiResult<T>> + Send {
	let task = tokio::spawn(operation);
	async move {
		match task.await {
			Ok(result) => result,
			Err(error) => match error.try_into_panic() {
				Ok(panic) => std::panic::resume_unwind(panic),
				Err(error) => Err(ApiError::internal(format!("lifecycle task failed: {error}"))),
			},
		}
	}
}

/// Admits, places, and starts one sandbox; records it in the store.
async fn launch(state: &AppState, plan: Plan) -> ApiResult<SandboxRef> {
	if plan.sources.is_empty() && !builds::published(state, &plan.build_id).await {
		return Err(
			ApiError::unavailable(
				"Failed to place sandbox: the template's build artifacts are not available on any \
				 live node",
			)
			.with_code("sandbox_no_compatible_node"),
		);
	}
	let state = state.clone();
	detached(async move {
		let lock = SandboxLock::acquire(&state, &plan.sandbox_id, Duration::ZERO).await?;
		let result = launch_locked(&state, &plan).await;
		lock.release().await;
		result
	})
	.await
}

async fn launch_locked(state: &AppState, plan: &Plan) -> ApiResult<SandboxRef> {
	let kv = &state.cluster.kv;
	let id = &plan.sandbox_id;
	let now = now_ms();
	let end =
		clamp_end(now, now + plan.timeout_ms, max_length_ms(plan.team.limits.max_length_hours));
	let mut route = SandboxRoute {
		team_id:              plan.team.id.clone(),
		node_id:              NodeId::default(),
		template_id:          plan.template_id.clone(),
		build_id:             plan.build_id.clone(),
		state:                RouteState::Running,
		started_at_ms:        now,
		end_at_ms:            end,
		auto_pause:           plan.flags.auto_pause,
		auto_resume:          plan.flags.auto_resume,
		allow_public_traffic: plan.ingress.allow_public_traffic,
		traffic_token:        (!plan.ingress.allow_public_traffic)
			.then(|| state.keys.traffic_token(id)),
		mask_request_host:    plan.ingress.mask_request_host.clone(),
		https_ports:          plan.ingress.https_ports.clone(),
		vcpus:                plan.artifact.vcpus,
		memory_mb:            plan.artifact.memory_mb,
		envd_version:         plan.artifact.envd_version.clone(),
	};
	let mut placement = PlacementRequest {
		vcpus: plan.artifact.vcpus,
		memory_mb: plan.artifact.memory_mb,
		arch: plan.artifact.arch.clone(),
		features: sandbox_features(&plan.policy, &plan.volume_mounts),
		pinned: plan.pinned,
		artifacts: artifact_locations(&plan.sources),
		gpus: plan.gpus,
		..PlacementRequest::default()
	};
	let request = CreateSandbox {
		sandbox_id:       id.clone(),
		team_id:          plan.team.id.clone(),
		template_id:      plan.template_id.clone(),
		build_id:         plan.build_id.clone(),
		artifact_sources: plan.sources.iter().map(|s| s.node_id).collect(),
		vcpus:            plan.artifact.vcpus,
		memory_mb:        plan.artifact.memory_mb,
		disk_mb:          plan.artifact.disk_mb,
		end_at_ms:        end,
		init:             guest_init(
			state,
			id,
			&plan.template_id,
			Some(&plan.artifact),
			&plan.env_vars,
		),
		network:          plan.policy.clone(),
		volume_mounts:    plan.volume_mounts.clone(),
		s3_mounts:        plan.s3.mounts.clone(),
		// Passed-through GPUs cannot fork a memory snapshot.
		cold_boot:        plan.gpus > 0,
		gpus:             plan.gpus,
	};

	let mut admitted = false;
	let mut last_error: Option<NodeError> = None;
	let mut started = None;
	for _ in 0..scheduler::MAX_ATTEMPTS {
		let statuses = scheduler::load_statuses(kv).await?;
		let candidate = match state
			.scheduler
			.place(&state.node_members(), &statuses, &placement)
		{
			Ok(candidate) => candidate,
			Err(reason) if !admitted => return Err(unplaceable(reason)),
			Err(_) => break,
		};
		route.node_id = candidate.node_id;
		if admitted {
			if let Err(error) = put_route(kv, id, &route).await {
				remove_route(kv, id, &plan.team.id).await.ok();
				return Err(error);
			}
		} else {
			routes::admit(kv, id, &route, plan.team.limits.max_concurrent_sandboxes).await?;
			admitted = true;
		}
		let client = match state.node(candidate.node_id) {
			Ok(client) => client,
			Err(error) => {
				remove_route(kv, id, &plan.team.id).await.ok();
				return Err(error);
			},
		};
		let guard = state.scheduler.begin(candidate.node_id, plan.gpus);
		match client.create_sandbox(&request).await {
			Ok(info) => {
				guard.finish(CreateOutcome::Success);
				started = Some((client, info));
				break;
			},
			Err(error) if retryable(&error) => {
				guard.finish(CreateOutcome::Failure);
				tracing::warn!(%error, node = %candidate.node_id, sandbox = %id, "sandbox create failed; trying another node");
				placement.excluded.push(candidate.node_id);
				last_error = Some(error);
			},
			Err(error) => {
				guard.finish(CreateOutcome::Failure);
				remove_route(kv, id, &plan.team.id).await.ok();
				return Err(create_failed(&error));
			},
		}
	}
	let Some((client, info)) = started else {
		if admitted {
			remove_route(kv, id, &plan.team.id).await.ok();
		}
		if let Some(error) = last_error {
			tracing::warn!(%error, sandbox = %id, "sandbox placement exhausted");
		}
		return Err(unplaceable(Unplaceable::NoCapacity));
	};
	if !plan.sources.iter().any(|s| s.node_id == route.node_id) {
		// The node fetched the build from a peer: record it as a holder so
		// placement still finds a copy once the builder is gone. Off the
		// create path: its first manifest request hashes the files.
		let (state, node, build_id) = (state.clone(), route.node_id, plan.build_id.clone());
		tokio::spawn(async move {
			if let Err(error) = crate::builds::record_holder(&state, node, &build_id).await {
				tracing::warn!(error = %error.message, %node, build = %build_id, "recording a new artifact holder failed");
			}
		});
	}

	let recorded = async {
		if !info.envd_version.is_empty() && info.envd_version != route.envd_version {
			route.envd_version.clone_from(&info.envd_version);
			put_route(kv, id, &route).await?;
		}
		record_launch(state, plan, &route, now, end).await
	}
	.await;
	if let Err(error) = recorded {
		// Nothing durable references the sandbox: undo the start and free its
		// admission slot.
		if let Err(kill_error) = client.kill_sandbox(id).await {
			tracing::warn!(error = %kill_error, sandbox = %id, "rollback kill after a failed create");
		}
		remove_route(kv, id, &plan.team.id).await.ok();
		return Err(error);
	}
	Ok(SandboxRef {
		sandbox_id:           id.clone(),
		template_id:          plan.template_id.clone(),
		alias:                plan.alias.clone(),
		envd_version:         route.envd_version,
		allow_public_traffic: route.allow_public_traffic,
	})
}

/// Inserts the store record and `created` event of a started sandbox.
async fn record_launch(
	state: &AppState,
	plan: &Plan,
	route: &SandboxRoute,
	now: u64,
	end: u64,
) -> ApiResult<()> {
	let id = &plan.sandbox_id;
	let execution_id = uuid::Uuid::new_v4().to_string();
	let node_id = route.node_id.to_string();
	let env_enc = (!plan.env_vars.is_empty()).then(|| {
		state
			.keys
			.encrypt(&serde_json::to_vec(&plan.env_vars).expect("env vars serialize"))
	});
	let redacted = plan.network.redacted();
	let row = NewSandboxRow {
		id,
		team_id: &plan.team.id,
		template_id: &plan.template_id,
		build_id: &plan.build_id,
		alias: plan.alias.as_deref(),
		node_id: &node_id,
		execution_id: &execution_id,
		vcpus: route.vcpus,
		memory_mb: route.memory_mb,
		disk_mb: plan.artifact.disk_mb,
		metadata: &plan.metadata,
		env_enc,
		auto_pause: plan.flags.auto_pause,
		auto_pause_memory: plan.flags.auto_pause_memory,
		auto_resume: plan.flags.auto_resume,
		allow_internet_access: plan.allow_internet_access,
		network: &redacted,
		egress_enc: encrypt_policy(state, &plan.policy),
		volume_mounts: &plan.volume_mounts,
		lifecycle: LifecycleExtra { auto_resume_timeout_s: plan.auto_resume_timeout_s },
		envd_version: &route.envd_version,
		started_at: datetime(now),
		end_at: datetime(end),
		s3_mounts: &plan.s3.records,
		s3_mounts_enc: plan.s3.enc.clone(),
		gpus: plan.gpus,
	};
	let subject = EventSubject {
		team_id:       &plan.team.id,
		sandbox_id:    id,
		execution_id:  &execution_id,
		template_id:   &plan.template_id,
		build_id:      &plan.build_id,
		metadata:      &plan.metadata,
		started_at_ms: now,
		vcpus:         route.vcpus,
		memory_mb:     route.memory_mb,
	};
	let created = subject.event(event_types::CREATED, None);
	state.store.insert_sandbox(&row, &created).await?;
	Ok(())
}

fn retryable(error: &NodeError) -> bool {
	matches!(error, NodeError::Unreachable(_)) || error.code() == "busy"
}

fn unplaceable(reason: Unplaceable) -> ApiError {
	match reason {
		Unplaceable::NoCompatibleNode => ApiError::unavailable(
			"Failed to place sandbox: no compatible node is available for the requested sandbox",
		)
		.with_code("sandbox_no_compatible_node"),
		Unplaceable::NoCapacity => ApiError::unavailable(
			"Failed to place sandbox: not enough capacity for the requested resources right now, \
			 please retry shortly",
		)
		.with_code("sandbox_capacity_unavailable"),
	}
}

fn create_failed(error: &NodeError) -> ApiError {
	let message = match error {
		NodeError::Api { message, .. } => message.clone(),
		NodeError::Unreachable(message) => message.clone(),
	};
	ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to place sandbox: {message}"))
		.with_code("sandbox_create_failed")
}

/// envd `/init` payload: template defaults overlaid with request env vars.
fn guest_init(
	state: &AppState,
	sandbox_id: &str,
	template_id: &str,
	artifact: Option<&ArtifactInfo>,
	env_vars: &BTreeMap<String, String>,
) -> GuestInit {
	let mut env = artifact.map(|a| a.env.clone()).unwrap_or_default();
	env.extend(env_vars.iter().map(|(k, v)| (k.clone(), v.clone())));
	GuestInit {
		env_vars:          env,
		envd_access_token: state.keys.envd_token(sandbox_id),
		default_user:      artifact
			.map(|a| a.default_user.clone())
			.filter(|u| !u.is_empty()),
		default_workdir:   artifact.and_then(|a| a.default_workdir.clone()),
		sandbox_id:        sandbox_id.to_owned(),
		template_id:       template_id.to_owned(),
		ca_bundle:         None,
	}
}

fn encrypt_policy(state: &AppState, policy: &NetworkPolicy) -> Vec<u8> {
	state
		.keys
		.encrypt(&serde_json::to_vec(policy).expect("network policy serializes"))
}

fn decrypt_policy(state: &AppState, row: &SandboxRow) -> ApiResult<NetworkPolicy> {
	let plain = state
		.keys
		.decrypt(&row.egress_enc)
		.map_err(|e| ApiError::internal(format!("egress policy: {e}")))?;
	serde_json::from_slice(&plain).map_err(|e| ApiError::internal(format!("egress policy: {e}")))
}

fn decrypt_env(state: &AppState, row: &SandboxRow) -> ApiResult<BTreeMap<String, String>> {
	let Some(enc) = row.env_enc.as_deref() else {
		return Ok(BTreeMap::new());
	};
	let plain = state
		.keys
		.decrypt(enc)
		.map_err(|e| ApiError::internal(format!("env vars: {e}")))?;
	serde_json::from_slice(&plain).map_err(|e| ApiError::internal(format!("env vars: {e}")))
}

/// Any recorded artifact of a build (template defaults for resumes).
async fn artifact_info(state: &AppState, build_id: &str) -> Option<ArtifactInfo> {
	let entries = state
		.cluster
		.kv
		.list(&format!("art/{build_id}/"))
		.await
		.ok()?;
	entries
		.iter()
		.find_map(|e| serde_json::from_slice(&e.value).ok())
}

/// Node features a sandbox needs: its egress policy's, plus S3 mounts for
/// volumes in the object store.
fn sandbox_features(policy: &NetworkPolicy, volumes: &[VolumeMount]) -> Vec<&'static str> {
	let mut features = required_features(policy);
	if volumes.iter().any(|volume| volume.object_store) {
		features.push(vmon_node::s3fs::FEATURE);
	}
	features
}

/// Placement view of a build's holders (artifact score bonus).
fn artifact_locations(sources: &[ArtifactSource]) -> Vec<ArtifactLocation> {
	sources
		.iter()
		.map(|s| ArtifactLocation {
			node_id:      s.node_id,
			backend:      s.info.backend.clone(),
			cpu_baseline: s.info.cpu_baseline.clone(),
			has_snapshot: s.info.has_snapshot,
		})
		.collect()
}

/// The node every mounted volume on node-local storage lives on (a hard
/// placement pin), unless volumes are shared.
async fn volume_pin(
	state: &AppState,
	team_id: &str,
	mounts: &[VolumeMount],
) -> ApiResult<Option<NodeId>> {
	let mut pin = None;
	if state.config.shared_volumes {
		return Ok(pin);
	}
	for mount in mounts {
		let volume = state
			.store
			.volume(team_id, &mount.volume_id)
			.await?
			.ok_or_else(|| ApiError::bad_request(format!("volume '{}' not found", mount.name)))?;
		if let Some(raw) = volume.node_id.as_deref() {
			let node = parse_node(raw)?;
			if pin.is_some_and(|pinned| pinned != node) {
				return Err(ApiError::bad_request("all mounted volumes must live on the same node"));
			}
			pin = Some(node);
		}
	}
	Ok(pin)
}

/// Request resuming `row`'s paused state on the node holding it, until
/// `end_at_ms` (no portable generation, metadata, or artifact sources). A
/// GPU sandbox always cold-boots and re-reserves its GPUs.
async fn resume_request(
	state: &AppState,
	row: &SandboxRow,
	policy: &NetworkPolicy,
	end_at_ms: u64,
	cold_boot: bool,
) -> ApiResult<ResumeSandbox> {
	Ok(ResumeSandbox {
		end_at_ms,
		init: guest_init(
			state,
			&row.id,
			&row.template_id,
			artifact_info(state, &row.build_id).await.as_ref(),
			&decrypt_env(state, row)?,
		),
		network: policy.clone(),
		volume_mounts: row.volume_mounts.clone(),
		s3_mounts: s3mounts::restore(state, row).await?,
		cold_boot: cold_boot || row.gpus > 0,
		remote: None,
		meta: None,
		artifact_sources: Vec::new(),
		gpus: row.gpus,
	})
}

/// Authoritative node metadata of `row`'s paused state (until `end_at_ms`),
/// for a node that does not hold it yet.
fn paused_meta(
	state: &AppState,
	row: &SandboxRow,
	network: NetworkPolicy,
	end_at_ms: u64,
) -> SandboxMeta {
	SandboxMeta {
		sandbox_id: row.id.clone(),
		team_id: row.team_id.clone(),
		template_id: row.template_id.clone(),
		build_id: row.build_id.clone(),
		state: NodeSandboxState::Paused,
		vcpus: row.vcpus,
		memory_mb: row.memory_mb,
		disk_mb: row.disk_mb,
		started_at_ms: unix_ms(row.started_at),
		end_at_ms,
		envd_version: row.envd_version.clone(),
		envd_access_token: state.keys.envd_token(&row.id),
		network,
		volume_mounts: row.volume_mounts.clone(),
		pid: None,
		tap: None,
		ended_at_ms: None,
		gpus: Vec::new(),
	}
}

fn parse_node(raw: &str) -> ApiResult<NodeId> {
	raw.parse()
		.map_err(|_| ApiError::internal(format!("bad node id {raw:?}")))
}

/// Live route and durable row of a sandbox, filtered by team ownership.
struct Current {
	route:     Option<SandboxRoute>,
	/// KV revision the route was read at (0 without a route).
	route_rev: u64,
	row:       Option<SandboxRow>,
}

async fn current(state: &AppState, team_id: Option<&str>, id: &str) -> ApiResult<Current> {
	let (route, route_rev) = match get_route_rev(&state.cluster.kv, id).await? {
		Some((route, rev)) => (Some(route), rev),
		None => (None, 0),
	};
	let row = state.store.sandbox(id).await?;
	let owner = route
		.as_ref()
		.map(|r| r.team_id.as_str())
		.or_else(|| row.as_ref().map(|r| r.team_id.as_str()));
	match (team_id, owner) {
		(_, None) => Err(ApiError::sandbox_not_found(id)),
		(Some(team), Some(owner)) if team != owner => Err(ApiError::sandbox_not_found(id)),
		_ => Ok(Current { route, route_rev, row }),
	}
}

const fn route_state_name(state: RouteState) -> &'static str {
	match state {
		RouteState::Running => "running",
		RouteState::Pausing => "pausing",
		RouteState::Resuming => "resuming",
		RouteState::Killing => "killing",
		RouteState::Snapshotting => "snapshotting",
	}
}

fn transition_conflict(id: &str, state: RouteState) -> ApiError {
	let message = match state {
		RouteState::Killing => format!("Sandbox \"{id}\" is being killed"),
		RouteState::Pausing => format!("Sandbox \"{id}\" is being paused"),
		RouteState::Snapshotting => format!("Sandbox \"{id}\" is being snapshotted"),
		other => format!("Sandbox \"{id}\" is in {} state", route_state_name(other)),
	};
	ApiError::conflict(message)
}

/// Kills a running or paused sandbox (and deletes its paused state).
/// `team_id = None` skips the ownership check (admin). 404 when neither exists.
///
/// A running sandbox no transition holds leaves the live catalog in one
/// compare-and-delete of its route ([`routes::claim_route`]), without the
/// sandbox lock; otherwise the kill waits for the lock.
pub async fn kill(
	state: &AppState,
	team_id: Option<&str>,
	id: &str,
	reason: KillReason,
) -> ApiResult<()> {
	let (state, team_id, id) = (state.clone(), team_id.map(str::to_owned), id.to_owned());
	detached(async move {
		let current = current(&state, team_id.as_deref(), &id).await?;
		if let Some(route) = &current.route
			&& routes::claim_route(&state.cluster.kv, &id, &route.team_id, current.route_rev).await?
		{
			return finish_kill(&state, &id, route.node_id, current.row.as_ref(), reason).await;
		}
		let lock = SandboxLock::acquire(&state, &id, routes::LOCK_WAIT).await?;
		let result = async {
			let current = self::current(&state, team_id.as_deref(), &id).await?;
			kill_locked(&state, &id, current, reason).await
		}
		.await;
		lock.release().await;
		result
	})
	.await
}

async fn kill_locked(
	state: &AppState,
	id: &str,
	current: Current,
	reason: KillReason,
) -> ApiResult<()> {
	let node = match (&current.route, &current.row) {
		(Some(route), _) => {
			remove_route(&state.cluster.kv, id, &route.team_id).await?;
			route.node_id
		},
		(None, Some(row)) if row.state() == SandboxState::Paused => parse_node(&row.node_id)?,
		_ => return Err(ApiError::sandbox_not_found(id)),
	};
	finish_kill(state, id, node, current.row.as_ref(), reason).await
}

/// Rest of a kill once the sandbox has left the live catalog (or was
/// paused): stop it on its node, then record it killed. A crash in between
/// is repaired by the reconciler (it kills VMs without a route and marks
/// route-less running rows killed).
async fn finish_kill(
	state: &AppState,
	id: &str,
	node: NodeId,
	row: Option<&SandboxRow>,
	reason: KillReason,
) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let portable = portable_cluster(state)
		|| (row.is_some_and(|row| row.state() == SandboxState::Paused)
			&& kv.get_consistent(&paused_store::key(id)).await?.is_some());
	if portable {
		// Fence publication first: a partitioned former holder that finishes
		// its upload late must not publish a generation of a killed sandbox.
		kv.put(&paused_store::deleted_key(id), b"true".to_vec(), Some(DELETED_FENCE_TTL))
			.await?;
	}
	if let Ok(client) = state.node(node) {
		match client.kill_sandbox(id).await {
			Ok(()) => {},
			Err(error) if error.is_not_found() => {},
			Err(error) => {
				tracing::warn!(%error, sandbox = id, "node kill failed; the reconciler retries orphans");
			},
		}
	} else {
		tracing::warn!(sandbox = id, node = %node, "node unavailable during kill; its state is reclaimed later");
	}
	if portable {
		// Independent of the holder's liveness: the objects outlive it.
		delete_paused_remote(state, id).await?;
		kv.delete(&paused_store::owner_key(id)).await?;
	}
	if let Some(row) = row {
		let killed = EventSubject::from_row(row).event(event_types::KILLED, Some(reason));
		state
			.store
			.mark_sandbox_killed(id, Utc::now(), &killed)
			.await?;
	}
	Ok(())
}

/// How long the kill fence `paused/<id>/deleted` outlives a kill: far longer
/// than any upload of the former holder can take.
const DELETED_FENCE_TTL: Duration = Duration::from_hours(7 * 24);

/// Whether `node` stores paused state in the object store.
fn has_object_store(state: &AppState, node: NodeId) -> bool {
	state.node_members().iter().any(|(id, meta)| {
		*id == node
			&& meta.node.as_ref().is_some_and(|cap| {
				cap.features
					.iter()
					.any(|feature| feature == paused_store::FEATURE)
			})
	})
}

/// Whether paused state anywhere in the cluster may be portable.
fn portable_cluster(state: &AppState) -> bool {
	state.config.object_store.is_some()
		|| state.node_members().iter().any(|(_, meta)| {
			meta.node.as_ref().is_some_and(|cap| {
				cap.features
					.iter()
					.any(|feature| feature == paused_store::FEATURE)
			})
		})
}

/// Removes a portable paused generation, then its record (a failure keeps
/// the record so the kill can be retried). Runs under the sandbox lock.
async fn delete_paused_remote(state: &AppState, id: &str) -> ApiResult<()> {
	let key = paused_store::key(id);
	let Some(entry) = state.cluster.kv.get_consistent(&key).await? else {
		return Ok(());
	};
	let remote: Remote = serde_json::from_slice(&entry.value)
		.map_err(|error| ApiError::internal(format!("paused sandbox record: {error}")))?;
	let store = PausedStore::for_remote(state.config.object_store.as_ref(), &remote)
		.await
		.map_err(|error| ApiError::unavailable(format!("paused-state store: {error:#}")))?;
	store.delete(&remote).await.map_err(|error| {
		ApiError::unavailable(format!("deleting paused sandbox state: {error:#}"))
	})?;
	state.cluster.kv.delete(&key).await?;
	Ok(())
}

/// The published portable generation of a paused sandbox, unless the cluster
/// has no object store. 404 for a killed (fenced) sandbox.
async fn paused_remote(state: &AppState, id: &str) -> ApiResult<Option<Remote>> {
	let kv = &state.cluster.kv;
	if !portable_cluster(state) {
		return Ok(None);
	}
	if kv
		.get_consistent(&paused_store::deleted_key(id))
		.await?
		.is_some()
	{
		return Err(ApiError::sandbox_not_found(id));
	}
	kv.get_consistent(&paused_store::key(id))
		.await?
		.map(|entry| serde_json::from_slice::<Remote>(&entry.value))
		.transpose()
		.map_err(|error| ApiError::internal(format!("paused sandbox record: {error}")))
}

/// Pauses a running sandbox (`memory=false` keeps only the filesystem; a
/// sandbox holding GPUs always pauses filesystem-only,
/// [`limits::captures_memory`]). 409 when already paused or transitioning.
pub async fn pause(
	state: &AppState,
	team_id: Option<&str>,
	id: &str,
	memory: bool,
) -> ApiResult<()> {
	let (state, team_id, id) = (state.clone(), team_id.map(str::to_owned), id.to_owned());
	detached(async move {
		let lock = SandboxLock::acquire(&state, &id, routes::LOCK_WAIT).await?;
		let result = async {
			let current = current(&state, team_id.as_deref(), &id).await?;
			pause_locked(&state, &id, current, memory).await
		}
		.await;
		lock.release().await;
		result
	})
	.await
}

async fn pause_locked(state: &AppState, id: &str, current: Current, memory: bool) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let Some(route) = current.route else {
		return match current.row {
			Some(row) if row.state() == SandboxState::Paused => Err(ApiError::conflict(format!(
				"Error pausing sandbox - sandbox '{id}' is already paused"
			))),
			_ => Err(ApiError::sandbox_not_found(id)),
		};
	};
	if route.state != RouteState::Running {
		return Err(ApiError::conflict(format!(
			"Sandbox '{id}' cannot be paused while in '{}' state",
			route_state_name(route.state)
		)));
	}
	let client = state.node(route.node_id)?;
	let memory = limits::captures_memory(memory, current.row.as_ref().map_or(0, |row| row.gpus));
	if has_object_store(state, route.node_id) {
		// Only the current holder may publish this pause's generation; a
		// restarted former holder is fenced off.
		let owner = serde_json::to_vec(&route.node_id)
			.map_err(|error| ApiError::internal(format!("paused sandbox owner: {error}")))?;
		kv.put(&paused_store::owner_key(id), owner, None).await?;
	}
	let mut pausing = route.clone();
	pausing.state = RouteState::Pausing;
	put_route(kv, id, &pausing).await?;
	if let Err(error) = client.pause_sandbox(id, &PauseSandbox { memory }).await {
		put_route(kv, id, &route).await?;
		if error.code() == "busy" {
			return Err(ApiError::unavailable(format!(
				"Sandbox '{id}' cannot be paused right now because its node is busy, please retry"
			)));
		}
		return Err(error.into());
	}
	// Durable state first: readers see `pausing` (reported as paused) until the
	// route goes.
	if let Some(row) = &current.row {
		let paused = EventSubject::from_row(row).event(event_types::PAUSED, None);
		state
			.store
			.mark_sandbox_paused(id, Utc::now(), &paused)
			.await?;
	}
	remove_route(kv, id, &route.team_id).await?;
	Ok(())
}

/// How a resume was requested.
#[derive(Clone, Copy, Debug)]
enum ResumeMode {
	/// `POST /v2/sandboxes/{id}/connect`: running → extend only.
	Connect { timeout_ms: u64, cold_boot: bool },
	/// Deprecated `POST /sandboxes/{id}/resume`: running → 409.
	Resume { timeout_ms: u64, cold_boot: bool },
	/// Ingress traffic for a paused sandbox with auto-resume.
	Ingress,
}

enum Resumed {
	AlreadyRunning(SandboxRoute),
	Resumed(SandboxRoute),
}

/// `POST /v2/sandboxes/{id}/connect` (and v1): 200 when running (extend
/// only), 201 after resuming a paused sandbox, 404 otherwise.
pub async fn connect(
	state: &AppState,
	team: &Team,
	id: &str,
	timeout_ms: u64,
	cold_boot: bool,
) -> ApiResult<Connected> {
	let (outcome, alias) =
		resume_with(state, Some(team), id, ResumeMode::Connect { timeout_ms, cold_boot }).await?;
	Ok(match outcome {
		Resumed::AlreadyRunning(route) => Connected::Running(sandbox_ref(id, &route, alias)),
		Resumed::Resumed(route) => Connected::Resumed(sandbox_ref(id, &route, alias)),
	})
}

/// Deprecated `POST /sandboxes/{id}/resume`: 409 when already running.
pub async fn resume(
	state: &AppState,
	team: &Team,
	id: &str,
	timeout_ms: u64,
	cold_boot: bool,
) -> ApiResult<SandboxRef> {
	let (outcome, alias) =
		resume_with(state, Some(team), id, ResumeMode::Resume { timeout_ms, cold_boot }).await?;
	match outcome {
		Resumed::Resumed(route) | Resumed::AlreadyRunning(route) => {
			Ok(sandbox_ref(id, &route, alias))
		},
	}
}

/// Resumes a paused sandbox with `autoResume` for the ingress proxy.
///
/// Returns the running route (unchanged when already running). 404 when
/// unknown, killed, or paused without auto-resume; 409 while another
/// transition is stuck; 429 at the team limit.
pub async fn resume_for_ingress(state: &AppState, sandbox_id: &str) -> ApiResult<SandboxRoute> {
	if let Some(route) = get_route(&state.cluster.kv, sandbox_id).await?
		&& route.state == RouteState::Running
	{
		return Ok(route);
	}
	let (outcome, _) = resume_with(state, None, sandbox_id, ResumeMode::Ingress).await?;
	match outcome {
		Resumed::Resumed(route) | Resumed::AlreadyRunning(route) => Ok(route),
	}
}

fn sandbox_ref(id: &str, route: &SandboxRoute, alias: Option<String>) -> SandboxRef {
	SandboxRef {
		sandbox_id: id.to_owned(),
		template_id: route.template_id.clone(),
		alias,
		envd_version: route.envd_version.clone(),
		allow_public_traffic: route.allow_public_traffic,
	}
}

async fn resume_with(
	state: &AppState,
	team: Option<&Team>,
	id: &str,
	mode: ResumeMode,
) -> ApiResult<(Resumed, Option<String>)> {
	let (state, team, id) = (state.clone(), team.cloned(), id.to_owned());
	detached(async move {
		let lock = SandboxLock::acquire(&state, &id, routes::LOCK_WAIT).await?;
		let result = resume_locked(&state, team.as_ref(), &id, mode).await;
		lock.release().await;
		result
	})
	.await
}

async fn resume_locked(
	state: &AppState,
	team: Option<&Team>,
	id: &str,
	mode: ResumeMode,
) -> ApiResult<(Resumed, Option<String>)> {
	let current = current(state, team.map(|t| t.id.as_str()), id).await?;
	let alias = current.row.as_ref().and_then(|r| r.alias.clone());
	if let Some(route) = current.route {
		return match (route.state, mode) {
			(
				RouteState::Running | RouteState::Snapshotting | RouteState::Resuming,
				ResumeMode::Connect { timeout_ms, .. },
			) => {
				let team = team.expect("connect is team-scoped");
				let extended =
					extend(state, id, route, timeout_ms, team.limits.max_length_hours).await?;
				Ok((Resumed::AlreadyRunning(extended), alias))
			},
			(
				RouteState::Running | RouteState::Snapshotting | RouteState::Resuming,
				ResumeMode::Resume { .. },
			) => Err(ApiError::conflict(format!("Sandbox {id} is already running"))),
			(
				RouteState::Running | RouteState::Snapshotting | RouteState::Resuming,
				ResumeMode::Ingress,
			) => Ok((Resumed::AlreadyRunning(route), alias)),
			(RouteState::Killing, ResumeMode::Resume { .. } | ResumeMode::Ingress) => {
				Err(ApiError::sandbox_not_found(id))
			},
			(other, _) => Err(transition_conflict(id, other)),
		};
	}
	let Some(row) = current.row.filter(|r| r.state() == SandboxState::Paused) else {
		return Err(ApiError::sandbox_not_found(id));
	};
	let owner;
	let team = if let Some(team) = team {
		team
	} else {
		owner = state
			.store
			.team_by_id(&row.team_id)
			.await?
			.ok_or_else(|| ApiError::sandbox_not_found(id))?;
		&owner
	};
	let (timeout_ms, cold_boot) = match mode {
		ResumeMode::Connect { timeout_ms, cold_boot }
		| ResumeMode::Resume { timeout_ms, cold_boot } => (timeout_ms, cold_boot),
		ResumeMode::Ingress if row.auto_resume => {
			let secs = limits::auto_resume_timeout_s(
				row.lifecycle.auto_resume_timeout_s,
				team.limits.max_length_hours,
			);
			(secs * 1000, false)
		},
		ResumeMode::Ingress => return Err(ApiError::sandbox_not_found(id)),
	};
	let route = resume_paused(state, team, &row, timeout_ms, cold_boot).await?;
	Ok((Resumed::Resumed(route), alias))
}

/// Extends (never shortens) a running sandbox's expiry.
async fn extend(
	state: &AppState,
	id: &str,
	mut route: SandboxRoute,
	ttl_ms: u64,
	max_length_hours: u32,
) -> ApiResult<SandboxRoute> {
	let end = extend_end(
		route.end_at_ms,
		now_ms(),
		route.started_at_ms,
		ttl_ms,
		max_length_ms(max_length_hours),
	);
	if end > route.end_at_ms {
		route.end_at_ms = end;
		apply_end(state, id, &route).await?;
	}
	Ok(route)
}

/// Persists a new `end_at_ms` (route, store, node safety deadline).
async fn apply_end(state: &AppState, id: &str, route: &SandboxRoute) -> ApiResult<()> {
	put_route(&state.cluster.kv, id, route).await?;
	state
		.store
		.set_sandbox_end_at(id, datetime(route.end_at_ms))
		.await?;
	match state.node(route.node_id) {
		Ok(client) => {
			if let Err(error) = client.set_deadline(id, route.end_at_ms).await {
				tracing::warn!(%error, sandbox = id, "node deadline update failed; the reconciler retries");
			}
		},
		Err(error) => {
			tracing::warn!(error = %error.message, sandbox = id, "node unavailable for deadline update");
		},
	}
	Ok(())
}

async fn resume_paused(
	state: &AppState,
	team: &Team,
	row: &SandboxRow,
	timeout_ms: u64,
	cold_boot: bool,
) -> ApiResult<SandboxRoute> {
	let kv = &state.cluster.kv;
	let id = row.id.as_str();
	let owner = parse_node(&row.node_id)?;
	let policy = decrypt_policy(state, row)?;
	let remote = paused_remote(state, id).await?;
	// GPU sandboxes pause filesystem-only: there is no memory to restore.
	let cold_boot = cold_boot || row.gpus > 0;
	let (node, client, cold_boot) = if let Some(remote) = &remote {
		// Volumes on node-local storage keep pinning the sandbox to them.
		let pin = volume_pin(state, &team.id, &row.volume_mounts).await?;
		let placement = PlacementRequest {
			vcpus: row.vcpus,
			memory_mb: row.memory_mb,
			arch: remote.arch.clone(),
			features: required_features(&policy),
			pinned: pin,
			gpus: row.gpus,
			..PlacementRequest::default()
		};
		let statuses = scheduler::load_statuses(kv).await?;
		let (candidate, cold_boot) = state
			.scheduler
			.place_paused(&state.node_members(), &statuses, &placement, owner, remote, cold_boot)
			.map_err(unplaceable)?;
		(candidate.node_id, state.node(candidate.node_id)?, cold_boot)
	} else {
		// A node-local pause needs its holder (not fresh-create capacity).
		let Ok(client) = state.node(owner) else {
			return Err(ApiError::unavailable(format!(
				"Sandbox \"{id}\" cannot be resumed right now: the node holding its paused state is \
				 unavailable"
			)));
		};
		(owner, client, cold_boot)
	};
	let relocated = node != owner;
	if relocated {
		tracing::info!(sandbox = id, from = %owner, to = %node, cold_boot, "resuming a paused sandbox on another node");
	}
	let ingress = row.network.ingress()?;
	let now = now_ms();
	let end = clamp_end(now, now + timeout_ms, max_length_ms(team.limits.max_length_hours));
	let mut route = SandboxRoute {
		team_id:              row.team_id.clone(),
		node_id:              node,
		template_id:          row.template_id.clone(),
		build_id:             row.build_id.clone(),
		state:                RouteState::Resuming,
		started_at_ms:        now,
		end_at_ms:            end,
		auto_pause:           row.auto_pause,
		auto_resume:          row.auto_resume,
		allow_public_traffic: ingress.allow_public_traffic,
		traffic_token:        (!ingress.allow_public_traffic).then(|| state.keys.traffic_token(id)),
		mask_request_host:    ingress.mask_request_host,
		https_ports:          ingress.https_ports,
		vcpus:                row.vcpus,
		memory_mb:            row.memory_mb,
		envd_version:         row.envd_version.clone(),
	};
	let artifact_sources = if relocated {
		builds::artifact_sources(state, &row.build_id)
			.await?
			.into_iter()
			.map(|source| source.node_id)
			.collect()
	} else {
		Vec::new()
	};
	let request = ResumeSandbox {
		// The holder may have lost its local copy too: always offer the record.
		meta: remote
			.as_ref()
			.map(|_| paused_meta(state, row, policy.clone(), end)),
		remote,
		artifact_sources,
		..resume_request(state, row, &policy, end, cold_boot).await?
	};
	routes::admit(kv, id, &route, team.limits.max_concurrent_sandboxes).await?;
	let info = match client.resume_sandbox(id, &request).await {
		Ok(info) => info,
		Err(error) => {
			let Some(info) = resumed_anyway(&client, id, &error).await else {
				remove_route(kv, id, &row.team_id).await.ok();
				if error.is_not_found() {
					// The node lost the paused state (e.g. a host crash before it
					// was durable): the sandbox is gone for good.
					let killed = EventSubject::from_row(row)
						.event(event_types::KILLED, Some(KillReason::Orphaned));
					state
						.store
						.mark_sandbox_killed(id, Utc::now(), &killed)
						.await?;
					return Err(ApiError::sandbox_not_found(id));
				}
				return Err(error.into());
			};
			tracing::warn!(%error, sandbox = id, "resume reported a failure but the node runs the sandbox");
			info
		},
	};
	if relocated && request.remote.is_some() {
		// The next pause's generation belongs to the new holder.
		let owner = serde_json::to_vec(&node)
			.map_err(|error| ApiError::internal(format!("paused sandbox owner: {error}")))?;
		kv.put(&paused_store::owner_key(id), owner, None).await?;
	}
	route.state = RouteState::Running;
	if !info.envd_version.is_empty() {
		route.envd_version = info.envd_version;
	}
	put_route(kv, id, &route).await?;
	let execution_id = uuid::Uuid::new_v4().to_string();
	let mut subject = EventSubject::from_row(row);
	subject.execution_id = &execution_id;
	subject.started_at_ms = now;
	let node_id = node.to_string();
	let resumed = ResumedExecution {
		id,
		node_id: &node_id,
		execution_id: &execution_id,
		started_at: datetime(now),
		end_at: datetime(end),
	};
	state
		.store
		.mark_sandbox_resumed(&resumed, &subject.event(event_types::RESUMED, None))
		.await?;
	Ok(route)
}

/// After a failed resume call, whether the node runs the sandbox anyway
/// (the call timed out or broke after the node resumed it). Dropping the
/// route then would make the reconciler kill the VM and, with it, the paused
/// state. Definite refusals (4xx) are not re-checked.
async fn resumed_anyway(client: &NodeClient, id: &str, error: &NodeError) -> Option<SandboxInfo> {
	if matches!(error, NodeError::Api { status: 400..=499, .. }) {
		return None;
	}
	match client.get_sandbox(id).await {
		Ok(info) if info.state == NodeSandboxState::Running => Some(info),
		Ok(_) => None,
		Err(check) => {
			tracing::warn!(error = %check, sandbox = id, "cannot tell whether a failed resume ran");
			None
		},
	}
}

/// Locks a running sandbox of `team` for a configuration change.
async fn running_locked(
	state: &AppState,
	team: &Team,
	id: &str,
) -> ApiResult<(SandboxLock, SandboxRoute, Option<SandboxRow>)> {
	let lock = SandboxLock::acquire(state, id, routes::LOCK_WAIT).await?;
	let current = match current(state, Some(&team.id), id).await {
		Ok(current) => current,
		Err(error) => {
			lock.release().await;
			return Err(error);
		},
	};
	match current.route {
		Some(route) if route.state == RouteState::Running => Ok((lock, route, current.row)),
		Some(route) => {
			lock.release().await;
			Err(transition_conflict(id, route.state))
		},
		None => {
			lock.release().await;
			match current.row {
				Some(row) if row.state() == SandboxState::Paused => {
					Err(ApiError::conflict(format!("Sandbox \"{id}\" is in paused state")))
				},
				_ => Err(ApiError::sandbox_not_found(id)),
			}
		},
	}
}

/// `POST /sandboxes/{id}/timeout`: sets `endAt = now + timeout` (may shorten).
/// Paused or unknown sandboxes are 404 (they are not in the live catalog).
pub async fn set_timeout(state: &AppState, team: &Team, id: &str, timeout_s: i64) -> ApiResult<()> {
	let route = get_route(&state.cluster.kv, id).await?;
	if route.as_ref().is_none_or(|r| r.team_id != team.id) {
		return Err(ApiError::sandbox_not_found(id));
	}
	let (lock, mut route, row) = running_locked(state, team, id).await?;
	let result = async {
		route.end_at_ms = timeout_end(
			now_ms(),
			route.started_at_ms,
			timeout_s,
			max_length_ms(team.limits.max_length_hours),
		)?;
		apply_end(state, id, &route).await?;
		if let Some(row) = &row {
			record_event(state, &EventSubject::from_row(row), event_types::UPDATED).await;
		}
		Ok(())
	}
	.await;
	lock.release().await;
	result
}

/// `POST /sandboxes/{id}/refreshes`: extends by `duration_ms` (never shortens).
pub async fn refresh(state: &AppState, team: &Team, id: &str, duration_ms: u64) -> ApiResult<()> {
	let (lock, route, _) = running_locked(state, team, id).await?;
	let result = extend(state, id, route, duration_ms, team.limits.max_length_hours)
		.await
		.map(drop);
	lock.release().await;
	result
}

/// `PUT /sandboxes/{id}/network`: replaces the egress policy of a running
/// sandbox.
pub async fn update_network(
	state: &AppState,
	team: &Team,
	id: &str,
	update: &NetworkUpdate,
) -> ApiResult<()> {
	let policy = egress_policy(&update.egress())?;
	let (lock, route, row) = running_locked(state, team, id).await?;
	let result = async {
		let row = row.ok_or_else(|| ApiError::sandbox_not_found(id))?;
		let features = required_features(&policy);
		let node_features = state
			.node_members()
			.into_iter()
			.find(|(node, _)| *node == route.node_id)
			.and_then(|(_, meta)| meta.node.map(|cap| cap.features))
			.unwrap_or_default();
		if let Some(missing) = features
			.iter()
			.find(|f| !node_features.iter().any(|have| have == *f))
		{
			return Err(ApiError::bad_request(format!(
				"the sandbox's node does not support this network configuration (missing feature \
				 {missing})"
			)));
		}
		state
			.node(route.node_id)?
			.update_network(id, &policy)
			.await?;
		let network = row.network.with_update(update).redacted();
		state
			.store
			.set_sandbox_network(
				id,
				&network,
				&encrypt_policy(state, &policy),
				update.allow_internet_access,
			)
			.await?;
		record_event(state, &EventSubject::from_row(&row), event_types::UPDATED).await;
		Ok(())
	}
	.await;
	lock.release().await;
	result
}

/// `POST /sandboxes/{id}/snapshots`: captures a running sandbox as a
/// (named) snapshot template; the source keeps running. A sandbox holding
/// GPUs is captured filesystem-only.
pub async fn snapshot(
	state: &AppState,
	team: &Team,
	id: &str,
	name: Option<&str>,
	memory: bool,
) -> ApiResult<SnapshotTarget> {
	let (state, team, id, name) =
		(state.clone(), team.clone(), id.to_owned(), name.map(str::to_owned));
	detached(async move {
		let (lock, route, row) = running_locked(&state, &team, &id).await?;
		let result = async {
			let row = row.ok_or_else(|| ApiError::sandbox_not_found(&id))?;
			let kind = SnapshotKind::Named;
			let (target, _) =
				snapshot_locked(&state, &team, &route, &row, name.as_deref(), kind, memory).await?;
			state
				.store
				.set_sandbox_snapshot_build(&id, &target.build_id)
				.await?;
			record_event(&state, &EventSubject::from_row(&row), event_types::CHECKPOINTED).await;
			Ok(target)
		}
		.await;
		lock.release().await;
		result
	})
	.await
}

async fn snapshot_locked(
	state: &AppState,
	team: &Team,
	route: &SandboxRoute,
	row: &SandboxRow,
	name: Option<&str>,
	kind: SnapshotKind,
	memory: bool,
) -> ApiResult<(SnapshotTarget, ArtifactInfo)> {
	let id = row.id.as_str();
	let client = state.node(route.node_id)?;
	let memory = limits::captures_memory(memory, row.gpus);
	let request = SnapshotRequest {
		source_sandbox_id: id,
		node_id: route.node_id,
		name,
		kind,
		vcpus: row.vcpus,
		memory_mb: row.memory_mb,
		disk_mb: row.disk_mb,
		envd_version: &route.envd_version,
	};
	let target = builds::begin_snapshot(state, team, request).await?;
	match capture(state, &client, id, route, &target, memory).await {
		Ok(artifact) => Ok((target, artifact)),
		Err(error) => {
			builds::fail_snapshot(state, &target, &error.message).await;
			if kind == SnapshotKind::Fork {
				builds::retire_fork(state, &target).await;
			}
			Err(error)
		},
	}
}

/// Snapshots the running sandbox `id` on its node into `target` and
/// completes the build; the route reads `snapshotting` meanwhile.
async fn capture(
	state: &AppState,
	client: &NodeClient,
	id: &str,
	route: &SandboxRoute,
	target: &SnapshotTarget,
	memory: bool,
) -> ApiResult<ArtifactInfo> {
	let kv = &state.cluster.kv;
	let mut snapshotting = route.clone();
	snapshotting.state = RouteState::Snapshotting;
	put_route(kv, id, &snapshotting).await?;
	let captured = client
		.snapshot_sandbox(id, &SnapshotSandbox {
			build_id: target.build_id.clone(),
			template_id: target.template_id.clone(),
			memory,
		})
		.await;
	put_route(kv, id, route).await?;
	let artifact = captured.map_err(|error| {
		if error.code() == "busy" {
			ApiError::unavailable(format!(
				"Sandbox '{id}' cannot be snapshotted right now because its node is busy, please retry"
			))
		} else {
			error.into()
		}
	})?;
	builds::finish_snapshot(state, target, &artifact).await?;
	Ok(artifact)
}

/// `POST /sandboxes/{id}/fork`: snapshots the source once, then starts `count`
/// children.
///
/// The snapshot goes into a hidden build and the children start on the same
/// node; each succeeds or fails independently. Afterwards the hidden template
/// is retired: its build's artifacts stay while any running or paused sandbox
/// uses them, then artifact GC reclaims them.
pub async fn fork(
	state: &AppState,
	team: &Team,
	id: &str,
	timeout_ms: u64,
	count: u32,
) -> ApiResult<Vec<ApiResult<SandboxRef>>> {
	let (state, team, id) = (state.clone(), team.clone(), id.to_owned());
	detached(async move {
		let (lock, route, row) = running_locked(&state, &team, &id).await?;
		let captured = async {
			let row = row.ok_or_else(|| ApiError::sandbox_not_found(&id))?;
			let (target, artifact) =
				snapshot_locked(&state, &team, &route, &row, None, SnapshotKind::Fork, true).await?;
			Ok::<_, ApiError>(ForkSource { row, target, artifact })
		}
		.await;
		lock.release().await;
		let fork = captured?;
		let children = launch_forks(&state, &team, &route, &fork, timeout_ms, count).await;
		builds::retire_fork(&state, &fork.target).await;
		children
	})
	.await
}

/// The captured source of a fork.
struct ForkSource {
	row:      SandboxRow,
	target:   SnapshotTarget,
	artifact: ArtifactInfo,
}

/// Starts `count` children of a fork on the source's node.
async fn launch_forks(
	state: &AppState,
	team: &Team,
	route: &SandboxRoute,
	fork: &ForkSource,
	timeout_ms: u64,
	count: u32,
) -> ApiResult<Vec<ApiResult<SandboxRef>>> {
	let ForkSource { row, target, artifact } = fork;
	let source = state
		.node_members()
		.into_iter()
		.find(|(node, _)| *node == route.node_id)
		.map(|(node, _)| ArtifactSource { node_id: node, info: artifact.clone() })
		.ok_or_else(|| ApiError::unavailable("the source sandbox's node left the cluster"))?;
	let env_vars = decrypt_env(state, row)?;
	let policy = decrypt_policy(state, row)?;
	let ingress = row.network.ingress()?;
	let flags = LifecycleFlags {
		auto_pause:        row.auto_pause,
		auto_pause_memory: row.auto_pause_memory,
		auto_resume:       row.auto_resume,
	};
	// Children re-mount the source's buckets (same device tags, so they
	// fork its memory snapshot).
	let s3 = s3mounts::ResolvedS3Mounts {
		mounts:  s3mounts::restore(state, row).await?,
		records: row.s3_mounts.clone().unwrap_or_default(),
		enc:     row.s3_mounts_enc.clone(),
	};
	let children = (0..count).map(|_| {
		let plan = Plan {
			team: team.clone(),
			sandbox_id: limits::new_sandbox_id(),
			template_id: row.template_id.clone(),
			build_id: target.build_id.clone(),
			alias: row.alias.clone(),
			artifact: artifact.clone(),
			sources: vec![source.clone()],
			pinned: Some(route.node_id),
			timeout_ms,
			env_vars: env_vars.clone(),
			metadata: row.metadata.clone(),
			flags,
			auto_resume_timeout_s: row.lifecycle.auto_resume_timeout_s,
			allow_internet_access: row.allow_internet_access,
			network: row.network.clone(),
			policy: policy.clone(),
			ingress: ingress.clone(),
			volume_mounts: row.volume_mounts.clone(),
			s3: s3.clone(),
			gpus: row.gpus,
		};
		// Type-erased: keeps the fork handler's future type shallow enough
		// for axum's `Handler: Send` check.
		let child: std::pin::Pin<Box<dyn Future<Output = ApiResult<SandboxRef>> + Send + '_>> =
			Box::pin(launch(state, plan));
		child
	});
	Ok(futures::future::join_all(children).await)
}

/// Kills every running and paused sandbox of a team (admin). Returns
/// `(killed, failed)`.
pub async fn kill_team(state: &AppState, team_id: &str) -> ApiResult<(u64, u64)> {
	let mut ids: Vec<String> = state
		.store
		.team_live_sandboxes(team_id)
		.await?
		.into_iter()
		.map(|(id, _)| id)
		.collect();
	let prefix = routes::team_prefix(team_id);
	for entry in state.cluster.kv.list(&prefix).await? {
		if let Some(id) = entry.key.strip_prefix(&prefix)
			&& !ids.iter().any(|known| known == id)
		{
			ids.push(id.to_owned());
		}
	}
	// Type-erased: the kill future is deep enough to overflow `Send`
	// inference of the admin handler.
	let results = futures::future::join_all(ids.iter().map(|id| {
		let kill: std::pin::Pin<Box<dyn Future<Output = ApiResult<()>> + Send + '_>> =
			Box::pin(kill(state, Some(team_id), id, KillReason::Admin));
		kill
	}))
	.await;
	let mut killed = 0;
	let mut failed = 0;
	for (id, result) in ids.iter().zip(results) {
		match result {
			Ok(()) => killed += 1,
			Err(error) if error.status == StatusCode::NOT_FOUND => {},
			Err(error) => {
				tracing::warn!(error = %error.message, sandbox = %id, "admin kill failed");
				failed += 1;
			},
		}
	}
	Ok((killed, failed))
}

/// Reaper action for one expired route: pause when `autoPause`, else kill.
/// Skips sandboxes another operation holds or that were extended meanwhile.
pub(crate) async fn expire(state: &AppState, id: &str) -> ApiResult<()> {
	let Ok(lock) = SandboxLock::acquire(state, id, Duration::ZERO).await else {
		return Ok(());
	};
	let result = async {
		let current = current(state, None, id).await?;
		let Some(route) = current.route.as_ref() else {
			return Ok(());
		};
		if route.end_at_ms > now_ms() {
			return Ok(());
		}
		if route.state != RouteState::Running {
			// We hold the lock, so no transition is in flight: its driver died
			// midway. The sandbox has expired, so end it rather than leave its
			// route (and admission slot) behind.
			tracing::warn!(
				sandbox = id,
				state = route_state_name(route.state),
				"expiring a sandbox left mid-transition"
			);
			return kill_locked(state, id, current, KillReason::Timeout).await;
		}
		if route.auto_pause {
			let memory = current.row.as_ref().is_none_or(|row| row.auto_pause_memory);
			match pause_locked(state, id, current, memory).await {
				Ok(()) => Ok(()),
				Err(error) => {
					tracing::warn!(error = %error.message, sandbox = id, "auto-pause failed; killing");
					let current = self::current(state, None, id).await?;
					kill_locked(state, id, current, KillReason::Timeout).await
				},
			}
		} else {
			kill_locked(state, id, current, KillReason::Timeout).await
		}
	}
	.await;
	lock.release().await;
	result
}

/// Subject of a sandbox lifecycle event.
struct EventSubject<'a> {
	team_id:       &'a str,
	sandbox_id:    &'a str,
	execution_id:  &'a str,
	template_id:   &'a str,
	build_id:      &'a str,
	metadata:      &'a BTreeMap<String, String>,
	started_at_ms: u64,
	vcpus:         u32,
	memory_mb:     u64,
}

impl<'a> EventSubject<'a> {
	/// The lifecycle event for this subject (data per the E2B webhook docs).
	fn event(&self, event_type: &'static str, kill: Option<KillReason>) -> NewSandboxEvent<'a> {
		let mut data = json!({
			"sandbox_metadata": self.metadata,
			"execution": {
				"started_at": datetime(self.started_at_ms).to_rfc3339_opts(SecondsFormat::Millis, true),
				"vcpu_count": self.vcpus,
				"memory_mb": self.memory_mb,
				"execution_time": now_ms().saturating_sub(self.started_at_ms),
			},
		});
		if let Some(reason) = kill {
			data["kill_reason"] = json!(reason.as_str());
		}
		NewSandboxEvent {
			team_id: self.team_id,
			sandbox_id: self.sandbox_id,
			execution_id: self.execution_id,
			template_id: self.template_id,
			build_id: self.build_id,
			event_type,
			data,
		}
	}

	fn from_row(row: &'a SandboxRow) -> Self {
		Self {
			team_id:       &row.team_id,
			sandbox_id:    &row.id,
			execution_id:  &row.execution_id,
			template_id:   &row.template_id,
			build_id:      &row.build_id,
			metadata:      &row.metadata,
			started_at_ms: unix_ms(row.started_at),
			vcpus:         row.vcpus,
			memory_mb:     row.memory_mb,
		}
	}
}

/// Records an informational event (`updated`, `checkpointed`) that has no
/// accompanying state change; failures are logged because the change it
/// describes already happened.
async fn record_event(state: &AppState, subject: &EventSubject<'_>, event_type: &'static str) {
	if let Err(error) = state
		.store
		.record_event(&subject.event(event_type, None))
		.await
	{
		tracing::warn!(%error, sandbox = subject.sandbox_id, event_type, "recording sandbox event failed");
	}
}

/// Node client for a sandbox's node (route if live, row otherwise) — logs and
/// metrics.
pub async fn node_for(
	state: &AppState,
	team: &Team,
	id: &str,
) -> ApiResult<(NodeClient, SandboxRow)> {
	let current = current(state, Some(&team.id), id).await?;
	let row = current.row.ok_or_else(|| ApiError::sandbox_not_found(id))?;
	let node = match &current.route {
		Some(route) => route.node_id,
		None => parse_node(&row.node_id)?,
	};
	Ok((state.node(node)?, row))
}

#[cfg(test)]
mod unit_tests {
	use super::*;

	#[test]
	fn mount_paths_must_be_absolute_and_clean() {
		for ok in ["/data", "/home/user/vol"] {
			assert!(validate_mount_path(ok).is_ok(), "{ok}");
		}
		for bad in ["", "/", "data", "/data/", "/a//b", "/a/./b", "/a/../b"] {
			assert!(validate_mount_path(bad).is_err(), "{bad}");
		}
	}

	#[test]
	fn placement_errors_carry_e2b_codes() {
		let full = unplaceable(Unplaceable::NoCapacity);
		assert_eq!(
			(full.status, full.error_code),
			(StatusCode::SERVICE_UNAVAILABLE, Some("sandbox_capacity_unavailable"))
		);
		let none = unplaceable(Unplaceable::NoCompatibleNode);
		assert_eq!(none.error_code, Some("sandbox_no_compatible_node"));
		let failed = create_failed(&NodeError::Api {
			status:  400,
			code:    "invalid".into(),
			message: "bad rootfs".into(),
		});
		assert_eq!(failed.status, StatusCode::INTERNAL_SERVER_ERROR);
		assert_eq!(failed.message, "Failed to place sandbox: bad rootfs");
		assert!(retryable(&NodeError::Unreachable("x".into())));
		assert!(retryable(&NodeError::Api {
			status:  503,
			code:    "busy".into(),
			message: String::new(),
		}));
		assert!(!retryable(&NodeError::Api {
			status:  400,
			code:    "invalid".into(),
			message: String::new(),
		}));
	}
}
