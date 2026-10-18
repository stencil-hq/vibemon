//! Offline sandbox migration (ARCHITECTURE.md §9.4 "Migration").
//!
//! The source node pauses the sandbox into node-local state (`handover`), the
//! target pulls that state over the cluster listener (`AdoptSandbox`) and
//! resumes it, and one KV transaction fenced on the sandbox lock moves route
//! and record to the target; only then does the source drop its copy. A
//! failure before that commit resumes the sandbox on the source. The raft
//! leader's evacuator migrates sandboxes off draining nodes.

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
	time::{Duration, Instant},
};

use chrono::Utc;
use parking_lot::Mutex;
use vmon_cluster::NodeId;
use vmon_node::{
	api::{
		AdoptSandbox, NetworkPolicy, PauseSandbox, ResumeSandbox, RouteState, SandboxInfo,
		SandboxRoute, SandboxState as NodeSandboxState,
	},
	client::{NodeClient, NodeError},
	paused_store,
};

use super::{
	EventSubject, artifact_locations,
	background::mark_orphan_killed,
	current, decrypt_policy, detached, now_ms, parse_node, paused_meta, resume_request,
	resumed_anyway,
	routes::{
		self, SandboxLock, get_route_consistent, is_locked, list_routes, put_route, remove_route,
		route_key,
	},
	sandbox_features, transition_conflict, unix_ms, unplaceable, volume_pin,
};
use crate::{
	builds::{self, ArtifactSource},
	error::{ApiError, ApiResult},
	scheduler::{self, NodeStatus, PlacementRequest, Unplaceable},
	state::AppState,
	store::{
		event_types,
		sandboxes::{MoveOutcome, SandboxMove, SandboxRow, SandboxState},
	},
};

/// Period of the evacuator.
pub(super) const EVACUATE_INTERVAL: Duration = Duration::from_secs(5);
/// Running sandboxes with less time left expire on their draining node.
const EVACUATE_MIN_LEFT_MS: u64 = 60_000;
/// Concurrent migrations off one draining node.
const EVACUATE_PER_NODE: usize = 2;
/// How long the evacuator leaves a sandbox alone after a failed migration.
const EVACUATE_BACKOFF: Duration = Duration::from_mins(1);

/// Where a migrated sandbox lives now ([`migrate`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migrated {
	pub sandbox_id: String,
	/// Node holding the sandbox.
	pub node_id:    NodeId,
	/// `running` or `paused` (a paused sandbox stays paused).
	pub state:      SandboxState,
}

/// Moves a running or node-local paused sandbox to `target` or a placed
/// compatible node, keeping its ID, memory, and disk.
///
/// Serves `POST /admin/sandboxes/{id}/migrate` and the evacuator. A target
/// equal to the current node is a no-op.
///
/// 404 when killed or unknown; 409 while another transition runs or when the
/// paused state is already portable; 503 (`sandbox_no_compatible_node`,
/// `sandbox_capacity_unavailable`) without a target.
pub async fn migrate(state: &AppState, id: &str, target: Option<NodeId>) -> ApiResult<Migrated> {
	migrate_with(state, id, target, routes::LOCK_WAIT).await
}

/// [`migrate`] waiting up to `wait` for the sandbox lock.
async fn migrate_with(
	state: &AppState,
	id: &str,
	target: Option<NodeId>,
	wait: Duration,
) -> ApiResult<Migrated> {
	let (state, id) = (state.clone(), id.to_owned());
	detached(async move {
		let lock = SandboxLock::acquire(&state, &id, wait).await?;
		let result = migrate_locked(&state, &lock, &id, target).await;
		lock.release().await;
		result
	})
	.await
}

async fn migrate_locked(
	state: &AppState,
	lock: &SandboxLock,
	id: &str,
	target: Option<NodeId>,
) -> ApiResult<Migrated> {
	let current = current(state, None, id).await?;
	match (current.route, current.row) {
		(Some(route), Some(row)) if route.state == RouteState::Running => {
			migrate_running(state, lock, route, &row, target).await
		},
		(Some(route), _) if route.state == RouteState::Killing => {
			Err(ApiError::sandbox_not_found(id))
		},
		(Some(route), _) if route.state != RouteState::Running => {
			Err(transition_conflict(id, route.state))
		},
		(None, Some(row)) if row.state() == SandboxState::Paused => {
			migrate_paused(state, lock, &row, target).await
		},
		_ => Err(ApiError::sandbox_not_found(id)),
	}
}

/// Target of a migration off `source`: `requested` when it qualifies, else a
/// placed node. Either way it has `source`'s architecture, backend, and CPU
/// baseline (the memory snapshot must restore), the sandbox's features,
/// volume pin, and capacity. Also returns the build's holders.
async fn choose_target(
	state: &AppState,
	source: NodeId,
	row: &SandboxRow,
	policy: &NetworkPolicy,
	requested: Option<NodeId>,
) -> ApiResult<(NodeId, Vec<ArtifactSource>)> {
	if row.gpus > 0 {
		// Passed-through GPUs have no memory snapshot to hand over.
		return Err(ApiError::bad_request(format!(
			"Sandbox \"{}\" holds GPUs and cannot be migrated; pause and resume it instead",
			row.id
		)));
	}
	let members = state.node_members();
	let Some(cap) = members
		.iter()
		.find(|(id, _)| *id == source)
		.and_then(|(_, meta)| meta.node.clone())
	else {
		return Err(ApiError::unavailable(format!("node {source} is not available")));
	};
	let pin = volume_pin(state, &row.team_id, &row.volume_mounts).await?;
	if requested.is_some_and(|node| pin.is_some_and(|pinned| pinned != node)) {
		return Err(unplaceable(Unplaceable::NoCompatibleNode));
	}
	let holders = builds::artifact_sources(state, &row.build_id).await?;
	let request = PlacementRequest {
		vcpus:        row.vcpus,
		memory_mb:    row.memory_mb,
		arch:         cap.arch,
		backend:      cap.backend,
		cpu_baseline: cap.cpu_baseline,
		features:     sandbox_features(policy, &row.volume_mounts),
		pinned:       requested.or(pin),
		artifacts:    artifact_locations(&holders),
		excluded:     vec![source],
		gpus:         row.gpus,
	};
	let statuses = scheduler::load_statuses(&state.cluster.kv).await?;
	let candidate = state
		.scheduler
		.place(&members, &statuses, &request)
		.map_err(unplaceable)?;
	Ok((candidate.node_id, holders))
}

/// Migrates the running sandbox of `route` (see the module docs).
async fn migrate_running(
	state: &AppState,
	lock: &SandboxLock,
	route: SandboxRoute,
	row: &SandboxRow,
	requested: Option<NodeId>,
) -> ApiResult<Migrated> {
	let kv = &state.cluster.kv;
	let id = row.id.as_str();
	let source = route.node_id;
	if requested == Some(source) {
		return Ok(Migrated {
			sandbox_id: id.to_owned(),
			node_id:    source,
			state:      SandboxState::Running,
		});
	}
	let policy = decrypt_policy(state, row)?;
	let (target, holders) = choose_target(state, source, row, &policy, requested).await?;
	let source_client = state.node(source)?;
	let target_client = state.node(target)?;
	// Everything the target needs exists before the sandbox stops. Deadline
	// changes take the sandbox lock, so the route's stays current throughout.
	let end = route.end_at_ms;
	let adopt = adopt_request(state, row, policy.clone(), end, source, &holders);
	let resume = resume_request(state, row, &policy, end, false).await?;

	let mut migrating = route.clone();
	migrating.state = RouteState::Migrating;
	put_route(kv, id, &migrating).await?;
	// A restarted source must not publish the handover pause.
	if let Err(error) = kv.delete(&paused_store::owner_key(id)).await {
		put_route(kv, id, &route).await?;
		return Err(error.into());
	}
	tracing::info!(sandbox = id, from = %source, to = %target, "migrating sandbox");
	let pause = PauseSandbox { memory: true, handover: true };
	if let Err(error) = source_client.pause_sandbox(id, &pause).await {
		if !paused_anyway(&source_client, id).await {
			put_route(kv, id, &route).await?;
			return Err(pause_failed(id, error));
		}
		tracing::warn!(%error, sandbox = id, "handover pause reported a failure but the node paused the sandbox");
	}
	let info = match hand_over(&target_client, id, &adopt, &resume).await {
		Ok(info) => info,
		Err(error) => {
			tracing::warn!(%error, sandbox = id, from = %source, to = %target, "migration failed; resuming on the source");
			drop_copy(&target_client, id, target).await;
			resume_in_place(state, &migrating, row).await?;
			return Err(migration_failed(id, target, &error));
		},
	};

	let mut running = route;
	running.node_id = target;
	if !info.envd_version.is_empty() {
		running.envd_version = info.envd_version;
	}
	let node_id = target.to_string();
	commit_move(state, &SandboxMove {
		id,
		from: SandboxState::Running,
		node_id: &node_id,
		fence: lock.fence(),
		route: Some((route_key(id), &running)),
		delete: Vec::new(),
	})
	.await?;
	drop_copy(&source_client, id, source).await;
	tracing::info!(sandbox = id, from = %source, to = %target, "sandbox migrated");
	Ok(Migrated { sandbox_id: id.to_owned(), node_id: target, state: SandboxState::Running })
}

/// Request adopting `row`'s paused state (until `end_at_ms`) from `source`;
/// the build's holders let the target link template files.
fn adopt_request(
	state: &AppState,
	row: &SandboxRow,
	policy: NetworkPolicy,
	end_at_ms: u64,
	source: NodeId,
	holders: &[ArtifactSource],
) -> AdoptSandbox {
	AdoptSandbox {
		meta: paused_meta(state, row, policy, end_at_ms),
		source,
		artifact_sources: holders.iter().map(|holder| holder.node_id).collect(),
	}
}

/// Target side of a running migration: adopt the source's paused state, then
/// resume it.
async fn hand_over(
	target: &NodeClient,
	id: &str,
	adopt: &AdoptSandbox,
	resume: &ResumeSandbox,
) -> Result<SandboxInfo, NodeError> {
	target.adopt_sandbox(id, adopt).await?;
	match target.resume_sandbox(id, resume).await {
		Ok(info) => Ok(info),
		Err(error) => resumed_anyway(target, id, &error).await.ok_or(error),
	}
}

/// Migrates a node-local paused sandbox: the target adopts the paused state,
/// then the record names it; the sandbox stays paused.
async fn migrate_paused(
	state: &AppState,
	lock: &SandboxLock,
	row: &SandboxRow,
	requested: Option<NodeId>,
) -> ApiResult<Migrated> {
	let kv = &state.cluster.kv;
	let id = row.id.as_str();
	let source = parse_node(&row.node_id)?;
	if requested == Some(source) {
		return Ok(Migrated {
			sandbox_id: id.to_owned(),
			node_id:    source,
			state:      SandboxState::Paused,
		});
	}
	if kv
		.get_consistent(&paused_store::deleted_key(id))
		.await?
		.is_some()
	{
		return Err(ApiError::sandbox_not_found(id));
	}
	if kv.get_consistent(&paused_store::key(id)).await?.is_some() {
		return Err(ApiError::conflict(format!(
			"Sandbox \"{id}\" is paused in the object store: any compatible node can resume it"
		)));
	}
	let policy = decrypt_policy(state, row)?;
	let (target, holders) = choose_target(state, source, row, &policy, requested).await?;
	let source_client = state.node(source)?;
	let target_client = state.node(target)?;
	let adopt = adopt_request(state, row, policy, unix_ms(row.end_at), source, &holders);
	tracing::info!(sandbox = id, from = %source, to = %target, "migrating paused sandbox");
	if let Err(error) = target_client.adopt_sandbox(id, &adopt).await {
		tracing::warn!(%error, sandbox = id, from = %source, to = %target, "paused sandbox migration failed");
		drop_copy(&target_client, id, target).await;
		return Err(migration_failed(id, target, &error));
	}
	let node_id = target.to_string();
	// The old holder must not publish a pause it no longer holds.
	commit_move(state, &SandboxMove {
		id,
		from: SandboxState::Paused,
		node_id: &node_id,
		fence: lock.fence(),
		route: None,
		delete: vec![paused_store::owner_key(id)],
	})
	.await?;
	drop_copy(&source_client, id, source).await;
	tracing::info!(sandbox = id, from = %source, to = %target, "paused sandbox migrated");
	Ok(Migrated { sandbox_id: id.to_owned(), node_id: target, state: SandboxState::Paused })
}

/// Commits a migration. On failure nothing more is undone here: the commit
/// may have applied (KV error), and recovery plus the reconciler remove
/// whichever copy route and record do not name.
async fn commit_move(state: &AppState, change: &SandboxMove<'_>) -> ApiResult<()> {
	let id = change.id;
	match state.store.move_sandbox(change).await? {
		MoveOutcome::Moved => Ok(()),
		MoveOutcome::Stale => {
			Err(ApiError::conflict(format!("Sandbox \"{id}\" changed state during its migration")))
		},
		MoveOutcome::Fenced => {
			Err(ApiError::conflict(format!("Sandbox \"{id}\" lost its lock during its migration")))
		},
	}
}

/// After a failed handover pause, whether the node paused the sandbox anyway
/// (the reply was lost).
async fn paused_anyway(client: &NodeClient, id: &str) -> bool {
	matches!(client.get_sandbox(id).await, Ok(info) if info.state == NodeSandboxState::Paused)
}

fn pause_failed(id: &str, error: NodeError) -> ApiError {
	if error.code() == "busy" {
		ApiError::unavailable(format!(
			"Sandbox '{id}' cannot be migrated right now because its node is busy, please retry"
		))
	} else {
		error.into()
	}
}

fn migration_failed(id: &str, target: NodeId, error: &NodeError) -> ApiError {
	ApiError::internal(format!("Migrating sandbox \"{id}\" to node {target} failed: {error}"))
}

/// Deletes a node's paused copy (a migrated source or a rolled-back target);
/// a failure is only logged, the reconciler kills copies nothing names.
async fn drop_copy(client: &NodeClient, id: &str, node: NodeId) {
	match client.drop_paused(id).await {
		Ok(()) => {},
		Err(error) if error.is_not_found() => {},
		Err(error) => {
			tracing::warn!(%error, sandbox = id, %node, "dropping a sandbox copy failed; the reconciler removes it");
		},
	}
}

/// Resumes the sandbox on the node of `route` from the paused state it holds
/// there (a rolled-back or interrupted migration) and marks the route
/// running. When that fails it stays paused there, as after a normal pause
/// (record paused, route removed, `paused` event).
async fn resume_in_place(
	state: &AppState,
	route: &SandboxRoute,
	row: &SandboxRow,
) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let id = row.id.as_str();
	let resumed: ApiResult<SandboxInfo> = async {
		let client = state.node(route.node_id)?;
		let policy = decrypt_policy(state, row)?;
		let request = resume_request(state, row, &policy, route.end_at_ms, false).await?;
		match client.resume_sandbox(id, &request).await {
			Ok(info) => Ok(info),
			Err(error) => match resumed_anyway(&client, id, &error).await {
				Some(info) => Ok(info),
				None => Err(ApiError::from(error)),
			},
		}
	}
	.await;
	match resumed {
		Ok(info) => {
			let mut running = route.clone();
			running.state = RouteState::Running;
			if !info.envd_version.is_empty() {
				running.envd_version = info.envd_version;
			}
			put_route(kv, id, &running).await
		},
		Err(error) => {
			tracing::warn!(error = %error.message, sandbox = id, node = %route.node_id, "resuming in place failed; the sandbox stays paused there");
			let paused = EventSubject::from_row(row).event(event_types::PAUSED, None);
			state
				.store
				.mark_sandbox_paused(id, Utc::now(), &paused)
				.await?;
			remove_route(kv, id, &route.team_id).await
		},
	}
}

/// Settles an unlocked `migrating` route (its driver died midway) on the node
/// it names; skipped while another operation holds the sandbox.
pub(super) async fn recover_migration(state: &AppState, id: &str) -> ApiResult<()> {
	let Ok(lock) = SandboxLock::acquire(state, id, Duration::ZERO).await else {
		return Ok(());
	};
	// Type-erased: the resume path is deep enough to overflow `Send`
	// inference of the reconciler task.
	let recovery: std::pin::Pin<Box<dyn Future<Output = ApiResult<()>> + Send + '_>> =
		Box::pin(recover_locked(state, id));
	let result = recovery.await;
	lock.release().await;
	result
}

/// [`recover_migration`] under the sandbox lock. The route's node still runs
/// the sandbox → route `running`; holds it paused (the handover pause) →
/// resumed there, else left paused there; lost it → killed as orphaned;
/// unreachable → left for the next pass. A copy the target already holds is
/// killed by the reconciler (the route does not name it).
pub(super) async fn recover_locked(state: &AppState, id: &str) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let Some(route) = get_route_consistent(kv, id)
		.await?
		.filter(|route| route.state == RouteState::Migrating)
	else {
		return Ok(());
	};
	let Ok(client) = state.node(route.node_id) else {
		return Ok(());
	};
	match client.get_sandbox(id).await {
		Ok(info) if info.state == NodeSandboxState::Running => {
			tracing::warn!(sandbox = id, node = %route.node_id, "an interrupted migration left the sandbox running; keeping it there");
			let mut running = route;
			running.state = RouteState::Running;
			put_route(kv, id, &running).await
		},
		Ok(_) => {
			tracing::warn!(sandbox = id, node = %route.node_id, "an interrupted migration left the sandbox paused; resuming it there");
			let row =
				state.store.sandbox(id).await?.ok_or_else(|| {
					ApiError::internal(format!("migrating sandbox {id} has no record"))
				})?;
			resume_in_place(state, &route, &row).await
		},
		Err(error) if error.is_not_found() => {
			tracing::warn!(sandbox = id, node = %route.node_id, "an interrupted migration lost the sandbox; marking killed");
			remove_route(kv, id, &route.team_id).await?;
			mark_orphan_killed(state, id).await
		},
		Err(error) => {
			tracing::debug!(%error, sandbox = id, "migration recovery cannot reach the node; retrying");
			Ok(())
		},
	}
}

/// Evacuator bookkeeping kept by the leader across passes.
#[derive(Clone, Debug, Default)]
pub(super) struct Evacuations {
	inner: Arc<Mutex<EvacuationState>>,
}

#[derive(Debug, Default)]
struct EvacuationState {
	/// Migrations in flight: sandbox → draining node.
	inflight: HashMap<String, NodeId>,
	/// Sandboxes whose last migration failed, skipped until the instant.
	backoff:  HashMap<String, Instant>,
}

impl Evacuations {
	/// Whether a migration of `id` off `node` may start now.
	fn may_start(&self, id: &str, node: NodeId) -> bool {
		let inner = self.inner.lock();
		!inner.inflight.contains_key(id)
			&& !inner.backoff.contains_key(id)
			&& inner
				.inflight
				.values()
				.filter(|source| **source == node)
				.count() < EVACUATE_PER_NODE
	}

	fn start(&self, id: &str, node: NodeId) {
		self.inner.lock().inflight.insert(id.to_owned(), node);
	}

	fn finish(&self, id: &str, failed: bool) {
		let mut inner = self.inner.lock();
		inner.inflight.remove(id);
		if failed {
			inner
				.backoff
				.insert(id.to_owned(), Instant::now() + EVACUATE_BACKOFF);
		}
	}
}

/// One evacuator pass over alive draining nodes (admin status `draining` or
/// the gossip flag): starts migrations of their unlocked running sandboxes
/// with over a minute left and, on nodes without an object store, of their
/// paused sandboxes (an object-store node's upload makes those portable), at
/// most [`EVACUATE_PER_NODE`] at a time per node.
pub(super) async fn evacuate(state: &AppState, evacuations: &Evacuations) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let now = Instant::now();
	evacuations
		.inner
		.lock()
		.backoff
		.retain(|_, until| *until > now);
	let statuses = scheduler::load_statuses(kv).await?;
	// Draining nodes → whether they store pauses in the object store.
	let draining: HashMap<NodeId, bool> = state
		.node_members()
		.into_iter()
		.filter_map(|(id, meta)| {
			let cap = meta.node?;
			let drain = cap.draining || statuses.get(&id) == Some(&NodeStatus::Draining);
			drain.then(|| (id, cap.features.iter().any(|f| f == paused_store::FEATURE)))
		})
		.collect();
	if draining.is_empty() {
		return Ok(());
	}
	let routes = list_routes(kv).await?;
	let routed: HashSet<&str> = routes.iter().map(|(id, _)| id.as_str()).collect();
	let min_end = now_ms() + EVACUATE_MIN_LEFT_MS;
	let mut candidates: Vec<(String, NodeId)> = routes
		.iter()
		.filter(|(_, route)| {
			route.state == RouteState::Running
				&& route.end_at_ms > min_end
				&& draining.contains_key(&route.node_id)
		})
		.map(|(id, route)| (id.clone(), route.node_id))
		.collect();
	for (id, row_state, node) in state.store.live_sandboxes().await? {
		if SandboxState::parse(&row_state) == SandboxState::Paused
			&& !routed.contains(id.as_str())
			&& let Ok(node) = node.parse::<NodeId>()
			&& draining.get(&node) == Some(&false)
		{
			candidates.push((id, node));
		}
	}
	for (id, node) in candidates {
		if !evacuations.may_start(&id, node) || is_locked(kv, &id).await? {
			continue;
		}
		evacuations.start(&id, node);
		let (state, evacuations) = (state.clone(), evacuations.clone());
		tokio::spawn(async move {
			let result = migrate_with(&state, &id, None, Duration::ZERO).await;
			evacuations.finish(&id, result.is_err());
			match result {
				Ok(migrated) => {
					tracing::info!(sandbox = %id, from = %node, to = %migrated.node_id, "evacuated sandbox off a draining node");
				},
				Err(error) => {
					tracing::warn!(error = %error.message, sandbox = %id, %node, "evacuating sandbox failed; retrying in a minute");
				},
			}
		});
	}
	Ok(())
}
