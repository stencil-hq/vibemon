//! Leader-only supervision (ARCHITECTURE.md §9.4).
//!
//! The reaper expires sandboxes past `endAt` every second; the reconciler
//! compares node inventories with KV routes and the store every ten seconds.
//! They run as separate tasks, so a reconciliation pass stalled on the store
//! does not hold up expiries, and one sandbox whose repair fails does not
//! end the pass for the others.

use std::{
	collections::{HashMap, HashSet},
	sync::Arc,
	time::{Duration, Instant},
};

use chrono::Utc;
use parking_lot::Mutex;
use vmon_cluster::{NodeId, gossip::MemberState};
use vmon_node::api::{PauseSandbox, RouteState, SandboxInfo, SandboxState as NodeSandboxState};

use super::{
	KillReason, now_ms, parse_node,
	routes::{get_route_consistent, is_locked, list_routes, remove_route},
};
use crate::{
	error::ApiResult,
	state::AppState,
	store::{event_types, sandboxes::SandboxState},
};

const REAPER_INTERVAL: Duration = Duration::from_secs(1);
const RECONCILE_INTERVAL: Duration = Duration::from_secs(10);
/// Bound on one node inventory call during reconciliation.
const LIST_TIMEOUT: Duration = Duration::from_secs(5);
/// How long after a (re)start a sandbox is exempt from orphan repair: a
/// running route whose VM the node does not list yet (the node create is
/// still booting it) or a running row without a route (the create path
/// between admission and the store insert). Under storage stalls both
/// windows outlast the sandbox lock when its renewal applies late, so this
/// is the safeguard that does not depend on the lock.
const ORPHAN_GRACE_MS: u64 = 120_000;
/// How long routes on a node that left gossip gracefully survive: `vmon
/// serve` shutdown leaves the VMMs running for the restarted node to adopt
/// (ARCHITECTURE.md §8.2), so a restart must not kill its sandboxes. `Dead`
/// (crashed or unreachable) nodes lose their routes immediately.
const LEFT_NODE_GRACE: Duration = Duration::from_mins(1);

/// When this leader first saw each node in gossip state `Left`.
#[derive(Debug, Default)]
pub struct Departures {
	left_since: HashMap<NodeId, Instant>,
}

impl Departures {
	/// Records `Left` nodes (first sighting wins) and forgets the others.
	fn observe(&mut self, states: impl IntoIterator<Item = (NodeId, MemberState)>, now: Instant) {
		let left: HashSet<NodeId> = states
			.into_iter()
			.filter(|(_, state)| *state == MemberState::Left)
			.map(|(id, _)| id)
			.collect();
		self.left_since.retain(|id, _| left.contains(id));
		for id in left {
			self.left_since.entry(id).or_insert(now);
		}
	}

	/// How long `node` has been seen `Left` (zero when it is not).
	fn left_for(&self, node: NodeId, now: Instant) -> Duration {
		self
			.left_since
			.get(&node)
			.map_or(Duration::ZERO, |since| now - *since)
	}
}

/// Whether routes on a node that is not `Alive` count as lost.
fn node_lost(state: Option<MemberState>, left_for: Duration) -> bool {
	match state {
		None | Some(MemberState::Dead) => true,
		Some(MemberState::Left) => left_for >= LEFT_NODE_GRACE,
		Some(MemberState::Alive | MemberState::Suspect) => false,
	}
}

/// Starts the reaper and reconciler; they act only while this driver is the
/// raft leader. Called once by [`crate::Driver::start`].
pub fn spawn_background(state: AppState) {
	tokio::spawn(run_reaper(state.clone()));
	tokio::spawn(run_reconciler(state));
}

/// Ticks every `period` while the cluster runs; `true` when this driver is
/// the raft leader at that tick, `None` once the cluster shut down.
struct LeaderTicks {
	leader:   tokio::sync::watch::Receiver<Option<NodeId>>,
	local:    NodeId,
	interval: tokio::time::Interval,
}

impl LeaderTicks {
	fn new(state: &AppState, period: Duration) -> Self {
		let mut interval = tokio::time::interval(period);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		Self { leader: state.cluster.leader(), local: state.cluster.local_id(), interval }
	}

	async fn tick(&mut self) -> Option<bool> {
		loop {
			tokio::select! {
				changed = self.leader.changed() => changed.ok()?,
				_ = self.interval.tick() => return Some(*self.leader.borrow() == Some(self.local)),
			}
		}
	}
}

async fn run_reaper(state: AppState) {
	let inflight: Arc<Mutex<HashSet<String>>> = Arc::default();
	let mut ticks = LeaderTicks::new(&state, REAPER_INTERVAL);
	while let Some(is_leader) = ticks.tick().await {
		if is_leader {
			reap(&state, &inflight).await;
		}
	}
}

async fn run_reconciler(state: AppState) {
	let mut departures = Departures::default();
	let mut ticks = LeaderTicks::new(&state, RECONCILE_INTERVAL);
	while let Some(is_leader) = ticks.tick().await {
		if is_leader && let Err(error) = reconcile(&state, &mut departures).await {
			tracing::warn!(error = %error.message, "sandbox reconciliation failed");
		}
	}
}

/// Spawns an expiry for every route past its `endAt` that is not already
/// being expired (transitional routes included: [`super::expire`] skips
/// those whose transition is still in flight).
async fn reap(state: &AppState, inflight: &Arc<Mutex<HashSet<String>>>) {
	let routes = match list_routes(&state.cluster.kv).await {
		Ok(routes) => routes,
		Err(error) => {
			tracing::debug!(error = %error.message, "reaper cannot list routes");
			return;
		},
	};
	let now = now_ms();
	for (id, route) in routes {
		if route.end_at_ms > now || !inflight.lock().insert(id.clone()) {
			continue;
		}
		let (state, inflight) = (state.clone(), inflight.clone());
		tokio::spawn(async move {
			if let Err(error) = super::expire(&state, &id).await {
				tracing::warn!(error = %error.message, sandbox = %id, "expiring sandbox failed");
			}
			inflight.lock().remove(&id);
		});
	}
}

/// One reconciliation pass (see module docs). Fails only when routes or rows
/// cannot be listed; a sandbox whose repair fails is logged and retried next
/// pass.
pub async fn reconcile(state: &AppState, departures: &mut Departures) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let members: HashMap<NodeId, (MemberState, String)> = state
		.cluster
		.members()
		.into_iter()
		.filter(|(_, meta)| meta.roles.node)
		.map(|(member, meta)| (member.id, (member.state, meta.cluster_url)))
		.collect();
	let now_instant = Instant::now();
	departures.observe(
		members
			.iter()
			.map(|(id, (member_state, _))| (*id, *member_state)),
		now_instant,
	);

	// Inventories first, routes second: a route is always written before its
	// VM exists, so every listed VM's route is visible to the later read.
	let inventories = futures::future::join_all(
		members
			.iter()
			.filter(|(_, (member_state, _))| *member_state == MemberState::Alive)
			.map(|(node, (_, url))| async move {
				let client = state.node_at(url).ok()?;
				let listed = tokio::time::timeout(LIST_TIMEOUT, client.list_sandboxes())
					.await
					.ok()?
					.ok()?;
				Some((*node, listed))
			}),
	)
	.await;
	let inventories: HashMap<NodeId, Vec<SandboxInfo>> = inventories.into_iter().flatten().collect();
	let routes = list_routes(kv).await?;
	let routed: HashMap<&str, NodeId> = routes
		.iter()
		.map(|(id, route)| (id.as_str(), route.node_id))
		.collect();

	let now = now_ms();
	for (id, route) in &routes {
		let repaired = async {
			if is_locked(kv, id).await? {
				return Ok(());
			}
			let gone = match members.get(&route.node_id) {
				// An unlocked `killing` route is a kill whose driver died (kills no
				// longer write that state): finish it.
				Some((MemberState::Alive, _)) if route.state == RouteState::Killing => true,
				Some((MemberState::Alive, _)) => {
					route.state == RouteState::Running
						&& now.saturating_sub(route.started_at_ms) >= ORPHAN_GRACE_MS
						&& inventories
							.get(&route.node_id)
							.is_some_and(|listed| !listed.iter().any(|info| info.sandbox_id == *id))
				},
				other => node_lost(
					other.map(|(member_state, _)| *member_state),
					departures.left_for(route.node_id, now_instant),
				),
			};
			if gone {
				tracing::warn!(sandbox = %id, node = %route.node_id, "sandbox lost with its node or VM; marking killed");
				remove_route(kv, id, &route.team_id).await?;
				mark_orphan_killed(state, id).await?;
				return Ok(());
			}
			if let Some(info) = inventories
				.get(&route.node_id)
				.and_then(|l| l.iter().find(|i| i.sandbox_id == *id))
				&& info.state == NodeSandboxState::Running
				&& info.end_at_ms != route.end_at_ms
				&& let Some((_, url)) = members.get(&route.node_id)
				&& let Ok(client) = state.node_at(url)
				&& let Err(error) = client.set_deadline(id, route.end_at_ms).await
			{
				tracing::debug!(%error, sandbox = %id, "deadline resync failed");
			}
			ApiResult::Ok(())
		};
		if let Err(error) = repaired.await {
			tracing::warn!(error = %error.message, sandbox = %id, "reconciling sandbox route failed");
		}
	}

	let rows: HashMap<String, (SandboxState, String)> = state
		.store
		.live_sandboxes()
		.await?
		.into_iter()
		.map(|(id, row_state, node)| (id, (SandboxState::parse(&row_state), node)))
		.collect();
	for (node, listed) in &inventories {
		for info in listed {
			let id = info.sandbox_id.as_str();
			if let Err(error) = reconcile_listed(state, &members, &routed, &rows, *node, info).await {
				tracing::warn!(error = %error.message, sandbox = id, "reconciling node sandbox failed");
			}
		}
	}

	for (id, (row_state, _)) in &rows {
		if *row_state != SandboxState::Running || routed.contains_key(id.as_str()) {
			continue;
		}
		if let Err(error) = reconcile_routeless_row(state, &members, id, now).await {
			tracing::warn!(error = %error.message, sandbox = %id, "reconciling sandbox row failed");
		}
	}
	Ok(())
}

/// Kills (or re-pauses) a VM a node lists that nothing legitimizes.
async fn reconcile_listed(
	state: &AppState,
	members: &HashMap<NodeId, (MemberState, String)>,
	routed: &HashMap<&str, NodeId>,
	rows: &HashMap<String, (SandboxState, String)>,
	node: NodeId,
	info: &SandboxInfo,
) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	let id = info.sandbox_id.as_str();
	// A VM is only legitimate on the node its route names: a create
	// retried elsewhere after a timeout may have left a copy behind.
	let legit = match info.state {
		NodeSandboxState::Running => routed.get(id) == Some(&node),
		NodeSandboxState::Paused => rows.get(id).is_some_and(|(row_state, row_node)| {
			*row_state == SandboxState::Paused && *row_node == node.to_string()
		}),
	};
	if legit || is_locked(kv, id).await? {
		return Ok(());
	}
	if info.state == NodeSandboxState::Running
		&& get_route_consistent(kv, id)
			.await?
			.is_some_and(|route| route.node_id == node)
	{
		return Ok(());
	}
	let Some(client) = members
		.get(&node)
		.and_then(|(_, url)| state.node_at(url).ok())
	else {
		return Ok(());
	};
	let paused_here = rows.get(id).is_some_and(|(row_state, row_node)| {
		*row_state == SandboxState::Paused && *row_node == node.to_string()
	});
	if info.state == NodeSandboxState::Running && paused_here {
		// A resume the driver gave up on (timeout) that the node completed:
		// pause it again rather than kill it with its paused state.
		tracing::warn!(sandbox = id, node = %node, "re-pausing a sandbox resumed without a route");
		if let Err(error) = client
			.pause_sandbox(id, &PauseSandbox { memory: true })
			.await
		{
			tracing::warn!(%error, sandbox = id, "re-pausing failed");
		}
		return Ok(());
	}
	tracing::warn!(sandbox = id, node = %node, state = ?info.state, "killing orphaned sandbox on node");
	if let Err(error) = client.kill_sandbox(id).await {
		tracing::warn!(%error, sandbox = id, "orphan kill failed");
	}
	Ok(())
}

/// Marks killed (and stops) a sandbox whose row says running but which has
/// no route, unless a transition holds it or it is within its start grace.
async fn reconcile_routeless_row(
	state: &AppState,
	members: &HashMap<NodeId, (MemberState, String)>,
	id: &str,
	now: u64,
) -> ApiResult<()> {
	let kv = &state.cluster.kv;
	if is_locked(kv, id).await? {
		return Ok(());
	}
	let Some(row) = state.store.sandbox(id).await? else {
		return Ok(());
	};
	if row.state() != SandboxState::Running
		|| now.saturating_sub(super::unix_ms(row.started_at)) < ORPHAN_GRACE_MS
		|| get_route_consistent(kv, id).await?.is_some()
	{
		return Ok(());
	}
	tracing::warn!(sandbox = %id, "running row without a route; marking killed");
	if let Ok(node) = parse_node(&row.node_id)
		&& let Some((MemberState::Alive, url)) = members.get(&node)
		&& let Ok(client) = state.node_at(url)
		&& let Err(error) = client.kill_sandbox(id).await
		&& !error.is_not_found()
	{
		tracing::warn!(%error, sandbox = %id, "kill of route-less sandbox failed");
	}
	mark_orphan_killed(state, id).await
}

async fn mark_orphan_killed(state: &AppState, id: &str) -> ApiResult<()> {
	if let Some(row) = state.store.sandbox(id).await? {
		let killed =
			super::EventSubject::from_row(&row).event(event_types::KILLED, Some(KillReason::Orphaned));
		state
			.store
			.mark_sandbox_killed(id, Utc::now(), &killed)
			.await?;
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn left_nodes_keep_routes_through_a_restart_but_dead_nodes_do_not() {
		let (a, b) = (NodeId(1), NodeId(2));
		let t0 = Instant::now();
		let mut departures = Departures::default();
		departures.observe([(a, MemberState::Left), (b, MemberState::Dead)], t0);
		let soon = t0 + Duration::from_secs(5);
		departures.observe([(a, MemberState::Left), (b, MemberState::Dead)], soon);
		assert_eq!(departures.left_for(a, soon), Duration::from_secs(5), "first sighting wins");
		assert!(!node_lost(Some(MemberState::Left), departures.left_for(a, soon)));
		assert!(node_lost(Some(MemberState::Dead), departures.left_for(b, soon)));
		assert!(node_lost(None, Duration::ZERO), "unknown node");
		assert!(!node_lost(Some(MemberState::Suspect), Duration::ZERO));

		let late = t0 + LEFT_NODE_GRACE;
		assert!(node_lost(Some(MemberState::Left), departures.left_for(a, late)), "never came back");

		// The node restarts (Alive again), then leaves once more: a fresh grace.
		departures.observe([(a, MemberState::Alive)], late);
		assert_eq!(departures.left_for(a, late), Duration::ZERO);
		departures.observe([(a, MemberState::Left)], late);
		assert!(!node_lost(
			Some(MemberState::Left),
			departures.left_for(a, late + Duration::from_secs(1))
		));
	}
}
