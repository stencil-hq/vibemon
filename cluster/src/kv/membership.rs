//! Gossip-driven raft bootstrap, membership reconciliation, TTL ticks, and
//! raft status publishing (ARCHITECTURE.md §5.4).

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	net::SocketAddr,
	sync::Arc,
	time::{Duration, Instant},
};

use openraft::{ChangeMembers, ServerState, async_runtime::WatchReceiver};
use rand::seq::SliceRandom;
use tokio::sync::watch;

use super::{
	machine::Command,
	raft::{RaftKv, RaftPeer, now_ms, reported_leader},
};
use crate::{
	MetaPublisher,
	gossip::{Gossip, Member, MemberState},
	meta::{MemberMeta, NodeId, RaftStatus},
};

const TICK: Duration = Duration::from_millis(500);
/// Membership changes are reconciled every this many ticks (2 s).
const MEMBERSHIP_EVERY: u64 = 4;
/// TTL expiry is checked every this many ticks (1 s).
const EXPIRE_EVERY: u64 = 2;
/// A learner within this many entries of the leader's log is promoted to voter.
const PROMOTE_LAG: u64 = 64;
const CHANGE_TIMEOUT: Duration = Duration::from_secs(5);

/// Alive/suspect driver members with decoded metadata, excluding `exclude`.
fn drivers(gossip: &Gossip, exclude: NodeId) -> Vec<(Member, MemberMeta)> {
	gossip
		.members()
		.into_iter()
		.filter(|m| m.id != exclude && matches!(m.state, MemberState::Alive | MemberState::Suspect))
		.filter_map(|m| MemberMeta::decode(&m.meta).map(|meta| (m, meta)))
		.filter(|(_, meta)| meta.roles.driver)
		.collect()
}

fn claims_leadership(member: &Member, meta: &MemberMeta) -> bool {
	meta
		.raft
		.as_ref()
		.is_some_and(|raft| raft.leader == Some(member.id))
}

/// Cluster URLs of live drivers other than `exclude`: self-reported leaders
/// first, then alive, then suspect members, shuffled within each group.
pub fn driver_urls(gossip: &Gossip, exclude: NodeId) -> Vec<String> {
	let mut found = drivers(gossip, exclude);
	found.shuffle(&mut rand::rng());
	found.sort_by_key(|(member, meta)| {
		(!claims_leadership(member, meta), member.state != MemberState::Alive)
	});
	found
		.into_iter()
		.map(|(member, meta)| meta.cluster_url(&member))
		.collect()
}

/// Cluster URL of member `id` through this process's gossip route to it;
/// `None` when gossip does not know the member or its metadata.
pub fn member_url(gossip: &Gossip, id: NodeId) -> Option<String> {
	let member = gossip.member(id)?;
	MemberMeta::decode(&member.meta).map(|meta| meta.cluster_url(&member))
}

/// Whether `url` (recorded in raft membership) is the cluster URL of one of
/// `member`'s advertised addresses.
fn advertises(member: &Member, meta: &MemberMeta, url: &str) -> bool {
	url.strip_prefix("http://")
		.and_then(|host| host.parse::<SocketAddr>().ok())
		.is_some_and(|recorded| {
			recorded.port() == meta.cluster_port
				&& member.addrs.iter().any(|addr| addr.ip() == recorded.ip())
		})
}

/// Cluster URL of the driver that gossips itself as raft leader (highest term
/// wins).
pub fn leader_url(gossip: &Gossip, exclude: NodeId) -> Option<String> {
	drivers(gossip, exclude)
		.into_iter()
		.filter(|(member, meta)| claims_leadership(member, meta))
		.max_by_key(|(_, meta)| meta.raft.as_ref().map_or(0, |raft| raft.term))
		.map(|(member, meta)| meta.cluster_url(&member))
}

/// Background loop run by every driver.
pub struct Reconciler {
	pub id:             NodeId,
	pub kv:             Arc<RaftKv>,
	pub gossip:         Gossip,
	pub meta:           MetaPublisher,
	pub expect:         usize,
	pub reap_after:     Duration,
	pub cluster_url:    String,
	/// Cluster UUID chosen by this driver when it bootstrapped, until the state
	/// machine records one.
	pending_cluster_id: Option<String>,
	/// Bootstrap voter set seen on the previous tick (must be stable for two
	/// ticks).
	candidate:          Option<Vec<NodeId>>,
	down_since:         HashMap<NodeId, Instant>,
}

impl Reconciler {
	/// Builds the loop state; call [`Reconciler::run`] to drive it.
	pub fn new(
		id: NodeId,
		kv: Arc<RaftKv>,
		gossip: Gossip,
		meta: MetaPublisher,
		expect: usize,
		reap_after: Duration,
		cluster_url: String,
	) -> Self {
		Self {
			id,
			kv,
			gossip,
			meta,
			expect: expect.max(1),
			reap_after,
			cluster_url,
			pending_cluster_id: None,
			candidate: None,
			down_since: HashMap::new(),
		}
	}

	/// Ticks until `stop` flips to true.
	pub async fn run(mut self, mut stop: watch::Receiver<bool>) {
		let mut ticker = tokio::time::interval(TICK);
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		let mut tick: u64 = 0;
		loop {
			tokio::select! {
				_ = ticker.tick() => {},
				_ = stop.changed() => return,
			}
			if *stop.borrow() {
				return;
			}
			self.publish_status();
			match self.kv.raft().is_initialized().await {
				Ok(false) => {
					self.try_bootstrap().await;
					continue;
				},
				Ok(true) => {},
				Err(_) => return,
			}
			if self.kv.is_leader() {
				self.ensure_cluster_id().await;
				if tick.is_multiple_of(EXPIRE_EVERY) {
					self.expire().await;
				}
				if tick.is_multiple_of(MEMBERSHIP_EVERY) {
					self.reconcile_membership().await;
				}
			}
			tick = tick.wrapping_add(1);
		}
	}

	fn status(&self) -> RaftStatus {
		let metrics = self.kv.raft().metrics().borrow_watched().clone();
		RaftStatus {
			cluster_id: self
				.kv
				.state()
				.cluster_id()
				.or_else(|| self.pending_cluster_id.clone()),
			leader:     reported_leader(self.id, &metrics),
			voter:      metrics
				.membership_config
				.membership()
				.voter_ids()
				.any(|id| id == self.id),
			term:       metrics.current_term,
		}
	}

	/// Mirrors raft state into gossip metadata (no-op when unchanged).
	fn publish_status(&self) {
		let status = self.status();
		self.meta.update(|meta| meta.raft = Some(status));
	}

	/// §5.4: the smallest of ≥ N alive, clusterless drivers initializes raft
	/// with exactly N voters.
	async fn try_bootstrap(&mut self) {
		let mut peers = drivers(&self.gossip, self.id);
		if peers.iter().any(|(_, meta)| {
			meta
				.raft
				.as_ref()
				.is_some_and(|raft| raft.cluster_id.is_some())
		}) {
			self.candidate = None;
			return;
		}
		peers.retain(|(member, _)| member.state == MemberState::Alive);
		let mut nodes: BTreeMap<NodeId, RaftPeer> = peers
			.into_iter()
			.map(|(member, meta)| (member.id, RaftPeer { cluster_url: meta.cluster_url(&member) }))
			.collect();
		nodes.insert(self.id, RaftPeer { cluster_url: self.cluster_url.clone() });
		let Some(voters) = bootstrap_voters(self.id, nodes.keys().copied(), self.expect) else {
			self.candidate = None;
			return;
		};
		if self.candidate.as_ref() != Some(&voters) {
			self.candidate = Some(voters);
			return;
		}
		nodes.retain(|id, _| voters.contains(id));
		let cluster_id = uuid::Uuid::new_v4().to_string();
		tracing::info!(%cluster_id, voters = ?voters, "bootstrapping raft cluster");
		self.pending_cluster_id = Some(cluster_id);
		self.publish_status();
		if let Err(error) = self.kv.raft().initialize(nodes).await {
			tracing::warn!(%error, "raft initialize failed");
		}
	}

	/// Records the bootstrap cluster UUID in the state machine (any leader
	/// finishes it).
	async fn ensure_cluster_id(&mut self) {
		if self.kv.state().cluster_id().is_some() {
			self.pending_cluster_id = None;
			return;
		}
		let cluster_id = self
			.pending_cluster_id
			.get_or_insert_with(|| uuid::Uuid::new_v4().to_string())
			.clone();
		let _ = self
			.kv
			.propose(Command::SetClusterId { now_ms: now_ms(), cluster_id })
			.await;
	}

	/// Proposes a clock tick when some TTL deadline has passed.
	async fn expire(&self) {
		let now = now_ms();
		if self.kv.state().next_expiry_ms().is_some_and(|at| at <= now) {
			let _ = self.kv.propose(Command::Expire { now_ms: now }).await;
		}
	}

	/// Adds alive drivers as learners → voters and reaps long-dead voters.
	async fn reconcile_membership(&mut self) {
		let metrics = self.kv.raft().metrics().borrow_watched().clone();
		if metrics.state != ServerState::Leader {
			return;
		}
		let membership = metrics.membership_config.membership().clone();
		if membership.get_joint_config().len() > 1 {
			return;
		}
		let voters: BTreeSet<NodeId> = membership.voter_ids().collect();
		let learners: BTreeSet<NodeId> = membership.learner_ids().collect();
		let alive: BTreeMap<NodeId, (Member, MemberMeta)> = drivers(&self.gossip, self.id)
			.into_iter()
			.filter(|(member, _)| member.state == MemberState::Alive)
			.map(|(member, meta)| (member.id, (member, meta)))
			.collect();
		let raft = self.kv.raft();

		// Peers whose recorded URL they no longer advertise (restart or address
		// change), or that came back after being seen down: a membership entry
		// records this leader's route for later leaders and for processes that
		// cannot resolve the peer through gossip (everyone else dials its own
		// gossip route, so route switches alone are not recorded), and the long
		// unreachable backoff of a revived peer ends (see
		// `network::PeerClient::backoff`).
		let refresh: BTreeMap<NodeId, RaftPeer> = membership
			.nodes()
			.filter_map(|(id, node)| {
				let (member, meta) = alive.get(id)?;
				let revived = self.down_since.contains_key(id);
				(revived || !advertises(member, meta, &node.cluster_url))
					.then(|| (*id, RaftPeer { cluster_url: meta.cluster_url(member) }))
			})
			.collect();
		if !refresh.is_empty() {
			tracing::info!(nodes = ?refresh.keys().collect::<Vec<_>>(), "refreshing raft peer addresses");
			for (id, peer) in &refresh {
				self.down_since.remove(id);
				self.kv.set_peer_url(*id, &peer.cluster_url);
			}
			let ids: Vec<NodeId> = refresh.keys().copied().collect();
			change(raft.change_membership(ChangeMembers::SetNodes(refresh), true)).await;
			if let Err(error) = raft.trigger().reset_backoff(ids).await {
				tracing::warn!(%error, "raft replication backoff reset failed");
			}
			return;
		}

		let last_log = metrics.last_log_index.unwrap_or(0);
		let matched = |id: &NodeId| {
			metrics
				.replication
				.as_ref()
				.and_then(|r| r.get(id).copied().flatten())
				.map(|log_id| log_id.index)
		};
		for (id, (member, meta)) in &alive {
			if voters.contains(id) {
				continue;
			}
			if !learners.contains(id) {
				tracing::info!(node = %id, "adding raft learner");
				let peer = RaftPeer { cluster_url: meta.cluster_url(member) };
				change(raft.add_learner(*id, peer, false)).await;
			} else if matched(id).is_some_and(|index| index + PROMOTE_LAG >= last_log) {
				tracing::info!(node = %id, "promoting raft learner to voter");
				change(raft.change_membership(ChangeMembers::AddVoterIds(BTreeSet::from([*id])), true))
					.await;
				return;
			}
		}

		let now = Instant::now();
		let mut gone = BTreeSet::new();
		for id in voters.iter().chain(&learners) {
			let down = *id != self.id
				&& self
					.gossip
					.member(*id)
					.is_none_or(|m| matches!(m.state, MemberState::Dead | MemberState::Left));
			if !down {
				self.down_since.remove(id);
				continue;
			}
			if now.duration_since(*self.down_since.entry(*id).or_insert(now)) >= self.reap_after {
				gone.insert(*id);
			}
		}
		self
			.down_since
			.retain(|id, _| voters.contains(id) || learners.contains(id));
		let reap_voters: BTreeSet<NodeId> = gone.intersection(&voters).copied().collect();
		if !reap_voters.is_empty() && reap_voters.len() < voters.len() {
			tracing::info!(nodes = ?reap_voters, "removing dead raft voters");
			change(raft.change_membership(ChangeMembers::RemoveVoters(reap_voters), false)).await;
			return;
		}
		let reap_learners: BTreeSet<NodeId> = gone.intersection(&learners).copied().collect();
		if !reap_learners.is_empty() {
			change(raft.change_membership(ChangeMembers::RemoveNodes(reap_learners), false)).await;
		}
	}
}

/// The N smallest ids when `self_id` is the smallest of at least `expect`
/// candidates.
fn bootstrap_voters(
	self_id: NodeId,
	candidates: impl Iterator<Item = NodeId>,
	expect: usize,
) -> Option<Vec<NodeId>> {
	let mut ids: Vec<NodeId> = candidates.collect();
	ids.sort_unstable();
	ids.dedup();
	if ids.len() < expect || ids.first() != Some(&self_id) {
		return None;
	}
	ids.truncate(expect);
	Some(ids)
}

/// Awaits a membership change with a timeout, logging failures (retried next
/// round).
async fn change<T, E: std::fmt::Display>(fut: impl Future<Output = Result<T, E>>) {
	match tokio::time::timeout(CHANGE_TIMEOUT, fut).await {
		Ok(Ok(_)) => {},
		Ok(Err(error)) => tracing::warn!(%error, "raft membership change failed"),
		Err(_) => tracing::warn!("raft membership change timed out"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn only_the_smallest_of_enough_candidates_bootstraps() {
		let ids = || [NodeId(5), NodeId(2), NodeId(9), NodeId(7)].into_iter();
		assert_eq!(
			bootstrap_voters(NodeId(2), ids(), 3),
			Some(vec![NodeId(2), NodeId(5), NodeId(7)])
		);
		assert_eq!(bootstrap_voters(NodeId(5), ids(), 3), None);
		assert_eq!(bootstrap_voters(NodeId(2), ids(), 5), None);
		assert_eq!(bootstrap_voters(NodeId(2), [NodeId(2)].into_iter(), 1), Some(vec![NodeId(2)]));
	}

	#[test]
	fn a_recorded_url_counts_while_any_of_its_addresses_is_advertised() {
		let addr = |s: &str| s.parse::<SocketAddr>().unwrap();
		let member = Member {
			id:           NodeId(1),
			addr:         addr("10.0.0.1:7946"),
			addrs:        vec![addr("10.0.0.1:7946"), addr("[fd7a:115c:a1e0::1]:7946")],
			state:        MemberState::Alive,
			incarnation:  1,
			meta_version: 1,
			meta:         Vec::new(),
		};
		let meta = MemberMeta { cluster_port: 7947, ..MemberMeta::default() };
		assert!(advertises(&member, &meta, "http://10.0.0.1:7947"));
		assert!(advertises(&member, &meta, "http://[fd7a:115c:a1e0::1]:7947"));
		assert!(!advertises(&member, &meta, "http://10.0.0.2:7947"), "address gone");
		assert!(!advertises(&member, &meta, "http://10.0.0.1:7100"), "port changed");
		assert!(!advertises(&member, &meta, "garbage"));
	}
}
