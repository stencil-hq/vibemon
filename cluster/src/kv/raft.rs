//! Driver-side raft node: openraft type config, startup, and leader-routed KV
//! operations.

use std::{
	future::Future,
	ops,
	path::Path,
	sync::Arc,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use openraft::{
	Config, EntryPayload, Raft, ReadPolicy, ServerState, SnapshotPolicy,
	async_runtime::WatchReceiver,
	error::{ClientWriteError, LinearizableReadError, RaftError},
	impls::leader_id_adv,
};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{mpsc, watch};

use super::{
	Entry, KvError, Range, Record, Scan, Txn, TxnResult, WatchEvent,
	http::{CallError, CopyRequest, GetRequest, Hop, KvClient, ListRequest},
	journal::{self, Copied},
	log_store::LogStore,
	machine::Command,
	membership,
	network::{Network, PeerUrls},
	state_machine::StateMachineStore,
};
use crate::{gossip::Gossip, meta::NodeId};

/// Raft member address as stored in the replicated membership.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftPeer {
	/// Internal HTTP base URL serving `/cluster/v1/raft/*`.
	pub cluster_url: String,
}

openraft::declare_raft_types!(
	/// openraft type configuration for the cluster KV.
	pub TypeConfig:
		D = Command,
		R = Vec<TxnResult>,
		NodeId = NodeId,
		Node = RaftPeer,
		LeaderId = LeaderId,
);

/// Leader identity `(term, node)` carried by votes and log ids (openraft's
/// default `leader_adv` mode, the shape stored by every release).
pub type LeaderId = leader_id_adv::LeaderId<u64, NodeId>;
/// Raft log id.
pub type LogId = openraft::LogId<LeaderId>;
/// Raft vote.
pub type Vote = openraft::Vote<LeaderId>;
/// Raft log entry type.
pub type LogEntry = openraft::Entry<LeaderId, EntryPayload<Command, NodeId, RaftPeer>>;
/// Membership with the id of the log entry that holds it.
pub type StoredMembership = openraft::StoredMembership<LeaderId, NodeId, RaftPeer>;
/// Metadata of a state machine snapshot.
pub type SnapshotMeta = openraft::SnapshotMeta<LeaderId, NodeId, RaftPeer>;
/// State machine snapshot: metadata plus serialized
/// [`super::machine::KvState`].
pub type Snapshot = openraft::Snapshot<LeaderId, NodeId, RaftPeer, Vec<u8>>;
/// openraft metrics of this type configuration.
pub type RaftMetrics = openraft::RaftMetrics<TypeConfig>;
/// The openraft handle of a driver.
pub type KvRaft = Raft<TypeConfig, StateMachineStore>;

/// Period of [`maintain_journal`].
const JOURNAL_TICK: Duration = Duration::from_secs(10);
/// Ticks between journal retention passes.
const JOURNAL_PRUNE_TICKS: u32 = 6;
/// Values per journal copy reply (hex doubles them on the wire).
const JOURNAL_COPY_BYTES: usize = 4 << 20;
/// How long a KV call waits for a reachable leader before failing with
/// [`KvError::NoLeader`].
const LEADER_WAIT: Duration = Duration::from_secs(8);
const RETRY_DELAY: Duration = Duration::from_millis(100);
/// A leader whose last quorum acknowledgement is older than this is not
/// reported as leader (a partitioned or deposed leader stops claiming it).
const LEADER_ACK_MAX_MS: u64 = 1500;

/// Leader to report to callers: openraft's `current_leader`, except that this
/// node counts as leader only once its leadership is confirmed for the current
/// term (see [`leadership_confirmed`]); used by the facade's leader watch and
/// the gossip `RaftStatus`.
pub fn reported_leader(id: NodeId, metrics: &RaftMetrics) -> Option<NodeId> {
	match metrics.current_leader {
		Some(leader) if leader == id => leadership_confirmed(id, metrics).then_some(id),
		other => other,
	}
}

/// Whether `id` leads with confirmed authority: it is leader under a committed
/// vote, has applied an entry of its own current term (the term's blank entry
/// reached a quorum), and a quorum acknowledged it within
/// [`LEADER_ACK_MAX_MS`].
///
/// A former leader restarting with its persisted vote re-establishes itself
/// as leader of its old term until it sees the higher term; neither quorum
/// acknowledgement nor a current-term applied entry exists in that window.
pub fn leadership_confirmed(id: NodeId, metrics: &RaftMetrics) -> bool {
	let vote = &metrics.vote;
	let leader_id = vote.leader_id();
	metrics.state == ServerState::Leader
		&& metrics.current_leader == Some(id)
		&& vote.is_committed()
		&& leader_id.node_id == id
		&& metrics.last_applied.is_some_and(|applied| {
			applied.leader_id.term == leader_id.term && applied.leader_id.node_id == id
		}) && millis_since_quorum_ack(metrics).is_some_and(|ms| ms <= LEADER_ACK_MAX_MS)
}

/// Milliseconds since a quorum last acknowledged this leader, as of the
/// metrics report; `Some(0)` for a leader that is a quorum by itself.
///
/// Its replacement `last_quorum_acked` has no value for a single-voter leader
/// (no follower ever acknowledges it), so it would never confirm one.
#[expect(
	deprecated,
	reason = "the only metric that counts a single-voter leader as quorum-acknowledged"
)]
const fn millis_since_quorum_ack(metrics: &RaftMetrics) -> Option<u64> {
	metrics.millis_since_quorum_ack
}

/// Leader-stamped wall clock in unix milliseconds.
pub fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Outcome of one local attempt inside [`RaftKv::via_leader`].
enum Attempt<T> {
	Done(Result<T, KvError>),
	/// Not leader; forward to this URL (or a gossip leader hint when `None`).
	Forward(Option<String>),
	/// Transient (no quorum yet); try again after a short delay.
	Retry,
}

/// A driver's raft participant plus everything needed to route KV calls.
pub struct RaftKv {
	id:     NodeId,
	raft:   KvRaft,
	sm:     StateMachineStore,
	client: KvClient,
	gossip: Gossip,
	/// Raft URLs the replication clients send to.
	urls:   PeerUrls,
}

impl RaftKv {
	/// Opens `<dir>` storage and starts the raft core (uninitialized until
	/// bootstrap or join).
	pub async fn start(
		id: NodeId,
		dir: &Path,
		gossip: Gossip,
		http: reqwest::Client,
		auth: Arc<HeaderValue>,
	) -> anyhow::Result<Arc<Self>> {
		let config = Config {
			cluster_name: "vmon".into(),
			heartbeat_interval: 150,
			election_timeout_min: 600,
			election_timeout_max: 1200,
			// Bounds a whole snapshot transfer, sent as one request.
			install_snapshot_timeout: 60_000,
			snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
			max_in_snapshot_log_to_keep: 500,
			..Config::default()
		}
		.validate()?;
		let log = LogStore::open(dir)?;
		let sm = StateMachineStore::open(dir)?;
		let urls = PeerUrls::default();
		let network = Network::new(http.clone(), auth.clone(), gossip.clone(), urls.clone());
		let raft = Raft::new(id, Arc::new(config), network, log, sm.clone()).await?;
		Ok(Arc::new(Self { id, raft, sm, client: KvClient::new(http, auth), gossip, urls }))
	}

	/// Sends this node's raft RPCs for `peer` to `url` from now on, including
	/// on its running replication stream.
	pub fn set_peer_url(&self, peer: NodeId, url: &str) {
		self.urls.set(peer, url);
	}

	/// The openraft handle (RPC handlers, membership reconciler).
	pub const fn raft(&self) -> &KvRaft {
		&self.raft
	}

	/// Applied state machine (reads, TTL scheduling, cluster id).
	pub const fn state(&self) -> &StateMachineStore {
		&self.sm
	}

	/// Whether this replica holds applied cluster state.
	pub fn has_state(&self) -> bool {
		self.sm.has_state()
	}

	/// Whether this process is the raft leader with confirmed authority
	/// ([`leadership_confirmed`]); gates leader-only background work.
	pub fn is_leader(&self) -> bool {
		leadership_confirmed(self.id, &self.raft.metrics().borrow_watched())
	}

	/// Leader as reported to callers ([`reported_leader`]).
	pub fn reported_leader(&self) -> Option<NodeId> {
		reported_leader(self.id, &self.raft.metrics().borrow_watched())
	}

	pub fn get_local(&self, key: &str) -> Option<Entry> {
		self.sm.get(key)
	}

	pub fn list_local(&self, prefix: &str) -> Vec<Entry> {
		self.sm.list(prefix)
	}

	pub fn range_local(&self, range: &Range) -> Vec<Entry> {
		self.sm.range(range)
	}

	pub fn watch(&self, prefix: &str) -> mpsc::Receiver<WatchEvent> {
		self.sm.watch(prefix)
	}

	/// Read barrier ([`super::Kv::sync`]): free on a confirmed leader; a
	/// follower fetches the leader's applied revision and waits to apply it.
	pub async fn sync(&self) -> Result<(), KvError> {
		if self.is_leader() {
			return Ok(());
		}
		let revision = self.leader_revision(false).await?;
		self.sm.wait_revision(revision, LEADER_WAIT).await
	}

	/// Applied revision of the confirmed leader, forwarded there when this node
	/// is a follower. Every write the leader acknowledged is applied at it.
	pub async fn leader_revision(&self, forwarded: bool) -> Result<u64, KvError> {
		let hop = if forwarded {
			Hop::Forwarded
		} else {
			Hop::Entry
		};
		self
			.via_leader("sync", &(), hop, || async {
				if self.is_leader() {
					Attempt::Done(Ok(self.sm.revision()))
				} else {
					self.to_leader()
				}
			})
			.await
	}

	/// Forward to the current raft leader, or retry while there is none (or
	/// it is this node, not yet confirmed).
	fn to_leader<T>(&self) -> Attempt<T> {
		let metrics = self.raft.metrics().borrow_watched().clone();
		match metrics.current_leader.filter(|leader| *leader != self.id) {
			Some(leader) => Attempt::Forward(self.forward_url(Some(leader), || {
				metrics
					.membership_config
					.membership()
					.get_node(&leader)
					.map(|peer| peer.cluster_url.clone())
			})),
			None => Attempt::Retry,
		}
	}

	/// URL to forward to `leader` at: this process's gossip route to it (LAN
	/// first), else the URL raft membership `recorded` for it.
	fn forward_url(
		&self,
		leader: Option<NodeId>,
		recorded: impl FnOnce() -> Option<String>,
	) -> Option<String> {
		leader
			.and_then(|id| membership::member_url(&self.gossip, id))
			.or_else(recorded)
	}

	/// Journal scan ([`super::Kv::scan`]) of local state; a follower whose
	/// journal has holes (it installed a raft snapshot and is still copying
	/// the records it skipped) asks the leader, which serves what it holds.
	pub async fn scan(&self, scan: &Scan, forwarded: bool) -> Result<Vec<Record>, KvError> {
		if forwarded || self.sm.journal_complete() || self.is_leader() {
			return self.sm.scan(scan).await;
		}
		self
			.via_leader("scan", scan, Hop::Entry, || async {
				if self.is_leader() {
					Attempt::Done(self.sm.scan(scan).await)
				} else {
					self.to_leader()
				}
			})
			.await
	}

	/// Journal scan served to a remote client (after a read barrier); a
	/// driver without state asks the leader.
	pub async fn scan_served(&self, scan: &Scan, forwarded: bool) -> Result<Vec<Record>, KvError> {
		if forwarded {
			return self.sm.scan(scan).await;
		}
		if self.has_state() {
			self.sync().await?;
			return self.scan(scan, false).await;
		}
		self
			.via_leader("scan", scan, Hop::Entry, || async { Attempt::Forward(None) })
			.await
	}

	/// This replica's journal records of `range` for a peer filling a hole.
	pub async fn copy_journal(&self, range: ops::Range<u64>) -> Result<Copied, KvError> {
		self
			.sm
			.copy_journal(range, JOURNAL_COPY_BYTES)
			.await
			.map_err(|error| KvError::Io(format!("journal: {error}")))
	}

	/// Copies the records of every journal hole from peers (believed leader
	/// first), staying with a peer while it makes progress.
	async fn backfill_journal(&self) {
		'holes: for hole in self.sm.journal_holes() {
			for url in membership::driver_urls(&self.gossip, self.id) {
				let mut missing = hole.clone();
				loop {
					let req = CopyRequest { from: missing.start, to: missing.end };
					let copied: Copied = match self
						.client
						.call(&url, "journal", &req, Hop::Forwarded)
						.await
					{
						Ok(copied) => copied,
						Err(error) => {
							tracing::debug!(%url, ?error, "journal copy failed");
							break;
						},
					};
					if let Err(error) = self.sm.fill_journal(missing.clone(), &copied) {
						tracing::warn!(%url, %error, "journal copy rejected");
						break;
					}
					let rest = self
						.sm
						.journal_holes()
						.into_iter()
						.find(|rest| rest.start < hole.end && hole.start < rest.end);
					match rest {
						None => continue 'holes,
						Some(rest) if rest.start > missing.start => missing = rest,
						// No progress from this peer: try the next one.
						Some(_) => break,
					}
				}
			}
		}
	}

	/// Follower read served to a remote client (after a read barrier); a driver
	/// without state asks the leader.
	pub async fn get_served(&self, key: &str) -> Result<Option<Entry>, KvError> {
		if self.has_state() {
			self.sync().await?;
			return Ok(self.get_local(key));
		}
		let req = GetRequest { key: key.to_owned(), consistent: false };
		self
			.via_leader("get", &req, Hop::Entry, || async { Attempt::Forward(None) })
			.await
	}

	/// Prefix list served to a remote client; a driver without state asks the
	/// leader.
	pub async fn list_served(&self, prefix: &str) -> Result<Vec<Entry>, KvError> {
		if self.has_state() {
			self.sync().await?;
			return Ok(self.list_local(prefix));
		}
		let req = ListRequest { prefix: prefix.to_owned() };
		self
			.via_leader("list", &req, Hop::Entry, || async { Attempt::Forward(None) })
			.await
	}

	/// Range read served to a remote client (after a read barrier); a driver
	/// without state asks the leader.
	pub async fn range_served(&self, range: &Range) -> Result<Vec<Entry>, KvError> {
		if self.has_state() {
			self.sync().await?;
			return Ok(self.range_local(range));
		}
		self
			.via_leader("range", range, Hop::Entry, || async { Attempt::Forward(None) })
			.await
	}

	/// Linearizable read: read-index on the leader, forwarded when this node is
	/// a follower.
	pub async fn get_consistent(
		&self,
		key: &str,
		forwarded: bool,
	) -> Result<Option<Entry>, KvError> {
		let req = GetRequest { key: key.to_owned(), consistent: true };
		let hop = if forwarded {
			Hop::Forwarded
		} else {
			Hop::Entry
		};
		self
			.via_leader("get", &req, hop, || async {
				match self.raft.ensure_linearizable(ReadPolicy::ReadIndex).await {
					Ok(_) => Attempt::Done(Ok(self.get_local(key))),
					Err(RaftError::APIError(LinearizableReadError::ForwardToLeader(f))) => {
						Attempt::Forward(
							self.forward_url(f.leader_id, || f.leader_node.map(|n| n.cluster_url)),
						)
					},
					Err(RaftError::APIError(LinearizableReadError::QuorumNotEnough(_))) => {
						Attempt::Retry
					},
					Err(RaftError::Fatal(fatal)) => {
						Attempt::Done(Err(KvError::Unavailable(fatal.to_string())))
					},
				}
			})
			.await
	}

	/// Commits `txns` as one [`Command::Txns`] raft entry, in order (stamped
	/// with this leader's clock), forwarding to the leader if needed.
	/// Concurrent callers' entries share log appends: openraft's API queue
	/// hands every write that arrived during the previous append to the next
	/// one (one `fdatasync`).
	///
	/// A forward is retried only when the leader was unreachable or had stepped
	/// down before proposing; an entry proposed by a leader that then lost
	/// leadership can still commit, so non-idempotent transactions should guard
	/// themselves with compares.
	pub async fn txns(&self, txns: Vec<Txn>, forwarded: bool) -> Result<Vec<TxnResult>, KvError> {
		let hop = if forwarded {
			Hop::Forwarded
		} else {
			Hop::Entry
		};
		self
			.via_leader("txns", &txns, hop, || async {
				let command = Command::Txns { now_ms: now_ms(), txns: txns.clone() };
				match self.propose(command).await {
					Ok(results) => Attempt::Done(Ok(results)),
					Err(ProposeError::Forward(url)) => Attempt::Forward(url),
					Err(ProposeError::Failed(error)) => Attempt::Done(Err(error)),
				}
			})
			.await
	}

	/// Proposes `command` locally; fails with [`ProposeError::Forward`] on
	/// followers.
	pub async fn propose(&self, command: Command) -> Result<Vec<TxnResult>, ProposeError> {
		match self.raft.client_write(command).await {
			Ok(response) => Ok(response.data),
			// A discarded entry (truncated or purged after this node lost
			// leadership) is forwarded like a refused proposal, as before
			// openraft 0.10 split the two; see `txns` on duplicate commits.
			Err(RaftError::APIError(
				ClientWriteError::ForwardToLeader(f) | ClientWriteError::LogEntryDiscarded(f),
			)) => Err(ProposeError::Forward(
				self.forward_url(f.leader_id, || f.leader_node.map(|n| n.cluster_url)),
			)),
			Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
				Err(ProposeError::Failed(KvError::Rejected(e.to_string())))
			},
			Err(RaftError::APIError(ClientWriteError::PreconditionFailed(e))) => {
				Err(ProposeError::Failed(KvError::Rejected(e.to_string())))
			},
			Err(RaftError::Fatal(fatal)) => {
				Err(ProposeError::Failed(KvError::Unavailable(fatal.to_string())))
			},
		}
	}

	/// Runs `attempt` locally; when not leader, forwards `req` to
	/// `{leader}/cluster/v1/kv/{op}`.
	///
	/// Retries until [`LEADER_WAIT`] while there is no reachable leader. A call
	/// that already is a forward (`Hop::Forwarded`) never forwards again.
	async fn via_leader<T, Req, F, Fut>(
		&self,
		op: &str,
		req: &Req,
		hop: Hop,
		mut attempt: F,
	) -> Result<T, KvError>
	where
		T: DeserializeOwned,
		Req: Serialize + Sync,
		F: FnMut() -> Fut,
		Fut: Future<Output = Attempt<T>>,
	{
		let deadline = Instant::now() + LEADER_WAIT;
		loop {
			match attempt().await {
				Attempt::Done(result) => return result,
				Attempt::Forward(_) if hop == Hop::Forwarded => return Err(KvError::NoLeader),
				Attempt::Forward(url) => {
					if let Some(url) = url.or_else(|| membership::leader_url(&self.gossip, self.id)) {
						match self.client.call(&url, op, req, Hop::Forwarded).await {
							Ok(value) => return Ok(value),
							Err(CallError::Kv(KvError::NoLeader) | CallError::Connect(_)) => {},
							Err(CallError::Kv(error)) => return Err(error),
						}
					}
				},
				Attempt::Retry => {},
			}
			if Instant::now() >= deadline {
				return Err(KvError::NoLeader);
			}
			tokio::time::sleep(RETRY_DELAY).await;
		}
	}
}

/// Background upkeep of a driver's journal until `stop`: makes appended
/// records durable every [`JOURNAL_TICK`], drops records older than
/// [`journal::RETENTION`], and copies holes from peers.
pub async fn maintain_journal(kv: Arc<RaftKv>, mut stop: watch::Receiver<bool>) {
	let mut tick = tokio::time::interval(JOURNAL_TICK);
	tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	let mut ticks = 0u32;
	loop {
		tokio::select! {
			_ = tick.tick() => {},
			_ = stop.changed() => return,
		}
		if let Err(error) = kv.sm.flush_journal().await {
			tracing::warn!(%error, "journal flush failed");
		}
		ticks = ticks.wrapping_add(1);
		if ticks.is_multiple_of(JOURNAL_PRUNE_TICKS) {
			let retention = u64::try_from(journal::RETENTION.as_millis()).unwrap_or(u64::MAX);
			match kv.sm.prune_journal(now_ms().saturating_sub(retention)) {
				Ok(0) => {},
				Ok(pruned) => tracing::debug!(pruned, "journal segments expired"),
				Err(error) => tracing::warn!(%error, "journal prune failed"),
			}
		}
		if !kv.sm.journal_complete() {
			kv.backfill_journal().await;
		}
	}
}

/// Failure of [`RaftKv::propose`].
pub enum ProposeError {
	/// This node is not the leader; the known leader URL, if any.
	Forward(Option<String>),
	Failed(KvError),
}

#[cfg(test)]
mod tests {
	use super::*;

	const ME: NodeId = NodeId(1);

	#[expect(deprecated, reason = "sets the metric `leadership_confirmed` reads")]
	fn leader_metrics(term: u64, applied_term: u64, ack_ms: Option<u64>) -> RaftMetrics {
		let mut metrics = RaftMetrics::new_initial(ME);
		metrics.state = ServerState::Leader;
		metrics.current_leader = Some(ME);
		metrics.current_term = term;
		metrics.vote = Vote::new_committed(term, ME);
		metrics.last_applied = Some(LogId::new(LeaderId { term: applied_term, node_id: ME }, 10));
		metrics.millis_since_quorum_ack = ack_ms;
		metrics
	}

	#[test]
	fn restarted_former_leader_is_not_reported_until_confirmed() {
		// Restart window: re-established leader of its persisted term, no quorum
		// ack yet.
		let restarted = leader_metrics(3, 3, None);
		assert!(!leadership_confirmed(ME, &restarted));
		assert_eq!(reported_leader(ME, &restarted), None);

		// Quorum acks but this term's blank entry not applied yet.
		assert!(!leadership_confirmed(ME, &leader_metrics(4, 3, Some(10))));
		// Confirmed: current-term entry applied and a fresh quorum ack.
		let confirmed = leader_metrics(4, 4, Some(10));
		assert!(leadership_confirmed(ME, &confirmed));
		assert_eq!(reported_leader(ME, &confirmed), Some(ME));
		// Quorum contact lost for longer than the window: no longer reported.
		assert!(!leadership_confirmed(ME, &leader_metrics(4, 4, Some(LEADER_ACK_MAX_MS + 1))));

		// Stepped down: follower state drops self immediately; other leaders pass
		// through.
		let mut follower = confirmed;
		follower.state = ServerState::Follower;
		assert_eq!(reported_leader(ME, &follower), None);
		follower.current_leader = Some(NodeId(2));
		assert_eq!(reported_leader(ME, &follower), Some(NodeId(2)));
	}
}
