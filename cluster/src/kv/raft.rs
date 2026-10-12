//! Driver-side raft node: openraft type config, startup, and leader-routed KV
//! operations.

use std::{
	future::Future,
	io::Cursor,
	path::Path,
	sync::Arc,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use openraft::{
	Config, Raft, RaftMetrics, ServerState, SnapshotPolicy,
	error::{CheckIsLeaderError, ClientWriteError, RaftError},
};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::mpsc;

use super::{
	Entry, KvError, Txn, TxnResult, WatchEvent,
	http::{CallError, GetRequest, Hop, KvClient, ListRequest},
	log_store::LogStore,
	machine::Command,
	membership,
	network::Network,
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
		R = TxnResult,
		NodeId = NodeId,
		Node = RaftPeer,
		SnapshotData = Cursor<Vec<u8>>,
);

/// Raft log entry type.
pub type LogEntry = openraft::Entry<TypeConfig>;

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
pub fn reported_leader(id: NodeId, metrics: &RaftMetrics<NodeId, RaftPeer>) -> Option<NodeId> {
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
pub fn leadership_confirmed(id: NodeId, metrics: &RaftMetrics<NodeId, RaftPeer>) -> bool {
	let vote = metrics.vote;
	let leader_id = vote.leader_id();
	metrics.state == ServerState::Leader
		&& metrics.current_leader == Some(id)
		&& vote.is_committed()
		&& leader_id.voted_for() == Some(id)
		&& metrics.last_applied.is_some_and(|applied| {
			applied.leader_id.term == leader_id.get_term() && applied.leader_id.node_id == id
		}) && metrics
		.millis_since_quorum_ack
		.is_some_and(|ms| ms <= LEADER_ACK_MAX_MS)
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
	raft:   Raft<TypeConfig>,
	sm:     StateMachineStore,
	client: KvClient,
	gossip: Gossip,
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
			install_snapshot_timeout: 10_000,
			snapshot_policy: SnapshotPolicy::LogsSinceLast(5000),
			max_in_snapshot_log_to_keep: 500,
			snapshot_max_chunk_size: 1 << 20,
			..Config::default()
		}
		.validate()?;
		let log = LogStore::open(dir)?;
		let sm = StateMachineStore::open(dir)?;
		let network = Network::new(http.clone(), auth.clone(), gossip.clone());
		let raft = Raft::new(id, Arc::new(config), network, log, sm.clone()).await?;
		Ok(Arc::new(Self { id, raft, sm, client: KvClient::new(http, auth), gossip }))
	}

	/// The openraft handle (RPC handlers, membership reconciler).
	pub const fn raft(&self) -> &Raft<TypeConfig> {
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
		leadership_confirmed(self.id, &self.raft.metrics().borrow())
	}

	/// Leader as reported to callers ([`reported_leader`]).
	pub fn reported_leader(&self) -> Option<NodeId> {
		reported_leader(self.id, &self.raft.metrics().borrow())
	}

	pub fn get_local(&self, key: &str) -> Option<Entry> {
		self.sm.get(key)
	}

	pub fn list_local(&self, prefix: &str) -> Vec<Entry> {
		self.sm.list(prefix)
	}

	pub fn watch(&self, prefix: &str) -> mpsc::Receiver<WatchEvent> {
		self.sm.watch(prefix)
	}

	/// Follower read served to a remote client; a driver without state asks the
	/// leader.
	pub async fn get_served(&self, key: &str) -> Result<Option<Entry>, KvError> {
		if self.has_state() {
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
			return Ok(self.list_local(prefix));
		}
		let req = ListRequest { prefix: prefix.to_owned() };
		self
			.via_leader("list", &req, Hop::Entry, || async { Attempt::Forward(None) })
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
				match self.raft.ensure_linearizable().await {
					Ok(_) => Attempt::Done(Ok(self.get_local(key))),
					Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(f))) => {
						Attempt::Forward(f.leader_node.map(|n| n.cluster_url))
					},
					Err(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))) => Attempt::Retry,
					Err(RaftError::Fatal(fatal)) => {
						Attempt::Done(Err(KvError::Unavailable(fatal.to_string())))
					},
				}
			})
			.await
	}

	/// Proposes a transaction (stamped with this leader's clock), forwarding to
	/// the leader if needed.
	///
	/// A forward is retried only when the leader was unreachable or had stepped
	/// down before proposing; an entry proposed by a leader that then lost
	/// leadership can still commit, so non-idempotent transactions should guard
	/// themselves with compares.
	pub async fn txn(&self, txn: Txn, forwarded: bool) -> Result<TxnResult, KvError> {
		let hop = if forwarded {
			Hop::Forwarded
		} else {
			Hop::Entry
		};
		self
			.via_leader("txn", &txn, hop, || async {
				match self
					.propose(Command::Txn { now_ms: now_ms(), txn: txn.clone() })
					.await
				{
					Ok(result) => Attempt::Done(Ok(result)),
					Err(ProposeError::Forward(url)) => Attempt::Forward(url),
					Err(ProposeError::Failed(error)) => Attempt::Done(Err(error)),
				}
			})
			.await
	}

	/// Proposes `command` locally; fails with [`ProposeError::Forward`] on
	/// followers.
	pub async fn propose(&self, command: Command) -> Result<TxnResult, ProposeError> {
		match self.raft.client_write(command).await {
			Ok(response) => Ok(response.data),
			Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
				Err(ProposeError::Forward(f.leader_node.map(|n| n.cluster_url)))
			},
			Err(RaftError::APIError(ClientWriteError::ChangeMembershipError(e))) => {
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

/// Failure of [`RaftKv::propose`].
pub enum ProposeError {
	/// This node is not the leader; the known leader URL, if any.
	Forward(Option<String>),
	Failed(KvError),
}

#[cfg(test)]
mod tests {
	use openraft::{CommittedLeaderId, LogId, Vote};

	use super::*;

	const ME: NodeId = NodeId(1);

	fn leader_metrics(
		term: u64,
		applied_term: u64,
		ack_ms: Option<u64>,
	) -> RaftMetrics<NodeId, RaftPeer> {
		let mut metrics = RaftMetrics::new_initial(ME);
		metrics.state = ServerState::Leader;
		metrics.current_leader = Some(ME);
		metrics.current_term = term;
		metrics.vote = Vote::new_committed(term, ME);
		metrics.last_applied = Some(LogId::new(CommittedLeaderId::new(applied_term, ME), 10));
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
