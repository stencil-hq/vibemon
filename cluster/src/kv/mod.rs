//! Raft-replicated KV store on openraft (ARCHITECTURE.md §5.3).
//!
//! Drivers run a raft voter ([`raft`]) whose state machine ([`machine`]) is an
//! ordered map with revisions and deterministic TTLs. Node-only processes get a
//! [`Kv`] that forwards every call to a live driver found through gossip.

mod http;
mod log_store;
mod machine;
mod membership;
mod network;
mod raft;
mod state_machine;
#[cfg(test)]
mod tests;

use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use self::http::{GetRequest, ListRequest};
pub(crate) use self::{
	http::{KvClient, routes},
	membership::Reconciler,
	raft::RaftKv,
};
use crate::{gossip::Gossip, meta::NodeId};

/// One stored key with its revisions; returned by reads and watch events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
	pub key:           String,
	#[serde(with = "hex_bytes")]
	pub value:         Vec<u8>,
	/// Revision of the write that created the key (reset after delete/expiry).
	pub create_rev:    u64,
	/// Revision of the last write to the key.
	pub mod_rev:       u64,
	/// Expiry instant in leader-stamped unix milliseconds, if written with a
	/// TTL.
	pub expires_at_ms: Option<u64>,
}

/// Guard evaluated atomically by [`Kv::txn`] before choosing its branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compare {
	/// The key is present (and not expired).
	Exists(String),
	/// The key is absent (or expired).
	Missing(String),
	/// The key's `mod_rev` equals `rev`; `rev == 0` matches an absent key.
	ModRev { key: String, rev: u64 },
	/// The key is present with exactly this value.
	Value {
		key:   String,
		#[serde(with = "hex_bytes")]
		value: Vec<u8>,
	},
	/// Fewer than `limit` live keys start with `prefix` (admission control).
	CountBelow { prefix: String, limit: u64 },
}

/// Mutation applied by a [`Txn`] branch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
	/// Writes a key; `ttl_ms` makes it expire relative to the applied command
	/// time.
	Put {
		key:    String,
		#[serde(with = "hex_bytes")]
		value:  Vec<u8>,
		ttl_ms: Option<u64>,
	},
	/// Removes a key if present.
	Delete { key: String },
	/// Removes every key starting with `prefix`.
	DeletePrefix { prefix: String },
}

/// Compare-and-swap transaction: `success` runs iff every compare holds, else
/// `failure`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Txn {
	pub compare: Vec<Compare>,
	pub success: Vec<Op>,
	pub failure: Vec<Op>,
}

/// Outcome of a [`Txn`]: which branch ran and the store revision after it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnResult {
	pub succeeded: bool,
	pub revision:  u64,
}

/// Applied-state change delivered to [`Kv::watch`] subscribers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WatchEvent {
	/// A key was written (new value and revisions).
	Put(Entry),
	/// A key was removed (delete, prefix delete, or TTL expiry) at `revision`.
	Delete { key: String, revision: u64 },
}

/// Failure of a KV call.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum KvError {
	/// No raft leader is reachable right now (election in progress or no
	/// quorum).
	#[error("no raft leader")]
	NoLeader,
	/// No driver could serve the call (none alive, all unreachable, raft
	/// stopped).
	#[error("kv unavailable: {0}")]
	Unavailable(String),
	/// The request was refused (malformed, unauthorized, or not applicable).
	#[error("kv request rejected: {0}")]
	Rejected(String),
	/// Local storage or transport failure.
	#[error("kv i/o error: {0}")]
	Io(String),
}

/// Capacity of each [`Kv::watch`] channel; a subscriber this far behind is
/// closed.
pub const WATCH_CAPACITY: usize = 1024;

/// Handle to the cluster KV; cheap to clone.
///
/// On drivers it reads the local raft state machine and proposes writes to the
/// leader; on node-only processes it forwards every call to a live driver.
#[derive(Clone)]
pub struct Kv {
	inner: Arc<KvInner>,
}

struct KvInner {
	local:  Option<Arc<RaftKv>>,
	remote: Remote,
}

impl Kv {
	/// Builds a handle; `local` is the driver's raft (None on node-only
	/// processes).
	pub(crate) fn new(
		self_id: NodeId,
		gossip: Gossip,
		client: KvClient,
		local: Option<Arc<RaftKv>>,
	) -> Self {
		Self { inner: Arc::new(KvInner { local, remote: Remote { self_id, gossip, client } }) }
	}

	/// Driver-side raft handle, if this process runs raft.
	pub(crate) fn local(&self) -> Option<&Arc<RaftKv>> {
		self.inner.local.as_ref()
	}

	/// Reads a key from local applied state (may lag the leader by about a
	/// heartbeat).
	pub async fn get(&self, key: &str) -> Result<Option<Entry>, KvError> {
		match self.ready_local() {
			Some(local) => Ok(local.get_local(key)),
			None => self.inner.remote.get(key, false).await,
		}
	}

	/// Reads a key linearizably (leader read-index).
	pub async fn get_consistent(&self, key: &str) -> Result<Option<Entry>, KvError> {
		match self.ready_local() {
			Some(local) => local.get_consistent(key, false).await,
			None => self.inner.remote.get(key, true).await,
		}
	}

	/// Lists live entries whose key starts with `prefix`, in key order (local
	/// applied state).
	pub async fn list(&self, prefix: &str) -> Result<Vec<Entry>, KvError> {
		match self.ready_local() {
			Some(local) => Ok(local.list_local(prefix)),
			None => self.inner.remote.list(prefix).await,
		}
	}

	/// Runs a transaction on the leader.
	pub async fn txn(&self, txn: Txn) -> Result<TxnResult, KvError> {
		match &self.inner.local {
			Some(local) => local.txn(txn, false).await,
			None => self.inner.remote.txn(&txn).await,
		}
	}

	/// Writes a key unconditionally; returns its new `mod_rev`.
	pub async fn put(
		&self,
		key: &str,
		value: Vec<u8>,
		ttl: Option<Duration>,
	) -> Result<u64, KvError> {
		let ttl_ms = ttl.map(|ttl| u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
		let txn =
			Txn { success: vec![Op::Put { key: key.to_owned(), value, ttl_ms }], ..Txn::default() };
		Ok(self.txn(txn).await?.revision)
	}

	/// Deletes a key; returns whether it existed.
	pub async fn delete(&self, key: &str) -> Result<bool, KvError> {
		let txn = Txn {
			compare: vec![Compare::Exists(key.to_owned())],
			success: vec![Op::Delete { key: key.to_owned() }],
			failure: Vec::new(),
		};
		Ok(self.txn(txn).await?.succeeded)
	}

	/// Subscribes to applied changes under `prefix` (drivers only).
	///
	/// On node-only processes the returned channel is already closed. A
	/// subscriber that falls [`WATCH_CAPACITY`] events behind is closed and must
	/// re-list.
	pub fn watch(&self, prefix: &str) -> mpsc::Receiver<WatchEvent> {
		match &self.inner.local {
			Some(local) => local.watch(prefix),
			None => mpsc::channel(1).1,
		}
	}

	/// Local raft handle once it has applied state; before that calls go through
	/// a peer driver.
	fn ready_local(&self) -> Option<&Arc<RaftKv>> {
		self.inner.local.as_ref().filter(|local| local.has_state())
	}
}

/// Client side used by node-only processes (and drivers that have not joined
/// raft yet).
struct Remote {
	self_id: NodeId,
	gossip:  Gossip,
	client:  KvClient,
}

impl Remote {
	async fn get(&self, key: &str, consistent: bool) -> Result<Option<Entry>, KvError> {
		self
			.call("get", &GetRequest { key: key.to_owned(), consistent })
			.await
	}

	async fn list(&self, prefix: &str) -> Result<Vec<Entry>, KvError> {
		self
			.call("list", &ListRequest { prefix: prefix.to_owned() })
			.await
	}

	async fn txn(&self, txn: &Txn) -> Result<TxnResult, KvError> {
		self.call("txn", txn).await
	}

	/// Sends to live drivers (believed leader first), moving on only when a
	/// driver is unreachable.
	async fn call<Req, Resp>(&self, op: &str, req: &Req) -> Result<Resp, KvError>
	where
		Req: Serialize + Sync,
		Resp: serde::de::DeserializeOwned,
	{
		let drivers = membership::driver_urls(&self.gossip, self.self_id);
		if drivers.is_empty() {
			return Err(KvError::Unavailable("no live driver in gossip".into()));
		}
		let mut last = String::new();
		for url in drivers {
			match self.client.call(&url, op, req, http::Hop::Entry).await {
				Ok(resp) => return Ok(resp),
				Err(http::CallError::Kv(error)) => return Err(error),
				Err(http::CallError::Connect(error)) => last = error,
			}
		}
		Err(KvError::Unavailable(format!("no driver reachable: {last}")))
	}
}

/// Serde adapter storing byte payloads as lowercase hex strings (compact JSON).
mod hex_bytes {
	use serde::{Deserialize, Deserializer, Serializer};

	pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&hex::encode(bytes))
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
		let raw = String::deserialize(deserializer)?;
		hex::decode(raw).map_err(serde::de::Error::custom)
	}
}
