//! openraft state machine over [`KvState`], with persisted snapshots and
//! watchers.
//!
//! Applied state lives in memory; durability comes from the snapshot file
//! (`<data>/raft/snapshot.bin`: one JSON line of [`SnapshotMeta`], then the
//! serialized [`KvState`]) plus log replay up to the persisted committed id.

use std::{
	fs,
	io::{self, Cursor},
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
};

use openraft::{
	EntryPayload, ErrorSubject, ErrorVerb, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta,
	StorageError, StoredMembership, storage::RaftStateMachine,
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;

use super::{
	Entry, TxnResult, WATCH_CAPACITY, WatchEvent,
	log_store::replace_file,
	machine::KvState,
	raft::{LogEntry, RaftPeer, TypeConfig},
};
use crate::meta::NodeId;

const SNAPSHOT_FILE: &str = "snapshot.bin";

type Membership = StoredMembership<NodeId, RaftPeer>;
type Meta = SnapshotMeta<NodeId, RaftPeer>;

/// Shared state machine handle: openraft drives it, [`super::Kv`] reads it.
#[derive(Clone)]
pub struct StateMachineStore {
	shared: Arc<Shared>,
}

struct Shared {
	dir:          PathBuf,
	state:        RwLock<AppliedState>,
	snapshot:     Mutex<Option<(Meta, Vec<u8>)>>,
	watchers:     Mutex<Vec<(String, mpsc::Sender<WatchEvent>)>>,
	snapshot_seq: AtomicU64,
}

struct AppliedState {
	kv:           KvState,
	last_applied: Option<LogId<NodeId>>,
	membership:   Membership,
}

impl StateMachineStore {
	/// Loads the last persisted snapshot from `dir` (if any).
	pub fn open(dir: &Path) -> io::Result<Self> {
		fs::create_dir_all(dir)?;
		let mut applied = AppliedState {
			kv:           KvState::default(),
			last_applied: None,
			membership:   Membership::default(),
		};
		let snapshot = match fs::read(dir.join(SNAPSHOT_FILE)) {
			Ok(bytes) => {
				let (meta, data) = decode_snapshot(&bytes)?;
				applied.kv = KvState::from_snapshot(&data).map_err(io::Error::other)?;
				applied.last_applied = meta.last_log_id;
				applied.membership = meta.last_membership.clone();
				Some((meta, data))
			},
			Err(error) if error.kind() == io::ErrorKind::NotFound => None,
			Err(error) => return Err(error),
		};
		let shared = Shared {
			dir:          dir.to_owned(),
			state:        RwLock::new(applied),
			snapshot:     Mutex::new(snapshot),
			watchers:     Mutex::new(Vec::new()),
			snapshot_seq: AtomicU64::new(0),
		};
		Ok(Self { shared: Arc::new(shared) })
	}

	/// Whether any log entry or snapshot has been applied (this replica holds
	/// cluster state).
	pub fn has_state(&self) -> bool {
		self.shared.state.read().last_applied.is_some()
	}

	/// Live entry for `key` in applied state.
	pub fn get(&self, key: &str) -> Option<Entry> {
		self.shared.state.read().kv.get(key)
	}

	/// Live entries under `prefix` in applied state.
	pub fn list(&self, prefix: &str) -> Vec<Entry> {
		self.shared.state.read().kv.list(prefix)
	}

	/// Cluster UUID recorded in applied state.
	pub fn cluster_id(&self) -> Option<String> {
		self.shared.state.read().kv.cluster_id().map(str::to_owned)
	}

	/// Earliest TTL deadline in applied state.
	pub fn next_expiry_ms(&self) -> Option<u64> {
		self.shared.state.read().kv.next_expiry_ms()
	}

	/// Registers a watcher for applied changes under `prefix`.
	pub fn watch(&self, prefix: &str) -> mpsc::Receiver<WatchEvent> {
		let (tx, rx) = mpsc::channel(WATCH_CAPACITY);
		self.shared.watchers.lock().push((prefix.to_owned(), tx));
		rx
	}

	fn notify(&self, events: Vec<WatchEvent>) {
		if events.is_empty() {
			return;
		}
		let mut watchers = self.shared.watchers.lock();
		watchers.retain(|(prefix, tx)| {
			for event in &events {
				let key = match event {
					WatchEvent::Put(entry) => &entry.key,
					WatchEvent::Delete { key, .. } => key,
				};
				if key.starts_with(prefix.as_str()) && tx.try_send(event.clone()).is_err() {
					return false;
				}
			}
			!tx.is_closed()
		});
	}

	fn persist_snapshot(&self, meta: &Meta, data: &[u8]) -> io::Result<()> {
		let mut bytes = serde_json::to_vec(meta).map_err(io::Error::other)?;
		bytes.push(b'\n');
		bytes.extend_from_slice(data);
		replace_file(&self.shared.dir.join(SNAPSHOT_FILE), &bytes)
	}
}

fn decode_snapshot(bytes: &[u8]) -> io::Result<(Meta, Vec<u8>)> {
	let split = bytes
		.iter()
		.position(|b| *b == b'\n')
		.ok_or_else(|| io::Error::other("snapshot file has no header"))?;
	let meta = serde_json::from_slice(&bytes[..split]).map_err(io::Error::other)?;
	Ok((meta, bytes[split + 1..].to_vec()))
}

fn snapshot_err(error: io::Error) -> StorageError<NodeId> {
	StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error)
}

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
	async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
		let (meta, data) = {
			let applied = self.shared.state.read();
			let seq = self.shared.snapshot_seq.fetch_add(1, Ordering::Relaxed);
			let snapshot_id = match applied.last_applied {
				Some(id) => format!("{}-{}-{seq}", id.leader_id.term, id.index),
				None => format!("none-{seq}"),
			};
			let meta = Meta {
				last_log_id: applied.last_applied,
				last_membership: applied.membership.clone(),
				snapshot_id,
			};
			(meta, applied.kv.to_snapshot())
		};
		let this = self.clone();
		let (meta2, data2) = (meta.clone(), data.clone());
		tokio::task::spawn_blocking(move || this.persist_snapshot(&meta2, &data2))
			.await
			.map_err(io::Error::other)
			.and_then(|r| r)
			.map_err(snapshot_err)?;
		*self.shared.snapshot.lock() = Some((meta.clone(), data.clone()));
		Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
	}
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
	type SnapshotBuilder = Self;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn applied_state(
		&mut self,
	) -> Result<(Option<LogId<NodeId>>, Membership), StorageError<NodeId>> {
		let applied = self.shared.state.read();
		Ok((applied.last_applied, applied.membership.clone()))
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn apply<I>(&mut self, entries: I) -> Result<Vec<TxnResult>, StorageError<NodeId>>
	where
		I: IntoIterator<Item = LogEntry> + Send,
		I::IntoIter: Send,
	{
		let mut events = Vec::new();
		let mut replies = Vec::new();
		{
			let mut applied = self.shared.state.write();
			for entry in entries {
				applied.last_applied = Some(entry.log_id);
				let reply = match entry.payload {
					EntryPayload::Blank => TxnResult { succeeded: true, revision: 0 },
					EntryPayload::Normal(command) => applied.kv.apply(&command, &mut events),
					EntryPayload::Membership(membership) => {
						applied.membership = StoredMembership::new(Some(entry.log_id), membership);
						TxnResult { succeeded: true, revision: 0 }
					},
				};
				replies.push(reply);
			}
		}
		self.notify(events);
		Ok(replies)
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		self.clone()
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn begin_receiving_snapshot(
		&mut self,
	) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
		Ok(Box::new(Cursor::new(Vec::new())))
	}

	async fn install_snapshot(
		&mut self,
		meta: &Meta,
		snapshot: Box<Cursor<Vec<u8>>>,
	) -> Result<(), StorageError<NodeId>> {
		let data = snapshot.into_inner();
		let kv = KvState::from_snapshot(&data).map_err(|e| {
			StorageError::from_io_error(
				ErrorSubject::Snapshot(None),
				ErrorVerb::Read,
				io::Error::other(e),
			)
		})?;
		let this = self.clone();
		let (meta2, data2) = (meta.clone(), data.clone());
		tokio::task::spawn_blocking(move || this.persist_snapshot(&meta2, &data2))
			.await
			.map_err(io::Error::other)
			.and_then(|r| r)
			.map_err(snapshot_err)?;
		let events = {
			let mut applied = self.shared.state.write();
			let events = applied.kv.diff(&kv);
			applied.kv = kv;
			applied.last_applied = meta.last_log_id;
			applied.membership = meta.last_membership.clone();
			events
		};
		*self.shared.snapshot.lock() = Some((meta.clone(), data));
		self.notify(events);
		Ok(())
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn get_current_snapshot(
		&mut self,
	) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
		Ok(self
			.shared
			.snapshot
			.lock()
			.as_ref()
			.map(|(meta, data)| Snapshot {
				meta:     meta.clone(),
				snapshot: Box::new(Cursor::new(data.clone())),
			}))
	}
}
