//! openraft state machine over [`KvState`], with persisted snapshots and
//! watchers.
//!
//! Applied state lives in memory; durability comes from the snapshot file
//! (`<data>/raft/snapshot.bin`: one JSON line of [`SnapshotMeta`], then the
//! serialized [`KvState`]) plus log replay up to the persisted committed id.

use std::{
	fs, io,
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use futures::{Stream, TryStreamExt};
use openraft::{
	EntryPayload, RaftSnapshotBuilder,
	storage::{EntryResponder, RaftStateMachine},
};
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, watch};

use super::{
	Entry, KvError, Range, WATCH_CAPACITY, WatchEvent,
	log_store::replace_file,
	machine::KvState,
	raft::{LogId, Snapshot, SnapshotMeta as Meta, StoredMembership as Membership, TypeConfig},
};

const SNAPSHOT_FILE: &str = "snapshot.bin";

/// Shared state machine handle: openraft drives it, [`super::Kv`] reads it.
#[derive(Clone)]
pub struct StateMachineStore {
	shared: Arc<Shared>,
}

struct Shared {
	dir:      PathBuf,
	state:    RwLock<AppliedState>,
	snapshot: Mutex<Option<(Meta, Vec<u8>)>>,
	watchers: Mutex<Vec<(String, mpsc::Sender<WatchEvent>)>>,
	/// Applied KV revision, for [`StateMachineStore::wait_revision`].
	revision: watch::Sender<u64>,
}

struct AppliedState {
	kv:           KvState,
	last_applied: Option<LogId>,
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
			dir:      dir.to_owned(),
			revision: watch::Sender::new(applied.kv.revision()),
			state:    RwLock::new(applied),
			snapshot: Mutex::new(snapshot),
			watchers: Mutex::new(Vec::new()),
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

	/// Live entries matching `range` in applied state.
	pub fn range(&self, range: &Range) -> Vec<Entry> {
		self.shared.state.read().kv.range(range)
	}

	/// Revision of the last applied change.
	pub fn revision(&self) -> u64 {
		*self.shared.revision.borrow()
	}

	/// Waits until applied state has reached `revision`.
	///
	/// # Errors
	///
	/// [`KvError::Unavailable`] when it does not within `timeout`.
	pub async fn wait_revision(&self, revision: u64, timeout: Duration) -> Result<(), KvError> {
		let mut applied = self.shared.revision.subscribe();
		match tokio::time::timeout(timeout, applied.wait_for(|at| *at >= revision)).await {
			Ok(Ok(_)) => Ok(()),
			// The sender lives in `self`, so the channel cannot close while we wait.
			Ok(Err(_)) | Err(_) => Err(KvError::Unavailable(format!(
				"local state at revision {} did not reach leader revision {revision} within \
				 {timeout:?}",
				self.revision()
			))),
		}
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

impl RaftSnapshotBuilder<TypeConfig> for StateMachineStore {
	type SnapshotData = Vec<u8>;

	async fn build_snapshot(&mut self) -> io::Result<Snapshot> {
		// Copy the state under the lock and serialize it outside, so applies
		// only wait for the copy.
		let (meta, kv) = {
			let applied = self.shared.state.read();
			let meta = Meta {
				last_log_id:     applied.last_applied,
				last_membership: applied.membership.clone(),
			};
			(meta, applied.kv.clone())
		};
		let this = self.clone();
		let meta2 = meta.clone();
		let data = tokio::task::spawn_blocking(move || {
			let data = kv.to_snapshot();
			this.persist_snapshot(&meta2, &data).map(|()| data)
		})
		.await
		.map_err(io::Error::other)??;
		*self.shared.snapshot.lock() = Some((meta.clone(), data.clone()));
		Ok(Snapshot { meta, snapshot: data })
	}
}

impl RaftStateMachine<TypeConfig> for StateMachineStore {
	type SnapshotBuilder = Self;
	type SnapshotData = Vec<u8>;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn applied_state(&mut self) -> io::Result<(Option<LogId>, Membership)> {
		let applied = self.shared.state.read();
		Ok((applied.last_applied, applied.membership.clone()))
	}

	/// Applies the batch under one state lock, then publishes the revision and
	/// watch events, and only then answers the proposers.
	async fn apply<S>(&mut self, entries: S) -> io::Result<()>
	where
		S: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + Send,
	{
		let entries: Vec<EntryResponder<TypeConfig>> = entries.try_collect().await?;
		let mut events = Vec::new();
		let mut replies = Vec::with_capacity(entries.len());
		let revision = {
			let mut applied = self.shared.state.write();
			for (entry, responder) in entries {
				applied.last_applied = Some(entry.log_id);
				let reply = match entry.payload {
					EntryPayload::Blank => Vec::new(),
					EntryPayload::Normal(command) => applied.kv.apply(&command, &mut events),
					EntryPayload::Membership(membership) => {
						applied.membership = Membership::new(Some(entry.log_id), membership);
						Vec::new()
					},
				};
				if let Some(responder) = responder {
					replies.push((responder, reply));
				}
			}
			applied.kv.revision()
		};
		self.shared.revision.send_replace(revision);
		self.notify(events);
		for (responder, reply) in replies {
			responder.send(reply);
		}
		Ok(())
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		self.clone()
	}

	async fn install_snapshot(&mut self, meta: &Meta, data: Vec<u8>) -> io::Result<()> {
		let kv = KvState::from_snapshot(&data).map_err(io::Error::other)?;
		let this = self.clone();
		let persisted = meta.clone();
		let data = tokio::task::spawn_blocking(move || {
			this.persist_snapshot(&persisted, &data).map(|()| data)
		})
		.await
		.map_err(io::Error::other)??;
		let (events, revision) = {
			let mut applied = self.shared.state.write();
			let events = applied.kv.diff(&kv);
			applied.kv = kv;
			applied.last_applied = meta.last_log_id;
			applied.membership = meta.last_membership.clone();
			(events, applied.kv.revision())
		};
		*self.shared.snapshot.lock() = Some((meta.clone(), data));
		self.shared.revision.send_replace(revision);
		self.notify(events);
		Ok(())
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftStateMachine"
	)]
	async fn get_current_snapshot(&mut self) -> io::Result<Option<Snapshot>> {
		Ok(self
			.shared
			.snapshot
			.lock()
			.as_ref()
			.map(|(meta, data)| Snapshot { meta: meta.clone(), snapshot: data.clone() }))
	}
}
