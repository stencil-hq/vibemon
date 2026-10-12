//! Durable raft log, vote, and committed index under `<data>/raft/`.
//!
//! The log is an append-only record file (`log.bin`): each record is
//! `len: u32 LE | fnv1a64(payload): u64 LE | payload (JSON)`. Records are
//! appended entries, truncations, and purges; replay rebuilds the in-memory
//! index and drops a torn tail. The file is compacted after purges. Vote and
//! committed id are small JSON files replaced atomically. Log and vote writes
//! are fsync'd before openraft is told they are durable; the committed id is
//! not (see [`LogStore::save_committed`]).

use std::{
	collections::BTreeMap,
	fmt::Debug,
	fs::{self, File, OpenOptions},
	io::{self, Read, Write},
	ops::RangeBounds,
	path::{Path, PathBuf},
	sync::Arc,
};

use openraft::{
	ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader, StorageError, Vote,
	storage::{LogFlushed, RaftLogStorage},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::raft::{LogEntry, TypeConfig};
use crate::meta::NodeId;

const LOG_FILE: &str = "log.bin";
const VOTE_FILE: &str = "vote.json";
const COMMITTED_FILE: &str = "committed.json";
const HEADER: usize = 12;

/// One record of `log.bin`.
#[derive(Serialize, Deserialize)]
enum Record {
	Entry(LogEntry),
	/// Remove entries with index >= this.
	Truncate(u64),
	/// Remove entries up to and including this id.
	Purge(LogId<NodeId>),
}

/// openraft log storage; clones share the same state (used as the log reader
/// too).
#[derive(Clone)]
pub struct LogStore {
	inner: Arc<Mutex<Inner>>,
}

struct Inner {
	dir:         PathBuf,
	file:        File,
	/// Records currently in `log.bin` (drives compaction).
	records:     usize,
	entries:     BTreeMap<u64, LogEntry>,
	last_purged: Option<LogId<NodeId>>,
	vote:        Option<Vote<NodeId>>,
	committed:   Option<LogId<NodeId>>,
}

impl LogStore {
	/// Opens (or creates) the log in `dir`, replaying records and dropping a
	/// torn tail.
	pub fn open(dir: &Path) -> io::Result<Self> {
		fs::create_dir_all(dir)?;
		let path = dir.join(LOG_FILE);
		let mut file = OpenOptions::new()
			.create(true)
			.read(true)
			.append(true)
			.open(&path)?;
		let mut bytes = Vec::new();
		file.read_to_end(&mut bytes)?;
		let mut entries = BTreeMap::new();
		let mut last_purged = None;
		let mut records = 0;
		let mut offset = 0;
		while let Some((record, len)) = decode_record(&bytes[offset..]) {
			offset += len;
			records += 1;
			match record {
				Record::Entry(entry) => {
					entries.insert(entry.log_id.index, entry);
				},
				Record::Truncate(index) => {
					entries.split_off(&index);
				},
				Record::Purge(log_id) => {
					entries = entries.split_off(&(log_id.index + 1));
					last_purged = Some(log_id);
				},
			}
		}
		if offset < bytes.len() {
			tracing::warn!(dropped = bytes.len() - offset, "raft log has a torn tail; truncating");
			file.set_len(offset as u64)?;
			file.sync_all()?;
		}
		let vote = read_json(&dir.join(VOTE_FILE))?;
		// Written without fsync: after a power loss it may be empty or torn.
		let committed = read_json(&dir.join(COMMITTED_FILE)).unwrap_or_else(|error| {
			tracing::warn!(%error, "raft committed index unreadable; re-learning it");
			None
		});
		let inner =
			Inner { dir: dir.to_owned(), file, records, entries, last_purged, vote, committed };
		Ok(Self { inner: Arc::new(Mutex::new(inner)) })
	}

	/// Runs blocking file work off the async executor.
	async fn blocking<T, F>(&self, f: F) -> Result<T, io::Error>
	where
		T: Send + 'static,
		F: FnOnce(&mut Inner) -> io::Result<T> + Send + 'static,
	{
		let inner = self.inner.clone();
		tokio::task::spawn_blocking(move || f(&mut inner.lock()))
			.await
			.map_err(io::Error::other)?
	}
}

impl Inner {
	fn write_records(&mut self, records: &[Record]) -> io::Result<()> {
		let mut buf = Vec::new();
		for record in records {
			encode_record(record, &mut buf);
		}
		self.file.write_all(&buf)?;
		self.file.sync_data()?;
		self.records += records.len();
		Ok(())
	}

	/// Rewrites `log.bin` with only live state once dead records dominate.
	fn maybe_compact(&mut self) -> io::Result<()> {
		if self.records <= 2 * self.entries.len() + 1024 {
			return Ok(());
		}
		let mut buf = Vec::new();
		let mut records = 0;
		if let Some(purged) = self.last_purged {
			encode_record(&Record::Purge(purged), &mut buf);
			records += 1;
		}
		for entry in self.entries.values() {
			encode_record(&Record::Entry(entry.clone()), &mut buf);
			records += 1;
		}
		let path = self.dir.join(LOG_FILE);
		replace_file(&path, &buf)?;
		self.file = OpenOptions::new().read(true).append(true).open(&path)?;
		self.records = records;
		Ok(())
	}

	fn last_log_id(&self) -> Option<LogId<NodeId>> {
		self
			.entries
			.values()
			.next_back()
			.map(|e| e.log_id)
			.or(self.last_purged)
	}
}

fn fnv1a64(bytes: &[u8]) -> u64 {
	bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
		(hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
	})
}

fn encode_record(record: &Record, buf: &mut Vec<u8>) {
	let payload = serde_json::to_vec(record).expect("raft log record serializes");
	let len = u32::try_from(payload.len()).expect("raft log record below 4 GiB");
	buf.extend_from_slice(&len.to_le_bytes());
	buf.extend_from_slice(&fnv1a64(&payload).to_le_bytes());
	buf.extend_from_slice(&payload);
}

/// Decodes the record at the start of `bytes`; `None` on end of data or a
/// torn/corrupt record.
fn decode_record(bytes: &[u8]) -> Option<(Record, usize)> {
	let header = bytes.get(..HEADER)?;
	let len = u32::from_le_bytes(header[..4].try_into().ok()?) as usize;
	let hash = u64::from_le_bytes(header[4..].try_into().ok()?);
	let payload = bytes.get(HEADER..HEADER + len)?;
	if fnv1a64(payload) != hash {
		return None;
	}
	Some((serde_json::from_slice(payload).ok()?, HEADER + len))
}

/// Atomically replaces `path` with `bytes` (temp file, fsync, rename, fsync
/// dir).
pub fn replace_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
	write_and_rename(path, bytes, true)
}

/// Replaces `path` with `bytes` via a temp file and rename, optionally
/// fsyncing the file and its directory.
fn write_and_rename(path: &Path, bytes: &[u8], durable: bool) -> io::Result<()> {
	let tmp = path.with_extension("tmp");
	{
		let mut file = File::create(&tmp)?;
		file.write_all(bytes)?;
		if durable {
			file.sync_all()?;
		}
	}
	fs::rename(&tmp, path)?;
	if durable && let Some(dir) = path.parent() {
		File::open(dir)?.sync_all()?;
	}
	Ok(())
}

fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
	match fs::read(path) {
		Ok(bytes) => serde_json::from_slice(&bytes)
			.map(Some)
			.map_err(io::Error::other),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error),
	}
}

fn io_err(
	subject: ErrorSubject<NodeId>,
	verb: ErrorVerb,
) -> impl FnOnce(io::Error) -> StorageError<NodeId> {
	move |error| StorageError::from_io_error(subject, verb, error)
}

impl RaftLogReader<TypeConfig> for LogStore {
	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogReader"
	)]
	async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
		&mut self,
		range: RB,
	) -> Result<Vec<LogEntry>, StorageError<NodeId>> {
		Ok(self
			.inner
			.lock()
			.entries
			.range(range)
			.map(|(_, entry)| entry.clone())
			.collect())
	}
}

impl RaftLogStorage<TypeConfig> for LogStore {
	type LogReader = Self;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
		let inner = self.inner.lock();
		Ok(LogState {
			last_purged_log_id: inner.last_purged,
			last_log_id:        inner.last_log_id(),
		})
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn get_log_reader(&mut self) -> Self::LogReader {
		self.clone()
	}

	async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
		let vote = *vote;
		self
			.blocking(move |inner| {
				replace_file(
					&inner.dir.join(VOTE_FILE),
					&serde_json::to_vec(&vote).map_err(io::Error::other)?,
				)?;
				inner.vote = Some(vote);
				Ok(())
			})
			.await
			.map_err(io_err(ErrorSubject::Vote, ErrorVerb::Write))
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
		Ok(self.inner.lock().vote)
	}

	/// Persists the committed id without fsync: openraft only uses it to
	/// re-apply logs at startup before a leader is known, and a stale or lost
	/// value is safe (entries are re-committed and re-applied). It never runs
	/// ahead of the durable log, which is fsync'd before commit. Saves two
	/// fsyncs per KV write.
	async fn save_committed(
		&mut self,
		committed: Option<LogId<NodeId>>,
	) -> Result<(), StorageError<NodeId>> {
		self
			.blocking(move |inner| {
				if inner.committed == committed {
					return Ok(());
				}
				let bytes = serde_json::to_vec(&committed).map_err(io::Error::other)?;
				write_and_rename(&inner.dir.join(COMMITTED_FILE), &bytes, false)?;
				inner.committed = committed;
				Ok(())
			})
			.await
			.map_err(io_err(ErrorSubject::Store, ErrorVerb::Write))
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
		Ok(self.inner.lock().committed)
	}

	async fn append<I>(
		&mut self,
		entries: I,
		callback: LogFlushed<TypeConfig>,
	) -> Result<(), StorageError<NodeId>>
	where
		I: IntoIterator<Item = LogEntry> + Send,
		I::IntoIter: Send,
	{
		let entries: Vec<LogEntry> = entries.into_iter().collect();
		let result = self
			.blocking(move |inner| {
				let records: Vec<Record> = entries.iter().cloned().map(Record::Entry).collect();
				inner.write_records(&records)?;
				for entry in entries {
					inner.entries.insert(entry.log_id.index, entry);
				}
				Ok(())
			})
			.await;
		let reply = match &result {
			Ok(()) => Ok(()),
			Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
		};
		callback.log_io_completed(reply);
		result.map_err(io_err(ErrorSubject::Logs, ErrorVerb::Write))
	}

	async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
		self
			.blocking(move |inner| {
				inner.write_records(&[Record::Truncate(log_id.index)])?;
				inner.entries.split_off(&log_id.index);
				Ok(())
			})
			.await
			.map_err(io_err(ErrorSubject::Logs, ErrorVerb::Delete))
	}

	async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
		self
			.blocking(move |inner| {
				inner.write_records(&[Record::Purge(log_id)])?;
				inner.entries = inner.entries.split_off(&(log_id.index + 1));
				inner.last_purged = Some(log_id);
				inner.maybe_compact()
			})
			.await
			.map_err(io_err(ErrorSubject::Logs, ErrorVerb::Delete))
	}
}

#[cfg(test)]
mod tests {
	use openraft::{CommittedLeaderId, EntryPayload};

	use super::*;

	fn entry(term: u64, index: u64) -> LogEntry {
		LogEntry {
			log_id:  LogId::new(CommittedLeaderId::new(term, NodeId(1)), index),
			payload: EntryPayload::Blank,
		}
	}

	fn indexes(store: &LogStore) -> Vec<u64> {
		store.inner.lock().entries.keys().copied().collect()
	}

	fn append_sync(store: &LogStore, entries: Vec<LogEntry>) {
		let mut inner = store.inner.lock();
		let records: Vec<Record> = entries.iter().cloned().map(Record::Entry).collect();
		inner.write_records(&records).unwrap();
		for e in entries {
			inner.entries.insert(e.log_id.index, e);
		}
	}

	#[tokio::test]
	async fn replay_applies_truncate_and_purge_and_keeps_vote() {
		let dir = tempfile::tempdir().unwrap();
		{
			let mut store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, (1..=10).map(|i| entry(1, i)).collect());
			store.truncate(entry(1, 8).log_id).await.unwrap();
			append_sync(&store, vec![entry(2, 8)]);
			store.purge(entry(1, 3).log_id).await.unwrap();
			store.save_vote(&Vote::new(2, NodeId(7))).await.unwrap();
			store
				.save_committed(Some(entry(2, 8).log_id))
				.await
				.unwrap();
		}
		let mut store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![4, 5, 6, 7, 8]);
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_purged_log_id, Some(entry(1, 3).log_id));
		assert_eq!(state.last_log_id, Some(entry(2, 8).log_id));
		assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(2, NodeId(7))));
		assert_eq!(store.read_committed().await.unwrap(), Some(entry(2, 8).log_id));
	}

	/// The committed id is written without fsync, so a power loss can leave it
	/// empty: the log must still open, with the committed id unknown.
	#[tokio::test]
	async fn torn_committed_index_is_treated_as_unknown() {
		let dir = tempfile::tempdir().unwrap();
		{
			let mut store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, vec![entry(1, 1), entry(1, 2)]);
			store
				.save_committed(Some(entry(1, 2).log_id))
				.await
				.unwrap();
		}
		fs::write(dir.path().join(COMMITTED_FILE), b"").unwrap();
		let mut store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![1, 2]);
		assert_eq!(store.read_committed().await.unwrap(), None);
	}

	#[tokio::test]
	async fn torn_tail_is_dropped_and_log_stays_appendable() {
		let dir = tempfile::tempdir().unwrap();
		{
			let store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, vec![entry(1, 1), entry(1, 2)]);
		}
		let path = dir.path().join(LOG_FILE);
		let full = fs::read(&path).unwrap();
		fs::write(&path, &full[..full.len() - 5]).unwrap();
		{
			let store = LogStore::open(dir.path()).unwrap();
			assert_eq!(indexes(&store), vec![1]);
			append_sync(&store, vec![entry(1, 2), entry(1, 3)]);
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![1, 2, 3]);
	}

	#[tokio::test]
	async fn purged_everything_reports_purged_id_as_last() {
		let dir = tempfile::tempdir().unwrap();
		let mut store = LogStore::open(dir.path()).unwrap();
		append_sync(&store, vec![entry(1, 1), entry(1, 2)]);
		store.purge(entry(1, 2).log_id).await.unwrap();
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_log_id, Some(entry(1, 2).log_id));
		let entries = store.try_get_log_entries(0..10).await.unwrap();
		assert!(entries.is_empty(), "unexpected entries: {entries:?}");
	}

	#[tokio::test]
	async fn compaction_preserves_state() {
		let dir = tempfile::tempdir().unwrap();
		{
			let mut store = LogStore::open(dir.path()).unwrap();
			for i in 1..=3000 {
				append_sync(&store, vec![entry(1, i)]);
				if i % 100 == 0 {
					store.purge(entry(1, i - 10).log_id).await.unwrap();
				}
			}
			assert!(store.inner.lock().records < 1200, "compaction ran");
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), (2991..=3000).collect::<Vec<_>>());
		assert_eq!(store.inner.lock().last_purged, Some(entry(1, 2990).log_id));
	}
}
