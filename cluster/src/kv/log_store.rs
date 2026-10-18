//! Durable raft log, vote, and committed index under `<data>/raft/`.
//!
//! The log lives in two preallocated slot files, `log.0` and `log.1`. A slot
//! is a header page (magic, epoch, checksum), then records from [`BODY`] on
//! (`len: u32 LE | fnv1a64(payload): u64 LE | payload (JSON)`: appended
//! entries, truncations, and purges), then zeros up to its end. Open replays
//! the slot with the highest valid epoch up to the first record that does not
//! decode (the zeros, or a torn tail, which is zeroed) and rebuilds the
//! in-memory index.
//!
//! Appends overwrite those zeros in place and are fdatasync'd before openraft
//! is told they are durable. The file's size and block map stay unchanged, so
//! that `fdatasync` needs no filesystem journal commit: on ext4
//! (`data=ordered`) a commit also waits for every other file's newly
//! allocated dirty data, which stalled KV writes for over a minute on a disk
//! shared with template builds. Only growing a slot (zero-fill, fsync)
//! commits the journal.
//!
//! Compaction (after purges) writes the live records into the other slot,
//! zeroes its stale tail, syncs, and only then writes that slot's header with
//! the next epoch, so a crash at any point leaves a complete slot with the
//! highest valid epoch. A single-file `log.bin` of earlier versions is
//! migrated on open. The vote is a small JSON file replaced atomically with
//! fsync; the committed id a fixed-size record overwritten in place without
//! fsync (see [`LogStore::save_committed`]).

use std::{
	collections::BTreeMap,
	fmt::Debug,
	fs::{self, File, OpenOptions},
	io::{self, Seek, SeekFrom, Write},
	ops::RangeBounds,
	path::{Path, PathBuf},
	sync::Arc,
};

use openraft::{
	LogState, RaftLogReader,
	storage::{IOFlushed, RaftLogStorage},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::raft::{LogEntry, LogId, TypeConfig, Vote};

const SLOT_FILES: [&str; 2] = ["log.0", "log.1"];
/// The single append-only log of earlier versions (migrated on open).
const LEGACY_LOG_FILE: &str = "log.bin";
const VOTE_FILE: &str = "vote.json";
const COMMITTED_FILE: &str = "committed.bin";
/// The committed id file of earlier versions (read once, then removed).
const LEGACY_COMMITTED_FILE: &str = "committed.json";
/// Record framing: payload length + checksum.
const HEADER: usize = 12;
const SLOT_MAGIC: &[u8; 8] = b"vmonrlg1";
/// Slot header: magic, epoch, checksum of both.
const SLOT_HEADER: usize = 24;
/// Offset of a slot's first record (the header owns the first page).
const BODY: u64 = 4096;
/// Initial size and growth step of a slot.
const SLOT_GROWTH: u64 = 8 << 20;
/// Size of `committed.bin`: one framed record, zero-padded.
const COMMITTED_LEN: usize = 256;
/// Capacity the reused encode buffer keeps between writes.
const ENCODE_BUF_KEEP: usize = 1 << 20;
static ZEROS: [u8; 64 << 10] = [0; 64 << 10];

/// One record of the log, as decoded on replay.
#[derive(Debug, PartialEq, Deserialize)]
pub(super) enum Record {
	Entry(LogEntry),
	/// Remove entries with index >= this.
	Truncate(u64),
	/// Remove entries up to and including this id.
	Purge(LogId),
}

/// A [`Record`] to write, borrowing its entry; encodes to the same bytes.
#[derive(Serialize)]
#[serde(rename = "Record")]
pub(super) enum RecordRef<'a> {
	Entry(&'a LogEntry),
	Truncate(u64),
	Purge(&'a LogId),
}

/// openraft log storage; clones share the same state (used as the log reader
/// too).
#[derive(Clone)]
pub struct LogStore {
	inner: Arc<Mutex<Inner>>,
}

struct Inner {
	slots:          [Slot; 2],
	/// The slot appends go to.
	active:         usize,
	/// Records in the active slot (drives compaction).
	records:        usize,
	entries:        BTreeMap<u64, LogEntry>,
	last_purged:    Option<LogId>,
	dir:            PathBuf,
	vote:           Option<Vote>,
	committed:      Option<LogId>,
	committed_file: File,
	/// Encode buffer reused across writes.
	buf:            Vec<u8>,
}

/// One preallocated log file.
struct Slot {
	file:  File,
	/// Epoch of its valid header; `None` when absent or torn.
	epoch: Option<u64>,
	/// File length; every byte up to it is written (allocated).
	len:   u64,
	/// End of its contents (zeros follow): the append offset of the active
	/// slot.
	end:   u64,
}

/// Log state rebuilt from records.
#[derive(Default)]
struct Replayed {
	entries:     BTreeMap<u64, LogEntry>,
	last_purged: Option<LogId>,
	records:     usize,
	/// Bytes of decodable records.
	len:         usize,
}

impl LogStore {
	/// Opens (or creates) the log in `dir`, replaying the newest slot,
	/// zeroing a torn tail, and migrating a `log.bin` of earlier versions.
	pub fn open(dir: &Path) -> io::Result<Self> {
		fs::create_dir_all(dir)?;
		let paths = SLOT_FILES.map(|name| dir.join(name));
		let [first, second] = [read_optional(&paths[0])?, read_optional(&paths[1])?];
		let epochs = [&first, &second].map(|bytes| bytes.as_deref().and_then(decode_slot_header));
		let active = match epochs {
			[Some(a), Some(b)] => Some(usize::from(b > a)),
			[Some(_), None] => Some(0),
			[None, Some(_)] => Some(1),
			[None, None] => None,
		};
		let committed_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(dir.join(COMMITTED_FILE))?;
		let committed = read_committed(dir)?;
		let vote = read_json(&dir.join(VOTE_FILE))?;
		let saved = Saved { dir: dir.to_owned(), vote, committed, committed_file };
		let legacy = dir.join(LEGACY_LOG_FILE);
		let mut inner = if let Some(active) = active {
			let mut state = Replayed::default();
			let mut slots = Vec::with_capacity(2);
			for (index, bytes) in [first, second].into_iter().enumerate() {
				let Some(bytes) = bytes else {
					slots.push(Slot::create(&paths[index], SLOT_GROWTH)?);
					sync_dir(dir)?;
					continue;
				};
				let body = usize::try_from(BODY)
					.ok()
					.and_then(|body| bytes.get(body..))
					.unwrap_or_default();
				let dirty = dirty_len(body);
				let mut slot = Slot::open(&paths[index], epochs[index], bytes.len() as u64)?;
				slot.end = BODY + dirty as u64;
				if index == active {
					state = replay(body);
					if dirty > state.len {
						tracing::warn!(
							dropped = dirty - state.len,
							"raft log has a torn tail; zeroing it"
						);
						slot.zero(BODY + state.len as u64, slot.end)?;
						slot.file.sync_data()?;
					}
					slot.end = BODY + state.len as u64;
				}
				slots.push(slot);
			}
			let Ok(slots) = <[Slot; 2]>::try_from(slots) else {
				unreachable!("two slots were opened")
			};
			Inner::new(slots, active, state, saved)
		} else {
			// Fresh, or the slots of an interrupted first open or migration
			// (never written to before a header was valid).
			let state = match fs::read(&legacy) {
				Ok(bytes) => {
					let state = replay(&bytes);
					if state.len < bytes.len() {
						tracing::warn!(
							dropped = bytes.len() - state.len,
							"raft log has a torn tail; dropping it"
						);
					}
					state
				},
				Err(error) if error.kind() == io::ErrorKind::NotFound => Replayed::default(),
				Err(error) => return Err(error),
			};
			let slots = [Slot::create(&paths[0], SLOT_GROWTH)?, Slot::create(&paths[1], SLOT_GROWTH)?];
			sync_dir(dir)?;
			// Slot 1 stands in as the active slot of epoch 0: the rewrite seals
			// slot 0 with epoch 1.
			let mut inner = Inner::new(slots, 1, state, saved);
			inner.rewrite()?;
			inner
		};
		match fs::remove_file(&legacy) {
			Ok(()) => sync_dir(dir)?,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
		inner.write_committed(inner.committed)?;
		match fs::remove_file(dir.join(LEGACY_COMMITTED_FILE)) {
			Ok(()) => {},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error),
		}
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

/// What [`Inner`] keeps beside the log.
struct Saved {
	dir:            PathBuf,
	vote:           Option<Vote>,
	committed:      Option<LogId>,
	committed_file: File,
}

impl Inner {
	fn new(slots: [Slot; 2], active: usize, state: Replayed, saved: Saved) -> Self {
		Self {
			slots,
			active,
			records: state.records,
			entries: state.entries,
			last_purged: state.last_purged,
			dir: saved.dir,
			vote: saved.vote,
			committed: saved.committed,
			committed_file: saved.committed_file,
			buf: Vec::new(),
		}
	}

	/// Appends `records` to the active slot and fdatasyncs them.
	fn write_records<'a>(
		&mut self,
		records: impl IntoIterator<Item = RecordRef<'a>>,
	) -> io::Result<()> {
		let mut buf = std::mem::take(&mut self.buf);
		let mut count = 0;
		for record in records {
			encode_record(&record, &mut buf);
			count += 1;
		}
		let result = self.write_active(&buf);
		buf.clear();
		buf.shrink_to(ENCODE_BUF_KEEP);
		self.buf = buf;
		result?;
		self.records += count;
		Ok(())
	}

	/// Appends encoded records to the active slot and fdatasyncs them.
	fn write_active(&mut self, bytes: &[u8]) -> io::Result<()> {
		let slot = &mut self.slots[self.active];
		let end = slot.end + bytes.len() as u64;
		slot.reserve(end)?;
		slot.write_at(slot.end, bytes)?;
		slot.file.sync_data()?;
		slot.end = end;
		Ok(())
	}

	/// Rewrites the log with only live state once dead records dominate.
	fn maybe_compact(&mut self) -> io::Result<()> {
		if self.records <= 2 * self.entries.len() + 1024 {
			return Ok(());
		}
		self.rewrite()
	}

	/// Writes the live state into the other slot, then seals it with the next
	/// epoch, which makes it the slot replayed on open.
	fn rewrite(&mut self) -> io::Result<()> {
		let mut buf = std::mem::take(&mut self.buf);
		let mut records = 0;
		if let Some(purged) = &self.last_purged {
			encode_record(&RecordRef::Purge(purged), &mut buf);
			records += 1;
		}
		for entry in self.entries.values() {
			encode_record(&RecordRef::Entry(entry), &mut buf);
			records += 1;
		}
		let result = self.write_rewrite(&buf, records);
		buf.clear();
		buf.shrink_to(ENCODE_BUF_KEEP);
		self.buf = buf;
		result
	}

	/// Writes `buf` (`records` encoded records) into the inactive slot and
	/// seals it as the new active slot.
	fn write_rewrite(&mut self, buf: &[u8], records: usize) -> io::Result<()> {
		let epoch = self.slots[self.active].epoch.unwrap_or(0) + 1;
		let target = 1 - self.active;
		let slot = &mut self.slots[target];
		let end = BODY + buf.len() as u64;
		slot.reserve(end)?;
		slot.write_at(BODY, buf)?;
		if slot.end > end {
			slot.zero(end, slot.end)?;
		}
		slot.file.sync_data()?;
		slot.seal(epoch)?;
		slot.end = end;
		self.active = target;
		self.records = records;
		Ok(())
	}

	/// Overwrites `committed.bin` in place (no fsync).
	fn write_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
		let mut buf = std::mem::take(&mut self.buf);
		encode_framed(&committed, &mut buf);
		let result = if buf.len() > COMMITTED_LEN {
			Err(io::Error::other("raft committed id record too large"))
		} else {
			buf.resize(COMMITTED_LEN, 0);
			self
				.committed_file
				.seek(SeekFrom::Start(0))
				.and_then(|_| self.committed_file.write_all(&buf))
		};
		buf.clear();
		self.buf = buf;
		result
	}

	fn last_log_id(&self) -> Option<LogId> {
		self
			.entries
			.values()
			.next_back()
			.map(|e| e.log_id)
			.or(self.last_purged)
	}
}

impl Slot {
	/// Creates (or truncates) `path` as an unsealed slot of `len` written
	/// zero bytes.
	fn create(path: &Path, len: u64) -> io::Result<Self> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.open(path)?;
		let mut slot = Self { file, epoch: None, len: 0, end: BODY };
		slot.grow(len)?;
		Ok(slot)
	}

	/// Opens an existing slot of `len` bytes.
	fn open(path: &Path, epoch: Option<u64>, len: u64) -> io::Result<Self> {
		let file = OpenOptions::new().read(true).write(true).open(path)?;
		Ok(Self { file, epoch, len, end: BODY })
	}

	fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
		self.file.seek(SeekFrom::Start(offset))?;
		self.file.write_all(bytes)
	}

	/// Overwrites `[from, to)` with zeros.
	fn zero(&mut self, from: u64, to: u64) -> io::Result<()> {
		self.file.seek(SeekFrom::Start(from))?;
		let mut left = to.saturating_sub(from);
		while left > 0 {
			let chunk = usize::try_from(left).map_or(ZEROS.len(), |left| left.min(ZEROS.len()));
			self.file.write_all(&ZEROS[..chunk])?;
			left -= chunk as u64;
		}
		Ok(())
	}

	/// Makes room for `size` bytes (plus a growth step) of written zeros.
	fn reserve(&mut self, size: u64) -> io::Result<()> {
		if size <= self.len {
			return Ok(());
		}
		self.grow((size + SLOT_GROWTH).next_multiple_of(SLOT_GROWTH))
	}

	/// Extends the file to `len` with written zeros, durably: the one
	/// operation that changes its block map, so later overwrites in place
	/// sync without a journal commit (unwritten preallocation would not do).
	fn grow(&mut self, len: u64) -> io::Result<()> {
		self.zero(self.len, len)?;
		self.file.sync_all()?;
		self.len = len;
		Ok(())
	}

	/// Writes and syncs the header with `epoch`.
	fn seal(&mut self, epoch: u64) -> io::Result<()> {
		self.write_at(0, &slot_header(epoch))?;
		self.file.sync_data()?;
		self.epoch = Some(epoch);
		Ok(())
	}
}

fn slot_header(epoch: u64) -> [u8; SLOT_HEADER] {
	let mut header = [0; SLOT_HEADER];
	header[..8].copy_from_slice(SLOT_MAGIC);
	header[8..16].copy_from_slice(&epoch.to_le_bytes());
	let hash = fnv1a64(&header[..16]);
	header[16..].copy_from_slice(&hash.to_le_bytes());
	header
}

/// The epoch of a slot whose contents start with `bytes`; `None` when the
/// header is missing or torn.
fn decode_slot_header(bytes: &[u8]) -> Option<u64> {
	let header = bytes.get(..SLOT_HEADER)?;
	if header[..8] != SLOT_MAGIC[..] || header[16..] != fnv1a64(&header[..16]).to_le_bytes() {
		return None;
	}
	header[8..16].try_into().ok().map(u64::from_le_bytes)
}

/// Applies the records at the start of `bytes`, up to the first that does
/// not decode.
fn replay(bytes: &[u8]) -> Replayed {
	let mut state = Replayed::default();
	while let Some((record, len)) = decode_record(&bytes[state.len..]) {
		state.len += len;
		state.records += 1;
		match record {
			Record::Entry(entry) => {
				state.entries.insert(entry.log_id.index, entry);
			},
			Record::Truncate(index) => {
				state.entries.split_off(&index);
			},
			Record::Purge(log_id) => {
				state.entries = state.entries.split_off(&(log_id.index + 1));
				state.last_purged = Some(log_id);
			},
		}
	}
	state
}

/// Length of `bytes` without its trailing zeros.
fn dirty_len(bytes: &[u8]) -> usize {
	bytes
		.iter()
		.rposition(|byte| *byte != 0)
		.map_or(0, |last| last + 1)
}

fn fnv1a64(bytes: &[u8]) -> u64 {
	bytes.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
		(hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3)
	})
}

/// Appends the JSON of `value` to `buf`, framed with its length and checksum.
fn encode_framed<T: Serialize + ?Sized>(value: &T, buf: &mut Vec<u8>) {
	let start = buf.len();
	buf.extend_from_slice(&[0; HEADER]);
	serde_json::to_writer(&mut *buf, value).expect("raft log record serializes");
	let payload = &buf[start + HEADER..];
	let len = u32::try_from(payload.len()).expect("raft log record below 4 GiB");
	let hash = fnv1a64(payload);
	buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
	buf[start + 4..start + HEADER].copy_from_slice(&hash.to_le_bytes());
}

/// The framed payload at the start of `bytes` and the framed length; `None`
/// on end of data or a torn/corrupt frame (zeros never form a frame: the
/// empty payload's checksum is not zero).
fn unframe(bytes: &[u8]) -> Option<(&[u8], usize)> {
	let header = bytes.get(..HEADER)?;
	let len = u32::from_le_bytes(header[..4].try_into().ok()?) as usize;
	let hash = u64::from_le_bytes(header[4..].try_into().ok()?);
	let payload = bytes.get(HEADER..HEADER + len)?;
	(fnv1a64(payload) == hash).then_some((payload, HEADER + len))
}

fn encode_record(record: &RecordRef<'_>, buf: &mut Vec<u8>) {
	encode_framed(record, buf);
}

/// Decodes the record at the start of `bytes`; `None` on end of data or a
/// torn/corrupt record.
fn decode_record(bytes: &[u8]) -> Option<(Record, usize)> {
	let (payload, len) = unframe(bytes)?;
	Some((serde_json::from_slice(payload).ok()?, len))
}

/// The committed id (`committed.bin` exists); unknown, and re-learned, when
/// unreadable. A never written `committed.bin` falls back to the
/// `committed.json` of earlier versions.
fn read_committed(dir: &Path) -> io::Result<Option<LogId>> {
	let bytes = fs::read(dir.join(COMMITTED_FILE))?;
	let committed = if bytes.is_empty() {
		read_json(&dir.join(LEGACY_COMMITTED_FILE)).map(Option::flatten)
	} else {
		unframe(&bytes)
			.and_then(|(payload, _)| serde_json::from_slice(payload).ok())
			.ok_or_else(|| io::Error::other("torn record"))
	};
	Ok(committed.unwrap_or_else(|error| {
		tracing::warn!(%error, "raft committed index unreadable; re-learning it");
		None
	}))
}

/// Atomically replaces `path` with `bytes` (temp file, fsync, rename, fsync
/// dir).
pub fn replace_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
	let tmp = path.with_extension("tmp");
	{
		let mut file = File::create(&tmp)?;
		file.write_all(bytes)?;
		file.sync_all()?;
	}
	fs::rename(&tmp, path)?;
	match path.parent() {
		Some(dir) => sync_dir(dir),
		None => Ok(()),
	}
}

fn sync_dir(dir: &Path) -> io::Result<()> {
	File::open(dir)?.sync_all()
}

/// The contents of `path`; `None` when it does not exist.
fn read_optional(path: &Path) -> io::Result<Option<Vec<u8>>> {
	match fs::read(path) {
		Ok(bytes) => Ok(Some(bytes)),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error),
	}
}

fn read_json<T: DeserializeOwned>(path: &Path) -> io::Result<Option<T>> {
	read_optional(path)?
		.map(|bytes| serde_json::from_slice(&bytes).map_err(io::Error::other))
		.transpose()
}

impl RaftLogReader<TypeConfig> for LogStore {
	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogReader"
	)]
	async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
		&mut self,
		range: RB,
	) -> io::Result<Vec<LogEntry>> {
		Ok(self
			.inner
			.lock()
			.entries
			.range(range)
			.map(|(_, entry)| entry.clone())
			.collect())
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogReader"
	)]
	async fn read_vote(&mut self) -> io::Result<Option<Vote>> {
		Ok(self.inner.lock().vote)
	}
}

impl RaftLogStorage<TypeConfig> for LogStore {
	type LogReader = Self;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn get_log_state(&mut self) -> io::Result<LogState<TypeConfig>> {
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

	async fn save_vote(&mut self, vote: &Vote) -> io::Result<()> {
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
	}

	/// Persists the committed id in place without fsync: openraft only uses
	/// it to re-apply logs at startup before a leader is known, and a stale or
	/// lost value is safe (entries are re-committed and re-applied). It never
	/// runs ahead of the durable log, which is fsync'd before commit. Saves two
	/// fsyncs per KV write, and the overwrite in place creates, renames, and
	/// allocates nothing.
	async fn save_committed(&mut self, committed: Option<LogId>) -> io::Result<()> {
		self
			.blocking(move |inner| {
				if inner.committed == committed {
					return Ok(());
				}
				inner.write_committed(committed)?;
				inner.committed = committed;
				Ok(())
			})
			.await
	}

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftLogStorage"
	)]
	async fn read_committed(&mut self) -> io::Result<Option<LogId>> {
		Ok(self.inner.lock().committed)
	}

	/// Writes and fdatasyncs every entry openraft hands over in one call (it
	/// merges queued appends, so concurrent writes share one sync), then
	/// reports them flushed.
	async fn append<I>(&mut self, entries: I, callback: IOFlushed<TypeConfig>) -> io::Result<()>
	where
		I: IntoIterator<Item = LogEntry> + Send,
		I::IntoIter: Send,
	{
		let entries: Vec<LogEntry> = entries.into_iter().collect();
		let result = self
			.blocking(move |inner| {
				inner.write_records(entries.iter().map(RecordRef::Entry))?;
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
		callback.io_completed(reply);
		result
	}

	async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> io::Result<()> {
		let since = last_log_id.map_or(0, |log_id| log_id.index + 1);
		self
			.blocking(move |inner| {
				inner.write_records([RecordRef::Truncate(since)])?;
				inner.entries.split_off(&since);
				Ok(())
			})
			.await
	}

	async fn purge(&mut self, log_id: LogId) -> io::Result<()> {
		self
			.blocking(move |inner| {
				inner.write_records([RecordRef::Purge(&log_id)])?;
				inner.entries = inner.entries.split_off(&(log_id.index + 1));
				inner.last_purged = Some(log_id);
				inner.maybe_compact()
			})
			.await
	}
}

#[cfg(test)]
mod tests {
	use openraft::EntryPayload;

	use super::*;
	use crate::{
		kv::{machine::Command, raft::LeaderId},
		meta::NodeId,
	};

	fn entry(term: u64, index: u64) -> LogEntry {
		LogEntry {
			log_id:  LogId::new(LeaderId { term, node_id: NodeId(1) }, index),
			payload: EntryPayload::Blank,
		}
	}

	/// An entry whose record takes about `len` bytes.
	fn big_entry(index: u64, len: usize) -> LogEntry {
		LogEntry {
			payload: EntryPayload::Normal(Command::SetClusterId {
				now_ms:     0,
				cluster_id: "x".repeat(len),
			}),
			..entry(1, index)
		}
	}

	fn indexes(store: &LogStore) -> Vec<u64> {
		store.inner.lock().entries.keys().copied().collect()
	}

	fn append_sync(store: &LogStore, entries: Vec<LogEntry>) {
		let mut inner = store.inner.lock();
		inner
			.write_records(entries.iter().map(RecordRef::Entry))
			.unwrap();
		for e in entries {
			inner.entries.insert(e.log_id.index, e);
		}
	}

	/// Path, append offset, and length of the active slot.
	fn active_slot(dir: &Path, store: &LogStore) -> (PathBuf, u64, u64) {
		let inner = store.inner.lock();
		let slot = &inner.slots[inner.active];
		(dir.join(SLOT_FILES[inner.active]), slot.end, slot.len)
	}

	fn overwrite(path: &Path, offset: u64, bytes: &[u8]) {
		let mut file = OpenOptions::new().write(true).open(path).unwrap();
		file.seek(SeekFrom::Start(offset)).unwrap();
		file.write_all(bytes).unwrap();
	}

	#[tokio::test]
	async fn replay_applies_truncate_and_purge_and_keeps_vote() {
		let dir = tempfile::tempdir().unwrap();
		{
			let mut store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, (1..=10).map(|i| entry(1, i)).collect());
			store
				.truncate_after(Some(entry(1, 7).log_id))
				.await
				.unwrap();
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
	/// torn: the log must still open, with the committed id unknown.
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
		overwrite(&dir.path().join(COMMITTED_FILE), 20, b"torn");
		let mut store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![1, 2]);
		assert_eq!(store.read_committed().await.unwrap(), None);
	}

	/// A torn append is dropped and zeroed, so shorter records written over
	/// it later replay without its leftovers.
	#[tokio::test]
	async fn torn_tail_is_zeroed_and_log_stays_appendable() {
		let dir = tempfile::tempdir().unwrap();
		let (path, end) = {
			let store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, vec![entry(1, 1), big_entry(2, 10_000)]);
			let (path, end, _) = active_slot(dir.path(), &store);
			(path, end)
		};
		overwrite(&path, end - 5, &[0; 5]);
		{
			let store = LogStore::open(dir.path()).unwrap();
			assert_eq!(indexes(&store), vec![1]);
			append_sync(&store, vec![entry(1, 2), entry(1, 3)]);
			let (_, end, len) = active_slot(dir.path(), &store);
			let bytes = fs::read(&path).unwrap();
			assert_eq!(bytes.len() as u64, len);
			assert!(
				bytes[usize::try_from(end).unwrap()..]
					.iter()
					.all(|b| *b == 0)
			);
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![1, 2, 3]);
	}

	/// Sizes and allocated blocks of the log files in `dir`.
	#[cfg(unix)]
	fn log_layout(dir: &Path) -> BTreeMap<String, (u64, u64)> {
		use std::os::unix::fs::MetadataExt;
		fs::read_dir(dir)
			.unwrap()
			.map(|entry| entry.unwrap())
			.filter(|entry| entry.file_name().to_string_lossy().starts_with("log"))
			.map(|entry| {
				let meta = entry.metadata().unwrap();
				(entry.file_name().into_string().unwrap(), (meta.len(), meta.blocks()))
			})
			.collect()
	}

	/// Appends change no log file's size or allocation: an `fdatasync` after
	/// such a change needs a filesystem journal commit, which on ext4
	/// (`data=ordered`) waits for every other file's newly allocated dirty
	/// data and stalled KV writes for over a minute beside template builds.
	#[cfg(unix)]
	#[tokio::test]
	async fn appends_change_no_file_size_or_allocation() {
		let dir = tempfile::tempdir().unwrap();
		{
			let store = LogStore::open(dir.path()).unwrap();
			let before = log_layout(dir.path());
			for i in 1..=200 {
				append_sync(&store, vec![entry(1, i)]);
			}
			assert_eq!(log_layout(dir.path()), before);
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), (1..=200).collect::<Vec<_>>());
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

	/// Compactions alternate between the slots; the target's longer stale
	/// contents are zeroed, and each reopen replays the newest slot.
	#[tokio::test]
	async fn compactions_alternate_slots_and_survive_reopen() {
		let dir = tempfile::tempdir().unwrap();
		let first = {
			let mut store = LogStore::open(dir.path()).unwrap();
			let first = store.inner.lock().active;
			append_sync(&store, (1..=50).map(|i| big_entry(i, 100)).collect());
			store.purge(entry(1, 40).log_id).await.unwrap();
			store.inner.lock().rewrite().unwrap();
			assert_ne!(store.inner.lock().active, first);
			append_sync(&store, vec![entry(1, 51)]);
			store.purge(entry(1, 45).log_id).await.unwrap();
			store.inner.lock().rewrite().unwrap();
			assert_eq!(store.inner.lock().active, first);
			let (path, end, _) = active_slot(dir.path(), &store);
			let bytes = fs::read(path).unwrap();
			assert!(
				bytes[usize::try_from(end).unwrap()..]
					.iter()
					.all(|b| *b == 0)
			);
			first
		};
		let mut store = LogStore::open(dir.path()).unwrap();
		assert_eq!(store.inner.lock().active, first);
		assert_eq!(indexes(&store), (46..=51).collect::<Vec<_>>());
		append_sync(&store, vec![entry(1, 52)]);
		drop(store);
		store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), (46..=52).collect::<Vec<_>>());
		let state = store.get_log_state().await.unwrap();
		assert_eq!(state.last_purged_log_id, Some(entry(1, 45).log_id));
	}

	/// A crash after compaction wrote the other slot but before its header
	/// was durable replays the previous slot, which still holds everything.
	#[tokio::test]
	async fn interrupted_compaction_keeps_the_previous_slot() {
		let dir = tempfile::tempdir().unwrap();
		let target = {
			let mut store = LogStore::open(dir.path()).unwrap();
			append_sync(&store, (1..=10).map(|i| entry(1, i)).collect());
			store.purge(entry(1, 4).log_id).await.unwrap();
			store.inner.lock().rewrite().unwrap();
			let (path, ..) = active_slot(dir.path(), &store);
			path
		};
		overwrite(&target, 0, &[0; SLOT_HEADER]);
		{
			let mut store = LogStore::open(dir.path()).unwrap();
			assert_ne!(active_slot(dir.path(), &store).0, target);
			assert_eq!(indexes(&store), (5..=10).collect::<Vec<_>>());
			let state = store.get_log_state().await.unwrap();
			assert_eq!(state.last_purged_log_id, Some(entry(1, 4).log_id));
			append_sync(&store, vec![entry(1, 11)]);
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), (5..=11).collect::<Vec<_>>());
	}

	/// Records beyond a slot's size grow it (zero-filled) and replay.
	#[tokio::test]
	async fn appends_beyond_the_slot_grow_it() {
		let dir = tempfile::tempdir().unwrap();
		{
			let store = LogStore::open(dir.path()).unwrap();
			for i in 1..=4 {
				append_sync(&store, vec![big_entry(i, 3 << 20)]);
			}
			let (path, end, len) = active_slot(dir.path(), &store);
			assert!(end > SLOT_GROWTH && len >= end);
			assert_eq!(fs::metadata(path).unwrap().len(), len);
		}
		let store = LogStore::open(dir.path()).unwrap();
		assert_eq!(indexes(&store), vec![1, 2, 3, 4]);
	}

	/// The single `log.bin` and `committed.json` of earlier versions are
	/// replayed (torn tail dropped), moved into the slots, and removed.
	#[tokio::test]
	async fn legacy_log_and_committed_id_are_migrated() {
		let dir = tempfile::tempdir().unwrap();
		let mut legacy = Vec::new();
		for i in 1..=5 {
			encode_record(&RecordRef::Entry(&entry(1, i)), &mut legacy);
		}
		encode_record(&RecordRef::Purge(&entry(1, 1).log_id), &mut legacy);
		encode_record(&RecordRef::Truncate(5), &mut legacy);
		legacy.extend_from_slice(b"torn");
		fs::write(dir.path().join(LEGACY_LOG_FILE), &legacy).unwrap();
		let committed = Some(entry(1, 4).log_id);
		fs::write(dir.path().join(LEGACY_COMMITTED_FILE), serde_json::to_vec(&committed).unwrap())
			.unwrap();
		for _ in 0..2 {
			let mut store = LogStore::open(dir.path()).unwrap();
			assert_eq!(indexes(&store), vec![2, 3, 4]);
			let state = store.get_log_state().await.unwrap();
			assert_eq!(state.last_purged_log_id, Some(entry(1, 1).log_id));
			assert_eq!(store.read_committed().await.unwrap(), committed);
			assert!(!dir.path().join(LEGACY_LOG_FILE).exists());
			assert!(!dir.path().join(LEGACY_COMMITTED_FILE).exists());
		}
	}
}
