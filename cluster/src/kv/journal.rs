//! On-disk journal of append-only history (ARCHITECTURE.md §5.3).
//!
//! [`super::Op::Append`] records are not kept in the in-memory map: every
//! driver's state machine writes them here. Their sequence numbers come from
//! the replicated state (dense from 1, in commit order) and their timestamps
//! from the leader-stamped command time, so every replica writes the same
//! records, and replaying a log entry rewrites exactly what a crash lost.
//!
//! Layout (`<data>/raft/journal/`): segment files `<first seq>.seg` of framed
//! records (`len: u32 LE | fnv1a64(payload): u64 LE | payload`; payload =
//! `seq: u64 | ts_ms: u64 | streams: u32 | (len: u32 | stream)… | value`, all
//! LE), ascending by seq, sealed past [`SEGMENT_BYTES`]. A sealed segment gets
//! `<first seq>.idx` once its data is fsynced: a header and its `(stream
//! hash, seq, offset)` entries sorted, binary-searched by scans. Only segments
//! without an index file keep their index in memory.
//!
//! Durability: appends are plain writes on the apply path, never fsynced
//! there. [`Journal::flush_plan`] fsyncs data and writes index files; the
//! state machine runs it before persisting a raft snapshot, so every record
//! below the snapshot's journal position is durable and every later one is
//! rewritten by log replay (appends below the next seq are skipped). Open
//! trusts indexed segments, validates the others, truncates the first torn
//! one, and drops every unindexed segment after it: what they held is either
//! replayed again or (records copied from a peer) fetched again.
//!
//! Holes: a replica that installs a raft snapshot skips the records it never
//! applied ([`Journal::advance`] records the gap in `holes.json`), and open may
//! drop records below the snapshot position that were copied from a peer but
//! not yet fsynced (sequence numbers are dense, so a gap between two segments
//! is missing data). The maintenance task copies holes from a peer
//! ([`Journal::fill`]). Retention drops whole sealed segments
//! ([`Journal::prune`]).

use std::{
	collections::HashMap,
	fs::{self, File, OpenOptions},
	io,
	ops::Range,
	os::unix::fs::FileExt,
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use serde::{Deserialize, Serialize};

use super::{
	Record, Scan, hex_bytes,
	log_store::{fnv1a64, replace_file},
};

/// Data size after which the active segment is sealed.
pub const SEGMENT_BYTES: u64 = 16 << 20;
/// How long records are kept: sealed segments whose newest record is older
/// are deleted.
pub const RETENTION: Duration = Duration::from_hours(30 * 24);
/// Frame header: payload length + checksum.
const FRAME: usize = 12;
const INDEX_MAGIC: &[u8; 8] = b"vmonjix1";
/// Index header: magic, entry count, first and last seq, first and last
/// timestamp, data length, checksum of the preceding bytes.
const INDEX_HEADER: usize = 64;
/// Index entry: stream hash, seq, data offset.
const INDEX_ENTRY: usize = 24;
const SEGMENT_EXT: &str = "seg";
const INDEX_EXT: &str = "idx";
const HOLES_FILE: &str = "holes.json";

/// A record with the streams it belongs to: what the state machine appends
/// and what drivers copy from each other to fill holes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
	pub seq:     u64,
	pub ts_ms:   u64,
	pub streams: Vec<String>,
	#[serde(with = "hex_bytes")]
	pub value:   Vec<u8>,
}

/// Records copied from a peer's journal (`/cluster/v1/kv/journal`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Copied {
	/// Oldest seq the peer holds.
	pub low:      u64,
	/// Whether the peer has no holes itself: only then did everything below
	/// `low` expire.
	pub complete: bool,
	/// Records of the requested range, ascending.
	pub records:  Vec<Appended>,
}

/// One segment file.
struct Segment {
	/// Seq in the file name; every record is at or above it.
	first:    u64,
	/// Seq of the last record, `None` while empty.
	last:     Option<u64>,
	first_ts: u64,
	last_ts:  u64,
	/// Valid data length (the append offset of the active segment).
	len:      u64,
	data:     Arc<File>,
	index:    Index,
}

enum Index {
	/// Stream hash → `(seq, offset)`, ascending: not yet sealed and indexed.
	Memory(HashMap<u64, Vec<(u64, u64)>>),
	/// Sealed, fsynced, and indexed in `file` (`count` entries).
	Disk { file: Arc<File>, count: u64 },
}

impl Segment {
	fn create(dir: &Path, first: u64) -> io::Result<Self> {
		let data = OpenOptions::new()
			.create(true)
			.truncate(true)
			.read(true)
			.write(true)
			.open(segment_path(dir, first))?;
		Ok(Self {
			first,
			last: None,
			first_ts: 0,
			last_ts: 0,
			len: 0,
			data: Arc::new(data),
			index: Index::Memory(HashMap::new()),
		})
	}

	/// A sealed segment whose index file is intact; `None` without one.
	fn open_indexed(dir: &Path, first: u64) -> io::Result<Option<Self>> {
		let Ok(index) = File::open(index_path(dir, first)) else {
			return Ok(None);
		};
		let mut header = [0u8; INDEX_HEADER];
		if index.read_exact_at(&mut header, 0).is_err() {
			return Ok(None);
		}
		let field = |at: usize| u64::from_le_bytes(header[at..at + 8].try_into().unwrap_or_default());
		let data = OpenOptions::new()
			.read(true)
			.write(true)
			.open(segment_path(dir, first))?;
		let (count, len) = (field(8), field(48));
		let valid = &header[..8] == INDEX_MAGIC
			&& field(56) == fnv1a64(&header[..56])
			&& index.metadata()?.len() == INDEX_HEADER as u64 + count * INDEX_ENTRY as u64
			&& data.metadata()?.len() >= len;
		if !valid {
			return Ok(None);
		}
		Ok(Some(Self {
			first,
			last: (count > 0).then(|| field(24)),
			first_ts: field(32),
			last_ts: field(40),
			len,
			data: Arc::new(data),
			index: Index::Disk { file: Arc::new(index), count },
		}))
	}

	/// An unindexed segment, validated record by record and truncated at the
	/// first torn one; the flag tells whether it was intact.
	fn recover(dir: &Path, first: u64) -> io::Result<(Self, bool)> {
		let path = segment_path(dir, first);
		let bytes = fs::read(&path)?;
		let data = OpenOptions::new().read(true).write(true).open(&path)?;
		let mut segment = Self {
			first,
			last: None,
			first_ts: 0,
			last_ts: 0,
			len: 0,
			data: Arc::new(data),
			index: Index::Memory(HashMap::new()),
		};
		let mut at = 0;
		while let Some((record, framed)) = bytes.get(at..).and_then(decode_framed) {
			let ordered = segment
				.last
				.map_or(record.seq >= first, |last| record.seq > last);
			if !ordered {
				break;
			}
			segment.push(&record, at as u64, framed as u64);
			at += framed;
		}
		let intact = at == bytes.len();
		if !intact {
			segment.data.set_len(at as u64)?;
		}
		let _ = fs::remove_file(index_path(dir, first));
		Ok((segment, intact))
	}

	/// Accounts a record written at `offset` (memory index only).
	fn push(&mut self, record: &Appended, offset: u64, framed: u64) {
		if self.last.is_none() {
			self.first_ts = record.ts_ms;
		}
		self.last = Some(record.seq);
		self.last_ts = record.ts_ms;
		self.len = offset + framed;
		if let Index::Memory(map) = &mut self.index {
			for stream in &record.streams {
				map.entry(stream_hash(stream))
					.or_default()
					.push((record.seq, offset));
			}
		}
	}

	/// First seq after this segment's records.
	fn end(&self) -> u64 {
		self.last.map_or(self.first, |last| last + 1)
	}

	const fn indexed(&self) -> bool {
		matches!(self.index, Index::Disk { .. })
	}

	/// Index file contents for this (sealed) segment.
	fn index_bytes(&self) -> Option<Vec<u8>> {
		let Index::Memory(map) = &self.index else {
			return None;
		};
		let mut entries: Vec<(u64, u64, u64)> = map
			.iter()
			.flat_map(|(hash, list)| list.iter().map(|(seq, offset)| (*hash, *seq, *offset)))
			.collect();
		entries.sort_unstable();
		let mut bytes = Vec::with_capacity(INDEX_HEADER + entries.len() * INDEX_ENTRY);
		bytes.extend_from_slice(INDEX_MAGIC);
		for field in [
			entries.len() as u64,
			self.first,
			self.last.unwrap_or(self.first),
			self.first_ts,
			self.last_ts,
			self.len,
		] {
			bytes.extend_from_slice(&field.to_le_bytes());
		}
		let checksum = fnv1a64(&bytes);
		bytes.extend_from_slice(&checksum.to_le_bytes());
		for (hash, seq, offset) in entries {
			for field in [hash, seq, offset] {
				bytes.extend_from_slice(&field.to_le_bytes());
			}
		}
		Some(bytes)
	}
}

/// The journal of one driver; the state machine keeps it behind a mutex and
/// does file I/O of scans and flushes outside it ([`ScanPlan`],
/// [`FlushPlan`]).
pub struct Journal {
	dir:           PathBuf,
	segment_bytes: u64,
	/// Ascending by seq, non-overlapping; the last one is the active segment.
	segments:      Vec<Segment>,
	/// Seq the next appended record carries.
	next:          u64,
	/// Skipped and expired ranges, persisted in `holes.json`.
	gaps:          Gaps,
	/// `gaps` as last written.
	saved_gaps:    Gaps,
	/// [`Journal::holes`], recomputed after every change of `gaps` or
	/// `segments`.
	holes:         Vec<Range<u64>>,
	/// Encode buffer reused across appends.
	buf:           Vec<u8>,
}

impl Journal {
	/// Opens (or creates) the journal in `dir`.
	///
	/// # Errors
	///
	/// I/O errors reading, truncating, or creating segment files.
	pub fn open(dir: &Path) -> io::Result<Self> {
		Self::open_with(dir, SEGMENT_BYTES)
	}

	fn open_with(dir: &Path, segment_bytes: u64) -> io::Result<Self> {
		fs::create_dir_all(dir)?;
		let mut firsts = Vec::new();
		for entry in fs::read_dir(dir)? {
			let path = entry?.path();
			if path.extension().is_some_and(|ext| ext == SEGMENT_EXT)
				&& let Some(first) = path
					.file_stem()
					.and_then(|stem| stem.to_str())
					.and_then(|stem| stem.parse::<u64>().ok())
			{
				firsts.push(first);
			}
		}
		firsts.sort_unstable();
		let mut segments: Vec<Segment> = Vec::new();
		let mut torn = false;
		for first in firsts {
			if let Some(segment) = Segment::open_indexed(dir, first)? {
				segments.push(segment);
				continue;
			}
			if torn {
				remove_files(dir, first)?;
				continue;
			}
			let (segment, intact) = Segment::recover(dir, first)?;
			torn = !intact;
			if segment.last.is_none() {
				remove_files(dir, first)?;
			} else {
				segments.push(segment);
			}
		}
		let next = segments.iter().map(Segment::end).max().unwrap_or(1);
		let gaps: Gaps = match fs::read(dir.join(HOLES_FILE)) {
			Ok(bytes) => serde_json::from_slice(&bytes).map_err(io::Error::other)?,
			Err(error) if error.kind() == io::ErrorKind::NotFound => Gaps::default(),
			Err(error) => return Err(error),
		};
		let mut journal = Self {
			dir: dir.to_owned(),
			segment_bytes,
			segments,
			next,
			saved_gaps: gaps.clone(),
			gaps,
			holes: Vec::new(),
			buf: Vec::new(),
		};
		if journal.segments.last().is_none_or(Segment::indexed) {
			journal.segments.push(Segment::create(dir, next)?);
		}
		journal.refresh()?;
		Ok(journal)
	}

	/// Appends records applied in order. Records below the next seq are
	/// already present (log replay) and skipped; a record above it leaves a
	/// hole.
	///
	/// # Errors
	///
	/// I/O errors writing the active segment or creating the next one.
	pub fn append(&mut self, records: &[Appended]) -> io::Result<()> {
		let records: Vec<&Appended> = records.iter().filter(|r| r.seq >= self.next).collect();
		let Some(first) = records.first() else {
			return Ok(());
		};
		if first.seq > self.next {
			self.advance(first.seq)?;
		}
		self.buf.clear();
		let active = self
			.segments
			.last_mut()
			.expect("journal has an active segment");
		let base = active.len;
		let mut placed = Vec::with_capacity(records.len());
		for record in &records {
			let at = self.buf.len();
			encode_framed(record, &mut self.buf);
			placed.push((base + at as u64, (self.buf.len() - at) as u64));
		}
		active.data.write_all_at(&self.buf, base)?;
		for (record, (offset, framed)) in records.iter().zip(placed) {
			active.push(record, offset, framed);
		}
		self.next = active.end();
		let full = active.len >= self.segment_bytes;
		if full {
			self.segments.push(Segment::create(&self.dir, self.next)?);
		}
		Ok(())
	}

	/// Moves the next seq to `next` when it is behind: the skipped seqs become
	/// a hole (a raft snapshot replaced the state that produced them).
	///
	/// # Errors
	///
	/// I/O errors creating the new active segment.
	pub fn advance(&mut self, next: u64) -> io::Result<()> {
		if next <= self.next {
			return Ok(());
		}
		let active = self.segments.pop().expect("journal has an active segment");
		if active.last.is_some() {
			self.segments.push(active);
		} else {
			remove_files(&self.dir, active.first)?;
		}
		self.segments.push(Segment::create(&self.dir, next)?);
		self.gaps.skipped.push(self.next..next);
		self.next = next;
		self.refresh()
	}

	/// Missing seq ranges below the next seq that a peer may still hold,
	/// ascending.
	pub const fn holes(&self) -> &[Range<u64>] {
		self.holes.as_slice()
	}

	/// Whether every record below the next seq is present (or expired).
	pub const fn complete(&self) -> bool {
		self.holes.is_empty()
	}

	/// Recomputes [`Journal::holes`], drops recorded ranges that no longer
	/// matter, and persists the rest when they changed.
	fn refresh(&mut self) -> io::Result<()> {
		let oldest = self.segments.first().map_or(self.next, |s| s.first);
		self.gaps.expired.retain(|range| range.end > oldest);
		let mut have: Vec<Range<u64>> = self.segments.iter().map(|s| s.first..s.end()).collect();
		have.extend(self.gaps.expired.iter().cloned());
		self
			.gaps
			.skipped
			.retain(|gap| !subtract(std::slice::from_ref(gap), &have).is_empty());
		if self.gaps != self.saved_gaps {
			let bytes = serde_json::to_vec(&self.gaps).map_err(io::Error::other)?;
			replace_file(&self.dir.join(HOLES_FILE), &bytes)?;
			self.saved_gaps.clone_from(&self.gaps);
		}
		let mut wanted = self.gaps.skipped.clone();
		wanted.extend(
			self
				.segments
				.windows(2)
				.map(|pair| pair[0].end()..pair[1].first),
		);
		let mut holes = subtract(&wanted, &have);
		holes.sort_by_key(|range| range.start);
		self.holes = merge(holes);
		Ok(())
	}

	/// Inserts `copied` (from a peer) into `hole`. Records the peer no longer
	/// holds (below its `low`) settle that part of the hole: they expired.
	///
	/// # Errors
	///
	/// I/O errors writing the new segment; [`io::ErrorKind::InvalidData`] when
	/// the records are not ascending inside `hole`.
	pub fn fill(&mut self, hole: Range<u64>, copied: &Copied) -> io::Result<()> {
		let mut start = hole.start;
		if copied.complete && copied.low > start {
			let expired = start..copied.low.min(hole.end);
			start = expired.end;
			self.gaps.expired.push(expired);
		}
		let records: Vec<&Appended> = copied
			.records
			.iter()
			.filter(|r| start <= r.seq && r.seq < hole.end)
			.collect();
		if records.is_empty() {
			return self.refresh();
		}
		let ascending = records.windows(2).all(|pair| pair[0].seq < pair[1].seq);
		let present = self
			.segments
			.iter()
			.any(|s| records.iter().any(|r| s.first <= r.seq && r.seq < s.end()));
		if !ascending || present {
			return Err(io::Error::new(
				io::ErrorKind::InvalidData,
				"copied records do not fit the hole",
			));
		}
		let mut segment = Segment::create(&self.dir, records[0].seq)?;
		self.buf.clear();
		let mut placed = Vec::with_capacity(records.len());
		for record in &records {
			let at = self.buf.len();
			encode_framed(record, &mut self.buf);
			placed.push((at as u64, (self.buf.len() - at) as u64));
		}
		segment.data.write_all_at(&self.buf, 0)?;
		for (record, (offset, framed)) in records.iter().zip(placed) {
			segment.push(record, offset, framed);
		}
		let at = self.segments.partition_point(|s| s.first < segment.first);
		self.segments.insert(at, segment);
		self.refresh()
	}

	/// Deletes sealed, indexed segments whose newest record is older than
	/// `cutoff_ms`; returns how many.
	///
	/// # Errors
	///
	/// I/O errors removing files.
	pub fn prune(&mut self, cutoff_ms: u64) -> io::Result<usize> {
		let active = self.segments.len() - 1;
		let expired: Vec<u64> = self.segments[..active]
			.iter()
			.filter(|s| s.indexed() && s.last_ts < cutoff_ms)
			.map(|s| s.first)
			.collect();
		for first in &expired {
			remove_files(&self.dir, *first)?;
		}
		self.segments.retain(|s| !expired.contains(&s.first));
		self.refresh()?;
		Ok(expired.len())
	}

	/// Plans `scan`; run the plan without holding the journal.
	pub fn plan(&self, scan: &Scan) -> ScanPlan {
		let hash = stream_hash(&scan.stream);
		let limit = scan.limit.unwrap_or(usize::MAX);
		let after = scan.after;
		let in_bound = |seq: u64| match after {
			None => true,
			Some(after) if scan.reverse => seq < after,
			Some(after) => seq > after,
		};
		let mut sources = Vec::new();
		let mut listed = 0usize;
		let mut visit = |segment: &Segment| {
			let overlaps = match after {
				None => segment.last.is_some(),
				Some(after) if scan.reverse => segment.first < after && segment.last.is_some(),
				Some(after) => segment.last.is_some_and(|last| last > after),
			};
			if !overlaps || listed >= limit {
				return;
			}
			match &segment.index {
				Index::Memory(map) => {
					let Some(list) = map.get(&hash) else {
						return;
					};
					let picked: Vec<(u64, u64)> = if scan.reverse {
						list
							.iter()
							.rev()
							.filter(|(seq, _)| in_bound(*seq))
							.take(limit - listed)
							.copied()
							.collect()
					} else {
						list
							.iter()
							.filter(|(seq, _)| in_bound(*seq))
							.take(limit - listed)
							.copied()
							.collect()
					};
					listed += picked.len();
					if !picked.is_empty() {
						sources.push(Source {
							data:    segment.data.clone(),
							entries: Entries::Listed(picked),
						});
					}
				},
				Index::Disk { file, count } => sources.push(Source {
					data:    segment.data.clone(),
					entries: Entries::Indexed { file: file.clone(), count: *count },
				}),
			}
		};
		if scan.reverse {
			self.segments.iter().rev().for_each(&mut visit);
		} else {
			self.segments.iter().for_each(&mut visit);
		}
		ScanPlan { stream: scan.stream.clone(), hash, after, limit, reverse: scan.reverse, sources }
	}

	/// Plans copying the records of `range` for a peer; run the plan without
	/// holding the journal.
	pub fn copy_plan(&self, range: Range<u64>) -> CopyPlan {
		let low = self.segments.first().map_or(self.next, |s| s.first);
		let sources = self
			.segments
			.iter()
			.filter(|s| s.first < range.end && s.end() > range.start)
			.map(|s| (s.data.clone(), s.len))
			.collect();
		CopyPlan { low, complete: self.complete(), range, sources }
	}

	/// Plans making every appended record durable: fsync the data of
	/// unindexed segments and write index files for the sealed ones. Run it
	/// without holding the journal, then hand it to [`Journal::flushed`].
	pub fn flush_plan(&self) -> FlushPlan {
		let active = self.segments.len() - 1;
		let mut plan = FlushPlan { dir: self.dir.clone(), sync: Vec::new(), seal: Vec::new() };
		for (i, segment) in self.segments.iter().enumerate() {
			if segment.indexed() {
				continue;
			}
			plan.sync.push(segment.data.clone());
			if i < active
				&& let Some(bytes) = segment.index_bytes()
			{
				plan.seal.push((segment.first, bytes));
			}
		}
		plan
	}

	/// Switches the segments a finished [`FlushPlan`] indexed to their index
	/// files (dropping their memory indexes).
	///
	/// # Errors
	///
	/// I/O errors opening the index files.
	pub fn flushed(&mut self, plan: &FlushPlan) -> io::Result<()> {
		for (first, _) in &plan.seal {
			let Some(segment) = self.segments.iter_mut().find(|s| s.first == *first) else {
				continue;
			};
			let file = File::open(index_path(&self.dir, *first))?;
			let count = (file.metadata()?.len() - INDEX_HEADER as u64) / INDEX_ENTRY as u64;
			segment.index = Index::Disk { file: Arc::new(file), count };
		}
		Ok(())
	}
}

/// Ranges the journal does not hold (`holes.json`).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Gaps {
	/// Skipped by [`Journal::advance`]; holes until filled.
	skipped: Vec<Range<u64>>,
	/// Expired on the peers too: never holes.
	expired: Vec<Range<u64>>,
}

/// File work of a [`Journal::flush_plan`].
pub struct FlushPlan {
	dir:  PathBuf,
	sync: Vec<Arc<File>>,
	seal: Vec<(u64, Vec<u8>)>,
}

impl FlushPlan {
	/// Fsyncs the data, then writes the index files (each atomically).
	///
	/// # Errors
	///
	/// I/O errors syncing or writing.
	pub fn run(&self) -> io::Result<()> {
		for data in &self.sync {
			data.sync_data()?;
		}
		for (first, bytes) in &self.seal {
			replace_file(&index_path(&self.dir, *first), bytes)?;
		}
		Ok(())
	}
}

/// File work of a [`Journal::plan`]: the matching index entries per segment,
/// in scan order.
pub struct ScanPlan {
	stream:  String,
	hash:    u64,
	after:   Option<u64>,
	limit:   usize,
	reverse: bool,
	sources: Vec<Source>,
}

struct Source {
	data:    Arc<File>,
	entries: Entries,
}

enum Entries {
	/// `(seq, offset)` already picked from a memory index, in scan order.
	Listed(Vec<(u64, u64)>),
	/// Entries to look up in an index file.
	Indexed { file: Arc<File>, count: u64 },
}

impl ScanPlan {
	/// Reads the records.
	///
	/// # Errors
	///
	/// I/O errors, or [`io::ErrorKind::InvalidData`] for a record that does
	/// not match its index entry.
	pub fn run(self) -> io::Result<Vec<Record>> {
		let mut out = Vec::new();
		for source in &self.sources {
			if out.len() >= self.limit {
				break;
			}
			let entries = match &source.entries {
				Entries::Listed(list) => list.clone(),
				Entries::Indexed { file, count } => {
					self.lookup(file, *count, self.limit - out.len())?
				},
			};
			for (seq, offset) in entries {
				if out.len() >= self.limit {
					break;
				}
				let record = read_record(&source.data, offset)?;
				if record.seq != seq {
					return Err(io::Error::new(
						io::ErrorKind::InvalidData,
						"journal index points elsewhere",
					));
				}
				// Another stream with the same hash shares the entry run.
				if record.streams.contains(&self.stream) {
					out.push(Record { seq, ts_ms: record.ts_ms, value: record.value });
				}
			}
		}
		Ok(out)
	}

	/// At most `want` `(seq, offset)` of this stream from an index file, in
	/// scan order.
	fn lookup(&self, file: &File, count: u64, want: usize) -> io::Result<Vec<(u64, u64)>> {
		let key = |i: u64| -> io::Result<(u64, u64, u64)> {
			let mut entry = [0u8; INDEX_ENTRY];
			file.read_exact_at(&mut entry, INDEX_HEADER as u64 + i * INDEX_ENTRY as u64)?;
			let field =
				|at: usize| u64::from_le_bytes(entry[at..at + 8].try_into().unwrap_or_default());
			Ok((field(0), field(8), field(16)))
		};
		// First entry at or above `(hash, seq)`.
		let lower = |hash: u64, seq: u64| -> io::Result<u64> {
			let (mut lo, mut hi) = (0, count);
			while lo < hi {
				let mid = lo + (hi - lo) / 2;
				let (h, s, _) = key(mid)?;
				if (h, s) < (hash, seq) {
					lo = mid + 1;
				} else {
					hi = mid;
				}
			}
			Ok(lo)
		};
		let run_start = lower(self.hash, 0)?;
		let run_end = match self.hash.checked_add(1) {
			Some(next) => lower(next, 0)?,
			None => count,
		};
		let (start, end) = match self.after {
			None => (run_start, run_end),
			Some(after) if self.reverse => (run_start, lower(self.hash, after)?.min(run_end)),
			Some(after) => (lower(self.hash, after.saturating_add(1))?.max(run_start), run_end),
		};
		let want = (want as u64).min(end.saturating_sub(start));
		let indexes: Box<dyn Iterator<Item = u64>> = if self.reverse {
			Box::new((end - want..end).rev())
		} else {
			Box::new(start..start + want)
		};
		indexes
			.map(|i| key(i).map(|(_, seq, offset)| (seq, offset)))
			.collect()
	}
}

/// File work of a [`Journal::copy_plan`].
pub struct CopyPlan {
	low:      u64,
	complete: bool,
	range:    Range<u64>,
	sources:  Vec<(Arc<File>, u64)>,
}

impl CopyPlan {
	/// Reads the range's records, stopping after about `max_bytes` of values.
	///
	/// # Errors
	///
	/// I/O errors reading segments.
	pub fn run(self, max_bytes: usize) -> io::Result<Copied> {
		let mut copied = Copied { low: self.low, complete: self.complete, records: Vec::new() };
		let mut bytes = 0;
		for (data, len) in &self.sources {
			let mut buf = vec![0; usize::try_from(*len).map_err(io::Error::other)?];
			data.read_exact_at(&mut buf, 0)?;
			let mut at = 0;
			while let Some((record, framed)) = buf.get(at..).and_then(decode_framed) {
				at += framed;
				if !self.range.contains(&record.seq) {
					continue;
				}
				bytes += record.value.len();
				copied.records.push(record);
				if bytes >= max_bytes {
					return Ok(copied);
				}
			}
		}
		Ok(copied)
	}
}

fn stream_hash(stream: &str) -> u64 {
	fnv1a64(stream.as_bytes())
}

/// Parts of `wanted` outside every range of `have`.
fn subtract(wanted: &[Range<u64>], have: &[Range<u64>]) -> Vec<Range<u64>> {
	let mut parts: Vec<Range<u64>> = wanted.iter().filter(|r| !r.is_empty()).cloned().collect();
	for cut in have.iter().filter(|r| !r.is_empty()) {
		parts = parts
			.into_iter()
			.flat_map(|part| {
				if cut.end <= part.start || part.end <= cut.start {
					vec![part]
				} else {
					[part.start..cut.start, cut.end..part.end]
						.into_iter()
						.filter(|r| !r.is_empty())
						.collect()
				}
			})
			.collect();
	}
	parts
}

/// Merges sorted ranges that touch or overlap.
fn merge(sorted: Vec<Range<u64>>) -> Vec<Range<u64>> {
	let mut out: Vec<Range<u64>> = Vec::with_capacity(sorted.len());
	for range in sorted {
		match out.last_mut() {
			Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
			_ => out.push(range),
		}
	}
	out
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
	dir.join(format!("{first:020}.{SEGMENT_EXT}"))
}

fn index_path(dir: &Path, first: u64) -> PathBuf {
	dir.join(format!("{first:020}.{INDEX_EXT}"))
}

fn remove_files(dir: &Path, first: u64) -> io::Result<()> {
	for path in [index_path(dir, first), segment_path(dir, first)] {
		match fs::remove_file(path) {
			Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
			_ => {},
		}
	}
	Ok(())
}

/// Appends `record` to `buf`, framed with its length and checksum.
fn encode_framed(record: &Appended, buf: &mut Vec<u8>) {
	let start = buf.len();
	buf.extend_from_slice(&[0; FRAME]);
	buf.extend_from_slice(&record.seq.to_le_bytes());
	buf.extend_from_slice(&record.ts_ms.to_le_bytes());
	buf.extend_from_slice(&len32(record.streams.len()).to_le_bytes());
	for stream in &record.streams {
		buf.extend_from_slice(&len32(stream.len()).to_le_bytes());
		buf.extend_from_slice(stream.as_bytes());
	}
	buf.extend_from_slice(&record.value);
	let payload = &buf[start + FRAME..];
	let len = len32(payload.len());
	let checksum = fnv1a64(payload);
	buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
	buf[start + 4..start + FRAME].copy_from_slice(&checksum.to_le_bytes());
}

/// Raft bodies are capped far below 4 GiB, so record parts always fit.
fn len32(len: usize) -> u32 {
	u32::try_from(len).expect("journal record parts are bounded by the raft body limit")
}

/// The record framed at the start of `bytes` and its framed length; `None`
/// at the end of data or on a torn/corrupt frame (zeros never form a frame:
/// the empty payload's checksum is not zero).
fn decode_framed(bytes: &[u8]) -> Option<(Appended, usize)> {
	let len = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
	let checksum = u64::from_le_bytes(bytes.get(4..FRAME)?.try_into().ok()?);
	let payload = bytes.get(FRAME..FRAME + len)?;
	(fnv1a64(payload) == checksum).then_some(())?;
	Some((decode_payload(payload)?, FRAME + len))
}

fn decode_payload(payload: &[u8]) -> Option<Appended> {
	let mut at = 0;
	let mut take = |n: usize| -> Option<&[u8]> {
		let part = payload.get(at..at + n)?;
		at += n;
		Some(part)
	};
	let u64_at = |bytes: &[u8]| u64::from_le_bytes(bytes.try_into().unwrap_or_default());
	let u32_at = |bytes: &[u8]| u32::from_le_bytes(bytes.try_into().unwrap_or_default()) as usize;
	let seq = u64_at(take(8)?);
	let ts_ms = u64_at(take(8)?);
	let count = u32_at(take(4)?);
	let mut streams = Vec::with_capacity(count.min(64));
	for _ in 0..count {
		let len = u32_at(take(4)?);
		streams.push(String::from_utf8(take(len)?.to_vec()).ok()?);
	}
	let value = payload.get(at..)?.to_vec();
	Some(Appended { seq, ts_ms, streams, value })
}

/// The record framed at `offset` of `data`.
fn read_record(data: &File, offset: u64) -> io::Result<Appended> {
	let mut header = [0u8; FRAME];
	data.read_exact_at(&mut header, offset)?;
	let len = u32::from_le_bytes(header[..4].try_into().unwrap_or_default()) as usize;
	let mut framed = vec![0u8; FRAME + len];
	data.read_exact_at(&mut framed, offset)?;
	decode_framed(&framed)
		.map(|(record, _)| record)
		.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "torn journal record"))
}

#[cfg(test)]
mod tests;
