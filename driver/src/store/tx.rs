//! Read views and optimistic transactions over the cluster KV and its journal.

use std::{collections::HashSet, time::Duration};

use chrono::{DateTime, Utc};
use rand::RngExt;
use serde::{Serialize, de::DeserializeOwned};
use vmon_cluster::kv::{Compare, Entry, Kv, Op, Range, Record, Scan, Txn};

use super::{Error, Result};

/// Commit attempts of one [`Tx`] before it gives up with
/// [`Error::Contention`].
const ATTEMPTS: u32 = 32;
/// Pause before the third attempt; doubles per attempt (jittered), capped at
/// [`MAX_BACKOFF`].
const BACKOFF: Duration = Duration::from_millis(2);
const MAX_BACKOFF: Duration = Duration::from_millis(100);
/// Page size of [`Read::scan_all`] and [`Read::journal_each`].
const PAGE: usize = 512;

/// A decoded journal record ([`Read::journal`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Logged<T> {
	/// Journal position: dense from 1 across all streams, in commit order.
	pub seq:   u64,
	/// Commit time (leader clock, ms); never decreases with `seq`.
	pub at:    DateTime<Utc>,
	pub value: T,
}

/// Linearizable read view ([`super::Store::read`]): its barrier ran when it
/// was created, so every read sees at least the state acknowledged before.
/// Successive reads may see later state; use a [`Tx`] when several reads must
/// agree.
pub struct Read<'s> {
	kv: &'s Kv,
}

impl<'s> Read<'s> {
	pub(super) const fn new(kv: &'s Kv) -> Self {
		Self { kv }
	}

	/// Record at `key`.
	pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
		self.kv.get(key).await?.map(|e| decode(&e)).transpose()
	}

	/// Records under `prefix`, in key order.
	pub async fn list<T: DeserializeOwned>(&self, prefix: &str) -> Result<Vec<T>> {
		self.kv.list(prefix).await?.iter().map(decode).collect()
	}

	/// `(key, record)` pairs of one range page.
	pub async fn scan<T: DeserializeOwned>(&self, range: &Range) -> Result<Vec<(String, T)>> {
		self.kv.range(range).await?.into_iter().map(keyed).collect()
	}

	/// Keys under `prefix`, in key order (index scans; values are ignored).
	pub async fn keys(&self, prefix: &str) -> Result<Vec<String>> {
		Ok(self
			.kv
			.list(prefix)
			.await?
			.into_iter()
			.map(|e| e.key)
			.collect())
	}

	/// One page of a journal stream ([`vmon_cluster::kv::Kv::scan`]).
	pub async fn journal<T: DeserializeOwned>(&self, scan: &Scan) -> Result<Vec<Logged<T>>> {
		self.kv.scan(scan).await?.iter().map(logged).collect()
	}

	/// Walks a journal stream page by page, handing each record to `visit`
	/// until it returns `false` or the stream ends. `scan.limit` is ignored.
	pub async fn journal_each<T: DeserializeOwned>(
		&self,
		scan: &Scan,
		mut visit: impl FnMut(Logged<T>) -> bool,
	) -> Result<()> {
		let mut page = Scan { limit: Some(PAGE), ..scan.clone() };
		loop {
			let records = self.kv.scan(&page).await?;
			let done = records.len() < PAGE;
			let Some(last) = records.last().map(|r| r.seq) else {
				return Ok(());
			};
			for record in &records {
				if !visit(logged(record)?) {
					return Ok(());
				}
			}
			if done {
				return Ok(());
			}
			page.after = Some(last);
		}
	}

	/// Walks `range` page by page, handing each `(key, record)` to `visit`
	/// until it returns `false` or the range ends. `range.limit` is ignored.
	pub async fn scan_all<T: DeserializeOwned>(
		&self,
		range: &Range,
		mut visit: impl FnMut(String, T) -> bool,
	) -> Result<()> {
		let mut page = Range { limit: Some(PAGE), ..range.clone() };
		loop {
			let entries = self.kv.range(&page).await?;
			let done = entries.len() < PAGE;
			let Some(last) = entries.last().map(|e| e.key.clone()) else {
				return Ok(());
			};
			for entry in entries {
				let (key, value) = keyed(entry)?;
				if !visit(key, value) {
					return Ok(());
				}
			}
			if done {
				return Ok(());
			}
			page.after = Some(last);
		}
	}
}

/// Optimistic transaction: every key it reads becomes a `ModRev` guard (an
/// absent key must stay absent), its writes are staged, and
/// [`Tx::commit`] applies them in one KV transaction iff no guarded key
/// changed. On a lost guard it resets to a fresh read view; the caller reruns
/// its body in a loop (see the module docs of [`super`]).
///
/// Guards cover the keys a read returned, not keys a scan *could* return:
/// protect "no such row" with [`Tx::get`] on a unique-index key or with
/// [`Tx::require`] (`Missing`, `CountBelow`). Reads see committed state, not
/// this transaction's staged writes. Returning before `commit` abandons the
/// transaction (a read-only decision on a linearizable view).
pub struct Tx<'s> {
	kv:      &'s Kv,
	compare: Vec<Compare>,
	guarded: HashSet<String>,
	ops:     Vec<Op>,
	attempt: u32,
}

impl<'s> Tx<'s> {
	pub(super) async fn begin(kv: &'s Kv) -> Result<Self> {
		kv.sync().await?;
		Ok(Self { kv, compare: Vec::new(), guarded: HashSet::new(), ops: Vec::new(), attempt: 1 })
	}

	/// Guarded read of the record at `key`.
	pub async fn get<T: DeserializeOwned>(&mut self, key: &str) -> Result<Option<T>> {
		let entry = self.kv.get(key).await?;
		self.guard(key, entry.as_ref().map_or(0, |e| e.mod_rev));
		entry.map(|e| decode(&e)).transpose()
	}

	/// Guarded read of the records under `prefix` (each returned key guarded).
	pub async fn list<T: DeserializeOwned>(&mut self, prefix: &str) -> Result<Vec<T>> {
		let entries = self.kv.list(prefix).await?;
		entries
			.iter()
			.map(|e| {
				self.guard(&e.key, e.mod_rev);
				decode(e)
			})
			.collect()
	}

	/// Guarded keys under `prefix` (index scans; values are ignored).
	pub async fn keys(&mut self, prefix: &str) -> Result<Vec<String>> {
		let entries = self.kv.list(prefix).await?;
		Ok(entries
			.into_iter()
			.map(|e| {
				self.guard(&e.key, e.mod_rev);
				e.key
			})
			.collect())
	}

	/// Adds a raw guard (`Missing` for uniqueness, `CountBelow` for limits).
	pub fn require(&mut self, compare: Compare) {
		self.compare.push(compare);
	}

	/// Stages a write of `value` (JSON) at `key`.
	pub fn put<T: Serialize + ?Sized>(&mut self, key: impl Into<String>, value: &T) {
		self
			.ops
			.push(Op::Put { key: key.into(), value: encode(value), ttl_ms: None });
	}

	/// [`Tx::put`] that expires `ttl` after the commit.
	pub fn put_ttl<T: Serialize + ?Sized>(
		&mut self,
		key: impl Into<String>,
		value: &T,
		ttl: Duration,
	) {
		self.ops.push(Op::Put {
			key:    key.into(),
			value:  encode(value),
			ttl_ms: Some(millis(ttl)),
		});
	}

	/// Stages an index entry (empty value) at `key`, expiring after `ttl` if
	/// given.
	pub fn mark(&mut self, key: impl Into<String>, ttl: Option<Duration>) {
		self
			.ops
			.push(Op::Put { key: key.into(), value: Vec::new(), ttl_ms: ttl.map(millis) });
	}

	/// Stages appending `value` (JSON) to the journal under every one of
	/// `streams` ([`Read::journal`]); history, not guarded and never compared.
	pub fn append<T: Serialize + ?Sized>(&mut self, streams: Vec<String>, value: &T) {
		self.ops.push(Op::Append { streams, value: encode(value) });
	}

	/// Stages the removal of `key`.
	pub fn delete(&mut self, key: impl Into<String>) {
		self.ops.push(Op::Delete { key: key.into() });
	}

	/// Stages the removal of every key under `prefix`.
	pub fn delete_prefix(&mut self, prefix: impl Into<String>) {
		self.ops.push(Op::DeletePrefix { prefix: prefix.into() });
	}

	/// Applies the staged writes iff every guard still holds.
	///
	/// `Ok(true)`: committed. `Ok(false)`: a guard failed; the transaction was
	/// reset onto a fresh read view and the caller must rerun its body. With
	/// nothing staged it commits trivially.
	///
	/// # Errors
	///
	/// [`Error::Kv`] when the KV call fails (the outcome is then unknown: the
	/// write may have applied), [`Error::Contention`] after too many lost
	/// guards.
	pub async fn commit(&mut self) -> Result<bool> {
		if self.ops.is_empty() {
			return Ok(true);
		}
		let txn = Txn {
			compare: std::mem::take(&mut self.compare),
			success: std::mem::take(&mut self.ops),
			failure: Vec::new(),
		};
		self.guarded.clear();
		if self.kv.txn(txn).await?.succeeded {
			return Ok(true);
		}
		if self.attempt >= ATTEMPTS {
			return Err(Error::Contention(self.attempt));
		}
		if self.attempt >= 2 {
			let backoff = BACKOFF
				.saturating_mul(1 << (self.attempt - 2).min(16))
				.min(MAX_BACKOFF);
			let jittered = backoff.mul_f64(rand::rng().random_range(0.5..1.5));
			tokio::time::sleep(jittered).await;
		}
		self.attempt += 1;
		self.kv.sync().await?;
		Ok(false)
	}

	fn guard(&mut self, key: &str, mod_rev: u64) {
		if self.guarded.insert(key.to_owned()) {
			self
				.compare
				.push(Compare::ModRev { key: key.to_owned(), rev: mod_rev });
		}
	}
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Vec<u8> {
	serde_json::to_vec(value).expect("store records serialize to JSON")
}

fn decode<T: DeserializeOwned>(entry: &Entry) -> Result<T> {
	serde_json::from_slice(&entry.value)
		.map_err(|error| Error::Corrupt { key: entry.key.clone(), error: error.to_string() })
}

fn logged<T: DeserializeOwned>(record: &Record) -> Result<Logged<T>> {
	let value = serde_json::from_slice(&record.value).map_err(|error| Error::Corrupt {
		key:   format!("journal record {}", record.seq),
		error: error.to_string(),
	})?;
	let ms = i64::try_from(record.ts_ms).unwrap_or(i64::MAX);
	let at = DateTime::from_timestamp_millis(ms).unwrap_or_default();
	Ok(Logged { seq: record.seq, at, value })
}

fn keyed<T: DeserializeOwned>(entry: Entry) -> Result<(String, T)> {
	let value = decode(&entry)?;
	Ok((entry.key, value))
}

fn millis(ttl: Duration) -> u64 {
	u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}
