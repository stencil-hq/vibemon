//! Deterministic KV state: ordered map, global revision, TTL index.
//!
//! Every [`Command`] carries the leader-stamped `now_ms`; applying it first
//! advances the applied clock (monotonically) and expires due keys, so every
//! replica reaches the same state from the same log.

use std::{
	collections::{BTreeMap, BTreeSet},
	fmt,
	ops::Bound,
};

use serde::{Deserialize, Serialize};

use super::{Compare, Entry, Op, Range, Txn, TxnResult, WatchEvent, stamp};

/// Raft log payload (openraft `D`), proposed by the leader.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
	/// Client transactions committed as one entry and applied in order, each
	/// seeing the effects of the ones before it ([`super::Kv::txns`]).
	Txns { now_ms: u64, txns: Vec<Txn> },
	/// One client transaction, as logged by earlier versions (replayed, no
	/// longer proposed).
	Txn { now_ms: u64, txn: Txn },
	/// Clock tick proposed by the leader so due TTL keys get removed.
	Expire { now_ms: u64 },
	/// Records the cluster UUID chosen at bootstrap; first writer wins.
	SetClusterId { now_ms: u64, cluster_id: String },
}

/// Short form for openraft's logs.
impl fmt::Display for Command {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Txns { now_ms, txns } => write!(f, "txns@{now_ms}({})", txns.len()),
			Self::Txn { now_ms, txn } => write!(
				f,
				"txn@{now_ms}(compare: {}, success: {}, failure: {})",
				txn.compare.len(),
				txn.success.len(),
				txn.failure.len()
			),
			Self::Expire { now_ms } => write!(f, "expire@{now_ms}"),
			Self::SetClusterId { now_ms, cluster_id } => {
				write!(f, "set-cluster-id@{now_ms}({cluster_id})")
			},
		}
	}
}

impl Command {
	const fn now_ms(&self) -> u64 {
		match self {
			Self::Txns { now_ms, .. }
			| Self::Txn { now_ms, .. }
			| Self::Expire { now_ms }
			| Self::SetClusterId { now_ms, .. } => *now_ms,
		}
	}
}

/// Full replicated state; serialized as the raft snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvState {
	entries:    BTreeMap<String, Entry>,
	revision:   u64,
	applied_ms: u64,
	cluster_id: Option<String>,
	/// `(expires_at_ms, key)` for every entry with a TTL; rebuilt on load.
	#[serde(skip)]
	ttl:        BTreeSet<(u64, String)>,
}

impl KvState {
	/// Rebuilds derived indexes after deserializing a snapshot.
	pub fn from_snapshot(bytes: &[u8]) -> serde_json::Result<Self> {
		let mut state: Self = serde_json::from_slice(bytes)?;
		state.ttl = state
			.entries
			.values()
			.filter_map(|e| e.expires_at_ms.map(|at| (at, e.key.clone())))
			.collect();
		Ok(state)
	}

	/// Serialized form stored in snapshots.
	pub fn to_snapshot(&self) -> Vec<u8> {
		serde_json::to_vec(self).expect("kv state serializes")
	}

	/// Revision of the last applied change.
	pub const fn revision(&self) -> u64 {
		self.revision
	}

	/// Cluster UUID recorded at bootstrap.
	pub fn cluster_id(&self) -> Option<&str> {
		self.cluster_id.as_deref()
	}

	/// Earliest TTL deadline, used by the leader to decide when to propose
	/// `Expire`.
	pub fn next_expiry_ms(&self) -> Option<u64> {
		self.ttl.first().map(|(at, _)| *at)
	}

	/// Live entry for `key` relative to the last applied time.
	pub fn get(&self, key: &str) -> Option<Entry> {
		self.entries.get(key).filter(|e| self.live(e)).cloned()
	}

	/// Live entries under `prefix`, in key order.
	pub fn list(&self, prefix: &str) -> Vec<Entry> {
		self
			.prefix_iter(prefix)
			.filter(|e| self.live(e))
			.cloned()
			.collect()
	}

	/// Live entries matching `range`, in its order.
	pub fn range(&self, range: &Range) -> Vec<Entry> {
		let prefix = range.prefix.as_str();
		let end = prefix_end(prefix);
		let mut lower = Bound::Included(prefix);
		let mut upper = end.as_deref().map_or(Bound::Unbounded, Bound::Excluded);
		match range.after.as_deref() {
			Some(after) if !range.reverse && after >= prefix => lower = Bound::Excluded(after),
			Some(after) if range.reverse && end.as_deref().is_none_or(|end| after < end) => {
				upper = Bound::Excluded(after);
			},
			_ => {},
		}
		let empty = match (lower, upper) {
			(Bound::Included(lo) | Bound::Excluded(lo), Bound::Excluded(hi)) => lo >= hi,
			_ => false,
		};
		if empty {
			return Vec::new();
		}
		let matches = self
			.entries
			.range::<str, _>((lower, upper))
			.map(|(_, entry)| entry);
		let live = |entry: &&Entry| entry.key.starts_with(prefix) && self.live(entry);
		let limit = range.limit.unwrap_or(usize::MAX);
		if range.reverse {
			matches.rev().filter(live).take(limit).cloned().collect()
		} else {
			matches.filter(live).take(limit).cloned().collect()
		}
	}

	/// Applies one command; appends resulting changes to `events`. Returns one
	/// result per transaction of [`Command::Txns`], else a single result.
	pub fn apply(&mut self, command: &Command, events: &mut Vec<WatchEvent>) -> Vec<TxnResult> {
		self.applied_ms = self.applied_ms.max(command.now_ms());
		self.expire(events);
		match command {
			Command::Txns { txns, .. } => txns.iter().map(|txn| self.apply_txn(txn, events)).collect(),
			Command::Txn { txn, .. } => vec![self.apply_txn(txn, events)],
			Command::Expire { .. } => vec![TxnResult { succeeded: true, revision: self.revision }],
			Command::SetClusterId { cluster_id, .. } => {
				let id = self.cluster_id.get_or_insert_with(|| cluster_id.clone());
				vec![TxnResult { succeeded: id == cluster_id, revision: self.revision }]
			},
		}
	}

	/// Evaluates `txn`'s compares and runs the chosen branch; the revision
	/// advances only if that changed something.
	fn apply_txn(&mut self, txn: &Txn, events: &mut Vec<WatchEvent>) -> TxnResult {
		let succeeded = txn.compare.iter().all(|c| self.check(c));
		let ops = if succeeded {
			&txn.success
		} else {
			&txn.failure
		};
		let rev = self.revision + 1;
		let mut changed = false;
		for op in ops {
			changed |= self.apply_op(op, rev, events);
		}
		if changed {
			self.revision = rev;
		}
		TxnResult { succeeded, revision: self.revision }
	}

	/// Diff between `self` and `next` as watch events (used when a snapshot
	/// replaces state).
	pub fn diff(&self, next: &Self) -> Vec<WatchEvent> {
		let mut events: Vec<WatchEvent> = self
			.entries
			.keys()
			.filter(|key| !next.entries.contains_key(*key))
			.map(|key| WatchEvent::Delete { key: key.clone(), revision: next.revision })
			.collect();
		events.extend(
			next
				.entries
				.values()
				.filter(|entry| self.entries.get(&entry.key) != Some(entry))
				.map(|entry| WatchEvent::Put(entry.clone())),
		);
		events
	}

	fn live(&self, entry: &Entry) -> bool {
		entry.expires_at_ms.is_none_or(|at| at > self.applied_ms)
	}

	fn prefix_iter<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = &'a Entry> + 'a {
		self
			.entries
			.range::<str, _>((std::ops::Bound::Included(prefix), std::ops::Bound::Unbounded))
			.map_while(move |(key, entry)| key.starts_with(prefix).then_some(entry))
	}

	/// Removes keys whose deadline has passed; one revision bump per expiring
	/// pass.
	fn expire(&mut self, events: &mut Vec<WatchEvent>) {
		let due: Vec<(u64, String)> = self
			.ttl
			.iter()
			.take_while(|(at, _)| *at <= self.applied_ms)
			.cloned()
			.collect();
		if due.is_empty() {
			return;
		}
		self.revision += 1;
		for item in due {
			self.ttl.remove(&item);
			self.entries.remove(&item.1);
			events.push(WatchEvent::Delete { key: item.1, revision: self.revision });
		}
	}

	fn check(&self, compare: &Compare) -> bool {
		match compare {
			Compare::Exists(key) => self.get(key).is_some(),
			Compare::Missing(key) => self.get(key).is_none(),
			Compare::ModRev { key, rev } => self.get(key).map_or(0, |e| e.mod_rev) == *rev,
			Compare::Value { key, value } => self.get(key).is_some_and(|e| e.value == *value),
			Compare::CountBelow { prefix, limit } => {
				let count = self.prefix_iter(prefix).filter(|e| self.live(e)).count();
				u64::try_from(count).unwrap_or(u64::MAX) < *limit
			},
		}
	}

	/// Applies one op at revision `rev`; returns whether anything changed.
	fn apply_op(&mut self, op: &Op, rev: u64, events: &mut Vec<WatchEvent>) -> bool {
		match op {
			Op::Put { key, value, ttl_ms } => {
				self.put(key.clone(), value, *ttl_ms, rev, events);
				true
			},
			Op::PutStamped { prefix, suffix, value, ttl_ms } => {
				self.put(format!("{prefix}{}{suffix}", stamp(rev)), value, *ttl_ms, rev, events);
				true
			},
			Op::Delete { key } => self.remove(key, rev, events),
			Op::DeletePrefix { prefix } => {
				let keys: Vec<String> = self.prefix_iter(prefix).map(|e| e.key.clone()).collect();
				let mut changed = false;
				for key in keys {
					changed |= self.remove(&key, rev, events);
				}
				changed
			},
		}
	}

	fn put(
		&mut self,
		key: String,
		value: &[u8],
		ttl_ms: Option<u64>,
		rev: u64,
		events: &mut Vec<WatchEvent>,
	) {
		let expires_at_ms = ttl_ms.map(|ttl| self.applied_ms.saturating_add(ttl));
		let create_rev = match self.entries.get(&key) {
			Some(old) => {
				if let Some(at) = old.expires_at_ms {
					self.ttl.remove(&(at, key.clone()));
				}
				old.create_rev
			},
			None => rev,
		};
		if let Some(at) = expires_at_ms {
			self.ttl.insert((at, key.clone()));
		}
		let entry =
			Entry { key: key.clone(), value: value.to_vec(), create_rev, mod_rev: rev, expires_at_ms };
		events.push(WatchEvent::Put(entry.clone()));
		self.entries.insert(key, entry);
	}

	fn remove(&mut self, key: &str, rev: u64, events: &mut Vec<WatchEvent>) -> bool {
		let Some(old) = self.entries.remove(key) else {
			return false;
		};
		if let Some(at) = old.expires_at_ms {
			self.ttl.remove(&(at, old.key));
		}
		events.push(WatchEvent::Delete { key: key.to_owned(), revision: rev });
		true
	}
}

/// Smallest string greater than every string starting with `prefix`; `None`
/// when no such bound exists (empty prefix, or only `char::MAX`s).
fn prefix_end(prefix: &str) -> Option<String> {
	let mut end = prefix.to_owned();
	while let Some(last) = end.pop() {
		let next = (u32::from(last) + 1..=u32::from(char::MAX)).find_map(char::from_u32);
		if let Some(next) = next {
			end.push(next);
			return Some(end);
		}
	}
	None
}

#[cfg(test)]
mod tests {
	use super::*;

	fn put(key: &str, value: &str, ttl_ms: Option<u64>) -> Op {
		Op::Put { key: key.into(), value: value.into(), ttl_ms }
	}

	fn txn(state: &mut KvState, now_ms: u64, compare: Vec<Compare>, success: Vec<Op>) -> TxnResult {
		let command = Command::Txn { now_ms, txn: Txn { compare, success, failure: Vec::new() } };
		state.apply(&command, &mut Vec::new())[0]
	}

	#[test]
	fn batched_txns_apply_in_order_with_one_result_and_revision_each() {
		let mut s = KvState::default();
		let guarded = |compare: Compare, success: Op| Txn {
			compare: vec![compare],
			success: vec![success],
			failure: Vec::new(),
		};
		let txns = vec![
			guarded(Compare::Missing("a".into()), put("a", "1", None)),
			// Sees the first txn's write: the key is no longer missing.
			guarded(Compare::Missing("a".into()), put("a", "2", None)),
			guarded(Compare::Value { key: "a".into(), value: b"1".to_vec() }, put("b", "1", None)),
			guarded(Compare::Exists("b".into()), Op::Delete { key: "a".into() }),
		];
		let results = s.apply(&Command::Txns { now_ms: 1, txns }, &mut Vec::new());
		let outcome: Vec<(bool, u64)> = results.iter().map(|r| (r.succeeded, r.revision)).collect();
		assert_eq!(outcome, [(true, 1), (false, 1), (true, 2), (true, 3)]);
		assert_eq!(s.get("a"), None);
		assert_eq!(s.get("b").map(|e| e.mod_rev), Some(2));
	}

	#[test]
	fn revisions_track_create_and_modify() {
		let mut s = KvState::default();
		assert_eq!(txn(&mut s, 1, vec![], vec![put("a", "1", None)]).revision, 1);
		assert_eq!(
			txn(&mut s, 2, vec![], vec![put("b", "1", None), put("a", "2", None)]).revision,
			2
		);
		let a = s.get("a").unwrap();
		assert_eq!((a.create_rev, a.mod_rev, a.value.as_slice()), (1, 2, b"2".as_slice()));
		// A no-op branch (delete of a missing key) does not bump the revision.
		assert_eq!(txn(&mut s, 3, vec![], vec![Op::Delete { key: "zz".into() }]).revision, 2);
		// Re-creating after delete starts a new create_rev.
		txn(&mut s, 4, vec![], vec![Op::Delete { key: "a".into() }]);
		txn(&mut s, 5, vec![], vec![put("a", "3", None)]);
		assert_eq!(s.get("a").unwrap().create_rev, 4);
	}

	#[test]
	fn range_pages_within_the_prefix_in_both_directions() {
		let mut s = KvState::default();
		let keys = ["a/1", "a/2", "a/3", "a0", "a~", "b/1"];
		txn(&mut s, 1, vec![], keys.iter().map(|k| put(k, "", None)).collect());
		txn(&mut s, 2, vec![], vec![put("a/x", "", Some(1))]);
		let scan = |s: &KvState, after: Option<&str>, limit: Option<usize>, reverse: bool| {
			let range = Range { prefix: "a/".into(), after: after.map(Into::into), limit, reverse };
			s.range(&range)
				.into_iter()
				.map(|e| e.key)
				.collect::<Vec<_>>()
		};
		assert_eq!(scan(&s, None, None, false), ["a/1", "a/2", "a/3", "a/x"]);
		assert_eq!(scan(&s, Some("a/1"), Some(2), false), ["a/2", "a/3"]);
		assert_eq!(scan(&s, None, Some(2), true), ["a/x", "a/3"]);
		assert_eq!(scan(&s, Some("a/3"), None, true), ["a/2", "a/1"]);
		// Cursors outside the prefix clamp to its bounds instead of leaking keys.
		assert_eq!(scan(&s, Some("a"), Some(1), false), ["a/1"]);
		assert_eq!(scan(&s, Some("zz"), Some(1), true), ["a/x"]);
		assert_eq!(scan(&s, Some("zz"), None, false), Vec::<String>::new());
		assert_eq!(scan(&s, Some("a"), None, true), Vec::<String>::new());
		// Expired keys drop out of scans.
		txn(&mut s, 10, vec![], vec![]);
		assert_eq!(scan(&s, None, Some(1), true), ["a/3"]);
	}

	#[test]
	fn stamped_keys_share_the_commit_revision_and_sort_in_commit_order() {
		let mut s = KvState::default();
		let stamped = |suffix: &str| Op::PutStamped {
			prefix: "ev/".into(),
			suffix: suffix.into(),
			value:  b"v".to_vec(),
			ttl_ms: None,
		};
		let first = txn(&mut s, 1, vec![], vec![stamped(""), stamped("/idx")]).revision;
		txn(&mut s, 2, vec![], vec![put("other", "", None)]);
		let second = txn(&mut s, 3, vec![], vec![stamped("")]).revision;
		let keys: Vec<String> = s.list("ev/").into_iter().map(|e| e.key).collect();
		assert_eq!(keys, [
			format!("ev/{}", stamp(first)),
			format!("ev/{}/idx", stamp(first)),
			format!("ev/{}", stamp(second)),
		]);
		assert_eq!(s.get(&keys[0]).unwrap().create_rev, first);
	}

	#[test]
	fn compares_choose_branch() {
		let mut s = KvState::default();
		txn(&mut s, 1, vec![], vec![put("k", "v", None)]);
		let rev = s.get("k").unwrap().mod_rev;
		let cases = [
			(Compare::Exists("k".into()), true),
			(Compare::Exists("nope".into()), false),
			(Compare::Missing("nope".into()), true),
			(Compare::Missing("k".into()), false),
			(Compare::ModRev { key: "k".into(), rev }, true),
			(Compare::ModRev { key: "k".into(), rev: rev + 1 }, false),
			(Compare::ModRev { key: "nope".into(), rev: 0 }, true),
			(Compare::Value { key: "k".into(), value: b"v".to_vec() }, true),
			(Compare::Value { key: "k".into(), value: b"w".to_vec() }, false),
			(Compare::Value { key: "nope".into(), value: Vec::new() }, false),
		];
		for (compare, expected) in cases {
			let command = Command::Txn {
				now_ms: 2,
				txn:    Txn {
					compare: vec![compare.clone()],
					success: vec![],
					failure: vec![put("failed", "1", None)],
				},
			};
			let mut events = Vec::new();
			assert_eq!(s.apply(&command, &mut events)[0].succeeded, expected, "{compare:?}");
			assert_eq!(
				events.is_empty(),
				expected,
				"failure branch runs iff compare fails: {compare:?}"
			);
		}
	}

	#[test]
	fn count_below_admits_up_to_limit_and_ignores_sibling_prefixes() {
		let mut s = KvState::default();
		txn(&mut s, 1, vec![], vec![put("team/a/sbxx", "", None), put("team/ab/sbx/1", "", None)]);
		let admit =
			|s: &mut KvState, id: &str| {
				txn(s, 2, vec![Compare::CountBelow { prefix: "team/a/sbx/".into(), limit: 2 }], vec![
					put(&format!("team/a/sbx/{id}"), "", None),
				])
				.succeeded
			};
		assert!(admit(&mut s, "1"));
		assert!(admit(&mut s, "2"));
		assert!(!admit(&mut s, "3"));
		assert_eq!(s.list("team/a/sbx/").len(), 2);
		txn(&mut s, 3, vec![], vec![Op::Delete { key: "team/a/sbx/1".into() }]);
		assert!(admit(&mut s, "3"));
	}

	#[test]
	fn ttl_expires_on_applied_time_and_put_refreshes() {
		let mut s = KvState::default();
		txn(&mut s, 1000, vec![], vec![put("lock", "x", Some(500)), put("keep", "y", None)]);
		assert_eq!(s.get("lock").unwrap().expires_at_ms, Some(1500));
		assert_eq!(s.next_expiry_ms(), Some(1500));
		// Refresh before expiry moves the deadline and keeps create_rev.
		txn(&mut s, 1400, vec![], vec![put("lock", "x", Some(500))]);
		assert_eq!(s.next_expiry_ms(), Some(1900));
		let mut events = Vec::new();
		s.apply(&Command::Expire { now_ms: 1899 }, &mut events);
		assert!(events.is_empty(), "unexpected events: {events:?}");
		let before = s.revision;
		s.apply(&Command::Expire { now_ms: 1900 }, &mut events);
		assert_eq!(events, vec![WatchEvent::Delete {
			key:      "lock".into(),
			revision: before + 1,
		}]);
		assert!(s.get("lock").is_none());
		assert!(s.get("keep").is_some());
		assert_eq!(s.next_expiry_ms(), None);
	}

	#[test]
	fn applied_clock_never_regresses_and_expired_keys_count_as_missing() {
		let mut s = KvState::default();
		txn(&mut s, 2000, vec![], vec![put("lease", "a", Some(100))]);
		// A new leader with a lagging clock must not resurrect or extend
		// anything.
		s.apply(&Command::Expire { now_ms: 2100 }, &mut Vec::new());
		let r = txn(&mut s, 1500, vec![Compare::Missing("lease".into())], vec![put(
			"lease",
			"b",
			Some(100),
		)]);
		assert!(r.succeeded);
		assert_eq!(s.get("lease").unwrap().expires_at_ms, Some(2200));
	}

	#[test]
	fn delete_prefix_and_snapshot_round_trip() {
		let mut s = KvState::default();
		txn(&mut s, 1, vec![], vec![
			put("p/1", "", Some(10_000)),
			put("p/2", "", None),
			put("q", "", None),
		]);
		let mut events = Vec::new();
		let command = Command::Txn {
			now_ms: 2,
			txn:    Txn { success: vec![Op::DeletePrefix { prefix: "p/".into() }], ..Txn::default() },
		};
		s.apply(&command, &mut events);
		assert_eq!(events.len(), 2);
		assert_eq!(s.list(""), vec![s.get("q").unwrap()]);
		assert_eq!(s.next_expiry_ms(), None);

		txn(&mut s, 3, vec![], vec![put("t", "", Some(5))]);
		s.apply(&Command::SetClusterId { now_ms: 4, cluster_id: "c1".into() }, &mut Vec::new());
		let second =
			s.apply(&Command::SetClusterId { now_ms: 4, cluster_id: "c2".into() }, &mut Vec::new());
		assert!(!second[0].succeeded);
		let restored = KvState::from_snapshot(&s.to_snapshot()).unwrap();
		assert_eq!(restored, s);
		assert_eq!(restored.cluster_id(), Some("c1"));
		assert_eq!(restored.next_expiry_ms(), Some(8));
	}

	#[test]
	fn diff_reports_removed_changed_and_added_keys() {
		let mut old = KvState::default();
		txn(&mut old, 1, vec![], vec![
			put("gone", "", None),
			put("same", "", None),
			put("changed", "1", None),
		]);
		let mut next = old.clone();
		txn(&mut next, 2, vec![], vec![
			Op::Delete { key: "gone".into() },
			put("changed", "2", None),
			put("new", "", None),
		]);
		let events = old.diff(&next);
		let keys: Vec<(&str, bool)> = events
			.iter()
			.map(|e| match e {
				WatchEvent::Put(entry) => (entry.key.as_str(), true),
				WatchEvent::Delete { key, .. } => (key.as_str(), false),
			})
			.collect();
		assert_eq!(keys, vec![("gone", false), ("changed", true), ("new", true)]);
	}
}
