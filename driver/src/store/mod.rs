//! Durable driver state in the cluster's raft KV (ARCHITECTURE.md §7).
//!
//! One [`Store`] over a [`Kv`] handle; methods are grouped by record type in
//! the child modules (each owner adds an `impl Store` block in its own file).
//! Records are JSON values under `db/<table>/…` keys; secondary indexes and
//! unique constraints are extra keys under `db/<table>.<index>/…` that the
//! same transaction writes.
//!
//! Reads go through [`Store::read`], which runs a read barrier first, so a
//! store read observes every write acknowledged before it (like the SQL
//! database it replaces). Writes run in [`Tx`]: an optimistic transaction
//! whose reads become `ModRev` guards of one KV transaction:
//!
//! ```ignore
//! let mut tx = self.tx().await?;
//! loop {
//!     let Some(mut row) = tx.get::<Row>(&key).await? else { return Ok(false) };
//!     row.state = State::Paused;
//!     tx.put(&key, &row);
//!     if tx.commit().await? {
//!         return Ok(true);
//!     }
//! }
//! ```

pub mod builds;
pub mod events;
pub mod keys;
pub mod sandboxes;
pub mod secrets;
pub mod teams;
pub mod templates;
pub mod tokens;
mod tx;
pub mod users;
pub mod volumes;
pub mod webhooks;

use std::{borrow::Cow, fmt, time::Duration};

use base64::Engine;
use chrono::{DateTime, SubsecRound, Utc};
use vmon_cluster::kv::{Kv, KvError};

pub(crate) use self::tx::{Read, Tx};
pub use self::{
	events::{EventQuery, NewSandboxEvent, SandboxEventRow, event_types},
	keys::{ApiKeyRow, NewApiKey},
	secrets::SecretRow,
	teams::Team,
	tokens::{RefreshTokenRow, Rotation},
	users::User,
	volumes::VolumeRow,
	webhooks::{DeliveryFilter, DeliveryRow, NewDelivery, StatsBucket, WebhookRow, WebhookWrite},
};

/// Retention of history records.
///
/// Sandbox events, webhook deliveries, build logs, and the records of killed
/// sandboxes expire this long after they are written. Bounded because the KV
/// holds all state in memory on every driver.
pub const HISTORY_TTL: Duration = Duration::from_hours(7 * 24);

/// Durable driver state in the cluster KV. Cheap to clone.
#[derive(Clone)]
pub struct Store {
	kv: Kv,
}

impl fmt::Debug for Store {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Store").finish_non_exhaustive()
	}
}

/// Failure of a store call.
#[derive(Debug, thiserror::Error)]
pub enum Error {
	/// The KV refused or could not serve the call (no leader, unreachable
	/// drivers, …). A failed commit may still have been applied.
	#[error(transparent)]
	Kv(#[from] KvError),
	/// A stored value does not decode as its record type.
	#[error("corrupt store record {key}: {error}")]
	Corrupt { key: String, error: String },
	/// A unique constraint (named) is already taken.
	#[error("{0} already exists")]
	Duplicate(&'static str),
	/// A transaction kept losing its guards to concurrent writers.
	#[error("store transaction kept conflicting after {0} attempts")]
	Contention(u32),
}

/// Result of a store call.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Store {
	/// Store over the cluster KV. Drivers read and write through their raft
	/// replica; `vmon admin` passes a [`Kv::connect`] handle.
	pub const fn new(kv: Kv) -> Self {
		Self { kv }
	}

	/// Linearizable read view: runs one read barrier, then reads current state.
	///
	/// # Errors
	///
	/// [`Error::Kv`] when the barrier fails (no leader within the wait).
	pub(crate) async fn read(&self) -> Result<Read<'_>> {
		self.kv.sync().await?;
		Ok(Read::new(&self.kv))
	}

	/// Starts an optimistic transaction on a fresh read view (see [`Tx`]).
	///
	/// # Errors
	///
	/// As [`Store::read`].
	pub(crate) async fn tx(&self) -> Result<Tx<'_>> {
		Tx::begin(&self.kv).await
	}

	/// Health probe: succeeds once this driver can serve linearizable reads.
	///
	/// # Errors
	///
	/// As [`Store::read`].
	pub async fn ping(&self) -> Result<()> {
		self.read().await.map(drop)
	}
}

/// Current time on the stored grid (milliseconds, like the `DATETIME(3)`
/// columns it replaces), so values echoed by clients compare equal.
pub fn now() -> DateTime<Utc> {
	Utc::now().trunc_subsecs(3)
}

/// Key `{table}/{segments…}`; each segment is escaped (`%` → `%25`, `/` →
/// `%2F`) so user-chosen names never split or merge segments.
pub(crate) fn key(table: &str, segments: &[&str]) -> String {
	let mut out = String::from(table);
	for segment in segments {
		out.push('/');
		out.push_str(&escape(segment));
	}
	out
}

/// [`key`] plus a trailing `/`: the scan prefix of exactly the children of
/// `segments` (`db/x/team1/` never matches `db/x/team10/…`).
pub(crate) fn prefix(table: &str, segments: &[&str]) -> String {
	let mut out = key(table, segments);
	out.push('/');
	out
}

/// Unescaped segments of `key` after `table/` (empty when `key` is not under
/// `table`).
pub(crate) fn segments(key: &str, table: &str) -> Vec<String> {
	key.strip_prefix(table)
		.and_then(|rest| rest.strip_prefix('/'))
		.map(|rest| rest.split('/').map(unescape).collect())
		.unwrap_or_default()
}

/// Last unescaped segment of `key` (the id at the end of an index key).
pub(crate) fn last_segment(key: &str) -> String {
	unescape(key.rsplit('/').next().unwrap_or(key))
}

/// Zero-padded decimal segment, so numbers sort as strings.
pub(crate) fn seq(n: u64) -> String {
	format!("{n:020}")
}

/// Sortable timestamp segment: µs since the epoch, zero-padded (pre-epoch
/// times clamp to 0).
pub(crate) fn time_segment(at: DateTime<Utc>) -> String {
	seq(u64::try_from(at.timestamp_micros()).unwrap_or(0))
}

fn escape(segment: &str) -> Cow<'_, str> {
	if !segment.contains(['%', '/']) {
		return Cow::Borrowed(segment);
	}
	Cow::Owned(segment.replace('%', "%25").replace('/', "%2F"))
}

fn unescape(segment: &str) -> String {
	if segment.contains('%') {
		segment.replace("%2F", "/").replace("%25", "%")
	} else {
		segment.to_owned()
	}
}

/// Serde adapter storing bytes (ciphertexts) as standard base64 strings.
pub(crate) mod b64 {
	use base64::{Engine, engine::general_purpose::STANDARD};
	use serde::{Deserialize, Deserializer, Serializer};

	pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_str(&STANDARD.encode(bytes))
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
		let raw = String::deserialize(deserializer)?;
		STANDARD.decode(raw).map_err(serde::de::Error::custom)
	}
}

/// [`b64`] for optional bytes (`null` when absent).
pub(crate) mod b64_opt {
	use base64::{Engine, engine::general_purpose::STANDARD};
	use serde::{Deserialize, Deserializer, Serializer};

	#[expect(clippy::ref_option, reason = "signature fixed by serde's `with` attribute")]
	pub fn serialize<S: Serializer>(
		bytes: &Option<Vec<u8>>,
		serializer: S,
	) -> Result<S::Ok, S::Error> {
		match bytes {
			Some(bytes) => serializer.serialize_some(&STANDARD.encode(bytes)),
			None => serializer.serialize_none(),
		}
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(
		deserializer: D,
	) -> Result<Option<Vec<u8>>, D::Error> {
		Option::<String>::deserialize(deserializer)?
			.map(|raw| STANDARD.decode(raw).map_err(serde::de::Error::custom))
			.transpose()
	}
}

/// Opaque keyset cursor shared by every paginated list (`nextToken` /
/// `X-Next-Token`): URL-safe base64 of `"<created_at micros>__<id>"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
	/// Sort timestamp of the last returned row (µs since the epoch).
	pub created_at_us: i64,
	/// Id of the last returned row (tie-breaker).
	pub id:            String,
}

impl Cursor {
	/// Renders the token sent in `X-Next-Token`.
	pub fn encode(&self) -> String {
		base64::engine::general_purpose::URL_SAFE_NO_PAD
			.encode(format!("{}__{}", self.created_at_us, self.id))
	}

	/// Parses a client-supplied `nextToken`; `None` when malformed.
	pub fn decode(token: &str) -> Option<Self> {
		let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
			.decode(token.trim_end_matches('='))
			.ok()?;
		let text = String::from_utf8(raw).ok()?;
		let (ts, id) = text.split_once("__")?;
		if id.is_empty() {
			return None;
		}
		Some(Self { created_at_us: ts.parse().ok()?, id: id.to_owned() })
	}
}

/// Converts a stored timestamp to the µs used by [`Cursor`].
pub const fn micros(at: DateTime<Utc>) -> i64 {
	at.timestamp_micros()
}

/// Inverse of [`micros`].
pub fn from_micros(us: i64) -> DateTime<Utc> {
	DateTime::from_timestamp_micros(us).unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod testing;

#[cfg(test)]
mod tests {
	use super::{Cursor, key, last_segment, prefix, segments};

	#[test]
	fn cursor_round_trips_and_rejects_garbage() {
		let cursor =
			Cursor { created_at_us: 1_700_000_000_123_000, id: "abc__def".into() };
		assert_eq!(Cursor::decode(&cursor.encode()), Some(cursor));
		assert_eq!(Cursor::decode("not base64!"), None);
		assert_eq!(
			Cursor::decode(&base64::Engine::encode(
				&base64::engine::general_purpose::URL_SAFE_NO_PAD,
				"12__"
			)),
			None
		);
		assert_eq!(
			Cursor::decode(&base64::Engine::encode(
				&base64::engine::general_purpose::URL_SAFE_NO_PAD,
				"x__id"
			)),
			None
		);
	}

	#[test]
	fn key_segments_escape_separators_and_round_trip() {
		let k = key("db/x", &["a/b", "50%", "c"]);
		assert_eq!(k, "db/x/a%2Fb/50%25/c");
		assert_eq!(segments(&k, "db/x"), ["a/b", "50%", "c"]);
		assert_eq!(last_segment(&key("db/x", &["t", "%2F"])), "%2F");
		assert_eq!(segments("db/xy/a", "db/x"), Vec::<String>::new());
		// A team's prefix never covers a team whose id extends it.
		assert!(!key("db/x", &["team10", "k"]).starts_with(&prefix("db/x", &["team1"])));
	}
}
