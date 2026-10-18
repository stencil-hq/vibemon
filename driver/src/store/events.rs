//! Sandbox lifecycle event log (events API, webhooks, team metrics). Written
//! by the sandbox lifecycle through [`Store::record_event`] or, together with
//! a state change, by the transitions in [`super::sandboxes`].
//!
//! Keys (every one written with [`HISTORY_TTL`] in the event's commit):
//! - `db/event/<stamp>` → [`SandboxEventRow`] (JSON; `seq` is not stored): the
//!   log in global commit order. `<stamp>` is the commit revision
//!   ([`vmon_cluster::kv::stamp`]) and is the event's `seq`.
//! - `db/event.team/<team>/<stamp>` → `{type, ts}`: the team's events in commit
//!   order (events API, webhook cursor, metrics timeline).
//! - `db/event.sandbox/<team>/<sandbox>/<stamp>` → `{type, ts}`: one sandbox's
//!   events (events API with a sandbox filter).
//!
//! The index values carry the event type and time so type filters and the
//! metrics timeline never load records they discard; only returned events
//! read their `db/event/<stamp>` record. A commit stages at most one event
//! (all stamped keys of a commit share its stamp). No guards: events are
//! append-only.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use vmon_cluster::kv::{Range, stamp};

use super::{Error, HISTORY_TTL, Read, Result, Store, Tx, last_segment, prefix};

/// Event type names (`SandboxEvent.type`, webhook subscriptions).
pub mod event_types {
	/// Sandbox created (fresh create, fork child).
	pub const CREATED: &str = "sandbox.lifecycle.created";
	/// Sandbox killed (request, timeout, admin, orphaned, …).
	pub const KILLED: &str = "sandbox.lifecycle.killed";
	/// Configuration changed (timeout, network).
	pub const UPDATED: &str = "sandbox.lifecycle.updated";
	/// Sandbox paused.
	pub const PAUSED: &str = "sandbox.lifecycle.paused";
	/// Sandbox resumed from a pause.
	pub const RESUMED: &str = "sandbox.lifecycle.resumed";
	/// Snapshot (`POST /sandboxes/{id}/snapshots`) captured.
	pub const CHECKPOINTED: &str = "sandbox.lifecycle.checkpointed";
	/// Every known type.
	pub const ALL: [&str; 6] = [CREATED, KILLED, UPDATED, PAUSED, RESUMED, CHECKPOINTED];
}

/// Version stamped into every serialized event.
pub const EVENT_VERSION: &str = "v2";
/// Retention advertised in webhook payloads: events expire after
/// [`HISTORY_TTL`].
#[expect(clippy::cast_possible_truncation, reason = "asserted at compile time to fit in u32")]
pub const EVENTS_TTL_DAYS: u32 = {
	let days = HISTORY_TTL.as_secs() / DAY.as_secs();
	assert!(days * DAY.as_secs() == HISTORY_TTL.as_secs(), "HISTORY_TTL is whole days");
	assert!(days < 1 << 32, "HISTORY_TTL in days fits in u32");
	days as u32
};
const DAY: Duration = Duration::from_hours(24);

/// Primary records, in commit order.
const EVENTS: &str = "db/event";
/// Per-team index.
const BY_TEAM: &str = "db/event.team";
/// Per-(team, sandbox) index.
const BY_SANDBOX: &str = "db/event.sandbox";

/// Insert payload for [`Store::record_event`].
#[derive(Clone, Debug)]
pub struct NewSandboxEvent<'a> {
	pub team_id:      &'a str,
	pub sandbox_id:   &'a str,
	/// UUID minted per create/resume of the sandbox.
	pub execution_id: &'a str,
	pub template_id:  &'a str,
	pub build_id:     &'a str,
	/// One of [`event_types`].
	pub event_type:   &'static str,
	/// `{sandbox_metadata,
	/// execution{started_at,vcpu_count,memory_mb,execution_time},
	/// kill_reason?}`.
	pub data:         Value,
}

/// A stored event (`db/event/<stamp>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxEventRow {
	/// Commit revision stamped into the event's key: monotonic in commit
	/// order (webhook cursor). Not stored in the record.
	#[serde(skip)]
	pub seq:          u64,
	pub id:           String,
	pub team_id:      String,
	pub sandbox_id:   String,
	pub execution_id: String,
	pub template_id:  String,
	pub build_id:     String,
	#[serde(rename = "type")]
	pub event_type:   String,
	pub ts:           DateTime<Utc>,
	#[serde(default)]
	pub data:         Option<Value>,
}

/// Value of the index keys; also decodes the type and time of a full
/// [`SandboxEventRow`] record (same field names).
#[derive(Debug, Serialize, Deserialize)]
struct EventRef {
	#[serde(rename = "type")]
	event_type: String,
	ts:         DateTime<Utc>,
}

/// Filters of the events API.
#[derive(Clone, Debug, Default)]
pub struct EventQuery {
	/// Restrict to one sandbox.
	pub sandbox_id: Option<String>,
	/// Restrict to these types (empty = all).
	pub types:      Vec<String>,
	pub offset:     u32,
	pub limit:      u32,
	/// Oldest first when true.
	pub order_asc:  bool,
}

/// Short label of an event type (`kill`, `create`, …; deprecated field).
pub fn event_label(event_type: &str) -> &str {
	match event_type {
		event_types::CREATED => "create",
		event_types::KILLED => "kill",
		event_types::UPDATED => "update",
		event_types::PAUSED => "pause",
		event_types::RESUMED => "resume",
		event_types::CHECKPOINTED => "checkpoint",
		other => other.rsplit('.').next().unwrap_or(other),
	}
}

fn rfc3339(at: DateTime<Utc>) -> String {
	at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

impl SandboxEventRow {
	/// `SandboxEvent` schema of the events API (camelCase).
	pub fn to_api_json(&self) -> Value {
		json!({
			"id": self.id,
			"version": EVENT_VERSION,
			"type": self.event_type,
			"eventCategory": "lifecycle",
			"eventLabel": event_label(&self.event_type),
			"eventData": self.data,
			"timestamp": rfc3339(self.ts),
			"sandboxId": self.sandbox_id,
			"sandboxExecutionId": self.execution_id,
			"sandboxTemplateId": self.template_id,
			"sandboxBuildId": self.build_id,
			"sandboxTeamId": self.team_id,
		})
	}

	/// Webhook payload (`snake_case`, per the E2B webhook documentation).
	pub fn to_webhook_json(&self) -> Value {
		json!({
			"id": self.id,
			"version": EVENT_VERSION,
			"type": self.event_type,
			"timestamp": rfc3339(self.ts),
			"event_category": "lifecycle",
			"event_label": event_label(&self.event_type),
			"event_data": self.data,
			"sandbox_id": self.sandbox_id,
			"sandbox_execution_id": self.execution_id,
			"sandbox_template_id": self.template_id,
			"sandbox_build_id": self.build_id,
			"sandbox_team_id": self.team_id,
			"events_ttl_days": EVENTS_TTL_DAYS,
		})
	}
}

/// Stages `event` (record and both index keys) into `tx` and returns its id.
/// The event commits iff the transaction does; a transaction stages at most
/// one event.
pub(super) fn stage_event(tx: &mut Tx<'_>, event: &NewSandboxEvent<'_>) -> String {
	let id = uuid::Uuid::new_v4().to_string();
	let ts = super::now();
	let row = SandboxEventRow {
		seq: 0,
		id: id.clone(),
		team_id: event.team_id.to_owned(),
		sandbox_id: event.sandbox_id.to_owned(),
		execution_id: event.execution_id.to_owned(),
		template_id: event.template_id.to_owned(),
		build_id: event.build_id.to_owned(),
		event_type: event.event_type.to_owned(),
		ts,
		data: Some(event.data.clone()),
	};
	let index = EventRef { event_type: event.event_type.to_owned(), ts };
	let ttl = Some(HISTORY_TTL);
	tx.put_stamped(prefix(EVENTS, &[]), "", &row, ttl);
	tx.put_stamped(prefix(BY_TEAM, &[event.team_id]), "", &index, ttl);
	tx.put_stamped(prefix(BY_SANDBOX, &[event.team_id, event.sandbox_id]), "", &index, ttl);
	id
}

/// `seq` of the event at a stamped key (its last segment).
fn seq_of(key: &str) -> Result<u64> {
	last_segment(key)
		.parse()
		.map_err(|error: std::num::ParseIntError| Error::Corrupt {
			key:   key.to_owned(),
			error: error.to_string(),
		})
}

/// Loads the records of `seqs` in order, skipping expired ones.
async fn load(r: &Read<'_>, seqs: Vec<u64>) -> Result<Vec<SandboxEventRow>> {
	let mut rows = Vec::with_capacity(seqs.len());
	for seq in seqs {
		let key = format!("{}{}", prefix(EVENTS, &[]), stamp(seq));
		if let Some(mut row) = r.get::<SandboxEventRow>(&key).await? {
			row.seq = seq;
			rows.push(row);
		}
	}
	Ok(rows)
}

/// Whether `event_type` passes a type filter (empty = all).
fn wanted(types: &[String], event_type: &str) -> bool {
	types.is_empty() || types.iter().any(|t| t == event_type)
}

impl Store {
	/// Appends an event and returns its id. Webhook delivery picks it up on
	/// its next poll.
	pub async fn record_event(&self, event: &NewSandboxEvent<'_>) -> Result<String> {
		let mut tx = self.tx().await?;
		loop {
			let id = stage_event(&mut tx, event);
			if tx.commit().await? {
				return Ok(id);
			}
		}
	}

	/// Events API page (offset pagination, newest first unless `order_asc`;
	/// commit order).
	pub async fn list_events(
		&self,
		team_id: &str,
		query: &EventQuery,
	) -> Result<Vec<SandboxEventRow>> {
		if query.limit == 0 {
			return Ok(Vec::new());
		}
		let index = match &query.sandbox_id {
			Some(sandbox_id) => prefix(BY_SANDBOX, &[team_id, sandbox_id]),
			None => prefix(BY_TEAM, &[team_id]),
		};
		let range = Range { prefix: index, reverse: !query.order_asc, ..Range::default() };
		let offset = usize::try_from(query.offset).unwrap_or(usize::MAX);
		let limit = usize::try_from(query.limit).unwrap_or(usize::MAX);
		let r = self.read().await?;
		let mut skipped = 0;
		let mut seqs = Vec::new();
		let mut corrupt = None;
		r.scan_all::<EventRef>(&range, |key, event| {
			if !wanted(&query.types, &event.event_type) {
				return true;
			}
			if skipped < offset {
				skipped += 1;
				return true;
			}
			match seq_of(&key) {
				Ok(seq) => seqs.push(seq),
				Err(error) => {
					corrupt = Some(error);
					return false;
				},
			}
			seqs.len() < limit
		})
		.await?;
		if let Some(error) = corrupt {
			return Err(error);
		}
		load(&r, seqs).await
	}

	/// Events of a team with `seq > after_seq` (oldest first) restricted to
	/// `types`; used by webhook delivery.
	pub async fn events_after(
		&self,
		team_id: &str,
		after_seq: u64,
		types: &[String],
		limit: u32,
	) -> Result<Vec<SandboxEventRow>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let limit = usize::try_from(limit).unwrap_or(usize::MAX);
		let index = prefix(BY_TEAM, &[team_id]);
		let range = Range {
			after: Some(format!("{index}{}", stamp(after_seq))),
			prefix: index,
			..Range::default()
		};
		let r = self.read().await?;
		let mut seqs = Vec::new();
		let mut corrupt = None;
		r.scan_all::<EventRef>(&range, |key, event| {
			if !wanted(types, &event.event_type) {
				return true;
			}
			match seq_of(&key) {
				Ok(seq) => seqs.push(seq),
				Err(error) => {
					corrupt = Some(error);
					return false;
				},
			}
			seqs.len() < limit
		})
		.await?;
		if let Some(error) = corrupt {
			return Err(error);
		}
		load(&r, seqs).await
	}

	/// Highest retained event sequence number (0 when none).
	pub async fn max_event_seq(&self) -> Result<u64> {
		let range =
			Range { prefix: prefix(EVENTS, &[]), limit: Some(1), reverse: true, ..Range::default() };
		let last = self.read().await?.scan::<EventRef>(&range).await?;
		last.first().map_or(Ok(0), |(key, _)| seq_of(key))
	}

	/// Cluster-wide start events (created + resumed) with `seq > after_seq`,
	/// and the highest `seq` of any type (`after_seq` when none); one ordered
	/// scan of the log, used as the fleet sampler's cursor.
	pub async fn start_events_after(&self, after_seq: u64) -> Result<(u64, u64)> {
		let events = prefix(EVENTS, &[]);
		let range = Range {
			after: Some(format!("{events}{}", stamp(after_seq))),
			prefix: events,
			..Range::default()
		};
		let mut starts = 0;
		let mut last = None;
		self
			.read()
			.await?
			.scan_all::<EventRef>(&range, |key, event| {
				starts += u64::from(is_start(&event.event_type));
				last = Some(key);
				true
			})
			.await?;
		let max = last.as_deref().map_or(Ok(after_seq), seq_of)?;
		Ok((starts, max))
	}

	/// `(ts, type)` of a team's start/stop events in `[start, end]`, oldest
	/// first, plus the net running count before `start` over the retained
	/// events, clamped at 0 (team metrics).
	pub async fn lifecycle_timeline(
		&self,
		team_id: &str,
		start: DateTime<Utc>,
		end: DateTime<Utc>,
	) -> Result<(i64, Vec<(DateTime<Utc>, String)>)> {
		let range = Range { prefix: prefix(BY_TEAM, &[team_id]), ..Range::default() };
		let mut baseline = 0i64;
		let mut events = Vec::new();
		self
			.read()
			.await?
			.scan_all::<EventRef>(&range, |key, event| {
				let delta = if is_start(&event.event_type) {
					1
				} else if is_stop(&event.event_type) {
					-1
				} else {
					return true;
				};
				if event.ts < start {
					baseline += delta;
				} else if event.ts <= end {
					events.push((event.ts, key, event.event_type));
				}
				true
			})
			.await?;
		// Index keys are in commit order; timestamps are minted just before
		// the commit, so order by time with the commit as tie-breaker.
		events.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
		Ok((baseline.max(0), events.into_iter().map(|(ts, _, ty)| (ts, ty)).collect()))
	}
}

/// Whether an event type starts an execution (+1 running sandbox).
pub fn is_start(event_type: &str) -> bool {
	event_type == event_types::CREATED || event_type == event_types::RESUMED
}

/// Whether an event type ends an execution (−1 running sandbox).
pub fn is_stop(event_type: &str) -> bool {
	event_type == event_types::PAUSED || event_type == event_types::KILLED
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::store::testing::TestStore;

	const TEAM: &str = "00000000-0000-4000-8000-0000000000aa";

	fn new_event<'a>(sandbox_id: &'a str, event_type: &'static str) -> NewSandboxEvent<'a> {
		NewSandboxEvent {
			team_id: TEAM,
			sandbox_id,
			execution_id: "00000000-0000-4000-8000-000000000001",
			template_id: "tpl00000000000000000",
			build_id: "00000000-0000-4000-8000-000000000000",
			event_type,
			data: json!({"n": 1}),
		}
	}

	async fn record(store: &Store, event_type: &'static str) {
		store
			.record_event(&new_event("icursor", event_type))
			.await
			.unwrap();
	}

	#[tokio::test]
	async fn start_cursor_counts_creates_and_resumes_and_advances_past_other_types() {
		let db = TestStore::start().await;
		let store = &db.store;
		let start = store.max_event_seq().await.unwrap();
		assert_eq!(store.start_events_after(start).await.unwrap(), (0, start));

		for ty in
			[event_types::CREATED, event_types::PAUSED, event_types::RESUMED, event_types::KILLED]
		{
			record(store, ty).await;
		}
		let max = store.max_event_seq().await.unwrap();
		assert_eq!(store.start_events_after(start).await.unwrap(), (2, max));
		assert_eq!(store.start_events_after(max).await.unwrap(), (0, max));

		record(store, event_types::UPDATED).await;
		let (starts, next) = store.start_events_after(max).await.unwrap();
		assert_eq!(starts, 0);
		assert!(next > max, "non-start events still advance the cursor");
		db.finish().await;
	}

	#[tokio::test]
	async fn listings_filter_paginate_and_follow_the_cursor() {
		let db = TestStore::start().await;
		let store = &db.store;
		let types = [event_types::CREATED, event_types::PAUSED, event_types::RESUMED];
		for ty in types {
			store.record_event(&new_event("ia", ty)).await.unwrap();
			store.record_event(&new_event("ib", ty)).await.unwrap();
		}
		store
			.record_event(&NewSandboxEvent {
				team_id: "other",
				..new_event("ia", event_types::KILLED)
			})
			.await
			.unwrap();

		let all = EventQuery { limit: 100, ..EventQuery::default() };
		let newest_first = store.list_events(TEAM, &all).await.unwrap();
		assert_eq!(newest_first.len(), 6, "other teams' events are not listed");
		assert!(newest_first.windows(2).all(|w| w[0].seq > w[1].seq));
		assert_eq!(newest_first[0].data, Some(json!({"n": 1})));

		let query = EventQuery {
			sandbox_id: Some("ia".into()),
			types:      vec![event_types::PAUSED.into(), event_types::RESUMED.into()],
			offset:     1,
			limit:      10,
			order_asc:  true,
		};
		let page = store.list_events(TEAM, &query).await.unwrap();
		assert_eq!(page.len(), 1);
		assert_eq!(
			(page[0].sandbox_id.as_str(), page[0].event_type.as_str()),
			("ia", event_types::RESUMED)
		);

		let first = newest_first.last().unwrap().seq;
		let after = store
			.events_after(TEAM, first, &[event_types::CREATED.into()], 10)
			.await
			.unwrap();
		assert_eq!(
			after
				.iter()
				.map(|e| e.sandbox_id.as_str())
				.collect::<Vec<_>>(),
			["ib"]
		);
		let limited = store.events_after(TEAM, 0, &[], 2).await.unwrap();
		assert_eq!(limited.iter().map(|e| e.seq).collect::<Vec<_>>(), [
			newest_first[5].seq,
			newest_first[4].seq
		]);
		db.finish().await;
	}

	#[tokio::test]
	async fn timeline_counts_the_baseline_and_lists_the_window() {
		let db = TestStore::start().await;
		let store = &db.store;
		record(store, event_types::CREATED).await;
		record(store, event_types::CREATED).await;
		record(store, event_types::PAUSED).await;
		tokio::time::sleep(std::time::Duration::from_millis(5)).await;
		let start = Utc::now();
		tokio::time::sleep(std::time::Duration::from_millis(5)).await;
		record(store, event_types::UPDATED).await;
		record(store, event_types::KILLED).await;
		let (baseline, events) = store
			.lifecycle_timeline(TEAM, start, Utc::now())
			.await
			.unwrap();
		assert_eq!(baseline, 1);
		assert_eq!(events.into_iter().map(|(_, ty)| ty).collect::<Vec<_>>(), [event_types::KILLED]);
		db.finish().await;
	}
}
