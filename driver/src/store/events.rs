//! Sandbox lifecycle event log (events API, webhooks, team metrics). Written
//! by the sandbox lifecycle through [`Store::record_event`] or, together with
//! a state change, by the transitions in [`super::sandboxes`].
//!
//! Events live in the raft-replicated journal, not the KV. Each event is one
//! record (value: [`SandboxEventRow`] JSON without `seq`/`ts`) indexed in
//! three streams:
//! - `ev`: every event, in commit order (fleet sampler, newest `seq`).
//! - `ev/<team>`: the team's events (events API, webhook cursor, metrics
//!   timeline).
//! - `ev/<team>/<sandbox>`: one sandbox's events (events API with a sandbox
//!   filter).
//!
//! An event's `seq` is its record's journal position and its `ts` the
//! record's commit time. Records expire [`HISTORY_RETENTION`] after commit.
//! No guards: events are append-only, and a commit may stage several.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use vmon_cluster::kv::Scan;

use super::{HISTORY_RETENTION, Logged, Result, Store, Tx, key};

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
/// [`HISTORY_RETENTION`].
#[expect(clippy::cast_possible_truncation, reason = "asserted at compile time to fit in u32")]
pub const EVENTS_TTL_DAYS: u32 = {
	let days = HISTORY_RETENTION.as_secs() / DAY.as_secs();
	assert!(days * DAY.as_secs() == HISTORY_RETENTION.as_secs(), "HISTORY_RETENTION is whole days");
	assert!(days < 1 << 32, "HISTORY_RETENTION in days fits in u32");
	days as u32
};
const DAY: Duration = Duration::from_hours(24);

/// Journal stream table: `ev`, `ev/<team>`, `ev/<team>/<sandbox>`.
const EVENTS: &str = "ev";

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

/// A journal event record (streams `ev`, `ev/<team>`, `ev/<team>/<sandbox>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxEventRow {
	/// Journal position of the record: dense, monotonic in commit order
	/// (webhook cursor). Not stored in the value.
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
	/// Commit time of the record. Not stored in the value.
	#[serde(skip)]
	pub ts:           DateTime<Utc>,
	#[serde(default)]
	pub data:         Option<Value>,
}

/// Decodes only the type of a [`SandboxEventRow`] record, so filters and
/// counters skip the rest of the value.
#[derive(Debug, Deserialize)]
struct EventRef {
	#[serde(rename = "type")]
	event_type: String,
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

/// Stages `event` (one journal record in `ev`, `ev/<team>` and
/// `ev/<team>/<sandbox>`) into `tx` and returns its id. The event commits iff
/// the transaction does.
pub(super) fn stage_event(tx: &mut Tx<'_>, event: &NewSandboxEvent<'_>) -> String {
	let id = uuid::Uuid::new_v4().to_string();
	let row = SandboxEventRow {
		seq:          0,
		id:           id.clone(),
		team_id:      event.team_id.to_owned(),
		sandbox_id:   event.sandbox_id.to_owned(),
		execution_id: event.execution_id.to_owned(),
		template_id:  event.template_id.to_owned(),
		build_id:     event.build_id.to_owned(),
		event_type:   event.event_type.to_owned(),
		ts:           DateTime::default(),
		data:         Some(event.data.clone()),
	};
	tx.append(
		vec![
			key(EVENTS, &[]),
			key(EVENTS, &[event.team_id]),
			key(EVENTS, &[event.team_id, event.sandbox_id]),
		],
		&row,
	);
	id
}

/// Event of a journal record, with its `seq` and `ts`.
fn row(logged: Logged<SandboxEventRow>) -> SandboxEventRow {
	SandboxEventRow { seq: logged.seq, ts: logged.at, ..logged.value }
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
		let stream = match &query.sandbox_id {
			Some(sandbox_id) => key(EVENTS, &[team_id, sandbox_id]),
			None => key(EVENTS, &[team_id]),
		};
		let scan = Scan { stream, reverse: !query.order_asc, ..Scan::default() };
		let offset = usize::try_from(query.offset).unwrap_or(usize::MAX);
		let limit = usize::try_from(query.limit).unwrap_or(usize::MAX);
		let mut skipped = 0;
		let mut rows = Vec::new();
		self
			.read()
			.await?
			.journal_each::<SandboxEventRow>(&scan, |logged| {
				if !wanted(&query.types, &logged.value.event_type) {
					return true;
				}
				if skipped < offset {
					skipped += 1;
					return true;
				}
				rows.push(row(logged));
				rows.len() < limit
			})
			.await?;
		Ok(rows)
	}

	/// Events of a team with `seq > after_seq` (oldest first; 0 = from the
	/// start) restricted to `types`; used by webhook delivery.
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
		let scan =
			Scan { stream: key(EVENTS, &[team_id]), after: Some(after_seq), ..Scan::default() };
		let mut rows = Vec::new();
		self
			.read()
			.await?
			.journal_each::<SandboxEventRow>(&scan, |logged| {
				if wanted(types, &logged.value.event_type) {
					rows.push(row(logged));
				}
				rows.len() < limit
			})
			.await?;
		Ok(rows)
	}

	/// Highest retained event sequence number (0 when none).
	pub async fn max_event_seq(&self) -> Result<u64> {
		let scan =
			Scan { stream: key(EVENTS, &[]), limit: Some(1), reverse: true, ..Scan::default() };
		let last = self.read().await?.journal::<EventRef>(&scan).await?;
		Ok(last.first().map_or(0, |logged| logged.seq))
	}

	/// Cluster-wide start events (created + resumed) with `seq > after_seq`,
	/// and the highest `seq` of any type (`after_seq` when none); one ordered
	/// walk of the log, used as the fleet sampler's cursor.
	pub async fn start_events_after(&self, after_seq: u64) -> Result<(u64, u64)> {
		let scan = Scan { stream: key(EVENTS, &[]), after: Some(after_seq), ..Scan::default() };
		let mut starts = 0;
		let mut max = after_seq;
		self
			.read()
			.await?
			.journal_each::<EventRef>(&scan, |logged| {
				starts += u64::from(is_start(&logged.value.event_type));
				max = logged.seq;
				true
			})
			.await?;
		Ok((starts, max))
	}

	/// `(ts, type)` of a team's start/stop events in `[start, end]`, ordered
	/// by (time, seq), plus the net running count before `start` over the
	/// retained events, clamped at 0 (team metrics).
	pub async fn lifecycle_timeline(
		&self,
		team_id: &str,
		start: DateTime<Utc>,
		end: DateTime<Utc>,
	) -> Result<(i64, Vec<(DateTime<Utc>, String)>)> {
		let scan = Scan { stream: key(EVENTS, &[team_id]), ..Scan::default() };
		let mut baseline = 0i64;
		let mut events = Vec::new();
		self
			.read()
			.await?
			.journal_each::<EventRef>(&scan, |logged| {
				// Commit time never decreases with seq: nothing later is in
				// the window.
				if logged.at > end {
					return false;
				}
				let delta = if is_start(&logged.value.event_type) {
					1
				} else if is_stop(&logged.value.event_type) {
					-1
				} else {
					return true;
				};
				if logged.at < start {
					baseline += delta;
				} else {
					events.push((logged.at, logged.value.event_type));
				}
				true
			})
			.await?;
		Ok((baseline.max(0), events))
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

	#[tokio::test]
	async fn one_commit_holds_several_events_sharing_its_time() {
		let db = TestStore::start().await;
		let store = &db.store;
		let before = store.max_event_seq().await.unwrap();
		let mut tx = store.tx().await.unwrap();
		loop {
			stage_event(&mut tx, &new_event("ia", event_types::CREATED));
			stage_event(&mut tx, &new_event("ib", event_types::CREATED));
			if tx.commit().await.unwrap() {
				break;
			}
		}
		let rows = store.events_after(TEAM, before, &[], 10).await.unwrap();
		assert_eq!(rows.iter().map(|e| e.seq).collect::<Vec<_>>(), [before + 1, before + 2]);
		assert_eq!(rows[0].ts, rows[1].ts, "ts is the shared commit time");
		assert_eq!(store.max_event_seq().await.unwrap(), before + 2);
		let only_ib =
			EventQuery { sandbox_id: Some("ib".into()), limit: 10, ..EventQuery::default() };
		assert_eq!(store.list_events(TEAM, &only_ib).await.unwrap().len(), 1);
		db.finish().await;
	}
}
