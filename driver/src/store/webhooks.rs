//! Lifecycle webhooks and their delivery attempts.
//!
//! Keys:
//! - `db/webhook/<id>` → [`WebhookRow`]; its `last_event_seq` cursor only moves
//!   forward (guarded read-modify-write).
//! - `db/webhook.team/<team>/<id>` → empty: the team's webhooks.
//!
//! Delivery attempts live in the journal, not the KV: each attempt is one
//! record whose value is its [`DeliveryRow`] (`ts` comes from the record's
//! commit time), appended in the same transaction as a guarded read of the
//! webhook record (an attempt recorded for a deleted webhook is dropped).
//! Streams:
//! - `wh/<webhook>`: every attempt, in commit (= time) order; walked by the
//!   stats.
//! - `wh/<webhook>/event/<event>`: the attempts of one event.
//! - `wh/<webhook>/groups`: the first attempt of each event (one per event
//!   group, in order of first attempts), paged by the deliveries list. A first
//!   attempt repeated after a leader change appends a second group record;
//!   readers keep only the group record that is also the earliest record of the
//!   event's stream.
//!
//! Records expire after [`super::HISTORY_RETENTION`]; deleting a webhook
//! leaves them unreachable (webhook ids are never reused).

use std::collections::BTreeMap;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::kv::Scan;

use super::{Logged, Result, Store, key, last_segment, now, prefix};

const WEBHOOKS: &str = "db/webhook";
const WEBHOOK_TEAMS: &str = "db/webhook.team";
/// Journal table of delivery attempts (`wh/<webhook>/…` streams).
const DELIVERIES: &str = "wh";
/// Segment of the `wh/<webhook>/groups` stream.
const GROUPS: &str = "groups";
/// Segment of the `wh/<webhook>/event/<event>` streams.
const EVENT: &str = "event";
/// Group records fetched per journal page of [`Store::delivery_groups`].
const GROUP_PAGE: usize = 128;

/// Page position of [`Store::delivery_groups`] (the deliveries API's
/// `nextCursor`): the journal seq of the last listed group's record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliveryCursor(u64);

impl DeliveryCursor {
	/// Opaque token sent as `nextCursor`.
	pub fn encode(self) -> String {
		URL_SAFE_NO_PAD.encode(self.0.to_string())
	}

	/// Parses a client-supplied cursor; `None` when malformed.
	pub fn decode(token: &str) -> Option<Self> {
		let raw = URL_SAFE_NO_PAD.decode(token.trim_end_matches('=')).ok()?;
		String::from_utf8(raw).ok()?.parse().ok().map(Self)
	}
}

/// One event's delivery attempts ([`Store::delivery_groups`]).
#[derive(Clone, Debug)]
pub struct DeliveryGroup {
	/// Resumes the listing after this group.
	pub cursor:   DeliveryCursor,
	pub event_id: String,
	/// Attempts matching the filter, oldest first (never empty).
	pub attempts: Vec<DeliveryRow>,
}

/// A registered webhook.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WebhookRow {
	pub id:                   String,
	pub team_id:              String,
	pub name:                 String,
	pub url:                  String,
	pub events:               Vec<String>,
	pub enabled:              bool,
	/// `Keys::encrypt` of the signature secret.
	#[serde(with = "crate::store::b64")]
	pub signature_secret_enc: Vec<u8>,
	/// Delivery cursor: events with a larger `seq` are pending.
	pub last_event_seq:       u64,
	pub created_at:           DateTime<Utc>,
	pub updated_at:           DateTime<Utc>,
}

/// Fields for create (all set) or update (`None` = keep).
#[derive(Clone, Debug, Default)]
pub struct WebhookWrite {
	pub name:                 Option<String>,
	pub url:                  Option<String>,
	pub events:               Option<Vec<String>>,
	pub enabled:              Option<bool>,
	pub signature_secret_enc: Option<Vec<u8>>,
}

/// One delivery attempt to record.
#[derive(Clone, Debug)]
pub struct NewDelivery {
	pub team_id:          String,
	pub webhook_id:       String,
	pub event_id:         String,
	pub sandbox_id:       String,
	pub event_type:       String,
	/// `success` | `failed`.
	pub status:           &'static str,
	pub duration_ms:      i32,
	pub request_url:      String,
	pub request_headers:  String,
	pub request_body:     String,
	pub response_status:  Option<i32>,
	pub response_headers: Option<String>,
	pub response_body:    Option<String>,
	/// `http_error|dns_error|timeout|transport_error|request_error|signature_error|canceled`.
	pub error_class:      Option<&'static str>,
	pub error_message:    Option<String>,
	/// The event's first attempt: opens its group in
	/// [`Store::delivery_groups`]. Repeating it (a new leader redelivering the
	/// event) is harmless: readers deduplicate groups by event.
	pub first:            bool,
}

/// A stored delivery attempt (`WebhookDelivery`); the journal record value
/// of the `wh/<webhook>…` streams.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeliveryRow {
	pub id:               String,
	pub team_id:          String,
	pub webhook_id:       String,
	pub event_id:         String,
	pub sandbox_id:       String,
	pub event_type:       String,
	pub status:           String,
	pub duration_ms:      i32,
	pub request_url:      String,
	pub request_headers:  String,
	pub request_body:     String,
	pub response_status:  Option<i32>,
	pub response_headers: Option<String>,
	pub response_body:    Option<String>,
	pub error_class:      Option<String>,
	pub error_message:    Option<String>,
	/// Commit time of the journal record (not stored in the value).
	#[serde(skip)]
	pub ts:               DateTime<Utc>,
}

/// What the stats decode of a stored [`DeliveryRow`].
#[derive(Debug, Deserialize)]
struct DeliveryStat {
	status:      String,
	event_type:  String,
	duration_ms: i32,
}

/// What [`Store::delivery_groups`] decodes of a group record (a stored
/// [`DeliveryRow`]).
#[derive(Debug, Deserialize)]
struct DeliveryHead {
	event_id:   String,
	event_type: String,
}

/// Filters of the deliveries list and stats.
#[derive(Clone, Debug, Default)]
pub struct DeliveryFilter {
	pub start:       Option<DateTime<Utc>>,
	pub end:         Option<DateTime<Utc>>,
	/// `success` / `failed` (empty = both).
	pub statuses:    Vec<String>,
	/// Event types (empty = all).
	pub event_types: Vec<String>,
}

impl DeliveryFilter {
	fn matches(&self, ts: DateTime<Utc>, status: &str, event_type: &str) -> bool {
		self.start.is_none_or(|start| ts >= start)
			&& self.end.is_none_or(|end| ts <= end)
			&& self.wants_type(event_type)
			&& (self.statuses.is_empty() || self.statuses.iter().any(|s| s == status))
	}

	fn wants_type(&self, event_type: &str) -> bool {
		self.event_types.is_empty() || self.event_types.iter().any(|t| t == event_type)
	}
}

/// One aggregated stats bucket.
#[derive(Clone, Debug, PartialEq)]
pub struct StatsBucket {
	/// Bucket start.
	pub start:  DateTime<Utc>,
	pub total:  i64,
	pub failed: i64,
	pub min_ms: f64,
	pub avg_ms: f64,
	pub max_ms: f64,
}

/// Running totals of one stats bucket.
struct BucketSum {
	total:  i64,
	failed: i64,
	min:    i32,
	max:    i32,
	sum:    f64,
}

impl BucketSum {
	#[allow(clippy::cast_precision_loss, reason = "attempt counts are far below 2^52")]
	fn finish(&self, start: DateTime<Utc>) -> StatsBucket {
		StatsBucket {
			start,
			total: self.total,
			failed: self.failed,
			min_ms: f64::from(self.min),
			avg_ms: self.sum / self.total as f64,
			max_ms: f64::from(self.max),
		}
	}
}

fn webhook_key(id: &str) -> String {
	key(WEBHOOKS, &[id])
}

fn team_key(team_id: &str, id: &str) -> String {
	key(WEBHOOK_TEAMS, &[team_id, id])
}

/// An attempt with its time taken from its record.
fn attempt_row(logged: Logged<DeliveryRow>) -> DeliveryRow {
	DeliveryRow { ts: logged.at, ..logged.value }
}

impl Store {
	/// Registers a webhook. Its cursor starts at the current newest event,
	/// so only events after registration are delivered.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn create_webhook(
		&self,
		team_id: &str,
		name: &str,
		url: &str,
		events: &[String],
		enabled: bool,
		signature_secret_enc: &[u8],
	) -> Result<WebhookRow> {
		let at = now();
		let row = WebhookRow {
			id: uuid::Uuid::new_v4().to_string(),
			team_id: team_id.to_owned(),
			name: name.to_owned(),
			url: url.to_owned(),
			events: events.to_vec(),
			enabled,
			signature_secret_enc: signature_secret_enc.to_vec(),
			last_event_seq: self.max_event_seq().await?,
			created_at: at,
			updated_at: at,
		};
		let mut tx = self.tx().await?;
		loop {
			tx.put(webhook_key(&row.id), &row);
			tx.mark(team_key(team_id, &row.id), None);
			if tx.commit().await? {
				return Ok(row);
			}
		}
	}

	/// Webhook by id within a team.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn webhook(&self, team_id: &str, id: &str) -> Result<Option<WebhookRow>> {
		let row: Option<WebhookRow> = self.read().await?.get(&webhook_key(id)).await?;
		Ok(row.filter(|row| row.team_id == team_id))
	}

	/// A team's webhooks, oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn list_webhooks(&self, team_id: &str) -> Result<Vec<WebhookRow>> {
		let r = self.read().await?;
		let mut rows = Vec::new();
		for index in r.keys(&prefix(WEBHOOK_TEAMS, &[team_id])).await? {
			// Index and row are written and deleted together.
			if let Some(row) = r
				.get::<WebhookRow>(&webhook_key(&last_segment(&index)))
				.await?
			{
				rows.push(row);
			}
		}
		rows.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
		Ok(rows)
	}

	/// Every enabled webhook (delivery loop).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn enabled_webhooks(&self) -> Result<Vec<WebhookRow>> {
		let r = self.read().await?;
		let mut rows: Vec<WebhookRow> = r.list(&prefix(WEBHOOKS, &[])).await?;
		rows.retain(|row| row.enabled);
		rows.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
		Ok(rows)
	}

	/// Applies the `Some` fields; `None` when the webhook does not exist.
	/// Re-enabling a disabled webhook skips events that happened while it
	/// was off.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn update_webhook(
		&self,
		team_id: &str,
		id: &str,
		patch: &WebhookWrite,
	) -> Result<Option<WebhookRow>> {
		let row_key = webhook_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx
				.get::<WebhookRow>(&row_key)
				.await?
				.filter(|row| row.team_id == team_id)
			else {
				return Ok(None);
			};
			if patch.enabled == Some(true) && !row.enabled {
				row.last_event_seq = row.last_event_seq.max(self.max_event_seq().await?);
			}
			if let Some(name) = &patch.name {
				row.name.clone_from(name);
			}
			if let Some(url) = &patch.url {
				row.url.clone_from(url);
			}
			if let Some(events) = &patch.events {
				row.events.clone_from(events);
			}
			if let Some(enabled) = patch.enabled {
				row.enabled = enabled;
			}
			if let Some(secret) = &patch.signature_secret_enc {
				row.signature_secret_enc.clone_from(secret);
			}
			row.updated_at = now();
			tx.put(row_key.clone(), &row);
			if tx.commit().await? {
				return Ok(Some(row));
			}
		}
	}

	/// Deletes a webhook; `false` when unknown. Its deliveries stay in the
	/// journal, unreachable through the API, until they expire.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delete_webhook(&self, team_id: &str, id: &str) -> Result<bool> {
		let row_key = webhook_key(id);
		let mut tx = self.tx().await?;
		loop {
			let found = tx.get::<WebhookRow>(&row_key).await?;
			if found.is_none_or(|row| row.team_id != team_id) {
				return Ok(false);
			}
			tx.delete(row_key.clone());
			tx.delete(team_key(team_id, id));
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Moves a webhook's delivery cursor forward (never backwards).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn advance_webhook_cursor(&self, id: &str, seq: u64) -> Result<()> {
		let row_key = webhook_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<WebhookRow>(&row_key).await? else {
				return Ok(());
			};
			if row.last_event_seq >= seq {
				return Ok(());
			}
			row.last_event_seq = seq;
			tx.put(row_key.clone(), &row);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Records one delivery attempt (timed by its commit) and returns its
	/// id. The attempt is dropped when the webhook no longer exists.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn insert_delivery(&self, d: &NewDelivery) -> Result<String> {
		let row = DeliveryRow {
			id:               uuid::Uuid::new_v4().to_string(),
			team_id:          d.team_id.clone(),
			webhook_id:       d.webhook_id.clone(),
			event_id:         d.event_id.clone(),
			sandbox_id:       d.sandbox_id.clone(),
			event_type:       d.event_type.clone(),
			status:           d.status.to_owned(),
			duration_ms:      d.duration_ms,
			request_url:      d.request_url.clone(),
			request_headers:  d.request_headers.clone(),
			request_body:     d.request_body.clone(),
			response_status:  d.response_status,
			response_headers: d.response_headers.clone(),
			response_body:    d.response_body.clone(),
			error_class:      d.error_class.map(str::to_owned),
			error_message:    d.error_message.clone(),
			// Not stored: read back from the record's commit time.
			ts:               DateTime::default(),
		};
		let hook = d.webhook_id.as_str();
		let mut streams =
			vec![key(DELIVERIES, &[hook]), key(DELIVERIES, &[hook, EVENT, &d.event_id])];
		if d.first {
			streams.push(key(DELIVERIES, &[hook, GROUPS]));
		}
		let hook_key = webhook_key(hook);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<WebhookRow>(&hook_key).await?.is_none() {
				return Ok(row.id);
			}
			tx.append(streams.clone(), &row);
			if tx.commit().await? {
				return Ok(row.id);
			}
		}
	}

	/// One page of delivery attempts grouped by event. Groups are ordered by
	/// their first attempt (the journal order of the `wh/<webhook>/groups`
	/// stream), and a group is in `filter`'s window when its first attempt
	/// is; each group lists its attempts matching `filter` (oldest first) and
	/// groups without any are skipped. `after` is the cursor of the last group
	/// of the previous page. Returns up to `limit` groups.
	///
	/// The journal cannot seek by time: groups outside the window on the
	/// scan's starting side are read and skipped.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delivery_groups(
		&self,
		webhook_id: &str,
		filter: &DeliveryFilter,
		after: Option<DeliveryCursor>,
		limit: u32,
		order_asc: bool,
	) -> Result<Vec<DeliveryGroup>> {
		let limit = limit as usize;
		let mut out = Vec::new();
		if limit == 0 {
			return Ok(out);
		}
		let r = self.read().await?;
		let mut scan = Scan {
			stream:  key(DELIVERIES, &[webhook_id, GROUPS]),
			after:   after.map(|cursor| cursor.0),
			limit:   Some(GROUP_PAGE),
			reverse: !order_asc,
		};
		let early = |at: DateTime<Utc>| filter.start.is_some_and(|start| at < start);
		let late = |at: DateTime<Utc>| filter.end.is_some_and(|end| at > end);
		loop {
			let page = r.journal::<DeliveryHead>(&scan).await?;
			let more = page.len() == GROUP_PAGE;
			scan.after = page.last().map(|group| group.seq);
			for group in page {
				let (past, before) = if order_asc {
					(late(group.at), early(group.at))
				} else {
					(early(group.at), late(group.at))
				};
				if past {
					return Ok(out);
				}
				if before || !filter.wants_type(&group.value.event_type) {
					continue;
				}
				let event = group.value.event_id;
				let attempts = r
					.journal::<DeliveryRow>(&Scan {
						stream: key(DELIVERIES, &[webhook_id, EVENT, &event]),
						..Scan::default()
					})
					.await?;
				// A repeated first attempt is not the event's group: only the
				// event's earliest record is.
				if attempts.first().is_none_or(|first| first.seq != group.seq) {
					continue;
				}
				let attempts: Vec<DeliveryRow> = attempts
					.into_iter()
					.map(attempt_row)
					.filter(|a| filter.matches(a.ts, &a.status, &a.event_type))
					.collect();
				if attempts.is_empty() {
					continue;
				}
				out.push(DeliveryGroup {
					cursor: DeliveryCursor(group.seq),
					event_id: event,
					attempts,
				});
				if out.len() >= limit {
					return Ok(out);
				}
			}
			if !more {
				return Ok(out);
			}
		}
	}

	/// Aggregates attempts in `filter`'s window into buckets of
	/// `bucket_secs`, oldest first. Walks the webhook's attempts newest
	/// first down to the window's start.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delivery_stats(
		&self,
		webhook_id: &str,
		filter: &DeliveryFilter,
		bucket_secs: i64,
	) -> Result<Vec<StatsBucket>> {
		let bucket_secs = bucket_secs.max(1);
		let scan = Scan { stream: key(DELIVERIES, &[webhook_id]), reverse: true, ..Scan::default() };
		let mut buckets = BTreeMap::<i64, BucketSum>::new();
		self
			.read()
			.await?
			.journal_each::<DeliveryStat>(&scan, |Logged { at, value: stat, .. }| {
				if filter.start.is_some_and(|start| at < start) {
					return false;
				}
				if filter.matches(at, &stat.status, &stat.event_type) {
					let bucket = at.timestamp_millis().div_euclid(bucket_secs * 1000);
					let sum = buckets.entry(bucket).or_insert(BucketSum {
						total:  0,
						failed: 0,
						min:    i32::MAX,
						max:    i32::MIN,
						sum:    0.0,
					});
					sum.total += 1;
					sum.failed += i64::from(stat.status == "failed");
					sum.min = sum.min.min(stat.duration_ms);
					sum.max = sum.max.max(stat.duration_ms);
					sum.sum += f64::from(stat.duration_ms);
				}
				true
			})
			.await?;
		Ok(buckets
			.iter()
			.map(|(bucket, sum)| {
				sum.finish(DateTime::from_timestamp(bucket * bucket_secs, 0).unwrap_or_default())
			})
			.collect())
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use chrono::DateTime;

	use super::{DeliveryFilter, NewDelivery, WebhookWrite};
	use crate::store::{Store, testing::TestStore};

	fn attempt(hook: &str, event: &str, status: &'static str, ms: i32, first: bool) -> NewDelivery {
		NewDelivery {
			team_id: "team".into(),
			webhook_id: hook.into(),
			event_id: event.into(),
			sandbox_id: "isbx".into(),
			event_type: if event.starts_with('k') {
				"killed"
			} else {
				"created"
			}
			.into(),
			status,
			duration_ms: ms,
			request_url: "http://hook".into(),
			request_headers: "{}".into(),
			request_body: "{}".into(),
			response_status: None,
			response_headers: None,
			response_body: None,
			error_class: (status == "failed").then_some("http_error"),
			error_message: None,
			first,
		}
	}

	async fn groups(
		store: &Store,
		hook: &str,
		filter: &DeliveryFilter,
		asc: bool,
		limit: u32,
	) -> Vec<String> {
		let mut out = Vec::new();
		let mut cursor = None;
		loop {
			let page = store
				.delivery_groups(hook, filter, cursor, limit, asc)
				.await
				.unwrap();
			let Some(last) = page.last() else {
				return out;
			};
			cursor = Some(last.cursor);
			out.extend(
				page
					.into_iter()
					.map(|group| format!("{}:{}", group.event_id, group.attempts.len())),
			);
		}
	}

	#[tokio::test]
	async fn webhook_cursor_only_moves_forward_and_deleted_hooks_drop_attempts() {
		let db = TestStore::start().await;
		let store = &db.store;
		let hook = store
			.create_webhook("team", "h", "http://hook", &["killed".into()], true, b"enc")
			.await
			.unwrap();
		assert_eq!(store.webhook("team", &hook.id).await.unwrap().unwrap().id, hook.id);
		assert!(store.webhook("other", &hook.id).await.unwrap().is_none());
		let seq = hook.last_event_seq;
		store
			.advance_webhook_cursor(&hook.id, seq + 10)
			.await
			.unwrap();
		store
			.advance_webhook_cursor(&hook.id, seq + 5)
			.await
			.unwrap();
		assert_eq!(
			store
				.webhook("team", &hook.id)
				.await
				.unwrap()
				.unwrap()
				.last_event_seq,
			seq + 10
		);

		let off = WebhookWrite { enabled: Some(false), ..WebhookWrite::default() };
		let updated = store
			.update_webhook("team", &hook.id, &off)
			.await
			.unwrap()
			.unwrap();
		assert!(!updated.enabled);
		assert!(store.enabled_webhooks().await.unwrap().is_empty());
		let renamed = WebhookWrite {
			name: Some("renamed".into()),
			enabled: Some(true),
			..WebhookWrite::default()
		};
		let back = store
			.update_webhook("team", &hook.id, &renamed)
			.await
			.unwrap()
			.unwrap();
		assert_eq!((back.name.as_str(), back.enabled), ("renamed", true));
		assert!(back.last_event_seq >= seq + 10, "re-enabling never moves the cursor back");
		assert!(
			store
				.update_webhook("other", &hook.id, &renamed)
				.await
				.unwrap()
				.is_none()
		);

		store
			.insert_delivery(&attempt(&hook.id, "k1", "success", 5, true))
			.await
			.unwrap();
		let all = DeliveryFilter::default();
		assert_eq!(groups(store, &hook.id, &all, true, 10).await, ["k1:1"]);
		assert!(!store.delete_webhook("other", &hook.id).await.unwrap());
		assert!(store.delete_webhook("team", &hook.id).await.unwrap());
		assert!(!store.delete_webhook("team", &hook.id).await.unwrap());
		assert_eq!(store.list_webhooks("team").await.unwrap().len(), 0);
		assert!(store.webhook("team", &hook.id).await.unwrap().is_none());
		// Attempts recorded after the delete are dropped.
		store
			.insert_delivery(&attempt(&hook.id, "k2", "success", 5, true))
			.await
			.unwrap();
		assert_eq!(groups(store, &hook.id, &all, true, 10).await, ["k1:1"]);
		db.finish().await;
	}

	#[tokio::test]
	async fn deliveries_group_page_and_aggregate() {
		let db = TestStore::start().await;
		let store = &db.store;
		let hook = store
			.create_webhook("team", "h", "http://hook", &["killed".into()], true, b"enc")
			.await
			.unwrap()
			.id;
		// k1: failed then retried successfully; c2, k3 and k4 later. Attempts
		// are timed by their commits: keep them apart on the ms grid.
		for (event, status, ms, first) in [
			("k1", "failed", 100, true),
			("k1", "success", 20, false),
			("c2", "success", 30, true),
			("k3", "failed", 40, true),
			("k4", "success", 50, true),
		] {
			store
				.insert_delivery(&attempt(&hook, event, status, ms, first))
				.await
				.unwrap();
			tokio::time::sleep(Duration::from_millis(3)).await;
		}
		let all = DeliveryFilter::default();
		let listed = store
			.delivery_groups(&hook, &all, None, 10, true)
			.await
			.unwrap();
		let first_at = |event: &str| {
			let group = listed.iter().find(|group| group.event_id == event).unwrap();
			group.attempts[0].ts
		};
		let asc = groups(store, &hook, &all, true, 2).await;
		assert_eq!(asc, ["k1:2", "c2:1", "k3:1", "k4:1"]);
		let mut desc = groups(store, &hook, &all, false, 1).await;
		desc.reverse();
		assert_eq!(desc, asc, "descending pages are the reverse order");

		let failed = DeliveryFilter { statuses: vec!["failed".into()], ..DeliveryFilter::default() };
		assert_eq!(groups(store, &hook, &failed, true, 10).await, ["k1:1", "k3:1"]);
		let window = DeliveryFilter {
			start: Some(first_at("c2")),
			end: Some(first_at("k4")),
			event_types: vec!["killed".into()],
			..DeliveryFilter::default()
		};
		assert_eq!(groups(store, &hook, &window, false, 10).await, ["k4:1", "k3:1"]);
		let to_k3 = DeliveryFilter { end: Some(first_at("k3")), ..window.clone() };
		assert_eq!(groups(store, &hook, &to_k3, true, 10).await, ["k3:1"]);

		// One bucket spanning every attempt (it starts at the epoch).
		let stats = store
			.delivery_stats(&hook, &all, 1_000_000_000_000)
			.await
			.unwrap();
		let summary: Vec<_> = stats
			.iter()
			.map(|b| (b.start, b.total, b.failed, b.min_ms, b.avg_ms, b.max_ms))
			.collect();
		let epoch = DateTime::from_timestamp(0, 0).unwrap();
		assert_eq!(summary, [(epoch, 5, 2, 20.0, 48.0, 100.0)]);
		let minutes = store.delivery_stats(&hook, &all, 60).await.unwrap();
		assert_eq!(minutes.iter().map(|b| b.total).sum::<i64>(), 5);
		assert!(minutes.iter().all(|b| b.start.timestamp() % 60 == 0));
		assert!(minutes.windows(2).all(|w| w[0].start < w[1].start));
		let windowed = store.delivery_stats(&hook, &window, 60).await.unwrap();
		let totals = windowed
			.iter()
			.fold((0, 0), |(total, failed), b| (total + b.total, failed + b.failed));
		assert_eq!(totals, (2, 1));

		// A new leader repeating k1's first attempt adds an attempt, not a
		// group.
		store
			.insert_delivery(&attempt(&hook, "k1", "success", 10, true))
			.await
			.unwrap();
		let asc = groups(store, &hook, &all, true, 1).await;
		assert_eq!(asc, ["k1:3", "c2:1", "k3:1", "k4:1"]);
		let mut desc = groups(store, &hook, &all, false, 1).await;
		desc.reverse();
		assert_eq!(desc, asc);
		db.finish().await;
	}
}
