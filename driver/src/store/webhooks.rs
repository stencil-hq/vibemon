//! Lifecycle webhooks and their delivery attempts.
//!
//! Keys:
//! - `db/webhook/<id>` → [`WebhookRow`]; its `last_event_seq` cursor only moves
//!   forward (guarded read-modify-write).
//! - `db/webhook.team/<team>/<id>` → empty: the team's webhooks.
//!
//! Deliveries (all written with [`HISTORY_TTL`], removed with their webhook;
//! an attempt recorded for a deleted webhook is dropped):
//! - `db/delivery/<webhook>/<event>/<time(ts)>/<id>` → [`DeliveryRow`]: the
//!   attempts of one event in time order.
//! - `db/delivery.event/<webhook>/<event>` → time of the event's first attempt
//!   (guarded: the first attempt creates the group exactly once).
//! - `db/delivery.group/<webhook>/<time(first attempt)>/<event>` → event type:
//!   groups in order of their first attempt, paged by range scans.
//! - `db/delivery.ts/<webhook>/<time(ts)>/<id>` → time, status, event type and
//!   duration of every attempt in time order, scanned by the stats.

use std::collections::BTreeMap;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::kv::Range;

use super::{
	Cursor, Error, HISTORY_TTL, Result, Store, from_micros, key, last_segment, micros, now, prefix,
	segments, seq, time_segment,
};

const WEBHOOKS: &str = "db/webhook";
const WEBHOOK_TEAMS: &str = "db/webhook.team";
const DELIVERIES: &str = "db/delivery";
const DELIVERY_EVENTS: &str = "db/delivery.event";
const DELIVERY_GROUPS: &str = "db/delivery.group";
const DELIVERY_TIMES: &str = "db/delivery.ts";
/// Groups fetched per range page of [`Store::delivery_groups`].
const GROUP_PAGE: usize = 128;

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
	pub ts:               DateTime<Utc>,
}

/// A stored delivery attempt (`WebhookDelivery`).
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
	pub ts:               DateTime<Utc>,
}

/// What the stats need of one attempt (`db/delivery.ts/…` value).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DeliveryStat {
	ts:          DateTime<Utc>,
	status:      String,
	event_type:  String,
	duration_ms: i32,
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

/// Where a [`Store::delivery_groups`] scan starts: strictly past the cursor
/// and inside the window bound on the scan's side.
fn groups_after(
	webhook_id: &str,
	filter: &DeliveryFilter,
	after: Option<&Cursor>,
	order_asc: bool,
) -> Option<String> {
	let base = prefix(DELIVERY_GROUPS, &[webhook_id]);
	let cursor = after.map(|c| {
		key(DELIVERY_GROUPS, &[webhook_id, &time_segment(from_micros(c.created_at_us)), &c.id])
	});
	if order_asc {
		// Every key of a group first attempted at or after `start` sorts
		// after `{base}{time(start)}`.
		let bound = filter
			.start
			.map(|start| format!("{base}{}", time_segment(start)));
		cursor.into_iter().chain(bound).max()
	} else {
		// … and every key of one first attempted at or before `end` sorts
		// before `{base}{time(end) + 1}`.
		let bound = filter.end.map(|end| {
			let us = u64::try_from(end.timestamp_micros()).unwrap_or(0);
			format!("{base}{}", seq(us.saturating_add(1)))
		});
		cursor.into_iter().chain(bound).min()
	}
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

	/// Deletes a webhook and its deliveries; `false` when unknown.
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
			for table in [DELIVERIES, DELIVERY_EVENTS, DELIVERY_GROUPS, DELIVERY_TIMES] {
				tx.delete_prefix(prefix(table, &[id]));
			}
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

	/// Records one delivery attempt and returns its id. The attempt is
	/// dropped when the webhook no longer exists (its deliveries are gone).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn insert_delivery(&self, d: &NewDelivery) -> Result<String> {
		let ts = d.ts.trunc_subsecs(3);
		let time = time_segment(ts);
		let row = DeliveryRow {
			id: uuid::Uuid::new_v4().to_string(),
			team_id: d.team_id.clone(),
			webhook_id: d.webhook_id.clone(),
			event_id: d.event_id.clone(),
			sandbox_id: d.sandbox_id.clone(),
			event_type: d.event_type.clone(),
			status: d.status.to_owned(),
			duration_ms: d.duration_ms,
			request_url: d.request_url.clone(),
			request_headers: d.request_headers.clone(),
			request_body: d.request_body.clone(),
			response_status: d.response_status,
			response_headers: d.response_headers.clone(),
			response_body: d.response_body.clone(),
			error_class: d.error_class.map(str::to_owned),
			error_message: d.error_message.clone(),
			ts,
		};
		let stat = DeliveryStat {
			ts,
			status: row.status.clone(),
			event_type: row.event_type.clone(),
			duration_ms: row.duration_ms,
		};
		let hook = d.webhook_id.as_str();
		let event = d.event_id.as_str();
		let event_key = key(DELIVERY_EVENTS, &[hook, event]);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<WebhookRow>(&webhook_key(hook)).await?.is_none() {
				return Ok(row.id);
			}
			if tx.get::<DateTime<Utc>>(&event_key).await?.is_none() {
				tx.put_ttl(event_key.clone(), &ts, HISTORY_TTL);
				tx.put_ttl(key(DELIVERY_GROUPS, &[hook, &time, event]), &row.event_type, HISTORY_TTL);
			}
			tx.put_ttl(key(DELIVERIES, &[hook, event, &time, &row.id]), &row, HISTORY_TTL);
			tx.put_ttl(key(DELIVERY_TIMES, &[hook, &time, &row.id]), &stat, HISTORY_TTL);
			if tx.commit().await? {
				return Ok(row.id);
			}
		}
	}

	/// One page of delivery attempts grouped by event. Groups are ordered by
	/// their first attempt (`(first ts, event id)`, the cursor), and a group
	/// is in `filter`'s window when its first attempt is; each group lists
	/// its attempts matching `filter` (oldest first) and groups without any
	/// are skipped. `after` is the cursor of the last group of the previous
	/// page. Returns up to `limit` groups.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delivery_groups(
		&self,
		webhook_id: &str,
		filter: &DeliveryFilter,
		after: Option<&Cursor>,
		limit: u32,
		order_asc: bool,
	) -> Result<Vec<(Cursor, Vec<DeliveryRow>)>> {
		let limit = limit as usize;
		let mut out = Vec::new();
		if limit == 0 {
			return Ok(out);
		}
		let r = self.read().await?;
		let mut range = Range {
			prefix:  prefix(DELIVERY_GROUPS, &[webhook_id]),
			after:   groups_after(webhook_id, filter, after, order_asc),
			limit:   Some(GROUP_PAGE),
			reverse: !order_asc,
		};
		loop {
			let page = r.scan::<String>(&range).await?;
			let more = page.len() == GROUP_PAGE;
			range.after = page.last().map(|(key, _)| key.clone());
			for (group, event_type) in page {
				let [_, time, event] =
					<[String; 3]>::try_from(segments(&group, DELIVERY_GROUPS)).map_err(|_| {
						Error::Corrupt { key: group.clone(), error: "not a delivery group key".into() }
					})?;
				let first = from_micros(time.parse().unwrap_or_default());
				let past = if order_asc {
					filter.end.is_some_and(|end| first > end)
				} else {
					filter.start.is_some_and(|start| first < start)
				};
				if past {
					return Ok(out);
				}
				if !filter.wants_type(&event_type) {
					continue;
				}
				let mut attempts: Vec<DeliveryRow> =
					r.list(&prefix(DELIVERIES, &[webhook_id, &event])).await?;
				attempts.retain(|a| filter.matches(a.ts, &a.status, &a.event_type));
				if attempts.is_empty() {
					continue;
				}
				out.push((Cursor { created_at_us: micros(first), id: event }, attempts));
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
	/// `bucket_secs`, oldest first.
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
		let base = prefix(DELIVERY_TIMES, &[webhook_id]);
		let range = Range {
			// Every attempt at or after `start` sorts after `{base}{time(start)}`.
			after:   filter
				.start
				.map(|start| format!("{base}{}", time_segment(start))),
			prefix:  base,
			limit:   None,
			reverse: false,
		};
		let mut buckets = BTreeMap::<i64, BucketSum>::new();
		self
			.read()
			.await?
			.scan_all::<DeliveryStat>(&range, |_, stat| {
				if filter.end.is_some_and(|end| stat.ts > end) {
					return false;
				}
				if filter.matches(stat.ts, &stat.status, &stat.event_type) {
					let bucket = stat.ts.timestamp_millis().div_euclid(bucket_secs * 1000);
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
	use chrono::{DateTime, Utc};

	use super::{DeliveryFilter, NewDelivery, WebhookWrite};
	use crate::store::{Store, testing::TestStore};

	fn at(secs: i64) -> DateTime<Utc> {
		DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
	}

	fn attempt(hook: &str, event: &str, status: &'static str, secs: i64, ms: i32) -> NewDelivery {
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
			ts: at(secs),
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
				.delivery_groups(hook, filter, cursor.as_ref(), limit, asc)
				.await
				.unwrap();
			let Some((last, _)) = page.last() else {
				return out;
			};
			cursor = Some(last.clone());
			out.extend(
				page
					.into_iter()
					.map(|(c, attempts)| format!("{}:{}", c.id, attempts.len())),
			);
		}
	}

	#[tokio::test]
	async fn webhook_cursor_only_moves_forward_and_delete_drops_deliveries() {
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
			.insert_delivery(&attempt(&hook.id, "k1", "success", 0, 5))
			.await
			.unwrap();
		let all = DeliveryFilter::default();
		assert_eq!(groups(store, &hook.id, &all, true, 10).await.len(), 1);
		assert!(!store.delete_webhook("other", &hook.id).await.unwrap());
		assert!(store.delete_webhook("team", &hook.id).await.unwrap());
		assert!(!store.delete_webhook("team", &hook.id).await.unwrap());
		assert_eq!(store.list_webhooks("team").await.unwrap().len(), 0);
		assert_eq!(groups(store, &hook.id, &all, true, 10).await.len(), 0);
		assert_eq!(
			store
				.delivery_stats(&hook.id, &all, 60)
				.await
				.unwrap()
				.len(),
			0
		);
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
		// k1: failed then retried successfully; c2 and k3 later; k4 has the
		// same first attempt time as k3.
		for (event, status, secs, ms) in [
			("k1", "failed", 0, 100),
			("k1", "success", 1, 20),
			("c2", "success", 70, 30),
			("k3", "failed", 130, 40),
			("k4", "success", 130, 50),
		] {
			store
				.insert_delivery(&attempt(&hook, event, status, secs, ms))
				.await
				.unwrap();
		}
		let all = DeliveryFilter::default();
		let asc = groups(store, &hook, &all, true, 2).await;
		assert_eq!(asc, ["k1:2", "c2:1", "k3:1", "k4:1"]);
		let mut desc = groups(store, &hook, &all, false, 1).await;
		desc.reverse();
		assert_eq!(desc, asc, "descending pages are the reverse order");

		let failed = DeliveryFilter { statuses: vec!["failed".into()], ..DeliveryFilter::default() };
		assert_eq!(groups(store, &hook, &failed, true, 10).await, ["k1:1", "k3:1"]);
		let window = DeliveryFilter {
			start: Some(at(60)),
			end: Some(at(130)),
			event_types: vec!["killed".into()],
			..DeliveryFilter::default()
		};
		assert_eq!(groups(store, &hook, &window, false, 10).await, ["k4:1", "k3:1"]);

		let stats = store.delivery_stats(&hook, &all, 60).await.unwrap();
		let summary: Vec<_> = stats
			.iter()
			.map(|b| (b.start, b.total, b.failed, b.min_ms, b.avg_ms, b.max_ms))
			.collect();
		// `at(0)` is a multiple of 60 s, so it starts a bucket.
		assert_eq!(summary, [
			(at(0), 2, 1, 20.0, 60.0, 100.0),
			(at(60), 1, 0, 30.0, 30.0, 30.0),
			(at(120), 2, 1, 40.0, 45.0, 50.0),
		]);
		let windowed = store.delivery_stats(&hook, &window, 60).await.unwrap();
		assert_eq!(windowed.iter().map(|b| b.total).sum::<i64>(), 2);
		db.finish().await;
	}
}
