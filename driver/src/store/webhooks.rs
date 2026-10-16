//! `webhooks` + `webhook_deliveries`.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, MySql, QueryBuilder, types::Json};

use super::{Cursor, Store, from_micros, retry_deadlocks};

/// A registered webhook.
#[derive(Clone, Debug, FromRow)]
pub struct WebhookRow {
	pub id:                   String,
	pub team_id:              String,
	pub name:                 String,
	pub url:                  String,
	pub events:               Json<Vec<String>>,
	pub enabled:              bool,
	/// `Keys::encrypt` of the signature secret.
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
#[derive(Clone, Debug, FromRow)]
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

const WEBHOOK_COLUMNS: &str = "id, team_id, name, url, events, enabled, signature_secret_enc, \
                               last_event_seq, created_at, updated_at";
const DELIVERY_COLUMNS: &str = "id, team_id, webhook_id, event_id, sandbox_id, event_type, \
                                status, duration_ms, request_url, request_headers, request_body, \
                                response_status, response_headers, response_body, error_class, \
                                error_message, ts";

fn push_filter(builder: &mut QueryBuilder<MySql>, filter: &DeliveryFilter) {
	if let Some(start) = filter.start {
		builder.push(" AND ts >= ").push_bind(start);
	}
	if let Some(end) = filter.end {
		builder.push(" AND ts <= ").push_bind(end);
	}
	for (column, values) in [("status", &filter.statuses), ("event_type", &filter.event_types)] {
		if values.is_empty() {
			continue;
		}
		builder.push(format!(" AND {column} IN ("));
		let mut separated = builder.separated(", ");
		for value in values {
			separated.push_bind(value.clone());
		}
		separated.push_unseparated(")");
	}
}

impl Store {
	/// Registers a webhook. Its cursor starts at the current newest event,
	/// so only events after registration are delivered.
	pub async fn create_webhook(
		&self,
		team_id: &str,
		name: &str,
		url: &str,
		events: &[String],
		enabled: bool,
		signature_secret_enc: &[u8],
	) -> sqlx::Result<WebhookRow> {
		let id = uuid::Uuid::new_v4().to_string();
		let id = id.as_str();
		let cursor = self.max_event_seq().await?;
		retry_deadlocks(|| async move {
			sqlx::query(
				"INSERT INTO webhooks (id, team_id, name, url, events, enabled, signature_secret_enc, \
				 last_event_seq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.bind(id)
			.bind(team_id)
			.bind(name)
			.bind(url)
			.bind(Json(events))
			.bind(enabled)
			.bind(signature_secret_enc)
			.bind(cursor)
			.execute(self.pool())
			.await
		})
		.await?;
		self
			.webhook(team_id, id)
			.await?
			.ok_or(sqlx::Error::RowNotFound)
	}

	/// Webhook by id within a team.
	pub async fn webhook(&self, team_id: &str, id: &str) -> sqlx::Result<Option<WebhookRow>> {
		let sql = format!("SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE team_id = ? AND id = ?");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// A team's webhooks, oldest first.
	pub async fn list_webhooks(&self, team_id: &str) -> sqlx::Result<Vec<WebhookRow>> {
		let sql = format!(
			"SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE team_id = ? ORDER BY created_at, id"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.fetch_all(self.pool())
			.await
	}

	/// Every enabled webhook (delivery loop).
	pub async fn enabled_webhooks(&self) -> sqlx::Result<Vec<WebhookRow>> {
		let sql = format!(
			"SELECT {WEBHOOK_COLUMNS} FROM webhooks WHERE enabled = TRUE ORDER BY created_at, id"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.fetch_all(self.pool())
			.await
	}

	/// Applies the `Some` fields; `None` when the webhook does not exist.
	/// Re-enabling a disabled webhook skips events that happened while it
	/// was off.
	pub async fn update_webhook(
		&self,
		team_id: &str,
		id: &str,
		patch: &WebhookWrite,
	) -> sqlx::Result<Option<WebhookRow>> {
		let Some(current) = self.webhook(team_id, id).await? else {
			return Ok(None);
		};
		let cursor = match patch.enabled {
			Some(true) if !current.enabled => Some(self.max_event_seq().await?),
			_ => None,
		};
		retry_deadlocks(|| async move {
			let mut builder =
				QueryBuilder::<MySql>::new("UPDATE webhooks SET updated_at = CURRENT_TIMESTAMP(3)");
			if let Some(name) = &patch.name {
				builder.push(", name = ").push_bind(name.clone());
			}
			if let Some(url) = &patch.url {
				builder.push(", url = ").push_bind(url.clone());
			}
			if let Some(events) = &patch.events {
				builder.push(", events = ").push_bind(Json(events.clone()));
			}
			if let Some(enabled) = patch.enabled {
				builder.push(", enabled = ").push_bind(enabled);
			}
			if let Some(cursor) = cursor {
				builder
					.push(", last_event_seq = GREATEST(last_event_seq, ")
					.push_bind(cursor)
					.push(")");
			}
			if let Some(secret) = &patch.signature_secret_enc {
				builder
					.push(", signature_secret_enc = ")
					.push_bind(secret.clone());
			}
			builder
				.push(" WHERE team_id = ")
				.push_bind(team_id.to_owned())
				.push(" AND id = ")
				.push_bind(id.to_owned());
			builder.build().execute(self.pool()).await
		})
		.await?;
		self.webhook(team_id, id).await
	}

	/// Deletes a webhook and its deliveries; `false` when unknown.
	pub async fn delete_webhook(&self, team_id: &str, id: &str) -> sqlx::Result<bool> {
		let done = retry_deadlocks(|| async move {
			sqlx::query("DELETE FROM webhooks WHERE team_id = ? AND id = ?")
				.bind(team_id)
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(done.rows_affected() > 0)
	}

	/// Moves a webhook's delivery cursor forward (never backwards).
	pub async fn advance_webhook_cursor(&self, id: &str, seq: u64) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query(
				"UPDATE webhooks SET last_event_seq = GREATEST(last_event_seq, ?) WHERE id = ?",
			)
			.bind(seq)
			.bind(id)
			.execute(self.pool())
			.await
		})
		.await
		.map(drop)
	}

	/// Records one delivery attempt.
	pub async fn insert_delivery(&self, d: &NewDelivery) -> sqlx::Result<String> {
		let id = uuid::Uuid::new_v4().to_string();
		let row_id = id.as_str();
		retry_deadlocks(|| async move {
			sqlx::query(
				"INSERT INTO webhook_deliveries (id, team_id, webhook_id, event_id, sandbox_id, \
				 event_type, status, duration_ms, request_url, request_headers, request_body, \
				 response_status, response_headers, response_body, error_class, error_message, ts) \
				 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.bind(row_id)
			.bind(&d.team_id)
			.bind(&d.webhook_id)
			.bind(&d.event_id)
			.bind(&d.sandbox_id)
			.bind(&d.event_type)
			.bind(d.status)
			.bind(d.duration_ms)
			.bind(&d.request_url)
			.bind(&d.request_headers)
			.bind(&d.request_body)
			.bind(d.response_status)
			.bind(&d.response_headers)
			.bind(&d.response_body)
			.bind(d.error_class)
			.bind(&d.error_message)
			.bind(d.ts)
			.execute(self.pool())
			.await
		})
		.await?;
		Ok(id)
	}

	/// One page of delivery attempts grouped by event. Groups are ordered by
	/// their first attempt; `after` is the cursor of the last group of the
	/// previous page. Returns up to `limit` groups.
	pub async fn delivery_groups(
		&self,
		webhook_id: &str,
		filter: &DeliveryFilter,
		after: Option<&Cursor>,
		limit: u32,
		order_asc: bool,
	) -> sqlx::Result<Vec<(Cursor, Vec<DeliveryRow>)>> {
		let mut builder = QueryBuilder::<MySql>::new(
			"SELECT event_id, MIN(ts) AS first_ts FROM webhook_deliveries WHERE webhook_id = ",
		);
		builder.push_bind(webhook_id.to_owned());
		push_filter(&mut builder, filter);
		builder.push(" GROUP BY event_id");
		if let Some(cursor) = after {
			let at = from_micros(cursor.created_at_us);
			let op = if order_asc { ">" } else { "<" };
			builder
				.push(format!(" HAVING (first_ts {op} "))
				.push_bind(at)
				.push(" OR (first_ts = ")
				.push_bind(at)
				.push(format!(" AND event_id {op} "))
				.push_bind(cursor.id.clone())
				.push("))");
		}
		builder.push(if order_asc {
			" ORDER BY first_ts ASC, event_id ASC"
		} else {
			" ORDER BY first_ts DESC, event_id DESC"
		});
		builder.push(" LIMIT ").push_bind(limit);
		let groups: Vec<(String, DateTime<Utc>)> =
			builder.build_query_as().fetch_all(self.pool()).await?;
		if groups.is_empty() {
			return Ok(Vec::new());
		}
		let mut attempts = QueryBuilder::<MySql>::new(format!(
			"SELECT {DELIVERY_COLUMNS} FROM webhook_deliveries WHERE webhook_id = "
		));
		attempts.push_bind(webhook_id.to_owned());
		push_filter(&mut attempts, filter);
		attempts.push(" AND event_id IN (");
		let mut separated = attempts.separated(", ");
		for (event_id, _) in &groups {
			separated.push_bind(event_id.clone());
		}
		separated.push_unseparated(") ORDER BY ts ASC, id ASC");
		let rows: Vec<DeliveryRow> = attempts.build_query_as().fetch_all(self.pool()).await?;
		Ok(groups
			.into_iter()
			.map(|(event_id, first_ts)| {
				let attempts = rows
					.iter()
					.filter(|r| r.event_id == event_id)
					.cloned()
					.collect();
				(Cursor { created_at_us: super::micros(first_ts), id: event_id }, attempts)
			})
			.collect())
	}

	/// Aggregates attempts in `filter`'s window into buckets of
	/// `bucket_secs`, oldest first.
	pub async fn delivery_stats(
		&self,
		webhook_id: &str,
		filter: &DeliveryFilter,
		bucket_secs: i64,
	) -> sqlx::Result<Vec<StatsBucket>> {
		let mut builder = QueryBuilder::<MySql>::new("SELECT CAST(FLOOR(UNIX_TIMESTAMP(ts) / ");
		builder.push_bind(bucket_secs);
		builder.push(
			") AS SIGNED) AS bucket, COUNT(*) AS total, CAST(SUM(status = 'failed') AS SIGNED) AS \
			 failed, CAST(MIN(duration_ms) AS DOUBLE) AS min_ms, CAST(AVG(duration_ms) AS DOUBLE) AS \
			 avg_ms, CAST(MAX(duration_ms) AS DOUBLE) AS max_ms FROM webhook_deliveries WHERE \
			 webhook_id = ",
		);
		builder.push_bind(webhook_id.to_owned());
		push_filter(&mut builder, filter);
		builder.push(" GROUP BY bucket ORDER BY bucket");
		let rows: Vec<(i64, i64, i64, f64, f64, f64)> =
			builder.build_query_as().fetch_all(self.pool()).await?;
		Ok(rows
			.into_iter()
			.map(|(bucket, total, failed, min_ms, avg_ms, max_ms)| StatsBucket {
				start: DateTime::from_timestamp(bucket * bucket_secs, 0).unwrap_or_default(),
				total,
				failed,
				min_ms,
				avg_ms,
				max_ms,
			})
			.collect())
	}
}
