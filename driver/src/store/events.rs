//! `sandbox_events`: the sandbox lifecycle event log (events API, webhooks,
//! team metrics). Written by the sandbox lifecycle via
//! [`Store::record_event`].

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Value, json};
use sqlx::{FromRow, MySql, QueryBuilder, types::Json};

use super::Store;

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
/// Retention advertised in webhook payloads.
pub const EVENTS_TTL_DAYS: u32 = 30;

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

/// A stored event.
#[derive(Clone, Debug, FromRow)]
pub struct SandboxEventRow {
	/// Monotonic insertion sequence (webhook cursor).
	pub seq:          u64,
	pub id:           String,
	pub team_id:      String,
	pub sandbox_id:   String,
	pub execution_id: String,
	pub template_id:  String,
	pub build_id:     String,
	#[sqlx(rename = "type")]
	pub event_type:   String,
	pub ts:           DateTime<Utc>,
	pub data:         Option<Json<Value>>,
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
			"eventData": self.data.as_ref().map(|d| &d.0),
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
			"event_data": self.data.as_ref().map(|d| &d.0),
			"sandbox_id": self.sandbox_id,
			"sandbox_execution_id": self.execution_id,
			"sandbox_template_id": self.template_id,
			"sandbox_build_id": self.build_id,
			"sandbox_team_id": self.team_id,
			"events_ttl_days": EVENTS_TTL_DAYS,
		})
	}
}

const EVENT_COLUMNS: &str =
	"seq, id, team_id, sandbox_id, execution_id, template_id, build_id, type, ts, data";

/// Inserts one event on `executor` (the pool, or a transaction so a sandbox
/// state change and its event commit together) and returns the event id.
pub(super) async fn insert_event<'e, E>(
	executor: E,
	event: &NewSandboxEvent<'_>,
) -> sqlx::Result<String>
where
	E: sqlx::Executor<'e, Database = MySql>,
{
	let id = uuid::Uuid::new_v4().to_string();
	sqlx::query(
		"INSERT INTO sandbox_events (id, team_id, sandbox_id, execution_id, template_id, build_id, \
		 type, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
	)
	.bind(&id)
	.bind(event.team_id)
	.bind(event.sandbox_id)
	.bind(event.execution_id)
	.bind(event.template_id)
	.bind(event.build_id)
	.bind(event.event_type)
	.bind(Json(&event.data))
	.execute(executor)
	.await?;
	Ok(id)
}

fn push_types(builder: &mut QueryBuilder<MySql>, types: &[String]) {
	if types.is_empty() {
		return;
	}
	builder.push(" AND type IN (");
	let mut separated = builder.separated(", ");
	for ty in types {
		separated.push_bind(ty.clone());
	}
	separated.push_unseparated(")");
}

impl Store {
	/// Appends an event and returns its id. Webhook delivery picks it up on
	/// its next poll.
	pub async fn record_event(&self, event: &NewSandboxEvent<'_>) -> sqlx::Result<String> {
		insert_event(self.pool(), event).await
	}

	/// Events API page (offset pagination, newest first unless `order_asc`).
	pub async fn list_events(
		&self,
		team_id: &str,
		query: &EventQuery,
	) -> sqlx::Result<Vec<SandboxEventRow>> {
		let mut builder = QueryBuilder::<MySql>::new(format!(
			"SELECT {EVENT_COLUMNS} FROM sandbox_events WHERE team_id = "
		));
		builder.push_bind(team_id.to_owned());
		if let Some(sandbox_id) = &query.sandbox_id {
			builder
				.push(" AND sandbox_id = ")
				.push_bind(sandbox_id.clone());
		}
		push_types(&mut builder, &query.types);
		builder.push(if query.order_asc {
			" ORDER BY ts ASC, seq ASC"
		} else {
			" ORDER BY ts DESC, seq DESC"
		});
		builder
			.push(" LIMIT ")
			.push_bind(query.limit)
			.push(" OFFSET ")
			.push_bind(query.offset);
		builder.build_query_as().fetch_all(self.pool()).await
	}

	/// Events of a team with `seq > after_seq` (oldest first) restricted to
	/// `types`; used by webhook delivery.
	pub async fn events_after(
		&self,
		team_id: &str,
		after_seq: u64,
		types: &[String],
		limit: u32,
	) -> sqlx::Result<Vec<SandboxEventRow>> {
		let mut builder = QueryBuilder::<MySql>::new(format!(
			"SELECT {EVENT_COLUMNS} FROM sandbox_events WHERE team_id = "
		));
		builder
			.push_bind(team_id.to_owned())
			.push(" AND seq > ")
			.push_bind(after_seq);
		push_types(&mut builder, types);
		builder.push(" ORDER BY seq ASC LIMIT ").push_bind(limit);
		builder.build_query_as().fetch_all(self.pool()).await
	}

	/// Highest event sequence number so far (0 when empty).
	pub async fn max_event_seq(&self) -> sqlx::Result<u64> {
		let max: Option<u64> = sqlx::query_scalar("SELECT MAX(seq) FROM sandbox_events")
			.fetch_one(self.pool())
			.await?;
		Ok(max.unwrap_or(0))
	}

	/// `(ts, type)` of a team's start/stop events in `[start, end]`, oldest
	/// first, plus the net running count before `start` (team metrics).
	pub async fn lifecycle_timeline(
		&self,
		team_id: &str,
		start: DateTime<Utc>,
		end: DateTime<Utc>,
	) -> sqlx::Result<(i64, Vec<(DateTime<Utc>, String)>)> {
		let counts: Vec<(String, i64)> = sqlx::query_as(
			"SELECT type, COUNT(*) FROM sandbox_events WHERE team_id = ? AND ts < ? AND type IN (?, \
			 ?, ?, ?) GROUP BY type",
		)
		.bind(team_id)
		.bind(start)
		.bind(event_types::CREATED)
		.bind(event_types::RESUMED)
		.bind(event_types::PAUSED)
		.bind(event_types::KILLED)
		.fetch_all(self.pool())
		.await?;
		let baseline = counts
			.iter()
			.map(|(ty, n)| if is_start(ty) { *n } else { -n })
			.sum::<i64>()
			.max(0);
		let events = sqlx::query_as(
			"SELECT ts, type FROM sandbox_events WHERE team_id = ? AND ts >= ? AND ts <= ? AND type \
			 IN (?, ?, ?, ?) ORDER BY ts, seq",
		)
		.bind(team_id)
		.bind(start)
		.bind(end)
		.bind(event_types::CREATED)
		.bind(event_types::RESUMED)
		.bind(event_types::PAUSED)
		.bind(event_types::KILLED)
		.fetch_all(self.pool())
		.await?;
		Ok((baseline, events))
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
