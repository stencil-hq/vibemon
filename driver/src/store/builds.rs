//! `builds` and `build_logs` (ARCHITECTURE.md §7).
//!
//! Owned by the template workstream; written by the build API and the build
//! supervisor ([`crate::builds`]), read by the template API.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, MySql, QueryBuilder, Transaction, types::Json};
use vmon_node::api::{ArtifactInfo, BuildReason};

use super::{Cursor, Store, from_micros, retry_deadlocks, templates::upsert_tags};

/// Build lifecycle as stored (and as E2B reports it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildStatus {
	Waiting,
	Building,
	Ready,
	Error,
}

impl BuildStatus {
	/// Column / wire value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Waiting => "waiting",
			Self::Building => "building",
			Self::Ready => "ready",
			Self::Error => "error",
		}
	}

	/// Parses a column value; unknown values read as `error`.
	pub fn parse(raw: &str) -> Self {
		match raw {
			"waiting" => Self::Waiting,
			"building" => Self::Building,
			"ready" => Self::Ready,
			_ => Self::Error,
		}
	}

	/// `ready` or `error`.
	pub const fn is_terminal(self) -> bool {
		matches!(self, Self::Ready | Self::Error)
	}
}

/// Build log level; ordered from least to most severe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
	Debug,
	Info,
	Warn,
	Error,
}

impl LogLevel {
	/// Column / wire value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Debug => "debug",
			Self::Info => "info",
			Self::Warn => "warn",
			Self::Error => "error",
		}
	}

	/// Strict parse of an E2B `LogLevel` query value.
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"debug" => Some(Self::Debug),
			"info" => Some(Self::Info),
			"warn" => Some(Self::Warn),
			"error" => Some(Self::Error),
			_ => None,
		}
	}

	/// Lenient mapping of node-reported levels onto the E2B set.
	pub fn normalize(raw: &str) -> Self {
		match raw.to_ascii_lowercase().as_str() {
			"debug" | "trace" => Self::Debug,
			"warn" | "warning" => Self::Warn,
			"error" | "fatal" | "panic" => Self::Error,
			_ => Self::Info,
		}
	}

	/// 1-based `MySQL` ENUM index (`level + 0`).
	const fn enum_index(self) -> u8 {
		match self {
			Self::Debug => 1,
			Self::Info => 2,
			Self::Warn => 3,
			Self::Error => 4,
		}
	}
}

/// One `builds` row.
#[derive(Clone, Debug, FromRow)]
pub struct BuildRow {
	pub id:               String,
	pub template_id:      String,
	pub team_id:          String,
	/// See [`BuildStatus`].
	pub status:           String,
	/// `build` | `snapshot` | `pause`.
	pub kind:             String,
	pub vcpus:            u32,
	pub memory_mb:        u64,
	pub disk_mb:          u64,
	pub min_free_disk_mb: Option<u64>,
	pub envd_version:     Option<String>,
	pub arch:             Option<String>,
	pub node_id:          Option<String>,
	pub tags:             Json<Vec<String>>,
	pub spec:             Option<Json<serde_json::Value>>,
	pub template_version: Option<String>,
	pub start_cmd:        Option<String>,
	pub ready_cmd:        Option<String>,
	pub reason:           Option<Json<BuildReason>>,
	pub artifact:         Option<Json<ArtifactInfo>>,
	pub created_at:       DateTime<Utc>,
	pub updated_at:       DateTime<Utc>,
	pub started_at:       Option<DateTime<Utc>>,
	pub finished_at:      Option<DateTime<Utc>>,
}

impl BuildRow {
	/// Parsed [`BuildStatus`].
	pub fn status(&self) -> BuildStatus {
		BuildStatus::parse(&self.status)
	}

	/// Rows matching `condition` (one `?` bound to `arg`) inside a transaction.
	pub(super) async fn select_where(
		tx: &mut Transaction<'_, MySql>,
		condition: &str,
		arg: &str,
	) -> sqlx::Result<Vec<Self>> {
		let sql = format!("SELECT {BUILD_COLUMNS} FROM builds WHERE {condition}");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(arg)
			.fetch_all(&mut **tx)
			.await
	}
}

/// One `build_logs` row.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct LogRow {
	pub seq:     u64,
	pub ts:      DateTime<Utc>,
	/// See [`LogLevel`].
	pub level:   String,
	pub step:    Option<String>,
	pub message: String,
}

/// A log line to persist (from a node's `BuildStatus`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLog {
	pub ts:      DateTime<Utc>,
	pub level:   LogLevel,
	pub step:    Option<String>,
	pub message: String,
}

/// Fields recorded when a build is handed to a builder node.
#[derive(Clone, Debug)]
pub struct BuildStart<'a> {
	pub node_id:          &'a str,
	pub arch:             &'a str,
	pub spec:             &'a serde_json::Value,
	pub template_version: &'a str,
	pub start_cmd:        Option<&'a str>,
	pub ready_cmd:        Option<&'a str>,
}

/// Direction of [`Store::build_logs_by_time`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogDirection {
	/// `ts >= cursor`, oldest first.
	Forward,
	/// `ts <= cursor`, newest first.
	Backward,
}

/// Reason of builds replaced by a newer `POST /v3/templates` (upstream text).
pub fn superseded_reason() -> BuildReason {
	BuildReason {
		message: "The build was canceled because it was superseded by a newer one.".into(),
		step:    None,
	}
}

pub(super) const BUILD_COLUMNS: &str =
	"id, template_id, team_id, status, kind, vcpus, memory_mb, disk_mb, min_free_disk_mb, \
	 envd_version, arch, node_id, tags, spec, template_version, start_cmd, ready_cmd, reason, \
	 artifact, created_at, updated_at, started_at, finished_at";

impl Store {
	/// Build by id.
	pub async fn build(&self, id: &str) -> sqlx::Result<Option<BuildRow>> {
		let sql = format!("SELECT {BUILD_COLUMNS} FROM builds WHERE id = ?");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// Builds of a template, newest first, keyset-paginated.
	pub async fn template_builds(
		&self,
		template_id: &str,
		after: Option<&Cursor>,
		limit: Option<u32>,
	) -> sqlx::Result<Vec<BuildRow>> {
		let mut query = QueryBuilder::<MySql>::new(format!(
			"SELECT {BUILD_COLUMNS} FROM builds WHERE template_id = "
		));
		query.push_bind(template_id);
		if let Some(cursor) = after {
			let at = from_micros(cursor.created_at_us);
			query
				.push(" AND (created_at < ")
				.push_bind(at)
				.push(" OR (created_at = ")
				.push_bind(at)
				.push(" AND id < ")
				.push_bind(cursor.id.clone())
				.push("))");
		}
		query.push(" ORDER BY created_at DESC, id DESC");
		if let Some(limit) = limit {
			query.push(" LIMIT ").push_bind(limit);
		}
		query.build_query_as().fetch_all(self.pool()).await
	}

	/// Latest build of each template (by `created_at`), any status.
	pub async fn latest_builds(&self, template_ids: &[String]) -> sqlx::Result<Vec<BuildRow>> {
		self.latest_builds_where(template_ids, "").await
	}

	/// Latest `ready` build of each template.
	pub async fn latest_ready_builds(&self, template_ids: &[String]) -> sqlx::Result<Vec<BuildRow>> {
		self
			.latest_builds_where(template_ids, " AND status = 'ready'")
			.await
	}

	async fn latest_builds_where(
		&self,
		template_ids: &[String],
		extra: &str,
	) -> sqlx::Result<Vec<BuildRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let mut query = QueryBuilder::<MySql>::new(format!(
			"SELECT {BUILD_COLUMNS} FROM (SELECT b.*, ROW_NUMBER() OVER (PARTITION BY template_id \
			 ORDER BY created_at DESC, id DESC) AS rn FROM builds b WHERE template_id IN ("
		));
		let mut list = query.separated(", ");
		for id in template_ids {
			list.push_bind(id);
		}
		query.push(format!("){extra}) ranked WHERE rn = 1"));
		query.build_query_as().fetch_all(self.pool()).await
	}

	/// Build ids of `kind = 'build'` in `building` (supervisor work list).
	pub async fn building_build_ids(&self) -> sqlx::Result<Vec<String>> {
		sqlx::query_scalar(
			"SELECT id FROM builds WHERE status = 'building' AND kind = 'build' ORDER BY created_at",
		)
		.fetch_all(self.pool())
		.await
	}

	/// Unfinished builds of a team (admin cancel).
	pub async fn team_unfinished_builds(&self, team_id: &str) -> sqlx::Result<Vec<BuildRow>> {
		let sql = format!(
			"SELECT {BUILD_COLUMNS} FROM builds WHERE team_id = ? AND status IN ('waiting', \
			 'building') ORDER BY created_at"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.fetch_all(self.pool())
			.await
	}

	/// Unfinished `kind = 'build'` builds of a template that request at least
	/// one of `tags` (cancelled when another build of those tags starts, like
	/// upstream's `GetConcurrentTemplateBuilds`).
	pub async fn template_building_builds(
		&self,
		template_id: &str,
		tags: &[String],
	) -> sqlx::Result<Vec<BuildRow>> {
		let sql = format!(
			"SELECT {BUILD_COLUMNS} FROM builds WHERE template_id = ? AND kind = 'build' AND status \
			 = 'building' AND JSON_OVERLAPS(tags, CAST(? AS JSON))"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(template_id)
			.bind(Json(tags))
			.fetch_all(self.pool())
			.await
	}

	/// Moves a `waiting` build to `building` on a node; `false` when the
	/// build was no longer waiting.
	pub async fn mark_build_started(&self, id: &str, start: &BuildStart<'_>) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let Some((template_id, _)) = lock_build_template(&mut tx, id).await? else {
				return Ok(false);
			};
			let result = sqlx::query(
				"UPDATE builds SET status = 'building', node_id = ?, arch = ?, spec = ?, \
				 template_version = ?, start_cmd = ?, ready_cmd = ?, started_at = NULL, updated_at = \
				 UTC_TIMESTAMP(3) WHERE id = ? AND status = 'waiting'",
			)
			.bind(start.node_id)
			.bind(start.arch)
			.bind(Json(start.spec))
			.bind(start.template_version)
			.bind(start.start_cmd)
			.bind(start.ready_cmd)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			if result.rows_affected() == 0 {
				return Ok(false);
			}
			sqlx::query(
				"UPDATE templates SET build_count = build_count + 1, updated_at = UTC_TIMESTAMP(3) \
				 WHERE id = ?",
			)
			.bind(&template_id)
			.execute(&mut *tx)
			.await?;
			tx.commit().await?;
			Ok(true)
		})
		.await
	}

	/// Records that the builder node started running a dispatched build (it
	/// left the node's queue); `false` when already recorded or no longer
	/// building.
	pub async fn mark_build_running(&self, id: &str) -> sqlx::Result<bool> {
		let result = retry_deadlocks(|| async move {
			sqlx::query(
				"UPDATE builds SET started_at = UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3) WHERE \
				 id = ? AND status = 'building' AND started_at IS NULL",
			)
			.bind(id)
			.execute(self.pool())
			.await
		})
		.await?;
		Ok(result.rows_affected() > 0)
	}

	/// Marks an unfinished build `ready` with its artifact, records the
	/// build facts, and points its requested tags at it. `false` when the
	/// build had already finished (e.g. cancelled meanwhile).
	pub async fn finish_build_ready(&self, id: &str, artifact: &ArtifactInfo) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let Some((template_id, live)) = lock_build_template(&mut tx, id).await? else {
				return Ok(false);
			};
			let result = sqlx::query(
				"UPDATE builds SET status = 'ready', artifact = ?, envd_version = ?, arch = ?, \
				 node_id = ?, vcpus = ?, memory_mb = ?, disk_mb = ?, reason = NULL, finished_at = \
				 UTC_TIMESTAMP(3), updated_at = UTC_TIMESTAMP(3) WHERE id = ? AND status IN \
				 ('waiting', 'building')",
			)
			.bind(Json(artifact))
			.bind(&artifact.envd_version)
			.bind(&artifact.arch)
			.bind(artifact.node_id.to_string())
			.bind(artifact.vcpus)
			.bind(artifact.memory_mb)
			.bind(artifact.disk_mb)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			if result.rows_affected() == 0 {
				return Ok(false);
			}
			if live {
				let Json(tags): Json<Vec<String>> =
					sqlx::query_scalar("SELECT tags FROM builds WHERE id = ?")
						.bind(id)
						.fetch_one(&mut *tx)
						.await?;
				upsert_tags(&mut tx, &template_id, id, &tags).await?;
				sqlx::query("UPDATE templates SET updated_at = UTC_TIMESTAMP(3) WHERE id = ?")
					.bind(&template_id)
					.execute(&mut *tx)
					.await?;
			}
			tx.commit().await?;
			Ok(true)
		})
		.await
	}

	/// Marks an unfinished build `error`; `false` when it had already finished.
	pub async fn fail_build(&self, id: &str, reason: &BuildReason) -> sqlx::Result<bool> {
		let result = retry_deadlocks(|| async move {
			sqlx::query(
				"UPDATE builds SET status = 'error', reason = ?, finished_at = UTC_TIMESTAMP(3), \
				 updated_at = UTC_TIMESTAMP(3) WHERE id = ? AND status IN ('waiting', 'building')",
			)
			.bind(Json(reason))
			.bind(id)
			.execute(self.pool())
			.await
		})
		.await?;
		Ok(result.rows_affected() == 1)
	}

	/// Number of persisted log lines (= next `seq`).
	pub async fn build_log_count(&self, build_id: &str) -> sqlx::Result<u64> {
		let next: i64 = sqlx::query_scalar(
			"SELECT CAST(COALESCE(MAX(seq) + 1, 0) AS SIGNED) FROM build_logs WHERE build_id = ?",
		)
		.bind(build_id)
		.fetch_one(self.pool())
		.await?;
		Ok(u64::try_from(next).unwrap_or(0))
	}

	/// Appends log lines with consecutive `seq` from `first_seq`; already
	/// stored sequence numbers are left untouched (idempotent re-delivery).
	pub async fn append_build_logs(
		&self,
		build_id: &str,
		first_seq: u64,
		logs: &[NewLog],
	) -> sqlx::Result<()> {
		const CHUNK: usize = 500;
		let mut base = first_seq;
		if logs.is_empty() {
			return Ok(());
		}
		for chunk in logs.chunks(CHUNK) {
			let first = base;
			retry_deadlocks(|| async move {
				let mut query = QueryBuilder::<MySql>::new(
					"INSERT IGNORE INTO build_logs (build_id, seq, ts, level, step, message) ",
				);
				query.push_values(chunk.iter().enumerate(), |mut row, (offset, log)| {
					row.push_bind(build_id)
						.push_bind(first + offset as u64)
						.push_bind(log.ts)
						.push_bind(log.level.as_str())
						.push_bind(log.step.clone())
						.push_bind(log.message.clone());
				});
				query.build().execute(self.pool()).await
			})
			.await?;
			base += chunk.len() as u64;
		}
		Ok(())
	}

	/// `logsOffset`/`limit` page over the lines at or above `min_level`
	/// (offset counts only matching lines), oldest first.
	pub async fn build_logs_page(
		&self,
		build_id: &str,
		min_level: LogLevel,
		offset: u64,
		limit: u32,
	) -> sqlx::Result<Vec<LogRow>> {
		sqlx::query_as(
			"SELECT seq, ts, level, step, message FROM build_logs WHERE build_id = ? AND level + 0 \
			 >= ? ORDER BY seq LIMIT ? OFFSET ?",
		)
		.bind(build_id)
		.bind(min_level.enum_index())
		.bind(limit)
		.bind(offset)
		.fetch_all(self.pool())
		.await
	}

	/// Time-cursor page (`GET …/builds/{id}/logs`).
	pub async fn build_logs_by_time(
		&self,
		build_id: &str,
		cursor: DateTime<Utc>,
		direction: LogDirection,
		min_level: LogLevel,
		limit: u32,
	) -> sqlx::Result<Vec<LogRow>> {
		let sql = match direction {
			LogDirection::Forward => {
				"SELECT seq, ts, level, step, message FROM build_logs WHERE build_id = ? AND level + 0 \
				 >= ? AND ts >= ? ORDER BY ts, seq LIMIT ?"
			},
			LogDirection::Backward => {
				"SELECT seq, ts, level, step, message FROM build_logs WHERE build_id = ? AND level + 0 \
				 >= ? AND ts <= ? ORDER BY ts DESC, seq DESC LIMIT ?"
			},
		};
		sqlx::query_as(sql)
			.bind(build_id)
			.bind(min_level.enum_index())
			.bind(cursor)
			.bind(limit)
			.fetch_all(self.pool())
			.await
	}

	/// `snapshot` builds stuck in `building` since before `before` (their
	/// driver died mid-snapshot).
	pub async fn stale_snapshot_builds(&self, before: DateTime<Utc>) -> sqlx::Result<Vec<String>> {
		sqlx::query_scalar(
			"SELECT id FROM builds WHERE kind = 'snapshot' AND status = 'building' AND updated_at < ?",
		)
		.bind(before)
		.fetch_all(self.pool())
		.await
	}

	/// Finished builds of deleted templates whose artifacts are not yet
	/// collected and that no running or paused sandbox row uses (so builds
	/// that stay in use cannot starve the batch).
	pub async fn builds_pending_gc(&self, limit: u32) -> sqlx::Result<Vec<BuildRow>> {
		let sql = format!(
			"SELECT {BUILD_COLUMNS} FROM builds WHERE artifacts_gc_at IS NULL AND status IN \
			 ('ready', 'error') AND template_id IN (SELECT id FROM templates WHERE deleted_at IS NOT \
			 NULL) AND id NOT IN (SELECT build_id FROM sandboxes WHERE state IN ('running', \
			 'paused')) ORDER BY updated_at LIMIT ?"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(limit)
			.fetch_all(self.pool())
			.await
	}

	/// Records that a build's artifacts were deleted from every holder.
	pub async fn mark_artifacts_collected(&self, id: &str) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query("UPDATE builds SET artifacts_gc_at = UTC_TIMESTAMP(3) WHERE id = ?")
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(())
	}

	/// Build ids referenced by paused sandboxes (artifacts must stay).
	pub async fn builds_of_paused_sandboxes(&self) -> sqlx::Result<Vec<String>> {
		sqlx::query_scalar("SELECT DISTINCT build_id FROM sandboxes WHERE state = 'paused'")
			.fetch_all(self.pool())
			.await
	}
}

/// Locks the template row of build `id` and returns its id and whether it is
/// live (not deleted); `None` for an unknown build.
///
/// A transaction that changes a build and its template locks the template
/// first, as template deletes and build registrations do. Locking the build
/// first closed deadlock cycles with them under concurrent builds.
async fn lock_build_template(
	tx: &mut Transaction<'_, MySql>,
	id: &str,
) -> sqlx::Result<Option<(String, bool)>> {
	// A build never changes templates, so this plain read needs no lock.
	let Some(template_id) =
		sqlx::query_scalar::<_, String>("SELECT template_id FROM builds WHERE id = ?")
			.bind(id)
			.fetch_optional(&mut **tx)
			.await?
	else {
		return Ok(None);
	};
	let deleted_at: Option<Option<DateTime<Utc>>> =
		sqlx::query_scalar("SELECT deleted_at FROM templates WHERE id = ? FOR UPDATE")
			.bind(&template_id)
			.fetch_optional(&mut **tx)
			.await?;
	Ok(deleted_at.map(|deleted_at| (template_id, deleted_at.is_none())))
}
