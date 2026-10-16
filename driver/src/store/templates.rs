//! `templates`, `template_aliases`, and `template_tags` (ARCHITECTURE.md §7).
//!
//! Owned by the template workstream; called by [`crate::api::templates`] and
//! [`crate::builds`]. Multi-row operations that must be atomic (registering a
//! build, registering a snapshot) run in one transaction here.

use chrono::{DateTime, Utc};
use sqlx::{FromRow, MySql, QueryBuilder, Transaction, types::Json};

use super::{Cursor, Store, from_micros, retry_deadlocks};
use crate::store::builds::{BuildRow, superseded_reason};

/// What a template row represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateKind {
	/// Built from steps (`POST /v3/templates`).
	Template,
	/// Snapshot template (`POST /sandboxes/{id}/snapshots`).
	Snapshot,
	/// Hidden snapshot backing `POST /sandboxes/{id}/fork`.
	Fork,
}

impl TemplateKind {
	/// Column value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Template => "template",
			Self::Snapshot => "snapshot",
			Self::Fork => "fork",
		}
	}
}

/// One `templates` row.
#[derive(Clone, Debug, FromRow)]
pub struct TemplateRow {
	pub id:                String,
	pub team_id:           String,
	pub public:            bool,
	/// `template` | `snapshot` | `fork` (see [`TemplateKind`]).
	pub kind:              String,
	pub source_sandbox_id: Option<String>,
	pub created_at:        DateTime<Utc>,
	pub updated_at:        DateTime<Utc>,
	pub spawn_count:       i64,
	pub last_spawned_at:   Option<DateTime<Utc>>,
	pub build_count:       i32,
	pub deleted_at:        Option<DateTime<Utc>>,
}

impl TemplateRow {
	/// Whether the row is of `kind`.
	pub fn is(&self, kind: TemplateKind) -> bool {
		self.kind == kind.as_str()
	}
}

/// One `template_aliases` row; `namespace` is a team slug or `""` (bare alias).
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct AliasRow {
	pub namespace:   String,
	pub alias:       String,
	pub template_id: String,
}

impl AliasRow {
	/// E2B "name": `namespace/alias`, or the bare alias.
	pub fn name(&self) -> String {
		if self.namespace.is_empty() {
			self.alias.clone()
		} else {
			format!("{}/{}", self.namespace, self.alias)
		}
	}
}

/// One `template_tags` row.
#[derive(Clone, Debug, FromRow)]
pub struct TagRow {
	pub template_id: String,
	pub tag:         String,
	pub build_id:    String,
	pub created_at:  DateTime<Utc>,
}

/// Input of [`Store::register_build`] (`POST /v3/templates`).
#[derive(Clone, Debug)]
pub struct RegisterBuild<'a> {
	pub team_id:               &'a str,
	/// Team slug the alias lives under.
	pub namespace:             &'a str,
	pub alias:                 &'a str,
	/// Template id used when the alias does not resolve yet.
	pub new_template_id:       &'a str,
	pub build_id:              &'a str,
	/// Deduplicated, validated tags (never empty).
	pub tags:                  &'a [String],
	pub vcpus:                 u32,
	pub memory_mb:             u64,
	pub disk_mb:               u64,
	pub min_free_disk_mb:      Option<u64>,
	/// Team limit on builds in `waiting`/`building`.
	pub max_concurrent_builds: u32,
}

/// Result of [`Store::register_build`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
	/// The build was created (in `waiting`) for `template_id`.
	Registered { template_id: String, public: bool },
	/// The alias belongs to another team's or another kind of template.
	AliasTaken,
	/// The team already has `limit` builds in progress.
	ConcurrencyLimit { limit: u32 },
}

/// Input of [`Store::register_snapshot`].
#[derive(Clone, Debug)]
pub struct RegisterSnapshot<'a> {
	pub team_id:           &'a str,
	/// [`TemplateKind::Snapshot`] or [`TemplateKind::Fork`].
	pub kind:              TemplateKind,
	pub source_sandbox_id: &'a str,
	/// Node capturing the snapshot (`builds.node_id`, so artifact GC also
	/// reaches artifacts of a capture that failed midway).
	pub node_id:           &'a str,
	/// `(namespace, alias)` for named snapshots.
	pub name:              Option<(&'a str, &'a str)>,
	pub tag:               &'a str,
	pub new_template_id:   &'a str,
	pub build_id:          &'a str,
	pub vcpus:             u32,
	pub memory_mb:         u64,
	pub disk_mb:           u64,
	pub envd_version:      &'a str,
}

/// Result of [`Store::register_snapshot`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotOutcome {
	/// The snapshot build was created (in `building`) for `template_id`.
	Registered { template_id: String },
	/// The name is used by a template that is not this team's snapshot.
	AliasTaken,
}

const TEMPLATE_COLUMNS: &str = "t.id, t.team_id, t.public, t.kind, t.source_sandbox_id, \
                                t.created_at, t.updated_at, t.spawn_count, t.last_spawned_at, \
                                t.build_count, t.deleted_at";

impl Store {
	/// Live (not deleted) template by id.
	pub async fn template(&self, id: &str) -> sqlx::Result<Option<TemplateRow>> {
		let sql = format!(
			"SELECT {TEMPLATE_COLUMNS} FROM templates t WHERE t.id = ? AND t.deleted_at IS NULL"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// Live template an alias points to.
	pub async fn template_by_alias(
		&self,
		namespace: &str,
		alias: &str,
	) -> sqlx::Result<Option<TemplateRow>> {
		let sql = format!(
			"SELECT {TEMPLATE_COLUMNS} FROM template_aliases a JOIN templates t ON t.id = \
			 a.template_id WHERE a.namespace = ? AND a.alias = ? AND t.deleted_at IS NULL"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(namespace)
			.bind(alias)
			.fetch_optional(self.pool())
			.await
	}

	/// Aliases of the given templates (any order).
	pub async fn aliases_of(&self, template_ids: &[String]) -> sqlx::Result<Vec<AliasRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let mut query = QueryBuilder::<MySql>::new(
			"SELECT namespace, alias, template_id FROM template_aliases WHERE template_id IN (",
		);
		let mut list = query.separated(", ");
		for id in template_ids {
			list.push_bind(id);
		}
		query.push(") ORDER BY namespace DESC, alias");
		query.build_query_as().fetch_all(self.pool()).await
	}

	/// A team's live templates of `kind`, newest first, keyset-paginated.
	/// `source_sandbox_id`/`alias` filter snapshot listings.
	pub async fn list_templates(
		&self,
		team_id: &str,
		kind: TemplateKind,
		filter: &TemplateFilter<'_>,
		after: Option<&Cursor>,
		limit: Option<u32>,
	) -> sqlx::Result<Vec<TemplateRow>> {
		let mut query = QueryBuilder::<MySql>::new(format!(
			"SELECT {TEMPLATE_COLUMNS} FROM templates t WHERE t.team_id = "
		));
		query
			.push_bind(team_id)
			.push(" AND t.kind = ")
			.push_bind(kind.as_str())
			.push(" AND t.deleted_at IS NULL");
		if let Some(sandbox) = filter.source_sandbox_id {
			query.push(" AND t.source_sandbox_id = ").push_bind(sandbox);
		}
		if let Some(alias) = filter.alias {
			query
				.push(
					" AND EXISTS (SELECT 1 FROM template_aliases a WHERE a.template_id = t.id AND \
					 a.alias = ",
				)
				.push_bind(alias)
				.push(")");
		}
		if filter.ready_only {
			query.push(
				" AND EXISTS (SELECT 1 FROM builds b WHERE b.template_id = t.id AND b.status = \
				 'ready')",
			);
		}
		if let Some(cursor) = after {
			let at = from_micros(cursor.created_at_us);
			query
				.push(" AND (t.created_at < ")
				.push_bind(at)
				.push(" OR (t.created_at = ")
				.push_bind(at)
				.push(" AND t.id < ")
				.push_bind(cursor.id.clone())
				.push("))");
		}
		query.push(" ORDER BY t.created_at DESC, t.id DESC");
		if let Some(limit) = limit {
			query.push(" LIMIT ").push_bind(limit);
		}
		query.build_query_as().fetch_all(self.pool()).await
	}

	/// Sets `public`; `false` when the template does not exist. Bare aliases
	/// follow: publishing a system team's template claims the free bare
	/// aliases of its names, unpublishing releases the template's bare aliases.
	pub async fn set_template_public(&self, id: &str, public: bool) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let result = sqlx::query(
				"UPDATE templates SET public = ?, updated_at = UTC_TIMESTAMP(3) WHERE id = ? AND \
				 deleted_at IS NULL",
			)
			.bind(public)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			if public {
				claim_bare_aliases(&mut tx, "t.id = ?", id).await?;
			} else {
				sqlx::query("DELETE FROM template_aliases WHERE template_id = ? AND namespace = ''")
					.bind(id)
					.execute(&mut *tx)
					.await?;
			}
			tx.commit().await?;
			Ok(result.rows_affected() == 1)
		})
		.await
	}

	/// Soft-deletes a template: sets `deleted_at`, frees its aliases and tags,
	/// and fails its unfinished builds. Returns the builds that were
	/// `building` (the caller cancels them on their node); `None` when the
	/// template did not exist.
	///
	/// Locks the template row, then its builds: every transaction that
	/// changes a template and its builds locks them in this order.
	pub async fn soft_delete_template(&self, id: &str) -> sqlx::Result<Option<Vec<BuildRow>>> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let deleted = sqlx::query(
				"UPDATE templates SET deleted_at = UTC_TIMESTAMP(3) WHERE id = ? AND deleted_at IS \
				 NULL",
			)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			if deleted.rows_affected() == 0 {
				return Ok(None);
			}
			let building =
				BuildRow::select_where(&mut tx, "template_id = ? AND status = 'building'", id).await?;
			sqlx::query("DELETE FROM template_aliases WHERE template_id = ?")
				.bind(id)
				.execute(&mut *tx)
				.await?;
			sqlx::query("DELETE FROM template_tags WHERE template_id = ?")
				.bind(id)
				.execute(&mut *tx)
				.await?;
			sqlx::query(
				"UPDATE builds SET status = 'error', reason = ?, finished_at = UTC_TIMESTAMP(3), \
				 updated_at = UTC_TIMESTAMP(3) WHERE template_id = ? AND status IN ('waiting', \
				 'building')",
			)
			.bind(Json(serde_json::json!({"message": "The template was deleted."})))
			.bind(id)
			.execute(&mut *tx)
			.await?;
			tx.commit().await?;
			Ok(Some(building))
		})
		.await
	}

	/// Hidden fork templates created before `before` that were never retired
	/// (their fork's driver died midway).
	pub async fn unretired_fork_templates(
		&self,
		before: DateTime<Utc>,
	) -> sqlx::Result<Vec<String>> {
		sqlx::query_scalar(
			"SELECT id FROM templates WHERE kind = 'fork' AND deleted_at IS NULL AND created_at < ?",
		)
		.bind(before)
		.fetch_all(self.pool())
		.await
	}

	/// Hard-deletes retired fork templates whose builds' artifacts are all
	/// collected (cascades to their builds, tags, and build logs): nothing
	/// refers to a hidden fork's rows once its artifacts are gone. Returns
	/// how many were deleted.
	///
	/// Candidates come from a non-locking read and are deleted by id, so only
	/// their rows get locked. (No `templates` index starts with `kind`: a
	/// single `DELETE … WHERE kind = 'fork'` scanned and locked every template
	/// row until it committed, stalling all template writes behind it.)
	pub async fn purge_collected_forks(&self) -> sqlx::Result<u64> {
		let candidates: Vec<String> = sqlx::query_scalar(
			"SELECT t.id FROM templates t WHERE t.kind = 'fork' AND t.deleted_at IS NOT NULL AND NOT \
			 EXISTS (SELECT 1 FROM builds b WHERE b.template_id = t.id AND b.artifacts_gc_at IS NULL)",
		)
		.fetch_all(self.pool())
		.await?;
		let mut purged = 0;
		for id in &candidates {
			let id = id.as_str();
			let done = retry_deadlocks(|| async move {
				sqlx::query(
					"DELETE FROM templates WHERE id = ? AND kind = 'fork' AND deleted_at IS NOT NULL \
					 AND NOT EXISTS (SELECT 1 FROM builds b WHERE b.template_id = templates.id AND \
					 b.artifacts_gc_at IS NULL)",
				)
				.bind(id)
				.execute(self.pool())
				.await
			})
			.await?;
			purged += done.rows_affected();
		}
		Ok(purged)
	}

	/// Adds `count` sandbox starts from the template, the last at
	/// `last_spawned_at` (never moving `last_spawned_at` back, since another
	/// driver may have written a later spawn first).
	pub async fn add_template_spawns(
		&self,
		id: &str,
		count: u64,
		last_spawned_at: DateTime<Utc>,
	) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query(
				"UPDATE templates SET spawn_count = spawn_count + ?, last_spawned_at = \
				 GREATEST(COALESCE(last_spawned_at, ?), ?) WHERE id = ?",
			)
			.bind(count)
			.bind(last_spawned_at)
			.bind(last_spawned_at)
			.bind(id)
			.execute(self.pool())
			.await
		})
		.await?;
		Ok(())
	}

	/// Tags of a template, oldest first.
	pub async fn template_tags(&self, template_id: &str) -> sqlx::Result<Vec<TagRow>> {
		sqlx::query_as(
			"SELECT template_id, tag, build_id, created_at FROM template_tags WHERE template_id = ? \
			 ORDER BY created_at, tag",
		)
		.bind(template_id)
		.fetch_all(self.pool())
		.await
	}

	/// Tags of several templates (any order).
	pub async fn tags_of(&self, template_ids: &[String]) -> sqlx::Result<Vec<TagRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let mut query = QueryBuilder::<MySql>::new(
			"SELECT template_id, tag, build_id, created_at FROM template_tags WHERE template_id IN (",
		);
		let mut list = query.separated(", ");
		for id in template_ids {
			list.push_bind(id);
		}
		query.push(") ORDER BY created_at, tag");
		query.build_query_as().fetch_all(self.pool()).await
	}

	/// Build a tag points to.
	pub async fn tag_build(&self, template_id: &str, tag: &str) -> sqlx::Result<Option<String>> {
		sqlx::query_scalar("SELECT build_id FROM template_tags WHERE template_id = ? AND tag = ?")
			.bind(template_id)
			.bind(tag)
			.fetch_optional(self.pool())
			.await
	}

	/// Points `tags` at `build_id` (insert or move).
	pub async fn assign_tags(
		&self,
		template_id: &str,
		build_id: &str,
		tags: &[String],
	) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			upsert_tags(&mut tx, template_id, build_id, tags).await?;
			tx.commit().await
		})
		.await
	}

	/// Removes tags; returns how many existed.
	pub async fn delete_tags(&self, template_id: &str, tags: &[String]) -> sqlx::Result<u64> {
		if tags.is_empty() {
			return Ok(0);
		}
		retry_deadlocks(|| async move {
			let mut query =
				QueryBuilder::<MySql>::new("DELETE FROM template_tags WHERE template_id = ");
			query.push_bind(template_id).push(" AND tag IN (");
			let mut list = query.separated(", ");
			for tag in tags {
				list.push_bind(tag);
			}
			query.push(")");
			Ok(query.build().execute(self.pool()).await?.rows_affected())
		})
		.await
	}

	/// `POST /v3/templates`: reuses the team's template for the alias (or
	/// creates it with its alias), supersedes earlier `waiting` builds sharing
	/// a tag, enforces the team build limit, and inserts a `waiting` build.
	pub async fn register_build(&self, req: &RegisterBuild<'_>) -> sqlx::Result<RegisterOutcome> {
		// Registrations probe alias rows `FOR UPDATE` and insert missing ones,
		// so concurrent registrations of new names deadlock on the shared gap.
		retry_deadlocks(|| self.register_build_once(req)).await
	}

	async fn register_build_once(&self, req: &RegisterBuild<'_>) -> sqlx::Result<RegisterOutcome> {
		let mut tx = self.pool().begin().await?;
		let existing = template_for_alias_locked(&mut tx, req.namespace, req.alias).await?;
		let (template_id, public) = match existing {
			Some(template)
				if template.team_id == req.team_id && template.is(TemplateKind::Template) =>
			{
				(template.id, template.public)
			},
			Some(_) => return Ok(RegisterOutcome::AliasTaken),
			None => {
				insert_template(
					&mut tx,
					req.new_template_id,
					req.team_id,
					TemplateKind::Template,
					None,
				)
				.await?;
				if !insert_alias(&mut tx, req.namespace, req.alias, req.new_template_id).await? {
					return Ok(RegisterOutcome::AliasTaken);
				}
				(req.new_template_id.to_owned(), false)
			},
		};
		sqlx::query(
			"UPDATE builds SET status = 'error', reason = ?, finished_at = UTC_TIMESTAMP(3), \
			 updated_at = UTC_TIMESTAMP(3) WHERE template_id = ? AND kind = 'build' AND status = \
			 'waiting' AND JSON_OVERLAPS(tags, ?)",
		)
		.bind(Json(superseded_reason()))
		.bind(&template_id)
		.bind(Json(req.tags))
		.execute(&mut *tx)
		.await?;
		let in_progress: i64 = sqlx::query_scalar(
			"SELECT COUNT(*) FROM builds WHERE team_id = ? AND kind = 'build' AND status IN \
			 ('waiting', 'building') FOR UPDATE",
		)
		.bind(req.team_id)
		.fetch_one(&mut *tx)
		.await?;
		if in_progress >= i64::from(req.max_concurrent_builds) {
			return Ok(RegisterOutcome::ConcurrencyLimit { limit: req.max_concurrent_builds });
		}
		sqlx::query(
			"INSERT INTO builds (id, template_id, team_id, status, kind, vcpus, memory_mb, disk_mb, \
			 min_free_disk_mb, tags, created_at, updated_at) VALUES (?, ?, ?, 'waiting', 'build', ?, \
			 ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
		)
		.bind(req.build_id)
		.bind(&template_id)
		.bind(req.team_id)
		.bind(req.vcpus)
		.bind(req.memory_mb)
		.bind(req.disk_mb)
		.bind(req.min_free_disk_mb)
		.bind(Json(req.tags))
		.execute(&mut *tx)
		.await?;
		sqlx::query("UPDATE templates SET updated_at = UTC_TIMESTAMP(3) WHERE id = ?")
			.bind(&template_id)
			.execute(&mut *tx)
			.await?;
		tx.commit().await?;
		Ok(RegisterOutcome::Registered { template_id, public })
	}

	/// Snapshot-as-template / fork: reuses the team's snapshot template for a
	/// name (or creates the template, with its alias when named) and inserts a
	/// `building` snapshot build carrying `tag`.
	pub async fn register_snapshot(
		&self,
		req: &RegisterSnapshot<'_>,
	) -> sqlx::Result<SnapshotOutcome> {
		retry_deadlocks(|| self.register_snapshot_once(req)).await
	}

	async fn register_snapshot_once(
		&self,
		req: &RegisterSnapshot<'_>,
	) -> sqlx::Result<SnapshotOutcome> {
		let mut tx = self.pool().begin().await?;
		let existing = match req.name {
			Some((namespace, alias)) => template_for_alias_locked(&mut tx, namespace, alias).await?,
			None => None,
		};
		let template_id = match existing {
			Some(template) if template.team_id == req.team_id && template.is(req.kind) => {
				sqlx::query(
					"UPDATE templates SET source_sandbox_id = ?, updated_at = UTC_TIMESTAMP(3) WHERE \
					 id = ?",
				)
				.bind(req.source_sandbox_id)
				.bind(&template.id)
				.execute(&mut *tx)
				.await?;
				template.id
			},
			Some(_) => return Ok(SnapshotOutcome::AliasTaken),
			None => {
				insert_template(
					&mut tx,
					req.new_template_id,
					req.team_id,
					req.kind,
					Some(req.source_sandbox_id),
				)
				.await?;
				if let Some((namespace, alias)) = req.name
					&& !insert_alias(&mut tx, namespace, alias, req.new_template_id).await?
				{
					return Ok(SnapshotOutcome::AliasTaken);
				}
				req.new_template_id.to_owned()
			},
		};
		sqlx::query(
			"INSERT INTO builds (id, template_id, team_id, status, kind, vcpus, memory_mb, disk_mb, \
			 envd_version, node_id, tags, created_at, updated_at, started_at) VALUES (?, ?, ?, \
			 'building', 'snapshot', ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), \
			 UTC_TIMESTAMP(3))",
		)
		.bind(req.build_id)
		.bind(&template_id)
		.bind(req.team_id)
		.bind(req.vcpus)
		.bind(req.memory_mb)
		.bind(req.disk_mb)
		.bind(req.envd_version)
		.bind(req.node_id)
		.bind(Json([req.tag]))
		.execute(&mut *tx)
		.await?;
		sqlx::query("UPDATE templates SET build_count = build_count + 1 WHERE id = ?")
			.bind(&template_id)
			.execute(&mut *tx)
			.await?;
		tx.commit().await?;
		Ok(SnapshotOutcome::Registered { template_id })
	}
}

/// Optional filters of [`Store::list_templates`].
#[derive(Clone, Copy, Debug, Default)]
pub struct TemplateFilter<'a> {
	pub source_sandbox_id: Option<&'a str>,
	/// Bare alias (any namespace).
	pub alias:             Option<&'a str>,
	/// Only templates with at least one `ready` build.
	pub ready_only:        bool,
}

/// Gives the live public templates of system teams matching `filter` (one
/// `?` bound to `value`, over `templates t`) a bare alias for each of their
/// names whose bare alias is still free.
pub(super) async fn claim_bare_aliases(
	tx: &mut Transaction<'_, MySql>,
	filter: &str,
	value: &str,
) -> sqlx::Result<u64> {
	let sql = format!(
		"INSERT IGNORE INTO template_aliases (namespace, alias, template_id) SELECT '', a.alias, \
		 a.template_id FROM template_aliases a JOIN templates t ON t.id = a.template_id JOIN teams \
		 tm ON tm.id = t.team_id WHERE a.namespace <> '' AND t.public AND t.deleted_at IS NULL AND \
		 tm.is_system AND {filter}"
	);
	let result = sqlx::query(sqlx::AssertSqlSafe(sql))
		.bind(value)
		.execute(&mut **tx)
		.await?;
	Ok(result.rows_affected())
}

async fn template_for_alias_locked(
	tx: &mut Transaction<'_, MySql>,
	namespace: &str,
	alias: &str,
) -> sqlx::Result<Option<TemplateRow>> {
	let sql = format!(
		"SELECT {TEMPLATE_COLUMNS} FROM template_aliases a JOIN templates t ON t.id = a.template_id \
		 WHERE a.namespace = ? AND a.alias = ? AND t.deleted_at IS NULL FOR UPDATE"
	);
	sqlx::query_as(sqlx::AssertSqlSafe(sql))
		.bind(namespace)
		.bind(alias)
		.fetch_optional(&mut **tx)
		.await
}

async fn insert_template(
	tx: &mut Transaction<'_, MySql>,
	id: &str,
	team_id: &str,
	kind: TemplateKind,
	source_sandbox_id: Option<&str>,
) -> sqlx::Result<()> {
	sqlx::query(
		"INSERT INTO templates (id, team_id, public, kind, source_sandbox_id, created_at, \
		 updated_at) VALUES (?, ?, FALSE, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3))",
	)
	.bind(id)
	.bind(team_id)
	.bind(kind.as_str())
	.bind(source_sandbox_id)
	.execute(&mut **tx)
	.await?;
	Ok(())
}

/// `false` when the alias is taken (a concurrent registration won).
async fn insert_alias(
	tx: &mut Transaction<'_, MySql>,
	namespace: &str,
	alias: &str,
	template_id: &str,
) -> sqlx::Result<bool> {
	let result = sqlx::query(
		"INSERT IGNORE INTO template_aliases (namespace, alias, template_id) VALUES (?, ?, ?)",
	)
	.bind(namespace)
	.bind(alias)
	.bind(template_id)
	.execute(&mut **tx)
	.await?;
	Ok(result.rows_affected() == 1)
}

/// Inserts or moves tags inside a transaction.
pub(super) async fn upsert_tags(
	tx: &mut Transaction<'_, MySql>,
	template_id: &str,
	build_id: &str,
	tags: &[String],
) -> sqlx::Result<()> {
	for tag in tags {
		sqlx::query(
			"INSERT INTO template_tags (template_id, tag, build_id, created_at) VALUES (?, ?, ?, \
			 UTC_TIMESTAMP(3)) ON DUPLICATE KEY UPDATE build_id = VALUES(build_id), created_at = \
			 VALUES(created_at)",
		)
		.bind(template_id)
		.bind(tag)
		.bind(build_id)
		.execute(&mut **tx)
		.await?;
	}
	Ok(())
}

#[cfg(test)]
mod tests;
