//! `teams` and `team_members`.

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use super::Store;
use crate::state::TeamLimits;

/// A tenant (E2B "team"/"project"); every API-key and sandbox belongs to one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Team {
	/// UUID v4.
	pub id:         String,
	pub name:       String,
	/// Unique URL-safe name (template namespace).
	pub slug:       String,
	pub limits:     TeamLimits,
	/// Blocked teams may read but not mutate (403 `team is blocked`).
	pub blocked:    bool,
	/// System teams publish the default templates: their public templates
	/// claim bare aliases, which every team resolves after its own namespace.
	pub system:     bool,
	pub created_at: DateTime<Utc>,
}

#[derive(FromRow)]
struct TeamRecord {
	id: String,
	name: String,
	slug: String,
	created_at: DateTime<Utc>,
	max_concurrent_sandboxes: u32,
	max_concurrent_builds: u32,
	max_vcpus: u32,
	max_memory_mb: u64,
	max_disk_mb: u64,
	max_length_hours: u32,
	blocked: bool,
	is_system: bool,
}

impl From<TeamRecord> for Team {
	fn from(r: TeamRecord) -> Self {
		Self {
			id:         r.id,
			name:       r.name,
			slug:       r.slug,
			limits:     TeamLimits {
				max_concurrent_sandboxes: r.max_concurrent_sandboxes,
				max_concurrent_builds:    r.max_concurrent_builds,
				max_vcpus:                r.max_vcpus,
				max_memory_mb:            r.max_memory_mb,
				max_disk_mb:              r.max_disk_mb,
				max_length_hours:         r.max_length_hours,
			},
			blocked:    r.blocked,
			system:     r.is_system,
			created_at: r.created_at,
		}
	}
}

#[derive(FromRow)]
struct MemberTeamRecord {
	#[sqlx(flatten)]
	team:       TeamRecord,
	is_default: bool,
}

const TEAM_COLUMNS: &str = "t.id, t.name, t.slug, t.created_at, t.max_concurrent_sandboxes, \
                            t.max_concurrent_builds, t.max_vcpus, t.max_memory_mb, t.max_disk_mb, \
                            t.max_length_hours, t.blocked, t.is_system";

/// Lower-case, `[a-z0-9-]` slug of a team name (never empty).
pub fn slugify(name: &str) -> String {
	let mut slug = String::with_capacity(name.len());
	for ch in name.chars() {
		if ch.is_ascii_alphanumeric() {
			slug.push(ch.to_ascii_lowercase());
		} else if !slug.ends_with('-') && !slug.is_empty() {
			slug.push('-');
		}
	}
	while slug.ends_with('-') {
		slug.pop();
	}
	if slug.is_empty() {
		String::from("team")
	} else {
		slug
	}
}

impl Store {
	/// Inserts a team with a unique slug derived from `name` (suffixing
	/// `-2`, `-3`, … on collision).
	pub async fn create_team(&self, name: &str, limits: TeamLimits) -> sqlx::Result<Team> {
		let id = uuid::Uuid::new_v4().to_string();
		let base = slugify(name);
		let mut attempt = 1u32;
		loop {
			let slug = if attempt == 1 {
				base.clone()
			} else {
				format!("{base}-{attempt}")
			};
			let inserted = sqlx::query(
				"INSERT INTO teams (id, name, slug, max_concurrent_sandboxes, max_concurrent_builds, \
				 max_vcpus, max_memory_mb, max_disk_mb, max_length_hours) VALUES (?, ?, ?, ?, ?, ?, \
				 ?, ?, ?)",
			)
			.bind(&id)
			.bind(name)
			.bind(&slug)
			.bind(limits.max_concurrent_sandboxes)
			.bind(limits.max_concurrent_builds)
			.bind(limits.max_vcpus)
			.bind(limits.max_memory_mb)
			.bind(limits.max_disk_mb)
			.bind(limits.max_length_hours)
			.execute(self.pool())
			.await;
			match inserted {
				Ok(_) => break,
				Err(sqlx::Error::Database(e)) if e.is_unique_violation() && attempt < 100 => {
					attempt += 1;
				},
				Err(e) => return Err(e),
			}
		}
		self.team_by_id(&id).await?.ok_or(sqlx::Error::RowNotFound)
	}

	/// Team by id.
	pub async fn team_by_id(&self, id: &str) -> sqlx::Result<Option<Team>> {
		let sql = format!("SELECT {TEAM_COLUMNS} FROM teams t WHERE t.id = ?");
		let row: Option<TeamRecord> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(id)
			.fetch_optional(self.pool())
			.await?;
		Ok(row.map(Team::from))
	}

	/// Team by id or slug (admin tooling accepts either).
	pub async fn team_by_ref(&self, id_or_slug: &str) -> sqlx::Result<Option<Team>> {
		let sql = format!("SELECT {TEAM_COLUMNS} FROM teams t WHERE t.id = ? OR t.slug = ? LIMIT 1");
		let row: Option<TeamRecord> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(id_or_slug)
			.bind(id_or_slug)
			.fetch_optional(self.pool())
			.await?;
		Ok(row.map(Team::from))
	}

	/// Every team, oldest first.
	pub async fn list_teams(&self) -> sqlx::Result<Vec<Team>> {
		let sql = format!("SELECT {TEAM_COLUMNS} FROM teams t ORDER BY t.created_at, t.id");
		let rows: Vec<TeamRecord> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.fetch_all(self.pool())
			.await?;
		Ok(rows.into_iter().map(Team::from).collect())
	}

	/// Replaces a team's limits; `false` when the team does not exist.
	pub async fn set_team_limits(&self, id: &str, limits: TeamLimits) -> sqlx::Result<bool> {
		// MySQL reports 0 affected rows when nothing changed, so existence is
		// checked separately.
		sqlx::query(
			"UPDATE teams SET max_concurrent_sandboxes = ?, max_concurrent_builds = ?, max_vcpus = \
			 ?, max_memory_mb = ?, max_disk_mb = ?, max_length_hours = ? WHERE id = ?",
		)
		.bind(limits.max_concurrent_sandboxes)
		.bind(limits.max_concurrent_builds)
		.bind(limits.max_vcpus)
		.bind(limits.max_memory_mb)
		.bind(limits.max_disk_mb)
		.bind(limits.max_length_hours)
		.bind(id)
		.execute(self.pool())
		.await?;
		Ok(self.team_by_id(id).await?.is_some())
	}

	/// Blocks or unblocks a team; `false` when the team does not exist.
	pub async fn set_team_blocked(&self, id: &str, blocked: bool) -> sqlx::Result<bool> {
		sqlx::query("UPDATE teams SET blocked = ? WHERE id = ?")
			.bind(blocked)
			.bind(id)
			.execute(self.pool())
			.await?;
		Ok(self.team_by_id(id).await?.is_some())
	}

	/// Marks or unmarks a team as a system team; `false` when the team does
	/// not exist. Marking claims the free bare aliases of its public
	/// templates; unmarking releases the bare aliases of its templates.
	pub async fn set_team_system(&self, id: &str, system: bool) -> sqlx::Result<bool> {
		let mut tx = self.pool().begin().await?;
		sqlx::query("UPDATE teams SET is_system = ? WHERE id = ?")
			.bind(system)
			.bind(id)
			.execute(&mut *tx)
			.await?;
		if system {
			super::templates::claim_bare_aliases(&mut tx, "t.team_id = ?", id).await?;
		} else {
			sqlx::query(
				"DELETE a FROM template_aliases a JOIN templates t ON t.id = a.template_id WHERE \
				 a.namespace = '' AND t.team_id = ?",
			)
			.bind(id)
			.execute(&mut *tx)
			.await?;
		}
		tx.commit().await?;
		Ok(self.team_by_id(id).await?.is_some())
	}

	/// Adds (or updates) a membership. Making it the default clears the
	/// user's other default flags.
	pub async fn add_team_member(
		&self,
		team_id: &str,
		user_id: &str,
		is_default: bool,
	) -> sqlx::Result<()> {
		let mut tx = self.pool().begin().await?;
		if is_default {
			sqlx::query("UPDATE team_members SET is_default = FALSE WHERE user_id = ?")
				.bind(user_id)
				.execute(&mut *tx)
				.await?;
		}
		sqlx::query(
			"INSERT INTO team_members (team_id, user_id, is_default) VALUES (?, ?, ?) ON DUPLICATE \
			 KEY UPDATE is_default = VALUES(is_default)",
		)
		.bind(team_id)
		.bind(user_id)
		.bind(is_default)
		.execute(&mut *tx)
		.await?;
		tx.commit().await
	}

	/// Teams the user belongs to with their `is_default` flag. When no
	/// membership is flagged default, the oldest membership is reported as
	/// default so the CLI always finds one.
	pub async fn user_teams(&self, user_id: &str) -> sqlx::Result<Vec<(Team, bool)>> {
		let sql = format!(
			"SELECT {TEAM_COLUMNS}, m.is_default FROM team_members m JOIN teams t ON t.id = \
			 m.team_id WHERE m.user_id = ? ORDER BY m.created_at, t.id"
		);
		let rows: Vec<MemberTeamRecord> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(user_id)
			.fetch_all(self.pool())
			.await?;
		let any_default = rows.iter().any(|r| r.is_default);
		Ok(rows
			.into_iter()
			.enumerate()
			.map(|(i, r)| {
				let is_default = r.is_default || (!any_default && i == 0);
				(Team::from(r.team), is_default)
			})
			.collect())
	}

	/// The team `team_id` if `user_id` is a member of it.
	pub async fn user_team(&self, user_id: &str, team_id: &str) -> sqlx::Result<Option<Team>> {
		let sql = format!(
			"SELECT {TEAM_COLUMNS} FROM team_members m JOIN teams t ON t.id = m.team_id WHERE \
			 m.user_id = ? AND m.team_id = ?"
		);
		let row: Option<TeamRecord> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(user_id)
			.bind(team_id)
			.fetch_optional(self.pool())
			.await?;
		Ok(row.map(Team::from))
	}

	/// The user's default team (see [`Store::user_teams`] for the fallback).
	pub async fn default_team(&self, user_id: &str) -> sqlx::Result<Option<Team>> {
		Ok(self
			.user_teams(user_id)
			.await?
			.into_iter()
			.find(|(_, is_default)| *is_default)
			.map(|(team, _)| team))
	}
}

#[cfg(test)]
mod tests {
	use super::slugify;

	#[test]
	fn slugify_normalizes_and_never_returns_empty() {
		assert_eq!(slugify("My Team!"), "my-team");
		assert_eq!(slugify("  --a__b--  "), "a-b");
		assert_eq!(slugify("ÄÖÜ"), "team");
	}
}
