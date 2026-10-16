//! `secrets` + `secret_versions`: versioned, encrypted team secrets.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, types::Json};

use super::{Cursor, Store, micros, retry_deadlocks};

/// Secret metadata (never the value).
#[derive(Clone, Debug, FromRow)]
pub struct SecretRow {
	/// `sec_` + 32 hex.
	pub id:              String,
	pub team_id:         String,
	/// Lower-cased name, unique per team.
	pub name:            String,
	pub current_version: i64,
	pub metadata:        Json<BTreeMap<String, String>>,
	pub created_at:      DateTime<Utc>,
	pub updated_at:      DateTime<Utc>,
}

impl SecretRow {
	/// Pagination cursor positioned after this row.
	pub fn cursor(&self) -> Cursor {
		Cursor { created_at_us: micros(self.created_at), id: self.id.clone() }
	}
}

const SECRET_COLUMNS: &str = "id, team_id, name, current_version, metadata, created_at, updated_at";

/// Prefix of secret identifiers (names may not use it).
pub const SECRET_ID_PREFIX: &str = "sec_";

impl Store {
	/// Creates a secret at version 1. A duplicate name surfaces as a unique
	/// violation (`sqlx::Error::Database`).
	pub async fn create_secret(
		&self,
		team_id: &str,
		name: &str,
		value_enc: &[u8],
		metadata: &BTreeMap<String, String>,
	) -> sqlx::Result<SecretRow> {
		let id = format!("{SECRET_ID_PREFIX}{}", uuid::Uuid::new_v4().simple());
		let id = id.as_str();
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			sqlx::query(
				"INSERT INTO secrets (id, team_id, name, current_version, metadata) VALUES (?, ?, ?, \
				 1, ?)",
			)
			.bind(id)
			.bind(team_id)
			.bind(name)
			.bind(Json(metadata))
			.execute(&mut *tx)
			.await?;
			sqlx::query(
				"INSERT INTO secret_versions (secret_id, version, value_enc) VALUES (?, 1, ?)",
			)
			.bind(id)
			.bind(value_enc)
			.execute(&mut *tx)
			.await?;
			tx.commit().await
		})
		.await?;
		self
			.secret(team_id, id)
			.await?
			.ok_or(sqlx::Error::RowNotFound)
	}

	/// Secret by id (`sec_…`) or by (case-insensitive) name.
	pub async fn secret(&self, team_id: &str, id_or_name: &str) -> sqlx::Result<Option<SecretRow>> {
		let sql = if id_or_name.starts_with(SECRET_ID_PREFIX) {
			format!("SELECT {SECRET_COLUMNS} FROM secrets WHERE team_id = ? AND id = ?")
		} else {
			format!("SELECT {SECRET_COLUMNS} FROM secrets WHERE team_id = ? AND name = ?")
		};
		let key = if id_or_name.starts_with(SECRET_ID_PREFIX) {
			id_or_name.to_owned()
		} else {
			id_or_name.to_lowercase()
		};
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(key)
			.fetch_optional(self.pool())
			.await
	}

	/// One page of a team's secrets, newest first, strictly after `after`.
	/// Fetches up to `limit` rows.
	pub async fn list_secrets(
		&self,
		team_id: &str,
		after: Option<&Cursor>,
		limit: u32,
	) -> sqlx::Result<Vec<SecretRow>> {
		match after {
			None => {
				let sql = format!(
					"SELECT {SECRET_COLUMNS} FROM secrets WHERE team_id = ? ORDER BY created_at DESC, \
					 id DESC LIMIT ?"
				);
				sqlx::query_as(sqlx::AssertSqlSafe(sql))
					.bind(team_id)
					.bind(limit)
					.fetch_all(self.pool())
					.await
			},
			Some(cursor) => {
				let sql = format!(
					"SELECT {SECRET_COLUMNS} FROM secrets WHERE team_id = ? AND (created_at < ? OR \
					 (created_at = ? AND id < ?)) ORDER BY created_at DESC, id DESC LIMIT ?"
				);
				let at = super::from_micros(cursor.created_at_us);
				sqlx::query_as(sqlx::AssertSqlSafe(sql))
					.bind(team_id)
					.bind(at)
					.bind(at)
					.bind(&cursor.id)
					.bind(limit)
					.fetch_all(self.pool())
					.await
			},
		}
	}

	/// Number of secrets a team holds.
	pub async fn count_secrets(&self, team_id: &str) -> sqlx::Result<i64> {
		sqlx::query_scalar("SELECT COUNT(*) FROM secrets WHERE team_id = ?")
			.bind(team_id)
			.fetch_one(self.pool())
			.await
	}

	/// Appends a new version and makes it current; `metadata` replaces the
	/// stored map when given. `None` when the secret does not exist.
	pub async fn update_secret(
		&self,
		team_id: &str,
		id: &str,
		value_enc: &[u8],
		metadata: Option<&BTreeMap<String, String>>,
	) -> sqlx::Result<Option<SecretRow>> {
		let found = retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let version: Option<i64> = sqlx::query_scalar(
				"SELECT current_version FROM secrets WHERE team_id = ? AND id = ? FOR UPDATE",
			)
			.bind(team_id)
			.bind(id)
			.fetch_optional(&mut *tx)
			.await?;
			let Some(version) = version else {
				return Ok(false);
			};
			let next = version + 1;
			sqlx::query(
				"INSERT INTO secret_versions (secret_id, version, value_enc) VALUES (?, ?, ?)",
			)
			.bind(id)
			.bind(next)
			.bind(value_enc)
			.execute(&mut *tx)
			.await?;
			match metadata {
				Some(metadata) => {
					sqlx::query(
						"UPDATE secrets SET current_version = ?, metadata = ?, updated_at = \
						 CURRENT_TIMESTAMP(3) WHERE id = ?",
					)
					.bind(next)
					.bind(Json(metadata))
					.bind(id)
					.execute(&mut *tx)
					.await?;
				},
				None => {
					sqlx::query(
						"UPDATE secrets SET current_version = ?, updated_at = CURRENT_TIMESTAMP(3) \
						 WHERE id = ?",
					)
					.bind(next)
					.bind(id)
					.execute(&mut *tx)
					.await?;
				},
			}
			tx.commit().await?;
			Ok(true)
		})
		.await?;
		if !found {
			return Ok(None);
		}
		self.secret(team_id, id).await
	}

	/// Deletes a secret with all versions; `false` when unknown.
	pub async fn delete_secret(&self, team_id: &str, id: &str) -> sqlx::Result<bool> {
		let done = retry_deadlocks(|| async move {
			sqlx::query("DELETE FROM secrets WHERE team_id = ? AND id = ?")
				.bind(team_id)
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(done.rows_affected() > 0)
	}

	/// Encrypted value of a secret version (current when `version` is
	/// `None`), looked up by id or name. Used when resolving secret markers
	/// for sandbox egress.
	pub async fn secret_value(
		&self,
		team_id: &str,
		id_or_name: &str,
		version: Option<i64>,
	) -> sqlx::Result<Option<Vec<u8>>> {
		let Some(secret) = self.secret(team_id, id_or_name).await? else {
			return Ok(None);
		};
		sqlx::query_scalar(
			"SELECT value_enc FROM secret_versions WHERE secret_id = ? AND version = ?",
		)
		.bind(&secret.id)
		.bind(version.unwrap_or(secret.current_version))
		.fetch_optional(self.pool())
		.await
	}
}
