//! `api_keys`: team API keys (SHA-256 for lookup, AES-GCM ciphertext so
//! `GET /teams` can return the key).

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use super::{Store, Team};

/// One stored API key (never carries the plaintext).
#[derive(Clone, Debug, FromRow)]
pub struct ApiKeyRow {
	pub id:               String,
	pub team_id:          String,
	pub key_hash:         String,
	/// `Keys::encrypt` of the full key text.
	pub key_enc:          Vec<u8>,
	pub mask_prefix:      String,
	pub mask_suffix:      String,
	pub name:             String,
	pub created_by:       Option<String>,
	/// Email of `created_by` (joined).
	pub created_by_email: Option<String>,
	pub created_at:       DateTime<Utc>,
	pub last_used_at:     Option<DateTime<Utc>>,
}

/// Insert payload for [`Store::insert_api_key`].
#[derive(Clone, Debug)]
pub struct NewApiKey<'a> {
	pub team_id:     &'a str,
	pub name:        &'a str,
	pub created_by:  Option<&'a str>,
	pub key_hash:    &'a str,
	pub key_enc:     &'a [u8],
	pub mask_prefix: &'a str,
	pub mask_suffix: &'a str,
}

const KEY_COLUMNS: &str = "k.id, k.team_id, k.key_hash, k.key_enc, k.mask_prefix, k.mask_suffix, \
                           k.name, k.created_by, u.email AS created_by_email, k.created_at, \
                           k.last_used_at";

impl Store {
	/// Stores a new key and returns its row.
	pub async fn insert_api_key(&self, key: &NewApiKey<'_>) -> sqlx::Result<ApiKeyRow> {
		let id = uuid::Uuid::new_v4().to_string();
		sqlx::query(
			"INSERT INTO api_keys (id, team_id, key_hash, key_enc, mask_prefix, mask_suffix, name, \
			 created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		)
		.bind(&id)
		.bind(key.team_id)
		.bind(key.key_hash)
		.bind(key.key_enc)
		.bind(key.mask_prefix)
		.bind(key.mask_suffix)
		.bind(key.name)
		.bind(key.created_by)
		.execute(self.pool())
		.await?;
		self
			.api_key(key.team_id, &id)
			.await?
			.ok_or(sqlx::Error::RowNotFound)
	}

	/// One key of a team.
	pub async fn api_key(&self, team_id: &str, id: &str) -> sqlx::Result<Option<ApiKeyRow>> {
		let sql = format!(
			"SELECT {KEY_COLUMNS} FROM api_keys k LEFT JOIN users u ON u.id = k.created_by WHERE \
			 k.team_id = ? AND k.id = ?"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// All keys of a team, newest first.
	pub async fn list_api_keys(&self, team_id: &str) -> sqlx::Result<Vec<ApiKeyRow>> {
		let sql = format!(
			"SELECT {KEY_COLUMNS} FROM api_keys k LEFT JOIN users u ON u.id = k.created_by WHERE \
			 k.team_id = ? ORDER BY k.created_at DESC, k.id DESC"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.fetch_all(self.pool())
			.await
	}

	/// The newest key `user_id` created in `team_id` (reused by `GET /teams`).
	pub async fn latest_user_api_key(
		&self,
		team_id: &str,
		user_id: &str,
	) -> sqlx::Result<Option<ApiKeyRow>> {
		let sql = format!(
			"SELECT {KEY_COLUMNS} FROM api_keys k LEFT JOIN users u ON u.id = k.created_by WHERE \
			 k.team_id = ? AND k.created_by = ? ORDER BY k.created_at DESC, k.id DESC LIMIT 1"
		);
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(user_id)
			.fetch_optional(self.pool())
			.await
	}

	/// Resolves a key hash to `(key id, team)`.
	pub async fn team_for_api_key(&self, key_hash: &str) -> sqlx::Result<Option<(String, Team)>> {
		let Some(row) = sqlx::query_as::<_, (String, String)>(
			"SELECT id, team_id FROM api_keys WHERE key_hash = ?",
		)
		.bind(key_hash)
		.fetch_optional(self.pool())
		.await?
		else {
			return Ok(None);
		};
		Ok(self.team_by_id(&row.1).await?.map(|team| (row.0, team)))
	}

	/// Renames a key; `false` when it does not belong to the team.
	pub async fn rename_api_key(&self, team_id: &str, id: &str, name: &str) -> sqlx::Result<bool> {
		sqlx::query("UPDATE api_keys SET name = ? WHERE team_id = ? AND id = ?")
			.bind(name)
			.bind(team_id)
			.bind(id)
			.execute(self.pool())
			.await?;
		Ok(self.api_key(team_id, id).await?.is_some())
	}

	/// Deletes a key and returns its hash (for cache invalidation).
	pub async fn delete_api_key(&self, team_id: &str, id: &str) -> sqlx::Result<Option<String>> {
		let mut tx = self.pool().begin().await?;
		let hash: Option<String> = sqlx::query_scalar(
			"SELECT key_hash FROM api_keys WHERE team_id = ? AND id = ? FOR UPDATE",
		)
		.bind(team_id)
		.bind(id)
		.fetch_optional(&mut *tx)
		.await?;
		if hash.is_some() {
			sqlx::query("DELETE FROM api_keys WHERE team_id = ? AND id = ?")
				.bind(team_id)
				.bind(id)
				.execute(&mut *tx)
				.await?;
		}
		tx.commit().await?;
		Ok(hash)
	}

	/// Stamps `last_used_at = now`.
	pub async fn touch_api_key(&self, id: &str) -> sqlx::Result<()> {
		sqlx::query("UPDATE api_keys SET last_used_at = CURRENT_TIMESTAMP(3) WHERE id = ?")
			.bind(id)
			.execute(self.pool())
			.await
			.map(drop)
	}
}
