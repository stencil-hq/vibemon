//! `refresh_tokens`: CLI login refresh tokens (stored as SHA-256).

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use super::Store;

/// A stored refresh token.
#[derive(Clone, Debug, FromRow)]
pub struct RefreshTokenRow {
	pub id:         String,
	pub user_id:    String,
	pub token_hash: String,
	pub created_at: DateTime<Utc>,
	pub expires_at: DateTime<Utc>,
	pub revoked_at: Option<DateTime<Utc>>,
}

/// Outcome of [`Store::rotate_refresh_token`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rotation {
	/// The old token was valid; it is now revoked and the new one stored.
	Rotated {
		/// Owner of the token.
		user_id: String,
	},
	/// Unknown or expired token.
	Invalid,
	/// An already-rotated/revoked token was presented again: every refresh
	/// token of the user has been revoked (token theft response).
	Reused,
}

impl Store {
	/// Stores a refresh token hash for `user_id`.
	pub async fn insert_refresh_token(
		&self,
		user_id: &str,
		token_hash: &str,
		expires_at: DateTime<Utc>,
	) -> sqlx::Result<String> {
		let id = uuid::Uuid::new_v4().to_string();
		sqlx::query(
			"INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES (?, ?, ?, ?)",
		)
		.bind(&id)
		.bind(user_id)
		.bind(token_hash)
		.bind(expires_at)
		.execute(self.pool())
		.await?;
		Ok(id)
	}

	/// Atomically exchanges `old_hash` for `new_hash` (refresh-token rotation).
	pub async fn rotate_refresh_token(
		&self,
		old_hash: &str,
		new_hash: &str,
		new_expires_at: DateTime<Utc>,
	) -> sqlx::Result<Rotation> {
		let mut tx = self.pool().begin().await?;
		let row: Option<RefreshTokenRow> = sqlx::query_as(
			"SELECT id, user_id, token_hash, created_at, expires_at, revoked_at FROM refresh_tokens \
			 WHERE token_hash = ? FOR UPDATE",
		)
		.bind(old_hash)
		.fetch_optional(&mut *tx)
		.await?;
		let Some(row) = row else {
			return Ok(Rotation::Invalid);
		};
		if row.revoked_at.is_some() {
			sqlx::query(
				"UPDATE refresh_tokens SET revoked_at = CURRENT_TIMESTAMP(3) WHERE user_id = ? AND \
				 revoked_at IS NULL",
			)
			.bind(&row.user_id)
			.execute(&mut *tx)
			.await?;
			tx.commit().await?;
			return Ok(Rotation::Reused);
		}
		if row.expires_at <= Utc::now() {
			return Ok(Rotation::Invalid);
		}
		sqlx::query("UPDATE refresh_tokens SET revoked_at = CURRENT_TIMESTAMP(3) WHERE id = ?")
			.bind(&row.id)
			.execute(&mut *tx)
			.await?;
		sqlx::query(
			"INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) VALUES (?, ?, ?, ?)",
		)
		.bind(uuid::Uuid::new_v4().to_string())
		.bind(&row.user_id)
		.bind(new_hash)
		.bind(new_expires_at)
		.execute(&mut *tx)
		.await?;
		tx.commit().await?;
		Ok(Rotation::Rotated { user_id: row.user_id })
	}

	/// Revokes a refresh token; `false` when unknown or already revoked.
	pub async fn revoke_refresh_token(&self, token_hash: &str) -> sqlx::Result<bool> {
		let done = sqlx::query(
			"UPDATE refresh_tokens SET revoked_at = CURRENT_TIMESTAMP(3) WHERE token_hash = ? AND \
			 revoked_at IS NULL",
		)
		.bind(token_hash)
		.execute(self.pool())
		.await?;
		Ok(done.rows_affected() > 0)
	}
}
