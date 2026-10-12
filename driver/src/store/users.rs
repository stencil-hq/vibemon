//! `users`: CLI/dashboard identities (email + argon2 password hash).

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use super::Store;

/// A human account that signs in through the CLI login page.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
pub struct User {
	/// UUID v4.
	pub id:            String,
	/// Lower-cased email.
	pub email:         String,
	/// Argon2 PHC string.
	pub password_hash: String,
	pub created_at:    DateTime<Utc>,
}

impl Store {
	/// Inserts a user; the email is stored lower-cased. Unique violations
	/// surface as `sqlx::Error::Database`.
	pub async fn create_user(&self, email: &str, password_hash: &str) -> sqlx::Result<User> {
		let id = uuid::Uuid::new_v4().to_string();
		sqlx::query("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)")
			.bind(&id)
			.bind(email.trim().to_lowercase())
			.bind(password_hash)
			.execute(self.pool())
			.await?;
		self.user_by_id(&id).await?.ok_or(sqlx::Error::RowNotFound)
	}

	/// User by id.
	pub async fn user_by_id(&self, id: &str) -> sqlx::Result<Option<User>> {
		sqlx::query_as("SELECT id, email, password_hash, created_at FROM users WHERE id = ?")
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// User by (case-insensitive) email.
	pub async fn user_by_email(&self, email: &str) -> sqlx::Result<Option<User>> {
		sqlx::query_as("SELECT id, email, password_hash, created_at FROM users WHERE email = ?")
			.bind(email.trim().to_lowercase())
			.fetch_optional(self.pool())
			.await
	}

	/// Replaces a user's password hash; `false` when the user is unknown.
	pub async fn set_user_password(&self, id: &str, password_hash: &str) -> sqlx::Result<bool> {
		let done = sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
			.bind(password_hash)
			.bind(id)
			.execute(self.pool())
			.await?;
		Ok(done.rows_affected() > 0)
	}
}
