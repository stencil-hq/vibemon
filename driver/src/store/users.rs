//! Users: CLI/dashboard identities (email + argon2 password hash).
//!
//! Keys:
//! - `db/user/<id>` → [`User`].
//! - `db/user.email/<email>` → user id (JSON string); unique index over the
//!   lower-cased email. [`Store::create_user`] guards it absent, so two
//!   concurrent sign-ups with one email cannot both commit.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Error, Result, Store, key};

/// A human account that signs in through the CLI login page.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
	/// UUID v4.
	pub id:            String,
	/// Lower-cased email.
	pub email:         String,
	/// Argon2 PHC string.
	pub password_hash: String,
	pub created_at:    DateTime<Utc>,
}

/// Key of the [`User`] record `id`.
pub(super) fn user_key(id: &str) -> String {
	key("db/user", &[id])
}

/// Key of the unique email index entry of `email` (already normalized).
fn email_key(email: &str) -> String {
	key("db/user.email", &[email])
}

/// Stored (and looked-up) form of an email.
fn normalize_email(email: &str) -> String {
	email.trim().to_lowercase()
}

impl Store {
	/// Inserts a user; the email is stored lower-cased.
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when the email is taken; store failures.
	pub async fn create_user(&self, email: &str, password_hash: &str) -> Result<User> {
		let user = User {
			id:            uuid::Uuid::new_v4().to_string(),
			email:         normalize_email(email),
			password_hash: password_hash.to_owned(),
			created_at:    super::now(),
		};
		let index = email_key(&user.email);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<String>(&index).await?.is_some() {
				return Err(Error::Duplicate("user email"));
			}
			tx.put(user_key(&user.id), &user);
			tx.put(index.as_str(), &user.id);
			if tx.commit().await? {
				return Ok(user);
			}
		}
	}

	/// User by id.
	pub async fn user_by_id(&self, id: &str) -> Result<Option<User>> {
		self.read().await?.get(&user_key(id)).await
	}

	/// User by (case-insensitive) email.
	pub async fn user_by_email(&self, email: &str) -> Result<Option<User>> {
		let r = self.read().await?;
		let Some(id) = r.get::<String>(&email_key(&normalize_email(email))).await? else {
			return Ok(None);
		};
		r.get(&user_key(&id)).await
	}

	/// Replaces a user's password hash; `false` when the user is unknown.
	pub async fn set_user_password(&self, id: &str, password_hash: &str) -> Result<bool> {
		let key = user_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut user) = tx.get::<User>(&key).await? else {
				return Ok(false);
			};
			password_hash.clone_into(&mut user.password_hash);
			tx.put(key.as_str(), &user);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}
}
