//! CLI login refresh tokens (stored as SHA-256).
//!
//! Keys (all expire at the token's `expires_at`, so expired tokens vanish):
//! - `db/rtoken/<hash>` → [`RefreshTokenRow`]. Rotation and revocation guard
//!   the record they read, so of two concurrent rotations of one token only one
//!   commits; the other reruns, sees it revoked and reports reuse.
//! - `db/rtoken.user/<user>/<hash>` → empty; the user's tokens (revoked all at
//!   once when a rotated token is presented again).

use std::time::Duration;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};

use super::{Error, Result, Store, Tx, key, last_segment, prefix};

/// A stored refresh token.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

fn token_key(hash: &str) -> String {
	key("db/rtoken", &[hash])
}

fn user_token_key(user_id: &str, hash: &str) -> String {
	key("db/rtoken.user", &[user_id, hash])
}

/// Time left until `at` (zero once it passed).
fn until(at: DateTime<Utc>) -> Duration {
	(at - Utc::now()).to_std().unwrap_or_default()
}

/// Stages `row` (and its user index entry) to expire at its `expires_at`.
fn stage(tx: &mut Tx<'_>, row: &RefreshTokenRow) {
	let ttl = until(row.expires_at);
	tx.put_ttl(token_key(&row.token_hash), row, ttl);
	tx.mark(user_token_key(&row.user_id, &row.token_hash), Some(ttl));
}

/// A fresh, unrevoked token record.
fn new_token(user_id: &str, token_hash: &str, expires_at: DateTime<Utc>) -> RefreshTokenRow {
	RefreshTokenRow {
		id:         uuid::Uuid::new_v4().to_string(),
		user_id:    user_id.to_owned(),
		token_hash: token_hash.to_owned(),
		created_at: super::now(),
		expires_at: expires_at.trunc_subsecs(3),
		revoked_at: None,
	}
}

impl Store {
	/// Stores a refresh token hash for `user_id`; returns the token's id.
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when the hash is already stored; store failures.
	pub async fn insert_refresh_token(
		&self,
		user_id: &str,
		token_hash: &str,
		expires_at: DateTime<Utc>,
	) -> Result<String> {
		let row = new_token(user_id, token_hash, expires_at);
		let key = token_key(token_hash);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<RefreshTokenRow>(&key).await?.is_some() {
				return Err(Error::Duplicate("refresh token"));
			}
			stage(&mut tx, &row);
			if tx.commit().await? {
				return Ok(row.id);
			}
		}
	}

	/// Atomically exchanges `old_hash` for `new_hash` (refresh-token rotation).
	pub async fn rotate_refresh_token(
		&self,
		old_hash: &str,
		new_hash: &str,
		new_expires_at: DateTime<Utc>,
	) -> Result<Rotation> {
		let old_key = token_key(old_hash);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut old) = tx.get::<RefreshTokenRow>(&old_key).await? else {
				return Ok(Rotation::Invalid);
			};
			let now = super::now();
			if old.revoked_at.is_some() {
				for index in tx
					.keys(&prefix("db/rtoken.user", &[old.user_id.as_str()]))
					.await?
				{
					let Some(mut token) = tx
						.get::<RefreshTokenRow>(&token_key(&last_segment(&index)))
						.await?
					else {
						continue;
					};
					if token.revoked_at.is_none() && token.expires_at > now {
						token.revoked_at = Some(now);
						stage(&mut tx, &token);
					}
				}
				if tx.commit().await? {
					return Ok(Rotation::Reused);
				}
				continue;
			}
			if old.expires_at <= now {
				return Ok(Rotation::Invalid);
			}
			if tx
				.get::<RefreshTokenRow>(&token_key(new_hash))
				.await?
				.is_some()
			{
				return Err(Error::Duplicate("refresh token"));
			}
			old.revoked_at = Some(now);
			stage(&mut tx, &old);
			stage(&mut tx, &new_token(&old.user_id, new_hash, new_expires_at));
			if tx.commit().await? {
				return Ok(Rotation::Rotated { user_id: old.user_id });
			}
		}
	}

	/// Revokes a refresh token; `false` when unknown or already revoked.
	pub async fn revoke_refresh_token(&self, token_hash: &str) -> Result<bool> {
		let key = token_key(token_hash);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<RefreshTokenRow>(&key).await? else {
				return Ok(false);
			};
			if row.revoked_at.is_some() {
				return Ok(false);
			}
			row.revoked_at = Some(super::now());
			stage(&mut tx, &row);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::store::testing::TestStore;

	#[tokio::test]
	async fn rotation_revokes_old_and_reuse_revokes_family() {
		let t = TestStore::start().await;
		let store = &t.store;
		let expires = Utc::now() + chrono::Duration::days(1);
		store
			.insert_refresh_token("u1", "a", expires)
			.await
			.unwrap();
		store
			.insert_refresh_token("u1", "other", expires)
			.await
			.unwrap();

		let rotated = store.rotate_refresh_token("a", "b", expires).await.unwrap();
		assert_eq!(rotated, Rotation::Rotated { user_id: "u1".into() });
		let unknown = store
			.rotate_refresh_token("missing", "x", expires)
			.await
			.unwrap();
		assert_eq!(unknown, Rotation::Invalid);

		assert_eq!(store.rotate_refresh_token("a", "c", expires).await.unwrap(), Rotation::Reused);
		assert!(!store.revoke_refresh_token("b").await.unwrap(), "reuse revoked the family");
		assert!(!store.revoke_refresh_token("other").await.unwrap(), "reuse revoked the family");
		assert_eq!(store.rotate_refresh_token("b", "d", expires).await.unwrap(), Rotation::Reused);
		t.finish().await;
	}

	#[tokio::test]
	async fn concurrent_rotations_have_one_winner() {
		let t = TestStore::start().await;
		let store = &t.store;
		let expires = Utc::now() + chrono::Duration::days(1);
		store
			.insert_refresh_token("u1", "a", expires)
			.await
			.unwrap();

		let (first, second) = tokio::join!(
			store.rotate_refresh_token("a", "b", expires),
			store.rotate_refresh_token("a", "c", expires),
		);
		let mut outcomes = [first.unwrap(), second.unwrap()];
		outcomes.sort_by_key(|o| matches!(o, Rotation::Reused));
		assert_eq!(outcomes, [Rotation::Rotated { user_id: "u1".into() }, Rotation::Reused]);
		t.finish().await;
	}

	#[tokio::test]
	async fn expired_tokens_are_invalid() {
		let t = TestStore::start().await;
		let store = &t.store;
		let past = Utc::now() - chrono::Duration::minutes(1);
		store.insert_refresh_token("u1", "a", past).await.unwrap();
		let later = Utc::now() + chrono::Duration::days(1);
		assert_eq!(store.rotate_refresh_token("a", "b", later).await.unwrap(), Rotation::Invalid);
		t.finish().await;
	}
}
