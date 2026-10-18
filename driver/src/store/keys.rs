//! Team API keys (SHA-256 for lookup, AES-GCM ciphertext so `GET /teams`
//! can return the key).
//!
//! Keys:
//! - `db/apikey/<team>/<id>` → [`ApiKeyRow`] (without `created_by_email`, which
//!   reads fill in from the creator's user record).
//! - `db/apikey.hash/<hash>` → `KeyRef` (`team_id`, `id`); unique index over
//!   the key hash, guarded absent on insert and deleted with the key.

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
	Error, Read, Result, Store, Team, User, key, prefix, teams::team_key, users::user_key,
};

/// How stale `last_used_at` must be before a use stamps it again.
///
/// `lastUsed` keeps minute precision without rewriting a key's record on
/// every use: one key shared by many clients makes that record the hottest
/// in the store.
pub const API_KEY_STAMP_INTERVAL: Duration = Duration::from_mins(1);

/// One stored API key (never carries the plaintext).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiKeyRow {
	pub id:               String,
	pub team_id:          String,
	pub key_hash:         String,
	/// `Keys::encrypt` of the full key text.
	#[serde(with = "crate::store::b64")]
	pub key_enc:          Vec<u8>,
	pub mask_prefix:      String,
	pub mask_suffix:      String,
	pub name:             String,
	pub created_by:       Option<String>,
	/// Email of `created_by` (looked up on read, not stored).
	#[serde(default, skip_serializing)]
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

/// Value of a hash index entry: where the key's record lives.
#[derive(Serialize, Deserialize)]
struct KeyRef {
	team_id: String,
	id:      String,
}

fn api_key_key(team_id: &str, id: &str) -> String {
	key("db/apikey", &[team_id, id])
}

fn hash_key(hash: &str) -> String {
	key("db/apikey.hash", &[hash])
}

/// Newest first (`created_at DESC, id DESC`).
fn newest_first(a: &ApiKeyRow, b: &ApiKeyRow) -> std::cmp::Ordering {
	b.created_at
		.cmp(&a.created_at)
		.then_with(|| b.id.cmp(&a.id))
}

/// Fills `created_by_email` of `rows` from their creators' user records.
async fn with_emails(r: &Read<'_>, mut rows: Vec<ApiKeyRow>) -> Result<Vec<ApiKeyRow>> {
	let mut emails: HashMap<String, Option<String>> = HashMap::new();
	for row in &mut rows {
		let Some(user_id) = &row.created_by else {
			continue;
		};
		row.created_by_email = if let Some(email) = emails.get(user_id) {
			email.clone()
		} else {
			let email = r.get::<User>(&user_key(user_id)).await?.map(|u| u.email);
			emails.insert(user_id.clone(), email.clone());
			email
		};
	}
	Ok(rows)
}

impl Store {
	/// Stores a new key and returns its row.
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when the hash is already stored; store failures.
	pub async fn insert_api_key(&self, key: &NewApiKey<'_>) -> Result<ApiKeyRow> {
		let mut row = ApiKeyRow {
			id:               uuid::Uuid::new_v4().to_string(),
			team_id:          key.team_id.to_owned(),
			key_hash:         key.key_hash.to_owned(),
			key_enc:          key.key_enc.to_vec(),
			mask_prefix:      key.mask_prefix.to_owned(),
			mask_suffix:      key.mask_suffix.to_owned(),
			name:             key.name.to_owned(),
			created_by:       key.created_by.map(str::to_owned),
			created_by_email: None,
			created_at:       super::now(),
			last_used_at:     None,
		};
		let index = hash_key(key.key_hash);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<KeyRef>(&index).await?.is_some() {
				return Err(Error::Duplicate("api key"));
			}
			row.created_by_email = match key.created_by {
				Some(user_id) => tx.get::<User>(&user_key(user_id)).await?.map(|u| u.email),
				None => None,
			};
			tx.put(api_key_key(&row.team_id, &row.id), &row);
			tx.put(index.as_str(), &KeyRef { team_id: row.team_id.clone(), id: row.id.clone() });
			if tx.commit().await? {
				return Ok(row);
			}
		}
	}

	/// One key of a team.
	pub async fn api_key(&self, team_id: &str, id: &str) -> Result<Option<ApiKeyRow>> {
		let r = self.read().await?;
		let Some(row) = r.get(&api_key_key(team_id, id)).await? else {
			return Ok(None);
		};
		Ok(with_emails(&r, vec![row]).await?.pop())
	}

	/// All keys of a team, newest first.
	pub async fn list_api_keys(&self, team_id: &str) -> Result<Vec<ApiKeyRow>> {
		let r = self.read().await?;
		let mut rows: Vec<ApiKeyRow> = r.list(&prefix("db/apikey", &[team_id])).await?;
		rows.sort_by(newest_first);
		with_emails(&r, rows).await
	}

	/// The newest key `user_id` created in `team_id` (reused by `GET /teams`).
	pub async fn latest_user_api_key(
		&self,
		team_id: &str,
		user_id: &str,
	) -> Result<Option<ApiKeyRow>> {
		let r = self.read().await?;
		let rows: Vec<ApiKeyRow> = r.list(&prefix("db/apikey", &[team_id])).await?;
		let Some(latest) = rows
			.into_iter()
			.filter(|row| row.created_by.as_deref() == Some(user_id))
			.min_by(newest_first)
		else {
			return Ok(None);
		};
		Ok(with_emails(&r, vec![latest]).await?.pop())
	}

	/// Resolves a key hash to `(key id, team)`.
	pub async fn team_for_api_key(&self, key_hash: &str) -> Result<Option<(String, Team)>> {
		let r = self.read().await?;
		let Some(found) = r.get::<KeyRef>(&hash_key(key_hash)).await? else {
			return Ok(None);
		};
		Ok(r
			.get::<Team>(&team_key(&found.team_id))
			.await?
			.map(|team| (found.id, team)))
	}

	/// Renames a key; `false` when it does not belong to the team.
	pub async fn rename_api_key(&self, team_id: &str, id: &str, name: &str) -> Result<bool> {
		let key = api_key_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<ApiKeyRow>(&key).await? else {
				return Ok(false);
			};
			name.clone_into(&mut row.name);
			tx.put(key.as_str(), &row);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Deletes a key and returns its hash (for cache invalidation).
	pub async fn delete_api_key(&self, team_id: &str, id: &str) -> Result<Option<String>> {
		let key = api_key_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(row) = tx.get::<ApiKeyRow>(&key).await? else {
				return Ok(None);
			};
			tx.delete(key.as_str());
			tx.delete(hash_key(&row.key_hash));
			if tx.commit().await? {
				return Ok(Some(row.key_hash));
			}
		}
	}

	/// Stamps `last_used_at = now` on key `id` of `team_id` unless it was
	/// stamped less than [`API_KEY_STAMP_INTERVAL`] ago; `false` when skipped
	/// (or the key is gone).
	pub async fn touch_api_key(&self, team_id: &str, id: &str) -> Result<bool> {
		let key = api_key_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<ApiKeyRow>(&key).await? else {
				return Ok(false);
			};
			let now = super::now();
			if row
				.last_used_at
				.is_some_and(|at| at >= now - API_KEY_STAMP_INTERVAL)
			{
				return Ok(false);
			}
			row.last_used_at = Some(now);
			tx.put(key.as_str(), &row);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{state::TeamLimits, store::testing::TestStore};

	#[tokio::test]
	async fn touch_skips_recently_stamped_keys() {
		let t = TestStore::start().await;
		let store = &t.store;
		let team = store
			.create_team("Acme", TeamLimits::default())
			.await
			.unwrap();
		let key = store
			.insert_api_key(&NewApiKey {
				team_id:     &team.id,
				name:        "k",
				created_by:  None,
				key_hash:    "hash",
				key_enc:     b"enc",
				mask_prefix: "e2b_",
				mask_suffix: "abcd",
			})
			.await
			.unwrap();
		let last_used = async || {
			store
				.api_key(&team.id, &key.id)
				.await
				.unwrap()
				.unwrap()
				.last_used_at
		};

		assert!(store.touch_api_key(&team.id, &key.id).await.unwrap());
		let first = last_used().await.expect("the first use is stamped");
		tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		assert!(!store.touch_api_key(&team.id, &key.id).await.unwrap());
		assert_eq!(last_used().await, Some(first), "a recent stamp is not rewritten");

		let mut tx = store.tx().await.unwrap();
		let stale = api_key_key(&team.id, &key.id);
		let mut row = tx.get::<ApiKeyRow>(&stale).await.unwrap().unwrap();
		row.last_used_at = Some(first - chrono::Duration::hours(1));
		tx.put(stale, &row);
		assert!(tx.commit().await.unwrap());
		assert!(store.touch_api_key(&team.id, &key.id).await.unwrap());
		assert!(last_used().await.unwrap() > first, "an old stamp is refreshed");
		t.finish().await;
	}

	#[tokio::test]
	async fn keys_resolve_list_newest_first_and_delete() {
		let t = TestStore::start().await;
		let store = &t.store;
		let team = store
			.create_team("Acme", TeamLimits::default())
			.await
			.unwrap();
		let user = store.create_user("dev@example.com", "hash").await.unwrap();
		let new = |key_hash, created_by| NewApiKey {
			team_id: &team.id,
			name: "k",
			created_by,
			key_hash,
			key_enc: b"enc",
			mask_prefix: "e2b_",
			mask_suffix: "abcd",
		};
		let older = store.insert_api_key(&new("h1", None)).await.unwrap();
		tokio::time::sleep(std::time::Duration::from_millis(3)).await;
		let newer = store
			.insert_api_key(&new("h2", Some(user.id.as_str())))
			.await
			.unwrap();
		assert_eq!(newer.created_by_email.as_deref(), Some("dev@example.com"));
		assert!(matches!(store.insert_api_key(&new("h1", None)).await, Err(Error::Duplicate(_))));

		let ids: Vec<_> = store
			.list_api_keys(&team.id)
			.await
			.unwrap()
			.into_iter()
			.map(|k| k.id)
			.collect();
		assert_eq!(ids, [newer.id.clone(), older.id.clone()]);
		let latest = store.latest_user_api_key(&team.id, &user.id).await.unwrap();
		assert_eq!(latest.map(|k| k.id), Some(newer.id.clone()));
		let (id, found) = store.team_for_api_key("h2").await.unwrap().unwrap();
		assert_eq!((id, found), (newer.id.clone(), team.clone()));

		assert_eq!(
			store
				.delete_api_key(&team.id, &newer.id)
				.await
				.unwrap()
				.as_deref(),
			Some("h2")
		);
		assert!(store.team_for_api_key("h2").await.unwrap().is_none());
		assert!(
			store
				.latest_user_api_key(&team.id, &user.id)
				.await
				.unwrap()
				.is_none()
		);
		t.finish().await;
	}
}
