//! Versioned, encrypted team secrets.
//!
//! Keys:
//! - `db/secret/<team>/<id>` → [`SecretRow`] (metadata, never the value).
//! - `db/secret.name/<team>/<lower-cased name>` → id: unique name per team
//!   (guarded read on create).
//! - `db/secret.version/<team>/<id>/<seq(version)>` → [`SecretVersion`]: the
//!   ciphertext of each version; written with the row, which every version
//!   write also updates (its guard serializes version numbers).
//!
//! Lists, counts and the cursor page are computed over the team's rows
//! (small per-team sets).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Cursor, Error, Read, Result, Store, key, micros, now, prefix, seq};

/// Secret metadata (never the value).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SecretRow {
	/// `sec_` + 32 hex.
	pub id:              String,
	pub team_id:         String,
	/// Lower-cased name, unique per team.
	pub name:            String,
	pub current_version: i64,
	pub metadata:        BTreeMap<String, String>,
	pub created_at:      DateTime<Utc>,
	pub updated_at:      DateTime<Utc>,
}

impl SecretRow {
	/// Pagination cursor positioned after this row.
	pub fn cursor(&self) -> Cursor {
		Cursor { created_at_us: micros(self.created_at), id: self.id.clone() }
	}
}

/// One stored version of a secret.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SecretVersion {
	/// `Keys::encrypt` of the value.
	#[serde(with = "crate::store::b64")]
	value_enc:  Vec<u8>,
	created_at: DateTime<Utc>,
}

/// Prefix of secret identifiers (names may not use it).
pub const SECRET_ID_PREFIX: &str = "sec_";

fn secret_key(team_id: &str, id: &str) -> String {
	key("db/secret", &[team_id, id])
}

fn name_key(team_id: &str, name: &str) -> String {
	key("db/secret.name", &[team_id, &name.to_lowercase()])
}

/// `None` for versions that cannot exist (below 1).
fn version_key(team_id: &str, id: &str, version: i64) -> Option<String> {
	let version = u64::try_from(version).ok().filter(|v| *v > 0)?;
	Some(key("db/secret.version", &[team_id, id, &seq(version)]))
}

/// Secret by id (`sec_…`) or by (case-insensitive) name.
async fn find(r: &Read<'_>, team_id: &str, id_or_name: &str) -> Result<Option<SecretRow>> {
	if id_or_name.starts_with(SECRET_ID_PREFIX) {
		return r.get(&secret_key(team_id, id_or_name)).await;
	}
	let Some(id) = r.get::<String>(&name_key(team_id, id_or_name)).await? else {
		return Ok(None);
	};
	r.get(&secret_key(team_id, &id)).await
}

impl Store {
	/// Creates a secret at version 1.
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when the team already has a secret of that name
	/// (case-insensitive); store failures otherwise.
	pub async fn create_secret(
		&self,
		team_id: &str,
		name: &str,
		value_enc: &[u8],
		metadata: &BTreeMap<String, String>,
	) -> Result<SecretRow> {
		let at = now();
		let row = SecretRow {
			id:              format!("{SECRET_ID_PREFIX}{}", uuid::Uuid::new_v4().simple()),
			team_id:         team_id.to_owned(),
			name:            name.to_owned(),
			current_version: 1,
			metadata:        metadata.clone(),
			created_at:      at,
			updated_at:      at,
		};
		let version = SecretVersion { value_enc: value_enc.to_vec(), created_at: at };
		let index = name_key(team_id, name);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<String>(&index).await?.is_some() {
				return Err(Error::Duplicate("secret"));
			}
			tx.put(secret_key(team_id, &row.id), &row);
			tx.put(index.clone(), &row.id);
			if let Some(key) = version_key(team_id, &row.id, 1) {
				tx.put(key, &version);
			}
			if tx.commit().await? {
				return Ok(row);
			}
		}
	}

	/// Secret by id (`sec_…`) or by (case-insensitive) name.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn secret(&self, team_id: &str, id_or_name: &str) -> Result<Option<SecretRow>> {
		let r = self.read().await?;
		find(&r, team_id, id_or_name).await
	}

	/// One page of a team's secrets, newest first, strictly after `after`.
	/// Fetches up to `limit` rows.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn list_secrets(
		&self,
		team_id: &str,
		after: Option<&Cursor>,
		limit: u32,
	) -> Result<Vec<SecretRow>> {
		let r = self.read().await?;
		let mut rows: Vec<SecretRow> = r.list(&prefix("db/secret", &[team_id])).await?;
		rows.sort_by(|a, b| (b.created_at, &b.id).cmp(&(a.created_at, &a.id)));
		let after = after.map(|c| (c.created_at_us, c.id.as_str()));
		Ok(rows
			.into_iter()
			.filter(|row| after.is_none_or(|after| (micros(row.created_at), row.id.as_str()) < after))
			.take(limit as usize)
			.collect())
	}

	/// Number of secrets a team holds.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn count_secrets(&self, team_id: &str) -> Result<i64> {
		let r = self.read().await?;
		let keys = r.keys(&prefix("db/secret", &[team_id])).await?;
		Ok(i64::try_from(keys.len()).unwrap_or(i64::MAX))
	}

	/// Appends a new version and makes it current; `metadata` replaces the
	/// stored map when given. `None` when the secret does not exist.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn update_secret(
		&self,
		team_id: &str,
		id: &str,
		value_enc: &[u8],
		metadata: Option<&BTreeMap<String, String>>,
	) -> Result<Option<SecretRow>> {
		let row_key = secret_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<SecretRow>(&row_key).await? else {
				return Ok(None);
			};
			let at = now();
			row.current_version += 1;
			if let Some(metadata) = metadata {
				row.metadata = metadata.clone();
			}
			row.updated_at = at;
			if let Some(key) = version_key(team_id, id, row.current_version) {
				tx.put(key, &SecretVersion { value_enc: value_enc.to_vec(), created_at: at });
			}
			tx.put(row_key.clone(), &row);
			if tx.commit().await? {
				return Ok(Some(row));
			}
		}
	}

	/// Deletes a secret with all versions; `false` when unknown.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delete_secret(&self, team_id: &str, id: &str) -> Result<bool> {
		let row_key = secret_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(row) = tx.get::<SecretRow>(&row_key).await? else {
				return Ok(false);
			};
			tx.delete(row_key.clone());
			tx.delete(name_key(team_id, &row.name));
			tx.delete_prefix(prefix("db/secret.version", &[team_id, id]));
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Encrypted value of a secret version (current when `version` is
	/// `None`), looked up by id or name. Used when resolving secret markers
	/// for sandbox egress.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn secret_value(
		&self,
		team_id: &str,
		id_or_name: &str,
		version: Option<i64>,
	) -> Result<Option<Vec<u8>>> {
		let r = self.read().await?;
		let Some(secret) = find(&r, team_id, id_or_name).await? else {
			return Ok(None);
		};
		let Some(key) = version_key(team_id, &secret.id, version.unwrap_or(secret.current_version))
		else {
			return Ok(None);
		};
		Ok(r.get::<SecretVersion>(&key).await?.map(|v| v.value_enc))
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use crate::store::{Error, testing::TestStore};

	#[tokio::test]
	async fn names_are_unique_per_team_and_versions_are_kept() {
		let db = TestStore::start().await;
		let store = &db.store;
		let empty = BTreeMap::new();
		let created = store
			.create_secret("t1", "key", b"v1", &empty)
			.await
			.unwrap();
		assert!(matches!(
			store.create_secret("t1", "KEY", b"x", &empty).await,
			Err(Error::Duplicate(_))
		));
		store
			.create_secret("t2", "key", b"other", &empty)
			.await
			.expect("names are unique per team only");
		assert_eq!(store.secret("t1", "Key").await.unwrap().unwrap().id, created.id);
		assert!(store.secret("t2", &created.id).await.unwrap().is_none());

		let meta = BTreeMap::from([("env".to_owned(), "prod".to_owned())]);
		let v2 = store
			.update_secret("t1", &created.id, b"v2", Some(&meta))
			.await
			.unwrap()
			.unwrap();
		assert_eq!((v2.current_version, &v2.metadata), (2, &meta));
		assert_eq!(
			store
				.secret_value("t1", "key", None)
				.await
				.unwrap()
				.unwrap(),
			b"v2"
		);
		assert_eq!(
			store
				.secret_value("t1", &created.id, Some(1))
				.await
				.unwrap()
				.unwrap(),
			b"v1"
		);
		assert!(
			store
				.secret_value("t1", "key", Some(0))
				.await
				.unwrap()
				.is_none()
		);
		assert_eq!(store.count_secrets("t1").await.unwrap(), 1);

		assert!(store.delete_secret("t1", &created.id).await.unwrap());
		assert!(!store.delete_secret("t1", &created.id).await.unwrap());
		assert!(
			store
				.secret_value("t1", &created.id, Some(1))
				.await
				.unwrap()
				.is_none()
		);
		store
			.create_secret("t1", "key", b"again", &empty)
			.await
			.expect("a deleted name is free again");
		db.finish().await;
	}
}
