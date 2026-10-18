//! Team volumes and where they are stored (one node, every node with
//! `--shared-volumes`, or the operator object store).
//!
//! Keys:
//! - `db/volume/<team>/<id>` → [`VolumeRow`].
//! - `db/volume.name/<team>/<lower-cased name>` → id: unique name per team
//!   (case-insensitive; guarded read on insert).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Error, Result, Store, key, now, prefix};

/// A team volume.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeRow {
	/// UUID v4.
	pub id:           String,
	pub team_id:      String,
	pub name:         String,
	/// Owning node (`NodeId` as 16 hex); `None` with `--shared-volumes` or
	/// in the object store.
	pub node_id:      Option<String>,
	pub created_at:   DateTime<Utc>,
	/// Stored under `volumes/<id>/` in the operator object store.
	pub object_store: bool,
}

fn volume_key(team_id: &str, id: &str) -> String {
	key("db/volume", &[team_id, id])
}

fn name_key(team_id: &str, name: &str) -> String {
	key("db/volume.name", &[team_id, &name.to_lowercase()])
}

impl Store {
	/// Inserts a volume row.
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when the team already has a volume of that name
	/// (case-insensitive); store failures otherwise.
	pub async fn insert_volume(
		&self,
		team_id: &str,
		name: &str,
		node_id: Option<&str>,
		object_store: bool,
	) -> Result<VolumeRow> {
		let row = VolumeRow {
			id: uuid::Uuid::new_v4().to_string(),
			team_id: team_id.to_owned(),
			name: name.to_owned(),
			node_id: node_id.map(str::to_owned),
			created_at: now(),
			object_store,
		};
		let index = name_key(team_id, name);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<String>(&index).await?.is_some() {
				return Err(Error::Duplicate("volume"));
			}
			tx.put(volume_key(team_id, &row.id), &row);
			tx.put(index.clone(), &row.id);
			if tx.commit().await? {
				return Ok(row);
			}
		}
	}

	/// Volume by id within a team.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn volume(&self, team_id: &str, id: &str) -> Result<Option<VolumeRow>> {
		self.read().await?.get(&volume_key(team_id, id)).await
	}

	/// Volume by (case-insensitive) name within a team (sandbox
	/// `volumeMounts`).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn volume_by_name(&self, team_id: &str, name: &str) -> Result<Option<VolumeRow>> {
		let r = self.read().await?;
		let Some(id) = r.get::<String>(&name_key(team_id, name)).await? else {
			return Ok(None);
		};
		r.get(&volume_key(team_id, &id)).await
	}

	/// A team's volumes, oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn list_volumes(&self, team_id: &str) -> Result<Vec<VolumeRow>> {
		let r = self.read().await?;
		let mut rows: Vec<VolumeRow> = r.list(&prefix("db/volume", &[team_id])).await?;
		rows.sort_by(|a, b| (a.created_at, &a.id).cmp(&(b.created_at, &b.id)));
		Ok(rows)
	}

	/// Deletes a volume row; `false` when unknown.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delete_volume(&self, team_id: &str, id: &str) -> Result<bool> {
		let row_key = volume_key(team_id, id);
		let mut tx = self.tx().await?;
		loop {
			let Some(row) = tx.get::<VolumeRow>(&row_key).await? else {
				return Ok(false);
			};
			tx.delete(row_key.clone());
			tx.delete(name_key(team_id, &row.name));
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::store::{Error, testing::TestStore};

	#[tokio::test]
	async fn names_are_unique_per_team_case_insensitively() {
		let db = TestStore::start().await;
		let store = &db.store;
		let a = store
			.insert_volume("t1", "Data", Some("00000000000000aa"), false)
			.await
			.unwrap();
		assert!(matches!(
			store.insert_volume("t1", "data", None, true).await,
			Err(Error::Duplicate(_))
		));
		// A later millisecond, so "oldest first" does not fall back to ids.
		tokio::time::sleep(std::time::Duration::from_millis(2)).await;
		let b = store.insert_volume("t1", "logs", None, true).await.unwrap();
		store
			.insert_volume("t2", "data", None, false)
			.await
			.expect("names are unique per team only");
		assert_eq!(store.volume_by_name("t1", "DATA").await.unwrap(), Some(a.clone()));
		assert_eq!(store.volume("t1", &b.id).await.unwrap(), Some(b.clone()));
		assert!(store.volume("t2", &a.id).await.unwrap().is_none());
		let listed: Vec<_> = store
			.list_volumes("t1")
			.await
			.unwrap()
			.into_iter()
			.map(|v| v.id)
			.collect();
		assert_eq!(listed, [a.id.clone(), b.id], "oldest first");

		assert!(store.delete_volume("t1", &a.id).await.unwrap());
		assert!(!store.delete_volume("t1", &a.id).await.unwrap());
		assert!(store.volume_by_name("t1", "data").await.unwrap().is_none());
		store
			.insert_volume("t1", "data", None, false)
			.await
			.expect("a deleted name is free again");
		db.finish().await;
	}
}
