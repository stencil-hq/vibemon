//! `volumes`: team volumes and where they are stored (one node, every node
//! with `--shared-volumes`, or the operator object store).

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use super::{Store, retry_deadlocks};

/// A team volume.
#[derive(Clone, Debug, PartialEq, Eq, FromRow)]
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

const VOLUME_COLUMNS: &str = "id, team_id, name, node_id, created_at, object_store";

impl Store {
	/// Inserts a volume row; a duplicate `(team, name)` surfaces as a unique
	/// violation.
	pub async fn insert_volume(
		&self,
		team_id: &str,
		name: &str,
		node_id: Option<&str>,
		object_store: bool,
	) -> sqlx::Result<VolumeRow> {
		let id = uuid::Uuid::new_v4().to_string();
		let id = id.as_str();
		retry_deadlocks(|| async move {
			sqlx::query(
				"INSERT INTO volumes (id, team_id, name, node_id, object_store) VALUES (?, ?, ?, ?, ?)",
			)
			.bind(id)
			.bind(team_id)
			.bind(name)
			.bind(node_id)
			.bind(object_store)
			.execute(self.pool())
			.await
		})
		.await?;
		self
			.volume(team_id, id)
			.await?
			.ok_or(sqlx::Error::RowNotFound)
	}

	/// Volume by id within a team.
	pub async fn volume(&self, team_id: &str, id: &str) -> sqlx::Result<Option<VolumeRow>> {
		let sql = format!("SELECT {VOLUME_COLUMNS} FROM volumes WHERE team_id = ? AND id = ?");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(id)
			.fetch_optional(self.pool())
			.await
	}

	/// Volume by name within a team (sandbox `volumeMounts`).
	pub async fn volume_by_name(
		&self,
		team_id: &str,
		name: &str,
	) -> sqlx::Result<Option<VolumeRow>> {
		let sql = format!("SELECT {VOLUME_COLUMNS} FROM volumes WHERE team_id = ? AND name = ?");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.bind(name)
			.fetch_optional(self.pool())
			.await
	}

	/// A team's volumes, oldest first.
	pub async fn list_volumes(&self, team_id: &str) -> sqlx::Result<Vec<VolumeRow>> {
		let sql =
			format!("SELECT {VOLUME_COLUMNS} FROM volumes WHERE team_id = ? ORDER BY created_at, id");
		sqlx::query_as(sqlx::AssertSqlSafe(sql))
			.bind(team_id)
			.fetch_all(self.pool())
			.await
	}

	/// Deletes a volume row; `false` when unknown.
	pub async fn delete_volume(&self, team_id: &str, id: &str) -> sqlx::Result<bool> {
		let done = retry_deadlocks(|| async move {
			sqlx::query("DELETE FROM volumes WHERE team_id = ? AND id = ?")
				.bind(team_id)
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(done.rows_affected() > 0)
	}
}
