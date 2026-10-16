//! `sandboxes` table (ARCHITECTURE.md §7): the durable record of every
//! sandbox. Written by [`crate::lifecycle`]; read by the sandbox API and the
//! reconciler.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{MySql, QueryBuilder, types::Json};
use vmon_node::api::VolumeMount;

use super::{NewSandboxEvent, Store, events::insert_event, retry_deadlocks};
use crate::lifecycle::network::NetworkConfig;

/// Durable sandbox state (`sandboxes.state`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxState {
	Running,
	Paused,
	Killed,
}

impl SandboxState {
	/// Column value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Running => "running",
			Self::Paused => "paused",
			Self::Killed => "killed",
		}
	}

	/// Parses a column value; unknown values read as `Killed` (never served).
	pub fn parse(raw: &str) -> Self {
		match raw {
			"running" => Self::Running,
			"paused" => Self::Paused,
			_ => Self::Killed,
		}
	}
}

/// Lifecycle settings stored in the `lifecycle` JSON column.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleExtra {
	/// Timeout applied when ingress traffic auto-resumes the sandbox (seconds).
	pub auto_resume_timeout_s: u64,
}

/// Non-secret part of a `vmonS3Mounts` item (`sandboxes.s3_mounts`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3MountRecord {
	pub path:               String,
	pub uri:                String,
	pub mode:               vmon_node::api::S3MountMode,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub endpoint:           Option<String>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub region:             Option<String>,
	/// Team secret holding the credentials, re-read on every launch.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub credentials_secret: Option<String>,
}

/// One `sandboxes` row.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct SandboxRow {
	pub id:                    String,
	pub team_id:               String,
	pub template_id:           String,
	pub build_id:              String,
	pub alias:                 Option<String>,
	pub node_id:               String,
	pub state:                 String,
	pub execution_id:          String,
	pub vcpus:                 u32,
	pub memory_mb:             u64,
	pub disk_mb:               u64,
	pub metadata:              Json<BTreeMap<String, String>>,
	pub env_enc:               Option<Vec<u8>>,
	pub auto_pause:            bool,
	pub auto_pause_memory:     bool,
	pub auto_resume:           bool,
	pub allow_internet_access: Option<bool>,
	pub network:               Json<NetworkConfig>,
	pub egress_enc:            Vec<u8>,
	pub volume_mounts:         Json<Vec<VolumeMount>>,
	pub lifecycle:             Json<LifecycleExtra>,
	pub envd_version:          String,
	pub started_at:            DateTime<Utc>,
	pub end_at:                DateTime<Utc>,
	pub paused_at:             Option<DateTime<Utc>>,
	pub ended_at:              Option<DateTime<Utc>>,
	pub snapshot_build_id:     Option<String>,
	/// `vmonS3Mounts` without credentials.
	pub s3_mounts:             Option<Json<Vec<S3MountRecord>>>,
	/// Encrypted inline credentials of `s3_mounts`, by index.
	pub s3_mounts_enc:         Option<Vec<u8>>,
}

impl SandboxRow {
	/// Typed `state`.
	pub fn state(&self) -> SandboxState {
		SandboxState::parse(&self.state)
	}
}

/// Insert payload for [`Store::insert_sandbox`] (state is always `running`).
#[derive(Clone, Debug)]
pub struct NewSandboxRow<'a> {
	pub id:                    &'a str,
	pub team_id:               &'a str,
	pub template_id:           &'a str,
	pub build_id:              &'a str,
	pub alias:                 Option<&'a str>,
	pub node_id:               &'a str,
	pub execution_id:          &'a str,
	pub vcpus:                 u32,
	pub memory_mb:             u64,
	pub disk_mb:               u64,
	pub metadata:              &'a BTreeMap<String, String>,
	pub env_enc:               Option<Vec<u8>>,
	pub auto_pause:            bool,
	pub auto_pause_memory:     bool,
	pub auto_resume:           bool,
	pub allow_internet_access: Option<bool>,
	pub network:               &'a NetworkConfig,
	pub egress_enc:            Vec<u8>,
	pub volume_mounts:         &'a [VolumeMount],
	pub lifecycle:             LifecycleExtra,
	pub envd_version:          &'a str,
	pub started_at:            DateTime<Utc>,
	pub end_at:                DateTime<Utc>,
	pub s3_mounts:             &'a [S3MountRecord],
	pub s3_mounts_enc:         Option<Vec<u8>>,
}

/// Sort direction of sandbox listings (by `started_at`, then `id`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ListOrder {
	Asc,
	#[default]
	Desc,
}

/// Filters for [`Store::list_sandboxes`] / [`Store::count_sandboxes`].
#[derive(Clone, Debug, Default)]
pub struct SandboxFilter<'a> {
	/// `None` lists every team (admin listings only).
	pub team_id:       Option<&'a str>,
	/// Non-empty subset of `running`/`paused`.
	pub states:        Vec<SandboxState>,
	/// Every pair must be present in `metadata`.
	pub metadata:      BTreeMap<String, String>,
	pub started_after: Option<DateTime<Utc>>,
	/// Template ID or alias.
	pub template:      Option<&'a str>,
}

/// Durable running/paused counts of one team ([`Store::live_counts_by_team`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct LiveCounts {
	pub running: u64,
	pub paused:  u64,
}

/// Input of [`Store::mark_sandbox_resumed`]: the new execution of a sandbox.
#[derive(Clone, Copy, Debug)]
pub struct ResumedExecution<'a> {
	pub id:           &'a str,
	pub node_id:      &'a str,
	pub execution_id: &'a str,
	pub started_at:   DateTime<Utc>,
	pub end_at:       DateTime<Utc>,
}

/// Inserts `event` and commits when the guarded update `applied`; otherwise
/// rolls back so neither the state change nor the event is written.
async fn commit_with_event(
	mut tx: sqlx::Transaction<'_, MySql>,
	applied: bool,
	event: &NewSandboxEvent<'_>,
) -> sqlx::Result<bool> {
	if !applied {
		tx.rollback().await?;
		return Ok(false);
	}
	insert_event(&mut *tx, event).await?;
	tx.commit().await?;
	Ok(true)
}

/// Keyset position: rows strictly after `(started_at, id)` in the list order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListAfter {
	pub started_at: DateTime<Utc>,
	pub id:         String,
}

const COLUMNS: &str = "id, team_id, template_id, build_id, alias, node_id, state, execution_id, \
                       vcpus, memory_mb, disk_mb, metadata, env_enc, auto_pause, \
                       auto_pause_memory, auto_resume, allow_internet_access, network, \
                       egress_enc, volume_mounts, lifecycle, envd_version, started_at, end_at, \
                       paused_at, ended_at, snapshot_build_id, s3_mounts, s3_mounts_enc";

impl Store {
	/// Records a freshly created sandbox (state `running`) together with its
	/// `created` event, in one transaction.
	pub async fn insert_sandbox(
		&self,
		row: &NewSandboxRow<'_>,
		event: &NewSandboxEvent<'_>,
	) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			sqlx::query(
				"INSERT INTO sandboxes (id, team_id, template_id, build_id, alias, node_id, state, \
				 execution_id, vcpus, memory_mb, disk_mb, metadata, env_enc, auto_pause, \
				 auto_pause_memory, auto_resume, allow_internet_access, network, egress_enc, \
				 volume_mounts, lifecycle, envd_version, started_at, end_at, s3_mounts, \
				 s3_mounts_enc) VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
				 ?, ?, ?, ?, ?, ?, ?, ?)",
			)
			.bind(row.id)
			.bind(row.team_id)
			.bind(row.template_id)
			.bind(row.build_id)
			.bind(row.alias)
			.bind(row.node_id)
			.bind(row.execution_id)
			.bind(row.vcpus)
			.bind(row.memory_mb)
			.bind(row.disk_mb)
			.bind(Json(row.metadata))
			.bind(row.env_enc.as_deref())
			.bind(row.auto_pause)
			.bind(row.auto_pause_memory)
			.bind(row.auto_resume)
			.bind(row.allow_internet_access)
			.bind(Json(row.network))
			.bind(row.egress_enc.as_slice())
			.bind(Json(row.volume_mounts))
			.bind(Json(row.lifecycle))
			.bind(row.envd_version)
			.bind(row.started_at)
			.bind(row.end_at)
			.bind((!row.s3_mounts.is_empty()).then_some(Json(row.s3_mounts)))
			.bind(row.s3_mounts_enc.as_deref())
			.execute(&mut *tx)
			.await?;
			insert_event(&mut *tx, event).await?;
			tx.commit().await
		})
		.await
	}

	/// Row by sandbox ID regardless of team or state.
	pub async fn sandbox(&self, id: &str) -> sqlx::Result<Option<SandboxRow>> {
		sqlx::query_as::<_, SandboxRow>(sqlx::AssertSqlSafe(format!(
			"SELECT {COLUMNS} FROM sandboxes WHERE id = ?"
		)))
		.bind(id)
		.fetch_optional(self.pool())
		.await
	}

	/// Paused → running after a resume (new execution, node, and lifetime
	/// window) plus `event`, atomically; `false` (nothing written) when the
	/// row was not paused.
	pub async fn mark_sandbox_resumed(
		&self,
		resumed: &ResumedExecution<'_>,
		event: &NewSandboxEvent<'_>,
	) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let done = sqlx::query(
				"UPDATE sandboxes SET state = 'running', node_id = ?, execution_id = ?, started_at = \
				 ?, end_at = ?, paused_at = NULL WHERE id = ? AND state = 'paused'",
			)
			.bind(resumed.node_id)
			.bind(resumed.execution_id)
			.bind(resumed.started_at)
			.bind(resumed.end_at)
			.bind(resumed.id)
			.execute(&mut *tx)
			.await?;
			commit_with_event(tx, done.rows_affected() == 1, event).await
		})
		.await
	}

	/// Running → paused plus `event`, atomically; `false` (nothing written)
	/// when the row was not running.
	pub async fn mark_sandbox_paused(
		&self,
		id: &str,
		paused_at: DateTime<Utc>,
		event: &NewSandboxEvent<'_>,
	) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let done = sqlx::query(
				"UPDATE sandboxes SET state = 'paused', paused_at = ? WHERE id = ? AND state = \
				 'running'",
			)
			.bind(paused_at)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			commit_with_event(tx, done.rows_affected() == 1, event).await
		})
		.await
	}

	/// Running/paused → killed plus `event`, atomically; `false` (nothing
	/// written) when the row was already killed or missing.
	pub async fn mark_sandbox_killed(
		&self,
		id: &str,
		ended_at: DateTime<Utc>,
		event: &NewSandboxEvent<'_>,
	) -> sqlx::Result<bool> {
		retry_deadlocks(|| async move {
			let mut tx = self.pool().begin().await?;
			let done = sqlx::query(
				"UPDATE sandboxes SET state = 'killed', ended_at = ? WHERE id = ? AND state <> \
				 'killed'",
			)
			.bind(ended_at)
			.bind(id)
			.execute(&mut *tx)
			.await?;
			commit_with_event(tx, done.rows_affected() == 1, event).await
		})
		.await
	}

	/// New expiry of a running sandbox.
	pub async fn set_sandbox_end_at(&self, id: &str, end_at: DateTime<Utc>) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query("UPDATE sandboxes SET end_at = ? WHERE id = ?")
				.bind(end_at)
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(())
	}

	/// Replaced egress configuration (`PUT /sandboxes/{id}/network`).
	pub async fn set_sandbox_network(
		&self,
		id: &str,
		network: &NetworkConfig,
		egress_enc: &[u8],
		allow_internet_access: Option<bool>,
	) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query(
				"UPDATE sandboxes SET network = ?, egress_enc = ?, allow_internet_access = ? WHERE id \
				 = ?",
			)
			.bind(Json(network))
			.bind(egress_enc)
			.bind(allow_internet_access)
			.bind(id)
			.execute(self.pool())
			.await
		})
		.await?;
		Ok(())
	}

	/// Remembers the latest snapshot build captured from a sandbox.
	pub async fn set_sandbox_snapshot_build(&self, id: &str, build_id: &str) -> sqlx::Result<()> {
		retry_deadlocks(|| async move {
			sqlx::query("UPDATE sandboxes SET snapshot_build_id = ? WHERE id = ?")
				.bind(build_id)
				.bind(id)
				.execute(self.pool())
				.await
		})
		.await?;
		Ok(())
	}

	/// One page of sandboxes. Returns up to `limit` rows after `after`.
	pub async fn list_sandboxes(
		&self,
		filter: &SandboxFilter<'_>,
		order: ListOrder,
		after: Option<&ListAfter>,
		limit: u32,
	) -> sqlx::Result<Vec<SandboxRow>> {
		let mut query = QueryBuilder::<MySql>::new(format!("SELECT {COLUMNS} FROM sandboxes"));
		push_filter(&mut query, filter);
		let (cmp, dir) = match order {
			ListOrder::Asc => (">", "ASC"),
			ListOrder::Desc => ("<", "DESC"),
		};
		if let Some(after) = after {
			query.push(format!(" AND (started_at {cmp} "));
			query.push_bind(after.started_at);
			query.push(" OR (started_at = ");
			query.push_bind(after.started_at);
			query.push(format!(" AND id {cmp} "));
			query.push_bind(after.id.clone());
			query.push("))");
		}
		query.push(format!(" ORDER BY started_at {dir}, id {dir} LIMIT "));
		query.push_bind(limit);
		query
			.build_query_as::<SandboxRow>()
			.fetch_all(self.pool())
			.await
	}

	/// Number of rows matching `filter` (pagination-independent totals).
	pub async fn count_sandboxes(&self, filter: &SandboxFilter<'_>) -> sqlx::Result<u64> {
		let mut query = QueryBuilder::<MySql>::new("SELECT COUNT(*) FROM sandboxes");
		push_filter(&mut query, filter);
		let count: i64 = query.build_query_scalar().fetch_one(self.pool()).await?;
		Ok(u64::try_from(count).unwrap_or_default())
	}

	/// IDs and states of a team's running and paused sandboxes (admin kill).
	pub async fn team_live_sandboxes(&self, team_id: &str) -> sqlx::Result<Vec<(String, String)>> {
		sqlx::query_as(
			"SELECT id, state FROM sandboxes WHERE team_id = ? AND state IN ('running', 'paused')",
		)
		.bind(team_id)
		.fetch_all(self.pool())
		.await
	}

	/// Running/paused row counts per team (one grouped query); teams without
	/// live sandboxes are absent.
	pub async fn live_counts_by_team(&self) -> sqlx::Result<HashMap<String, LiveCounts>> {
		let rows: Vec<(String, String, i64)> = sqlx::query_as(
			"SELECT team_id, state, COUNT(*) FROM sandboxes WHERE state IN ('running', 'paused') \
			 GROUP BY team_id, state",
		)
		.fetch_all(self.pool())
		.await?;
		let mut counts: HashMap<String, LiveCounts> = HashMap::new();
		for (team_id, state, n) in rows {
			let entry = counts.entry(team_id).or_default();
			let n = u64::try_from(n).unwrap_or_default();
			match SandboxState::parse(&state) {
				SandboxState::Running => entry.running = n,
				SandboxState::Paused => entry.paused = n,
				SandboxState::Killed => {},
			}
		}
		Ok(counts)
	}

	/// `(id, state, node_id)` of every running or paused sandbox (reconciler).
	pub async fn live_sandboxes(&self) -> sqlx::Result<Vec<(String, String, String)>> {
		sqlx::query_as(
			"SELECT id, state, node_id FROM sandboxes WHERE state IN ('running', 'paused')",
		)
		.fetch_all(self.pool())
		.await
	}
}

fn push_filter(query: &mut QueryBuilder<MySql>, filter: &SandboxFilter<'_>) {
	query.push(" WHERE state IN (");
	let mut states = query.separated(", ");
	for state in &filter.states {
		states.push_bind(state.as_str());
	}
	if filter.states.is_empty() {
		states.push("'running', 'paused'");
	}
	query.push(")");
	if let Some(team_id) = filter.team_id {
		query.push(" AND team_id = ");
		query.push_bind(team_id);
	}
	if !filter.metadata.is_empty() {
		query.push(" AND JSON_CONTAINS(metadata, CAST(");
		query.push_bind(serde_json::to_string(&filter.metadata).unwrap_or_default());
		query.push(" AS JSON))");
	}
	if let Some(after) = filter.started_after {
		// Inclusive like upstream, on the column's millisecond grid: a client
		// feeding back a sandbox's own `startedAt` must still see it.
		query.push(" AND started_at >= ");
		query.push_bind(after.trunc_subsecs(3));
	}
	if let Some(template) = filter.template {
		query.push(" AND (template_id = ");
		query.push_bind(template);
		query.push(" OR alias = ");
		query.push_bind(template);
		query.push(")");
	}
}

#[cfg(test)]
mod tests {
	//! Gated on `VMON_MYSQL_TEST_URL` (see [`crate::store::testdb`]).

	use chrono::Duration;

	use super::*;
	use crate::{
		state::TeamLimits,
		store::{EventQuery, event_types, testdb::TestDb},
	};

	fn event<'a>(id: &'a str, team: &'a str, event_type: &'static str) -> NewSandboxEvent<'a> {
		NewSandboxEvent {
			team_id: team,
			sandbox_id: id,
			execution_id: "00000000-0000-4000-8000-000000000001",
			template_id: "tpl00000000000000000",
			build_id: "00000000-0000-4000-8000-000000000000",
			event_type,
			data: serde_json::json!({}),
		}
	}

	async fn event_types_of(store: &Store, team: &str, id: &str) -> Vec<String> {
		let query = EventQuery {
			sandbox_id: Some(id.to_owned()),
			limit: 100,
			order_asc: true,
			..EventQuery::default()
		};
		store
			.list_events(team, &query)
			.await
			.unwrap()
			.into_iter()
			.map(|e| e.event_type)
			.collect()
	}

	fn new_row<'a>(
		id: &'a str,
		team: &'a str,
		started_at: DateTime<Utc>,
		metadata: &'a BTreeMap<String, String>,
		network: &'a NetworkConfig,
	) -> NewSandboxRow<'a> {
		NewSandboxRow {
			id,
			team_id: team,
			template_id: "tpl00000000000000000",
			build_id: "00000000-0000-4000-8000-000000000000",
			alias: Some("base"),
			node_id: "00000000000000aa",
			execution_id: "00000000-0000-4000-8000-000000000001",
			vcpus: 2,
			memory_mb: 512,
			disk_mb: 2048,
			metadata,
			env_enc: None,
			auto_pause: true,
			auto_pause_memory: true,
			auto_resume: false,
			allow_internet_access: Some(false),
			network,
			egress_enc: vec![1, 2, 3],
			volume_mounts: &[],
			lifecycle: LifecycleExtra { auto_resume_timeout_s: 300 },
			envd_version: "0.6.4",
			started_at,
			end_at: started_at + Duration::minutes(5),
			s3_mounts: &[],
			s3_mounts_enc: None,
		}
	}

	#[tokio::test]
	async fn s3_mounts_round_trip_without_credentials() {
		let Some(db) = TestDb::create().await else {
			return;
		};
		let store = db.store.clone();
		let team = store
			.create_team("sandbox-s3", TeamLimits::default())
			.await
			.unwrap();
		let net = NetworkConfig::default();
		let meta = BTreeMap::new();
		let id = format!("i{}", &uuid::Uuid::new_v4().simple().to_string()[..20]);
		let records = [S3MountRecord {
			path:               "/data".into(),
			uri:                "s3://bucket/prefix".into(),
			mode:               vmon_node::api::S3MountMode::Writeback,
			endpoint:           Some("http://minio:9000".into()),
			region:             None,
			credentials_secret: Some("bucket-keys".into()),
		}];
		let mut row = new_row(&id, &team.id, Utc::now().trunc_subsecs(3), &meta, &net);
		row.s3_mounts = &records;
		row.s3_mounts_enc = Some(vec![9, 9, 9]);
		store
			.insert_sandbox(&row, &event(&id, &team.id, event_types::CREATED))
			.await
			.unwrap();
		let stored = store.sandbox(&id).await.unwrap().unwrap();
		assert_eq!(stored.s3_mounts.unwrap().0, records);
		assert_eq!(stored.s3_mounts_enc.as_deref(), Some([9u8, 9, 9].as_slice()));
	}

	#[tokio::test]
	async fn sandbox_rows_round_trip_filter_and_paginate() {
		let Some(db) = TestDb::create().await else {
			return;
		};
		let store = db.store.clone();
		let team = store
			.create_team("sandbox-store", TeamLimits::default())
			.await
			.unwrap();
		let base = DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
		let net = NetworkConfig { allow_public_traffic: Some(false), ..NetworkConfig::default() };
		let red = BTreeMap::from([
			("color".to_owned(), "red".to_owned()),
			("k=&".to_owned(), "v w".to_owned()),
		]);
		let blue = BTreeMap::from([("color".to_owned(), "blue".to_owned())]);
		let ids: Vec<String> = (0..4)
			.map(|i| format!("i{}{i}", &uuid::Uuid::new_v4().simple().to_string()[..19]))
			.collect();
		for (i, id) in ids.iter().enumerate() {
			let meta = if i % 2 == 0 { &red } else { &blue };
			let at = base + Duration::seconds(i64::try_from(i).unwrap());
			store
				.insert_sandbox(
					&new_row(id, &team.id, at, meta, &net),
					&event(id, &team.id, event_types::CREATED),
				)
				.await
				.unwrap();
		}

		let row = store.sandbox(&ids[0]).await.unwrap().unwrap();
		assert_eq!(row.state(), SandboxState::Running);
		assert_eq!(row.metadata.0, red);
		assert_eq!(row.network.0, net);
		assert_eq!(row.started_at, base);
		assert_eq!(row.allow_internet_access, Some(false));
		assert_eq!(row.lifecycle.0.auto_resume_timeout_s, 300);

		// State transitions only apply from the expected source state.
		let paused = |id| event(id, &team.id, event_types::PAUSED);
		let killed = |id| event(id, &team.id, event_types::KILLED);
		let resumed_event = |id| event(id, &team.id, event_types::RESUMED);
		assert!(
			store
				.mark_sandbox_paused(&ids[1], base, &paused(&ids[1]))
				.await
				.unwrap()
		);
		assert!(
			!store
				.mark_sandbox_paused(&ids[1], base, &paused(&ids[1]))
				.await
				.unwrap()
		);
		assert!(
			store
				.mark_sandbox_killed(&ids[3], base, &killed(&ids[3]))
				.await
				.unwrap()
		);
		assert!(
			!store
				.mark_sandbox_killed(&ids[3], base, &killed(&ids[3]))
				.await
				.unwrap()
		);
		let resume = |id, started_at| ResumedExecution {
			id,
			node_id: "00000000000000bb",
			execution_id: "e2",
			started_at,
			end_at: started_at,
		};
		assert!(
			!store
				.mark_sandbox_resumed(&resume(&ids[0], base), &resumed_event(&ids[0]))
				.await
				.unwrap()
		);
		let resumed_at = base + Duration::seconds(1);
		assert!(
			store
				.mark_sandbox_resumed(&resume(&ids[1], resumed_at), &resumed_event(&ids[1]))
				.await
				.unwrap()
		);
		assert_eq!(store.sandbox(&ids[1]).await.unwrap().unwrap().node_id, "00000000000000bb");
		assert!(
			store
				.mark_sandbox_paused(&ids[1], base, &paused(&ids[1]))
				.await
				.unwrap()
		);
		// Every applied transition wrote exactly one event; refused ones wrote
		// none.
		assert_eq!(event_types_of(&store, &team.id, &ids[0]).await, [event_types::CREATED]);
		assert_eq!(event_types_of(&store, &team.id, &ids[1]).await, [
			event_types::CREATED,
			event_types::PAUSED,
			event_types::RESUMED,
			event_types::PAUSED
		]);
		assert_eq!(event_types_of(&store, &team.id, &ids[3]).await, [
			event_types::CREATED,
			event_types::KILLED
		]);

		let all = SandboxFilter { team_id: Some(&team.id), ..SandboxFilter::default() };
		let listed = store
			.list_sandboxes(&all, ListOrder::Desc, None, 10)
			.await
			.unwrap();
		let got: Vec<&str> = listed.iter().map(|r| r.id.as_str()).collect();
		assert_eq!(
			got,
			vec![ids[2].as_str(), ids[1].as_str(), ids[0].as_str()],
			"killed rows are never listed"
		);

		// Keyset pagination in both directions.
		let page = store
			.list_sandboxes(&all, ListOrder::Asc, None, 2)
			.await
			.unwrap();
		assert_eq!(page.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![
			ids[0].clone(),
			ids[1].clone()
		]);
		let after = ListAfter { started_at: page[1].started_at, id: page[1].id.clone() };
		let rest = store
			.list_sandboxes(&all, ListOrder::Asc, Some(&after), 2)
			.await
			.unwrap();
		assert_eq!(rest.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![ids[2].clone()]);

		let running = SandboxFilter { states: vec![SandboxState::Running], ..all.clone() };
		assert_eq!(store.count_sandboxes(&running).await.unwrap(), 2);
		let red_only =
			SandboxFilter { metadata: BTreeMap::from([("k=&".into(), "v w".into())]), ..all.clone() };
		let listed = store
			.list_sandboxes(&red_only, ListOrder::Desc, None, 10)
			.await
			.unwrap();
		assert_eq!(listed.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![
			ids[2].clone(),
			ids[0].clone()
		]);
		let at_first =
			SandboxFilter { started_after: Some(base + Duration::microseconds(700)), ..all.clone() };
		assert_eq!(store.count_sandboxes(&at_first).await.unwrap(), 3);
		let after_first =
			SandboxFilter { started_after: Some(base + Duration::milliseconds(1)), ..all.clone() };
		assert_eq!(store.count_sandboxes(&after_first).await.unwrap(), 2);
		let by_alias = SandboxFilter { template: Some("base"), ..all.clone() };
		assert_eq!(store.count_sandboxes(&by_alias).await.unwrap(), 3);
		let by_other = SandboxFilter { template: Some("other"), ..all };
		assert_eq!(store.count_sandboxes(&by_other).await.unwrap(), 0);

		let live = store.team_live_sandboxes(&team.id).await.unwrap();
		assert_eq!(live.len(), 3);
		db.drop_db().await;
	}

	#[tokio::test]
	async fn listing_without_team_spans_teams_and_counts_group_by_team() {
		let Some(db) = TestDb::create().await else {
			return;
		};
		let store = db.store.clone();
		let a = store
			.create_team("list-a", TeamLimits::default())
			.await
			.unwrap();
		let b = store
			.create_team("list-b", TeamLimits::default())
			.await
			.unwrap();
		let base = DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
		let net = NetworkConfig::default();
		let meta = BTreeMap::new();
		let rows = [("ilista0", &a.id), ("ilista1", &a.id), ("ilistb0", &b.id), ("ilistb1", &b.id)];
		for (i, (id, team)) in rows.iter().enumerate() {
			let at = base + Duration::seconds(i64::try_from(i).unwrap());
			store
				.insert_sandbox(
					&new_row(id, team, at, &meta, &net),
					&event(id, team, event_types::CREATED),
				)
				.await
				.unwrap();
		}
		assert!(
			store
				.mark_sandbox_paused("ilista1", base, &event("ilista1", &a.id, event_types::PAUSED))
				.await
				.unwrap()
		);
		assert!(
			store
				.mark_sandbox_killed("ilistb1", base, &event("ilistb1", &b.id, event_types::KILLED))
				.await
				.unwrap()
		);

		let ids = |rows: Vec<SandboxRow>| rows.into_iter().map(|r| r.id).collect::<Vec<_>>();
		let every = SandboxFilter::default();
		let listed = store
			.list_sandboxes(&every, ListOrder::Desc, None, 10)
			.await
			.unwrap();
		assert_eq!(ids(listed), ["ilistb0", "ilista1", "ilista0"]);
		let page = store
			.list_sandboxes(&every, ListOrder::Desc, None, 1)
			.await
			.unwrap();
		let after = ListAfter { started_at: page[0].started_at, id: page[0].id.clone() };
		let rest = store
			.list_sandboxes(&every, ListOrder::Desc, Some(&after), 10)
			.await
			.unwrap();
		assert_eq!(ids(rest), ["ilista1", "ilista0"]);
		let running = SandboxFilter { states: vec![SandboxState::Running], ..every };
		assert_eq!(store.count_sandboxes(&running).await.unwrap(), 2);

		let only_a = SandboxFilter { team_id: Some(&a.id), ..SandboxFilter::default() };
		let listed = store
			.list_sandboxes(&only_a, ListOrder::Desc, None, 10)
			.await
			.unwrap();
		assert_eq!(ids(listed), ["ilista1", "ilista0"]);
		let a_running = SandboxFilter { states: vec![SandboxState::Running], ..only_a };
		assert_eq!(store.count_sandboxes(&a_running).await.unwrap(), 1);

		let counts = store.live_counts_by_team().await.unwrap();
		assert_eq!(
			counts,
			HashMap::from([
				(a.id.clone(), LiveCounts { running: 1, paused: 1 }),
				(b.id.clone(), LiveCounts { running: 1, paused: 0 }),
			])
		);
		db.drop_db().await;
	}

	/// A state change and its event commit together: when the event insert
	/// fails, the transition is rolled back too (no killed row without its
	/// `killed` event, no created row without `created`).
	#[tokio::test]
	async fn state_change_rolls_back_when_its_event_cannot_be_written() {
		let Some(db) = TestDb::create().await else {
			return;
		};
		let store = db.store.clone();
		let team = store
			.create_team("sandbox-atomic", TeamLimits::default())
			.await
			.unwrap();
		let base = DateTime::from_timestamp_millis(Utc::now().timestamp_millis()).unwrap();
		let net = NetworkConfig::default();
		let meta = BTreeMap::new();
		let id = "iatomic0000000000000a";
		// `sandbox_events.type` is VARCHAR(64): an overlong type makes the event
		// insert fail.
		const BROKEN: &str =
			"sandbox.lifecycle.this-event-type-is-longer-than-the-sixty-four-char-column";
		const _: () = assert!(BROKEN.len() > 64);
		let broken = BROKEN;

		let row = new_row(id, &team.id, base, &meta, &net);
		assert!(
			store
				.insert_sandbox(&row, &event(id, &team.id, broken))
				.await
				.is_err()
		);
		assert!(store.sandbox(id).await.unwrap().is_none(), "the create rolled back with its event");

		store
			.insert_sandbox(&row, &event(id, &team.id, event_types::CREATED))
			.await
			.unwrap();
		assert!(
			store
				.mark_sandbox_killed(id, base, &event(id, &team.id, broken))
				.await
				.is_err()
		);
		assert_eq!(store.sandbox(id).await.unwrap().unwrap().state(), SandboxState::Running);
		assert!(
			store
				.mark_sandbox_paused(id, base, &event(id, &team.id, broken))
				.await
				.is_err()
		);
		assert_eq!(store.sandbox(id).await.unwrap().unwrap().state(), SandboxState::Running);
		assert_eq!(event_types_of(&store, &team.id, id).await, [event_types::CREATED]);

		// A retry after the failure applies the transition with its event.
		assert!(
			store
				.mark_sandbox_killed(id, base, &event(id, &team.id, event_types::KILLED))
				.await
				.unwrap()
		);
		assert_eq!(store.sandbox(id).await.unwrap().unwrap().state(), SandboxState::Killed);
		assert_eq!(event_types_of(&store, &team.id, id).await, [
			event_types::CREATED,
			event_types::KILLED
		]);
		db.drop_db().await;
	}
}
