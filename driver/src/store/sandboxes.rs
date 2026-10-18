//! Durable sandbox records (ARCHITECTURE.md §7). Written by
//! [`crate::lifecycle`]; read by the sandbox API and the reconciler.
//!
//! Keys:
//! - `db/sandbox/<id>` → [`SandboxRow`] (JSON; ciphertexts base64). Records of
//!   killed sandboxes keep no secrets (`env_enc`, `egress_enc`, `s3_mounts_enc`
//!   emptied) and expire [`KILLED_SANDBOX_TTL`] after the kill.
//! - `db/sandbox.live/<team>/<id>` → empty: index of the team's running and
//!   paused sandboxes (team listings, counts, the reconciler, artifact GC).
//!   Written with the record by [`Store::insert_sandbox`], removed by
//!   [`Store::mark_sandbox_killed`].
//!
//! Guards: [`Store::insert_sandbox`] requires the record to be absent; every
//! state transition is a guarded read of the record checking its current
//! state (the old `WHERE state = …`), and commits the record change together
//! with its event ([`super::events`]) in one transaction.

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use vmon_node::api::VolumeMount;

use super::{
	KILLED_SANDBOX_TTL, NewSandboxEvent, Read, Result, Store, Tx, events::stage_event, key,
	last_segment, prefix,
};
use crate::lifecycle::network::NetworkConfig;

/// Sandbox records.
const SANDBOXES: &str = "db/sandbox";
/// Live (running/paused) index per team.
const LIVE: &str = "db/sandbox.live";

/// Durable sandbox state ([`SandboxRow::state`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxState {
	Running,
	Paused,
	Killed,
}

impl SandboxState {
	/// Stored value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Running => "running",
			Self::Paused => "paused",
			Self::Killed => "killed",
		}
	}

	/// Parses a stored value; unknown values read as `Killed` (never served).
	pub fn parse(raw: &str) -> Self {
		match raw {
			"running" => Self::Running,
			"paused" => Self::Paused,
			_ => Self::Killed,
		}
	}
}

/// Lifecycle settings of a sandbox ([`SandboxRow::lifecycle`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleExtra {
	/// Timeout applied when ingress traffic auto-resumes the sandbox (seconds).
	pub auto_resume_timeout_s: u64,
}

/// Non-secret part of a `vmonS3Mounts` item ([`SandboxRow::s3_mounts`]).
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

/// One sandbox record (`db/sandbox/<id>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SandboxRow {
	pub id:                    String,
	pub team_id:               String,
	pub template_id:           String,
	pub build_id:              String,
	pub alias:                 Option<String>,
	pub node_id:               String,
	/// [`SandboxState::as_str`].
	pub state:                 String,
	pub execution_id:          String,
	pub vcpus:                 u32,
	pub memory_mb:             u64,
	pub disk_mb:               u64,
	pub metadata:              BTreeMap<String, String>,
	#[serde(with = "crate::store::b64_opt")]
	pub env_enc:               Option<Vec<u8>>,
	pub auto_pause:            bool,
	pub auto_pause_memory:     bool,
	pub auto_resume:           bool,
	pub allow_internet_access: Option<bool>,
	pub network:               NetworkConfig,
	#[serde(with = "crate::store::b64")]
	pub egress_enc:            Vec<u8>,
	pub volume_mounts:         Vec<VolumeMount>,
	pub lifecycle:             LifecycleExtra,
	pub envd_version:          String,
	pub started_at:            DateTime<Utc>,
	pub end_at:                DateTime<Utc>,
	pub paused_at:             Option<DateTime<Utc>>,
	pub ended_at:              Option<DateTime<Utc>>,
	pub snapshot_build_id:     Option<String>,
	/// `vmonS3Mounts` without credentials.
	pub s3_mounts:             Option<Vec<S3MountRecord>>,
	/// Encrypted inline credentials of `s3_mounts`, by index.
	#[serde(with = "crate::store::b64_opt")]
	pub s3_mounts_enc:         Option<Vec<u8>>,
}

impl SandboxRow {
	/// Typed `state`.
	pub fn state(&self) -> SandboxState {
		SandboxState::parse(&self.state)
	}

	/// Whether the row passes `filter` (states default to running + paused).
	fn matches(&self, filter: &SandboxFilter<'_>) -> bool {
		let state = self.state();
		let state_ok = if filter.states.is_empty() {
			state != SandboxState::Killed
		} else {
			filter.states.contains(&state)
		};
		state_ok
			&& filter.team_id.is_none_or(|team| team == self.team_id)
			&& filter
				.metadata
				.iter()
				.all(|(k, v)| self.metadata.get(k) == Some(v))
			// Inclusive like upstream, on the stored millisecond grid: a client
			// feeding back a sandbox's own `startedAt` must still see it.
			&& filter
				.started_after
				.is_none_or(|after| self.started_at >= after.trunc_subsecs(3))
			// Template ids and aliases compare case-insensitively.
			&& filter.template.is_none_or(|template| {
				self.template_id.eq_ignore_ascii_case(template)
					|| self
						.alias
						.as_deref()
						.is_some_and(|alias| alias.eq_ignore_ascii_case(template))
			})
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

impl NewSandboxRow<'_> {
	/// The stored record (state `running`, timestamps on the ms grid).
	fn record(&self) -> SandboxRow {
		SandboxRow {
			id:                    self.id.to_owned(),
			team_id:               self.team_id.to_owned(),
			template_id:           self.template_id.to_owned(),
			build_id:              self.build_id.to_owned(),
			alias:                 self.alias.map(str::to_owned),
			node_id:               self.node_id.to_owned(),
			state:                 SandboxState::Running.as_str().to_owned(),
			execution_id:          self.execution_id.to_owned(),
			vcpus:                 self.vcpus,
			memory_mb:             self.memory_mb,
			disk_mb:               self.disk_mb,
			metadata:              self.metadata.clone(),
			env_enc:               self.env_enc.clone(),
			auto_pause:            self.auto_pause,
			auto_pause_memory:     self.auto_pause_memory,
			auto_resume:           self.auto_resume,
			allow_internet_access: self.allow_internet_access,
			network:               self.network.clone(),
			egress_enc:            self.egress_enc.clone(),
			volume_mounts:         self.volume_mounts.to_vec(),
			lifecycle:             self.lifecycle,
			envd_version:          self.envd_version.to_owned(),
			started_at:            self.started_at.trunc_subsecs(3),
			end_at:                self.end_at.trunc_subsecs(3),
			paused_at:             None,
			ended_at:              None,
			snapshot_build_id:     None,
			s3_mounts:             (!self.s3_mounts.is_empty()).then(|| self.s3_mounts.to_vec()),
			s3_mounts_enc:         self.s3_mounts_enc.clone(),
		}
	}
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

/// Keyset position: rows strictly after `(started_at, id)` in the list order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListAfter {
	pub started_at: DateTime<Utc>,
	pub id:         String,
}

fn sandbox_key(id: &str) -> String {
	key(SANDBOXES, &[id])
}

fn live_key(team_id: &str, id: &str) -> String {
	key(LIVE, &[team_id, id])
}

/// Stages `row` at its key. Killed records lose their secrets and expire
/// [`KILLED_SANDBOX_TTL`] after this write.
fn stage_row(tx: &mut Tx<'_>, row: &mut SandboxRow) {
	let key = sandbox_key(&row.id);
	if row.state() == SandboxState::Killed {
		row.env_enc = None;
		row.egress_enc = Vec::new();
		row.s3_mounts_enc = None;
		tx.put_ttl(key, row, KILLED_SANDBOX_TTL);
	} else {
		tx.put(key, row);
	}
}

/// Running and paused records of one team (`Some`) or of every team.
async fn live_rows(r: &Read<'_>, team_id: Option<&str>) -> Result<Vec<SandboxRow>> {
	let index = match team_id {
		Some(team_id) => prefix(LIVE, &[team_id]),
		None => prefix(LIVE, &[]),
	};
	let mut rows = Vec::new();
	for entry in r.keys(&index).await? {
		let id = last_segment(&entry);
		if let Some(row) = r.get::<SandboxRow>(&sandbox_key(&id)).await?
			&& row.state() != SandboxState::Killed
		{
			rows.push(row);
		}
	}
	Ok(rows)
}

/// `build_id` of every running or paused sandbox (artifact GC keeps them).
pub(super) async fn live_build_ids(r: &Read<'_>) -> Result<HashSet<String>> {
	Ok(live_rows(r, None)
		.await?
		.into_iter()
		.map(|row| row.build_id)
		.collect())
}

impl Store {
	/// Records a freshly created sandbox (state `running`) together with its
	/// `created` event, in one transaction.
	///
	/// # Errors
	///
	/// [`super::Error::Duplicate`] when a sandbox with this id exists.
	pub async fn insert_sandbox(
		&self,
		row: &NewSandboxRow<'_>,
		event: &NewSandboxEvent<'_>,
	) -> Result<()> {
		let record = row.record();
		let key = sandbox_key(row.id);
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<SandboxRow>(&key).await?.is_some() {
				return Err(super::Error::Duplicate("sandbox"));
			}
			tx.put(&key, &record);
			tx.mark(live_key(row.team_id, row.id), None);
			stage_event(&mut tx, event);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Row by sandbox ID regardless of team or state (killed rows until they
	/// expire).
	pub async fn sandbox(&self, id: &str) -> Result<Option<SandboxRow>> {
		self.read().await?.get(&sandbox_key(id)).await
	}

	/// Paused → running after a resume (new execution, node, and lifetime
	/// window) plus `event`, atomically; `false` (nothing written) when the
	/// row was not paused.
	pub async fn mark_sandbox_resumed(
		&self,
		resumed: &ResumedExecution<'_>,
		event: &NewSandboxEvent<'_>,
	) -> Result<bool> {
		self
			.transition(resumed.id, SandboxState::Paused, event, |row| {
				SandboxState::Running.as_str().clone_into(&mut row.state);
				resumed.node_id.clone_into(&mut row.node_id);
				resumed.execution_id.clone_into(&mut row.execution_id);
				row.started_at = resumed.started_at.trunc_subsecs(3);
				row.end_at = resumed.end_at.trunc_subsecs(3);
				row.paused_at = None;
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
	) -> Result<bool> {
		self
			.transition(id, SandboxState::Running, event, |row| {
				SandboxState::Paused.as_str().clone_into(&mut row.state);
				row.paused_at = Some(paused_at.trunc_subsecs(3));
			})
			.await
	}

	/// Running/paused → killed plus `event`, atomically; `false` (nothing
	/// written) when the row was already killed or missing. The record drops
	/// out of the live index, loses its secrets, and expires after
	/// [`KILLED_SANDBOX_TTL`].
	pub async fn mark_sandbox_killed(
		&self,
		id: &str,
		ended_at: DateTime<Utc>,
		event: &NewSandboxEvent<'_>,
	) -> Result<bool> {
		let key = sandbox_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<SandboxRow>(&key).await? else {
				return Ok(false);
			};
			if row.state() == SandboxState::Killed {
				return Ok(false);
			}
			SandboxState::Killed.as_str().clone_into(&mut row.state);
			row.ended_at = Some(ended_at.trunc_subsecs(3));
			tx.delete(live_key(&row.team_id, id));
			stage_row(&mut tx, &mut row);
			stage_event(&mut tx, event);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// `from` → whatever `change` sets, plus `event`, in one transaction;
	/// `false` (nothing written) unless the row is in state `from`. The live
	/// index is unchanged (both states are live).
	async fn transition(
		&self,
		id: &str,
		from: SandboxState,
		event: &NewSandboxEvent<'_>,
		change: impl Fn(&mut SandboxRow),
	) -> Result<bool> {
		let key = sandbox_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<SandboxRow>(&key).await? else {
				return Ok(false);
			};
			if row.state() != from {
				return Ok(false);
			}
			change(&mut row);
			stage_row(&mut tx, &mut row);
			stage_event(&mut tx, event);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Applies `change` to the row (any state); no-op when it is missing.
	async fn update_sandbox(&self, id: &str, change: impl Fn(&mut SandboxRow)) -> Result<()> {
		let key = sandbox_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx.get::<SandboxRow>(&key).await? else {
				return Ok(());
			};
			change(&mut row);
			stage_row(&mut tx, &mut row);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// New expiry of a running sandbox.
	pub async fn set_sandbox_end_at(&self, id: &str, end_at: DateTime<Utc>) -> Result<()> {
		let end_at = end_at.trunc_subsecs(3);
		self.update_sandbox(id, |row| row.end_at = end_at).await
	}

	/// Replaced egress configuration (`PUT /sandboxes/{id}/network`).
	pub async fn set_sandbox_network(
		&self,
		id: &str,
		network: &NetworkConfig,
		egress_enc: &[u8],
		allow_internet_access: Option<bool>,
	) -> Result<()> {
		self
			.update_sandbox(id, |row| {
				row.network = network.clone();
				egress_enc.clone_into(&mut row.egress_enc);
				row.allow_internet_access = allow_internet_access;
			})
			.await
	}

	/// Remembers the latest snapshot build captured from a sandbox.
	pub async fn set_sandbox_snapshot_build(&self, id: &str, build_id: &str) -> Result<()> {
		self
			.update_sandbox(id, |row| row.snapshot_build_id = Some(build_id.to_owned()))
			.await
	}

	/// One page of sandboxes. Returns up to `limit` rows after `after`.
	pub async fn list_sandboxes(
		&self,
		filter: &SandboxFilter<'_>,
		order: ListOrder,
		after: Option<&ListAfter>,
		limit: u32,
	) -> Result<Vec<SandboxRow>> {
		let r = self.read().await?;
		let mut rows: Vec<SandboxRow> = live_rows(&r, filter.team_id)
			.await?
			.into_iter()
			.filter(|row| row.matches(filter))
			.filter(|row| {
				after.is_none_or(|after| {
					let position = (row.started_at, row.id.as_str());
					let cursor = (after.started_at, after.id.as_str());
					match order {
						ListOrder::Asc => position > cursor,
						ListOrder::Desc => position < cursor,
					}
				})
			})
			.collect();
		rows.sort_by(|a, b| (a.started_at, &a.id).cmp(&(b.started_at, &b.id)));
		if order == ListOrder::Desc {
			rows.reverse();
		}
		rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
		Ok(rows)
	}

	/// Number of rows matching `filter` (pagination-independent totals).
	pub async fn count_sandboxes(&self, filter: &SandboxFilter<'_>) -> Result<u64> {
		let r = self.read().await?;
		let rows = live_rows(&r, filter.team_id).await?;
		let count = rows.iter().filter(|row| row.matches(filter)).count();
		Ok(u64::try_from(count).unwrap_or(u64::MAX))
	}

	/// IDs and states of a team's running and paused sandboxes (admin kill).
	pub async fn team_live_sandboxes(&self, team_id: &str) -> Result<Vec<(String, String)>> {
		let r = self.read().await?;
		Ok(live_rows(&r, Some(team_id))
			.await?
			.into_iter()
			.map(|row| (row.id, row.state))
			.collect())
	}

	/// Running/paused counts per team; teams without live sandboxes are
	/// absent.
	pub async fn live_counts_by_team(&self) -> Result<HashMap<String, LiveCounts>> {
		let r = self.read().await?;
		let mut counts: HashMap<String, LiveCounts> = HashMap::new();
		for row in live_rows(&r, None).await? {
			let entry = counts.entry(row.team_id.clone()).or_default();
			match row.state() {
				SandboxState::Running => entry.running += 1,
				SandboxState::Paused => entry.paused += 1,
				SandboxState::Killed => {},
			}
		}
		Ok(counts)
	}

	/// `(id, state, node_id)` of every running or paused sandbox (reconciler).
	pub async fn live_sandboxes(&self) -> Result<Vec<(String, String, String)>> {
		let r = self.read().await?;
		Ok(live_rows(&r, None)
			.await?
			.into_iter()
			.map(|row| (row.id, row.state, row.node_id))
			.collect())
	}

	/// Build ids referenced by paused sandboxes (artifacts must stay).
	pub async fn builds_of_paused_sandboxes(&self) -> Result<Vec<String>> {
		let r = self.read().await?;
		let builds: HashSet<String> = live_rows(&r, None)
			.await?
			.into_iter()
			.filter(|row| row.state() == SandboxState::Paused)
			.map(|row| row.build_id)
			.collect();
		Ok(builds.into_iter().collect())
	}
}

#[cfg(test)]
impl Store {
	/// Rewrites a stored sandbox record in place (tests that simulate state
	/// the API cannot produce, e.g. a lost owner node).
	pub(crate) async fn edit_sandbox_for_test(&self, id: &str, change: impl Fn(&mut SandboxRow)) {
		self
			.update_sandbox(id, change)
			.await
			.expect("edit sandbox record");
	}
}

#[cfg(test)]
mod tests {
	use chrono::Duration;

	use super::*;
	use crate::store::{Error, EventQuery, event_types, now, testing::TestStore};

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
	async fn s3_mounts_round_trip_and_secrets_are_dropped_on_kill() {
		let db = TestStore::start().await;
		let store = &db.store;
		let team = "team-s3";
		let net = NetworkConfig::default();
		let meta = BTreeMap::new();
		let id = "is3000000000000000000";
		let records = [S3MountRecord {
			path:               "/data".into(),
			uri:                "s3://bucket/prefix".into(),
			mode:               vmon_node::api::S3MountMode::Writeback,
			endpoint:           Some("http://minio:9000".into()),
			region:             None,
			credentials_secret: Some("bucket-keys".into()),
		}];
		let mut row = new_row(id, team, now(), &meta, &net);
		row.s3_mounts = &records;
		row.s3_mounts_enc = Some(vec![9, 9, 9]);
		row.env_enc = Some(vec![7]);
		store
			.insert_sandbox(&row, &event(id, team, event_types::CREATED))
			.await
			.unwrap();
		assert!(matches!(
			store
				.insert_sandbox(&row, &event(id, team, event_types::CREATED))
				.await,
			Err(Error::Duplicate(_))
		));
		let stored = store.sandbox(id).await.unwrap().unwrap();
		assert_eq!(stored.s3_mounts.as_deref(), Some(records.as_slice()));
		assert_eq!(stored.s3_mounts_enc.as_deref(), Some([9u8, 9, 9].as_slice()));
		assert_eq!(stored.env_enc.as_deref(), Some([7u8].as_slice()));
		assert_eq!(stored.egress_enc, [1u8, 2, 3]);

		let ended_at = now();
		assert!(
			store
				.mark_sandbox_killed(id, ended_at, &event(id, team, event_types::KILLED))
				.await
				.unwrap()
		);
		let killed = store.sandbox(id).await.unwrap().unwrap();
		assert_eq!(killed.state(), SandboxState::Killed);
		assert_eq!(killed.ended_at, Some(ended_at));
		assert_eq!(killed.s3_mounts.as_deref(), Some(records.as_slice()));
		assert!(killed.s3_mounts_enc.is_none() && killed.env_enc.is_none());
		assert_eq!(killed.egress_enc, Vec::<u8>::new());
		store
			.set_sandbox_network(id, &net, &[4, 5], None)
			.await
			.unwrap();
		assert!(
			store
				.sandbox(id)
				.await
				.unwrap()
				.unwrap()
				.egress_enc
				.is_empty(),
			"killed records never regain secrets"
		);
		db.finish().await;
	}

	#[tokio::test]
	async fn sandbox_rows_round_trip_filter_and_paginate() {
		let db = TestStore::start().await;
		let store = &db.store;
		let team = "team-store";
		let base = now();
		let net = NetworkConfig { allow_public_traffic: Some(false), ..NetworkConfig::default() };
		let red = BTreeMap::from([
			("color".to_owned(), "red".to_owned()),
			("k=&".to_owned(), "v w".to_owned()),
		]);
		let blue = BTreeMap::from([("color".to_owned(), "blue".to_owned())]);
		let ids: Vec<String> = (0..4).map(|i| format!("isandbox00000000000{i}")).collect();
		for (i, id) in ids.iter().enumerate() {
			let meta = if i % 2 == 0 { &red } else { &blue };
			let at = base + Duration::seconds(i64::try_from(i).unwrap());
			store
				.insert_sandbox(
					&new_row(id, team, at, meta, &net),
					&event(id, team, event_types::CREATED),
				)
				.await
				.unwrap();
		}

		let row = store.sandbox(&ids[0]).await.unwrap().unwrap();
		assert_eq!(row.state(), SandboxState::Running);
		assert_eq!(row.metadata, red);
		assert_eq!(row.network, net);
		assert_eq!(row.started_at, base);
		assert_eq!(row.allow_internet_access, Some(false));
		assert_eq!(row.lifecycle.auto_resume_timeout_s, 300);

		// State transitions only apply from the expected source state.
		let paused = |id| event(id, team, event_types::PAUSED);
		let killed = |id| event(id, team, event_types::KILLED);
		let resumed_event = |id| event(id, team, event_types::RESUMED);
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
		assert!(
			!store
				.mark_sandbox_killed("imissing", base, &killed("imissing"))
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
		assert_eq!(store.builds_of_paused_sandboxes().await.unwrap(), [
			"00000000-0000-4000-8000-000000000000"
		]);
		// Every applied transition wrote exactly one event; refused ones wrote
		// none.
		assert_eq!(event_types_of(store, team, &ids[0]).await, [event_types::CREATED]);
		assert_eq!(event_types_of(store, team, &ids[1]).await, [
			event_types::CREATED,
			event_types::PAUSED,
			event_types::RESUMED,
			event_types::PAUSED
		]);
		assert_eq!(event_types_of(store, team, &ids[3]).await, [
			event_types::CREATED,
			event_types::KILLED
		]);
		assert_eq!(event_types_of(store, team, "imissing").await.len(), 0);

		let all = SandboxFilter { team_id: Some(team), ..SandboxFilter::default() };
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
		let by_alias = SandboxFilter { template: Some("BASE"), ..all.clone() };
		assert_eq!(store.count_sandboxes(&by_alias).await.unwrap(), 3);
		let by_other = SandboxFilter { template: Some("other"), ..all };
		assert_eq!(store.count_sandboxes(&by_other).await.unwrap(), 0);

		let live = store.team_live_sandboxes(team).await.unwrap();
		assert_eq!(live.len(), 3);
		db.finish().await;
	}

	#[tokio::test]
	async fn listing_without_team_spans_teams_and_counts_group_by_team() {
		let db = TestStore::start().await;
		let store = &db.store;
		let (a, b) = ("team-a", "team-b");
		let base = now();
		let net = NetworkConfig::default();
		let meta = BTreeMap::new();
		let rows = [("ilista0", a), ("ilista1", a), ("ilistb0", b), ("ilistb1", b)];
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
				.mark_sandbox_paused("ilista1", base, &event("ilista1", a, event_types::PAUSED))
				.await
				.unwrap()
		);
		assert!(
			store
				.mark_sandbox_killed("ilistb1", base, &event("ilistb1", b, event_types::KILLED))
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

		let only_a = SandboxFilter { team_id: Some(a), ..SandboxFilter::default() };
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
				(a.to_owned(), LiveCounts { running: 1, paused: 1 }),
				(b.to_owned(), LiveCounts { running: 1, paused: 0 }),
			])
		);
		let mut live = store.live_sandboxes().await.unwrap();
		live.sort();
		assert_eq!(live.iter().map(|(id, ..)| id.as_str()).collect::<Vec<_>>(), [
			"ilista0", "ilista1", "ilistb0"
		]);
		let r = store.read().await.unwrap();
		assert_eq!(
			live_build_ids(&r).await.unwrap(),
			HashSet::from(["00000000-0000-4000-8000-000000000000".to_owned()])
		);
		db.finish().await;
	}
}
