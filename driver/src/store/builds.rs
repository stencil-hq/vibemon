//! Template builds and their logs (ARCHITECTURE.md §7).
//!
//! Owned by the template workstream; written by the build API and the build
//! supervisor ([`crate::builds`]), read by the template API.
//!
//! Keys (`<t>` = [`time_segment`] of `created_at`; index entries hold the
//! build id as a JSON string so ranged scans decode them):
//! - `db/build/<id>` → [`BuildRow`].
//! - `db/build.template/<template_id>/<t>/<id>`: every build of a template in
//!   creation order ([`Store::template_builds`], [`Store::latest_builds`],
//!   template deletes).
//! - `db/build.ready/<template_id>/<t>/<id>`: `ready` builds
//!   ([`Store::latest_ready_builds`], `ready_only` template listings).
//! - `db/build.unfinished/<team_id>/<kind>/<id>`: a team's `waiting` and
//!   `building` builds. [`Store::register_build`] admits a build with
//!   `CountBelow` over the team's `build` kind (see [`super::templates`]).
//! - `db/build.building/<kind>/<t>/<id>`: `building` builds by kind (the
//!   supervisor work list, stale snapshots).
//! - `db/build.gc/<updated_at>/<id>`: finished builds of deleted templates
//!   whose artifacts are not collected yet; written by template deletes,
//!   removed by [`Store::mark_artifacts_collected`].
//! - `db/blog/<build_id>/<seq>` → [`LogRow`] and
//!   `db/blog.ts/<build_id>/<ts>/<seq>` → `{seq, level}`: log lines in sequence
//!   and in time order, written with [`HISTORY_TTL`] and deleted with their
//!   build.
//!
//! [`put_build`] moves the status indexes with the record. Each transition
//! reads the build (and the template it changes) in its transaction, so a
//! concurrent transition fails the commit and the rerun sees its status.

use std::collections::HashSet;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::kv::{Compare, Range};
use vmon_node::api::{ArtifactInfo, BuildReason};

use super::{
	Cursor, HISTORY_TTL, Read, Result, Store, Tx, from_micros, key, now, prefix, seq,
	templates::{TemplateRow, template_key, upsert_tags},
	time_segment,
};

const BUILD: &str = "db/build";
const BY_TEMPLATE: &str = "db/build.template";
const READY: &str = "db/build.ready";
const UNFINISHED: &str = "db/build.unfinished";
const BUILDING: &str = "db/build.building";
const GC: &str = "db/build.gc";
const LOG: &str = "db/blog";
const LOG_BY_TIME: &str = "db/blog.ts";

/// `kind` of builds made from steps (`POST /v3/templates`).
pub(super) const KIND_BUILD: &str = "build";
/// `kind` of snapshot and fork captures.
pub(super) const KIND_SNAPSHOT: &str = "snapshot";

/// Build lifecycle as stored (and as E2B reports it).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildStatus {
	Waiting,
	Building,
	Ready,
	Error,
}

impl BuildStatus {
	/// Stored / wire value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Waiting => "waiting",
			Self::Building => "building",
			Self::Ready => "ready",
			Self::Error => "error",
		}
	}

	/// Parses a stored value; unknown values read as `error`.
	pub fn parse(raw: &str) -> Self {
		match raw {
			"waiting" => Self::Waiting,
			"building" => Self::Building,
			"ready" => Self::Ready,
			_ => Self::Error,
		}
	}

	/// `ready` or `error`.
	pub const fn is_terminal(self) -> bool {
		matches!(self, Self::Ready | Self::Error)
	}
}

/// Build log level; ordered from least to most severe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
	Debug,
	Info,
	Warn,
	Error,
}

impl LogLevel {
	/// Stored / wire value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Debug => "debug",
			Self::Info => "info",
			Self::Warn => "warn",
			Self::Error => "error",
		}
	}

	/// Strict parse of an E2B `LogLevel` query value.
	pub fn parse(raw: &str) -> Option<Self> {
		match raw {
			"debug" => Some(Self::Debug),
			"info" => Some(Self::Info),
			"warn" => Some(Self::Warn),
			"error" => Some(Self::Error),
			_ => None,
		}
	}

	/// Lenient mapping of node-reported levels onto the E2B set.
	pub fn normalize(raw: &str) -> Self {
		match raw.to_ascii_lowercase().as_str() {
			"debug" | "trace" => Self::Debug,
			"warn" | "warning" => Self::Warn,
			"error" | "fatal" | "panic" => Self::Error,
			_ => Self::Info,
		}
	}
}

/// A build record (`db/build/<id>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BuildRow {
	pub id:               String,
	pub template_id:      String,
	pub team_id:          String,
	/// See [`BuildStatus`].
	pub status:           String,
	/// `build` | `snapshot`.
	pub kind:             String,
	pub vcpus:            u32,
	pub memory_mb:        u64,
	pub disk_mb:          u64,
	pub min_free_disk_mb: Option<u64>,
	pub envd_version:     Option<String>,
	pub arch:             Option<String>,
	pub node_id:          Option<String>,
	pub tags:             Vec<String>,
	pub spec:             Option<serde_json::Value>,
	pub template_version: Option<String>,
	pub start_cmd:        Option<String>,
	pub ready_cmd:        Option<String>,
	pub reason:           Option<BuildReason>,
	pub artifact:         Option<ArtifactInfo>,
	pub created_at:       DateTime<Utc>,
	pub updated_at:       DateTime<Utc>,
	pub started_at:       Option<DateTime<Utc>>,
	pub finished_at:      Option<DateTime<Utc>>,
	/// When the artifacts of this build of a deleted template were deleted
	/// from every holder.
	#[serde(default)]
	pub artifacts_gc_at:  Option<DateTime<Utc>>,
}

impl BuildRow {
	/// Parsed [`BuildStatus`].
	pub fn status(&self) -> BuildStatus {
		BuildStatus::parse(&self.status)
	}
}

/// A stored build log line (`db/blog/<build_id>/<seq>`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
	pub seq:     u64,
	pub ts:      DateTime<Utc>,
	/// See [`LogLevel`].
	pub level:   String,
	pub step:    Option<String>,
	pub message: String,
}

/// Value of a `db/blog.ts` entry: what level filtering needs before the line
/// is loaded.
#[derive(Serialize, Deserialize)]
struct LogRef {
	seq:   u64,
	level: String,
}

/// A log line to persist (from a node's `BuildStatus`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLog {
	pub ts:      DateTime<Utc>,
	pub level:   LogLevel,
	pub step:    Option<String>,
	pub message: String,
}

/// Fields recorded when a build is handed to a builder node.
#[derive(Clone, Debug)]
pub struct BuildStart<'a> {
	pub node_id:          &'a str,
	pub arch:             &'a str,
	pub spec:             &'a serde_json::Value,
	pub template_version: &'a str,
	pub start_cmd:        Option<&'a str>,
	pub ready_cmd:        Option<&'a str>,
}

/// Direction of [`Store::build_logs_by_time`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogDirection {
	/// `ts >= cursor`, oldest first.
	Forward,
	/// `ts <= cursor`, newest first.
	Backward,
}

/// Reason of builds replaced by a newer `POST /v3/templates` (upstream text).
pub fn superseded_reason() -> BuildReason {
	BuildReason {
		message: "The build was canceled because it was superseded by a newer one.".into(),
		step:    None,
	}
}

/// Key of the [`BuildRow`] `id`.
pub(super) fn build_key(id: &str) -> String {
	key(BUILD, &[id])
}

/// Prefix of the build index of template `template_id`.
fn template_builds_prefix(template_id: &str) -> String {
	prefix(BY_TEMPLATE, &[template_id])
}

/// Prefix of the unfinished builds of `kind` of team `team_id`.
pub(super) fn unfinished_prefix(team_id: &str, kind: &str) -> String {
	prefix(UNFINISHED, &[team_id, kind])
}

/// Status-dependent index keys of `build`.
fn status_index(build: &BuildRow) -> Vec<String> {
	let created = time_segment(build.created_at);
	let status = build.status();
	let mut keys = Vec::new();
	if !status.is_terminal() {
		keys.push(key(UNFINISHED, &[&build.team_id, &build.kind, &build.id]));
	}
	match status {
		BuildStatus::Building => keys.push(key(BUILDING, &[&build.kind, &created, &build.id])),
		BuildStatus::Ready => keys.push(key(READY, &[&build.template_id, &created, &build.id])),
		BuildStatus::Waiting | BuildStatus::Error => {},
	}
	keys
}

fn template_index_key(build: &BuildRow) -> String {
	key(BY_TEMPLATE, &[&build.template_id, &time_segment(build.created_at), &build.id])
}

fn gc_key(build: &BuildRow) -> String {
	key(GC, &[&time_segment(build.updated_at), &build.id])
}

fn log_key(build_id: &str, n: u64) -> String {
	key(LOG, &[build_id, &seq(n)])
}

/// Stages `new` (replacing `old`, the record read in this transaction; `None`
/// for an insert) and moves its status indexes.
pub(super) fn put_build(tx: &mut Tx<'_>, old: Option<&BuildRow>, new: &BuildRow) {
	let fresh = status_index(new);
	let stale = old.map(status_index).unwrap_or_default();
	for index in stale.iter().filter(|index| !fresh.contains(index)) {
		tx.delete(index.as_str());
	}
	for index in fresh.into_iter().filter(|index| !stale.contains(index)) {
		tx.put(index, &new.id);
	}
	if old.is_none() {
		tx.put(template_index_key(new), &new.id);
	}
	tx.put(build_key(&new.id), new);
}

/// `build` failed with `reason` at `at`.
pub(super) fn failed_build(build: &BuildRow, reason: BuildReason, at: DateTime<Utc>) -> BuildRow {
	BuildRow {
		status: BuildStatus::Error.as_str().into(),
		reason: Some(reason),
		finished_at: Some(at),
		updated_at: at,
		..build.clone()
	}
}

/// Stages the GC index entry of a finished build of a template being
/// deleted (unless its artifacts are already collected).
pub(super) fn mark_pending_gc(tx: &mut Tx<'_>, build: &BuildRow) {
	if build.artifacts_gc_at.is_none() {
		tx.put(gc_key(build), &build.id);
	}
}

/// Stages the removal of `build` with its indexes and log lines (template
/// hard deletes).
pub(super) fn delete_build(tx: &mut Tx<'_>, build: &BuildRow) {
	for index in status_index(build) {
		tx.delete(index);
	}
	tx.delete(template_index_key(build));
	tx.delete(gc_key(build));
	tx.delete_prefix(prefix(LOG, &[&build.id]));
	tx.delete_prefix(prefix(LOG_BY_TIME, &[&build.id]));
	tx.delete(build_key(&build.id));
}

/// Builds of template `template_id` (guarded), oldest first.
pub(super) async fn builds_of(tx: &mut Tx<'_>, template_id: &str) -> Result<Vec<BuildRow>> {
	let ids: Vec<String> = tx.list(&template_builds_prefix(template_id)).await?;
	let mut builds = Vec::with_capacity(ids.len());
	for id in ids {
		if let Some(build) = tx.get::<BuildRow>(&build_key(&id)).await? {
			builds.push(build);
		}
	}
	Ok(builds)
}

/// Whether template `template_id` has a `ready` build.
pub(super) async fn has_ready_build(r: &Read<'_>, template_id: &str) -> Result<bool> {
	let range = Range { prefix: prefix(READY, &[template_id]), limit: Some(1), ..Range::default() };
	Ok(!r.scan::<String>(&range).await?.is_empty())
}

/// Whether two tag lists share a tag (`JSON_OVERLAPS`).
pub(super) fn overlaps(tags: &[String], other: &[String]) -> bool {
	tags.iter().any(|tag| other.contains(tag))
}

/// Range limit for a caller-supplied page size.
pub(super) fn page_limit(limit: u32) -> usize {
	usize::try_from(limit).unwrap_or(usize::MAX)
}

impl Store {
	/// Build by id.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn build(&self, id: &str) -> Result<Option<BuildRow>> {
		self.read().await?.get(&build_key(id)).await
	}

	/// Builds of a template, newest first, keyset-paginated.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn template_builds(
		&self,
		template_id: &str,
		after: Option<&Cursor>,
		limit: Option<u32>,
	) -> Result<Vec<BuildRow>> {
		let r = self.read().await?;
		let range = Range {
			prefix:  template_builds_prefix(template_id),
			after:   after.map(|cursor| {
				let at = time_segment(from_micros(cursor.created_at_us));
				key(BY_TEMPLATE, &[template_id, &at, &cursor.id])
			}),
			limit:   limit.map(page_limit),
			reverse: true,
		};
		let mut builds = Vec::new();
		for (_, id) in r.scan::<String>(&range).await? {
			if let Some(build) = r.get::<BuildRow>(&build_key(&id)).await? {
				builds.push(build);
			}
		}
		Ok(builds)
	}

	/// Latest build of each template (by `created_at`), any status.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn latest_builds(&self, template_ids: &[String]) -> Result<Vec<BuildRow>> {
		self.latest_builds_in(BY_TEMPLATE, template_ids).await
	}

	/// Latest `ready` build of each template.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn latest_ready_builds(&self, template_ids: &[String]) -> Result<Vec<BuildRow>> {
		self.latest_builds_in(READY, template_ids).await
	}

	/// Last build of each template in the per-template index `table`.
	async fn latest_builds_in(&self, table: &str, template_ids: &[String]) -> Result<Vec<BuildRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let r = self.read().await?;
		let mut builds = Vec::with_capacity(template_ids.len());
		for template_id in template_ids {
			let range = Range {
				prefix: prefix(table, &[template_id]),
				limit: Some(1),
				reverse: true,
				..Range::default()
			};
			if let Some((_, id)) = r.scan::<String>(&range).await?.pop()
				&& let Some(build) = r.get::<BuildRow>(&build_key(&id)).await?
			{
				builds.push(build);
			}
		}
		Ok(builds)
	}

	/// Build ids of `kind = 'build'` in `building`, oldest first (supervisor
	/// work list).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn building_build_ids(&self) -> Result<Vec<String>> {
		self
			.read()
			.await?
			.list(&prefix(BUILDING, &[KIND_BUILD]))
			.await
	}

	/// Unfinished builds of a team (admin cancel), oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn team_unfinished_builds(&self, team_id: &str) -> Result<Vec<BuildRow>> {
		let r = self.read().await?;
		let mut builds = Vec::new();
		for id in r.list::<String>(&prefix(UNFINISHED, &[team_id])).await? {
			if let Some(build) = r.get::<BuildRow>(&build_key(&id)).await? {
				builds.push(build);
			}
		}
		builds.sort_by(|a, b| {
			a.created_at
				.cmp(&b.created_at)
				.then_with(|| a.id.cmp(&b.id))
		});
		Ok(builds)
	}

	/// Unfinished `kind = 'build'` builds of a template that request at least
	/// one of `tags` (cancelled when another build of those tags starts, like
	/// upstream's `GetConcurrentTemplateBuilds`).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn template_building_builds(
		&self,
		template_id: &str,
		tags: &[String],
	) -> Result<Vec<BuildRow>> {
		let r = self.read().await?;
		let mut builds = Vec::new();
		for id in r.list::<String>(&prefix(BUILDING, &[KIND_BUILD])).await? {
			if let Some(build) = r.get::<BuildRow>(&build_key(&id)).await?
				&& build.template_id == template_id
				&& build.status() == BuildStatus::Building
				&& overlaps(&build.tags, tags)
			{
				builds.push(build);
			}
		}
		Ok(builds)
	}

	/// Moves a `waiting` build to `building` on a node; `false` when the
	/// build was no longer waiting.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn mark_build_started(&self, id: &str, start: &BuildStart<'_>) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(build) = tx.get::<BuildRow>(&build_key(id)).await? else {
				return Ok(false);
			};
			let Some(mut template) = tx
				.get::<TemplateRow>(&template_key(&build.template_id))
				.await?
			else {
				return Ok(false);
			};
			if build.status() != BuildStatus::Waiting {
				return Ok(false);
			}
			let at = now();
			let started = BuildRow {
				status: BuildStatus::Building.as_str().into(),
				node_id: Some(start.node_id.to_owned()),
				arch: Some(start.arch.to_owned()),
				spec: Some(start.spec.clone()),
				template_version: Some(start.template_version.to_owned()),
				start_cmd: start.start_cmd.map(str::to_owned),
				ready_cmd: start.ready_cmd.map(str::to_owned),
				started_at: None,
				updated_at: at,
				..build.clone()
			};
			put_build(&mut tx, Some(&build), &started);
			template.build_count += 1;
			template.updated_at = at;
			tx.put(template_key(&template.id), &template);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Records that the builder node started running a dispatched build (it
	/// left the node's queue); `false` when already recorded or no longer
	/// building.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn mark_build_running(&self, id: &str) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(build) = tx.get::<BuildRow>(&build_key(id)).await? else {
				return Ok(false);
			};
			if build.status() != BuildStatus::Building || build.started_at.is_some() {
				return Ok(false);
			}
			let at = now();
			let running = BuildRow { started_at: Some(at), updated_at: at, ..build.clone() };
			put_build(&mut tx, Some(&build), &running);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Marks an unfinished build `ready` with its artifact, records the
	/// build facts, and points its requested tags at it. `false` when the
	/// build had already finished (e.g. cancelled meanwhile).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn finish_build_ready(&self, id: &str, artifact: &ArtifactInfo) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(build) = tx.get::<BuildRow>(&build_key(id)).await? else {
				return Ok(false);
			};
			let Some(mut template) = tx
				.get::<TemplateRow>(&template_key(&build.template_id))
				.await?
			else {
				return Ok(false);
			};
			if build.status().is_terminal() {
				return Ok(false);
			}
			let at = now();
			let ready = BuildRow {
				status: BuildStatus::Ready.as_str().into(),
				artifact: Some(artifact.clone()),
				envd_version: Some(artifact.envd_version.clone()),
				arch: Some(artifact.arch.clone()),
				node_id: Some(artifact.node_id.to_string()),
				vcpus: artifact.vcpus,
				memory_mb: artifact.memory_mb,
				disk_mb: artifact.disk_mb,
				reason: None,
				finished_at: Some(at),
				updated_at: at,
				..build.clone()
			};
			put_build(&mut tx, Some(&build), &ready);
			if template.deleted_at.is_none() {
				upsert_tags(&mut tx, &template.id, id, &build.tags);
				template.updated_at = at;
				tx.put(template_key(&template.id), &template);
			}
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Marks an unfinished build `error`; `false` when it had already finished.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn fail_build(&self, id: &str, reason: &BuildReason) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(build) = tx.get::<BuildRow>(&build_key(id)).await? else {
				return Ok(false);
			};
			if build.status().is_terminal() {
				return Ok(false);
			}
			put_build(&mut tx, Some(&build), &failed_build(&build, reason.clone(), now()));
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Number of persisted log lines (= next `seq`).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn build_log_count(&self, build_id: &str) -> Result<u64> {
		let range = Range {
			prefix: prefix(LOG, &[build_id]),
			limit: Some(1),
			reverse: true,
			..Range::default()
		};
		let last = self.read().await?.scan::<LogRow>(&range).await?;
		Ok(last.first().map_or(0, |(_, line)| line.seq + 1))
	}

	/// Appends log lines with consecutive `seq` from `first_seq` in one
	/// commit; already stored sequence numbers are left untouched (idempotent
	/// re-delivery).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn append_build_logs(
		&self,
		build_id: &str,
		first_seq: u64,
		logs: &[NewLog],
	) -> Result<()> {
		if logs.is_empty() {
			return Ok(());
		}
		let lines: Vec<LogRow> = logs
			.iter()
			.zip(first_seq..)
			.map(|(log, n)| LogRow {
				seq:     n,
				ts:      log.ts.trunc_subsecs(3),
				level:   log.level.as_str().into(),
				step:    log.step.clone(),
				message: log.message.clone(),
			})
			.collect();
		let stored = Range {
			prefix:  prefix(LOG, &[build_id]),
			after:   first_seq.checked_sub(1).map(|n| log_key(build_id, n)),
			limit:   Some(lines.len()),
			reverse: false,
		};
		let mut tx = self.tx().await?;
		loop {
			let existing: HashSet<u64> = tx
				.scan::<LogRow>(&stored)
				.await?
				.into_iter()
				.map(|(_, line)| line.seq)
				.collect();
			for line in lines.iter().filter(|line| !existing.contains(&line.seq)) {
				let line_key = log_key(build_id, line.seq);
				tx.require(Compare::Missing(line_key.clone()));
				tx.put_ttl(line_key, line, HISTORY_TTL);
				tx.put_ttl(
					key(LOG_BY_TIME, &[build_id, &time_segment(line.ts), &seq(line.seq)]),
					&LogRef { seq: line.seq, level: line.level.clone() },
					HISTORY_TTL,
				);
			}
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// `logsOffset`/`limit` page over the lines at or above `min_level`
	/// (offset counts only matching lines), oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn build_logs_page(
		&self,
		build_id: &str,
		min_level: LogLevel,
		offset: u64,
		limit: u32,
	) -> Result<Vec<LogRow>> {
		let want = page_limit(limit);
		let mut page = Vec::new();
		if want == 0 {
			return Ok(page);
		}
		let mut skip = offset;
		let range = Range { prefix: prefix(LOG, &[build_id]), ..Range::default() };
		self
			.read()
			.await?
			.scan_all::<LogRow>(&range, |_, line| {
				if LogLevel::normalize(&line.level) >= min_level {
					if skip > 0 {
						skip -= 1;
					} else {
						page.push(line);
					}
				}
				page.len() < want
			})
			.await?;
		Ok(page)
	}

	/// Time-cursor page (`GET …/builds/{id}/logs`): lines at or above
	/// `min_level` ordered by `(ts, seq)` from `cursor` in `direction`.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn build_logs_by_time(
		&self,
		build_id: &str,
		cursor: DateTime<Utc>,
		direction: LogDirection,
		min_level: LogLevel,
		limit: u32,
	) -> Result<Vec<LogRow>> {
		let want = page_limit(limit);
		if want == 0 {
			return Ok(Vec::new());
		}
		let from = key(LOG_BY_TIME, &[build_id, &time_segment(cursor)]);
		let range = match direction {
			// Every key of an instant at or after `cursor` sorts after its bare
			// time segment.
			LogDirection::Forward => Range {
				prefix: prefix(LOG_BY_TIME, &[build_id]),
				after: Some(from),
				..Range::default()
			},
			// `~` sorts after every `seq` segment of the cursor's instant.
			LogDirection::Backward => Range {
				prefix: prefix(LOG_BY_TIME, &[build_id]),
				after: Some(format!("{from}/~")),
				reverse: true,
				..Range::default()
			},
		};
		let r = self.read().await?;
		let mut seqs = Vec::new();
		r.scan_all::<LogRef>(&range, |_, line| {
			if LogLevel::normalize(&line.level) >= min_level {
				seqs.push(line.seq);
			}
			seqs.len() < want
		})
		.await?;
		let mut page = Vec::with_capacity(seqs.len());
		for n in seqs {
			// Expired lines are skipped.
			if let Some(line) = r.get::<LogRow>(&log_key(build_id, n)).await? {
				page.push(line);
			}
		}
		Ok(page)
	}

	/// `snapshot` builds stuck in `building` since before `before` (their
	/// driver died mid-snapshot).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn stale_snapshot_builds(&self, before: DateTime<Utc>) -> Result<Vec<String>> {
		let r = self.read().await?;
		let mut ids = Vec::new();
		for id in r
			.list::<String>(&prefix(BUILDING, &[KIND_SNAPSHOT]))
			.await?
		{
			if let Some(build) = r.get::<BuildRow>(&build_key(&id)).await?
				&& build.updated_at < before
			{
				ids.push(build.id);
			}
		}
		Ok(ids)
	}

	/// Finished builds of deleted templates whose artifacts are not yet
	/// collected and that no running or paused sandbox uses (so builds that
	/// stay in use cannot starve the batch), least recently updated first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn builds_pending_gc(&self, limit: u32) -> Result<Vec<BuildRow>> {
		let want = page_limit(limit);
		if want == 0 {
			return Ok(Vec::new());
		}
		let r = self.read().await?;
		let in_use = super::sandboxes::live_build_ids(&r).await?;
		let mut ids = Vec::new();
		let range = Range { prefix: prefix(GC, &[]), ..Range::default() };
		r.scan_all::<String>(&range, |_, id| {
			if !in_use.contains(&id) {
				ids.push(id);
			}
			ids.len() < want
		})
		.await?;
		let mut builds = Vec::with_capacity(ids.len());
		for id in ids {
			if let Some(build) = r.get::<BuildRow>(&build_key(&id)).await?
				&& build.artifacts_gc_at.is_none()
			{
				builds.push(build);
			}
		}
		Ok(builds)
	}

	/// Records that a build's artifacts were deleted from every holder.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn mark_artifacts_collected(&self, id: &str) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			let Some(build) = tx.get::<BuildRow>(&build_key(id)).await? else {
				return Ok(());
			};
			if build.artifacts_gc_at.is_some() {
				return Ok(());
			}
			tx.delete(gc_key(&build));
			tx.put(build_key(id), &BuildRow { artifacts_gc_at: Some(now()), ..build });
			if tx.commit().await? {
				return Ok(());
			}
		}
	}
}

#[cfg(test)]
impl Store {
	/// Every stored build record (tests).
	pub(crate) async fn stored_builds(&self) -> Result<Vec<BuildRow>> {
		self.read().await?.list(&prefix(BUILD, &[])).await
	}
}
