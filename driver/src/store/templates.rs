//! Templates, their aliases, and their tags (ARCHITECTURE.md §7).
//!
//! Owned by the template workstream; called by [`crate::api::templates`] and
//! [`crate::builds`]. Multi-record changes (registering a build or a snapshot,
//! deleting a template) run in one transaction here.
//!
//! Keys (`<t>` = [`time_segment`] of `created_at`; alias and tag segments are
//! lower-cased, so names compare case-insensitively; index entries hold the
//! template id as a JSON string so ranged scans decode them):
//! - `db/template/<id>` → [`TemplateRow`], live and soft-deleted.
//! - `db/template.team/<team_id>/<kind>/<t>/<id>`: a team's live templates by
//!   kind, in creation order ([`Store::list_templates`] walks it backwards;
//!   [`sync_system_aliases`] walks the team's templates).
//! - `db/template.fork/<id>`: fork templates until purged
//!   ([`Store::unretired_fork_templates`], [`Store::purge_collected_forks`]).
//! - `db/alias/<namespace>/<alias>` → [`AliasRow`]: the unique name → template
//!   map (namespace `""` = bare alias). Claims read the key in their
//!   transaction, so an absent alias stays absent until they commit.
//! - `db/alias.template/<template_id>/<namespace>/<alias>` → [`AliasRow`]: the
//!   same entries by template, written and deleted with the primary ones.
//! - `db/tag/<template_id>/<tag>` → [`TagRow`].
//!
//! Builds are in [`super::builds`]. Registrations guard admission with
//! `CountBelow` over the team's unfinished `build` builds; soft deletes fail
//! the template's unfinished builds and queue its builds for artifact GC; fork
//! purges hard-delete a template with its aliases, tags, builds, and build
//! logs.

use std::collections::HashSet;

use chrono::{DateTime, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::kv::{Compare, Range};
use vmon_node::api::BuildReason;

use super::{
	Cursor, Read, Result, Store, Team, Tx,
	builds::{
		BuildRow, BuildStatus, KIND_BUILD, KIND_SNAPSHOT, build_key, builds_of, delete_build,
		failed_build, has_ready_build, mark_pending_gc, overlaps, page_limit, put_build,
		superseded_reason, unfinished_prefix,
	},
	from_micros, key, now, prefix,
	teams::team_key,
	time_segment,
};

const TEMPLATE: &str = "db/template";
const BY_TEAM: &str = "db/template.team";
const FORKS: &str = "db/template.fork";
const ALIAS: &str = "db/alias";
const ALIAS_BY_TEMPLATE: &str = "db/alias.template";
const TAG: &str = "db/tag";
/// Index entries per page of [`Store::list_templates`].
const LIST_PAGE: usize = 256;

/// What a template represents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TemplateKind {
	/// Built from steps (`POST /v3/templates`).
	Template,
	/// Snapshot template (`POST /sandboxes/{id}/snapshots`).
	Snapshot,
	/// Hidden snapshot backing `POST /sandboxes/{id}/fork`.
	Fork,
}

impl TemplateKind {
	/// Stored value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Template => "template",
			Self::Snapshot => "snapshot",
			Self::Fork => "fork",
		}
	}
}

/// A template record (`db/template/<id>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TemplateRow {
	pub id:                String,
	pub team_id:           String,
	pub public:            bool,
	/// `template` | `snapshot` | `fork` (see [`TemplateKind`]).
	pub kind:              String,
	pub source_sandbox_id: Option<String>,
	pub created_at:        DateTime<Utc>,
	pub updated_at:        DateTime<Utc>,
	pub spawn_count:       i64,
	pub last_spawned_at:   Option<DateTime<Utc>>,
	pub build_count:       i32,
	pub deleted_at:        Option<DateTime<Utc>>,
}

impl TemplateRow {
	/// Whether the template is of `kind`.
	pub fn is(&self, kind: TemplateKind) -> bool {
		self.kind == kind.as_str()
	}
}

/// A template alias; `namespace` is a team slug or `""` (bare alias).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AliasRow {
	pub namespace:   String,
	pub alias:       String,
	pub template_id: String,
}

impl AliasRow {
	/// E2B "name": `namespace/alias`, or the bare alias.
	pub fn name(&self) -> String {
		if self.namespace.is_empty() {
			self.alias.clone()
		} else {
			format!("{}/{}", self.namespace, self.alias)
		}
	}
}

/// A template tag (`db/tag/<template_id>/<tag>`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TagRow {
	pub template_id: String,
	pub tag:         String,
	pub build_id:    String,
	pub created_at:  DateTime<Utc>,
}

/// Input of [`Store::register_build`] (`POST /v3/templates`).
#[derive(Clone, Debug)]
pub struct RegisterBuild<'a> {
	pub team_id:               &'a str,
	/// Team slug the alias lives under.
	pub namespace:             &'a str,
	pub alias:                 &'a str,
	/// Template id used when the alias does not resolve yet.
	pub new_template_id:       &'a str,
	pub build_id:              &'a str,
	/// Deduplicated, validated tags (never empty).
	pub tags:                  &'a [String],
	pub vcpus:                 u32,
	pub memory_mb:             u64,
	pub disk_mb:               u64,
	pub min_free_disk_mb:      Option<u64>,
	/// Team limit on builds in `waiting`/`building`.
	pub max_concurrent_builds: u32,
}

/// Result of [`Store::register_build`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
	/// The build was created (in `waiting`) for `template_id`.
	Registered { template_id: String, public: bool },
	/// The alias belongs to another team's or another kind of template.
	AliasTaken,
	/// The team already has `limit` builds in progress.
	ConcurrencyLimit { limit: u32 },
}

/// Input of [`Store::register_snapshot`].
#[derive(Clone, Debug)]
pub struct RegisterSnapshot<'a> {
	pub team_id:           &'a str,
	/// [`TemplateKind::Snapshot`] or [`TemplateKind::Fork`].
	pub kind:              TemplateKind,
	pub source_sandbox_id: &'a str,
	/// Node capturing the snapshot (the build's `node_id`, so artifact GC
	/// also reaches artifacts of a capture that failed midway).
	pub node_id:           &'a str,
	/// `(namespace, alias)` for named snapshots.
	pub name:              Option<(&'a str, &'a str)>,
	pub tag:               &'a str,
	pub new_template_id:   &'a str,
	pub build_id:          &'a str,
	pub vcpus:             u32,
	pub memory_mb:         u64,
	pub disk_mb:           u64,
	pub envd_version:      &'a str,
}

/// Result of [`Store::register_snapshot`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotOutcome {
	/// The snapshot build was created (in `building`) for `template_id`.
	Registered { template_id: String },
	/// The name is used by a template that is not this team's snapshot.
	AliasTaken,
}

/// Optional filters of [`Store::list_templates`].
#[derive(Clone, Copy, Debug, Default)]
pub struct TemplateFilter<'a> {
	pub source_sandbox_id: Option<&'a str>,
	/// Bare alias (any namespace).
	pub alias:             Option<&'a str>,
	/// Only templates with at least one `ready` build.
	pub ready_only:        bool,
}

/// Key of the [`TemplateRow`] `id`.
pub(super) fn template_key(id: &str) -> String {
	key(TEMPLATE, &[id])
}

fn team_index_key(template: &TemplateRow) -> String {
	key(BY_TEAM, &[
		&template.team_id,
		&template.kind,
		&time_segment(template.created_at),
		&template.id,
	])
}

fn fork_key(id: &str) -> String {
	key(FORKS, &[id])
}

fn alias_key(namespace: &str, alias: &str) -> String {
	key(ALIAS, &[&namespace.to_lowercase(), &alias.to_lowercase()])
}

fn alias_index_key(row: &AliasRow) -> String {
	key(ALIAS_BY_TEMPLATE, &[
		&row.template_id,
		&row.namespace.to_lowercase(),
		&row.alias.to_lowercase(),
	])
}

fn aliases_prefix(template_id: &str) -> String {
	prefix(ALIAS_BY_TEMPLATE, &[template_id])
}

fn tag_key(template_id: &str, tag: &str) -> String {
	key(TAG, &[template_id, &tag.to_lowercase()])
}

fn tags_prefix(template_id: &str) -> String {
	prefix(TAG, &[template_id])
}

fn deleted_reason() -> BuildReason {
	BuildReason { message: "The template was deleted.".into(), step: None }
}

/// A fresh template record created at `at`.
fn new_template(
	id: &str,
	team_id: &str,
	kind: TemplateKind,
	source_sandbox_id: Option<&str>,
	at: DateTime<Utc>,
) -> TemplateRow {
	TemplateRow {
		id:                id.to_owned(),
		team_id:           team_id.to_owned(),
		public:            false,
		kind:              kind.as_str().to_owned(),
		source_sandbox_id: source_sandbox_id.map(str::to_owned),
		created_at:        at,
		updated_at:        at,
		spawn_count:       0,
		last_spawned_at:   None,
		build_count:       0,
		deleted_at:        None,
	}
}

/// Stages `template`; `created` also writes the indexes of a new template.
fn put_template(tx: &mut Tx<'_>, template: &TemplateRow, created: bool) {
	if created {
		tx.put(team_index_key(template), &template.id);
		if template.is(TemplateKind::Fork) {
			tx.put(fork_key(&template.id), &template.id);
		}
	}
	tx.put(template_key(&template.id), template);
}

fn put_alias(tx: &mut Tx<'_>, row: &AliasRow) {
	tx.put(alias_key(&row.namespace, &row.alias), row);
	tx.put(alias_index_key(row), row);
}

fn delete_alias(tx: &mut Tx<'_>, row: &AliasRow) {
	tx.delete(alias_key(&row.namespace, &row.alias));
	tx.delete(alias_index_key(row));
}

/// Guarded lookup of an alias: its entry and the live template it names.
async fn resolve_alias(
	tx: &mut Tx<'_>,
	namespace: &str,
	alias: &str,
) -> Result<(Option<AliasRow>, Option<TemplateRow>)> {
	let Some(entry) = tx.get::<AliasRow>(&alias_key(namespace, alias)).await? else {
		return Ok((None, None));
	};
	let template = tx
		.get::<TemplateRow>(&template_key(&entry.template_id))
		.await?
		.filter(|t| t.deleted_at.is_none());
	Ok((Some(entry), template))
}

/// Stages a bare alias for each name of template `template_id` whose bare
/// alias is free; `claimed` holds the bare alias keys this transaction
/// already staged (its reads do not see them).
async fn claim_bare_aliases(
	tx: &mut Tx<'_>,
	template_id: &str,
	claimed: &mut HashSet<String>,
) -> Result<()> {
	for row in tx.list::<AliasRow>(&aliases_prefix(template_id)).await? {
		if row.namespace.is_empty() {
			continue;
		}
		let bare_key = alias_key("", &row.alias);
		if claimed.contains(&bare_key) || tx.get::<AliasRow>(&bare_key).await?.is_some() {
			continue;
		}
		put_alias(tx, &AliasRow {
			namespace:   String::new(),
			alias:       row.alias,
			template_id: template_id.to_owned(),
		});
		claimed.insert(bare_key);
	}
	Ok(())
}

/// Stages the removal of the bare aliases of template `template_id`.
async fn release_bare_aliases(tx: &mut Tx<'_>, template_id: &str) -> Result<()> {
	for row in tx
		.list::<AliasRow>(&prefix(ALIAS_BY_TEMPLATE, &[template_id, ""]))
		.await?
	{
		delete_alias(tx, &row);
	}
	Ok(())
}

/// Stages, inside the caller's transaction ([`Store::set_team_system`]),
/// claiming (`system`: each live public template of `team` gets a bare alias
/// for each of its names whose bare alias is free) or releasing (`!system`:
/// the bare aliases of the team's templates are dropped).
///
/// # Errors
///
/// Store failures.
pub(super) async fn sync_system_aliases(tx: &mut Tx<'_>, team: &Team, system: bool) -> Result<()> {
	let mut claimed = HashSet::new();
	for id in tx.list::<String>(&prefix(BY_TEAM, &[&team.id])).await? {
		if system {
			let template = tx.get::<TemplateRow>(&template_key(&id)).await?;
			if template.is_some_and(|t| t.public && t.deleted_at.is_none()) {
				claim_bare_aliases(tx, &id, &mut claimed).await?;
			}
		} else {
			release_bare_aliases(tx, &id).await?;
		}
	}
	Ok(())
}

/// Stages pointing `tags` of template `template_id` at `build_id` (insert or
/// move).
pub(super) fn upsert_tags(tx: &mut Tx<'_>, template_id: &str, build_id: &str, tags: &[String]) {
	let at = now();
	for tag in tags {
		tx.put(tag_key(template_id, tag), &TagRow {
			template_id: template_id.to_owned(),
			tag:         tag.clone(),
			build_id:    build_id.to_owned(),
			created_at:  at,
		});
	}
}

/// Whether `template` passes `filter` (`alias` lower-cased).
async fn listed(
	r: &Read<'_>,
	template: &TemplateRow,
	filter: &TemplateFilter<'_>,
	alias: Option<&str>,
) -> Result<bool> {
	if template.deleted_at.is_some()
		|| filter
			.source_sandbox_id
			.is_some_and(|sandbox| template.source_sandbox_id.as_deref() != Some(sandbox))
	{
		return Ok(false);
	}
	if let Some(alias) = alias
		&& !r
			.list::<AliasRow>(&aliases_prefix(&template.id))
			.await?
			.iter()
			.any(|row| row.alias.to_lowercase() == alias)
	{
		return Ok(false);
	}
	Ok(!filter.ready_only || has_ready_build(r, &template.id).await?)
}

/// Sorts tags for listings: oldest first, then by tag.
fn sort_tags(tags: &mut [TagRow]) {
	tags.sort_by(|a, b| {
		a.created_at
			.cmp(&b.created_at)
			.then_with(|| a.tag.cmp(&b.tag))
	});
}

impl Store {
	/// Live (not deleted) template by id.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn template(&self, id: &str) -> Result<Option<TemplateRow>> {
		let template = self
			.read()
			.await?
			.get::<TemplateRow>(&template_key(id))
			.await?;
		Ok(template.filter(|t| t.deleted_at.is_none()))
	}

	/// Live template an alias points to (case-insensitive).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn template_by_alias(
		&self,
		namespace: &str,
		alias: &str,
	) -> Result<Option<TemplateRow>> {
		let r = self.read().await?;
		let Some(entry) = r.get::<AliasRow>(&alias_key(namespace, alias)).await? else {
			return Ok(None);
		};
		let template = r
			.get::<TemplateRow>(&template_key(&entry.template_id))
			.await?;
		Ok(template.filter(|t| t.deleted_at.is_none()))
	}

	/// Aliases of the given templates (namespaced before bare, then by alias).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn aliases_of(&self, template_ids: &[String]) -> Result<Vec<AliasRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let r = self.read().await?;
		let mut rows = Vec::new();
		for id in template_ids {
			rows.extend(r.list::<AliasRow>(&aliases_prefix(id)).await?);
		}
		rows.sort_by(|a, b| {
			b.namespace
				.cmp(&a.namespace)
				.then_with(|| a.alias.cmp(&b.alias))
		});
		Ok(rows)
	}

	/// A team's live templates of `kind`, newest first, keyset-paginated.
	/// `source_sandbox_id`/`alias` filter snapshot listings.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn list_templates(
		&self,
		team_id: &str,
		kind: TemplateKind,
		filter: &TemplateFilter<'_>,
		after: Option<&Cursor>,
		limit: Option<u32>,
	) -> Result<Vec<TemplateRow>> {
		let want = limit.map_or(usize::MAX, page_limit);
		let mut rows = Vec::new();
		if want == 0 {
			return Ok(rows);
		}
		let alias = filter.alias.map(str::to_lowercase);
		let page = want.min(LIST_PAGE);
		let mut range = Range {
			prefix:  prefix(BY_TEAM, &[team_id, kind.as_str()]),
			after:   after.map(|cursor| {
				let at = time_segment(from_micros(cursor.created_at_us));
				key(BY_TEAM, &[team_id, kind.as_str(), &at, &cursor.id])
			}),
			limit:   Some(page),
			reverse: true,
		};
		let r = self.read().await?;
		loop {
			let entries = r.scan::<String>(&range).await?;
			let done = entries.len() < page;
			range.after = entries.last().map(|(index, _)| index.clone());
			for (_, id) in entries {
				let Some(template) = r.get::<TemplateRow>(&template_key(&id)).await? else {
					continue;
				};
				if listed(&r, &template, filter, alias.as_deref()).await? {
					rows.push(template);
					if rows.len() >= want {
						return Ok(rows);
					}
				}
			}
			if done {
				return Ok(rows);
			}
		}
	}

	/// Sets `public`; `false` when the template does not exist. Bare aliases
	/// follow: publishing a system team's template claims the free bare
	/// aliases of its names, unpublishing releases the template's bare aliases.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn set_template_public(&self, id: &str, public: bool) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(mut template) = tx
				.get::<TemplateRow>(&template_key(id))
				.await?
				.filter(|t| t.deleted_at.is_none())
			else {
				return Ok(false);
			};
			if public {
				let team = tx.get::<Team>(&team_key(&template.team_id)).await?;
				if team.is_some_and(|team| team.system) {
					claim_bare_aliases(&mut tx, id, &mut HashSet::new()).await?;
				}
			} else {
				release_bare_aliases(&mut tx, id).await?;
			}
			template.public = public;
			template.updated_at = now();
			put_template(&mut tx, &template, false);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Soft-deletes a template: sets `deleted_at`, frees its aliases and tags,
	/// fails its unfinished builds, and queues its builds for artifact GC.
	/// Returns the builds that were `building` (the caller cancels them on
	/// their node); `None` when the template did not exist.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn soft_delete_template(&self, id: &str) -> Result<Option<Vec<BuildRow>>> {
		let mut tx = self.tx().await?;
		loop {
			let Some(mut template) = tx
				.get::<TemplateRow>(&template_key(id))
				.await?
				.filter(|t| t.deleted_at.is_none())
			else {
				return Ok(None);
			};
			let at = now();
			let mut building = Vec::new();
			for build in builds_of(&mut tx, id).await? {
				if build.status().is_terminal() {
					mark_pending_gc(&mut tx, &build);
					continue;
				}
				let failed = failed_build(&build, deleted_reason(), at);
				put_build(&mut tx, Some(&build), &failed);
				mark_pending_gc(&mut tx, &failed);
				if build.status() == BuildStatus::Building {
					building.push(build);
				}
			}
			for row in tx.list::<AliasRow>(&aliases_prefix(id)).await? {
				delete_alias(&mut tx, &row);
			}
			tx.delete_prefix(tags_prefix(id));
			tx.delete(team_index_key(&template));
			template.deleted_at = Some(at);
			put_template(&mut tx, &template, false);
			if tx.commit().await? {
				return Ok(Some(building));
			}
		}
	}

	/// Hidden fork templates created before `before` that were never retired
	/// (their fork's driver died midway).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn unretired_fork_templates(&self, before: DateTime<Utc>) -> Result<Vec<String>> {
		let r = self.read().await?;
		let mut ids = Vec::new();
		for id in r.list::<String>(&prefix(FORKS, &[])).await? {
			if let Some(template) = r.get::<TemplateRow>(&template_key(&id)).await?
				&& template.deleted_at.is_none()
				&& template.created_at < before
			{
				ids.push(template.id);
			}
		}
		Ok(ids)
	}

	/// Hard-deletes retired fork templates whose builds' artifacts are all
	/// collected (with their aliases, tags, builds, and build logs): nothing
	/// refers to a hidden fork once its artifacts are gone. Returns how many
	/// were deleted. Each fork goes in its own transaction.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn purge_collected_forks(&self) -> Result<u64> {
		let candidates: Vec<String> = self.read().await?.list(&prefix(FORKS, &[])).await?;
		let mut purged = 0;
		for id in candidates {
			let mut tx = self.tx().await?;
			loop {
				let Some(template) = tx
					.get::<TemplateRow>(&template_key(&id))
					.await?
					.filter(|t| t.deleted_at.is_some())
				else {
					break;
				};
				let builds = builds_of(&mut tx, &id).await?;
				if builds.iter().any(|build| build.artifacts_gc_at.is_none()) {
					break;
				}
				for build in &builds {
					delete_build(&mut tx, build);
				}
				for row in tx.list::<AliasRow>(&aliases_prefix(&id)).await? {
					delete_alias(&mut tx, &row);
				}
				tx.delete_prefix(tags_prefix(&id));
				tx.delete(team_index_key(&template));
				tx.delete(fork_key(&id));
				tx.delete(template_key(&id));
				if tx.commit().await? {
					purged += 1;
					break;
				}
			}
		}
		Ok(purged)
	}

	/// Adds `count` sandbox starts from the template, the last at
	/// `last_spawned_at` (never moving `last_spawned_at` back, since another
	/// driver may have written a later spawn first).
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn add_template_spawns(
		&self,
		id: &str,
		count: u64,
		last_spawned_at: DateTime<Utc>,
	) -> Result<()> {
		let count = i64::try_from(count).unwrap_or(i64::MAX);
		let last = last_spawned_at.trunc_subsecs(3);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut template) = tx.get::<TemplateRow>(&template_key(id)).await? else {
				return Ok(());
			};
			template.spawn_count = template.spawn_count.saturating_add(count);
			template.last_spawned_at = Some(template.last_spawned_at.map_or(last, |at| at.max(last)));
			put_template(&mut tx, &template, false);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Tags of a template, oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn template_tags(&self, template_id: &str) -> Result<Vec<TagRow>> {
		let mut tags = self
			.read()
			.await?
			.list::<TagRow>(&tags_prefix(template_id))
			.await?;
		sort_tags(&mut tags);
		Ok(tags)
	}

	/// Tags of several templates, oldest first.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn tags_of(&self, template_ids: &[String]) -> Result<Vec<TagRow>> {
		if template_ids.is_empty() {
			return Ok(Vec::new());
		}
		let r = self.read().await?;
		let mut tags = Vec::new();
		for id in template_ids {
			tags.extend(r.list::<TagRow>(&tags_prefix(id)).await?);
		}
		sort_tags(&mut tags);
		Ok(tags)
	}

	/// Build a tag points to.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn tag_build(&self, template_id: &str, tag: &str) -> Result<Option<String>> {
		let row = self
			.read()
			.await?
			.get::<TagRow>(&tag_key(template_id, tag))
			.await?;
		Ok(row.map(|row| row.build_id))
	}

	/// Points `tags` at `build_id` (insert or move); nothing changes for a
	/// deleted template.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn assign_tags(
		&self,
		template_id: &str,
		build_id: &str,
		tags: &[String],
	) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			let template = tx.get::<TemplateRow>(&template_key(template_id)).await?;
			if template.is_none_or(|t| t.deleted_at.is_some()) {
				return Ok(());
			}
			upsert_tags(&mut tx, template_id, build_id, tags);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Removes tags; returns how many existed.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn delete_tags(&self, template_id: &str, tags: &[String]) -> Result<u64> {
		if tags.is_empty() {
			return Ok(0);
		}
		let keys: HashSet<String> = tags.iter().map(|tag| tag_key(template_id, tag)).collect();
		let mut tx = self.tx().await?;
		loop {
			let mut deleted = 0;
			for tag in &keys {
				if tx.get::<TagRow>(tag).await?.is_some() {
					tx.delete(tag.as_str());
					deleted += 1;
				}
			}
			if tx.commit().await? {
				return Ok(deleted);
			}
		}
	}

	/// `POST /v3/templates`: reuses the team's template for the alias (or
	/// creates it with its alias), supersedes earlier `waiting` builds sharing
	/// a tag, enforces the team build limit, and inserts a `waiting` build.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn register_build(&self, req: &RegisterBuild<'_>) -> Result<RegisterOutcome> {
		let unfinished = unfinished_prefix(req.team_id, KIND_BUILD);
		let limit = u64::from(req.max_concurrent_builds);
		let mut tx = self.tx().await?;
		loop {
			let at = now();
			let (mut template, created) =
				match resolve_alias(&mut tx, req.namespace, req.alias).await? {
					(_, Some(template))
						if template.team_id == req.team_id && template.is(TemplateKind::Template) =>
					{
						(template, false)
					},
					(Some(_), _) => return Ok(RegisterOutcome::AliasTaken),
					(None, _) => (
						new_template(req.new_template_id, req.team_id, TemplateKind::Template, None, at),
						true,
					),
				};
			if created {
				put_alias(&mut tx, &AliasRow {
					namespace:   req.namespace.to_owned(),
					alias:       req.alias.to_owned(),
					template_id: template.id.clone(),
				});
			}

			// Supersede, then count what stays in progress. The listed builds
			// are guarded and `CountBelow` keeps the set from growing, so the
			// decision still holds at commit.
			let ids: Vec<String> = tx.list(&unfinished).await?;
			let unfinished_count = u64::try_from(ids.len()).unwrap_or(u64::MAX);
			let mut active = unfinished_count;
			for build_id in ids {
				let Some(build) = tx.get::<BuildRow>(&build_key(&build_id)).await? else {
					continue;
				};
				if build.template_id == template.id
					&& build.status() == BuildStatus::Waiting
					&& overlaps(&build.tags, req.tags)
				{
					put_build(&mut tx, Some(&build), &failed_build(&build, superseded_reason(), at));
					active -= 1;
				}
			}
			if active >= limit {
				return Ok(RegisterOutcome::ConcurrencyLimit { limit: req.max_concurrent_builds });
			}
			tx.require(Compare::CountBelow {
				prefix: unfinished.clone(),
				limit:  unfinished_count + 1,
			});

			put_build(&mut tx, None, &BuildRow {
				id:               req.build_id.to_owned(),
				template_id:      template.id.clone(),
				team_id:          req.team_id.to_owned(),
				status:           BuildStatus::Waiting.as_str().into(),
				kind:             KIND_BUILD.into(),
				vcpus:            req.vcpus,
				memory_mb:        req.memory_mb,
				disk_mb:          req.disk_mb,
				min_free_disk_mb: req.min_free_disk_mb,
				envd_version:     None,
				arch:             None,
				node_id:          None,
				tags:             req.tags.to_vec(),
				spec:             None,
				template_version: None,
				start_cmd:        None,
				ready_cmd:        None,
				reason:           None,
				artifact:         None,
				created_at:       at,
				updated_at:       at,
				started_at:       None,
				finished_at:      None,
				artifacts_gc_at:  None,
			});
			template.updated_at = at;
			put_template(&mut tx, &template, created);
			if tx.commit().await? {
				return Ok(RegisterOutcome::Registered {
					template_id: template.id,
					public:      template.public,
				});
			}
		}
	}

	/// Snapshot-as-template / fork: reuses the team's snapshot template for a
	/// name (or creates the template, with its alias when named) and inserts a
	/// `building` snapshot build carrying `tag`.
	///
	/// # Errors
	///
	/// Store failures.
	pub async fn register_snapshot(&self, req: &RegisterSnapshot<'_>) -> Result<SnapshotOutcome> {
		let mut tx = self.tx().await?;
		loop {
			let at = now();
			let found = match req.name {
				Some((namespace, alias)) => resolve_alias(&mut tx, namespace, alias).await?,
				None => (None, None),
			};
			let (mut template, created) = match found {
				(_, Some(template)) if template.team_id == req.team_id && template.is(req.kind) => {
					(template, false)
				},
				(Some(_), _) => return Ok(SnapshotOutcome::AliasTaken),
				(None, _) => (
					new_template(
						req.new_template_id,
						req.team_id,
						req.kind,
						Some(req.source_sandbox_id),
						at,
					),
					true,
				),
			};
			if created {
				if let Some((namespace, alias)) = req.name {
					put_alias(&mut tx, &AliasRow {
						namespace:   namespace.to_owned(),
						alias:       alias.to_owned(),
						template_id: template.id.clone(),
					});
				}
			} else {
				template.source_sandbox_id = Some(req.source_sandbox_id.to_owned());
				template.updated_at = at;
			}
			template.build_count += 1;
			put_template(&mut tx, &template, created);
			put_build(&mut tx, None, &BuildRow {
				id:               req.build_id.to_owned(),
				template_id:      template.id.clone(),
				team_id:          req.team_id.to_owned(),
				status:           BuildStatus::Building.as_str().into(),
				kind:             KIND_SNAPSHOT.into(),
				vcpus:            req.vcpus,
				memory_mb:        req.memory_mb,
				disk_mb:          req.disk_mb,
				min_free_disk_mb: None,
				envd_version:     Some(req.envd_version.to_owned()),
				arch:             None,
				node_id:          Some(req.node_id.to_owned()),
				tags:             vec![req.tag.to_owned()],
				spec:             None,
				template_version: None,
				start_cmd:        None,
				ready_cmd:        None,
				reason:           None,
				artifact:         None,
				created_at:       at,
				updated_at:       at,
				started_at:       Some(at),
				finished_at:      None,
				artifacts_gc_at:  None,
			});
			if tx.commit().await? {
				return Ok(SnapshotOutcome::Registered { template_id: template.id });
			}
		}
	}
}

#[cfg(test)]
impl Store {
	/// Every stored template record, soft-deleted ones included (tests).
	pub(crate) async fn stored_templates(&self) -> Result<Vec<TemplateRow>> {
		self.read().await?.list(&prefix(TEMPLATE, &[])).await
	}
}

#[cfg(test)]
mod tests;
