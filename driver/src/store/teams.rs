//! Teams and team memberships.
//!
//! Keys:
//! - `db/team/<id>` → [`Team`] (read by other store modules via [`team_key`]).
//! - `db/team.slug/<slug>` → team id (JSON string); unique slug index.
//!   [`Store::create_team`] guards it absent and moves on to the next suffixed
//!   candidate when it is taken; [`Store::delete_team`] frees it.
//! - `db/member/<team>/<user>` → `Member` (`is_default`, `created_at`).
//! - `db/member.user/<user>/<team>` → empty; reverse index of the user's
//!   memberships, written and deleted together with the member record.
//!   [`Store::add_team_member`] guards the user's memberships it reads when
//!   clearing their default flags.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{Error, Result, Store, key, last_segment, prefix};
use crate::state::TeamLimits;

/// Slug candidates [`Store::create_team`] tries (`slug`, `slug-2`, …).
const SLUG_ATTEMPTS: u32 = 100;

/// A tenant (E2B "team"/"project"); every API-key and sandbox belongs to one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Team {
	/// UUID v4.
	pub id:         String,
	pub name:       String,
	/// Unique URL-safe name (template namespace).
	pub slug:       String,
	pub limits:     TeamLimits,
	/// Blocked teams may read but not mutate (403 `team is blocked`).
	pub blocked:    bool,
	/// System teams publish the default templates: their public templates
	/// claim bare aliases, which every team resolves after its own namespace.
	pub system:     bool,
	pub created_at: DateTime<Utc>,
}

/// Membership of a user in a team.
#[derive(Serialize, Deserialize)]
struct Member {
	/// The user's default team (at most one flagged per user).
	is_default: bool,
	created_at: DateTime<Utc>,
}

/// Key of the [`Team`] record `id`.
pub(super) fn team_key(id: &str) -> String {
	key("db/team", &[id])
}

fn slug_key(slug: &str) -> String {
	key("db/team.slug", &[slug])
}

fn member_key(team_id: &str, user_id: &str) -> String {
	key("db/member", &[team_id, user_id])
}

fn user_member_key(user_id: &str, team_id: &str) -> String {
	key("db/member.user", &[user_id, team_id])
}

/// A new team named `name`, without a slug yet.
fn fresh_team(name: &str, limits: TeamLimits) -> Team {
	Team {
		id: uuid::Uuid::new_v4().to_string(),
		name: name.to_owned(),
		slug: String::new(),
		limits,
		blocked: false,
		system: false,
		created_at: super::now(),
	}
}

/// Lower-case, `[a-z0-9-]` slug of a team name (never empty).
pub fn slugify(name: &str) -> String {
	let mut slug = String::with_capacity(name.len());
	for ch in name.chars() {
		if ch.is_ascii_alphanumeric() {
			slug.push(ch.to_ascii_lowercase());
		} else if !slug.ends_with('-') && !slug.is_empty() {
			slug.push('-');
		}
	}
	while slug.ends_with('-') {
		slug.pop();
	}
	if slug.is_empty() {
		String::from("team")
	} else {
		slug
	}
}

impl Store {
	/// Inserts a team with a unique slug derived from `name` (suffixing
	/// `-2`, `-3`, … on collision).
	///
	/// # Errors
	///
	/// [`Error::Duplicate`] when every slug candidate is taken; store
	/// failures.
	pub async fn create_team(&self, name: &str, limits: TeamLimits) -> Result<Team> {
		let base = slugify(name);
		let mut team = fresh_team(name, limits);
		let mut tx = self.tx().await?;
		'commit: loop {
			for attempt in 1..=SLUG_ATTEMPTS {
				let slug = if attempt == 1 {
					base.clone()
				} else {
					format!("{base}-{attempt}")
				};
				let index = slug_key(&slug);
				if tx.get::<String>(&index).await?.is_some() {
					continue;
				}
				team.slug = slug;
				tx.put(team_key(&team.id), &team);
				tx.put(index, &team.id);
				if tx.commit().await? {
					return Ok(team);
				}
				continue 'commit;
			}
			return Err(Error::Duplicate("team slug"));
		}
	}

	/// The team holding the slug of `name`, or a new team with that slug and
	/// `limits`; `true` when it was created. Never suffixes the slug, so reruns
	/// of `vmon admin create-team --if-missing` find the same team.
	pub async fn ensure_team(&self, name: &str, limits: TeamLimits) -> Result<(Team, bool)> {
		let mut team = fresh_team(name, limits);
		team.slug = slugify(name);
		let index = slug_key(&team.slug);
		let mut tx = self.tx().await?;
		loop {
			if let Some(id) = tx.get::<String>(&index).await?
				&& let Some(existing) = tx.get::<Team>(&team_key(&id)).await?
			{
				return Ok((existing, false));
			}
			tx.put(team_key(&team.id), &team);
			tx.put(index.as_str(), &team.id);
			if tx.commit().await? {
				return Ok((team, true));
			}
		}
	}

	/// Removes the team record, its memberships, and its slug (free for the
	/// next [`Store::create_team`]); `false` when the team does not exist.
	/// Records the team owns elsewhere (keys, templates, volumes, secrets,
	/// webhooks) must be removed first: see `admin::delete_team`.
	pub async fn delete_team(&self, id: &str) -> Result<bool> {
		let key = team_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(team) = tx.get::<Team>(&key).await? else {
				return Ok(false);
			};
			for member in tx.keys(&prefix("db/member", &[id])).await? {
				tx.delete(user_member_key(&last_segment(&member), id));
				tx.delete(member);
			}
			let index = slug_key(&team.slug);
			if tx.get::<String>(&index).await?.as_deref() == Some(id) {
				tx.delete(index);
			}
			tx.delete(key.as_str());
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Team by id.
	pub async fn team_by_id(&self, id: &str) -> Result<Option<Team>> {
		self.read().await?.get(&team_key(id)).await
	}

	/// Team by id or (case-insensitive) slug (admin tooling accepts either).
	pub async fn team_by_ref(&self, id_or_slug: &str) -> Result<Option<Team>> {
		let r = self.read().await?;
		if let Some(team) = r.get(&team_key(id_or_slug)).await? {
			return Ok(Some(team));
		}
		let Some(id) = r
			.get::<String>(&slug_key(&id_or_slug.to_lowercase()))
			.await?
		else {
			return Ok(None);
		};
		r.get(&team_key(&id)).await
	}

	/// Every team, oldest first.
	pub async fn list_teams(&self) -> Result<Vec<Team>> {
		let mut teams: Vec<Team> = self.read().await?.list(&prefix("db/team", &[])).await?;
		teams.sort_by(|a, b| {
			a.created_at
				.cmp(&b.created_at)
				.then_with(|| a.id.cmp(&b.id))
		});
		Ok(teams)
	}

	/// Replaces a team's limits; `false` when the team does not exist.
	pub async fn set_team_limits(&self, id: &str, limits: TeamLimits) -> Result<bool> {
		self.update_team(id, |team| team.limits = limits).await
	}

	/// Blocks or unblocks a team; `false` when the team does not exist.
	pub async fn set_team_blocked(&self, id: &str, blocked: bool) -> Result<bool> {
		self.update_team(id, |team| team.blocked = blocked).await
	}

	/// Marks or unmarks a team as a system team; `false` when the team does
	/// not exist. Marking claims the free bare aliases of its public
	/// templates; unmarking releases the bare aliases of its templates.
	pub async fn set_team_system(&self, id: &str, system: bool) -> Result<bool> {
		let key = team_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut team) = tx.get::<Team>(&key).await? else {
				return Ok(false);
			};
			team.system = system;
			tx.put(key.as_str(), &team);
			super::templates::sync_system_aliases(&mut tx, &team, system).await?;
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Applies `change` to the team record `id`; `false` when it does not
	/// exist.
	async fn update_team(&self, id: &str, change: impl Fn(&mut Team)) -> Result<bool> {
		let key = team_key(id);
		let mut tx = self.tx().await?;
		loop {
			let Some(mut team) = tx.get::<Team>(&key).await? else {
				return Ok(false);
			};
			change(&mut team);
			tx.put(key.as_str(), &team);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Adds (or updates) a membership. Making it the default clears the
	/// user's other default flags.
	pub async fn add_team_member(
		&self,
		team_id: &str,
		user_id: &str,
		is_default: bool,
	) -> Result<()> {
		let key = member_key(team_id, user_id);
		let mut tx = self.tx().await?;
		loop {
			if is_default {
				for index in tx.keys(&prefix("db/member.user", &[user_id])).await? {
					let other = last_segment(&index);
					if other == team_id {
						continue;
					}
					let other_key = member_key(&other, user_id);
					if let Some(mut member) = tx.get::<Member>(&other_key).await?
						&& member.is_default
					{
						member.is_default = false;
						tx.put(other_key, &member);
					}
				}
			}
			let created_at = tx
				.get::<Member>(&key)
				.await?
				.map_or_else(super::now, |m| m.created_at);
			tx.put(key.as_str(), &Member { is_default, created_at });
			tx.mark(user_member_key(user_id, team_id), None);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Teams the user belongs to with their `is_default` flag, oldest
	/// membership first. When no membership is flagged default, the oldest
	/// membership is reported as default so the CLI always finds one.
	pub async fn user_teams(&self, user_id: &str) -> Result<Vec<(Team, bool)>> {
		let r = self.read().await?;
		let mut rows = Vec::new();
		for index in r.keys(&prefix("db/member.user", &[user_id])).await? {
			let team_id = last_segment(&index);
			let Some(member) = r.get::<Member>(&member_key(&team_id, user_id)).await? else {
				continue;
			};
			let Some(team) = r.get::<Team>(&team_key(&team_id)).await? else {
				continue;
			};
			rows.push((member, team));
		}
		rows.sort_by(|(ma, ta), (mb, tb)| {
			ma.created_at
				.cmp(&mb.created_at)
				.then_with(|| ta.id.cmp(&tb.id))
		});
		let any_default = rows.iter().any(|(m, _)| m.is_default);
		Ok(rows
			.into_iter()
			.enumerate()
			.map(|(i, (m, team))| (team, m.is_default || (!any_default && i == 0)))
			.collect())
	}

	/// The team `team_id` if `user_id` is a member of it.
	pub async fn user_team(&self, user_id: &str, team_id: &str) -> Result<Option<Team>> {
		let r = self.read().await?;
		if r
			.get::<Member>(&member_key(team_id, user_id))
			.await?
			.is_none()
		{
			return Ok(None);
		}
		r.get(&team_key(team_id)).await
	}

	/// The user's default team (see [`Store::user_teams`] for the fallback).
	pub async fn default_team(&self, user_id: &str) -> Result<Option<Team>> {
		Ok(self
			.user_teams(user_id)
			.await?
			.into_iter()
			.find(|(_, is_default)| *is_default)
			.map(|(team, _)| team))
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::store::testing::TestStore;

	/// Lets [`super::super::now`] advance, so records created next sort after.
	async fn tick() {
		tokio::time::sleep(Duration::from_millis(3)).await;
	}

	#[test]
	fn slugify_normalizes_and_never_returns_empty() {
		assert_eq!(slugify("My Team!"), "my-team");
		assert_eq!(slugify("  --a__b--  "), "a-b");
		assert_eq!(slugify("ÄÖÜ"), "team");
	}

	#[tokio::test]
	async fn create_team_suffixes_taken_slugs() {
		let t = TestStore::start().await;
		let store = &t.store;
		let first = store
			.create_team("Acme", TeamLimits::default())
			.await
			.unwrap();
		tick().await;
		let second = store
			.create_team("ACME", TeamLimits::default())
			.await
			.unwrap();
		tick().await;
		let third = store
			.create_team("acme!", TeamLimits::default())
			.await
			.unwrap();
		let slugs = [&first.slug, &second.slug, &third.slug];
		assert_eq!(slugs, ["acme", "acme-2", "acme-3"]);
		assert_eq!(store.team_by_ref("ACME-2").await.unwrap(), Some(second.clone()));
		assert_eq!(store.team_by_ref(&third.id).await.unwrap(), Some(third.clone()));
		let ids: Vec<_> = store
			.list_teams()
			.await
			.unwrap()
			.into_iter()
			.map(|t| t.id)
			.collect();
		assert_eq!(ids, [first.id, second.id, third.id]);
		t.finish().await;
	}

	#[tokio::test]
	async fn ensure_team_reuses_its_slug_and_delete_frees_it() {
		let t = TestStore::start().await;
		let store = &t.store;
		let (team, created) = store
			.ensure_team("Acme", TeamLimits::default())
			.await
			.unwrap();
		assert!(created);
		let rerun = store
			.ensure_team("ACME!", TeamLimits::default())
			.await
			.unwrap();
		assert_eq!(rerun, (team.clone(), false));
		let user = store.create_user("dev@example.com", "hash").await.unwrap();
		store
			.add_team_member(&team.id, &user.id, true)
			.await
			.unwrap();

		assert!(store.delete_team(&team.id).await.unwrap());
		assert!(!store.delete_team(&team.id).await.unwrap());
		assert_eq!(store.team_by_ref("acme").await.unwrap(), None);
		let r = store.read().await.unwrap();
		assert_eq!(r.keys(&prefix("db/member", &[&team.id])).await.unwrap(), Vec::<String>::new());
		assert_eq!(
			r.keys(&prefix("db/member.user", &[&user.id]))
				.await
				.unwrap(),
			Vec::<String>::new()
		);
		let reborn = store
			.create_team("Acme", TeamLimits::default())
			.await
			.unwrap();
		assert_eq!(reborn.slug, "acme");
		t.finish().await;
	}

	#[tokio::test]
	async fn memberships_track_one_default_with_fallback() {
		let t = TestStore::start().await;
		let store = &t.store;
		let a = store.create_team("A", TeamLimits::default()).await.unwrap();
		let b = store.create_team("B", TeamLimits::default()).await.unwrap();
		let user = store.create_user("Dev@Example.com", "hash").await.unwrap();
		assert!(matches!(
			store.create_user("dev@example.COM ", "hash").await,
			Err(Error::Duplicate(_))
		));

		store.add_team_member(&a.id, &user.id, false).await.unwrap();
		tick().await;
		store.add_team_member(&b.id, &user.id, false).await.unwrap();
		assert_eq!(store.default_team(&user.id).await.unwrap(), Some(a.clone()), "oldest fallback");

		store.add_team_member(&b.id, &user.id, true).await.unwrap();
		store.add_team_member(&a.id, &user.id, true).await.unwrap();
		let teams = store.user_teams(&user.id).await.unwrap();
		assert_eq!(teams, [(a.clone(), true), (b.clone(), false)]);
		assert_eq!(store.user_team(&user.id, &b.id).await.unwrap(), Some(b));
		assert_eq!(store.user_team("nobody", &a.id).await.unwrap(), None);
		t.finish().await;
	}
}
