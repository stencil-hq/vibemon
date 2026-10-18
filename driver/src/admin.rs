//! Operator actions behind `vmon admin …` (wired by the `vmon` binary) and
//! shared by the API handlers that mint keys.

use anyhow::{Context, bail};

use crate::{
	crypto::{self, Keys},
	state::TeamLimits,
	store::{self, ApiKeyRow, NewApiKey, Store, Team, User},
};

/// Name of the key minted together with a new team.
const DEFAULT_KEY_NAME: &str = "Default API key";

/// Generates, stores, and returns a new API key `(row, plaintext)`.
pub async fn create_api_key(
	store: &Store,
	keys: &Keys,
	team_id: &str,
	name: &str,
	created_by: Option<&str>,
) -> store::Result<(ApiKeyRow, String)> {
	let key = crypto::generate_api_key();
	let hash = crypto::hash_api_key(&key).expect("generated keys are well-formed");
	let (mask_prefix, mask_suffix) = crypto::mask_api_key(&key);
	let enc = keys.encrypt_str(&key);
	let row = store
		.insert_api_key(&NewApiKey {
			team_id,
			name,
			created_by,
			key_hash: &hash,
			key_enc: &enc,
			mask_prefix: &mask_prefix,
			mask_suffix: &mask_suffix,
		})
		.await?;
	Ok((row, key))
}

/// Creates a team with `limits` and its first API key; returns both.
pub async fn create_team(
	store: &Store,
	keys: &Keys,
	name: &str,
	limits: TeamLimits,
) -> anyhow::Result<(Team, String)> {
	let name = name.trim();
	if name.is_empty() {
		bail!("team name must not be empty");
	}
	let team = store
		.create_team(name, limits)
		.await
		.context("creating team")?;
	let (_, key) = create_api_key(store, keys, &team.id, DEFAULT_KEY_NAME, None)
		.await
		.context("creating API key")?;
	Ok((team, key))
}

/// Creates a CLI user and, when `team` (id or slug) is given, makes it the
/// user's default team.
pub async fn create_user(
	store: &Store,
	email: &str,
	password: &str,
	team: Option<&str>,
) -> anyhow::Result<User> {
	let email = email.trim();
	if !email.contains('@') {
		bail!("invalid email address: {email}");
	}
	if password.len() < 8 {
		bail!("password must be at least 8 characters");
	}
	let team = match team {
		Some(reference) => Some(
			store
				.team_by_ref(reference)
				.await?
				.with_context(|| format!("team {reference} not found"))?,
		),
		None => None,
	};
	if store.user_by_email(email).await?.is_some() {
		bail!("a user with email {email} already exists");
	}
	let user = store
		.create_user(email, &crypto::hash_password(password))
		.await
		.context("creating user")?;
	if let Some(team) = team {
		store
			.add_team_member(&team.id, &user.id, true)
			.await
			.context("adding team membership")?;
	}
	Ok(user)
}

/// Adds an existing user to a team (id or slug).
pub async fn add_member(
	store: &Store,
	email: &str,
	team: &str,
	make_default: bool,
) -> anyhow::Result<()> {
	let user = store
		.user_by_email(email)
		.await?
		.with_context(|| format!("user {email} not found"))?;
	let team = store
		.team_by_ref(team)
		.await?
		.with_context(|| format!("team {team} not found"))?;
	store
		.add_team_member(&team.id, &user.id, make_default)
		.await?;
	Ok(())
}

/// Mints an additional API key for a team (id or slug).
pub async fn create_team_api_key(
	store: &Store,
	keys: &Keys,
	team: &str,
	name: &str,
) -> anyhow::Result<String> {
	let team = store
		.team_by_ref(team)
		.await?
		.with_context(|| format!("team {team} not found"))?;
	let (_, key) = create_api_key(store, keys, &team.id, name, None).await?;
	Ok(key)
}

/// Every team.
pub async fn list_teams(store: &Store) -> anyhow::Result<Vec<Team>> {
	Ok(store.list_teams().await?)
}

/// Replaces a team's limits (team by id or slug); returns the updated team.
pub async fn set_team_limits(
	store: &Store,
	team: &str,
	limits: TeamLimits,
) -> anyhow::Result<Team> {
	let found = store
		.team_by_ref(team)
		.await?
		.with_context(|| format!("team {team} not found"))?;
	store.set_team_limits(&found.id, limits).await?;
	store.team_by_id(&found.id).await?.context("team vanished")
}

/// Blocks or unblocks a team (id or slug).
pub async fn set_team_blocked(store: &Store, team: &str, blocked: bool) -> anyhow::Result<Team> {
	let found = store
		.team_by_ref(team)
		.await?
		.with_context(|| format!("team {team} not found"))?;
	store.set_team_blocked(&found.id, blocked).await?;
	store.team_by_id(&found.id).await?.context("team vanished")
}

/// Marks or unmarks a team (id or slug) as a system team (publisher of the
/// default templates; see [`Team::system`]).
pub async fn set_team_system(store: &Store, team: &str, system: bool) -> anyhow::Result<Team> {
	let found = store
		.team_by_ref(team)
		.await?
		.with_context(|| format!("team {team} not found"))?;
	store.set_team_system(&found.id, system).await?;
	store.team_by_id(&found.id).await?.context("team vanished")
}
