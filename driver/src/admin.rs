//! Operator actions behind `vmon admin …` (wired by the `vmon` binary) and
//! shared by the API handlers that mint keys or delete teams.
//!
//! Most actions only touch the store, which `vmon admin` reaches through the
//! cluster KV. [`delete_team`] also kills sandboxes and deletes node data, so
//! it runs on a driver: `vmon admin delete-team` calls it through
//! [`request_team_delete`].

use std::{fmt::Write, time::Duration};

use anyhow::{Context, bail};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
	api, builds,
	crypto::{self, Keys},
	error::{ApiError, ApiResult},
	lifecycle,
	state::{AppState, TeamLimits},
	store::{self, ApiKeyRow, NewApiKey, Store, Team, User},
};

/// Name of the key minted together with a new team.
const DEFAULT_KEY_NAME: &str = "Default API key";

/// Most sandbox or build IDs a refused [`delete_team`] names.
const NAMED_BLOCKERS: usize = 10;

/// Build failure reason recorded when [`delete_team`] cancels a build.
const DELETED_TEAM_REASON: &str = "The build was cancelled because its team was deleted.";

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
	let team = store
		.create_team(team_name(name)?, limits)
		.await
		.context("creating team")?;
	let (_, key) = create_api_key(store, keys, &team.id, DEFAULT_KEY_NAME, None)
		.await
		.context("creating API key")?;
	Ok((team, key))
}

/// [`create_team`] unless a team already holds the slug of `name`: that team
/// is returned unchanged, without an API key (`create-team --if-missing`).
pub async fn ensure_team(
	store: &Store,
	keys: &Keys,
	name: &str,
	limits: TeamLimits,
) -> anyhow::Result<(Team, Option<String>)> {
	let (team, created) = store
		.ensure_team(team_name(name)?, limits)
		.await
		.context("creating team")?;
	if !created {
		return Ok((team, None));
	}
	let (_, key) = create_api_key(store, keys, &team.id, DEFAULT_KEY_NAME, None)
		.await
		.context("creating API key")?;
	Ok((team, Some(key)))
}

fn team_name(name: &str) -> anyhow::Result<&str> {
	let name = name.trim();
	if name.is_empty() {
		bail!("team name must not be empty");
	}
	Ok(name)
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

/// What [`delete_team`] removed with a team (`DELETE /admin/teams/{teamID}`
/// body).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeletedTeam {
	#[serde(rename = "teamID")]
	pub team_id:          String,
	pub slug:             String,
	pub killed_sandboxes: u64,
	pub cancelled_builds: u64,
	pub templates:        u64,
	pub volumes:          u64,
	pub secrets:          u64,
	pub webhooks:         u64,
	pub api_keys:         u64,
}

/// Deletes a team and everything it owns, freeing its slug.
///
/// Removes, in order: sandboxes, builds, templates (soft-deleted; the build
/// GC collects their artifacts once unused), volumes with their data,
/// secrets, webhooks, API keys, and finally memberships and the team record.
///
/// Without `force` the delete is refused while the team has running or paused
/// sandboxes or unfinished builds, and for a system team (other teams resolve
/// its public templates). The team is blocked for the teardown, so a delete
/// that fails midway leaves a blocked team that a rerun finishes.
///
/// # Errors
///
/// 409 naming the blockers when not forced; 503 when some sandboxes could not
/// be killed; store, KV, and node failures.
pub async fn delete_team(state: &AppState, team: &Team, force: bool) -> ApiResult<DeletedTeam> {
	if !force {
		if team.system {
			return Err(ApiError::conflict(format!(
				"team '{}' is a system team whose public templates other teams use; force the delete \
				 to remove it",
				team.slug
			)));
		}
		refuse_live(state, team).await?;
	}
	state.store.set_team_blocked(&team.id, true).await?;
	state.auth_cache.invalidate_team(&team.id);
	if !force && let Err(error) = refuse_live(state, team).await {
		// Something started before the block took effect.
		if !team.blocked {
			state.store.set_team_blocked(&team.id, false).await?;
		}
		return Err(error);
	}

	let mut deleted =
		DeletedTeam { team_id: team.id.clone(), slug: team.slug.clone(), ..DeletedTeam::default() };
	let (killed, failed) = lifecycle::kill_team(state, &team.id).await?;
	deleted.killed_sandboxes = killed;
	if failed > 0 {
		return Err(ApiError::unavailable(format!(
			"{failed} sandboxes of team '{}' could not be killed; the team stays blocked, retry the \
			 delete",
			team.slug
		)));
	}
	for build in state.store.team_unfinished_builds(&team.id).await? {
		if builds::cancel_build(state, &build, DELETED_TEAM_REASON).await {
			deleted.cancelled_builds += 1;
		}
	}
	for id in state.store.team_template_ids(&team.id).await? {
		match builds::delete_template(state, &id).await {
			Ok(()) => deleted.templates += 1,
			Err(error) if error.status == StatusCode::NOT_FOUND => {},
			Err(error) => return Err(error),
		}
	}
	for volume in state.store.list_volumes(&team.id).await? {
		api::volumes::remove_volume(state, &volume).await?;
		deleted.volumes += 1;
	}
	for secret in state.store.list_secrets(&team.id, None, u32::MAX).await? {
		if state.store.delete_secret(&team.id, &secret.id).await? {
			deleted.secrets += 1;
		}
	}
	for hook in state.store.list_webhooks(&team.id).await? {
		if state.store.delete_webhook(&team.id, &hook.id).await? {
			deleted.webhooks += 1;
		}
	}
	for key in state.store.list_api_keys(&team.id).await? {
		if let Some(hash) = state.store.delete_api_key(&team.id, &key.id).await? {
			state.auth_cache.invalidate_key(&hash);
			deleted.api_keys += 1;
		}
	}
	state.store.delete_team(&team.id).await?;
	tracing::info!(team = %team.id, slug = %team.slug, ?deleted, "deleted team");
	Ok(deleted)
}

/// 409 naming the team's live sandboxes and unfinished builds, if any.
async fn refuse_live(state: &AppState, team: &Team) -> ApiResult<()> {
	let sandboxes = lifecycle::team_sandbox_ids(state, &team.id).await?;
	let builds: Vec<String> = state
		.store
		.team_unfinished_builds(&team.id)
		.await?
		.into_iter()
		.map(|build| build.id)
		.collect();
	let mut blockers = Vec::new();
	if !sandboxes.is_empty() {
		blockers.push(format!("running or paused sandboxes {}", named(&sandboxes)));
	}
	if !builds.is_empty() {
		blockers.push(format!("unfinished builds {}", named(&builds)));
	}
	if blockers.is_empty() {
		return Ok(());
	}
	Err(ApiError::conflict(format!(
		"team '{}' has {}; force the delete to kill and cancel them",
		team.slug,
		blockers.join(" and ")
	)))
}

/// `ids` as a comma-separated list of at most [`NAMED_BLOCKERS`] entries.
fn named(ids: &[String]) -> String {
	let mut list = ids
		.iter()
		.take(NAMED_BLOCKERS)
		.map(String::as_str)
		.collect::<Vec<_>>()
		.join(", ");
	if ids.len() > NAMED_BLOCKERS {
		let _ = write!(list, " and {} more", ids.len() - NAMED_BLOCKERS);
	}
	list
}

/// `vmon admin delete-team`: runs [`delete_team`] on a driver.
///
/// Sends `DELETE /driver/v1/teams/{team}` (team by id or slug; see
/// [`crate::Driver::cluster_router`]) to the first reachable driver cluster
/// listener among `cluster_urls`.
///
/// # Errors
///
/// No driver reachable; the driver's refusal or failure message.
pub async fn request_team_delete(
	cluster_urls: &[String],
	secret: &str,
	team: &str,
	force: bool,
) -> anyhow::Result<DeletedTeam> {
	let query = [("force", force.to_string())];
	let request = ClusterRequest {
		method: reqwest::Method::DELETE,
		path:   &["teams", team],
		query:  &query,
		body:   None,
	};
	cluster_json(cluster_urls, secret, &request).await
}

/// [`cluster_call`] decoding a JSON reply.
///
/// # Errors
///
/// As [`cluster_call`]; a reply that is not `T`.
pub async fn cluster_json<T: serde::de::DeserializeOwned>(
	cluster_urls: &[String],
	secret: &str,
	request: &ClusterRequest<'_>,
) -> anyhow::Result<T> {
	cluster_call(cluster_urls, secret, request)
		.await?
		.json()
		.await
		.context("reading the driver's reply")
}

/// [`cluster_call`] returning the reply body as text (empty for 204).
///
/// # Errors
///
/// As [`cluster_call`].
pub async fn cluster_text(
	cluster_urls: &[String],
	secret: &str,
	request: &ClusterRequest<'_>,
) -> anyhow::Result<String> {
	cluster_call(cluster_urls, secret, request)
		.await?
		.text()
		.await
		.context("reading the driver's reply")
}

pub use reqwest::Method;

/// One `/driver/v1/…` request of `vmon admin` ([`cluster_call`]).
pub struct ClusterRequest<'a> {
	pub method: reqwest::Method,
	/// Path segments below `/driver/v1` (each escaped).
	pub path:   &'a [&'a str],
	pub query:  &'a [(&'a str, String)],
	pub body:   Option<&'a serde_json::Value>,
}

/// Sends `request` to the first reachable driver cluster listener among
/// `cluster_urls` (cluster-secret bearer) and returns its 2xx response; else
/// fails with no driver reachable or the driver's refusal or failure message.
async fn cluster_call(
	cluster_urls: &[String],
	secret: &str,
	request: &ClusterRequest<'_>,
) -> anyhow::Result<reqwest::Response> {
	#[derive(Deserialize)]
	struct ErrorBody {
		message: String,
	}

	let http = reqwest::Client::builder()
		.connect_timeout(Duration::from_secs(2))
		.build()
		.context("building http client")?;
	let mut unreachable = Vec::new();
	for base in cluster_urls {
		let mut url = reqwest::Url::parse(base).with_context(|| format!("cluster URL {base}"))?;
		url.path_segments_mut()
			.map_err(|()| anyhow::anyhow!("cluster URL {base} cannot take a path"))?
			.pop_if_empty()
			.extend(["driver", "v1"])
			.extend(request.path);
		let mut call = http
			.request(request.method.clone(), url)
			.query(request.query)
			.bearer_auth(secret);
		if let Some(body) = request.body {
			call = call.json(body);
		}
		let response = match call.send().await {
			Ok(response) => response,
			Err(error) if error.is_connect() => {
				unreachable.push(format!("{base}: {error}"));
				continue;
			},
			Err(error) => return Err(error).with_context(|| format!("calling {base}")),
		};
		let status = response.status();
		if status.is_success() {
			return Ok(response);
		}
		let message = match response.json::<ErrorBody>().await {
			Ok(body) => body.message,
			Err(_) if status == StatusCode::UNAUTHORIZED => {
				format!("{base} rejected the cluster secret")
			},
			Err(_) if status == StatusCode::NOT_FOUND => {
				format!("{base} serves no driver API (is it a driver's cluster listener?)")
			},
			Err(_) => format!("{base} answered {status}"),
		};
		bail!("{message}");
	}
	bail!("no driver reachable: {}", unreachable.join("; "))
}
